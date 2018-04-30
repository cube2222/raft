package raft

import (
	"log"

	"github.com/cube2222/raft"
	"github.com/pkg/errors"
)

// TODO: Add the election timeout everywhere
type Raft struct {
	config *raft.Config

	commitIndex int
	lastApplied int

	cluster   raft.Cluster
	commitLog raft.CommitLog
	termState raft.TermState

	applyEntries chan *raft.Entry
}

func (r *Raft) Run() {

	for {
		if err := r.loop(); err != nil {
			log.Printf("Error while looping: Term State: %v, Error: %v", getTermDebugInfo(r.termState), err)
		}
	}
}

func getTermDebugInfo(state raft.TermState) string {
	return ""
}

func (r *Raft) loop() error {
	if err := r.handleMessages(); err != nil {
		return errors.Wrap(err, "couldn't handle messages")
	}

	if r.commitIndex > r.lastApplied {
		endIndex, err := r.commitLog.MaxIndex()
		if err != nil {
			return errors.Wrap(err, "couldn't get highest index of commit log")
		}

		for r.lastApplied < endIndex {
			toBeApplied := r.lastApplied + 1
			entry, err := r.commitLog.Get(toBeApplied)
			if err != nil {
				return errors.Wrap(err, "couldn't get entry to apply from the commit log")
			}

			r.applyEntries <- entry

			r.lastApplied = toBeApplied
		}
	}

	if r.termState.GetRole() == raft.Follower || r.termState.GetRole() == raft.Candidate {
		if r.termState.ShouldInitiateElection() {
			if err := r.initiateElection(); err != nil {
				return errors.Wrap(err, "couldn't initiate election")
			}
		}
	}

	switch r.termState.GetRole() {
	case raft.Follower:
	case raft.Candidate:
	case raft.Leader:
	}
}

func (r *Raft) initiateElection() error {

}

func (r *Raft) handleMessages() error {
	for i := 0; i < r.config.MessagesToHandlePerLoop; i++ {
		vote, err := r.cluster.ReceiveVote()
		if err != nil && err != raft.ErrNoMessages {
			return errors.Wrap(err, "couldn't receive messages")
		}
		if err == nil {
			if err := r.handleVote(vote); err != nil {
				log.Printf("Error handling vote: %v", err)
			}
			continue
		}

		voteRequest, err := r.cluster.ReceiveVoteRequest()
		if err != nil && err != raft.ErrNoMessages {
			return errors.Wrap(err, "couldn't receive messages")
		}
		if err == nil {
			vote, err := r.handleVoteRequest(voteRequest)
			if err != nil {
				if err := voteRequest.RespondError(err); err != nil {
					log.Printf("couldn't respond with vote request handling error: %v", err)
				}
				continue
			}

			if err := voteRequest.Respond(r.cluster.Self(), r.termState.GetTerm(), vote); err != nil {
				log.Printf("couldn't respond with vote: %v", err)
			}
			continue
		}

		appendEntriesResponse, err := r.cluster.ReceiveAppendEntriesResponse()
		if err != nil && err != raft.ErrNoMessages {
			return errors.Wrap(err, "couldn't receive messages")
		}
		if err == nil {
			if err := r.handleAppendEntriesResponse(appendEntriesResponse); err != nil {
				log.Printf("Error handling append entries response: %v", err)
			}
			continue
		}

		appendEntries, err := r.cluster.ReceiveAppendEntries()
		if err != nil && err != raft.ErrNoMessages {
			return errors.Wrap(err, "couldn't receive messages")
		}
		if err == nil {
			appendEntriesResponse, err := r.handleAppendEntries(appendEntries)
			if err != nil {
				if err := appendEntries.RespondError(err); err != nil {
					log.Printf("couldn't respond with append entries handling error: %v", err)
				}
				continue
			}

			if err := appendEntries.Respond(r.cluster.Self(), r.termState.GetTerm(), appendEntriesResponse); err != nil {
				log.Printf("couldn't respond with append entries response: %v", err)
			}
			continue
		}
	}

	return nil
}

func (r *Raft) handleTermOverride(msg *raft.Message) error {
	if msg.Term > r.termState.GetTerm() {
		if err := r.termState.OverrideTerm(msg.Term); err != nil {
			return errors.Wrap(err, "couldn't override term")
		}
	}
}

func (r *Raft) handleAppendEntries(msg *raft.AppendEntries) (*raft.AppendEntriesResponse, error) {
	if err := r.handleTermOverride(&msg.Message); err != nil {
		return nil, errors.Wrap(err, "couldn't override term")
	}

	// 1. Reply false if term < currentTerm
	if msg.Term < r.termState.GetTerm() {
		r.termState.SetLeader(msg.Leader)
		return &raft.AppendEntriesResponse{
			Success: false,
		}, nil
	}

	if r.termState.GetRole() == raft.Leader {
		return nil, errors.New("received append entries as leader, this shouldn't happen")
	}

	if r.termState.GetRole() == raft.Candidate {
		r.termState.AbortElection()
	}

	if r.termState.GetRole() != raft.Follower {
		return nil, errors.New("invalid role, handling append entries not as follower")
	}

	if r.termState.GetLeader() == nil {
		r.termState.SetLeader(msg.Leader)
		return &raft.AppendEntriesResponse{
			Success: false,
		}, nil
	}

	r.termState.ResetElectionTimeout()

	// 2. Reply false if log doesn't contain an entry at PreviousLogIndex whose term matches PreviousLogTerm
	exists, err := r.commitLog.Exists(msg.PreviousLogIndex, msg.PreviousLogTerm)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't check if log entry with index %v exists", msg.PreviousLogIndex)
	}

	if !exists && msg.PreviousLogIndex != 0 {
		return &raft.AppendEntriesResponse{
			Success: false,
		}, nil
	}

	nextEntryIndex := msg.PreviousLogIndex + 1
	for _, newEntry := range msg.Entries {
		// 3. If an existing entry conflicts with a new one (same index but different terms),
		// delete the existing entry and all that follow it
		entry, err := r.commitLog.Get(nextEntryIndex)
		if err == nil {
			if entry.Term == newEntry.Term {
				// Entry is already present
				continue
			}
			if err := r.commitLog.DeleteFrom(nextEntryIndex); err != nil {
				return nil, errors.Wrap(err, "couldn't delete invalid entries")
			}
		}
		if err != nil && err != raft.ErrNoSuchEntry {
			return nil, errors.Wrapf(err, "couldn't get current entry at index %v", nextEntryIndex)
		}

		// 4. Append any new entries not already in the log
		if _, err := r.commitLog.Append(&newEntry); err != nil {
			return nil, errors.Wrap(err, "couldn't append new entry to the commit log")
		}
	}

	// 5. If leader commit index > commitIndex, set commitIndex = min(leader commit index, index of last new entry)
	if msg.CommitIndex > r.commitIndex {
		maxEntryIndex, err := r.commitLog.MaxIndex()
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get max entry index from the commit log")
		}

		if msg.CommitIndex > maxEntryIndex {
			r.commitIndex = maxEntryIndex
		} else {
			r.commitIndex = msg.CommitIndex
		}
	}

	return &raft.AppendEntriesResponse{
		Success: true,
	}, nil
}

func (r *Raft) handleAppendEntriesResponse(msg *raft.AppendEntriesResponse) error {
	if err := r.handleTermOverride(&msg.Message); err != nil {
		return errors.Wrap(err, "couldn't override term")
	}

	if msg.Term < r.termState.GetTerm() {
		log.Println("old append entries response")
		return nil
	}

	if r.termState.GetRole() != raft.Leader {
		return errors.New("only the leader should get appen entries responses")
	}

	// This response is old and irrelevant
	if r.termState.GetNextIndex(msg.Sender) != msg.PreviousIndex+1 {
		return nil
	}

	if msg.Success == false {
		// Decrement the next index for this node
		r.termState.SetNextIndex(msg.Sender, msg.PreviousIndex)
		return nil
	}

	// msg.Success == true
	r.termState.SetNextIndex(msg.Sender, msg.MaxEntryIndex+1)
	r.termState.SetMatchIndex(msg.Sender, msg.MaxEntryIndex)

	return nil
}

func (r *Raft) handleVoteRequest(msg *raft.VoteRequest) (*raft.Vote, error) {
	if err := r.handleTermOverride(&msg.Message); err != nil {
		return nil, errors.Wrap(err, "couldn't override term")
	}

	if msg.Term < r.termState.GetTerm() {
		return &raft.Vote{
			Granted: false,
		}, nil
	}

	maxIndex, err := r.commitLog.MaxIndex()
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get commit log max index")
	}

	lastEntry, err := r.commitLog.Get(maxIndex)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't get last entry from commit log")
	}

	if msg.LastLogIndex < maxIndex || msg.LastLogTerm < lastEntry.Term {
		return &raft.Vote{
			Granted: false,
		}, nil
	}

	if err := r.termState.VoteFor(msg.Sender); err != nil {
		return &raft.Vote{
			Granted: false,
		}, nil
	}

	r.termState.ResetElectionTimeout()

	return &raft.Vote{
		Granted: true,
	}, nil
}

func (r *Raft) handleVote(msg *raft.Vote) error {
	if err := r.handleTermOverride(&msg.Message); err != nil {
		return errors.Wrap(err, "couldn't override term")
	}

	if msg.Term < r.termState.GetTerm() {
		return errors.New("old vote")
	}

	if r.termState.GetRole() != raft.Candidate {
		return nil
	}

	r.termState.AddVote(msg.Sender)
}
