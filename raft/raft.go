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
		if err := r.sendAppendEntries(); err != nil {
			return errors.Wrap(err, "couldn't send append entries")
		}

	}

	return nil
}

func (r *Raft) sendAppendEntries() error {
	lastLogIndex, err := r.commitLog.MaxIndex()
	if err != nil {
		return errors.Wrap(err, "couldn't get last log index")
	}

	for _, node := range r.cluster.GetMembers() {
		nextIndex := r.termState.GetNextIndex(node)

		if lastLogIndex >= nextIndex || r.termState.ShouldSendHeartbeat(node) {
			if err := r.sendSingleAppendEntries(node); err != nil {
				log.Printf("Couldn't send append entries to member %v: %v", node, err)
				continue
			}
		}
	}
	return nil
}

func (r *Raft) sendSingleAppendEntries(node raft.Node) error {
	r.termState.ResetHeartbeatTimeout(node)

	lastLogIndex, err := r.commitLog.MaxIndex()
	if err != nil {
		return errors.Wrap(err, "couldn't get last log index")
	}
	entry, err := r.commitLog.Get(lastLogIndex)
	if err != nil {
		return errors.Wrap(err, "couldn't get last log entry")
	}

	previousLogIndex := lastLogIndex
	previousLogTerm := entry.Term

	var entries []raft.Entry
	nextIndex := r.termState.GetNextIndex(node)
	if lastLogIndex >= nextIndex {
		entry, err := r.commitLog.Get(lastLogIndex)
		if err != nil {
			return errors.Wrap(err, "couldn't get nextIndex log entry")
		}

		previousLogIndex = nextIndex
		previousLogTerm = entry.Term

		entries = []raft.Entry{*entry}
	}

	err = r.cluster.SendAppendEntries(&raft.AppendEntries{
		Message: raft.Message{
			Sender: r.cluster.Self(),
			Term:   r.termState.GetTerm(),
		},
		Leader:           r.cluster.Self(),
		PreviousLogIndex: previousLogIndex,
		PreviousLogTerm:  previousLogTerm,
		Entries:          entries,
		CommitIndex:      r.commitIndex,
	}, node)
	if err != nil {
		return errors.Wrapf(err, "couldn't send append entries to member %v", node)
	}

	return nil
}

func (r *Raft) initiateElection() error {
	lastLogIndex, err := r.commitLog.MaxIndex()
	if err != nil {
		return errors.Wrap(err, "couldn't get last log index")
	}
	lastMessage, err := r.commitLog.Get(lastLogIndex)
	if err != nil {
		return errors.Wrap(err, "couldn't get most recent log entry")
	}

	if err := r.termState.InitiateElection(); err != nil {
		return errors.Wrap(err, "couldn't initiate election term state")
	}

	for _, node := range r.cluster.GetMembers() {
		err := r.cluster.SendVoteRequest(&raft.VoteRequest{
			Message: raft.Message{
				Sender: r.cluster.Self(),
				Term:   r.termState.GetTerm(),
			},
			Candidate:    r.cluster.Self(),
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastMessage.Term,
		}, node)
		if err != nil {
			log.Printf("Couldn't send vote request to member %v: %v", node, err)
		}
	}

	return nil
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
	if msg.Term >= r.termState.GetTerm() {
		if err := r.termState.OverrideTerm(msg.Term); err != nil {
			return errors.Wrap(err, "couldn't override term")
		}
	}
	return nil
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
	if r.termState.VoteCount() > len(r.cluster.GetMembers())/2 {
		if err := r.becomeLeader(); err != nil {
			return errors.Wrap(err, "couldn't become leader")
		}
	}
	return nil
}

func (r *Raft) becomeLeader() error {
	lastLogIndex, err := r.commitLog.MaxIndex()
	if err != nil {
		return errors.Wrap(err, "couldn't get last log index")
	}
	lastMessage, err := r.commitLog.Get(lastLogIndex)
	if err != nil {
		return errors.Wrap(err, "couldn't get most recent log entry")
	}

	r.termState.BecomeLeader()

	for _, node := range r.cluster.GetMembers() {
		err := r.cluster.SendAppendEntries(&raft.AppendEntries{
			Message: raft.Message{
				Sender: r.cluster.Self(),
				Term:   r.termState.GetTerm(),
			},
			Leader:           r.cluster.Self(),
			PreviousLogIndex: lastLogIndex,
			PreviousLogTerm:  lastMessage.Term,
			Entries:          nil,
			CommitIndex:      r.commitIndex,
		}, node)
		if err != nil {
			return errors.Wrapf(err, "couldn't send append entries to member %v", node)
		}
	}
	return nil
}
