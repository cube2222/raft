package raft

import (
	"log"

	"github.com/cube2222/raft"
	"github.com/pkg/errors"
)

// TODO: Add the election timeout everywhere
type Raft struct {
	config *raft.Config

	commitIndex  int
	lastCommited int

	cluster   raft.Cluster
	commitLog raft.CommitLog
	termState raft.TermState
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
	panic("not yet implemented")
}

func (r *Raft) handleMessages() error {
	for i := 0; i < r.config.MessagesToHandlePerLoop; i++ {
		vote, err := r.cluster.ReceiveVote()
		if err != nil && err != raft.ErrNoMessages {
			return errors.Wrap(err, "couldn't receive messages")
		}
		if err == nil {
			if err := r.handleVote(vote); err != nil {
				// TODO: respond to the vote
				panic("not yet implemented")
			}
			continue
		}

		voteRequest, err := r.cluster.ReceiveVoteRequest()
		if err != nil && err != raft.ErrNoMessages {
			return errors.Wrap(err, "couldn't receive messages")
		}
		if err == nil {
			if vote, err := r.handleVoteRequest(voteRequest); err != nil {
				// TODO: respond to the vote
				panic("not yet implemented")
			} else {
				// TODO: respond to the vote
				panic("not yet implemented")
				log.Println(vote)
			}
			continue
		}

		appendEntriesResponse, err := r.cluster.ReceiveAppendEntriesResponse()
		if err != nil && err != raft.ErrNoMessages {
			return errors.Wrap(err, "couldn't receive messages")
		}
		if err == nil {
			if err := r.handleAppendEntriesResponse(appendEntriesResponse); err != nil {
				// TODO: respond to the vote
				panic("not yet implemented")
			}
			continue
		}

		appendEntries, err := r.cluster.ReceiveAppendEntries()
		if err != nil && err != raft.ErrNoMessages {
			return errors.Wrap(err, "couldn't receive messages")
		}
		if err == nil {
			if vote, err := r.handleAppendEntries(appendEntries); err != nil {
				// TODO: respond to the vote
				panic("not yet implemented")
			} else {
				// TODO: respond to the vote
				panic("not yet implemented")
				log.Println(vote)
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

func (r *Raft) respondAppendEntries(msg *raft.AppendEntries, res *raft.AppendEntriesResponse) error {
	return msg.Respond(r.cluster.Self(), r.termState.GetTerm(), res)
}

func (r *Raft) respondAppendEntriesErr(msg *raft.AppendEntries, err error) error {
	return msg.RespondError(err)
}

func (r *Raft) handleAppendEntriesResponse(msg *raft.AppendEntriesResponse) error {
	panic("not yet implemented")
}

func (r *Raft) handleVoteRequest(msg *raft.VoteRequest) (*raft.Vote, error) {
	panic("not yet implemented")
}

func (r *Raft) handleVote(msg *raft.Vote) error {
	panic("not yet implemented")
}
