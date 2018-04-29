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
		msg, err := r.cluster.ReceiveMessage()
		if err != nil && err != raft.ErrNoMessages {
			return errors.Wrap(err, "couldn't receive messages")
		}
		if err == raft.ErrNoMessages {
			return nil
		}

		if err := r.handleMessage(msg); err != nil {
			return errors.Wrapf(err, "couldn't handle message %+v", msg)
		}
	}

	return nil
}

func (r *Raft) handleMessage(msg *raft.Message) error {
	if msg.Term > r.termState.GetTerm() {
		if err := r.termState.OverrideTerm(msg.Term); err != nil {
			return errors.Wrap(err, "couldn't override term")
		}
	}
	// If something just returns an error then this will make sure we send something back.
	// This is a noop if something has already been sent.
	defer r.respond(msg, nil)

	switch payload := msg.Payload.(type) {
	case raft.AppendEntries:
		return r.handleAppendEntries(msg, &payload)
	case raft.AppendEntriesResponse:
		return r.handleAppendEntriesResponse(msg, &payload)
	case raft.VoteRequest:
		return r.handleVoteRequest(msg, &payload)
	case raft.Vote:
		return r.handleVote(msg, &payload)
	default:
		return errors.Errorf("Unknown message type %+v", msg)
	}
}

func (r *Raft) handleAppendEntries(msg *raft.Message, payload *raft.AppendEntries) error {
	// 1. Reply false if term < currentTerm
	if msg.Term < r.termState.GetTerm() {
		r.termState.SetLeader(payload.Leader)
		return r.respond(msg, &raft.AppendEntriesResponse{
			Success: false,
		})
	}

	if r.termState.GetRole() == raft.Leader {
		return errors.New("received append entries as leader, this shouldn't happen")
	}

	if r.termState.GetRole() == raft.Candidate {
		r.termState.AbortElection()
	}

	if r.termState.GetRole() != raft.Follower {
		return errors.New("invalid role, handling append entries not as follower")
	}

	if r.termState.GetLeader() == nil {
		r.termState.SetLeader(payload.Leader)
		return r.respond(msg, &raft.AppendEntriesResponse{
			Success: false,
		})
	}

	// 2. Reply false if log doesn't contain an entry at PreviousLogIndex whose term matches PreviousLogTerm
	exists, err := r.commitLog.Exists(payload.PreviousLogIndex, payload.PreviousLogTerm)
	if err != nil {
		return errors.Wrapf(err, "couldn't check if log entry with index %v exists", payload.PreviousLogIndex)
	}

	if !exists && payload.PreviousLogIndex != 0 {
		return r.respond(msg, &raft.AppendEntriesResponse{
			Success: false,
		})
	}

	nextEntryIndex := payload.PreviousLogIndex + 1
	for _, newEntry := range payload.Entries {
		// 3. If an existing entry conflicts with a new one (same index but different terms),
		// delete the existing entry and all that follow it
		entry, err := r.commitLog.Get(nextEntryIndex)
		if err == nil {
			if entry.Term == newEntry.Term {
				// Entry is already present
				continue
			}
			if err := r.commitLog.DeleteFrom(nextEntryIndex); err != nil {
				return errors.Wrap(err, "couldn't delete invalid entries")
			}
		}
		if err != nil && err != raft.ErrNoSuchEntry {
			return errors.Wrapf(err, "couldn't get current entry at index %v", nextEntryIndex)
		}

		// 4. Append any new entries not already in the log
		if _, err := r.commitLog.Append(&newEntry); err != nil {
			return errors.Wrap(err, "couldn't append new entry to the commit log")
		}
	}

	// 5. If leader commit index > commitIndex, set commitIndex = min(leader commit index, index of last new entry)
	if payload.CommitIndex > r.commitIndex {
		maxEntryIndex, err := r.commitLog.MaxIndex()
		if err != nil {
			return errors.Wrap(err, "couldn't get max entry index from the commit log")
		}

		if payload.CommitIndex > maxEntryIndex {
			r.commitIndex = maxEntryIndex
		} else {
			r.commitIndex = payload.CommitIndex
		}
	}

	return r.respond(msg, &raft.AppendEntriesResponse{
		Success: true,
	})
}

func (r *Raft) handleAppendEntriesResponse(msg *raft.Message, payload *raft.AppendEntriesResponse) error {
	panic("not yet implemented")
}

func (r *Raft) handleVoteRequest(msg *raft.Message, payload *raft.VoteRequest) error {
	panic("not yet implemented")
}

func (r *Raft) handleVote(msg *raft.Message, payload *raft.Vote) error {
	panic("not yet implemented")
}

func (r *Raft) respond(msg *raft.Message, payload interface{}) error {
	return raft.Respond(msg, r.cluster.Self(), r.termState.GetTerm(), payload)
}
