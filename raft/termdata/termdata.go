package termdata

import (
	"context"
	"encoding/json"
	"os"
	"sync"

	"github.com/cube2222/raft"
	"github.com/pkg/errors"
)

type TermData struct {
	nodeID string

	leader            string
	role              raft.Role
	term              int64
	termContext       context.Context
	termContextCancel context.CancelFunc
	votedFor          string

	mutex sync.RWMutex
}

func NewTermData(nodeID string) (*TermData, error) {
	snapshot, err := loadSnapshot()
	if err != nil {
		return nil, errors.Wrap(err, "Couldn't load snapshot")
	}

	td := TermData{
		nodeID:   nodeID,
		role:     raft.Follower,
		term:     snapshot.Term,
		votedFor: snapshot.VotedFor,
	}
	td.termContext, td.termContextCancel = context.WithCancel(context.Background())

	return &td, nil
}

func (td *TermData) OverrideTerm(prevTerm, term int64) bool {
	td.mutex.Lock()
	defer td.mutex.Unlock()

	if td.term != prevTerm || term <= td.term {
		return false
	}

	td.role = raft.Follower
	td.term = term
	td.votedFor = ""
	td.leader = ""

	td.termContextCancel()

	td.termContext, td.termContextCancel = context.WithCancel(context.Background())

	td.getSnapshotInternal().persist()

	return true
}

func (td *TermData) InitiateElection() (*TermDataSnapshot, error) {
	td.mutex.Lock()
	defer td.mutex.Unlock()

	td.role = raft.Candidate
	td.term = td.term + 1
	td.votedFor = td.nodeID
	td.leader = ""

	td.termContextCancel()

	td.termContext, td.termContextCancel = context.WithCancel(context.Background())

	snapshot := td.getSnapshotInternal()
	if err := snapshot.persist(); err != nil {
		return nil, errors.Wrap(err, "Couldn't persist new term")
	}

	return snapshot, nil
}

func (td *TermData) BecomeLeader(term int64) bool {
	td.mutex.Lock()
	defer td.mutex.Unlock()
	if td.term != term {
		return false
	}

	td.role = raft.Leader
	td.leader = td.nodeID
	return true
}

func (td *TermData) AbortElection() {
	td.mutex.Lock()
	defer td.mutex.Unlock()
	if td.role == raft.Candidate {
		td.role = raft.Follower
	}
}

func (td *TermData) GetRole() raft.Role {
	td.mutex.RLock()
	defer td.mutex.RUnlock()
	return td.role
}

func (td *TermData) Context() context.Context {
	td.mutex.RLock()
	defer td.mutex.RUnlock()
	return td.termContext
}

func (td *TermData) GetLeader() string {
	td.mutex.RLock()
	defer td.mutex.RUnlock()
	return td.leader
}

func (td *TermData) SetLeader(leader string) {
	// If the leader is ok, don't create write contention
	td.mutex.RLock()
	prevLeader := td.leader
	td.mutex.RUnlock()
	if prevLeader == leader {
		return
	}

	td.mutex.Lock()
	defer td.mutex.Unlock()
	td.leader = leader
}

func (td *TermData) GetTerm() int64 {
	td.mutex.RLock()
	defer td.mutex.RUnlock()
	return td.term
}

func (td *TermData) SetVotedFor(term int64, nodeID string) bool {
	td.mutex.Lock()
	defer td.mutex.Unlock()
	if term != td.term {
		return false
	}
	if td.votedFor != "" && td.votedFor != nodeID {
		return false
	}
	td.votedFor = nodeID
	td.getSnapshotInternal().persist()
	return true
}

type TermDataSnapshot struct {
	Leader      string          `json:"-"`
	Role        raft.Role       `json:"-"`
	Term        int64           `json:"term"`
	TermContext context.Context `json:"-"`
	VotedFor    string          `json:"voted_for"`
}

func (td *TermData) getSnapshotInternal() *TermDataSnapshot {
	return &TermDataSnapshot{
		Leader:      td.leader,
		Role:        td.role,
		Term:        td.term,
		TermContext: td.termContext,
		VotedFor:    td.votedFor,
	}
}

func (td *TermData) GetSnapshot() *TermDataSnapshot {
	td.mutex.RLock()
	defer td.mutex.RUnlock()

	return td.getSnapshotInternal()
}

func (td *TermDataSnapshot) persist() error {
	file, err := os.Create("termdata.json")
	if err != nil {
		return errors.Wrap(err, "Couldn't create file to persist term data")
	}
	defer file.Close()
	if err = json.NewEncoder(file).Encode(td); err != nil {
		return errors.Wrap(err, "Couldn't encode term data to persistent storage")
	}
	return nil
}

func loadSnapshot() (*TermDataSnapshot, error) {
	var snapshot TermDataSnapshot
	file, err := os.Open("termdata.json")
	if err != nil && !os.IsNotExist(err) {
		return nil, errors.Wrap(err, "Couldn't open persisted term data")
	}
	if file != nil {
		defer file.Close()
		err := json.NewDecoder(file).Decode(&snapshot)
		if err != nil {
			return nil, errors.Wrap(err, "Couldn't decode term data file, file corrupted")
		}
	}
	return &snapshot, nil
}
