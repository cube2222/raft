package raft

import (
	"context"
	"sync"
	"os"
	"log"
	"encoding/json"
)

type role int

const (
	Follower  role = iota
	Candidate
	Leader
)

type termData struct {
	nodeID string

	leader            string
	role              role
	term              int64
	termContext       context.Context
	termContextCancel context.CancelFunc
	votedFor          string

	mutex sync.RWMutex
}

func NewTermData(nodeID string) *termData {
	snapshot := loadSnapshot()

	td := termData{
		nodeID: nodeID,
		role:   Follower,
		term:   snapshot.Term,
		votedFor: snapshot.VotedFor,
	}
	td.termContext, td.termContextCancel = context.WithCancel(context.Background())

	return &td
}

func (td *termData) OverrideTerm(prevTerm, term int64) bool {
	td.mutex.Lock()
	defer td.mutex.Unlock()

	if td.term != prevTerm || term <= td.term {
		return false
	}

	td.role = Follower
	td.term = term
	td.votedFor = ""
	td.leader = ""

	td.termContextCancel()

	td.termContext, td.termContextCancel = context.WithCancel(context.Background())

	td.getSnapshotInternal().persist()

	return true
}

func (td *termData) InitiateElection() (*TermDataSnapshot) {
	td.mutex.Lock()
	defer td.mutex.Unlock()

	td.role = Candidate
	td.term = td.term + 1
	td.votedFor = td.nodeID
	td.leader = ""

	td.termContextCancel()

	td.termContext, td.termContextCancel = context.WithCancel(context.Background())

	snapshot := td.getSnapshotInternal()
	snapshot.persist()

	return snapshot
}

func (td *termData) BecomeLeader(term int64) bool {
	td.mutex.Lock()
	defer td.mutex.Unlock()
	if td.term != term {
		return false
	}

	td.role = Leader
	td.leader = td.nodeID
	return true
}

func (td *termData) AbortElection() {
	td.mutex.Lock()
	defer td.mutex.Unlock()
	if td.role == Candidate {
		td.role = Follower
	}
}

func (td *termData) GetRole() role {
	td.mutex.RLock()
	defer td.mutex.RUnlock()
	return td.role
}

func (td *termData) Context() context.Context {
	td.mutex.RLock()
	defer td.mutex.RUnlock()
	return td.termContext
}

func (td *termData) GetLeader() string {
	td.mutex.RLock()
	defer td.mutex.RUnlock()
	return td.leader
}

func (td *termData) SetLeader(leader string) {
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

func (td *termData) GetTerm() int64 {
	td.mutex.RLock()
	defer td.mutex.RUnlock()
	return td.term
}

func (td *termData) SetVotedFor(term int64, nodeID string) bool {
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
	Leader      string `json:"-"`
	Role        role `json:"-"`
	Term        int64 `json:"term"`
	TermContext context.Context `json:"-"`
	VotedFor    string `json:"voted_for"`
}

func (td *termData) getSnapshotInternal() *TermDataSnapshot {
	return &TermDataSnapshot{
		Leader:      td.leader,
		Role:        td.role,
		Term:        td.term,
		TermContext: td.termContext,
		VotedFor:    td.votedFor,
	}
}

func (td *termData) GetSnapshot() *TermDataSnapshot {
	td.mutex.RLock()
	defer td.mutex.RUnlock()

	return td.getSnapshotInternal()
}

func (td *TermDataSnapshot) persist() {
	file, err := os.Create("termdata.json")
	if err != nil {
		// TODO: Proper error handling
		log.Fatalf("Why doesn't file io work :( ? %v", err)
	}
	defer file.Close()
	json.NewEncoder(file).Encode(td)
}

func loadSnapshot() *TermDataSnapshot {
	// TODO: Do fstat, then potentially open with error checking
	file, _ := os.Open("termdata.json")
	defer file.Close()
	var snapshot TermDataSnapshot
	if file != nil {
		err := json.NewDecoder(file).Decode(&snapshot)
		if err != nil {
			// TODO: Proper error handling
			log.Fatalf("File corrupted: %v", err)
		}
	}
	return &snapshot
}
