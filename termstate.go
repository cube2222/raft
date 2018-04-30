package raft

import "context"

type Role int

const (
	Unknown   Role = iota
	Follower  
	Candidate 
	Leader    
)

type Snapshot interface {
	Context() context.Context
	Term() int
}

type snapshot struct {
	ctx context.Context
	term int
}

type TermState interface {
	GetSnapshot() Snapshot

	// Persistent
	GetTerm() int
	OverrideTerm(newTerm int) error
	InitiateElection(Snapshot) error
	VoteFor(Snapshot, Node) error

	// Volatile
	GetRole() Role
	GetLeader() *Node
	SetLeader(Snapshot, Node) error
	BecomeLeader(Snapshot) error
	AbortElection(Snapshot) error
}
