package raft

type Role int

const (
	Unknown   Role = iota
	Follower  
	Candidate 
	Leader    
)

type TermState interface {
	// Persistent
	GetTerm() int
	OverrideTerm(newTerm int) error
	InitiateElection() error
	VoteFor(Node) error

	// Volatile
	GetRole() Role
	GetLeader() *Node
	SetLeader(Node)
	BecomeLeader()
	AbortElection()
}
