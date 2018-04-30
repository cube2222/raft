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
	ResetElectionTimeout()
	ShouldInitiateElection() bool

	// Volatile leader
	GetNextIndex(node Node) int
	SetNextIndex(node Node, index int)
	GetMatchIndex(node Node) int
	SetMatchIndex(node Node, index int)

	// Volatile candidate
	VoteCount() int
	AddVote(Node)
}
