package raft

type Role int

const (
	Follower  Role = iota
	Candidate
	Leader
)

type Raft interface {
	RaftServer

	Run()
	GetDebugData() []Entry
}
