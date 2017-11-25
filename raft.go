package raft

import (
	"github.com/hashicorp/serf/serf"
)

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
	QuorumSize() int
	OtherHealthyMembers() []serf.Member
}
