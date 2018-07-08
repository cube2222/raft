package inmemory

import (
	"log"
	"math/rand"
	"time"

	"github.com/cube2222/raft"
	"github.com/pkg/errors"
)

type inMemoryTermState struct {
	self raft.Node

	term int
	role raft.Role
	leader *raft.Node
	electionTimeout time.Time

	votedFor *raft.Node
	voteCount int

	matchIndex map[raft.Node]int
	nextIndex map[raft.Node]int
}

func (s *inMemoryTermState) GetTerm() int {
	return s.term
}

func (s *inMemoryTermState) OverrideTerm(newTerm int) error {
	if newTerm < s.term {
		log.Fatal("newTerm smaller in override")
	}
	s.term = newTerm
	s.role = raft.Follower
	return nil
}

func (s *inMemoryTermState) InitiateElection() error {
	s.term++
	s.ResetElectionTimeout()
	s.role = raft.Candidate

	s.votedFor = &s.self
	s.voteCount = 1
	return nil
}

func (s *inMemoryTermState) VoteFor(node raft.Node) error {
	if s.votedFor != nil && *s.votedFor != node {
		return errors.Errorf("already voted for %v", s.votedFor)
	}
	s.votedFor = &node
	return nil
}

func (s *inMemoryTermState) GetRole() raft.Role {
	return s.role
}

func (s *inMemoryTermState) GetLeader() *raft.Node {
	return s.leader
}

func (s *inMemoryTermState) SetLeader(leader raft.Node) {
	s.leader = &leader
}

func (s *inMemoryTermState) BecomeLeader() {
	s.leader = &s.self
	s.role = raft.Leader

}

func (s *inMemoryTermState) ResetElectionTimeout() {
	s.electionTimeout = time.Now().Add(time.Second*2 + 2 * time.Second * time.Duration(rand.Float32()))
}

func (s *inMemoryTermState) ShouldInitiateElection() bool {
	if s.electionTimeout.After(time.Now()) {
		return true
	} else {
		return false
	}
}

func (s *inMemoryTermState) GetNextIndex(node raft.Node) int {
	if index, ok := s.nextIndex[node]; ok {
		return index
	}

	return -1
}

func (s *inMemoryTermState) SetNextIndex(node raft.Node, index int) {
	s.nextIndex[node] = index
}

func (s *inMemoryTermState) GetMatchIndex(node raft.Node) int {
	panic("implement me")
}

func (s *inMemoryTermState) SetMatchIndex(node raft.Node, index int) {
	panic("implement me")
}

func (s *inMemoryTermState) ShouldSendHeartbeat(node raft.Node) bool {
	panic("implement me")
}

func (s *inMemoryTermState) ResetHeartbeatTimeout(node raft.Node) {
	panic("implement me")
}

func (s *inMemoryTermState) VoteCount() int {
	panic("implement me")
}

func (s *inMemoryTermState) AddVote(raft.Node) {
	panic("implement me")
}

