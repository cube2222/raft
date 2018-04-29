package raft

import "github.com/pkg/errors"

type Node string

func (n *Node) Name() string {
	return string(*n)
}

type Cluster interface {
	Self() Node
	ReceiveMessage() (*Message, error)
	SendMessage(msg *Message, recipient Node) error
	GetHealthyMembers() []Node
}

var ErrNoMessages = errors.New("No messages waiting")

type Message struct {
	Sender          Node
	Term            int
	ResponseChannel chan<- *Message

	Payload interface{}

	// Left for implementation details
	Metadata interface{}
}

type AppendEntries struct {
	Leader Node

	PreviousLogIndex int
	PreviousLogTerm  int

	Entries     []Entry
	CommitIndex int
}

type Entry struct {
	Term int
}

type AppendEntriesResponse struct {
	Success bool
}

type VoteRequest struct {
	Candidate Node

	LastLogIndex int
	LastLogTerm  int
}

type Vote struct {
	Granted bool
}

func Respond(msg *Message, sender Node, term int, payload interface{}) error {
	if msg.ResponseChannel == nil {
		return errors.New("Can't respond to this message")
	}

	msg.ResponseChannel <- &Message{
		Sender:   sender,
		Term:     term,
		Payload:  payload,
		Metadata: msg.Metadata,
	}

	msg.ResponseChannel = nil
	return nil
}
