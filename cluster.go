package raft

import "github.com/pkg/errors"

type Node string

func (n *Node) Name() string {
	return string(*n)
}

type Cluster interface {
	Self() Node
	ReceiveAppendEntries() (*AppendEntries, error)
	ReceiveAppendEntriesResponse() (*AppendEntriesResponse, error)
	ReceiveVoteRequest() (*VoteRequest, error)
	ReceiveVote() (*Vote, error)
	SendMessage(msg *Message, recipient Node) error
	GetHealthyMembers() []Node
}

var ErrNoMessages = errors.New("No messages waiting")

type Message struct {
	Sender Node
	Term   int

	// Left for implementation details
	Metadata interface{}
}

type AppendEntries struct {
	Message

	Leader Node

	PreviousLogIndex int
	PreviousLogTerm  int

	Entries     []Entry
	CommitIndex int

	responseChannel    chan<- *AppendEntriesResponse
	responseErrChannel chan<- error
}

type Entry struct {
	Term int
}

func (msg *AppendEntries) Respond(sender Node, term int, response *AppendEntriesResponse) error {
	if msg.responseChannel == nil {
		return errors.New("Can't respond to this message")
	}

	response.Sender = sender
	response.Term = term
	response.Metadata = msg.Metadata
	response.PreviousIndex = msg.PreviousLogIndex
	response.MaxEntryIndex = msg.PreviousLogIndex + len(msg.Entries)
	msg.responseChannel <- response

	msg.responseChannel = nil
	msg.responseErrChannel = nil
	return nil
}

func (msg *AppendEntries) RespondError(err error) error {
	if msg.responseChannel == nil {
		return errors.New("Can't respond to this message")
	}

	msg.responseErrChannel <- err

	msg.responseChannel = nil
	msg.responseErrChannel = nil
	return nil
}

type AppendEntriesResponse struct {
	Message

	MaxEntryIndex int
	PreviousIndex int

	Success bool
}

type VoteRequest struct {
	Message

	Candidate Node

	LastLogIndex int
	LastLogTerm  int

	responseChannel    chan<- *Vote
	responseErrChannel chan<- error
}

type Vote struct {
	Message

	Granted bool
}

func (msg *VoteRequest) Respond(sender Node, term int, response *Vote) error {
	if msg.responseChannel == nil {
		return errors.New("Can't respond to this message")
	}

	response.Sender = sender
	response.Term = term
	response.Metadata = msg.Metadata
	msg.responseChannel <- response

	msg.responseChannel = nil
	msg.responseErrChannel = nil
	return nil
}

func (msg *VoteRequest) RespondError(err error) error {
	if msg.responseChannel == nil {
		return errors.New("Can't respond to this message")
	}

	msg.responseErrChannel <- err

	msg.responseChannel = nil
	msg.responseErrChannel = nil
	return nil
}
