package raft

import "github.com/pkg/errors"

type CommitLog interface {
	Exists(index int, term int) (bool, error)
	Get(index int) (*Entry, error)
	DeleteFrom(index int) error
	Append(*Entry) (int, error)
	MaxIndex() (int, error)
}

var ErrNoSuchEntry = errors.New("No such entry")
