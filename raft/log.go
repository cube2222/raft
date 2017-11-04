package raft

import (
	"github.com/cube2222/raft"
	"github.com/pkg/errors"
)

type logEntry struct {
	entry raft.Entry
	term  int64
}

// TODO: Mutexes :)
type entryLog struct {
	log []logEntry
}

func NewEntryLog() *entryLog {
	return &entryLog{
		log: make([]logEntry, 0),
	}
}

func (l *entryLog) Exists(index int64, term int64) bool {
	if index <= 0 {
		return false
	}
	index = index - 1
	if int64(len(l.log)) <= index {
		return false
	}
	if l.log[index].term != term {
		return false
	}

	return true
}

var ErrDoesNotExist = errors.Errorf("Entry does not exist")

func (l *entryLog) Get(index int64) (*logEntry, error) {
	if index <= 0 {
		return nil, ErrDoesNotExist
	}
	index = index - 1
	if int64(len(l.log)) <= index {
		return nil, ErrDoesNotExist
	}
	return &l.log[index], nil
}

func (l *entryLog) DeleteFrom(index int64) {
	index = index - 1
	if int64(len(l.log)) <= index {
		return
	}

	l.log = l.log[:index]
}

func (l *entryLog) Append(entry *raft.Entry, term int64) {
	l.log = append(l.log, logEntry{
		entry: *entry,
		term:  term,
	})
}

func (l *entryLog) MaxIndex() int64 {
	// Not subtracting 1 is not a bug, indexes are from 1, so we have to subtract a one, but also add it, so we have the proper index.
	return int64(len(l.log))
}

func (l *entryLog) GetLastEntry() *logEntry {
	if len(l.log) == 0 {
		return nil
	}

	return &l.log[len(l.log)-1]
}
