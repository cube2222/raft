package raft

import (
	"github.com/cube2222/raft"
	"github.com/pkg/errors"
	"sync"
	"os"
	"encoding/json"
	"log"
)

type entryLog struct {
	log   []raft.Entry
	mutex sync.RWMutex
}

func NewEntryLog() (*entryLog, error) {
	l := &entryLog{
		log: make([]raft.Entry, 0),
	}

	file, err := os.Open("log.json")
	if !os.IsNotExist(err) {
		return nil, errors.Wrap(err, "Couldn't open persisted commit log")
	}
	if file != nil {
		err := json.NewDecoder(file).Decode(&l.log)
		if err != nil {
			return nil, errors.Wrap(err, "Couldn't decode commit log file, file corrupted")
		}
	}

	return l, nil
}

func (l *entryLog) persist() error {
	file, err := os.Create("log.json")
	if err != nil {
		return errors.Wrap(err, "Couldn't create file to persist commit log")
	}
	defer file.Close()
	if err = json.NewEncoder(file).Encode(l.log); err != nil {
		return errors.Wrap(err, "Couldn't encode commit log to persistent storage")
	}
	return nil
}

func (l *entryLog) Exists(index int64, term int64) bool {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	if index <= 0 {
		return false
	}
	index = index - 1
	if int64(len(l.log)) <= index {
		return false
	}
	if l.log[index].Term != term {
		return false
	}

	return true
}

var ErrDoesNotExist = errors.Errorf("Entry does not exist")

func (l *entryLog) Get(index int64) (*raft.Entry, error) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	if index <= 0 {
		return nil, ErrDoesNotExist
	}
	index = index - 1
	if int64(len(l.log)) <= index {
		return nil, ErrDoesNotExist
	}
	return &l.log[index], nil
}

func (l *entryLog) DeleteFrom(index int64) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	index = index - 1
	if int64(len(l.log)) <= index {
		return nil
	}

	l.log = l.log[:index]
	return errors.Wrap(
		l.persist(),
		"Couldn't persist deletion of entries from given index",
	)
}

func (l *entryLog) Append(entry *raft.Entry, term int64) (int64, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	l.log = append(l.log, *entry)
	err := l.persist()
	if err != nil {
		// If there's en error when persisting, undo adding that entry
		l.log = l.log[:len(l.log)-1]
		return -1, errors.Wrap(err, "Couldn't persist log")
	}
	// Keep in mind, indexes are from 1.
	return int64(len(l.log)), nil
}

func (l *entryLog) MaxIndex() int64 {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	// Not subtracting 1 is not a bug, indexes are from 1, so we have to subtract a one, but also add it, so we have the proper index.
	return int64(len(l.log))
}

func (l *entryLog) GetLastEntry() *raft.Entry {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	if len(l.log) == 0 {
		return nil
	}

	return &l.log[len(l.log)-1]
}
