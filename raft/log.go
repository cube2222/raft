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

func NewEntryLog() *entryLog {
	l := &entryLog{
		log: make([]raft.Entry, 0),
	}

	// TODO: Do fstat, then potentially open with error checking
	file, _ := os.Open("log.json")
	if file != nil {
		err := json.NewDecoder(file).Decode(&l.log)
		if err != nil {
			// TODO: Proper error handling
			log.Fatalf("File corrupted: %v", err)
		}
	}

	return l
}

func (l *entryLog) persist() {
	file, err := os.Create("log.json")
	if err != nil {
		// TODO: Proper error handling
		log.Fatalf("Why doesn't file io work :( ? %v", err)
	}
	defer file.Close()
	json.NewEncoder(file).Encode(l.log)
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

func (l *entryLog) DeleteFrom(index int64) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	index = index - 1
	if int64(len(l.log)) <= index {
		return
	}

	l.log = l.log[:index]
	l.persist()
}

func (l *entryLog) Append(entry *raft.Entry, term int64) int64 {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	l.log = append(l.log, *entry)
	l.persist()
	// Keep in mind, indexes are from 1.
	return int64(len(l.log))
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
