package entrylog

import (
	"encoding/json"
	"os"
	"sync"

	"github.com/cube2222/raft"
	"github.com/pkg/errors"
)

type EntryLog struct {
	log   []raft.Entry
	mutex sync.RWMutex
}

func NewEntryLog() (*EntryLog, error) {
	l := &EntryLog{
		log: make([]raft.Entry, 0),
	}

	file, err := os.Open("log.json")
	if err != nil && !os.IsNotExist(err) {
		return nil, errors.Wrap(err, "Couldn't open persisted commit log")
	}
	if file != nil {
		defer file.Close()
		err := json.NewDecoder(file).Decode(&l.log)
		if err != nil {
			return nil, errors.Wrap(err, "Couldn't decode commit log file, file corrupted")
		}
	}

	return l, nil
}

func (l *EntryLog) persist() error {
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

func (l *EntryLog) Exists(index int64, term int64) bool {
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

func (l *EntryLog) Get(index int64) (*raft.Entry, error) {
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

func (l *EntryLog) DeleteFrom(index int64) error {
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

func (l *EntryLog) Append(entry *raft.Entry) (int64, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	l.log = append(l.log, *entry)
	err := l.persist()
	if err != nil {
		// If there's en error when persisting, undo adding that entry
		l.log = l.log[:len(l.log)-1]
		return -1, errors.Wrap(err, "Couldn't persist log")
	}
	// Keep in mind, indexes are from 1, so the length
	// will actually be the last index.
	return int64(len(l.log)), nil
}

func (l *EntryLog) MaxIndex() int64 {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	// Not subtracting 1 is not a bug, indexes are from 1,
	// so we have to subtract a one, but also add one, so we have the proper index.
	return int64(len(l.log))
}

func (l *EntryLog) GetLastEntry() *raft.Entry {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	if len(l.log) == 0 {
		return nil
	}

	return &l.log[len(l.log)-1]
}

func (l *EntryLog) DebugData() []raft.Entry {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	dest := make([]raft.Entry, len(l.log))
	copy(dest, l.log)

	return dest
}