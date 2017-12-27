package raft

import (
	"sync"
	"time"
)

type leaderData struct {
	lastLogIndex int64

	nextIndex      map[string]int64 // Reinitialize to last log index + 1 for everybody
	nextIndexMutex sync.RWMutex

	matchIndex      map[string]int64 // Reinitiallize to 0 for everybody
	matchIndexMutex sync.RWMutex

	lastAppendEntries      map[string]time.Time // Reintialize to zero
	lastAppendEntriesMutex sync.RWMutex
}

func NewLeaderData(lastLogIndex int64) *leaderData {
	return &leaderData{
		lastLogIndex: lastLogIndex + 1,

		nextIndex:         make(map[string]int64),
		matchIndex:        make(map[string]int64),
		lastAppendEntries: make(map[string]time.Time),
	}
}

// This doesn't use a mutex, acquire the mutex yourself.
func (ld *leaderData) internalGetNextIndex(node string) int64 {
	index, ok := ld.nextIndex[node]
	if !ok {
		return ld.lastLogIndex
	}

	return index
}

func (ld *leaderData) GetNextIndex(node string) int64 {
	ld.nextIndexMutex.RLock()
	defer ld.nextIndexMutex.RUnlock()

	return ld.internalGetNextIndex(node)
}

func (ld *leaderData) SetNextIndex(node string, prevIndex, newIndex int64) bool {
	ld.nextIndexMutex.Lock()
	defer ld.nextIndexMutex.Unlock()
	if ld.internalGetNextIndex(node) == prevIndex {
		ld.nextIndex[node] = newIndex
		return true
	} else {
		return false
	}
}

func (ld *leaderData) GetMatchIndex(node string) int64 {
	ld.matchIndexMutex.RLock()
	defer ld.matchIndexMutex.RUnlock()
	return ld.matchIndex[node]
}

func (ld *leaderData) NoteMatchIndex(node string, newIndex int64) {
	ld.matchIndexMutex.Lock()
	defer ld.matchIndexMutex.Unlock()

	// This should be monotonically rising, so we'll make it be
	if ld.matchIndex[node] < newIndex {
		ld.matchIndex[node] = newIndex
	}
}

func (ld *leaderData) GetLastAppendEntries(node string) time.Time {
	ld.lastAppendEntriesMutex.RLock()
	defer ld.lastAppendEntriesMutex.RUnlock()
	return ld.lastAppendEntries[node]
}

func (ld *leaderData) NoteAppendEntries(node string) {
	// We note the time here, in case there is lock contention, and we have to wait to get the lock.
	now := time.Now()

	ld.lastAppendEntriesMutex.Lock()
	defer ld.lastAppendEntriesMutex.Unlock()

	// In case someone concurrently updated the append entries to a newer time that this function was supposed to.
	// We're not meaning to ever decrement this, we can't undo what has already happened.
	if ld.lastAppendEntries[node].Before(now) {
		ld.lastAppendEntries[node] = now
	}
}
