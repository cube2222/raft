package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/cube2222/raft"
	"github.com/cube2222/raft/cluster"
	"github.com/cube2222/raft/raft/entrylog"
	"github.com/cube2222/raft/raft/termdata"
	"github.com/hashicorp/serf/serf"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
	"github.com/uber-go/atomic"
	"golang.org/x/net/context"
)

type Applyable interface {
	Apply(entry *raft.Entry) error
}

type Raft struct {
	cluster     *cluster.Cluster
	clusterSize int
	rpcPort     int
	applyable   Applyable

	// On all servers, persistent
	// Term specific
	termData *termdata.TermData

	log *entrylog.EntryLog

	// On all servers, volatile
	commitIndex *atomic.Int64
	lastApplied int64

	// On leader, reinitialized after election
	leaderData *leaderData

	// On non-leader
	electionTimeout time.Time
}

func NewRaft(ctx context.Context, cluster *cluster.Cluster, applyable Applyable, localNodeName string, opts ... func(*Raft)) (raft.Raft, error) {
	entryLog, err := entrylog.NewEntryLog()
	if err != nil {
		return nil, errors.Wrap(err, "Couldn't load entry log")
	}

	termData, err := termdata.NewTermData(localNodeName)
	if err != nil {
		return nil, errors.Wrap(err, "Couldn't load term data")
	}

	raftInstance := &Raft{
		cluster:     cluster,
		clusterSize: 3,
		rpcPort:     8001,
		applyable:   applyable,

		termData: termData,
		log:      entryLog,

		commitIndex: atomic.NewInt64(0),
		lastApplied: 0,

		leaderData: NewLeaderData(0),
	}
	raftInstance.resetElectionTimeout()

	for _, opt := range opts {
		opt(raftInstance)
	}

	return raftInstance, nil
}

func WithBootstrapClusterSize(n int) func(r *Raft) {
	return func(r *Raft) {
		r.clusterSize = n
	}
}

func WithRPCPort(port int) func(r *Raft) {
	return func(r *Raft) {
		r.rpcPort = port
	}
}

func (r *Raft) Run() {
	go func(raft *Raft) {
		for range time.Tick(time.Second * 3) {
			log.Printf("***** Role: %v", raft.termData.GetRole())
		}
	}(r)

	for range time.Tick(time.Millisecond * 25) {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Println("**************************************************** I PAAAAAAANICKEEEEEED ************", r)
				}
			}()
			if err := r.tick(); err != nil {
				log.Printf("Error when ticking: %v", err)
			}
		}()
	}
}

func (r *Raft) tick() error {
	commitIndex := r.commitIndex.Load()

	for commitIndex > r.lastApplied {
		nextEntry, err := r.log.Get(r.lastApplied + 1)
		if err != nil {
			if err == entrylog.ErrDoesNotExist {
				log.Fatal("Commited entry is nonexistent. Shouldn't EVER happen.")
			}
			return errors.Wrap(err, "Couldn't get next entry to be applied")
		}
		if err := r.applyable.Apply(nextEntry); err != nil {
			return errors.Wrap(err, "Error when applying entry")
		}
		r.lastApplied += 1
	}

	ctx := r.termData.Context()
	switch r.termData.GetRole() {
	case raft.Follower, raft.Candidate:
		if time.Now().After(r.electionTimeout) {
			r.resetElectionTimeout()
			go r.startElection()
		}
	case raft.Leader:
		tdSnapshot := r.termData.GetSnapshot()
		r.propagateMessages(ctx, tdSnapshot)
		r.updateCommitIndex()
	}

	return nil
}

func (r *Raft) startElection() {
	log.Println("*********** Starting election **************")
	tdSnapshot, err := r.termData.InitiateElection()
	if err != nil {
		log.Println(errors.Wrap(err, "Couldn't initiate election"))
		return
	}

	votes := 1
	votesChan := make(chan bool)

	wg := sync.WaitGroup{}

	for _, member := range r.cluster.OtherHealthyMembers() {
		wg.Add(1)
		go r.askForVote(tdSnapshot.TermContext, tdSnapshot, member.Name, votesChan)
	}

	answersReceived := 0
	bContinue := true
	for i := 0; i < r.clusterSize-1 && bContinue; i++ {
		select {
		case vote := <-votesChan:
			if vote == true {
				votes += 1
			}
			answersReceived += 1

			if votes > r.clusterSize/2 {
				go drainChannel(votesChan, r.clusterSize-1-answersReceived)
				bContinue = false
			}
		case <-tdSnapshot.TermContext.Done():
			log.Println("************* Election aborted ****************")
			go drainChannel(votesChan, r.clusterSize-1-answersReceived)
			return
		}
	}

	if votes > r.clusterSize/2 {
		r.initializeLeadership(tdSnapshot.TermContext, tdSnapshot.Term)
	}
}

func (r *Raft) askForVote(ctx context.Context, tdSnapshot *termdata.TermDataSnapshot, member string, voteChan chan<- bool) {
	conn, err := r.cluster.GetgRPCConnection(ctx, member, r.rpcPort)
	if err != nil {
		log.Printf("Error when dialing to ask for vote: %v", err)
		return
	}

	cli := raft.NewRaftClient(conn)

	var lastIndexTerm int64 = 0
	if maxEntry := r.log.GetLastEntry(); maxEntry != nil {
		lastIndexTerm = maxEntry.Term
	}

	res, err := cli.RequestVote(ctx, &raft.RequestVoteRequest{
		Term:         tdSnapshot.Term,
		CandidateID:  r.cluster.LocalNodeName(),
		LastLogIndex: r.log.MaxIndex(),
		LastLogTerm:  lastIndexTerm,
	})
	if err != nil {
		log.Printf("Error when requesting vote: %v", err)
		voteChan <- false
		return
	}

	if tdSnapshot.Term < res.Term {
		log.Println("Becoming follower because of term override")
		r.becomeFollower(tdSnapshot.Term, res.Term)
	}

	voteChan <- res.VoteGranted
}

func drainChannel(ch <-chan bool, count int) {
	for i := 0; i < count; i++ {
		<-ch
	}
}

func (r *Raft) initializeLeadership(ctx context.Context, term int64) {
	log.Printf("*************** I'm becoming leader! Term: %v ******************", term)
	ok := r.termData.BecomeLeader(term)
	if !ok {
		log.Printf("*************** Couldn't become leader. Term: %v ******************", r.termData.GetTerm())
		return
	}

	r.leaderData = NewLeaderData(r.log.MaxIndex())
}

func (r *Raft) propagateMessages(ctx context.Context, tdSnapshot *termdata.TermDataSnapshot) {
	ctx, _ = context.WithTimeout(ctx, time.Millisecond*60)
	wg := sync.WaitGroup{}
	for _, member := range r.cluster.OtherHealthyMembers() {
		noPreviousAppendEntries := r.leaderData.GetLastAppendEntries(member.Name).IsZero()
		if noPreviousAppendEntries ||
			r.log.MaxIndex() >= r.leaderData.GetNextIndex(member.Name) ||
			time.Since(r.leaderData.GetLastAppendEntries(member.Name)) < time.Millisecond*200 {

			wg.Add(1)
			go func(node serf.Member) {
				r.sendAppendEntries(ctx, tdSnapshot, node.Name, noPreviousAppendEntries)
				wg.Done()
			}(member)
		}
	}
	wg.Wait()
}

func (r *Raft) sendAppendEntries(ctx context.Context, tdSnapshot *termdata.TermDataSnapshot, member string, empty bool) {
	conn, err := r.cluster.GetgRPCConnection(ctx, member, r.rpcPort)
	if err != nil {
		log.Printf("Error when dialing to send append entries: %v", err)
		return
	}

	cli := raft.NewRaftClient(conn)

	nextIndex := r.leaderData.GetNextIndex(member)
	prevIndex := nextIndex - 1
	var prevTerm int64 = 0
	entry, _ := r.log.Get(prevIndex)
	if entry != nil {
		prevTerm = entry.Term
	}

	var payload *raft.Entry
	if !empty {
		if r.log.MaxIndex() >= nextIndex {
			// If this errors than payload ends up being nil, that's ok.
			entry, _ = r.log.Get(nextIndex)
			if entry != nil {
				payload = entry
			}
		}
	}
	res, err := cli.AppendEntries(ctx, &raft.AppendEntriesRequest{
		Term:         tdSnapshot.Term,
		LeaderID:     r.cluster.LocalNodeName(),
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entry:        payload,
		LeaderCommit: r.commitIndex.Load(),
	})
	if err != nil {
		log.Printf("Couldn't send append entries to %v: %v", member, err)
		return
	}

	r.leaderData.NoteAppendEntries(member)

	if tdSnapshot.Term < res.Term {
		log.Println("Becoming follower because of term override")
		r.becomeFollower(tdSnapshot.Term, res.Term)
		return
	}

	if res.Success {
		if payload != nil {
			r.leaderData.SetNextIndex(member, nextIndex, nextIndex+1)
			r.leaderData.NoteMatchIndex(member, nextIndex)
		} else {
			r.leaderData.NoteMatchIndex(member, prevIndex)
		}
	} else {
		if nextIndex != 1 {
			r.leaderData.SetNextIndex(member, nextIndex, nextIndex-1)
		}
	}
}

func (r *Raft) updateCommitIndex() {
	commitIndex := r.commitIndex.Load()
	if r.log.MaxIndex() == commitIndex {
		return
	}

	for candidate := r.log.MaxIndex(); candidate > commitIndex; candidate-- {
		log.Printf("****** Trying candidate: %v", candidate)

		if entry, err := r.log.Get(candidate); err != nil {
			return
		} else {
			if entry.Term != r.termData.GetTerm() {
				return
			}
		}

		oks := 1
		for _, node := range r.cluster.OtherMembers() {
			if r.leaderData.GetMatchIndex(node.Name) >= candidate {
				oks += 1
			} else {
				log.Printf("****** Node %v match index is too low: %v", node.Name, r.leaderData.GetMatchIndex(node.Name))
			}
		}
		if oks > r.clusterSize/2 {
			log.Printf("****** Success with candidate: %v", candidate)
			// We do Atomic Compare and Set so that it only gets updated if it haven't changed since we read it.
			// This may be important in case something got overriden for some reason and bumped up.
			// Because that could potentially break the monotonicity of the commit index.
			r.commitIndex.CAS(commitIndex, candidate)
			break
		}
	}
}

func (r *Raft) becomeFollower(prevTerm, term int64) {
	r.resetElectionTimeout()
	r.termData.OverrideTerm(prevTerm, term)
}

func (r *Raft) resetElectionTimeout() {
	r.electionTimeout = time.Now().Add(time.Millisecond * time.Duration(3000+rand.Intn(2000)))
}

func (r *Raft) AppendEntries(ctx context.Context, req *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	tdSnapshot := r.termData.GetSnapshot()

	if req.Term < tdSnapshot.Term {
		log.Printf("***** False because old term: %v current: %v.", req.Term, tdSnapshot.Term)
		return &raft.AppendEntriesResponse{
			Term:    tdSnapshot.Term,
			Success: false,
		}, nil
	}

	if req.Term > tdSnapshot.Term {
		log.Println("Becoming follower because of term override")
		r.becomeFollower(tdSnapshot.Term, req.Term)
	}
	if tdSnapshot.Role == raft.Candidate {
		r.termData.AbortElection()
		tdSnapshot = r.termData.GetSnapshot()
	}

	// This really shouldn't happen, cause it's possible only with a split brain
	if tdSnapshot.Role == raft.Leader {
		log.Fatal("Received append entries, even though I'm the leader with the current term.")
	}

	if success := r.termData.SetLeader(req.Term, req.LeaderID); !success {
		return &raft.AppendEntriesResponse{
			Term:    r.termData.GetTerm(),
			Success: false,
		}, nil
	}
	r.resetElectionTimeout()

	if !r.log.Exists(req.PrevLogIndex, req.PrevLogTerm) && req.PrevLogIndex != 0 {
		log.Printf("***** False because prev log term doesn't exists. Previous log term: %v Actual max: %v", req.PrevLogIndex, r.log.MaxIndex())
		return &raft.AppendEntriesResponse{
			Term:    tdSnapshot.Term,
			Success: false,
		}, nil
	}

	if req.Entry != nil {
		curLogIndex := req.PrevLogIndex + 1

		shouldInsert := true

		entry, err := r.log.Get(curLogIndex)
		if err == nil {
			if entry.Term != req.Term {
				if err := r.log.DeleteFrom(curLogIndex); err != nil {
					return nil, errors.Wrap(err, "Couldn't persist my cleared log")
				}
			} else {
				shouldInsert = false
			}
		}

		if shouldInsert {
			r.log.Append(req.Entry)
			log.Println("*************** Added new entry to my log")
		}
	}

	commitIndex := r.commitIndex.Load()
	if req.LeaderCommit > commitIndex {
		maxEntryIndex := r.log.MaxIndex()
		if maxEntryIndex < req.LeaderCommit {
			log.Printf("****** Setting commit index: %v", maxEntryIndex)
			r.commitIndex.CAS(commitIndex, maxEntryIndex)
		} else {
			log.Printf("****** Setting commit index: %v", req.LeaderCommit)
			r.commitIndex.CAS(commitIndex, req.LeaderCommit)
		}
	}

	return &raft.AppendEntriesResponse{
		Term:    tdSnapshot.Term,
		Success: true,
	}, nil
}

func (r *Raft) RequestVote(ctx context.Context, req *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	tdSnapshot := r.termData.GetSnapshot()
	if req.Term <= tdSnapshot.Term {
		return &raft.RequestVoteResponse{
			Term:        tdSnapshot.Term,
			VoteGranted: false,
		}, nil
	}

	if tdSnapshot.Term < req.Term {
		r.termData.OverrideTerm(tdSnapshot.Term, req.Term)
		tdSnapshot = r.termData.GetSnapshot()
	}

	var curLastLogIndex, curLastLogTerm int64
	lastEntry := r.log.GetLastEntry()
	lastIndex := r.log.MaxIndex()

	if lastEntry != nil {
		curLastLogIndex = lastIndex
		curLastLogTerm = lastEntry.Term
	} else {
		curLastLogIndex = 0
		curLastLogTerm = 0
	}

	if tdSnapshot.VotedFor == "" || tdSnapshot.VotedFor == req.CandidateID {
		if req.LastLogIndex >= curLastLogIndex && req.LastLogTerm >= curLastLogTerm {

			if r.termData.SetVotedFor(tdSnapshot.Term, req.CandidateID) {
				log.Printf("****** Voting for: %v ******", req.CandidateID)

				return &raft.RequestVoteResponse{
					Term:        tdSnapshot.Term,
					VoteGranted: true,
				}, nil
			}
		}
	}

	return &raft.RequestVoteResponse{
		Term:        tdSnapshot.Term,
		VoteGranted: false,
	}, nil
}

func (r *Raft) NewEntry(ctx context.Context, entry *raft.Entry) (*raft.EntryResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if entry.ID == "" {
		entry.ID = uuid.NewV4().String()
	}
	tdSnapshot := r.termData.GetSnapshot()
	if tdSnapshot.Role != raft.Leader {
		for _, member := range r.cluster.OtherMembers() {
			if member.Name == tdSnapshot.Leader {
				log.Printf("Redirecting new entry to leader: %v", tdSnapshot.Leader)
				conn, err := r.cluster.GetgRPCConnection(ctx, member.Name, r.rpcPort)
				if err != nil {
					log.Printf("Error when dialing to send append entries: %v", err)
					return nil, err
				}

				cli := raft.NewRaftClient(conn)

				return cli.NewEntry(ctx, entry)
			}
		}

		return nil, errors.Errorf("Couldn't find leader")
	}
	entry.Term = tdSnapshot.Term

	log.Println("****** Adding new entry to log as leader.")
	entryIndex, err := r.log.Append(entry)
	if err != nil {
		return nil, errors.Wrap(err, "Couldn't append entry to my log")
	}

	ticker := time.NewTicker(time.Millisecond * 5)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if r.commitIndex.Load() >= entryIndex {
				finalEntry, err := r.log.Get(entryIndex)
				if err != nil {
					return nil, errors.Wrap(err, "Inexistant entry commited")
				}
				if finalEntry.ID == entry.ID {
					return &raft.EntryResponse{}, nil
				} else {
					return nil, errors.Errorf("Operation overriden. New ID: %v", finalEntry.ID)
				}
			}
		case <-ctx.Done():
			return nil, errors.Wrap(ctx.Err(), "Context cancelled")
		}
	}

	return &raft.EntryResponse{}, nil
}

func (r *Raft) GetDebugData() []raft.Entry {
	return r.log.DebugData()
}
