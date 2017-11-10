package raft

import (
	"github.com/cube2222/raft"
	"golang.org/x/net/context"
	"log"
	"time"
	"math/rand"
	"github.com/hashicorp/serf/serf"
	"github.com/pkg/errors"
	"sync"
	"fmt"
	"google.golang.org/grpc"
	"github.com/satori/go.uuid"
)

type Applyable interface {
	Apply(entry *raft.Entry) error
}

type Raft struct {
	cluster     *serf.Serf
	clusterSize int
	applyable   Applyable

	// On all servers, in theory persistent
	// Term specific
	termData *termData

	log *entryLog

	// On all servers, volatile
	// TODO: To też tylko monotonicznie rosnące
	commitIndex int64
	lastApplied int64

	// On leader, reinitialized after collection
	leaderData *leaderData

	// On non-leader
	electionTimeout time.Time
}

func NewRaft(applyable Applyable, name, clusterAddress string, opts ... func(*Raft)) (*Raft, error) {
	cluster, err := setupCluster(
		name,
		clusterAddress,
	)
	if err != nil {
		return nil, errors.Wrap(err, "Couldn't setup cluster.")
	}

	raftInstance := &Raft{
		cluster:     cluster,
		clusterSize: 3,
		applyable:   applyable,

		termData: NewTermData(cluster.LocalMember().Name),
		log:      NewEntryLog(),

		commitIndex: 0,
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

func (r *Raft) Close() {
	r.cluster.Leave()
}

func setupCluster(nodeName string, clusterAddr string) (*serf.Serf, error) {
	conf := serf.DefaultConfig()
	conf.Init()

	conf.MemberlistConfig.Name = nodeName

	cluster, err := serf.Create(conf)
	if err != nil {
		return nil, errors.Wrap(err, "Couldn't create cluster")
	}

	_, err = cluster.Join([]string{clusterAddr}, true)
	if err != nil {
		log.Printf("Couldn't join cluster, starting own: %v\n", err)
	}

	return cluster, nil
}

func (r *Raft) tick() error {
	if n := r.cluster.NumNodes(); n < r.clusterSize {
		return errors.Errorf("Waiting for all nodes to join. Currently: %d Want: %d", n, r.clusterSize)
	}

	for r.commitIndex > r.lastApplied {
		nextEntry, err := r.log.Get(r.lastApplied + 1)
		if err != nil {
			if err == ErrDoesNotExist {
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
	case Follower, Candidate:
		if time.Now().After(r.electionTimeout) {
			r.startElection()
		}
	case Leader:
		tdSnapshot := r.termData.GetSnapshot()
		r.propagateMessages(ctx, tdSnapshot)
		r.updateCommitIndex()
	}

	return nil
}

func (r *Raft) startElection() {
	log.Println("*********** Starting election **************")
	tdSnapshot := r.termData.InitiateElection()

	r.resetElectionTimeout()
	votes := 1
	votesChan := make(chan bool)

	wg := sync.WaitGroup{}

	for _, node := range r.cluster.Members() {
		if node.Name != r.cluster.LocalMember().Name && node.Status == serf.StatusAlive {
			wg.Add(1)
			go r.askForVote(tdSnapshot.TermContext, tdSnapshot, fmt.Sprintf("%v:%v", node.Addr, 8001), votesChan)
		}
	}

	answersReceived := 0
	bContinue := true
	for i := 0; i < r.clusterSize-1 && bContinue; i++ {
		select {
		case vote := <-votesChan:
			if vote == true {
				votes += 1
			} else {
				answersReceived += 1
			}

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

func (r *Raft) askForVote(ctx context.Context, tdSnapshot *TermDataSnapshot, address string, voteChan chan<- bool) {
	// TODO: Tu MUSI być cachowanie połączeń
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Printf("Error when dialing to ask for vote: %v", err)
		return
	}
	defer conn.Close()

	cli := raft.NewRaftClient(conn)

	var lastIndexTerm int64 = 0
	if maxEntry := r.log.GetLastEntry(); maxEntry != nil {
		lastIndexTerm = maxEntry.Term
	}

	res, err := cli.RequestVote(ctx, &raft.RequestVoteRequest{
		Term:         tdSnapshot.Term,
		CandidateID:  r.cluster.LocalMember().Name,
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
	}

	r.leaderData = NewLeaderData(r.log.MaxIndex())
}

func (r *Raft) propagateMessages(ctx context.Context, tdSnapshot *TermDataSnapshot) {
	ctx, _ = context.WithTimeout(ctx, time.Millisecond*60)
	wg := sync.WaitGroup{}
	for _, node := range r.cluster.Members() {
		if node.Name != r.cluster.LocalMember().Name && node.Status == serf.StatusAlive {
			if r.leaderData.GetLastAppendEntries(node.Name).IsZero() {
				wg.Add(1)
				go func(node serf.Member) {
					r.sendAppendEntries(ctx, tdSnapshot, node.Name, fmt.Sprintf("%v:%v", node.Addr, 8001), true)
					wg.Done()
				}(node)
			} else if r.log.MaxIndex() >= r.leaderData.GetNextIndex(node.Name) || time.Since(r.leaderData.GetLastAppendEntries(node.Name)) < time.Millisecond*200 {
				wg.Add(1)
				go func(node serf.Member) {
					r.sendAppendEntries(ctx, tdSnapshot, node.Name, fmt.Sprintf("%v:%v", node.Addr, 8001), false)
					wg.Done()
				}(node)
			}
		}
	}
	wg.Wait()
}

func (r *Raft) sendAppendEntries(ctx context.Context, tdSnapshot *TermDataSnapshot, nodeID string, address string, empty bool) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Printf("Error when dialing to send append entries: %v", err)
		return
	}
	defer conn.Close()

	cli := raft.NewRaftClient(conn)

	nextIndex := r.leaderData.GetNextIndex(nodeID)
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
		LeaderID:     r.cluster.LocalMember().Name,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entry:        payload,
		LeaderCommit: r.commitIndex,
	})
	if err != nil {
		log.Printf("Couldn't send append entries to %v: %v", nodeID, err)
		return
	}

	r.leaderData.NoteAppendEntries(nodeID)

	if tdSnapshot.Term < res.Term {
		log.Println("Becoming follower because of term override")
		r.becomeFollower(tdSnapshot.Term, res.Term)
		return
	}

	if res.Success {
		if payload != nil {
			r.leaderData.SetNextIndex(nodeID, nextIndex, nextIndex+1)
			r.leaderData.NoteMatchIndex(nodeID, nextIndex)
		} else {
			r.leaderData.NoteMatchIndex(nodeID, prevIndex)
		}
	} else {
		if nextIndex != 1 {
			r.leaderData.SetNextIndex(nodeID, nextIndex, nextIndex-1)
		}
	}
}

func (r *Raft) updateCommitIndex() {
	if r.log.MaxIndex() == r.commitIndex {
		return
	}

	for candidate := r.log.MaxIndex(); candidate > r.commitIndex; candidate-- {
		log.Printf("****** Trying candidate: %v", candidate)

		if entry, err := r.log.Get(candidate); err != nil {
			return
		} else {
			if entry.Term != r.termData.GetTerm() {
				return
			}
		}

		oks := 1
		for _, node := range r.cluster.Members() {
			if node.Name != r.cluster.LocalMember().Name {
				if r.leaderData.GetMatchIndex(node.Name) >= candidate {
					oks += 1
				} else {
					log.Printf("****** Node %v match index is too low: %v", node.Name, r.leaderData.GetMatchIndex(node.Name))
				}
			}
		}
		if oks > r.clusterSize/2 {
			log.Printf("****** Success with candidate: %v", candidate)
			// TODO: To też musi lecieć w term data, żeby zadbać o to, żeby nie doszło do update'a jeżeli już nie jesteśmy liderem
			r.commitIndex = candidate
			break
		}
	}
}

func (r *Raft) becomeFollower(prevTerm, term int64) {
	r.termData.OverrideTerm(prevTerm, term)
	r.resetElectionTimeout()
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

	if req.Term < tdSnapshot.Term {
		log.Println("Becoming follower because of term override")
		r.becomeFollower(tdSnapshot.Term, req.Term)
	}
	if tdSnapshot.Role == Candidate {
		r.termData.AbortElection()
		tdSnapshot = r.termData.GetSnapshot()
	}

	// This really shouldn't happen, cause it's possible only with a split brain
	if tdSnapshot.Role == Leader {
		log.Fatal("Received append entries, even though I'm the leader with the current term.")
	}

	r.termData.SetLeader(req.LeaderID)
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
				r.log.DeleteFrom(curLogIndex)
			} else {
				shouldInsert = false
			}
		}

		if shouldInsert {
			r.log.Append(req.Entry, req.Term)
			log.Println("*************** Added new entry to my log")
		}
	}

	if req.LeaderCommit > r.commitIndex {
		maxEntryIndex := r.log.MaxIndex()
		if maxEntryIndex < req.LeaderCommit {
			log.Printf("****** Setting commit index: %v", maxEntryIndex)
			r.commitIndex = maxEntryIndex
		} else {
			log.Printf("****** Setting commit index: %v", req.LeaderCommit)
			r.commitIndex = req.LeaderCommit
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
	if entry.ID == "" {
		entry.ID = uuid.NewV4().String()
	}
	tdSnapshot := r.termData.GetSnapshot()
	if tdSnapshot.Role != Leader {
		for _, node := range r.cluster.Members() {
			if node.Name == tdSnapshot.Leader {
				log.Printf("Redirecting new entry to leader: %v", tdSnapshot.Leader)
				conn, err := grpc.Dial(fmt.Sprintf("%v:%v", node.Addr, 8001), grpc.WithInsecure())
				if err != nil {
					log.Printf("Error when dialing to send append entries: %v", err)
					return nil, err
				}
				defer conn.Close()

				cli := raft.NewRaftClient(conn)

				return cli.NewEntry(ctx, entry)
			}
		}

		return nil, errors.Errorf("Couldn't find leader")
	}
	entry.Term = tdSnapshot.Term

	log.Println("****** Adding new entry to log as leader.")
	entryIndex := r.log.Append(entry, tdSnapshot.Term)

	for {
		if r.commitIndex >= entryIndex {
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
		time.Sleep(time.Millisecond * 5)
	}

	return &raft.EntryResponse{}, nil
}

func (r *Raft) GetDebugData() []raft.Entry {
	return r.log.log
}
