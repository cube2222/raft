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
)

type role int

const (
	Follower  role = iota
	Candidate
	Leader
)

type Applyable interface {
	Apply(entry *raft.Entry) error
}

// TODO: Zrób globalny term context.
type Raft struct {
	curRole     role
	leaderID    string
	cluster     *serf.Serf
	clusterSize int
	applyable   Applyable

	// On all servers, in theory persistent
	curTerm              int64
	curTermContext       context.Context
	curTermContextCancel context.CancelFunc
	votedFor             string
	log                  *entryLog

	// On all servers, volatile
	commitIndex int64
	lastApplied int64

	// On leader, reinitialized after collection
	leaderData *leaderData

	// On non-leader
	electionTimeout time.Time
}

func NewRaft(applyable Applyable, name, advertiseAddress, clusterAddress string) (*Raft, error) {
	cluster, err := setupCluster(
		name,
		advertiseAddress,
		clusterAddress,
	)
	if err != nil {
		log.Printf("Name: %v, AdvertiseAddress: %v, ClusterAddress: %v", name, advertiseAddress, clusterAddress)
		return nil, errors.Wrap(err, "Couldn't setup cluster.")
	}

	curTermContext, cancel := context.WithCancel(context.Background())

	raftInstance := &Raft{
		curRole:     Follower,
		leaderID:    "",
		cluster:     cluster,
		clusterSize: 3,
		applyable:   applyable,

		curTerm:              0,
		curTermContext:       curTermContext,
		curTermContextCancel: cancel,
		votedFor:             "",
		log:                  NewEntryLog(),

		commitIndex: 0,
		lastApplied: 0,

		leaderData: NewLeaderData(0),

		electionTimeout: time.Now().Add(time.Millisecond * time.Duration(1000+rand.Intn(2000))),
	}

	return raftInstance, nil
}

func (r *Raft) Run() {
	go func(raft *Raft) {
		for range time.Tick(time.Second*3) {
			log.Printf("***** %v is Leader with term %v", raft.leaderID, raft.curTerm)
		}
	}(r)

	for range time.Tick(time.Millisecond * 50) {
		if err := r.Tick(); err != nil {
			log.Printf("Error when ticking: %v", err)
		}
	}
}

func (r *Raft) Close() {
	r.cluster.Leave()
}

func setupCluster(nodeName string, advertiseAddr string, clusterAddr string) (*serf.Serf, error) {
	conf := serf.DefaultConfig()
	conf.Init()

	conf.MemberlistConfig.Name = nodeName
	conf.MemberlistConfig.AdvertiseAddr = advertiseAddr

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

func (r *Raft) Tick() error {
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
		if err := r.applyable.Apply(&nextEntry.entry); err != nil {
			return errors.Wrap(err, "Error when applying entry")
		}
		r.lastApplied += 1
	}

	ctx := r.curTermContext
	switch r.curRole {
	case Follower, Candidate:
		if time.Now().After(r.electionTimeout) {
			r.StartElection()
		}
	case Leader:
		r.PropagateMessages(ctx)
		r.UpdateCommitIndex()
	}

	return nil
}

func (r *Raft) StartElection() {
	log.Println("*********** Starting election **************")
	r.curTermContextCancel()

	r.curTerm += 1
	r.leaderID = ""
	r.electionTimeout = time.Now().Add(time.Millisecond * time.Duration(1000+rand.Intn(2000)))
	ctx, cancel := context.WithCancel(context.Background())
	r.curTermContext, r.curTermContextCancel = ctx, cancel
	votes := 1
	votesChan := make(chan bool)

	wg := sync.WaitGroup{}
	wg.Add(r.clusterSize - 1)

	for _, node := range r.cluster.Members() {
		if node.Name != r.cluster.LocalMember().Name && node.Status == serf.StatusAlive {
			go r.AskForVote(ctx, fmt.Sprintf("%v:%v", node.Addr, 8001), votesChan)
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
		case <-r.curTermContext.Done():
			log.Println("************* Election aborted ****************")
			go drainChannel(votesChan, r.clusterSize-1-answersReceived)
			return
		}
	}

	if votes > r.clusterSize/2 {
		r.InitializeLeadership(r.curTermContext)
	}
}

func (r *Raft) AskForVote(ctx context.Context, address string, voteChan chan<- bool) {
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
		lastIndexTerm = maxEntry.term
	}

	res, err := cli.RequestVote(ctx, &raft.RequestVoteRequest{
		Term:         r.curTerm,
		CandidateID:  r.cluster.LocalMember().Name,
		LastLogIndex: r.log.MaxIndex(),
		LastLogTerm:  lastIndexTerm,
	})
	if err != nil {
		log.Printf("Error when requesting vote: %v", err)
		voteChan <- false
		return
	}

	if r.curTerm < res.Term {
		log.Println("Becoming follower because of term override")
		r.electionTimeout = time.Now().Add(time.Millisecond * time.Duration(1000+rand.Intn(2000)))
		voteChan <- false
		r.curTermContextCancel()
		r.curTermContext, r.curTermContextCancel = context.WithCancel(context.Background())
		r.curTerm = res.Term
		r.curRole = Follower
		r.votedFor = ""
	}

	voteChan <- res.VoteGranted
}

func drainChannel(ch <-chan bool, count int) {
	for i := 0; i < count; i++ {
		<-ch
	}
}

func (r *Raft) InitializeLeadership(ctx context.Context) {
	log.Printf("*************** I'm becoming leader! Term: %v ******************", r.curTerm)
	r.curRole = Leader
	r.leaderID = r.cluster.LocalMember().Name

	r.leaderData = NewLeaderData(r.log.MaxIndex())
}

func (r *Raft) PropagateMessages(ctx context.Context) {
	wg := sync.WaitGroup{}
	wg.Add(r.cluster.NumNodes() - 1)
	for _, node := range r.cluster.Members() {
		if node.Name != r.cluster.LocalMember().Name && node.Status == serf.StatusAlive {
			if r.leaderData.GetLastAppendEntries(node.Name).IsZero() {
				go func(node serf.Member) {
					r.SendAppendEntries(ctx, node.Name, fmt.Sprintf("%v:%v", node.Addr, 8001), true)
					wg.Done()
				}(node)
			} else if r.log.MaxIndex() >= r.leaderData.GetNextIndex(node.Name) || time.Since(r.leaderData.GetLastAppendEntries(node.Name)) < time.Millisecond*200 {
				go func(node serf.Member) {
					r.SendAppendEntries(ctx, node.Name, fmt.Sprintf("%v:%v", node.Addr, 8001), false)
					wg.Done()
				}(node)
			}
		}
	}
	wg.Wait()
}

func (r *Raft) SendAppendEntries(ctx context.Context, nodeID string, address string, empty bool) {
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
		prevTerm = entry.term
	}

	var payload *raft.Entry
	if !empty {
		if r.log.MaxIndex() >= nextIndex {
			// If this errors than payload ends up being nil, that's ok.
			entry, _ = r.log.Get(nextIndex)
			if entry != nil {
				payload = &entry.entry
			}
		}
	}
	res, err := cli.AppendEntries(ctx, &raft.AppendEntriesRequest{
		Term:         r.curTerm,
		LeaderID:     r.cluster.LocalMember().Name,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entry:        payload,
		LeaderCommit: r.commitIndex,
	})
	if err != nil {
		log.Printf("Couldn't send append entries: %v", err)
		return
	}

	r.leaderData.NoteAppendEntries(nodeID)

	if res.Term < r.curTerm {
		if r.curTerm < res.Term {
			log.Println("Becoming follower because of term override")
			r.electionTimeout = time.Now().Add(time.Millisecond * time.Duration(1000+rand.Intn(2000)))
			r.curTermContextCancel()
			r.curTermContext, r.curTermContextCancel = context.WithCancel(context.Background())
			r.curTerm = res.Term
			r.curRole = Follower
			r.votedFor = ""
		}
		return
	}

	if res.Success {
		r.leaderData.SetNextIndex(nodeID, nextIndex, nextIndex + 1)
		r.leaderData.NoteMatchIndex(nodeID, nextIndex)
	} else {
		if nextIndex != 1 {
			r.leaderData.SetNextIndex(nodeID, nextIndex, nextIndex - 1)
		}
	}
}

func (r *Raft) UpdateCommitIndex() {
	if r.log.MaxIndex() == r.commitIndex {
		return
	}

	for candidate := r.log.MaxIndex(); candidate > r.commitIndex; candidate-- {

		if entry, err := r.log.Get(candidate); err != nil {
			return
		} else {
			if entry.term != r.curTerm {
				return
			}
		}

		oks := 1
		for _, node := range r.cluster.Members() {
			if node.Name != r.cluster.LocalMember().Name {
				if r.leaderData.GetMatchIndex(node.Name) >= candidate {
					oks += 1
				}
			}
		}
		if oks >= r.clusterSize/2 {
			r.commitIndex = candidate
			break
		}
	}
}

func (r *Raft) AppendEntries(ctx context.Context, req *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	if req.Term < r.curTerm {
		return &raft.AppendEntriesResponse{
			Term:    r.curTerm,
			Success: false,
		}, nil
	}

	if r.curTerm < req.Term {
		log.Println("Becoming follower because of term override")
		r.curTermContextCancel()
		r.curTermContext, r.curTermContextCancel = context.WithCancel(context.Background())
		r.curTerm = req.Term
		r.curRole = Follower
		r.votedFor = ""
	}
	if r.curRole == Candidate {
		r.curRole = Follower
	}

	// This really shouldn't happen, cause it's possible only with a split brain
	if r.curRole == Leader {
		log.Fatal("Received append entries, even though I'm the leader with the current term.")
	}

	r.leaderID = req.LeaderID
	r.electionTimeout = time.Now().Add(time.Millisecond * time.Duration(1000+rand.Intn(2000)))

	if !r.log.Exists(req.PrevLogIndex, req.PrevLogTerm) && req.PrevLogIndex != 0 {
		return &raft.AppendEntriesResponse{
			Term:    r.curTerm,
			Success: false,
		}, nil
	}

	if req.Entry != nil {
		curLogIndex := req.PrevLogIndex + 1

		shouldInsert := true

		entry, err := r.log.Get(curLogIndex)
		if err == nil {
			if entry.term != req.Term {
				r.log.DeleteFrom(curLogIndex)
			} else {
				shouldInsert = false
			}
		}

		if shouldInsert {
			r.log.Append(req.Entry, req.Term)
		}
	}

	if req.LeaderCommit > r.commitIndex {
		maxEntryIndex := r.log.MaxIndex()
		if maxEntryIndex < req.LeaderCommit {
			r.commitIndex = maxEntryIndex
		} else {
			r.commitIndex = req.LeaderCommit
		}
	}

	return &raft.AppendEntriesResponse{
		Term:    r.curTerm,
		Success: true,
	}, nil
}

func (r *Raft) RequestVote(ctx context.Context, req *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	if req.Term < r.curTerm {
		return &raft.RequestVoteResponse{
			Term:        r.curTerm,
			VoteGranted: false,
		}, nil
	}

	if r.curTerm < req.Term {
		r.curTerm = req.Term
		r.curRole = Follower
		r.votedFor = ""
	}

	var curLastLogIndex, curLastLogTerm int64
	lastEntry := r.log.GetLastEntry()
	lastIndex := r.log.MaxIndex()

	if lastEntry != nil {
		curLastLogIndex = lastIndex
		curLastLogTerm = lastEntry.term
	} else {
		curLastLogIndex = 0
		curLastLogTerm = 0
	}

	if r.votedFor == "" || r.votedFor == req.CandidateID {
		if req.LastLogIndex >= curLastLogIndex && req.LastLogTerm >= curLastLogTerm {

			r.votedFor = req.CandidateID

			log.Printf("****** Voting for: %v ******", r.votedFor)

			return &raft.RequestVoteResponse{
				Term:        r.curTerm,
				VoteGranted: true,
			}, nil
		}
	}

	return &raft.RequestVoteResponse{
		Term:        r.curTerm,
		VoteGranted: false,
	}, nil
}

func (r *Raft) NewEntry(ctx context.Context, entry *raft.Entry) (*raft.EntryResponse, error) {
	if r.curRole != Leader {
		for _, node := range r.cluster.Members() {
			log.Printf("Redirecting new entry to leader: %v", r.leaderID)
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
