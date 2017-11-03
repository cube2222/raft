package main

import (
	"github.com/cube2222/raft/raft"
	raft2 "github.com/cube2222/raft"
	"log"
	"os"
	"net"
	"google.golang.org/grpc"
)

type MyApplyable struct {
}

func (*MyApplyable) Apply(entry *raft2.Entry) error {
	log.Printf("Applying: %s", entry.Data)
	panic("implement me")
}

func main() {
	myApplyable := &MyApplyable{}

	clusterAddress := ""
	if len(os.Args) == 4 {
		clusterAddress = os.Args[3]
	}

	myRaft, err := raft.NewRaft(myApplyable, os.Args[1], os.Args[2], clusterAddress)
	if err != nil {
		log.Fatal(err)
	}

	lis, err := net.Listen("tcp", ":")
	if err != nil {
		log.Fatal(err)
	}
	s := grpc.NewServer()
	raft2.RegisterRaftServer(s, myRaft)

	go myRaft.Run()

	if err := s.Serve(lis); err != nil {
		log.Fatal(err)
	}
}
