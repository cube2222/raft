package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"

	"github.com/cube2222/raft"
	"github.com/cube2222/raft/cluster"
	raftimpl "github.com/cube2222/raft/raft"
	"github.com/cube2222/raft/testdb"
	"google.golang.org/grpc"
)

func main() {
	db := testdb.NewDocumentDatabase()

	config := LoadConfig()

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal("Couldn't get hostname")
	}

	// Trying to connect to everybody in cluster
	cluster, err := cluster.NewCluster(ctx, hostname, config.ClusterAddresses)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	myRaft, err := raftimpl.NewRaft(ctx, cluster, db, hostname)
	if err != nil {
		log.Fatal(err)
	}

	// You could in theory create one read interface and one write interface for the document db so there
	// isn't any circular coupling.
	db.SetRaftModule(myRaft)

	lis, err := net.Listen("tcp", ":8001")
	if err != nil {
		log.Fatal(err)
	}
	s := grpc.NewServer()
	raft.RegisterRaftServer(s, myRaft)

	go myRaft.Run()

	go http.ListenAndServe(":8002", db.GetHTTPHandler())

	if err := s.Serve(lis); err != nil {
		log.Fatal(err)
	}
}
