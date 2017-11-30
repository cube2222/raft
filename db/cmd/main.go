package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"

	"github.com/cube2222/raft"
	"github.com/cube2222/raft/cluster"
	"github.com/cube2222/raft/db"
	"github.com/cube2222/raft/db/command"
	"github.com/cube2222/raft/db/query"
	raftimpl "github.com/cube2222/raft/raft"
	"github.com/gorilla/mux"
	"google.golang.org/grpc"
)

func main() {
	ctx := context.Background()
	config := LoadConfig()

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("Couldn't get hostname: %v", err)
	}

	// Trying to connect to everybody in cluster
	dbCluster, err := cluster.NewCluster(ctx, hostname, config.ClusterAddresses)
	if err != nil {
		log.Fatalf("Couldn't setup new cluster: %v", err)
	}

	queryHandler := query.NewQueryHandler(dbCluster)
	if err != nil {
		log.Fatalf("Couldn't set up query handler: %v", err)
	}

	myRaft, err := raftimpl.NewRaft(ctx, dbCluster, queryHandler, hostname)
	if err != nil {
		log.Fatal(err)
	}

	lis, err := net.Listen("tcp", ":8001")
	if err != nil {
		log.Fatal(err)
	}
	s := grpc.NewServer()
	raft.RegisterRaftServer(s, myRaft)
	db.RegisterDBServer(s, queryHandler)

	commandHandler := command.NewCommandHandler(myRaft)

	go myRaft.Run()

	m := mux.NewRouter()
	m.PathPrefix("/command").Subrouter().NewRoute().Handler(http.StripPrefix("/command", commandHandler.HTTPHandler()))
	m.PathPrefix("/query").Subrouter().NewRoute().Handler(http.StripPrefix("/query", queryHandler.HTTPHandler()))

	go http.ListenAndServe(":8002", m)

	if err := s.Serve(lis); err != nil {
		log.Fatal(err)
	}
}
