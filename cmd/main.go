package main

import (
	"github.com/cube2222/raft/raft"
	raft2 "github.com/cube2222/raft"
	"log"
	"os"
	"net"
	"google.golang.org/grpc"
	"net/http"
	"io/ioutil"
	"fmt"
)

type MyApplyable struct {
}

func (*MyApplyable) Apply(entry *raft2.Entry) error {
	log.Printf("******************* Applying: %s", entry.Data)
	return nil
}

func main() {
	myApplyable := &MyApplyable{}

	clusterAddress := ""
	if len(os.Args) == 3 {
		clusterAddress = os.Args[2]
	}

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal("Couldn't get hostname")
	}

	myRaft, err := raft.NewRaft(myApplyable, hostname, os.Args[1], clusterAddress)
	if err != nil {
		log.Fatal(err)
	}

	lis, err := net.Listen("tcp", ":8001")
	if err != nil {
		log.Fatal(err)
	}
	s := grpc.NewServer()
	raft2.RegisterRaftServer(s, myRaft)

	go myRaft.Run()

	go http.ListenAndServe(":8002", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			fmt.Fprint(w, err)
			return
		}
		_, err = myRaft.NewEntry(r.Context(), &raft2.Entry{
			Data: data,
		})
		if err != nil {
			fmt.Fprint(w, err)
			return
		}
		fmt.Fprint(w, "Success")
	}))

	if err := s.Serve(lis); err != nil {
		log.Fatal(err)
	}
}
