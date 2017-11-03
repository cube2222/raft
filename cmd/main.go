package main

import (
	"github.com/cube2222/raft/raft"
	raft2 "github.com/cube2222/raft"
	"log"
	"os"
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

	myRaft, err := raft.NewRaft(myApplyable, "node-" + os.Args[1], os.Args[2], clusterAddress)
	if err != nil {
		log.Fatal(err)
	}


}
