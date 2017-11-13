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
	"encoding/json"
	"bytes"
	"github.com/pkg/errors"
	"github.com/gorilla/mux"
	"sync"
)

type AddObject struct {
	ID         string      `json:"id"`
	Collection string      `json:"collection"`
	Object     interface{} `json:"object"`
}

type MyApplyable struct {
	Storage      map[string]map[string]interface{}
	StorageMutex sync.RWMutex
}

func (ma *MyApplyable) Apply(entry *raft2.Entry) error {
	log.Printf("******************* Applying: %s", entry.Data)
	var operation AddObject
	err := json.NewDecoder(bytes.NewReader(entry.Data)).Decode(&operation)
	if err != nil {
		return errors.Wrap(err, "Couldn't decode object")
	}

	ma.StorageMutex.Lock()
	if _, ok := ma.Storage[operation.Collection]; !ok {
		ma.Storage[operation.Collection] = make(map[string]interface{})
	}
	ma.Storage[operation.Collection][operation.ID] = operation.Object
	ma.StorageMutex.Unlock()

	return nil
}

func main() {
	myApplyable := &MyApplyable{
		Storage: make(map[string]map[string]interface{}),
	}

	config := LoadConfig()

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal("Couldn't get hostname")
	}

	myRaft, err := raft.NewRaft(myApplyable, hostname, config.ClusterAddress)
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

	m := mux.NewRouter()
	m.HandleFunc("/command", func(w http.ResponseWriter, r *http.Request) {
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
	})
	m.HandleFunc("/debug", func(w http.ResponseWriter, r *http.Request) {
		entries := myRaft.GetDebugData()
		for _, entry := range entries {
			fmt.Fprintf(w, "ID: %s\n Term: %v\n Data:\n%s\n", entry.ID, entry.Term, entry.Data)
		}
	})
	m.HandleFunc("/{collection}/{id}", func(w http.ResponseWriter, r *http.Request) {
		// TODO: Quorum read
		vars := mux.Vars(r)

		myApplyable.StorageMutex.RLock()
		defer myApplyable.StorageMutex.RUnlock()

		collection, ok := myApplyable.Storage[vars["collection"]]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		obj, ok := collection[vars["id"]]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err := json.NewEncoder(w).Encode(&obj); err != nil {
			fmt.Fprint(w, "Error when encoding object: %v", err)
		}
	})

	go http.ListenAndServe(":8002", m)

	if err := s.Serve(lis); err != nil {
		log.Fatal(err)
	}
}
