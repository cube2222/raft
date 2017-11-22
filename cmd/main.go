package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"sync"

	raft2 "github.com/cube2222/raft"
	"github.com/cube2222/raft/raft"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

type AddObject struct {
	ID         string      `json:"id"`
	Collection string      `json:"collection"`
	Object     interface{} `json:"object"`
}

type MyApplyable struct {
	Storage      map[string]map[string]Document
	StorageMutex sync.RWMutex
}

type Document struct {
	Object   interface{}
	Revision int
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
		ma.Storage[operation.Collection] = make(map[string]Document)
	}
	base := ma.Storage[operation.Collection][operation.ID]
	base.Revision += 1
	base.Object = operation.Object
	ma.Storage[operation.Collection][operation.ID] = base
	ma.StorageMutex.Unlock()

	return nil
}

func main() {
	myApplyable := &MyApplyable{
		Storage: make(map[string]map[string]Document),
	}

	config := LoadConfig()

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal("Couldn't get hostname")
	}

	ctx := context.Background()
	myRaft, err := raft.NewRaft(ctx, myApplyable, hostname, config.ClusterAddresses)
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
		fmt.Fprint(w, "Document store:\n")
		myApplyable.StorageMutex.RLock()
		for name, collection := range myApplyable.Storage {
			fmt.Fprintf(w, "Collection %v :\n", name)
			for id, document := range collection {
				fmt.Fprintf(w, "ID: %s\n Document: %+v\n", id, document)
			}
			fmt.Fprint(w, "\n")
		}
		defer myApplyable.StorageMutex.RUnlock()

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
		document, ok := collection[vars["id"]]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err := json.NewEncoder(w).Encode(&document.Object); err != nil {
			fmt.Fprint(w, "Error when encoding object: %v", err)
		}
	})

	go http.ListenAndServe(":8002", m)

	if err := s.Serve(lis); err != nil {
		log.Fatal(err)
	}
}
