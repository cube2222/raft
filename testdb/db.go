package testdb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/cube2222/raft"
	raftimpl "github.com/cube2222/raft/raft"
	"github.com/gorilla/mux"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
)

type DocumentDatabase struct {
	// Zrobić z tego Rafta interfejs w raft.go powierzchownym
	Raft         raftimpl.Raft
	Storage      map[string]map[string]Document
	StorageMutex sync.RWMutex
}

type Document struct {
	Object   interface{}
	Exists   bool
	Revision int
}

type Operation struct {
	Type      string
	Operation map[string]interface{}
}

type Set struct {
	ID         string      `json:"id"`
	Collection string      `json:"collection"`
	Object     interface{} `json:"object"`
}

const SetOperation = "set"

type Clear struct {
	ID         string `json:"id"`
	Collection string `json:"collection"`
}

const ClearOperation = "clear"

func (db *DocumentDatabase) Apply(entry *raft.Entry) error {
	log.Printf("******************* Applying: %s", entry.Data)
	var operation Operation
	err := json.NewDecoder(bytes.NewReader(entry.Data)).Decode(&operation)
	if err != nil {
		return errors.Wrap(err, "Couldn't decode object")
	}

	db.StorageMutex.Lock()
	defer db.StorageMutex.Unlock()
	switch operation.Type {
	case SetOperation:
		var SetOperation Set
		if err := mapstructure.Decode(operation.Operation, &SetOperation); err != nil {
			return errors.Wrap(err, "Couldn't decode operation")
		}

		if _, ok := db.Storage[SetOperation.Collection]; !ok {
			db.Storage[SetOperation.Collection] = make(map[string]Document)
		}
		base := db.Storage[SetOperation.Collection][SetOperation.ID]
		base.Revision += 1
		base.Object = SetOperation.Object
		base.Exists = false
		db.Storage[SetOperation.Collection][SetOperation.ID] = base

	case ClearOperation:
		var ClearOperation Clear
		if err := mapstructure.Decode(operation.Operation, &ClearOperation); err != nil {
			return errors.Wrap(err, "Couldn't decode operation")
		}

		if _, ok := db.Storage[ClearOperation.Collection]; !ok {
			// Collection doesn't exist => Object doesn't exist too.
			break
		}
		base := db.Storage[ClearOperation.Collection][ClearOperation.ID]
		base.Exists = true
		db.Storage[ClearOperation.Collection][ClearOperation.ID] = base
	}

	return nil
}

func (db *DocumentDatabase) GetDocument(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	doc, err := db.getUpToDateDocument(r.Context(), vars["collection"], vars["id"])
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Couldn't get document: %v", err)
		return
	}

	if doc.Exists == false {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if err := json.NewEncoder(w).Encode(&doc.Object); err != nil {
		fmt.Fprint(w, "Error when encoding object: %v", err)
	}
}

func (db *DocumentDatabase) getUpToDateDocument(ctx context.Context, collection, id string) (*Document, error) {
	resChan := make(chan *remoteDocumentResponse)

	others := db.Raft.OtherHealthyMembers()
	quorum := db.Raft.QuorumSize()

	ctx, cancel := context.WithTimeout(ctx, time.Second*5)

	for _, member := range others {
		go getDocumentFromNode(ctx, fmt.Sprintf("%s:8002", member.Addr), collection, id, resChan)
	}

	oks := 1
	responses := []*Document{db.getLocalDocument(collection, id)}
	for i := 0; i < len(others); i++ {
		res := <-resChan
		if res.err != nil {
			if res.err == documentNotFound {
				oks += 1
				responses = append(responses, &Document{Exists: true})
				continue
			}
			continue
		}

		oks += 1
		responses = append(responses, res.doc)

		if oks >= quorum {
			cancel()
			go drain(len(others)-i, resChan)
			break
		}
	}

	if oks < quorum {
		return nil, errors.Errorf("Couldn't contact quorum. Got %v out of %v", oks, quorum)
	}

	mostCurrent := responses[0]
	for _, doc := range responses {
		if doc.Revision > mostCurrent.Revision {
			mostCurrent = doc
		}
	}

	return mostCurrent, nil
}

func drain(n int, channel chan *remoteDocumentResponse) {
	for i := 0; i < n; i++ {
		<-channel
	}
	close(channel)
}

func (db *DocumentDatabase) getLocalDocument(collectionName, id string) *Document {
	db.StorageMutex.RLock()
	defer db.StorageMutex.RUnlock()

	collection, ok := db.Storage[collectionName]
	if !ok {
		return &Document{Exists: false}
	}
	document, ok := collection[id]
	if !ok {
		return &Document{Exists: false}
	}

	return &document
}

func getDocumentFromNode(ctx context.Context, address, collection, id string, resChan chan<- *remoteDocumentResponse) {
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s/%s/%s", address, collection, id), nil)
	if err != nil {
		resChan <- &remoteDocumentResponse{
			err: errors.Wrap(err, "Couldn't create request to get document"),
		}
		return
	}
	req = req.WithContext(ctx)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		resChan <- &remoteDocumentResponse{
			err: errors.Wrap(err, "Couldn't do request to get document"),
		}
		return
	}

	if res.StatusCode == http.StatusNotFound {
		resChan <- &remoteDocumentResponse{
			err: documentNotFound,
		}
		return
	}
	if res.StatusCode != http.StatusOK {
		resChan <- &remoteDocumentResponse{
			err: errors.Errorf("Unindentified error code %v", res.StatusCode),
		}
		return
	}

	var doc Document
	err = json.NewDecoder(res.Body).Decode(&doc)
	if err != nil {
		resChan <- &remoteDocumentResponse{
			err: errors.Wrap(err, "Couldn't decode document"),
		}
		return
	}

	resChan <- &remoteDocumentResponse{
		doc: &doc,
	}
}

var documentNotFound = errors.New("Document does not exist")

type remoteDocumentResponse struct {
	doc *Document
	err error
}
