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
	"github.com/gorilla/mux"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
)

// Rozdziel na reader i writer
type DocumentDatabase struct {
	Raft raft.Raft

	StorageMutex sync.RWMutex
	Storage      map[string]map[string]Document
}

type Document struct {
	Object   interface{}
	Exists   bool
	Revision int
}

func NewDocumentDatabase() *DocumentDatabase {
	return &DocumentDatabase{
		Storage: make(map[string]map[string]Document),
	}
}

func (db *DocumentDatabase) SetRaftModule(raftModule raft.Raft) {
	db.Raft = raftModule
}

type Operation struct {
	Type      string      `json:"type"`
	Operation interface{} `json:"operation"`
}

type Put struct {
	ID         string      `json:"id"`
	Collection string      `json:"collection"`
	Object     interface{} `json:"object"`
}

const PutOperation = "put"

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
	case PutOperation:
		var PutOperation Put
		if err := mapstructure.Decode(operation.Operation, &PutOperation); err != nil {
			return errors.Wrap(err, "Couldn't decode operation")
		}

		if _, ok := db.Storage[PutOperation.Collection]; !ok {
			db.Storage[PutOperation.Collection] = make(map[string]Document)
		}
		base := db.Storage[PutOperation.Collection][PutOperation.ID]
		base.Revision += 1
		base.Object = PutOperation.Object
		base.Exists = false
		db.Storage[PutOperation.Collection][PutOperation.ID] = base

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

func (db *DocumentDatabase) GetHTTPHandler() http.Handler {
	m := mux.NewRouter()
	m.HandleFunc("/{collection}/{id}", db.GetDocument).Methods(http.MethodGet)
	m.HandleFunc("/", db.PutDocument).Methods(http.MethodPut)
	m.HandleFunc("/{collection}/{id}", db.ClearDocument).Methods(http.MethodDelete)
	m.HandleFunc("/debug", db.DebugInfo).Methods(http.MethodGet)
	return m
}

func (db *DocumentDatabase) PutDocument(w http.ResponseWriter, r *http.Request) {
	var putOperation Put
	if err := json.NewDecoder(r.Body).Decode(&putOperation); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Bad json body: %v", err)
		return
	}

	genericOperation := Operation{
		Type:      PutOperation,
		Operation: putOperation,
	}

	buf := bytes.NewBuffer(nil)
	if err := json.NewEncoder(buf).Encode(&genericOperation); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Couldn't encode operation: %v", err)
		return
	}

	_, err := db.Raft.NewEntry(r.Context(), &raft.Entry{
		Data: buf.Bytes(),
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Couldn't create new commit log entry: %v", err)
		return
	}
}

func (db *DocumentDatabase) ClearDocument(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	if vars["collection"] == "" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Missing collection name.")
		return
	}
	if vars["id"] == "" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Missing document id.")
		return
	}

	genericOperation := Operation{
		Type: ClearOperation,
		Operation: Clear{
			Collection: vars["collection"],
			ID:         vars["id"],
		},
	}

	buf := bytes.NewBuffer(nil)
	if err := json.NewEncoder(buf).Encode(&genericOperation); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Couldn't encode operation: %v", err)
		return
	}

	_, err := db.Raft.NewEntry(r.Context(), &raft.Entry{
		Data: buf.Bytes(),
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, "Couldn't create new commit log entry: %v", err)
		return
	}
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


// Możesz ten wewnętrzny odczyt zrobić jednak na rafcie, a udostępniać interfejs http, tak że http szuka quorum
// ale komunikacja do quorum jest po grpc, i tez lokalnie wtedy mozna to tak odpalić.
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

func (db *DocumentDatabase) DebugInfo(w http.ResponseWriter, r *http.Request) {
	entries := db.Raft.GetDebugData()
	for _, entry := range entries {
		fmt.Fprintf(w, "ID: %s\n Term: %v\n Data:\n%s\n", entry.ID, entry.Term, entry.Data)
	}
	fmt.Fprint(w, "Document store:\n")

	db.StorageMutex.RLock()
	for name, collection := range db.Storage {
		fmt.Fprintf(w, "Collection %v :\n", name)
		for id, document := range collection {
			fmt.Fprintf(w, "ID: %s\n Document: %+v\n", id, document)
		}
		fmt.Fprint(w, "\n")
	}
	defer db.StorageMutex.RUnlock()
}
