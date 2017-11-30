package query

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
	"github.com/cube2222/raft/cluster"
	"github.com/cube2222/raft/db"
	"github.com/gorilla/mux"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
)

type queryHandler struct {
	storageMutex sync.RWMutex
	storage      map[string]map[string]Document

	cluster *cluster.Cluster
}

func NewQueryHandler(cluster *cluster.Cluster) db.QueryHandler {
	return &queryHandler{
		storage: make(map[string]map[string]Document),
		cluster: cluster,
	}
}

func (handler *queryHandler) GetDocument(ctx context.Context, r *db.DocumentRequest) (*db.EncodedDocument, error) {
	doc := handler.getLocalDocument(r.Collection, r.Id)

	encoded, err := encodeDocument(doc)
	if err != nil {
		return nil, errors.Wrap(err, "Couldn't encode document")
	}

	return encoded, nil
}

func (handler *queryHandler) getLocalDocument(collectionName, id string) *Document {
	handler.storageMutex.RLock()
	defer handler.storageMutex.RUnlock()

	collection, ok := handler.storage[collectionName]
	if !ok {
		return &Document{Exists: false}
	}
	document, ok := collection[id]
	if !ok {
		return &Document{Exists: false}
	}

	return &document
}

func (handler *queryHandler) Apply(entry *raft.Entry) error {
	log.Printf("******************* Applying: %s", entry.Data)
	var operation db.Operation
	err := json.NewDecoder(bytes.NewReader(entry.Data)).Decode(&operation)
	if err != nil {
		return errors.Wrap(err, "Couldn't decode object")
	}

	handler.storageMutex.Lock()
	defer handler.storageMutex.Unlock()
	switch operation.Type {
	case db.PutOperation:
		var PutOperation db.Put
		if err := mapstructure.Decode(operation.Operation, &PutOperation); err != nil {
			return errors.Wrap(err, "Couldn't decode operation")
		}

		if _, ok := handler.storage[PutOperation.Collection]; !ok {
			handler.storage[PutOperation.Collection] = make(map[string]Document)
		}
		base := handler.storage[PutOperation.Collection][PutOperation.ID]
		base.Revision += 1
		base.Object = PutOperation.Object
		base.Exists = true
		handler.storage[PutOperation.Collection][PutOperation.ID] = base

	case db.ClearOperation:
		var ClearOperation db.Clear
		if err := mapstructure.Decode(operation.Operation, &ClearOperation); err != nil {
			return errors.Wrap(err, "Couldn't decode operation")
		}

		if _, ok := handler.storage[ClearOperation.Collection]; !ok {
			// Collection doesn't exist => Object doesn't exist too.
			break
		}
		base := handler.storage[ClearOperation.Collection][ClearOperation.ID]
		base.Revision += 1
		base.Exists = false
		handler.storage[ClearOperation.Collection][ClearOperation.ID] = base
	}

	return nil
}

func (handler *queryHandler) HTTPHandler() http.Handler {
	m := mux.NewRouter()
	m.HandleFunc("/{collection}/{id}", handler.getDocument).Methods(http.MethodGet)
	m.HandleFunc("/debug", handler.debugInfo).Methods(http.MethodGet)
	return m
}

func (handler *queryHandler) getDocument(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	doc, err := handler.getUpToDateDocument(r.Context(), vars["collection"], vars["id"])
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Couldn't get document: %v", err)
		return
	}

	if !doc.Exists {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if err := json.NewEncoder(w).Encode(&doc.Object); err != nil {
		fmt.Fprint(w, "Error when encoding object: %v", err)
	}
}

func (handler *queryHandler) getUpToDateDocument(ctx context.Context, collection, id string) (*Document, error) {
	resChan := make(chan *remoteDocumentResponse)

	others := handler.cluster.OtherHealthyMembers()
	quorum := 1 + handler.cluster.NumMembers()/2

	ctx, cancel := context.WithTimeout(ctx, time.Second*5)

	for _, member := range others {
		go handler.getDocumentFromNode(ctx, member.Name, collection, id, resChan)
	}

	oks := 1
	responses := []*Document{handler.getLocalDocument(collection, id)}
	for i := 0; i < len(others); i++ {
		res := <-resChan
		if res.err != nil {
			log.Printf("Couldn't get response from member: %v", res.member)
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

func (handler *queryHandler) getDocumentFromNode(ctx context.Context, member, collection, id string, resChan chan<- *remoteDocumentResponse) {
	res := &remoteDocumentResponse{
		member: member,
	}

	// TODO: Make this port configurable.
	conn, err := handler.cluster.GetgRPCConnection(ctx, member, 8001)
	if err != nil {
		res.err = errors.Wrap(err, "Couldn't get gRPC connection")
		resChan <- res
		return
	}

	cli := db.NewDBClient(conn)
	encoded, err := cli.GetDocument(ctx, &db.DocumentRequest{
		Collection: collection,
		Id:         id,
	})
	if err != nil {
		res.err = errors.Wrap(err, "Couldn't get document")
		resChan <- res
		return
	}

	doc, err := decodeDocument(encoded)
	if err != nil {
		res.err = errors.Wrap(err, "Couldn't decode encoded document")
		resChan <- res
		return
	}

	res.doc = doc
	resChan <- res
}

var documentNotFound = errors.New("Document does not exist")

type remoteDocumentResponse struct {
	member string
	doc    *Document
	err    error
}

func (handler *queryHandler) debugInfo(w http.ResponseWriter, r *http.Request) {
	handler.storageMutex.RLock()
	defer handler.storageMutex.RUnlock()
	for name, collection := range handler.storage {
		fmt.Fprintf(w, "Collection %v :\n", name)
		for id, document := range collection {
			fmt.Fprintf(w, "ID: %s\n Document: %+v\n", id, document)
		}
		fmt.Fprint(w, "\n")
	}
}
