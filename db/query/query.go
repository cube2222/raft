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

	cluster cluster.Cluster
}

func (handler *queryHandler) GetDocument(context.Context, *db.DocumentRequest) (*db.EncodedDocument, error) {
	panic("implement me")
}

type Document struct {
	Object   interface{}
	Exists   bool
	Revision int
}

func NewQueryHandler() db.QueryHandler {
	return &queryHandler{
		storage: make(map[string]map[string]Document),
	}
}

func (handler *queryHandler) HTTPHandler() http.Handler {
	m := mux.NewRouter()
	m.HandleFunc("/{collection}/{id}", handler.getDocument).Methods(http.MethodGet)
	//m.HandleFunc("/debug", handler).Methods(http.MethodGet)
	return m
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
		base.Exists = false
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
		base.Exists = true
		handler.storage[ClearOperation.Collection][ClearOperation.ID] = base
	}

	return nil
}

func (handler *queryHandler) getDocument(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	doc, err := handler.getUpToDateDocument(r.Context(), vars["collection"], vars["id"])
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

func (handler *queryHandler) getUpToDateDocument(ctx context.Context, collection, id string) (*Document, error) {
	resChan := make(chan *remoteDocumentResponse)

	others := handler.cluster.OtherHealthyMembers()
	quorum := 1 + handler.cluster.NumMembers()/2

	ctx, cancel := context.WithTimeout(ctx, time.Second*5)

	for _, member := range others {
		go getDocumentFromNode(ctx, fmt.Sprintf("%s:8002", member.Addr), collection, id, resChan)
	}

	oks := 1
	responses := []*Document{handler.getLocalDocument(collection, id)}
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
