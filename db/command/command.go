package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/cube2222/raft"
	"github.com/cube2222/raft/db"
	"github.com/gorilla/mux"
)

type commandHandler struct {
	raft raft.Raft
}

func NewCommandHandler(raft raft.Raft) db.CommandHandler {
	return &commandHandler{
		raft: raft,
	}
}

func (handler *commandHandler) HTTPHandler() http.Handler {
	m := mux.NewRouter()
	m.HandleFunc("/", handler.putDocument).Methods(http.MethodPut)
	m.HandleFunc("/{collection}/{id}", handler.clearDocument).Methods(http.MethodDelete)
	m.HandleFunc("/debug", handler.DebugInfo).Methods(http.MethodGet)
	return m
}

func (handler *commandHandler) putDocument(w http.ResponseWriter, r *http.Request) {
	var putOperation db.Put
	if err := json.NewDecoder(r.Body).Decode(&putOperation); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Bad json body: %v", err)
		return
	}

	genericOperation := db.Operation{
		Type:      db.PutOperation,
		Operation: putOperation,
	}

	buf := bytes.NewBuffer(nil)
	if err := json.NewEncoder(buf).Encode(&genericOperation); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Couldn't encode operation: %v", err)
		return
	}

	_, err := handler.raft.NewEntry(r.Context(), &raft.Entry{
		Data: buf.Bytes(),
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Couldn't create new commit log entry: %v", err)
		return
	}
}

func (handler *commandHandler) clearDocument(w http.ResponseWriter, r *http.Request) {
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

	genericOperation := db.Operation{
		Type: db.ClearOperation,
		Operation: db.Clear{
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

	_, err := handler.raft.NewEntry(r.Context(), &raft.Entry{
		Data: buf.Bytes(),
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, "Couldn't create new commit log entry: %v", err)
		return
	}
}

func (handler *commandHandler) DebugInfo(w http.ResponseWriter, r *http.Request) {
	entries := handler.raft.GetDebugData()
	for _, entry := range entries {
		fmt.Fprintf(w, "ID: %s\n Term: %v\n Data:\n%s\n", entry.ID, entry.Term, entry.Data)
	}
}
