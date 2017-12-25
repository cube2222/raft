package command

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/cube2222/raft"
	"github.com/cube2222/raft/db"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
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

	// Label tha payload as a put operation
	genericOperation := db.Operation{
		Type:      db.PutOperation,
		Operation: putOperation,
	}

	err := handler.addToLog(r.Context(), genericOperation)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Couldn't add operation to log: %v", err)
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

	// Create a clear operation with the parameters supplied by the client
	// by means of query parameters.
	genericOperation := db.Operation{
		Type: db.ClearOperation,
		Operation: db.Clear{
			Collection: vars["collection"],
			ID:         vars["id"],
		},
	}

	err := handler.addToLog(r.Context(), genericOperation)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Couldn't add operation to log: %v", err)
	}
}

func (handler *commandHandler) addToLog(ctx context.Context, operation db.Operation) error {
	// Encode the whole operation as a message to put into the commit log
	buf := bytes.NewBuffer(nil)
	if err := json.NewEncoder(buf).Encode(&operation); err != nil {
		return errors.Wrap(err, "Couldn't encode operation")
	}

	// Add the resulting message (operation) to the commit log
	_, err := handler.raft.NewEntry(ctx, &raft.Entry{
		Data: buf.Bytes(),
	})
	if err != nil {
		return errors.Wrap(err, "Couldn't encode operation")
	}

	return nil
}

func (handler *commandHandler) DebugInfo(w http.ResponseWriter, r *http.Request) {
	entries := handler.raft.GetDebugData()
	for _, entry := range entries {
		fmt.Fprintf(w, "ID: %s\n Term: %v\n Data:\n%s\n", entry.ID, entry.Term, entry.Data)
	}
}
