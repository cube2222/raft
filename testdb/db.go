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
