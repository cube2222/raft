package query

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
)

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
