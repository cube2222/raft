package db

import (
	"net/http"

	"github.com/cube2222/raft/raft"
)

type QueryHandler interface {
	raft.Applyable
	HTTPHandler() http.Handler
}

type CommandHandler interface {
	HTTPHandler() http.Handler
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