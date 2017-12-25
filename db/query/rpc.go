package query

import (
	"context"

	"github.com/cube2222/raft/db"
	"github.com/pkg/errors"
)

func (handler *queryHandler) GetDocument(ctx context.Context, r *db.DocumentRequest) (*db.EncodedDocument, error) {
	doc := handler.getLocalDocument(r.Collection, r.Id)

	encoded, err := encodeDocument(doc)
	if err != nil {
		return nil, errors.Wrap(err, "Couldn't encode document")
	}

	return encoded, nil
}
