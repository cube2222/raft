package query

import (
	"encoding/json"

	"github.com/cube2222/raft/db"
	"github.com/pkg/errors"
)

type Document struct {
	Object   interface{}
	Exists   bool
	Revision int
}

func encodeDocument(doc *Document) (*db.EncodedDocument, error) {
	encoded := &db.EncodedDocument{
		Revision: int64(doc.Revision),
		Exists: doc.Exists,
	}

	if !encoded.Exists {
		return encoded, nil
	}

	data, err := json.Marshal(&doc.Object)
	if err != nil {
		return nil, errors.Wrap(err, "Couldn't marshal the object contained in the document")
	}

	encoded.Data = data
	return encoded, nil
}

func decodeDocument(encoded *db.EncodedDocument) (*Document, error) {
	doc := &Document{
		Revision: int(encoded.Revision),
		Exists: encoded.Exists,
	}

	if !doc.Exists {
		return doc, nil
	}

	err := json.Unmarshal(encoded.Data, &doc.Object)
	if err != nil {
		return nil, errors.Wrap(err, "Couldn't unmarshal encoded object")
	}

	return doc, nil
}
