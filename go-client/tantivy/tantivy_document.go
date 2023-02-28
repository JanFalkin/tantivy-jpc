package tantivy

import (
	"encoding/json"
	"fmt"

	"github.com/eluv-io/errors-go"
)

type TDocument struct {
	*JPCId
	schema []interface{}
}

func (td *TDocument) CreateIndex() (*TIndex, error) {
	e := errors.Template("TDocument.CreateIndex", errors.K.Invalid, "TempDir", td.TempDir)
	_, err := td.callTantivy("index", "create", msi{"directory": td.TempDir})
	if err != nil {
		return nil, e(err, "reason", "index create failed")
	}
	return &TIndex{
		JPCId: td.JPCId,
	}, nil
}

func (td *TDocument) Create() (uint, error) {
	s, err := td.callTantivy("document", "create", msi{})
	if err != nil {
		return 0, err
	}
	var data msi
	err = json.Unmarshal([]byte(s), &data)
	if err != nil {
		return 0, err
	}
	c, ok := data["document_count"]
	if !ok {
		return 0, fmt.Errorf("document_count element not found in data %v or data not able to be type asserted to uint", data)
	}
	return uint(c.(float64)), nil
}

func (td *TDocument) AddText(field int, value string, doc_id uint) (int, error) {
	_, err := td.callTantivy("document", "add_text", msi{
		"field":  field,
		"value":  value,
		"id":     td.JPCId.id,
		"doc_id": doc_id,
	})
	if err != nil {
		return -1, err
	}
	return 0, nil
}

func (td *TDocument) AddInt(field int, value int64, doc_id uint) (int, error) {
	_, err := td.callTantivy("document", "add_int", msi{
		"field":  field,
		"value":  value,
		"id":     td.JPCId.id,
		"doc_id": doc_id,
	})
	if err != nil {
		return -1, err
	}
	return 0, nil
}

func (td *TDocument) AddUInt(field int, value uint64, doc_id uint) (int, error) {
	_, err := td.callTantivy("document", "add_uint", msi{
		"field":  field,
		"value":  value,
		"id":     td.JPCId.id,
		"doc_id": doc_id,
	})
	if err != nil {
		return -1, err
	}
	return 0, nil
}
