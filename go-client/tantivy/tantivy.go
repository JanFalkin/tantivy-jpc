package tantivy

// #cgo linux,amd64 LDFLAGS:-L${SRCDIR}/packaged/lib/linux-amd64
// #cgo darwin,amd64 LDFLAGS:-L${SRCDIR}/packaged/lib/darwin-amd64
// #cgo darwin,arm64 LDFLAGS:-L${SRCDIR}/packaged/lib/darwin-aarch64
// #cgo CFLAGS: -I${SRCDIR}/packaged/include
// #cgo LDFLAGS: -ltantivy_jpc -lm -ldl -pthread
// #cgo linux,amd64 LDFLAGS: -Wl,--allow-multiple-definition
//
// #include "tantivy-jpc.h"
// #include <stdlib.h>
// char* internal_malloc(int sz){
//	return (char*)malloc(sz);
//}
import "C"
import (
	"encoding/json"
	"fmt"
	"sync"
	"unsafe"

	"github.com/eluv-io/errors-go"
	uuid "github.com/satori/go.uuid"
)

var doOnce sync.Once

func LibInit() {
	doOnce.Do(func() {
		C.init()
	})
}

func ClearSession(sessionID string) {
	C.term(C.CString(sessionID))
}

type msi map[string]interface{}

const defaultMemSize = 5000000

// The ccomsBuf is a raw byte buffer for tantivy-jpc to send results. A single mutex guards its use.
type JPCId struct {
	id       string
	TempDir  string
	ccomsBuf *C.char
	bufLen   int32
}

func (j *JPCId) ID() string {
	return j.id
}

type TSearcher struct {
	*TQueryParser
}

func (s *TSearcher) Search() (string, error) {
	return s.callTantivy("searcher", "search", msi{})
}

func (s *TSearcher) FuzzySearch() (string, error) {
	return s.callTantivy("fuzzy_searcher", "fuzzy_searcher", msi{})
}

type TQueryParser struct {
	*TIndex
}

func (qp *TQueryParser) ForIndex(fields []string) (uint, error) {
	_, err := qp.callTantivy("query_parser", "for_index", msi{
		"fields": fields,
	})
	if err != nil {
		return 0, err
	}
	return 0, nil
}

func (qp *TQueryParser) ParseQuery(query string) (*TSearcher, error) {
	_, err := qp.callTantivy("query_parser", "parse_query", msi{
		"query": query,
	})
	if err != nil {
		return nil, err
	}
	return &TSearcher{qp}, nil
}

func (qp *TQueryParser) ParseFuzzyQuery(field, term string) (*TSearcher, error) {
	_, err := qp.callTantivy("query_parser", "parse_fuzzy_query", msi{
		"term":  []string{term},
		"field": []string{field},
	})
	if err != nil {
		return nil, err
	}
	return &TSearcher{qp}, nil
}

type TIndexReader struct {
	*TIndex
}

func (idr *TIndexReader) Searcher() (*TQueryParser, error) {
	_, err := idr.callTantivy("index_reader", "searcher", msi{})
	if err != nil {
		return nil, err
	}
	return &TQueryParser{idr.TIndex}, nil
}

type TIndexWriter struct {
	*TIndex
}

func (idw *TIndexWriter) Commit() (uint64, error) {
	s, err := idw.callTantivy("indexwriter", "commit", msi{})
	if err != nil {
		return 0, err
	}
	var data msi
	err = json.Unmarshal([]byte(s), &data)
	if err != nil {
		return 0, err
	}
	c, ok := data["id"]
	if !ok {
		return 0, fmt.Errorf("document_count element not found in data %v or data not able to be type asserted to uint", data)
	}
	return uint64(c.(float64)), nil
}

func (idw *TIndexWriter) AddDocument(docid uint) (uint, error) {
	s, err := idw.callTantivy("indexwriter", "add_document", msi{"id": docid})
	if err != nil {
		return 0, err
	}
	var data msi
	err = json.Unmarshal([]byte(s), &data)
	if err != nil {
		return 0, err
	}
	c, ok := data["opstamp"]
	if !ok {
		return 0, fmt.Errorf("document_count element not found in data %v or data not able to be type asserted to uint", data)
	}
	return uint(c.(float64)), nil
}

type TIndex struct {
	*JPCId
}

func (idx *TIndex) CreateIndexWriter() (*TIndexWriter, error) {
	return &TIndexWriter{idx}, nil
}

func (idx *TIndex) ReaderBuilder() (*TIndexReader, error) {
	_, err := idx.callTantivy("index", "reader_builder", msi{})
	if err != nil {
		return nil, err
	}
	return &TIndexReader{idx}, nil
}

type TDocument struct {
	*JPCId
	schema []interface{}
}

func (td *TDocument) CreateIndex() (*TIndex, error) {
	e := errors.Template("TDocument.CreateIndex", errors.K.Invalid, "TempDir", td.TempDir)

	if td.TempDir == "" {
		return nil, e("reason", "TempDir is empty")
	}
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

type TBuilder struct {
	*JPCId
}

type StorageKind uint

const (
	STRING StorageKind = 1
	TEXT   StorageKind = 2
)

func NewBuilder(td string, memsize ...int32) (*TBuilder, error) {
	var memSizeToUse int32
	if len(memsize) > 0 {
		memSizeToUse = memsize[0]
	} else {
		memSizeToUse = defaultMemSize
	}
	u := uuid.NewV4()
	tb := TBuilder{
		JPCId: &JPCId{
			id:       u.String(),
			TempDir:  td,
			bufLen:   memSizeToUse,
			ccomsBuf: C.internal_malloc(C.int(memSizeToUse)),
		},
	}
	return &tb, nil
}

func (tb *TBuilder) CreateIndex() (*TIndex, error) {
	e := errors.Template("TBuilder.CreateIndex", errors.K.Invalid, "TempDir", tb.TempDir)

	if tb.TempDir == "" {
		return nil, e("reason", "TempDir is empty")
	}
	_, err := tb.callTantivy("index", "create", msi{"directory": tb.TempDir})
	if err != nil {
		return nil, e(err, "reason", "index create failed")
	}
	return &TIndex{
		JPCId: tb.JPCId,
	}, nil
}

func (tb *TBuilder) standardReturnHandler(s string, err error) (int, error) {
	if err != nil {
		return -1, err
	}
	var fieldData msi
	err = json.Unmarshal([]byte(s), &fieldData)
	if err != nil {
		return -1, err
	}

	c, ok := fieldData["field"]
	if !ok {
		return 0, fmt.Errorf("field element not found in data %v or data not able to be type asserted to int", fieldData)
	}

	return int(c.(float64)), nil

}

func (tb *TBuilder) AddTextField(name string, fieldType StorageKind, stored bool) (int, error) {
	s, err := tb.callTantivy("builder", "add_text_field", msi{
		"name":   name,
		"type":   fieldType,
		"stored": true,
		"id":     tb.JPCId.id,
	})
	return tb.standardReturnHandler(s, err)

}

func (tb *TBuilder) AddDateField(name string, fieldType StorageKind, stored bool) (int, error) {
	s, err := tb.callTantivy("builder", "add_date_field", msi{
		"name":   name,
		"type":   fieldType,
		"stored": true,
		"id":     tb.JPCId.id,
	})

	return tb.standardReturnHandler(s, err)
}

func (tb *TBuilder) AddU64Field(name string, fieldType StorageKind, stored bool) (int, error) {
	s, err := tb.callTantivy("builder", "add_u64_field", msi{
		"name":   name,
		"type":   fieldType,
		"stored": true,
		"id":     tb.JPCId.id,
	})

	return tb.standardReturnHandler(s, err)
}

func (tb *TBuilder) AddI64Field(name string, fieldType StorageKind, stored bool) (int, error) {
	s, err := tb.callTantivy("builder", "add_i64_field", msi{
		"name":   name,
		"type":   fieldType,
		"stored": true,
		"id":     tb.JPCId.id,
	})

	return tb.standardReturnHandler(s, err)
}

func (tb *TBuilder) AddF64Field(name string, fieldType StorageKind, stored bool) (int, error) {
	s, err := tb.callTantivy("builder", "add_f64_field", msi{
		"name":   name,
		"type":   fieldType,
		"stored": true,
		"id":     tb.JPCId.id,
	})

	return tb.standardReturnHandler(s, err)
}

func (tb *TBuilder) Build() (*TDocument, error) {
	s, err := tb.callTantivy("builder", "build", msi{})
	if err != nil {
		return nil, err
	}

	var fieldData msi
	err = json.Unmarshal([]byte(s), &fieldData)
	if err != nil {
		return nil, err
	}

	schema, ok := fieldData["schema"]
	if !ok {
		return nil, fmt.Errorf("schema element not found in data %v or data not able to be type asserted to uint", fieldData)
	}

	return &TDocument{
		JPCId:  tb.JPCId,
		schema: schema.([]interface{}),
	}, nil

}

func (jpc *JPCId) callTantivy(object, method string, params msi) (string, error) {
	f := map[string]interface{}{
		"id":     jpc.id,
		"jpc":    "1.0",
		"obj":    object,
		"method": method,
		"params": params,
	}
	b, err := json.Marshal(f)
	if err != nil {
		return "", err
	}
	sb := string(b)
	p := C.CString(sb)
	defer C.free(unsafe.Pointer(p))
	crb := (*C.uchar)(unsafe.Pointer(jpc.ccomsBuf))
	cs := (*C.uchar)(unsafe.Pointer(p))
	prbl := (*C.ulong)(unsafe.Pointer(&jpc.bufLen))
	ttret := C.tantivy_jpc(cs, C.ulong(uint64(len(sb))), crb, prbl)
	if ttret < 0 {
		return "", errors.E("Tantivy JPC Failed", errors.K.Invalid, "desc", C.GoString(jpc.ccomsBuf))
	}
	returnData := C.GoString(jpc.ccomsBuf)
	return returnData, nil
}
