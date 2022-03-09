package tantivy

// #cgo linux,amd64 LDFLAGS: -Wl,-rpath,${SRCDIR}/packaged/lib/linux-amd64 -L${SRCDIR}/packaged/lib/linux-amd64
// #cgo darwin,amd64 LDFLAGS: -Wl,-rpath,${SRCDIR}/packaged/lib/darwin-amd64 -L${SRCDIR}/packaged/lib/darwin-amd64
// #cgo darwin,arm64 LDFLAGS: -Wl,-rpath,${SRCDIR}/packaged/lib/darwin-aarch64 -L${SRCDIR}/packaged/lib/darwin-aarch64
// #cgo CFLAGS: -I${SRCDIR}/packaged/include
// #cgo LDFLAGS: -ltantivy_jpc
//
// #include "tantivy_jpc.h"
import "C"
import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"unsafe"

	uuid "github.com/satori/go.uuid"
)

func LibInit() {
	C.init()
}

type msi map[string]interface{}

type JPCId struct {
	id string
}
type TSearcher struct {
	*TQueryParser
}

func (s *TSearcher) Search() (string, error) {
	return callTantivy(s.JPCId.id, "searcher", "search", msi{})
}

type TQueryParser struct {
	*TIndex
}

func (qp *TQueryParser) ForIndex(fields []string) (uint, error) {
	_, err := callTantivy(qp.JPCId.id, "query_parser", "for_index", msi{
		"fields": fields,
	})
	if err != nil {
		return 0, err
	}
	return 0, nil
}

func (qp *TQueryParser) ParseQuery(query string) (*TSearcher, error) {
	_, err := callTantivy(qp.JPCId.id, "query_parser", "parse_query", msi{
		"query": query,
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
	_, err := callTantivy(idr.JPCId.id, "index_reader", "searcher", msi{})
	if err != nil {
		return nil, err
	}
	return &TQueryParser{idr.TIndex}, nil
}

type TIndexWriter struct {
	*TIndex
}

func (idw *TIndexWriter) Commit() (uint64, error) {
	s, err := callTantivy(idw.JPCId.id, "indexwriter", "commit", msi{})
	if err != nil {
		return 0, err
	}
	var data msi
	err = json.Unmarshal([]byte(s), &data)
	if err != nil {
		panic(err)
	}
	c, ok := data["id"]
	if !ok {
		return 0, fmt.Errorf("document_count element not found in data %v or data not able to be type asserted to uint", data)
	}
	return uint64(c.(float64)), nil
}

func (idw *TIndexWriter) AddDocument(docid uint) (uint, error) {
	s, err := callTantivy(idw.JPCId.id, "indexwriter", "add_document", msi{
		"id": docid,
	})
	if err != nil {
		return 0, err
	}
	var data msi
	err = json.Unmarshal([]byte(s), &data)
	if err != nil {
		panic(err)
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
	_, err := callTantivy(idx.JPCId.id, "index", "reader_builder", msi{})
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
	tempDir, err := ioutil.TempDir("", "tantivy_idx")
	if err != nil {
		panic(err)
	}
	_, err = callTantivy(td.JPCId.id, "index", "create", msi{"directory": tempDir})
	if err != nil {
		return nil, err
	}
	return &TIndex{
		JPCId: &JPCId{td.JPCId.id},
	}, nil
}

func (td *TDocument) Create() (uint, error) {
	s, err := callTantivy(td.id, "document", "create", msi{})
	if err != nil {
		return 0, err
	}
	var data msi
	err = json.Unmarshal([]byte(s), &data)
	if err != nil {
		panic(err)
	}
	c, ok := data["document_count"]
	if !ok {
		return 0, fmt.Errorf("document_count element not found in data %v or data not able to be type asserted to uint", data)
	}
	return uint(c.(float64)), nil
}

func (td *TDocument) AddText(field int, value string, doc_id uint) (int, error) {
	_, err := callTantivy(td.id, "document", "add_text", msi{
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

func NewBuilder() (*TBuilder, error) {
	u := uuid.NewV4()
	tb := TBuilder{
		JPCId: &JPCId{
			id: u.String(),
		},
	}
	return &tb, nil
}

func (td *TBuilder) AddTextField(name string, indexed bool) (int, error) {
	s, err := callTantivy(td.id, "builder", "add_text_field", msi{
		"name":  name,
		"index": indexed,
		"id":    td.JPCId.id,
	})

	if err != nil {
		return -1, err
	}
	fmt.Println("s={}", s)

	var fieldData msi
	err = json.Unmarshal([]byte(s), &fieldData)
	if err != nil {
		panic(err)
	}

	c, ok := fieldData["field"]
	if !ok {
		return 0, fmt.Errorf("field element not found in data %v or data not able to be type asserted to int", fieldData)
	}
	fmt.Println("Here3")

	return int(c.(float64)), nil
}

func (td *TBuilder) Build() (*TDocument, error) {
	s, err := callTantivy(td.JPCId.id, "builder", "build", msi{})
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
		JPCId: &JPCId{
			id: td.id,
		},
		schema: schema.([]interface{}),
	}, nil

}

func callTantivy(u, object, method string, params msi) (string, error) {
	f := map[string]interface{}{
		"id":     u,
		"jpc":    "1.0",
		"obj":    object,
		"method": method,
		"params": params,
	}
	b, err := json.Marshal(f)
	if err != nil {
		return "", err
	}
	p := C.CString(string(b))
	rb := make([]byte, 5000000)
	csrb := C.CString(string(rb))
	crb := (*C.uchar)(unsafe.Pointer(csrb))
	cs := (*C.uchar)(unsafe.Pointer(p))
	rbl := len(rb)
	prbl := (*C.ulong)(unsafe.Pointer(&rbl))
	r := C.jpc(cs, C.ulong(uint64(len(string(b)))), crb, prbl)
	returnData := C.GoString(csrb)
	fmt.Printf("return value %v \n", r)
	return returnData, nil
}
