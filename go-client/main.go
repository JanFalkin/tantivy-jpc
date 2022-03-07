package main

/*
#cgo CFLAGS: -I../target
#cgo LDFLAGS: -ltantivy_jrpc -L.
#include "tantivy_jrpc.h"
*/
import "C"
import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"unsafe"

	uuid "github.com/nu7hatch/gouuid"
)

/*
  pub id: &'a str,
  pub jpc: &'a str,
  pub obj: &'a str,
  pub method: &'a str,
  pub params: serde_json::Value,
*/
type msi map[string]interface{}

type JRPCId struct {
	id string
}

type TDocument struct {
	*JRPCId
	schema []interface{}
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
		"id":     td.JRPCId.id,
		"doc_id": doc_id,
	})
	if err != nil {
		return -1, err
	}
	return 0, nil
}

type TBuilder struct {
	*JRPCId
}

func NewBuilder() (*TBuilder, error) {
	u, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}
	tb := TBuilder{
		JRPCId: &JRPCId{
			id: u.String(),
		},
	}
	return &tb, nil
}

func (td *TBuilder) AddTextField(name string, indexed bool) (int, error) {
	s, err := callTantivy(td.id, "builder", "add_text_field", msi{
		"name":  name,
		"index": indexed,
		"id":    td.JRPCId.id,
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
	s, err := callTantivy(td.JRPCId.id, "builder", "build", msi{})
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
		JRPCId: &JRPCId{
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
	fmt.Printf("return value %v ret buffer %v\n", r, returnData)
	return returnData, nil
}

func main() {
	fmt.Println("Hello World")
	C.init()
	builder, err := NewBuilder()
	if err != nil {
		panic(err)
	}
	fmt.Println("Here")
	idxFieldTitle, err := builder.AddTextField("title", false)
	if err != nil {
		panic(err)
	}
	idxFieldBody, err := builder.AddTextField("body", false)
	if err != nil {
		panic(err)
	}

	doc, err := builder.Build()
	if err != nil {
		panic(err)
	}
	doc1, err := doc.Create()
	if err != nil {
		panic(err)
	}
	doc2, err := doc.Create()
	if err != nil {
		panic(err)
	}
	doc.AddText(idxFieldTitle, "The Old Man and the Sea", doc1)
	doc.AddText(idxFieldBody, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish.", doc1)
	doc.AddText(idxFieldTitle, "Of Mice and Men", doc2)
	doc.AddText(idxFieldBody, `A few miles south of Soledad, the Salinas River drops in close to the hillside
	bank and runs deep and green. The water is warm too, for it has slipped twinkling
	over the yellow sands in the sunlight before reaching the narrow pool. On one
	side of the river the golden foothill slopes curve up to the strong and rocky
	Gabilan Mountains, but on the valley side the water is lined with trees—willows
	fresh and green with every spring, carrying in their lower leaf junctures the
	debris of the winter's flooding; and sycamores with mottled, white, recumbent
	limbs and branches that arch over the pool`, doc2)

	td, err := ioutil.TempDir("", "tantivy_idx")
	if err != nil {
		panic(err)
	}
	callTantivy(doc.JRPCId.id, "index", "create", msi{"directory": td})
	callTantivy(doc.JRPCId.id, "indexwriter", "add_document", msi{
		"id": 1,
	})
	callTantivy(doc.JRPCId.id, "indexwriter", "add_document", msi{
		"id": 2,
	})
	callTantivy(doc.JRPCId.id, "indexwriter", "commit", msi{})

	callTantivy(doc.JRPCId.id, "index", "reader_builder", msi{})

	callTantivy(doc.JRPCId.id, "index_reader", "searcher", msi{})

	callTantivy(doc.JRPCId.id, "query_parser", "for_index", msi{
		"fields": []string{"title", "body"},
	})

	callTantivy(doc.JRPCId.id, "query_parser", "parse_query", msi{
		"query": "sea ",
	})
	callTantivy(doc.JRPCId.id, "searcher", "search", msi{})

}
