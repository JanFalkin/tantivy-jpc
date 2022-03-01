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

func callTantivy(u, object, method string, params msi) error {
	f := map[string]interface{}{
		"id":     u,
		"jpc":    "1.0",
		"obj":    object,
		"method": method,
		"params": params,
	}
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	p := C.CString(string(b))
	rb := make([]byte, 500)
	csrb := C.CString(string(rb))
	crb := (*C.uchar)(unsafe.Pointer(csrb))
	cs := (*C.uchar)(unsafe.Pointer(p))
	rbl := len(rb)
	prbl := (*C.ulong)(unsafe.Pointer(&rbl))
	r := C.jpc(cs, C.ulong(uint64(len(string(b)))), crb, prbl)
	fmt.Printf("return value %v ret buffer %v\n", r, C.GoString(csrb))
	return nil
}

func main() {
	fmt.Println("Hello World")
	C.init()
	u, err := uuid.NewV4()
	if err != nil {
		panic("failed to get UUID")
	}
	id := u.String()
	callTantivy(id, "builder", "add_text_field", msi{
		"name":  "kewlness",
		"index": false,
	})
	callTantivy(id, "builder", "add_text_field", msi{
		"name":  "superKewlness",
		"index": false,
	})
	callTantivy(id, "builder", "build", msi{})
	callTantivy(id, "document", "create", msi{})
	callTantivy(id, "document", "create", msi{})
	callTantivy(id, "document", "add_text", msi{
		"field":  0,
		"value":  "Something to index with some the random KLJBDfLBFLSEbfebgrfiwfqwhuvac vnasdjbgfn",
		"id":     0,
		"doc_id": 1,
	})
	callTantivy(id, "document", "add_text", msi{
		"field":  1,
		"value":  "Another value that is different than the first YSDLJFLSKfioSGYU",
		"id":     0,
		"doc_id": 2,
	})
	td, err := ioutil.TempDir("", "tantivy_idx")
	if err != nil {
		panic(err)
	}
	callTantivy(id, "index", "create", msi{"directory": td})
	callTantivy(id, "indexwriter", "add_document", msi{
		"id": 1,
	})
	callTantivy(id, "indexwriter", "add_document", msi{
		"id": 2,
	})
	callTantivy(id, "indexwriter", "commit", msi{})

	callTantivy(id, "index", "reader_builder", msi{})

	callTantivy(id, "index_reader", "searcher", msi{})

	callTantivy(id, "query_parser", "for_index", msi{})

	callTantivy(id, "query_parser", "parse_query", msi{
		"query": "the",
	})
	callTantivy(id, "searcher", "search", msi{})

}
