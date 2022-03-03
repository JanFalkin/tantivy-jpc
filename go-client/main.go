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
	rb := make([]byte, 5000000)
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
		"name":  "title",
		"index": false,
	})
	callTantivy(id, "builder", "add_text_field", msi{
		"name":  "body",
		"index": false,
	})
	callTantivy(id, "builder", "build", msi{})
	callTantivy(id, "document", "create", msi{})
	callTantivy(id, "document", "create", msi{})
	callTantivy(id, "document", "add_text", msi{
		"field":  0,
		"value":  "The Old Man and the Sea",
		"id":     0,
		"doc_id": 1,
	})
	callTantivy(id, "document", "add_text", msi{
		"field":  1,
		"value":  "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish.",
		"id":     0,
		"doc_id": 1,
	})
	callTantivy(id, "document", "add_text", msi{
		"field":  0,
		"value":  "Of Mice and Men",
		"id":     0,
		"doc_id": 2,
	})
	callTantivy(id, "document", "add_text", msi{
		"field": 1,
		"value": `A few miles south of Soledad, the Salinas River drops in close to the hillside
		bank and runs deep and green. The water is warm too, for it has slipped twinkling
		over the yellow sands in the sunlight before reaching the narrow pool. On one
		side of the river the golden foothill slopes curve up to the strong and rocky
		Gabilan Mountains, but on the valley side the water is lined with trees—willows
		fresh and green with every spring, carrying in their lower leaf junctures the
		debris of the winter's flooding; and sycamores with mottled, white, recumbent
		limbs and branches that arch over the pool`,
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

	callTantivy(id, "query_parser", "for_index", msi{
		"fields": []string{"title", "body"},
	})

	callTantivy(id, "query_parser", "parse_query", msi{
		"query": "sea ",
	})
	callTantivy(id, "searcher", "search", msi{})

}
