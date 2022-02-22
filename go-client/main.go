package main

/*
#cgo CFLAGS: -I../target
#cgo LDFLAGS: -ltantivy_jrpc -L../target/release
#include "tantivy_jrpc.h"
*/
import "C"
import (
	"encoding/json"
	"fmt"
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

func main() {
	fmt.Println("Hello World")
	u, err := uuid.NewV4()
	if err != nil {
		panic("failed to create new ID")
	}
	f := map[string]interface{}{
		"id":     u.String(),
		"jpc":    "1.0",
		"obj":    "document",
		"method": "add_text",
		"params": msi{
			"a": "b",
		},
	}
	b, err := json.Marshal(f)
	p := C.CString(string(b))
	cs := (*C.uchar)(unsafe.Pointer(p))
	C.jpc(cs, C.ulong(uint64(len(string(b)))))
}
