package main

/*
#cgo CFLAGS: -I../target
#cgo LDFLAGS: -ltantivy_jrpc -L../target/release
#include "tantivy_jrpc.h"
*/
import "C"
import (
	"fmt"
	"unsafe"
)

func main() {
	fmt.Println("Hello World")
	p := C.CString("{}")
	cs := (*C.uchar)(unsafe.Pointer(p))
	C.jpc(cs, C.ulong(3))
}
