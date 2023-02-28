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
	"sync"
	"unsafe"

	"github.com/eluv-io/errors-go"
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

func cAlloc(sz int32) *C.char {
	return C.internal_malloc(C.int(sz))
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
