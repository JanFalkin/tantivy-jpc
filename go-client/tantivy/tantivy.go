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

func SetKB(k float64, b float64) {
	C.set_k_and_b(C.float(k), C.float(b))
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
	var pcomsBuf **C.char
	var blen int64
	sb := string(b)
	pcJPCParams := C.CString(sb)
	pCDesctination := (**C.uchar)(unsafe.Pointer(pcomsBuf))
	defer func() {
		C.free_buffer(pCDesctination)
	}()
	cJPCParams := (*C.uchar)(unsafe.Pointer(pcJPCParams))
	pDestinationLen := (*C.ulong)(unsafe.Pointer(&blen))
	ttret := C.tantivy_jpc(cJPCParams, C.ulong(uint64(len(sb))), &pCDesctination, pDestinationLen)
	if ttret < 0 {
		return "", errors.E("Tantivy JPC Failed", errors.K.Invalid, "desc", string(C.GoBytes(unsafe.Pointer(*pCDesctination), C.int(*pDestinationLen))))
	}
	returnData := string(C.GoBytes(unsafe.Pointer(*pCDesctination), C.int(*pDestinationLen)))
	return returnData, nil
}
