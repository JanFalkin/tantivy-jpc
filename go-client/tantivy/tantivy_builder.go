package tantivy

import (
	"encoding/json"
	"fmt"

	"github.com/eluv-io/errors-go"
	uuid "github.com/satori/go.uuid"
)

type StorageKind uint

const (
	STRING StorageKind = 1
	TEXT   StorageKind = 2
	INT    StorageKind = 3
	UINT   StorageKind = 4
)

type TBuilder struct {
	*JPCId
}

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
			ccomsBuf: cAlloc(memSizeToUse),
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

func (tb *TBuilder) AddTextField(name string, fieldType StorageKind, stored bool, indexed bool, fast bool) (int, error) {
	s, err := tb.callTantivy("builder", "add_text_field", msi{
		"name":    name,
		"type":    fieldType,
		"stored":  stored,
		"indexed": indexed,
		"id":      tb.JPCId.id,
		"fast":    fast,
	})
	return tb.standardReturnHandler(s, err)

}

func (tb *TBuilder) AddDateField(name string, fieldType StorageKind, stored bool, indexed bool, fast bool) (int, error) {
	s, err := tb.callTantivy("builder", "add_date_field", msi{
		"name":    name,
		"type":    fieldType,
		"stored":  stored,
		"indexed": indexed,
		"id":      tb.JPCId.id,
		"fast":    fast,
	})

	return tb.standardReturnHandler(s, err)
}

func (tb *TBuilder) AddU64Field(name string, fieldType StorageKind, stored bool, indexed bool, fast bool) (int, error) {
	s, err := tb.callTantivy("builder", "add_u64_field", msi{
		"name":    name,
		"type":    fieldType,
		"stored":  stored,
		"indexed": indexed,
		"id":      tb.JPCId.id,
		"fast":    fast,
	})

	return tb.standardReturnHandler(s, err)
}

func (tb *TBuilder) AddI64Field(name string, fieldType StorageKind, stored bool, indexed bool, fast bool) (int, error) {
	s, err := tb.callTantivy("builder", "add_i64_field", msi{
		"name":    name,
		"type":    fieldType,
		"stored":  stored,
		"indexed": indexed,
		"id":      tb.JPCId.id,
		"fast":    fast,
	})

	return tb.standardReturnHandler(s, err)
}

func (tb *TBuilder) AddF64Field(name string, fieldType StorageKind, stored bool, indexed bool, fast bool) (int, error) {
	s, err := tb.callTantivy("builder", "add_f64_field", msi{
		"name":    name,
		"type":    fieldType,
		"stored":  stored,
		"indexed": indexed,
		"id":      tb.JPCId.id,
		"fast":    fast,
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
