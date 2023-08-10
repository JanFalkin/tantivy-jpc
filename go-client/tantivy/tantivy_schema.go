package tantivy

import (
	"encoding/json"
	"strconv"
)

type TSchema struct {
	*JPCId
}

type FieldEntry struct {
	Name    string                 `json:"name"`
	Type    string                 `json:"type"`
	Options map[string]interface{} `json:"options"`
}

func (idr *TSchema) GetFieldEntry(fieldName string) (*FieldEntry, error) {
	s, err := idr.callTantivy("schema", "get_field_entry", msi{"field": []string{fieldName}})
	if err != nil {
		return nil, err
	}
	fe := FieldEntry{}
	err = json.Unmarshal([]byte(s), &fe)
	if err != nil {
		return nil, err
	}
	return &fe, nil
}

func (idr *TSchema) NumFields() (uint64, error) {
	s, err := idr.callTantivy("schema", "num_fields", msi{})
	if err != nil {
		return 0, err
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}
	return uint64(v), nil
}

func (idr *TSchema) Fields() (msi, error) {
	s, err := idr.callTantivy("schema", "fields", msi{})
	if err != nil {
		return nil, err
	}
	jm := msi{}
	err = json.Unmarshal([]byte(s), &jm)
	if err != nil {
		return nil, err
	}
	return jm, nil
}

func (idr *TSchema) GetField(name string) (uint64, error) {
	s, err := idr.callTantivy("schema", "get_field", msi{"field": []string{name}})
	if err != nil {
		return 0, err
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}
	return uint64(v), nil
}
