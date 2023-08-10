package tantivy

import "strconv"

type TSchema struct {
	*JPCId
}

func (idr *TSchema) GetFieldEntry(fieldName string) (string, error) {
	s, err := idr.callTantivy("schema", "get_field_entry", msi{"field": []string{fieldName}})
	if err != nil {
		return "", err
	}
	return s, nil
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
