package tantivy

type TQueryParser struct {
	*TIndex
}

func (qp *TQueryParser) ForIndex(fields []string) (uint, error) {
	_, err := qp.callTantivy("query_parser", "for_index", msi{
		"fields": fields,
	})
	if err != nil {
		return 0, err
	}
	return 0, nil
}

func (qp *TQueryParser) ParseQuery(query string) (*TSearcher, error) {
	_, err := qp.callTantivy("query_parser", "parse_query", msi{
		"query": query,
	})
	if err != nil {
		return nil, err
	}
	return &TSearcher{qp}, nil
}

func (qp *TQueryParser) ParseFuzzyQuery(field, term string) (*TSearcher, error) {
	_, err := qp.callTantivy("query_parser", "parse_fuzzy_query", msi{
		"term":  []string{term},
		"field": []string{field},
	})
	if err != nil {
		return nil, err
	}
	return &TSearcher{qp}, nil
}
