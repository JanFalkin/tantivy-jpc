package tantivy

type TSearcher struct {
	*TQueryParser
}

func (s *TSearcher) Docset(scoring bool, topLimit uint64, offset uint64) (string, error) {
	return s.callTantivy("searcher", "docset", msi{
		"top_limit": topLimit,
		"offset":    offset,
		"scoring":   scoring,
	})
}

func (s *TSearcher) GetDocument(explain bool, score float32, docId uint32, segOrd uint32, snippetField int64) (string, error) {
	return s.callTantivy("searcher", "get_document", msi{
		"segment_ord":   segOrd,
		"doc_id":        docId,
		"score":         score,
		"explain":       explain,
		"snippet_field": snippetField,
	})
}

func (s *TSearcher) Search(explain bool, topLimit uint64, offset uint64, ordered bool) (string, error) {
	args := msi{}
	if topLimit >= 1 {
		args["top_limit"] = topLimit
	}
	if explain {
		args["explain"] = true
	}
	args["scoring"] = ordered
	args["offset"] = offset
	return s.callTantivy("searcher", "search", args)
}

func (s *TSearcher) SearchRaw() (string, error) {
	args := msi{}
	return s.callTantivy("searcher", "search_raw", args)
}

func (s *TSearcher) FuzzySearch(topLimit ...uint64) (string, error) {
	args := msi{}
	if len(topLimit) >= 1 {
		args["top_limit"] = topLimit[0]
	}
	return s.callTantivy("fuzzy_searcher", "fuzzy_searcher", msi{})
}

func (s *TSearcher) Snippets(fieldId uint64, docs []uint) (string, error) {
	args := msi{"field_id": fieldId, "documents": docs}

	return s.callTantivy("searcher", "snippet", args)
}
