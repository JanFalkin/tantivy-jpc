package tantivy

type TSearcher struct {
	*TQueryParser
}

const NOSNIPPET = -1

func (s *TSearcher) Docset(scoring bool, topLimit uint64, offset uint64) (string, error) {
	return s.callTantivy("searcher", "docset", msi{
		"top_limit": topLimit,
		"offset":    offset,
		"scoring":   scoring,
	})
}

func (s *TSearcher) GetDocument(explain bool, score float32, docId uint32, segOrd uint32, snippetField ...string) (string, error) {
	return s.callTantivy("searcher", "get_document", msi{
		"segment_ord":   segOrd,
		"doc_id":        docId,
		"score":         score,
		"explain":       explain,
		"snippet_field": snippetField,
	})
}

func (s *TSearcher) Search(explain bool, topLimit uint64, offset uint64, ordered bool, snippetField ...string) (string, error) {
	args := msi{"scoring": ordered, "offset": offset, "snippet_field": snippetField}
	if topLimit >= 1 {
		args["top_limit"] = topLimit
	}
	if explain {
		args["explain"] = true
	}
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
