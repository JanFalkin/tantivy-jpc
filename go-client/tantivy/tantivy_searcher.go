package tantivy

type TSearcher struct {
	*TQueryParser
}

func (s *TSearcher) Search(explain bool, topLimit ...uint64) (string, error) {
	args := msi{}
	if len(topLimit) >= 1 {
		args["top_limit"] = topLimit[0]
	}
	if explain {
		args["explain"] = true
	}
	return s.callTantivy("searcher", "search", args)
}

func (s *TSearcher) FuzzySearch(topLimit ...uint64) (string, error) {
	args := msi{}
	if len(topLimit) >= 1 {
		args["top_limit"] = topLimit[0]
	}
	return s.callTantivy("fuzzy_searcher", "fuzzy_searcher", msi{})
}
