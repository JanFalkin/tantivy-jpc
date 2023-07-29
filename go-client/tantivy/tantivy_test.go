package tantivy

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/eluv-io/log-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const resultSet1 = `[{"doc":{"body":["He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish. The water was warm but fishless."],"test":[555],"title":["The Old Man and the Sea"]},"score":,"explain":"noexplain"}]`
const resultSetNick = "[{\"doc\":{\"body\":[\"He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish. The water was warm but fishless.\"],\"order\":[1],\"title\":[\"The Old Man and the Sea\"]},\"score\":1.3338714,\"explain\":\"noexplain\"}]"

type jm = map[string]interface{}

func makeFuzzyIndex(t *testing.T, td string, useExisting bool) *TIndex {
	builder, err := NewBuilder(td)
	require.NoError(t, err)
	idxFieldTitle, err := builder.AddTextField("title", TEXT, true, true, "")
	require.NoError(t, err)
	require.EqualValues(t, 0, idxFieldTitle)
	idxFieldInt, err := builder.AddI64Field("test", INT, true, true, false)
	require.NoError(t, err)
	require.EqualValues(t, 1, idxFieldInt)

	doc, err := builder.Build()
	require.NoError(t, err)
	doc1, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 1, doc1)
	doc2, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 2, doc2)
	doc3, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 3, doc3)
	doc4, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 4, doc4)

	_, err = doc.AddText(idxFieldTitle, "The Name of the Wind", doc1)
	require.NoError(t, err)
	_, err = doc.AddInt(idxFieldInt, 444, doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldTitle, "The Diary of Muadib", doc2)
	require.NoError(t, err)
	_, err = doc.AddInt(idxFieldInt, 555, doc2)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldTitle, "A Dairy Cow", doc3)
	require.NoError(t, err)
	_, err = doc.AddInt(idxFieldInt, 666, doc3)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldTitle, "The Diary of a Young Girl", doc4)
	require.NoError(t, err)
	_, err = doc.AddInt(idxFieldInt, 777, doc4)
	require.NoError(t, err)

	idx, err := doc.CreateIndex()
	require.NoError(t, err)
	if !useExisting {
		idw, err := idx.CreateIndexWriter()
		require.NoError(t, err)
		opst1, err := idw.AddDocument(doc1)
		require.NoError(t, err)
		require.EqualValues(t, 0, opst1)
		opst2, err := idw.AddDocument(doc2)
		require.NoError(t, err)
		require.EqualValues(t, 1, opst2)
		opst3, err := idw.AddDocument(doc3)
		require.NoError(t, err)
		require.EqualValues(t, 2, opst3)
		opst4, err := idw.AddDocument(doc4)
		require.NoError(t, err)
		require.EqualValues(t, 3, opst4)

		fmt.Printf("op1 = %v op2 = %v op3 = %v op4 = %v\n ", opst1, opst2, opst3, opst4)
		idCommit, err := idw.Commit()
		require.NoError(t, err)
		fmt.Printf("commit id = %v", idCommit)
	}
	return idx
}

func makeIndex(t *testing.T, td string, useExisting bool) *TIndex {
	builder, err := NewBuilder(td)
	require.NoError(t, err)
	idxFieldTitle, err := builder.AddTextField("title", TEXT, true, true, "")
	require.NoError(t, err)
	require.EqualValues(t, 0, idxFieldTitle)
	idxFieldBody, err := builder.AddTextField("body", TEXT, true, true, "")
	require.NoError(t, err)
	require.EqualValues(t, 1, idxFieldBody)
	idxFieldInt, err := builder.AddI64Field("test", INT, true, true, true)
	require.NoError(t, err)
	require.EqualValues(t, 2, idxFieldInt)
	doc, err := builder.Build()
	require.NoError(t, err)
	doc1, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 1, doc1)
	doc2, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 2, doc2)
	_, err = doc.AddText(idxFieldTitle, "The Old Man and the Sea", doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish. The water was warm but fishless.", doc1)
	require.NoError(t, err)
	_, err = doc.AddInt(idxFieldInt, 555, doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldTitle, "Of Mice and Men", doc2)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody, `A few miles south of Soledad, the Salinas River drops in close to the hillside
	bank and runs deep and green. The water is warm too, for it has slipped twinkling
	over the yellow sands in the sunlight before reaching the narrow pool. On one
	side of the river the golden foothill slopes curve up to the strong and rocky
	Gabilan Mountains, but on the valley side the water is lined with trees—willows
	fresh and green with every spring, carrying in their lower leaf junctures the
	debris of the winter's flooding; and sycamores with mottled, white, recumbent
	limbs and branches that arch over the pool`, doc2)
	require.NoError(t, err)
	_, err = doc.AddInt(idxFieldInt, 666, doc2)
	require.NoError(t, err)

	idx, err := doc.CreateIndex()
	require.NoError(t, err)
	if !useExisting {
		idw, err := idx.CreateIndexWriter()
		require.NoError(t, err)
		opst1, err := idw.AddDocument(doc1)
		require.NoError(t, err)
		require.EqualValues(t, 0, opst1)
		opst2, err := idw.AddDocument(doc2)
		require.NoError(t, err)
		require.EqualValues(t, 1, opst2)
		fmt.Printf("op1 = %v op2 = %v\n", opst1, opst2)
		idCommit, err := idw.Commit()
		require.NoError(t, err)
		fmt.Printf("commit id = %v", idCommit)
	}
	return idx
}

func loadIndex(t *testing.T, td string) *TIndex {
	builder, err := NewBuilder(td)
	require.NoError(t, err)
	doc, err := builder.Build()
	require.NoError(t, err)
	idx, err := doc.CreateIndex()
	require.NoError(t, err)
	return idx

}

func testExpectedIndex(t *testing.T, idx *TIndex) {
	rb, err := idx.ReaderBuilder()
	require.NoError(t, err)
	expectedBody := "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish. The water was warm but fishless."
	qp, err := rb.Searcher()
	require.NoError(t, err)

	_, err = qp.ForIndex([]string{"title", "body"})
	require.NoError(t, err)

	searcher, err := qp.ParseQuery("title:Sea")
	require.NoError(t, err)
	s, err := searcher.Search(false, 0, 0, true)
	require.NoError(t, err)
	results := []map[string]interface{}{}
	err = json.Unmarshal([]byte(s), &results)
	require.NoError(t, err)
	require.EqualValues(t, expectedBody, results[0]["doc"].(map[string]interface{})["body"].([]interface{})[0].(string))

	searcherAgain, err := qp.ParseQuery("body:mottled")
	require.NoError(t, err)
	sAgain, err := searcherAgain.Search(true, 0, 0, true)
	require.NoError(t, err)
	err = json.Unmarshal([]byte(sAgain), &results)
	require.NoError(t, err)
	exp, ok := results[0]["doc"].(map[string]interface{})["title"].([]interface{})[0].(string)
	require.EqualValues(t, true, ok)
	require.EqualValues(t, "Of Mice and Men", exp)
}

func testAltExpectedIndex(t *testing.T, idx *TIndex) {
	rb, err := idx.ReaderBuilder()
	require.NoError(t, err)
	qp, err := rb.Searcher()
	require.NoError(t, err)

	_, err = qp.ForIndex([]string{"title", "body", "test"})
	require.NoError(t, err)

	searcher, err := qp.ParseQuery("title:Sea AND test:555")
	require.NoError(t, err)
	s, err := searcher.Search(false, 0, 0, true)
	require.NoError(t, err)
	results := []map[string]interface{}{}
	err = json.Unmarshal([]byte(s), &results)
	require.NoError(t, err)
	require.EqualValues(t, "The Old Man and the Sea", results[0]["doc"].(map[string]interface{})["title"].([]interface{})[0].(string))

	searcherAgain, err := qp.ParseQuery("body:mottled AND test:666")
	require.NoError(t, err)
	s, err = searcherAgain.Search(true, 0, 0, true)
	require.NoError(t, err)
	err = json.Unmarshal([]byte(s), &results)
	require.NoError(t, err)
	require.EqualValues(t, "Of Mice and Men", results[0]["doc"].(map[string]interface{})["title"].([]interface{})[0].(string))
}

func testExpectedTopIndex(t *testing.T, idx *TIndex) {
	rb, err := idx.ReaderBuilder()
	require.NoError(t, err)

	qp, err := rb.Searcher()
	require.NoError(t, err)

	_, err = qp.ForIndex([]string{"title", "body"})
	require.NoError(t, err)

	searcher, err := qp.ParseQuery("title:Mice OR title:Man")
	require.NoError(t, err)
	s, err := searcher.Search(false, uint64(1), 0, true)
	require.NoError(t, err)
	var res []interface{}
	err = json.Unmarshal([]byte(s), &res)
	require.NoError(t, err)
	require.EqualValues(t, 1, len(res))
}

func testFuzzyExpectedIndex(t *testing.T, idx *TIndex) {
	rb, err := idx.ReaderBuilder()
	require.NoError(t, err)

	qp, err := rb.Searcher()
	require.NoError(t, err)

	searcher, err := qp.ParseFuzzyQuery("title", "Diari")
	require.NoError(t, err)
	s, err := searcher.FuzzySearch()
	log.Info("return", s)
	require.NoError(t, err)
	resultSet := []interface{}{}
	err = json.Unmarshal([]byte(s), &resultSet)
	require.NoError(t, err)
	compareResults(t, resultSet)

}

func compareResults(t *testing.T, res []interface{}) {
	require.EqualValues(t, 2, len(res))
	for _, v := range res {
		innerArray := v.(jm)["doc"].(jm)["field_values"]
		innerMap := innerArray.([]interface{})[0].(jm)
		inner := innerMap["value"].(string)
		b := inner == "The Diary of a Young Girl" || inner == "The Diary of Muadib"
		require.EqualValues(t, true, b)
	}

}
func TestTantivyBasic(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)
	fmt.Printf("WD = %s", wd)
	t.Setenv("LD_LIBRARY_PATH", ".")
	LibInit()
	td, err := ioutil.TempDir("", "tindex*")
	defer func(err error) {
		if err == nil {
			if os.RemoveAll(td) != nil {
				log.Error("unable to cleanup temp dir", "val", td)
			}
		}
	}(err)
	assert.NoError(t, err)
	idx := makeIndex(t, td, false)
	testExpectedIndex(t, idx)
}

func TestTantivyIntField(t *testing.T) {
	t.Setenv("LD_LIBRARY_PATH", ".")
	LibInit()
	idx := makeIndex(t, "", false)
	testAltExpectedIndex(t, idx)
}

func TestRawSearch(t *testing.T) {
	builder, err := NewBuilder("")
	require.NoError(t, err)
	idxFieldTitle, err := builder.AddTextField("title", TEXT, true, true, "")
	require.NoError(t, err)
	require.EqualValues(t, 0, idxFieldTitle)
	idxFieldBody, err := builder.AddTextField("body", TEXT, true, true, "")
	require.NoError(t, err)
	require.EqualValues(t, 1, idxFieldBody)
	idxFieldInt, err := builder.AddI64Field("order", INT, true, true, true)
	require.NoError(t, err)
	require.EqualValues(t, 2, idxFieldInt)
	doc, err := builder.Build()
	require.NoError(t, err)
	doc1, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 1, doc1)
	doc2, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 2, doc2)
	_, err = doc.AddText(idxFieldTitle, "The Old Man and the Sea", doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish. The water was warm but fishless.", doc1)
	require.NoError(t, err)
	_, err = doc.AddInt(idxFieldInt, 1, doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldTitle, "Of Mice and Men", doc2)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody, `A few miles south of Soledad, the Salinas River drops in close to the hillside
	bank and runs deep and green. The water is warm too, for it has slipped twinkling
	over the yellow sands in the sunlight before reaching the narrow pool. On one
	side of the river the golden foothill slopes curve up to the strong and rocky
	Gabilan Mountains, but on the valley side the water is lined with trees—willows
	fresh and green with every spring, carrying in their lower leaf junctures the
	debris of the winter's flooding; and sycamores with mottled, white, recumbent
	limbs and branches that arch over the pool`, doc2)
	require.NoError(t, err)
	_, err = doc.AddInt(idxFieldInt, 2, doc2)
	require.NoError(t, err)

	indexer, err := doc.CreateIndex()
	require.NoError(t, err)

	idw, err := indexer.CreateIndexWriter()
	require.NoError(t, err)
	opst1, err := idw.AddDocument(doc1)
	require.NoError(t, err)
	require.EqualValues(t, 0, opst1)
	opst2, err := idw.AddDocument(doc2)
	require.NoError(t, err)
	require.EqualValues(t, 1, opst2)
	fmt.Printf("op1 = %v op2 = %v\n", opst1, opst2)
	idCommit, err := idw.Commit()
	require.NoError(t, err)
	fmt.Printf("commit id = %v", idCommit)

	rb, err := indexer.ReaderBuilder()
	require.NoError(t, err)
	qp, err := rb.Searcher()
	require.NoError(t, err)

	_, err = qp.ForIndex([]string{"title", "body", "order"})
	require.NoError(t, err)

	searcher, err := qp.ParseQuery("order:1")
	require.NoError(t, err)
	s, err := searcher.SearchRaw()
	require.NoError(t, err)
	results := []map[string]interface{}{}
	err = json.Unmarshal([]byte(s), &results)
	require.NoError(t, err)
	require.EqualValues(t, "The Old Man and the Sea", results[0]["title"].([]interface{})[0].(string))

	searcherAgain, err := qp.ParseQuery("order:2")
	require.NoError(t, err)
	s, err = searcherAgain.SearchRaw()
	require.NoError(t, err)
	err = json.Unmarshal([]byte(s), &results)
	require.NoError(t, err)
	require.EqualValues(t, "Of Mice and Men", results[0]["title"].([]interface{})[0].(string))
}

func TestDocsetSearch(t *testing.T) {
	builder, err := NewBuilder("")
	require.NoError(t, err)
	idxFieldTitle, err := builder.AddTextField("title", TEXT, true, true, "")
	require.NoError(t, err)
	require.EqualValues(t, 0, idxFieldTitle)
	idxFieldBody, err := builder.AddTextField("body", TEXT, true, true, "")
	require.NoError(t, err)
	require.EqualValues(t, 1, idxFieldBody)
	idxFieldInt, err := builder.AddI64Field("order", INT, true, true, true)
	require.NoError(t, err)
	require.EqualValues(t, 2, idxFieldInt)
	doc, err := builder.Build()
	require.NoError(t, err)
	doc1, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 1, doc1)
	doc2, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 2, doc2)
	_, err = doc.AddText(idxFieldTitle, "The Old Man and the Sea", doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish. The water was warm but fishless.", doc1)
	require.NoError(t, err)
	_, err = doc.AddInt(idxFieldInt, 1, doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldTitle, "Of Mice and Men", doc2)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody, `A few miles south of Soledad, the Salinas River drops in close to the hillside
	bank and runs deep and green. The water is warm too, for it has slipped twinkling
	over the yellow sands in the sunlight before reaching the narrow pool. On one
	side of the river the golden foothill slopes curve up to the strong and rocky
	Gabilan Mountains, but on the valley side the water is lined with trees—willows
	fresh and green with every spring, carrying in their lower leaf junctures the
	debris of the winter's flooding; and sycamores with mottled, white, recumbent
	limbs and branches that arch over the pool`, doc2)
	require.NoError(t, err)
	_, err = doc.AddInt(idxFieldInt, 2, doc2)
	require.NoError(t, err)

	indexer, err := doc.CreateIndex()
	require.NoError(t, err)

	idw, err := indexer.CreateIndexWriter()
	require.NoError(t, err)
	opst1, err := idw.AddDocument(doc1)
	require.NoError(t, err)
	require.EqualValues(t, 0, opst1)
	opst2, err := idw.AddDocument(doc2)
	require.NoError(t, err)
	require.EqualValues(t, 1, opst2)
	fmt.Printf("op1 = %v op2 = %v\n", opst1, opst2)
	idCommit, err := idw.Commit()
	require.NoError(t, err)
	fmt.Printf("commit id = %v", idCommit)

	rb, err := indexer.ReaderBuilder()
	require.NoError(t, err)
	qp, err := rb.Searcher()
	require.NoError(t, err)

	_, err = qp.ForIndex([]string{"title", "body", "order"})
	require.NoError(t, err)

	searcher, err := qp.ParseQuery("title:Sea OR title:Mice")
	require.NoError(t, err)
	s, err := searcher.Docset(true, 20, 0)
	require.NoError(t, err)
	results := map[string]interface{}{}
	err = json.Unmarshal([]byte(s), &results)
	require.NoError(t, err)
	resElement, ok := results["docset"].([]interface{})[0].(jm)
	require.EqualValues(t, true, ok)
	sDoc, err := searcher.GetDocument(true, float32(resElement["score"].(float64)), uint32(resElement["doc_id"].(float64)), uint32(resElement["segment_ord"].(float64)))
	require.NoError(t, err)
	log.Info(sDoc)
	err = json.Unmarshal([]byte(sDoc), &results)
	require.NoError(t, err)

	require.EqualValues(t, "Of Mice and Men", results["doc"].(jm)["title"].([]interface{})[0].(string))
	resElement, ok = results["docset"].([]interface{})[1].(jm)
	require.EqualValues(t, true, ok)
	sDoc, err = searcher.GetDocument(true, float32(resElement["score"].(float64)), uint32(resElement["doc_id"].(float64)), uint32(resElement["segment_ord"].(float64)))
	require.NoError(t, err)
	log.Info(sDoc)
	err = json.Unmarshal([]byte(sDoc), &results)
	require.NoError(t, err)

	require.EqualValues(t, "The Old Man and the Sea", results["doc"].(jm)["title"].([]interface{})[0].(string))

	// searcherAgain, err := qp.ParseQuery("order:2")
	// require.NoError(t, err)
	// s, err = searcherAgain.SearchRaw()
	// require.NoError(t, err)
	// err = json.Unmarshal([]byte(s), &results)
	// require.NoError(t, err)
	// require.EqualValues(t, "Of Mice and Men", results[0]["title"].([]interface{})[0].(string))
}

func TestStops(t *testing.T) {
	builder, err := NewBuilder("")
	require.NoError(t, err)
	idxFieldTitle, err := builder.AddTextField("title", TEXT, true, true, "")
	require.NoError(t, err)
	require.EqualValues(t, 0, idxFieldTitle)
	idxFieldBody, err := builder.AddTextField("body", TEXT, true, true, "")
	require.NoError(t, err)
	require.EqualValues(t, 1, idxFieldBody)
	idxFieldInt, err := builder.AddI64Field("order", INT, true, true, true)
	require.NoError(t, err)
	require.EqualValues(t, 2, idxFieldInt)
	doc, err := builder.Build()
	require.NoError(t, err)
	doc1, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 1, doc1)
	doc2, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 2, doc2)
	_, err = doc.AddText(idxFieldTitle, "The Old Man and the Sea", doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish. The water was warm but fishless.", doc1)
	require.NoError(t, err)
	_, err = doc.AddInt(idxFieldInt, 1, doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldTitle, "Of Mice and Men", doc2)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody, `A few miles south of Soledad, the Salinas River drops in close to the hillside
	bank and runs deep and green. The water is warm too, for it has slipped twinkling
	over the yellow sands in the sunlight before reaching the narrow pool. On one
	side of the river the golden foothill slopes curve up to the strong and rocky
	Gabilan Mountains, but on the valley side the water is lined with trees—willows
	fresh and green with every spring, carrying in their lower leaf junctures the
	debris of the winter's flooding; and sycamores with mottled, white, recumbent
	limbs and branches that arch over the pool`, doc2)
	require.NoError(t, err)
	_, err = doc.AddInt(idxFieldInt, 2, doc2)
	require.NoError(t, err)

	indexer, err := doc.CreateIndex()
	require.NoError(t, err)

	idw, err := indexer.CreateIndexWriter()
	require.NoError(t, err)
	opst1, err := idw.AddDocument(doc1)
	require.NoError(t, err)
	require.EqualValues(t, 0, opst1)
	opst2, err := idw.AddDocument(doc2)
	require.NoError(t, err)
	require.EqualValues(t, 1, opst2)
	fmt.Printf("op1 = %v op2 = %v\n", opst1, opst2)
	idCommit, err := idw.Commit()
	require.NoError(t, err)
	fmt.Printf("commit id = %v", idCommit)

	rb, err := indexer.ReaderBuilder()
	require.NoError(t, err)
	qp, err := rb.Searcher()
	require.NoError(t, err)

	_, err = qp.ForIndex([]string{"title", "body"})
	require.NoError(t, err)

	searcher, err := qp.ParseQuery("title:the")
	require.NoError(t, err)
	s, err := searcher.Search(false, 0, 0, true)
	require.NoError(t, err)
	results := []map[string]interface{}{}
	err = json.Unmarshal([]byte(s), &results)
	require.NoError(t, err)
	require.EqualValues(t, 0, len(results))
}

func TestIndexer(t *testing.T) {
	builder, err := NewBuilder("")
	require.NoError(t, err)
	idxFieldTitle, err := builder.AddTextField("title", TEXT, true, true, "")
	require.NoError(t, err)
	require.EqualValues(t, 0, idxFieldTitle)
	idxFieldBody, err := builder.AddTextField("body", TEXT, true, true, "")
	require.NoError(t, err)
	require.EqualValues(t, 1, idxFieldBody)
	idxFieldInt, err := builder.AddI64Field("order", INT, true, true, true)
	require.NoError(t, err)
	require.EqualValues(t, 2, idxFieldInt)
	doc, err := builder.Build()
	require.NoError(t, err)
	doc1, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 1, doc1)
	doc2, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 2, doc2)
	_, err = doc.AddText(idxFieldTitle, "The Old Man and the Sea", doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish. The water was warm but fishless.", doc1)
	require.NoError(t, err)
	_, err = doc.AddInt(idxFieldInt, 1, doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldTitle, "Of Mice and Men", doc2)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody, `A few miles south of Soledad, the Salinas River drops in close to the hillside
	bank and runs deep and green. The water is warm too, for it has slipped twinkling
	over the yellow sands in the sunlight before reaching the narrow pool. On one
	side of the river the golden foothill slopes curve up to the strong and rocky
	Gabilan Mountains, but on the valley side the water is lined with trees—willows
	fresh and green with every spring, carrying in their lower leaf junctures the
	debris of the winter's flooding; and sycamores with mottled, white, recumbent
	limbs and branches that arch over the pool`, doc2)
	require.NoError(t, err)
	_, err = doc.AddInt(idxFieldInt, 2, doc2)
	require.NoError(t, err)

	indexer, err := doc.CreateIndex()
	require.NoError(t, err)

	idw, err := indexer.CreateIndexWriter()
	require.NoError(t, err)
	opst1, err := idw.AddDocument(doc1)
	require.NoError(t, err)
	require.EqualValues(t, 0, opst1)
	opst2, err := idw.AddDocument(doc2)
	require.NoError(t, err)
	require.EqualValues(t, 1, opst2)
	fmt.Printf("op1 = %v op2 = %v\n", opst1, opst2)
	idCommit, err := idw.Commit()
	require.NoError(t, err)
	fmt.Printf("commit id = %v", idCommit)

	rb, err := indexer.ReaderBuilder()
	require.NoError(t, err)
	qp, err := rb.Searcher()
	require.NoError(t, err)

	_, err = qp.ForIndex([]string{"title", "body"})
	require.NoError(t, err)

	searcher, err := qp.ParseQuery("order:1")
	require.NoError(t, err)
	s, err := searcher.Search(false, 0, 0, true)
	require.NoError(t, err)
	results := []map[string]interface{}{}
	err = json.Unmarshal([]byte(s), &results)
	require.NoError(t, err)
	require.EqualValues(t, "The Old Man and the Sea", results[0]["doc"].(map[string]interface{})["title"].([]interface{})[0].(string))

	searcherAgain, err := qp.ParseQuery("order:2")
	require.NoError(t, err)
	s, err = searcherAgain.Search(true, 0, 0, true)
	require.NoError(t, err)
	err = json.Unmarshal([]byte(s), &results)
	require.NoError(t, err)
	require.EqualValues(t, "Of Mice and Men", results[0]["doc"].(map[string]interface{})["title"].([]interface{})[0].(string))
}

func TestTantivyFuzzy(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)
	fmt.Printf("WD = %s", wd)
	t.Setenv("LD_LIBRARY_PATH", ".")
	LibInit()
	td, err := ioutil.TempDir("", "tindex*")
	defer func(err error) {
		if err == nil {
			if os.RemoveAll(td) != nil {
				log.Error("unable to cleanup temp dir", "val", td)
			}
		}
	}(err)
	assert.NoError(t, err)
	idx := makeFuzzyIndex(t, td, false)
	testFuzzyExpectedIndex(t, idx)
}

func TestTantivyTopLimit(t *testing.T) {
	idx := makeIndex(t, "", false)
	testExpectedTopIndex(t, idx)

}
func TestTantivyIndexReuse(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)
	fmt.Printf("WD = %s", wd)
	t.Setenv("LD_LIBRARY_PATH", ".")
	LibInit()
	td, err := ioutil.TempDir("", "tindex*")
	defer func(err error) {
		if err == nil {
			if os.RemoveAll(td) != nil {
				log.Error("unable to cleanup temp dir", "val", td)
			}
		}
	}(err)
	assert.NoError(t, err)
	_ = makeIndex(t, td, false)

	idx := loadIndex(t, td)
	testExpectedIndex(t, idx)
}

func TestTantivyStress(t *testing.T) {
	builder, err := NewBuilder("")
	require.NoError(t, err)
	fieldIds := map[string]int{}
	fields := []string{"title", "body", "speech", "shot", "action", "logo", "segment", "celeb", "cast"}
	fieldsLong := []string{"description", "has_field"}
	for _, f := range fields {
		fieldIds[f], err = builder.AddTextField(f, TEXT, true, true, "")
		require.NoError(t, err)
	}
	for _, f := range fieldsLong {
		fieldIds[f], err = builder.AddTextField(f, TEXT, true, true, "")
		require.NoError(t, err)
	}

	doc, err := builder.Build()
	require.NoError(t, err)
	ti, err := doc.CreateIndex()
	require.NoError(t, err)
	tiw, err := ti.CreateIndexWriter()
	require.NoError(t, err)

	text := "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish."
	text2 := `A few miles south of Soledad, the Salinas River drops in close to the hillside
	bank and runs deep and green. The water is warm too, for it has slipped twinkling
	over the yellow sands in the sunlight before reaching the narrow pool. On one
	side of the river the golden foothill slopes curve up to the strong and rocky
	Gabilan Mountains, but on the valley side the water is lined with trees—willows
	fresh and green with every spring, carrying in their lower leaf junctures the
	debris of the winter's flooding; and sycamores with mottled, white, recumbent
	limbs and branches that arch over the pool`
	for i := 0; i < 1041; i++ {
		newDoc, err := doc.Create()
		require.NoError(t, err)
		for _, f := range fields {
			_, err = doc.AddText(fieldIds[f], text, newDoc)
			require.NoError(t, err)
		}
		for _, f := range fieldsLong {
			_, err = doc.AddText(fieldIds[f], text2, newDoc)
			require.NoError(t, err)
		}
		_, err = tiw.AddDocument(newDoc)
		require.NoError(t, err)
	}
	_, err = tiw.Commit()
	require.NoError(t, err)
}

func TestTantivyDeleteTerm(t *testing.T) {
	builder, err := NewBuilder("")
	require.NoError(t, err)
	idxFieldTitle, err := builder.AddTextField("title", TEXT, true, true, "")
	require.NoError(t, err)
	require.EqualValues(t, 0, idxFieldTitle)
	idxFieldInt, err := builder.AddI64Field("test", INT, true, true, true)
	require.NoError(t, err)
	require.EqualValues(t, 1, idxFieldInt)
	doc, err := builder.Build()
	require.NoError(t, err)
	d1, err := doc.Create()
	require.NoError(t, err)
	ti, err := doc.CreateIndex()
	require.NoError(t, err)
	tiw, err := ti.CreateIndexWriter()
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldTitle, "FooFoo", d1)
	require.NoError(t, err)
	_, err = doc.AddInt(idxFieldInt, 444, d1)
	require.NoError(t, err)
	_, err = tiw.DeleteTerm("test", 444)
	require.NoError(t, err)

}

func TestChangeKB(t *testing.T) {
	LibInit()
	SetKB(1.0, 0.80)

	//	idx := makeIndex(t, "", false)
	//	testExpectedTopIndex(t, idx)

}
