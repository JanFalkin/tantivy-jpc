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

const resultSet1 = `{"body":["He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish. The water was warm but fishless."],"title":["The Old Man and the Sea"]}`
const resultSet2 = `{"body":["A few miles south of Soledad, the Salinas River drops in close to the hillside\n\tbank and runs deep and green. The water is warm too, for it has slipped twinkling\n\tover the yellow sands in the sunlight before reaching the narrow pool. On one\n\tside of the river the golden foothill slopes curve up to the strong and rocky\n\tGabilan Mountains, but on the valley side the water is lined with trees—willows\n\tfresh and green with every spring, carrying in their lower leaf junctures the\n\tdebris of the winter's flooding; and sycamores with mottled, white, recumbent\n\tlimbs and branches that arch over the pool"],"title":["Of Mice and Men"]}`

type jim = map[int]interface{}
type jm = map[string]interface{}

func makeFuzzyIndex(t *testing.T, td string, useExisting bool) *TIndex {
	builder, err := NewBuilder(td)
	require.NoError(t, err)
	idxFieldTitle, err := builder.AddTextField("title", TEXT, true)
	require.NoError(t, err)
	require.EqualValues(t, 0, idxFieldTitle)
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
	_, err = doc.AddText(idxFieldTitle, "The Diary of Muadib", doc2)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldTitle, "A Dairy Cow", doc3)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldTitle, "The Diary of a Young Girl", doc4)
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
	idxFieldTitle, err := builder.AddTextField("title", TEXT, true)
	require.NoError(t, err)
	require.EqualValues(t, 0, idxFieldTitle)
	idxFieldBody, err := builder.AddTextField("body", TEXT, true)
	require.NoError(t, err)
	require.EqualValues(t, 1, idxFieldBody)
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

	qp, err := rb.Searcher()
	require.NoError(t, err)

	_, err = qp.ForIndex([]string{"title", "body"})
	require.NoError(t, err)

	searcher, err := qp.ParseQuery("Sea")
	require.NoError(t, err)
	s, err := searcher.Search()
	require.NoError(t, err)
	require.EqualValues(t, resultSet1+"\n", s)

	searcherAgain, err := qp.ParseQuery("mottled")
	require.NoError(t, err)
	s, err = searcherAgain.Search()
	require.NoError(t, err)
	require.EqualValues(t, resultSet2+"\n", s)
}

func testFuzzyExpectedIndex(t *testing.T, idx *TIndex) {
	rb, err := idx.ReaderBuilder()
	require.NoError(t, err)

	qp, err := rb.Searcher()
	require.NoError(t, err)

	searcher, err := qp.ParseFuzzyQuery("title", "Diary")
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
		innerArray := v.(jm)["field_values"]
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
