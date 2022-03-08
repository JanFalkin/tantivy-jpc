package tantivy

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTantivy(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)
	fmt.Printf("WD = %s", wd)
	t.Setenv("LD_LIBRARY_PATH", ".")
	LibInit()
	builder, err := NewBuilder()
	require.NoError(t, err)
	idxFieldTitle, err := builder.AddTextField("title", false)
	require.NoError(t, err)
	idxFieldBody, err := builder.AddTextField("body", false)
	require.NoError(t, err)
	doc, err := builder.Build()
	require.NoError(t, err)
	doc1, err := doc.Create()
	require.NoError(t, err)
	doc2, err := doc.Create()
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldTitle, "The Old Man and the Sea", doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish.", doc1)
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
	idw, err := idx.CreateIndexWriter()
	require.NoError(t, err)
	opst1, err := idw.AddDocument(doc1)
	require.NoError(t, err)
	opst2, err := idw.AddDocument(doc2)
	require.NoError(t, err)
	fmt.Printf("op1 = %v op2 = %v\n", opst1, opst2)

	idCommit, err := idw.Commit()
	require.NoError(t, err)
	fmt.Printf("commit id = %v", idCommit)

	rb, err := idx.ReaderBuilder()
	require.NoError(t, err)

	qp, err := rb.Searcher()
	require.NoError(t, err)

	_, err = qp.ForIndex([]string{"title", "body"})
	require.NoError(t, err)

	searcher, err := qp.ParseQuery("sea")
	require.NoError(t, err)
	searcher.Search()

	searcherAgain, err := qp.ParseQuery("mottled")
	require.NoError(t, err)
	searcherAgain.Search()

}
