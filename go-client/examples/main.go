package main

import (
	"encoding/json"
	"fmt"

	"github.com/JanFalkin/tantivy-jpc/go-client/tantivy"
)

const ofMiceAndMen = `A few miles south of Soledad, the Salinas River drops in close to the hillside
bank and runs deep and green. The water is warm too, for it has slipped twinkling
over the yellow sands in the sunlight before reaching the narrow pool. On one
side of the river the golden foothill slopes curve up to the strong and rocky
Gabilan Mountains, but on the valley side the water is lined with trees—willows
fresh and green with every spring, carrying in their lower leaf junctures the
debris of the winter's flooding; and sycamores with mottled, white, recumbent
limbs and branches that arch over the pool`
const oldMan = "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish."

func doRun() {
	tantivy.LibInit("debug")
	builder, err := tantivy.NewBuilder("")
	if err != nil {
		panic(err)
	}
	idxFieldTitle, err := builder.AddTextField("title", tantivy.TEXT, true, true, "")
	if err != nil {
		panic(err)
	}
	idxFieldBody, err := builder.AddTextField("body", tantivy.TEXT, false, true, "")
	if err != nil {
		panic(err)
	}

	idxFieldOrder, err := builder.AddI64Field("order", tantivy.INT, true, true, true)
	if err != nil {
		panic(err)
	}

	doc, err := builder.Build()
	if err != nil {
		panic(err)
	}
	doc1, err := doc.Create()
	if err != nil {
		panic(err)
	}
	doc2, err := doc.Create()
	if err != nil {
		panic(err)
	}
	doc.AddText(idxFieldTitle, "The Old Man and the Sea", doc1)
	doc.AddText(idxFieldBody, oldMan, doc1)
	doc.AddInt(idxFieldOrder, 111, doc1)
	doc.AddText(idxFieldTitle, "Of Mice and Men", doc2)
	doc.AddText(idxFieldBody, ofMiceAndMen, doc2)
	doc.AddInt(idxFieldOrder, 222, doc2)

	idx, err := doc.CreateIndex()
	if err != nil {
		panic(err)
	}
	_, err = idx.SetMultiThreadExecutor(8)
	if err != nil {
		panic(err)
	}

	idw, err := idx.CreateIndexWriter()
	if err != nil {
		panic(err)
	}
	_, err = idw.AddDocument(doc1)
	if err != nil {
		panic(err)
	}
	_, err = idw.AddDocument(doc2)
	if err != nil {
		panic(err)
	}

	_, err = idw.Commit()
	if err != nil {
		panic(err)
	}

	rb, err := idx.ReaderBuilder()
	if err != nil {
		panic(err)
	}

	qp, err := rb.Searcher()
	if err != nil {
		panic(err)
	}

	_, err = qp.ForIndex([]string{"title", "body"})
	if err != nil {
		panic(err)
	}

	searcher, err := qp.ParseQuery("order:111")
	if err != nil {
		panic(err)
	}

	var sr []map[string]interface{}

	s, err := searcher.Search(false, 0, 0, true, tantivy.NOSNIPPET)
	if err != nil {
		panic(err)
	}

	err = json.Unmarshal([]byte(s), &sr)
	if err != nil {
		panic(err)
	}
	if sr[0]["doc"].(map[string]interface{})["title"].([]interface{})[0] != "The Old Man and the Sea" {
		panic("expected value not received")
	}
	if err != nil {
		panic(err)
	}
	searcherAgain, err := qp.ParseQuery("order:222")
	if err != nil {
		panic(err)
	}
	s, err = searcherAgain.Search(false, 0, 0, true, tantivy.NOSNIPPET)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal([]byte(s), &sr)
	if err != nil {
		panic(err)
	}

	if sr[0]["doc"].(map[string]interface{})["title"].([]interface{})[0] != "Of Mice and Men" {
		panic("expected value not received")
	}

	tantivy.ClearSession(builder.ID())
	fmt.Println("It worked!!!")

}

func main() {
	doRun()
}
