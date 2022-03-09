# c-api access to Tantivy Search using JPC 1.0

## Installing

### Install Golang > 1.16

https://go.dev/dl/

## Changing or updating build targets for tantivy_jrpc.so(dll/dynlib)
### Install Rust

```
curl https://sh.rustup.rs -sSf | sh -s -- -y
source $HOME/.cargo/env
```
## Building

```
cargo build

```

### Golang

### Via go get

```
go get github.com/JanFalkin/tantivy_jrpc/go-client/tantivy

```

### A Simple Example
```
package main

import (
	"fmt"

	"github.com/JanFalkin/tantivy_jrpc/go-client/tantivy"
)

func main() {
	fmt.Println("Hello World")
	tantivy.LibInit()
	builder, err := tantivy.NewBuilder()
	if err != nil {
		panic(err)
	}
	fmt.Println("Here")
	idxFieldTitle, err := builder.AddTextField("title", false)
	if err != nil {
		panic(err)
	}
	idxFieldBody, err := builder.AddTextField("body", false)
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
	doc.AddText(idxFieldBody, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish.", doc1)
	doc.AddText(idxFieldTitle, "Of Mice and Men", doc2)
	doc.AddText(idxFieldBody, `A few miles south of Soledad, the Salinas River drops in close to the hillside
	bank and runs deep and green. The water is warm too, for it has slipped twinkling
	over the yellow sands in the sunlight before reaching the narrow pool. On one
	side of the river the golden foothill slopes curve up to the strong and rocky
	Gabilan Mountains, but on the valley side the water is lined with trees—willows
	fresh and green with every spring, carrying in their lower leaf junctures the
	debris of the winter's flooding; and sycamores with mottled, white, recumbent
	limbs and branches that arch over the pool`, doc2)

	idx, err := doc.CreateIndex()
	if err != nil {
		panic(err)
	}
	idw, err := idx.CreateIndexWriter()
	if err != nil {
		panic(err)
	}
	opst1, err := idw.AddDocument(doc1)
	if err != nil {
		panic(err)
	}
	opst2, err := idw.AddDocument(doc2)
	if err != nil {
		panic(err)
	}
	fmt.Printf("op1 = %v op2 = %v\n", opst1, opst2)

	idCommit, err := idw.Commit()
	if err != nil {
		panic(err)
	}
	fmt.Printf("commit id = %v", idCommit)

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

	searcher, err := qp.ParseQuery("sea")
	if err != nil {
		panic(err)
	}
	searcher.Search()

	searcherAgain, err := qp.ParseQuery("mottled")
	if err != nil {
		panic(err)
	}
	searcherAgain.Search()

}
````


```

