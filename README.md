# c-api access to Tantivy Search using JPC 1.0

## Installing

### Install Golang > 1.16

<https://go.dev/dl/>

## Changing or updating build targets for tantivy-jpc.a

### Install Rust

```
curl https://sh.rustup.rs -sSf | sh -s -- -y
source $HOME/.cargo/env
cargo install cargo-post
```

## Building

```
cargo build --release

```

### Golang

### Via go get

```
go get github.com/JanFalkin/tantivy-jpc/go-client/tantivy

```

### A Simple Example

```
package main

import (
 "fmt"
 "io/ioutil"
 "os"

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

func main() {
 tantivy.LibInit()
 td, err := ioutil.TempDir("", "tindex")
 defer func() {
  if err == nil {
   os.RemoveAll(td)
  }
 }()
 builder, err := tantivy.NewBuilder(td)
 if err != nil {
  panic(err)
 }
 idxFieldTitle, err := builder.AddTextField("title", tantivy.TEXT, true)
 if err != nil {
  panic(err)
 }
 idxFieldBody, err := builder.AddTextField("body", tantivy.TEXT, true)
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
 doc.AddText(idxFieldTitle, "Of Mice and Men", doc2)
 doc.AddText(idxFieldBody, ofMiceAndMen, doc2)

 idx, err := doc.CreateIndex()
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

 searcher, err := qp.ParseQuery("Old Man")
 if err != nil {
  panic(err)
 }

 var sr map[string][]string

 s, err := searcher.Search()
 if err != nil {
  panic(err)
 }

 err = json.Unmarshal([]byte(s), &sr)
 if err != nil {
  panic(err)
 }
 if sr["title"][1] != oldMan {
  panic("expcted value not received")
 }
 searcherAgain, err := qp.ParseQuery("Mice Men")
 if err != nil {
  panic(err)
 }
 s, err = searcherAgain.Search()
 if err != nil {
  panic(err)
 }
 err = json.Unmarshal([]byte(s), &sr)
 if err != nil {
  panic(err)
 }

 if sr["title"][1] != ofMiceAndMen {
  panic("expcted value not received")
 }

}

```

```
