use crate::info;
use crate::json;
use crate::tantivy_jpc;
use crate::InternalCallResult;
use crate::ResultElement;
use crate::{Deserialize, Serialize};
extern crate scopeguard;

use scopeguard::defer;

pub mod tests {
    extern crate tempdir;

    use tantivy::schema::FieldEntry;
    use tempdir::TempDir;
    use uuid::Uuid;

    use crate::{free_data, ErrorKinds};

    use super::*;
    use serde_json::Map;
    use std::rc::Rc;

    pub static mut GSIZE: usize = 0;

    macro_rules! call_simple_type {
        //() => {};
        ($self:ident, $j_param:ident, $method:literal) => {{
            let v = &$self.call_jpc("builder".to_string(), $method.to_string(), $j_param, true);
            let temp_map: serde_json::Value = match serde_json::from_slice(v) {
                Ok(sv) => sv,
                Err(e) => {
                    info!("return value not json {e}");
                    return -22;
                }
            };
            temp_map["field"].as_i64().unwrap_or(0)
        }};
    }

    #[derive(Debug)]
    pub struct FakeContext {
        pub id: String,
        pub buf: Vec<u8>,
        pub ret_len: usize,
        pub dirs: Vec<TempDir>,
    }
    #[derive(Debug)]
    pub struct TestDocument<'a> {
        pub temp_dir: String,
        pub ctx: Rc<&'a FakeContext>,
    }

    #[derive(Serialize, Deserialize, Clone, Debug, Default)]
    pub struct TestDocResult {
        pub opstamp: u64,
    }
    #[derive(Serialize, Deserialize, Clone, Debug, Default)]
    pub struct TestCreateDocumentResult {
        pub document_count: usize,
    }

    #[derive(Serialize, Deserialize, Clone, Debug, Default)]
    pub struct TestBuilderAddTextResult {
        pub schema: serde_json::Value,
    }
    #[derive(Serialize, Deserialize)]
    pub struct TestTitleResult {
        pub title: Vec<ResultElement>,
    }

    #[allow(clippy::all)]
    pub struct TestIndex<'a> {
        ctx: Rc<&'a FakeContext>,
        temp_dir: String,
    }

    #[allow(clippy::all)]
    pub struct TestIndexReader<'a> {
        ctx: Rc<&'a FakeContext>,
    }

    #[allow(clippy::all)]
    pub struct TestQueryParser<'a> {
        ctx: Rc<&'a FakeContext>,
    }

    #[allow(clippy::all)]
    pub struct TestSearcher<'a> {
        ctx: Rc<&'a FakeContext>,
    }

    #[allow(clippy::all)]
    pub struct TestSchema<'a> {
        ctx: Rc<&'a FakeContext>,
    }

    impl TestSchema<'_> {
        pub fn get_field_entry(&self, name: &str) -> InternalCallResult<FieldEntry> {
            let b = self.ctx.call_jpc(
                "schema".to_string(),
                "get_field_entry".to_string(),
                json!({"field": vec![name]}),
                true,
            );
            let fe: FieldEntry = serde_json::from_slice(&b).unwrap();
            Ok(fe)
        }
        pub fn num_fields(&self) -> InternalCallResult<u64> {
            let b = self.ctx.call_jpc(
                "schema".to_string(),
                "num_fields".to_string(),
                json!({}),
                true,
            );
            let s: u64 = serde_json::from_slice(&b).unwrap();
            Ok(s)
        }
    }

    impl TestSearcher<'_> {
        pub fn get_document(
            &mut self,
            explain: bool,
            score: f32,
            segment_ord: u32,
            doc_id: u32,
            field_id: Vec<String>,
        ) -> InternalCallResult<String> {
            let b = self.ctx.call_jpc(
                "searcher".to_string(),
                "get_document".to_string(),
                json!({ "explain": explain, "score":score, "segment_ord": segment_ord, "doc_id": doc_id, "snippet_field" : field_id}),
                true,
            );
            let s = std::str::from_utf8(&b).unwrap();
            Ok(s.to_string())
        }
        pub fn docset(&mut self, top: u64, score: bool) -> InternalCallResult<String> {
            let b = self.ctx.call_jpc(
                "searcher".to_string(),
                "docset".to_string(),
                json!({ "top_limit": top, "scoring":score }),
                true,
            );
            let s = std::str::from_utf8(&b).unwrap();
            Ok(s.to_string())
        }
        pub fn search(
            &mut self,
            top: u64,
            score: bool,
            snippets: Vec<i64>,
        ) -> InternalCallResult<String> {
            let b = self.ctx.call_jpc(
                "searcher".to_string(),
                "search".to_string(),
                json!({ "top_limit": top, "scoring":score, "snippet_field":snippets }),
                true,
            );
            let s = std::str::from_utf8(&b).unwrap();
            Ok(s.to_string())
        }
        pub fn search_raw(&mut self, limit: u64) -> InternalCallResult<String> {
            let b = self.ctx.call_jpc(
                "searcher".to_string(),
                "search_raw".to_string(),
                json!({ "limit": limit }),
                true,
            );
            let s = std::str::from_utf8(&b).unwrap();
            Ok(s.to_string())
        }
        pub fn fuzzy_search(&mut self, top: u64) -> InternalCallResult<String> {
            let b = self.ctx.call_jpc(
                "fuzzy_searcher".to_string(),
                "fuzzy_searcher".to_string(),
                json!({ "top_limit": top }),
                true,
            );
            let s = std::str::from_utf8(&b).unwrap();
            Ok(s.to_string())
        }
    }

    impl TestQueryParser<'_> {
        pub fn for_index(&mut self, v: Vec<String>) -> InternalCallResult<i32> {
            self.ctx.call_jpc(
                "query_parser".to_string(),
                "for_index".to_string(),
                json!({
                    "fields": v,
                }),
                false,
            );
            Ok(0)
        }
        pub fn parse_query(&mut self, query: String) -> InternalCallResult<TestSearcher> {
            self.ctx.call_jpc(
                "query_parser".to_string(),
                "parse_query".to_string(),
                json!({ "query": query }),
                false,
            );
            Ok(TestSearcher {
                ctx: self.ctx.clone(),
            })
        }
        pub fn parse_fuzzy_query(
            &mut self,
            term: String,
            field: String,
        ) -> InternalCallResult<TestSearcher> {
            self.ctx.call_jpc(
                "query_parser".to_string(),
                "parse_fuzzy_query".to_string(),
                json!({"term": [term], "field" : [field]}),
                false,
            );
            Ok(TestSearcher {
                ctx: self.ctx.clone(),
            })
        }
    }
    impl TestIndexReader<'_> {
        pub fn searcher(&mut self) -> InternalCallResult<TestQueryParser> {
            self.ctx.call_jpc(
                "index_reader".to_string(),
                "searcher".to_string(),
                json!({}),
                false,
            );
            Ok(TestQueryParser {
                ctx: self.ctx.clone(),
            })
        }
    }

    impl TestIndex<'_> {
        pub fn add_document(&mut self, doc_id: i32) -> Result<u64, u32> {
            let _ = self.temp_dir;
            let s = self.ctx.call_jpc(
                "indexwriter".to_string(),
                "add_document".to_string(),
                json!({ "id": doc_id }),
                true,
            );
            let resmap: TestDocResult = serde_json::from_slice(&s).unwrap();
            Ok(resmap.opstamp)
        }

        pub fn commit(&mut self) -> Result<i64, u32> {
            let r = self.ctx.call_jpc(
                "indexwriter".to_string(),
                "commit".to_string(),
                json!({}),
                true,
            );
            let i: Map<String, serde_json::Value> = serde_json::from_slice(&r).unwrap();
            Ok(i["id"].as_i64().unwrap())
        }
        pub fn reader_builder(&mut self) -> InternalCallResult<TestIndexReader> {
            self.ctx.call_jpc(
                "index".to_string(),
                "reader_builder".to_string(),
                json!({}),
                false,
            );
            Ok(TestIndexReader {
                ctx: self.ctx.clone(),
            })
        }

        pub fn schema(&mut self) -> InternalCallResult<TestSchema> {
            Ok(TestSchema {
                ctx: self.ctx.clone(),
            })
        }

        pub fn delete_term<T: serde::Serialize>(&mut self, name: String, term: T) -> i64 {
            self.ctx.call_jpc(
                "indexwriter".to_string(),
                "delete_term".to_string(),
                json!({"field" : name, "term" : term}),
                false,
            );
            0
        }
    }

    impl TestDocument<'_> {
        pub fn create(&mut self) -> Result<usize, i32> {
            let tdc: TestCreateDocumentResult = serde_json::from_slice(&self.ctx.call_jpc(
                "document".to_string(),
                "create".to_string(),
                json!({}),
                true,
            ))
            .unwrap();
            Ok(tdc.document_count)
        }
        pub fn add_text(&mut self, field: i32, value: String, doc_id: u32) -> i64 {
            self.ctx.call_jpc(
                "document".to_string(),
                "add_text".to_string(),
                json!({"field":  field,"value":  value, "id":  self.ctx.id,  "doc_id": doc_id}),
                false,
            );
            0
        }
        pub fn add_int(&mut self, field: i32, value: i64, doc_id: u32) -> i64 {
            self.ctx.call_jpc(
                "document".to_string(),
                "add_int".to_string(),
                json!({"field":  field,"value":  value, "id":  self.ctx.id,  "doc_id": doc_id}),
                false,
            );
            0
        }
        pub fn create_index(&mut self) -> Result<TestIndex, std::io::Error> {
            self.ctx.call_jpc(
                "index".to_string(),
                "create".to_string(),
                json!({"directory":  self.temp_dir, "memsize": 100000000}),
                false,
            );
            Ok(TestIndex {
                ctx: self.ctx.clone(),
                temp_dir: self.temp_dir.clone(),
            })
        }
    }

    impl Default for FakeContext {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Drop for FakeContext {
        fn drop(&mut self) {
            for t in 0..self.dirs.len() {
                let td = &self.dirs[t];
                let _ = std::fs::remove_dir_all(td);
            }
        }
    }

    impl FakeContext {
        pub fn new() -> FakeContext {
            FakeContext {
                id: Uuid::new_v4().to_string(),
                buf: vec![0; 5000000],
                ret_len: 0,
                dirs: <Vec<TempDir>>::default(),
            }
        }

        unsafe fn ptr_to_vec(&self, ptr: *const u8, sz: &usize) -> Vec<u8> {
            let slice = unsafe { std::slice::from_raw_parts(ptr, *sz) };
            slice.to_vec()
        }

        pub fn call_jpc(
            &self,
            object: String,
            method: String,
            params: serde_json::Value,
            do_ret: bool,
        ) -> Vec<u8> {
            let my_ret_ptr = &mut usize::default();
            let mut p: *const u8 = std::ptr::null_mut();
            let call_p = json!({
                "id":     self.id,
                "jpc":    "1.0",
                "obj":    object,
                "method": method,
                "params": params,
            });
            let mut sp = serde_json::to_vec(&call_p).unwrap_or_default();
            info!("calling tantivy-jpc json = {}", call_p);
            let iret: i64;
            unsafe {
                iret = tantivy_jpc(sp.as_mut_ptr(), sp.len(), &mut p, my_ret_ptr);
            }
            let sl = unsafe { self.ptr_to_vec(p, my_ret_ptr) };
            defer! {
                unsafe{free_data(iret);}
            }
            match std::str::from_utf8(&sl) {
                Ok(s) => println!("stringified = {}", s),
                Err(err) => {
                    println!("ERROR = {err} sl = {sl:?}")
                }
            };
            if do_ret {
                let v: serde_json::Value =
                    serde_json::from_slice(&sl).unwrap_or(json!({"result" : "empty"}));
                info!("Val = {}", v);
                match std::str::from_utf8(&sl) {
                    Ok(s) => info!("stringified = {}", s),
                    Err(err) => panic!("ERROR = {err} p = {sl:?}"),
                };
                sl
            } else {
                vec![]
            }
        }
        pub fn add_text_field(
            &mut self,
            name: String,
            a_type: i32,
            stored: bool,
            indexed: bool,
            tokenizer: String,
            basic: bool,
        ) -> i64 {
            let j_param = json!({
                "name":   name,
                "type":   a_type,
                "stored": stored,
                "indexed" : indexed,
                "id":     self.id,
                "tokenizer" : tokenizer,
                "basic" : basic,
            });
            let s = &self.call_jpc(
                "builder".to_string(),
                "add_text_field".to_string(),
                j_param,
                true,
            );
            info!("builder ret  = {:?}", s);
            let i: serde_json::Value = serde_json::from_slice(s).unwrap();
            i["field"].as_i64().unwrap()
        }

        pub fn add_date_field(
            &mut self,
            name: String,
            a_type: i32,
            stored: bool,
            indexed: bool,
        ) -> i64 {
            let j_param = json!({
                "name":   name,
                "type":   a_type,
                "stored": stored,
                "indexed" : indexed,
                "id":     self.id,
            });
            call_simple_type!(self, j_param, "add_date_field")
        }
        pub fn add_i64_field(
            &mut self,
            name: String,
            a_type: i32,
            stored: bool,
            indexed: bool,
        ) -> i64 {
            let j_param = json!({
                "name":   name,
                "type":   a_type,
                "stored": stored,
                "indexed" : indexed,
                "id":     self.id,
            });
            call_simple_type!(self, j_param, "add_i64_field")
        }
        pub fn add_u64_field(
            &mut self,
            name: String,
            a_type: i32,
            stored: bool,
            indexed: bool,
        ) -> i64 {
            let j_param = json!({
                "name":   name,
                "type":   a_type,
                "stored": stored,
                "indexed" : indexed,
                "id":     self.id,
            });
            call_simple_type!(self, j_param, "add_u64_field")
        }
        pub fn add_f64_field(
            &mut self,
            name: String,
            a_type: i32,
            stored: bool,
            indexed: bool,
        ) -> i64 {
            let j_param = json!({
                "name":   name,
                "type":   a_type,
                "stored": stored,
                "indexed" : indexed,
                "id":     self.id,
            });
            call_simple_type!(self, j_param, "add_f64_field")
        }

        pub fn build(&mut self, in_memory: bool) -> InternalCallResult<TestDocument> {
            if in_memory {
                let _s =
                    self.call_jpc("builder".to_string(), "build".to_string(), json!({}), false);
                return Ok(TestDocument {
                    ctx: Rc::new(self),
                    temp_dir: "".to_string(),
                });
            }
            let td = TempDir::new("TantivyBitcodeTest")?;
            self.dirs.append(&mut vec![td]);
            let td_ref = self.dirs.last().unwrap();
            let s = self.call_jpc("builder".to_string(), "build".to_string(), json!({}), false);
            info!("build returned={:?}", s);
            let tdir = td_ref
                .path()
                .to_str()
                .to_owned()
                .ok_or(ErrorKinds::NotExist("temp path not available".to_string()))?;
            Ok(TestDocument {
                ctx: Rc::new(self),
                temp_dir: tdir.to_string(),
            })
        }
    }

    #[test]
    fn basic_index() {
        crate::test_init();
        let mut ctx = FakeContext::new();
        assert_eq!(
            ctx.add_text_field(
                "title".to_string(),
                2,
                true,
                true,
                "en_stem_with_stop_words".to_string(),
                false
            ),
            0
        );
        assert_eq!(
            ctx.add_text_field(
                "body".to_string(),
                2,
                true,
                true,
                "en_stem_with_stop_words".to_string(),
                false
            ),
            1
        );
        let mut td = match ctx.build(true) {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("failed with error {}", e.to_string()));
            }
        };
        let doc1 = match td.create() {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("doc1 create failed error {}", e.to_string()));
            }
        };

        let doc2 = match td.create() {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("doc2 create failed error {}", e.to_string()));
            }
        };
        assert_eq!(
            td.add_text(0, "The Old Man and the Sea".to_string(), doc1 as u32),
            0
        );
        assert_eq!(td.add_text(1, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish.".to_string(), doc1 as u32), 0);
        assert_eq!(
            td.add_text(0, "Of Mice and Men".to_string(), doc2 as u32),
            0
        );
        assert_eq!(td.add_text(1, r#"A few miles south of Soledad, the Salinas River drops in close to the hillside bank and runs deep and green. The water is warm too, for it has slipped twinkling over the yellow sands in the sunlight before reaching the narrow pool. On one side of the river the golden foothill slopes curve up to the strong and rocky Gabilan Mountains, but on the valley side the water is lined with trees—willows fresh and green with every spring, carrying in their lower leaf junctures the debris of the winter's flooding; and sycamores with mottled, white, recumbent limbs and branches that arch over the pool"#.to_string(), doc2 as u32), 0);
        let mut ti = match td.create_index() {
            Ok(i) => i,
            Err(e) => panic!("failed to create index err ={} ", e),
        };
        let op1 = ti.add_document(doc1 as i32).unwrap();
        let op2 = ti.add_document(doc2 as i32).unwrap();
        assert_eq!(op1, 0);
        assert_eq!(op2, 1);
        ti.commit().unwrap();
        let mut rb = ti.reader_builder().unwrap();
        let mut qp = rb.searcher().unwrap();
        qp.for_index(vec!["title".to_string()]).unwrap();
        let mut searcher = qp.parse_query("Sea".to_string()).unwrap();
        let sres = &searcher.search(1, true, vec![]).unwrap();
        let title_result: Vec<ResultElement> = serde_json::from_str(sres).unwrap();
        assert_eq!(
            title_result[0].doc.0.get("title").unwrap()[0]
                .as_text()
                .unwrap(),
            "The Old Man and the Sea".to_string()
        );
        match crate::do_term(&ti.ctx.id) {
            Ok(o) => o,
            Err(e) => panic!("exception = {e}"),
        };
    }

    #[test]
    fn basic_index_snippet() {
        crate::test_init();
        let mut ctx = FakeContext::new();
        assert_eq!(
            ctx.add_text_field(
                "title".to_string(),
                2,
                true,
                true,
                "en_stem_with_stop_words".to_string(),
                false
            ),
            0
        );
        assert_eq!(
            ctx.add_text_field(
                "body".to_string(),
                2,
                true,
                true,
                "en_stem_with_stop_words".to_string(),
                false
            ),
            1
        );
        let mut td = match ctx.build(true) {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("failed with error {}", e.to_string()));
            }
        };
        let doc1 = match td.create() {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("doc1 create failed error {}", e.to_string()));
            }
        };

        let doc2 = match td.create() {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("doc2 create failed error {}", e.to_string()));
            }
        };
        assert_eq!(
            td.add_text(0, "The Old Man and the Sea".to_string(), doc1 as u32),
            0
        );
        assert_eq!(td.add_text(1, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish.".to_string(), doc1 as u32), 0);
        assert_eq!(
            td.add_text(0, "Of Mice and Men".to_string(), doc2 as u32),
            0
        );
        assert_eq!(td.add_text(1, r#"A few miles south of Soledad, the Salinas River drops in close to the hillside bank and runs deep and green. The water is warm too, for it has slipped twinkling over the yellow sands in the sunlight before reaching the narrow pool. On one side of the river the golden foothill slopes curve up to the strong and rocky Gabilan Mountains, but on the valley side the water is lined with trees—willows fresh and green with every spring, carrying in their lower leaf junctures the debris of the winter's flooding; and sycamores with mottled, white, recumbent limbs and branches that arch over the pool"#.to_string(), doc2 as u32), 0);
        let mut ti = match td.create_index() {
            Ok(i) => i,
            Err(e) => panic!("failed to create index err ={} ", e),
        };
        let op1 = ti.add_document(doc1 as i32).unwrap();
        let op2 = ti.add_document(doc2 as i32).unwrap();
        assert_eq!(op1, 0);
        assert_eq!(op2, 1);
        ti.commit().unwrap();
        let mut rb = ti.reader_builder().unwrap();
        let mut qp = rb.searcher().unwrap();
        qp.for_index(vec!["title".to_string(), "body".to_string()])
            .unwrap();
        let mut searcher = qp.parse_query("sycamores".to_string()).unwrap();
        let sres = &searcher.search(10, true, vec![]).unwrap();
        let title_result: Vec<ResultElement> = serde_json::from_str(sres).unwrap();
        assert_eq!(
            title_result[0].doc.0.get("title").unwrap()[0]
                .as_text()
                .unwrap(),
            "Of Mice and Men".to_string()
        );
        match crate::do_term(&ti.ctx.id) {
            Ok(o) => o,
            Err(e) => panic!("exception = {e}"),
        };
    }

    #[test]
    fn test_docset() {
        crate::test_init();
        let mut ctx = FakeContext::new();
        assert_eq!(
            ctx.add_text_field(
                "title".to_string(),
                2,
                true,
                true,
                "en_stem_with_stop_words".to_string(),
                false
            ),
            0
        );
        assert_eq!(
            ctx.add_text_field(
                "body".to_string(),
                2,
                true,
                true,
                "en_stem_with_stop_words".to_string(),
                false
            ),
            1
        );
        let mut td = match ctx.build(true) {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("failed with error {}", e.to_string()));
            }
        };
        let doc1 = match td.create() {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("doc1 create failed error {}", e.to_string()));
            }
        };

        let doc2 = match td.create() {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("doc2 create failed error {}", e.to_string()));
            }
        };
        assert_eq!(
            td.add_text(0, "The Old Man and the Sea".to_string(), doc1 as u32),
            0
        );
        assert_eq!(td.add_text(1, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish.".to_string(), doc1 as u32), 0);
        assert_eq!(
            td.add_text(0, "Of Mice and Men".to_string(), doc2 as u32),
            0
        );
        assert_eq!(td.add_text(1, r#"A few miles south of Soledad, the Salinas River drops in close to the hillside bank and runs deep and green. The water is warm too, for it has slipped twinkling over the yellow sands in the sunlight before reaching the narrow pool. On one side of the river the golden foothill slopes curve up to the strong and rocky Gabilan Mountains, but on the valley side the water is lined with trees—willows fresh and green with every spring, carrying in their lower leaf junctures the debris of the winter's flooding; and sycamores with mottled, white, recumbent limbs and branches that arch over the pool"#.to_string(), doc2 as u32), 0);
        let mut ti = match td.create_index() {
            Ok(i) => i,
            Err(e) => panic!("failed to create index err ={} ", e),
        };
        let op1 = ti.add_document(doc1 as i32).unwrap();
        let op2 = ti.add_document(doc2 as i32).unwrap();
        assert_eq!(op1, 0);
        assert_eq!(op2, 1);
        ti.commit().unwrap();
        let mut rb = ti.reader_builder().unwrap();
        let mut qp = rb.searcher().unwrap();
        qp.for_index(vec!["title".to_string()]).unwrap();
        let mut searcher = qp
            .parse_query("title:Sea OR title:Men".to_string())
            .unwrap();
        let sres: serde_json::Value =
            serde_json::from_str(&searcher.docset(4, true).unwrap()).unwrap();

        info!("RESULT={}", sres);

        let vals = sres
            .as_object()
            .unwrap()
            .get("docset")
            .unwrap()
            .as_array()
            .unwrap();
        for v in vals {
            let doc_id = v.get("doc_id").unwrap().as_u64().unwrap() as u32;
            let score = v.get("score").unwrap().as_f64().unwrap() as f32;
            let segment_ord = v.get("segment_ord").unwrap().as_u64().unwrap() as u32;
            let res = searcher
                .get_document(true, score, segment_ord, doc_id, vec!["".to_string()])
                .unwrap();
            let _re: ResultElement = serde_json::from_str(&res).unwrap();
            info!("Result= {res}");
        }

        match crate::do_term(&ti.ctx.id) {
            Ok(o) => o,
            Err(e) => panic!("exception = {e}"),
        };
    }

    #[test]
    fn test_schema() {
        crate::test_init();
        let mut ctx = FakeContext::new();
        assert_eq!(
            ctx.add_text_field(
                "title".to_string(),
                2,
                true,
                true,
                "en_stem_with_stop_words".to_string(),
                true
            ),
            0
        );
        assert_eq!(
            ctx.add_text_field(
                "body".to_string(),
                2,
                true,
                true,
                "en_stem_with_stop_words".to_string(),
                true
            ),
            1
        );
        assert_eq!(
            ctx.add_text_field(
                "body2".to_string(),
                2,
                true,
                true,
                "en_stem_with_stop_words".to_string(),
                true
            ),
            2
        );
        let mut td = match ctx.build(true) {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("failed with error {}", e.to_string()));
            }
        };
        let doc1 = match td.create() {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("doc1 create failed error {}", e.to_string()));
            }
        };

        let doc2 = match td.create() {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("doc2 create failed error {}", e.to_string()));
            }
        };
        assert_eq!(
            td.add_text(0, "The Old Man and the Sea".to_string(), doc1 as u32),
            0
        );
        assert_eq!(td.add_text(1, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish.".to_string(), doc1 as u32), 0);
        assert_eq!(td.add_text(2, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish.".to_string(), doc1 as u32), 0);
        assert_eq!(
            td.add_text(0, "Of Mice and Men".to_string(), doc2 as u32),
            0
        );
        assert_eq!(td.add_text(1, r#"A few miles south of Soledad, the Salinas River drops in close to the hillside bank and runs deep and green. The water is warm too, for it has slipped twinkling over the yellow sands in the sunlight before reaching the narrow pool. On one side of the river the golden foothill slopes curve up to the strong and rocky Gabilan Mountains, but on the valley side the water is lined with trees—willows fresh and green with every spring, carrying in their lower leaf junctures the debris of the winter's flooding; and sycamores with mottled, white, recumbent limbs and branches that arch over the pool"#.to_string(), doc2 as u32), 0);
        assert_eq!(td.add_text(2, r#"A few miles south of Soledad, the Salinas River drops in close to the hillside bank and runs deep and green. The water is warm too, for it has slipped twinkling over the yellow sands in the sunlight before reaching the narrow pool. On one side of the river the golden foothill slopes curve up to the strong and rocky Gabilan Mountains, but on the valley side the water is lined with trees—willows fresh and green with every spring, carrying in their lower leaf junctures the debris of the winter's flooding; and sycamores with mottled, white, recumbent limbs and branches that arch over the pool"#.to_string(), doc2 as u32), 0);
        let mut ti = match td.create_index() {
            Ok(i) => i,
            Err(e) => panic!("failed to create index err ={} ", e),
        };
        let op1 = ti.add_document(doc1 as i32).unwrap();
        let op2 = ti.add_document(doc2 as i32).unwrap();
        assert_eq!(op1, 0);
        assert_eq!(op2, 1);
        ti.commit().unwrap();
        let sc = ti.schema().unwrap();
        let n = sc.num_fields().unwrap();
        assert_eq!(n, 3);
        let d = sc.get_field_entry("body").unwrap();
        assert_eq!(d.name(), "body");
    }

    #[test]
    fn test_docset_snippet() {
        crate::test_init();
        let mut ctx = FakeContext::new();
        assert_eq!(
            ctx.add_text_field(
                "title".to_string(),
                2,
                true,
                true,
                "en_stem_with_stop_words".to_string(),
                false
            ),
            0
        );
        assert_eq!(
            ctx.add_text_field(
                "body".to_string(),
                2,
                true,
                true,
                "en_stem_with_stop_words".to_string(),
                false
            ),
            1
        );
        assert_eq!(
            ctx.add_text_field(
                "body2".to_string(),
                2,
                true,
                true,
                "en_stem_with_stop_words".to_string(),
                false
            ),
            2
        );
        let mut td = match ctx.build(true) {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("failed with error {}", e.to_string()));
            }
        };
        let doc1 = match td.create() {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("doc1 create failed error {}", e.to_string()));
            }
        };

        let doc2 = match td.create() {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("doc2 create failed error {}", e.to_string()));
            }
        };
        assert_eq!(
            td.add_text(0, "The Old Man and the Sea".to_string(), doc1 as u32),
            0
        );
        assert_eq!(td.add_text(1, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish.".to_string(), doc1 as u32), 0);
        assert_eq!(td.add_text(2, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish.".to_string(), doc1 as u32), 0);
        assert_eq!(
            td.add_text(0, "Of Mice and Men".to_string(), doc2 as u32),
            0
        );
        assert_eq!(td.add_text(1, r#"A few miles south of Soledad, the Salinas River drops in close to the hillside bank and runs deep and green. The water is warm too, for it has slipped twinkling over the yellow sands in the sunlight before reaching the narrow pool. On one side of the river the golden foothill slopes curve up to the strong and rocky Gabilan Mountains, but on the valley side the water is lined with trees—willows fresh and green with every spring, carrying in their lower leaf junctures the debris of the winter's flooding; and sycamores with mottled, white, recumbent limbs and branches that arch over the pool"#.to_string(), doc2 as u32), 0);
        assert_eq!(td.add_text(2, r#"A few miles south of Soledad, the Salinas River drops in close to the hillside bank and runs deep and green. The water is warm too, for it has slipped twinkling over the yellow sands in the sunlight before reaching the narrow pool. On one side of the river the golden foothill slopes curve up to the strong and rocky Gabilan Mountains, but on the valley side the water is lined with trees—willows fresh and green with every spring, carrying in their lower leaf junctures the debris of the winter's flooding; and sycamores with mottled, white, recumbent limbs and branches that arch over the pool"#.to_string(), doc2 as u32), 0);
        let mut ti = match td.create_index() {
            Ok(i) => i,
            Err(e) => panic!("failed to create index err ={} ", e),
        };
        let op1 = ti.add_document(doc1 as i32).unwrap();
        let op2 = ti.add_document(doc2 as i32).unwrap();
        assert_eq!(op1, 0);
        assert_eq!(op2, 1);
        ti.commit().unwrap();
        let mut rb = ti.reader_builder().unwrap();
        let mut qp = rb.searcher().unwrap();
        qp.for_index(vec![
            "title".to_string(),
            "body".to_string(),
            "body2".to_string(),
        ])
        .unwrap();
        let mut searcher = qp.parse_query("twinkling".to_string()).unwrap();
        let sres: serde_json::Value =
            serde_json::from_str(&searcher.docset(4, true).unwrap()).unwrap();

        info!("RESULT={}", sres);

        let vals = sres
            .as_object()
            .unwrap()
            .get("docset")
            .unwrap()
            .as_array()
            .unwrap();
        for v in vals {
            let doc_id = v.get("doc_id").unwrap().as_u64().unwrap() as u32;
            let score = v.get("score").unwrap().as_f64().unwrap() as f32;
            let segment_ord = v.get("segment_ord").unwrap().as_u64().unwrap() as u32;
            let res = searcher
                .get_document(
                    true,
                    score,
                    segment_ord,
                    doc_id,
                    vec!["body".to_string(), "body2".to_string()],
                )
                .unwrap();
            let re: ResultElement = serde_json::from_str(&res).unwrap();
            assert!(re.snippet_html != None);
            let mut hm = crate::HashMap::<String, String>::new();
            hm.insert("body2".to_string(), "A few miles south of Soledad, the Salinas River drops in close to the hillside bank and runs deep and green. The water is warm too, for it has slipped <b>twinkling</b> over the yellow sands in the sunlight before reaching the narrow pool. On one side of the river the golden foothill slopes curve up to the strong and rocky Gabilan Mountains, but on the valley side the water is lined with trees—willows fresh and green with every spring, carrying in their lower leaf junctures the debris of the winter&#x27;s flooding; and sycamores with mottled, white, recumbent limbs and branches that arch over the pool".to_string());
            hm.insert("body".to_string(), "A few miles south of Soledad, the Salinas River drops in close to the hillside bank and runs deep and green. The water is warm too, for it has slipped <b>twinkling</b> over the yellow sands in the sunlight before reaching the narrow pool. On one side of the river the golden foothill slopes curve up to the strong and rocky Gabilan Mountains, but on the valley side the water is lined with trees—willows fresh and green with every spring, carrying in their lower leaf junctures the debris of the winter&#x27;s flooding; and sycamores with mottled, white, recumbent limbs and branches that arch over the pool".to_string());
            assert_eq!(re.snippet_html, Some(hm));
            info!("Result= {res}");
        }

        match crate::do_term(&ti.ctx.id) {
            Ok(o) => o,
            Err(e) => panic!("exception = {e}"),
        };
    }

    #[test]
    fn test_raw_search() {
        crate::test_init();
        let mut ctx = FakeContext::new();
        assert_eq!(
            ctx.add_text_field(
                "title".to_string(),
                2,
                true,
                true,
                "en_stem_with_stop_words".to_string(),
                false
            ),
            0
        );
        assert_eq!(
            ctx.add_text_field(
                "body".to_string(),
                2,
                true,
                true,
                "en_stem_with_stop_words".to_string(),
                false
            ),
            1
        );
        let mut td = match ctx.build(true) {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("failed with error {}", e.to_string()));
            }
        };
        let doc1 = match td.create() {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("doc1 create failed error {}", e.to_string()));
            }
        };

        let doc2 = match td.create() {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("doc2 create failed error {}", e.to_string()));
            }
        };
        assert_eq!(
            td.add_text(0, "The Old Man and the Sea".to_string(), doc1 as u32),
            0
        );
        assert_eq!(td.add_text(1, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish.".to_string(), doc1 as u32), 0);
        assert_eq!(
            td.add_text(0, "Of Mice and Men".to_string(), doc2 as u32),
            0
        );
        assert_eq!(td.add_text(1, r#"A few miles south of Soledad, the Salinas River drops in close to the hillside bank and runs deep and green. The water is warm too, for it has slipped twinkling over the yellow sands in the sunlight before reaching the narrow pool. On one side of the river the golden foothill slopes curve up to the strong and rocky Gabilan Mountains, but on the valley side the water is lined with trees—willows fresh and green with every spring, carrying in their lower leaf junctures the debris of the winter's flooding; and sycamores with mottled, white, recumbent limbs and branches that arch over the pool"#.to_string(), doc2 as u32), 0);
        let mut ti = match td.create_index() {
            Ok(i) => i,
            Err(e) => panic!("failed to create index err ={} ", e),
        };
        let op1 = ti.add_document(doc1 as i32).unwrap();
        let op2 = ti.add_document(doc2 as i32).unwrap();
        assert_eq!(op1, 0);
        assert_eq!(op2, 1);
        ti.commit().unwrap();
        let mut rb = ti.reader_builder().unwrap();
        let mut qp = rb.searcher().unwrap();
        qp.for_index(vec!["title".to_string(), "body".to_string()])
            .unwrap();
        let mut searcher = qp
            .parse_query("title:Sea OR title:Mice".to_string())
            .unwrap();
        let rs = searcher.search_raw(0).unwrap();
        let val: Vec<crate::HashMap<String, serde_json::Value>> =
            serde_json::from_slice(rs.as_bytes()).unwrap();
        //let expected = r#"{"body":["He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish."],"title":["The Old Man and the Sea"]}\n{"body":["A few miles south of Soledad, the Salinas River drops in close to the hillside bank and runs deep and green. The water is warm too, for it has slipped twinkling over the yellow sands in the sunlight before reaching the narrow pool. On one side of the river the golden foothill slopes curve up to the strong and rocky Gabilan Mountains, but on the valley side the water is lined with trees—willows fresh and green with every spring, carrying in their lower leaf junctures the debris of the winter's flooding; and sycamores with mottled, white, recumbent limbs and branches that arch over the pool"],"title":["Of Mice and Men"]}\n"#;
        //let formatted_string = expected.replace("\\n", "\n");
        //assert_eq!(rs, formatted_string);
        assert_eq!(2, val.len());
        match crate::do_term(&ti.ctx.id) {
            Ok(o) => o,
            Err(e) => panic!("exception = {e}"),
        };
    }

    #[test]
    fn test_all_fields() {
        crate::test_init();
        let mut ctx = FakeContext::new();
        assert_eq!(
            ctx.add_text_field(
                "title".to_string(),
                2,
                true,
                true,
                "en_stem_with_stop_words".to_string(),
                false
            ),
            0
        );
        assert_eq!(
            ctx.add_text_field(
                "body".to_string(),
                2,
                true,
                true,
                "en_stem_with_stop_words".to_string(),
                false
            ),
            1
        );
        assert_eq!(ctx.add_i64_field("order".to_string(), 3, true, true), 2);

        let mut td = match ctx.build(true) {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("failed with error {}", e.to_string()));
            }
        };
        let doc1 = match td.create() {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("doc1 create failed error {}", e.to_string()));
            }
        };

        let doc2 = match td.create() {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("doc2 create failed error {}", e.to_string()));
            }
        };
        assert_eq!(
            td.add_text(0, "The Old Man and the Sea".to_string(), doc1 as u32),
            0
        );
        assert_eq!(td.add_text(1, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish.".to_string(), doc1 as u32), 0);
        assert_eq!(td.add_int(2, 111, doc1 as u32), 0);

        assert_eq!(
            td.add_text(0, "Of Mice and Men".to_string(), doc2 as u32),
            0
        );
        assert_eq!(td.add_text(1, r#"A few miles south of Soledad, the Salinas River drops in close to the hillside bank and runs deep and green. The water is warm too, for it has slipped twinkling over the yellow sands in the sunlight before reaching the narrow pool. On one side of the river the golden foothill slopes curve up to the strong and rocky Gabilan Mountains, but on the valley side the water is lined with trees—willows fresh and green with every spring, carrying in their lower leaf junctures the debris of the winter's flooding; and sycamores with mottled, white, recumbent limbs and branches that arch over the pool"#.to_string(), doc2 as u32), 0);
        assert_eq!(td.add_int(2, 222, doc2 as u32), 0);
        let mut ti = match td.create_index() {
            Ok(i) => i,
            Err(e) => panic!("failed to create index err ={} ", e),
        };
        let op1 = ti.add_document(doc1 as i32).unwrap();
        let op2 = ti.add_document(doc2 as i32).unwrap();
        assert_eq!(op1, 0);
        assert!(op2 >= 1);
        ti.commit().unwrap();
        let mut rb = ti.reader_builder().unwrap();
        let mut qp = rb.searcher().unwrap();
        qp.for_index(vec!["title".to_string(), "body".to_string()])
            .unwrap();
        let mut searcher = qp.parse_query("order:111".to_string()).unwrap();
        let sres = &searcher.search(1, false, vec![]).unwrap();
        let title_result: Vec<ResultElement> = serde_json::from_str(sres).unwrap();
        assert_eq!(
            title_result[0].doc.0.get("title").unwrap()[0]
                .as_text()
                .unwrap(),
            "The Old Man and the Sea".to_string()
        );

        let _ = crate::do_term(&ti.ctx.id);
    }

    #[test]
    fn top_limit() {
        crate::test_init();
        let mut ctx = FakeContext::new();
        assert_eq!(
            ctx.add_text_field(
                "title".to_string(),
                2,
                true,
                true,
                "en_stem_with_stop_words".to_string(),
                false
            ),
            0
        );
        assert_eq!(
            ctx.add_text_field(
                "body".to_string(),
                2,
                true,
                true,
                "en_stem_with_stop_words".to_string(),
                false
            ),
            1
        );
        let mut td = match ctx.build(true) {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("failed with error {}", e.to_string()));
            }
        };
        let doc1 = match td.create() {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("doc1 create failed error {}", e.to_string()));
            }
        };

        let doc2 = match td.create() {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("doc2 create failed error {}", e.to_string()));
            }
        };
        assert_eq!(
            td.add_text(0, "The Old Man and the Sea".to_string(), doc1 as u32),
            0
        );
        assert_eq!(td.add_text(1, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish.".to_string(), doc1 as u32), 0);
        assert_eq!(
            td.add_text(0, "Of Mice and Man".to_string(), doc2 as u32),
            0
        );
        assert_eq!(td.add_text(1, r#"A few miles south of Soledad, the Salinas River drops in close to the hillside bank and runs deep and green. The water is warm too, for it has slipped twinkling over the yellow sands in the sunlight before reaching the narrow pool. On one side of the river the golden foothill slopes curve up to the strong and rocky Gabilan Mountains, but on the valley side the water is lined with trees—willows fresh and green with every spring, carrying in their lower leaf junctures the debris of the winter's flooding; and sycamores with mottled, white, recumbent limbs and branches that arch over the pool"#.to_string(), doc2 as u32), 0);
        let mut ti = match td.create_index() {
            Ok(i) => i,
            Err(e) => panic!("failed to create index err ={} ", e),
        };
        let op1 = ti.add_document(doc1 as i32).unwrap();
        let op2 = ti.add_document(doc2 as i32).unwrap();
        assert_eq!(op1, 0);
        assert_eq!(op2, 1);
        ti.commit().unwrap();
        let mut rb = ti.reader_builder().unwrap();
        let mut qp = rb.searcher().unwrap();
        qp.for_index(vec!["title".to_string()]).unwrap();
        let mut top_searcher = qp.parse_query("Man".to_string()).unwrap();
        let sres = &top_searcher.search(1, true, vec![]).unwrap();
        let title_result: Vec<ResultElement> = serde_json::from_str(sres).unwrap();
        assert_eq!(1, title_result.len());
        let sres = &top_searcher.search(2, true, vec![]).unwrap();
        let title_result: Vec<ResultElement> = serde_json::from_str(sres).unwrap();
        assert_eq!(2, title_result.len());
        let _ = crate::do_term(&ti.ctx.id);
    }

    #[test]
    fn basic_index_fuzzy() {
        crate::test_init();
        let mut ctx = FakeContext::new();
        assert_eq!(
            ctx.add_text_field(
                "title".to_string(),
                2,
                true,
                true,
                "en_stem_with_stop_words".to_string(),
                false
            ),
            0
        );
        let mut td = match ctx.build(true) {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("failed with error {}", e.to_string()));
            }
        };
        let doc1 = match td.create() {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("doc1 create failed error {}", e.to_string()));
            }
        };

        let doc2 = match td.create() {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("doc2 create failed error {}", e.to_string()));
            }
        };

        let doc3 = match td.create() {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("doc3 create failed error {}", e.to_string()));
            }
        };

        let doc4 = match td.create() {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("doc4 create failed error {}", e.to_string()));
            }
        };
        assert_eq!(
            td.add_text(0, "The Name of the Wind".to_string(), doc1 as u32),
            0
        );
        assert_eq!(
            td.add_text(0, "The Diary of Muadib".to_string(), doc2 as u32),
            0
        );
        assert_eq!(td.add_text(0, "A Dairy Cow".to_string(), doc3 as u32), 0);
        assert_eq!(
            td.add_text(0, "The Diary of a Young Girl".to_string(), doc4 as u32),
            0
        );
        let mut ti = match td.create_index() {
            Ok(i) => i,
            Err(e) => panic!("failed to create index err ={} ", e),
        };
        let _op1 = ti.add_document(doc1 as i32).unwrap();
        let op2 = ti.add_document(doc2 as i32).unwrap();
        let op3 = ti.add_document(doc3 as i32).unwrap();
        let op4 = ti.add_document(doc4 as i32).unwrap();
        assert!(op2 >= 1);
        assert!(op3 >= 2);
        assert!(op4 >= 3);
        ti.commit().unwrap();
        let mut rb = ti.reader_builder().unwrap();
        let mut qp = rb.searcher().unwrap();
        let mut searcher = qp
            .parse_fuzzy_query("diari".to_string(), "title".to_string())
            .unwrap();
        let sres = &searcher.fuzzy_search(2).unwrap();
        let vret: Vec<serde_json::Value> = serde_json::from_str(sres).unwrap();
        assert_eq!(vret.len(), 2);
        let _ = crate::do_term(&ti.ctx.id);
    }

    #[test]
    fn test_delete_term() {
        crate::test_init();
        let mut ctx = FakeContext::new();
        assert_eq!(
            ctx.add_text_field(
                "title".to_string(),
                2,
                true,
                true,
                "en_stem_with_stop_words".to_string(),
                false
            ),
            0
        );
        assert_eq!(
            ctx.add_text_field(
                "body".to_string(),
                2,
                true,
                true,
                "en_stem_with_stop_words".to_string(),
                false
            ),
            1
        );
        assert_eq!(ctx.add_i64_field("order".to_string(), 3, false, true), 2);

        let mut td = match ctx.build(true) {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("failed with error {}", e.to_string()));
            }
        };
        let doc1 = match td.create() {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("doc1 create failed error {}", e.to_string()));
            }
        };
        assert_eq!(
            td.add_text(0, "The Old Man and the Sea".to_string(), doc1 as u32),
            0
        );
        assert_eq!(td.add_text(1, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish.".to_string(), doc1 as u32), 0);
        assert_eq!(td.add_int(2, 232, doc1 as u32), 0);
        let mut ti = match td.create_index() {
            Ok(i) => i,
            Err(e) => panic!("failed to create index err ={} ", e),
        };
        ti.delete_term("order".to_string(), 232);
        let _ = crate::do_term(&ti.ctx.id);
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub struct TestResultError {
        pub error: String,
        pub id: String,
        pub jpc: String,
    }

    #[test]
    fn bad_params_call_jpc() {
        fn base_tester(ctx: &FakeContext, object: String, method: String, expected: &str) {
            let s = &ctx.call_jpc(object, method, json!({}), true);
            let r: TestResultError = serde_json::from_str(std::str::from_utf8(s).unwrap()).unwrap();
            assert_eq!(r.error, expected);
        }

        crate::test_init();
        let ctx = FakeContext::new();
        base_tester(
            &ctx,
            "nothing".to_string(),
            "some_function".to_string(),
            "Not Recognized : `some_function`",
        );
        base_tester(
            &ctx,
            "builder".to_string(),
            "some_function".to_string(),
            "handle builder error=BadParams : `Unknown method some_function`",
        );
        base_tester(
            &ctx,
            "query_parser".to_string(),
            "some_function".to_string(),
            "handle query parser error=BadParams : `Unknown method some_function`",
        );
        base_tester(
            &ctx,
            "index".to_string(),
            "some_function".to_string(),
            "handle index error=BadParams : `Finalized : `A schema must be created before an index``",
        );
        base_tester(
            &ctx,
            "searcher".to_string(),
            "some_function".to_string(),
            "handle searcher error=NotExist : `unknown method some_function`",
        );
        base_tester(
            &ctx,
            "fuzzy_searcher".to_string(),
            "some_function".to_string(),
            "handle searcher error=NotExist : `expecting method fuzzy_searcher found some_function`",
        );
    }

    #[test]
    fn all_simple_fields() {
        crate::test_init();
        let mut ctx = FakeContext::new();
        assert_eq!(
            ctx.add_text_field(
                "title".to_string(),
                2,
                true,
                true,
                "en_stem_with_stop_words".to_string(),
                false
            ),
            0
        );
        assert_eq!(
            ctx.add_text_field(
                "body".to_string(),
                2,
                true,
                true,
                "en_stem_with_stop_words".to_string(),
                false
            ),
            1
        );
        assert_eq!(ctx.add_date_field("date".to_string(), 2, true, true), 2);
        assert_eq!(ctx.add_u64_field("someu64".to_string(), 2, true, true), 3);
        assert_eq!(ctx.add_i64_field("somei64".to_string(), 2, true, true), 4);
        assert_eq!(ctx.add_f64_field("somef64".to_string(), 2, true, true), 5);
    }

    #[test]
    fn test_camelcase() {
        crate::test_init();
        let mut ctx = FakeContext::new();
        assert_eq!(
            ctx.add_text_field(
                "title".to_string(),
                2,
                true,
                true,
                "filename".to_string(),
                false
            ),
            0
        );
        assert_eq!(
            ctx.add_text_field(
                "body".to_string(),
                2,
                true,
                true,
                "filename".to_string(),
                false
            ),
            1
        );
        let mut td = match ctx.build(true) {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("failed with error {}", e.to_string()));
            }
        };
        let doc1 = match td.create() {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("doc1 create failed error {}", e.to_string()));
            }
        };

        let doc2 = match td.create() {
            Ok(t) => t,
            Err(e) => {
                panic!("{}", format!("doc2 create failed error {}", e.to_string()));
            }
        };
        assert_eq!(
            td.add_text(0, "abc Hello1989World test".to_string(), doc1 as u32),
            0
        );
        assert_eq!(td.add_text(1, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish.".to_string(), doc1 as u32), 0);
        assert_eq!(
            td.add_text(0, "Of Mice and Men".to_string(), doc2 as u32),
            0
        );
        assert_eq!(td.add_text(1, r#"A few miles south of Soledad, the Salinas River drops in close to the hillside bank and runs deep and green. The water is warm too, for it has slipped twinkling over the yellow sands in the sunlight before reaching the narrow pool. On one side of the river the golden foothill slopes curve up to the strong and rocky Gabilan Mountains, but on the valley side the water is lined with trees—willows fresh and green with every spring, carrying in their lower leaf junctures the debris of the winter's flooding; and sycamores with mottled, white, recumbent limbs and branches that arch over the pool"#.to_string(), doc2 as u32), 0);
        let mut ti = match td.create_index() {
            Ok(i) => i,
            Err(e) => panic!("failed to create index err ={} ", e),
        };
        let op1 = ti.add_document(doc1 as i32).unwrap();
        let op2 = ti.add_document(doc2 as i32).unwrap();
        assert_eq!(op1, 0);
        assert_eq!(op2, 1);
        ti.commit().unwrap();
        let mut rb = ti.reader_builder().unwrap();
        let mut qp = rb.searcher().unwrap();
        qp.for_index(vec!["title".to_string()]).unwrap();
        let mut searcher = qp.parse_query("1989".to_string()).unwrap();
        let sres = &searcher.search(1, true, vec![]).unwrap();
        let title_result: Vec<ResultElement> = serde_json::from_str(sres).unwrap();
        assert_eq!(
            title_result[0].doc.0.get("title").unwrap()[0]
                .as_text()
                .unwrap(),
            "abc Hello1989World test".to_string()
        );
        match crate::do_term(&ti.ctx.id) {
            Ok(o) => o,
            Err(e) => panic!("exception = {e}"),
        };
    }
}
