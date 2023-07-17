use crate::debug;
use crate::make_internal_json_error;
use crate::ErrorKinds;
use crate::InternalCallResult;
use crate::TantivySession;

extern crate serde;
extern crate serde_derive;
extern crate serde_json;
use log::error;
use serde_derive::{Deserialize, Serialize};
use std::fmt::Write;
use tantivy::collector::{Count, TopDocs};
use tantivy::schema::Field;
use tantivy::schema::NamedFieldDocument;
use tantivy::Document;
use tantivy::SnippetGenerator;

#[derive(Serialize, Deserialize, Debug)]
pub struct ResultElement {
    pub doc: NamedFieldDocument,
    pub score: f32,
    pub explain: String,
}

#[derive(Serialize, Deserialize)]
pub struct ResultElementDoc {
    pub doc: Document,
    pub score: f32,
}

impl TantivySession {
    pub fn handle_fuzzy_searcher(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> InternalCallResult<u32> {
        debug!("FuzzySearcher");
        if method != "fuzzy_searcher" {
            return Err(ErrorKinds::NotExist(format!(
                "expecting method fuzzy_searcher found {method}"
            )));
        }
        const DEF_LIMIT: u64 = 2;
        let top_limit = match params.as_object() {
            Some(p) => p
                .get("top_limit")
                .and_then(|u| u.as_u64())
                .unwrap_or(DEF_LIMIT),
            None => DEF_LIMIT,
        };
        let query = match self.fuzzy_q.as_deref() {
            Some(dq) => dq,
            None => {
                return make_internal_json_error(ErrorKinds::NotExist(
                    "dyn query not created".to_string(),
                ));
            }
        };
        let idx = match &self.index {
            Some(r) => r,
            None => {
                return make_internal_json_error(ErrorKinds::NotExist(
                    "Reader unavliable".to_string(),
                ))
            }
        };

        let rdr = idx.reader()?;
        let searcher = rdr.searcher();
        let td = match searcher.search(&*query, &(TopDocs::with_limit(top_limit as usize), Count)) {
            Ok(td) => td,
            Err(e) => {
                return make_internal_json_error(ErrorKinds::Search(format!("tantivy error = {e}")))
            }
        };
        debug!("search complete len = {}, td = {:?}", td.0.len(), td);
        let mut vret = Vec::<ResultElementDoc>::new();
        for (score, doc_address) in td.0 {
            let retrieved_doc = searcher.doc(doc_address)?;
            vret.push(ResultElementDoc {
                doc: retrieved_doc,
                score,
            });
        }
        let mut s = "".to_string();
        match writeln!(s, "{}", serde_json::to_string(&vret)?) {
            Ok(_) => {}
            Err(_) => {
                return make_internal_json_error(ErrorKinds::NotExist(
                    "format write to string failed".to_string(),
                ))
            }
        };
        self.return_buffer = s;

        self.fuzzy_q = Some(Box::new(query.clone()));
        if self.return_buffer.is_empty() {
            self.return_buffer = r#"{ "result" : "EMPTY"}"#.to_string();
        }
        Ok(0)
    }

    fn do_search(&mut self, params: serde_json::Value) -> InternalCallResult<u32> {
        const DEF_LIMIT: u64 = 10;
        let (top_limit, explain) = match params.as_object() {
            Some(p) => (
                p.get("top_limit")
                    .and_then(|u| u.as_u64())
                    .unwrap_or(DEF_LIMIT),
                p.get("explain").and_then(|u| u.as_bool()).unwrap_or(false),
            ),
            None => (DEF_LIMIT, false),
        };
        let query = match self.dyn_q.as_ref() {
            Some(dq) => dq,
            None => {
                return make_internal_json_error(ErrorKinds::NotExist(
                    "dyn query not created".to_string(),
                ));
            }
        };
        let idx = match &self.index {
            Some(r) => r,
            None => {
                return make_internal_json_error(ErrorKinds::NotExist(
                    "Reader unavailable".to_string(),
                ))
            }
        };

        let rdr = idx.reader()?;
        let searcher = rdr.searcher();

        let td = match searcher.search(query, &TopDocs::with_limit(top_limit as usize)) {
            Ok(td) => td,
            Err(e) => {
                return make_internal_json_error(ErrorKinds::Search(format!("tantivy error = {e}")))
            }
        };
        debug!("search complete len = {}, td = {:?}", td.len(), td);
        let mut vret: Vec<ResultElement> = Vec::<ResultElement>::new();
        for (score, doc_address) in td {
            let retrieved_doc = searcher.doc(doc_address)?;
            let schema = self
                .schema
                .as_ref()
                .ok_or_else(|| ErrorKinds::NotExist("Schema not present".to_string()))?;
            let named_doc = schema.to_named_doc(&retrieved_doc);
            let mut s: String = "noexplain".to_string();
            if explain {
                s = query.explain(&searcher, doc_address)?.to_pretty_json();
            }
            debug!("retrieved doc {:?}", retrieved_doc.field_values());
            vret.append(&mut vec![ResultElement {
                doc: named_doc,
                score,
                explain: s,
            }]);
        }
        self.return_buffer = serde_json::to_string(&vret)?;
        debug!("ret = {}", self.return_buffer);
        Ok(0)
    }

    fn do_create_snippet_generator(
        &mut self,
        params: serde_json::Value,
    ) -> InternalCallResult<u32> {
        let searcher = match &self.leased_item {
            Some(s) => &*s,
            None => {
                return make_internal_json_error(ErrorKinds::NotExist(
                    "create snippet called with no searcher set".to_string(),
                ))
            }
        };
        let query = match &self.dyn_q {
            Some(s) => &*s,
            None => {
                return make_internal_json_error(ErrorKinds::NotExist(
                    "create snippet called with no searcher set".to_string(),
                ))
            }
        };
        let field_id: u64 = match params.as_object() {
            Some(p) => p.get("field_id").and_then(|u| u.as_u64()).unwrap_or(0),
            None => 0,
        };
        let f = Field::from_field_id(field_id as u32);
        self.snippet_generators = Some(SnippetGenerator::create(&searcher, query, f)?);
        Ok(0)
    }

    pub fn handle_searcher(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> InternalCallResult<u32> {
        debug!("Searcher");
        return match method {
            "search" => self.do_search(params),
            "snippet" => self.do_create_snippet_generator(params),
            _ => {
                error!("unknown method {method}");
                Err(ErrorKinds::NotExist(format!("unknown method {method}")))
            }
        };
    }
}
