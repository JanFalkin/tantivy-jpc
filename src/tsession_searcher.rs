use crate::info;
use crate::make_internal_json_error;
use crate::ErrorKinds;
use crate::InternalCallResult;
use crate::TantivySession;

extern crate serde;
extern crate serde_derive;
extern crate serde_json;
use serde_derive::{Deserialize, Serialize};
use std::fmt::Write;
use tantivy::collector::{Count, TopDocs};
use tantivy::schema::NamedFieldDocument;
use tantivy::Document;

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

impl<'a> TantivySession<'a> {
    pub fn handle_fuzzy_searcher(
        &mut self,
        _method: &str,
        params: serde_json::Value,
    ) -> InternalCallResult<u32> {
        info!("Searcher");
        const DEF_LIMIT: u64 = 2;
        let top_limit = match params.as_object() {
            Some(p) => p
                .get("top_limit")
                .map(|u| u.as_u64())
                .flatten()
                .unwrap_or(DEF_LIMIT),
            None => DEF_LIMIT,
        };
        let query = match self.fuzzy_q.take() {
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
        info!("search complete len = {}, td = {:?}", td.0.len(), td);
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

        self.fuzzy_q = Some(query);
        if self.return_buffer.is_empty() {
            self.return_buffer = r#"{ "result" : "EMPTY"}"#.to_string();
        }
        Ok(0)
    }
    pub fn handle_searcher(
        &mut self,
        _method: &str,
        params: serde_json::Value,
    ) -> InternalCallResult<u32> {
        info!("Searcher");
        const DEF_LIMIT: u64 = 10;
        let (top_limit, explain) = match params.as_object() {
            Some(p) => (
                p.get("top_limit")
                    .map(|u| u.as_u64())
                    .flatten()
                    .unwrap_or(DEF_LIMIT),
                p.get("explain")
                    .map(|u| u.as_bool())
                    .flatten()
                    .unwrap_or(false),
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
                    "Reader unavliable".to_string(),
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
        info!("search complete len = {}, td = {:?}", td.len(), td);
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
            info!("retrieved doc {:?}", retrieved_doc.field_values());
            vret.append(&mut vec![ResultElement {
                doc: named_doc,
                score,
                explain: s,
            }]);
        }
        self.return_buffer = serde_json::to_string(&vret)?;
        info!("ret = {}", self.return_buffer);
        Ok(0)
    }
}
