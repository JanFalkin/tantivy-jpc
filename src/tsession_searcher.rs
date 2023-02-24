use crate::TantivySession;
use crate::InternalCallResult;
use crate::make_internal_json_error;
use crate::ErrorKinds;
use crate::info;

extern crate serde;
extern crate serde_derive;
extern crate serde_json;
use tantivy::Document;
use tantivy::collector::{Count, TopDocs};
use tantivy::schema::{NamedFieldDocument};
use std::fmt::Write;
use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct ResultElement{
	pub doc:NamedFieldDocument,
    pub score:f32,
}

#[derive(Serialize, Deserialize)]
pub struct ResultElementDoc{
	pub doc:Document,
    pub score:f32,
}


impl<'a> TantivySession<'a>{

    pub fn handle_fuzzy_searcher(&mut self, _method:&str, _obj: &str, _params:serde_json::Value)  -> InternalCallResult<u32>{
        info!("Searcher");
        let query = match self.fuzzy_q.take(){
            Some(dq) => dq,
            None => {
                return make_internal_json_error(ErrorKinds::NotExist("dyn query not created".to_string()));
            }
        };
        let idx = match &self.index{
            Some(r) => r,
            None => return make_internal_json_error(ErrorKinds::NotExist("Reader unavliable".to_string()))
        };

        let rdr = idx.reader()?;
        let searcher = rdr.searcher();
        let td = match searcher.search(&*query, &(TopDocs::with_limit(2), Count)){
            Ok(td) => td,
            Err(e) => return make_internal_json_error(ErrorKinds::Search(format!("tantivy error = {e}"))),
        };
        info!("search complete len = {}, td = {:?}", td.0.len(), td);
        let mut vret = Vec::<ResultElementDoc>::new();
        for (score, doc_address) in td.0 {
            let retrieved_doc = searcher.doc(doc_address)?;
            vret.push(ResultElementDoc{doc:retrieved_doc, score});
        }
        let mut s = "".to_string();
        match writeln!(s, "{}", serde_json::to_string(&vret)?){
            Ok(_) => {},
            Err (_) => return make_internal_json_error(ErrorKinds::NotExist("format write to string failed".to_string())),
        };
        self.return_buffer += &s;

        self.fuzzy_q = Some(query);
        if self.return_buffer.is_empty(){
            self.return_buffer = r#"{ "result" : "EMPTY"}"#.to_string();
        }
        Ok(0)
    }
    pub fn handle_searcher(&mut self, _method:&str, _obj: &str, _params:serde_json::Value)  -> InternalCallResult<u32>{
        info!("Searcher");
        let query = match self.dyn_q.as_ref(){
            Some(dq) => dq,
            None => {
                return make_internal_json_error(ErrorKinds::NotExist("dyn query not created".to_string()));
            }
        };
        let li = match self.leased_item.as_ref(){
            Some(li) => li,
            None => return make_internal_json_error(ErrorKinds::NotExist("leased item not found".to_string())),
        };
        let td = match li.search(query, &TopDocs::with_limit(10)){
            Ok(td) => td,
            Err(e) => return make_internal_json_error(ErrorKinds::Search(format!("tantivy error = {e}"))),
        };
        info!("search complete len = {}, td = {:?}", td.len(), td);
        let mut vret:Vec<ResultElement> = Vec::<ResultElement>::new();
        for (score, doc_address) in td {
            let retrieved_doc = li.doc(doc_address)?;
            let schema = self.schema.as_ref().ok_or_else(|| ErrorKinds::NotExist("Schema not present".to_string()))?;
            let named_doc = schema.to_named_doc(&retrieved_doc);
            info!("retrieved doc {:?}",retrieved_doc.field_values());
            vret.append(&mut vec![ResultElement{doc:named_doc,score}]);
        }
        self.return_buffer = serde_json::to_string(&vret)?;
        info!("ret = {}", self.return_buffer);
    Ok(0)
    }
}
