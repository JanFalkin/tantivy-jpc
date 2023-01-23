use crate::TantivySession;
use crate::InternalCallResult;
use crate::make_internal_json_error;
use crate::ErrorKinds;
use crate::info;

extern crate serde;
extern crate serde_derive;
extern crate serde_json;
use tantivy::collector::TopDocs;
use std::fmt::Write;



impl<'a> TantivySession<'a>{

    pub fn handle_fuzzy_searcher(&mut self, _method:&str, _obj: &str, _params:serde_json::Value)  -> InternalCallResult<u32>{
        info!("Searcher");
        let query = match self.fuzzy_q.as_ref(){
            Some(dq) => dq,
            None => {
                return make_internal_json_error(ErrorKinds::NotExist("dyn query not created".to_string()));
            }
        };
        let li = match self.leased_item.as_ref(){
            Some(li) => li,
            None => return make_internal_json_error(ErrorKinds::NotExist("leased item not found".to_string())),
        };
        let q = *query.clone();
        let td = match li.search(&q, &TopDocs::with_limit(10)){
            Ok(td) => td,
            Err(e) => return make_internal_json_error(ErrorKinds::Search(format!("tantivy error = {e}"))),
        };
        info!("search complete len = {}, td = {:?}", td.len(), td);
        for (_score, doc_address) in td {
            let retrieved_doc = li.doc(doc_address)?;
            let schema = self.schema.as_ref().ok_or_else(|| ErrorKinds::NotExist("Schema not present".to_string()))?;
            let named_doc = schema.to_named_doc(&retrieved_doc);
            let mut s = "".to_string();
            match writeln!(s, "{}", serde_json::to_string(&named_doc)?){
                Ok(_) => {},
                Err (_) => return make_internal_json_error(ErrorKinds::NotExist("format write to string failed".to_string())),
            };
            self.return_buffer += &s;
            info!("{} n={} vals={:?}", s, s.len(), retrieved_doc.field_values());
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
        for (_score, doc_address) in td {
            let retrieved_doc = li.doc(doc_address)?;
            let schema = self.schema.as_ref().ok_or_else(|| ErrorKinds::NotExist("Schema not present".to_string()))?;
            let named_doc = schema.to_named_doc(&retrieved_doc);
            let mut s = "".to_string();
            match writeln!(s, "{}", serde_json::to_string(&named_doc)?){
                Ok(_) => {},
                Err (_) => return make_internal_json_error(ErrorKinds::NotExist("format write to string failed".to_string())),
            };
            self.return_buffer += &s;
            info!("{} n={} vals={:?}", s, s.len(), retrieved_doc.field_values());
        }
        Ok(0)
    }
}
