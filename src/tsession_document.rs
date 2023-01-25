use std::collections::HashMap;

use crate::TantivySession;
use crate::InternalCallResult;
use crate::make_internal_json_error;
use crate::ErrorKinds;
use crate::info;

extern crate serde;
extern crate serde_derive;
extern crate serde_json;

use serde_json::json;
use tantivy::schema::{Field};
use tantivy::{Document};


impl<'a> TantivySession<'a>{
    pub fn handle_document(&mut self, method:&str, _obj: &str, params:serde_json::Value)  -> InternalCallResult<u32>{
        info!("Document");
        match method {
            "add_text" => {
                let doc = self.doc.as_mut();
                let d = match doc{
                    Some(v) => v,
                    None => return make_internal_json_error(ErrorKinds::BadInitialization("add_text with no doucments created".to_string())),
                };
                let m = match params.as_object(){
                    Some(m)=> m,
                    None => return make_internal_json_error(ErrorKinds::BadParams("invalid parameters pass to Document add_text".to_string()))
                };
                let doc_idx = m.get("doc_id").unwrap_or(&json!{0}).as_u64().unwrap_or(0) as usize - 1;
                let field_idx = m.get("field").unwrap_or(&json!{0}).as_u64().unwrap_or(0) as u32;
                let f  = Field::from_field_id(field_idx);
                info!("add_text: name = {:?}", m);
                match m.get("field"){
                    Some(f) => {f.as_i64()},
                    None => {return make_internal_json_error(ErrorKinds::BadParams("field must contain integer id".to_string()))}
                };
                let field_val = match m.get("value") {
                    Some(v) => {
                        v.as_str().unwrap_or("empty")
                    },
                    None => {return make_internal_json_error(ErrorKinds::BadInitialization("field text required for document".to_string()))}
                };
                let cur_doc = match d.get_mut(&doc_idx){
                    Some(d) => d,
                    None => {return make_internal_json_error(ErrorKinds::BadInitialization(format!("document at index {doc_idx} does not exist")))}
                };
                cur_doc.add_field_value(f,field_val);
            },
            "create" => {
                let doc = self.doc.as_mut();
                let length:usize;
                match doc{
                    Some(x) => {
                        let l = x.len();
                        x.insert(l, Document::default());
                        length = x.len();
                    },
                    None => {
                        let nd= Document::default();
                        let mut hm = HashMap::<usize, Document>::new();
                        hm.insert(0, nd);
                        self.doc = Some(hm);
                        length = 1;
                    },
                };
                self.return_buffer = json!({"document_count" : length}).to_string()
            },
            &_ => {}
        };
        Ok(0)
    }
}