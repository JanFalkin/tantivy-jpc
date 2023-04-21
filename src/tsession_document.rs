use std::collections::HashMap;

use crate::info;
use crate::make_internal_json_error;
use crate::ErrorKinds;
use crate::InternalCallResult;
use crate::TantivySession;

extern crate serde;
extern crate serde_derive;
extern crate serde_json;

use serde_json::json;
use tantivy::schema::Field;
use tantivy::Document;

fn string_val(v: serde_json::Value) -> tantivy::schema::Value {
    tantivy::schema::Value::Str(v.as_str().unwrap_or("empty").to_string())
}

fn int_val(v: serde_json::Value) -> tantivy::schema::Value {
    tantivy::schema::Value::I64(v.as_i64().unwrap_or(0))
}

fn uint_val(v: serde_json::Value) -> tantivy::schema::Value {
    tantivy::schema::Value::U64(v.as_u64().unwrap_or(0))
}

impl<'a> TantivySession<'a> {
    fn handle_add_field(
        &mut self,
        params: serde_json::Value,
        func: fn(v: serde_json::Value) -> tantivy::schema::Value,
    ) -> InternalCallResult<u32> {
        let doc = self.doc.as_mut();
        let d = match doc {
            Some(v) => v,
            None => {
                return make_internal_json_error(ErrorKinds::BadInitialization(
                    "add_text with no doucments created".to_string(),
                ))
            }
        };
        let m = match params.as_object() {
            Some(m) => m,
            None => {
                return make_internal_json_error(ErrorKinds::BadParams(
                    "invalid parameters pass to Document add_text".to_string(),
                ))
            }
        };
        let doc_idx = m.get("doc_id").unwrap_or(&json! {0}).as_u64().unwrap_or(0) as usize - 1;
        let field_idx = m.get("field").unwrap_or(&json! {0}).as_u64().unwrap_or(0) as u32;
        let f = Field::from_field_id(field_idx);
        info!("add_text: name = {:?}", m);
        match m.get("field") {
            Some(f) => f.as_i64(),
            None => {
                return make_internal_json_error(ErrorKinds::BadParams(
                    "field must contain integer id".to_string(),
                ))
            }
        };
        let field_val = match m.get("value") {
            Some(v) => func(v.clone()),
            None => {
                return make_internal_json_error(ErrorKinds::BadInitialization(
                    "field text required for document".to_string(),
                ))
            }
        };
        let cur_doc = match d.get_mut(&doc_idx) {
            Some(d) => d,
            None => {
                return make_internal_json_error(ErrorKinds::BadInitialization(format!(
                    "document at index {doc_idx} does not exist"
                )))
            }
        };
        cur_doc.add_field_value(f, field_val);
        Ok(0)
    }
    pub fn handle_document(
        &mut self,
        method: &str,
        _obj: &str,
        params: serde_json::Value,
    ) -> InternalCallResult<u32> {
        info!("Document");
        match method {
            "add_text" => {
                self.handle_add_field(params, string_val)?;
                0
            }
            "add_int" => {
                self.handle_add_field(params, int_val)?;
                0
            }
            "add_uint" => {
                self.handle_add_field(params, uint_val)?;
                0
            }
            "create" => {
                let doc = self.doc.as_mut();
                let length: usize;
                match doc {
                    Some(x) => {
                        let d = Document::default();
                        let l = x.len();
                        x.insert(l, d);
                        length = x.len();
                    }
                    None => {
                        let nd = Document::default();
                        let mut hm = HashMap::<usize, Document>::new();
                        hm.insert(0, nd);
                        self.doc = Some(hm);
                        length = 1;
                    }
                };
                self.return_buffer = json!({ "document_count": length }).to_string();
                0
            }
            &_ => 0,
        };
        Ok(0)
    }
}
