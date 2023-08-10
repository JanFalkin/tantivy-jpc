use std::collections::HashMap;

use crate::debug;
use crate::make_internal_json_error;
use crate::ErrorKinds;
use crate::InternalCallResult;
use crate::TantivySession;

extern crate serde;
extern crate serde_derive;
extern crate serde_json;
use tantivy::schema::{Field, FieldEntry};

impl TantivySession {
    pub fn handler_schema(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> InternalCallResult<u32> {
        debug!("Schema");
        let sc = match &self.schema {
            Some(s) => s,
            None => {
                return make_internal_json_error(ErrorKinds::Search(
                    "handler schema called with no schema".to_string(),
                ))
            }
        };
        let ve = Vec::<serde_json::Value>::new();
        fn get_fields<'a>(
            params: &'a serde_json::Value,
            empty: &'a Vec<serde_json::Value>,
        ) -> &'a Vec<serde_json::Value> {
            match params.as_object() {
                Some(p) => {
                    let v = p.get("field").unwrap();
                    debug!("p={p:?} v={v}");
                    p.get("field").and_then(|u| u.as_array()).unwrap_or(empty) // Default to an empty vector
                }
                None => empty, // Default values
            }
        }

        match method {
            "get_field_entry" => {
                let fields = get_fields(&params, &ve);
                let field = sc.get_field(fields[0].as_str().unwrap_or(""))?;
                self.return_buffer = serde_json::to_string(sc.get_field_entry(field))?;
            }
            "num_fields" => {
                let c = sc.fields().count();
                self.return_buffer = serde_json::to_string(&c)?;
            }
            "fields" => {
                let hashmap: HashMap<Field, FieldEntry> = sc
                    .fields()
                    .map(|(field, field_entry)| (field, field_entry.clone())) // Assuming you want to clone FieldEntry
                    .collect();
                self.return_buffer = serde_json::to_string(&hashmap)?;
            }
            "get_field" => {
                let fields = get_fields(&params, &ve);
                self.return_buffer =
                    serde_json::to_string(&sc.get_field(fields[0].as_str().unwrap_or(""))?)?;
            }
            "convert_named_doc" => {
                return Err(ErrorKinds::NotExist(
                    "convert_named_doc not implemented".to_string(),
                ))
            }
            "to_named_doc" => {
                return Err(ErrorKinds::NotExist(
                    "to_named_doc not implemented".to_string(),
                ))
            }
            "to_json" => return Err(ErrorKinds::NotExist("to_json not implemented".to_string())),
            "parse_document" => {
                return Err(ErrorKinds::NotExist(
                    "parse_document not implemented".to_string(),
                ))
            }
            "json_object_to_doc" => {
                return Err(ErrorKinds::NotExist(
                    "json_object_to_doc not implemented".to_string(),
                ))
            }
            "find_field" => {
                return Err(ErrorKinds::NotExist(
                    "find_field not implemented".to_string(),
                ))
            }
            &_ => {}
        };
        Ok(0)
    }
}
