use std::collections::HashMap;

use crate::debug;
use crate::make_internal_json_error;
use crate::ErrorKinds;
use crate::InternalCallResult;
use crate::TantivySession;

extern crate serde;
extern crate serde_derive;
extern crate serde_json;
use serde_json::json;
use tantivy::schema::{
    Field, FieldEntry, Schema, TextFieldIndexing, TextOptions, STORED, STRING, TEXT,
};

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
                let field = sc.get_field(fields[0].as_str().unwrap())?;
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
                    serde_json::to_string(&sc.get_field(fields[0].as_str().unwrap())?)?;
            }
            "convert_named_doc" => {}
            "to_named_doc" => {}
            "to_json" => {}
            "parse_document" => {}
            "json_object_to_doc" => {}
            "find_field" => {}
            &_ => {}
        };
        Ok(0)
    }
}
