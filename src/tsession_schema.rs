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
    IndexRecordOption, NumericOptions, Schema, TextFieldIndexing, TextOptions, STORED, STRING, TEXT,
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
        let fields = match params.as_object() {
            Some(p) => {
                p.get("field")
                    .and_then(|u| u.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|item| item.as_str())
                            .collect::<Vec<&str>>()
                    })
                    .unwrap_or_else(Vec::new) // Default to an empty vector
            }
            None => vec![], // Default values
        };

        match method {
            "get_field_entry" => {
                let field = sc.get_field(fields[0])?;
                self.return_buffer = serde_json::to_string(sc.get_field_entry(field))?;
            }
            "get_field_name" => {
                let field = sc.get_field(fields[0])?;
                self.return_buffer = serde_json::to_string(sc.get_field_entry(field).name())?;
            }
            "num_fields" => {
                let c = sc.fields().count();
                self.return_buffer = serde_json::to_string(&c)?;
            }
            "fields" => {}
            "get_field" => {}
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
