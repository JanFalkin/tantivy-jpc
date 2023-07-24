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
use tantivy::DateOptions;

macro_rules! impl_simple_type {
    () => {};
    ($self:ident, $handler_params:ident, $handler_obj:ident, $handler_func:ident, $default_type:ident) => {
        let field_params =
            Self::extract_field_params($handler_params)?;
        let mut ni: $default_type;
        if field_params.stored {
            ni = $default_type::default().set_stored();
        } else {
            ni = $default_type::default();
        }
        if field_params.indexed {
            ni = ni.set_indexed();
        }
        if field_params.fast {
            ni = ni.set_fast();
        }
        let f = $handler_obj.$handler_func(&field_params.name, ni);
        $self.return_buffer = json!({ "field": f }).to_string();
    };
}

//let (name, field_type, stored, _indexed, fast) =
#[derive(Clone)]
pub struct ParamData {
    pub name: String,
    pub field_type: u64,
    pub stored: bool,
    pub indexed: bool,
    pub fast: bool,
    pub tokenizer: String,
}

impl TantivySession {
    pub fn extract_field_params(params: serde_json::Value) -> InternalCallResult<ParamData> {
        let m = match params.as_object() {
            Some(x) => x,
            None => {
                return make_internal_json_error(ErrorKinds::BadParams(
                    "parameters are not a json object".to_string(),
                ))
            }
        };
        let name = match m.get("name") {
            Some(x) => x
                .as_str()
                .ok_or_else(|| ErrorKinds::BadParams("name has no value stored".to_string()))?,
            None => {
                return make_internal_json_error(ErrorKinds::BadParams(
                    "name param not found".to_string(),
                ))
            }
        };
        let field_type = match m.get("type") {
            Some(v) => match v.as_u64() {
                Some(b) => b,
                None => {
                    return make_internal_json_error(ErrorKinds::BadParams(
                        "field type must be either 1 or 2 for STRING or TEXT".to_string(),
                    ))
                }
            },
            None => {
                return make_internal_json_error(ErrorKinds::BadParams(
                    "type must be specified".to_string(),
                ))
            }
        };
        let stored = match m.get("stored") {
            Some(v) => match v.as_bool() {
                Some(b) => b,
                None => {
                    return make_internal_json_error(ErrorKinds::BadParams(
                        "field stored must be true or false".to_string(),
                    ))
                }
            },
            None => false,
        };
        let indexed = match m.get("indexed") {
            Some(v) => match v.as_bool() {
                Some(b) => b,
                None => {
                    return make_internal_json_error(ErrorKinds::BadParams(
                        "field indexed must be true or false".to_string(),
                    ))
                }
            },
            None => false,
        };
        let fast = match m.get("fast") {
            Some(v) => v.as_bool().unwrap_or(false),
            None => false,
        };

        const TOKENIZER_DEFAULT: &str = "en_stem_with_stop_words";
        let jtok = json!(TOKENIZER_DEFAULT);
        let tok = m
            .get("tokenizer")
            .unwrap_or(&jtok)
            .as_str()
            .unwrap_or(TOKENIZER_DEFAULT);

        Ok(ParamData {
            name: name.to_string(),
            field_type,
            stored,
            indexed,
            fast,
            tokenizer: tok.to_string(),
        })
    }
    pub fn handler_builder(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> InternalCallResult<u32> {
        debug!("SchemaBuilder");
        let sb = match &mut self.builder {
            Some(x) => x,
            None => {
                self.builder = Some(Box::default());
                self.builder.as_mut().ok_or(ErrorKinds::BadInitialization(
                    "Unable to get default Schema as mutable".to_string(),
                ))? // should be safe
            }
        };
        match method {
            "add_text_field" => {
                let field_params = Self::extract_field_params(params)?;
                let mut ti: TextOptions;
                match field_params.field_type {
                    1 => {
                        debug!("Found STRING");
                        ti = STRING
                    }
                    2 => {
                        debug!("Found TEXT");
                        ti = TEXT
                    }
                    _ => {
                        return make_internal_json_error(ErrorKinds::BadParams(
                            "index must be a boolean value".to_string(),
                        ))
                    }
                };
                if field_params.stored {
                    ti = ti | STORED;
                }
                if field_params.field_type != 1 {
                    ti = ti.set_indexing_options(
                        TextFieldIndexing::default()
                            .set_tokenizer(&field_params.tokenizer)
                            .set_index_option(IndexRecordOption::WithFreqsAndPositions),
                    );
                }
                if field_params.fast {
                    ti = ti.set_fast(None);
                }

                debug!(
                    "add_text_field: name = {}, field_type = {} stored = {}",
                    &field_params.name, &field_params.field_type, &field_params.stored
                );
                let f = sb.add_text_field(&field_params.name, ti);
                self.return_buffer = json!({ "field": f }).to_string();
            }
            "add_date_field" => {
                impl_simple_type!(self, params, sb, add_date_field, DateOptions);
            }
            "add_u64_field" => {
                impl_simple_type!(self, params, sb, add_u64_field, NumericOptions);
            }
            "add_i64_field" => {
                impl_simple_type!(self, params, sb, add_i64_field, NumericOptions);
            }
            "add_f64_field" => {
                impl_simple_type!(self, params, sb, add_f64_field, NumericOptions);
            }
            "build" => {
                let sb = match self.builder.take() {
                    Some(x) => x,
                    None => {
                        return make_internal_json_error(ErrorKinds::BadInitialization(
                            "schema_builder not created".to_string(),
                        ))
                    }
                };
                let schema: Schema = sb.build();
                self.return_buffer = json!({ "schema": schema }).to_string();
                debug!("{}", self.return_buffer);
                self.schema = Some(schema)
            }
            &_ => {
                let e = ErrorKinds::BadParams(format!("Unknown method {method}"));
                self.make_json_error(&e.to_string());
                return Err(e);
            }
        };

        Ok(0)
    }
}
