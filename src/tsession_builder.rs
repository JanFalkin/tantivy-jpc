use crate::TantivySession;
use crate::InternalCallResult;
use crate::make_internal_json_error;
use crate::ErrorKinds;
use crate::info;

extern crate serde;
extern crate serde_derive;
extern crate serde_json;
use tantivy::DateOptions;
use serde_json::json;
use tantivy::schema::{TextOptions, Schema, STRING, TEXT, STORED, NumericOptions};


macro_rules! impl_simple_type {
    () => {};
    ($self:ident, $handler_params:ident, $handler_obj:ident, $handler_func:ident, $default_type:ident) => {
        let (name, _field_type, stored) = Self::extract_params($handler_params)?;
        let mut ni: $default_type = $default_type::default();
        if stored{
            ni = ni.set_stored();
        }
        info!("add_date_field: name = {}, field_type = {} stored = {}", &name, &_field_type, &stored);
        let f = $handler_obj.$handler_func(&name,ni);
        $self.return_buffer = json!({"field" : f}).to_string();
        info!("{}", $self.return_buffer);
    }
 }


impl<'a> TantivySession<'a>{

    pub fn extract_params(params:serde_json::Value) -> InternalCallResult<(String,u64,bool)>{
        let m = match params.as_object(){
            Some(x)=> x,
            None => return make_internal_json_error(ErrorKinds::BadParams("parameters are not a json object".to_string())),
        };
        let name = match m.get("name"){
            Some(x) => x.as_str().ok_or_else(|| ErrorKinds::BadParams("name has no value stored".to_string()))?,
            None  => return make_internal_json_error(ErrorKinds::BadParams("name param not found".to_string())),
        };
        let field_type = match m.get("type"){
            Some(v) => match v.as_u64() {
                Some(b) => b,
                None => return make_internal_json_error(ErrorKinds::BadParams("field type must be either 1 or 2 for STRING or TEXT".to_string())),
            }
            None => return make_internal_json_error(ErrorKinds::BadParams("type must be specified".to_string())),
        };
        let stored = match m.get("stored"){
            Some(v) => match v.as_bool() {
                Some(b) => b,
                None => return make_internal_json_error(ErrorKinds::BadParams("field stored must be true or false".to_string())),
            }
            None => false,
        };
        Ok((name.to_string(), field_type, stored))

    }
    pub fn handler_builder(&mut self, method:&str, _obj: &str, params:serde_json::Value)  -> InternalCallResult<u32>{
        info!("SchemaBuilder");
        let sb = match &mut self.builder{
            Some(x) => x,
            None => {
                self.builder = Some(Box::default());
                self.builder.as_mut().unwrap() // should be safe
            }
        };
        match method {
            "add_text_field" => {
                let (name, field_type, stored) = Self::extract_params(params)?;

                let mut ti: TextOptions;
                match field_type{
                    1 => {
                        info!("Found STRING");
                        ti = STRING
                    },
                    2 => {
                        info!("Found TEXT");
                        ti = TEXT
                    },
                    _ => {
                        return make_internal_json_error(ErrorKinds::BadParams("index must be a boolean value".to_string()))
                    },
                };
                if stored{
                    ti = ti | STORED;
                }
                info!("add_text_field: name = {}, field_type = {} stored = {}", &name, &field_type, &stored);
                let f = sb.add_text_field(&name,ti);
                self.return_buffer = json!({"field" : f}).to_string();
                info!("{}", self.return_buffer);
            },
            "add_date_field" => {
                impl_simple_type!(self, params, sb, add_date_field, DateOptions);
            },
            "add_u64_field" => {
                impl_simple_type!(self, params, sb, add_u64_field, NumericOptions);
            },
            "add_i64_field" => {
                impl_simple_type!(self, params, sb, add_i64_field, NumericOptions);
            },
            "add_f64_field" => {
                impl_simple_type!(self, params, sb, add_f64_field, NumericOptions);
            },
            "build" => {
                let sb = match self.builder.take(){
                    Some(x) => x,
                    None => return make_internal_json_error(ErrorKinds::BadInitialization("schema_builder not created".to_string()))
                };
                let schema:Schema = sb.build();
                self.return_buffer = json!({ "schema" : schema}).to_string();
                info!("{}", self.return_buffer);
                self.schema = Some(schema)
            },
            &_ => {}
        };

        Ok(0)
    }
}
