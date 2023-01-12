use crate::TantivySession;
use crate::InternalCallResult;
use crate::make_internal_json_error;
use crate::ErrorKinds;
use crate::info;

extern crate serde;
extern crate serde_derive;
extern crate serde_json;
use tantivy::schema::Field;
use tantivy::query::QueryParser;


impl<'a> TantivySession<'a>{
    pub fn handle_query_parser(&mut self, method:&str, _obj: &str, params:serde_json::Value)  -> InternalCallResult<u32>{
        let m = match params.as_object(){
            Some(m)=> m,
            None => return make_internal_json_error::<u32>(ErrorKinds::BadParams("invalid parameters pass to query_parser add_text".to_string()))
        };
        info!("QueryParser");
        if method == "for_index"{
            let mut v_out:Vec<Field> = Vec::<Field>::new();
            let idx = match &self.index{
                Some(idx) => {idx},
                None => {return make_internal_json_error::<u32>(ErrorKinds::NotExist("index is None".to_string()))}
            };
            info!("QueryParser aquired");
            let schema = match self.schema.as_ref(){
                Some(s) => s,
                None => return make_internal_json_error(ErrorKinds::BadInitialization("schema not available during for_index".to_string()))
            };
            let request_fields = m.get("fields").ok_or_else(|| ErrorKinds::BadParams("fields not present".to_string()))?.as_array().ok_or_else(|| ErrorKinds::BadParams("fields not present".to_string()))?;
            for v in request_fields{
                let v_str = v.as_str().unwrap_or_default();
                if let Some(f) = schema.get_field(v_str) {
                     v_out.append(vec![f].as_mut())
                }
            }
            self.query_parser = Some(Box::new(QueryParser::for_index(idx, v_out)));
        }
        if method == "parse_query"{
            let qp = match &self.query_parser{
                Some(qp) => {qp},
                None => {return make_internal_json_error::<u32>(ErrorKinds::NotExist("index is None".to_string()))}
            };
            let query = match m.get("query"){
                Some(q)=> match q.as_str(){
                    Some(s) => s,
                    None => return make_internal_json_error::<u32>(ErrorKinds::BadParams("query parameter must be a string".to_string()))
                },
                None=> {return make_internal_json_error::<u32>(ErrorKinds::BadParams("parameter 'query' missing".to_string()))}
            };
            self.dyn_q = match qp.parse_query(query){
                Ok(qp) => Some(qp),
                Err(_e) => {
                    return make_internal_json_error::<u32>(ErrorKinds::BadParams(format!("query parser error : {_e}")))
                }
            };
        }
        Ok(0)
    }

}