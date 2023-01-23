use crate::TantivySession;
use crate::InternalCallResult;
use crate::make_internal_json_error;
use crate::ErrorKinds;
use crate::info;

extern crate serde;
extern crate serde_derive;
extern crate serde_json;
use tantivy::query::FuzzyTermQuery;
use tantivy::schema::{Field,Term};
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
        if method == "parse_fuzzy_query"{
            let schema = match self.schema.as_ref(){
                Some(s) => s,
                None => return make_internal_json_error(ErrorKinds::BadInitialization("schema not available during for_index".to_string()))
            };
            let request_field = m.get("field").ok_or_else(|| ErrorKinds::BadParams("fields not present".to_string()))?.as_array().ok_or_else(|| ErrorKinds::BadParams("field not present".to_string()))?;
            if request_field.len() != 1{
                return make_internal_json_error(ErrorKinds::BadInitialization("Requesting more than one field in fuzzy query disallowed".to_string()))
            }
            let fuzzy_term = m.get("term").ok_or_else(|| ErrorKinds::BadParams("term not present".to_string()))?.as_array().ok_or_else(|| ErrorKinds::BadParams("term not present".to_string()))?;
            if fuzzy_term.len() != 1{
                return make_internal_json_error(ErrorKinds::BadInitialization("Requesting more than one term in fuzzy query disallowed".to_string()))
            }

            let field = &request_field[0];
            let f_str = field.as_str().unwrap_or_default();
            if let Some(f) = schema.get_field(f_str) {
                let f_term = &fuzzy_term[0];
                let t = Term::from_field_text(f, &f_term.to_string());
                let q = FuzzyTermQuery::new(t, 100, true);
                self.fuzzy_q = Some(Box::new(q));
            }
        }
        Ok(0)
    }

}