use crate::TantivySession;
use crate::InternalCallResult;
use crate::make_internal_json_error;
use crate::ErrorKinds;
use crate::info;

extern crate serde;
extern crate serde_derive;
extern crate serde_json;
use serde_json::json;
use tantivy::Term;


impl<'a> TantivySession<'a>{
    pub fn create_index(&mut self, params:serde_json::Value) -> InternalCallResult<Box<tantivy::Index>>{
        let def_json = &json!("");
        let dir_to_use = {
            let this = (if let Some(m) = params.as_object() {
                m
            } else {
                return make_internal_json_error(ErrorKinds::BadParams("invalid parameters pass to Document add_text".to_string()));
            }).get("directory");
            if let Some(x) = this {
                x
            } else {
                def_json
            }
        }.as_str().unwrap_or("");
        if !dir_to_use.is_empty(){
            let idx = match tantivy::Index::open_in_dir(dir_to_use){
                Ok(p) => p,
                Err(err) => {
                    info!("error={}\n", err);
                    tantivy::Index::create_in_dir(dir_to_use, if let Some(s) = &self.schema {
                        s.to_owned()
                    } else {
                        return  make_internal_json_error(ErrorKinds::BadInitialization("A schema must be created before an index".to_string()));
                    })?
                },
            };
            Ok(Box::new(idx))

        }else{
            info!("Creating index in RAM");
            self.index = Some(Box::new(tantivy::Index::create_in_ram(match &self.schema {
            Some(s) => s.to_owned(),
            None => return  make_internal_json_error(ErrorKinds::BadInitialization("A schema must be created before an index".to_string()))
        })));
            let r = self.index.clone().ok_or_else(|| ErrorKinds::Other("failed to clone index".to_string()))?;
            Ok(r)

        }
    }

    pub fn handle_index(&mut self, method:&str, _obj: &str, params:serde_json::Value)  -> InternalCallResult<u32>{
        info!("Index");
        let idx = match &self.index {
            Some(x) => x,
            None => {
                match self.create_index(params){
                    Ok(x) => {
                        self.index = Some(x);
                        let r = self.index.as_ref().unwrap();
                        self.schema = Some(r.schema());
                        r
                    },
                    Err(err) => {
                        let buf = format!("{err}");
                        return make_internal_json_error(ErrorKinds::BadParams(buf));
                    },
                }
            }
        };
        match method {
            "reader_builder" => {
                info!("Reader Builder");
                self.index_reader_builder = Some(Box::new(idx.reader_builder()));
                idx
            },
            "create" => {
                idx
            }
            &_ => {
                return make_internal_json_error(ErrorKinds::UnRecognizedCommand(format!("unknown method {method}")))
            }
        };
        Ok(0)
    }
    pub fn handle_index_writer(&mut self, method:&str, _obj: &str, params:serde_json::Value)  -> InternalCallResult<u32>{
        info!("IndexWriter");
        let writer = match self.indexwriter.as_mut(){
            Some(x) => x,
            None => {
                let bi = match self.index.as_mut().take(){
                    Some(x) => x,
                    None => return make_internal_json_error(ErrorKinds::BadInitialization("need index created for writer".to_string())),
                };
                self.indexwriter = Some(Box::new((*bi).writer(150000000).unwrap()));
                self.indexwriter.as_mut().ok_or(ErrorKinds::BadInitialization("need index created for writer".to_string()))?
            },
        };
        match method {
            "add_document" => {
                let mut doc = self.doc.take();
                let d = doc.as_mut().ok_or(ErrorKinds::NotExist("No value for hash in Documents".to_string()))?;
                let m = params.as_object().ok_or(ErrorKinds::BadParams("invalid parameters pass to Document add_text".to_string()))?;
                let doc_idx = m.get("id").unwrap_or(&json!{0_i32}).as_u64().unwrap_or(0) as usize -1;
                let rm = d.remove(&doc_idx).ok_or(ErrorKinds::BadInitialization("need index created for writer".to_string()))?;
                let os = writer.add_document(rm)?;
                self.return_buffer = json!({"opstamp": os}).to_string();
                self.doc = doc;
                info!("{}", self.return_buffer);
            },
            "delete_term" =>{
                let m = params.as_object().ok_or(ErrorKinds::BadParams("invalid parameters pass delete_term, params not an object".to_string()))?;
                let writer = match self.indexwriter.as_mut(){
                    Some(x) => x,
                    None => {
                        let bi = match self.index.as_mut().take(){
                            Some(x) => x,
                            None => return make_internal_json_error(ErrorKinds::BadInitialization("need index created for writer".to_string())),
                        };
                        self.indexwriter = Some(Box::new((*bi).writer(150000000).unwrap()));
                        self.indexwriter.as_mut().ok_or(ErrorKinds::BadInitialization("need index created for writer".to_string()))?
                    },
                };
                let schema = match self.schema.as_ref(){
                    Some(s) => s,
                    None => return make_internal_json_error(ErrorKinds::BadInitialization("schema not available during delete_term".to_string()))
                };
                let request_field = m.get("field").ok_or_else(|| ErrorKinds::BadParams("fields not present".to_string()))?.as_array().ok_or_else(|| ErrorKinds::BadParams("field not present".to_string()))?;
                if request_field.len() != 1{
                    return make_internal_json_error(ErrorKinds::BadInitialization("Requesting more than one field in delete_term disallowed".to_string()))
                }
                let field = &request_field[0];
                let f_str = match field.as_str(){
                    Some(s) => s,
                    None => return make_internal_json_error(ErrorKinds::BadInitialization("Field requested is not present".to_string()))
                };
                let terms = m.get("term").ok_or_else(|| ErrorKinds::BadParams("term not present".to_string()))?.as_array().ok_or_else(|| ErrorKinds::BadParams("term not present".to_string()))?;
                if terms.len() != 1{
                    return make_internal_json_error(ErrorKinds::BadInitialization("Requesting more than one term in fuzzy query disallowed".to_string()))
                }
                let str_term = terms[0].as_str().ok_or(ErrorKinds::BadInitialization("term not coercable to str".to_string()))?;
                if let Ok(f) = schema.get_field(f_str) {
                    let term = Term::from_field_text(f, str_term);
                    let ostamp = writer.delete_term(term);
                    //NOTE DELETIONS WILL NOT BE VISIBLE UNTIL AFTER COMMIT
                    self.return_buffer = json!({"opstamp": ostamp}).to_string();
                }
            },
            "commit" => {
                match writer.commit(){
                    Ok(x)=>{
                        self.return_buffer = json!({"id": x}).to_string();
                        info!("{}", self.return_buffer);
                        self.indexwriter = None;
                        x
                    },
                    Err(err) => return make_internal_json_error(ErrorKinds::NotFinalized(format!("failed to commit indexwriter, {err}")))
                };
            },
            _ => {}
        }

        Ok(0)
    }
    pub fn handle_index_reader(&mut self, method:&str, _obj: &str, _params:serde_json::Value)  -> InternalCallResult<u32>{
        info!("IndexReader");
        match method {
            "searcher" => {
                if let Some(idx) = self.index_reader_builder.as_ref() {
                    info!("got index reader@@@@@@");
                    match (*idx).clone().reload_policy(tantivy::ReloadPolicy::OnCommit).try_into() {
                        Ok(idx_read) => {
                            info!("Got leased item");
                            self.leased_item = Some(Box::new(idx_read.searcher()))
                        },
                        Err(err) => {return make_internal_json_error(ErrorKinds::Other(format!("tantivy error {err}")))}
                    }
                }
            }
            &_ => {}
        }
        Ok(0)
    }

}