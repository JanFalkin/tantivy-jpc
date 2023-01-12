use crate::TantivySession;
use crate::InternalCallResult;
use crate::make_internal_json_error;
use crate::ErrorKinds;
use crate::info;

extern crate serde;
extern crate serde_derive;
extern crate serde_json;
use serde_json::json;


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
            self.index = Some(Box::new(tantivy::Index::create_in_ram(match self.schema.clone() {
            Some(s) => s,
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
        let writer = match self.indexwriter.as_mut().take(){
            Some(x) => x,
            None => {
                let bi = match self.index.as_mut().take(){
                    Some(x) => x,
                    None => return make_internal_json_error(ErrorKinds::BadInitialization("need index created for writer".to_string())),
                };
                self.indexwriter = Some(Box::new((*bi).writer(150000000).unwrap()));
                self.indexwriter.as_mut().unwrap()
            },
        };
        match method {
            "add_document" => {
                let doc = &self.doc;
                let d = match doc{
                    Some(x) => x,
                    None => {
                        return make_internal_json_error(ErrorKinds::NotExist("document needs to be created".to_string()))
                    },
                };
                let m = match params.as_object(){
                    Some(m)=> m,
                    None => return make_internal_json_error(ErrorKinds::BadParams("invalid parameters pass to Document add_text".to_string()))
                };
                let doc_idx = m.get("id").unwrap_or(&json!{0_i32}).as_u64().unwrap_or(0) as usize -1;
                let os = writer.add_document(d[doc_idx].to_owned());
                self.return_buffer = json!({"opstamp": os.unwrap_or(0)}).to_string();
                info!("{}", self.return_buffer);
            },
            "commit" => {
                match writer.commit(){
                    Ok(x)=>{
                        self.return_buffer = json!({"id": x}).to_string();
                        info!("{}", self.return_buffer);
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