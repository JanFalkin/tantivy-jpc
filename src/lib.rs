extern crate serde;
extern crate serde_derive;
extern crate serde_json;
extern crate lazy_static;
extern crate tempdir;
use log::{info};
use serde_json::json;
use serde_derive::{Serialize, Deserialize};
use tantivy::collector::TopDocs;
use std::str;
use std::collections::HashMap;
use tantivy::Document;
use tantivy::schema::{Field, TextOptions, Schema, SchemaBuilder, STRING, TEXT, STORED};
use tantivy::{LeasedItem, Searcher};
use tantivy::query::{Query, QueryParser};

use lazy_static::lazy_static;
use std::sync::Mutex;

extern crate thiserror;
use thiserror::Error;

lazy_static! {
  static ref TANTIVY_MAP: Mutex<HashMap<String, TantivyEntry<'static>>> = Mutex::new(HashMap::new());
  static ref ERRORS: Mutex<HashMap<String, Vec<String>>> = Mutex::new(HashMap::new());
}



struct TantivyEntry<'a>{
    pub(crate) id:&'a str,
    pub(crate) doc:Option<Box<Vec<tantivy::Document>>>,
    pub(crate) builder:Option<Box<tantivy::schema::SchemaBuilder>>,
    pub(crate) schema:Option<tantivy::schema::Schema>,
    pub(crate) index:Option<Box<tantivy::Index>>,
    pub(crate) indexwriter:Option<Box<tantivy::IndexWriter>>,
    pub(crate) index_reader_builder:Option<Box<tantivy::IndexReaderBuilder>>,
    pub(crate) leased_item:Option<Box<LeasedItem<Searcher>>>,
    pub(crate) query_parser:Option<Box<QueryParser>>,
    pub(crate) dyn_q:Option<Box<dyn Query>>,
    return_buffer:String,
}

impl<'a> TantivyEntry<'a>{
    fn new(id:&'a str) -> TantivyEntry<'a>{
        TantivyEntry{
            id,
            doc:None,
            builder:None,
            schema:None,
            index:None,
            indexwriter:None,
            index_reader_builder:None,
            leased_item:None,
            query_parser:None,
            dyn_q: None,
            return_buffer:String::new(),
        }
    }
    fn create_index(&mut self, params:serde_json::Value) -> InternalCallResult<Box<tantivy::Index>>{
        let def_json = &json!("");
        let dir_to_use = {
            let this = (if let Some(m) = params.as_object() {
                m
            } else {
                return make_internal_json_error(ErrorKinds::BadParams(format!("invalid parameters pass to Document add_text")));
            }).get("directory");
            if let Some(x) = this {
                x
            } else {
                def_json
            }
        }.as_str().unwrap_or("");
        if dir_to_use != ""{
            let idx = match tantivy::Index::create_in_dir(dir_to_use, (match self.schema.clone() {
            Some(s) => s,
            None => return  make_internal_json_error(ErrorKinds::BadParams(format!("A schema must be created before an index")))
        }).clone()){
                Ok(p) => p,
                Err(_) => {
                    let td = match tempdir::TempDir::new("indexer"){
                        Ok(tmp) => {
                            tantivy::Index::create_in_dir(tmp, if let Some(s) = self.schema.clone() {
                                s
                            } else {
                                return  make_internal_json_error(ErrorKinds::BadInitialization(format!("A schema must be created before an index")));
                            }).unwrap()
                        },
                        Err(_) => return make_internal_json_error(ErrorKinds::IO(format!("failed to create TempDir")))
                    };
                    td
                },
            };
            self.index = Some(Box::new(idx));
            Ok(self.index.clone().unwrap())

        }else{
            info!("Creating index in RAM");
            self.index = Some(Box::new(tantivy::Index::create_in_ram(match self.schema.clone() {
            Some(s) => s,
            None => return  make_internal_json_error(ErrorKinds::BadInitialization(format!("A schema must be created before an index")))
        })));
            Ok(self.index.clone().unwrap())

        }
    }
    fn handle_query_parser(&mut self, method:&str, _obj: &str, params:serde_json::Value)  -> InternalCallResult<u32>{
        let m = match params.as_object(){
            Some(m)=> m,
            None => return make_internal_json_error::<u32>(ErrorKinds::BadParams(format!("invalid parameters pass to query_parser add_text")))
        };
        info!("QueryParser");
        if method == "for_index"{
            let mut v_out:Vec<Field> = Vec::<Field>::new();
            let idx = match &self.index{
                Some(idx) => {idx},
                None => {return make_internal_json_error::<u32>(ErrorKinds::NotExist(format!("index is None")))}
            };
            info!("QueryParser aquired");
            let schema = match self.schema.as_ref(){
                Some(s) => s,
                None => return make_internal_json_error(ErrorKinds::BadInitialization(format!("schema not available during for_index")))
            };
            let request_fields = m.get("fields").ok_or(ErrorKinds::BadParams(format!("fields not present")))?.as_array().ok_or(ErrorKinds::BadParams(format!("fields not present")))?;
            for v in request_fields{
                let v_str = v.as_str().unwrap_or_default();
                match schema.get_field(v_str){
                    Some(f) => v_out.append(vec![f].as_mut()),
                    None => {},
                }
            }
            self.query_parser = Some(Box::new(QueryParser::for_index(&idx, v_out)));
        }
        if method == "parse_query"{
            let qp = match &self.query_parser{
                Some(qp) => {qp},
                None => {return make_internal_json_error::<u32>(ErrorKinds::NotExist(format!("index is None")))}
            };
            let query = match m.get("query"){
                Some(q)=> match q.as_str(){
                    Some(s) => s,
                    None => return make_internal_json_error::<u32>(ErrorKinds::BadParams(format!("query parameter must be a string")))
                },
                None=> {return make_internal_json_error::<u32>(ErrorKinds::BadParams(format!("parameter 'query' missing")))}
            };
            self.dyn_q = match qp.parse_query(query){
                Ok(qp) => Some(qp),
                Err(_e) => {
                    return make_internal_json_error::<u32>(ErrorKinds::BadParams(format!("query parser error : {}", _e)))
                }
            };
        }
        Ok(0)
    }
    fn handle_searcher(&mut self, _method:&str, _obj: &str, _params:serde_json::Value)  -> InternalCallResult<u32>{
        info!("Searcher");
        let query = self.dyn_q.as_ref().unwrap();
        let li = match self.leased_item.as_ref(){
            Some(li) => li,
            None => return make_internal_json_error(ErrorKinds::NotExist(format!("leased item not found"))),
        };
        let td = match li.search(&*query, &TopDocs::with_limit(10)){
            Ok(td) => td,
            Err(e) => return make_internal_json_error(ErrorKinds::Search(format!("tantivy error = {}", e))),
        };
        info!("search complete len = {}, td = {:?}", td.len(), td);
        for (_score, doc_address) in td {
            let retrieved_doc = li.doc(doc_address).unwrap();
            let schema = self.schema.as_ref().unwrap();
            self.return_buffer += &format!("{}", schema.to_json(&retrieved_doc));
            info!("{} n={} vals={:?}", schema.to_json(&retrieved_doc), retrieved_doc.len(), retrieved_doc.field_values());
        }
        Ok(0)
    }
    fn handle_index(&mut self, method:&str, _obj: &str, params:serde_json::Value)  -> InternalCallResult<u32>{
        info!("Index");
        let idx = match &self.index {
            Some(x) => x,
            None => {
                match self.create_index(params){
                    Ok(_) => {
                        self.index.as_ref().unwrap()
                    },
                    Err(err) => {
                        let buf = format!("{}", err);
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
            &_ => {
                return make_internal_json_error(ErrorKinds::UnRecognizedCommand(format!("unknown method {}", method)))
            }
        };
        Ok(0)
    }
    fn handle_index_writer(&mut self, method:&str, _obj: &str, params:serde_json::Value)  -> InternalCallResult<u32>{
        info!("IndexWriter");
        let writer = match self.indexwriter.as_mut().take(){
            Some(x) => x,
            None => {
                let bi = match self.index.as_mut().take(){
                    Some(x) => x,
                    None => return make_internal_json_error(ErrorKinds::BadInitialization(format!("need index created for writer"))),
                };
                self.indexwriter = Some(Box::new((*bi).writer(150000000).unwrap()));
                self.indexwriter.as_mut().unwrap()
            },
        };
        match method {
            "add_document" => {
                let doc = self.doc.clone();
                let d = match doc{
                    Some(x) => x,
                    None => {
                        return make_internal_json_error(ErrorKinds::NotExist(format!("document needs to be created")))
                    },
                };
                let m = match params.as_object(){
                    Some(m)=> m,
                    None => return make_internal_json_error(ErrorKinds::BadParams(format!("invalid parameters pass to Document add_text")))
                };
                let doc_idx = m.get("id").unwrap_or(&json!{0}).as_u64().unwrap_or(0) as usize;
                let docs = *(d);
                let os = writer.add_document(docs[doc_idx].clone());
                self.return_buffer = json!({"opstamp": os}).to_string();
                info!("{}", self.return_buffer);
            },
            "commit" => {
                match writer.commit(){
                    Ok(x)=>{
                        self.return_buffer = json!({"id": x}).to_string();
                        info!("{}", self.return_buffer);
                        x
                    },
                    Err(err) => return make_internal_json_error(ErrorKinds::NotFinalized(format!("failed to commit indexwriter, {}", err)))
                };
            },
            _ => {}
        }

        Ok(0)
    }
    fn handle_index_reader(&mut self, method:&str, _obj: &str, _params:serde_json::Value)  -> InternalCallResult<u32>{
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
                        Err(err) => {return make_internal_json_error(ErrorKinds::Other(format!("tantivy error {}", err)))}
                    }
                }
            }
            &_ => {}
        }
        Ok(0)
    }
    fn handle_document(&mut self, method:&str, _obj: &str, params:serde_json::Value)  -> InternalCallResult<u32>{
        info!("Document");
        match method {
            "add_text" => {
                let doc = self.doc.as_mut().take();
                let d = match doc{
                    Some(x) => {
                        let v = x;
                        v.append(&mut vec![Document::new()]);
                        self.doc = Some(Box::new((**v).clone()));
                        self.doc.as_mut().unwrap()
                    },
                    None => {
                        return make_internal_json_error(ErrorKinds::BadInitialization(format!("add_text with no doucments created")))
                    }
                };
                let m = match params.as_object(){
                    Some(m)=> m,
                    None => return make_internal_json_error(ErrorKinds::BadParams(format!("invalid parameters pass to Document add_text")))
                };
                let doc_idx = m.get("doc_id").unwrap_or(&json!{0}).as_u64().unwrap_or(0) as usize;
                let field_idx = m.get("id").unwrap_or(&json!{0}).as_u64().unwrap_or(0) as u32;
                let x = d;
                let f  = Field::from_field_id(field_idx);
                info!("add_text: name = {:?}", m);
                match m.get("field"){
                    Some(f) => {f.as_i64()},
                    None => {return make_internal_json_error(ErrorKinds::BadParams(format!("field must contain integer id")))}
                };
                let field_val = match m.get("value") {
                    Some(v) => {
                        v.as_str().unwrap_or("empty")
                    },
                    None => {return make_internal_json_error(ErrorKinds::BadInitialization(format!("field text required for document")))}
                };
                let cur_doc = match x.get_mut(doc_idx){
                    Some(d) => d,
                    None => {return make_internal_json_error(ErrorKinds::BadInitialization(format!("document at index {} does not exist", doc_idx)))}
                };
                cur_doc.add_text(f,field_val);
            },
            "create" => {
                let doc = self.doc.as_mut().take();
                match doc{
                    Some(x) => {
                        let mut v = (**x).clone();
                        v.append(&mut vec![Document::new()]);
                        self.doc = Some(Box::new(v));
                    },
                    None => {
                        let nd= Document::new();
                        self.doc = Some(Box::new(vec![nd]));
                        self.doc.as_mut().unwrap();
                    },
                };
                let v = *self.doc.clone().unwrap();
                self.return_buffer = json!({"document_count" : v.len()}).to_string()
            },
            &_ => {}
        };
        Ok(0)
    }
    fn handler_builder(&mut self, method:&str, _obj: &str, params:serde_json::Value)  -> InternalCallResult<u32>{
        info!("SchemaBuilder");
        let sb = match &mut self.builder{
            Some(x) => x,
            None => {
                self.builder = Some(Box::new(SchemaBuilder::default()));
                self.builder.as_mut().unwrap()
            }
        };
        match method {
            "add_text_field" => {

                let m = match params.as_object(){
                    Some(x)=> x,
                    None => return make_internal_json_error(ErrorKinds::BadParams(format!("parameters are not a json object"))),
                };
                let name = match m.get("name"){
                    Some(x) => x.as_str().unwrap(),
                    None  => return make_internal_json_error(ErrorKinds::BadParams(format!("name param not found"))),
                };
                info!("add_text_field: name = {}", &name);
                let indexed = match m.get("index"){
                    Some(v) => match v.as_bool() {
                        Some(b) => b,
                        None => return make_internal_json_error(ErrorKinds::BadParams(format!("index must be a boolean value"))),
                    }
                    None => false,
                };
                let ti: TextOptions;
                if indexed{
                    info!("Indexed!!!!!!");
                    ti = STRING
                }else {
                    ti = TEXT | STORED
                }
                let f = sb.add_text_field(name,ti);
                self.return_buffer = json!({"field" : f}).to_string();
                info!("{}", self.return_buffer);
            },
            "build" => {
                let sb = match self.builder.take(){
                    Some(x) => x,
                    None => return make_internal_json_error(ErrorKinds::BadInitialization(format!("schema_builder not created")))
                };
                let schema:Schema = sb.build();
                self.return_buffer = json!({ "schema" : schema}).to_string();
                info!("{}", self.return_buffer);
                self.schema = Some(schema)
            },
            &_ => {}
        };

        return Ok(0)
    }
    pub fn do_method(&mut self, method:&str, obj: &str, params:serde_json::Value) -> (*const u8, usize){
        info!("In do_method");
        match obj {
            "query_parser" => {
                if let Err(e) = self.handle_query_parser(method,obj,params) {
                    return make_json_error(&format!("handle query parser error={}", e), self.id)
                };
            },
            "searcher" =>{
                if let Err(e) = self.handle_searcher(method,obj,params) {
                    return make_json_error(&format!("handle searcher error={}", e), self.id)
                };
            }
            "index" =>{
                if let Err(e) = self.handle_index(method,obj,params) {
                    return make_json_error(&format!("handle index error={}", e), self.id)
                };
            },
            "indexwriter" => {
                if let Err(e) = self.handle_index_writer(method,obj,params) {
                    return make_json_error(&format!("handle index writer error={}", e), self.id)
                };
            },
            "index_reader" => {
                if let Err(e) = self.handle_index_reader(method,obj,params) {
                    return make_json_error(&format!("handle index reader error={}", e), self.id)
                };
            },
            "document" => {
                if let Err(e) = self.handle_document(method,obj,params) {
                    return make_json_error(&format!("handle document error={}", e), self.id)
                };
            },
            "builder" => {
                if let Err(e) = self.handler_builder(method,obj,params) {
                    return make_json_error(&format!("handle builder error={}", e), self.id)
                };
            },
            "schema" => {
            },
            &_ => {}
        };
        let _ = &self.doc;
        let _ = &self.builder;
            (self.return_buffer.as_ptr() as *const u8, self.return_buffer.len())
    }
}
/// Bitcode representation of a incomming client request
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Request<'a> {
  pub id: &'a str,
  pub jpc: &'a str,
  pub obj: &'a str,
  pub method: &'a str,
  pub params: serde_json::Value,
}

/// make_json_error translates the bitcode [ElvError<T>] to an error response to the client
/// # Arguments
/// * `err`- the error to be translated to a response
pub fn make_json_error(err:&str, id:&str) -> (*const u8, usize){
    info!("error={}", err);
    let msg = json!(
        {
        "error" :  err,
        "jpc" : "1.0",
        "id"  : id,
        }
    );
    let vr = match serde_json::to_string(&msg){
        Ok(x)=> x,
        Err(err)=> format!("{}", err),
    };
    info!("returning  result = {}", vr);
    let mut t = ERRORS.lock().unwrap();
    match t.get_mut(id){
        Some(errs) => {
            let mut v = vec![err.to_string()];
            errs.append(& mut v)
        },
        None => {
            t.insert(id.to_string(), vec![err.to_string()]);
        }
    };
    let buf = vr.as_bytes();
    (buf.as_ptr() as *const u8, buf.len())
}

pub fn make_internal_json_error<T>(ek:ErrorKinds) -> InternalCallResult<T>{
    info!("error={}", ek);
    Err(ek)
}

#[derive(Error, Debug, Clone)]
pub enum ErrorKinds {
  #[error("Other Error : `{0}`")]
  Other(String),
  #[error("Not Recognized : `{0}`")]
  UnRecognizedCommand(String),
  #[error("Permission : `{0}`")]
  Permission(String),
  #[error("IO : `{0}`")]
  IO(String),
  #[error("Exist : `{0}`")]
  Utf8Error(std::str::Utf8Error),
  #[error("NotExist : `{0}`")]
  NotExist(String),
  #[error("IsDir : `{0}`")]
  IsDir(String),
  #[error("NotDir : `{0}`")]
  NotDir(String),
  #[error("Finalized : `{0}`")]
  BadInitialization(String),
  #[error("NotFinalized : `{0}`")]
  NotFinalized(String),
  #[error("BadParams : `{0}`")]
  BadParams(String),
  #[error("Search : `{0}`")]
  Search(String),
}

impl From<std::str::Utf8Error> for ErrorKinds {
    fn from(e:std::str::Utf8Error) -> Self{
        ErrorKinds::Utf8Error(e)
    }
}

pub type InternalCallResult<T> = std::result::Result<T, ErrorKinds>;


/// # Safety
///
#[no_mangle]
pub unsafe extern "C" fn init() -> u8{
    env_logger::init();
    0
}

/**
jpc is the main entry point into a translation layer from Rust to Go for Tantivy
this function will
# Steps
  * parse the input for the appropriately formatted json
  * Modify internal state to reflect json requests
*/
/// # Safety
///
#[no_mangle]
pub unsafe extern "C" fn jpc<>(msg: *const u8, len:usize, ret:*mut u8, ret_len:*mut usize) -> i64 {
  info!("In jpc");
  let input_string = match str::from_utf8(std::slice::from_raw_parts(msg, len)){
      Ok(x) => x,
      Err(err) => {
          *ret_len  = err.to_string().len();
          std::ptr::copy(err.to_string().as_ptr(), ret, *ret_len);
          return -1;
      }
  };
  info!("parameters = {}", input_string);
  let json_params: Request = match serde_json::from_str(input_string){
    Ok(m) => {m},
    Err(_err) => {
          let (r,sz) = make_json_error("parse failed for http", "ID not found");
          *ret_len = sz;
          std::ptr::copy(r, ret, sz);
          return -1;
    }
  };
  info!("Request parsed");
  let mut tm = TANTIVY_MAP.lock().unwrap();
  let entity:&mut TantivyEntry<'static> = match json_params.obj {
        "document" | "builder" | "index" | "indexwriter" | "query_parser" | "searcher" | "index_reader" => {
            match tm.get_mut(json_params.id){
                Some(x) => x,
                None => {
                    let te = TantivyEntry::new(json_params.id);
                    tm.insert(json_params.id.to_owned(), te);
                    tm.get_mut(json_params.id).unwrap()
                },
            }
        }
        _ =>  {
            let msg = ErrorKinds::UnRecognizedCommand(json_params.method.to_string()).to_string();
            std::ptr::copy(msg.as_ptr() as *const u8, ret, msg.len());
            return -1;
        }
    };
    let (return_val, ret_sz) = entity.do_method(json_params.method, json_params.obj, json_params.params);
    std::ptr::copy(return_val, ret, ret_sz);
    *ret_len = ret_sz;
    entity.return_buffer.clear();
    0
}
#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
