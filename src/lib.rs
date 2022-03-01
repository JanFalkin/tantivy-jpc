extern crate serde;
extern crate serde_derive;
extern crate serde_json;
extern crate lazy_static;
extern crate tempdir;
use log::{info};
use serde_json::json;
use serde_derive::{Serialize, Deserialize};
use std::str;
use std::collections::HashMap;
use tantivy::Document;
use tantivy::schema::{Field, TextOptions, Schema, SchemaBuilder};
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

static mut RETURN_MSG:String = String::new();


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
        }
    }
    fn create_index(&mut self, params:serde_json::Value) -> InternalCallResult<Box<tantivy::Index>>{
        let def_json = &json!("");
        let dir_to_use = {
            let this = (if let Some(m) = params.as_object() {
                m
            } else {
                return make_internal_json_error(ErrorKinds::BadHttpParams("invalid parameters pass to Document add_text"));
            }).get("directory");
            if let Some(x) = this {
                x
            } else {
                def_json
            }
        }.as_str().unwrap_or("");
        if dir_to_use != ""{
            let idx = match tantivy::Index::create_in_dir(dir_to_use, (match self.schema.take() {
            Some(s) => s,
            None => return  make_internal_json_error(ErrorKinds::BadHttpParams("A schema must be created before an index"))
        }).clone()){
                Ok(p) => p,
                Err(_) => {
                    let td = match tempdir::TempDir::new("indexer"){
                        Ok(tmp) => {
                            tantivy::Index::create_in_dir(tmp, if let Some(s) = self.schema.take() {
                                s
                            } else {
                                return  make_internal_json_error(ErrorKinds::BadInitialization("A schema must be created before an index"));
                            }).unwrap()
                        },
                        Err(_) => return make_internal_json_error(ErrorKinds::IO("failed to create TempDir"))
                    };
                    td
                },
            };
            self.index = Some(Box::new(idx));
            Ok(self.index.clone().unwrap())

        }else{
            info!("Creating index in RAM");
            self.index = Some(Box::new(tantivy::Index::create_in_ram(match self.schema.take() {
            Some(s) => s,
            None => return  make_internal_json_error(ErrorKinds::BadInitialization("A schema must be created before an index"))
        })));
            Ok(self.index.clone().unwrap())

        }
    }
    pub fn do_method(&mut self, method:&str, obj: &str, params:serde_json::Value) -> (*const u8, usize){
        info!("In do_method");
        match obj {
            "query_parser" => {
                info!("QueryParser");
                if method == "for_index"{
                    let idx = match &self.index{
                        Some(idx) => {idx},
                        None => {return make_json_error("index is None", self.id)}
                    };
                    info!("QueryParser aquired");
                    self.query_parser = Some(Box::new(QueryParser::for_index(&idx, vec![Field::from_field_id(0), Field::from_field_id(1)])));
                }
                if method == "parse_query"{
                    let qp = match &self.query_parser{
                        Some(qp) => {qp},
                        None => {return make_json_error("index is None", self.id)}
                    };
                    self.dyn_q = match qp.parse_query("Something"){
                        Ok(qp) => Some(qp),
                        Err(_e) => return make_json_error(&format!("query parser error {}", _e), self.id)
                    };
                }
            },
            "index" =>{
                info!("Index");
                match self.index.as_mut() {
                    Some(x) => x,
                    None if method == "create" => &mut {
                        match self.create_index(params){
                            Ok(idx) => {
                                idx
                            },
                            Err(err) => {
                                let buf = format!("{}", err);
                                return (buf.as_ptr() as *const u8, buf.len());
                            },
                        }
                    },
                    None if method == "reader_builder" => {
                            match self.create_index(params){
                                Ok(_) => {},
                                Err(err) => {
                                    let buf = format!("{}", err);
                                    return (buf.as_ptr() as *const u8, buf.len());
                                },
                            }
                            self.index_reader_builder = Some(Box::new(self.index.clone().unwrap().reader_builder()));
                            self.index.as_mut().unwrap()
                        }
                    None => {
                            return  make_json_error("index must first be created", self.id);
                        }
                };
            },
            "indexwriter" => {
                info!("IndexWriter");
                let writer = match self.indexwriter.as_mut().take(){
                    Some(x) => x,
                    None => {
                        let bi = match self.index.as_mut().take(){
                            Some(x) => x,
                            None => return make_json_error("need index created for writer", self.id),
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
                                return make_json_error("document needs to be created", self.id)
                            },
                        };
                        let m = match params.as_object(){
                            Some(m)=> m,
                            None => return make_json_error("invalid parameters pass to Document add_text", self.id)
                        };
                        let doc_idx = m.get("id").unwrap_or(&json!{0}).as_u64().unwrap_or(0) as usize;
                        let docs = *(d);
                        let os = writer.add_document(docs[doc_idx].clone());
                        info!("add document opstamp = {}", os)
                    },
                    "commit" => {
                        match writer.commit(){
                            Ok(x)=>{
                                info!("commit hash = {}", x);
                                x
                            },
                            Err(err) => return make_json_error(&format!("failed to commit indexwriter, {}", err), self.id)
                        };
                    },
                    _ => {}
                }

            },
            "index_reader" => {
                info!("Document");
                match method {
                    "searcher" => {
                        if let Some(idx) = self.index_reader_builder.clone() {
                            let f = (*idx).reload_policy(tantivy::ReloadPolicy::OnCommit).try_into();
                            match f {
                                Ok(idx_read) => {
                                    self.leased_item = Some(Box::new(idx_read.searcher()))
                                },
                                Err(_err) => {return make_json_error("tantivy error", self.id);}
                            }
                        }
                    }
                    &_ => {}
                }
            },
            "document" => {
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
                                return make_json_error("add_text with no doucments created", self.id)
                            }
                        };
                        let m = match params.as_object(){
                            Some(m)=> m,
                            None => return make_json_error("invalid parameters pass to Document add_text", self.id)
                        };
                        let doc_idx = m.get("doc_id").unwrap_or(&json!{0}).as_u64().unwrap_or(0) as usize;
                        let field_idx = m.get("id").unwrap_or(&json!{0}).as_u64().unwrap_or(0) as u32;
                        let x = d;
                        let f  = Field::from_field_id(field_idx);
                        info!("add_text: name = {:?}", m);
                        match m.get("field"){
                            Some(f) => {f.as_i64()},
                            None => {return make_json_error("field must contain integer id", self.id)}
                        };
                        let field_val = match m.get("value") {
                            Some(v) => {
                                v.as_str().unwrap_or("empty")
                            },
                            None => {return make_json_error("field text required for document", self.id)}
                        };
                        let cur_doc = match x.get_mut(doc_idx){
                            Some(d) => d,
                            None => {return make_json_error(&format!("document at index {} does not exist", doc_idx), self.id)}
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
                        unsafe{
                        RETURN_MSG = json!({"id" : v.len()}).to_string()
                        }
                    },
                    &_ => {}
                };
            },
            "builder" => {
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
                            None => return make_json_error("parameters are not a json object", self.id),
                        };
                        let name = match m.get("name"){
                            Some(x) => x.as_str().unwrap(),
                            None  => return make_json_error("name param not found", self.id),
                        };
                        info!("add_text_field: name = {}", &name);
                        let f = sb.add_text_field(name, TextOptions::default());
                        info!("field = {:?}", &f);
                    },
                    "build" => {
                        let sb = match self.builder.take(){
                            Some(x) => x,
                            None => return make_json_error("schema_builder not created", self.id)
                        };
                        let schema:Schema = sb.build();
                        info!("new schema {:?}", schema);
                        self.schema = Some(schema)
                    },
                    &_ => {}
                };
            },
            "schema" => {
            },
            &_ => {}
        };
        let _ = &self.doc;
        let _ = &self.builder;
        unsafe {
            (RETURN_MSG.as_str().as_ptr() as *const u8, RETURN_MSG.len())
        }
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

#[repr(C)]
#[derive(Error, Debug, Clone, Copy)]
pub enum ErrorKinds {
  #[error("Other Error : `{0}`")]
  Other(&'static str),
  #[error("Not Recognized : `{0}`")]
  UnRecognizedCommand(&'static str),
  #[error("Permission : `{0}`")]
  Permission(&'static str),
  #[error("IO : `{0}`")]
  IO(&'static str),
  #[error("Exist : `{0}`")]
  Utf8Error(std::str::Utf8Error),
  #[error("NotExist : `{0}`")]
  NotExist(&'static str),
  #[error("IsDir : `{0}`")]
  IsDir(&'static str),
  #[error("NotDir : `{0}`")]
  NotDir(&'static str),
  #[error("Finalized : `{0}`")]
  BadInitialization(&'static str),
  #[error("NotFinalized : `{0}`")]
  NotFinalized(&'static str),
  #[error("BadHttpParams : `{0}`")]
  BadHttpParams(&'static str),
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
        "document" | "builder" | "index" | "indexwriter" | "query_parser" => {
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
            let msg = ErrorKinds::UnRecognizedCommand(json_params.method).to_string();
            std::ptr::copy(msg.as_ptr() as *const u8, ret, msg.len());
            return -1;
        }
    };
    let (return_val, ret_sz) = entity.do_method(json_params.method, json_params.obj, json_params.params);
    std::ptr::copy(return_val, ret, ret_sz);
    *ret_len = ret_sz;
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
