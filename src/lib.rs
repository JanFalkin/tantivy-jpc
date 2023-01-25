#![feature(try_trait_v2)]


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
use tantivy::{Searcher, TantivyError};
use tantivy::query::{Query, QueryParser, FuzzyTermQuery};

use lazy_static::lazy_static;
use std::sync::Mutex;

extern crate thiserror;
use thiserror::Error;

lazy_static! {
  static ref TANTIVY_MAP: Mutex<HashMap<String, TantivySession<'static>>> = Mutex::new(HashMap::new());
  static ref ERRORS: Mutex<HashMap<String, Vec<String>>> = Mutex::new(HashMap::new());
}

pub mod tsession_builder;
pub mod tsession_index;
pub mod tsession_query_parser;
pub mod tsession_searcher;
pub mod tsession_document;
pub mod tsession_tests;

pub use self::tsession_builder::*;
pub use self::tsession_index::*;
pub use self::tsession_query_parser::*;
pub use self::tsession_searcher::*;
pub use self::tsession_document::*;
pub use self::tsession_tests::*;


// TantivySession provides a point of access to all Tantivy functionality on and for an Index.
// each TantivySession will maintain a given Option for it's lifetime and each will be a unique
// conversation based on the TantivySession::id.
struct TantivySession<'a>{
    pub(crate) id:&'a str,
    pub(crate) doc:Option<HashMap<usize, tantivy::Document>>,
    pub(crate) builder:Option<Box<tantivy::schema::SchemaBuilder>>,
    pub(crate) schema:Option<tantivy::schema::Schema>,
    pub(crate) index:Option<Box<tantivy::Index>>,
    pub(crate) indexwriter:Option<Box<tantivy::IndexWriter>>,
    pub(crate) index_reader_builder:Option<Box<tantivy::IndexReaderBuilder>>,
    pub(crate) leased_item:Option<Box<Searcher>>,
    pub(crate) query_parser:Option<Box<QueryParser>>,
    pub(crate) dyn_q:Option<Box<dyn Query>>,
    pub(crate) fuzzy_q:Option<Box<FuzzyTermQuery>>,


    return_buffer:String,
}

impl<'a> TantivySession<'a>{
    fn new(id:&'a str) -> TantivySession<'a>{
        TantivySession{
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
            fuzzy_q: None,
            return_buffer:String::new(),
        }
    }
    // do_method is a translation from a string json method to an actual call.  All json params are passed
    pub fn do_method(&mut self, method:&str, obj: &str, params:serde_json::Value) -> (*const u8, usize){
        info!("In do_method");
        match obj {
            "query_parser" => {
                if let Err(e) = self.handle_query_parser(method,obj,params) {
                    return make_json_error(&format!("handle query parser error={e}"), self.id)
                };
            },
            "searcher" =>{
                if let Err(e) = self.handle_searcher(method,obj,params) {
                    return make_json_error(&format!("handle searcher error={e}"), self.id)
                };
            }
            "fuzzy_searcher" =>{
                if let Err(e) = self.handle_fuzzy_searcher(method,obj,params) {
                    return make_json_error(&format!("handle searcher error={e}"), self.id)
                };
            }
            "index" =>{
                if let Err(e) = self.handle_index(method,obj,params) {
                    return make_json_error(&format!("handle index error={e}"), self.id)
                };
            },
            "indexwriter" => {
                if let Err(e) = self.handle_index_writer(method,obj,params) {
                    return make_json_error(&format!("handle index writer error={e}"), self.id)
                };
            },
            "index_reader" => {
                if let Err(e) = self.handle_index_reader(method,obj,params) {
                    return make_json_error(&format!("handle index reader error={e}"), self.id)
                };
            },
            "document" => {
                if let Err(e) = self.handle_document(method,obj,params) {
                    return make_json_error(&format!("handle document error={e}"), self.id)
                };
            },
            "builder" => {
                if let Err(e) = self.handler_builder(method,obj,params) {
                    return make_json_error(&format!("handle builder error={e}"), self.id)
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
        Err(err)=> format!("{err}"),
    };
    info!("returning  result = {}", vr);
    let mut t = ERRORS.lock().unwrap();
    let mt = t.get_mut(id);
    match mt{
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
    info!("error={ek}");
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

impl From<std::io::Error> for ErrorKinds {
    fn from(e:std::io::Error) -> Self{
        ErrorKinds::IO(e.to_string())
    }
}

impl From<tantivy::directory::error::OpenDirectoryError> for ErrorKinds {
    fn from(e:tantivy::directory::error::OpenDirectoryError) -> Self{
        ErrorKinds::IO(e.to_string())
    }
}

impl From<TantivyError> for ErrorKinds{
    fn from(e:tantivy::error::TantivyError) -> Self{
        ErrorKinds::BadParams(e.to_string())
    }
}

impl From<serde_json::Error> for ErrorKinds{
    fn from(e:serde_json::Error) -> Self{
        ErrorKinds::BadInitialization(e.to_string())
    }
}




pub type InternalCallResult<T> = std::result::Result<T, ErrorKinds>;

/// # Safety
///
#[no_mangle]
pub unsafe extern "C" fn init() -> u8{
    if let Ok(_res) = env_logger::try_init() {
        return 0;
    }
    1
}

/**
tantivy_jpc is the main entry point into a translation layer from Rust to Go for Tantivy
this function will
# Steps
  * parse the input for the appropriately formatted json
  * Modify internal state to reflect json requests
*/
/// # Safety
///
#[no_mangle]
pub unsafe extern "C" fn tantivy_jpc<>(msg: *const u8, len:usize, ret:*mut u8, ret_len:*mut usize) -> i64 {
  info!("In tantivy_jpc");
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
  let mut tm = match TANTIVY_MAP.lock(){
    Ok(t) => t,
    Err(e) => {
        info!("TANTIVY_MAP lock failed {e}");
        return -1;
    },
  };
  let entity:&mut TantivySession<'static> = match json_params.obj {
        "document" | "builder" | "index" | "indexwriter" | "query_parser" | "searcher" | "index_reader" | "fuzzy_searcher"=> {
            let cur_session = tm.get_mut(json_params.id);
            match cur_session{
                Some(x) => x,
                None => {
                    let te = TantivySession::new(json_params.id);
                    tm.insert(json_params.id.to_owned(), te);
                    tm.get_mut(json_params.id).unwrap() //should be ok just put in
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
