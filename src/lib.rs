#![feature(try_trait_v2)]

extern crate lazy_static;
extern crate serde;
extern crate serde_derive;
extern crate serde_json;
extern crate tempdir;
use log::{error, info};
use serde_derive::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::ffi::{c_char, CStr};
use std::str;
use tantivy::query::{FuzzyTermQuery, Query, QueryParser};
use tantivy::{Searcher, TantivyError};

use chrono::format::ParseError;
use lazy_static::lazy_static;
use std::sync::Mutex;

extern crate thiserror;
use thiserror::Error;

lazy_static! {
    static ref TANTIVY_MAP: Mutex<HashMap<String, TantivySession>> = Mutex::new(HashMap::new());
    static ref ERRORS: Mutex<HashMap<String, Vec<String>>> = Mutex::new(HashMap::new());
    static ref DATA_MAP: Mutex<HashMap<i64, XferData>> = Mutex::new(HashMap::new());
}

pub mod tsession_builder;
pub mod tsession_document;
pub mod tsession_index;
pub mod tsession_query_parser;
pub mod tsession_searcher;
pub mod tsession_tests;

pub use self::tsession_builder::*;
pub use self::tsession_document::*;
pub use self::tsession_index::*;
pub use self::tsession_query_parser::*;
pub use self::tsession_searcher::*;
pub use self::tsession_tests::*;

// TantivySession provides a point of access to all Tantivy functionality on and for an Index.
// each TantivySession will maintain a given Option for it's lifetime and each will be a unique
// conversation based on the TantivySession::id.
struct TantivySession {
    pub(crate) id: String,
    pub(crate) doc: Option<HashMap<usize, tantivy::Document>>,
    pub(crate) builder: Option<Box<tantivy::schema::SchemaBuilder>>,
    pub(crate) schema: Option<tantivy::schema::Schema>,
    pub(crate) index: Option<Box<tantivy::Index>>,
    pub(crate) indexwriter: Option<Box<tantivy::IndexWriter>>,
    pub(crate) index_reader_builder: Option<Box<tantivy::IndexReaderBuilder>>,
    pub(crate) leased_item: Option<Box<Searcher>>,
    pub(crate) query_parser: Option<Box<QueryParser>>,
    pub(crate) dyn_q: Option<Box<dyn Query>>,
    pub(crate) fuzzy_q: Option<Box<FuzzyTermQuery>>,

    return_buffer: String,
}

#[derive(Clone)]
pub struct XferData {
    pub bytes: String,
}

impl TantivySession {
    fn new(id: &str) -> TantivySession {
        TantivySession {
            id: id.to_string(),
            doc: None,
            builder: None,
            schema: None,
            index: None,
            indexwriter: None,
            index_reader_builder: None,
            leased_item: None,
            query_parser: None,
            dyn_q: None,
            fuzzy_q: None,
            return_buffer: String::new(),
        }
    }

    /// make_json_error translates the bitcode [ElvError<T>] to an error response to the client
    /// # Arguments
    /// * `err`- the error to be translated to a response
    pub fn make_json_error(&mut self, err: &str) {
        info!("error={}", err);
        let msg = json!(
            {
            "error" :  err,
            "jpc" : "1.0",
            "id"  : self.id,
            }
        );
        self.return_buffer = match serde_json::to_string(&msg) {
            Ok(x) => x,
            Err(err) => format!("{err}"),
        };
    }

    #[allow(clippy::all)]
    unsafe fn send_result_to_golang(
        &self,
        go_memory: &mut *const u8,
        go_memory_sz: *mut usize,
    ) -> i64 {
        *go_memory = self.return_buffer.as_bytes().as_ptr() as *mut u8;
        *go_memory_sz = self.return_buffer.as_bytes().len();
        0
    }

    // do_method is a translation from a string json method to an actual call.  All json params are passed
    pub fn do_method(&mut self, method: &str, obj: &str, params: serde_json::Value) {
        info!("In do_method");
        match obj {
            "query_parser" => {
                if let Err(e) = self.handle_query_parser(method, params) {
                    return self.make_json_error(&format!("handle query parser error={e}"));
                };
            }
            "searcher" => {
                if let Err(e) = self.handle_searcher(method, params) {
                    return self.make_json_error(&format!("handle searcher error={e}"));
                };
            }
            "fuzzy_searcher" => {
                if let Err(e) = self.handle_fuzzy_searcher(method, params) {
                    return self.make_json_error(&format!("handle searcher error={e}"));
                };
            }
            "index" => {
                if let Err(e) = self.handle_index(method, params) {
                    return self.make_json_error(&format!("handle index error={e}"));
                };
            }
            "indexwriter" => {
                if let Err(e) = self.handle_index_writer(method, params) {
                    return self.make_json_error(&format!("handle index writer error={e}"));
                };
            }
            "index_reader" => {
                if let Err(e) = self.handle_index_reader(method, params) {
                    return self.make_json_error(&format!("handle index serde_json::from_str(std::str::from_utf8(s).unwrap()).unwrap();reader error={e}"));
                };
            }
            "document" => {
                if let Err(e) = self.handle_document(method, params) {
                    return self.make_json_error(&format!("handle document error={e}"));
                };
            }
            "builder" => {
                if let Err(e) = self.handler_builder(method, params) {
                    return self.make_json_error(&format!("handle builder error={e}"));
                };
            }
            "schema" => {}
            &_ => {}
        };
        let _ = &self.doc;
        let _ = &self.builder;
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
pub fn make_json_error(err: &str, id: &str) -> String {
    info!("error={}", err);
    let msg = json!(
        {
        "error" :  err,
        "jpc" : "1.0",
        "id"  : id,
        }
    );
    let vr = match serde_json::to_string(&msg) {
        Ok(x) => x,
        Err(err) => format!("{err}"),
    };
    info!("returning  result = {}", vr);
    vr
}

pub fn make_internal_json_error<T>(ek: ErrorKinds) -> InternalCallResult<T> {
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
    fn from(e: std::str::Utf8Error) -> Self {
        ErrorKinds::Utf8Error(e)
    }
}

impl From<std::io::Error> for ErrorKinds {
    fn from(e: std::io::Error) -> Self {
        ErrorKinds::IO(e.to_string())
    }
}

impl From<tantivy::directory::error::OpenDirectoryError> for ErrorKinds {
    fn from(e: tantivy::directory::error::OpenDirectoryError) -> Self {
        ErrorKinds::IO(e.to_string())
    }
}

impl From<TantivyError> for ErrorKinds {
    fn from(e: tantivy::error::TantivyError) -> Self {
        ErrorKinds::BadParams(e.to_string())
    }
}

impl From<serde_json::Error> for ErrorKinds {
    fn from(e: serde_json::Error) -> Self {
        ErrorKinds::BadInitialization(e.to_string())
    }
}

impl From<ParseError> for ErrorKinds {
    fn from(e: ParseError) -> Self {
        ErrorKinds::BadParams(e.to_string())
    }
}

impl From<std::net::AddrParseError> for ErrorKinds {
    fn from(e: std::net::AddrParseError) -> Self {
        ErrorKinds::BadParams(e.to_string())
    }
}

pub type InternalCallResult<T> = Result<T, ErrorKinds>;

/// # Safety
///
#[no_mangle]
pub unsafe extern "C" fn init() -> u8 {
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("trace"))
        .try_init();
    0
}

pub fn test_init() {
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("trace"))
        .try_init();
}

fn do_term(s: &str) -> InternalCallResult<String> {
    match TANTIVY_MAP.lock().as_mut() {
        Ok(t) => {
            info!("removing {s}");
            t.remove_entry(s)
                .ok_or(ErrorKinds::NotExist(format!("Entry {s} is not available")))
        }
        Err(e) => {
            info!("TANTIVY_MAP lock failed {e}");
            return Err(ErrorKinds::BadParams(format!(
                "TANTIVY_MAP lock failed e = {e}"
            )));
        }
    }?;
    Ok(s.to_string())
}

/// # Safety
///
/// Terminate a tantivy session by its ID.
///
#[no_mangle]
pub unsafe extern "C" fn term(s: *const c_char) -> i8 {
    let c_str = CStr::from_ptr(s).to_str().unwrap_or("");
    if !c_str.is_empty() {
        match do_term(c_str) {
            Ok(_) => {
                info!("tag cleaned");
                0
            }
            Err(_) => {
                error!("tag NOT cleaned");
                -1
            }
        };
    }
    0
}

/// # Safety
///
/// This function will directly affect the way Tantivyoreders it's result set.  This is for advanced use only and should
/// be avoided unless you understand all the specifics of these 2 globals. Note this will only persist as long as the
/// current instance is loaded and will reset on a new invocation of tantivy
#[no_mangle]
pub unsafe extern "C" fn set_k_and_b(k: f32, b: f32) -> i8 {
    tantivy::query::do_set_k_and_b(k, b);
    0
}

#[test]
fn test_kb() {
    unsafe {
        set_k_and_b(1.0, 1.0);
    }
}
/// # Safety
///
#[no_mangle]
pub unsafe extern "C" fn free_data(handle: i64) -> std::ffi::c_int {
    let mut map = match DATA_MAP.lock() {
        Ok(m) => m,
        Err(e) => {
            error!("free_data failed to lock data map err={e}");
            return -1;
        }
    };
    if handle == 0 {
        return 0;
    }
    match map.remove(&handle) {
        Some(_data) => {
            0 // success
        }
        None => -1, // error: handle not found in the map
    }
}

#[allow(clippy::all)]
unsafe fn send_to_golang(
    val_to_send: String,
    go_memory: &mut *const u8,
    go_memory_sz: *mut usize,
) -> i64 {
    let mut map = match DATA_MAP.lock() {
        Ok(l) => l,
        Err(e) => {
            error!("failed to lock DATA-MAP {e}");
            return -1;
        }
    };
    let mut handle = rand::random::<i64>();
    if handle < 0 {
        handle = handle * -1;
    }
    let data = XferData { bytes: val_to_send };
    map.insert(handle, data);
    let mem = match map.get(&handle) {
        Some(m) => m,
        None => {
            error!("failed to lock DATA-MAP entry {handle}");
            return -1;
        }
    };
    *go_memory = mem.bytes.as_ptr() as *mut u8;
    *go_memory_sz = mem.bytes.len();
    handle
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
pub unsafe extern "C" fn tantivy_jpc(
    msg: *const u8,
    len: usize,
    ret: &mut *const u8,
    ret_len: *mut usize,
) -> i64 {
    info!("In tantivy_jpc");
    let input_string = match str::from_utf8(std::slice::from_raw_parts(msg, len)) {
        Ok(x) => x,
        Err(err) => {
            error!("failed error = {err}");
            return send_to_golang(err.to_string(), ret, ret_len);
        }
    };
    let json_params: Request = match serde_json::from_str(input_string) {
        Ok(m) => m,
        Err(_err) => {
            let r = make_json_error("parse failed for http", "ID not found");
            return send_to_golang(r, ret, ret_len);
        }
    };
    let mut tm = match TANTIVY_MAP.lock() {
        Ok(t) => t,
        Err(e) => {
            info!("TANTIVY_MAP lock failed {e}");
            return -1;
        }
    };
    let entity: &mut TantivySession = match json_params.obj {
        "document" | "builder" | "index" | "indexwriter" | "query_parser" | "searcher"
        | "index_reader" | "fuzzy_searcher" => {
            let cur_session = tm.get_mut(json_params.id);
            match cur_session {
                Some(x) => x,
                None => {
                    let te = TantivySession::new(json_params.id);
                    tm.insert(json_params.id.to_owned(), te);
                    match tm.get_mut(json_params.id) {
                        Some(s) => s,
                        None => {
                            let msg = ErrorKinds::NotExist(format!(
                                "Session {} not found",
                                json_params.id
                            ))
                            .to_string();
                            return send_to_golang(msg, ret, ret_len);
                        }
                    } //should be ok just put in
                }
            }
        }
        _ => {
            let msg = make_json_error(
                &ErrorKinds::UnRecognizedCommand(json_params.method.to_string()).to_string(),
                "noid",
            );

            return send_to_golang(msg, ret, ret_len);
        }
    };
    entity.do_method(json_params.method, json_params.obj, json_params.params);
    entity.send_result_to_golang(ret, ret_len)
}
