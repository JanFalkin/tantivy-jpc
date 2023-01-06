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
use tantivy::schema::{Field, TextOptions, Schema, STRING, TEXT, STORED, NumericOptions};
use tantivy::{LeasedItem, Searcher};
use tantivy::query::{Query, QueryParser};

use lazy_static::lazy_static;
use std::sync::Mutex;

extern crate thiserror;
use thiserror::Error;

lazy_static! {
  static ref TANTIVY_MAP: Mutex<HashMap<String, TantivySession<'static>>> = Mutex::new(HashMap::new());
  static ref ERRORS: Mutex<HashMap<String, Vec<String>>> = Mutex::new(HashMap::new());
}

macro_rules! impl_simple_type {
    () => {};
    ($self:ident, $handler_params:ident, $handler_obj:ident, $handler_func:ident) => {
        let (name, _field_type, stored) = Self::extract_params($handler_params)?;
        let mut ni: NumericOptions = NumericOptions::default();
        if stored{
            ni = ni.set_stored();
        }
        info!("add_date_field: name = {}, field_type = {} stored = {}", &name, &_field_type, &stored);
        let f = $handler_obj.$handler_func(&name,ni);
        $self.return_buffer = json!({"field" : f}).to_string();
        info!("{}", $self.return_buffer);
    }
 }

// TantivySession provides a point of access to all Tantivy functionality on and for an Index.
// each TantivySession will maintain a given Option for it's lifetime and each will be a unique
// conversation based on the TantivySession::id.
struct TantivySession<'a>{
    pub(crate) id:&'a str,
    pub(crate) doc:Option<Vec<tantivy::Document>>,
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
            return_buffer:String::new(),
        }
    }
    fn create_index(&mut self, params:serde_json::Value) -> InternalCallResult<Box<tantivy::Index>>{
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
            let mdir = tantivy::directory::MmapDirectory::open(dir_to_use)?;
            let idx = match tantivy::Index::open_or_create(mdir, match &self.schema {
                Some(s) => s.to_owned(),
                None => return  make_internal_json_error(ErrorKinds::BadParams("A schema must be created before an index".to_string()))
             }){
                Ok(p) => p,
                Err(err) => {
                    info!("error={}\n", err);
                    match tempdir::TempDir::new("indexer"){
                        Ok(tmp) => {
                            tantivy::Index::create_in_dir(tmp, if let Some(s) = &self.schema {
                                s.to_owned()
                            } else {
                                return  make_internal_json_error(ErrorKinds::BadInitialization("A schema must be created before an index".to_string()));
                            }).unwrap()
                        },
                        Err(_) => return make_internal_json_error(ErrorKinds::IO("failed to create TempDir".to_string()))
                    }
                },
            };
            self.index = Some(Box::new(idx));
            Ok(self.index.clone().unwrap())

        }else{
            info!("Creating index in RAM");
            self.index = Some(Box::new(tantivy::Index::create_in_ram(match self.schema.clone() {
            Some(s) => s,
            None => return  make_internal_json_error(ErrorKinds::BadInitialization("A schema must be created before an index".to_string()))
        })));
            Ok(self.index.clone().unwrap())

        }
    }
    fn handle_query_parser(&mut self, method:&str, _obj: &str, params:serde_json::Value)  -> InternalCallResult<u32>{
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
    fn handle_searcher(&mut self, _method:&str, _obj: &str, _params:serde_json::Value)  -> InternalCallResult<u32>{
        info!("Searcher");
        let query = match self.dyn_q.as_ref(){
            Some(dq) => dq,
            None => {
                return make_internal_json_error(ErrorKinds::NotExist("dyn query not created".to_string()));
            }
        };
        let li = match self.leased_item.as_ref(){
            Some(li) => li,
            None => return make_internal_json_error(ErrorKinds::NotExist("leased item not found".to_string())),
        };
        let td = match li.search(query, &TopDocs::with_limit(10)){
            Ok(td) => td,
            Err(e) => return make_internal_json_error(ErrorKinds::Search(format!("tantivy error = {e}"))),
        };
        info!("search complete len = {}, td = {:?}", td.len(), td);
        for (_score, doc_address) in td {
            let retrieved_doc = li.doc(doc_address).unwrap();
            let schema = self.schema.as_ref().unwrap();
            self.return_buffer += &schema.to_json(&retrieved_doc).to_string();
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
    fn handle_index_writer(&mut self, method:&str, _obj: &str, params:serde_json::Value)  -> InternalCallResult<u32>{
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
                        Err(err) => {return make_internal_json_error(ErrorKinds::Other(format!("tantivy error {err}")))}
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
                    Some(v) => v,
                    None => return make_internal_json_error(ErrorKinds::BadInitialization("add_text with no doucments created".to_string())),
                };
                let m = match params.as_object(){
                    Some(m)=> m,
                    None => return make_internal_json_error(ErrorKinds::BadParams("invalid parameters pass to Document add_text".to_string()))
                };
                let doc_idx = m.get("doc_id").unwrap_or(&json!{0}).as_u64().unwrap_or(0) as usize - 1;
                let field_idx = m.get("field").unwrap_or(&json!{0}).as_u64().unwrap_or(0) as u32;
                let f  = Field::from_field_id(field_idx);
                info!("add_text: name = {:?}", m);
                match m.get("field"){
                    Some(f) => {f.as_i64()},
                    None => {return make_internal_json_error(ErrorKinds::BadParams("field must contain integer id".to_string()))}
                };
                let field_val = match m.get("value") {
                    Some(v) => {
                        v.as_str().unwrap_or("empty")
                    },
                    None => {return make_internal_json_error(ErrorKinds::BadInitialization("field text required for document".to_string()))}
                };
                let cur_doc = match d.get_mut(doc_idx){
                    Some(d) => d,
                    None => {return make_internal_json_error(ErrorKinds::BadInitialization(format!("document at index {doc_idx} does not exist")))}
                };
                cur_doc.add_text(f,field_val);
            },
            "create" => {
                let doc = self.doc.as_mut().take();
                let length:usize;
                match doc{
                    Some(x) => {
                        x.push(Document::new());
                        length = x.len();
                    },
                    None => {
                        let nd= Document::new();
                        self.doc = Some(vec![nd]);
                        length = 1;
                        self.doc.as_mut().unwrap();
                    },
                };
                self.return_buffer = json!({"document_count" : length}).to_string()
            },
            &_ => {}
        };
        Ok(0)
    }

    fn extract_params(params:serde_json::Value) -> InternalCallResult<(String,u64,bool)>{
        let m = match params.as_object(){
            Some(x)=> x,
            None => return make_internal_json_error(ErrorKinds::BadParams("parameters are not a json object".to_string())),
        };
        let name = match m.get("name"){
            Some(x) => x.as_str().unwrap(),
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
    fn handler_builder(&mut self, method:&str, _obj: &str, params:serde_json::Value)  -> InternalCallResult<u32>{
        info!("SchemaBuilder");
        let sb = match &mut self.builder{
            Some(x) => x,
            None => {
                self.builder = Some(Box::default());
                self.builder.as_mut().unwrap()
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
                impl_simple_type!(self, params, sb, add_date_field);
            },
            "add_u64_field" => {
                impl_simple_type!(self, params, sb, add_u64_field);
            },
            "add_i64_field" => {
                impl_simple_type!(self, params, sb, add_i64_field);
            },
            "add_f64_field" => {
                impl_simple_type!(self, params, sb, add_f64_field);
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
  let mut tm = TANTIVY_MAP.lock().unwrap();
  let entity:&mut TantivySession<'static> = match json_params.obj {
        "document" | "builder" | "index" | "indexwriter" | "query_parser" | "searcher" | "index_reader" => {
            let cur_session = tm.get_mut(json_params.id);
            match cur_session{
                Some(x) => x,
                None => {
                    let te = TantivySession::new(json_params.id);
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
pub mod tests {
    extern crate tempdir;
    use tempdir::TempDir;
    use uuid::{Uuid};

    use super::*;
    use serde_json::Map;

    pub static mut TEMPDIRS: Vec<TempDir> = vec![];

    macro_rules! call_simple_type {
        //() => {};
        ($self:ident, $j_param:ident, $method:literal) => {
            {
                let v = &$self.call_jpc("builder".to_string(), $method.to_string(), $j_param, true);
                let temp_map:serde_json::Value = serde_json::from_slice(v).unwrap();
                temp_map["field"].as_i64().unwrap()
            }
        }
     }




    #[derive(Clone, Serialize, Deserialize, Debug)]
    pub struct FakeContext{
        pub id:String,
        pub buf:Vec<u8>,
        pub ret_len:usize,

    }
    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct TestDocument{
        pub     temp_dir:String,
        pub ctx:    FakeContext,

    }

    #[derive(Serialize, Deserialize, Clone, Debug, Default)]
    pub struct TestDocResult {
        pub opstamp: u64,
    }
    #[derive(Serialize, Deserialize, Clone, Debug, Default)]
    pub struct TestCreateDocumentResult{
        pub document_count: usize
    }

    #[derive(Serialize, Deserialize, Clone, Debug, Default)]
    pub struct TestBuilderAddTextResult {
        pub schema: serde_json::Value,
    }
    #[derive(Serialize, Deserialize, Clone, Debug, Default)]
    pub struct TestTitleResult {
        pub title: Vec<String>,
    }


    pub struct TestIndex{
        ctx:    FakeContext,
        temp_dir: String,
    }

    pub struct TestIndexReader{
        ctx:    FakeContext,
    }

    pub struct TestQueryParser{
        ctx:    FakeContext,
    }

    pub struct TestSearcher{
        ctx:    FakeContext,
    }

    impl TestSearcher{
        pub fn search(&mut self)-> InternalCallResult<String>{
            let b = self.ctx.call_jpc("searcher".to_string(), "search".to_string(), json!({}),true);
            let s = std::str::from_utf8(&b).unwrap();
            Ok(s.to_string())
        }
    }

    impl TestQueryParser{
        pub fn for_index(&mut self, v:Vec<String>)-> InternalCallResult<i32>{
            self.ctx.call_jpc("query_parser".to_string(), "for_index".to_string(), json!({
                "fields": v,
            }), false);
            Ok(0)
        }
        pub fn parse_query(&mut self, query:String) ->  InternalCallResult<TestSearcher> {
            self.ctx.call_jpc("query_parser".to_string(), "parse_query".to_string(), json!({"query": query}), false);
            Ok(TestSearcher{ctx:self.ctx.clone()})
        }
    }
    impl TestIndexReader{
        pub fn searcher(&mut self) -> InternalCallResult<TestQueryParser>{
            self.ctx.call_jpc("index_reader".to_string(), "searcher".to_string(), json!({}),false);
            Ok(TestQueryParser{ctx:self.ctx.clone()})
        }
    }

    impl TestIndex{
        pub fn add_document(&mut self, doc_id:i32) ->Result<u64, u32>{
            let _ = self.temp_dir;
            let s = self.ctx.call_jpc("indexwriter".to_string(), "add_document".to_string(), json!({"id": doc_id}), true);
            let resmap:TestDocResult = serde_json::from_slice(&s).unwrap();
            Ok(resmap.opstamp)
        }

        pub fn commit(&mut self) -> Result<i64, u32>{
            let r = self.ctx.call_jpc("indexwriter".to_string(), "commit".to_string(), json!({}), true);
            let i:Map<String,serde_json::Value> = serde_json::from_slice(&r).unwrap();
            Ok(i["id"].as_i64().unwrap())

        }
        pub fn reader_builder(&mut self)-> InternalCallResult<TestIndexReader>{
            self.ctx.call_jpc("index".to_string(), "reader_builder".to_string(), json!({}),false);
            Ok(TestIndexReader{ctx:self.ctx.clone()})
        }
    }

    impl TestDocument{
        pub fn create(&mut self) -> Result<usize, i32>{
            let tdc:TestCreateDocumentResult = serde_json::from_slice(&self.ctx.call_jpc("document".to_string(), "create".to_string(), json!({}), true)).unwrap();
            Ok(tdc.document_count)
        }
        pub fn add_text(&mut self, field:i32, value:String, doc_id:u32) -> i64 {
            self.ctx.call_jpc("document".to_string(), "add_text".to_string(), json!({"field":  field,"value":  value, "id":  self.ctx.id,  "doc_id": doc_id}),false);
            0
        }
        pub fn create_index(&mut self) -> Result<TestIndex, std::io::Error>{
            self.ctx.call_jpc("index".to_string(), "create".to_string(), json!({"directory":  self.temp_dir}), false);
            Ok(TestIndex{
                ctx:self.ctx.clone(),
                temp_dir:self.temp_dir.clone(),
            })
        }
    }

    impl Default for FakeContext {
       fn default() -> Self {
            Self::new()
       }
    }

    impl FakeContext {
        pub fn new() -> FakeContext{
            FakeContext{
                id: Uuid::new_v4().to_string(),
                buf: vec![0; 5000000],
                ret_len:0,

            }
        }
        pub fn call_jpc(&mut self, object:String, method:String, params:serde_json::Value, do_ret:bool)-> Vec<u8>{
            let my_ret_ptr = &mut self.ret_len as *mut usize;
            let call_p = json!({
                "id":     self.id,
                "jpc":    "1.0",
                "obj":    object,
                "method": method,
                "params": params,
            });
            let sp = call_p.to_string();
            let ar = sp.as_ptr();
            let p = self.buf.as_mut_ptr();
            info!("calling tantivy_jpc json = {}", call_p);
            unsafe{
            tantivy_jpc(ar, sp.len(), p, my_ret_ptr);
            let sl = std::slice::from_raw_parts(p, self.ret_len);
            if do_ret{
                let v:serde_json::Value = serde_json::from_slice(sl).unwrap();
                info!("Val = {}", v);
                match std::str::from_utf8(sl){
                    Ok(s) => info!("stringified = {}", s),
                    Err(err) => panic!("ERROR = {err} sl = {sl:?}")
                };
                sl.to_vec()
            }else{
                println!("NO RETURNED REQUESTED");
                vec![]
            }
        }
        }
        pub fn add_text_field(&mut self, name:String, a_type:i32, stored:bool) -> i64{
            let j_param = json!({
                "name":   name,
                "type":   a_type,
                "stored": stored,
                "id":     self.id,
            });
            let s = &self.call_jpc("builder".to_string(), "add_text_field".to_string(), j_param, true);
            info!("builder ret  = {:?}", s);
            let i:serde_json::Value = serde_json::from_slice(s).unwrap();
            i["field"].as_i64().unwrap()
        }

        pub fn add_date_field(&mut self, name:String, a_type:i32, stored:bool) -> i64{
            let j_param = json!({
                "name":   name,
                "type":   a_type,
                "stored": stored,
                "id":     self.id,
            });
            call_simple_type!(self, j_param, "add_date_field")
        }
        pub fn add_i64_field(&mut self, name:String, a_type:i32, stored:bool) -> i64{
            let j_param = json!({
                "name":   name,
                "type":   a_type,
                "stored": stored,
                "id":     self.id,
            });
            call_simple_type!(self, j_param, "add_i64_field")
        }
        pub fn add_u64_field(&mut self, name:String, a_type:i32, stored:bool) -> i64{
            let j_param = json!({
                "name":   name,
                "type":   a_type,
                "stored": stored,
                "id":     self.id,
            });
            call_simple_type!(self, j_param, "add_u64_field")
        }
        pub fn add_f64_field(&mut self, name:String, a_type:i32, stored:bool) -> i64{
            let j_param = json!({
                "name":   name,
                "type":   a_type,
                "stored": stored,
                "id":     self.id,
            });
            call_simple_type!(self, j_param, "add_f64_field")
        }
        pub fn build(&mut self)  -> InternalCallResult<TestDocument> {
            let td = TempDir::new("TantivyBitcodeTest")?;
            let td_ref:&TempDir;
            let mut v:Vec<TempDir> = vec![td];
            unsafe{
                TEMPDIRS.append(v.as_mut());
                td_ref = TEMPDIRS.last().unwrap();
            }

            let s = self.call_jpc("builder".to_string(), "build".to_string(), json!({}), false);
            info!("build returned={:?}", s);
            Ok(TestDocument{
                ctx:self.clone(),
                temp_dir: td_ref.path().to_owned().to_str().unwrap().to_string(),
            })
        }
    }

    #[test]
    fn basic_index(){
        unsafe{init()};
        let mut ctx = FakeContext::new();
        assert_eq!(ctx.add_text_field("title".to_string(), 2, true), 0);
        assert_eq!(ctx.add_text_field("body".to_string(), 2, true), 1);
        let mut td = match ctx.build(){
            Ok(t) => t,
            Err(e) => {
                panic!("{}",format!("failed with error {}", e.to_string()));
            }
        };
        let doc1 = match td.create(){
            Ok(t) => t,
            Err(e) => {
                panic!("{}",format!("doc1 create failed error {}", e.to_string()));
            }
        };

        let doc2 = match td.create(){
            Ok(t) => t,
            Err(e) => {
                panic!("{}",format!("doc2 create failed error {}", e.to_string()));
            }
        };
        assert_eq!(td.add_text(0, "The Old Man and the Sea".to_string(), doc1 as u32), 0);
        assert_eq!(td.add_text(1, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish.".to_string(), doc1 as u32), 0);
        assert_eq!(td.add_text(0, "Of Mice and Men".to_string(), doc2 as u32), 0);
        assert_eq!(td.add_text(1, r#"A few miles south of Soledad, the Salinas River drops in close to the hillside bank and runs deep and green. The water is warm too, for it has slipped twinkling over the yellow sands in the sunlight before reaching the narrow pool. On one side of the river the golden foothill slopes curve up to the strong and rocky Gabilan Mountains, but on the valley side the water is lined with trees—willows fresh and green with every spring, carrying in their lower leaf junctures the debris of the winter's flooding; and sycamores with mottled, white, recumbent limbs and branches that arch over the pool"#.to_string(), doc2 as u32), 0);
        let mut ti = match td.create_index(){
            Ok(i) => i,
            Err(e) => panic!("failed to create index err ={} ", e)
        };
        let op1 = ti.add_document(doc1 as i32).unwrap();
        let op2 = ti.add_document(doc2 as i32).unwrap();
        assert_eq!(op1, 0);
        assert_eq!(op2, 1);
        ti.commit().unwrap();
        let mut rb = ti.reader_builder().unwrap();
        let mut qp = rb.searcher().unwrap();
        qp.for_index(vec!["title".to_string()]).unwrap();
        let mut searcher = qp.parse_query("Sea".to_string()).unwrap();
        let sres = &searcher.search().unwrap();
        let title_result:TestTitleResult = serde_json::from_str(sres).unwrap();
        assert_eq!(title_result.title[0], "The Old Man and the Sea".to_string());
    }
    // #[test]
    // fn from_existing(){
    //     let mut sess = TantivySession::new("test");
    //     match sess.handler_builder("add_text_field", "", json!({
    //         "name":   "title",
    //         "type":   2,
    //         "stored": true,
    //     })){
    //         Ok(x) => x,
    //         Err(e) => panic!("error={}",e),
    //     };
    //     match sess.handler_builder("add_text_field", "", json!({
    //         "name":   "body",
    //         "type":   2,
    //         "stored": true,
    //     })){
    //         Ok(x) => x,
    //         Err(e) => panic!("error={}",e),
    //     };
    //     match sess.handler_builder("build", "", json!({})){
    //         Ok(x) => x,
    //         Err(e) => panic!("error={}",e),
    //     };
    //     let idxO = sess.create_index(json!({"directory" : "/tmp/llvm_working_dir/140c52d6-c1a0-4e86-8422-b577a65aa7b0/hqp_JWSQEhKs5tEtc9kAPBrtKfrB3AVVc6omW8VcXgvr3p6hFbas"}));
    //     let idx = match idxO{
    //         Ok(i) => i,
    //         Err(e) => panic!("error={}",e),
    //     };
    // }

    #[test]
    fn all_simple_fields(){
        unsafe{init()};
        let mut ctx = FakeContext::new();
        assert_eq!(ctx.add_text_field("title".to_string(), 2, true), 0);
        assert_eq!(ctx.add_text_field("body".to_string(), 2, true), 1);
        assert_eq!(ctx.add_date_field("date".to_string(), 2, true), 2);
        assert_eq!(ctx.add_u64_field("someu64".to_string(), 2, true), 3);
        assert_eq!(ctx.add_i64_field("somei64".to_string(), 2, true), 4);
        assert_eq!(ctx.add_f64_field("somef64".to_string(), 2, true), 5);
    }
}
