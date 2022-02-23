extern crate serde;
extern crate serde_derive;
extern crate serde_json;
extern crate lazy_static;
use log::{info};
use serde_json::json;
use serde_derive::{Serialize, Deserialize};
use std::str;
use std::collections::HashMap;
use tantivy::Document;
use tantivy::schema::{Field, TextOptions, Schema, SchemaBuilder};
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
    pub(crate) doc:Option<Box<tantivy::Document>>,
    pub(crate) builder:Option<Box<tantivy::schema::SchemaBuilder>>,
    pub(crate) schema:Option<tantivy::schema::Schema>,
}

impl<'a> TantivyEntry<'a>{
    fn new(id:&'a str) -> TantivyEntry<'a>{
        TantivyEntry{
            id,
            doc:None,
            builder:None,
            schema:None,
        }
    }
    pub fn do_method(&mut self, method:&str, obj: &str, params:serde_json::Value) -> *const u8{
        info!("In do_method");
        match obj {
            "document" => {
                info!("SchemaBuilder");
                let doc = self.doc.as_mut().take();
                let d = match doc{
                    Some(x) => x,
                    None => {
                        let dt: Document = Document::new();
                        let bt = Box::new(dt);
                        self.doc = Some(bt);
                        let t = self.doc.as_mut();
                        t.unwrap()
                    },
                };
                match method {
                    "add_text" => {
                        let x = d;
                        let f  = Field::from_field_id(0);
                        let s = params.as_str().unwrap();
                        info!("add_text: name = {}", s);
                        x.add_text(f,s);
                    },
                    &_ => {}
                };
            },
            "builder" => {
                info!("SchemaBuilder");
                match method {
                    "add_text_field" => {
                        let sb = match &mut self.builder{
                            Some(x) => x,
                            None => {
                                let isb = SchemaBuilder::default();
                                let bisb = Box::new(isb);
                                self.builder = Some(bisb);
                                let tb = self.builder.as_mut();
                                tb.unwrap()
                            }
                        };
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
        }
        let _ = &self.doc;
        let _ = &self.builder;
        vec![].as_ptr() as *const u8
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
pub fn make_json_error(err:&str, id:&str) -> *const u8{
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
    vr.as_bytes().as_ptr() as *const u8
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
  Finalized(&'static str),
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

pub type CallResult = std::result::Result<Vec<u8>, ErrorKinds>;


#[no_mangle]
pub unsafe extern "C" fn init() -> u8{
    env_logger::init();
    0
}
/**
jpc is the main entry point into a wasm bitcode for the web assembly procedure calls
this function will
# Steps
  * parse the input for the appropriately formatted json
  * construct a BitcodeContext from the json
  * attempt to call the method using the incomming path
  * return results to the caller
# Safety

*/
#[no_mangle]
pub unsafe extern "C" fn jpc<>(msg: *const u8, len:usize) -> *const u8 {
  info!("In jpc");
  let input_string = match str::from_utf8(std::slice::from_raw_parts(msg, len)){
      Ok(x) => x,
      Err(err) => return err.to_string().as_ptr() as *const u8
  };
  info!("parameters = {}", input_string);
  let json_params: Request = match serde_json::from_str(input_string){
    Ok(m) => {m},
    Err(_err) => {
      return make_json_error("parse failed for http", "ID not found");
    }
  };
  info!("Request parsed");
  let mut tm = TANTIVY_MAP.lock().unwrap();
  let entity:&mut TantivyEntry<'static> = match json_params.obj {
        "document" => {
            match tm.get_mut(json_params.id){
                Some(x) => x,
                None => {
                    let te = TantivyEntry::new(json_params.id);
                    tm.insert(json_params.id.to_owned(), te);
                    tm.get_mut(json_params.id).unwrap()
                },
            }
        }
        "builder" => {
            match tm.get_mut(json_params.id){
                Some(x) => x,
                None => {
                    let te = TantivyEntry::new(json_params.id);
                    tm.insert(json_params.id.to_owned(), te);
                    tm.get_mut(json_params.id).unwrap()
                },
            }
        }
        _ => return ErrorKinds::UnRecognizedCommand(json_params.method).to_string().as_ptr() as *const u8
    };
    entity.do_method(json_params.method, json_params.obj, json_params.params)
}
#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
