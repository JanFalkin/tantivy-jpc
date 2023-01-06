extern crate cbindgen;
use std::env;
use std::path::PathBuf;
use cbindgen::{Config,Language,ParseConfig, ParseExpandConfig,Profile, ExportConfig, ItemType, MangleConfig,RenameRule};


fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    let package_name = env::var("CARGO_PKG_NAME").unwrap();
    let output_file = target_dir()
        .join(format!("{package_name}.h"))
        .display()
        .to_string();

    let output_secondary_header = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap())
        .join(format!("go-client/tantivy/packaged/include/{package_name}.h"))
        .display()
        .to_string();

    let config = Config {
        namespace: Some(String::from("ffi")),
        language: Language::C,
        parse: ParseConfig{
            parse_deps:true,
            clean:true,
            exclude : vec![],
            expand : ParseExpandConfig{
                crates: vec!["collections".to_owned()],
                all_features: true,
                default_features:true,
                features: None,
                profile:Profile::Release,
            },
            extra_bindings:vec![],
            include:Some(vec!["collections".to_owned()]),
        },
        export: ExportConfig{
            include: vec![],
            exclude: vec![],
            prefix : Some("".to_owned()),
            item_types : vec![ItemType::Enums, ItemType::Structs, ItemType::Functions],
            body: std::collections::HashMap::<String,String>::new(),
            mangle: MangleConfig{
                rename_types: RenameRule::default(),
                remove_underscores : false,
            },
            pre_body: std::collections::HashMap::<String,String>::new(),
            rename: std::collections::HashMap::<String,String>::new(),
            renaming_overrides_prefixing : false,
        },
        ..Default::default()
    };

    cbindgen::generate_with_config(crate_dir, config)
      .unwrap()
      .write_to_file(&output_file);
    // make a copy for go get
    if std::fs::copy(&output_file, &output_secondary_header).is_err(){
        panic!("header file copy {} to {} FAILED", &output_file, &output_secondary_header);
    }

}

/// Find the location of the `target/` directory. Note that this may be
/// overridden by `cmake`, so we also need to check the `CARGO_TARGET_DIR`
/// variable.
fn target_dir() -> PathBuf {
    if let Ok(target) = env::var("CARGO_TARGET_DIR") {
        PathBuf::from(target)
    } else {
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap()).join("target")
    }
}
