extern crate cbindgen;
use std::env;
use std::path::PathBuf;
use cbindgen::{Config,Language,ParseConfig, ParseExpandConfig,Profile, ExportConfig, ItemType, MangleConfig,RenameRule};
use std::os::unix::fs;


fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    let package_name = env::var("CARGO_PKG_NAME").unwrap();
    let output_file = target_dir()
        .join(format!("{}.h", package_name))
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

    cbindgen::generate_with_config(&crate_dir, config)
      .unwrap()
      .write_to_file(&output_file);

    let output_link_source = target_dir()
      .join(format!("{}/lib{}.so", env::var("PROFILE").unwrap_or_else(|_| "release".to_string()), package_name))
      .display()
      .to_string();

    let output_link_dest = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap())
      .join(format!("go-client/lib{}.so", package_name))
      .display()
      .to_string();

    match std::fs::remove_file(output_link_dest.clone()){
        Ok(_) => {},
        Err(err) => println!("link probably doesn't exist {}", err)
    }

    match fs::symlink(output_link_source, output_link_dest){
        Ok(_) => {},
        Err(err) => {panic!("{}", err)}
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
