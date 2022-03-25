use std::env;
use std::path::PathBuf;

fn main() {
    let package_name = "tantivy_jpc";
    let os = env::var("CRATE_TARGET_DIR").unwrap_or("".to_string());
    let prof = std::env::var("CRATE_PROFILE").unwrap();
    let tt = env::consts::OS;
    let delivery_location:String;
    let suffix:String;
    match os {
        "macos" => {
            suffix = "dylib".to_string();
            delivery_location = "go-client/tantivy/packaged/lib/darwin-amd64/libtantivy_jpc.dylib";
        },
        "linux" => {
            suffix = "so".to_string();
            delivery_location = "go-client/tantivy/packaged/lib/linux-amd64/libtantivy_jpc.so";
        }
        _ => {
            panic!("Unkown os")
        }
    };

    let output_binary_source = target_dir()
        .join(format!("{}/lib{}.{}", prof, package_name, suffix))
        .display()
        .to_string();

    let output_binary_dest = PathBuf::from(env::var("CRATE_MANIFEST_DIR").unwrap())
        .join(delivery_location)
        .display()
        .to_string();


    if std::fs::copy(&output_binary_source, &output_binary_dest).is_err(){
        panic!("lib file copy {} to {} FAILED", &output_binary_source, &output_binary_dest);
    }

    println!("Success! copied {} to {} triple={}", &output_binary_source, &output_binary_dest, &tt);

}

/// Find the location of the `target/` directory. Note that this may be
/// overridden by `cmake`, so we also need to check the `CARGO_TARGET_DIR`
/// variable.
fn target_dir() -> PathBuf {
    if let Ok(target) = env::var("CRATE_TARGET_DIR") {
        PathBuf::from(target)
    } else {
        PathBuf::from(env::var("CRATE_MANIFEST_DIR").unwrap()).join("target")
    }
}
