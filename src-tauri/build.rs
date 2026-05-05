use std::path::PathBuf;

fn main() {
    tauri_build::build();

    // Embed an rpath so the runtime loader finds our patched
    // librdkafka in vendor/librdkafka/install/lib without the user
    // having to set DYLD_LIBRARY_PATH / LD_LIBRARY_PATH.
    // CARGO_MANIFEST_DIR is always set when cargo runs the build script;
    // fall back to "." if a downstream tool invokes us without it.
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_owned());
    let mut dylib_dir = PathBuf::from(manifest_dir);
    dylib_dir.pop();
    dylib_dir.push("vendor/librdkafka/install/lib");
    let dylib_dir = dylib_dir.to_string_lossy();

    if cfg!(target_os = "macos") {
        println!("cargo:rustc-link-arg=-Wl,-rpath,{dylib_dir}");
    } else if cfg!(target_os = "linux") {
        println!("cargo:rustc-link-arg=-Wl,-rpath,{dylib_dir}");
        println!("cargo:rustc-link-arg=-Wl,--enable-new-dtags");
    }
    // Windows: no rpath equivalent. Users must set PATH or copy the DLL
    // beside the executable.
}
