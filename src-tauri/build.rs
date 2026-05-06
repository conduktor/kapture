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

    // Skip the absolute build-machine rpath in release builds so the shipped
    // binary doesn't leak the maintainer's filesystem layout (Codex finding
    // [5]) and can't accidentally pick up a stale dylib that happens to
    // exist at the same path on a user's machine. Release builds resolve
    // dylibs through @executable_path/../Frameworks after relocation.
    let profile = std::env::var("PROFILE").unwrap_or_else(|_| "debug".to_owned());
    let is_release = profile == "release";

    if cfg!(target_os = "macos") {
        if !is_release {
            println!("cargo:rustc-link-arg=-Wl,-rpath,{dylib_dir}");
        }
        // Release builds are relocated into Kapture.app/Contents/Frameworks
        // by tools/relocate-macos-dylibs.sh. Embed the matching rpath so
        // the loader resolves @rpath/lib*.dylib at runtime from inside the
        // .app. Harmless in dev (the dir doesn't exist; the loader falls
        // through to the vendor/ rpath above).
        println!("cargo:rustc-link-arg=-Wl,-rpath,@executable_path/../Frameworks");
    } else if cfg!(target_os = "linux") {
        if !is_release {
            println!("cargo:rustc-link-arg=-Wl,-rpath,{dylib_dir}");
        }
        println!("cargo:rustc-link-arg=-Wl,--enable-new-dtags");
    }
    // Windows: no rpath equivalent. Users must set PATH or copy the DLL
    // beside the executable.
}
