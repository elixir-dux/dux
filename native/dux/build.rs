fn main() {
    // DuckDB on Windows uses Restart Manager APIs (RmStartSession, etc.)
    // which require linking against rstrtmgr.lib
    if cfg!(target_os = "windows") {
        println!("cargo:rustc-link-lib=rstrtmgr");
    }
}
