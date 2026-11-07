fn main() {
    pyo3_build_config::use_pyo3_cfgs();
    // Patched: worker model is chosen by GRANIAN_MT_WORKERS at build time instead
    // of following the interpreter's Py_GIL_DISABLED. Unset = process workers
    // (MPServer, BUILD_GIL=true) even on a free-threaded interpreter.
    println!("cargo:rustc-check-cfg=cfg(granian_mt_workers)");
    println!("cargo:rerun-if-env-changed=GRANIAN_MT_WORKERS");
    if std::env::var("GRANIAN_MT_WORKERS").map(|v| v == "1").unwrap_or(false) {
        println!("cargo:rustc-cfg=granian_mt_workers");
    }
}