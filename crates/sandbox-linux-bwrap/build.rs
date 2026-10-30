use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=vendor/bubblewrap");
    println!("cargo:rerun-if-changed=include");
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("linux") {
        return;
    }
    let output = PathBuf::from(std::env::var_os("OUT_DIR").expect("OUT_DIR"));
    std::fs::write(
        output.join("config.h"),
        "#define PACKAGE_STRING \"bubblewrap 0.11.2\"\n#define ENABLE_REQUIRE_USERNS 1\n",
    )
    .expect("write bubblewrap build configuration");
    cc::Build::new()
        .include(&output)
        .include("include")
        .include("vendor/bubblewrap")
        .define("_GNU_SOURCE", None)
        .define("main", Some("klaus_bwrap_main"))
        .files([
            "vendor/bubblewrap/bubblewrap.c",
            "vendor/bubblewrap/bind-mount.c",
            "vendor/bubblewrap/network.c",
            "vendor/bubblewrap/utils.c",
        ])
        .warnings(false)
        .compile("klaus_bwrap");
}
