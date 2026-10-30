//! Pinned ripgrep executable asset for win32-arm64. The pre-built binary
//! from the matching `@vscode/ripgrep` npm release is vendored into this crate
//! as `data/rg.xz` and embedded into the Rust binary at compile time via
//! `include_bytes!`; nothing is fetched from npm or the network at runtime.

#![no_std]

pub const TARGET_ID: &str = "win32-arm64";
pub const FILE_NAME: &str = "rg.exe";
pub const NPM_PACKAGE: &str = "@vscode/ripgrep-win32-arm64";
pub const NPM_VERSION: &str = "1.18.0";
pub const NPM_INTEGRITY: &str = "sha512-vfTIjq1OHnzUjxZcHVQAMbnggp8dpGf+0QKFOZHwWPqFwXxQC8eCWM+5NUdoJ6yrElCeMzoUTXoK/LdZaniB+Q==";
pub const RG_XZ: &[u8] = include_bytes!("../data/rg.xz");
pub const RG_XZ_LEN: usize = 1292600;
pub const RG_XZ_SHA256: &str = "cfad138e7e5c46f4ed44edb132b962a533051482f99251c049046890da92c173";
pub const RG_RAW_LEN: usize = 4883968;
pub const RG_RAW_SHA256: &str = "0015ba7dfb2d8b62c39332d64e50c6e85f75230125e10c50a950ed14995c79bf";
