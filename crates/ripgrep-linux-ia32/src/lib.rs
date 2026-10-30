//! Pinned ripgrep executable asset for linux-ia32. The pre-built binary
//! from the matching `@vscode/ripgrep` npm release is vendored into this crate
//! as `data/rg.xz` and embedded into the Rust binary at compile time via
//! `include_bytes!`; nothing is fetched from npm or the network at runtime.

#![no_std]

pub const TARGET_ID: &str = "linux-ia32";
pub const FILE_NAME: &str = "rg";
pub const NPM_PACKAGE: &str = "@vscode/ripgrep-linux-ia32";
pub const NPM_VERSION: &str = "1.18.0";
pub const NPM_INTEGRITY: &str = "sha512-YWLkSUtFd4Jh5EepIhA9RJSfv3uMAVMo+2rBIGHPBnvgLrZciIs2cDKei1/p6Wc/aCzUoHyMAg2R6tw4ZCBKGg==";
pub const RG_XZ: &[u8] = include_bytes!("../data/rg.xz");
pub const RG_XZ_LEN: usize = 1457884;
pub const RG_XZ_SHA256: &str = "37c4c54b236e6d1e46adef3a32c8eef056a9af191a32cd0a78228f308ce52631";
pub const RG_RAW_LEN: usize = 4512608;
pub const RG_RAW_SHA256: &str = "47763deda9f135d94176258090162bc2b2faece1bb8b806c5d02be4e9ac43f18";
