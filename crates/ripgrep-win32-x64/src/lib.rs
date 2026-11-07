//! Pinned ripgrep executable asset for win32-x64. The pre-built binary
//! from the matching `@vscode/ripgrep` npm release is vendored into this crate
//! as `data/rg.xz` and embedded into the Rust binary at compile time via
//! `include_bytes!`; nothing is fetched from npm or the network at runtime.

#![no_std]

pub const TARGET_ID: &str = "win32-x64";
pub const FILE_NAME: &str = "rg.exe";
pub const NPM_PACKAGE: &str = "@vscode/ripgrep-win32-x64";
pub const NPM_VERSION: &str = "1.18.0";
pub const NPM_INTEGRITY: &str = "sha512-KNPvtElldqILHdnAetujPaowkNbpqJy3ssIGGN6F6Kve9Qi+nNLI2DN01O83JjCEVQbCzl8Ov3QZ9Eov3BR8Dg==";
pub const RG_XZ: &[u8] = include_bytes!("../data/rg.xz");
pub const RG_XZ_LEN: usize = 1491984;
pub const RG_XZ_SHA256: &str = "5a04aad8d31242ca81d3792ddc1d25203fd88cca49a9a89e8e66a251061ad37e";
pub const RG_RAW_LEN: usize = 5429760;
pub const RG_RAW_SHA256: &str = "f9dde63498b3193f098355dbec97af99dc4f6b8fa0df5ed04114a03012c042cb";
