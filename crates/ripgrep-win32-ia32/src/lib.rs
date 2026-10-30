//! Pinned ripgrep executable asset for win32-ia32. The pre-built binary
//! from the matching `@vscode/ripgrep` npm release is vendored into this crate
//! as `data/rg.xz` and embedded into the Rust binary at compile time via
//! `include_bytes!`; nothing is fetched from npm or the network at runtime.

#![no_std]

pub const TARGET_ID: &str = "win32-ia32";
pub const FILE_NAME: &str = "rg.exe";
pub const NPM_PACKAGE: &str = "@vscode/ripgrep-win32-ia32";
pub const NPM_VERSION: &str = "1.18.0";
pub const NPM_INTEGRITY: &str = "sha512-//rfAE+BOw5AC2EMmepmiE36jUuevtQYNQqqlw1s3m9FlRxjxEut97RkRPHAu9BG4mSojatZx+kXZXNdyI9caQ==";
pub const RG_XZ: &[u8] = include_bytes!("../data/rg.xz");
pub const RG_XZ_LEN: usize = 1435860;
pub const RG_XZ_SHA256: &str = "67eb31c1f231b6d64d73dfb1692c1eeab44e0d186508f1a5c93f46fa0bf668b9";
pub const RG_RAW_LEN: usize = 4761600;
pub const RG_RAW_SHA256: &str = "72ca0c53d04d8c13cd245af0aedcd1e12dd20aa24439cb0d1fc5951547f3cceb";
