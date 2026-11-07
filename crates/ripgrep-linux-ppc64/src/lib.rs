//! Pinned ripgrep executable asset for linux-ppc64. The pre-built binary
//! from the matching `@vscode/ripgrep` npm release is vendored into this crate
//! as `data/rg.xz` and embedded into the Rust binary at compile time via
//! `include_bytes!`; nothing is fetched from npm or the network at runtime.

#![no_std]

pub const TARGET_ID: &str = "linux-ppc64";
pub const FILE_NAME: &str = "rg";
pub const NPM_PACKAGE: &str = "@vscode/ripgrep-linux-ppc64";
pub const NPM_VERSION: &str = "1.18.0";
pub const NPM_INTEGRITY: &str = "sha512-quXVY8fwQ8O/lvU1yrSqSl3jlUzysRSb+AfUfCL/tRtphxsKlFvPAejryZ6vg4Bgvn8XL74xb4qMCDmWgYrT5w==";
pub const RG_XZ: &[u8] = include_bytes!("../data/rg.xz");
pub const RG_XZ_LEN: usize = 1402556;
pub const RG_XZ_SHA256: &str = "0bb277ed61df04b5703e2da1d208c6969f6edba5b3261526c729250453a54614";
pub const RG_RAW_LEN: usize = 5967656;
pub const RG_RAW_SHA256: &str = "921b000ad16eda974d9dffc1780771c917b540429b7afc4f72d1742d742abbf2";
