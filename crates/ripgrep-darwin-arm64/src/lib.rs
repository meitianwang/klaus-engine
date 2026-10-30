//! Pinned ripgrep executable asset for darwin-arm64. The pre-built binary
//! from the matching `@vscode/ripgrep` npm release is vendored into this crate
//! as `data/rg.xz` and embedded into the Rust binary at compile time via
//! `include_bytes!`; nothing is fetched from npm or the network at runtime.

#![no_std]

pub const TARGET_ID: &str = "darwin-arm64";
pub const FILE_NAME: &str = "rg";
pub const NPM_PACKAGE: &str = "@vscode/ripgrep-darwin-arm64";
pub const NPM_VERSION: &str = "1.18.0";
pub const NPM_INTEGRITY: &str = "sha512-r3ktHSvbFycQNF6sl7sNDPocpsI7J+mEzh1IaZFkY0spm3k2Z9t8hPAeOK7+p0l6p6/swkQC14XWX01low+94Q==";
pub const RG_XZ: &[u8] = include_bytes!("../data/rg.xz");
pub const RG_XZ_LEN: usize = 1285164;
pub const RG_XZ_SHA256: &str = "ed6dfbf1e723614cb588496a398caa31afc438d6969d79b1fba3db28609fa6ad";
pub const RG_RAW_LEN: usize = 4528512;
pub const RG_RAW_SHA256: &str = "6ef40346bf31fcce79d9614c7745c198542925a0c7d4911e1ffe794c53392ac1";
