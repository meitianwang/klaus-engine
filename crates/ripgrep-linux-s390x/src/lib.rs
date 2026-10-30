//! Pinned ripgrep executable asset for linux-s390x. The pre-built binary
//! from the matching `@vscode/ripgrep` npm release is vendored into this crate
//! as `data/rg.xz` and embedded into the Rust binary at compile time via
//! `include_bytes!`; nothing is fetched from npm or the network at runtime.

#![no_std]

pub const TARGET_ID: &str = "linux-s390x";
pub const FILE_NAME: &str = "rg";
pub const NPM_PACKAGE: &str = "@vscode/ripgrep-linux-s390x";
pub const NPM_VERSION: &str = "1.18.0";
pub const NPM_INTEGRITY: &str = "sha512-rTOcJFGGcl2c07RUOWUo4U1ndnemKhY6A9hnMB18uk7jSgJc0d/QLBGWMWpumdtoJtpizn/wIv5mXIisJukusQ==";
pub const RG_XZ: &[u8] = include_bytes!("../data/rg.xz");
pub const RG_XZ_LEN: usize = 1156048;
pub const RG_XZ_SHA256: &str = "440a0d9462db39df01feae09b394aae6023955e37f26d56dbd830815c0d92eb4";
pub const RG_RAW_LEN: usize = 4836808;
pub const RG_RAW_SHA256: &str = "3932fc118f402eb72cc180d6238ed4d805a375c513613e3ce61320433cc6d760";
