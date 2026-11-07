//! Pinned ripgrep executable asset for linux-arm64. The pre-built binary
//! from the matching `@vscode/ripgrep` npm release is vendored into this crate
//! as `data/rg.xz` and embedded into the Rust binary at compile time via
//! `include_bytes!`; nothing is fetched from npm or the network at runtime.

#![no_std]

pub const TARGET_ID: &str = "linux-arm64";
pub const FILE_NAME: &str = "rg";
pub const NPM_PACKAGE: &str = "@vscode/ripgrep-linux-arm64";
pub const NPM_VERSION: &str = "1.18.0";
pub const NPM_INTEGRITY: &str = "sha512-lQ/5zTG++U0E3IhVgS4EPTTn/U4okncaRMM5GOFfOYZywS4nuD31GhkHbNYlDk5CuDC68+hYJ0/eQeyCKJDA+g==";
pub const RG_XZ: &[u8] = include_bytes!("../data/rg.xz");
pub const RG_XZ_LEN: usize = 1416008;
pub const RG_XZ_SHA256: &str = "91e3bc2ed6e225e4345ca89668896b5a0675bd6c64ed9560fdd6f4f96056a598";
pub const RG_RAW_LEN: usize = 4771240;
pub const RG_RAW_SHA256: &str = "e152ea689d6e8420357e592f0d8253b96476c164118ca3e6e13074fa1705ddda";
