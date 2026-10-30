//! Pinned ripgrep executable asset for linux-arm. The pre-built binary
//! from the matching `@vscode/ripgrep` npm release is vendored into this crate
//! as `data/rg.xz` and embedded into the Rust binary at compile time via
//! `include_bytes!`; nothing is fetched from npm or the network at runtime.

#![no_std]

pub const TARGET_ID: &str = "linux-arm";
pub const FILE_NAME: &str = "rg";
pub const NPM_PACKAGE: &str = "@vscode/ripgrep-linux-arm";
pub const NPM_VERSION: &str = "1.18.0";
pub const NPM_INTEGRITY: &str = "sha512-GDAvufNDHu8zqLEmXstalQF0Wh6wQvdsBi/Vg3Yi3CK4a8XoFXqqXVEHEZ9xQz3t0NfoSEc9JbvK9DDS6FxyxQ==";
pub const RG_XZ: &[u8] = include_bytes!("../data/rg.xz");
pub const RG_XZ_LEN: usize = 1138992;
pub const RG_XZ_SHA256: &str = "d9d52628013a19afc833f59732fc4a33238a3984d46004bb9bbca6c0b42b3967";
pub const RG_RAW_LEN: usize = 4115312;
pub const RG_RAW_SHA256: &str = "0d8353810445d0606cc3c714601fd6ed0465b03746107ce2bb173499ffc34a67";
