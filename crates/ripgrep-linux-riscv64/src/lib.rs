//! Pinned ripgrep executable asset for linux-riscv64. The pre-built binary
//! from the matching `@vscode/ripgrep` npm release is vendored into this crate
//! as `data/rg.xz` and embedded into the Rust binary at compile time via
//! `include_bytes!`; nothing is fetched from npm or the network at runtime.

#![no_std]

pub const TARGET_ID: &str = "linux-riscv64";
pub const FILE_NAME: &str = "rg";
pub const NPM_PACKAGE: &str = "@vscode/ripgrep-linux-riscv64";
pub const NPM_VERSION: &str = "1.18.0";
pub const NPM_INTEGRITY: &str = "sha512-f5kBQBrWfQt8Q7OhSORuNDei5dkYagBj3y4jImSUXGMy8B/Ke7SltSRcUtjPv166FAFfHCAmWuZp3+cWnX2/Vw==";
pub const RG_XZ: &[u8] = include_bytes!("../data/rg.xz");
pub const RG_XZ_LEN: usize = 1252272;
pub const RG_XZ_SHA256: &str = "22b32f8e859a2257bc4fa4edb442a1b682a6014bf74a32e1105ebf96360ac087";
pub const RG_RAW_LEN: usize = 3684352;
pub const RG_RAW_SHA256: &str = "c0a0ed0c04224325ea9b9929ef8a7daf5fa2a3a6713d6f466ac0df820c079808";
