//! Pinned ripgrep executable asset for linux-x64. The pre-built binary
//! from the matching `@vscode/ripgrep` npm release is vendored into this crate
//! as `data/rg.xz` and embedded into the Rust binary at compile time via
//! `include_bytes!`; nothing is fetched from npm or the network at runtime.

#![no_std]

pub const TARGET_ID: &str = "linux-x64";
pub const FILE_NAME: &str = "rg";
pub const NPM_PACKAGE: &str = "@vscode/ripgrep-linux-x64";
pub const NPM_VERSION: &str = "1.18.0";
pub const NPM_INTEGRITY: &str = "sha512-mQ3bVrUpnD2vs7QT0vX90Lt0cnUq467uFtEktIdsJJmW296RoSULRGqWgzG1AKxyBpNDD6l4ZO4qKf6SgyC23Q==";
pub const RG_XZ: &[u8] = include_bytes!("../data/rg.xz");
pub const RG_XZ_LEN: usize = 1742860;
pub const RG_XZ_SHA256: &str = "aa32a30e41ed2d72178f129b473748cb055d55d5dd700269e4d80dfe5aca3393";
pub const RG_RAW_LEN: usize = 5728032;
pub const RG_RAW_SHA256: &str = "193906679498de4d939345b937fa24e0e69a03c244bd70c859f5e41232713f21";
