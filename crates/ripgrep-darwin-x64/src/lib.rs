//! Pinned ripgrep executable asset for darwin-x64. The pre-built binary
//! from the matching `@vscode/ripgrep` npm release is vendored into this crate
//! as `data/rg.xz` and embedded into the Rust binary at compile time via
//! `include_bytes!`; nothing is fetched from npm or the network at runtime.

#![no_std]

pub const TARGET_ID: &str = "darwin-x64";
pub const FILE_NAME: &str = "rg";
pub const NPM_PACKAGE: &str = "@vscode/ripgrep-darwin-x64";
pub const NPM_VERSION: &str = "1.18.0";
pub const NPM_INTEGRITY: &str = "sha512-25b4gWbL138dGuQU244ebCKKc0q05ULBMoFSz9oAEUHNeqK/lOJViDS7DRvbDazzAzSEdan391Znks/R5mkaTQ==";
pub const RG_XZ: &[u8] = include_bytes!("../data/rg.xz");
pub const RG_XZ_LEN: usize = 1454004;
pub const RG_XZ_SHA256: &str = "2d2947e2eac9af3b261e50bb94dfaecec973aa990b3014ea2f6747d1ac6e2fed";
pub const RG_RAW_LEN: usize = 4742676;
pub const RG_RAW_SHA256: &str = "f999495980a5e6f1e7d26461ef5768b4013a62df610ed7d77a8b2de247a5b228";
