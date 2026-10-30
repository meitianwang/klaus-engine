//! The `apply_patch` text-format parser and applier (pure logic).
//!
//! The parser and applier are implemented in the `parser` and `apply`
//! submodules. The `tool` wrapper (which couples to the Tool trait) is
//! kept separate from this pure logic.

pub mod apply;
pub mod parser;

pub use apply::{
    apply_patch_ops, seek_sequence, ApplyError, ApplyKind, ApplyResult, FileIO, SeekHit, SeekPass,
};
pub use parser::{
    parse_patch, parse_patch_detailed, Hunk, HunkChange, HunkChangeType, ParsedPatch, PatchOp,
    PatchParseError,
};
pub mod tool;
