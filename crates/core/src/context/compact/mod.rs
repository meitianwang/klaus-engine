//! Model-backed context checkpoints and provider-neutral window accounting.

pub mod auto;
pub mod engine;
pub mod full_compact;
pub mod outcome;
pub mod policy;
pub(crate) mod retry;
pub mod window;

pub use engine::{create_compactor, CompactorOptions};
pub use full_compact::{create_full_compactor, FullCompactorOptions};
pub use outcome::{
    CompactionDiagnostic, CompactionDiagnosticKind, CompactionDiagnosticStage, CompactionNoopReason,
};
