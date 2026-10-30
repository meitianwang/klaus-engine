//! The `workflow` submodule: run a deterministic orchestration script that
//! fans out and coordinates many sub-agents.
//!
//! Layout:
//!  - [`meta`]         — parse + validate the static `meta` header a workflow
//!    script must begin with (self-contained: the object
//!    literal is evaluated natively, no host callback).
//!  - [`discover`]     — load disk-registered named workflows and persist new
//!    ones (the `.klaus/workflows/` registry).
//!  - [`builtin_workflows`] — the workflows the engine ships by default.
//!  - [`runtime`]      — run a workflow script to completion: budget, journal
//!    cache, caps, spawn, accounting. The script-execution
//!    step (the host's non-portable scripting VM) is modeled
//!    as the injected [`runtime::WorkflowScriptRunner`] seam;
//!    the surrounding orchestration lives here.

pub mod builtin_workflows;
pub mod discover;
pub mod meta;
pub mod runtime;

use std::fmt;

use crate::runtime_values::RuntimeString;

/// An error raised while parsing or running a workflow.
///
/// Carries only the human-facing message — the sole thing surfaced when a
/// workflow throws (a thrown error's `.message`). The runtime draws a
/// `TypeError` vs `Error` distinction internally, but it is NOT observable
/// (both are caught identically and only the message is read), so a single
/// type suffices; [`WorkflowError::type_error`] exists purely to document
/// intent at the call site.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowError {
    pub message: RuntimeString,
}

impl WorkflowError {
    /// A plain workflow failure.
    pub fn new(message: impl Into<RuntimeString>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// A `TypeError`-flavored failure. Behaves identically to
    /// [`WorkflowError::new`] (the constructor name never reaches the surfaced
    /// message).
    pub fn type_error(message: impl Into<RuntimeString>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for WorkflowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let projected = self.message.to_node_utf8();
        f.write_str(std::str::from_utf8(&projected).expect("Node UTF-8 projection is valid"))
    }
}

impl std::error::Error for WorkflowError {}
