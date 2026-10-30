//! Windows execution boundaries and native sandbox preparation.
//!
//! The configured native level selects either a write-restricted caller token
//! or a prepared local sandbox account. Both preserve the ordinary read policy;
//! the elevated account also supports explicit read boundaries and managed
//! networking. A job object owns the process tree, and ConPTY forwards terminal
//! input and resize events.
//!
//! The engine re-enters its wrapper with a framed request over an inherited pipe.
//! Preparation completes before command launch, and the wrapper retains process,
//! terminal, and permission cleanup until the target has drained. The separate
//! AppContainer implementation remains available for explicit internal boundaries.

#[cfg(windows)]
mod access;
#[cfg(windows)]
pub use access::AccessError;

pub mod acl;
#[cfg(windows)]
mod acl_lease;
#[cfg(windows)]
mod acl_object;
pub mod acl_projection;
#[cfg(windows)]
mod acl_store;
pub mod boundary;
pub mod capability;
#[cfg(windows)]
pub mod elevated;
pub mod filter;
pub mod identity;
pub mod loopback;
pub mod native;
pub mod null_device;
pub mod preparation;
pub mod process;
pub mod protocol;
pub mod pty;
pub mod service;
pub mod unelevated;
pub mod wrapper;

pub use boundary::{AbsPath, FilesystemBoundary, NetworkBoundary, PathError};
pub use protocol::{
    parse_wrapper_argv, wrapper_argv, ControlMessage, ProtocolError, SandboxRequest, TerminalSize,
    WrapperHandles,
};

/// First argument of a wrapper invocation.
///
/// The engine's own binary is the wrapper, so this marker is what tells an
/// otherwise ordinary startup "you are the sandbox, not the agent". It is
/// deliberately not a plausible user-supplied argument.
pub const WRAPPER_ARG: &str = "--klaus-sandbox-win-wrapper";

/// Native process-security execution with explicit platform availability.
pub mod mxc;
