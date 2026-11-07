//! code-mode: the wire protocol for the model-facing runtime, the session
//! seam that decides WHERE cells run, and the in-process cell runtime that
//! backs the default carrier; the `code` tool itself lives in the `tools`
//! layer.

// `protocol`, `session`, and `host` stay `pub`: the crate's integration tests
// exercise the two wire contracts and the session seam from outside the crate.
// `runtime` is in-crate wiring only.
pub mod host;
pub mod host_service;
pub mod protocol;
pub mod remote_session;
pub(crate) mod runtime;
pub mod session;
