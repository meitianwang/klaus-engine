//! The code-mode host connection: the wire contract between an engine (client)
//! and a host that runs cells on its behalf.
//!
//! This is the SECOND wire in code-mode and must not be confused with
//! [`super::protocol`], which is the model-facing runtime contract. This one
//! carries the [`super::session`] seam across a process or network boundary:
//!
//! - operations travel client → host (open a session, execute, wait,
//!   terminate, shut down) and are answered by id;
//! - delegate callbacks travel host → client (invoke a nested tool, deliver
//!   status text) and are answered by id, so nested tool calls keep running
//!   through the ENGINE's pipeline even though the cell runs elsewhere;
//! - both directions can cancel an in-flight request by id.
//!
//! Frames are length-prefixed exact JSON ([`codec`]). Payload shapes are
//! version-pinned in [`payload`]: `Wire*` types own the V1 representation so
//! the internal runtime types stay free to evolve without silently changing
//! what goes over the connection.

pub mod codec;
pub mod error;
pub mod message;
pub mod payload;
pub mod types;

/// How many delegate calls one host may have in flight per connection.
///
/// The client answers every delegate request from its own task, so an unbounded
/// host could otherwise pin unbounded client work. Exceeding the bound fails
/// the offending call, not the connection.
pub const MAX_PENDING_DELEGATE_CALLS: usize = 512;

pub use codec::{EncodedFrame, FramedReader, FramedWriter, MAX_FRAME_BYTES};
pub use error::HandshakeRejectReason;
pub use message::{
    ClientHello, ClientHelloError, ClientToHost, DelegateRequest, DelegateResponse, HostHello,
    HostRequest, HostResponse, HostToClient, WireResult,
};
pub use payload::{
    WireCellId, WireContentItem, WireExecuteRequest, WireImageDetail, WireNestedToolCall,
    WireReplEvalRequest, WireReplOutcome, WireRuntimeResponse, WireToolDefinition, WireToolKind,
    WireWaitOutcome, WireWaitRequest,
};
pub use types::{
    Capability, CapabilitySet, DelegateRequestId, DuplicateCapability, InvalidIdentifier,
    InvalidSupportedProtocolVersions, ProtocolVersion, RequestId, SessionId,
    SupportedProtocolVersions, MAX_SAFE_CORRELATION_ID,
};
