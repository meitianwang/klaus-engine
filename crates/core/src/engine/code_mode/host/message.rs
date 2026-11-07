//! The framed messages of the host connection.
//!
//! Two independent request/response streams share one connection, each with its
//! own id space:
//!
//! - client → host [`HostRequest`], answered by [`HostToClient::Response`] (an
//!   `execute` is answered twice: once with the cell id, then once with the
//!   cell's first frontier as [`HostToClient::InitialResponse`]);
//! - host → client [`DelegateRequest`], answered by
//!   [`ClientToHost::DelegateResponse`].
//!
//! Either side may cancel an outstanding request of the stream it owns. Every
//! response carries [`WireResult`], so a failure is a normal answer rather than
//! a connection-level fault.

use std::fmt;

use crate::engine::code_mode::host::error::HandshakeRejectReason;
use crate::engine::code_mode::host::payload::{
    WireCellId, WireExecuteRequest, WireNestedToolCall, WireReplEvalRequest, WireReplOutcome,
    WireRuntimeResponse, WireWaitOutcome, WireWaitRequest,
};
use crate::engine::code_mode::host::types::{
    Capability, CapabilitySet, DelegateRequestId, ProtocolVersion, RequestId, SessionId,
    SupportedProtocolVersions,
};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};
use crate::types::json::Json;

/// The client's opening offer: the versions it speaks plus the capabilities it
/// demands (`required`) or merely accepts (`optional`).
///
/// A capability may not be both, which is why construction is fallible and the
/// decoder enforces the same rule.
#[derive(Clone, Debug, PartialEq)]
pub struct ClientHello {
    supported_versions: SupportedProtocolVersions,
    required_capabilities: CapabilitySet,
    optional_capabilities: CapabilitySet,
}

impl ClientHello {
    pub fn new(
        supported_versions: SupportedProtocolVersions,
        required_capabilities: CapabilitySet,
        optional_capabilities: CapabilitySet,
    ) -> Result<Self, ClientHelloError> {
        if let Some(capability) = required_capabilities
            .iter()
            .find(|capability| optional_capabilities.contains(capability))
        {
            return Err(ClientHelloError::OverlappingCapability(capability.clone()));
        }
        Ok(Self {
            supported_versions,
            required_capabilities,
            optional_capabilities,
        })
    }

    pub fn supported_versions(&self) -> &SupportedProtocolVersions {
        &self.supported_versions
    }

    pub fn required_capabilities(&self) -> &CapabilitySet {
        &self.required_capabilities
    }

    pub fn optional_capabilities(&self) -> &CapabilitySet {
        &self.optional_capabilities
    }
}

impl RuntimeEncode for ClientHello {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("supportedVersions", &self.supported_versions)?;
        object.field("requiredCapabilities", &self.required_capabilities)?;
        object.field("optionalCapabilities", &self.optional_capabilities)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ClientHello {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        let (supported_versions, required_capabilities, optional_capabilities) =
            runtime_codec::decode_object(value, true, |object| {
                Ok((
                    object.required("supportedVersions", &[])?,
                    object.required("requiredCapabilities", &[])?,
                    object.required("optionalCapabilities", &[])?,
                ))
            })?;
        Self::new(
            supported_versions,
            required_capabilities,
            optional_capabilities,
        )
        .map_err(|error| {
            RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(RuntimeString::from(
                error.to_string(),
            )))
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientHelloError {
    OverlappingCapability(Capability),
}

impl fmt::Display for ClientHelloError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OverlappingCapability(capability) => write!(
                formatter,
                "capability {capability} cannot be both required and optional"
            ),
        }
    }
}

impl std::error::Error for ClientHelloError {}

/// The host's acceptance: the single version now in force and the capabilities
/// it will honour for this connection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostHello {
    selected_version: ProtocolVersion,
    capabilities: CapabilitySet,
}

impl HostHello {
    pub fn new(selected_version: ProtocolVersion, capabilities: CapabilitySet) -> Self {
        Self {
            selected_version,
            capabilities,
        }
    }

    pub fn selected_version(&self) -> ProtocolVersion {
        self.selected_version
    }

    pub fn capabilities(&self) -> &CapabilitySet {
        &self.capabilities
    }
}

impl RuntimeEncode for HostHello {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("selectedVersion", &self.selected_version)?;
        object.field("capabilities", &self.capabilities)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for HostHello {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, true, |object| {
            Ok(Self {
                selected_version: object.required("selectedVersion", &[])?,
                capabilities: object.required("capabilities", &[])?,
            })
        })
    }
}

/// One operation a client asks the host to perform.
#[derive(Clone, Debug, PartialEq)]
pub enum HostRequest {
    /// Open a session; every later request names it by id.
    OpenSession { session_id: SessionId },
    Execute {
        session_id: SessionId,
        request: WireExecuteRequest,
    },
    Wait {
        session_id: SessionId,
        request: WireWaitRequest,
    },
    Terminate {
        session_id: SessionId,
        cell_id: WireCellId,
    },
    /// Cancel every cell of the session and forget it.
    ShutdownSession { session_id: SessionId },
    /// Evaluate one script in the session's persistent REPL runtime. Answered
    /// once, when the evaluation settles: [`HostResponse::ReplCompleted`].
    ReplEval {
        session_id: SessionId,
        request: WireReplEvalRequest,
    },
    /// Interrupt the running REPL evaluation named by `exec_id`, if any. This
    /// is idempotent: an unknown or already-finished id is a no-op success.
    ReplTerminate {
        session_id: SessionId,
        exec_id: RuntimeString,
    },
}

impl RuntimeEncode for HostRequest {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        match self {
            Self::OpenSession { session_id } => {
                object.field("sessionId", session_id)?;
                runtime_codec::encode_internal(out, "method", &[], "session/open", &object.finish())
            }
            Self::Execute {
                session_id,
                request,
            } => {
                object.field("sessionId", session_id)?;
                object.field("request", request)?;
                runtime_codec::encode_internal(
                    out,
                    "method",
                    &[],
                    "session/execute",
                    &object.finish(),
                )
            }
            Self::Wait {
                session_id,
                request,
            } => {
                object.field("sessionId", session_id)?;
                object.field("request", request)?;
                runtime_codec::encode_internal(out, "method", &[], "session/wait", &object.finish())
            }
            Self::Terminate {
                session_id,
                cell_id,
            } => {
                object.field("sessionId", session_id)?;
                object.field("cellId", cell_id)?;
                runtime_codec::encode_internal(
                    out,
                    "method",
                    &[],
                    "session/terminate",
                    &object.finish(),
                )
            }
            Self::ShutdownSession { session_id } => {
                object.field("sessionId", session_id)?;
                runtime_codec::encode_internal(
                    out,
                    "method",
                    &[],
                    "session/shutdown",
                    &object.finish(),
                )
            }
            Self::ReplEval {
                session_id,
                request,
            } => {
                object.field("sessionId", session_id)?;
                object.field("request", request)?;
                runtime_codec::encode_internal(out, "method", &[], "repl/eval", &object.finish())
            }
            Self::ReplTerminate {
                session_id,
                exec_id,
            } => {
                object.field("sessionId", session_id)?;
                object.field("execId", exec_id)?;
                runtime_codec::encode_internal(
                    out,
                    "method",
                    &[],
                    "repl/terminate",
                    &object.finish(),
                )
            }
        }
    }
}

impl RuntimeDecode for HostRequest {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            true,
            "method",
            &[],
            &[
                runtime_codec::RuntimeEnumVariant::new("session/open", &[]),
                runtime_codec::RuntimeEnumVariant::new("session/execute", &[]),
                runtime_codec::RuntimeEnumVariant::new("session/wait", &[]),
                runtime_codec::RuntimeEnumVariant::new("session/terminate", &[]),
                runtime_codec::RuntimeEnumVariant::new("session/shutdown", &[]),
                runtime_codec::RuntimeEnumVariant::new("repl/eval", &[]),
                runtime_codec::RuntimeEnumVariant::new("repl/terminate", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::OpenSession {
                    session_id: object.required("sessionId", &[])?,
                }),
                1 => Ok(Self::Execute {
                    session_id: object.required("sessionId", &[])?,
                    request: object.required("request", &[])?,
                }),
                2 => Ok(Self::Wait {
                    session_id: object.required("sessionId", &[])?,
                    request: object.required("request", &[])?,
                }),
                3 => Ok(Self::Terminate {
                    session_id: object.required("sessionId", &[])?,
                    cell_id: object.required("cellId", &[])?,
                }),
                4 => Ok(Self::ShutdownSession {
                    session_id: object.required("sessionId", &[])?,
                }),
                5 => Ok(Self::ReplEval {
                    session_id: object.required("sessionId", &[])?,
                    request: object.required("request", &[])?,
                }),
                _ => Ok(Self::ReplTerminate {
                    session_id: object.required("sessionId", &[])?,
                    exec_id: object.required("execId", &[])?,
                }),
            },
        )
    }
}

/// The host's answer to one [`HostRequest`].
#[derive(Clone, Debug, PartialEq)]
pub enum HostResponse {
    SessionReady {
        session_id: SessionId,
    },
    /// The cell was admitted; its first frontier follows separately as
    /// [`HostToClient::InitialResponse`] under the same request id.
    ExecutionStarted {
        cell_id: WireCellId,
    },
    WaitCompleted {
        outcome: WireWaitOutcome,
    },
    SessionClosed {
        session_id: SessionId,
    },
    /// The REPL evaluation settled; the outcome carries the console sections,
    /// the formatted last-expression text and the structured result value.
    ReplCompleted {
        outcome: WireReplOutcome,
    },
    /// The named REPL evaluation was asked to interrupt.
    ReplTerminated {
        exec_id: RuntimeString,
    },
}

impl RuntimeEncode for HostResponse {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        match self {
            Self::SessionReady { session_id } => {
                object.field("sessionId", session_id)?;
                runtime_codec::encode_internal(out, "type", &[], "session/ready", &object.finish())
            }
            Self::ExecutionStarted { cell_id } => {
                object.field("cellId", cell_id)?;
                runtime_codec::encode_internal(
                    out,
                    "type",
                    &[],
                    "execution/started",
                    &object.finish(),
                )
            }
            Self::WaitCompleted { outcome } => {
                object.field("outcome", outcome)?;
                runtime_codec::encode_internal(out, "type", &[], "wait/completed", &object.finish())
            }
            Self::SessionClosed { session_id } => {
                object.field("sessionId", session_id)?;
                runtime_codec::encode_internal(out, "type", &[], "session/closed", &object.finish())
            }
            Self::ReplCompleted { outcome } => {
                object.field("outcome", outcome)?;
                runtime_codec::encode_internal(out, "type", &[], "repl/completed", &object.finish())
            }
            Self::ReplTerminated { exec_id } => {
                object.field("execId", exec_id)?;
                runtime_codec::encode_internal(
                    out,
                    "type",
                    &[],
                    "repl/terminated",
                    &object.finish(),
                )
            }
        }
    }
}

impl RuntimeDecode for HostResponse {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            true,
            "type",
            &[],
            &[
                runtime_codec::RuntimeEnumVariant::new("session/ready", &[]),
                runtime_codec::RuntimeEnumVariant::new("execution/started", &[]),
                runtime_codec::RuntimeEnumVariant::new("wait/completed", &[]),
                runtime_codec::RuntimeEnumVariant::new("session/closed", &[]),
                runtime_codec::RuntimeEnumVariant::new("repl/completed", &[]),
                runtime_codec::RuntimeEnumVariant::new("repl/terminated", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::SessionReady {
                    session_id: object.required("sessionId", &[])?,
                }),
                1 => Ok(Self::ExecutionStarted {
                    cell_id: object.required("cellId", &[])?,
                }),
                2 => Ok(Self::WaitCompleted {
                    outcome: object.required("outcome", &[])?,
                }),
                3 => Ok(Self::SessionClosed {
                    session_id: object.required("sessionId", &[])?,
                }),
                4 => Ok(Self::ReplCompleted {
                    outcome: object.required("outcome", &[])?,
                }),
                _ => Ok(Self::ReplTerminated {
                    exec_id: object.required("execId", &[])?,
                }),
            },
        )
    }
}

/// One callback the host asks the client's delegate to perform.
#[derive(Clone, Debug, PartialEq)]
pub enum DelegateRequest {
    InvokeTool {
        invocation: WireNestedToolCall,
    },
    Notify {
        call_id: RuntimeString,
        cell_id: WireCellId,
        text: RuntimeString,
    },
}

impl RuntimeEncode for DelegateRequest {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        match self {
            Self::InvokeTool { invocation } => {
                object.field("invocation", invocation)?;
                runtime_codec::encode_internal(out, "type", &[], "tool/invoke", &object.finish())
            }
            Self::Notify {
                call_id,
                cell_id,
                text,
            } => {
                object.field("callId", call_id)?;
                object.field("cellId", cell_id)?;
                object.field("text", text)?;
                runtime_codec::encode_internal(
                    out,
                    "type",
                    &[],
                    "notification/send",
                    &object.finish(),
                )
            }
        }
    }
}

impl RuntimeDecode for DelegateRequest {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            true,
            "type",
            &[],
            &[
                runtime_codec::RuntimeEnumVariant::new("tool/invoke", &[]),
                runtime_codec::RuntimeEnumVariant::new("notification/send", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::InvokeTool {
                    invocation: object.required("invocation", &[])?,
                }),
                _ => Ok(Self::Notify {
                    call_id: object.required("callId", &[])?,
                    cell_id: object.required("cellId", &[])?,
                    text: object.required("text", &[])?,
                }),
            },
        )
    }
}

/// The client's answer to one [`DelegateRequest`].
#[derive(Clone, Debug, PartialEq)]
pub enum DelegateResponse {
    ToolResult { result: Json },
    NotificationDelivered,
}

impl RuntimeEncode for DelegateResponse {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        match self {
            Self::ToolResult { result } => {
                object.field("result", result)?;
                runtime_codec::encode_internal(out, "type", &[], "tool/result", &object.finish())
            }
            Self::NotificationDelivered => runtime_codec::encode_internal(
                out,
                "type",
                &[],
                "notification/delivered",
                &object.finish(),
            ),
        }
    }
}

impl RuntimeDecode for DelegateResponse {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            true,
            "type",
            &[],
            &[
                runtime_codec::RuntimeEnumVariant::new("tool/result", &[]),
                runtime_codec::RuntimeEnumVariant::new("notification/delivered", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::ToolResult {
                    result: object.required("result", &[])?,
                }),
                _ => Ok(Self::NotificationDelivered),
            },
        )
    }
}

/// A response that may have failed: transported as data so one failed operation
/// never tears down the connection.
#[derive(Clone, Debug, PartialEq)]
pub enum WireResult<T> {
    Ok { value: T },
    Err { message: RuntimeString },
}

impl<T> WireResult<T> {
    pub fn from_result(result: Result<T, RuntimeString>) -> Self {
        match result {
            Ok(value) => Self::Ok { value },
            Err(message) => Self::Err { message },
        }
    }

    pub fn into_result(self) -> Result<T, RuntimeString> {
        match self {
            Self::Ok { value } => Ok(value),
            Self::Err { message } => Err(message),
        }
    }
}

impl<T: RuntimeEncode> RuntimeEncode for WireResult<T> {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        match self {
            Self::Ok { value } => {
                object.field("value", value)?;
                runtime_codec::encode_internal(out, "status", &[], "ok", &object.finish())
            }
            Self::Err { message } => {
                object.field("message", message)?;
                runtime_codec::encode_internal(out, "status", &[], "error", &object.finish())
            }
        }
    }
}

impl<T: RuntimeDecode> RuntimeDecode for WireResult<T> {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            true,
            "status",
            &[],
            &[
                runtime_codec::RuntimeEnumVariant::new("ok", &[]),
                runtime_codec::RuntimeEnumVariant::new("error", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::Ok {
                    value: object.required("value", &[])?,
                }),
                _ => Ok(Self::Err {
                    message: object.required("message", &[])?,
                }),
            },
        )
    }
}

/// Every frame a client may send.
#[derive(Clone, Debug, PartialEq)]
pub enum ClientToHost {
    ClientHello(ClientHello),
    Request {
        id: RequestId,
        request: HostRequest,
    },
    CancelRequest {
        id: RequestId,
    },
    DelegateResponse {
        id: DelegateRequestId,
        result: WireResult<DelegateResponse>,
    },
}

impl RuntimeEncode for ClientToHost {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        match self {
            Self::ClientHello(hello) => {
                object.flatten(hello)?;
                runtime_codec::encode_internal(
                    out,
                    "type",
                    &[],
                    "connection/hello",
                    &object.finish(),
                )
            }
            Self::Request { id, request } => {
                object.field("id", id)?;
                object.field("request", request)?;
                runtime_codec::encode_internal(
                    out,
                    "type",
                    &[],
                    "operation/request",
                    &object.finish(),
                )
            }
            Self::CancelRequest { id } => {
                object.field("id", id)?;
                runtime_codec::encode_internal(
                    out,
                    "type",
                    &[],
                    "operation/cancel",
                    &object.finish(),
                )
            }
            Self::DelegateResponse { id, result } => {
                object.field("id", id)?;
                object.field("result", result)?;
                runtime_codec::encode_internal(
                    out,
                    "type",
                    &[],
                    "delegate/response",
                    &object.finish(),
                )
            }
        }
    }
}

impl RuntimeDecode for ClientToHost {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            true,
            "type",
            &[],
            &[
                runtime_codec::RuntimeEnumVariant::new("connection/hello", &[]),
                runtime_codec::RuntimeEnumVariant::new("operation/request", &[]),
                runtime_codec::RuntimeEnumVariant::new("operation/cancel", &[]),
                runtime_codec::RuntimeEnumVariant::new("delegate/response", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::ClientHello(
                    ClientHello::new(
                        object.required("supportedVersions", &[])?,
                        object.required("requiredCapabilities", &[])?,
                        object.required("optionalCapabilities", &[])?,
                    )
                    .map_err(|error| {
                        RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(RuntimeString::from(
                            error.to_string(),
                        )))
                    })?,
                )),
                1 => Ok(Self::Request {
                    id: object.required("id", &[])?,
                    request: object.required("request", &[])?,
                }),
                2 => Ok(Self::CancelRequest {
                    id: object.required("id", &[])?,
                }),
                _ => Ok(Self::DelegateResponse {
                    id: object.required("id", &[])?,
                    result: object.required("result", &[])?,
                }),
            },
        )
    }
}

/// Every frame a host may send.
#[derive(Clone, Debug, PartialEq)]
pub enum HostToClient {
    HostHello(HostHello),
    HandshakeRejected {
        reason: HandshakeRejectReason,
    },
    Response {
        id: RequestId,
        result: WireResult<HostResponse>,
    },
    /// The first frontier of a cell started by the `execute` with this id.
    InitialResponse {
        id: RequestId,
        result: WireResult<WireRuntimeResponse>,
    },
    DelegateRequest {
        id: DelegateRequestId,
        session_id: SessionId,
        request: DelegateRequest,
    },
    CancelDelegateRequest {
        id: DelegateRequestId,
    },
    CellClosed {
        session_id: SessionId,
        cell_id: WireCellId,
    },
}

impl RuntimeEncode for HostToClient {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        match self {
            Self::HostHello(hello) => {
                object.flatten(hello)?;
                runtime_codec::encode_internal(
                    out,
                    "type",
                    &[],
                    "connection/ready",
                    &object.finish(),
                )
            }
            Self::HandshakeRejected { reason } => {
                object.field("reason", reason)?;
                runtime_codec::encode_internal(
                    out,
                    "type",
                    &[],
                    "connection/rejected",
                    &object.finish(),
                )
            }
            Self::Response { id, result } => {
                object.field("id", id)?;
                object.field("result", result)?;
                runtime_codec::encode_internal(
                    out,
                    "type",
                    &[],
                    "operation/response",
                    &object.finish(),
                )
            }
            Self::InitialResponse { id, result } => {
                object.field("id", id)?;
                object.field("result", result)?;
                runtime_codec::encode_internal(
                    out,
                    "type",
                    &[],
                    "execute/initialResponse",
                    &object.finish(),
                )
            }
            Self::DelegateRequest {
                id,
                session_id,
                request,
            } => {
                object.field("id", id)?;
                object.field("sessionId", session_id)?;
                object.field("request", request)?;
                runtime_codec::encode_internal(
                    out,
                    "type",
                    &[],
                    "delegate/request",
                    &object.finish(),
                )
            }
            Self::CancelDelegateRequest { id } => {
                object.field("id", id)?;
                runtime_codec::encode_internal(
                    out,
                    "type",
                    &[],
                    "delegate/cancel",
                    &object.finish(),
                )
            }
            Self::CellClosed {
                session_id,
                cell_id,
            } => {
                object.field("sessionId", session_id)?;
                object.field("cellId", cell_id)?;
                runtime_codec::encode_internal(out, "type", &[], "cell/closed", &object.finish())
            }
        }
    }
}

impl RuntimeDecode for HostToClient {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            true,
            "type",
            &[],
            &[
                runtime_codec::RuntimeEnumVariant::new("connection/ready", &[]),
                runtime_codec::RuntimeEnumVariant::new("connection/rejected", &[]),
                runtime_codec::RuntimeEnumVariant::new("operation/response", &[]),
                runtime_codec::RuntimeEnumVariant::new("execute/initialResponse", &[]),
                runtime_codec::RuntimeEnumVariant::new("delegate/request", &[]),
                runtime_codec::RuntimeEnumVariant::new("delegate/cancel", &[]),
                runtime_codec::RuntimeEnumVariant::new("cell/closed", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::HostHello(HostHello::new(
                    object.required("selectedVersion", &[])?,
                    object.required("capabilities", &[])?,
                ))),
                1 => Ok(Self::HandshakeRejected {
                    reason: object.required("reason", &[])?,
                }),
                2 => Ok(Self::Response {
                    id: object.required("id", &[])?,
                    result: object.required("result", &[])?,
                }),
                3 => Ok(Self::InitialResponse {
                    id: object.required("id", &[])?,
                    result: object.required("result", &[])?,
                }),
                4 => Ok(Self::DelegateRequest {
                    id: object.required("id", &[])?,
                    session_id: object.required("sessionId", &[])?,
                    request: object.required("request", &[])?,
                }),
                5 => Ok(Self::CancelDelegateRequest {
                    id: object.required("id", &[])?,
                }),
                _ => Ok(Self::CellClosed {
                    session_id: object.required("sessionId", &[])?,
                    cell_id: object.required("cellId", &[])?,
                }),
            },
        )
    }
}
