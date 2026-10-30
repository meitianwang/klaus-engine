//! Engine and provider error types.

use serde::{Deserialize, Serialize};
use std::fmt;

use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};

fn write_node_projected_runtime_string(
    formatter: &mut fmt::Formatter<'_>,
    value: &RuntimeString,
) -> fmt::Result {
    let bytes = value.to_node_utf8();
    formatter.write_str(std::str::from_utf8(&bytes).expect("Node UTF-8 projection is valid UTF-8"))
}

fn decode_provider_unit_enum<T: Copy>(
    value: &RuntimeJson,
    variants: &[RuntimeEnumVariant<'_>],
    values: &[T],
) -> Result<T, RuntimeCodecError> {
    runtime_codec::decode_external(value, variants, |variant| match variant {
        RuntimeExternalVariant::Unit { variant } => values
            .get(variant)
            .copied()
            .ok_or_else(|| RuntimeCodecError::new(RuntimeCodecErrorKind::UnknownEnumVariant)),
        RuntimeExternalVariant::Payload { .. } => Err(RuntimeCodecError::new(
            RuntimeCodecErrorKind::InvalidEnumShape,
        )),
    })
}

/// Why a provider stream errored. Drives the retry/recovery matrix (§0.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderErrorKind {
    /// 401/403 — refresh + single retry, else terminal.
    Auth,
    /// 429 — backoff, honor Retry-After.
    RateLimit,
    /// 5xx — backoff.
    Overloaded,
    /// The input exceeds the model's token context window.
    PromptTooLong,
    /// The serialized request or media payload exceeds the service's byte limit.
    RequestTooLarge,
    /// billing/usage exhausted (insufficient_quota / usage_limit_reached) —
    /// terminal, not a transient 429.
    Quota,
    /// output truncated — escalate (enhancement).
    MaxOutput,
    /// content filtered.
    Withheld,
    /// transport failure.
    Network,
    /// A configured hard deadline elapsed. Terminal: retrying would reset the
    /// same deadline and turn a finite policy into an infinite loop.
    Timeout,
    /// 400 — non-retryable.
    InvalidRequest,
    Unknown,
}

impl RuntimeEncode for ProviderErrorKind {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Auth => "auth",
            Self::RateLimit => "rate_limit",
            Self::Overloaded => "overloaded",
            Self::PromptTooLong => "prompt_too_long",
            Self::RequestTooLarge => "request_too_large",
            Self::Quota => "quota",
            Self::MaxOutput => "max_output",
            Self::Withheld => "withheld",
            Self::Network => "network",
            Self::Timeout => "timeout",
            Self::InvalidRequest => "invalid_request",
            Self::Unknown => "unknown",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for ProviderErrorKind {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("auth", &[]),
                RuntimeEnumVariant::new("rate_limit", &[]),
                RuntimeEnumVariant::new("overloaded", &[]),
                RuntimeEnumVariant::new("prompt_too_long", &[]),
                RuntimeEnumVariant::new("request_too_large", &[]),
                RuntimeEnumVariant::new("quota", &[]),
                RuntimeEnumVariant::new("max_output", &[]),
                RuntimeEnumVariant::new("withheld", &[]),
                RuntimeEnumVariant::new("network", &[]),
                RuntimeEnumVariant::new("timeout", &[]),
                RuntimeEnumVariant::new("invalid_request", &[]),
                RuntimeEnumVariant::new("unknown", &[]),
            ],
            &[
                Self::Auth,
                Self::RateLimit,
                Self::Overloaded,
                Self::PromptTooLong,
                Self::RequestTooLarge,
                Self::Quota,
                Self::MaxOutput,
                Self::Withheld,
                Self::Network,
                Self::Timeout,
                Self::InvalidRequest,
                Self::Unknown,
            ],
        )
    }
}

impl ProviderErrorKind {
    /// The wire/string form (`'auth' | 'rate_limit' | ...`).
    pub fn as_str(&self) -> &'static str {
        match self {
            ProviderErrorKind::Auth => "auth",
            ProviderErrorKind::RateLimit => "rate_limit",
            ProviderErrorKind::Overloaded => "overloaded",
            ProviderErrorKind::PromptTooLong => "prompt_too_long",
            ProviderErrorKind::RequestTooLarge => "request_too_large",
            ProviderErrorKind::Quota => "quota",
            ProviderErrorKind::MaxOutput => "max_output",
            ProviderErrorKind::Withheld => "withheld",
            ProviderErrorKind::Network => "network",
            ProviderErrorKind::Timeout => "timeout",
            ProviderErrorKind::InvalidRequest => "invalid_request",
            ProviderErrorKind::Unknown => "unknown",
        }
    }
}

/// One native media block after all wire projection and request-budget filters.
/// The transport attaches this request-local map to a rejection so its reported
/// coordinates can repair the original content rather than an already-pruned block.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderMediaOrigin {
    pub wire_message_index: usize,
    pub wire_content_index: usize,
    pub wire_output_index: Option<usize>,
    pub message_index: usize,
    pub content_index: Option<usize>,
    pub extra_user_blocks: bool,
    pub output_index: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderError {
    pub kind: ProviderErrorKind,
    pub message: RuntimeString,
    pub status: Option<u16>,
    pub code: Option<RuntimeString>,
    pub error_type: Option<RuntimeString>,
    /// Internal per-request origins, not a diagnostic wire field. `Some([])`
    /// means the final request contained no native media.
    #[doc(hidden)]
    pub media_origins: Option<Vec<ProviderMediaOrigin>>,
    /// The transport has finished request-local media recovery. Upper layers
    /// must not rebuild the original body or open another recovery episode.
    #[doc(hidden)]
    pub media_repair_complete: bool,
    pub retry_after_ms: Option<u64>,
    /// Server-assigned request id for the failed call — diagnostic only; never
    /// drives retry.
    pub request_id: Option<RuntimeString>,
}

/// The transport boundary at which a native failure occurred.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderFailurePhase {
    Establishment,
    Streaming,
}

impl ProviderError {
    pub fn failure_phase(&self) -> ProviderFailurePhase {
        if self
            .code
            .as_ref()
            .is_some_and(|code| code == "klaus_stream_interrupted")
        {
            ProviderFailurePhase::Streaming
        } else {
            ProviderFailurePhase::Establishment
        }
    }

    /// Mark a local read failure without replacing an upstream error code.
    pub fn mark_stream_interrupted(mut self) -> Self {
        if self.status.is_none() && self.code.is_none() {
            self.code = Some("klaus_stream_interrupted".into());
        }
        self
    }
}

impl RuntimeEncode for ProviderError {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("kind", &self.kind)?;
        object.field("message", &self.message)?;
        if let Some(value) = &self.status {
            object.field("status", value)?;
        }
        if let Some(value) = &self.code {
            object.field("code", value)?;
        }
        if let Some(value) = &self.error_type {
            object.field("type", value)?;
        }
        if let Some(value) = &self.retry_after_ms {
            object.field("retryAfterMs", value)?;
        }
        if let Some(value) = &self.request_id {
            object.field("requestId", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ProviderError {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                kind: object.required("kind", &[])?,
                message: object.required("message", &[])?,
                status: object.optional("status", &[])?,
                code: object.optional("code", &[])?,
                error_type: object.optional("type", &[])?,
                media_origins: None,
                media_repair_complete: false,
                retry_after_ms: object.optional("retryAfterMs", &[])?,
                request_id: object.optional("requestId", &[])?,
            })
        })
    }
}

impl fmt::Display for ProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_node_projected_runtime_string(f, &self.message)
    }
}

impl std::error::Error for ProviderError {}

/// The severity of a [`ToolError`]. `Model` errors become tool_result errors
/// the model can recover from; `Fatal` halts the turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ToolErrorKind {
    Model,
    Fatal,
}

impl RuntimeEncode for ToolErrorKind {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Model => "model",
            Self::Fatal => "fatal",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for ToolErrorKind {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("model", &[]),
                RuntimeEnumVariant::new("fatal", &[]),
            ],
            &[Self::Model, Self::Fatal],
        )
    }
}

/// A tool failure. `model` errors become tool_result errors the model can
/// recover from; `fatal` halts the turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolError {
    pub kind: ToolErrorKind,
    pub message: RuntimeString,
}

impl ToolError {
    /// A recoverable (`model`) tool error — the default kind.
    pub fn new(message: impl Into<RuntimeString>) -> Self {
        ToolError {
            kind: ToolErrorKind::Model,
            message: message.into(),
        }
    }

    /// A tool error of the given severity.
    pub fn with_kind(message: impl Into<RuntimeString>, kind: ToolErrorKind) -> Self {
        ToolError {
            kind,
            message: message.into(),
        }
    }

    /// A fatal tool error that halts the turn.
    pub fn fatal(message: impl Into<RuntimeString>) -> Self {
        ToolError::with_kind(message, ToolErrorKind::Fatal)
    }
}

impl RuntimeEncode for ToolError {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("kind", &self.kind)?;
        object.field("message", &self.message)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ToolError {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                kind: object.required("kind", &[])?,
                message: object.required("message", &[])?,
            })
        })
    }
}

impl fmt::Display for ToolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_node_projected_runtime_string(f, &self.message)
    }
}

impl std::error::Error for ToolError {}

/// A JavaScript rejection produced by a nested code-mode tool call. Keeping
/// this structured until the VM boundary preserves the fixed runtime's
/// `ToolError` name/kind while ordinary `isError` results remain plain Errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodeModeToolRejection {
    pub name: &'static str,
    pub kind: Option<ToolErrorKind>,
    pub message: RuntimeString,
}

impl CodeModeToolRejection {
    pub fn plain(message: impl Into<RuntimeString>) -> Self {
        Self {
            name: "Error",
            kind: None,
            message: message.into(),
        }
    }

    pub fn tool(error: ToolError) -> Self {
        Self {
            name: "ToolError",
            kind: Some(error.kind),
            message: error.message,
        }
    }

    pub fn kind_str(&self) -> Option<&'static str> {
        match self.kind {
            Some(ToolErrorKind::Model) => Some("model"),
            Some(ToolErrorKind::Fatal) => Some("fatal"),
            None => None,
        }
    }
}

impl fmt::Display for CodeModeToolRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: ", self.name)?;
        write_node_projected_runtime_string(formatter, &self.message)
    }
}

impl From<ToolError> for CodeModeToolRejection {
    fn from(error: ToolError) -> Self {
        Self::tool(error)
    }
}

/// Stable reason a submitted user input was rejected before persistence or a
/// provider request. These codes are shared with protocol clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputValidationErrorCode {
    InvalidDocumentMime,
    InvalidDocumentBase64,
    InvalidDocumentSignature,
    InvalidDocumentFilename,
    DocumentTooLarge,
    TooManyDocuments,
    RequestTooLarge,
    /// The per-session steering queue entered a terminal durability failure and
    /// can no longer admit inputs without risking a false acknowledgement.
    QueueUnavailable,
    UnsupportedInput,
}

impl RuntimeEncode for InputValidationErrorCode {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::InvalidDocumentMime => "invalid_document_mime",
            Self::InvalidDocumentBase64 => "invalid_document_base64",
            Self::InvalidDocumentSignature => "invalid_document_signature",
            Self::InvalidDocumentFilename => "invalid_document_filename",
            Self::DocumentTooLarge => "document_too_large",
            Self::TooManyDocuments => "too_many_documents",
            Self::RequestTooLarge => "request_too_large",
            Self::QueueUnavailable => "queue_unavailable",
            Self::UnsupportedInput => "unsupported_input",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for InputValidationErrorCode {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("invalid_document_mime", &[]),
                RuntimeEnumVariant::new("invalid_document_base64", &[]),
                RuntimeEnumVariant::new("invalid_document_signature", &[]),
                RuntimeEnumVariant::new("invalid_document_filename", &[]),
                RuntimeEnumVariant::new("document_too_large", &[]),
                RuntimeEnumVariant::new("too_many_documents", &[]),
                RuntimeEnumVariant::new("request_too_large", &[]),
                RuntimeEnumVariant::new("queue_unavailable", &[]),
                RuntimeEnumVariant::new("unsupported_input", &[]),
            ],
            &[
                Self::InvalidDocumentMime,
                Self::InvalidDocumentBase64,
                Self::InvalidDocumentSignature,
                Self::InvalidDocumentFilename,
                Self::DocumentTooLarge,
                Self::TooManyDocuments,
                Self::RequestTooLarge,
                Self::QueueUnavailable,
                Self::UnsupportedInput,
            ],
        )
    }
}

/// Non-secret metadata for a rejected input. Document bytes and submitted
/// filenames are deliberately absent from this shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputValidationErrorDetail {
    pub code: InputValidationErrorCode,
    pub feature: Option<RuntimeString>,
    pub provider: Option<RuntimeString>,
    pub model: Option<RuntimeString>,
    pub limit_bytes: Option<u64>,
    pub limit_count: Option<u64>,
}

impl RuntimeEncode for InputValidationErrorDetail {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("code", &self.code)?;
        if let Some(value) = &self.feature {
            object.field("feature", value)?;
        }
        if let Some(value) = &self.provider {
            object.field("provider", value)?;
        }
        if let Some(value) = &self.model {
            object.field("model", value)?;
        }
        if let Some(value) = &self.limit_bytes {
            object.field("limitBytes", value)?;
        }
        if let Some(value) = &self.limit_count {
            object.field("limitCount", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for InputValidationErrorDetail {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                code: object.required("code", &[])?,
                feature: object.optional("feature", &[])?,
                provider: object.optional("provider", &[])?,
                model: object.optional("model", &[])?,
                limit_bytes: object.optional("limitBytes", &[])?,
                limit_count: object.optional("limitCount", &[])?,
            })
        })
    }
}

/// A user-input validation failure raised before history or rollout mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputValidationError {
    pub message: String,
    pub detail: InputValidationErrorDetail,
}

impl InputValidationError {
    pub fn new(code: InputValidationErrorCode, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            detail: InputValidationErrorDetail {
                code,
                feature: None,
                provider: None,
                model: None,
                limit_bytes: None,
                limit_count: None,
            },
        }
    }
}

impl fmt::Display for InputValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for InputValidationError {}

/// The code carried by a terminal [`EngineError`] surfaced on `turn.failed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EngineErrorCode {
    AuthFailed,
    PromptTooLong,
    BudgetExhausted,
    ProviderError,
    Aborted,
    InvalidInput,
    Internal,
    /// The rapid-refill thrash breaker fired: compaction kept succeeding but the
    /// context refilled to the limit within a few turns, repeatedly. The turn is
    /// halted with a remediation message rather than churning the budget away.
    CompactionThrash,
    /// The behavioral loop detector fired a SECOND time (after a recovery
    /// nudge): the model kept repeating the same tool call / text. The turn is
    /// halted rather than burning the whole turn budget spinning.
    LoopDetected,
}

impl RuntimeEncode for EngineErrorCode {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::AuthFailed => "auth_failed",
            Self::PromptTooLong => "prompt_too_long",
            Self::BudgetExhausted => "budget_exhausted",
            Self::ProviderError => "provider_error",
            Self::Aborted => "aborted",
            Self::InvalidInput => "invalid_input",
            Self::Internal => "internal",
            Self::CompactionThrash => "compaction_thrash",
            Self::LoopDetected => "loop_detected",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for EngineErrorCode {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("auth_failed", &[]),
                RuntimeEnumVariant::new("prompt_too_long", &[]),
                RuntimeEnumVariant::new("budget_exhausted", &[]),
                RuntimeEnumVariant::new("provider_error", &[]),
                RuntimeEnumVariant::new("aborted", &[]),
                RuntimeEnumVariant::new("invalid_input", &[]),
                RuntimeEnumVariant::new("internal", &[]),
                RuntimeEnumVariant::new("compaction_thrash", &[]),
                RuntimeEnumVariant::new("loop_detected", &[]),
            ],
            &[
                Self::AuthFailed,
                Self::PromptTooLong,
                Self::BudgetExhausted,
                Self::ProviderError,
                Self::Aborted,
                Self::InvalidInput,
                Self::Internal,
                Self::CompactionThrash,
                Self::LoopDetected,
            ],
        )
    }
}

/// A terminal engine error surfaced on `turn.failed`.
#[derive(Debug)]
pub struct EngineError {
    pub code: EngineErrorCode,
    pub message: RuntimeString,
    /// The originating error, if any. Not carried across the wire.
    pub cause: Option<Box<dyn std::error::Error + Send + Sync>>,
    /// Server request id carried through from a ProviderError, for
    /// diagnostics/logs.
    pub request_id: Option<RuntimeString>,
    /// The normalized provider error kind (`rate_limit` / `overloaded` /
    /// `network` / …) when this failure originated at the provider, so hosts
    /// can localize the card instead of parsing the raw message. Absent for
    /// non-provider failures.
    pub provider_kind: Option<ProviderErrorKind>,
    /// Server-suggested wait (ms) captured when the retry budget was exhausted,
    /// so a host can offer a timed retry instead of making the user guess.
    /// Absent when the provider gave no timing hint.
    pub retry_after_ms: Option<u64>,
}

#[derive(Debug)]
struct ProviderFailureCause {
    error: ProviderError,
    recovery: RuntimeString,
}

impl fmt::Display for ProviderFailureCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.error, f)
    }
}

impl std::error::Error for ProviderFailureCause {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

impl EngineError {
    pub fn from_provider(error: ProviderError, recovery: impl Into<RuntimeString>) -> Self {
        Self {
            code: if error.kind == ProviderErrorKind::PromptTooLong {
                EngineErrorCode::PromptTooLong
            } else {
                EngineErrorCode::ProviderError
            },
            message: error.message.clone(),
            request_id: error.request_id.clone(),
            provider_kind: Some(error.kind),
            retry_after_ms: error.retry_after_ms,
            cause: Some(Box::new(ProviderFailureCause {
                error,
                recovery: recovery.into(),
            })),
        }
    }

    pub fn provider_error(&self) -> Option<&ProviderError> {
        let cause = self.cause.as_deref()?;
        cause
            .downcast_ref::<ProviderFailureCause>()
            .map(|cause| &cause.error)
            .or_else(|| cause.downcast_ref::<ProviderError>())
    }

    pub fn recovery_reason(&self) -> Option<&RuntimeString> {
        self.cause
            .as_deref()?
            .downcast_ref::<ProviderFailureCause>()
            .map(|cause| &cause.recovery)
    }
}

impl RuntimeEncode for EngineError {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("code", &self.code)?;
        object.field("message", &self.message)?;
        if let Some(error) = self.provider_error() {
            object.field("providerError", error)?;
        }
        if let Some(reason) = self.recovery_reason() {
            object.field("recoveryReason", reason)?;
        }
        if let Some(value) = &self.request_id {
            object.field("requestId", value)?;
        }
        if let Some(value) = &self.provider_kind {
            object.field("kind", value)?;
        }
        if let Some(value) = &self.retry_after_ms {
            object.field("retryAfterMs", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for EngineError {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            let provider: Option<ProviderError> = object.optional("providerError", &[])?;
            let recovery: Option<RuntimeString> = object.optional("recoveryReason", &[])?;
            Ok(Self {
                code: object.required("code", &[])?,
                message: object.required("message", &[])?,
                cause: provider.map(|error| {
                    Box::new(ProviderFailureCause {
                        error,
                        recovery: recovery.unwrap_or_default(),
                    }) as Box<dyn std::error::Error + Send + Sync>
                }),
                request_id: object.optional("requestId", &[])?,
                provider_kind: object.optional("kind", &[])?,
                retry_after_ms: object.optional("retryAfterMs", &[])?,
            })
        })
    }
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_node_projected_runtime_string(f, &self.message)
    }
}

impl std::error::Error for EngineError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause
            .as_ref()
            .map(|c| c.as_ref() as &(dyn std::error::Error + 'static))
    }
}

/// Why a turn ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalReason {
    Completed,
    Aborted,
    PromptTooLong,
    AuthFailed,
    /// The turn hit its configured turn-count allowance. Distinct from a spend
    /// cap so clients can say WHICH limit ended the turn.
    TurnLimitExhausted,
    /// The session-cumulative USD ceiling was reached.
    SpendLimitExhausted,
    Error,
}

impl RuntimeEncode for TerminalReason {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Completed => "completed",
            Self::Aborted => "aborted",
            Self::PromptTooLong => "prompt_too_long",
            Self::AuthFailed => "auth_failed",
            Self::TurnLimitExhausted => "turn_limit_exhausted",
            Self::SpendLimitExhausted => "spend_limit_exhausted",
            Self::Error => "error",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for TerminalReason {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("completed", &[]),
                RuntimeEnumVariant::new("aborted", &[]),
                RuntimeEnumVariant::new("prompt_too_long", &[]),
                RuntimeEnumVariant::new("auth_failed", &[]),
                RuntimeEnumVariant::new("turn_limit_exhausted", &[]),
                RuntimeEnumVariant::new("spend_limit_exhausted", &[]),
                // Legacy rollouts recorded one shared tag before the split;
                // reading them as the turn ceiling keeps old sessions resumable.
                RuntimeEnumVariant::new("budget_exhausted", &[]),
                RuntimeEnumVariant::new("error", &[]),
            ],
            &[
                Self::Completed,
                Self::Aborted,
                Self::PromptTooLong,
                Self::AuthFailed,
                Self::TurnLimitExhausted,
                Self::SpendLimitExhausted,
                Self::TurnLimitExhausted,
                Self::Error,
            ],
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn input_validation_error_detail_remains_a_by_value_public_field() {
        let error = InputValidationError::new(
            InputValidationErrorCode::UnsupportedInput,
            "unsupported input",
        );

        let detail: InputValidationErrorDetail = error.detail;

        assert_eq!(detail.code, InputValidationErrorCode::UnsupportedInput);
    }

    #[test]
    fn queue_unavailable_has_a_stable_runtime_codec_tag() {
        let encoded = runtime_codec::encode_value(&InputValidationErrorCode::QueueUnavailable)
            .expect("queue-unavailable code encodes");
        assert_eq!(encoded, RuntimeJson::String("queue_unavailable".into()));
        assert_eq!(
            runtime_codec::decode_value::<InputValidationErrorCode>(&encoded)
                .expect("queue-unavailable code decodes"),
            InputValidationErrorCode::QueueUnavailable
        );
    }
}
