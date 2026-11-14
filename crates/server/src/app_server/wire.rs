//! JSON-RPC 2.0 wire envelope and typed API surface for the app-server layer.
//!
//! The app-server layer uses JSON-RPC 2.0 messages with the `"jsonrpc"`
//! header omitted on the wire, newline-delimited over stdio. The
//! runtime payloads are passed through as native runtime JSON. Content fields
//! use scoped codecs; identifiers, provider settings and other service-owned
//! controls retain their ordinary serde types and number validation.
//! The inner Op/Event schema stays owned by `klaus-agent-protocol`.

pub use super::approval_review_wire::*;
pub use super::queue_wire::*;
use super::runtime_wire::{RawJson, RuntimeValue};
use klaus_agent_core::runtime_values::RuntimeString;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Version of the app-server API contract. Bump on breaking changes.
pub const APP_SERVER_PROTOCOL_VERSION: u32 = 1;

// --- JSON-RPC error codes ---------------------------------------------------

pub const PARSE_ERROR: i64 = -32700;
pub const INVALID_REQUEST: i64 = -32600;
pub const METHOD_NOT_FOUND: i64 = -32601;
pub const INVALID_PARAMS: i64 = -32602;
/// Engine-side failure surfaced from the inner protocol (`error` reply event).
pub const ENGINE_ERROR: i64 = -32000;
/// Request ingress is saturated; the client should retry with backoff.
pub const OVERLOADED: i64 = -32001;
pub const NOT_INITIALIZED: i64 = -32002;
pub const ALREADY_INITIALIZED: i64 = -32003;
pub const UNKNOWN_SESSION: i64 = -32004;
/// A client-supplied submission id collides with one already in flight.
pub const DUPLICATE_SUBMISSION: i64 = -32005;
/// `session/start` names a thread that already has a live session on this
/// connection. The handle IS the thread id, so a second start would mean two
/// writers for one rollout; the client closes the live one first.
pub const SESSION_ALREADY_LIVE: i64 = -32006;

// --- envelope helpers -------------------------------------------------------

/// Build a success response for `id`.
pub fn response(id: &Value, result: Value) -> Value {
    serde_json::json!({ "id": id, "result": result })
}

/// Build an error response for `id`.
pub fn error_response(id: &Value, code: i64, message: &str) -> Value {
    serde_json::json!({ "id": id, "error": { "code": code, "message": message } })
}

/// Build an error response carrying machine-readable `data`, for failures a
/// client must branch on rather than only display (a stale-version config write
/// is retried after re-reading; a rejected value is shown).
pub fn error_response_with_data(id: &Value, code: i64, message: &str, data: Value) -> Value {
    serde_json::json!({ "id": id, "error": { "code": code, "message": message, "data": data } })
}

/// Build a server-initiated notification.
pub fn notification(method: &str, params: Value) -> Value {
    serde_json::json!({ "method": method, "params": params })
}

/// Build a server-initiated request (reverse RPC, e.g. approvals).
pub fn request(id: u64, method: &str, params: Value) -> Value {
    serde_json::json!({ "id": id, "method": method, "params": params })
}

/// An incoming message, classified. JSON-RPC allows requests (method + id),
/// notifications (method, no id) and responses (id, result | error).
pub enum Incoming {
    Request {
        id: Value,
        method: String,
        params: Value,
    },
    Notification {
        method: String,
        params: Value,
    },
    Response {
        id: Value,
        result: Result<Value, Value>,
    },
    Invalid,
}

/// Classify one parsed JSON line.
pub fn classify(value: &Value) -> Incoming {
    let obj = match value.as_object() {
        Some(obj) => obj,
        None => return Incoming::Invalid,
    };
    let params = obj.get("params").cloned().unwrap_or(Value::Null);
    match (obj.get("method").and_then(Value::as_str), obj.get("id")) {
        (Some(method), Some(id)) => Incoming::Request {
            id: id.clone(),
            method: method.to_string(),
            params,
        },
        (Some(method), None) => Incoming::Notification {
            method: method.to_string(),
            params,
        },
        (None, Some(id)) => {
            if let Some(error) = obj.get("error") {
                Incoming::Response {
                    id: id.clone(),
                    result: Err(error.clone()),
                }
            } else {
                Incoming::Response {
                    id: id.clone(),
                    result: Ok(obj.get("result").cloned().unwrap_or(Value::Null)),
                }
            }
        }
        (None, None) => Incoming::Invalid,
    }
}

// --- typed API surface (exported to clients via the schema bundle) ----------

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClientInfo {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

/// Per-connection capabilities negotiated at `initialize`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClientCapabilities {
    /// Exact notification method names to suppress on this connection.
    /// Matching is exact (no wildcards); unknown names are accepted and ignored.
    #[serde(default)]
    pub opt_out_notification_methods: Vec<String>,
    /// Opt into the typed Thread/Turn/Item product notification surface
    /// (`thread/*`, `turn/started`, `item/*`). Off by default: raw
    /// `session/event` passthrough stays the only stream.
    #[serde(default)]
    pub product_events: bool,
    /// Add one monotonic stream cursor to every session-scoped notification.
    /// Clients use the cursor with `session/follow` to join an atomic history
    /// snapshot to the live event stream without a read/subscribe race.
    #[serde(default)]
    pub revisioned_session_events: bool,
}

/// `initialize` request params. Must be the first request on a connection.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InitializeParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_info: Option<ClientInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<ClientCapabilities>,
}

/// `initialize` result.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InitializeResult {
    pub user_agent: String,
    pub protocol_version: u32,
    #[serde(default)]
    pub capabilities: ServerCapabilities,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServerCapabilities {
    /// Versioned hard task filesystem/mode boundary; absent on older engines.
    pub task_resource_scope: u32,
    #[serde(default)]
    pub composer_predictions: bool,
    pub task_commands: bool,
    pub task_command_external_network: bool,
    #[serde(default)]
    pub task_identity: bool,
}

/// `session/start` request params. `init` is the raw `init_session` payload of
/// the inner protocol (same shape the stdio Op wire accepts).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionStartParams {
    #[schemars(with = "SessionRoutingInitSchema")]
    pub init: RawJson,
    /// Persisted thread id to resume (continue an existing conversation).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume: Option<String>,
    /// Persisted thread id to fork (a NEW session seeded from its history).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fork: Option<String>,
}

/// Fork one already-live, idle session into a new persistent ordinary thread.
/// The full captured model history becomes visible in the new thread, including
/// a side task's hidden prefix. `init.threadId` may pin the new thread identity.
/// The source stays unchanged. SessionStartResult is returned only after the
/// destination transcript is flushed, so the thread can be closed and resumed.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionForkLiveParams {
    /// Persist an inherited goal snapshot and defer it until an explicit turn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherit_goal: Option<bool>,
    /// An already initialized live source; never activated from disk implicitly.
    pub source_session_id: String,
    /// The ordinary session init. Its threadId names the new persistent target.
    #[schemars(with = "SessionRoutingInitSchema")]
    pub init: RawJson,
    /// Keep messages strictly before this UUID; an unknown UUID is an error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_message_uuid: Option<String>,
}

/// Start an ephemeral side task in one request. A live parent contributes its
/// completed context prefix; a cold parent is read from its persisted record.
/// The inherited context is hidden from the new thread's visible transcript.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionStartSideTaskParams {
    pub parent_thread_id: String,
    /// Optional live endpoint identity. Absent uses the parent's thread id.
    /// An unmaterialized endpoint does not trigger parent startup or inference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_session_id: Option<String>,
    /// Child initialization, including its provider and optional threadId.
    #[schemars(with = "SessionRoutingInitSchema")]
    pub init: RawJson,
}

/// `session/start` acknowledges the endpoint init; `session/prepare` waits for
/// the restored core and its event adapters to be published.
/// `initialized` is the raw `InitSessionResult` JSON.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionStartResult {
    pub session_id: String,
    #[schemars(schema_with = "any_schema")]
    pub initialized: RuntimeValue,
}

/// Identify the retained endpoint whose restoration must complete.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionPrepareParams {
    pub session_id: String,
}

/// A preparation receipt contains no history and never starts inference.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionPrepareResult {
    pub session_id: String,
    pub ready: bool,
}

/// Fence future inference captures without waiting for the turn queue.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionBeginModelUpdateParams {
    pub session_id: String,
    pub change_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_revision: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionBeginModelUpdateResult {
    pub revision: u64,
    pub change_id: String,
}

/// Fail only the named capture fence after host-side preparation rejects.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionFailModelUpdateParams {
    pub session_id: String,
    pub change_id: String,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionFailModelUpdateResult {
    pub revision: u64,
    pub change_id: String,
}

/// A complete inference selection prepared independently of existing captures.
/// Unknown/non-inference init fields are rejected by the facade. An omitted
/// expectedRevision captures the revision before preparation; publication uses
/// compare-and-swap and, when supplied, the matching changeId fence.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionReconfigureModelParams {
    pub session_id: String,
    #[schemars(with = "SessionRoutingInitSchema")]
    pub init: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_revision: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub change_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionReconfigureModelResult {
    pub revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub change_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionModelRuntimeResult {
    pub revision: u64,
    pub model: String,
    pub model_tiers: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_tier_targets: Option<Value>,
    /// Non-secret advisor status, or null when advisor consultation is off.
    #[serde(default)]
    pub advisor_target: Value,
    /// Non-secret orchestration execution target status, or null when reusing the medium tier.
    #[serde(default)]
    pub orchestrator_target: Value,
    /// Non-secret per-feature target status; a null slot follows the main model.
    #[serde(default)]
    pub feature_targets: Value,
    pub effort: Value,
    pub thinking: Value,
    #[serde(default)]
    pub ultra_mode: bool,
    #[serde(default)]
    pub orchestration_mode: bool,
    #[serde(default)]
    pub orchestrator_fallback_to_main: bool,
    #[serde(default)]
    pub fast_mode: bool,
    pub context_window: u64,
    pub max_output_tokens: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_retry_tokens: Option<u64>,
    pub capabilities: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub change_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_change_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_update_error: Option<String>,
}

/// Non-secret ownership captured by the engine for a particular request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PublicInference {
    pub revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub change_id: Option<String>,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tier: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionModelRuntimeParams {
    pub session_id: String,
}

/// `turn/start` request params. `input`/`options` are raw inner-protocol JSON.
/// A client-supplied `submissionId` becomes the turn id verbatim, so transport
/// shims keep their own event correlation (same contract as every promoted
/// method).
/// `options.metaInput` marks engine-authored continuations; `displayMetadata`
/// preserves the original human-facing text for a user-initiated goal turn.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TurnStartParams {
    pub session_id: String,
    #[schemars(schema_with = "any_schema")]
    pub input: RuntimeValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "any_schema_opt")]
    pub options: Option<RuntimeValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// Continue pending main-thread input only after this exact completed submission.
/// The acknowledgement is followed by `pendingResume` on `session/rawEvent`;
/// `started: false` is terminal and does not synthesize a completed turn.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionResumePendingParams {
    pub session_id: String,
    pub after_submission_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// Explicit user-owned continuation of preserved pending main-thread input.
/// Unlike resumePending this does not require a successful prior completion.
/// The acknowledgement is followed by the same correlated pendingResume event.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionResumeQueuedParams {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `turn/start` result. The turn id doubles as the inner submission id: every
/// streamed `session/event` for this turn echoes it as `event.submissionId`.
/// The result acknowledges admission to the session's op queue, not successful
/// turn execution. A closing endpoint or failed enqueue returns ENGINE_ERROR
/// instead and does not retain the rejected submission as an in-flight turn.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TurnStartResult {
    pub turn_id: String,
}

/// One admission decision, made under the engine's foreground admission gate.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TurnSubmitParams {
    pub session_id: String,
    #[schemars(schema_with = "any_schema")]
    pub input: RuntimeValue,
    pub mode: TurnSubmitMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "any_schema_opt")]
    pub options: Option<RuntimeValue>,
    /// Required and nonempty for steer mode; optional for automatic admission.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1))]
    pub expected_turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "any_schema_opt")]
    pub display_metadata: Option<RuntimeValue>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum TurnSubmitMode {
    StartIfIdle,
    ContinueIfIdle,
    Steer,
    StartOrSteer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum TurnSubmitStatus {
    Started,
    Steered,
    NotSubmitted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum TurnSubmitReasonCode {
    NoActiveTurn,
    ExpectedTurnMismatch,
    NotIdle,
    SessionClosing,
    ActiveTurnNotSteerable,
    EmptyInput,
    InvalidInput,
    SessionUnavailable,
    QueuePaused,
    AdmissionUnknown,
    QueueEntryChanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum TurnSubmitKind {
    Review,
    Compact,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TurnSubmitReason {
    pub code: TurnSubmitReasonCode,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_kind: Option<TurnSubmitKind>,
}

/// `started` confirms admission, and `steered` confirms queue acceptance.
/// Steering still has a separate, later InputApplied receipt.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TurnSubmitResult {
    pub status: TurnSubmitStatus,
    pub submission_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<TurnSubmitReason>,
}

/// `turn/interrupt` request params. A client-supplied `submissionId` becomes
/// the inner correlation id verbatim (same contract as `turn/start`).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TurnInterruptParams {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<TurnInterruptOrigin>,
    /// The foreground facade submission id, distinct from this request's correlation id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_turn_id: Option<String>,
}

/// `turn/interrupt` response: the settled interruption outcome. The reply is
/// deferred until cancellation lands (or answers `accepted:false` at once when
/// nothing was running), so its arrival confirms the interruption.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TurnInterruptResponse {
    pub submission_id: String,
    pub accepted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interrupted_turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal_pause_error: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum TurnInterruptOrigin {
    UserStop,
    System,
    TargetedTurn,
}

/// Ack carrying the submission id assigned to a forwarded op.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SubmissionAck {
    pub submission_id: String,
}

/// `session/subscribe` and `session/unsubscribe` request params: arm or gate
/// this connection's event stream for one bound session.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionSubscriptionParams {
    pub session_id: String,
}

/// Subscription state after the call.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionSubscriptionResult {
    pub session_id: String,
    pub subscribed: bool,
}

/// Cursor supplied when joining a live session stream. `streamId` scopes the
/// numeric revision to one live engine session; a restarted session therefore
/// cannot be mistaken for an older point in the same stream.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionFollowParams {
    pub session_id: String,
    /// Preserve the complete original transcript across prompt compaction.
    /// The persisted read and live overlay share the ordered follow boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_original_history: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_revision: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// One retained raw engine event in a replay response.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionFollowEvent {
    pub revision: u64,
    #[schemars(schema_with = "any_schema")]
    pub event: RuntimeValue,
}

/// Atomic join result for a session stream. `mode=snapshot` carries the exact
/// history terminal reply and the revision at that ordered boundary. With
/// `includeOriginalHistory`, the snapshot preserves original saved messages,
/// carries `activeItems: [{turnId, item}]` at that same cut, and provides
/// `coveredMessageIds` / `coveredToolResults: [{turnId, toolUseId}]` so post-cut notifications can be
/// reconciled by authoritative identities even when core persistence runs ahead
/// of the event pump. The default carries the existing model-context history.
/// `mode=replay` carries every retained event after the supplied cursor. A
/// stale or foreign cursor falls back to a snapshot rather than silently
/// skipping data.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionFollowResult {
    pub session_id: String,
    pub stream_id: String,
    pub revision: u64,
    pub mode: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "any_schema_opt")]
    pub snapshot: Option<RuntimeValue>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<SessionFollowEvent>,
    pub available_from_revision: u64,
}

/// `session/close` request params.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionCloseParams {
    pub session_id: String,
}

/// Shared params for promoted session-scoped request methods
/// (`session/history`, `session/mcpRuntime`, `session/shutdown`, ...): they
/// forward one fixed inner op and reply with its terminal payload (the reply
/// envelope minus `submissionId`). Engine-side error replies pass through
/// verbatim inside the result (`error` / `errorDetail` keys), matching the
/// native stdio reply shape exactly. A client-supplied `submissionId` becomes
/// the inner correlation id verbatim, so `cancel_request` keeps working.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionRequestParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionPredictionContextResult {
    #[schemars(required)]
    pub turn_id: Option<String>,
    pub model: String,
    pub provider: String,
    pub plan_mode: bool,
    pub running: bool,
}

/// An ephemeral next-message query against one terminal turn.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionPredictMessageParams {
    pub session_id: String,
    pub request_id: String,
    pub source_turn_id: String,
    #[serde(with = "super::runtime_wire::string")]
    #[schemars(with = "String")]
    pub prompt: RuntimeString,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "any_schema_opt")]
    pub effort: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionCancelPredictionParams {
    pub session_id: String,
    pub request_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SessionPredictionResult {
    #[schemars(required, with = "Option<String>")]
    #[serde(default, with = "super::runtime_wire::optional_string")]
    pub suggestion: Option<RuntimeString>,
}

/// `session/generateTitle` request params.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionGenerateTitleParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "super::runtime_wire::optional_string")]
    #[schemars(with = "Option<String>")]
    pub instructions: Option<RuntimeString>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "super::runtime_wire::optional_string")]
    #[schemars(with = "Option<String>")]
    pub prompt: Option<RuntimeString>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub persist: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `session/taskList` request params. `options` is the raw inner-protocol
/// task-list query options object.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionTaskListParams {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "any_schema_opt")]
    pub options: Option<RuntimeValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionTaskIdentityParams {
    pub session_id: String,
    #[serde(with = "super::runtime_wire::string")]
    #[schemars(with = "String")]
    pub task_id: RuntimeString,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionTaskIdentityResult {
    pub task_identity: Option<TaskIdentity>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TaskIdentity {
    #[serde(with = "super::runtime_wire::string")]
    #[schemars(with = "String")]
    pub task_id: RuntimeString,
    #[serde(with = "super::runtime_wire::string")]
    #[schemars(with = "String")]
    pub generation: RuntimeString,
    #[serde(with = "super::runtime_wire::string")]
    #[schemars(with = "String")]
    pub fingerprint: RuntimeString,
    pub status: String,
    #[serde(with = "super::runtime_wire::string")]
    #[schemars(with = "String")]
    pub subject: RuntimeString,
    #[serde(with = "super::runtime_wire::string")]
    #[schemars(with = "String")]
    pub description: RuntimeString,
}

/// `engine/listSessions` request params. `options` is the raw inner-protocol
/// list options object (limit, cursor, ...).
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EngineListSessionsParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "any_schema_opt")]
    pub options: Option<RuntimeValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// Inspect catalog metadata without loading or resuming the transcript.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EngineReadSessionSummaryParams {
    pub session_id: String,
}

/// The row has the exact inner-protocol `ThreadSummary` shape used by listSessions.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EngineReadSessionSummaryResult {
    pub session: Option<RuntimeValue>,
}

/// Read-only lifetime profile metrics, with UTC daily token buckets.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EngineProfileActivityParams {}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProfileActivityDay {
    pub date: String,
    pub tokens: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EngineProfileActivityResult {
    pub days: Vec<ProfileActivityDay>,
    pub total_tokens: u64,
    pub peak_daily_tokens: u64,
    pub longest_task_ms: u64,
    pub longest_streak_days: u64,
    pub current_streak_days: u64,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub incomplete: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub partial: bool,
}

/// Read one thread without acquiring a continuation writer. Cursor is an exclusive turn id.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EngineReadThreadParams {
    pub thread_id: String,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Read complete durable messages without activating their provider or a writer.
///
/// Turn paging: pages are slices of the
/// oldest-first turn sequence, `cursor` names the turn the NEXT page starts at, and
/// an unknown cursor is refused rather than silently re-reading from the head.
/// `itemsView` bounds what each message carries: `full` (the default) keeps every
/// block, `summary` keeps the turn's first user and last assistant message, and
/// `notLoaded` keeps turn membership only. Summary selection clones the chosen
/// messages whole, so a first user message still carries its own image blocks.
/// Replies may also carry `presentationRecords`: the returned users' matching
/// `user_display` records, separate from raw history membership and page limits.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EngineReadThreadHistoryParams {
    pub thread_id: String,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub items_view: Option<String>,
    /// Turn traversal order. `desc` (the default) walks from the newest turn
    /// towards the oldest, `asc` from the oldest towards the newest; the
    /// cursor names the turn the page starts at in that direction.
    #[serde(default)]
    pub sort_direction: Option<String>,
}

/// The page envelope a paged history read returns beside its rows.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ThreadHistoryPage {
    pub order: String,
    pub limit: usize,
    pub next_cursor: Option<String>,
    pub has_more: bool,
    /// Cursor for the page on the other side of this one, so a client that
    /// changes traversal direction can walk back without re-reading the head.
    pub backwards_cursor: Option<String>,
}

/// Page the messages of ONE turn without carrying any other turn's rows.
///
/// `cursor` is storage-owned. A separate `beforeItemId` selects older items
/// by stable identity, independently of the storage cursor revision.
/// Missing boundaries are refused; `limit` counts messages, not turns.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EngineReadThreadItemsParams {
    pub thread_id: String,
    pub turn_id: String,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub sort_direction: Option<String>,
    /// A stable message anchor; returns older items without interpreting it as a cursor.
    #[serde(default)]
    pub before_item_id: Option<String>,
}

/// Read all saved file-change metadata without loading transcript items.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EngineReadThreadFileChangesParams {
    pub thread_id: String,
}

/// Read complete durable child messages without resuming the parent session.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EngineReadSubagentHistoryParams {
    pub thread_id: String,
    pub agent_id: String,
}

/// A host normalizes its ecosystem format; this endpoint never reads `source.path`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EngineImportExternalSessionParams {
    pub thread_id: String,
    pub cwd: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub source: ExternalSessionSource,
    pub messages: Vec<ExternalSessionMessage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_metadata: Option<serde_json::Map<String, Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExternalSessionSource {
    pub provider_id: String,
    pub format: ExternalSessionFormat,
    pub path: String,
    pub content_sha256: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
pub enum ExternalSessionFormat {
    #[serde(rename = "claude-jsonl")]
    ClaudeJsonl,
    #[serde(rename = "codex-jsonl")]
    CodexJsonl,
    #[serde(rename = "kiro-chat")]
    KiroChat,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExternalSessionMessage {
    pub role: ExternalSessionRole,
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_message_id: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ExternalSessionRole {
    User,
    Assistant,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EngineImportExternalSessionResult {
    pub thread_id: String,
    pub persisted: bool,
    pub already_imported: bool,
    pub message_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ThreadReadResult {
    pub thread: ThreadReadInfo,
    pub page: ThreadReadPage,
    pub turns: Vec<ThreadReadTurn>,
    /// Complete failed-turn diagnostics for the returned page only.
    #[serde(default, skip_serializing_if = "empty_turn_error_details")]
    pub turn_error_details: Option<std::collections::BTreeMap<String, RuntimeValue>>,
}

fn empty_turn_error_details(
    value: &Option<std::collections::BTreeMap<String, RuntimeValue>>,
) -> bool {
    value.as_ref().is_none_or(|details| details.is_empty())
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ThreadReadInfo {
    pub id: String,
    #[serde(with = "super::runtime_wire::string")]
    #[schemars(with = "String")]
    pub title: RuntimeString,
    #[serde(with = "super::runtime_wire::string")]
    #[schemars(with = "String")]
    pub preview: RuntimeString,
    pub cwd: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub archived: bool,
    pub status: ThreadReadStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ThreadReadStatus {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub active_flags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ThreadReadPage {
    pub order: String,
    pub limit: usize,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ThreadReadTurn {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal_continuation: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_origin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal_id: Option<String>,
    pub status: String,
    pub error: Option<ThreadReadError>,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub duration_ms: Option<u64>,
    /// Product items retain complete source content; clients choose their own output budget.
    pub items: Vec<RuntimeValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ThreadReadError {
    #[serde(with = "super::runtime_wire::string")]
    #[schemars(with = "String")]
    pub message: RuntimeString,
    #[serde(default, with = "super::runtime_wire::optional_string")]
    #[schemars(with = "Option<String>")]
    pub additional_details: Option<RuntimeString>,
}

/// Manual compression uses the same cold threshold and worker as automatic maintenance.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EngineCompressRolloutsParams {}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum RolloutMigrationMode {
    #[default]
    DryRun,
    Apply,
}

/// Preview is the default; applying a preview may pin its per-path source revisions.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EngineMigrateRolloutsParams {
    #[serde(default)]
    pub mode: RolloutMigrationMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<std::collections::BTreeMap<String, serde_json::Value>>")]
    pub expected_source_revisions: Option<RawJson>,
}

/// `engine/shutdown` request params.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EngineShutdownParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `engine/builtinSkills` request params. The roster is a property of the
/// BINARY, not of any session, so this query takes no arguments and needs no
/// session to answer.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EngineBuiltinSkillsParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `engine/builtinAgents` request params. Like the skill roster, the built-in
/// agent roster is compiled into the binary, so no session is needed to list it.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EngineBuiltinAgentsParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `qoder/*` and `qwenwork/*` request params for the calls that
/// take nothing but a correlation id: status, logout, importLocal, loginStart.
///
/// These subscriptions are endpoint state, not session state — a settings page
/// signs in with no session bound — so these carry no session id, like the config
/// and rule-store methods.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct QoderParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// The account site used for a WorkBuddy browser login.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum WorkBuddyRegion {
    Cn,
    Intl,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkBuddyLoginStartParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<WorkBuddyRegion>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `qoder/loginPoll` and `qwenwork/loginPoll` params: the
/// flow handle the corresponding `loginStart` call answered with, passed back
/// verbatim so the shape stays the login flow's own business.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct QoderPollParams {
    #[schemars(schema_with = "any_schema")]
    pub flow: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `qoder/adoptCredential` and `qwenwork/adoptCredential` params: a profile the
/// HOST read out of a store the engine deliberately cannot open. QwenWork also
/// carries the device id that its safeStorage token was issued under.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct QoderAdoptParams {
    #[schemars(schema_with = "any_schema")]
    pub user_info: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// A host-imported Cursor access-token snapshot. Refresh credentials stay with
/// the application that owns them; the engine never refreshes a copied token.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CursorAdoptParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_credential_id: Option<String>,
    #[schemars(schema_with = "any_schema")]
    pub user_info: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// Browser login owns its PKCE verifier and private installation identity.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CursorLoginStartParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CursorLoginFlow {
    pub flow_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CursorLoginFlowParams {
    pub flow: CursorLoginFlow,
}

/// Conditional cleanup cannot erase a credential another client just adopted.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CursorLogoutParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_credential_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// Refreshing the Cursor catalog is explicit and never introduces an Auto model.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CursorModelsParams {
    #[serde(default)]
    pub read: QoderCatalogRead,
    #[serde(default)]
    pub force_refresh: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// Audio bytes stay inside this request and are never included in diagnostics.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SubscriptionTranscribeParams {
    pub audio_base64: String,
    #[serde(default)]
    pub mime_type: Option<String>,
    #[serde(default)]
    pub language: Option<String>,
    #[serde(default)]
    pub operation_id: Option<String>,
    /// Keep retained-audio recovery on the credential that opened dictation.
    #[serde(default)]
    pub expected_credential_generation: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SubscriptionTranscriptResult {
    pub text: String,
}

/// A standalone text request identifies a configured source, never credentials.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EngineCompleteTextParams {
    pub operation_id: String,
    pub source: String,
    pub model_id: String,
    pub system: String,
    pub prompt: String,
    pub max_output_tokens: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EngineCompleteTextResult {
    pub text: String,
    pub source: String,
    pub model_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SubscriptionCancelTranscriptionParams {
    pub operation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SubscriptionCancelTranscriptionResult {
    pub cancelled: bool,
}

/// One account-native image request. Pixel dimensions remain provider-controlled.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CursorImageParams {
    pub prompt: String,
    #[serde(default)]
    pub size: Option<String>,
    #[serde(default)]
    pub reference_images: Vec<CursorReferenceImage>,
    #[serde(default)]
    pub model_selection: Option<String>,
    #[serde(default)]
    pub operation_id: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CursorReferenceImage {
    pub data_base64: String,
    pub mime_type: String,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CursorAudioParams {
    pub text: String,
    #[serde(default)]
    pub voice: Option<String>,
    #[serde(default)]
    pub speed: Option<f64>,
    #[serde(default)]
    pub operation_id: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CursorVideoParams {
    pub prompt: String,
    #[serde(default)]
    pub seconds: Option<u32>,
    #[serde(default)]
    pub size: Option<String>,
    #[serde(default)]
    pub model_selection: Option<String>,
    #[serde(default)]
    pub operation_id: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CursorSearchParams {
    pub query: String,
    #[serde(default)]
    pub model_selection: Option<String>,
    #[serde(default)]
    pub operation_id: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CursorCancelOperationParams {
    pub operation_id: String,
}

#[derive(Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkBuddyOperationParams {
    #[serde(default)]
    pub operation_id: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkBuddyReferenceImage {
    pub data_base64: String,
    pub mime_type: String,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkBuddyImageParams {
    pub prompt: String,
    #[serde(default)]
    pub size: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub reference_images: Vec<WorkBuddyReferenceImage>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub operation_id: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkBuddyVideoParams {
    pub prompt: String,
    #[serde(default)]
    pub seconds: Option<u32>,
    #[serde(default)]
    pub size: Option<String>,
    #[serde(default)]
    pub reference_images: Vec<WorkBuddyReferenceImage>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub operation_id: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkBuddySpeechParams {
    pub text: String,
    #[serde(default)]
    pub voice: Option<String>,
    #[serde(default)]
    pub speed: Option<f64>,
    #[serde(default)]
    pub operation_id: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkBuddyTranscribeParams {
    pub audio_base64: String,
    #[serde(default)]
    pub mime_type: Option<String>,
    #[serde(default)]
    pub language: Option<String>,
    #[serde(default)]
    pub operation_id: Option<String>,
    /// Keep retained audio on the login that opened the native stream.
    #[serde(default)]
    pub expected_credential_generation: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkBuddySearchParams {
    pub query: String,
    #[serde(default)]
    pub allowed_domains: Vec<String>,
    #[serde(default)]
    pub blocked_domains: Vec<String>,
    #[serde(default)]
    pub freshness: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub operation_id: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkBuddyFetchParams {
    pub url: String,
    #[serde(default)]
    pub prompt: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub operation_id: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkBuddyCancelParams {
    pub operation_id: String,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AntigravityAdoptParams {
    pub credential: super::antigravity::auth::Credential,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct KiroAdoptParams {
    pub credential: super::kiro::auth::Credential,
    #[serde(default)]
    pub local_follow: bool,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TraeAdoptParams {
    pub credential: super::trae::auth::Credential,
}

#[derive(Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TraeLoginStartParams {
    #[serde(default)]
    pub excluded_device_ids: Vec<String>,
    #[serde(default)]
    pub local_device_proof: Option<super::trae::auth::LocalDeviceProof>,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TraeLoginFlow {
    pub flow_id: String,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TraeLoginFlowParams {
    pub flow: TraeLoginFlow,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TraeReferenceImage {
    pub data_base64: String,
    pub mime_type: String,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TraeImageParams {
    pub prompt: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub size: Option<String>,
    #[serde(default)]
    pub reference_images: Vec<TraeReferenceImage>,
    #[serde(default)]
    pub operation_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TraeVideoParams {
    pub prompt: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub size: Option<String>,
    #[serde(default)]
    pub seconds: Option<u32>,
    #[serde(default)]
    pub resolution: Option<String>,
    #[serde(default)]
    pub reference_images: Vec<TraeReferenceImage>,
    #[serde(default)]
    pub operation_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TraeSearchParams {
    pub query: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub max_results: Option<u32>,
    #[serde(default)]
    pub allowed_domains: Vec<String>,
    #[serde(default)]
    pub blocked_domains: Vec<String>,
    #[serde(default)]
    pub freshness: Option<String>,
    #[serde(default)]
    pub operation_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
}

#[derive(Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AntigravityLoginStartParams {}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AntigravityLoginFlow {
    pub flow_id: String,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AntigravityLoginFlowParams {
    pub flow: AntigravityLoginFlow,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AntigravityReferenceImage {
    pub data_base64: String,
    pub mime_type: String,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AntigravityImageParams {
    pub prompt: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub size: Option<String>,
    #[serde(default)]
    pub reference_images: Vec<AntigravityReferenceImage>,
    #[serde(default)]
    pub operation_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AntigravitySearchParams {
    pub query: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub allowed_domains: Vec<String>,
    #[serde(default)]
    pub blocked_domains: Vec<String>,
    #[serde(default)]
    pub freshness: Option<String>,
    #[serde(default)]
    pub operation_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AntigravityTranscribeParams {
    pub audio_base64: String,
    pub mime_type: String,
    #[serde(default)]
    pub language: Option<String>,
    #[serde(default)]
    pub operation_id: Option<String>,
    /// Bind retained-audio recovery to the account that opened live dictation.
    #[serde(default)]
    pub expected_credential_generation: Option<String>,
}

/// Which catalog read `qoder/models` or `qwenwork/models`
/// performs. The reads are not interchangeable, so the caller states which one it
/// needs rather than getting a cache-freshness knob and hoping.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum QoderCatalogRead {
    /// A picker's read (the default). Signed out is an ERROR, so the picker paints
    /// EMPTY and agrees with the settings pane's "not signed in" instead of
    /// fabricating options from a weeks-old file on disk; a cold start waits
    /// briefly for the live catalog for the same reason.
    #[default]
    Warm,
    /// A settings page's read: stale-while-revalidate, falling back to whatever
    /// catalog is at hand so the pane paints at once and refreshes behind itself.
    Cached,
}

/// `qoder/models` and `qwenwork/models` params.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct QoderModelsParams {
    #[serde(default)]
    pub read: QoderCatalogRead,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `codex/models` params. Endpoint state like the Qoder ones: a settings page
/// reads the catalog with no session bound.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CodexModelsParams {
    /// Which freshness the caller needs; defaults to the picker's warm read.
    #[serde(default)]
    pub read: QoderCatalogRead,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `subscription/models` params. Endpoint state like the two above: a model
/// picker reads the Claude lineup with no session bound, and with no credential
/// either — the lineup is static product knowledge.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionModelsParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// Resolve a persisted model choice without starting a session or requiring a key.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ModelSelectionResolveParams {
    pub source: String,
    pub selection: String,
    #[serde(default)]
    pub model_tier_selections: serde_json::Map<String, Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_tier_targets: Option<ModelTierTargets>,
}

/// A source-qualified choice suitable for persistence and session startup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ModelSelectionResolution {
    pub source: String,
    pub selection: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_tier_targets: Option<Value>,
}

/// An explicit catalog identity, never a provider model alias.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ModelSourceRef {
    pub source: String,
    pub model_id: String,
}

/// Auxiliary and orchestration model targets resolve engine-owned source
/// references into concrete provider/auth/capabilities/pricing bindings.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ModelTierTarget {
    Source {
        source: String,
        #[serde(rename = "modelId")]
        model_id: String,
    },
    Resolved {
        source: String,
        #[serde(rename = "modelId")]
        model_id: String,
        provider: Box<Value>,
        auth: Box<Value>,
        #[schemars(with = "ModelCapabilitiesInitSchema")]
        capabilities: Box<Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pricing: Option<Value>,
    },
    Unavailable {
        source: String,
        #[serde(rename = "modelId")]
        model_id: String,
        code: String,
        message: String,
    },
}

/// Presence opts into global routing; missing/null slots follow the main model.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelTierTargets {
    #[serde(default)]
    pub high: Option<ModelTierTarget>,
    #[serde(default)]
    pub medium: Option<ModelTierTarget>,
    #[serde(default)]
    pub low: Option<ModelTierTarget>,
}

/// Independent per-feature auxiliary targets. A present slot opts that feature
/// into its own model; missing/null slots follow the session's main model.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FeatureTargets {
    #[serde(default)]
    pub title: Option<ModelTierTarget>,
    #[serde(default)]
    pub prediction: Option<ModelTierTarget>,
    #[serde(default)]
    pub memory: Option<ModelTierTarget>,
}

/// Publish routing fields without closing the larger, established session-init
/// shape. The exact protocol decoder remains the validator of provider/auth.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionRoutingInitSchema {
    pub collaboration_mode: Option<CollaborationMode>,
    pub provider: Option<Value>,
    pub auth: Option<Value>,
    pub config: Option<Value>,
    /// Runtime network selection, independent from persisted config.
    pub network_allowed: Option<bool>,
    #[schemars(with = "Option<ModelCapabilitiesInitSchema>")]
    pub capabilities: Option<Value>,
    #[schemars(with = "Option<Vec<ModelCapabilitiesEntrySchema>>")]
    pub capabilities_by_model: Option<Value>,
    pub pricing: Option<Value>,
    pub model_selection: Option<String>,
    pub model_source: Option<ModelSourceRef>,
    pub model_tier_targets: Option<ModelTierTargets>,
    /// Independent target; omitted/null turns the advisor off on start and full reconfigure.
    pub advisor_target: Option<ModelTierTarget>,
    /// Optional explicit execution target for orchestration mode; omitted/null reuses the medium tier.
    pub orchestrator_target: Option<ModelTierTarget>,
    /// Whether orchestration mode is active for this conversation.
    pub orchestration_mode: Option<bool>,
    /// Whether orchestration mode automatically falls back to the chat model on execution-model failure.
    pub orchestrator_fallback_to_main: Option<bool>,
    /// Independent targets for named auxiliary features (title generation,
    /// message prediction, memory maintenance); an omitted/null slot follows
    /// the session's main model on start and full reconfigure.
    pub feature_targets: Option<FeatureTargets>,
}

#[derive(Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QwenWorkCapabilitiesParams {}

/// A native image generation or editing operation.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QwenWorkImageParams {
    pub prompt: String,
    #[serde(default)]
    pub size: Option<String>,
    #[serde(default)]
    pub reference_images: Vec<CursorReferenceImage>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub operation_id: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QwenWorkVideoParams {
    pub prompt: String,
    #[serde(default)]
    pub seconds: Option<u32>,
    #[serde(default)]
    pub size: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub operation_id: Option<String>,
}

/// Music creation is independent of text-to-speech.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QwenWorkMusicParams {
    pub prompt: String,
    #[serde(default)]
    pub lyrics: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub operation_id: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QwenWorkSearchParams {
    pub query: String,
    #[serde(default)]
    pub count: Option<u32>,
    #[serde(default)]
    pub time_range: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub operation_id: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QwenWorkCancelOperationParams {
    pub operation_id: String,
}

/// `qoder/generateImage` params.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct QoderImageParams {
    pub prompt: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<String>,
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `qoder/setThinkingLevel` and `qwenwork/setThinkingLevel` params. Absent means
/// "no tier", which the bridge maps onto whatever the selected model advertises
/// as its default.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct QoderThinkingParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub level: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `kimi/setApiKey` params: a Kimi Code API key created in the console.
///
/// This is the credential Kimi documents for third-party clients, and it is the
/// one a host sends. The device-flow login needs no such call — its tokens are
/// earned by `kimi/loginStart` and `kimi/loginPoll` and never cross back to the
/// host, so this is the only method that carries a secret in.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct KimiApiKeyParams {
    pub api_key: String,
    /// Which gateway the key belongs to. Absent means the domestic coding
    /// endpoint; an overseas key has to say so, because a key issued by one
    /// region is refused by the other.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_base: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `bailian/setApiKey` params: an Aliyun Bailian Token Plan API key (`sk-sp-…`).
///
/// Token Plan has no login flow, so this is the only way a credential arrives —
/// the key is minted in the Bailian console and pasted here.
///
/// `region` selects the site rather than taking a free-form base URL, because
/// Token Plan serves exactly one region per site (Beijing domestically, Singapore
/// internationally) and a key is bound to the site that issued it. Letting a caller
/// spell an arbitrary host would only produce a key that fails with 401 and no
/// explanation of why.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BailianApiKeyParams {
    pub api_key: String,
    /// `"cn"` (default) or `"intl"`. Absent keeps whichever site the stored key
    /// already belongs to, so re-entering a key cannot silently move it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `minimax/setApiKey` params: a MiniMax M Plan subscription key (`sk-cp-…`).
///
/// M Plan has no login flow, so this is the only way a credential arrives — the key
/// is minted in the MiniMax console and pasted here.
///
/// `region` is optional in a different sense than Bailian's: the China and global
/// sites both mint `sk-cp-` keys and do not share them, and the region is not
/// recoverable from the key. Absent means DETECT the site by trying each one, not
/// "default to China" — a defaulted site would make every key from the other site
/// fail with an error that blames the key.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MinimaxApiKeyParams {
    pub api_key: String,
    /// `"cn"` or `"global"`. Absent detects the site from the key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `ark/setApiKey` params: a Volcengine Ark Coding Plan API key.
///
/// Coding Plan has no login flow, so this is the only way a credential arrives —
/// the key is minted on the plan page and pasted here. Its shape is not published,
/// so unlike the Bailian params there is no prefix for a caller to get wrong: the
/// service gives the only verdict that means anything.
///
/// `region` selects the SITE the key was minted on rather than taking a free-form
/// base URL. The plan is served per Volcengine region, and a key is bound to the
/// region that issued it; letting a caller spell an arbitrary host would only
/// produce a 401 with no explanation of why.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ArkApiKeyParams {
    pub api_key: String,
    /// The Volcengine region the key belongs to, e.g. `"cn-beijing"`. Absent keeps
    /// whichever region the stored key already belongs to, so re-entering a key
    /// cannot silently move it to a host that will refuse it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `zhipu/setApiKey` params: a Zhipu GLM Coding Plan API key.
///
/// Coding Plan has no login flow, so this is the only way a credential arrives —
/// the key is minted on the plan page and pasted here. Its shape is not published
/// (an `id.secret` pair with no declared lengths or alphabet), so unlike the
/// Bailian params there is no prefix for a caller to get wrong: the service gives
/// the only verdict that means anything.
///
/// `region` selects the SITE rather than taking a free-form base URL, because the
/// plan is sold by two platforms whose accounts, subscriptions and keys do not
/// cross. Letting a caller spell an arbitrary host would only produce a key that
/// fails with 401 and no explanation of why.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ZhipuApiKeyParams {
    pub api_key: String,
    /// `"cn"` (default) or `"intl"`. Absent keeps whichever site the stored key
    /// already belongs to, so re-entering a key cannot silently move it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// One row of the `engine/builtinSkills` reply: the identity a host needs to
/// list a code-defined built-in skill, without its body (the prompt text is
/// the engine's business and would dwarf the roster).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BuiltinSkillRow {
    pub name: String,
    pub description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub when_to_use: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub argument_hint: Option<String>,
    pub user_invocable: bool,
}

/// `engine/builtinSkills` result — the one-key envelope named after the query,
/// matching every other promoted query's reply shape.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EngineBuiltinSkillsResult {
    pub builtin_skills: Vec<BuiltinSkillRow>,
}

/// One row of the `engine/builtinAgents` reply: the identity and declared
/// routing of a code-defined built-in agent role. The model field carries the
/// tier or model id the role declares (`low`/`medium`/`high` or a concrete id);
/// it is absent when the role declares nothing, which is exactly the case where
/// the delegating model picks the tier per task. The prompt body stays out: it
/// is the engine's business and would dwarf the roster.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BuiltinAgentRow {
    pub name: String,
    pub description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// The declared reasoning effort: a named level or, for the integer form,
    /// its decimal text. A string keeps the wire schema scalar while the card
    /// chip only ever prints this value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disallowed_tools: Option<Vec<String>>,
}

/// `engine/builtinAgents` result — the one-key envelope named after the query,
/// matching every other promoted query's reply shape.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EngineBuiltinAgentsResult {
    pub builtin_agents: Vec<BuiltinAgentRow>,
}

/// `engine/permissionRules` request params. The durable rule store belongs to the
/// ENGINE, not to any session, so this query takes no session id — a host with no
/// live session (a settings page on its own) can still read and edit it.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PermissionRulesParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// Reload only the engine-owned durable rule layer of an existing session.
/// This does not create a core Session or replace project/session-only rules.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PermissionRulesReloadParams {
    pub session_id: String,
}

/// `engine/addPermissionRules` request params: the rules to append and which
/// class they join (`allow` when absent — the grant an approval produces).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PermissionRulesEditParams {
    /// `[{ toolName, argumentPrefix? }]` — the same rule shape the engine's
    /// approval suggestions carry.
    pub rules: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub behavior: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `engine/removePermissionRule` request params: the rule STRING as stored
/// (`Read`, `Bash(git status)`), matched on its semantic identity.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PermissionRuleRemoveParams {
    pub rule: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub behavior: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// The rule store as it stands after a read or an edit — rule strings per class,
/// so a host renders exactly what the engine will evaluate.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PermissionRulesResult {
    pub allow: Vec<String>,
    pub deny: Vec<String>,
    pub ask: Vec<String>,
}

/// `config/read` request params. Session-less for the same reason the rule store
/// is: the config file is endpoint state, and a settings page can be open with no
/// session bound at all.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConfigReadParams {
    /// Read a config file other than the user's (a project's
    /// `<cwd>/.klaus/config.toml`). Absent reads `<config home>/config.toml`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_path: Option<String>,
    /// Working directory whose project config joins the resolved view, so a host
    /// can show which keys a repo overrides.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// One edit in a config write: a dotted `keyPath` (quote a segment that itself
/// contains dots), the value to store, and how it combines with what is there.
/// A `null` value deletes, whatever the strategy says — TOML has no null.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConfigEditParam {
    pub key_path: String,
    #[schemars(schema_with = "any_schema")]
    #[serde(default)]
    pub value: Value,
    /// `upsert` (default) | `replace` | `delete`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge_strategy: Option<String>,
}

/// `config/value/write` request params: one edit, spelled out inline.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConfigValueWriteParams {
    pub key_path: String,
    #[schemars(schema_with = "any_schema")]
    #[serde(default)]
    pub value: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge_strategy: Option<String>,
    /// The `version` the caller last read. A mismatch refuses the write instead
    /// of overwriting whatever landed in between.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `config/batchWrite` request params: edits applied in order, all or nothing.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConfigBatchWriteParams {
    pub edits: Vec<ConfigEditParam>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// The config as it stands after a read or a write. A write answers with the
/// same shape a read does, so a host updates its view from the reply instead of
/// racing a re-read.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConfigResult {
    /// The file `values` and `version` describe, and the one a write targets.
    pub path: String,
    /// Content version to echo back as `expectedVersion` on the next write.
    pub version: String,
    /// Every top-level key as stored in that file.
    #[schemars(schema_with = "any_schema")]
    pub values: Value,
    /// The engine slice folded over the schema defaults (plus the project file
    /// when `cwd` was given): what a session would actually resolve.
    #[schemars(schema_with = "any_schema")]
    pub resolved: Value,
    /// Winning layer per resolved key (`defaults` | `user` | `project` | ...), so
    /// a settings page can mark a value it cannot change from here.
    #[schemars(schema_with = "any_schema")]
    pub sources: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub migration_warnings: Option<Vec<ConfigMigrationWarning>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConfigMigrationWarning {
    pub code: String,
    pub message: String,
    pub keys: Vec<String>,
}

/// Native Windows preparation state for the endpoint's effective selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum WindowsSandboxReadiness {
    Ready,
    NotConfigured,
    UpdateRequired,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WindowsSandboxReadinessParams {}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WindowsSandboxReadinessResult {
    pub status: WindowsSandboxReadiness,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum WindowsSandboxSetupMode {
    Elevated,
    Unelevated,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WindowsSandboxSetupStartParams {
    pub mode: WindowsSandboxSetupMode,
    #[serde(default, deserialize_with = "optional_absolute_setup_cwd")]
    #[schemars(with = "Option<String>")]
    pub cwd: Option<std::path::PathBuf>,
}

fn optional_absolute_setup_cwd<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<std::path::PathBuf>, D::Error> {
    let cwd = Option::<std::path::PathBuf>::deserialize(deserializer)?;
    if cwd.as_ref().is_some_and(|cwd| !cwd.is_absolute()) {
        return Err(serde::de::Error::custom("cwd must be an absolute path"));
    }
    Ok(cwd)
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WindowsSandboxSetupStartResult {
    pub started: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WindowsSandboxSetupCompletedNotification {
    pub mode: WindowsSandboxSetupMode,
    pub success: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ModelCatalogChangedNotification {
    pub source: String,
}

/// `model/credential/write` request params: store a BYO model provider's API key
/// in the endpoint credential store, keyed by the provider `id`. The engine is
/// the only reader (session assembly) and writer; the key never lands in the
/// config file, and the shell references it by id afterwards.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ModelCredentialWriteParams {
    pub id: String,
    pub api_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `model/credential/delete` request params: drop a BYO model provider's stored
/// API key (a model the user removed).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ModelCredentialDeleteParams {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `model/credential/*` reply: a bare acknowledgement (the value is never read
/// back over the wire).
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ModelCredentialResult {
    pub ok: bool,
}

/// `model/credential/read` request params: fetch a BYO model provider's API key
/// for a SHELL-SIDE call the host makes itself (voice transcription reuses an
/// OpenAI model's key). Session auth never uses this path — the engine reads the
/// key itself via the stored-api-key auth — so the key leaves the store only for
/// the host feature that must call the provider directly.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ModelCredentialReadParams {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `model/credential/read` reply: the stored key, or absent when none is set.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ModelCredentialReadResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
}

/// `hooks/read` request params. Session-less: the hooks file is endpoint state
/// and a settings page can be open with no session bound.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HooksReadParams {
    /// Read a hooks file other than the user's (a project's
    /// `<cwd>/.klaus/hooks.json`). Absent reads `<config home>/hooks.json`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `hooks/write` request params: the whole hook set for one file, replacing it.
/// An empty object clears the file, so a removed event really disappears.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HooksWriteParams {
    #[schemars(schema_with = "any_schema")]
    #[serde(default)]
    pub hooks: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// The hooks file as it stands, plus the roster it is validated against — the
/// engine's own dispatch list, so a host renders exactly the events that can
/// fire instead of keeping a copy that drifts.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HooksResult {
    pub path: String,
    #[schemars(schema_with = "any_schema")]
    pub hooks: Value,
    pub events: Vec<String>,
}

/// `session/createSideTask` request params. `threadId` is the id the created
/// side-task thread must use (mirrors the inner `create_side_task` op).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionCreateSideTaskParams {
    pub session_id: String,
    pub thread_id: String,
    /// Thread id the side-task thread must adopt (mirrors the inner
    /// `create_side_task.newThreadId`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `session/renameThread` request params: re-key a persisted, inactive thread
/// onto a new id (mirrors the inner `rename_thread` op).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionRenameThreadParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub thread_id: String,
    pub new_thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `session/updateThreadHostMetadata` request params: replace a thread's
/// opaque host-owned metadata attachment (omitted -> remove). The engine
/// stores the value verbatim and never interprets its shape, so a host can
/// keep its session bookkeeping on the thread itself and evolve the payload
/// without a wire change.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionUpdateThreadHostMetadataParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_metadata: Option<RuntimeValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// Shared params for promoted methods addressing one reverse request
/// (`session/cancelRequest`, `session/agentMemorySnapshotDecision`).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionRequestIdParams {
    pub session_id: String,
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// Shared params for promoted methods resolving one reverse request with a
/// raw inner-protocol response payload (`session/approve`,
/// `session/resolveElicitation`, `session/resolveMcpAuthorization`,
/// `session/resolveMcpElicitation`).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionRequestResolutionParams {
    pub session_id: String,
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "any_schema_opt")]
    pub response: Option<RuntimeValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// Shared params for promoted methods addressing one MCP server by name
/// (`session/mcpAuthorizationStatus`, `session/revokeMcpAuthorization`,
/// `session/reconnectMcpServer`, `session/removeMcpServer`).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionMcpServerParams {
    pub session_id: String,
    pub server_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `session/upsertMcpServer` request params. `server` is the raw
/// inner-protocol MCP server init object.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionMcpServerUpsertParams {
    pub session_id: String,
    #[schemars(schema_with = "any_schema")]
    pub server: RuntimeValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `session/reloadMcpServers` request params. `servers` is the raw
/// inner-protocol MCP server init list.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionMcpServersReloadParams {
    pub session_id: String,
    #[schemars(schema_with = "any_schema")]
    pub servers: RuntimeValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `session/setInstalledPluginEnabled` request params. The plugin ledger is
/// endpoint-owned, so `session_id` is optional: without it the op runs on the
/// endpoint control handle rather than requiring a live conversation.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionPluginEnabledParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub name: String,
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `session/uninstallPlugin` request params. `session_id` is optional for the
/// same reason as the enable op.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionPluginUninstallParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remove_files: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `session/installPlugin` request params. `source_dir` is the host-resolved,
/// already-fetched plugin package directory; the engine materializes it into the
/// store and records the ledger.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionPluginInstallParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub marketplace: String,
    pub source_dir: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// When present, update an existing installation only if its version still
    /// matches. An empty string denotes an existing record without a version.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_installed_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionBundledDiagramMigrationParams {
    pub source_dir: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionBundledDiagramMigrationResult {
    pub migrated: bool,
    pub restart_required: bool,
}

/// One marketplace root for `session/pluginCatalog`: the host has already put the
/// packages on disk under `dir`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PluginCatalogRootParam {
    pub marketplace: String,
    pub dir: String,
}

/// `session/pluginCatalog` request params.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionPluginCatalogParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default)]
    pub roots: Vec<PluginCatalogRootParam>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `session/resolveDynamicToolCall` request params. `response` is the raw
/// inner-protocol dynamic tool response payload.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionDynamicToolCallParams {
    pub session_id: String,
    pub call_id: String,
    #[schemars(schema_with = "any_schema")]
    pub response: RuntimeValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `session/rewind` request params. `scope` is the raw inner-protocol rewind
/// scope object.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionRewindParams {
    pub session_id: String,
    pub num_turns: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "any_schema_opt")]
    pub scope: Option<RuntimeValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `session/rewindPreview` request params. No `scope`: only the file half of a
/// rewind is destructive, so only that half needs previewing.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionRewindPreviewParams {
    pub session_id: String,
    pub num_turns: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct QuestionRequestedNotification {
    pub session_id: String,
    pub submission_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_revision: Option<u64>,
    #[schemars(schema_with = "any_schema")]
    pub question: RuntimeValue,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct QuestionResolvedNotification {
    pub session_id: String,
    pub submission_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_revision: Option<u64>,
    #[schemars(schema_with = "any_schema")]
    pub question_resolved: RuntimeValue,
}

/// `session/appendDisplayMessage` request params: one host-rendered message
/// persisted as a completed turn without starting a model turn.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionAppendDisplayMessageParams {
    pub session_id: String,
    /// Host-owned stable identity, used to deduplicate a retried append.
    pub message_id: String,
    pub text: String,
    /// Renderer metadata persisted beside the message; the engine never
    /// interprets its shape.
    #[schemars(schema_with = "any_schema")]
    pub metadata: RuntimeValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `session/steer` request params. `input` is the raw inner-protocol input
/// payload; `priority` is the steer priority string.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionSteerParams {
    pub session_id: String,
    /// When supplied, reject unless this exact foreground turn is still live.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_turn_id: Option<String>,
    /// Stable UUID selected by the Host for reconciliation after stop/reconnect.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_message_id: Option<String>,
    #[schemars(schema_with = "any_schema")]
    pub input: RuntimeValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<String>,
    /// How the drain must persist this steer once applied: `"user"` lands it as
    /// its own user message (a human follow-up), `"delegation"` keeps the legacy
    /// tool-result shape (a cross-conversation envelope). Absent means
    /// `"delegation"`, so hosts that predate the field keep their behaviour.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery: Option<String>,
    /// Host rendering metadata kept beside an applied human message, never model-visible.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "any_schema_opt")]
    pub display_metadata: Option<RuntimeValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// Optional clarification answers target the request's exact admitted turn.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionAnswerQuestionParams {
    pub session_id: String,
    pub request_id: String,
    pub turn_id: String,
    pub client_response_id: String,
    #[schemars(schema_with = "any_schema")]
    pub answers: RuntimeValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "any_schema")]
    pub attachments: Option<RuntimeValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionPendingQuestionsParams {
    pub session_id: String,
}

/// `session/setModel` request params.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionSetModelParams {
    pub session_id: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// The source of a host goal instruction. Only explicit human controls carry user provenance.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GoalOrigin {
    User,
    Automatic,
}

/// A loaded endpoint or a saved thread, addressable without inference startup.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionGoalParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_goal_id",
        skip_serializing_if = "Option::is_none"
    )]
    pub expected_goal_id: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "deserialize_goal_revision",
        skip_serializing_if = "Option::is_none"
    )]
    pub expected_goal_revision: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<GoalOrigin>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// Full objective text, read from the current generation's trusted preparation.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionReadGoalObjectiveParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    /// Required current generation. Objective text never supplies read authority.
    pub expected_goal_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionReadGoalObjectiveResult {
    pub goal_id: String,
    #[serde(with = "super::runtime_wire::string")]
    #[schemars(with = "String")]
    pub objective: RuntimeString,
}

/// Goal mutations preserve the distinction between omitted, null and positive budgets.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionSetGoalParams {
    /// Metadata for a preparation made by a trusted human objective control.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owned_resources: Option<GoalOwnedResourcesParams>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_request_id: Option<String>,
    /// Explicit composer submission records one human objective message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submit: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "super::runtime_wire::optional_string")]
    #[schemars(with = "Option<String>")]
    pub display_text: Option<RuntimeString>,
    #[serde(flatten)]
    pub target: SessionGoalParams,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "super::runtime_wire::optional_string")]
    #[schemars(with = "Option<String>")]
    pub objective: Option<RuntimeString>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_goal_budget",
        skip_serializing_if = "Option::is_none"
    )]
    pub token_budget: Option<Option<i64>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GoalOwnedResourcesParams {
    pub preparation_id: String,
    pub files: Vec<GoalOwnedResourceFileParams>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GoalOwnedResourceFileParams {
    pub name: String,
    pub size_bytes: u64,
    pub sha256: String,
}

impl From<GoalOwnedResourcesParams>
    for klaus_agent_core::goal::owned_resources::GoalOwnedResourcesInput
{
    fn from(value: GoalOwnedResourcesParams) -> Self {
        Self {
            preparation_id: value.preparation_id,
            files: value
                .files
                .into_iter()
                .map(
                    |file| klaus_agent_core::goal::owned_resources::GoalOwnedResourceFile {
                        name: file.name,
                        size_bytes: file.size_bytes,
                        sha256: file.sha256,
                    },
                )
                .collect(),
        }
    }
}

fn deserialize_goal_revision<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<i64>, D::Error> {
    let revision = i64::deserialize(deserializer)?;
    if !(0..=9_007_199_254_740_991).contains(&revision) {
        return Err(serde::de::Error::custom(
            "expectedGoalRevision must be a nonnegative safe integer",
        ));
    }
    Ok(Some(revision))
}

fn deserialize_optional_goal_id<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Option<String>>, D::Error> {
    Option::<String>::deserialize(deserializer).map(Some)
}

fn deserialize_goal_budget<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Option<i64>>, D::Error> {
    let budget = Option::<i64>::deserialize(deserializer)?;
    if budget.is_some_and(|budget| !(1..=9_007_199_254_740_991).contains(&budget)) {
        return Err(serde::de::Error::custom(
            "tokenBudget must be a positive safe integer or null",
        ));
    }
    Ok(Some(budget))
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ThreadGoal {
    pub thread_id: String,
    pub goal_id: String,
    #[serde(with = "super::runtime_wire::string")]
    #[schemars(with = "String")]
    pub objective: RuntimeString,
    pub status: ThreadGoalStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_budget: Option<i64>,
    pub tokens_used: i64,
    pub time_used_seconds: i64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ThreadGoalStatus {
    Active,
    Paused,
    Blocked,
    UsageLimited,
    BudgetLimited,
    Complete,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionGoalResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_goal: Option<ThreadGoal>,
    pub goal_revision: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal_pursuit_eligible: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal_pursuit_eligibility_notice: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal_cleared: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal_admission: Option<GoalAdmission>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GoalAdmission {
    Scheduled,
    Steered,
    Deferred,
}

/// Replace the complete host-discovered skill catalog for one loaded session.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionSetSkillsParams {
    pub session_id: String,
    /// Skill records use the same wire shape as `session/start` init.skills.
    pub skills: Vec<RuntimeValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `session/setPermissionMode` request params.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionPermissionModeParams {
    pub session_id: String,
    pub mode: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `session/setRuntimePolicy` request params — immediate (bypasses the op queue).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionSetRuntimePolicyParams {
    pub session_id: String,
    /// Deprecated input alias, translated at the protocol boundary.
    #[serde(default, skip_serializing)]
    #[schemars(skip)]
    pub permission_mode: Option<String>,
    #[serde(default, skip_serializing)]
    #[schemars(skip)]
    pub pre_plan_permission_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "optional_approval_policy_schema")]
    pub approval_policy: Option<klaus_agent_core::types::permission::ApprovalPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "optional_approvals_reviewer_schema")]
    pub approvals_reviewer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<SandboxWorkspaceWriteSchema>")]
    pub sandbox_workspace_write: Option<klaus_agent_core::schemas::config::SandboxWorkspaceWrite>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network_allowed: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// Exact persisted workspace sandbox table.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct SandboxWorkspaceWriteSchema {
    #[serde(default)]
    pub writable_roots: Vec<String>,
    #[serde(default)]
    pub network_access: bool,
    #[serde(default)]
    pub exclude_tmpdir_env_var: bool,
    #[serde(default)]
    pub exclude_slash_tmp: bool,
}

/// `session/setRuntimeModel` request params — immediate (bypasses the op queue).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionSetRuntimeModelParams {
    pub session_id: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `session/updateConfig` request params. `update` is the raw inner-protocol
/// session config update object.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionConfigUpdateParams {
    pub session_id: String,
    #[schemars(schema_with = "session_config_update_schema")]
    pub update: RawJson,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `session/subagentTranscript` request params.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionSubagentTranscriptParams {
    pub session_id: String,
    pub thread_id: String,
    pub agent_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// Explicit input to an existing child conversation.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionSendSubagentMessageParams {
    pub session_id: String,
    pub agent_id: String,
    #[serde(with = "super::runtime_wire::string")]
    #[schemars(with = "String")]
    pub message: RuntimeString,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "super::runtime_wire::optional_string")]
    #[schemars(with = "Option<String>")]
    pub summary: Option<RuntimeString>,
    /// Trusted client input, never accepted from the model-facing message tool.
    #[serde(default)]
    pub user_initiated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// A stable message boundary for a selected manual compaction.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionCompactSelection {
    #[serde(with = "super::runtime_wire::string")]
    #[schemars(with = "String")]
    pub message_id: RuntimeString,
    pub direction: SessionCompactDirection,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum SessionCompactDirection {
    From,
    UpTo,
}

/// `session/compactStart` request params.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionCompactParams {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection: Option<SessionCompactSelection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "super::runtime_wire::optional_string")]
    #[schemars(with = "Option<String>")]
    pub instructions: Option<RuntimeString>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `session/sideQuestion` request params. `question` uses the same raw input
/// codec as `turn/start`: a string or inline text/image/document content parts.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionSideQuestionParams {
    pub session_id: String,
    #[schemars(schema_with = "raw_input_schema")]
    pub question: RuntimeValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// Shared params for promoted thread lifecycle methods
/// (`session/resumeSession`, `session/forkSession`, `session/archiveSession`,
/// `session/unarchiveSession`, `session/deleteSession`).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionThreadParams {
    /// Explicit persistent fork opt-in; ordinary forks are isolated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherit_goal: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub thread_id: String,
    /// `session/forkSession` only: thread id the forked thread must adopt
    /// (mirrors the inner `fork_session.newThreadId`). Other lifecycle
    /// methods ignore it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_thread_id: Option<String>,
    /// `session/forkSession` only: cut point — the branch inherits history
    /// strictly BEFORE the message with this uuid (mirrors the inner
    /// `fork_session.beforeMessageUuid`). Other lifecycle methods ignore it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_message_uuid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `session/updateTitle` request params.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionUpdateTitleParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub thread_id: String,
    #[serde(with = "super::runtime_wire::string")]
    #[schemars(with = "String")]
    pub title: RuntimeString,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `session/runSetup` request params. `request` is the raw inner-protocol
/// setup run request object.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionRunSetupParams {
    pub session_id: String,
    #[schemars(schema_with = "any_schema")]
    pub request: RuntimeValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `session/resolveAgentMemorySnapshotDecision` request params. `decision` is
/// the raw inner-protocol snapshot decision payload.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionMemorySnapshotDecisionParams {
    pub session_id: String,
    pub request_id: String,
    #[schemars(schema_with = "any_schema")]
    pub decision: RuntimeValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `session/settleAgentMemorySnapshotUnavailable` request params.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionMemorySnapshotUnavailableParams {
    pub session_id: String,
    pub request_id: String,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `session/callMcpTool` request params. `arguments` is the tool's own input
/// object, passed to the server verbatim.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionMcpToolCallParams {
    pub session_id: String,
    pub server_name: String,
    pub tool: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "any_schema_opt")]
    pub arguments: Option<RuntimeValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `session/readMcpResource` request params.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionMcpResourceParams {
    pub session_id: String,
    pub server_name: String,
    pub uri: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `session/getMcpPrompt` request params. `arguments` is the prompt's own
/// argument object.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionMcpPromptParams {
    pub session_id: String,
    pub server_name: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "any_schema_opt")]
    pub arguments: Option<RuntimeValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionBackgroundProcessOutputParams {
    pub session_id: String,
    pub shell_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_offset: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionBackgroundProcessStopParams {
    pub session_id: String,
    pub shell_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `session/editQueued` request params. `edit` is the raw inner-protocol
/// `QueueEdit` object; `expectedRevision` is the optimistic-concurrency guard,
/// so a caller that read a stale snapshot is rejected instead of silently
/// overwriting a concurrent mutation.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionQueueEditParams {
    pub session_id: String,
    pub mutation_id: String,
    pub uuid: String,
    pub expected_revision: u64,
    #[schemars(schema_with = "any_schema")]
    pub edit: RuntimeValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `session/removeQueued` request params.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionQueueRemoveParams {
    pub session_id: String,
    pub mutation_id: String,
    pub uuid: String,
    pub expected_revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `session/reorderQueued` request params. An absent `beforeUuid` moves the
/// entry to the end.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionQueueReorderParams {
    pub session_id: String,
    pub mutation_id: String,
    pub uuid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_uuid: Option<String>,
    pub expected_revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `session/reviewStart` request params. `target` is the raw inner-protocol
/// `ReviewTarget` object (`{ type: "uncommitted_changes" }`,
/// `{ type: "base_branch", branch }`, ...).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionReviewParams {
    pub session_id: String,
    #[schemars(schema_with = "any_schema")]
    pub target: RuntimeValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "super::runtime_wire::optional_string")]
    #[schemars(with = "Option<String>")]
    pub user_facing_hint: Option<RuntimeString>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

/// `session/event` notification: one raw inner protocol event line, tagged
/// with the owning session. Approval / elicitation asks are NOT forwarded here
/// (they arrive as reverse requests instead).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionEventNotification {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_revision: Option<u64>,
    #[schemars(schema_with = "any_schema")]
    pub event: RuntimeValue,
}

/// `taskList/changed` product notification (requires `productEvents`). The
/// task list is a complete authoritative replacement, not an item delta.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TaskListChangedNotification {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_revision: Option<u64>,
    pub task_list: TaskListChangePayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackgroundProcessChangedNotification {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_revision: Option<u64>,
    #[schemars(schema_with = "any_schema")]
    pub event: RuntimeValue,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TaskListChangePayload {
    pub task_list_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<String>,
    pub plan_kind: String,
    pub generation: String,
    pub revision: u64,
    pub fingerprint: String,
    pub tasks: Vec<RuntimeValue>,
}

/// An additional notification gate; inherit preserves all host delivery policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum TerminalNotificationDisposition {
    Inherit,
    Suppress,
}

impl From<klaus_agent_protocol::types::ProtocolTerminalNotificationDisposition>
    for TerminalNotificationDisposition
{
    fn from(value: klaus_agent_protocol::types::ProtocolTerminalNotificationDisposition) -> Self {
        match value {
            klaus_agent_protocol::types::ProtocolTerminalNotificationDisposition::Inherit => {
                Self::Inherit
            }
            klaus_agent_protocol::types::ProtocolTerminalNotificationDisposition::Suppress => {
                Self::Suppress
            }
        }
    }
}

fn deserialize_present_terminal_disposition<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<TerminalNotificationDisposition>, D::Error> {
    TerminalNotificationDisposition::deserialize(deserializer).map(Some)
}

/// `turn/completed` notification: the final `result` of a `turn/start`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TurnCompletedNotification {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_revision: Option<u64>,
    pub turn_id: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_terminal_disposition"
    )]
    #[schemars(with = "TerminalNotificationDisposition")]
    pub terminal_notification_disposition: Option<TerminalNotificationDisposition>,
    #[schemars(schema_with = "any_schema")]
    pub result: RuntimeValue,
}

/// `thread/deleted` notification: a persisted thread is gone from the store.
/// It is broadcast to every connection the moment the delete commits, so a
/// client's session list stays authoritative without polling — and without
/// having to infer the outcome from its own request reply.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ThreadDeletedNotification {
    pub thread_id: String,
}

/// `session/closed` notification: the endpoint unloaded an idle session. Its
/// engine runtime and child processes are gone; opening the session again
/// resumes it from stored history.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionClosedNotification {
    pub session_id: String,
}

/// One reason a session counts as active: it is waiting for a client answer.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "camelCase")]
pub enum SessionActiveFlag {
    WaitingOnApproval,
    WaitingOnUserInput,
}

/// Lifecycle status of one session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum SessionStatus {
    NotLoaded,
    Idle,
    Active {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        active_flags: Vec<SessionActiveFlag>,
    },
}

/// `session/statusChanged` notification: broadcast on every lifecycle
/// transition, including to clients that unsubscribed from the session's
/// event stream, so list surfaces stay current while a session is
/// unload-eligible.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionStatusChangedNotification {
    pub session_id: String,
    pub status: SessionStatus,
}

/// `thread/started` product notification (requires `productEvents`).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ThreadStartedNotification {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_revision: Option<u64>,
    pub submission_id: String,
    pub thread_id: String,
}

/// Authoritative successor admission; preparation/start events do not settle it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    rename_all = "camelCase",
    try_from = "GoalContinuationNotificationFields"
)]
pub struct GoalContinuationNotification {
    pub session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_revision: Option<u64>,
    pub decision_id: String,
    pub prior_submission_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub goal_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub successor_submission_id: Option<String>,
    pub pending: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub continued: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prior_completion_notification_disposition: Option<TerminalNotificationDisposition>,
}
#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GoalContinuationNotificationFields {
    session_id: String,
    stream_id: Option<String>,
    stream_revision: Option<u64>,
    decision_id: String,
    prior_submission_id: String,
    goal_id: Option<String>,
    successor_submission_id: Option<String>,
    pending: bool,
    #[serde(default, deserialize_with = "deserialize_present_continuation_bool")]
    continued: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_present_terminal_disposition")]
    #[schemars(with = "TerminalNotificationDisposition")]
    prior_completion_notification_disposition: Option<TerminalNotificationDisposition>,
}
fn deserialize_present_continuation_bool<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<bool>, D::Error> {
    bool::deserialize(deserializer).map(Some)
}
impl TryFrom<GoalContinuationNotificationFields> for GoalContinuationNotification {
    type Error = String;
    fn try_from(fields: GoalContinuationNotificationFields) -> Result<Self, String> {
        if fields.session_id.is_empty()
            || fields.decision_id.is_empty()
            || fields.prior_submission_id.is_empty()
            || fields.goal_id.as_ref().is_some_and(String::is_empty)
            || fields
                .successor_submission_id
                .as_ref()
                .is_some_and(String::is_empty)
            || fields.pending == fields.continued.is_some()
            || fields.pending == fields.prior_completion_notification_disposition.is_some()
            || fields.continued == Some(true)
                && fields.prior_completion_notification_disposition
                    != Some(TerminalNotificationDisposition::Suppress)
        {
            return Err(
                "continuation disposition requires identities, continued and prior presentation exactly when resolved, and suppression when admitted"
                    .into(),
            );
        }
        Ok(Self {
            session_id: fields.session_id,
            stream_id: fields.stream_id,
            stream_revision: fields.stream_revision,
            decision_id: fields.decision_id,
            prior_submission_id: fields.prior_submission_id,
            goal_id: fields.goal_id,
            successor_submission_id: fields.successor_submission_id,
            pending: fields.pending,
            continued: fields.continued,
            prior_completion_notification_disposition: fields
                .prior_completion_notification_disposition,
        })
    }
}
impl JsonSchema for GoalContinuationNotification {
    fn schema_name() -> String {
        "GoalContinuationNotification".into()
    }
    fn json_schema(generator: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        let mut schema = GoalContinuationNotificationFields::json_schema(generator);
        if let schemars::schema::Schema::Object(object) = &mut schema {
            object.subschemas.get_or_insert_with(Default::default).one_of=Some(vec![
                serde_json::from_value(serde_json::json!({"properties":{"pending":{"const":true}},"not":{"anyOf":[{"required":["continued"]},{"required":["priorCompletionNotificationDisposition"]}]}})).expect("pending schema is valid"),
                serde_json::from_value(serde_json::json!({"properties":{"pending":{"const":false},"continued":{"const":true},"priorCompletionNotificationDisposition":{"const":"suppress"}},"required":["continued","priorCompletionNotificationDisposition"]})).expect("admitted schema is valid"),
                serde_json::from_value(serde_json::json!({"properties":{"pending":{"const":false},"continued":{"const":false},"priorCompletionNotificationDisposition":{"enum":["inherit","suppress"]}},"required":["continued","priorCompletionNotificationDisposition"]})).expect("refused schema is valid"),
            ]);
        }
        schema
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
/// `turn/started` / `turn/failed` product notifications (requires
/// `productEvents`). `detail` carries the raw engine payload (`turnId` /
/// `error`) without re-modelling it here.
pub struct TurnLifecycleNotification {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal_continuation: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_origin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_revision: Option<u64>,
    pub submission_id: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_terminal_disposition"
    )]
    #[schemars(with = "TerminalNotificationDisposition")]
    pub terminal_notification_disposition: Option<TerminalNotificationDisposition>,
    #[schemars(schema_with = "any_schema")]
    pub detail: RuntimeValue,
}

/// `item/started` / `item/updated` / `item/completed` product notifications
/// (requires `productEvents`). `item` is the engine's ThreadItem product
/// object, passed through verbatim.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ItemNotification {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_revision: Option<u64>,
    pub submission_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[schemars(schema_with = "any_schema")]
    pub item: RuntimeValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inference: Option<PublicInference>,
}

/// `item/agentMessage/delta` / `item/reasoning/delta` product notifications
/// (requires `productEvents`).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DeltaNotification {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_revision: Option<u64>,
    pub submission_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[serde(with = "super::runtime_wire::string")]
    #[schemars(with = "String")]
    pub delta: RuntimeString,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inference: Option<PublicInference>,
}

/// `approval/request` server->client request params. `request` is the raw
/// `approval.requested` engine event request payload; `submissionId` echoes
/// the inner protocol event's submission id so shims can reconstruct it.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalRequestParams {
    pub session_id: String,
    pub submission_id: String,
    #[schemars(schema_with = "any_schema")]
    pub request: RuntimeValue,
}

/// Expected client result for `approval/request`: a raw `ApprovalResponse`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalRequestResult {
    #[schemars(schema_with = "any_schema")]
    pub response: RuntimeValue,
}

/// `elicitation/request` server->client request params. `elicitation` is the
/// raw inner-protocol elicitation payload (`{ requestId, request }`);
/// `submissionId` echoes the inner protocol event's submission id.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ElicitationRequestParams {
    pub session_id: String,
    pub submission_id: String,
    #[schemars(schema_with = "elicitation_schema")]
    pub elicitation: RuntimeValue,
}

/// Expected client result for `elicitation/request`: a raw `ElicitResponseWire`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ElicitationRequestResult {
    #[schemars(schema_with = "elicitation_response_schema")]
    pub response: RuntimeValue,
}

/// Preserve stable identifiers while admitting historical question definitions.
fn elicitation_schema(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    let tool = klaus_agent_core::tools::builtins::ask_user::create_ask_user_tool();
    let mut questions = tool.static_input_schema()["properties"]["questions"].clone();
    if let Some(required) = questions["items"]["required"].as_array_mut() {
        required.retain(|field| field != "id");
    }
    serde_json::from_value(serde_json::json!({
        "type": "object",
        "required": ["requestId", "request"],
        "properties": {
            "requestId": {"type": "string"},
            "request": {
                "type": "object", "required": ["prompt"],
                "properties": {
                    "prompt": {"type": "string"},
                    "options": {"type": "array", "items": {"type": "string"}},
                    "multiSelect": {"type": "boolean"},
                    "questions": questions,
                    "attachmentsSupported": {"type": "boolean"},
                    "metadata": {"type": "object", "additionalProperties": true}
                }
            }
        }
    }))
    .expect("elicitation schema is valid")
}

fn elicitation_response_schema(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    serde_json::from_value(serde_json::json!({
        "oneOf": [
            {"type": "object", "required": ["kind", "answers"], "properties": {
                "kind": {"const": "answers"},
                "answers": {"type": "object", "additionalProperties": {"type": "string"}},
                "attachments": {"type": "object", "description": "At most 20 images across answered questions, each at most 10 MiB decoded and at most 45 MiB in total.", "additionalProperties": {
                    "type": "array", "maxItems": 20, "items": {
                        "type": "object", "required": ["type", "base64", "mimeType"],
                        "properties": {
                            "type": {"const": "image"},
                            "base64": {"type": "string", "maxLength": 13981016},
                            "mimeType": {"enum": ["image/jpeg", "image/png", "image/gif", "image/webp"]}
                        }
                    }
                }}
            }},
            {"type": "object", "required": ["kind", "values"], "properties": {
                "kind": {"const": "accept"}, "values": {"type": "array", "items": {"type": "string"}}
            }},
            {"type": "object", "required": ["kind"], "properties": {
                "kind": {"const": "decline"}, "reason": {"type": "string"}
            }}
        ]
    })).expect("elicitation response schema is valid")
}

/// Schema for the exact string-or-content-parts Input wire codec.
fn raw_input_schema(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    serde_json::from_value(serde_json::json!({
        "title": "rawInput",
        "description": "A string or inline content parts using the exact Input codec. Native audio/video parts require an exact model-declared MIME type and bounded payloads. Paths are not attachment inputs.",
        "anyOf": [
            {"type": "string"},
            {"type": "array", "items": {"oneOf": [
                {"type":"object", "required":["type","text"], "properties":{
                    "type":{"const":"text"}, "text":{"type":"string"}
                }},
                {"type":"object", "required":["type","base64","mimeType"], "properties":{
                    "type":{"const":"image"}, "base64":{"type":"string"}, "mimeType":{"type":"string"}
                }},
                {"type":"object", "required":["type","base64","mimeType"], "properties":{
                    "type":{"const":"document"}, "base64":{"type":"string"}, "mimeType":{"type":"string"},
                    "filename":{"type":["string","null"]}
                }},
                {"type":"object", "required":["type","base64","mimeType"], "properties":{
                    "type":{"const":"media"}, "base64":{"type":"string"}, "mimeType":{"type":"string"},
                    "filename":{"type":["string","null"]}
                }}
            ]}}
        ]
    })).expect("raw input schema is valid")
}

/// Schema helper: raw passthrough JSON accepts anything.
fn session_config_update_schema(
    _: &mut schemars::gen::SchemaGenerator,
) -> schemars::schema::Schema {
    serde_json::from_value(serde_json::json!({
        "type":"object", "additionalProperties":false, "minProperties":1,
        "properties":{
            "ultraMode":{"type":"boolean"},
            "goal":{"type":"boolean"},
            "maximumGoalTokenBudget":{"anyOf":[
                {"type":"integer","minimum":1,"maximum":9007199254740991_i64},
                {"type":"null"}
            ]},
            "model":{"type":"string","minLength":1},
            "permissionMode":{"enum":["default","plan","acceptEdits","auto","bypassPermissions","dontAsk"]},
            "modelVerbosity":{"enum":[null,"low","medium","high"]},
            "reasoningSummary":{"enum":["auto","concise","detailed","none"]},
            "webSearchAccess":{"enum":["disabled","cached","indexed","live"]},
            "subagentModelCatalog":{"type":"boolean"},
            "subagentModelOptions":{"type":"array","maxItems":4096,"items":{
                "type":"object","additionalProperties":false,
                "properties":{"value":{"type":"string","minLength":1},"label":{"type":"string","minLength":1}},
                "required":["value","label"]
            }},
            "approvalReview":{"anyOf":[{"type":"null"},{"type":"boolean"},{
                "type":"object","additionalProperties":false,
                "properties":{
                    "transcript_mode":{"enum":["line","json"]},
                    "async_classifier_mode":{"enum":["snapshot","conversation"]},
                    "async_classifier_conversation_token_limit":{"type":"integer","minimum":1},
                    "enabled":{"type":"boolean"},
                    "free_guardian":{"type":"boolean","deprecated":true},
                    "thread_context":{"type":"boolean","deprecated":true},
                    "persist_scores":{"type":"boolean"},
                    "classifier_instructions":{"type":"string"},
                    "review_threshold":{"type":"number","minimum":0,"maximum":1},
                    "max_tool_call_lag":{"type":"integer","minimum":0},
                    "reasoning_effort":{"type":"string"},
                    "max_action_tokens":{"type":"integer","minimum":100,"maximum":100000},
                    "max_classifier_instruction_tokens":{"type":"integer","minimum":100,"maximum":100000},
                    "reuse_parent_compaction":{"type":"boolean"},
                    "max_parent_compaction_tokens":{"type":"integer","minimum":100,"maximum":100000},
                    "review_scope":{"type":"object","additionalProperties":false,"properties":{
                        "computer_use_only":{"type":"boolean"},"sandboxed_exec_commands":{"type":"boolean"}
                    }},
                    "transcript":{"type":"object","additionalProperties":false,"properties":{
                        "sources":{"type":"array","items":{"enum":["tool_calls","tool_outputs","reasoning"]}},
                        "include_images":{"type":"boolean"},
                        "max_message_entry_tokens":{"type":"integer","minimum":100,"maximum":100000},
                        "max_tool_entry_tokens":{"type":"integer","minimum":100,"maximum":100000},
                        "max_message_transcript_tokens":{"type":"integer","minimum":100,"maximum":100000},
                        "max_tool_transcript_tokens":{"type":"integer","minimum":100,"maximum":100000},
                        "max_recent_non_user_entries":{"type":"integer","minimum":1}
                    }}
                }
            }]}

        }
    })).expect("session configuration schema is valid")
}

fn any_schema(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    schemars::schema::Schema::Bool(true)
}

fn any_schema_opt(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    schemars::schema::Schema::Bool(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_request_notification_and_response() {
        match classify(&serde_json::json!({ "id": 1, "method": "initialize", "params": {} })) {
            Incoming::Request { method, .. } => assert_eq!(method, "initialize"),
            _ => panic!("expected request"),
        }
        match classify(&serde_json::json!({ "method": "initialized" })) {
            Incoming::Notification { method, .. } => assert_eq!(method, "initialized"),
            _ => panic!("expected notification"),
        }
        match classify(&serde_json::json!({ "id": 7, "result": { "ok": true } })) {
            Incoming::Response { result: Ok(v), .. } => assert_eq!(v["ok"], true),
            _ => panic!("expected success response"),
        }
        match classify(&serde_json::json!({ "id": 7, "error": { "code": -1, "message": "x" } })) {
            Incoming::Response { result: Err(e), .. } => assert_eq!(e["code"], -1),
            _ => panic!("expected error response"),
        }
        assert!(matches!(
            classify(&serde_json::json!(42)),
            Incoming::Invalid
        ));
        assert!(matches!(
            classify(&serde_json::json!({})),
            Incoming::Invalid
        ));
    }

    #[test]
    fn params_round_trip_in_camel_case() {
        let params: SessionStartParams = serde_json::from_value(serde_json::json!({
            "init": { "cwd": "/tmp" },
            "resume": "thread_1"
        }))
        .unwrap();
        assert_eq!(params.resume.as_deref(), Some("thread_1"));
        let encoded = serde_json::to_value(&TurnStartResult {
            turn_id: "turn_1".into(),
        })
        .unwrap();
        assert_eq!(encoded, serde_json::json!({ "turnId": "turn_1" }));
    }

    #[test]
    fn plugin_install_version_guard_is_optional_and_keeps_empty_string() {
        for expected in [None, Some("1.0.0"), Some("")] {
            let mut value = serde_json::json!({
                "marketplace": "local",
                "sourceDir": "/tmp/plugin-package",
                "version": "2.0.0"
            });
            if let Some(expected) = expected {
                value["expectedInstalledVersion"] = serde_json::json!(expected);
            }
            let params: SessionPluginInstallParams = serde_json::from_value(value.clone()).unwrap();
            assert_eq!(params.expected_installed_version.as_deref(), expected);
            assert_eq!(serde_json::to_value(params).unwrap(), value);
        }
    }
}

/// Select an existing provider entry with an explicit realtime model declaration.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RealtimeCapabilitiesParams {
    pub auth_mode: Option<String>,
    pub model_provider_id: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RealtimeCapabilitiesResult {
    pub available: bool,
    pub source_id: String,
    pub protocol: String,
    pub voices: Vec<String>,
    pub default_voice: String,
    pub transcript_source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RealtimeExistingCall {
    pub r#type: String,
    pub call_id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RealtimeStartParams {
    pub auth_mode: Option<String>,
    pub protocol: Option<String>,
    pub voice: Option<String>,
    pub session_id: String,
    pub request_id: String,
    pub model_provider_id: Option<String>,
    pub offer_sdp: Option<String>,
    pub transport: Option<RealtimeExistingCall>,
    #[serde(default)]
    pub flush_transcript_tail_on_session_end: bool,
    pub instructions: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RealtimeStartResult {
    pub protocol: String,
    pub transcript_source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub answer_sdp: Option<String>,
    pub call_id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RealtimeSessionParams {
    pub session_id: String,
    pub request_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DictationStartParams {
    pub operation_id: String,
    pub source: String,
    /// Optional recognition language hint for sources that require one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refinement: Option<DictationRefinementConfig>,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum DictationRefinementRole {
    User,
    Assistant,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DictationRefinementMessage {
    pub role: DictationRefinementRole,
    /// UTF-8 bytes are validated at the facade boundary.
    #[schemars(length(max = 8192))]
    pub content: String,
}
#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DictationRefinementConfig {
    #[schemars(length(max = 40))]
    pub messages: Vec<DictationRefinementMessage>,
}
impl DictationRefinementConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.messages.len() > 40
            || self
                .messages
                .iter()
                .any(|message| message.content.len() > 8192 || message.content.contains('\0'))
            || self
                .messages
                .iter()
                .map(|message| message.content.len())
                .sum::<usize>()
                > 32768
        {
            return Err("dictation-refinement-invalid");
        }
        Ok(())
    }
}
impl<'de> Deserialize<'de> for DictationRefinementConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize, JsonSchema)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            messages: Vec<DictationRefinementMessage>,
        }
        let raw = Raw::deserialize(deserializer)?;
        let value = Self {
            messages: raw.messages,
        };
        value.validate().map_err(serde::de::Error::custom)?;
        Ok(value)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DictationAudioParams {
    pub operation_id: String,
    pub seq: u64,
    pub audio_base64: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DictationEndParams {
    pub operation_id: String,
    #[serde(default)]
    pub cancel: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RealtimeAppendTextParams {
    pub session_id: String,
    pub request_id: String,
    pub role: String,
    pub text: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RealtimeAppendSpeechParams {
    pub session_id: String,
    pub request_id: String,
    pub text: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RealtimeAppendAudioParams {
    pub session_id: String,
    pub request_id: String,
    pub audio_base64: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RealtimeHandoffParams {
    pub session_id: String,
    pub request_id: String,
    pub handoff_id: String,
    pub text: String,
}

/// Independent collaboration settings. Legacy aliases decode inside the engine.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionCollaborationModeParams {
    pub session_id: Option<String>,
    pub thread_id: Option<String>,
    pub mode: CollaborationMode,
    pub expected_revision: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionPlanDocumentParams {
    pub session_id: Option<String>,
    pub thread_id: Option<String>,
    pub plan_id: String,
    pub revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionSavePlanDocumentParams {
    pub session_id: String,
    pub plan_id: String,
    pub revision: u64,
    #[serde(with = "super::runtime_wire::string")]
    #[schemars(with = "String")]
    pub content: RuntimeString,
    #[serde(default, with = "super::runtime_wire::optional_string")]
    #[schemars(with = "Option<String>")]
    pub expected_content: Option<RuntimeString>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionAcceptPlanParams {
    pub session_id: String,
    pub plan_id: String,
    pub revision: u64,
    pub client_request_id: String,
    #[serde(default, with = "super::runtime_wire::optional_string")]
    #[schemars(with = "Option<String>")]
    pub content: Option<RuntimeString>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionPlanFeedbackParams {
    pub session_id: String,
    pub plan_id: String,
    pub revision: u64,
    pub client_request_id: String,
    #[serde(with = "super::runtime_wire::string")]
    #[schemars(with = "String")]
    pub feedback: RuntimeString,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionSettlePlanImplementationParams {
    pub session_id: Option<String>,
    pub thread_id: Option<String>,
    pub plan_id: String,
    pub revision: u64,
    pub client_request_id: String,
    pub outcome: PlanAdmissionOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PlanAdmissionOutcome {
    Started,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum UserInputResponseStatus {
    Answered,
    Skipped,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UserInputAnswer {
    #[serde(with = "super::runtime_wire::strings")]
    #[schemars(with = "Vec<String>")]
    pub answers: Vec<RuntimeString>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionAnswerUserInputParams {
    pub session_id: String,
    pub request_id: String,
    pub turn_id: String,
    pub tool_call_id: String,
    pub answers: std::collections::BTreeMap<String, UserInputAnswer>,
    pub status: UserInputResponseStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProposedPlan {
    pub id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub revision: u64,
    #[serde(with = "super::runtime_wire::string")]
    #[schemars(with = "String")]
    pub content: RuntimeString,
    pub status: PlanArtifactStatus,
    pub review_pending: bool,
    pub document_path: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PlanArtifactStatus {
    Writing,
    Proposed,
    Accepted,
    Superseded,
    Interrupted,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProgressStepStatus {
    Pending,
    InProgress,
    Completed,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProgressStep {
    #[serde(with = "super::runtime_wire::string")]
    #[schemars(with = "String")]
    pub step: RuntimeString,
    pub status: ProgressStepStatus,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PlanningProgress {
    #[serde(default, with = "super::runtime_wire::optional_string")]
    #[schemars(with = "Option<String>")]
    pub explanation: Option<RuntimeString>,
    pub plan: Vec<ProgressStep>,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PlanImplementationReceipt {
    pub client_request_id: String,
    pub plan_id: String,
    pub revision: u64,
    #[serde(with = "super::runtime_wire::string")]
    #[schemars(with = "String")]
    pub input: RuntimeString,
    pub implementation_id: String,
    pub kind: PlanActionKind,
    pub admission: PlanAdmissionStatus,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PlanActionKind {
    Implementation,
    Feedback,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PlanAdmissionStatus {
    Pending,
    Started,
    Failed,
    Cancelled,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PlanningSnapshot {
    pub mode: CollaborationMode,
    pub revision: u64,
    pub plans: Vec<ProposedPlan>,
    pub progress: Option<PlanningProgress>,
    pub implementation: Option<PlanImplementationReceipt>,
    pub questions: Vec<PlanningQuestionRecord>,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CollaborationMode {
    #[serde(
        alias = "code",
        alias = "pair_programming",
        alias = "execute",
        alias = "custom"
    )]
    Default,
    Plan,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PlanDocument {
    pub plan_id: String,
    pub revision: u64,
    pub path: String,
    #[serde(with = "super::runtime_wire::string")]
    #[schemars(with = "String")]
    pub content: RuntimeString,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PlanningResult {
    pub planning: PlanningSnapshot,
    pub document: Option<PlanDocument>,
    #[serde(default, with = "super::runtime_wire::optional_string")]
    #[schemars(with = "Option<String>")]
    pub implementation_input: Option<RuntimeString>,
    #[serde(default, with = "super::runtime_wire::optional_string")]
    #[schemars(with = "Option<String>")]
    pub feedback_input: Option<RuntimeString>,
    pub implementation_id: Option<String>,
    pub accepted: Option<bool>,
}

/// Read a sleeping thread without initializing provider transports or a turn.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionPlanningAddressParams {
    pub session_id: Option<String>,
    pub thread_id: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionReadPlanDocumentParams {
    pub session_id: Option<String>,
    pub thread_id: Option<String>,
    pub plan_id: String,
    pub revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PlanningQuestionStatus {
    Pending,
    Answered,
    Skipped,
    Cancelled,
    Expired,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PlanningQuestionRecord {
    pub request_id: String,
    pub turn_id: String,
    pub tool_call_id: String,
    pub status: PlanningQuestionStatus,
    pub request: PlanningQuestionRequest,
    pub answers: Option<std::collections::BTreeMap<String, UserInputAnswer>>,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PlanningQuestionRequest {
    #[serde(with = "super::runtime_wire::string")]
    #[schemars(with = "String")]
    pub prompt: RuntimeString,
    pub questions: Vec<PlanningQuestion>,
    pub metadata: PlanningQuestionMetadata,
    pub attachments_supported: Option<bool>,
    #[serde(default, with = "super::runtime_wire::optional_strings")]
    #[schemars(with = "Option<Vec<String>>")]
    pub options: Option<Vec<RuntimeString>>,
    pub multi_select: Option<bool>,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PlanningQuestionMetadata {
    pub kind: String,
    pub is_blocking: bool,
    pub turn_id: String,
    pub tool_call_id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PlanningQuestion {
    pub id: String,
    #[serde(with = "super::runtime_wire::string")]
    #[schemars(with = "String")]
    pub header: RuntimeString,
    #[serde(with = "super::runtime_wire::string")]
    #[schemars(with = "String")]
    pub question: RuntimeString,
    pub options: Vec<PlanningQuestionOption>,
    pub is_other: Option<bool>,
    pub is_secret: Option<bool>,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PlanningQuestionOption {
    #[serde(with = "super::runtime_wire::string")]
    #[schemars(with = "String")]
    pub label: RuntimeString,
    #[serde(with = "super::runtime_wire::string")]
    #[schemars(with = "String")]
    pub description: RuntimeString,
}

/// Schema projection of the closed per-model initialization metadata contract.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ModelCapabilitiesInitSchema {
    pub tools: Option<bool>,
    pub forced_tool_choice_with_thinking: Option<bool>,
    pub effort: Option<ModelEffortCapabilityInitSchema>,
    pub output_verbosity: Option<ModelOutputVerbosityInitSchema>,
    pub auto_review_model_override: Option<String>,
    #[serde(default)]
    pub requires_sandboxed_review: bool,
    #[serde(default)]
    pub node_repl_auto_review_required: bool,
    pub guardian: Option<serde_json::Map<String, Value>>,
    pub guardian_v2: Option<serde_json::Map<String, Value>>,
    pub model_messages: Option<serde_json::Map<String, Value>>,
    pub comp_hash: Option<String>,
    pub supports_reasoning_summary_parameter: Option<bool>,
    pub default_reasoning_summary: Option<ModelReasoningSummaryInitSchema>,
    #[serde(default)]
    pub supported_reasoning_efforts: Vec<String>,
    pub default_reasoning_effort: Option<String>,
    pub multi_agent_reasoning_effort: Option<String>,
    pub effective_context_window_percent: Option<i64>,
    #[serde(default)]
    pub used_fallback_model_metadata: bool,
    pub model_specialty: Option<String>,
    #[serde(default)]
    pub supports_image_detail_original: bool,
    pub thinking: Option<ModelThinkingCapabilityInitSchema>,
    #[serde(default)]
    pub structured_output: bool,
    #[serde(default)]
    pub web_search: bool,
    #[serde(default)]
    pub prompt_cache_control: bool,
    #[serde(default)]
    pub native_pdf: bool,
    #[serde(default)]
    pub vision: bool,
    #[serde(default)]
    pub one_million_context: bool,
    #[serde(default)]
    pub task_budgets: bool,
    #[serde(default)]
    pub fast_mode: bool,
    pub context_window: Option<u64>,
    pub max_output_tokens: Option<u64>,
    pub image_limits: Option<ModelImageLimitsInitSchema>,
    pub max_media_blocks: Option<std::num::NonZeroU32>,
    pub reasoning_dialect: Option<String>,
    pub gemini_thinking: Option<ModelGeminiThinkingInitSchema>,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ModelCapabilitiesEntrySchema {
    pub model: String,
    pub capabilities: ModelCapabilitiesInitSchema,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelEffortCapabilityInitSchema {
    #[serde(default)]
    pub levels: Vec<String>,
    pub default: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ModelOutputVerbosityInitSchema {
    Low,
    Medium,
    High,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ModelReasoningSummaryInitSchema {
    Auto,
    Concise,
    Detailed,
    None,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelThinkingCapabilityInitSchema {
    #[serde(default)]
    pub adaptive: bool,
    #[serde(default)]
    pub interleaved: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ModelImageLimitsInitSchema {
    pub max_width: Option<std::num::NonZeroU32>,
    pub max_height: Option<std::num::NonZeroU32>,
    pub max_base64_size: Option<std::num::NonZeroU32>,
    pub target_raw_size: Option<std::num::NonZeroU32>,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ModelGeminiThinkingInitSchema {
    pub mode: String,
    pub min: Option<u64>,
    pub max: Option<u64>,
    #[serde(default)]
    pub can_disable: bool,
}

#[cfg(test)]
mod approval_review_metadata_schema_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn session_start_schema_publishes_the_closed_review_metadata_bridge() {
        let schema = serde_json::to_value(schemars::schema_for!(SessionRoutingInitSchema)).unwrap();
        let metadata = &schema["definitions"]["ModelCapabilitiesInitSchema"];
        assert_eq!(metadata["additionalProperties"], false);
        for key in [
            "autoReviewModelOverride",
            "requiresSandboxedReview",
            "nodeReplAutoReviewRequired",
            "guardian",
            "guardianV2",
            "modelMessages",
            "compHash",
            "supportsReasoningSummaryParameter",
            "defaultReasoningSummary",
            "supportedReasoningEfforts",
            "defaultReasoningEffort",
            "multiAgentReasoningEffort",
            "effectiveContextWindowPercent",
            "usedFallbackModelMetadata",
            "modelSpecialty",
            "supportsImageDetailOriginal",
        ] {
            assert!(
                metadata["properties"][key].is_object(),
                "missing metadata field {key}"
            );
        }
        assert!(schema["properties"]["capabilities"].is_object());
        assert!(schema["properties"]["capabilitiesByModel"].is_object());
        let encoded = serde_json::to_string(&schema).unwrap();
        assert!(encoded.contains("ModelCapabilitiesEntrySchema"));
    }

    #[test]
    fn model_init_schema_accepts_optional_multi_agent_effort_metadata() {
        let absent: ModelCapabilitiesInitSchema =
            serde_json::from_value(serde_json::json!({})).unwrap();
        assert!(absent.multi_agent_reasoning_effort.is_none());
        let declared: ModelCapabilitiesInitSchema = serde_json::from_value(serde_json::json!({
            "multiAgentReasoningEffort": "custom/Exact"
        }))
        .unwrap();
        assert_eq!(
            declared.multi_agent_reasoning_effort.as_deref(),
            Some("custom/Exact")
        );
        assert_eq!(
            serde_json::to_value(declared).unwrap()["multiAgentReasoningEffort"],
            "custom/Exact"
        );
    }

    #[test]
    fn model_init_schema_preserves_optional_forced_tool_choice_thinking_metadata() {
        let absent: ModelCapabilitiesInitSchema = serde_json::from_value(json!({})).unwrap();
        assert_eq!(absent.forced_tool_choice_with_thinking, None);
        for supported in [true, false] {
            let declared: ModelCapabilitiesInitSchema = serde_json::from_value(json!({
                "forcedToolChoiceWithThinking": supported
            }))
            .unwrap();
            assert_eq!(declared.forced_tool_choice_with_thinking, Some(supported));
        }
        assert!(
            serde_json::from_value::<ModelCapabilitiesInitSchema>(json!({
                "forcedToolChoiceWithThinking": "false"
            }))
            .is_err()
        );
        let schema =
            serde_json::to_value(schemars::schema_for!(ModelCapabilitiesInitSchema)).unwrap();
        assert_eq!(schema["additionalProperties"], json!(false));
        assert!(schema["properties"]["forcedToolChoiceWithThinking"].is_object());
    }
}

fn optional_approval_policy_schema(
    _: &mut schemars::gen::SchemaGenerator,
) -> schemars::schema::Schema {
    serde_json::from_value(serde_json::json!({
        "anyOf":[
            {"type":"null"},
            {"enum":["untrusted","on-request","never"]},
            {"type":"object","additionalProperties":false,"required":["granular"],"properties":{
                "granular":{"type":"object","required":["sandbox_approval","rules","mcp_elicitations"],"properties":{
                    "sandbox_approval":{"type":"boolean"},
                    "rules":{"type":"boolean"},
                    "skill_approval":{"type":"boolean","default":false},
                    "request_permissions":{"type":"boolean","default":false},
                    "mcp_elicitations":{"type":"boolean"}
                }}
            }}
        ]
    })).expect("approval policy schema is valid")
}

fn optional_approvals_reviewer_schema(
    _: &mut schemars::gen::SchemaGenerator,
) -> schemars::schema::Schema {
    serde_json::from_value(serde_json::json!({
        "anyOf": [
            {"type": "null"},
            {"type": "string", "enum": ["user", "auto_review"]}
        ]
    }))
    .expect("approval reviewer schema is valid")
}
