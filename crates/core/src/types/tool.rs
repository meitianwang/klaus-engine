//! The Tool trait + the `ToolContext` seam a tool's `call` receives.

use crate::browser::{BrowserHostToolMapping, BrowserRuntime};
use crate::hooks::types::HookEvent;
use crate::lsp::types::LspController;
use crate::runtime_process::{RuntimeEnv, RuntimeProcessContext};
use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeMap, RuntimeObject, RuntimeObjectEncoder, RuntimePath,
    RuntimeString,
};
use crate::schemas::content::{ContentBlock, OutputBlock};
use crate::schemas::effort::EffortValue;
use crate::types::abort::AbortSignal;
use crate::types::events::{EngineEvent, ToolDomainEvent};
use crate::types::file_state::FileStateCache;
use crate::types::json::{Json, JsonSchema};
use crate::types::permission::{PermissionContext, PermissionResult};
use crate::types::provider::ToolSpec;
use crate::types::sandbox::SandboxBackend;
use async_trait::async_trait;
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;

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

/// A boxed future returned by a context callback.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// An engine event stream (an async stream of `EngineEvent`).
pub type EngineEventStream = Pin<Box<dyn Stream<Item = EngineEvent> + Send>>;

/// How prominently a tool is offered to the model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolExposure {
    Direct,
    Deferred,
    DirectModelOnly,
    Hidden,
}

impl RuntimeEncode for ToolExposure {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Direct => "direct",
            Self::Deferred => "deferred",
            Self::DirectModelOnly => "direct_model_only",
            Self::Hidden => "hidden",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for ToolExposure {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("direct", &[]),
                RuntimeEnumVariant::new("deferred", &[]),
                RuntimeEnumVariant::new("direct_model_only", &[]),
                RuntimeEnumVariant::new("hidden", &[]),
            ],
            &[
                Self::Direct,
                Self::Deferred,
                Self::DirectModelOnly,
                Self::Hidden,
            ],
        )
    }
}

impl ToolExposure {
    /// Whether the tool belongs in the current model request. Deferred tools
    /// become visible only after their stable internal name has been surfaced.
    pub(crate) fn is_model_visible(self, surfaced: bool) -> bool {
        match self {
            Self::Direct | Self::DirectModelOnly => true,
            Self::Deferred => surfaced,
            Self::Hidden => false,
        }
    }

    /// Whether an otherwise-visible tool may be bridged into nested code mode.
    /// Only the model-only exposure is excluded here. Currently exercised by
    /// the exposure-contract tests; the production bridge wiring re-attaches
    /// here when the session-level code-mode seam lands.
    #[cfg(test)]
    pub(crate) fn allows_code_mode_bridge(self) -> bool {
        match self {
            Self::Direct | Self::Deferred | Self::Hidden => true,
            Self::DirectModelOnly => false,
        }
    }
}

/// The model-facing input style a tool exposes through provider specs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolInputKind {
    JsonSchema,
    Freeform { format: RuntimeString },
}

impl RuntimeEncode for ToolInputKind {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let (tag, fields) = match self {
            Self::JsonSchema => ("jsonSchema", RuntimeObjectEncoder::new()),
            Self::Freeform { format } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("format", format)?;
                ("freeform", fields)
            }
        };
        runtime_codec::encode_internal(out, "type", &[], tag, &fields.finish())
    }
}

impl RuntimeDecode for ToolInputKind {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            false,
            "type",
            &[],
            &[
                RuntimeEnumVariant::new("jsonSchema", &[]),
                RuntimeEnumVariant::new("freeform", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::JsonSchema),
                1 => Ok(Self::Freeform {
                    format: object.required("format", &[])?,
                }),
                _ => Err(RuntimeCodecError::new(
                    RuntimeCodecErrorKind::UnknownEnumVariant,
                )),
            },
        )
    }
}

/// The result of a tool's `validate_input`.
#[derive(Debug, Clone, PartialEq)]
pub enum ValidationResult {
    Ok,
    Err {
        message: String,
        error_code: Option<i64>,
    },
    /// A trusted post-parse value caused the fixed implementation to throw.
    /// This halts the turn and must never be downgraded into a model-visible
    /// validation error or routed through Rust's panic machinery.
    Fatal {
        error: crate::types::errors::ToolError,
    },
}

/// Where a sub-agent runs relative to the parent's swarm team.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwarmSpawn {
    pub peer: RuntimeString,
}

/// Isolation mode for a spawned sub-agent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Isolation {
    /// Run in its own isolated git worktree.
    Worktree,
}

/// How much of the parent conversation a spawned sub-agent inherits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForkContext {
    All,
    None,
    Recent(NonZeroUsize),
    /// A trusted post-validation positive JS number whose exact value cannot be
    /// represented by [`ForkContext::Recent`] (for example `0.5` or `1e20`).
    /// Session forking applies JavaScript `Math.trunc` semantics at the point
    /// where the parent conversation is snapshotted.
    PositiveNumber(PositiveForkNumber),
}

impl RuntimeEncode for ForkContext {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        match self {
            Self::All => true.encode_runtime(out),
            Self::None => false.encode_runtime(out),
            Self::Recent(count) => count.get().encode_runtime(out),
            Self::PositiveNumber(value) => value.get().encode_runtime(out),
        }
    }
}

impl RuntimeDecode for ForkContext {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        if let RuntimeJson::Bool(value) = value {
            return Ok(if *value { Self::All } else { Self::None });
        }
        let Some(number) = value.as_number().map(|number| number.as_f64()) else {
            return Err(RuntimeCodecError::new(
                RuntimeCodecErrorKind::NoMatchingUntaggedVariant,
            ));
        };
        let positive = PositiveForkNumber::new(number).ok_or_else(|| {
            RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(RuntimeString::from(
                "fork context count must be positive and finite",
            )))
        })?;
        if number.fract() == 0.0 && number < 2.0_f64.powi(usize::BITS as i32) {
            let count = NonZeroUsize::new(number as usize).ok_or_else(|| {
                RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(RuntimeString::from(
                    "fork context count must be positive",
                )))
            })?;
            Ok(Self::Recent(count))
        } else {
            Ok(Self::PositiveNumber(positive))
        }
    }
}

/// A finite positive JavaScript number retained bit-for-bit for trusted
/// `fork_context` rewrites.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PositiveForkNumber(u64);

impl PositiveForkNumber {
    /// Retain a finite positive number by its IEEE-754 bits.
    pub fn new(value: f64) -> Option<Self> {
        (value.is_finite() && value > 0.0).then(|| Self(value.to_bits()))
    }

    /// Recover the exact retained number.
    pub fn get(self) -> f64 {
        f64::from_bits(self.0)
    }

    pub(crate) fn truncated_turns(self) -> usize {
        let value = self.get().trunc();
        if value < 1.0 {
            0
        } else if value >= 2.0_f64.powi(usize::BITS as i32) {
            usize::MAX
        } else {
            value as usize
        }
    }
}

impl Serialize for ForkContext {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::All => serializer.serialize_bool(true),
            Self::None => serializer.serialize_bool(false),
            Self::Recent(count) => serializer.serialize_u64(count.get() as u64),
            Self::PositiveNumber(value) => serializer.serialize_f64(value.get()),
        }
    }
}

impl<'de> Deserialize<'de> for ForkContext {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ForkContextVisitor;

        impl serde::de::Visitor<'_> for ForkContextVisitor {
            type Value = ForkContext;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a boolean or a positive number")
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(if value {
                    ForkContext::All
                } else {
                    ForkContext::None
                })
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if value == 0 {
                    return Err(E::custom("fork context count must be positive"));
                }
                Ok(usize::try_from(value)
                    .ok()
                    .and_then(NonZeroUsize::new)
                    .map(ForkContext::Recent)
                    .unwrap_or_else(|| {
                        ForkContext::PositiveNumber(
                            PositiveForkNumber::new(value as f64)
                                .expect("positive u64 is a finite positive f64"),
                        )
                    }))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let value = u64::try_from(value)
                    .map_err(|_| E::custom("fork context count must be positive"))?;
                self.visit_u64(value)
            }

            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let value = PositiveForkNumber::new(value)
                    .ok_or_else(|| E::custom("fork context count must be positive and finite"))?;
                let number = value.get();
                if number.fract() == 0.0 && number < 2.0_f64.powi(usize::BITS as i32) {
                    let count = NonZeroUsize::new(number as usize)
                        .expect("positive integral fork number is nonzero");
                    Ok(ForkContext::Recent(count))
                } else {
                    Ok(ForkContext::PositiveNumber(value))
                }
            }
        }

        deserializer.deserialize_any(ForkContextVisitor)
    }
}

/// An opaque step snapshot, created only by the admitted query.
#[doc(hidden)]
#[derive(Clone)]
pub struct InheritedSubagentContext {
    pub(crate) system_prompt: Vec<crate::types::provider::SystemPromptPart>,
    pub(crate) tools: Vec<Arc<dyn Tool>>,
    pub(crate) mcp: crate::engine::session::SubagentMcpCapture,
}

#[derive(Clone, Default)]
pub struct SpawnSubAgentOptions {
    pub prompt: RuntimeString,
    /// Stable message-routing name, separate from the role and task description.
    pub name: Option<RuntimeString>,
    /// Non-string `agentType` value forwarded by Task after a trusted rewrite.
    /// Trusted rewrites forward every truthy value without a second schema parse; the
    /// typed field below remains the normal path for strings and engine callers.
    pub task_agent_type_raw: Option<Json>,
    pub agent_type: Option<RuntimeString>,
    /// Restrict the child's tool set. Defaults to the parent's FULL set.
    pub tools: Option<Vec<Arc<dyn Tool>>>,
    /// Auto-approve these tool names in the child's permission context WITHOUT
    /// restricting which tools it can see.
    pub auto_allow_tools: Option<Vec<RuntimeString>>,
    pub model: Option<RuntimeString>,
    /// The `model` selection is an internal feature-slot target (background
    /// memory maintenance forks), not a user-requested subagent model: it skips
    /// the tier-only subagent model policy and resolves against the captured
    /// feature targets, falling back to the parent bundle when unavailable.
    pub feature_target_model: bool,
    /// Canonical policy restrictions, intersected with the parent authority.
    pub runtime_policy: Option<crate::engine::session::RuntimePolicyUpdate>,
    /// Per-call reasoning effort for the child.
    pub effort: Option<EffortValue>,
    /// Raw parent-step selection, below configured defaults, call fields and
    /// explicit role fields. A captured None remains an explicit snapshot.
    pub inherited_effort: Option<Option<EffortValue>>,
    /// Trusted query-owned snapshot; never decoded from tool arguments.
    #[doc(hidden)]
    pub inherited_context: Option<Arc<InheritedSubagentContext>>,
    /// Compatibility input retained for internal callers. Child policy is
    /// derived from the final raw reasoning selection.
    pub ultra_mode: Option<bool>,
    /// Compatibility snapshot retained for internal callers.
    pub inherited_ultra_mode: Option<bool>,
    /// Resume from a prior sub-agent's transcript, by its agentId.
    pub resume: Option<RuntimeString>,
    /// Non-string `resume` value forwarded by Task after a trusted rewrite.
    /// Kept separate from `resume` so internal typed resume updates (persistent
    /// teammate wakes, workflow callers) retain their existing behavior.
    pub task_resume_raw: Option<Json>,
    /// Override how much parent context the child inherits.
    pub fork_context: Option<ForkContext>,
    /// Force structured output matching this schema.
    pub output_schema: Option<RuntimeObject>,
    /// Run this sub-agent in its own isolated git worktree. Mutually exclusive
    /// with `cwd`.
    pub isolation: Option<Isolation>,
    /// Run this sub-agent under a different working directory. Takes precedence
    /// over a worktree. Mutually exclusive with `isolation`.
    pub cwd: Option<RuntimePath>,
    /// Place this sub-agent into the parent's swarm team as a named peer.
    pub swarm: Option<SwarmSpawn>,
    /// Run as a PERSISTENT teammate.
    pub persistent: Option<bool>,
    /// Scope this sub-agent to a memory directory (the auto-memory extraction
    /// fork).
    pub memory_dir: Option<RuntimePath>,
}

/// A foreground sub-agent run: its externally visible `agent_id` plus its events.
/// Trusted Task rewrites can forward a non-string `resume`, and foreground
/// progress preserves that raw JSON value. Background handles remain UUIDs.
pub struct ForegroundSubAgentRun {
    pub agent_id: Json,
    pub events: EngineEventStream,
}

/// Result of starting a background (fire-and-forget) sub-agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpawnBackgroundAgentResult {
    pub agent_id: RuntimeString,
    /// Absolute path the agent's final result is written to when it completes.
    pub output_file: RuntimePath,
}

impl RuntimeEncode for SpawnBackgroundAgentResult {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("agentId", &self.agent_id)?;
        object.field("outputFile", &self.output_file)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for SpawnBackgroundAgentResult {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                agent_id: object.required("agentId", &[])?,
                output_file: object.required("outputFile", &[])?,
            })
        })
    }
}

impl SpawnSubAgentOptions {
    /// Explicit role fields precede call fields; a captured parent None must not
    /// silently restore an older session's Ultra config.
    pub(crate) fn effective_effort(
        &self,
        role: Option<EffortValue>,
        session: Option<EffortValue>,
    ) -> Option<EffortValue> {
        role.or_else(|| self.effort.clone())
            .or_else(|| self.inherited_effort.clone().unwrap_or(session))
    }
}

/// Spawn an in-process sub-agent; yields its event stream (workflow runtime
/// path).
pub type SpawnSubAgent = Arc<dyn Fn(SpawnSubAgentOptions) -> EngineEventStream + Send + Sync>;

/// Spawn an in-process foreground sub-agent, returning its id + event stream.
pub type SpawnForegroundSubAgent = Arc<
    dyn Fn(SpawnSubAgentOptions) -> BoxFuture<'static, Result<ForegroundSubAgentRun, RuntimeString>>
        + Send
        + Sync,
>;

/// Start a sub-agent in the BACKGROUND.
pub type SpawnBackgroundAgent = Arc<
    dyn Fn(
            SpawnSubAgentOptions,
        ) -> BoxFuture<'static, Result<SpawnBackgroundAgentResult, RuntimeString>>
        + Send
        + Sync,
>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElicitRequest {
    pub prompt: RuntimeString,
    /// Validated structured questions, preserved verbatim for the client.
    pub questions: Option<Vec<RuntimeObject>>,
    /// Set only when this handler validates and forwards question image attachments.
    pub attachments_supported: Option<bool>,
    /// Optional caller attribution; never treated as a user answer.
    pub metadata: Option<RuntimeObject>,
    /// Choices to present; omit for free-text.
    pub options: Option<Vec<RuntimeString>>,
    pub multi_select: Option<bool>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ElicitResponse {
    UserInput {
        answers: RuntimeMap<Vec<RuntimeString>>,
        status: RuntimeString,
    },
    /// Answers keyed by stable question ID, or by text for historical definitions.
    Answers {
        answers: RuntimeMap<RuntimeString>,
        /// User-supplied images keyed by an answered question.
        attachments: Option<Box<RuntimeMap<Vec<ContentBlock>>>>,
    },
    Accept {
        values: Vec<RuntimeString>,
    },
    Decline {
        reason: Option<RuntimeString>,
    },
}

/// Host-supplied handler that asks the user a structured question.
#[async_trait]
pub trait ElicitHandler: Send + Sync {
    async fn elicit(&self, req: ElicitRequest, signal: AbortSignal) -> ElicitResponse;
}

/// One MCP resource descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpResourceDescriptor {
    pub uri: RuntimeString,
    pub name: Option<RuntimeString>,
    pub mime_type: Option<RuntimeString>,
    pub description: Option<RuntimeString>,
}

impl RuntimeEncode for McpResourceDescriptor {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("uri", &self.uri)?;
        if let Some(value) = &self.name {
            object.field("name", value)?;
        }
        if let Some(value) = &self.mime_type {
            object.field("mimeType", value)?;
        }
        if let Some(value) = &self.description {
            object.field("description", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for McpResourceDescriptor {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                uri: object.required("uri", &[])?,
                name: object.optional("name", &[])?,
                mime_type: object.optional("mimeType", &[])?,
                description: object.optional("description", &[])?,
            })
        })
    }
}

/// One block returned by reading an MCP resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpResourceContent {
    pub uri: Option<RuntimeString>,
    pub text: Option<RuntimeString>,
    pub mime_type: Option<RuntimeString>,
    pub blob: Option<RuntimeString>,
}

impl RuntimeEncode for McpResourceContent {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        if let Some(value) = &self.uri {
            object.field("uri", value)?;
        }
        if let Some(value) = &self.text {
            object.field("text", value)?;
        }
        if let Some(value) = &self.mime_type {
            object.field("mimeType", value)?;
        }
        if let Some(value) = &self.blob {
            object.field("blob", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for McpResourceContent {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                uri: object.optional("uri", &[])?,
                text: object.optional("text", &[])?,
                mime_type: object.optional("mimeType", &[])?,
                blob: object.optional("blob", &[])?,
            })
        })
    }
}

/// One argument a prompt advertises.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpPromptArgument {
    pub name: RuntimeString,
    pub description: Option<RuntimeString>,
    pub required: Option<bool>,
}

impl RuntimeEncode for McpPromptArgument {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("name", &self.name)?;
        if let Some(value) = &self.description {
            object.field("description", value)?;
        }
        if let Some(value) = &self.required {
            object.field("required", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for McpPromptArgument {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                name: object.required("name", &[])?,
                description: object.optional("description", &[])?,
                required: object.optional("required", &[])?,
            })
        })
    }
}

/// A prompt a server advertises (name + optional description + argument schema).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpPromptDescriptor {
    pub name: RuntimeString,
    pub description: Option<RuntimeString>,
    pub arguments: Option<Vec<McpPromptArgument>>,
}

impl RuntimeEncode for McpPromptDescriptor {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("name", &self.name)?;
        if let Some(value) = &self.description {
            object.field("description", value)?;
        }
        if let Some(value) = &self.arguments {
            object.field("arguments", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for McpPromptDescriptor {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                name: object.required("name", &[])?,
                description: object.optional("description", &[])?,
                arguments: object.optional("arguments", &[])?,
            })
        })
    }
}

/// A connected MCP server, exposed to the resource/prompt tools. The engine does
/// not own the connection lifecycle — the host connects/closes.
#[async_trait]
pub trait McpResourceClient: Send + Sync {
    /// Only engine-owned connection adapters expose a reconnectable native client.
    fn native_mcp_client(&self) -> Option<crate::mcp::client::ManagedClient> {
        None
    }
    /// The engine prompt fragment containing this adapter's initialize instructions.
    fn instructions_group(&self) -> &'static str {
        "mcp-instructions"
    }
    fn name(&self) -> &RuntimeString;
    /// The server's `initialize` instructions, if any (already length-capped).
    fn instructions(&self) -> Option<&RuntimeString> {
        None
    }
    async fn list_resources(&self) -> Vec<McpResourceDescriptor>;
    async fn read_resource(&self, uri: &RuntimeString) -> Vec<McpResourceContent>;
    /// Optional: a server's prompts. `None` for adapters that predate the prompt
    /// tools.
    async fn list_prompts(&self) -> Option<Vec<McpPromptDescriptor>> {
        None
    }
    async fn get_prompt(
        &self,
        _name: &RuntimeString,
        _args: Option<RuntimeObject>,
    ) -> Option<RuntimeString> {
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackgroundTaskStatus {
    Running,
    Completed,
    Failed,
    /// The parent session restarted while this task was live, so its process is
    /// gone and it will never produce more output. Distinct from `Failed`: the
    /// task did not error, and whatever it wrote before the restart is still on
    /// disk and readable. Distinct from `Completed`: no final answer exists, so a
    /// reader must not mistake partial output for a finished result.
    Interrupted,
}

impl RuntimeEncode for BackgroundTaskStatus {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Interrupted => "interrupted",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for BackgroundTaskStatus {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("running", &[]),
                RuntimeEnumVariant::new("completed", &[]),
                RuntimeEnumVariant::new("failed", &[]),
                RuntimeEnumVariant::new("interrupted", &[]),
            ],
            &[
                Self::Running,
                Self::Completed,
                Self::Failed,
                Self::Interrupted,
            ],
        )
    }
}

/// A live background sub-agent, exposed to the `task_output` / `task_stop` tools.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackgroundTaskHandle {
    pub agent_id: RuntimeString,
    pub output_file: RuntimePath,
    pub status: BackgroundTaskStatus,
    /// Short label for the task (the spawning prompt/description).
    pub description: Option<RuntimeString>,
    /// The agent's transcript accumulated SO FAR (the incremental disk
    /// transcript). Returned while the task is still running; replaced by the
    /// clean final answer once complete.
    pub partial: Option<RuntimeString>,
}

impl RuntimeEncode for BackgroundTaskHandle {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("agentId", &self.agent_id)?;
        object.field("outputFile", &self.output_file)?;
        object.field("status", &self.status)?;
        if let Some(value) = &self.description {
            object.field("description", value)?;
        }
        if let Some(value) = &self.partial {
            object.field("partial", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for BackgroundTaskHandle {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                agent_id: object.required("agentId", &[])?,
                output_file: object.required("outputFile", &[])?,
                status: object.required("status", &[])?,
                description: object.optional("description", &[])?,
                partial: object.optional("partial", &[])?,
            })
        })
    }
}

/// The session's current worktree, as seen by the worktree tools.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveWorktree {
    pub worktree_path: RuntimePath,
    pub worktree_name: RuntimeString,
    pub worktree_branch: Option<RuntimeString>,
    /// HEAD at creation — the baseline the exit tool uses to count new commits.
    pub original_head_commit: Option<RuntimeString>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorktreeEnterResult {
    pub worktree_path: RuntimePath,
    pub worktree_branch: Option<RuntimeString>,
    pub message: RuntimeString,
}

/// Whether an exited worktree is kept or removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorktreeExitAction {
    Keep,
    Remove,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorktreeExitResult {
    pub action: WorktreeExitAction,
    pub original_cwd: RuntimePath,
    pub worktree_path: RuntimePath,
    pub worktree_branch: Option<RuntimeString>,
    pub message: RuntimeString,
}

/// Enter/leave an isolated git worktree, switching this Session's live cwd into
/// it (no process-wide chdir — the switch is per-Session).
#[async_trait]
pub trait WorktreeController: Send + Sync {
    fn current(&self) -> Option<ActiveWorktree>;
    async fn enter(&self, name: Option<RuntimeString>) -> WorktreeEnterResult;
    async fn exit(&self, action: WorktreeExitAction) -> WorktreeExitResult;
}

/// One persisted result of a workflow `agent()` call, for deterministic resume.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkflowJournalEntry {
    /// The workflow run this result belongs to (the run id).
    pub run_id: RuntimeString,
    /// Deterministic launch index of the `agent()` call within the run.
    pub idx: u64,
    /// Hash of (prompt, normalized opts) — verifies the cached call still
    /// matches.
    pub hash: RuntimeString,
    /// The call's result (validated object / text / null), JSON-serializable.
    pub result: RuntimeJson,
    /// Output tokens the call spent, so resume can rebuild the budget.
    pub output_tokens: u64,
}

impl RuntimeEncode for WorkflowJournalEntry {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("runId", &self.run_id)?;
        object.field("idx", &self.idx)?;
        object.field("hash", &self.hash)?;
        object.field("result", &self.result)?;
        object.field("outputTokens", &self.output_tokens)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for WorkflowJournalEntry {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                run_id: object.required("runId", &[])?,
                idx: object.required("idx", &[])?,
                hash: object.required("hash", &[])?,
                result: object.required("result", &[])?,
                output_tokens: object.required("outputTokens", &[])?,
            })
        })
    }
}

pub use crate::engine::query::RetainedToolTasks;

/// Append/replay the workflow result journal (the resume "journal").
#[async_trait]
pub trait WorkflowJournal: Send + Sync {
    /// Await admission before publishing the result to callers or replay.
    /// An error does not undo the completed agent's effects.
    async fn append(&self, entry: WorkflowJournalEntry) -> std::io::Result<()>;
    fn prior(&self, run_id: &RuntimeString) -> Vec<WorkflowJournalEntry>;

    /// Keep accepted agent operations inside the owning session's shutdown fence.
    fn operation_tasks(&self) -> Option<Arc<RetainedToolTasks>> {
        None
    }
}

/// One peer in a swarm team, as the swarm tools see it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwarmPeerInfo {
    pub peer: RuntimeString,
    pub last_seen: i64,
    /// True when `last_seen` is within the liveness window.
    pub active: bool,
}

/// The session's current swarm identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwarmIdentity {
    pub team: RuntimeString,
    pub peer: RuntimeString,
}

/// Outcome of `TeamCreate`, including the shared-format team-config
/// path and fixed leader agent id exposed to the model/client boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwarmCreateResult {
    pub identity: SwarmIdentity,
    pub team_file_path: RuntimeString,
    pub lead_agent_id: RuntimeString,
}

/// Outcome of a `SwarmController::send`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwarmSendResult {
    pub recipients: Vec<RuntimeString>,
}

/// Outcome of a `SwarmController::remove`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwarmRemoveResult {
    pub removed: bool,
    pub reason: Option<RuntimeString>,
}

impl RuntimeEncode for SwarmPeerInfo {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("peer", &self.peer)?;
        object.field("lastSeen", &self.last_seen)?;
        object.field("active", &self.active)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for SwarmPeerInfo {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                peer: object.required("peer", &[])?,
                last_seen: object.required("lastSeen", &[])?,
                active: object.required("active", &[])?,
            })
        })
    }
}

impl RuntimeEncode for SwarmIdentity {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("team", &self.team)?;
        object.field("peer", &self.peer)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for SwarmIdentity {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                team: object.required("team", &[])?,
                peer: object.required("peer", &[])?,
            })
        })
    }
}

impl RuntimeEncode for SwarmSendResult {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("recipients", &self.recipients)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for SwarmSendResult {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                recipients: object.required("recipients", &[])?,
            })
        })
    }
}

impl RuntimeEncode for SwarmRemoveResult {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("removed", &self.removed)?;
        if let Some(value) = &self.reason {
            object.field("reason", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for SwarmRemoveResult {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                removed: object.required("removed", &[])?,
                reason: object.optional("reason", &[])?,
            })
        })
    }
}

/// Multi-agent swarm coordination (SendMessage / ListPeers / TeamCreate /
/// TeamDelete). Present only when `config.swarm` is enabled.
#[async_trait]
pub trait SwarmController: Send + Sync {
    /// The session's current swarm identity, or `None` if none is assigned yet.
    fn current(&self) -> Option<SwarmIdentity>;
    /// Non-consuming check of unread mail for this controller's current identity.
    async fn has_pending_input(&self) -> Result<bool, RuntimeString> {
        let Some(identity) = self.current() else {
            return Ok(false);
        };
        crate::tasks::swarm_store::MailboxDeliveryState::default()
            .has_pending(&identity.team, &identity.peer)
            .await
            .map_err(|error| error.to_string().into())
    }
    /// TeamCreate: create a new team with the fixed `team-lead` identity and
    /// persist its shared-format `config.json`.
    async fn create(
        &self,
        team_name: RuntimeString,
        description: Option<RuntimeString>,
        agent_type: Option<RuntimeString>,
    ) -> SwarmCreateResult;
    /// ListPeers: the team roster (requires an identity).
    async fn peers(&self) -> Vec<SwarmPeerInfo>;
    /// SendMessage: to a peer by name, or to all peers with `'*'`.
    async fn send(&self, to: &RuntimeString, body: &RuntimeString) -> SwarmSendResult;
    /// TeamDelete: remove a team dir (fail-closed unless `force`).
    async fn remove(&self, team: &RuntimeString, force: bool) -> SwarmRemoveResult;
}

/// A validated plain-text message to an ordinary sub-agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubagentMessageRequest {
    /// Explicit follow-up work may start a new turn; ordinary mail only queues.
    pub trigger_turn: bool,
    pub target: RuntimeString,
    pub message: RuntimeString,
    pub summary: RuntimeString,
    pub sender_agent_id: Option<RuntimeString>,
    /// Set only by a trusted human-input route, never by a model tool call.
    pub user_initiated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubagentMessageResult {
    pub agent_id: RuntimeString,
    pub resumed: bool,
}

impl RuntimeEncode for SubagentMessageResult {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("agentId", &self.agent_id)?;
        object.field("resumed", &self.resumed)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for SubagentMessageResult {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                agent_id: object.required("agentId", &[])?,
                resumed: object.required("resumed", &[])?,
            })
        })
    }
}

/// Query + control the session's background sub-agents (run_in_background).
#[async_trait]
pub trait BackgroundTaskRegistry: Send + Sync {
    /// Bind only a query's sealed MCP capture when handing off a message.
    #[doc(hidden)]
    fn with_inherited_mcp(
        &self,
        _context: &InheritedSubagentContext,
    ) -> Option<Arc<dyn BackgroundTaskRegistry>> {
        None
    }
    /// Captured by the host when creating the real caller's ToolContext.
    fn caller_agent_id(&self) -> Option<RuntimeString> {
        None
    }
    /// True only for an engine-resolved builtin one-shot identity.
    fn is_one_shot(&self, _agent_id: &RuntimeString) -> Option<bool> {
        None
    }
    fn get(&self, agent_id: &RuntimeString) -> Option<BackgroundTaskHandle>;
    fn list(&self) -> Vec<BackgroundTaskHandle>;
    /// Pending input for this caller must be applied before a long wait resumes.
    fn has_pending_input(&self) -> bool {
        false
    }
    /// Abort a running task; returns false if no such running task.
    fn stop(&self, agent_id: &RuntimeString) -> bool;
    /// A trusted user cancellation blocks automatic message-triggered recovery.
    fn stop_by_user(&self, agent_id: &RuntimeString) -> bool {
        self.stop(agent_id)
    }
    /// None means the target is outside the ordinary sub-agent registry.
    /// A known target's rejection must never fall through to a team mailbox.
    async fn send_message(
        &self,
        _request: SubagentMessageRequest,
    ) -> Option<Result<SubagentMessageResult, RuntimeString>> {
        None
    }
    /// Resolve once every persistent teammate spawned by this session is idle
    /// (parked) or terminated. A no-op when there are no working teammates.
    async fn wait_for_idle(&self);
}

/// One scheduled cron job, as surfaced by CronList (file-backed + session-only
/// merged; session jobs carry `durable: false`).
#[derive(Debug, Clone, PartialEq)]
pub struct CronJobInfo {
    pub id: RuntimeString,
    pub cron: RuntimeString,
    pub prompt: RuntimeString,
    pub recurring: bool,
    /// `false` = session-only (in-memory); `true` = persisted to
    /// `.klaus/scheduled_tasks.json`.
    pub durable: bool,
}

/// Scheduled prompts (CronCreate / CronList / CronDelete). Present only when
/// `config.cron` is enabled.
#[async_trait]
pub trait CronController: Send + Sync {
    /// CronCreate: register an already-validated task and start the session's
    /// scheduler. Returns the short job id.
    async fn create(
        &self,
        cron: RuntimeString,
        prompt: RuntimeString,
        recurring: bool,
        durable: bool,
    ) -> Result<RuntimeString, RuntimeString>;
    /// CronList: every live job (durable + session-only).
    async fn list(&self) -> Vec<CronJobInfo>;
    /// CronDelete: cancel a job by id. `Err` carries the failure message.
    async fn delete(&self, id: &RuntimeString) -> Result<(), RuntimeString>;
}

/// A skill context modification layered onto the rest of the current turn.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SkillContextMod {
    /// Local orchestration override for the remainder of this turn.
    pub ultra_mode: Option<bool>,
    /// Native path from the resolved skill whose body was successfully expanded.
    pub invoked_skill_path: Option<RuntimeString>,
    /// Exact expanded instructions delivered to the running agent. Retain this
    /// receipt instead of expanding shell directives again after compaction.
    pub invoked_skill_content: Option<RuntimeString>,
    pub model: Option<String>,
    pub effort: Option<EffortValue>,
    pub allow_tools: Option<Vec<String>>,
}

/// Options for `ToolContext::run_bash`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunBashOptions {
    pub shell: Option<String>,
    pub allow_tools: Option<Vec<String>>,
}

/// Input for a `ToolContext::run_hook` dispatch.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RunHookInput {
    pub tool_name: Option<RuntimeString>,
    pub tool_input: Option<Json>,
}

/// The outcome of a `ToolContext::run_hook` dispatch.
#[derive(Debug, Clone, PartialEq)]
pub struct RunHookResult {
    pub blocked: bool,
    pub system_messages: Vec<RuntimeString>,
}

/// The model-visible result of a `ToolContext::invoke_tool` reentrant call.
#[derive(Debug, Clone, PartialEq)]
pub struct InvokeToolResult {
    pub is_error: bool,
    pub output: Option<Json>,
    pub content: Vec<OutputBlock>,
}

// ---- callback type aliases used by ToolContext ----

/// `emit`: send a `ToolDomainEvent`.
pub type EmitFn = Arc<dyn Fn(ToolDomainEvent) -> BoxFuture<'static, ()> + Send + Sync>;
/// Mint a fresh identifier (random UUID v4 in production).
pub type UuidFn = Arc<dyn Fn() -> String + Send + Sync>;
/// `shell_env`: resolve the captured environment map, or `None`.
pub type ShellEnvFn = Arc<dyn Fn() -> BoxFuture<'static, Option<RuntimeEnv>> + Send + Sync>;
/// `surface_tools`: take an exact list of runtime tool names.
pub type NamesFn = Box<
    dyn Fn(Vec<RuntimeString>) -> BoxFuture<'static, Result<(), crate::types::errors::ToolError>>
        + Send
        + Sync,
>;

pub type AdvisorQueryFn = Arc<
    dyn Fn(
            RuntimeString,
            crate::types::provider::ModelRequest,
            AbortSignal,
        ) -> BoxFuture<'static, Result<RuntimeString, crate::types::errors::ToolError>>
        + Send
        + Sync,
>;

/// The session owns the inference target; the query supplies its current request.
pub struct AdvisorContext {
    pub query: AdvisorQueryFn,
    pub request: Option<crate::types::provider::ModelRequest>,
}
/// `activate_skill_paths`: take the exact runtime paths touched by a file tool.
pub type PathsFn = Box<dyn Fn(Vec<RuntimePath>) + Send + Sync>;
/// `tool_catalog`: list the `ToolCatalogEntry` items.
pub type ToolCatalogFn = Box<dyn Fn() -> Vec<ToolCatalogEntry> + Send + Sync>;
/// `apply_skill_context`: apply a `SkillContextMod`.
pub type ApplySkillContextFn = Box<dyn Fn(SkillContextMod) + Send + Sync>;
/// `run_bash`: run a command, yielding its output.
pub type RunBashFn = Arc<
    dyn Fn(
            String,
            Option<RunBashOptions>,
        ) -> BoxFuture<'static, Result<String, crate::types::errors::ToolError>>
        + Send
        + Sync,
>;
/// `mcp_clients`: list the connected `McpResourceClient`s.
pub type McpClientsFn = Box<dyn Fn() -> Vec<Arc<dyn McpResourceClient>> + Send + Sync>;
/// `run_hook`: dispatch a hook, yielding a `RunHookResult`.
pub type RunHookFn =
    Arc<dyn Fn(HookEvent, RunHookInput) -> BoxFuture<'static, RunHookResult> + Send + Sync>;
/// `invoke_tool`: invoke a tool, yielding an `InvokeToolResult`.
pub type InvokeToolFn = Arc<
    dyn Fn(
            RuntimeString,
            RuntimeJson,
        ) -> BoxFuture<'static, Result<InvokeToolResult, crate::types::errors::ToolError>>
        + Send
        + Sync,
>;
/// `request_approval`: ask for a mid-run user approval, resolving `true` when
/// approved. Routes through the same pipeline as a pre-call permission ask.
pub type RequestApprovalFn = Arc<dyn Fn(RuntimeString) -> BoxFuture<'static, bool> + Send + Sync>;
/// A single engine-visible network access request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkAccessRequest {
    pub id: RuntimeString,
    pub url: RuntimeString,
    pub host: RuntimeString,
    pub protocol: RuntimeString,
    pub port: u16,
    pub tool_name: RuntimeString,
}

impl RuntimeEncode for NetworkAccessRequest {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("id", &self.id)?;
        object.field("url", &self.url)?;
        object.field("host", &self.host)?;
        object.field("protocol", &self.protocol)?;
        object.field("port", &self.port)?;
        object.field("toolName", &self.tool_name)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for NetworkAccessRequest {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                id: object.required("id", &[])?,
                url: object.required("url", &[])?,
                host: object.required("host", &[])?,
                protocol: object.required("protocol", &[])?,
                port: object.required("port", &[])?,
                tool_name: object.required("toolName", &[])?,
            })
        })
    }
}

/// `request_network_access`: ask for a network access decision, resolving
/// `true` when the access may proceed.
pub type RequestNetworkAccessFn =
    Arc<dyn Fn(NetworkAccessRequest) -> BoxFuture<'static, bool> + Send + Sync>;
/// `enqueue_background_notice`: queue a background-completion notice for the
/// model's next continuation.
pub type EnqueueBackgroundNoticeFn = Arc<dyn Fn(RuntimeString) + Send + Sync>;
/// `record_rollout_event`: ask the host to append a structured rollout event.
/// Tools use this as a persistence seam without depending on the store layer.
pub type RecordRolloutEventFn = Arc<
    dyn Fn(
            RuntimeString,
            Option<RuntimeJson>,
        ) -> BoxFuture<'static, Result<(), crate::types::errors::ToolError>>
        + Send
        + Sync,
>;

/// The session's plan-mode plan-file identity: the resolved plans directory,
/// the bound document name, and this context's own plan document. A sub-agent
/// carries the same directory + name but its own `<name>-agent-<id>.md` path.
///
/// Present only once `create_plan` has BOUND a name. Nothing derives one on
/// demand: naming a document is what grants plan mode's write exemption, so a
/// context that could name one implicitly would exempt writes before any plan
/// existed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanFileContext {
    /// The resolved plans directory for this session.
    pub plans_dir: RuntimePath,
    /// The bound plan document name (names `<name>.md` and `<name>-agent-*.md`).
    pub name: RuntimeString,
    /// This context's own plan file path.
    pub path: RuntimePath,
}

/// What a tool's `call` receives. Optional fields are present only when the host
/// wires the corresponding capability.
pub struct ToolContext {
    /// The exact process namespace owned by the enclosing engine/session.
    /// Spawn-bearing tools must pass this context through explicitly.
    pub process_context: RuntimeProcessContext,
    /// Session-owned background shell registry. Tests provide an explicit
    /// isolated registry; production contexts share the enclosing Session's.
    pub background_processes: Arc<crate::command_runtime::background::BackgroundCommandRegistry>,
    pub cwd: RuntimePath,
    pub signal: AbortSignal,
    pub file_state: Arc<dyn FileStateCache>,
    pub permission: PermissionContext,
    /// Immutable effect ceiling captured when this query entered planning.
    pub planning_read_only: bool,
    /// Independent policies captured atomically with this tool's sandbox backend.
    pub approval_policy: Option<crate::types::permission::ApprovalPolicy>,
    pub sandbox_mode: Option<crate::types::permission::SandboxMode>,
    pub approvals_reviewer: Option<crate::types::permission::ApprovalsReviewer>,
    pub web_search_access: Option<crate::schemas::config::WebSearchAccess>,
    pub sandbox: Arc<dyn SandboxBackend>,
    /// Whether sandboxed shell commands may reach the network. `None` → deny
    /// (the safe default).
    pub network_allowed: Option<bool>,
    /// When a sandbox blocks a shell command, may the tool ASK to re-run it
    /// WITHOUT the sandbox? `None`/`Some(false)` ⇒ never escalate — surface the
    /// denial as-is.
    pub sandbox_retry_on_denial: Option<bool>,
    /// Request a mid-run user approval (the bash tool's sandbox-denial →
    /// re-run-unsandboxed escalation). Absent in contexts with no approval flow —
    /// the caller then treats it as "cannot escalate".
    pub request_approval: Option<RequestApprovalFn>,
    /// Request approval for engine-visible network egress. Absent means this
    /// context does not gate network requests.
    pub request_network_access: Option<RequestNetworkAccessFn>,
    /// Emit structured, render-agnostic progress.
    pub emit: EmitFn,
    /// Enqueue a background-completion notice for the model's next continuation:
    /// a `run_in_background` shell that EXITS tells the model instead of the model
    /// polling `bash_output` forever. Absent in contexts with no session queue.
    pub enqueue_background_notice: Option<EnqueueBackgroundNoticeFn>,
    /// Append a host-managed rollout event. Absent in ephemeral/test contexts.
    pub record_rollout_event: Option<RecordRolloutEventFn>,
    /// Whether the ACTIVE model reads PDFs natively. `None` ⇒ treat as native.
    pub native_pdf: Option<bool>,
    /// Whether the ACTIVE model understands images. `None` ⇒ treat as capable.
    pub vision: Option<bool>,
    /// Resolve the captured interactive login-shell environment, or `None` when
    /// capture is off / unavailable.
    pub shell_env: Option<ShellEnvFn>,
    /// Sub-agent nesting depth (0 = top level).
    pub depth: Option<u32>,
    /// Also forward a sub-agent's assistant TEXT as `agent_progress`.
    pub forward_subagent_text: Option<bool>,
    /// Available when the host supports sub-agents (the `task` tool).
    pub spawn_sub_agent: Option<SpawnForegroundSubAgent>,
    /// Start a background sub-agent.
    pub spawn_background_agent: Option<SpawnBackgroundAgent>,
    /// Spawn a sub-agent for the workflow runtime.
    pub spawn_workflow_agent: Option<SpawnSubAgent>,
    /// Make deferred tools visible to the model next turn (used by tool_search).
    pub surface_tools: Option<NamesFn>,
    /// Notify that file paths were touched, so a conditional skill activates.
    pub activate_skill_paths: Option<PathsFn>,
    /// Enumerate all tools (for tool_search to match against).
    pub tool_catalog: Option<ToolCatalogFn>,
    pub advisor: Option<AdvisorContext>,
    /// Ask the user a structured question (used by ask_user).
    pub elicit: Option<Arc<dyn ElicitHandler>>,
    /// Optional questions retain an exact admitted-turn scope.
    pub async_question: Option<crate::engine::questions::AsyncQuestionContext>,
    /// Historical plan-file identity, restored when a saved session has one.
    /// Grants the plan-file write exemption. `None` for sessions without one.
    pub plan_file: Option<PlanFileContext>,
    /// Layer a skill's context onto the REST of the current turn.
    pub apply_skill_context: Option<ApplySkillContextFn>,
    /// Run an inline shell command from a skill body through the permission gate.
    pub run_bash: Option<RunBashFn>,
    /// Connected MCP servers (for list_mcp_resources / read_mcp_resource).
    pub mcp_clients: Option<McpClientsFn>,
    /// Background sub-agent registry (for task_output / task_stop).
    pub background_tasks: Option<Arc<dyn BackgroundTaskRegistry>>,
    /// Which durable task list the current Task tools read/write.
    pub task_list_id: Option<RuntimeString>,
    /// Current-plan binding captured by the originating run.
    pub task_plan: Option<crate::tasks::task_store::TaskPlanBinding>,
    /// Enter/leave an isolated git worktree.
    pub worktree: Option<Arc<dyn WorktreeController>>,
    /// Persisted result journal for the `workflow` tool's deterministic resume.
    pub workflow_journal: Option<Arc<dyn WorkflowJournal>>,
    /// Multi-agent swarm coordination.
    pub swarm: Option<Arc<dyn SwarmController>>,
    /// Scheduled prompts (cron jobs).
    pub cron: Option<Arc<dyn CronController>>,
    /// The thread's goal runtime (goal mode). Absent when the host did not
    /// enable goal mode, which is what makes the goal tools refuse.
    pub goal: Option<Arc<crate::goal::runtime::GoalRuntime>>,
    /// Generation captured by this turn. A model-created goal may bind an empty scope once.
    pub goal_generation: Option<Arc<std::sync::Mutex<crate::goal::runtime::GoalRequestIdentity>>>,
    /// Exact validated files for the currently admitted goal intent.
    pub goal_owned_resources: Option<Arc<crate::goal::owned_resources::GoalOwnedResources>>,
    /// Language-server intelligence.
    pub lsp: Option<Arc<dyn LspController>>,
    /// Host/plugin supplied browser automation runtime.
    pub browser: Option<Arc<dyn BrowserRuntime>>,
    /// Browser runtime implemented by host/plugin tools in the active tool pool.
    pub browser_tool_mapping: Option<BrowserHostToolMapping>,
    /// Dispatch a lifecycle hook from inside a tool.
    pub run_hook: Option<RunHookFn>,
    /// Run ANOTHER tool by name through the FULL engine pipeline.
    pub invoke_tool: Option<InvokeToolFn>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolCatalogEntry {
    pub name: RuntimeString,
    pub description: RuntimeString,
    pub search_hint: Option<RuntimeString>,
    pub exposure: ToolExposure,
    pub spec: Option<ToolSpec>,
}

impl RuntimeEncode for ToolCatalogEntry {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("name", &self.name)?;
        object.field("description", &self.description)?;
        if let Some(value) = &self.search_hint {
            object.field("searchHint", value)?;
        }
        object.field("exposure", &self.exposure)?;
        if let Some(value) = &self.spec {
            object.field("spec", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ToolCatalogEntry {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                name: object.required("name", &[])?,
                description: object.required("description", &[])?,
                search_hint: object.optional("searchHint", &[])?,
                exposure: object.required("exposure", &[])?,
                spec: object.optional("spec", &[])?,
            })
        })
    }
}

/// Media/metadata carried on a tool result that must never be persisted (the
/// `mcpMeta` payload).
#[derive(Debug, Clone, PartialEq)]
pub struct McpMeta {
    pub meta: Option<RuntimeJson>,
    pub structured_content: Option<RuntimeJson>,
}

impl RuntimeEncode for McpMeta {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        if let Some(value) = &self.meta {
            object.field("_meta", value)?;
        }
        if let Some(value) = &self.structured_content {
            object.field("structuredContent", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for McpMeta {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                meta: object.optional("_meta", &[])?,
                structured_content: object.optional("structuredContent", &[])?,
            })
        })
    }
}

/// The serializable payload of a tool result (safe for rollout / wire / worker).
#[derive(Debug, Clone, PartialEq)]
pub struct ToolResultSerializable {
    pub output: Json,
    /// Pre-computed model-visible content.
    pub model_content: Vec<OutputBlock>,
    pub is_error: Option<bool>,
    pub mcp_meta: Option<McpMeta>,
    /// Media attached to the SAME user turn as this result but NOT placed inside
    /// the tool_result content — currently a native PDF `document` from a Read.
    pub extra_user_blocks: Option<Vec<ContentBlock>>,
}

/// In-process-only side effects of a tool call (closures that must never be
/// persisted).
#[derive(Default)]
pub struct ToolResultSideEffects {
    /// Only honored for non-concurrency-safe tools.
    pub context_modifier: Option<Box<dyn FnOnce(ToolContext) -> ToolContext + Send>>,
}

/// The result of a tool call, split into a serializable payload and in-process
/// side effects.
pub struct ToolResult {
    pub serializable: ToolResultSerializable,
    pub side_effects: Option<ToolResultSideEffects>,
}

/// Progress callback threaded into a tool's `call`.
pub type ToolProgress = Box<dyn Fn(ToolDomainEvent) -> BoxFuture<'static, ()> + Send + Sync>;

/// Permission class for path-rule matching.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PermissionPathKind {
    Read,
    Edit,
}

impl RuntimeEncode for PermissionPathKind {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Read => "read",
            Self::Edit => "edit",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for PermissionPathKind {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("read", &[]),
                RuntimeEnumVariant::new("edit", &[]),
            ],
            &[Self::Read, Self::Edit],
        )
    }
}

/// The path target(s) a file tool operates on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathTargets {
    One(RuntimePath),
    Many(Vec<RuntimePath>),
}

/// Engine-owned restrictions that must be reconstructed before cold tool recovery.
#[derive(Clone)]
pub enum BrowserDependencyPersistence {
    Operations {
        inner: Arc<dyn Tool>,
        owners: Vec<RuntimeString>,
        mapping: Box<BrowserHostToolMapping>,
    },
    Search {
        inner: Arc<dyn Tool>,
    },
}

/// Native effects that retain a narrower enforcement path during planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanningToolCapability {
    None,
    Shell,
    Research,
}

/// A registry-side tool. The pool/dispatch use this object-safe, erased form;
/// an ergonomic typed wrapper lives in `tools/build_tool.rs`.
#[async_trait]
pub trait Tool: Send + Sync {
    /// A sealed binding minted by the engine's native MCP connection wrapper.
    fn native_mcp_binding(&self) -> Option<crate::mcp::tool_wrapper::NativeMcpToolBinding> {
        None
    }
    /// Keep engine-owned restriction wrappers around a replacement native executor.
    fn rebind_native_mcp(&self, replacement: Arc<dyn Tool>) -> Arc<dyn Tool> {
        replacement
    }
    /// Source objects stay in memory; persistence records only capability evidence.
    fn browser_dependency_persistence(&self) -> Option<BrowserDependencyPersistence> {
        None
    }
    /// Native owners whose implementation dependency must survive role filtering.
    fn browser_dependency_owners(&self) -> Vec<RuntimeString> {
        Vec::new()
    }
    /// A hidden browser implementation may bind a request to its actual native caller.
    /// None leaves ordinary tool dispatch unchanged; false rejects an untrusted request.
    fn browser_dependency_authorization(
        &self,
        _caller: Option<&RuntimeString>,
        _input: &Json,
    ) -> Option<bool> {
        None
    }
    /// Exact model/provider-visible tool name.
    fn name(&self) -> RuntimeString {
        self.static_name().into()
    }
    /// Scalar-only fallback for engine-owned builtins. Dynamic/external tools
    /// override [`Self::name`] and never implement or call this method.
    fn static_name(&self) -> &str {
        panic!("tool must implement exact name() or scalar-only static_name()")
    }
    fn aliases(&self) -> Vec<RuntimeString> {
        self.static_aliases()
            .iter()
            .map(RuntimeString::from)
            .collect()
    }
    fn static_aliases(&self) -> &[String] {
        &[]
    }
    /// Static, model-facing purpose (becomes the ToolSpec description).
    fn description(&self) -> RuntimeString {
        self.static_description().into()
    }
    /// Scalar-only fallback for engine-owned builtin descriptions.
    fn static_description(&self) -> &str {
        panic!("tool must implement exact description() or scalar-only static_description()")
    }
    /// The tool's input JSON Schema (the normalized, erased form).
    fn input_schema(&self) -> Json {
        crate::types::json::runtime_schema_from_static(self.static_input_schema())
    }
    /// Scalar-only fallback for engine-owned, statically validated schemas.
    fn static_input_schema(&self) -> &JsonSchema {
        panic!("tool must implement exact input_schema() or scalar-only static_input_schema()")
    }
    /// Whether the tool is schema/function-style or raw/freeform.
    fn input_kind(&self) -> ToolInputKind {
        ToolInputKind::JsonSchema
    }
    /// A JSON Schema describing structured output, if any.
    fn output_schema(&self) -> Option<Json> {
        self.static_output_schema()
            .map(crate::types::json::runtime_schema_from_static)
    }
    fn static_output_schema(&self) -> Option<&JsonSchema> {
        None
    }
    fn exposure(&self) -> ToolExposure {
        ToolExposure::Direct
    }
    fn search_hint(&self) -> Option<RuntimeString> {
        self.static_search_hint().map(RuntimeString::from)
    }
    fn static_search_hint(&self) -> Option<&str> {
        None
    }
    /// Max chars before the result spills to disk. `None` = never (e.g. read).
    fn max_result_size_chars(&self) -> Option<u64> {
        None
    }
    /// A non-secret source/server/account/implementation identity for cold recovery.
    /// Opaque adapters must override this; a display name is never proof of authority.
    fn persistence_identity(&self) -> Option<RuntimeString> {
        let implementation = std::any::type_name::<Self>();
        if self.mcp_server().is_some()
            || self.task_scope_host_capability()
            || !implementation.starts_with(concat!(env!("CARGO_CRATE_NAME"), "::tools::builtins::"))
        {
            None
        } else {
            Some(implementation.into())
        }
    }

    /// Registry-owned identity for approval review. Model arguments cannot override it.
    fn approval_metadata(&self) -> Option<crate::types::permission::ApprovalActionMetadata> {
        None
    }

    /// Origin MCP server name when this tool was wrapped from an MCP server.
    fn mcp_server(&self) -> Option<RuntimeString> {
        self.static_mcp_server().map(RuntimeString::from)
    }
    fn static_mcp_server(&self) -> Option<&str> {
        None
    }
    /// A trusted host adapter for a task-specific capability. Native tools do
    /// not gain this authority by sharing a host capability's visible name.
    fn task_scope_host_capability(&self) -> bool {
        false
    }

    fn is_enabled(&self) -> bool;
    fn is_concurrency_safe(&self, input: &Json) -> bool;
    fn is_read_only(&self, input: &Json) -> bool;
    /// Private goal metadata has its own authority checks. This category does
    /// not grant workspace I/O or bypass explicit rules, hooks or task scope.
    fn is_goal_state_control(&self) -> bool {
        false
    }
    /// Trusted native capability; a visible tool name grants no planning exception.
    fn planning_capability(&self, _input: &Json) -> PlanningToolCapability {
        PlanningToolCapability::None
    }
    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        None
    }

    /// Recover the fixed Zod object's declaration order at an object path.
    ///
    /// [`JsonSchema`] uses `serde_json::Map`, which is a `BTreeMap` in this
    /// workspace and therefore erases declaration order. Most schemas retain
    /// enough order in `required`; tools with differently ordered optional
    /// fields override this sidecar. The root path is empty, object keys and
    /// array indexes are appended as strings for nested schemas.
    fn input_schema_property_order(
        &self,
        _object_path: &[String],
    ) -> Option<&'static [&'static str]> {
        None
    }

    /// Pre-normalize transforms that the erased JSON Schema cannot express.
    /// Native tools normally leave this untouched; the Grep/Todo input
    /// preprocessors override it before generic schema parsing.
    fn preprocess_input(&self, input: &Json) -> Json {
        input.clone()
    }

    /// Parse and normalize model-supplied input exactly once before validation,
    /// hooks, permissions, and execution. Native function tools use the generic
    /// schema policy, MCP tools use a permissive loose object, and freeform
    /// tools preserve their raw value. Tools with an exact bespoke Zod boundary
    /// may override this method.
    fn parse_input(&self, input: &Json) -> Result<Json, crate::types::errors::ToolError> {
        match self.input_kind() {
            ToolInputKind::Freeform { .. } => Ok(input.clone()),
            ToolInputKind::JsonSchema if self.mcp_server().is_some() => {
                crate::tools::input_parser::parse_mcp_loose_object(input)
            }
            ToolInputKind::JsonSchema => {
                let preprocessed = self.preprocess_input(input);
                let schema = self.input_schema();
                crate::tools::input_parser::parse_native_input(&schema, &preprocessed, &|path| {
                    self.input_schema_property_order(path)
                })
            }
        }
    }

    async fn validate_input(&self, _input: &Json, _ctx: &ToolContext) -> Option<ValidationResult> {
        None
    }
    async fn check_permissions(
        &self,
        _input: &Json,
        _ctx: &ToolContext,
    ) -> Option<PermissionResult> {
        None
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, crate::types::errors::ToolError>;

    /// Execute a model-issued tool call with its stable tool-use id. Existing
    /// tools inherit the original behavior; host bridges can override this to
    /// correlate an out-of-process response without minting a second id.
    async fn call_with_id(
        &self,
        _call_id: &RuntimeString,
        input: Json,
        ctx: &ToolContext,
        on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, crate::types::errors::ToolError> {
        self.call(input, ctx, on_progress).await
    }

    /// Short present-tense activity label, optional.
    fn summarize(&self, _input: &Json) -> Option<RuntimeString> {
        None
    }
    /// For bash/exec: the command string shown on the command_execution item.
    fn to_command_string(&self, _input: &Json) -> Option<RuntimeString> {
        None
    }
    /// For file tools: the filesystem path(s) this call operates on.
    fn get_path(&self, _input: &Json) -> Option<PathTargets> {
        None
    }
    /// Raw JavaScript-compatible path value used by the permission engine.
    ///
    /// Existing tools inherit a lossless conversion from [`Self::get_path`].
    /// Tools whose trusted hook input can bypass their schema may override this
    /// method so arrays and invalid element types reach the permission boundary
    /// in their original form instead of being silently discarded.
    fn get_raw_path(&self, input: &Json) -> Option<Json> {
        self.get_path(input).map(|targets| match targets {
            PathTargets::One(path) => Json::String(path.into_runtime_string()),
            PathTargets::Many(paths) => Json::Array(
                paths
                    .into_iter()
                    .map(|path| Json::String(path.into_runtime_string()))
                    .collect(),
            ),
        })
    }
    /// Permission class for path-rule matching. Absent for non-file tools.
    fn permission_path_kind(&self) -> Option<PermissionPathKind> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::{runtime_json, ScalarJson};
    use crate::types::errors::ToolError;
    use serde_json::json;

    struct ParserPolicyTool {
        schema: JsonSchema,
        input_kind: ToolInputKind,
        mcp_server: Option<&'static str>,
    }

    struct UnusedWorkflowRunner;

    #[async_trait::async_trait]
    impl crate::tools::builtins::workflow::WorkflowRunner for UnusedWorkflowRunner {
        async fn run<'a>(
            &self,
            _input: crate::tools::builtins::workflow::WorkflowRunInput,
            _deps: crate::tools::builtins::workflow::WorkflowRuntimeDeps<'a>,
        ) -> Result<crate::tools::builtins::workflow::WorkflowRunResult, RuntimeString> {
            panic!("property-order tests never execute the workflow")
        }
    }

    #[async_trait::async_trait]
    impl Tool for ParserPolicyTool {
        fn static_name(&self) -> &str {
            "parser_policy"
        }

        fn static_description(&self) -> &str {
            "exercise the default input parser policy"
        }

        fn static_input_schema(&self) -> &JsonSchema {
            &self.schema
        }

        fn input_kind(&self) -> ToolInputKind {
            self.input_kind.clone()
        }

        fn static_mcp_server(&self) -> Option<&str> {
            self.mcp_server
        }

        fn is_enabled(&self) -> bool {
            true
        }

        fn is_concurrency_safe(&self, _input: &Json) -> bool {
            true
        }

        fn is_read_only(&self, _input: &Json) -> bool {
            true
        }

        async fn call(
            &self,
            _input: Json,
            _ctx: &ToolContext,
            _on_progress: Option<ToolProgress>,
        ) -> Result<ToolResult, ToolError> {
            panic!("parser policy tests never execute the tool")
        }
    }

    fn parser_policy_tool(schema: serde_json::Value) -> ParserPolicyTool {
        ParserPolicyTool {
            schema: schema.as_object().cloned().unwrap(),
            input_kind: ToolInputKind::JsonSchema,
            mcp_server: None,
        }
    }

    fn runtime(value: serde_json::Value) -> RuntimeJson {
        runtime_codec::encode_value(
            &ScalarJson::validate_static_schema(value).expect("test JSON is scalar"),
        )
        .expect("scalar test JSON has an exact runtime encoding")
    }

    #[test]
    fn direct_model_only_is_visible_to_model_but_not_code_mode_or_search() {
        let tools = [
            ("read", ToolExposure::Direct),
            ("bridge", ToolExposure::DirectModelOnly),
            ("later", ToolExposure::Deferred),
        ];

        let model_visible = tools
            .iter()
            .filter(|(_, exposure)| exposure.is_model_visible(false))
            .copied()
            .collect::<Vec<_>>();
        let model_names = model_visible
            .iter()
            .map(|(name, _)| *name)
            .collect::<Vec<_>>();
        let code_mode_names = model_visible
            .iter()
            .filter(|(_, exposure)| exposure.allows_code_mode_bridge())
            .map(|(name, _)| *name)
            .collect::<Vec<_>>();
        let discoverable_names = tools
            .iter()
            .filter(|(_, exposure)| *exposure == ToolExposure::Deferred)
            .map(|(name, _)| *name)
            .collect::<Vec<_>>();

        assert_eq!(model_names, vec!["read", "bridge"]);
        assert_eq!(code_mode_names, vec!["read"]);
        assert_eq!(discoverable_names, vec!["later"]);

        assert!(ToolExposure::Deferred.is_model_visible(true));
        assert!(!ToolExposure::Hidden.is_model_visible(true));
    }

    #[test]
    fn fork_context_serde_preserves_exact_values_and_rejects_invalid_inputs() {
        let cases = [
            (json!(true), ForkContext::All),
            (json!(false), ForkContext::None),
            (json!(7), ForkContext::Recent(NonZeroUsize::new(7).unwrap())),
            (
                json!(0.5),
                ForkContext::PositiveNumber(PositiveForkNumber::new(0.5).unwrap()),
            ),
            (
                json!(1.5),
                ForkContext::PositiveNumber(PositiveForkNumber::new(1.5).unwrap()),
            ),
            (
                json!(1e20),
                ForkContext::PositiveNumber(PositiveForkNumber::new(1e20).unwrap()),
            ),
        ];

        for (wire, expected) in cases {
            let parsed: ForkContext = serde_json::from_value(wire.clone()).unwrap();
            assert_eq!(parsed, expected);
            assert_eq!(serde_json::to_value(parsed).unwrap(), wire);
        }

        for invalid in [json!(0), json!(-1), json!("3")] {
            assert!(
                serde_json::from_value::<ForkContext>(invalid.clone()).is_err(),
                "invalid ForkContext was accepted: {invalid}"
            );
        }

        if u64::MAX > usize::MAX as u64 {
            assert_eq!(
                serde_json::from_value::<ForkContext>(json!(u64::MAX)).unwrap(),
                ForkContext::PositiveNumber(PositiveForkNumber::new(u64::MAX as f64).unwrap())
            );
        }
    }

    #[test]
    fn default_native_parser_recursively_strips_unknowns_and_applies_defaults_first() {
        let tool = parser_policy_tool(json!({
            "type": "object",
            "properties": {
                "nested": {
                    "type": "object",
                    "properties": {
                        "keep": { "type": "string" },
                        "enabled": { "type": "boolean", "default": true }
                    },
                    "required": ["keep", "enabled"],
                    "additionalProperties": false
                },
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": { "value": { "type": "integer" } },
                        "required": ["value"],
                        "additionalProperties": false
                    }
                }
            },
            "required": ["nested", "items"]
        }));

        let parsed = tool
            .parse_input(&runtime(json!({
                "nested": { "keep": "yes", "drop": "nested-extra" },
                "items": [{ "value": 1, "drop": "array-extra" }],
                "drop": "root-extra"
            })))
            .unwrap();

        assert_eq!(
            parsed,
            runtime(json!({
                "nested": { "keep": "yes", "enabled": true },
                "items": [{ "value": 1 }]
            }))
        );
    }

    /// A property declared as a bare object states no inner contract, so its
    /// keys are the caller's, not the schema's. Host dynamic tools declare
    /// free-form action payloads exactly that way, and stripping them handed
    /// the host an empty payload for every call that carried one.
    #[test]
    fn default_native_parser_preserves_an_object_property_with_no_declared_shape() {
        let tool = parser_policy_tool(json!({
            "type": "object",
            "properties": {
                "key": { "type": "string" },
                "action": { "type": "string" },
                "arguments": { "type": "object" }
            }
        }));

        let parsed = tool
            .parse_input(&runtime(json!({
                "key": "docs",
                "action": "search",
                "arguments": { "query": "side task" },
                "drop": "root-extra"
            })))
            .unwrap();

        assert_eq!(
            parsed,
            runtime(json!({
                "key": "docs",
                "action": "search",
                "arguments": { "query": "side task" }
            }))
        );
    }

    #[test]
    fn default_native_parser_handles_javascript_nonfinite_and_negative_zero() {
        let strip_unknown = parser_policy_tool(json!({
            "type": "object",
            "properties": { "thought": { "type": "string" } },
            "required": ["thought"],
            "additionalProperties": false
        }));
        let input =
            runtime_json::parse(&RuntimeString::from(r#"{"thought":"ok","extra":1e400}"#)).unwrap();
        assert_eq!(
            strip_unknown.parse_input(&input).unwrap(),
            runtime(json!({"thought": "ok"}))
        );

        let number = parser_policy_tool(json!({
            "type": "object",
            "properties": { "x": { "type": "number" } },
            "required": ["x"],
            "additionalProperties": false
        }));
        let nonfinite = runtime_json::parse(&RuntimeString::from(r#"{"x":1e400}"#)).unwrap();
        assert_eq!(
            number.parse_input(&nonfinite).unwrap_err().message,
            "Invalid arguments: x: Invalid input: expected number, received number"
        );

        let negative_zero = runtime_json::parse(&RuntimeString::from(r#"{"x":-0}"#)).unwrap();
        let parsed = number.parse_input(&negative_zero).unwrap();
        let value = parsed["x"].as_f64().unwrap();
        assert_eq!(value, 0.0);
        assert!(value.is_sign_negative());
    }

    #[test]
    fn default_native_parser_reports_zod_compatible_issues_in_required_order() {
        let tool = parser_policy_tool(json!({
            "type": "object",
            "properties": {
                "missing": { "type": "string" },
                "typed": { "type": "string" },
                "choice": { "type": "string", "enum": ["a", "b"] },
                "unioned": {
                    "anyOf": [
                        { "type": "string", "const": "all" },
                        { "type": "integer", "exclusiveMinimum": 0 }
                    ]
                },
                "integer": { "type": "integer" },
                "small": { "type": "number", "minimum": 1 },
                "large": { "type": "number", "maximum": 3 }
            },
            "required": [
                "missing", "typed", "choice", "unioned", "integer", "small", "large"
            ],
            "additionalProperties": false
        }));

        let error = tool
            .parse_input(&runtime(json!({
                "typed": 7,
                "choice": "c",
                "unioned": false,
                "integer": 1.5,
                "small": 0,
                "large": 4
            })))
            .unwrap_err();

        assert_eq!(
            error.message,
            concat!(
                "Invalid arguments: ",
                "missing: Invalid input: expected string, received undefined; ",
                "typed: Invalid input: expected string, received number; ",
                "choice: Invalid option: expected one of \"a\"|\"b\"; ",
                "unioned: Invalid input; ",
                "integer: Invalid input: expected int, received number; ",
                "small: Too small: expected number to be >=1; ",
                "large: Too big: expected number to be <=3"
            )
        );
    }

    #[test]
    fn ask_user_parser_preserves_fixed_zod_optional_issue_order() {
        let tool = crate::tools::builtins::ask_user::create_ask_user_tool();

        let error = tool
            .parse_input(&runtime(json!({
                "question": "Pick one",
                "options": 7,
                "multiSelect": "yes"
            })))
            .unwrap_err();

        assert_eq!(
            error.message,
            concat!(
                "Invalid arguments: ",
                "options: Invalid input: expected array, received number; ",
                "multiSelect: Invalid input: expected boolean, received string"
            )
        );
    }

    #[test]
    fn task_update_parser_preserves_fixed_zod_optional_issue_order() {
        let tool = crate::tools::builtins::task_tools::create_task_update_tool();

        let error = tool
            .parse_input(&runtime(json!({
                "taskId": "1",
                "subject": 1,
                "description": false,
                "activeForm": 2,
                "status": false,
                "addBlocks": 3,
                "addBlockedBy": 4,
                "owner": 5,
                "metadata": 6
            })))
            .unwrap_err();

        assert_eq!(
            error.message,
            concat!(
                "Invalid arguments: ",
                "subject: Invalid input: expected string, received number; ",
                "description: Invalid input: expected string, received boolean; ",
                "activeForm: Invalid input: expected string, received number; ",
                "status: Invalid input; ",
                "addBlocks: Invalid input: expected array, received number; ",
                "addBlockedBy: Invalid input: expected array, received number; ",
                "owner: Invalid input: expected string, received number; ",
                "metadata: Invalid input: expected record, received number"
            )
        );
    }

    #[test]
    fn generic_native_order_sidecars_cover_schema_fields_and_compound_issues() {
        struct Case {
            tool: Arc<dyn Tool>,
            input: Json,
            declaration: &'static [&'static str],
            issues: &'static [&'static str],
        }

        let workflow = crate::tools::builtins::workflow::create_workflow_tool(
            crate::tools::builtins::workflow::WorkflowToolOptions {
                workflows: None,
                allow_inline_script: None,
                runner: Arc::new(UnusedWorkflowRunner),
            },
        );
        let cases = vec![
            Case {
                tool: crate::tools::builtins::ask_user::create_ask_user_tool(),
                input: runtime(json!({ "questions": 7, "answers": 7, "metadata": 7 })),
                declaration: &["questions", "answers", "metadata"],
                issues: &["questions", "answers", "metadata"],
            },
            Case {
                tool: crate::tools::builtins::bash::create_bash_tool(),
                input: runtime(json!({
                    "command": "true",
                    "timeout": "x",
                    "cwd": 1,
                    "runInBackground": "x"
                })),
                declaration: &[
                    "command",
                    "timeout",
                    "cwd",
                    "yieldMs",
                    "runInBackground",
                    "sandboxPermissions",
                    "justification",
                ],
                issues: &["timeout", "cwd", "runInBackground"],
            },
            Case {
                tool: crate::tools::builtins::interactive_shell::create_exec_command_tool(),
                input: runtime(json!({
                    "command": "true",
                    "yieldMs": "x",
                    "cwd": 1,
                    "tty": "x",
                    "cols": "x",
                    "rows": "x"
                })),
                declaration: &[
                    "command",
                    "yieldMs",
                    "cwd",
                    "tty",
                    "cols",
                    "rows",
                    "sandboxPermissions",
                    "justification",
                ],
                issues: &["yieldMs", "cwd", "tty", "cols", "rows"],
            },
            Case {
                tool: crate::tools::builtins::file_search::create_file_search_tool(),
                input: runtime(json!({ "query": "q", "path": 1, "limit": "x" })),
                declaration: &["query", "path", "limit"],
                issues: &["path", "limit"],
            },
            Case {
                tool: crate::tools::builtins::grep::create_grep_tool(),
                input: runtime(json!({
                    "pattern": "q",
                    "path": 1,
                    "glob": 1,
                    "output_mode": 1,
                    "-B": true,
                    "-A": true,
                    "-C": true,
                    "context": true,
                    "-n": 1,
                    "-i": 1,
                    "type": 1,
                    "head_limit": true,
                    "offset": true,
                    "multiline": 1,
                    "-o": 1
                })),
                declaration: &[
                    "pattern",
                    "path",
                    "glob",
                    "output_mode",
                    "-B",
                    "-A",
                    "-C",
                    "context",
                    "-n",
                    "-i",
                    "type",
                    "head_limit",
                    "offset",
                    "multiline",
                    "-o",
                ],
                issues: &[
                    "path",
                    "glob",
                    "output_mode",
                    "-B",
                    "-A",
                    "-C",
                    "context",
                    "-n",
                    "-i",
                    "type",
                    "head_limit",
                    "offset",
                    "multiline",
                    "-o",
                ],
            },
            Case {
                tool: crate::tools::builtins::ls::create_ls_tool(),
                input: runtime(json!({ "path": 1, "depth": "x", "all": "x" })),
                declaration: &["path", "depth", "all"],
                issues: &["path", "depth", "all"],
            },
            Case {
                tool: crate::tools::builtins::read::create_read_tool(),
                input: runtime(json!({ "path": "x", "offset": "x", "limit": "x", "pages": 1 })),
                declaration: &["path", "offset", "limit", "pages"],
                issues: &["offset", "limit", "pages"],
            },
            Case {
                tool: crate::tools::builtins::task_control::create_task_stop_tool(),
                input: runtime(json!({ "task_id": 1, "shell_id": 1 })),
                declaration: &["task_id", "shell_id"],
                issues: &["task_id", "shell_id"],
            },
            Case {
                tool: crate::tools::builtins::swarm_tools::create_team_create_tool(),
                input: runtime(json!({ "team_name": 1, "description": 1, "agent_type": 1 })),
                declaration: &["team_name", "description", "agent_type"],
                issues: &["team_name", "description", "agent_type"],
            },
            Case {
                tool: workflow,
                input: runtime(json!({
                    "script": 1,
                    "name": false,
                    "args": null,
                    "resume_from_run_id": 1,
                    "budget_tokens": "x"
                })),
                declaration: &[
                    "script",
                    "name",
                    "args",
                    "resume_from_run_id",
                    "budget_tokens",
                ],
                issues: &["script", "name", "resume_from_run_id", "budget_tokens"],
            },
            Case {
                tool: crate::tools::builtins::task_tools::create_task_update_tool(),
                input: runtime(json!({
                    "taskId": "1",
                    "subject": 1,
                    "description": false,
                    "activeForm": 1,
                    "status": false,
                    "addBlocks": 1,
                    "addBlockedBy": 1,
                    "owner": 1,
                    "metadata": 1
                })),
                declaration: &[
                    "taskId",
                    "subject",
                    "description",
                    "activeForm",
                    "status",
                    "addBlocks",
                    "addBlockedBy",
                    "owner",
                    "metadata",
                ],
                issues: &[
                    "subject",
                    "description",
                    "activeForm",
                    "status",
                    "addBlocks",
                    "addBlockedBy",
                    "owner",
                    "metadata",
                ],
            },
        ];

        for case in cases {
            assert_eq!(
                case.tool.input_schema_property_order(&[]),
                Some(case.declaration),
                "{:?} declaration order",
                case.tool.name()
            );
            let mut declared_fields = case
                .declaration
                .iter()
                .map(|field| RuntimeString::from(*field))
                .collect::<Vec<_>>();
            declared_fields.sort_unstable();
            let mut schema_fields = case
                .tool
                .input_schema()
                .get("properties")
                .and_then(Json::as_object)
                .unwrap()
                .keys()
                .collect::<Vec<_>>();
            schema_fields.sort_unstable();
            assert_eq!(
                declared_fields,
                schema_fields,
                "{:?} sidecar field coverage",
                case.tool.name()
            );

            let error = case.tool.parse_input(&case.input).unwrap_err();
            let message = String::from_utf16(error.message.as_utf16())
                .expect("native parser issue text is scalar");
            let issue_paths = message
                .strip_prefix("Invalid arguments: ")
                .unwrap()
                .split("; ")
                .map(|issue| issue.split_once(": ").unwrap().0)
                .collect::<Vec<_>>();
            assert_eq!(
                issue_paths,
                case.issues,
                "{:?} issue order",
                case.tool.name()
            );
        }
    }

    #[test]
    fn mcp_parser_is_a_permissive_loose_object() {
        let mut tool = parser_policy_tool(json!({
            "type": "object",
            "properties": { "advertised": { "type": "string" } },
            "required": ["advertised"],
            "additionalProperties": false
        }));
        tool.mcp_server = Some("example-server");
        let raw = runtime(json!({
            "server_owned": { "nested": true },
            "advertised": 7
        }));

        assert_eq!(tool.parse_input(&raw).unwrap(), raw);
        assert_eq!(
            tool.parse_input(&runtime(json!("not-an-object")))
                .unwrap_err()
                .message,
            "Invalid arguments: (root): Invalid input: expected object, received string"
        );
    }

    #[test]
    fn freeform_parser_is_identity() {
        let tool = ParserPolicyTool {
            schema: json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            })
            .as_object()
            .cloned()
            .unwrap(),
            input_kind: ToolInputKind::Freeform {
                format: "text".into(),
            },
            mcp_server: None,
        };
        let raw = runtime(json!("*** Begin Patch\n*** End Patch"));

        assert_eq!(tool.parse_input(&raw).unwrap(), raw);
    }

    #[test]
    fn grep_preprocessor_coerces_before_generic_validation() {
        let tool = crate::tools::builtins::grep::create_grep_tool();

        assert_eq!(
            tool.parse_input(&runtime(json!({
                "pattern": "needle",
                "head_limit": "5",
                "-i": "true",
                "extra": "strip-me"
            })))
            .unwrap(),
            runtime(json!({
                "pattern": "needle",
                "-i": true,
                "head_limit": 5
            }))
        );
        assert_eq!(
            tool.parse_input(&runtime(
                json!({ "pattern": "needle", "head_limit": "five" })
            ))
            .unwrap_err()
            .message,
            "Invalid arguments: head_limit: Invalid input: expected number, received string"
        );
    }
}

#[cfg(test)]
mod ultra_inheritance_tests {
    use super::*;
    use crate::schemas::effort::EffortLevel;

    #[test]
    fn explicit_parent_none_does_not_restore_session_ultra() {
        let xhigh = Some(EffortValue::Level(EffortLevel::Xhigh));
        let low = Some(EffortValue::Level(EffortLevel::Low));
        let mut opts = SpawnSubAgentOptions::default();
        assert_eq!(opts.effective_effort(None, xhigh.clone()), xhigh);
        opts.inherited_effort = Some(None);
        assert_eq!(opts.effective_effort(None, xhigh.clone()), None);
        assert_eq!(opts.effective_effort(low.clone(), xhigh.clone()), low);
        opts.effort = low.clone();
        assert_eq!(opts.effective_effort(xhigh.clone(), xhigh.clone()), xhigh);
        opts.effort = None;
        opts.inherited_effort = Some(xhigh.clone());
        assert_eq!(opts.effective_effort(None, low), xhigh);
    }

    #[test]
    fn raw_role_effort_precedes_call_effort_without_rewriting_ultra() {
        let opts = SpawnSubAgentOptions {
            effort: Some(EffortValue::Level(EffortLevel::Low)),
            inherited_effort: Some(Some(EffortValue::Level(EffortLevel::Ultra))),
            ..Default::default()
        };
        assert_eq!(
            opts.effective_effort(None, None),
            Some(EffortValue::Level(EffortLevel::Low))
        );
        assert_eq!(
            opts.effective_effort(Some(EffortValue::Level(EffortLevel::Ultra)), None),
            Some(EffortValue::Level(EffortLevel::Ultra))
        );
    }
}
