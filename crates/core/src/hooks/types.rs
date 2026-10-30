//! Hook lifecycle types.

use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
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

/// Lifecycle points a hook can fire on — the SINGLE authority for which events
/// the engine dispatches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HookEvent {
    SessionStart,
    SessionEnd,
    UserPromptSubmit,
    PreToolUse,
    PostToolUse,
    /// Fires when a tool THREW (vs returning a result) — a distinct event from
    /// PostToolUse. Carries `error`/`is_interrupt`
    /// instead of a result.
    PostToolUseFailure,
    Stop,
    SubagentStop,
    PreCompact,
    PostCompact,
    Notification,
    /// SubagentStart fires before a sub-agent's first model call.
    SubagentStart,
    /// Setup fires on a host-driven init/maintenance pass, before SessionStart.
    Setup,
    /// TodoV2: a `block` decision vetoes the create.
    TaskCreated,
    /// TodoV2: a `block` decision vetoes the complete.
    TaskCompleted,
    /// Worktree: a `WorktreeCreate` handler that returns `worktreePath` takes
    /// over creation.
    WorktreeCreate,
    /// Worktree: tears down a host-managed worktree on exit.
    WorktreeRemove,
}

impl HookEvent {
    /// The wire name, which is also the name users write in their hook config and
    /// therefore the only name worth showing them. Extracted from the encoder so
    /// a second caller cannot drift from a duplicated match arm.
    pub fn wire_name(self) -> &'static str {
        match self {
            Self::SessionStart => "SessionStart",
            Self::SessionEnd => "SessionEnd",
            Self::UserPromptSubmit => "UserPromptSubmit",
            Self::PreToolUse => "PreToolUse",
            Self::PostToolUse => "PostToolUse",
            Self::PostToolUseFailure => "PostToolUseFailure",
            Self::Stop => "Stop",
            Self::SubagentStop => "SubagentStop",
            Self::PreCompact => "PreCompact",
            Self::PostCompact => "PostCompact",
            Self::Notification => "Notification",
            Self::SubagentStart => "SubagentStart",
            Self::Setup => "Setup",
            Self::TaskCreated => "TaskCreated",
            Self::TaskCompleted => "TaskCompleted",
            Self::WorktreeCreate => "WorktreeCreate",
            Self::WorktreeRemove => "WorktreeRemove",
        }
    }
}

impl RuntimeEncode for HookEvent {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(out, self.wire_name())
    }
}

impl RuntimeDecode for HookEvent {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("SessionStart", &[]),
                RuntimeEnumVariant::new("SessionEnd", &[]),
                RuntimeEnumVariant::new("UserPromptSubmit", &[]),
                RuntimeEnumVariant::new("PreToolUse", &[]),
                RuntimeEnumVariant::new("PostToolUse", &[]),
                RuntimeEnumVariant::new("PostToolUseFailure", &[]),
                RuntimeEnumVariant::new("Stop", &[]),
                RuntimeEnumVariant::new("SubagentStop", &[]),
                RuntimeEnumVariant::new("PreCompact", &[]),
                RuntimeEnumVariant::new("PostCompact", &[]),
                RuntimeEnumVariant::new("Notification", &[]),
                RuntimeEnumVariant::new("SubagentStart", &[]),
                RuntimeEnumVariant::new("Setup", &[]),
                RuntimeEnumVariant::new("TaskCreated", &[]),
                RuntimeEnumVariant::new("TaskCompleted", &[]),
                RuntimeEnumVariant::new("WorktreeCreate", &[]),
                RuntimeEnumVariant::new("WorktreeRemove", &[]),
            ],
            &[
                Self::SessionStart,
                Self::SessionEnd,
                Self::UserPromptSubmit,
                Self::PreToolUse,
                Self::PostToolUse,
                Self::PostToolUseFailure,
                Self::Stop,
                Self::SubagentStop,
                Self::PreCompact,
                Self::PostCompact,
                Self::Notification,
                Self::SubagentStart,
                Self::Setup,
                Self::TaskCreated,
                Self::TaskCompleted,
                Self::WorktreeCreate,
                Self::WorktreeRemove,
            ],
        )
    }
}

/// The full ordered set of hook events.
pub const HOOK_EVENTS: [HookEvent; 17] = [
    HookEvent::SessionStart,
    HookEvent::SessionEnd,
    HookEvent::UserPromptSubmit,
    HookEvent::PreToolUse,
    HookEvent::PostToolUse,
    HookEvent::PostToolUseFailure,
    HookEvent::Stop,
    HookEvent::SubagentStop,
    HookEvent::PreCompact,
    HookEvent::PostCompact,
    HookEvent::Notification,
    HookEvent::SubagentStart,
    HookEvent::Setup,
    HookEvent::TaskCreated,
    HookEvent::TaskCompleted,
    HookEvent::WorktreeCreate,
    HookEvent::WorktreeRemove,
];

/// The matcher value for trigger-keyed events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HookTrigger {
    Init,
    Maintenance,
    Compact,
    Manual,
    Auto,
}

impl RuntimeEncode for HookTrigger {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Init => "init",
            Self::Maintenance => "maintenance",
            Self::Compact => "compact",
            Self::Manual => "manual",
            Self::Auto => "auto",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for HookTrigger {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("init", &[]),
                RuntimeEnumVariant::new("maintenance", &[]),
                RuntimeEnumVariant::new("compact", &[]),
                RuntimeEnumVariant::new("manual", &[]),
                RuntimeEnumVariant::new("auto", &[]),
            ],
            &[
                Self::Init,
                Self::Maintenance,
                Self::Compact,
                Self::Manual,
                Self::Auto,
            ],
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct HookInput {
    pub event: HookEvent,
    pub cwd: RuntimeString,
    pub session_id: RuntimeString,
    pub tool_name: Option<RuntimeString>,
    pub tool_input: Option<RuntimeJson>,
    pub tool_result: Option<RuntimeJson>,
    /// For `PostToolUseFailure`: the formatted error text the tool threw.
    pub error: Option<RuntimeString>,
    /// For `PostToolUseFailure`: true when the throw was an abort (user
    /// interrupt).
    pub is_interrupt: Option<bool>,
    pub prompt: Option<RuntimeString>,
    pub message: Option<RuntimeString>,
    /// For `SubagentStart`: the sub-agent's role/type (the matcher).
    pub agent_type: Option<RuntimeString>,
    /// For `SubagentStart`: the sub-agent's id.
    pub agent_id: Option<RuntimeString>,
    /// The matcher value for trigger-keyed events (Setup / PreCompact / PostCompact).
    pub trigger: Option<HookTrigger>,
}

impl RuntimeEncode for HookInput {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("event", &self.event)?;
        object.field("cwd", &self.cwd)?;
        object.field("sessionId", &self.session_id)?;
        if let Some(value) = &self.tool_name {
            object.field("toolName", value)?;
        }
        if let Some(value) = &self.tool_input {
            object.field("toolInput", value)?;
        }
        if let Some(value) = &self.tool_result {
            object.field("toolResult", value)?;
        }
        if let Some(value) = &self.error {
            object.field("error", value)?;
        }
        if let Some(value) = &self.is_interrupt {
            object.field("isInterrupt", value)?;
        }
        if let Some(value) = &self.prompt {
            object.field("prompt", value)?;
        }
        if let Some(value) = &self.message {
            object.field("message", value)?;
        }
        if let Some(value) = &self.agent_type {
            object.field("agentType", value)?;
        }
        if let Some(value) = &self.agent_id {
            object.field("agentId", value)?;
        }
        if let Some(value) = &self.trigger {
            object.field("trigger", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for HookInput {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                event: object.required("event", &[])?,
                cwd: object.required("cwd", &[])?,
                session_id: object.required("sessionId", &[])?,
                tool_name: object.optional("toolName", &[])?,
                tool_input: object.optional("toolInput", &[])?,
                tool_result: object.optional("toolResult", &[])?,
                error: object.optional("error", &[])?,
                is_interrupt: object.optional("isInterrupt", &[])?,
                prompt: object.optional("prompt", &[])?,
                message: object.optional("message", &[])?,
                agent_type: object.optional("agentType", &[])?,
                agent_id: object.optional("agentId", &[])?,
                trigger: object.optional("trigger", &[])?,
            })
        })
    }
}

/// Three-state result: abort halts the turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookResult {
    Success,
    FailedContinue,
    FailedAbort,
}

impl RuntimeEncode for HookResult {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Success => "success",
            Self::FailedContinue => "failed_continue",
            Self::FailedAbort => "failed_abort",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for HookResult {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("success", &[]),
                RuntimeEnumVariant::new("failed_continue", &[]),
                RuntimeEnumVariant::new("failed_abort", &[]),
            ],
            &[Self::Success, Self::FailedContinue, Self::FailedAbort],
        )
    }
}

/// For PreToolUse/Stop: override the decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HookDecision {
    Approve,
    Block,
    Ask,
}

impl RuntimeEncode for HookDecision {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Approve => "approve",
            Self::Block => "block",
            Self::Ask => "ask",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for HookDecision {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("approve", &[]),
                RuntimeEnumVariant::new("block", &[]),
                RuntimeEnumVariant::new("ask", &[]),
            ],
            &[Self::Approve, Self::Block, Self::Ask],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct HookOutcome {
    pub result: Option<HookResult>,
    /// For PreToolUse/Stop: override the decision.
    pub decision: Option<HookDecision>,
    /// For PreToolUse: replace the tool input.
    pub updated_input: Option<RuntimeJson>,
    /// For PostToolUse: REPLACE the model-visible tool output.
    pub updated_tool_output: Option<RuntimeJson>,
    /// Surfaced to the user as a notification.
    pub system_message: Option<RuntimeString>,
    /// Injected into the conversation.
    pub additional_context: Option<RuntimeString>,
    /// A PreToolUse `ask`/`block` reason.
    pub permission_reason: Option<RuntimeString>,
    /// For `WorktreeCreate`: the path a host-managed (non-git) worktree was
    /// created at.
    pub worktree_path: Option<RuntimeString>,
}

impl RuntimeEncode for HookOutcome {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        if let Some(value) = &self.result {
            object.field("result", value)?;
        }
        if let Some(value) = &self.decision {
            object.field("decision", value)?;
        }
        if let Some(value) = &self.updated_input {
            object.field("updatedInput", value)?;
        }
        if let Some(value) = &self.updated_tool_output {
            object.field("updatedToolOutput", value)?;
        }
        if let Some(value) = &self.system_message {
            object.field("systemMessage", value)?;
        }
        if let Some(value) = &self.additional_context {
            object.field("additionalContext", value)?;
        }
        if let Some(value) = &self.permission_reason {
            object.field("permissionReason", value)?;
        }
        if let Some(value) = &self.worktree_path {
            object.field("worktreePath", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for HookOutcome {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                result: object.optional("result", &[])?,
                decision: object.optional("decision", &[])?,
                updated_input: object.optional("updatedInput", &[])?,
                updated_tool_output: object.optional("updatedToolOutput", &[])?,
                system_message: object.optional("systemMessage", &[])?,
                additional_context: object.optional("additionalContext", &[])?,
                permission_reason: object.optional("permissionReason", &[])?,
                worktree_path: object.optional("worktreePath", &[])?,
            })
        })
    }
}

/// A hook function callback (the in-process function handler variant).
#[async_trait]
pub trait HookFn: Send + Sync {
    async fn run(&self, input: HookInput) -> HookOutcome;
}

/// How a hook is executed: an in-process function, or a spawned command.
#[derive(Clone)]
pub enum HookHandler {
    Fn(Arc<dyn HookFn>),
    Command {
        command: String,
        timeout_ms: Option<u64>,
    },
}

#[derive(Clone)]
pub struct Hook {
    pub event: HookEvent,
    /// Tool-name / command-prefix pattern (e.g. "Bash(git *)"). `None` = all.
    pub matcher: Option<String>,
    pub handler: HookHandler,
    /// Run this hook in the BACKGROUND: `dispatch` does not await
    /// it, so it never blocks the tool call or turn.
    pub is_async: bool,
}

/// Aggregate of all matching hooks' outcomes for one dispatch.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct CombinedHookOutcome {
    /// A hook `decision: 'block'` — deny just THIS call; the agent loop
    /// continues.
    pub blocked: bool,
    /// The deny reason from the FIRST hook that blocked.
    pub block_reason: Option<RuntimeString>,
    /// A PreToolUse hook `decision: 'ask'` — escalate to the user permission
    /// prompt.
    pub asked: bool,
    /// The ask reason from the FIRST hook that asked.
    pub ask_reason: Option<RuntimeString>,
    /// A hook `result: 'failed_abort'` — HALT the whole turn.
    pub aborted: bool,
    /// A PreToolUse hook approved the call — auto-allow, skipping the prompt.
    pub approved: bool,
    pub updated_input: Option<RuntimeJson>,
    /// A PostToolUse hook's replacement for the model-visible output (last writer
    /// wins).
    pub updated_tool_output: Option<RuntimeJson>,
    pub system_messages: Vec<RuntimeString>,
    pub additional_context: Vec<RuntimeString>,
    /// Last `worktreePath` set by a `WorktreeCreate` handler, if any.
    pub worktree_path: Option<RuntimeString>,
}

impl RuntimeEncode for CombinedHookOutcome {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("blocked", &self.blocked)?;
        if let Some(value) = &self.block_reason {
            object.field("blockReason", value)?;
        }
        object.field("asked", &self.asked)?;
        if let Some(value) = &self.ask_reason {
            object.field("askReason", value)?;
        }
        object.field("aborted", &self.aborted)?;
        object.field("approved", &self.approved)?;
        if let Some(value) = &self.updated_input {
            object.field("updatedInput", value)?;
        }
        if let Some(value) = &self.updated_tool_output {
            object.field("updatedToolOutput", value)?;
        }
        object.field("systemMessages", &self.system_messages)?;
        object.field("additionalContext", &self.additional_context)?;
        if let Some(value) = &self.worktree_path {
            object.field("worktreePath", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for CombinedHookOutcome {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                blocked: object.required("blocked", &[])?,
                block_reason: object.optional("blockReason", &[])?,
                asked: object.required("asked", &[])?,
                ask_reason: object.optional("askReason", &[])?,
                aborted: object.required("aborted", &[])?,
                approved: object.required("approved", &[])?,
                updated_input: object.optional("updatedInput", &[])?,
                updated_tool_output: object.optional("updatedToolOutput", &[])?,
                system_messages: object.required("systemMessages", &[])?,
                additional_context: object.required("additionalContext", &[])?,
                worktree_path: object.optional("worktreePath", &[])?,
            })
        })
    }
}
