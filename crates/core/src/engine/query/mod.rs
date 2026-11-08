//! The model-call loop: drives ONE turn. It assembles the [`ModelRequest`],
//! streams [`ModelStreamEvent`]s from the injected call-model seam, accumulates
//! the assistant blocks in stream order, dispatches tool calls (permission +
//! hooks + sandbox), and folds in compaction / loop-detection / token-budget
//! accounting — surfacing everything as [`EngineEvent`]s on a single outward
//! stream.
//!
//! Results travel as events (`turn.completed` / `item.completed`), never as the
//! stream's own return value — a consumer that only drains the event stream sees
//! the whole turn.
//!
//! ## API assumptions (load-bearing)
//!
//! The owning `Session` constructs [`QueryParams`]. Two shapes are intentionally
//! modeled with Rust ownership:
//!
//! - `tool_context_base` is a FACTORY (`Arc<dyn Fn() -> ToolContext + Send +
//!   Sync>`) rather than a single value: [`ToolContext`] holds `Box<dyn Fn>`
//!   fields that are not `Clone`, so the loop mints a fresh base per tool call
//!   and overwrites the per-call fields (`emit`, `signal`, `request_approval`,
//!   `surface_tools`, `tool_catalog`, `apply_skill_context`, `invoke_tool`) plus
//!   the turn-scoped permission grants. The scalars the loop reads directly
//!   (`cwd`, `task_list_id`, `background_tasks`) are captured once from a probe.
//! - `Tool::parse_input` is the single input-parsing boundary.
//!   Provider-normalized raw input remains immutable for item events,
//!   lifecycle records, and model-visible history; the parsed effective input
//!   flows through validation, PreToolUse, permission, and execution exactly
//!   once. Trusted Hook/permission rewrites are not schema-parsed a second time.

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use crate::context::total_tokens_reminder::PaddedCountdownSpend;
use crate::engine::async_queue::AsyncQueue;
use crate::engine::deps::EngineDeps;
use crate::engine::message_queue::MessageQueue;
use crate::engine::session::{InferenceActivityGuard, InferenceBundle, InferenceSlot};
use crate::hooks::bus::HookBus;
use crate::permission::engine::PermissionEngine;
use crate::provider::behavior::{
    normalize_tool_input_for_behavior, resolve_internal_tool_name, ProviderBehaviorProfile,
};
use crate::runtime_values::{RuntimeJson, RuntimePath, RuntimeString};
use crate::schemas::config::ResolvedConfig;
use crate::schemas::content::{Citation, NativeSearchResult, ToolUseBlock};
use crate::schemas::effort::EffortValue;
use crate::schemas::message::{Message, ToolResultMessage, Usage};
use crate::sync_ext::MutexExt;
use crate::types::abort::AbortSignal;
use crate::types::errors::{InputValidationError, ProviderError, ToolError};
use crate::types::events::{EngineEvent, InferenceIdentity, ToolDomainEvent, ToolLifecycleRecord};
use crate::types::json::Json;
use crate::types::permission::{ApprovalHandler, ApprovalReviewer, PermissionUpdate};
use crate::types::provider::{
    CacheTtl, ModelRequest, ModelStopReason, ProviderInfo, ProviderToolCallKind, SystemPromptPart,
    ThinkingConfig, ToolInputFormat, ToolSpec, WebSearchAction, WebSearchRequest,
};
use crate::types::reminder::ReminderProvider;
use crate::types::tool::{BackgroundTaskRegistry, Tool, ToolContext};

mod compaction;
mod orchestration;
mod precompute;
pub(crate) use precompute::PrecomputeRegistry;
pub(super) mod request;
mod result_trim;
mod streaming;
mod support;
pub(super) mod tool_dispatch;
mod tool_tasks;
pub(crate) use tool_dispatch::gate_request_images;
pub use tool_tasks::RetainedToolTasks;
mod turn;

pub use compaction::compaction_summary_of;
use compaction::*;
pub use request::abortable_sleep;
#[cfg(any(test, feature = "testing"))]
pub(crate) use request::catalog_entry;
use request::*;
pub(crate) use request::{finalize_instruction_system, visible_tool_specs};
#[cfg(any(test, feature = "testing"))]
pub(crate) use result_trim::tool_result_spill_limit_from_override;
use result_trim::*;
// Shared with the persisted-transcript projection so a rebuilt buffer renders a
// tool failure exactly the way the live event did.
pub(crate) use result_trim::tool_result_error_detail;
// Consumed by the trace harnesses under `testing`; the production callers
// import straight from `result_trim`.
#[cfg(any(test, feature = "testing"))]
pub(crate) use result_trim::{render_spilled_tool_result, render_trimmed_tool_result};
use streaming::*;
use support::*;
use tool_dispatch::*;
#[cfg(any(test, feature = "testing"))]
pub use turn::query;
pub(crate) use turn::{query_isolated, query_with_inference};

// ---------------------------------------------------------------------------
// Tuning constants.
// ---------------------------------------------------------------------------

/// The output budget a turn falls back to when its config carries no resolved
/// number. `config_for_turn` always resolves one from the model, so this only
/// covers a config built by hand.
pub(crate) const DEFAULT_TURN_OUTPUT_TOKENS: u64 = 8_192;
const MAX_STOP_CONTINUATIONS: u32 = 3;
const MAX_OUTPUT_SCHEMA_RESAMPLES: u32 = 2;
/// Truncation recovery: two shapes, one for a cut that already produced visible
/// work and one for a cut that produced none. The ceilings below are Klaus's own
/// numbers — what is being pinned is the recovery SHAPE each one describes.
///
/// A response cut off at the output ceiling with visible text already on screen:
/// inject a continuation nudge and keep going.
pub(crate) const MAX_TRUNCATION_CONTINUATIONS: u32 = 3;
/// A response cut off having produced NO visible text (the whole budget went to
/// reasoning): inject a "keep thinking concise" reminder and re-sample.
/// Exhaustion blocks on the user instead of settling silently.
pub(crate) const MAX_EMPTY_CONTENT_RETRIES: u32 = 2;
/// Retries for a closing response that came back EMPTY but stopped NORMALLY
/// (not at the output ceiling) after the turn had already produced visible
/// work. One retry: enough to recover a gateway hiccup or a model that simply
/// forgot to close, not enough to loop on a provider that keeps answering
/// empty. Once spent, the turn fails instead of settling as a completion —
/// a task whose model went silent is unfinished, and settling it silently is
/// what makes the agent look like it stopped working on its own.
pub(crate) const MAX_EMPTY_CLOSING_RETRIES: u32 = 1;
/// A tool call whose arguments were cut off mid-JSON: re-ask with a larger
/// budget, then with an instruction to halve the output (the two prompts are
/// `...I will increase the token limit...` and `...reduce the output content
/// by half`).
pub(crate) const MAX_TOOL_OVER_LIMIT_RETRIES: u32 = 2;
/// Token-budget auto-continue: keep going while total output is under 90% of the
/// target; after `BUDGET_MIN_CONTINUATIONS` continuations, stop once
/// `BUDGET_DIMINISHING_STREAK` of them in a row each add fewer than
/// `BUDGET_DIMINISHING_TOKENS` output tokens.
const BUDGET_COMPLETION_THRESHOLD: f64 = 0.9;
const BUDGET_MIN_CONTINUATIONS: u32 = 3;
const BUDGET_DIMINISHING_TOKENS: u64 = 500;
const BUDGET_DIMINISHING_STREAK: u32 = 2;
/// Steering modes the main loop injects mid-turn. Other modes stay queued for the
/// host's post-turn routing.
const INLINE_DRAIN_MODES: [&str; 3] = ["prompt", "task-notification", "subagent-message"];
const MAX_TURN_PDF_BYTES: usize = crate::fs::pdf::PDF_TARGET_RAW_SIZE as usize;
const MAX_TURN_PDF_DOCUMENTS: usize = 32;

// ---------------------------------------------------------------------------
// Public parameter / record types.
// ---------------------------------------------------------------------------

/// The system-prompt input: either the resolved parts or a (possibly-async)
/// thunk producing them.
pub type SystemPromptThunk =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Vec<SystemPromptPart>> + Send>> + Send + Sync>;

/// A system prompt that is either already-resolved parts or a thunk computing
/// them each turn.
#[derive(Clone)]
pub enum SystemPromptResolvable {
    /// Already-resolved parts. Production sessions always pass a thunk; this
    /// arm is constructed by the trace harnesses and tests.
    #[cfg_attr(not(any(test, feature = "testing")), allow(dead_code))]
    Parts(Vec<SystemPromptPart>),
    Fn(SystemPromptThunk),
}

impl std::fmt::Debug for SystemPromptResolvable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SystemPromptResolvable::Parts(p) => f.debug_tuple("Parts").field(p).finish(),
            SystemPromptResolvable::Fn(_) => f.write_str("Fn(..)"),
        }
    }
}

/// Recompute the live tool set between turns (the host's `refresh_tools`).
pub type RefreshTools = Box<dyn Fn() -> Vec<Arc<dyn Tool>> + Send + Sync>;
pub struct AsyncToolRefresh {
    pub tools: Vec<Arc<dyn Tool>>,
    pub refresh_system_prompt: bool,
}

pub type AsyncRefreshTools = Box<
    dyn Fn() -> Pin<Box<dyn Future<Output = Result<AsyncToolRefresh, String>> + Send>>
        + Send
        + Sync,
>;
/// Live cumulative session spend (USD) reader, used to enforce `max_budget_usd`.
pub type CostSoFar = Box<dyn Fn() -> f64 + Send + Sync>;
/// Consumer-observed usage owned by one physical provider request. Its Drop
/// implementation must settle any observed sample without awaiting persistence.
pub trait RequestUsageReceipt: Send {
    fn observe(&mut self, usage: &Usage);
    fn settle(&mut self);
}

pub type PrepareRequestUsage = Arc<
    dyn Fn(
            RuntimeString,
            RuntimeString,
            AbortSignal,
        ) -> futures::future::BoxFuture<
            'static,
            Result<Box<dyn RequestUsageReceipt>, RuntimeString>,
        > + Send
        + Sync,
>;

/// Persistence hook — invoked for every canonical message as it is produced.
pub type OnMessage = Box<
    dyn Fn(Message) -> futures::future::BoxFuture<'static, Result<(), RuntimeString>> + Send + Sync,
>;
/// Apply approval-driven permission updates back into the owning Session.
pub type OnPermissionUpdate = Box<dyn Fn(Vec<PermissionUpdate>) + Send + Sync>;
/// Durability hook for compaction — invoked once each time the loop compacts.
/// Apply the source-aware replacement contract on [`CompactionRecord`].
pub type OnCompaction = Box<
    dyn Fn(CompactionRecord) -> futures::future::BoxFuture<'static, Result<(), RuntimeString>>
        + Send
        + Sync,
>;
/// Restore portable context from durable records without consulting its former provider.
pub type RecoverCompactionHistory = Arc<
    dyn Fn(Vec<Message>) -> futures::future::BoxFuture<'static, Result<Vec<Message>, RuntimeString>>
        + Send
        + Sync,
>;
/// Writeback invoked when `tool_search` surfaces deferred tools.
pub type OnSurfaceTools = Box<
    dyn Fn(Vec<RuntimeString>) -> futures::future::BoxFuture<'static, Result<(), ToolError>>
        + Send
        + Sync,
>;
/// Durability hook for fresh per-message tool-result budget replacements.
pub type OnToolResultBudgetRecords = Arc<
    dyn Fn(
            Vec<(RuntimeString, RuntimeString)>,
        ) -> futures::future::BoxFuture<'static, Result<(), ToolError>>
        + Send
        + Sync,
>;
pub type OnToolLifecycle = Box<
    dyn Fn(ToolLifecycleRecord) -> futures::future::BoxFuture<'static, Result<(), ToolError>>
        + Send
        + Sync,
>;
/// One-shot/idempotent durability boundary invoked immediately after the
/// provider adapter accepts a request and before its stream is polled.
pub type OnProviderRequestAdmitted =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<(), RuntimeString>> + Send>> + Send + Sync>;
/// Durable receipt returned after a synchronous provider-call phase signal.
/// The signal itself is deliberately not async: the owning Session must know
/// whether a request may cross the provider boundary before `call_model`, and
/// whether that boundary returned normally before any caller-owned await.
pub type ProviderCallAdmissionReceipt =
    Pin<Box<dyn Future<Output = Result<(), RuntimeString>> + Send>>;

/// Per-turn provider boundary used by one-time context owners. Implementations
/// must make every method idempotent because model fallback/retry can invoke the
/// same guard more than once after its first durable terminal.
pub trait ProviderCallAdmission: Send + Sync {
    /// Validate or restore the complete request immediately before provider admission.
    fn prepare_request(&self, _request: &mut ModelRequest) -> Result<(), RuntimeString> {
        Ok(())
    }

    /// Return only newly admitted source context for the owning query history.
    fn take_admitted_request_context(&self) -> Option<(Vec<Message>, Vec<SystemPromptPart>)> {
        None
    }

    /// A local input budget refusal may compact once before rebuilding the request.
    fn input_budget_exceeded(&self) -> bool {
        false
    }

    /// A summary-service failure supersedes a prior local input-budget refusal.
    fn clear_input_budget_failure(&self) {}

    /// Synchronously acknowledge that the next provider factory call may run.
    /// An error is fail-closed: `call_model` is not invoked.
    fn before_call(&self) -> Result<(), RuntimeString>;

    /// Synchronously record that `call_model` returned a stream, then return the
    /// durable terminal receipt that must settle before stream polling.
    fn call_returned(&self) -> ProviderCallAdmissionReceipt;

    /// Synchronously record that the provider factory panicked, then return the
    /// conservative outcome-unknown durability receipt.
    fn call_panicked(&self) -> ProviderCallAdmissionReceipt;
}
/// Base tool context factory: mints a fresh base [`ToolContext`] for each tool
/// call. The loop wires the per-call `emit`/`signal` (and the reentry seams) onto
/// the minted value.
pub type ToolContextFactory = Arc<dyn Fn() -> ToolContext + Send + Sync>;

/// A main query captures at each normal step; independent work keeps the guard
/// admitted by its owner before the worker is scheduled.
type CaptureInferenceFuture =
    Pin<Box<dyn Future<Output = Result<Arc<InferenceActivityGuard>, String>> + Send>>;
type CaptureNextInference = Arc<dyn Fn(AbortSignal) -> CaptureInferenceFuture + Send + Sync>;

pub(crate) struct InferenceCapture {
    pub slot: InferenceSlot,
    pub next: Option<CaptureNextInference>,
    pub model_override: Option<RuntimeString>,
    pub effort_override: Option<EffortValue>,
    /// Per-turn local orchestration override; `None` reads the captured bundle.
    pub ultra_override: Option<bool>,
}

impl InferenceCapture {
    async fn capture(&self, signal: &AbortSignal) -> Result<Arc<InferenceActivityGuard>, String> {
        let guard = if let Some(next) = &self.next {
            tokio::select! {
                biased;
                _ = signal.cancelled() => return Err("inference capture cancelled".to_string()),
                result = next(signal.clone()) => result?,
            }
        } else {
            self.slot.lock_recover().clone().ok_or_else(|| {
                "fixed inference capture is missing its admitted guard".to_string()
            })?
        };
        *self.slot.lock_recover() = Some(guard.clone());
        Ok(guard)
    }
}

/// A durable compaction checkpoint handed to `on_compaction`. `replacement_history`
/// is the post-compaction snapshot of `source_message_ids`. A callback must keep
/// typed question answers admitted outside that source; they were not summarized.
/// `replaced_range` (`[0, replaced_count)`) and `summary` are trace metadata.
#[derive(Debug, Clone)]
pub struct CompactionRecord {
    /// History repair replaces a snapshot without retiring real context usage.
    pub retires_context: bool,
    pub summary: RuntimeString,
    pub replaced_range: (usize, usize),
    pub replacement_history: Vec<Message>,
    pub source_message_ids: Vec<RuntimeString>,
}

/// The parameters that drive one turn of the loop.
pub struct QueryParams {
    pub thread_id: RuntimeString,
    pub turn_id: RuntimeString,
    pub model: RuntimeString,
    pub provider_info: Option<ProviderInfo>,
    pub messages: Vec<Message>,
    /// Messages delivered to the model but excluded from compaction input.
    /// They are reinserted into the exact replacement snapshot after compaction.
    pub compaction_protected_message_ids: HashSet<RuntimeString>,
    /// Shared only by queries that own the resumable conversation.
    pub context_window: Option<crate::context::window_usage::ContextWindowUsage>,
    /// Session-scoped background compaction precompute state. Present only on
    /// queries that own the resumable conversation; auxiliary and child
    /// queries keep the synchronous compaction path.
    pub precompute: Option<Arc<precompute::PrecomputeRegistry>>,
    pub tools: Vec<Arc<dyn Tool>>,
    pub tool_specs: Vec<ToolSpec>,
    /// Optional host callback to recompute the live tool set between turns.
    pub refresh_tools: Option<RefreshTools>,
    pub refresh_tools_async: Option<AsyncRefreshTools>,
    /// Immutable root restriction, also inherited by its internal child queries.
    pub agent_tool_policy: Option<crate::engine::agent_definitions::AgentDefinition>,
    pub system_prompt: SystemPromptResolvable,
    pub config: ResolvedConfig,
    /// Optional ceiling for an isolated child run. Main conversation turns are
    /// deliberately unbounded unless a root definition is selected; fork, sub-agent, and maintenance callers set
    /// their own scoped limit here.
    pub max_turns: Option<u32>,
    /// Live cumulative session spend (USD) reader; absent → the budget cap is
    /// inert.
    pub cost_so_far: Option<CostSoFar>,
    /// Base tool context; per-call `emit` + `signal` are wired by the loop.
    pub tool_context_base: ToolContextFactory,
    pub permission_engine: Arc<dyn PermissionEngine>,
    pub hook_bus: Option<Arc<HookBus>>,
    pub signal: crate::types::abort::AbortSignal,
    /// Session-owned accepted tool executions; absent uses a query-local owner.
    pub retained_tool_tasks: Option<Arc<RetainedToolTasks>>,
    pub approval_handler: Option<Arc<dyn ApprovalHandler>>,
    pub approval_reviewer: Option<Arc<dyn ApprovalReviewer>>,
    pub output_schema: Option<RuntimeJson>,
    /// Soft OUTPUT-token target: while total output stays under ~90% of this, a
    /// natural stop re-prompts the model to keep working.
    pub token_budget: Option<u64>,
    /// Goal mode's runtime for this thread. Present only when goal mode is on.
    ///
    /// The turn loop charges the goal per provider response, which is also how
    /// a budget stop is noticed WHILE the turn runs rather than one turn late.
    pub goal: Option<std::sync::Arc<crate::goal::runtime::GoalRuntime>>,
    /// Generation-bound root accounting for descendant inference; never grants goal mutation tools.
    pub goal_request_identity:
        Option<Arc<std::sync::Mutex<crate::goal::runtime::GoalRequestIdentity>>>,
    pub goal_pending_input: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    pub goal_execution_enabled: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    pub goal_usage: Option<(
        std::sync::Arc<crate::goal::runtime::GoalRuntime>,
        RuntimeString,
    )>,
    /// Native server-side web search for this turn.
    pub web_search: Option<WebSearchRequest>,
    pub on_message: Option<OnMessage>,
    pub prepare_request_usage: Option<PrepareRequestUsage>,
    /// Child transcript durability must precede parent queue consumption.
    pub message_rollout: Option<Arc<dyn crate::store::session_store::LiveRollout>>,
    pub on_permission_update: Option<OnPermissionUpdate>,
    pub on_compaction: Option<OnCompaction>,
    /// Authoritative conversation snapshot used to refresh retained users after a summary.
    pub compaction_history: Option<Arc<dyn Fn() -> Vec<Message> + Send + Sync>>,
    /// On-demand expansion of incompatible opaque checkpoints from original local records.
    pub recover_compaction_history: Option<RecoverCompactionHistory>,
    /// Completed summary items recorded by the owning conversation.
    pub compaction_output: Option<crate::context::compact::full_compact::CompactionOutputSink>,
    /// Tools already surfaced via `tool_search` earlier in this session.
    pub surfaced_tools: Option<Vec<RuntimeString>>,
    pub on_surface_tools: Option<OnSurfaceTools>,
    pub on_tool_lifecycle: Option<OnToolLifecycle>,
    pub provider_call_admission: Option<Arc<dyn ProviderCallAdmission>>,
    pub on_provider_request_admitted: Option<OnProviderRequestAdmitted>,
    pub emit_thread_started: bool,
    /// Per-turn ephemeral context sources.
    pub reminder_providers: Vec<Arc<dyn ReminderProvider>>,
    /// External-edit scan recomputed before EVERY model request this turn.
    pub edited_files_reminder: Option<Arc<dyn ReminderProvider>>,
    pub effort: Option<EffortValue>,
    /// Resolved local orchestration mode for this turn (per-turn override
    /// folded over the session config before the loop starts).
    pub ultra_mode: bool,
    /// Ordinary root and child turns receive delegation policy even when their
    /// current tools exclude spawning. Internal and side-task queries omit it.
    pub multi_agent_instructions: bool,
    pub thinking: Option<ThinkingConfig>,
    /// Detached fork whose message tail no later request resumes.
    pub skip_cache_write: Option<bool>,
    /// Use the 1-hour prompt-cache TTL instead of the default 5 minutes.
    pub cache_ttl: Option<CacheTtl>,
    /// Stable per-conversation cache-routing key.
    pub cache_key: Option<RuntimeString>,
    /// Shared cache-routing domain for internal sessions spawned from one
    /// parent thread. Wire providers prefer this over `cache_key` when routing
    /// prompt caching; per-session identity and trace attribution keep using
    /// the unique `cache_key`.
    pub cache_domain: Option<RuntimeString>,
    /// This loop's plan-mode plan document, when the session has one. Feeds
    /// the plan-mode reminder's "read-only except plan file" guidance.
    pub plan_file_path: Option<RuntimePath>,
    /// Mid-turn steering queue.
    pub message_queue: Option<Arc<MessageQueue>>,
    /// Decoded native PDF bytes submitted on the initial user turn.
    pub input_pdf_bytes: usize,
    /// Native PDF parts submitted on the initial user turn.
    pub input_pdf_documents: usize,
    /// Revalidate native PDF support against the model used by each actual
    /// provider request, including model fallbacks and mid-turn overrides.
    #[allow(clippy::type_complexity)]
    pub validate_native_pdf:
        Option<Arc<dyn Fn(&RuntimeString) -> Result<(), InputValidationError> + Send + Sync>>,
    /// This loop's agent identity for steering scope: `None` = the main thread.
    pub agent_id: Option<RuntimeString>,
    /// Session-scoped memory of per-message tool-result budget decisions (see
    /// [`crate::context::tool_result_budget`]). Present on turn loops whose
    /// replacements must survive into later requests and resume; absent on
    /// maintenance/fork queries whose view-only passes must not freeze ids in
    /// the owning session's state.
    pub tool_result_budget_state:
        Option<Arc<std::sync::Mutex<crate::context::tool_result_budget::ReplacementState>>>,
    /// Frozen native request replacements owned by this main conversation.
    /// Auxiliary and child queries never inherit the parent's hint controller.
    pub context_hint_state: Option<Arc<crate::context::context_hints::ContextHintState>>,
    /// Canonical read-state cache, present on the same turns as
    /// `context_hint_state`. A hint clear drops the baselines of cleared reads
    /// so the next edit re-reads files the model can no longer see.
    pub file_state: Option<Arc<crate::tools::file_state::InMemoryFileStateCache>>,
    /// Persist fresh budget replacement records so a resumed session re-applies
    /// the same bodies byte-identically. Called with this turn's new records.
    pub on_tool_result_budget_records: Option<OnToolResultBudgetRecords>,
    /// This session has been idle past the longest prompt-cache lifetime, so the
    /// cached prefix is gone and aged prior-turn thinking blocks are no longer
    /// worth retransmitting. Sticky once set: the decision is cache-key material.
    pub thinking_context_cold: bool,
    /// Long-horizon token spend carried across compactions, for the padded
    /// remaining-context countdown. Zero for every other reminder mode.
    pub padded_countdown_spend: PaddedCountdownSpend,
    /// Agent-owned task anchor, retained across automatic continuation queries.
    pub task_countdown_anchor:
        Option<Arc<Mutex<crate::context::total_tokens_reminder::TaskCountdownAnchor>>>,
    /// This sub-agent's role, surfaced on the SubagentStop hook input.
    pub agent_type: Option<RuntimeString>,
    /// Mid-turn model override written by `set_active_turn_model`. The query
    /// loop drains this (take) into `TurnContext.model` before each LLM call.
    pub live_model_override: Option<Arc<Mutex<Option<RuntimeString>>>>,
}

impl std::fmt::Debug for QueryParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryParams")
            .field("thread_id", &self.thread_id)
            .field("turn_id", &self.turn_id)
            .field("model", &self.model)
            .field(
                "provider_name",
                &self.provider_info.as_ref().map(|info| &info.name),
            )
            .field("messages", &self.messages.len())
            .field("tools", &self.tools.len())
            .field("agent_id", &self.agent_id)
            .finish_non_exhaustive()
    }
}

/// The live turn model/effort override read by every `prepare_model_request` this
/// turn (an inline skill may switch them mid-turn).
#[derive(Debug, Clone)]
pub struct TurnOverride {
    pub model: RuntimeString,
    pub effort: Option<EffortValue>,
    /// Live local orchestration mode; separate from the provider effort.
    pub ultra_mode: bool,
}

// ---------------------------------------------------------------------------
// Internal per-turn state.
// ---------------------------------------------------------------------------

/// Immutable identity for one query turn. Runtime seams use this instead of
/// reaching back into `QueryParams` when emitting turn-scoped events.
#[derive(Debug, Clone)]
struct TurnRuntimeState {
    thread_id: RuntimeString,
    turn_id: RuntimeString,
}

impl TurnRuntimeState {
    fn from_params(params: &QueryParams) -> Self {
        Self {
            thread_id: params.thread_id.clone(),
            turn_id: params.turn_id.clone(),
        }
    }

    fn thread_started_event(&self) -> EngineEvent {
        EngineEvent::ThreadStarted {
            thread_id: self.thread_id.clone(),
        }
    }

    fn turn_started_event(&self) -> EngineEvent {
        EngineEvent::TurnStarted {
            turn_id: self.turn_id.clone(),
        }
    }

    fn tool_event(&self, tool_use_id: RuntimeString, event: ToolDomainEvent) -> EngineEvent {
        EngineEvent::ToolEvent {
            turn_id: self.turn_id.clone(),
            tool_use_id,
            event,
        }
    }
}

/// Project the stored menu preset into a request's raw reasoning selection.
/// Explicit reasoning writes replace an inherited preset without rewriting the
/// saved native preference.
fn selected_effort(
    native: Option<EffortValue>,
    enabled: bool,
    effort_override: Option<EffortValue>,
    preset_override: Option<bool>,
) -> Option<EffortValue> {
    match preset_override {
        Some(true) => Some(EffortValue::Level(
            crate::schemas::effort::EffortLevel::Ultra,
        )),
        Some(false) => effort_override.or(native),
        None => effort_override.or({
            if enabled {
                Some(EffortValue::Level(
                    crate::schemas::effort::EffortLevel::Ultra,
                ))
            } else {
                native
            }
        }),
    }
}

/// Mutable per-turn context layered on by an inline `skill`.
#[derive(Debug, Default)]
struct TurnContext {
    model: RuntimeString,
    effort: Option<EffortValue>,
    ultra_mode: bool,
    model_override: Option<RuntimeString>,
    effort_override: Option<EffortValue>,
    ultra_override: Option<bool>,
    granted_tools: HashSet<RuntimeString>,
    /// Set when a hook returns `failed_abort` (the main loop stops without
    /// another model call after the current tool batch settles).
    hook_aborted: bool,
}

/// Attribute at enqueue time: a backpressured consumer may drain an old request
/// after the next step has already captured a different bundle.
struct EventQueue {
    queue: Arc<AsyncQueue<EngineEvent>>,
    inference: Mutex<Option<InferenceIdentity>>,
    persistence_failed: Mutex<bool>,
}

impl EventQueue {
    fn new(queue: Arc<AsyncQueue<EngineEvent>>) -> Self {
        Self {
            queue,
            inference: Mutex::new(None),
            persistence_failed: Mutex::new(false),
        }
    }

    fn bind(&self, inference: Option<InferenceIdentity>) {
        *self.inference.lock_recover() = inference;
    }

    fn push(&self, mut event: EngineEvent) {
        let failed = self.persistence_failed.lock_recover();
        if *failed {
            return;
        }
        event.attribute_inference(self.inference.lock_recover().clone());
        self.queue.push(event);
    }

    fn fail_persistence(&self, message: RuntimeString) {
        let mut failed = self.persistence_failed.lock_recover();
        if *failed {
            return;
        }
        *failed = true;
        let mut event = EngineEvent::TurnFailed {
            inference: None,
            error: crate::types::errors::EngineError {
                code: crate::types::errors::EngineErrorCode::Internal,
                message,
                cause: None,
                request_id: None,
                provider_kind: None,
                retry_after_ms: None,
            },
        };
        event.attribute_inference(self.inference.lock_recover().clone());
        self.queue.push(event);
        self.queue.close();
    }

    fn close(&self) {
        self.queue.close();
    }

    async fn next(&self) -> Option<EngineEvent> {
        self.queue.next().await
    }
}

/// Shared state the tool-execution seams capture (all `'static` behind `Arc`).
struct Shared {
    persistence_in_flight: Arc<tool_tasks::QueryPersistence>,
    params: QueryParams,
    deps: Arc<EngineDeps>,
    inference: Option<InferenceCapture>,
    captured: Mutex<Option<Arc<InferenceActivityGuard>>>,
    step_model: Mutex<RuntimeString>,
    step_subagent_context: Mutex<Option<Arc<crate::types::tool::InheritedSubagentContext>>>,
    q: Arc<EventQueue>,
    turn_state: TurnRuntimeState,
    /// Swappable working tool set (a between-turns `refresh_tools` may replace it).
    tools: Mutex<Arc<Vec<Arc<dyn Tool>>>>,
    tool_specs: Mutex<Arc<Vec<ToolSpec>>>,
    /// Deferred tools surfaced via `tool_search` this session.
    surfaced: Mutex<HashSet<RuntimeString>>,
    turn_context: Mutex<TurnContext>,
    /// Runtime values read directly by the loop (probed once from the base factory).
    cwd: RuntimePath,
    task_list_id: Option<RuntimeString>,
    task_plan: Option<crate::tasks::task_store::TaskPlanBinding>,
    background_tasks: Option<Arc<dyn BackgroundTaskRegistry>>,
    advisor_request: Mutex<Option<crate::types::provider::ModelRequest>>,
    /// Externally written mid-turn model override. Drained into `turn_context`
    /// before each LLM request so a model switch takes effect immediately.
    live_model_override: Option<Arc<Mutex<Option<RuntimeString>>>>,
    /// Truncation-recovery bookkeeping, shared between the stream assembler and
    /// the turn loop.
    ///
    /// It lives here rather than in a loop local because the two halves run in
    /// different places and one of them is time-critical: the assembler must
    /// decide whether to write the truncation footnote into the assistant text
    /// BEFORE that message is persisted (the rollout store is append-only, so a
    /// later correction cannot rewrite what already landed), while only the loop
    /// knows whether a continuation is still allowed.
    truncation: Mutex<TruncationRecovery>,
    reminder_spend: Mutex<PaddedCountdownSpend>,
    task_countdown_anchor: Arc<Mutex<crate::context::total_tokens_reminder::TaskCountdownAnchor>>,
    reminder_anchor_pending: Mutex<bool>,
}

impl Shared {
    fn bundle(&self) -> Option<Arc<InferenceBundle>> {
        self.captured
            .lock_recover()
            .as_ref()
            .map(|guard| guard.bundle.clone())
    }

    fn config(&self) -> ResolvedConfig {
        self.captured_config()
    }

    fn captured_config(&self) -> ResolvedConfig {
        // Fixed workers deliberately carry their own scoped configuration,
        // including explicit maintenance/sub-agent output ceilings.
        if self
            .inference
            .as_ref()
            .is_some_and(|capture| capture.next.is_some())
        {
            if let Some(bundle) = self.bundle() {
                let model = self.step_model.lock_recover().clone();
                return bundle.config_for_model(&model);
            }
        }
        self.params.config.clone()
    }

    fn deps(&self) -> EngineDeps {
        if let Some(bundle) = self.bundle() {
            return bundle.deps.to_engine_deps();
        }
        let deps = self.deps.clone();
        EngineDeps {
            provider_id: deps.provider_id.clone(),
            call_model: Box::new(move |request, signal| (deps.call_model)(request, signal)),
            estimate_tokens: self.deps.estimate_tokens.clone(),
            compactor: self.deps.compactor.clone(),
            uuid: self.deps.uuid.clone(),
            now: self.deps.now.clone(),
        }
    }

    async fn capture_step(&self) -> Result<bool, String> {
        let Some(capture) = &self.inference else {
            return Ok(false);
        };
        if capture.next.is_none() {
            return Ok(false);
        }
        self.q.bind(None);
        let guard = capture.capture(&self.params.signal).await?;
        let mut current = self.captured.lock_recover();
        let changed = current.as_ref().is_none_or(|old| {
            old.bundle.revision != guard.bundle.revision
                || std::iter::once(&guard.bundle.config.model)
                    .chain(guard.bundle.config.fallback_models.iter())
                    .chain(capture.model_override.iter())
                    .any(|model| {
                        old.bundle.provider.capabilities(model)
                            != guard.bundle.provider.capabilities(model)
                    })
        });
        if changed {
            let mut context = self.turn_context.lock_recover();
            context.model = context
                .model_override
                .clone()
                .unwrap_or_else(|| guard.bundle.config.model.clone());
            context.effort = selected_effort(
                guard.bundle.config.effort.clone(),
                guard.bundle.config.ultra_mode,
                context.effort_override.clone(),
                context.ultra_override,
            );
            context.ultra_mode = matches!(
                context.effort,
                Some(EffortValue::Level(
                    crate::schemas::effort::EffortLevel::Ultra
                ))
            );
        }
        *current = Some(guard);
        Ok(changed)
    }

    fn validate_pdf(&self, model: &RuntimeString) -> Result<(), Box<InputValidationError>> {
        if let Some(bundle) = self.bundle() {
            return crate::engine::session::validate_native_pdf_provider(
                bundle.provider.as_ref(),
                model,
            )
            .map_err(Box::new);
        }
        match &self.params.validate_native_pdf {
            Some(validate) => validate(model).map_err(Box::new),
            None => Ok(()),
        }
    }

    fn validate_media(&self, request: &ModelRequest) -> Result<(), Box<InputValidationError>> {
        if let Some(bundle) = self.bundle() {
            let metadata = bundle.provider.model_metadata(&request.model);
            crate::provider::native_media::validate_request(
                request,
                &metadata.media_input_mime_types,
            )
            .map_err(Box::new)?;
        }
        Ok(())
    }
}

/// Where a turn stands in the output-truncation recovery ladder.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct TruncationRecovery {
    /// Continuations already granted for a response cut off WITH visible text.
    continuations: u32,
    /// Set once the assembler gave up and wrote the footnote into the message it
    /// was about to persist. The loop reads it to stop offering continuations —
    /// a further nudge after the footnote would contradict what the user can
    /// already read on screen.
    footnote_written: bool,
    /// Retries already granted for tool calls cut off mid-arguments. The
    /// assembler increments this and rides the matching instruction on the
    /// dropped call's error result; the loop reads it to decide whether the
    /// budget ladder still applies.
    tool_over_limit_retries: u32,
}

/// The result of one `stream_and_dispatch`.
enum Outcome {
    Ok {
        messages: Vec<Message>,
        usage: Option<Usage>,
        tool_results: Vec<ToolResultMessage>,
        /// Retain the driver from a committed tool exchange through its final
        /// canonical result callbacks, including a dropped event consumer.
        tool_result_handoff: Option<tool_tasks::QueryPersistenceGuard>,
        stop_reason: ModelStopReason,
        final_text: RuntimeString,
        /// Whether a `sleep` tool ran this iteration.
        sleep_ran: bool,
        /// Unparseable calls at the output ceiling, named in arrival order.
        /// Either the stop reason or measured output usage can establish the
        /// ceiling for safe retries. Only an explicit truncation can authorize
        /// partial writes. The matching results are in `tool_results` as errors.
        truncated_tool_calls: Vec<RuntimeString>,
    },
    Error {
        error: ProviderError,
        emitted_text: bool,
        partial_uuid: RuntimeString,
    },
    InputError {
        error: InputValidationError,
    },
    Fatal {
        error: ToolError,
    },
    ProviderAdmissionError {
        message: RuntimeString,
    },
}

type ToolCallResult = Result<ToolResultMessage, ToolError>;

/// One accumulator entry, in stream-arrival order. Text remains in growable
/// exact UTF-16 buffers until a durable or externally visible snapshot is needed.
enum Assembly {
    Text {
        text: Vec<u16>,
        citations: Vec<Citation>,
        message_id: RuntimeString,
        phase: Option<RuntimeString>,
    },
    Thinking {
        text: Vec<u16>,
        message_id: RuntimeString,
        signature: Option<RuntimeString>,
        encrypted_content: Option<RuntimeString>,
    },
    Redacted {
        data: RuntimeString,
    },
    Tool {
        id: RuntimeString,
    },
    Search {
        id: RuntimeString,
        query: RuntimeString,
        action: Option<WebSearchAction>,
        results: Vec<NativeSearchResult>,
        error: Option<RuntimeString>,
    },
}

struct PartialTool {
    name: RuntimeString,
    raw_input: RuntimeString,
    parsed_input: Option<Json>,
    thought_signature: Option<RuntimeString>,
    tool_kind: Option<ProviderToolCallKind>,
    input_format: ToolInputFormat,
}

impl PartialTool {
    fn finalize_input(&mut self) {
        if self.input_format == ToolInputFormat::Json && self.parsed_input.is_none() {
            // A failed parse must remain distinguishable from a valid empty object.
            // Dispatch and truncation recovery both depend on this state.
            self.parsed_input = crate::runtime_values::runtime_json::parse(&self.raw_input).ok();
        }
    }

    /// Recover a call whose argument JSON was cut off by the output ceiling.
    ///
    /// A truncation lands mid-document, so what arrived is a PREFIX: closing the
    /// open string and the open braces/brackets turns it into a valid document
    /// holding every field the model managed to emit in full. Parsing the repaired
    /// text with the runtime's own parser is what keeps this safe — the unescaping
    /// is the parser's, not a hand-rolled approximation, so recovered file content
    /// cannot be silently mangled.
    ///
    /// Partial escape sequences are excluded from the recovered prefix. Invalid
    /// escapes still fail the runtime parser and leave the call dropped; tool
    /// validation also applies before any recovered values can reach the disk.
    fn salvage_truncated_input(&mut self) -> Option<Json> {
        if self.input_format != ToolInputFormat::Json {
            return None;
        }
        let repaired = repair_truncated_json(&self.raw_input, Some("content"))?;
        let parsed = crate::runtime_values::runtime_json::parse(&repaired).ok()?;
        self.parsed_input = Some(parsed.clone());
        Some(parsed)
    }

    fn provider_input(&self) -> Json {
        match self.input_format {
            ToolInputFormat::Raw => Json::String(self.raw_input.clone()),
            ToolInputFormat::Json => self
                .parsed_input
                .clone()
                .unwrap_or_else(|| safe_runtime_json_parse(&self.raw_input)),
        }
    }

    fn to_tool_use_block(&self, id: &RuntimeString) -> ToolUseBlock {
        ToolUseBlock {
            id: id.clone(),
            name: self.name.clone(),
            input: self.provider_input(),
            thought_signature: self.thought_signature.clone(),
            tool_kind: self.tool_kind,
        }
    }

    /// Whether the accumulated arguments can be read at all.
    ///
    /// A `Json` call whose text does not parse is a PREFIX of what the model meant
    /// to send: the stream ended mid-call. [`Self::provider_input`] answers an empty
    /// object for it, which would hand the tool a call with every argument missing
    /// — an edit with no path, a command with no command. `Raw` format is passed
    /// through verbatim by design, and an empty payload is a legitimate no-argument
    /// call.
    fn arguments_are_parseable(&self) -> bool {
        match self.input_format {
            ToolInputFormat::Raw => true,
            ToolInputFormat::Json => {
                self.parsed_input.is_some()
                    || self.raw_input.is_empty()
                    || crate::runtime_values::runtime_json::parse(&self.raw_input).is_ok()
            }
        }
    }
}

#[derive(Debug, Clone)]
struct ResolvedToolCall {
    visible: ToolUseBlock,
    internal_name: RuntimeString,
    initial_internal_input: Json,
    effective_internal_input: Json,
    trusted_caller: Option<RuntimeString>,
    approval_call_source: crate::types::permission::ApprovalActionCallSource,
    profile: ProviderBehaviorProfile,
}

impl ResolvedToolCall {
    fn new(visible: ToolUseBlock, profile: ProviderBehaviorProfile) -> Self {
        let internal_name = resolve_internal_tool_name(&visible.name, &profile);
        let initial_internal_input =
            normalize_tool_input_for_behavior(&visible.name, visible.input.clone(), &profile);
        Self {
            visible,
            internal_name,
            effective_internal_input: initial_internal_input.clone(),
            initial_internal_input,
            trusted_caller: None,
            approval_call_source: Default::default(),
            profile,
        }
    }

    fn from_internal(
        internal: ToolUseBlock,
        profile: ProviderBehaviorProfile,
        caller: RuntimeString,
    ) -> Self {
        let internal_name = internal.name.clone();
        let initial_internal_input = internal.input.clone();
        Self {
            visible: internal,
            internal_name,
            effective_internal_input: initial_internal_input.clone(),
            initial_internal_input,
            trusted_caller: Some(caller),
            approval_call_source: Default::default(),
            profile,
        }
    }

    fn replace_effective_input(&mut self, input: Json) {
        self.effective_internal_input = input;
    }
}

fn successful_result_provider_metadata(
    call: &ResolvedToolCall,
    output: &Json,
) -> (Option<ProviderToolCallKind>, Option<Json>) {
    let tool_kind = call.visible.tool_kind.or_else(|| {
        (call.internal_name == "tool_search").then_some(ProviderToolCallKind::ToolSearch)
    });
    let native_tool_output =
        (tool_kind == Some(ProviderToolCallKind::ToolSearch)).then(|| output.clone());
    (tool_kind, native_tool_output)
}

#[cfg(test)]
mod tests;
