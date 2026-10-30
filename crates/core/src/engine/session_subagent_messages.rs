//! Ordinary workers retain identity and serialize message/exit handoff.

use crate::engine::agent_definitions::AgentDefinition;
use crate::engine::message_queue::{
    QueuePriority, QueueTerminalState, QueuedMessage, QueuedValue, SteerDelivery,
};
use crate::engine::resource_governor::{ResidencySlot, SlotAdmissionError};
use crate::engine::session::{
    runtime_string_diagnostic, AnyTool, BackgroundAgentEntry, GetConversation, InferenceBundle,
    RunSubAgentOpts, Session, SubagentCapabilityCapture, SubagentPermissionOrigin,
};
use crate::engine::subagent_lifecycle::{SubagentKind, SubagentLifecycleRecord, SubagentStatus};
use crate::runtime_values::{
    runtime_codec, RuntimeDecode, RuntimeJson, RuntimeObject, RuntimePath, RuntimeString,
};
use crate::schemas::config::ResolvedConfig;
use crate::schemas::content::{ContentBlock, TextBlock};
use crate::schemas::message::{Message, UserMessage};
use crate::schemas::rollout::RolloutItem;
use crate::store::session_store::LiveRollout;
use crate::sync_ext::MutexExt;
use crate::types::errors::TerminalReason;
use crate::types::permission::PermissionContext;
use crate::types::provider::SystemPromptPart;
use crate::types::tool::{
    BackgroundTaskStatus, SpawnBackgroundAgentResult, SpawnSubAgentOptions, SubagentMessageRequest,
    SubagentMessageResult,
};
use futures::{FutureExt, StreamExt};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

const SNAPSHOT_EVENT: &str = "ordinary_subagent_creation";
const CREATION_COMMIT_EVENT: &str = "ordinary_subagent_creation_committed";

fn decode_boundary_review_floor(
    value: &RuntimeJson,
) -> Result<Option<crate::types::permission::ApprovalPolicy>, RuntimeString> {
    // Absence is a historical writer. Version 1 requires an explicit policy:
    // null clears the previous boundary floor; only untrusted can add a floor.
    if required::<u32>(value, "version")? != 1 {
        return Err("Unsupported sub-agent review floor evidence version.".into());
    }
    let policy =
        required::<Option<crate::types::permission::ApprovalPolicy>>(value, "approvalPolicy")?;
    if policy
        .is_some_and(|policy| policy != crate::types::permission::ApprovalPolicy::UnlessTrusted)
    {
        return Err("Invalid sub-agent review floor policy.".into());
    }
    Ok(policy)
}
const CANCEL_EVENT: &str = "ordinary_subagent_user_cancelled";
const MESSAGE_MODE: &str = "subagent-message";
const MISSING_RESUME_CONTEXT: &str = "The sub-agent recovery context is unavailable.";

fn ordinary_parent_cancellations(items: &[RolloutItem]) -> HashMap<String, (u64, bool)> {
    let mut cancellation = HashMap::new();
    for item in items {
        let RolloutItem::Event(event) = item else {
            continue;
        };
        if event.event != CANCEL_EVENT {
            continue;
        }
        let Some(payload) = &event.payload else {
            continue;
        };
        let Some(id) = payload
            .get("agentId")
            .and_then(RuntimeJson::as_str)
            .and_then(|id| id.try_to_utf8().ok())
        else {
            continue;
        };
        let generation = payload
            .get("generation")
            .and_then(RuntimeJson::as_u64)
            .unwrap_or(0);
        let cancelled = payload
            .get("userCancelled")
            .and_then(RuntimeJson::as_bool)
            .unwrap_or(true);
        let entry = cancellation.entry(id).or_insert((0_u64, false));
        if generation >= entry.0 {
            *entry = (generation, cancelled);
        }
    }
    cancellation
}

fn ordinary_foreground_failure(
    sender: &tokio::sync::mpsc::UnboundedSender<crate::types::events::EngineEvent>,
    message: RuntimeString,
) {
    let _ = sender.send(crate::types::events::EngineEvent::TurnFailed {
        inference: None,
        error: crate::types::errors::EngineError {
            code: crate::types::errors::EngineErrorCode::InvalidInput,
            message,
            cause: None,
            request_id: None,
            provider_kind: None,
            retry_after_ms: None,
        },
    });
}
#[path = "session_subagent_collaboration_origin.rs"]
mod collaboration_origin;
pub(crate) use collaboration_origin::SubagentCollaborationOrigin;

#[path = "session_subagent_residency.rs"]
mod residency;
#[path = "session_subagent_snapshot.rs"]
mod snapshot;

#[derive(Clone)]
pub(crate) struct SubagentCreationSnapshot {
    pub(crate) tools: Vec<AnyTool>,
    pub(crate) permission: PermissionContext,
    pub(crate) permission_origin: Option<SubagentPermissionOrigin>,
    pub(crate) runtime_capabilities: SubagentCapabilityCapture,
    // None retains verified legacy instructions without inventing a selected mode.
    pub(crate) collaboration_origin: Option<SubagentCollaborationOrigin>,
    pub(crate) config: ResolvedConfig,
    pub(crate) definition: Option<AgentDefinition>,
    pub(crate) agent_type: Option<RuntimeString>,
    pub(crate) bundle: Arc<InferenceBundle>,
    pub(crate) runtime_policy: Arc<crate::engine::session::RuntimePolicySnapshot>,
    // Derived from the latest terminal boundary, separately from immutable creation config.
    pub(crate) boundary_approval_policy: Option<crate::types::permission::ApprovalPolicy>,
    pub(crate) system_prompt: Vec<SystemPromptPart>,
    pub(crate) cwd: RuntimePath,
    pub(crate) max_turns: Option<u32>,
    pub(crate) effort: Option<crate::schemas::effort::EffortValue>,
    pub(crate) ultra_mode: bool,
    pub(crate) output_schema: Option<RuntimeObject>,
    pub(crate) web_search: Option<crate::types::provider::WebSearchRequest>,
    pub(crate) in_fork_child: bool,
    pub(crate) fork_seed: Vec<Message>,
    pub(crate) depth: u32,
    pub(crate) name: Option<RuntimeString>,
    pub(crate) one_shot: bool,
    pub(crate) mcp_clients: Vec<crate::mcp::client::ManagedClient>,
    pub(crate) mcp_runtime: Arc<crate::engine::session::SubagentMcpRuntime>,
    pub(crate) parent_agent_id: Option<String>,
    pub(crate) task_plan: crate::tasks::task_store::TaskPlanBinding,
    pub(crate) task_list_id: RuntimeString,
}

/// The sender's effective query configuration, captured before message handoff.
#[derive(Clone)]
pub(crate) struct SubagentResumeContext {
    pub(crate) mcp: crate::engine::session::SubagentMcpCapture,
    pub(crate) bundle: Arc<InferenceBundle>,
    pub(crate) permission: PermissionContext,
    pub(crate) runtime_policy: Arc<crate::engine::session::RuntimePolicySnapshot>,
    pub(crate) cwd: RuntimePath,
    pub(crate) runtime_capabilities: SubagentCapabilityCapture,
}

#[derive(Clone)]
struct OrdinaryBindings {
    depth: u32,
    spawn_turn_id: Option<RuntimeString>,
    parent_agent_id: Option<String>,
}

struct OrdinaryWorker {
    committed: bool,
    loaded: Option<Arc<tokio::sync::RwLock<()>>>,
    locally_evicted: bool,
    // Produced only when this session successfully unloads the target runtime.
    evicted_capabilities: Option<SubagentCapabilityCapture>,
    observable: bool,
    terminal_status: Option<SubagentStatus>,
    residency_config: Option<ResolvedConfig>,

    bindings: OrdinaryBindings,
    snapshot: Option<Arc<SubagentCreationSnapshot>>,
    boundary_approval_policy: Option<crate::types::permission::ApprovalPolicy>,
    running: bool,
    user_cancelled: bool,
    one_shot: bool,
    abort: CancellationToken,
    exited: Arc<Notify>,
    rollout: Option<Arc<dyn LiveRollout>>,
    error: Option<RuntimeString>,
    creation_metadata: Option<RuntimeJson>,
    cancel_generation: u64,
    cancel_receipts: Vec<Arc<CancellationReceipt>>,
    notify_completion: bool,
    event_followers: Vec<tokio::sync::mpsc::UnboundedSender<crate::types::events::EngineEvent>>,
    worktree: Option<crate::engine::session::AgentWorktree>,
}

struct ResumeReservation {
    queue: Arc<crate::engine::message_queue::MessageQueue>,
    items: Vec<Arc<QueuedMessage>>,
    committed: bool,
}
impl Drop for ResumeReservation {
    fn drop(&mut self) {
        if !self.committed {
            self.queue.release_pending_resume(&self.items);
        }
    }
}

struct OwnedForegroundEvents {
    events: crate::engine::session::EngineEventStream,
    observation: CancellationToken,
}
impl futures::Stream for OwnedForegroundEvents {
    type Item = crate::types::events::EngineEvent;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.get_mut().events.as_mut().poll_next(context)
    }
}
impl Drop for OwnedForegroundEvents {
    fn drop(&mut self) {
        self.observation.cancel();
    }
}

struct OrdinaryEventFollower {
    runtime: Arc<OrdinarySubagentRuntime>,
    id: String,
    sender: tokio::sync::mpsc::UnboundedSender<crate::types::events::EngineEvent>,
}

impl Drop for OrdinaryEventFollower {
    fn drop(&mut self) {
        if let Some(worker) = self.runtime.state.lock_recover().workers.get_mut(&self.id) {
            worker
                .event_followers
                .retain(|follower| !follower.same_channel(&self.sender));
        }
    }
}

struct CancellationReceipt {
    result: Mutex<Option<Result<(), RuntimeString>>>,
    completed: Notify,
}
impl CancellationReceipt {
    async fn wait(&self) -> Result<(), RuntimeString> {
        loop {
            let notified = self.completed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(result) = self.result.lock_recover().clone() {
                return result;
            }
            notified.await;
        }
    }
}

async fn settle_cancellation_receipts(
    receipts: Vec<Arc<CancellationReceipt>>,
) -> Result<(), RuntimeString> {
    let mut failure = None;
    for receipt in receipts {
        if let Err(error) = receipt.wait().await {
            failure.get_or_insert(error);
        }
    }
    failure.map_or(Ok(()), Err)
}

#[derive(Default)]
struct OrdinaryState {
    workers: HashMap<String, OrdinaryWorker>,
    // Bindings are never removed or reassigned: an old name cannot silently address a new worker.
    names: HashMap<RuntimeString, HashSet<String>>,
    next_message: u64,
    processing_message: u64,
    cancellation_epoch: u64,
}

#[derive(Default)]
pub(crate) struct OrdinarySubagentRuntime {
    state: Mutex<OrdinaryState>,
    external_cancel_receipts: Mutex<Vec<(String, Arc<CancellationReceipt>)>>,
    pending_cleanup: tokio_util::task::TaskTracker,
    pending_cleanup_error: Mutex<Option<RuntimeString>>,
    hydration: tokio::sync::Mutex<()>,
    loading: tokio::sync::Mutex<()>,
    // Some(empty) proves a successful parent read; None requires provenance I/O.
    parent_cancellation_seed: Mutex<Option<HashMap<String, (u64, bool)>>>,
    message_turn_changed: Notify,
    #[cfg(test)]
    handoff_gate: Mutex<Option<TestHandoffGate>>,
    #[cfg(test)]
    creation_gate: Mutex<Option<(Arc<Notify>, Arc<tokio::sync::Semaphore>)>>,
}

/// Creation is accepted only after the child's durable initial input is ready.
pub(crate) struct OrdinaryCreationAdmission {
    response: Mutex<Option<tokio::sync::oneshot::Sender<Result<(), RuntimeString>>>>,
    slot: Mutex<Option<ResidencySlot>>,
    committed: AtomicBool,
    abort: CancellationToken,
    description: RuntimeString,
    persistent: bool,
    activation: Notify,
    publication: Mutex<Option<CreationPublication>>,
    owner_started: AtomicBool,
    observable: bool,
    cleanup_token: Mutex<Option<tokio_util::task::task_tracker::TaskTrackerToken>>,
}

struct CreationPublication {
    kind: SubagentKind,
    context: crate::engine::subagent_lifecycle::SubagentContext,
    model: RuntimeString,
    model_source: Option<RuntimeString>,
}

impl OrdinaryCreationAdmission {
    pub(crate) fn is_committed(&self) -> bool {
        self.committed.load(Ordering::Acquire)
    }

    pub(crate) async fn drive(&self, task: impl std::future::Future<Output = ()>) {
        tokio::pin!(task);
        tokio::select! {
            biased;
            _ = self.abort.cancelled() => {
                if self.is_committed() {
                    task.await;
                }
            },
            _ = &mut task => {},
        }
    }

    pub(crate) fn stage(
        &self,
        kind: SubagentKind,
        context: crate::engine::subagent_lifecycle::SubagentContext,
        model: RuntimeString,
        model_source: Option<RuntimeString>,
    ) {
        *self.publication.lock_recover() = Some(CreationPublication {
            kind,
            context,
            model,
            model_source,
        });
    }

    pub(crate) fn reject(&self, error: RuntimeString) {
        if let Some(response) = self.response.lock_recover().take() {
            let _ = response.send(Err(error));
        }
    }
}

pub(crate) struct PendingCreation {
    admission: Arc<OrdinaryCreationAdmission>,
    accepted: bool,
    session: std::sync::Weak<Session>,
    id: String,
}

impl PendingCreation {
    pub(crate) fn disarm(&mut self) {
        self.accepted = true;
    }
    pub(crate) async fn wait(
        mut self,
        accepted: tokio::sync::oneshot::Receiver<Result<(), RuntimeString>>,
    ) -> Result<(), RuntimeString> {
        tokio::select! {
            biased;
            result = accepted => result.map_err(|_| RuntimeString::from("Sub-agent initialization owner stopped."))??,
            _ = self.admission.abort.cancelled() => return Err("Sub-agent creation was cancelled.".into()),
        }
        let session = self
            .session
            .upgrade()
            .ok_or_else(|| RuntimeString::from("The parent session is shutting down."))?;
        session.persist_ordinary_creation_commit(&self.id).await?;
        session
            .commit_ordinary_creation(&self.id, &self.admission)
            .await?;
        self.accepted = true;
        self.admission.activation.notify_one();
        Ok(())
    }
}

impl Drop for PendingCreation {
    fn drop(&mut self) {
        if !self.accepted {
            self.admission.abort.cancel();
            self.admission.slot.lock_recover().take();
            if !self.admission.owner_started.load(Ordering::Acquire) {
                if let Some(session) = self.session.upgrade() {
                    session.retain_pending_ordinary_cleanup(self.id.clone(), &self.admission);
                }
            }
        }
    }
}

pub(crate) struct CreationOwner {
    session: Arc<Session>,
    id: String,
    admission: Arc<OrdinaryCreationAdmission>,
}

pub(crate) struct DirectSubagentTurn {
    session: Arc<Session>,
    id: String,
}

impl Drop for DirectSubagentTurn {
    fn drop(&mut self) {
        if let Some(worker) = self
            .session
            .ordinary_runtime()
            .state
            .lock_recover()
            .workers
            .get_mut(&self.id)
        {
            worker.running = false;
            worker
                .terminal_status
                .get_or_insert(SubagentStatus::Interrupted);
            worker.exited.notify_waiters();
        }
    }
}

impl Drop for CreationOwner {
    fn drop(&mut self) {
        if self.admission.committed.load(Ordering::Acquire) {
            if let Some(worker) = self
                .session
                .ordinary_runtime()
                .state
                .lock_recover()
                .workers
                .get_mut(&self.id)
            {
                worker.running = false;
                if worker.terminal_status.is_none() {
                    worker.terminal_status = Some(SubagentStatus::Interrupted);
                }
                worker.exited.notify_waiters();
            }
        }
        if !self.admission.committed.load(Ordering::Acquire) {
            self.admission
                .reject("Sub-agent initialization owner stopped before input acceptance.".into());
            self.admission.abort.cancel();
            self.admission.slot.lock_recover().take();
            self.session
                .retain_pending_ordinary_cleanup(self.id.clone(), &self.admission);
        }
    }
}

#[cfg(test)]
struct TestHandoffGate {
    entered: Arc<tokio::sync::Semaphore>,
    release: Arc<tokio::sync::Semaphore>,
    outcome: tokio::sync::oneshot::Sender<bool>,
}

struct MessageTurn {
    runtime: Arc<OrdinarySubagentRuntime>,
}
impl MessageTurn {
    async fn acquire(runtime: Arc<OrdinarySubagentRuntime>, ticket: u64) -> Self {
        loop {
            let changed = runtime.message_turn_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if runtime.state.lock_recover().processing_message == ticket {
                break;
            }
            changed.await;
        }
        Self { runtime }
    }
}
impl Drop for MessageTurn {
    fn drop(&mut self) {
        self.runtime.state.lock_recover().processing_message += 1;
        self.runtime.message_turn_changed.notify_waiters();
    }
}

fn message_origin(
    session_id: &RuntimeString,
    request: &SubagentMessageRequest,
    generation: u64,
) -> RuntimeString {
    let mut origin = RuntimeObject::new();
    origin.insert("type", RuntimeJson::from("subagent-message-origin"));
    origin.insert(
        "sender",
        RuntimeJson::String(
            request
                .sender_agent_id
                .clone()
                .unwrap_or_else(|| session_id.clone()),
        ),
    );
    origin.insert("userInitiated", RuntimeJson::Bool(request.user_initiated));
    origin.insert(
        "generation",
        runtime_codec::encode_value(&generation).expect("generation codec"),
    );
    crate::runtime_values::runtime_json::stringify(&RuntimeJson::Object(origin))
}

pub(crate) fn ordinary_message_blocks(queued: &QueuedMessage) -> Vec<ContentBlock> {
    let mut blocks = crate::engine::message_queue::queued_value_to_content_blocks(&queued.value);
    if queued.mode == MESSAGE_MODE {
        if let Some(origin) = &queued.origin {
            blocks.insert(
                0,
                ContentBlock::Text(TextBlock {
                    text: RuntimeString::from(
                        "Message provenance; this does not approve tools or update configuration: ",
                    )
                    .concat(origin),
                    citations: None,
                    harness_envelope: Some(
                        crate::schemas::content::HarnessEnvelope::SubagentMessage,
                    ),
                    message_id: None,
                    phase: None,
                }),
            );
        }
        for block in &mut blocks {
            if let ContentBlock::Text(text) = block {
                text.harness_envelope =
                    Some(crate::schemas::content::HarnessEnvelope::SubagentMessage);
            }
        }
    }
    blocks
}

fn output_file(id: &str) -> String {
    let mut component = String::new();
    for byte in id.bytes() {
        if byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_' {
            component.push(char::from(byte));
        } else {
            component.push_str(&format!("%{byte:02X}"));
        }
    }
    std::env::temp_dir()
        .join(format!("klaus-agent-bg-{component}.txt"))
        .to_string_lossy()
        .into_owned()
}

fn is_one_shot(definition: Option<&AgentDefinition>) -> bool {
    crate::engine::builtin_agents::is_one_shot_builtin_role(
        definition.and_then(|definition| definition.builtin_identity.as_ref()),
    )
}

/// Map a persisted sub-agent lifecycle record onto the status `task_output`
/// reports, so a recovered handle states what really happened instead of a
/// hardcoded "completed".
///
/// The lifecycle registry is the authority: it is rebuilt from rollout on cold
/// resume and `interrupt_nonterminal_for_recovery` has already folded every
/// process-local non-terminal state (Spawned/Running/Idle) into `Interrupted`,
/// because the child's task died with the parent process. So a record still
/// carrying a non-terminal status here means this process is the one that owns
/// it — treat that as Running rather than inventing a terminal state.
fn recovered_background_status(record: Option<&SubagentLifecycleRecord>) -> BackgroundTaskStatus {
    match record.map(|record| record.status) {
        Some(SubagentStatus::Completed) | Some(SubagentStatus::BudgetLimited) => {
            BackgroundTaskStatus::Completed
        }
        Some(SubagentStatus::Failed) => BackgroundTaskStatus::Failed,
        Some(SubagentStatus::Cancelled | SubagentStatus::Interrupted) => {
            BackgroundTaskStatus::Interrupted
        }
        // Still process-local: this session owns the live child.
        Some(SubagentStatus::Spawned | SubagentStatus::Running | SubagentStatus::Idle) | None => {
            BackgroundTaskStatus::Running
        }
    }
}

fn resolve_target(
    state: &OrdinaryState,
    target: &RuntimeString,
) -> Option<Result<String, RuntimeString>> {
    if let Ok(id) = target.try_to_utf8() {
        if state
            .workers
            .get(&id)
            .is_some_and(|worker| worker.committed)
        {
            return Some(Ok(id));
        }
    }
    let mut ids = state.names.get(target).cloned().unwrap_or_default();
    if target.as_utf16().len() >= 3 {
        for (id, worker) in &state.workers {
            if !worker.committed {
                continue;
            }
            if RuntimeString::from(id.clone())
                .as_utf16()
                .starts_with(target.as_utf16())
            {
                ids.insert(id.clone());
            }
        }
        for (name, bindings) in &state.names {
            if name.as_utf16().starts_with(target.as_utf16()) {
                ids.extend(bindings.iter().cloned());
            }
        }
    }
    match ids.len() {
        0 => None,
        1 => Some(Ok(ids.into_iter().next().expect("one candidate"))),
        _ => Some(Err(
            "Ambiguous sub-agent target; use an unambiguous agent ID.".into(),
        )),
    }
}

fn bind_name(state: &mut OrdinaryState, id: &str, name: Option<&RuntimeString>) {
    if let Some(name) = name.filter(|name| !name.is_empty()) {
        state
            .names
            .entry(name.clone())
            .or_default()
            .insert(id.to_owned());
    }
}

fn required<T: RuntimeDecode>(value: &RuntimeJson, key: &str) -> Result<T, RuntimeString> {
    let field = value.get(key).ok_or_else(|| {
        RuntimeString::from(format!("Missing sub-agent creation metadata: {key}"))
    })?;
    runtime_codec::decode_value(field).map_err(|error| {
        RuntimeString::from(format!(
            "Invalid sub-agent creation metadata {key}: {error}"
        ))
    })
}

impl Session {
    pub(crate) fn own_direct_subagent_turn(self: &Arc<Self>, id: &str) -> DirectSubagentTurn {
        if let Some(worker) = self
            .ordinary_runtime()
            .state
            .lock_recover()
            .workers
            .get_mut(id)
        {
            worker.running = true;
            worker.terminal_status = None;
        }
        DirectSubagentTurn {
            session: self.clone(),
            id: id.to_owned(),
        }
    }
    pub(crate) fn creation_owner(
        self: &Arc<Self>,
        id: &str,
        admission: &Arc<OrdinaryCreationAdmission>,
    ) -> CreationOwner {
        admission.owner_started.store(true, Ordering::Release);
        CreationOwner {
            session: self.clone(),
            id: id.to_owned(),
            admission: admission.clone(),
        }
    }

    #[cfg(test)]
    pub(crate) fn set_ordinary_creation_gate(
        &self,
        entered: Arc<Notify>,
        release: Arc<tokio::sync::Semaphore>,
    ) {
        *self.ordinary_runtime().creation_gate.lock_recover() = Some((entered, release));
    }

    #[cfg(test)]
    pub(crate) fn gate_ordinary_handoff(
        &self,
        entered: Arc<tokio::sync::Semaphore>,
        release: Arc<tokio::sync::Semaphore>,
        outcome: tokio::sync::oneshot::Sender<bool>,
    ) {
        *self.ordinary_runtime().handoff_gate.lock_recover() = Some(TestHandoffGate {
            entered,
            release,
            outcome,
        });
    }

    pub(crate) fn ordinary_is_one_shot(&self, agent_id: &RuntimeString) -> Option<bool> {
        let id = agent_id.try_to_utf8().ok()?;
        self.ordinary_runtime()
            .state
            .lock_recover()
            .workers
            .get(&id)
            .and_then(|worker| {
                worker
                    .snapshot
                    .as_ref()
                    .map(|snapshot| is_one_shot(snapshot.definition.as_ref()))
            })
    }

    pub(crate) fn ordinary_captured_approval_policy(
        &self,
        agent_id: &str,
    ) -> Option<crate::types::permission::ApprovalPolicy> {
        self.ordinary_runtime()
            .state
            .lock_recover()
            .workers
            .get(agent_id)
            .and_then(|worker| {
                worker.snapshot.as_ref().and_then(|snapshot| {
                    if worker.boundary_approval_policy
                        == Some(crate::types::permission::ApprovalPolicy::UnlessTrusted)
                    {
                        worker.boundary_approval_policy
                    } else {
                        snapshot.config.approval_policy
                    }
                })
            })
    }

    pub(crate) fn ordinary_live_approval_policy(
        &self,
        parent_agent_id: Option<&str>,
    ) -> crate::types::permission::ApprovalPolicy {
        let live = self
            .runtime_policy_snapshot()
            .state
            .approval_policy
            .unwrap_or_default();
        if parent_agent_id.and_then(|parent| self.ordinary_captured_approval_policy(parent))
            == Some(crate::types::permission::ApprovalPolicy::UnlessTrusted)
        {
            crate::types::permission::ApprovalPolicy::UnlessTrusted
        } else {
            live
        }
    }

    pub(crate) fn capture_ordinary_review_floor(&self, id: &str) -> Option<RuntimeJson> {
        let runtime = self.ordinary_runtime();
        let parent = runtime
            .state
            .lock_recover()
            .workers
            .get(id)?
            .bindings
            .parent_agent_id
            .clone();
        let floor = (self.ordinary_live_approval_policy(parent.as_deref())
            == crate::types::permission::ApprovalPolicy::UnlessTrusted)
            .then_some(crate::types::permission::ApprovalPolicy::UnlessTrusted);
        let mut state = runtime.state.lock_recover();
        let worker = state.workers.get_mut(id)?;
        let snapshot = worker.snapshot.as_mut()?;
        if floor.is_none() && worker.boundary_approval_policy.is_none() {
            return None;
        }
        if snapshot.boundary_approval_policy != floor {
            let mut updated = (**snapshot).clone();
            updated.boundary_approval_policy = floor;
            *snapshot = Arc::new(updated);
        }
        worker.boundary_approval_policy = floor;
        let mut evidence = crate::runtime_values::RuntimeObject::new();
        evidence.insert("version", RuntimeJson::Number(1_u32.into()));
        evidence.insert(
            "approvalPolicy",
            floor.map_or(RuntimeJson::Null, |_| {
                RuntimeJson::String("untrusted".into())
            }),
        );
        Some(RuntimeJson::Object(evidence))
    }

    fn ordinary_runtime(&self) -> Arc<OrdinarySubagentRuntime> {
        self.background_tasks.lock_recover().ordinary.clone()
    }

    #[cfg(test)]
    pub(crate) fn ordinary_capability_retention_for_testing(
        &self,
        id: &str,
    ) -> (
        Option<std::sync::Weak<SubagentCreationSnapshot>>,
        Option<SubagentCapabilityCapture>,
    ) {
        let runtime = self.ordinary_runtime();
        let state = runtime.state.lock_recover();
        let worker = state.workers.get(id).expect("known ordinary worker");
        (
            worker.snapshot.as_ref().map(Arc::downgrade),
            worker.evicted_capabilities.clone(),
        )
    }

    /// None is reserved for targets outside ordinary workers; known failures never fall through.
    pub async fn send_subagent_message(
        self: &Arc<Self>,
        request: SubagentMessageRequest,
    ) -> Option<Result<SubagentMessageResult, RuntimeString>> {
        self.send_subagent_message_in_context(request, None).await
    }

    pub(crate) async fn send_subagent_message_in_context(
        self: &Arc<Self>,
        mut request: SubagentMessageRequest,
        resume_context: Option<SubagentResumeContext>,
    ) -> Option<Result<SubagentMessageResult, RuntimeString>> {
        if let Err(error) = crate::schemas::subagent_message::validate_text_message(
            &request.target,
            &request.message,
            &request.summary,
        ) {
            return Some(Err(error));
        }
        let resume_context = resume_context
            .map(Ok)
            .unwrap_or_else(|| self.ordinary_resume_context(request.sender_agent_id.as_ref()));
        let (sender, receiver) = tokio::sync::oneshot::channel();
        {
            let mut admission = self.background_tasks.lock_recover();
            if admission.closing || self.lifecycle.is_disposed() {
                return Some(Err("The parent session is shutting down.".into()));
            }
            let runtime = admission.ordinary.clone();
            let (ticket, generation, sender_generation, cancellation_epoch) = {
                let mut state = runtime.state.lock_recover();
                if request.target == "parent" && !state.workers.contains_key("parent") {
                    let sender = request
                        .sender_agent_id
                        .as_ref()
                        .and_then(|sender| sender.try_to_utf8().ok())
                        .and_then(|sender| state.workers.get(&sender));
                    let Some(sender) =
                        sender.filter(|sender| !sender.user_cancelled && !request.user_initiated)
                    else {
                        return Some(Err(
                            "The parent recipient requires an active child sender.".into()
                        ));
                    };
                    request.target = sender
                        .bindings
                        .parent_agent_id
                        .clone()
                        .map(RuntimeString::from)
                        .unwrap_or_else(|| self.id.clone());
                }
                let ticket = state.next_message;
                state.next_message += 1;
                let generation = resolve_target(&state, &request.target)
                    .and_then(Result::ok)
                    .and_then(|id| {
                        state
                            .workers
                            .get(&id)
                            .map(|worker| worker.cancel_generation)
                    });
                let sender_generation = request
                    .sender_agent_id
                    .as_ref()
                    .and_then(|sender| sender.try_to_utf8().ok())
                    .and_then(|sender| state.workers.get(&sender))
                    .map(|worker| worker.cancel_generation);
                (
                    ticket,
                    generation,
                    sender_generation,
                    state.cancellation_epoch,
                )
            };
            let this = self.clone();
            admission.handles.push(tokio::spawn(async move {
                let _turn = MessageTurn::acquire(runtime, ticket).await;
                let result = this
                    .send_subagent_message_owned(
                        request,
                        generation,
                        sender_generation,
                        cancellation_epoch,
                        resume_context,
                    )
                    .await;
                let _ = sender.send(result);
            }));
        }
        receiver
            .await
            .unwrap_or_else(|_| Some(Err("The retained message owner failed.".into())))
    }

    async fn send_subagent_message_owned(
        self: &Arc<Self>,
        request: SubagentMessageRequest,
        accepted_generation: Option<u64>,
        accepted_sender_generation: Option<u64>,
        accepted_cancellation_epoch: u64,
        resume_context: Result<SubagentResumeContext, RuntimeString>,
    ) -> Option<Result<SubagentMessageResult, RuntimeString>> {
        let runtime = self.ordinary_runtime();
        if let Err(error) = self.hydrate_ordinary_workers(&runtime).await {
            // Failed discovery cannot prove that a name or prefix belongs to the team namespace.
            return Some(Err(error));
        }
        if request.target == self.id {
            if request.trigger_turn {
                return Some(Err("Follow-up tasks cannot target the root agent.".into()));
            }
            return Some(
                self.send_ordinary_root_message(
                    &runtime,
                    &request,
                    accepted_sender_generation,
                    accepted_cancellation_epoch,
                )
                .await,
            );
        }
        let pre_target = resolve_target(&runtime.state.lock_recover(), &request.target);
        let _residency_pin = if let Some(Ok(id)) = &pre_target {
            match self
                .ensure_ordinary_loaded_in_context(
                    id,
                    request.trigger_turn,
                    resume_context.as_ref().ok(),
                )
                .await
            {
                Ok(pin) => pin,
                Err(error) => {
                    return Some(Err(if error == MISSING_RESUME_CONTEXT {
                        resume_context.as_ref().err().cloned().unwrap_or(error)
                    } else {
                        error
                    }));
                }
            }
        } else {
            None
        };
        if let Some(Ok(id)) = &pre_target {
            if let Err(error) = self.await_cancel_durability(Some(id)).await {
                return Some(Err(error));
            }
            let needs_snapshot =
                runtime
                    .state
                    .lock_recover()
                    .workers
                    .get(id)
                    .is_some_and(|worker| {
                        worker.loaded.is_some()
                            && (!worker.running || worker.abort.is_cancelled())
                            && worker.snapshot.is_none()
                    });
            if needs_snapshot {
                let resume_context = match &resume_context {
                    Ok(context) => context,
                    Err(error) => return Some(Err(error.clone())),
                };
                if let Err(error) = self
                    .ordinary_snapshot_for_recovery_in_context(
                        id,
                        false,
                        request.user_initiated,
                        Some(resume_context),
                    )
                    .await
                {
                    return Some(Err(error));
                }
            }
        }
        let (id, resumed, mut launch, exited, stopping, message_generation, message_uuid) = {
            let mut state = runtime.state.lock_recover();
            let resolved = resolve_target(&state, &request.target);
            if resolved.is_none() {
                let known = self
                    .subagents
                    .lifecycle
                    .lock_recover()
                    .get(request.target.clone())
                    .is_some_and(|record| record.kind != SubagentKind::Persistent);
                return known.then_some(Err(
                    "Missing original sub-agent authority metadata; recovery is denied.".into(),
                ));
            }
            let id = match resolved.expect("known ordinary target") {
                Ok(id) => id,
                Err(error) => return Some(Err(error)),
            };
            if accepted_cancellation_epoch != state.cancellation_epoch {
                return Some(Err(
                    "A newer user cancellation superseded this recovery request.".into(),
                ));
            }
            if let Some(sender) = request
                .sender_agent_id
                .as_ref()
                .and_then(|sender| sender.try_to_utf8().ok())
                .and_then(|sender| state.workers.get(&sender))
            {
                if sender.user_cancelled
                    || accepted_sender_generation != Some(sender.cancel_generation)
                {
                    return Some(Err(
                        "A newer sender cancellation superseded this message.".into()
                    ));
                }
            }
            let worker = state.workers.get_mut(&id).expect("resolved worker");
            if accepted_generation.is_some_and(|generation| generation != worker.cancel_generation)
            {
                return Some(Err(
                    "A newer user cancellation superseded this recovery request.".into(),
                ));
            }
            if self.lifecycle.is_disposed() {
                return Some(Err("The parent session is shutting down.".into()));
            }
            if worker.user_cancelled && (!request.user_initiated || !request.trigger_turn) {
                return Some(Err(
                    "This sub-agent was cancelled by the user; explicit user recovery is required."
                        .into(),
                ));
            }
            let stopping = worker.running && worker.abort.is_cancelled();
            let resumed = request.trigger_turn && (!worker.running || stopping);
            if resumed {
                if worker.one_shot {
                    return Some(Err("This one-shot sub-agent cannot be resumed.".into()));
                }
                if let Some(error) = &worker.error {
                    return Some(Err(error.clone()));
                }
                if worker.snapshot.is_none() {
                    return Some(Err(
                        "The original sub-agent authority snapshot is unavailable.".into(),
                    ));
                }
            }
            // Sending and the terminal pending-message check use this same mutex.
            // Enqueue happens before any resumed worker can be published.
            let message_uuid = RuntimeString::from((self.deps.uuid)());
            let origin_generation = worker.cancel_generation
                + u64::from(worker.user_cancelled && request.user_initiated);
            let queued = QueuedMessage {
                trigger_turn: Some(request.trigger_turn),
                value: QueuedValue::Text(request.message.clone()),
                mode: MESSAGE_MODE.into(),
                uuid: Some(message_uuid.clone()),
                session_id: Some(self.id.clone()),
                priority: Some(QueuePriority::Next),
                is_meta: Some(true),
                origin: Some(message_origin(&self.id, &request, origin_generation)),
                agent_id: Some(id.clone().into()),
                correlation_id: None,
                delivery: Some(SteerDelivery::User),
                display_metadata: None,
                pdf_bytes: 0,
                pdf_documents: 0,
            };
            if let Err(error) = self.conversation.queue().enqueue(queued) {
                return Some(Err(error));
            }

            let launch = request.trigger_turn && !worker.running;
            if launch {
                // Message-driven continuation is detached even for an originally foreground child.
                worker.notify_completion = true;
                worker.running = true;
                worker.abort = CancellationToken::new();
            }
            (
                id,
                resumed,
                launch,
                worker.exited.clone(),
                stopping,
                worker.cancel_generation,
                message_uuid,
            )
        };
        if let Err(error) = self.conversation.queue().await_pending_durability().await {
            if launch {
                if let Some(worker) = runtime.state.lock_recover().workers.get_mut(&id) {
                    worker.running = false;
                    worker.exited.notify_waiters();
                }
            }
            return Some(Err(error));
        }
        if !request.trigger_turn {
            return Some(Ok(SubagentMessageResult {
                agent_id: id.into(),
                resumed: false,
            }));
        }
        let committed_generation = if request.user_initiated {
            self.commit_ordinary_user_recovery(&id, message_generation)
                .await
        } else {
            Ok(message_generation)
        };
        let launch_needed = committed_generation.and_then(|generation| {
            self.ensure_ordinary_message_owner(&id, &message_uuid, generation)
                .map(|needed| (generation, needed))
        });
        let (committed_generation, needed) = match launch_needed {
            Ok(value) => value,
            Err(error) => {
                if launch {
                    let mut state = runtime.state.lock_recover();
                    let worker = state.workers.get_mut(&id).expect("known worker");
                    worker.running = false;
                    worker.exited.notify_waiters();
                }
                return Some(Err(error));
            }
        };
        launch |= needed;
        if launch {
            if let Err(error) = self.launch_ordinary_continuation(&id) {
                let mut state = runtime.state.lock_recover();
                if let Some(worker) = state.workers.get_mut(&id) {
                    worker.running = false;
                    worker.exited.notify_waiters();
                }
                return Some(Err(error));
            }
        } else if stopping {
            // The current owner drains, flushes and tears down before starting the next run.
            loop {
                let notified = exited.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let stopped = {
                    let state = runtime.state.lock_recover();
                    let worker = state.workers.get(&id).expect("known worker");
                    !worker.running || !worker.abort.is_cancelled()
                };
                if stopped {
                    break;
                }
                notified.await;
            }
        }
        match self.ensure_ordinary_message_owner(&id, &message_uuid, committed_generation) {
            Ok(true) => {
                if let Err(error) = self.launch_ordinary_continuation(&id) {
                    let mut state = runtime.state.lock_recover();
                    let worker = state.workers.get_mut(&id).expect("known worker");
                    worker.running = false;
                    worker.exited.notify_waiters();
                    return Some(Err(error));
                }
            }
            Ok(false) => {}
            Err(error) => return Some(Err(error)),
        }
        Some(Ok(SubagentMessageResult {
            agent_id: id.into(),
            resumed,
        }))
    }

    async fn send_ordinary_root_message(
        &self,
        runtime: &OrdinarySubagentRuntime,
        request: &SubagentMessageRequest,
        accepted_sender_generation: Option<u64>,
        accepted_cancellation_epoch: u64,
    ) -> Result<SubagentMessageResult, RuntimeString> {
        if request.trigger_turn || request.user_initiated {
            return Err(
                "Messages to the primary agent are passive mail from an existing child.".into(),
            );
        }
        {
            let state = runtime.state.lock_recover();
            if accepted_cancellation_epoch != state.cancellation_epoch
                || self.lifecycle.is_disposed()
            {
                return Err("A newer cancellation superseded this message.".into());
            }
            let sender = request
                .sender_agent_id
                .as_ref()
                .and_then(|sender| sender.try_to_utf8().ok())
                .and_then(|sender| state.workers.get(&sender))
                .ok_or_else(|| {
                    RuntimeString::from(
                        "Messages to the primary agent require an existing child sender.",
                    )
                })?;
            if sender.user_cancelled || accepted_sender_generation != Some(sender.cancel_generation)
            {
                return Err("A newer cancellation superseded this message.".into());
            }
            self.conversation.queue().enqueue(QueuedMessage {
                trigger_turn: Some(false),
                value: QueuedValue::Text(request.message.clone()),
                mode: MESSAGE_MODE.into(),
                uuid: Some((self.deps.uuid)().into()),
                session_id: Some(self.id.clone()),
                priority: Some(QueuePriority::Next),
                is_meta: Some(true),
                origin: Some(message_origin(&self.id, request, sender.cancel_generation)),
                agent_id: None,
                correlation_id: None,
                delivery: Some(SteerDelivery::User),
                display_metadata: None,
                pdf_bytes: 0,
                pdf_documents: 0,
            })?;
        }
        self.conversation.queue().await_pending_durability().await?;
        Ok(SubagentMessageResult {
            agent_id: self.id.clone(),
            resumed: false,
        })
    }

    fn ensure_ordinary_message_owner(
        &self,
        id: &str,
        message_uuid: &RuntimeString,
        generation: u64,
    ) -> Result<bool, RuntimeString> {
        let runtime = self.ordinary_runtime();
        let mut state = runtime.state.lock_recover();
        let worker = state.workers.get_mut(id).expect("known worker");
        if worker.user_cancelled || worker.cancel_generation != generation {
            return Err("A newer user cancellation superseded this message.".into());
        }
        if let Some(error) = self.conversation.queue().persistence_error() {
            return Err(error);
        }
        let pending = self
            .conversation
            .queue()
            .admitted_snapshot()
            .iter()
            .any(|item| item.uuid.as_ref() == Some(message_uuid));
        if !worker.running && pending {
            worker.running = true;
            worker.abort = CancellationToken::new();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn commit_ordinary_user_recovery(
        &self,
        id: &str,
        expected: u64,
    ) -> Result<u64, RuntimeString> {
        let runtime = self.ordinary_runtime();
        // The input checkpoint is already durable before its trusted cancellation clear is written.
        let clear = {
            let mut admission = self.background_tasks.lock_recover();
            let mut state = runtime.state.lock_recover();
            let worker = state.workers.get_mut(id).expect("known worker");
            if worker.cancel_generation != expected {
                return Err("A newer user cancellation superseded this recovery request.".into());
            }
            if !worker.user_cancelled {
                return Ok(expected);
            }
            worker.cancel_generation += 1;
            let receipt = self.retain_cancel_flush(
                &mut admission,
                Some(id.to_owned()),
                Some(Self::user_cancellation_event(
                    id,
                    false,
                    worker.cancel_generation,
                )),
                None,
                Vec::new(),
            );
            worker.cancel_receipts.push(receipt.clone());
            (worker.cancel_generation, receipt)
        };
        clear.1.wait().await?;
        let mut state = runtime.state.lock_recover();
        let worker = state.workers.get_mut(id).expect("known worker");
        if worker.cancel_generation != clear.0 {
            return Err("A newer user cancellation superseded this recovery request.".into());
        }
        worker.user_cancelled = false;
        Ok(clear.0)
    }

    fn user_cancellation_event(id: &str, cancelled: bool, generation: u64) -> RolloutItem {
        let mut payload = RuntimeObject::new();
        payload.insert("agentId", RuntimeJson::String(id.into()));
        payload.insert("userCancelled", RuntimeJson::Bool(cancelled));
        payload.insert(
            "generation",
            runtime_codec::encode_value(&generation).expect("generation codec"),
        );
        RolloutItem::Event(crate::schemas::rollout::EventItem {
            event: CANCEL_EVENT.into(),
            turn_id: None,
            payload: Some(RuntimeJson::Object(payload)),
        })
    }

    pub(crate) fn stop_ordinary_subagent(&self, id: &RuntimeString, user: bool) -> Option<bool> {
        let id = id.try_to_utf8().ok()?;
        let mut admission = self.background_tasks.lock_recover();
        let runtime = admission.ordinary.clone();
        let mut state = runtime.state.lock_recover();
        let worker = state.workers.get_mut(&id)?;
        if !worker.running {
            return Some(false);
        }
        // Physical cancellation is independent of rollout backpressure.
        worker.abort.cancel();
        if admission.closing || self.lifecycle.is_disposed() {
            return Some(false);
        }
        let lifecycle = self.cancel_subagent_state(&id);
        if user {
            worker.user_cancelled = true;
            worker.cancel_generation += 1;
            let stale = self
                .conversation
                .queue()
                .admitted_snapshot()
                .into_iter()
                .filter(|message| {
                    message.agent_id.as_ref() == Some(&RuntimeString::from(id.clone()))
                        && message.mode == MESSAGE_MODE
                })
                .collect::<Vec<_>>();
            let receipt = self.retain_cancel_flush(
                &mut admission,
                Some(id.to_owned()),
                Some(Self::user_cancellation_event(
                    &id,
                    true,
                    worker.cancel_generation,
                )),
                lifecycle,
                stale,
            );
            worker.cancel_receipts.push(receipt);
        } else {
            let receipt = self.retain_cancel_flush(
                &mut admission,
                Some(id.to_owned()),
                None,
                lifecycle,
                Vec::new(),
            );
            worker.cancel_receipts.push(receipt);
        }
        drop(state);
        drop(admission);
        Some(true)
    }

    pub(crate) fn cancel_ordinary_runtimes_for_shutdown(&self) {
        let runtime = self.ordinary_runtime();
        for worker in runtime.state.lock_recover().workers.values() {
            worker.abort.cancel();
        }
    }

    fn retain_cancel_flush(
        &self,
        admission: &mut crate::engine::session::BackgroundRunRegistry,
        child_id: Option<String>,
        cancellation: Option<RolloutItem>,
        lifecycle: Option<SubagentLifecycleRecord>,
        stale: Vec<Arc<QueuedMessage>>,
    ) -> Arc<CancellationReceipt> {
        let receipt = Arc::new(CancellationReceipt {
            result: Mutex::new(None),
            completed: Notify::new(),
        });
        let owned = receipt.clone();
        let session = self.snapshot.resolve_self();
        let parent = self.persistence.rollout().cloned();
        let queue = self.conversation.queue().clone();
        // The cancellation operation owns producer leases, rather than a
        // writer clone that may already have retired with a completed turn.
        admission.handles.push(tokio::spawn(async move {
            let result = std::panic::AssertUnwindSafe(async {
                let session = session.ok_or_else(|| {
                    RuntimeString::from("Sub-agent cancellation owner is unavailable.")
                })?;
                let child_lease = session
                    .acquire_parent_subagent_rollout(child_id.as_deref())
                    .await?;
                let child = child_lease.as_ref().map(|lease| lease.writer());
                let lifecycle_parent = lifecycle.as_ref().and_then(|record| {
                    (record.context.depth > 1)
                        .then(|| record.context.parent_agent_id.clone())
                        .flatten()
                });
                let lifecycle_lease = session
                    .acquire_parent_subagent_rollout(
                        lifecycle_parent
                            .as_ref()
                            .and_then(|id| id.try_to_utf8().ok())
                            .as_deref(),
                    )
                    .await?;
                let lifecycle_rollout = if lifecycle_parent.is_some() {
                    lifecycle_lease.as_ref().map(|lease| lease.writer())
                } else {
                    parent.clone()
                };
                let mut writes: Vec<(Arc<dyn LiveRollout>, Vec<RolloutItem>)> = Vec::new();
                let mut add = |rollout: Arc<dyn LiveRollout>, item: RolloutItem| {
                    if let Some((_, items)) = writes
                        .iter_mut()
                        .find(|(writer, _)| Arc::ptr_eq(writer, &rollout))
                    {
                        items.push(item);
                    } else {
                        writes.push((rollout, vec![item]));
                    }
                };
                if let Some(event) = cancellation {
                    if let Some(child) = &child {
                        add(child.clone(), event.clone());
                    }
                    if let Some(parent) = &parent {
                        add(parent.clone(), event);
                    }
                }
                if let (Some(rollout), Some(record)) = (lifecycle_rollout, lifecycle) {
                    add(
                        rollout,
                        RolloutItem::SubagentLifecycle(
                            crate::schemas::rollout::SubagentLifecycleItem { record },
                        ),
                    );
                }
                for (writer, items) in &mut writes {
                    let prepared = writer
                        .prepare_batch(items.len())
                        .await
                        .map_err(|error| RuntimeString::from(error.to_string()))?;
                    prepared.commit(std::mem::take(items));
                }
                queue.release_pending_resume(&stale);
                if !stale.is_empty() {
                    queue
                        .remove_with_state_durable(&stale, QueueTerminalState::Interrupted)
                        .await?;
                }
                for (writer, _) in &writes {
                    writer
                        .flush()
                        .await
                        .map_err(|error| RuntimeString::from(error.to_string()))?;
                }
                if let Some(child) = child
                    .filter(|child| !writes.iter().any(|(writer, _)| Arc::ptr_eq(writer, child)))
                {
                    child
                        .flush()
                        .await
                        .map_err(|error| RuntimeString::from(error.to_string()))?;
                }
                Ok(())
            })
            .catch_unwind()
            .await
            .unwrap_or_else(|_| Err("Sub-agent cancellation persistence owner panicked.".into()));
            *owned.result.lock_recover() = Some(result);
            owned.completed.notify_waiters();
        }));
        receipt
    }

    pub(crate) async fn await_cancel_durability(
        &self,
        id: Option<&str>,
    ) -> Result<(), RuntimeString> {
        let runtime = self.ordinary_runtime();
        let mut receipts = {
            let state = runtime.state.lock_recover();
            state
                .workers
                .iter()
                .filter(|(key, _)| id.is_none_or(|id| id == key.as_str()))
                .flat_map(|(_, worker)| worker.cancel_receipts.iter().cloned())
                .collect::<Vec<_>>()
        };
        receipts.extend(
            runtime
                .external_cancel_receipts
                .lock_recover()
                .iter()
                .filter(|(key, _)| id.is_none_or(|id| id == key.as_str()))
                .map(|(_, receipt)| receipt.clone()),
        );
        settle_cancellation_receipts(receipts).await
    }

    pub(crate) async fn await_ordinary_cancellation_persistence(
        &self,
    ) -> Result<(), RuntimeString> {
        self.await_cancel_durability(None).await
    }

    pub(crate) fn retain_pending_ordinary_cleanup(
        self: &Arc<Self>,
        id: String,
        admission: &OrdinaryCreationAdmission,
    ) {
        let Some(token) = admission.cleanup_token.lock_recover().take() else {
            return;
        };
        let session = self.clone();
        let runtime = self.ordinary_runtime();
        // The creation guard registered this token before acquiring resources.
        // A late Drop transfers the same admitted cleanup, even after close.
        tokio::spawn(async move {
            let _cleanup = token;
            if std::panic::AssertUnwindSafe(session.cleanup_pending_ordinary(&id))
                .catch_unwind()
                .await
                .is_err()
            {
                runtime
                    .pending_cleanup_error
                    .lock_recover()
                    .get_or_insert_with(|| "Pending sub-agent cleanup owner panicked.".into());
            }
        });
    }

    pub(crate) async fn await_pending_ordinary_cleanup(&self) -> Result<(), RuntimeString> {
        let runtime = self.ordinary_runtime();
        // Creator owners must be fenced before final teardown. Tokens admitted
        // by their guards keep this wait pending through any late cleanup Drop.
        runtime.pending_cleanup.close();
        runtime.pending_cleanup.wait().await;
        let error = runtime.pending_cleanup_error.lock_recover().clone();
        error.map_or(Ok(()), Err)
    }

    fn note_pending_ordinary_cleanup_error(&self, error: RuntimeString) {
        self.ordinary_runtime()
            .pending_cleanup_error
            .lock_recover()
            .get_or_insert(error);
    }

    pub(crate) fn retain_external_subagent_cancellation(&self, id: &str) -> bool {
        let mut admission = self.background_tasks.lock_recover();
        if admission.closing || self.lifecycle.is_disposed() {
            return false;
        }
        let lifecycle = self.cancel_subagent_state(id);
        let receipt = self.retain_cancel_flush(&mut admission, None, None, lifecycle, Vec::new());
        admission
            .ordinary
            .external_cancel_receipts
            .lock_recover()
            .push((id.to_owned(), receipt));
        true
    }

    pub(crate) fn note_subagent_persistence_error(&self, id: &str, error: RuntimeString) {
        if let Some(worker) = self
            .ordinary_runtime()
            .state
            .lock_recover()
            .workers
            .get_mut(id)
        {
            worker.error = Some(error.clone());
        }
        if let Some(entry) = self.subagents.background_agents.lock_recover().get_mut(id) {
            entry.status = BackgroundTaskStatus::Failed;
            entry.partial = error.clone();
        }
        tracing::error!(agent_id = id, error = ?error, "sub-agent persistence failed");
    }

    pub(crate) fn retained_subagent_persistence_error(&self, id: &str) -> Option<RuntimeString> {
        self.ordinary_runtime()
            .state
            .lock_recover()
            .workers
            .get(id)
            .and_then(|worker| worker.error.clone())
    }

    fn restore_ordinary_lifecycle_records(&self, items: &[RolloutItem]) {
        let restored = crate::engine::subagent_lifecycle::SubagentLifecycleRegistry::restore_latest(
            items
                .iter()
                .filter_map(|item| match item {
                    RolloutItem::SubagentLifecycle(item) => Some(item.record.clone()),
                    _ => None,
                })
                .collect(),
        );
        let mut current = self.subagents.lifecycle.lock_recover();
        for record in restored.snapshot().agents {
            if current.get(&record.agent_id).is_some() {
                continue;
            }
            let id = record.agent_id.clone();
            let interrupted = !record.status.is_terminal();
            let _ = current.spawn(record);
            if interrupted {
                let _ = current.mark_interrupted(
                    id,
                    (self.deps.now)(),
                    crate::engine::subagent_lifecycle::RECOVERY_INTERRUPTED_REASON,
                );
            }
        }
    }

    /// Reuse the authoritative continuation read, including its successful empty
    /// cancellation set. Lifecycle state was already restored by construction.
    pub(crate) fn restore_ordinary_parent_seed(&self, items: &[RolloutItem]) {
        *self
            .ordinary_runtime()
            .parent_cancellation_seed
            .lock_recover() = Some(ordinary_parent_cancellations(items));
    }

    async fn hydrate_ordinary_workers(
        self: &Arc<Self>,
        runtime: &Arc<OrdinarySubagentRuntime>,
    ) -> Result<(), RuntimeString> {
        self.await_cancel_durability(None).await?;
        let _hydration = runtime.hydration.lock().await;
        let Some(store) = self.persistence.store() else {
            return Ok(());
        };
        let session_id = self
            .id
            .try_to_utf8()
            .map_err(|_| RuntimeString::from("Invalid session identity."))?;
        let seed = runtime.parent_cancellation_seed.lock_recover().clone();
        let (cancellation, cancellation_error) = match seed {
            Some(seed) => (seed, None),
            None => match store.read_thread(&session_id).await {
                Ok(parent) => {
                    self.restore_ordinary_lifecycle_records(&parent.raw_items);
                    let seed = ordinary_parent_cancellations(&parent.raw_items);
                    *runtime.parent_cancellation_seed.lock_recover() = Some(seed.clone());
                    (seed, None)
                }
                Err(error) => (
                    HashMap::new(),
                    Some(RuntimeString::from(format!(
                        "Parent cancellation provenance is unavailable: {error}"
                    ))),
                ),
            },
        };
        let ids = store
            .list_sub_agents(&session_id)
            .await
            .map_err(|error| RuntimeString::from(error.to_string()))?;
        let mut loaded_agents = Vec::new();
        for id in ids {
            let loaded = store.read_sub_agent(&session_id, &id).await;
            if let Ok(loaded) = &loaded {
                self.restore_ordinary_lifecycle_records(&loaded.raw_items);
            }
            loaded_agents.push((id, loaded));
        }
        // Descendant lifecycle rows belong to their parent's sidechain. Restore
        // all of them before publishing handles, regardless of directory order.
        for (id, loaded) in loaded_agents {
            if runtime
                .state
                .lock_recover()
                .workers
                .get(&id)
                .is_some_and(|worker| {
                    !worker.committed
                        || worker.snapshot.is_some()
                        || worker.creation_metadata.is_some()
                        || worker.error.is_some()
                })
            {
                continue;
            }
            let known_record = self
                .subagents
                .lifecycle
                .lock_recover()
                .get(id.as_str())
                .cloned();
            let known_kind = known_record.as_ref().map(|record| record.kind);
            if known_kind == Some(SubagentKind::Persistent) {
                continue;
            }
            let mut saved_name = None;
            let mut creation_metadata = None;
            let mut bindings = OrdinaryBindings {
                depth: 0,
                spawn_turn_id: None,
                parent_agent_id: None,
            };
            let mut bindings_restored = false;
            let mut cancel_generation = 0_u64;
            let mut boundary_approval_policy = None;
            let mut boundary_review_error = None;
            let (user_cancelled, error, persisted_partial) = match loaded {
                Ok(loaded) => {
                    let mut creation = None;
                    let mut creation_committed = false;
                    let mut cancelled = false;
                    // The saved history is the only record of what this agent got
                    // done before the restart. Project it once, before the event
                    // scan consumes the item stream.
                    let persisted_partial =
                        crate::tools::subagent_stream::project_persisted_transcript(
                            &crate::store::projection::project_for_prompt(&loaded.raw_items),
                            None,
                        );
                    for item in loaded.raw_items {
                        if let RolloutItem::Event(ev) = item {
                            if ev.event == SNAPSHOT_EVENT {
                                // Creation authority is first-write-wins.
                                if creation.is_none() {
                                    creation = ev.payload;
                                }
                            } else if ev.event == CREATION_COMMIT_EVENT {
                                creation_committed = ev
                                    .payload
                                    .as_ref()
                                    .and_then(|value| value.as_bool())
                                    .unwrap_or(false);
                            } else if ev.event == "turn_lifecycle" {
                                if let Some(value) = ev
                                    .payload
                                    .as_ref()
                                    .and_then(|payload| payload.get("canonicalReviewFloor"))
                                {
                                    match decode_boundary_review_floor(value) {
                                        Ok(floor) => boundary_approval_policy = floor,
                                        Err(error) => boundary_review_error = Some(error),
                                    }
                                }
                            } else if ev.event == CANCEL_EVENT {
                                if let Some(payload) = ev.payload {
                                    let generation = payload
                                        .get("generation")
                                        .and_then(RuntimeJson::as_u64)
                                        .unwrap_or(0);
                                    if generation >= cancel_generation {
                                        cancel_generation = generation;
                                        cancelled = payload
                                            .get("userCancelled")
                                            .and_then(RuntimeJson::as_bool)
                                            .unwrap_or(true);
                                    }
                                }
                            }
                        }
                    }
                    match creation {
                        Some(value) => {
                            if value.get("admissionPending").and_then(RuntimeJson::as_bool) == Some(true) && !creation_committed {
                                continue;
                            }
                            saved_name = required::<Option<RuntimeString>>(&value, "name").unwrap_or_default();
                            creation_metadata = Some(value.clone());
                            match snapshot::decode_bindings(&value) {
                                Ok(restored) => {
                                    bindings = restored;
                                    bindings_restored = true;
                                    // Discovery restores durable identity and history. Runtime
                                    // services are constructed only when this child is addressed.
                                    if let Some(error) = boundary_review_error {
                                        (cancelled, Some(error), RuntimeString::default())
                                    } else {
                                        (cancelled, None, persisted_partial)
                                    }
                                }
                                Err(error) => (cancelled, Some(error), RuntimeString::default()),
                            }
                        },
                        None if known_kind.is_none() => continue,
                        None => (cancelled, Some("Missing original sub-agent authority metadata; recovery is denied.".into()), RuntimeString::default()),
                    }
                }
                Err(_) if known_kind.is_some() => (false, None, RuntimeString::default()),
                Err(_) => continue,
            };
            let known_record = self
                .subagents
                .lifecycle
                .lock_recover()
                .get(id.as_str())
                .cloned()
                .or(known_record);
            // Only malformed or missing authority metadata is permanent; transport availability is retried.
            let name = saved_name;
            let builtin = creation_metadata.as_ref().and_then(|metadata| {
                required::<Option<RuntimeString>>(metadata, "builtinIdentity")
                    .ok()
                    .flatten()
            });
            let one_shot =
                crate::engine::builtin_agents::is_one_shot_builtin_role(builtin.as_ref());
            let mut state = runtime.state.lock_recover();
            if state.workers.get(&id).is_some_and(|worker| {
                worker.snapshot.is_some()
                    || worker.creation_metadata.is_some()
                    || worker.error.is_some()
            }) {
                continue;
            }
            bind_name(&mut state, &id, name.as_ref());
            state.workers.insert(
                id.clone(),
                OrdinaryWorker {
                    committed: true,
                    loaded: None,
                    locally_evicted: false,
                    evicted_capabilities: None,
                    observable: true,
                    terminal_status: None,
                    residency_config: None,

                    bindings: bindings.clone(),
                    snapshot: None,
                    boundary_approval_policy,
                    running: false,
                    user_cancelled: cancellation
                        .get(&id)
                        .filter(|(generation, _)| *generation >= cancel_generation)
                        .map(|(_, cancelled)| *cancelled)
                        .unwrap_or(user_cancelled),
                    one_shot,
                    abort: CancellationToken::new(),
                    exited: Arc::new(Notify::new()),
                    rollout: None,
                    error,
                    creation_metadata,
                    cancel_generation: cancellation
                        .get(&id)
                        .map(|(generation, _)| *generation)
                        .unwrap_or(0)
                        .max(cancel_generation),
                    cancel_receipts: Vec::new(),
                    notify_completion: true,
                    event_followers: Vec::new(),
                    worktree: None,
                },
            );
            // Incomplete authority is a refusal target, not a fabricated completion handle.
            if bindings_restored {
                // Publish the handle with the lifecycle record's REAL terminal
                // state, not a hardcoded "completed". A task the restart caught
                // mid-flight is Interrupted: its process is gone, it will never
                // finish, and claiming Completed would let `task_output` hand the
                // model a partial buffer as if it were the final answer. The
                // registry's non-terminal states were already folded to
                // Interrupted by `interrupt_nonterminal_for_recovery` during
                // construction, so this also catches anything still live here.
                let status = recovered_background_status(known_record.as_ref());
                let description = known_record
                    .as_ref()
                    .map(|record| record.context.description.clone())
                    .unwrap_or_default();
                self.publish_ordinary_handle(
                    &id,
                    CancellationToken::new(),
                    status,
                    description,
                    &bindings,
                );
                // The clean output file is written only on completion, so an
                // interrupted task has none. Reattach the partial work projected
                // from its saved transcript so `task_output` returns what the
                // agent actually did before the restart instead of empty.
                if !persisted_partial.is_empty() {
                    if let Some(entry) =
                        self.subagents.background_agents.lock_recover().get_mut(&id)
                    {
                        entry.partial = persisted_partial.clone();
                    }
                }
            }
        }
        cancellation_error.map_or(Ok(()), Err)
    }

    pub(crate) async fn ordinary_snapshot_for_resume(
        self: &Arc<Self>,
        id: &str,
    ) -> Result<Arc<SubagentCreationSnapshot>, RuntimeString> {
        self.ordinary_snapshot_for_recovery(id, false, false).await
    }

    /// Reload every persisted sub-agent of a cold-resumed session into live
    /// workers and repopulate the `task_output` handle table.
    ///
    /// A resumed session otherwise carries its lifecycle ledger (so the sidebar
    /// and `session/subagentLifecycle` can report the children) while the handle
    /// table stays empty, because hydration used to run only when the model
    /// addressed a specific child. The parent then could not read a background
    /// task it had spawned before the restart, even though the child's work was
    /// still on disk. Cold resume calls this once so the children are addressable
    /// again from the first turn.
    ///
    /// A reload failure is a warning, never a resume failure: one unreadable
    /// child transcript must not make the whole session unresumable, and the
    /// lazy hydration on the message path still retries that child later.
    pub(crate) async fn recover_ordinary_subagents_on_resume(self: &Arc<Self>) {
        if let Err(error) = self
            .hydrate_ordinary_workers(&self.ordinary_runtime())
            .await
        {
            tracing::warn!(
                session_id = %runtime_string_diagnostic(&self.id),
                reason = ?error,
                "sub-agent reload on resume failed; children stay lazily recoverable"
            );
        }
    }

    async fn ordinary_snapshot_for_recovery(
        self: &Arc<Self>,
        id: &str,
        allow_owned_continuation: bool,
        trusted_user: bool,
    ) -> Result<Arc<SubagentCreationSnapshot>, RuntimeString> {
        self.ordinary_snapshot_for_recovery_in_context(
            id,
            allow_owned_continuation,
            trusted_user,
            None,
        )
        .await
    }

    pub(crate) fn ordinary_resume_context(
        &self,
        sender: Option<&RuntimeString>,
    ) -> Result<SubagentResumeContext, RuntimeString> {
        if let Some(sender) = sender.filter(|sender| *sender != &self.id) {
            let runtime = self.ordinary_runtime();
            let state = runtime.state.lock_recover();
            let sender = sender
                .try_to_utf8()
                .map_err(|error| RuntimeString::from(error.to_string()))?;
            let snapshot = state
                .workers
                .get(&sender)
                .and_then(|worker| worker.snapshot.as_ref())
                .ok_or_else(|| {
                    RuntimeString::from("The sender's runtime configuration is unavailable.")
                })?;
            return Ok(SubagentResumeContext {
                mcp: snapshot.mcp_runtime.capture(&snapshot.tools),
                bundle: snapshot.bundle.clone(),
                permission: snapshot.permission.clone(),
                runtime_policy: snapshot.runtime_policy.clone(),
                cwd: snapshot.cwd.clone(),
                runtime_capabilities: snapshot.runtime_capabilities.clone(),
            });
        }
        let runtime_policy = self.runtime_policy_snapshot();
        Ok(SubagentResumeContext {
            mcp: self.current_subagent_mcp_capture(),
            bundle: self.inference_snapshot(),
            permission: self.permission_context_for_policy(&runtime_policy),
            runtime_policy,
            cwd: self.cwd_now(),
            runtime_capabilities: SubagentCapabilityCapture::capture(
                self.runtime_capability_snapshot_for_turn(&Default::default()),
            ),
        })
    }

    pub(crate) fn capture_subagent_resume_context(
        &self,
        sender: Option<&RuntimeString>,
        capture: Option<&Arc<crate::engine::session::InferenceActivityGuard>>,
        permission: &PermissionContext,
        runtime_capabilities: Option<&SubagentCapabilityCapture>,
    ) -> Result<SubagentResumeContext, RuntimeString> {
        let mut context = self.ordinary_resume_context(sender)?;
        if let Some(capture) = capture {
            context.bundle = capture.bundle.clone();
        }
        context.permission = permission.clone();
        if let Some(capabilities) = runtime_capabilities {
            context.runtime_capabilities = capabilities.clone();
        }
        Ok(context)
    }

    async fn ordinary_snapshot_for_recovery_in_context(
        self: &Arc<Self>,
        id: &str,
        allow_owned_continuation: bool,
        trusted_user: bool,
        resume_context: Option<&SubagentResumeContext>,
    ) -> Result<Arc<SubagentCreationSnapshot>, RuntimeString> {
        let runtime = self.ordinary_runtime();
        self.hydrate_ordinary_workers(&runtime).await?;
        self.await_cancel_durability(Some(id)).await?;
        let (snapshot, metadata, boundary_approval_policy, evicted_capabilities) = {
            let state = runtime.state.lock_recover();
            let worker = state
                .workers
                .get(id)
                .ok_or_else(|| RuntimeString::from("Missing original authority metadata."))?;
            if worker.user_cancelled && !trusted_user {
                return Err(
                    "This sub-agent was cancelled by the user; explicit user recovery is required."
                        .into(),
                );
            }
            if worker.one_shot && !allow_owned_continuation {
                return Err("This one-shot sub-agent cannot be resumed.".into());
            }
            if let Some(error) = &worker.error {
                return Err(error.clone());
            }
            (
                worker.snapshot.clone(),
                worker.creation_metadata.clone(),
                worker.boundary_approval_policy,
                worker.evicted_capabilities.clone(),
            )
        };
        if let Some(snapshot) = snapshot {
            return Ok(snapshot);
        }
        let metadata = metadata.ok_or_else(|| {
            RuntimeString::from("The original creation authority is unavailable.")
        })?;
        let default_context;
        let resume_context = match resume_context {
            Some(context) => context,
            None => {
                default_context = self.ordinary_resume_context(None)?;
                &default_context
            }
        };
        let mut snapshot = self
            .decode_ordinary_snapshot(&metadata, resume_context, evicted_capabilities.as_ref())
            .await?;
        snapshot.boundary_approval_policy = boundary_approval_policy;
        let snapshot = Arc::new(snapshot);
        if snapshot.one_shot && !allow_owned_continuation {
            return Err("This one-shot sub-agent cannot be resumed.".into());
        }
        if let Some(worker) = runtime.state.lock_recover().workers.get_mut(id) {
            worker.one_shot = snapshot.one_shot;
            worker.snapshot = Some(snapshot.clone());
        }
        Ok(snapshot)
    }

    pub(crate) async fn freeze_ordinary_snapshot(
        &self,
        id: &str,
        snapshot: SubagentCreationSnapshot,
        rollout: Option<Arc<dyn LiveRollout>>,
    ) -> Result<(), RuntimeString> {
        let runtime = self.ordinary_runtime();
        let parent = rollout
            .as_ref()
            .and_then(|_| self.persistence.rollout().cloned());
        let same_writer = matches!((&rollout, &parent), (Some(child), Some(parent)) if Arc::ptr_eq(child, parent));
        let child_admission = match &rollout {
            Some(writer) => Some(
                writer
                    .prepare_batch(if same_writer { 5 } else { 3 })
                    .await
                    .map_err(|error| RuntimeString::from(error.to_string()))?,
            ),
            None => None,
        };
        let parent_admission = match &parent {
            Some(writer) if !same_writer => Some(
                writer
                    .prepare_batch(2)
                    .await
                    .map_err(|error| RuntimeString::from(error.to_string()))?,
            ),
            _ => None,
        };
        let mut state = runtime.state.lock_recover();
        let Some(worker) = state.workers.get_mut(id) else {
            return Ok(());
        };
        let mut child_records = Vec::new();
        let mut parent_records = Vec::new();
        let upgrade_history = worker
            .creation_metadata
            .as_ref()
            .and_then(|value| value.get("version"))
            .and_then(|value| runtime_codec::decode_value::<u32>(value).ok())
            .is_some_and(|version| version < 6);
        if worker.snapshot.is_none() || upgrade_history {
            // A recovered worker keeps its validated creation authority. Only
            // its durable representation changes to own the inherited context.
            let snapshot = worker
                .snapshot
                .clone()
                .unwrap_or_else(|| Arc::new(snapshot));
            if snapshot.depth != worker.bindings.depth {
                return Err("The original sub-agent depth binding changed.".into());
            }
            if snapshot.parent_agent_id != worker.bindings.parent_agent_id {
                return Err("The original direct-parent binding changed.".into());
            }
            let mut metadata = snapshot.encode(&worker.bindings.spawn_turn_id)?;
            if !worker.committed {
                metadata
                    .as_object_mut()
                    .expect("creation metadata object")
                    .insert("admissionPending", RuntimeJson::Bool(true));
            }
            worker.creation_metadata = Some(metadata.clone());
            if rollout.is_some() {
                child_records.push(RolloutItem::Event(crate::schemas::rollout::EventItem {
                    event: SNAPSHOT_EVENT.into(),
                    turn_id: None,
                    payload: Some(metadata),
                }));
                let cancelled = Self::user_cancellation_event(
                    id,
                    worker.user_cancelled,
                    worker.cancel_generation,
                );
                child_records.push(cancelled.clone());
                if same_writer {
                    child_records.push(cancelled);
                } else if parent.is_some() {
                    parent_records.push(cancelled);
                }
            }
            worker.one_shot = snapshot.one_shot;
            worker.snapshot = Some(snapshot);
        }
        worker.rollout = rollout.clone();
        if rollout.is_some() {
            let cancelled =
                Self::user_cancellation_event(id, worker.user_cancelled, worker.cancel_generation);
            child_records.push(cancelled.clone());
            if same_writer {
                child_records.push(cancelled);
            } else if parent.is_some() {
                parent_records.push(cancelled);
            }
        }
        if let Some(admission) = child_admission {
            admission.commit(child_records);
        }
        if let Some(admission) = parent_admission {
            admission.commit(parent_records);
        }
        Ok(())
    }

    fn publish_ordinary_handle(
        &self,
        id: &str,
        abort: CancellationToken,
        status: BackgroundTaskStatus,
        description: RuntimeString,
        bindings: &OrdinaryBindings,
    ) {
        let mut entries = self.subagents.background_agents.lock_recover();
        if let Some(entry) = entries.get_mut(id) {
            entry.abort = abort;
            entry.status = status;
            entry.notified = false;
        } else {
            entries.insert(
                id.to_owned(),
                BackgroundAgentEntry {
                    parent_agent_id: bindings.parent_agent_id.clone().map(RuntimeString::from),
                    abort,
                    output_file: output_file(id),
                    status,
                    description,
                    partial: RuntimeString::default(),
                    teammate: false,
                    mailbox_live: false,
                    idle: false,
                    shutdown_requested: false,
                    on_idle: Vec::new(),
                    notified: false,
                    depth: bindings.depth,
                    spawn_turn_id: bindings.spawn_turn_id.clone(),
                },
            );
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn spawn_ordinary_subagent(
        self: &Arc<Self>,
        opts: SpawnSubAgentOptions,
        depth: u32,
        permission: PermissionContext,
        get_conversation: GetConversation,
        in_fork_child: bool,
        parent_agent_id: Option<String>,
        task_plan: Option<crate::tasks::task_store::TaskPlanBinding>,
        capture: Option<Arc<crate::engine::session::InferenceActivityGuard>>,
        initiating_turn_id: Option<RuntimeString>,
        permission_origin: Option<SubagentPermissionOrigin>,
        runtime_capabilities: Option<SubagentCapabilityCapture>,
        collaboration_mode: crate::planning::CollaborationMode,
    ) -> Result<SpawnBackgroundAgentResult, RuntimeString> {
        self.spawn_ordinary_worker(
            opts,
            depth,
            permission,
            get_conversation,
            in_fork_child,
            parent_agent_id,
            task_plan,
            None,
            capture,
            initiating_turn_id,
            permission_origin,
            runtime_capabilities,
            collaboration_mode,
            SubagentKind::Background,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn spawn_ordinary_foreground(
        self: &Arc<Self>,
        opts: SpawnSubAgentOptions,
        depth: u32,
        parent_signal: CancellationToken,
        permission: PermissionContext,
        get_conversation: GetConversation,
        in_fork_child: bool,
        parent_agent_id: Option<String>,
        task_plan: Option<crate::tasks::task_store::TaskPlanBinding>,
        capture: Option<Arc<crate::engine::session::InferenceActivityGuard>>,
        initiating_turn_id: Option<RuntimeString>,
        permission_origin: Option<SubagentPermissionOrigin>,
        runtime_capabilities: Option<SubagentCapabilityCapture>,
        collaboration_mode: crate::planning::CollaborationMode,
        lifecycle_kind: SubagentKind,
    ) -> Result<crate::types::tool::ForegroundSubAgentRun, RuntimeString> {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let observation = parent_signal.child_token();
        let creation = self.spawn_ordinary_worker(
            opts,
            depth,
            permission,
            get_conversation,
            in_fork_child,
            parent_agent_id,
            task_plan,
            Some((observation.clone(), sender, capture)),
            None,
            initiating_turn_id,
            permission_origin,
            runtime_capabilities,
            collaboration_mode,
            lifecycle_kind,
        );
        let result = tokio::select! {
            biased;
            _ = parent_signal.cancelled() => return Err("Sub-agent creation was cancelled.".into()),
            result = creation => result?,
        };
        Ok(crate::types::tool::ForegroundSubAgentRun {
            agent_id: RuntimeJson::String(result.agent_id),
            events: Self::subagent_observation_stream(receiver, observation),
        })
    }

    pub(in crate::engine::session) fn subagent_observation_stream(
        mut receiver: tokio::sync::mpsc::UnboundedReceiver<crate::types::events::EngineEvent>,
        observation: CancellationToken,
    ) -> crate::engine::session::EngineEventStream {
        let signal = observation.clone();
        let events = Box::pin(async_stream::stream! {
            loop {
                tokio::select! {
                    biased;
                    _ = signal.cancelled() => break,
                    event = receiver.recv() => match event {
                        Some(event) => yield event,
                        None => break,
                    },
                }
            }
        });
        Box::pin(OwnedForegroundEvents {
            events,
            observation,
        })
    }

    pub(in crate::engine::session) fn subagent_creation_is_committed(&self, id: &str) -> bool {
        self.ordinary_runtime()
            .state
            .lock_recover()
            .workers
            .get(id)
            .is_some_and(|worker| worker.committed)
    }

    async fn wait_ordinary_foreground(
        self: &Arc<Self>,
        follower: OrdinaryEventFollower,
        signal: CancellationToken,
    ) {
        let runtime = &follower.runtime;
        let id = &follower.id;
        let exited = {
            let state = runtime.state.lock_recover();
            let Some(worker) = state.workers.get(id) else {
                return;
            };
            worker.exited.clone()
        };
        loop {
            let notified = exited.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if runtime
                .state
                .lock_recover()
                .workers
                .get(id)
                .is_none_or(|worker| !worker.running)
            {
                break;
            }
            tokio::select! {
                _ = &mut notified => {},
                _ = signal.cancelled() => break,
                _ = follower.sender.closed() => break,
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn spawn_ordinary_worker(
        self: &Arc<Self>,
        opts: SpawnSubAgentOptions,
        depth: u32,
        permission: PermissionContext,
        get_conversation: GetConversation,
        in_fork_child: bool,
        parent_agent_id: Option<String>,
        task_plan: Option<crate::tasks::task_store::TaskPlanBinding>,
        foreground: Option<(
            CancellationToken,
            tokio::sync::mpsc::UnboundedSender<crate::types::events::EngineEvent>,
            Option<Arc<crate::engine::session::InferenceActivityGuard>>,
        )>,
        parent_capture: Option<Arc<crate::engine::session::InferenceActivityGuard>>,
        initiating_turn_id: Option<RuntimeString>,
        permission_origin: Option<SubagentPermissionOrigin>,
        runtime_capabilities: Option<SubagentCapabilityCapture>,
        collaboration_mode: crate::planning::CollaborationMode,
        lifecycle_kind: SubagentKind,
    ) -> Result<SpawnBackgroundAgentResult, RuntimeString> {
        let parent_capture = foreground
            .as_ref()
            .and_then(|(_, _, capture)| capture.clone())
            .or(parent_capture);
        let resume_context = if opts.resume.is_some() {
            let sender = parent_agent_id
                .as_ref()
                .map(|id| RuntimeString::from(id.clone()));
            Some(self.capture_subagent_resume_context(
                sender.as_ref(),
                parent_capture.as_ref(),
                &permission,
                runtime_capabilities.as_ref(),
            )?)
        } else {
            None
        };
        let initial_inference = if opts.resume.is_none() {
            Some(
                parent_capture
                    .map(Ok)
                    .unwrap_or_else(|| {
                        self.begin_inference_activity_for(opts.model.as_ref())
                            .map(Arc::new)
                    })
                    .map_err(RuntimeString::from)?,
            )
        } else {
            None
        };
        let id = opts
            .resume
            .as_ref()
            .and_then(|id| id.try_to_utf8().ok())
            .unwrap_or_else(|| (self.deps.uuid)());
        // Configuration and residency can await while the root goal changes.
        // Keep this lease, including a captured absent generation, through admission.
        let captured_goal_lease = opts.resume.is_none().then(|| {
            self.goal_runtime().map(|goal| {
                Arc::new(goal.register_descendant(id.clone(), parent_agent_id.as_deref()))
            })
        });
        let fork_conversation = if lifecycle_kind == SubagentKind::Workflow {
            None
        } else {
            crate::engine::session::fork_snapshot(self.config.fork, &opts, || get_conversation())
        };
        let mut preparation = RunSubAgentOpts {
            inference_capture: initial_inference.clone(),
            fork_conversation: fork_conversation.clone(),
            parent_agent_id: parent_agent_id.clone(),
            initiating_turn_id: initiating_turn_id.clone(),
            ..Default::default()
        };
        if opts.resume.is_none() {
            let signal = foreground
                .as_ref()
                .map(|(signal, _, _)| signal.clone())
                .unwrap_or_default();
            self.prepare_fresh_subagent(&opts, &mut preparation, &signal)
                .await?;
            self.governor
                .check_execution_capacity(None)
                .map_err(|error| RuntimeString::from(error.to_string()))?;
        }
        let slot = if let Some(inference) = &initial_inference {
            Some(
                self.reserve_subagent_residency(
                    parent_agent_id.as_deref(),
                    &inference.bundle.config,
                )
                .await?,
            )
        } else {
            None
        };
        let result = SpawnBackgroundAgentResult {
            agent_id: id.clone().into(),
            output_file: RuntimePath::from(output_file(&id)),
        };
        let bindings = OrdinaryBindings {
            depth: depth + 1,
            spawn_turn_id: initiating_turn_id.clone(),
            parent_agent_id: parent_agent_id.clone(),
        };
        let captured_permission_origin = opts.resume.is_none().then_some(permission_origin);
        if self.background_tasks.lock_recover().closing || self.lifecycle.is_disposed() {
            return Err("The parent session is shutting down.".into());
        }
        if opts.resume.is_some() {
            let request = SubagentMessageRequest {
                trigger_turn: true,
                target: id.clone().into(),
                summary: crate::schemas::subagent_message::message_preview(&opts.prompt),
                message: opts.prompt,
                sender_agent_id: parent_agent_id.map(RuntimeString::from),
                user_initiated: false,
            };
            if let Some((signal, sender, _)) = foreground {
                // Subscribe before sending so an immediately running target's
                // first event cannot be lost during creation acceptance.
                let runtime = self.ordinary_runtime();
                self.hydrate_ordinary_workers(&runtime).await?;
                let target = resolve_target(&runtime.state.lock_recover(), &request.target)
                    .ok_or_else(|| {
                        RuntimeString::from("Unknown ordinary sub-agent resume target.")
                    })??;
                runtime
                    .state
                    .lock_recover()
                    .workers
                    .get_mut(&target)
                    .expect("resolved worker")
                    .event_followers
                    .push(sender.clone());
                let follower = OrdinaryEventFollower {
                    runtime,
                    id: target,
                    sender,
                };
                match self
                    .send_subagent_message_in_context(request, resume_context)
                    .await
                {
                    Some(Ok(_)) => {}
                    Some(Err(error)) => return Err(error),
                    None => return Err("Unknown ordinary sub-agent resume target.".into()),
                }
                let this = self.clone();
                self.background_tasks
                    .lock_recover()
                    .handles
                    .push(tokio::spawn(async move {
                        this.wait_ordinary_foreground(follower, signal).await;
                    }));
            } else {
                match self
                    .send_subagent_message_in_context(request, resume_context)
                    .await
                {
                    Some(Ok(_)) => {}
                    Some(Err(error)) => return Err(error),
                    None => return Err("Unknown ordinary sub-agent resume target.".into()),
                }
            }
            return Ok(result);
        }
        let inference = initial_inference.expect("fresh spawn captures inference before admission");
        let runtime = self.ordinary_runtime();
        // Creation and observation may be cancelled by their caller. Once
        // accepted, this worker is owned only by its own lifetime token.
        let abort = CancellationToken::new();
        let event_sender = foreground.as_ref().map(|(_, sender, _)| sender.clone());
        let is_foreground = foreground.is_some();
        drop(foreground);
        let (response, accepted) = tokio::sync::oneshot::channel();
        let creation = Arc::new(OrdinaryCreationAdmission {
            response: Mutex::new(Some(response)),
            slot: Mutex::new(slot),
            committed: AtomicBool::new(false),
            abort: abort.clone(),
            description: opts.prompt.clone(),
            persistent: false,
            activation: Notify::new(),
            publication: Mutex::new(None),
            owner_started: AtomicBool::new(false),
            observable: opts.memory_dir.is_none(),
            cleanup_token: Mutex::new(Some(runtime.pending_cleanup.token())),
        });
        let pending = PendingCreation {
            admission: creation.clone(),
            accepted: false,
            session: Arc::downgrade(self),
            id: id.clone(),
        };
        {
            let mut state = runtime.state.lock_recover();
            state.workers.insert(
                id.clone(),
                OrdinaryWorker {
                    committed: false,
                    loaded: Some(Arc::new(tokio::sync::RwLock::new(()))),
                    locally_evicted: false,
                    evicted_capabilities: None,
                    observable: opts.memory_dir.is_none(),
                    terminal_status: None,
                    residency_config: Some(inference.bundle.config.clone()),

                    bindings: bindings.clone(),
                    snapshot: None,
                    boundary_approval_policy: None,
                    running: true,
                    user_cancelled: false,
                    one_shot: false,
                    abort: abort.clone(),
                    exited: Arc::new(Notify::new()),
                    rollout: None,
                    error: None,
                    creation_metadata: None,
                    cancel_generation: 0,
                    cancel_receipts: Vec::new(),
                    notify_completion: !is_foreground,
                    event_followers: Vec::new(),
                    worktree: None,
                },
            );
        }
        {
            let mut admission = self.background_tasks.lock_recover();
            if admission.closing || self.lifecycle.is_disposed() {
                drop(admission);
                return Err("The parent session is shutting down.".into());
            }
            let this = self.clone();
            let worker_id = id.clone();
            let creation = creation.clone();
            let owner = self.creation_owner(&id, &creation);
            admission.handles.push(tokio::spawn(async move {
                let _owner = owner;
                #[cfg(test)]
                {
                    let gate = this.ordinary_runtime().creation_gate.lock_recover().take();
                    if let Some((entered, release)) = gate {
                        entered.notify_one();
                        tokio::select! {
                            _ = creation.abort.cancelled() => {
                                creation.reject("Sub-agent preparation was cancelled.".into());
                                this.cleanup_pending_ordinary(&worker_id).await;
                                return;
                            },
                            permit = release.acquire() => { permit.expect("creation gate remains open").forget(); },
                        }
                    }
                }
                creation
                    .drive(this.clone().run_ordinary_subagent(
                        worker_id.clone(),
                        Some((
                            opts,
                            permission,
                            RunSubAgentOpts {
                                prepared: preparation.prepared,
                                creation_admission: Some(creation.clone()),
                                execution_checked: true,
                                collaboration_mode: Some(collaboration_mode),
                                inference_capture: Some(inference),
                                goal_lease: captured_goal_lease,
                                permission_origin: captured_permission_origin,
                                runtime_capabilities,
                                foreground_event_sender: event_sender,
                                task_plan,
                                fork_conversation,
                                in_fork_child: Some(in_fork_child),
                                lifecycle_kind: Some(lifecycle_kind),
                                parent_agent_id,
                                initiating_turn_id,
                                ..Default::default()
                            },
                        )),
                        depth + 1,
                    ))
                    .await;
                if !creation.committed.load(Ordering::Acquire) {
                    creation
                        .reject("Sub-agent initialization ended before input acceptance.".into());
                    this.cleanup_pending_ordinary(&worker_id).await;
                }
            }));
        }
        pending.wait(accepted).await?;
        Ok(result)
    }

    fn launch_ordinary_continuation(self: &Arc<Self>, id: &str) -> Result<(), RuntimeString> {
        let mut admission = self.background_tasks.lock_recover();
        if admission.closing || self.lifecycle.is_disposed() {
            return Err("The parent session is shutting down.".into());
        }
        {
            let state = admission.ordinary.state.lock_recover();
            let worker = state
                .workers
                .get(id)
                .ok_or_else(|| RuntimeString::from("Unknown ordinary sub-agent resume target."))?;
            if worker.user_cancelled || worker.abort.is_cancelled() {
                return Err("A newer cancellation superseded this continuation.".into());
            }
            self.publish_ordinary_handle(
                id,
                worker.abort.clone(),
                BackgroundTaskStatus::Running,
                RuntimeString::default(),
                &worker.bindings,
            );
        }
        let this = self.clone();
        let id = id.to_owned();
        admission.handles.push(tokio::spawn(async move {
            this.run_ordinary_subagent(id, None, 0).await;
        }));
        Ok(())
    }

    fn run_ordinary_subagent(
        self: Arc<Self>,
        id: String,
        mut first: Option<(SpawnSubAgentOptions, PermissionContext, RunSubAgentOpts)>,
        depth: u32,
    ) -> futures::future::BoxFuture<'static, ()> {
        Box::pin(async move {
            let runtime = self.ordinary_runtime();
            loop {
                let creation_admission = first
                    .as_ref()
                    .and_then(|(_, _, run)| run.creation_admission.clone());
                let collaboration_mode = first
                    .as_ref()
                    .and_then(|(_, _, run)| run.collaboration_mode)
                    .unwrap_or_else(|| self.planning.mode());
                let (abort, initiating_turn_id, parent_agent_id) = {
                    let state = runtime.state.lock_recover();
                    let worker = state.workers.get(&id).expect("published worker");
                    (
                        worker.abort.clone(),
                        worker.bindings.spawn_turn_id.clone(),
                        worker.bindings.parent_agent_id.clone(),
                    )
                };
                // Each run owns its partial output. Earlier completed answers
                // remain in history and cannot become this run's failed activity.
                if let Some(entry) = self.subagents.background_agents.lock_recover().get_mut(&id) {
                    entry.partial = RuntimeString::default();
                }
                let recovery_goal_lease = first.is_none().then(|| {
                    self.goal_runtime().map(|goal| {
                        Arc::new(goal.register_descendant(id.clone(), parent_agent_id.as_deref()))
                    })
                });
                let admission = if abort.is_cancelled() {
                    Err(RuntimeString::from("Sub-agent preparation was cancelled."))
                } else {
                    Ok(())
                };
                let prepared = match (admission, first.take()) {
                    (Err(error), first) => {
                        if let Some((_, _, run)) = first {
                            if let Some(sender) = &run.foreground_event_sender {
                                ordinary_foreground_failure(sender, error.clone());
                            }
                        }
                        Err(error)
                    }
                    (Ok(()), Some((opts, permission, run))) => Ok((opts, permission, run, depth)),
                    (Ok(()), None) => {
                        match self.ordinary_snapshot_for_recovery(&id, true, false).await {
                            Ok(snapshot) => match self.begin_inference_activity() {
                                Ok(inference) => {
                                    let capture =
                                        Arc::new(inference).with_bundle(snapshot.bundle.clone());
                                    Ok((
                                        SpawnSubAgentOptions {
                                            prompt: RuntimeString::default(),
                                            resume: Some(id.clone().into()),
                                            name: snapshot.name.clone(),
                                            agent_type: snapshot.agent_type.clone(),
                                            model: Some(snapshot.config.model.clone()),
                                            tools: Some(snapshot.tools.clone()),
                                            effort: snapshot.effort.clone(),
                                            ultra_mode: Some(snapshot.ultra_mode),
                                            output_schema: snapshot.output_schema.clone(),
                                            cwd: Some(snapshot.cwd.clone()),
                                            ..Default::default()
                                        },
                                        snapshot.permission.clone(),
                                        RunSubAgentOpts {
                                            execution_checked: true,
                                            residency_config: self.ordinary_residency_config(&id),
                                            collaboration_mode: Some(collaboration_mode),
                                            creation_snapshot: Some(snapshot.clone()),
                                            permission_origin: Some(
                                                snapshot.permission_origin.clone(),
                                            ),
                                            goal_lease: recovery_goal_lease,
                                            message_continuation: true,
                                            in_fork_child: Some(snapshot.in_fork_child),
                                            inference_capture: Some(capture),
                                            cwd_override: Some(snapshot.cwd.clone()),
                                            max_turns_override: snapshot.max_turns,
                                            parent_agent_id: snapshot.parent_agent_id.clone(),
                                            initiating_turn_id,
                                            task_plan: Some(snapshot.task_plan.fork()),
                                            lifecycle_kind: Some(SubagentKind::Background),
                                            ..Default::default()
                                        },
                                        snapshot.depth,
                                    ))
                                }
                                Err(error) => Err(error.into()),
                            },
                            Err(error) => Err(error),
                        }
                    }
                };
                // A direct child's lifetime owns its parent's receipt writer,
                // independently of whether that parent still has an active turn.
                #[cfg(any(test, feature = "testing"))]
                let diagnostic_lease = crate::goal::testing_phase::Span::start(
                    "child.parentLease",
                    &format!(
                        "thread={:?} worker={id} parent={parent_agent_id:?}",
                        self.id
                    ),
                );
                let parent_lease = self
                    .acquire_parent_subagent_rollout(parent_agent_id.as_deref())
                    .await;
                #[cfg(any(test, feature = "testing"))]
                diagnostic_lease.finish(if parent_lease.is_ok() { "ok" } else { "error" });
                let (prepared, _parent_rollout_lease) = match parent_lease {
                    Ok(lease) => (prepared, lease),
                    Err(error) => (Err(error), None),
                };
                let mut failure = None;
                let mut final_text = RuntimeString::default();
                let mut aborted = false;
                let mut terminal_seen = false;
                let mut run_started = false;
                match prepared {
                    Ok((opts, permission, run, depth)) => {
                        let event_sender = run.foreground_event_sender.clone();
                        let mut stream = self.run_sub_agent_isolated(
                            opts,
                            depth,
                            abort.clone(),
                            permission,
                            id.clone(),
                            run,
                        );
                        #[cfg(any(test, feature = "testing"))]
                        crate::goal::testing_phase::mark(
                            "child.stream.firstPoll",
                            &format!("thread={:?} worker={id}", self.id),
                        );
                        #[cfg(any(test, feature = "testing"))]
                        let mut diagnostic_first_event = true;
                        while let Some(event) = stream.next().await {
                            #[cfg(any(test, feature = "testing"))]
                            if diagnostic_first_event {
                                diagnostic_first_event = false;
                                crate::goal::testing_phase::mark(
                                    "child.stream.firstEvent",
                                    &format!(
                                        "thread={:?} worker={id} turn_started={}",
                                        self.id,
                                        matches!(
                                            &event,
                                            crate::types::events::EngineEvent::TurnStarted { .. }
                                        )
                                    ),
                                );
                            }
                            if matches!(
                                &event,
                                crate::types::events::EngineEvent::TurnStarted { .. }
                            ) {
                                run_started = true;
                            }
                            if let Some(item) = crate::tools::subagent_stream::terminal_item(&event)
                            {
                                if let Some(entry) =
                                    self.subagents.background_agents.lock_recover().get_mut(&id)
                                {
                                    entry.partial =
                                        crate::tools::subagent_stream::append_transcript(
                                            &entry.partial,
                                            item,
                                            None,
                                        );
                                }
                            }
                            if let crate::types::events::EngineEvent::ItemCompleted {
                                item: crate::types::events::ThreadItem::AgentMessage { text, .. },
                                ..
                            } = &event
                            {
                                final_text = text.clone();
                            }
                            if let crate::types::events::EngineEvent::TurnFailed { error, .. } =
                                &event
                            {
                                terminal_seen = true;
                                failure = Some(error.message.clone());
                                if let Some(admission) = &creation_admission {
                                    admission.reject(error.message.clone());
                                }
                            }
                            if let crate::types::events::EngineEvent::TurnCompleted {
                                stop_reason,
                                ..
                            } = &event
                            {
                                terminal_seen = true;
                                aborted = *stop_reason == TerminalReason::Aborted;
                                if failure.is_none() {
                                    failure =
                                        crate::tools::subagent_stream::background_terminal_failure(
                                            *stop_reason,
                                        );
                                }
                            }
                            let followers = runtime
                                .state
                                .lock_recover()
                                .workers
                                .get(&id)
                                .map(|worker| worker.event_followers.clone())
                                .unwrap_or_default();
                            for follower in followers {
                                match runtime_codec::encode_value(&event)
                                    .and_then(|value| runtime_codec::decode_value(&value))
                                {
                                    Ok(copy) => {
                                        let _ = follower.send(copy);
                                    }
                                    Err(error) => ordinary_foreground_failure(
                                        &follower,
                                        error.to_string().into(),
                                    ),
                                }
                            }
                            if let Some(sender) = &event_sender {
                                let _ = sender.send(event);
                            }
                        }
                    }
                    Err(error) => {
                        failure = Some(error);
                    }
                }
                if let Some(admission) = &creation_admission {
                    if !admission.committed.load(Ordering::Acquire) {
                        admission.reject(
                            failure.unwrap_or_else(|| {
                                "Sub-agent initialization was cancelled.".into()
                            }),
                        );
                        return;
                    }
                }
                if failure.is_none() && (aborted || abort.is_cancelled()) {
                    failure =
                        crate::tools::subagent_stream::terminal_failure(TerminalReason::Aborted);
                }
                if failure.is_none() && !terminal_seen {
                    failure = Some("Sub-agent stream ended without a terminal event.".into());
                }
                if let Err(error) = self.await_cancel_durability(Some(&id)).await {
                    failure = Some(error);
                }
                let leftover = self
                    .conversation
                    .queue()
                    .admitted_snapshot()
                    .into_iter()
                    .filter(|message| {
                        message.agent_id.as_ref() == Some(&RuntimeString::from(id.clone()))
                            && message.mode == MESSAGE_MODE
                    })
                    .collect::<Vec<_>>();
                self.conversation.queue().release_pending_resume(&leftover);
                if let Err(error) = self.conversation.queue().await_pending_durability().await {
                    failure = Some(error);
                }
                #[cfg(test)]
                let handoff_gate = runtime.handoff_gate.lock_recover().take();
                #[cfg(test)]
                if let Some(gate) = &handoff_gate {
                    gate.entered.add_permits(1);
                    gate.release
                        .acquire()
                        .await
                        .expect("handoff gate remains open")
                        .forget();
                }
                let partial = self
                    .subagents
                    .background_agents
                    .lock_recover()
                    .get(&id)
                    .map(|entry| entry.partial.clone())
                    .unwrap_or_default();
                let body = match &failure {
                    Some(error) => {
                        crate::tools::subagent_stream::incomplete_background_output(error, &partial)
                    }
                    None if final_text.is_empty() => "(no output)".into(),
                    None => final_text.clone(),
                };
                crate::engine::session::write_output_atomic(
                    &output_file(&id),
                    &crate::engine::session::runtime_background_output_to_file_host(&body),
                )
                .await;
                if failure.is_some() {
                    if let Some(entry) =
                        self.subagents.background_agents.lock_recover().get_mut(&id)
                    {
                        entry.partial = body.clone();
                    }
                }
                let (again, notice) = {
                    let mut state = runtime.state.lock_recover();
                    let worker = state.workers.get_mut(&id).expect("published worker");
                    worker.rollout = None;
                    let pending =
                        self.conversation
                            .queue()
                            .admitted_snapshot()
                            .iter()
                            .any(|message| {
                                message.mode == MESSAGE_MODE
                                    && message.triggers_turn()
                                    && message.agent_id.as_ref()
                                        == Some(&RuntimeString::from(id.clone()))
                            });
                    let again = pending
                        // Refused admission cannot immediately retry against the
                        // same full pool. Input accepted by a running child still
                        // gets its normal recovery handoff after that run exits.
                        && (failure.is_none() || run_started)
                        && self.conversation.queue().persistence_error().is_none()
                        && !worker.user_cancelled
                        && worker.snapshot.is_some()
                        && worker.error.is_none()
                        && !self.lifecycle.is_disposed();
                    let mut notice = None;
                    if again {
                        worker.abort = CancellationToken::new();
                        worker.notify_completion = true;
                        self.publish_ordinary_handle(
                            &id,
                            worker.abort.clone(),
                            BackgroundTaskStatus::Running,
                            RuntimeString::default(),
                            &worker.bindings,
                        );
                    } else {
                        worker.running = false;
                        self.publish_ordinary_handle(
                            &id,
                            worker.abort.clone(),
                            if aborted || abort.is_cancelled() {
                                BackgroundTaskStatus::Interrupted
                            } else if failure.is_some() {
                                BackgroundTaskStatus::Failed
                            } else {
                                BackgroundTaskStatus::Completed
                            },
                            RuntimeString::default(),
                            &worker.bindings,
                        );
                        if worker.notify_completion && !aborted && !abort.is_cancelled() {
                            notice = self.take_background_completion_notice(&id);
                        }
                    }
                    worker.exited.notify_waiters();
                    (again, notice)
                };
                #[cfg(test)]
                if let Some(gate) = handoff_gate {
                    let _ = gate.outcome.send(again);
                }
                if let Some((payload, status, spawn_turn_id)) = notice {
                    self.publish_background_completion_notice(&id, payload, status, spawn_turn_id)
                        .await;
                }
                if !again {
                    break;
                }
            }
        })
    }

    pub(crate) fn retain_ordinary_worktree(
        &self,
        id: &str,
        worktree: &mut Option<crate::engine::session::AgentWorktree>,
    ) -> bool {
        let runtime = self.ordinary_runtime();
        let mut state = runtime.state.lock_recover();
        let Some(worker) = state.workers.get_mut(id) else {
            return false;
        };
        if worker.one_shot {
            return false;
        }
        if worktree.is_some() {
            worker.worktree = worktree.take();
        }
        true
    }

    pub(crate) fn ordinary_retains_clients(&self, id: &str) -> bool {
        self.ordinary_runtime()
            .state
            .lock_recover()
            .workers
            .get(id)
            .is_some_and(|worker| !worker.one_shot && worker.snapshot.is_some())
    }

    pub(crate) async fn close_ordinary_worker_resources(&self) {
        let runtime = self.ordinary_runtime();
        let resources = {
            let mut state = runtime.state.lock_recover();
            state
                .workers
                .values_mut()
                .map(|worker| {
                    let clients = worker
                        .snapshot
                        .as_ref()
                        .map(|snapshot| snapshot.mcp_clients.clone())
                        .unwrap_or_default();
                    (clients, worker.worktree.take())
                })
                .collect::<Vec<_>>()
        };
        for (clients, worktree) in resources {
            for client in clients {
                client.close().await;
            }
            // A persisted worker still owns its cwd after the parent process exits.
            if self.persistence.store().is_none() {
                self.teardown_agent_isolation(&worktree).await;
            }
        }
    }

    /// The next run claims leftover messages only after the previous run has fully exited.
    pub(crate) async fn append_ordinary_continuation(
        &self,
        id: &str,
        history: &mut Vec<Message>,
        rollout: Option<&Arc<dyn LiveRollout>>,
    ) -> Result<(), RuntimeString> {
        let agent_id = RuntimeString::from(id);
        let queue = self.conversation.queue();
        let reserved = queue.reserve_agent_resume_durable(&agent_id).await?;
        let mut reservation = ResumeReservation {
            queue: queue.clone(),
            items: reserved.clone(),
            committed: false,
        };
        for queued in &reserved {
            let uuid = queued.uuid.clone().ok_or_else(|| {
                RuntimeString::from("A sub-agent message requires a durable identity.")
            })?;
            if history
                .iter()
                .any(|message| matches!(message, Message::User(user) if user.uuid == uuid))
            {
                continue;
            }
            if !matches!(&queued.value, QueuedValue::Text(_)) {
                return Err("Only validated text is supported for ordinary messages.".into());
            }
            let message = Message::User(UserMessage {
                question_answer: None,
                uuid,
                content: ordinary_message_blocks(queued),
                is_meta: Some(true),
            });
            if let Some(rollout) = rollout {
                rollout.append_message(message.clone()).await;
            }
            history.push(message);
        }
        if let Some(rollout) = rollout {
            rollout
                .flush()
                .await
                .map_err(|error| RuntimeString::from(error.to_string()))?;
        }
        queue
            .remove_with_state_durable(&reserved, QueueTerminalState::Drained)
            .await?;
        reservation.committed = true;
        Ok(())
    }
}

#[cfg(test)]
mod cancellation_receipt_tests {
    use super::*;

    #[tokio::test]
    async fn a_failed_receipt_does_not_fence_another_pending_cancellation_owner() {
        let failed = Arc::new(CancellationReceipt {
            result: Mutex::new(Some(Err("first cancellation failed".into()))),
            completed: Notify::new(),
        });
        let pending = Arc::new(CancellationReceipt {
            result: Mutex::new(None),
            completed: Notify::new(),
        });
        let mut barrier = Box::pin(settle_cancellation_receipts(vec![failed, pending.clone()]));
        assert!(futures::poll!(barrier.as_mut()).is_pending());
        *pending.result.lock_recover() = Some(Ok(()));
        pending.completed.notify_waiters();
        assert_eq!(barrier.await.unwrap_err(), "first cancellation failed");
    }
}
