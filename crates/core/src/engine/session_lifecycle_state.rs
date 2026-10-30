use super::AgentMemorySnapshotMergeDelivery;
use super::DreamNowCompletion;
use super::Session;
use crate::engine::query::ProviderCallAdmission;
use crate::engine::query::ProviderCallAdmissionReceipt;
use crate::engine::subagent_lifecycle::SubagentKind;
use crate::hooks::setup::SetupContextAdmissionRecord;
use crate::hooks::setup::SetupContextAdmissionStatus;
use crate::hooks::setup::SetupLifecycleSnapshot;
use crate::hooks::setup::SetupRunRecord;
use crate::hooks::setup::SetupRunResult;
use crate::hooks::setup::SetupRunStatus;
use crate::memory::agent_memory::AgentMemoryMergeChoice;
use crate::memory::maintenance::MemoryMaintenanceEvent;
use crate::memory::maintenance::MemoryMaintenanceEventSink;
use crate::memory::maintenance::MemoryMaintenanceRunKind;
use crate::memory::maintenance::MemoryMaintenanceStatus;
use crate::memory::snapshot_decision::AgentMemorySnapshotApplyError;
use crate::memory::snapshot_decision::AgentMemorySnapshotDecisionAudit;
use crate::memory::snapshot_decision::AgentMemorySnapshotDecisionRequest;
use crate::memory::snapshot_decision::AgentMemorySnapshotDecisionSource;
use crate::memory::snapshot_decision::AgentMemorySnapshotDecisionStatus;
use crate::runtime_values::RuntimePath;
use crate::runtime_values::RuntimeString;
use crate::schemas::message::Message;
use crate::sync_ext::MutexExt;
use crate::tasks::task_store::TaskListSubscription;
use crate::types::tool::BackgroundTaskStatus;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

#[path = "session_subagent_messages.rs"]
mod subagent_messages;
pub(crate) use subagent_messages::{
    ordinary_message_blocks, OrdinaryCreationAdmission, OrdinarySubagentRuntime,
    SubagentCollaborationOrigin, SubagentCreationSnapshot, SubagentResumeContext,
};

// ---------------------------------------------------------------------------
// Internal background-agent + isolation state
// ---------------------------------------------------------------------------

/// A background (run_in_background) sub-agent OR a persistent swarm teammate.
pub(crate) struct BackgroundAgentEntry {
    pub(crate) parent_agent_id: Option<RuntimeString>,
    pub(crate) abort: CancellationToken,
    pub(crate) output_file: String,
    pub(crate) status: BackgroundTaskStatus,
    pub(crate) description: RuntimeString,
    /// Transcript accumulated as the agent runs (incremental disk transcript).
    pub(crate) partial: RuntimeString,
    /// True for a persistent teammate (vs a one-shot background agent).
    pub(crate) teammate: bool,
    /// The persistent mailbox owner can outlive a failed inference turn.
    pub(crate) mailbox_live: bool,
    /// True while a persistent teammate is parked waiting for its next message.
    pub(crate) idle: bool,
    /// Ask a persistent teammate to exit after its current turn.
    pub(crate) shutdown_requested: bool,
    /// Callbacks fired when a teammate goes idle or terminates (wait-for-idle).
    pub(crate) on_idle: Vec<tokio::sync::oneshot::Sender<()>>,
    /// Set once the terminal completion notice has been enqueued (guards double-send).
    pub(crate) notified: bool,
    /// Tree depth of this child: 1 = spawned directly by this session, 2+ =
    /// nested. Nested children settle their lifecycle rows one hop down, in
    /// their direct parent's sidechain, and their terminal receipts never
    /// enter this session's ledger.
    pub(crate) depth: u32,
    /// The turn of this session that spawned the child. Terminal receipts
    /// settle into that turn even when the child finishes long after it
    /// closed; without it a late receipt would strand its row.
    pub(crate) spawn_turn_id: Option<RuntimeString>,
}

/// The git/hook fields of a spawned agent's worktree isolation.
pub(crate) enum AgentWorktree {
    Hook {
        /// The worktree path (the isolation cwd carries it; kept for the faithful model).
        #[allow(dead_code)]
        path: RuntimePath,
        notice: RuntimeString,
    },
    Git {
        path: RuntimePath,
        notice: RuntimeString,
        root: RuntimePath,
        branch: RuntimeString,
        head: Option<RuntimeString>,
    },
}

impl AgentWorktree {
    pub(crate) fn notice(&self) -> &RuntimeString {
        match self {
            AgentWorktree::Hook { notice, .. } | AgentWorktree::Git { notice, .. } => notice,
        }
    }
}

/// A spawned agent's resolved isolation: the cwd to run under + an optional
/// worktree handle to reclaim afterward.
pub(crate) struct AgentIsolation {
    pub(crate) cwd_override: Option<RuntimePath>,
    pub(crate) worktree: Option<AgentWorktree>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MaintenanceTaskKind {
    AutoMemory,
    AutoDream,
    ManualDream,
}

pub(crate) struct MaintenanceTask {
    pub(crate) run_id: RuntimeString,
    pub(crate) kind: MaintenanceTaskKind,
    pub(crate) cancel: CancellationToken,
    pub(crate) handle: tokio::task::JoinHandle<()>,
    pub(crate) manual_completion:
        Option<Arc<Mutex<Option<tokio::sync::oneshot::Sender<DreamNowCompletion>>>>>,
}

/// Owns one successful `try_begin_dream` transition.  The flag belongs to the
/// spawned owner, not to any particular success branch: dropping an unstarted,
/// cancelled, aborted, or panicking worker must make Dream admission reentrant.
pub(crate) struct DreamInProgressGuard {
    pub(crate) session: Weak<Session>,
    pub(crate) owner: u64,
}

impl DreamInProgressGuard {
    pub(crate) fn new(session: &Arc<Session>, owner: u64) -> Self {
        Self {
            session: Arc::downgrade(session),
            owner,
        }
    }
}

impl Drop for DreamInProgressGuard {
    fn drop(&mut self) {
        if let Some(session) = self.session.upgrade() {
            session.finish_dream_owner(self.owner);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum MaintenanceRunKey {
    AutoMemory(RuntimeString),
    AutoDream(RuntimeString),
}

impl MaintenanceRunKey {
    pub(crate) fn from_event(event: &MemoryMaintenanceEvent) -> Self {
        match event {
            MemoryMaintenanceEvent::AutoMemory { run } => Self::AutoMemory(run.run_id.clone()),
            MemoryMaintenanceEvent::AutoDream { run } => Self::AutoDream(run.dream_run_id.clone()),
        }
    }

    pub(crate) fn run_kind(&self) -> MemoryMaintenanceRunKind {
        match self {
            Self::AutoMemory(_) => MemoryMaintenanceRunKind::AutoMemory,
            Self::AutoDream(_) => MemoryMaintenanceRunKind::AutoDream,
        }
    }

    pub(crate) fn run_id(&self) -> RuntimeString {
        match self {
            Self::AutoMemory(run_id) | Self::AutoDream(run_id) => run_id.clone(),
        }
    }
}

pub(crate) fn memory_maintenance_event_status(
    event: &MemoryMaintenanceEvent,
) -> MemoryMaintenanceStatus {
    match event {
        MemoryMaintenanceEvent::AutoMemory { run } => run.status,
        MemoryMaintenanceEvent::AutoDream { run } => run.status,
    }
}

pub(crate) struct MaintenanceProvisional {
    pub(crate) owner_id: u64,
    pub(crate) event: MemoryMaintenanceEvent,
    pub(crate) cursor: Option<RuntimeString>,
}

pub(crate) struct MaintenanceDurabilityTaskControl {
    pub(crate) abort: tokio::task::AbortHandle,
}

pub(crate) type MaintenanceRecoveryReceipt =
    tokio::sync::watch::Receiver<Option<Result<(), RuntimeString>>>;

pub(crate) struct MaintenanceRegistry {
    pub(crate) closing: bool,
    pub(crate) tasks: Vec<MaintenanceTask>,
    pub(crate) durability_tracker: TaskTracker,
    pub(crate) durability_closed: bool,
    pub(crate) next_durability_task_id: u64,
    pub(crate) durability_tasks: HashMap<u64, MaintenanceDurabilityTaskControl>,
    pub(crate) provisionals: HashMap<MaintenanceRunKey, MaintenanceProvisional>,
    pub(crate) persistence_failed_runs: HashSet<MaintenanceRunKey>,
    pub(crate) pending_recovery: Vec<MemoryMaintenanceEvent>,
    /// Receipts for constructor-synthesized recovery terminals only. They stay
    /// addressable after settlement so concurrent/repeated resume barriers see
    /// the same verdict without ever joining a later live run.
    pub(crate) recovery_receipts: HashMap<MaintenanceRunKey, MaintenanceRecoveryReceipt>,
    pub(crate) failures: Vec<ShutdownPhaseFailure>,
}

impl Default for MaintenanceRegistry {
    fn default() -> Self {
        Self {
            closing: false,
            tasks: Vec::new(),
            durability_tracker: TaskTracker::new(),
            durability_closed: false,
            next_durability_task_id: 0,
            durability_tasks: HashMap::new(),
            provisionals: HashMap::new(),
            persistence_failed_runs: HashSet::new(),
            pending_recovery: Vec::new(),
            recovery_receipts: HashMap::new(),
            failures: Vec::new(),
        }
    }
}

pub(crate) struct MaintenanceDurabilityTaskRegistration {
    pub(crate) session: Weak<Session>,
    pub(crate) task_id: u64,
    pub(crate) key: MaintenanceRunKey,
}

impl Drop for MaintenanceDurabilityTaskRegistration {
    fn drop(&mut self) {
        let Some(session) = self.session.upgrade() else {
            return;
        };
        let Ok(mut registry) = session.maintenance_tasks.lock() else {
            session.install_memory_maintenance_query_fatal_direct(
                Some(&self.key),
                "memory maintenance durability owner ended while its registry was poisoned",
            );
            return;
        };
        registry.durability_tasks.remove(&self.task_id);
        let lost_provisional = registry
            .provisionals
            .get(&self.key)
            .is_some_and(|provisional| provisional.owner_id == self.task_id);
        if lost_provisional {
            registry.provisionals.remove(&self.key);
        }
        drop(registry);
        if lost_provisional {
            session.install_memory_maintenance_query_fatal(
                Some(&self.key),
                "memory maintenance durability owner ended before publishing a durable verdict",
            );
        }
    }
}

#[derive(Default)]
pub(crate) struct MaintenanceAdmissionState {
    pub(crate) closing: bool,
    pub(crate) active: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ShutdownPhaseFailure {
    pub(crate) message: String,
    pub(crate) timed_out: bool,
}

impl ShutdownPhaseFailure {
    pub(crate) fn error(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            timed_out: false,
        }
    }

    pub(crate) fn timeout(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            timed_out: true,
        }
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, pattern: &str) -> bool {
        self.message.contains(pattern)
    }
}

impl std::fmt::Display for ShutdownPhaseFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

pub(crate) type ShutdownPhaseResult = Result<(), ShutdownPhaseFailure>;

/// Typed core shutdown completion used by protocol adapters. Human-readable
/// details remain available without asking the wire layer to infer timeout
/// identity from English error text.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionShutdownOutcome {
    pub errors: Vec<String>,
    pub timed_out: bool,
}

impl SessionShutdownOutcome {
    pub fn ok(&self) -> bool {
        self.errors.is_empty()
    }

    pub fn error_message(&self) -> Option<String> {
        (!self.errors.is_empty()).then(|| self.errors.join("; "))
    }

    pub(crate) fn push_failure(&mut self, failure: ShutdownPhaseFailure) {
        self.timed_out |= failure.timed_out;
        self.errors.push(failure.message);
    }

    pub(crate) fn push_error(&mut self, error: impl Into<String>) {
        self.errors.push(error.into());
    }

    pub(crate) fn push_timeout(&mut self, error: impl Into<String>) {
        self.timed_out = true;
        self.errors.push(error.into());
    }

    pub(crate) fn into_result(self) -> Result<(), String> {
        match self.error_message() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

#[derive(Default)]
pub(crate) struct MaintenanceAdmissionTracker {
    pub(crate) state: Mutex<MaintenanceAdmissionState>,
    pub(crate) idle: tokio::sync::Notify,
}

impl MaintenanceAdmissionTracker {
    pub(crate) fn begin(self: &Arc<Self>) -> Option<MaintenanceAdmissionGuard> {
        let mut state = self.state.lock_recover();
        if state.closing {
            return None;
        }
        state.active = state
            .active
            .checked_add(1)
            .expect("maintenance admission count overflow");
        Some(MaintenanceAdmissionGuard {
            tracker: self.clone(),
        })
    }

    pub(crate) fn close(&self) {
        let mut state = self.state.lock_recover();
        state.closing = true;
        if state.active == 0 {
            self.idle.notify_waiters();
        }
    }

    pub(crate) async fn wait_for_idle(
        &self,
        deadline: tokio::time::Instant,
    ) -> ShutdownPhaseResult {
        let wait = async {
            loop {
                let notified = self.idle.notified();
                if self.state.lock_recover().active == 0 {
                    return;
                }
                notified.await;
            }
        };
        tokio::time::timeout_at(deadline, wait).await.map_err(|_| {
            ShutdownPhaseFailure::timeout(
                "memory maintenance admission timed out during the shared shutdown grace period",
            )
        })
    }
}

pub(crate) struct MaintenanceAdmissionGuard {
    pub(crate) tracker: Arc<MaintenanceAdmissionTracker>,
}

impl Drop for MaintenanceAdmissionGuard {
    fn drop(&mut self) {
        let mut state = self.tracker.state.lock_recover();
        state.active = state
            .active
            .checked_sub(1)
            .expect("maintenance admission guard underflow");
        if state.active == 0 {
            self.tracker.idle.notify_waiters();
        }
    }
}

/// Cancellation-safe Setup attempt. Dropping a correlated call while its hook
/// future is pending must terminalize the exact registered phase. Before an
/// external effect is admitted the request is safely retryable; afterwards its
/// outcome is unknowable and must fail closed against duplicate execution.
pub(crate) struct SetupAttemptGuard {
    pub(crate) session: Arc<Session>,
    pub(crate) request_id: RuntimeString,
    pub(crate) finished: bool,
}

/// The correlated SDK/protocol caller is not the Setup execution owner after
/// external-effect admission. Dropping only the waiter selects the conservative
/// first verdict; the Session-owned attempt retains the serial permit until it
/// either finishes or shutdown aborts and joins it.
pub(crate) struct SetupWaiterGuard {
    pub(crate) session: Arc<Session>,
    pub(crate) request_id: RuntimeString,
    pub(crate) finished: bool,
}

impl SetupWaiterGuard {
    pub(crate) fn complete(mut self) {
        self.finished = true;
    }
}

impl Drop for SetupWaiterGuard {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        self.session
            .settle_setup_attempt_after_owner_loss(&self.request_id);
    }
}

impl SetupAttemptGuard {
    pub(crate) fn complete(mut self) {
        self.finished = true;
    }
}

impl Drop for SetupAttemptGuard {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        self.session
            .settle_setup_attempt_after_owner_loss(&self.request_id);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SetupEffectPhase {
    /// The durable Started barrier has not yet admitted external hook effects.
    PreEffect,
    /// Hook dispatch has been admitted and may already have changed external
    /// state, even if the caller has not observed a result.
    EffectAdmitted,
}

#[derive(Clone, Debug)]
pub(crate) struct ActiveSetupAttempt {
    pub(crate) run: SetupRunRecord,
    pub(crate) phase: SetupEffectPhase,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub(crate) enum SetupOwnedTaskKind {
    Attempt,
    Durability,
    InitClose,
    ContextAdmission,
}

#[cfg(test)]
pub(crate) static SETUP_OWNED_TASK_PANIC_INJECTIONS: std::sync::LazyLock<
    Mutex<HashSet<(RuntimeString, SetupOwnedTaskKind)>>,
> = std::sync::LazyLock::new(|| Mutex::new(HashSet::new()));

#[cfg(test)]
pub(crate) fn inject_setup_owned_task_panic(request_id: RuntimeString, kind: SetupOwnedTaskKind) {
    SETUP_OWNED_TASK_PANIC_INJECTIONS
        .lock_recover()
        .insert((request_id, kind));
}

#[cfg(test)]
pub(crate) fn maybe_inject_setup_owned_task_panic(
    request_id: &RuntimeString,
    kind: SetupOwnedTaskKind,
) {
    if SETUP_OWNED_TASK_PANIC_INJECTIONS
        .lock_recover()
        .remove(&(request_id.clone(), kind))
    {
        panic!("injected Session-owned Setup {kind:?} task wrapper panic");
    }
}

pub(crate) struct SetupOwnedTaskControl {
    pub(crate) kind: SetupOwnedTaskKind,
    pub(crate) abort: tokio::task::AbortHandle,
}

pub(crate) struct SetupReceipt {
    pub(crate) started_send: tokio::sync::watch::Sender<Option<Result<(), RuntimeString>>>,
    pub(crate) started_receive: tokio::sync::watch::Receiver<Option<Result<(), RuntimeString>>>,
    pub(crate) changed: Arc<tokio::sync::Notify>,
    pub(crate) owner_registered: bool,
    pub(crate) started_cancel: crate::types::abort::AbortSignal,
    pub(crate) selected: Option<SetupRunResult>,
    pub(crate) terminal_record: Option<SetupRunRecord>,
    pub(crate) send: tokio::sync::watch::Sender<Option<SetupRunResult>>,
    pub(crate) receive: tokio::sync::watch::Receiver<Option<SetupRunResult>>,
}

impl SetupReceipt {
    pub(crate) fn pending() -> Self {
        let (send, receive) = tokio::sync::watch::channel(None);
        let (started_send, started_receive) = tokio::sync::watch::channel(None);
        Self {
            started_send,
            started_receive,
            changed: Arc::new(tokio::sync::Notify::new()),
            owner_registered: false,
            started_cancel: crate::types::abort::AbortSignal::new(),
            selected: None,
            terminal_record: None,
            send,
            receive,
        }
    }

    pub(crate) fn ready(result: SetupRunResult) -> Self {
        let (send, receive) = tokio::sync::watch::channel(Some(result.clone()));
        let (started_send, started_receive) = tokio::sync::watch::channel(Some(Ok(())));
        Self {
            started_send,
            started_receive,
            changed: Arc::new(tokio::sync::Notify::new()),
            owner_registered: true,
            started_cancel: crate::types::abort::AbortSignal::new(),
            selected: Some(result),
            terminal_record: None,
            send,
            receive,
        }
    }
}

pub(crate) struct SetupInitCloseReceipt {
    pub(crate) send: tokio::sync::watch::Sender<Option<Result<(), RuntimeString>>>,
    pub(crate) receive: tokio::sync::watch::Receiver<Option<Result<(), RuntimeString>>>,
}

impl SetupInitCloseReceipt {
    pub(crate) fn pending() -> Self {
        let (send, receive) = tokio::sync::watch::channel(None);
        Self { send, receive }
    }

    pub(crate) fn ready(result: Result<(), RuntimeString>) -> Self {
        let (send, receive) = tokio::sync::watch::channel(Some(result));
        Self { send, receive }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SetupContextProviderPhase {
    Prepared,
    Calling,
    Admitted,
    ReleaseRequested,
    OutcomeUnknownRequested,
}

#[derive(Clone)]
pub(crate) struct ActiveSetupContextAdmission {
    pub(crate) record: SetupContextAdmissionRecord,
    pub(crate) phase: SetupContextProviderPhase,
    pub(crate) changed: Arc<tokio::sync::Notify>,
}

pub(crate) struct SetupContextAdmissionReceipt {
    pub(crate) started_send: tokio::sync::watch::Sender<Option<Result<(), RuntimeString>>>,
    pub(crate) started_receive: tokio::sync::watch::Receiver<Option<Result<(), RuntimeString>>>,
    pub(crate) terminal_send: tokio::sync::watch::Sender<Option<Result<(), RuntimeString>>>,
    pub(crate) terminal_receive: tokio::sync::watch::Receiver<Option<Result<(), RuntimeString>>>,
    pub(crate) terminal_record: Option<SetupContextAdmissionRecord>,
    pub(crate) terminal_record_handle: Arc<Mutex<Option<SetupContextAdmissionRecord>>>,
    pub(crate) provider_lease_released: bool,
    pub(crate) owner_registered: bool,
}

impl SetupContextAdmissionReceipt {
    pub(crate) fn pending() -> Self {
        let (started_send, started_receive) = tokio::sync::watch::channel(None);
        let (terminal_send, terminal_receive) = tokio::sync::watch::channel(None);
        Self {
            started_send,
            started_receive,
            terminal_send,
            terminal_receive,
            terminal_record: None,
            terminal_record_handle: Arc::new(Mutex::new(None)),
            provider_lease_released: false,
            owner_registered: false,
        }
    }

    pub(crate) fn ready(record: SetupContextAdmissionRecord) -> Self {
        let (started_send, started_receive) = tokio::sync::watch::channel(Some(Ok(())));
        let terminal = if record.status == SetupContextAdmissionStatus::PersistenceFailed {
            Err(record
                .error
                .clone()
                .unwrap_or_else(|| "Setup context admission persistence failed".into()))
        } else {
            Ok(())
        };
        let (terminal_send, terminal_receive) = tokio::sync::watch::channel(Some(terminal));
        let terminal_record_handle = Arc::new(Mutex::new(Some(record.clone())));
        Self {
            started_send,
            started_receive,
            terminal_send,
            terminal_receive,
            terminal_record: Some(record),
            terminal_record_handle,
            provider_lease_released: true,
            owner_registered: false,
        }
    }

    pub(crate) fn set_terminal_record(&mut self, record: SetupContextAdmissionRecord) {
        *self.terminal_record_handle.lock_recover() = Some(record.clone());
        self.terminal_record = Some(record);
    }
}

pub(crate) struct SetupContextClaim {
    pub(crate) additional_context: Vec<RuntimeString>,
    pub(crate) admission: Arc<dyn ProviderCallAdmission>,
}

pub(crate) struct SetupContextProviderAdmission {
    pub(crate) session: Weak<Session>,
    pub(crate) admission_id: RuntimeString,
}

impl ProviderCallAdmission for SetupContextProviderAdmission {
    fn before_call(&self) -> Result<(), RuntimeString> {
        let session = self
            .session
            .upgrade()
            .ok_or_else(|| RuntimeString::from("Setup context Session owner was dropped"))?;
        session.setup_context_before_provider_call(&self.admission_id)
    }

    fn call_returned(&self) -> ProviderCallAdmissionReceipt {
        let Some(session) = self.session.upgrade() else {
            return Box::pin(async {
                Err(RuntimeString::from(
                    "Setup context Session owner was dropped after provider admission",
                ))
            });
        };
        session.setup_context_provider_call_returned(&self.admission_id)
    }

    fn call_panicked(&self) -> ProviderCallAdmissionReceipt {
        let Some(session) = self.session.upgrade() else {
            return Box::pin(async {
                Err(RuntimeString::from(
                    "Setup context Session owner was dropped after provider factory panic",
                ))
            });
        };
        session.setup_context_provider_call_panicked(&self.admission_id)
    }
}

impl Drop for SetupContextProviderAdmission {
    fn drop(&mut self) {
        if let Some(session) = self.session.upgrade() {
            session.setup_context_provider_lease_dropped(&self.admission_id);
        }
    }
}

pub(crate) struct SetupDurabilityRegistry {
    /// Closing is the Setup admission boundary. It is set before shutdown
    /// waits for `setup_serial`, then every active attempt is force-settled in
    /// the same critical section.
    pub(crate) closing: bool,
    pub(crate) active: HashMap<RuntimeString, ActiveSetupAttempt>,
    pub(crate) receipts: HashMap<RuntimeString, SetupReceipt>,
    /// Process-local durability verdict for the first-run InitClosed marker.
    /// Replayed closed state starts ready; a live close remains query-hidden
    /// and blocks provider admission until its retained flush owner settles.
    pub(crate) init_close_receipt: Option<SetupInitCloseReceipt>,
    pub(crate) active_context_admission: Option<ActiveSetupContextAdmission>,
    pub(crate) context_admission_receipts: HashMap<RuntimeString, SetupContextAdmissionReceipt>,
    pub(crate) context_recovery_receipt_ids: Vec<RuntimeString>,
    pub(crate) task_tracker: TaskTracker,
    pub(crate) next_task_id: u64,
    pub(crate) tasks: HashMap<u64, SetupOwnedTaskControl>,
    /// Request ids whose terminal records were synthesized while restoring the
    /// constructor input. This immutable set is the resume barrier boundary:
    /// later live Setup runs are deliberately never added to it.
    pub(crate) recovery_receipt_ids: Vec<RuntimeString>,
    pub(crate) failures: Vec<String>,
}

impl Default for SetupDurabilityRegistry {
    fn default() -> Self {
        Self {
            closing: false,
            active: HashMap::new(),
            receipts: HashMap::new(),
            init_close_receipt: None,
            active_context_admission: None,
            context_admission_receipts: HashMap::new(),
            context_recovery_receipt_ids: Vec::new(),
            task_tracker: TaskTracker::new(),
            next_task_id: 0,
            tasks: HashMap::new(),
            recovery_receipt_ids: Vec::new(),
            failures: Vec::new(),
        }
    }
}

impl SetupDurabilityRegistry {
    /// Replayed terminal records have already crossed the previous process's
    /// durability boundary. Give them completed process-local receipts so a
    /// replay never falls through a missing-receipt success path.
    pub(crate) fn from_lifecycle(lifecycle: &SetupLifecycleSnapshot) -> Self {
        let mut registry = Self::default();
        if lifecycle.init_closed_at.is_some() {
            registry.init_close_receipt = Some(SetupInitCloseReceipt::ready(Ok(())));
        }
        if let Some(admission) = lifecycle.context_admission.clone() {
            if admission.status != SetupContextAdmissionStatus::Started {
                registry.context_admission_receipts.insert(
                    admission.admission_id.clone(),
                    SetupContextAdmissionReceipt::ready(admission),
                );
            }
        }
        for run in &lifecycle.runs {
            if run.status == SetupRunStatus::Started {
                continue;
            }
            let result = SetupRunResult::from_record(run);
            registry
                .receipts
                .insert(run.request_id.clone(), SetupReceipt::ready(result));
        }
        for result in lifecycle.idempotency_results.values() {
            registry.receipts.insert(
                result.request_id.clone(),
                SetupReceipt::ready(result.clone()),
            );
        }
        registry
    }
}

/// Removes completed/aborted task controls without retaining completed
/// JoinHandles forever. TaskTracker remains the cancellation-safe join owner.
pub(crate) struct SetupOwnedTaskRegistration {
    pub(crate) session: Weak<Session>,
    pub(crate) task_id: u64,
}

impl Drop for SetupOwnedTaskRegistration {
    fn drop(&mut self) {
        let Some(session) = self.session.upgrade() else {
            return;
        };
        if let Ok(mut registry) = session.setup.durability.lock() {
            registry.tasks.remove(&self.task_id);
        };
    }
}

pub(crate) enum TaskListSubscriptionReaper {
    Tokio(tokio::task::JoinHandle<Result<(), String>>),
    Thread {
        handle: std::thread::JoinHandle<()>,
        completion: tokio::sync::watch::Receiver<Option<Result<(), String>>>,
    },
}

#[derive(Default)]
pub(crate) struct TaskListSubscriptionRegistry {
    pub(crate) closing: bool,
    pub(crate) next_id: u64,
    pub(crate) active: HashMap<u64, Arc<TaskListSubscription>>,
    pub(crate) retiring_tx: Option<tokio::sync::mpsc::UnboundedSender<Arc<TaskListSubscription>>>,
    pub(crate) reaper: Option<TaskListSubscriptionReaper>,
}

pub(crate) async fn reap_task_list_subscriptions(
    mut retiring: tokio::sync::mpsc::UnboundedReceiver<Arc<TaskListSubscription>>,
) -> Result<(), String> {
    let mut errors = Vec::new();
    while let Some(subscription) = retiring.recv().await {
        if let Err(error) = subscription.close_and_wait().await {
            errors.push(error.to_string());
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

#[derive(Default)]
pub(crate) struct BackgroundRunRegistry {
    pub(crate) ordinary: Arc<OrdinarySubagentRuntime>,
    pub(crate) closing: bool,
    pub(crate) handles: Vec<tokio::task::JoinHandle<()>>,
}

pub(crate) type SnapshotDurabilityReceipt =
    tokio::sync::watch::Receiver<Option<Result<(), RuntimeString>>>;
pub(crate) type SnapshotOrderedReply =
    Box<dyn FnOnce(AgentMemorySnapshotDecisionAudit) + Send + 'static>;

pub(crate) enum SnapshotDurabilityOperation {
    Register {
        request: AgentMemorySnapshotDecisionRequest,
    },
    Resolve {
        request_id: RuntimeString,
        decision: AgentMemoryMergeChoice,
        source: AgentMemorySnapshotDecisionSource,
        ordered_reply: Option<SnapshotOrderedReply>,
    },
    Terminal {
        request_id: RuntimeString,
        expected: AgentMemorySnapshotDecisionStatus,
        status: AgentMemorySnapshotDecisionStatus,
        error: Option<RuntimeString>,
    },
    Apply {
        request_id: RuntimeString,
    },
    RecoverMarked {
        request_id: RuntimeString,
    },
    SettleUnavailable {
        request_id: RuntimeString,
        status: AgentMemorySnapshotDecisionStatus,
        error: Option<RuntimeString>,
        without_consumer: bool,
        ordered_reply: Option<SnapshotOrderedReply>,
    },
    ConstructorRecovery {
        audit: AgentMemorySnapshotDecisionAudit,
    },
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum SnapshotDurabilityOperationKind {
    Register,
    Resolve,
    Terminal,
    Apply,
    RecoverMarked,
    SettleUnavailable,
    ConstructorRecovery,
}

impl SnapshotDurabilityOperation {
    pub(crate) fn request_id(&self) -> &RuntimeString {
        match self {
            Self::Register { request } => &request.request_id,
            Self::Resolve { request_id, .. }
            | Self::Terminal { request_id, .. }
            | Self::Apply { request_id }
            | Self::RecoverMarked { request_id }
            | Self::SettleUnavailable { request_id, .. } => request_id,
            Self::ConstructorRecovery { audit } => &audit.request.request_id,
        }
    }

    pub(crate) fn kind(&self) -> SnapshotDurabilityOperationKind {
        match self {
            Self::Register { .. } => SnapshotDurabilityOperationKind::Register,
            Self::Resolve { .. } => SnapshotDurabilityOperationKind::Resolve,
            Self::Terminal { .. } => SnapshotDurabilityOperationKind::Terminal,
            Self::Apply { .. } => SnapshotDurabilityOperationKind::Apply,
            Self::RecoverMarked { .. } => SnapshotDurabilityOperationKind::RecoverMarked,
            Self::SettleUnavailable { .. } => SnapshotDurabilityOperationKind::SettleUnavailable,
            Self::ConstructorRecovery { .. } => {
                SnapshotDurabilityOperationKind::ConstructorRecovery
            }
        }
    }
}

pub(crate) enum SnapshotDurabilityResult {
    Register(Result<AgentMemorySnapshotDecisionAudit, AgentMemorySnapshotApplyError>),
    Resolve(
        Result<
            (
                AgentMemorySnapshotDecisionAudit,
                Option<AgentMemorySnapshotMergeDelivery>,
            ),
            AgentMemorySnapshotApplyError,
        >,
    ),
    Terminal(Result<AgentMemorySnapshotDecisionAudit, AgentMemorySnapshotApplyError>),
    Apply(Result<AgentMemorySnapshotDecisionAudit, AgentMemorySnapshotApplyError>),
    RecoverMarked(Result<Option<AgentMemorySnapshotDecisionAudit>, AgentMemorySnapshotApplyError>),
    SettleUnavailable(
        Result<
            (
                AgentMemorySnapshotDecisionAudit,
                Option<AgentMemorySnapshotMergeDelivery>,
            ),
            AgentMemorySnapshotApplyError,
        >,
    ),
    ConstructorRecovery(Result<(), RuntimeString>),
}

pub(crate) struct SnapshotDurabilityProvisional {
    pub(crate) owner_id: u64,
    pub(crate) receipt: SnapshotDurabilityReceipt,
}

pub(crate) struct SnapshotDurabilityTaskControl {
    pub(crate) abort: tokio::task::AbortHandle,
}

pub(crate) struct SnapshotDurabilityRegistry {
    pub(crate) closing: bool,
    pub(crate) tracker_closed: bool,
    pub(crate) task_tracker: TaskTracker,
    pub(crate) next_task_id: u64,
    pub(crate) tasks: HashMap<u64, SnapshotDurabilityTaskControl>,
    pub(crate) provisionals: HashMap<RuntimeString, SnapshotDurabilityProvisional>,
    pub(crate) pending_recovery: Vec<AgentMemorySnapshotDecisionAudit>,
    pub(crate) recovery_receipts: HashMap<RuntimeString, SnapshotDurabilityReceipt>,
    pub(crate) failures: Vec<ShutdownPhaseFailure>,
    /// Once rollout durability and the live registry can no longer be proven
    /// equivalent, every snapshot entrypoint fails closed with the same typed
    /// error. Old queryable state must never be served as authoritative again.
    pub(crate) fatal_persistence_inconsistency: Option<RuntimeString>,
}

impl Default for SnapshotDurabilityRegistry {
    fn default() -> Self {
        Self {
            closing: false,
            tracker_closed: false,
            task_tracker: TaskTracker::new(),
            next_task_id: 0,
            tasks: HashMap::new(),
            provisionals: HashMap::new(),
            pending_recovery: Vec::new(),
            recovery_receipts: HashMap::new(),
            failures: Vec::new(),
            fatal_persistence_inconsistency: None,
        }
    }
}

pub(crate) struct SnapshotDurabilityTaskRegistration {
    pub(crate) session: Weak<Session>,
    pub(crate) request_id: RuntimeString,
    pub(crate) task_id: u64,
}

impl Drop for SnapshotDurabilityTaskRegistration {
    fn drop(&mut self) {
        let Some(session) = self.session.upgrade() else {
            return;
        };
        let Ok(mut registry) = session.snapshot.durability.lock() else {
            return;
        };
        registry.tasks.remove(&self.task_id);
        if registry
            .provisionals
            .get(&self.request_id)
            .is_some_and(|provisional| provisional.owner_id == self.task_id)
        {
            registry.provisionals.remove(&self.request_id);
        }
    }
}

/// Closes the narrow cancellation window where a streamed sub-agent future is
/// dropped after a snapshot request was published but before the Host replied.
/// The compensating transition is admitted through the same background-task
/// registry that shutdown closes and drains.
pub(crate) struct SnapshotDecisionAttemptGuard {
    pub(crate) session: Weak<Session>,
    pub(crate) request_id: RuntimeString,
    pub(crate) armed: bool,
}

impl SnapshotDecisionAttemptGuard {
    pub(crate) fn new(session: &Arc<Session>, request_id: RuntimeString) -> Self {
        Self {
            session: Arc::downgrade(session),
            request_id,
            armed: true,
        }
    }

    pub(crate) fn complete(&mut self) {
        self.armed = false;
    }
}

impl Drop for SnapshotDecisionAttemptGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Some(session) = self.session.upgrade() else {
            return;
        };
        // Register synchronously behind any in-flight Requested owner. The
        // Session tracker, not this dropped caller, owns the disconnect chain.
        // Shutdown closes live admission and performs the same repair after
        // active work drains.
        let _ = session.schedule_snapshot_durability_operation(
            SnapshotDurabilityOperation::SettleUnavailable {
                request_id: self.request_id.clone(),
                status: AgentMemorySnapshotDecisionStatus::Disconnected,
                error: Some(RuntimeString::from(
                    "snapshot decision request was cancelled before Host resolution",
                )),
                without_consumer: true,
                ordered_reply: None,
            },
            false,
            None,
        );
    }
}

#[derive(Clone)]
pub(crate) enum MaintenanceEventSinkRegistration {
    /// Trusted adapters (the protocol FIFO) must be nonblocking and receive the
    /// event in the same linearized call that persists it.
    Ordered(MemoryMaintenanceEventSink),
    /// Arbitrary SDK/Host callbacks run on a dedicated serial thread so a
    /// blocking or panicking observer cannot strand core maintenance.
    Isolated(MemoryMaintenanceEventSink),
}

pub(crate) struct IsolatedMaintenanceSinkDispatcher {
    pub(crate) sender:
        Option<std::sync::mpsc::Sender<(MemoryMaintenanceEventSink, MemoryMaintenanceEvent)>>,
    pub(crate) finished: Option<tokio::sync::oneshot::Receiver<()>>,
}

#[derive(Default)]
pub(crate) struct IsolatedMaintenanceSinkState {
    pub(crate) closing: bool,
    pub(crate) dispatcher: Option<IsolatedMaintenanceSinkDispatcher>,
}

/// One validated inference and role view, owned by the initiating capture.
pub(crate) struct PreparedSubagentConfig {
    pub(in crate::engine::session) capture: Arc<super::InferenceActivityGuard>,
    pub(in crate::engine::session) fork_path: bool,
    pub(in crate::engine::session) resuming_fork: bool,
    pub(in crate::engine::session) type_name: Option<RuntimeString>,
    pub(in crate::engine::session) definition:
        Option<crate::engine::agent_definitions::AgentDefinition>,
    pub(in crate::engine::session) effort: Option<crate::schemas::effort::EffortValue>,
}

/// Options for `run_sub_agent` (the per-run knobs beyond the spawn options).
#[derive(Default)]
pub(crate) struct RunSubAgentOpts {
    pub(crate) prepared: Option<PreparedSubagentConfig>,
    pub(crate) creation_admission: Option<Arc<subagent_messages::OrdinaryCreationAdmission>>,
    pub(crate) execution_checked: bool,
    pub(crate) local_creation: bool,
    /// The current load's concurrency policy, separate from saved creation authority.
    pub(crate) residency_config: Option<crate::schemas::config::ResolvedConfig>,
    pub(crate) creation_snapshot: Option<Arc<SubagentCreationSnapshot>>,
    /// The admitted query view, separate from immutable creation evidence.
    pub(crate) collaboration_mode: Option<crate::planning::CollaborationMode>,
    /// Captured before publication; explicit legacy absence must not recapture.
    pub(crate) permission_origin: Option<Option<super::SubagentPermissionOrigin>>,
    pub(crate) runtime_capabilities: Option<super::SubagentCapabilityCapture>,
    pub(crate) message_continuation: bool,
    pub(crate) received_messages: Vec<Message>,
    pub(crate) foreground_event_sender:
        Option<tokio::sync::mpsc::UnboundedSender<crate::types::events::EngineEvent>>,
    pub(crate) inference_capture: Option<Arc<super::InferenceActivityGuard>>,
    /// The outer option distinguishes an uncaptured caller from captured absence.
    pub(crate) goal_lease: Option<Option<Arc<crate::goal::runtime::GoalDescendantLease>>>,
    pub(crate) task_plan: Option<crate::tasks::task_store::TaskPlanBinding>,
    pub(crate) cwd_override: Option<RuntimePath>,
    pub(crate) lifecycle_kind: Option<SubagentKind>,
    pub(crate) parent_agent_id: Option<String>,
    pub(crate) fork_conversation: Option<Vec<Message>>,
    pub(crate) in_fork_child: Option<bool>,
    pub(crate) suppress_swarm_reminder: Option<bool>,
    pub(crate) worktree_notice: Option<RuntimeString>,
    /// Per-run query budget. Auto Memory extraction is capped
    /// at five assistant turns; ordinary sub-agents and Dream
    /// retain their independently resolved limits.
    pub(crate) max_turns_override: Option<u32>,
    pub(crate) message_observer: Option<MessageObserver>,
    /// The turn of the SPAWNING context that initiated this child. Lifecycle
    /// rows stamp it as their owner so a late completion still settles into
    /// the turn that started the child. Absent (direct test spawns) falls back
    /// to the child's own turn.
    pub(crate) initiating_turn_id: Option<RuntimeString>,
}

/// Observer receiving every message a run appends to its transcript.
pub(crate) type MessageObserver = Arc<dyn Fn(&Message) + Send + Sync>;

// ---------------------------------------------------------------------------
// Durable-persistence gate
// ---------------------------------------------------------------------------

/// One durable-persistence gate. Three session domains (Setup, memory
/// maintenance, and Agent Memory snapshot decisions) share this exact
/// concurrency shape:
///
/// - `event_gate` linearizes authoritative state mutation, rollout enqueue,
///   and live delivery so reconnect replay cannot disagree with observed
///   event order;
/// - `serial` serializes each append+flush and its visibility promotion so a
///   later terminal cannot overtake a still-running earlier barrier;
/// - `finalized` is the one-way fence installed before the rollout's final
///   flush/shutdown boundary.
pub(crate) struct PersistenceGate<G> {
    pub(crate) event_gate: G,
    pub(crate) serial: Arc<tokio::sync::Mutex<()>>,
    pub(crate) finalized: AtomicBool,
}

impl<G: Default> PersistenceGate<G> {
    pub(crate) fn new() -> Self {
        Self {
            event_gate: G::default(),
            serial: Arc::new(tokio::sync::Mutex::new(())),
            finalized: AtomicBool::new(false),
        }
    }
}

/// Gate whose event order is linearized under a synchronous lock.
pub(crate) type SyncPersistenceGate = PersistenceGate<Mutex<()>>;
/// Gate whose event order is linearized under an async lock (held across
/// awaits by its owner).
pub(crate) type AsyncPersistenceGate = PersistenceGate<tokio::sync::Mutex<()>>;
