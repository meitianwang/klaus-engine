//! Generation-bound lifecycle, accounting, and idle admission for one thread.

use crate::goal::accounting::{BudgetLimitedGoalDisposition, GoalAccountingState};
use crate::goal::steering::{budget_limit_prompt, objective_updated_prompt};
use crate::goal::store;
use crate::goal::types::{
    GoalAccountingMode, GoalAccountingOutcome, GoalUpdate, ThreadGoal, ThreadGoalStatus,
    GOAL_OBJECTIVE_MAX_LENGTH,
};
use crate::runtime_values::RuntimeString;
use crate::types::tool::UuidFn;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{OwnedMutexGuard, Semaphore, SemaphorePermit};

/// An ordered authoritative snapshot. A clear retains its revision tombstone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoalSnapshot {
    pub goal: Option<ThreadGoal>,
    pub revision: i64,
}
pub type GoalObserver = Arc<dyn Fn(GoalSnapshot) + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GoalStopReason {
    TurnError,
    UsageLimit,
}
impl GoalStopReason {
    fn status(self) -> ThreadGoalStatus {
        match self {
            Self::TurnError => ThreadGoalStatus::Blocked,
            Self::UsageLimit => ThreadGoalStatus::UsageLimited,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GoalCreateOutcome {
    Created(ThreadGoal),
    Rejected(ThreadGoal),
    InvalidObjective(String),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GoalUpdateOutcome {
    Updated(ThreadGoal),
    Missing,
    Forbidden(String),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GoalContinuation {
    Continue { goal: ThreadGoal, prompt: String },
    Stop,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoalHostUpdate {
    pub goal: ThreadGoal,
    pub steer: Option<String>,
    pub turn_in_flight: bool,
    pub revision: i64,
}

#[derive(Debug, Default)]
struct SafetyState {
    generation: Option<RuntimeString>,
    in_flight: bool,
    turn_epoch: u64,
    turn_intent: i64,
    execution_turn: bool,
    automatic: bool,
    provider_started: bool,
    budget_wrapup: bool,
    goal_context_turn: bool,
    human_submission: bool,
    activity: bool,
    empty_final: bool,
    executed_failure: bool,
    successful_tool: bool,
    empty_turns: u8,
    failed_execution_turns: u8,
}

#[derive(Debug, Clone, Default)]
pub struct GoalRequestIdentity {
    pub generation: Option<RuntimeString>,
    pub intent: u64,
    pub durable_intent: i64,
    pub pursuit_epoch: u64,
    pub accounting_enabled: bool,
    pub goal_work: bool,
    pub provider_admitted: bool,
}
struct GoalPermit<'a> {
    _gate: OwnedMutexGuard<()>,
    _state: SemaphorePermit<'a>,
}
pub struct GoalProviderLease {
    _gate: OwnedMutexGuard<()>,
    generation: Option<RuntimeString>,
    wrapup: bool,
}
impl GoalProviderLease {
    pub async fn accepted(self, runtime: &GoalRuntime) -> io::Result<()> {
        runtime.safety().provider_started = true;
        if self.wrapup {
            if let Some(id) = &self.generation {
                if store::acknowledge_budget_wrapup(&runtime.thread_key(), id).await? {
                    runtime.publish().await?;
                }
            }
        }
        Ok(())
    }
}
#[cfg(any(test, feature = "testing"))]
pub type GoalAdmissionBarrier =
    Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>;
#[derive(Debug, Clone, Copy)]
pub(crate) enum GoalTurnSettlement {
    Completed,
    Aborted,
    Failed(GoalStopReason),
}
#[derive(Debug, Clone, Copy)]
struct GoalPendingCleanup {
    epoch: u64,
    settlement: GoalTurnSettlement,
}
pub(crate) struct GoalTurnGuard {
    runtime: Arc<GoalRuntime>,
    cleanup: GoalPendingCleanup,
    armed: bool,
}
impl GoalTurnGuard {
    pub(crate) fn note_terminal(&mut self, settlement: GoalTurnSettlement) {
        self.cleanup.settlement = settlement;
    }
    pub(crate) async fn finish(mut self) -> io::Result<()> {
        self.armed = false;
        self.runtime.enqueue_cleanup(self.cleanup);
        self.runtime.wait_for_idle_cleanup().await
    }
}
impl Drop for GoalTurnGuard {
    fn drop(&mut self) {
        if self.armed {
            self.runtime.enqueue_cleanup(self.cleanup);
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub struct GoalStopCapture {
    authorization_epoch: u64,
}
#[derive(Debug, Default)]
struct GoalPursuitState {
    stop_epoch: u64,
    authorization_epoch: u64,
    suppressed: bool,
}

#[cfg(any(test, feature = "testing"))]
pub type GoalPhaseObserver = Arc<dyn Fn(&'static str) + Send + Sync>;

#[derive(Debug, Default)]
pub struct GoalCreatePreconditions {
    pub expected_goal_id: Option<RuntimeString>,
    pub expected_absent: bool,
    pub expected_revision: Option<i64>,
}

type GoalOwnedResourceCache = (
    RuntimeString,
    u64,
    crate::goal::owned_resources::GoalOwnedResourcesManifest,
    Arc<crate::goal::owned_resources::GoalOwnedResources>,
);

pub struct GoalRuntime {
    thread_id: RuntimeString,
    accounting: Arc<GoalAccountingState>,
    accounting_jobs: Arc<tokio::sync::Mutex<()>>,
    pending_descendant: Arc<Mutex<HashMap<RuntimeString, i64>>>,
    pending_cleanup: Mutex<Option<GoalPendingCleanup>>,
    cleanup_task: tokio::sync::Mutex<()>,
    settlement_changed: tokio::sync::Notify,
    state_lock: Semaphore,
    admission_gate: Arc<tokio::sync::Mutex<()>>,
    intent: AtomicU64,
    durable_intent: AtomicI64,
    contextual_inputs: Mutex<VecDeque<(RuntimeString, u64, bool)>>,
    run_owner: String,
    #[cfg(any(test, feature = "testing"))]
    admission_barrier: Mutex<Option<GoalAdmissionBarrier>>,
    #[cfg(any(test, feature = "testing"))]
    phase_observer: Mutex<Option<GoalPhaseObserver>>,
    #[cfg(any(test, feature = "testing"))]
    stop_snapshot_barrier: Mutex<Option<GoalAdmissionBarrier>>,
    #[cfg(any(test, feature = "testing"))]
    stop_cancel_barrier: Mutex<Option<GoalAdmissionBarrier>>,
    #[cfg(any(test, feature = "testing"))]
    model_create_barrier: Mutex<Option<GoalAdmissionBarrier>>,
    safety: Mutex<SafetyState>,
    observer: Option<GoalObserver>,
    uuid: UuidFn,
    now: Arc<dyn Fn() -> i64 + Send + Sync>,
    elapsed: Arc<dyn Fn() -> i64 + Send + Sync>,
    #[cfg(any(test, feature = "testing"))]
    frozen_elapsed: Mutex<Option<i64>>,
    maximum_budget: Mutex<Option<i64>>,
    enabled: AtomicBool,
    idle: tokio::sync::Notify,
    progress_enabled: AtomicBool,
    callback_epoch: AtomicU64,
    watching: AtomicBool,
    pursuit: Arc<Mutex<GoalPursuitState>>,
    execution_enabled: AtomicBool,
    idle_observer: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    pending_submission: Mutex<Option<RuntimeString>>,
    pending_submission_message: Mutex<Option<RuntimeString>>,
    descendants: Mutex<HashMap<String, Option<RuntimeString>>>,
    owned_cache: Mutex<Option<GoalOwnedResourceCache>>,
}
impl std::fmt::Debug for GoalRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GoalRuntime")
            .field("thread_id", &self.thread_id)
            .finish_non_exhaustive()
    }
}
impl GoalRuntime {
    pub fn new(
        thread_id: RuntimeString,
        uuid: UuidFn,
        now: Arc<dyn Fn() -> i64 + Send + Sync>,
        observer: Option<GoalObserver>,
    ) -> Self {
        Self::with_clock(thread_id, uuid, now.clone(), now, observer, None)
    }
    pub fn with_clock(
        thread_id: RuntimeString,
        uuid: UuidFn,
        now: Arc<dyn Fn() -> i64 + Send + Sync>,
        elapsed: Arc<dyn Fn() -> i64 + Send + Sync>,
        observer: Option<GoalObserver>,
        maximum_budget: Option<i64>,
    ) -> Self {
        // Local commits and shared storage observation can discover the same
        // snapshot. Keep one publication funnel for both, including detached
        // accounting work, without holding the watermark across host code.
        let observer = observer.map(|sink| -> GoalObserver {
            let last = Mutex::new(None);
            Arc::new(move |snapshot: GoalSnapshot| {
                let mut prior = last
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if prior.as_ref() == Some(&snapshot) {
                    return;
                }
                *prior = Some(snapshot.clone());
                drop(prior);
                sink(snapshot);
            })
        });
        Self {
            run_owner: (uuid)(),
            #[cfg(any(test, feature = "testing"))]
            admission_barrier: Mutex::new(None),
            #[cfg(any(test, feature = "testing"))]
            phase_observer: Mutex::new(None),
            #[cfg(any(test, feature = "testing"))]
            stop_snapshot_barrier: Mutex::new(None),
            #[cfg(any(test, feature = "testing"))]
            stop_cancel_barrier: Mutex::new(None),
            #[cfg(any(test, feature = "testing"))]
            model_create_barrier: Mutex::new(None),
            thread_id,
            admission_gate: Arc::new(tokio::sync::Mutex::new(())),
            intent: AtomicU64::new(0),
            durable_intent: AtomicI64::new(0),
            contextual_inputs: Mutex::new(VecDeque::new()),
            accounting: Arc::new(GoalAccountingState::new()),
            accounting_jobs: Arc::new(tokio::sync::Mutex::new(())),
            pending_descendant: Arc::new(Mutex::new(HashMap::new())),
            pending_cleanup: Mutex::new(None),
            cleanup_task: tokio::sync::Mutex::new(()),
            settlement_changed: tokio::sync::Notify::new(),
            state_lock: Semaphore::new(1),
            safety: Mutex::new(SafetyState::default()),
            observer,
            uuid,
            now,
            elapsed,
            #[cfg(any(test, feature = "testing"))]
            frozen_elapsed: Mutex::new(None),
            maximum_budget: Mutex::new(maximum_budget),
            enabled: AtomicBool::new(true),
            idle: tokio::sync::Notify::new(),
            progress_enabled: AtomicBool::new(false),
            callback_epoch: AtomicU64::new(0),
            watching: AtomicBool::new(false),
            pursuit: Arc::new(Mutex::new(GoalPursuitState::default())),
            execution_enabled: AtomicBool::new(true),
            idle_observer: Mutex::new(None),
            pending_submission: Mutex::new(None),
            pending_submission_message: Mutex::new(None),
            descendants: Mutex::new(HashMap::new()),
            owned_cache: Mutex::new(None),
        }
    }
    pub(crate) fn turn_guard(self: &Arc<Self>, epoch: u64) -> GoalTurnGuard {
        GoalTurnGuard {
            runtime: self.clone(),
            cleanup: GoalPendingCleanup {
                epoch,
                settlement: GoalTurnSettlement::Aborted,
            },
            armed: true,
        }
    }
    fn enqueue_cleanup(self: &Arc<Self>, cleanup: GoalPendingCleanup) {
        self.pending_cleanup
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_or_insert(cleanup);
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let runtime = self.clone();
            handle.spawn(async move {
                if let Err(error) = runtime.wait_for_idle_cleanup().await {
                    tracing::warn!(reason=%error,"goal turn cleanup remains pending");
                }
            });
        }
    }
    /// Retry retained terminal accounting before another actual turn is admitted.
    pub async fn wait_for_idle_cleanup(&self) -> io::Result<()> {
        let _serial = self.cleanup_task.lock().await;
        loop {
            let cleanup = *self
                .pending_cleanup
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(cleanup) = cleanup else {
                return Ok(());
            };
            let result = match cleanup.settlement {
                GoalTurnSettlement::Completed => self
                    .on_turn_end_bound(false, cleanup.epoch)
                    .await
                    .map(|_| ()),
                GoalTurnSettlement::Aborted => self.on_turn_aborted_bound(cleanup.epoch).await,
                GoalTurnSettlement::Failed(reason) => self
                    .on_turn_failed_bound(reason, cleanup.epoch)
                    .await
                    .map(|_| ()),
            };
            if let Err(error) = result {
                self.release_stale_turn(cleanup.epoch);
                return Err(error);
            }
            *self
                .pending_cleanup
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
            self.settlement_changed.notify_waiters();
        }
    }
    /// Wait for an admitted goal turn, including retained terminal accounting.
    /// Controls may supersede it while the caller holds no mutation or admission lock.
    pub async fn wait_for_goal_turn_settlement(
        &self,
        generation: &RuntimeString,
    ) -> io::Result<()> {
        loop {
            let changed = self.settlement_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let in_flight = {
                let safety = self.safety();
                safety.in_flight
                    && safety.generation.as_ref() == Some(generation)
                    && safety.turn_epoch == self.callback_epoch()
            };
            if !in_flight {
                return self.wait_for_idle_cleanup().await;
            }
            changed.await;
        }
    }
    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::SeqCst)
    }
    pub fn set_initial_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::SeqCst);
    }
    pub async fn set_enabled(&self, enabled: bool, executing: bool) -> io::Result<()> {
        let _permit = self.permit().await;
        self.account_progress_locked(
            GoalAccountingMode::ActiveOrStopped,
            BudgetLimitedGoalDisposition::KeepActive,
        )
        .await?;
        self.enabled.store(enabled, Ordering::SeqCst);
        self.intent.fetch_add(1, Ordering::SeqCst);
        self.execution_enabled
            .store(enabled && executing, Ordering::SeqCst);
        if enabled {
            let record = store::get_record(&self.thread_key()).await?;
            if executing
                && !self.continuation_suppressed()
                && !self.turn_in_flight()
                && !record.continuation_deferred
            {
                if let Some(goal) = record.goal.filter(|goal| goal.status.is_active()) {
                    self.accounting
                        .mark_goal_active_from_now(&goal.goal_id, self.elapsed_ms());
                }
            }
        } else {
            self.accounting.clear_active_goal_at(self.elapsed_ms());
        }
        self.publish().await?;
        self.notify_idle();
        Ok(())
    }
    async fn resource_update(
        &self,
        objective: bool,
        input: Option<crate::goal::owned_resources::GoalOwnedResourcesInput>,
    ) -> io::Result<Option<Option<crate::goal::owned_resources::GoalOwnedResourcesManifest>>> {
        match input {
            Some(input) => Ok(Some(Some(
                crate::goal::owned_resources::validate(
                    &self.thread_key(),
                    &input,
                    std::path::Path::new(&crate::fs::paths::config_home()),
                )
                .await?,
            ))),
            None if objective => Ok(Some(None)),
            None => Ok(None),
        }
    }
    pub async fn owned_read_resources(
        &self,
        identity: &GoalRequestIdentity,
    ) -> io::Result<Option<Arc<crate::goal::owned_resources::GoalOwnedResources>>> {
        if !self.enabled() || identity.intent != self.intent.load(Ordering::SeqCst) {
            return Ok(None);
        }
        let Some(id) = &identity.generation else {
            return Ok(None);
        };
        let Some(manifest) = store::get_goal_owned_resources_for_intent(
            &self.thread_key(),
            id,
            identity.durable_intent,
        )
        .await?
        else {
            return Ok(None);
        };
        if let Some((cached_id, epoch, cached_manifest, resources)) = &*self
            .owned_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            if cached_id == id && *epoch == identity.intent && cached_manifest == &manifest {
                return Ok(Some(resources.clone()));
            }
        }
        let resources = Arc::new(
            crate::goal::owned_resources::GoalOwnedResources::restore(
                &manifest,
                std::path::Path::new(&crate::fs::paths::config_home()),
            )
            .await?,
        );
        *self
            .owned_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some((id.clone(), identity.intent, manifest, resources.clone()));
        Ok(Some(resources))
    }
    pub async fn guard_owned_read(
        &self,
        identity: &GoalRequestIdentity,
        resources: &crate::goal::owned_resources::GoalOwnedResources,
    ) -> io::Result<OwnedMutexGuard<()>> {
        let gate = self.admission_gate.clone().lock_owned().await;
        let current = self.owned_read_resources(identity).await?;
        if current.as_deref() != Some(resources) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "The goal file capability changed before this read",
            ));
        }
        Ok(gate)
    }
    pub fn thread_id(&self) -> &RuntimeString {
        &self.thread_id
    }
    fn thread_key(&self) -> String {
        self.thread_id.try_to_utf8().unwrap_or_default()
    }
    fn now_ms(&self) -> i64 {
        (self.now)()
    }
    fn elapsed_ms(&self) -> i64 {
        #[cfg(any(test, feature = "testing"))]
        if let Some(elapsed) = *self
            .frozen_elapsed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            return elapsed;
        }
        (self.elapsed)()
    }
    #[cfg(any(test, feature = "testing"))]
    pub fn testing_freeze_elapsed(&self) {
        let elapsed = self.elapsed_ms();
        *self
            .frozen_elapsed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(elapsed);
    }
    #[cfg(any(test, feature = "testing"))]
    pub fn testing_unfreeze_elapsed(&self) {
        *self
            .frozen_elapsed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
    async fn permit(&self) -> GoalPermit<'_> {
        let gate = self.admission_gate.clone().lock_owned().await;
        let state = self
            .state_lock
            .acquire()
            .await
            .expect("goal state semaphore remains open");
        GoalPermit {
            _gate: gate,
            _state: state,
        }
    }
    pub fn request_identity(&self) -> GoalRequestIdentity {
        GoalRequestIdentity {
            generation: self.safety().generation.clone(),
            intent: self.intent.load(Ordering::SeqCst),
            durable_intent: self.durable_intent.load(Ordering::SeqCst),
            pursuit_epoch: self.resume_epoch(),
            accounting_enabled: self.enabled() && self.execution_enabled.load(Ordering::SeqCst),
            goal_work: self.active_generation().is_some(),
            provider_admitted: false,
        }
    }
    pub fn register_context_input(&self, id: RuntimeString, wrapup: bool) {
        let mut inputs = self
            .contextual_inputs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if inputs.len() >= 256 {
            inputs.pop_front();
        }
        inputs.push_back((id, self.intent.load(Ordering::SeqCst), wrapup));
    }
    #[cfg(any(test, feature = "testing"))]
    pub fn testing_before_stop_snapshot(&self, barrier: Option<GoalAdmissionBarrier>) {
        *self
            .stop_snapshot_barrier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = barrier;
    }
    #[cfg(any(test, feature = "testing"))]
    pub(crate) async fn testing_wait_stop_snapshot(&self) {
        let barrier = self
            .stop_snapshot_barrier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(barrier) = barrier {
            barrier().await;
        }
    }
    #[cfg(any(test, feature = "testing"))]
    pub fn testing_before_stop_cancel(&self, barrier: Option<GoalAdmissionBarrier>) {
        *self
            .stop_cancel_barrier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = barrier;
    }
    /// Test-build phase barrier. Production builds return immediately.
    pub async fn testing_wait_stop_cancel(&self) {
        #[cfg(any(test, feature = "testing"))]
        {
            let barrier = self
                .stop_cancel_barrier
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            if let Some(barrier) = barrier {
                barrier().await;
            }
        }
    }
    #[cfg(any(test, feature = "testing"))]
    pub fn testing_before_model_create(&self, barrier: Option<GoalAdmissionBarrier>) {
        *self
            .model_create_barrier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = barrier;
    }
    #[cfg(any(test, feature = "testing"))]
    pub fn testing_set_phase_observer(&self, observer: Option<GoalPhaseObserver>) {
        *self
            .phase_observer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = observer;
    }
    #[cfg(any(test, feature = "testing"))]
    pub(crate) fn testing_note_phase(&self, phase: &'static str) {
        let observer = self
            .phase_observer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(observer) = observer {
            observer(phase);
        }
    }
    #[cfg(any(test, feature = "testing"))]
    pub fn testing_before_automatic_provider_admission(
        &self,
        barrier: Option<GoalAdmissionBarrier>,
    ) {
        *self
            .admission_barrier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = barrier;
    }
    /// The final synchronous claim occurs after durable run admission, with no
    /// suspension before the provider factory. The ordinary admission owner
    /// checks queued user work under its own mutex at this linearization point.
    pub fn claim_provider_now(
        &self,
        identity: &Mutex<GoalRequestIdentity>,
        pending: Option<&Arc<dyn Fn() -> bool + Send + Sync>>,
        executing: Option<&Arc<dyn Fn() -> bool + Send + Sync>>,
    ) -> bool {
        let captured = identity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        self.enabled()
            && captured.intent == self.intent.load(Ordering::SeqCst)
            && !((captured.goal_work || self.is_automatic_turn()) && self.continuation_suppressed())
            && !(self.is_automatic_turn()
                && !captured.provider_admitted
                && (pending.is_some_and(|pending| pending())
                    || executing.is_some_and(|executing| !executing())))
    }
    pub async fn prepare_provider_request(
        &self,
        messages: &[crate::schemas::message::Message],
        identity: &Mutex<GoalRequestIdentity>,
        pending: Option<&Arc<dyn Fn() -> bool + Send + Sync>>,
        executing: Option<&Arc<dyn Fn() -> bool + Send + Sync>>,
    ) -> io::Result<GoalProviderLease> {
        self.prepare_provider_request_with_signal(messages, identity, pending, executing, None)
            .await
    }

    pub(crate) async fn prepare_provider_request_with_signal(
        &self,
        messages: &[crate::schemas::message::Message],
        identity: &Mutex<GoalRequestIdentity>,
        pending: Option<&Arc<dyn Fn() -> bool + Send + Sync>>,
        executing: Option<&Arc<dyn Fn() -> bool + Send + Sync>>,
        signal: Option<&crate::types::abort::AbortSignal>,
    ) -> io::Result<GoalProviderLease> {
        let cancelled = async {
            match signal {
                Some(signal) => signal.cancelled().await,
                None => futures::future::pending::<()>().await,
            }
        };
        tokio::pin!(cancelled);
        #[cfg(any(test, feature = "testing"))]
        {
            let first = !identity
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .provider_admitted;
            let barrier = if first && self.is_automatic_turn() {
                self.admission_barrier
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone()
            } else {
                None
            };
            if let Some(barrier) = barrier {
                tokio::select! {
                    biased;
                    _ = &mut cancelled => return Err(io::Error::new(io::ErrorKind::Interrupted, "goal request cancelled before admission")),
                    _ = barrier() => {},
                }
            }
        }
        // Only pre-effect waits are cancellable. Once the write lease is held,
        // its durable admission finishes before the caller checks cancellation.
        let gate = tokio::select! {
            biased;
            _ = &mut cancelled => return Err(io::Error::new(io::ErrorKind::Interrupted, "goal request cancelled before admission")),
            gate = self.admission_gate.clone().lock_owned() => gate,
        };
        let (next_intent, wrapup) = {
            let inputs = self
                .contextual_inputs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut next = None;
            let mut wrapup = false;
            for message in messages {
                if let crate::schemas::message::Message::User(user) = message {
                    if let Some((_, epoch, budget)) =
                        inputs.iter().rev().find(|(id, _, _)| id == &user.uuid)
                    {
                        next = Some(*epoch);
                        wrapup |= *budget;
                    }
                }
            }
            (next, wrapup)
        };
        let captured = {
            let mut captured = identity
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(epoch) = next_intent.filter(|epoch| *epoch > captured.intent) {
                captured.intent = epoch;
                captured.durable_intent = self.durable_intent.load(Ordering::SeqCst);
                captured.pursuit_epoch = self.resume_epoch();
            }
            captured.clone()
        };
        let record = store::get_record(&self.thread_key()).await?;
        let generation = record.goal.as_ref().map(|goal| goal.goal_id.clone());
        let stale = captured.generation != generation
            || captured.intent != self.intent.load(Ordering::SeqCst)
            || captured.durable_intent != record.intent_revision;
        let automatic = self.is_automatic_turn();
        if !self.enabled()
            || stale
            || (executing.is_some_and(|executing| !executing())
                && automatic
                && !captured.provider_admitted)
            || ((captured.goal_work || automatic) && self.continuation_suppressed())
            || (automatic
                && !captured.provider_admitted
                && (pending.is_some_and(|pending| pending()) || record.continuation_deferred))
        {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "goal admission superseded",
            ));
        }
        if automatic
            && !captured.provider_admitted
            && record.goal.as_ref().is_none_or(|goal| {
                !(goal.status.is_active()
                    || (goal.status == ThreadGoalStatus::BudgetLimited && wrapup))
            })
        {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "goal admission superseded",
            ));
        }
        if let Some(id) = generation.as_ref().filter(|_| {
            captured.goal_work
                && record.goal.as_ref().is_some_and(|goal| {
                    goal.status.is_active()
                        || goal.status == ThreadGoalStatus::BudgetLimited
                            && (wrapup || captured.provider_admitted)
                })
        }) {
            let (admitted, changed) =
                store::begin_goal_run_with_change(&self.thread_key(), id, &self.run_owner).await?;
            if !admitted {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "goal admission superseded",
                ));
            }
            if changed {
                self.publish().await?;
            }
        }
        self.safety().turn_intent = record.intent_revision;
        Ok(GoalProviderLease {
            _gate: gate,
            generation,
            wrapup: captured.goal_work && wrapup,
        })
    }
    pub async fn recover_interrupted_run(&self) -> io::Result<()> {
        let _permit = self.permit().await;
        if store::recover_goal_run(&self.thread_key(), &self.run_owner, self.now_ms())
            .await?
            .is_some()
        {
            self.publish().await?;
        }
        Ok(())
    }
    async fn finish_run(&self) -> io::Result<()> {
        if self.continuation_suppressed()
            || !self.enabled()
            || !self.execution_enabled.load(Ordering::SeqCst)
            || self
                .goal()
                .await?
                .is_none_or(|goal| !goal.status.is_active())
        {
            self.accounting.clear_active_goal_at(self.elapsed_ms());
        }
        if !self.has_descendant_work() {
            let generation = self.safety().generation.clone();
            if let Some(id) = generation {
                if store::finish_goal_run_with_change(&self.thread_key(), &id, &self.run_owner)
                    .await?
                {
                    self.publish().await?;
                }
            }
        }
        Ok(())
    }
    fn safety(&self) -> std::sync::MutexGuard<'_, SafetyState> {
        self.safety
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    async fn publish(&self) -> io::Result<GoalSnapshot> {
        let record = store::get_record(&self.thread_key()).await?;
        self.durable_intent
            .store(record.intent_revision, Ordering::SeqCst);
        let snapshot = GoalSnapshot {
            goal: record.goal,
            revision: record.revision,
        };
        if let Some(observer) = &self.observer {
            observer(snapshot.clone());
        }
        self.settlement_changed.notify_waiters();
        self.idle.notify_one();
        Ok(snapshot)
    }
    pub async fn goal(&self) -> io::Result<Option<ThreadGoal>> {
        store::get_goal(&self.thread_key()).await
    }
    pub async fn snapshot(&self) -> io::Result<GoalSnapshot> {
        let record = store::get_record(&self.thread_key()).await?;
        Ok(GoalSnapshot {
            goal: record.goal,
            revision: record.revision,
        })
    }
    pub fn active_generation(&self) -> Option<RuntimeString> {
        self.accounting.active_goal_id()
    }
    pub fn turn_in_flight(&self) -> bool {
        self.safety().in_flight
    }
    pub fn remember_submission_message(&self, id: RuntimeString) {
        *self
            .pending_submission_message
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(id);
    }
    pub fn take_submission_message(&self) -> Option<RuntimeString> {
        self.pending_submission_message
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
    pub fn mark_submission_pending(&self, id: &RuntimeString) {
        *self
            .pending_submission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(id.clone());
    }
    pub fn is_submission_turn(&self) -> bool {
        self.safety().human_submission
    }
    pub fn is_goal_context_turn(&self) -> bool {
        self.safety().goal_context_turn
    }
    pub async fn refresh_context_input(
        &self,
        messages: &mut [crate::schemas::message::Message],
        id: &RuntimeString,
        expansion: Option<(
            &GoalRequestIdentity,
            &[crate::schemas::content::ContentBlock],
        )>,
    ) -> io::Result<()> {
        let _permit = self.permit().await;
        let record = store::get_record(&self.thread_key()).await?;
        if let Some((authority, _)) = expansion {
            let generation = record.goal.as_ref().map(|goal| goal.goal_id.clone());
            if authority.generation != generation
                || authority.intent != self.intent.load(Ordering::SeqCst)
                || authority.durable_intent != record.intent_revision
                || authority.pursuit_epoch != self.resume_epoch()
            {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "goal context expansion superseded",
                ));
            }
        }
        let Some(goal) = record.goal else {
            return Ok(());
        };
        let budget = goal.status == ThreadGoalStatus::BudgetLimited;
        let prompt = if budget {
            budget_limit_prompt(&goal)
        } else {
            self.continuation_prompt(&goal)
        };
        if let Some(crate::schemas::message::Message::User(user))=messages.iter_mut().find(|message|matches!(message,crate::schemas::message::Message::User(user) if &user.uuid==id)) {
            user.content=vec![crate::schemas::content::ContentBlock::Text(crate::schemas::content::TextBlock {text:prompt.into(),citations:None,harness_envelope:None,message_id:None,phase:None})];
            if let Some((_, added)) = expansion {
                user.content.extend_from_slice(added);
            }
            self.register_context_input(id.clone(),budget);
        }
        Ok(())
    }
    pub fn is_budget_wrapup_turn(&self) -> bool {
        self.safety().budget_wrapup
    }
    pub fn is_automatic_turn(&self) -> bool {
        self.safety().automatic
    }
    pub fn set_progress_tool_enabled(&self, enabled: bool) {
        self.progress_enabled.store(enabled, Ordering::SeqCst);
    }
    fn continuation_prompt(&self, goal: &ThreadGoal) -> String {
        crate::goal::steering::continuation_prompt_with_progress(
            goal,
            self.progress_enabled.load(Ordering::SeqCst),
        )
    }
    /// Capture Stop authority without changing a replacement turn's pursuit state.
    pub fn begin_stop(&self) -> GoalStopCapture {
        let mut pursuit = self
            .pursuit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        pursuit.stop_epoch = pursuit
            .stop_epoch
            .checked_add(1)
            .expect("goal stop epoch exhausted");
        GoalStopCapture {
            authorization_epoch: pursuit.authorization_epoch,
        }
    }
    /// Keep the captured goal authority fixed through a synchronous SQL commit.
    /// The caller has already acquired the database transaction and foreground lease.
    pub fn guard_stop_control(
        &self,
        capture: GoalStopCapture,
        signal: &crate::types::abort::AbortSignal,
        operation: &mut dyn FnMut() -> io::Result<store::GoalControlOutcome>,
    ) -> io::Result<store::GoalControlOutcome> {
        let pursuit = self
            .pursuit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if pursuit.authorization_epoch != capture.authorization_epoch || signal.is_cancelled() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "goal pause lost its captured goal authority",
            ));
        }
        let result = operation();
        drop(pursuit);
        result
    }
    pub fn suppress_continuation_for(&self, capture: GoalStopCapture) -> bool {
        let mut pursuit = self
            .pursuit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if pursuit.authorization_epoch != capture.authorization_epoch {
            return false;
        }
        pursuit.suppressed = true;
        drop(pursuit);
        self.idle.notify_one();
        true
    }
    pub fn suppress_continuation(&self) {
        let capture = self.begin_stop();
        self.suppress_continuation_for(capture);
    }
    fn resume_epoch(&self) -> u64 {
        self.pursuit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .stop_epoch
    }
    /// The successful mutation can replace an older Stop, never one that arrived
    /// while that mutation was waiting for persistence or publication.
    fn resume_pursuit(&self, captured_stop_epoch: u64) {
        let mut pursuit = self
            .pursuit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if pursuit.stop_epoch == captured_stop_epoch {
            pursuit.authorization_epoch = pursuit
                .authorization_epoch
                .checked_add(1)
                .expect("goal pursuit epoch exhausted");
            pursuit.suppressed = false;
        }
    }
    pub fn continuation_suppressed(&self) -> bool {
        self.pursuit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .suppressed
    }
    fn release_stale_turn(&self, epoch: u64) {
        let mut safety = self.safety();
        if safety.turn_epoch == epoch {
            safety.in_flight = false;
        }
        drop(safety);
        self.settlement_changed.notify_waiters();
        self.notify_idle();
    }
    pub fn callback_epoch(&self) -> u64 {
        self.callback_epoch.load(Ordering::SeqCst)
    }
    pub fn watch_external_changes(self: &Arc<Self>) {
        if self.watching.swap(true, Ordering::SeqCst) {
            return;
        }
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut changes = loop {
                if weak.strong_count() == 0 {
                    return;
                }
                match store::subscribe_changes().await {
                    Ok(changes) => break changes,
                    Err(error) => tracing::error!(reason=%error, "goal observation failed"),
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            };
            let mut previous: Option<GoalSnapshot> = None;
            let mut generation = None;
            loop {
                let Some(runtime) = weak.upgrade() else {
                    break;
                };
                let retry = match runtime.snapshot().await {
                    Ok(snapshot) => {
                        // A replaced database may restart its revision counter.
                        if previous.as_ref().is_some_and(|value| value != &snapshot) {
                            let next = snapshot.goal.as_ref().map(|goal| goal.goal_id.clone());
                            if generation != next && runtime.safety().generation != next {
                                runtime.callback_epoch.fetch_add(1, Ordering::SeqCst);
                            }
                            if let Some(observer) = &runtime.observer {
                                observer(snapshot.clone());
                            }
                            runtime.idle.notify_one();
                        }
                        generation = snapshot.goal.as_ref().map(|goal| goal.goal_id.clone());
                        previous = Some(snapshot);
                        false
                    }
                    Err(error) => {
                        tracing::error!(reason=%error,"goal observation failed");
                        true
                    }
                };
                drop(runtime);
                // Waiting owns no runtime. The cheap liveness check releases an
                // abandoned subscription even when storage never changes again.
                loop {
                    tokio::select! {
                        changed = changes.changed() => { if changed.is_err() { return; } break; }
                        _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                            if weak.strong_count() == 0 { return; }
                            // Legacy-file repair need not change SQLite, so an
                            // unsuccessful snapshot retains a bounded retry.
                            if retry { break; }
                        }
                    }
                }
            }
        });
    }
    pub fn set_idle_observer(&self, observer: Option<Arc<dyn Fn() + Send + Sync>>) {
        *self
            .idle_observer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = observer;
    }
    pub fn notify_idle(&self) {
        self.idle.notify_one();
        let observer = self
            .idle_observer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(observer) = observer {
            observer();
        }
    }
    pub async fn idle_event(&self) {
        self.idle.notified().await;
    }
    pub fn has_descendant_work(&self) -> bool {
        !self
            .descendants
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    }
    pub fn register_descendant(
        self: &Arc<Self>,
        agent: String,
        parent: Option<&str>,
    ) -> GoalDescendantLease {
        let mut descendants = self
            .descendants
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let generation = parent
            .and_then(|id| descendants.get(id).cloned())
            .unwrap_or_else(|| self.active_generation());
        descendants.insert(agent.clone(), generation.clone());
        GoalDescendantLease {
            runtime: Arc::downgrade(self),
            agent,
            generation,
        }
    }
    pub fn note_event(&self, event: &crate::types::events::EngineEvent) {
        if !self.safety().execution_turn {
            return;
        }
        use crate::types::events::{EngineEvent, ThreadItem, ToolDomainEvent};
        let active = match event {
            // Streaming assistant text is provisional until its item completes.
            EngineEvent::TextDelta { .. }
            | EngineEvent::ItemStarted {
                item: ThreadItem::AgentMessage { .. },
                ..
            }
            | EngineEvent::ItemUpdated {
                item: ThreadItem::AgentMessage { .. },
                ..
            } => false,
            EngineEvent::ItemStarted { item, .. }
            | EngineEvent::ItemUpdated { item, .. }
            | EngineEvent::ItemCompleted { item, .. } => match item {
                ThreadItem::AgentMessage { text, .. } => {
                    text.try_to_utf8().is_ok_and(|text| !text.trim().is_empty())
                }
                // The stream assembler records completed summaries from typed
                // provider provenance. A generic displayed reasoning item does
                // not distinguish a summary from raw or encrypted reasoning.
                ThreadItem::Reasoning { .. } => false,
                _ => true,
            },
            EngineEvent::ToolEvent { event, .. } => match event {
                ToolDomainEvent::Stdout { chunk } | ToolDomainEvent::Stderr { chunk } => {
                    !chunk.is_empty()
                }
                ToolDomainEvent::Status { message } => !message.is_empty(),
                _ => true,
            },
            EngineEvent::QuestionRequested { .. }
            | EngineEvent::QuestionResolved { .. }
            | EngineEvent::ApprovalRequested { .. }
            | EngineEvent::Compaction { .. }
            | EngineEvent::CompactionProgress { .. }
            | EngineEvent::InputApplied { .. } => true,
            _ => false,
        };
        if let EngineEvent::ItemCompleted {
            item: ThreadItem::AgentMessage { text, phase, .. },
            ..
        } = event
        {
            if text.try_to_utf8().is_ok_and(|text| text.trim().is_empty())
                && phase.as_ref().is_none_or(|phase| phase == "final_answer")
            {
                self.safety().empty_final = true;
            }
        }
        if active {
            self.note_activity();
        }
    }

    pub fn note_empty_final(&self) {
        let mut safety = self.safety();
        if safety.execution_turn {
            safety.empty_final = true;
        }
    }
    pub fn note_activity(&self) {
        let mut safety = self.safety();
        if safety.execution_turn {
            safety.activity = true;
        }
    }
    /// Called only after the actual handler ran. Denials and validation failures never enter this audit.
    pub fn note_tool_outcome(&self, execution_tool: bool, succeeded: bool) {
        let mut safety = self.safety();
        if !safety.execution_turn {
            return;
        }
        safety.activity = true;
        if succeeded {
            safety.successful_tool = true;
            safety.failed_execution_turns = 0;
        } else if execution_tool {
            safety.executed_failure = true;
        }
    }
    pub async fn on_turn_start(&self) -> io::Result<Option<ThreadGoal>> {
        self.on_turn_start_for(false, None, true).await
    }
    /// An automatic admission must still own the generation when the request starts.
    pub async fn on_turn_start_for(
        &self,
        automatic: bool,
        expected: Option<&RuntimeString>,
        executing: bool,
    ) -> io::Result<Option<ThreadGoal>> {
        #[cfg(any(test, feature = "testing"))]
        self.testing_note_phase("goal.cleanup");
        self.wait_for_idle_cleanup().await?;
        #[cfg(any(test, feature = "testing"))]
        self.testing_note_phase("goal.permit");
        let _permit = self.permit().await;
        #[cfg(any(test, feature = "testing"))]
        self.testing_note_phase("goal.accounting");
        self.account_progress_locked(
            GoalAccountingMode::ActiveOnly,
            BudgetLimitedGoalDisposition::KeepActive,
        )
        .await?;
        let record = store::get_record(&self.thread_key()).await?;
        self.durable_intent
            .store(record.intent_revision, Ordering::SeqCst);
        if automatic
            && (!executing
                || self.continuation_suppressed()
                || record.continuation_deferred
                || record.goal.as_ref().is_none_or(|goal| {
                    !(goal.status.is_active() || goal.status == ThreadGoalStatus::BudgetLimited)
                        || expected.is_some_and(|id| id != &goal.goal_id)
                }))
        {
            self.safety().in_flight = false;
            if !executing {
                self.execution_enabled.store(false, Ordering::SeqCst);
                self.accounting.clear_active_goal_at(self.elapsed_ms());
            }
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "goal admission was superseded",
            ));
        }
        if !automatic && record.continuation_deferred {
            if let Some(goal) = &record.goal {
                let changed = store::set_continuation_deferral_if_with_change(
                    &self.thread_key(),
                    false,
                    Some(goal.goal_id.clone()),
                )
                .await?
                .map_err(|_| io::Error::other("goal changed before explicit turn"))?;
                if changed {
                    self.publish().await?;
                }
            }
        }
        self.accounting.start_turn(self.elapsed_ms());
        {
            let mut safety = self.safety();
            let generation = record.goal.as_ref().map(|goal| goal.goal_id.clone());
            if safety.generation != generation {
                *safety = SafetyState::default();
            }
            safety.generation = generation;
            safety.in_flight = true;
            safety.turn_epoch = self.callback_epoch();
            safety.turn_intent = record.intent_revision;
            safety.execution_turn = executing && self.enabled();
            let human_submission = record.goal.as_ref().is_some_and(|goal| {
                self.pending_submission
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .as_ref()
                    == Some(&goal.goal_id)
            });
            if human_submission {
                self.pending_submission
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
            }
            safety.automatic = automatic && !human_submission;
            safety.provider_started = false;
            safety.goal_context_turn = automatic;
            safety.human_submission = human_submission;
            safety.budget_wrapup = automatic
                && record
                    .goal
                    .as_ref()
                    .is_some_and(|goal| goal.status == ThreadGoalStatus::BudgetLimited);
            safety.activity = false;
            safety.empty_final = false;
            safety.executed_failure = false;
            safety.successful_tool = false;
        }
        self.execution_enabled.store(executing, Ordering::SeqCst);
        if executing && !self.continuation_suppressed() {
            if let Some(goal) = &record.goal {
                if goal.status.is_active()
                    || (automatic && goal.status == ThreadGoalStatus::BudgetLimited)
                {
                    self.accounting.mark_goal_active(&goal.goal_id);
                } else {
                    self.accounting.clear_active_goal_at(self.elapsed_ms());
                }
            } else {
                self.accounting.clear_active_goal_at(self.elapsed_ms());
            }
        } else {
            self.accounting.clear_active_goal_at(self.elapsed_ms());
        }
        Ok(record.goal)
    }
    pub fn on_token_usage_for(
        &self,
        generation: Option<&RuntimeString>,
        usage: &crate::schemas::message::Usage,
    ) {
        if let Some(generation) = generation {
            self.accounting.record_token_usage_for(generation, usage);
        }
    }
    pub fn on_token_usage(&self, usage: &crate::schemas::message::Usage) {
        self.accounting.record_token_usage(usage);
    }
    pub async fn account_progress(
        &self,
        mode: GoalAccountingMode,
        disposition: BudgetLimitedGoalDisposition,
    ) -> io::Result<Option<String>> {
        let _permit = self.permit().await;
        self.account_progress_locked(mode, disposition).await
    }
    pub async fn pause_for_shutdown(&self) -> io::Result<()> {
        self.pause_for_shutdown_guarded(None).await
    }
    pub async fn pause_for_shutdown_guarded(
        &self,
        control_guard: Option<store::GoalControlGuard>,
    ) -> io::Result<()> {
        let guarded = control_guard.is_some();
        if !guarded {
            self.suppress_continuation();
        }
        let _permit = self.permit().await;
        if let Some(goal) = self.goal().await?.filter(|goal| goal.status.is_active()) {
            self.apply_control_locked(
                store::GoalControlMutation::Set(store::GoalRequestMutation::Update {
                    update: GoalUpdate {
                        status: Some(ThreadGoalStatus::Paused),
                        expected_goal_id: Some(goal.goal_id),
                        control_guard,
                        ..Default::default()
                    },
                    now_ms: self.now_ms(),
                }),
                None,
                None,
            )
            .await?;
            self.intent.fetch_add(1, Ordering::SeqCst);
            self.publish().await?;
        } else if guarded {
            // A fenced no-op has no authority to change a replacement turn's
            // clock or continuation state. The interrupt owner makes its final
            // synchronous reservation check before applying the stop latch.
            return Ok(());
        } else {
            self.account_progress_locked(
                GoalAccountingMode::ActiveOrStopped,
                BudgetLimitedGoalDisposition::KeepActive,
            )
            .await?;
        }
        self.accounting.clear_active_goal_at(self.elapsed_ms());
        Ok(())
    }
    pub async fn flush_stopped_progress(&self) -> io::Result<()> {
        let _permit = self.permit().await;
        self.account_progress_locked(
            GoalAccountingMode::ActiveOrStopped,
            BudgetLimitedGoalDisposition::ClearActive,
        )
        .await?;
        self.accounting.clear_active_goal_at(self.elapsed_ms());
        Ok(())
    }
    pub async fn flush_progress(&self) -> io::Result<()> {
        self.account_progress(
            GoalAccountingMode::ActiveOnly,
            BudgetLimitedGoalDisposition::KeepActive,
        )
        .await?;
        Ok(())
    }
    async fn account_progress_locked(
        &self,
        mode: GoalAccountingMode,
        disposition: BudgetLimitedGoalDisposition,
    ) -> io::Result<Option<String>> {
        let serial = self.accounting_jobs.clone().lock_owned().await;
        let accounting = self.accounting.clone();
        let pending = self.pending_descendant.clone();
        let thread = self.thread_key();
        let elapsed = self.elapsed_ms();
        let now = self.now_ms();
        let observer = self.observer.clone();
        // The task owns both the database write and its local acknowledgement.
        // Cancelling a caller cannot leave a committed additive write unacknowledged.
        tokio::spawn(async move {
            let _serial = serial;
            let samples: Vec<_> = pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .map(|(id, tokens)| (id.clone(), *tokens))
                .collect();
            let mut changed = false;
            for (generation, tokens) in samples {
                let outcome = store::account_usage(
                    &thread,
                    GoalAccountingMode::ActiveOrStopped,
                    tokens,
                    0,
                    Some(generation.clone()),
                    now,
                )
                .await?;
                changed |= matches!(outcome, GoalAccountingOutcome::Updated(_));
                let mut samples = pending
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(remaining) = samples.get_mut(&generation) {
                    *remaining = remaining.saturating_sub(tokens);
                    if *remaining == 0 {
                        samples.remove(&generation);
                    }
                }
            }
            if let Some(snapshot) = accounting.progress_snapshot(elapsed) {
                match store::account_usage(
                    &thread,
                    mode,
                    snapshot.token_delta,
                    snapshot.time_delta_seconds,
                    Some(snapshot.goal_id.clone()),
                    now,
                )
                .await?
                {
                    GoalAccountingOutcome::Updated(goal) => {
                        accounting.mark_progress_accounted(
                            &snapshot,
                            elapsed,
                            goal.status == ThreadGoalStatus::BudgetLimited
                                && disposition == BudgetLimitedGoalDisposition::ClearActive,
                        );
                        changed = true;
                    }
                    GoalAccountingOutcome::Unchanged(goal) => {
                        if goal
                            .as_ref()
                            .is_none_or(|goal| goal.goal_id != snapshot.goal_id)
                        {
                            accounting.clear_active_goal_at(elapsed);
                        }
                    }
                }
            }
            let mut record = store::get_record(&thread).await?;
            let pending_wrapup = if let Some(goal) = record
                .goal
                .as_ref()
                .filter(|goal| goal.status == ThreadGoalStatus::BudgetLimited)
            {
                let (pending, created) =
                    store::pending_budget_wrapup_with_change(&thread, &goal.goal_id).await?;
                if created {
                    changed = true;
                    record = store::get_record(&thread).await?;
                }
                pending
            } else {
                false
            };
            if changed {
                if let Some(observer) = observer {
                    observer(GoalSnapshot {
                        goal: record.goal.clone(),
                        revision: record.revision,
                    });
                }
            }
            Ok(record
                .goal
                .filter(|_| pending_wrapup)
                .map(|goal| budget_limit_prompt(&goal)))
        })
        .await
        .map_err(|error| io::Error::other(format!("goal accounting task failed: {error}")))?
    }
    async fn apply_control_locked(
        &self,
        mutation: store::GoalControlMutation,
        receipt: Option<store::GoalControlReceipt>,
        resources: Option<Option<crate::goal::owned_resources::GoalOwnedResourcesManifest>>,
    ) -> io::Result<store::GoalControlOutcome> {
        let serial = self.accounting_jobs.clone().lock_owned().await;
        let accounting = self.accounting.clone();
        let pending = self.pending_descendant.clone();
        let thread = self.thread_key();
        let elapsed = self.elapsed_ms();
        let now = self.now_ms();
        tokio::spawn(async move {
            let _serial = serial;
            let current = store::get_record(&thread)
                .await?
                .goal
                .map(|goal| goal.goal_id);
            // A retired generation can never be rebound on this thread.
            pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .retain(|id, _| Some(id) == current.as_ref());
            let snapshot = accounting
                .progress_snapshot(elapsed)
                .filter(|snapshot| Some(&snapshot.goal_id) == current.as_ref());
            let descendant_tokens = current
                .as_ref()
                .and_then(|id| {
                    pending
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .get(id)
                        .copied()
                })
                .unwrap_or(0);
            let progress = current
                .as_ref()
                .filter(|_| snapshot.is_some() || descendant_tokens > 0)
                .map(|id| store::GoalControlAccounting {
                    mode: GoalAccountingMode::ActiveOrStopped,
                    goal_id: id.clone(),
                    token_delta: snapshot
                        .as_ref()
                        .map_or(0, |snapshot| snapshot.token_delta)
                        .saturating_add(descendant_tokens),
                    time_delta_seconds: snapshot
                        .as_ref()
                        .map_or(0, |snapshot| snapshot.time_delta_seconds),
                    now_ms: now,
                });
            let outcome =
                store::apply_goal_control(&thread, mutation, progress, receipt, resources).await?;
            if !outcome
                .receipt
                .as_ref()
                .is_some_and(|receipt| receipt.replayed)
            {
                if let (Some(snapshot), Some(GoalAccountingOutcome::Updated(_))) =
                    (&snapshot, &outcome.accounting)
                {
                    accounting.mark_progress_accounted(snapshot, elapsed, false);
                }
                if outcome.accounting.is_some() {
                    if let Some(id) = current {
                        let mut samples = pending
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        if let Some(tokens) = samples.get_mut(&id) {
                            *tokens = tokens.saturating_sub(descendant_tokens);
                            if *tokens == 0 {
                                samples.remove(&id);
                            }
                        }
                    }
                }
            }
            Ok(outcome)
        })
        .await
        .map_err(|error| io::Error::other(format!("goal control task failed: {error}")))?
    }
    async fn release_stale_intent_turn(&self) -> io::Result<bool> {
        let record = store::get_record(&self.thread_key()).await?;
        let stale = self.safety().turn_intent != record.intent_revision;
        if stale {
            {
                let mut safety = self.safety();
                safety.in_flight = false;
                safety.empty_turns = 0;
                safety.failed_execution_turns = 0;
            }
            self.finish_run().await?;
        }
        Ok(stale)
    }
    /// Settle an idle event before deciding whether an admission may be claimed.
    pub async fn on_turn_end(&self, pending_user_input: bool) -> io::Result<GoalContinuation> {
        self.on_turn_end_bound(pending_user_input, self.callback_epoch())
            .await
    }
    pub async fn on_turn_end_bound(
        &self,
        pending_user_input: bool,
        epoch: u64,
    ) -> io::Result<GoalContinuation> {
        let _permit = self.permit().await;
        if epoch != self.callback_epoch() {
            self.release_stale_turn(epoch);
            return Ok(GoalContinuation::Stop);
        }
        self.account_progress_locked(
            GoalAccountingMode::ActiveOrStopped,
            BudgetLimitedGoalDisposition::ClearActive,
        )
        .await?;
        if !self.safety().execution_turn {
            self.safety().in_flight = false;
            self.finish_run().await?;
            return Ok(GoalContinuation::Stop);
        }
        if self.release_stale_intent_turn().await? {
            return Ok(GoalContinuation::Stop);
        }
        let stop_for_safety = {
            let mut safety = self.safety();
            safety.in_flight = false;
            if safety.automatic && !safety.activity && safety.empty_final {
                safety.empty_turns = safety.empty_turns.saturating_add(1);
            } else {
                safety.empty_turns = 0;
            }
            if safety.successful_tool {
                safety.failed_execution_turns = 0;
            } else if safety.executed_failure {
                safety.failed_execution_turns = safety.failed_execution_turns.saturating_add(1);
            }
            safety.empty_turns >= 3 || safety.failed_execution_turns >= 3
        };
        if stop_for_safety {
            self.stop_locked(GoalStopReason::TurnError).await?;
            return Ok(GoalContinuation::Stop);
        }
        self.finish_run().await?;
        self.continuation_locked(pending_user_input).await
    }
    pub async fn continuation_if_idle(
        &self,
        pending_user_input: bool,
    ) -> io::Result<GoalContinuation> {
        self.wait_for_idle_cleanup().await?;
        let _permit = self.permit().await;
        self.account_progress_locked(
            GoalAccountingMode::ActiveOnly,
            BudgetLimitedGoalDisposition::ClearActive,
        )
        .await?;
        self.continuation_locked(pending_user_input).await
    }
    async fn continuation_locked(&self, pending_user_input: bool) -> io::Result<GoalContinuation> {
        if !self.enabled()
            || !self.execution_enabled.load(Ordering::SeqCst)
            || self.continuation_suppressed()
            || pending_user_input
            || self.safety().in_flight
            || self.has_descendant_work()
        {
            return Ok(GoalContinuation::Stop);
        }
        let record = store::get_record(&self.thread_key()).await?;
        if record.continuation_deferred {
            return Ok(GoalContinuation::Stop);
        }
        let Some(goal) = record.goal else {
            return Ok(GoalContinuation::Stop);
        };
        if goal.status == ThreadGoalStatus::BudgetLimited {
            let (pending, changed) =
                store::pending_budget_wrapup_with_change(&self.thread_key(), &goal.goal_id).await?;
            if changed {
                self.publish().await?;
            }
            if pending {
                return Ok(GoalContinuation::Continue {
                    prompt: budget_limit_prompt(&goal),
                    goal,
                });
            }
        }
        if !goal.status.is_active() || goal.budget_exhausted() {
            return Ok(GoalContinuation::Stop);
        }
        Ok(GoalContinuation::Continue {
            prompt: self.continuation_prompt(&goal),
            goal,
        })
    }
    pub async fn on_turn_failed(&self, reason: GoalStopReason) -> io::Result<Option<ThreadGoal>> {
        self.on_turn_failed_bound(reason, self.callback_epoch())
            .await
    }
    pub async fn on_turn_failed_bound(
        &self,
        reason: GoalStopReason,
        epoch: u64,
    ) -> io::Result<Option<ThreadGoal>> {
        let _permit = self.permit().await;
        if epoch != self.callback_epoch() {
            self.release_stale_turn(epoch);
            return Ok(None);
        }
        self.account_progress_locked(
            GoalAccountingMode::ActiveOnly,
            BudgetLimitedGoalDisposition::ClearActive,
        )
        .await?;
        if !self.safety().execution_turn {
            self.safety().in_flight = false;
            self.finish_run().await?;
            return self.goal().await;
        }
        if self.release_stale_intent_turn().await? {
            return self.goal().await;
        }
        let result = self.stop_locked(reason).await?;
        self.finish_run().await?;
        Ok(result)
    }
    async fn stop_locked(&self, reason: GoalStopReason) -> io::Result<Option<ThreadGoal>> {
        self.safety().in_flight = false;
        let expected = self.safety().generation.clone();
        self.accounting.clear_active_goal_at(self.elapsed_ms());
        let Some(existing) = self.goal().await? else {
            return Ok(None);
        };
        if expected.as_ref().is_some_and(|id| id != &existing.goal_id) {
            return Ok(Some(existing));
        }
        let quota_over_budget = reason == GoalStopReason::UsageLimit
            && existing.status == ThreadGoalStatus::BudgetLimited;
        if !existing.status.is_active() && !quota_over_budget {
            return Ok(Some(existing));
        }
        let expected_intent = self.safety().turn_intent;
        let result = store::update_goal(
            &self.thread_key(),
            GoalUpdate {
                status: Some(reason.status()),
                expected_goal_id: Some(existing.goal_id.clone()),
                expected_intent_revision: Some(expected_intent),
                ..Default::default()
            },
            self.now_ms(),
        )
        .await?;
        let updated = result.map_err(|_| io::Error::other("goal changed while stopping"))?;
        self.publish().await?;
        Ok(updated)
    }
    /// Internal aborts settle usage; only the user-stop path requests a pause.
    pub async fn on_turn_aborted(&self) -> io::Result<()> {
        self.on_turn_aborted_bound(self.callback_epoch()).await
    }
    pub async fn on_turn_aborted_bound(&self, epoch: u64) -> io::Result<()> {
        let _permit = self.permit().await;
        if epoch != self.callback_epoch() {
            self.release_stale_turn(epoch);
            return Ok(());
        }
        self.account_progress_locked(
            GoalAccountingMode::ActiveOrStopped,
            BudgetLimitedGoalDisposition::ClearActive,
        )
        .await?;
        if !self.safety().execution_turn {
            self.safety().in_flight = false;
            self.finish_run().await?;
            return Ok(());
        }
        if self.release_stale_intent_turn().await? {
            return Ok(());
        }
        let exhausted = {
            let mut safety = self.safety();
            safety.in_flight = false;
            if safety.automatic && safety.provider_started && !safety.activity {
                safety.empty_turns = safety.empty_turns.saturating_add(1);
            } else {
                safety.empty_turns = 0;
            }
            safety.empty_turns >= 3
        };
        if exhausted {
            self.stop_locked(GoalStopReason::TurnError).await?;
        }
        if self.continuation_suppressed()
            || !self.enabled()
            || !self.execution_enabled.load(Ordering::SeqCst)
            || self
                .goal()
                .await?
                .is_none_or(|goal| !goal.status.is_active())
        {
            self.accounting.clear_active_goal_at(self.elapsed_ms());
        }
        self.finish_run().await?;
        Ok(())
    }
    pub async fn synchronize_execution_mode(&self, executing: bool) -> io::Result<()> {
        let _permit = self.permit().await;
        if self.turn_in_flight() {
            return Ok(());
        }
        let executing = executing && self.enabled();
        let was_executing = self.execution_enabled.swap(executing, Ordering::SeqCst);
        self.account_progress_locked(
            GoalAccountingMode::ActiveOnly,
            BudgetLimitedGoalDisposition::ClearActive,
        )
        .await?;
        let record = store::get_record(&self.thread_key()).await?;
        if executing && !self.continuation_suppressed() && !record.continuation_deferred {
            if let Some(goal) = record.goal.filter(|goal| goal.status.is_active()) {
                if was_executing && self.active_generation().as_ref() == Some(&goal.goal_id) {
                    self.accounting.mark_goal_active(&goal.goal_id);
                } else {
                    self.accounting
                        .mark_goal_active_from_now(&goal.goal_id, self.elapsed_ms());
                }
            }
        } else {
            self.accounting.clear_active_goal_at(self.elapsed_ms());
        }
        self.notify_idle();
        Ok(())
    }
    pub fn freeze_clock_for_planning(&self) {
        self.accounting.pause_clock(self.elapsed_ms());
    }
    pub async fn suspend_accounting(&self) -> io::Result<()> {
        self.flush_progress().await?;
        self.accounting.clear_active_goal_at(self.elapsed_ms());
        Ok(())
    }
    pub fn record_descendant_usage(
        &self,
        generation: RuntimeString,
        usage: &crate::schemas::message::Usage,
    ) {
        let mut pending = self
            .pending_descendant
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let tokens = pending.entry(generation).or_default();
        *tokens = tokens.saturating_add(crate::goal::accounting::goal_token_delta_for_usage(usage));
    }
    pub async fn on_descendant_usage(
        &self,
        generation: RuntimeString,
        usage: &crate::schemas::message::Usage,
    ) -> io::Result<()> {
        self.record_descendant_usage(generation, usage);
        let _permit = self.permit().await;
        self.account_progress_locked(
            GoalAccountingMode::ActiveOrStopped,
            BudgetLimitedGoalDisposition::KeepActive,
        )
        .await?;
        Ok(())
    }
    pub fn maximum_budget(&self) -> Option<i64> {
        *self
            .maximum_budget
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub async fn set_maximum_budget(&self, maximum: Option<i64>) -> io::Result<()> {
        if maximum.is_some_and(|value| value <= 0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "maximum goal budget must be positive",
            ));
        }
        let _permit = self.permit().await;
        *self
            .maximum_budget
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = maximum;
        Ok(())
    }
    fn valid_budget(&self, budget: Option<i64>) -> Result<Option<i64>, String> {
        if budget.is_some_and(|value| value <= 0) {
            return Err("token budget must be a positive integer".into());
        }
        let maximum_budget = self.maximum_budget();
        if maximum_budget.is_some_and(|value| value <= 0) {
            return Err("maximum goal budget must be positive".into());
        }
        match (budget, maximum_budget) {
            (Some(value), Some(maximum)) if value > maximum => Err(format!(
                "token budget exceeds the maximum goal budget of {maximum}"
            )),
            (None, Some(maximum)) => Ok(Some(maximum)),
            _ => Ok(budget),
        }
    }
    pub fn validate_objective(objective: &RuntimeString) -> Result<RuntimeString, String> {
        let text = objective
            .try_to_utf8()
            .map_err(|_| "objective must be valid Unicode".to_string())?;
        let text = text.trim();
        if text.is_empty() {
            return Err("objective must not be empty".into());
        }
        if text.chars().count() > GOAL_OBJECTIVE_MAX_LENGTH {
            return Err(format!(
                "objective is limited to {GOAL_OBJECTIVE_MAX_LENGTH} characters"
            ));
        }
        Ok(text.into())
    }
    pub async fn create_goal(
        &self,
        objective: RuntimeString,
        token_budget: Option<i64>,
    ) -> GoalCreateOutcome {
        self.create_goal_inner(objective, token_budget, None, false, None)
            .await
    }
    pub async fn create_goal_checked(
        &self,
        objective: RuntimeString,
        token_budget: Option<i64>,
        identity: Option<&GoalRequestIdentity>,
    ) -> GoalCreateOutcome {
        self.create_goal_inner(objective, token_budget, identity, true, None)
            .await
    }
    pub async fn create_goal_checked_with_signal(
        &self,
        objective: RuntimeString,
        token_budget: Option<i64>,
        identity: Option<&GoalRequestIdentity>,
        signal: &crate::types::abort::AbortSignal,
    ) -> GoalCreateOutcome {
        if signal.is_cancelled() {
            return GoalCreateOutcome::InvalidObjective("The goal request was cancelled".into());
        }
        self.create_goal_inner(
            objective,
            token_budget,
            identity,
            true,
            Some(self.request_control_guard(signal, identity)),
        )
        .await
    }
    fn request_control_guard(
        &self,
        signal: &crate::types::abort::AbortSignal,
        identity: Option<&GoalRequestIdentity>,
    ) -> store::GoalControlGuard {
        let signal = signal.clone();
        let pursuit = self.pursuit.clone();
        let epoch = identity.map_or_else(|| self.resume_epoch(), |identity| identity.pursuit_epoch);
        store::GoalControlGuard::new(move |operation| {
            let state = pursuit
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if signal.is_cancelled() || state.stop_epoch != epoch {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "The goal request was cancelled before its mutation",
                ));
            }
            // SQLite acquired BEGIN before this synchronous guard. Stop cannot
            // overtake a model write already admitted to its commit boundary.
            let result = operation();
            drop(state);
            result
        })
    }
    async fn create_goal_inner(
        &self,
        objective: RuntimeString,
        token_budget: Option<i64>,
        identity: Option<&GoalRequestIdentity>,
        require_safe_json: bool,
        control_guard: Option<store::GoalControlGuard>,
    ) -> GoalCreateOutcome {
        let pursuit_epoch =
            identity.map_or_else(|| self.resume_epoch(), |identity| identity.pursuit_epoch);
        #[cfg(any(test, feature = "testing"))]
        if identity.is_some() {
            let barrier = self
                .model_create_barrier
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            if let Some(barrier) = barrier {
                barrier().await;
            }
        }
        let _permit = self.permit().await;
        {
            let acknowledged = self.accounting_jobs.lock().await;
            drop(acknowledged);
        }
        if !self.enabled() {
            return GoalCreateOutcome::InvalidObjective("Goal mode is disabled".into());
        }
        if let Some(identity) = identity {
            let current = match store::get_record(&self.thread_key()).await {
                Ok(record) if record.intent_revision == identity.durable_intent => record.goal.map(|goal| goal.goal_id),
                Ok(_) => return GoalCreateOutcome::InvalidObjective("The goal intent changed after this request; start a new explicitly authorized turn before creating a goal.".into()),
                Err(error) => {
                    return GoalCreateOutcome::InvalidObjective(format!(
                        "goal could not be read: {error}"
                    ))
                }
            };
            if identity.intent != self.intent.load(Ordering::SeqCst)
                || identity.pursuit_epoch != self.resume_epoch()
                || identity.generation != current
            {
                return GoalCreateOutcome::InvalidObjective(
                    "The goal intent changed after this request; start a new explicitly authorized turn before creating a goal.".into(),
                );
            }
        }
        let objective = match Self::validate_objective(&objective) {
            Ok(value) => value,
            Err(error) => return GoalCreateOutcome::InvalidObjective(error),
        };
        let budget = match self.valid_budget(token_budget) {
            Ok(value) => value,
            Err(error) => return GoalCreateOutcome::InvalidObjective(error),
        };
        let now = self.now_ms();
        let goal = ThreadGoal {
            thread_id: self.thread_id.clone(),
            goal_id: (self.uuid)().into(),
            objective,
            status: ThreadGoalStatus::Active,
            token_budget: budget,
            tokens_used: 0,
            time_used_seconds: 0,
            created_at_ms: now,
            updated_at_ms: now,
        };
        if require_safe_json {
            if let Err(error) = crate::goal::tools::validate_goal_result(&goal) {
                return GoalCreateOutcome::InvalidObjective(error);
            }
        }
        match store::insert_goal_intent_guarded(
            goal,
            identity.and_then(|identity| identity.generation.clone()),
            identity.is_some_and(|identity| identity.generation.is_none()),
            None,
            identity.map(|identity| identity.durable_intent),
            control_guard,
        )
        .await
        {
            Ok(Ok(created)) => {
                self.resume_pursuit(pursuit_epoch);
                self.intent.fetch_add(1, Ordering::SeqCst);
                if self.execution_enabled.load(Ordering::SeqCst) && !self.continuation_suppressed()
                {
                    self.accounting
                        .mark_goal_active_from_now(&created.goal_id, self.elapsed_ms());
                }
                {
                    let mut safety = self.safety();
                    safety.generation = Some(created.goal_id.clone());
                    safety.empty_turns = 0;
                    safety.failed_execution_turns = 0;
                }
                match self.publish().await {
                    Ok(_) => {
                        self.safety().turn_intent = identity
                            .map(|identity| {
                                identity
                                    .durable_intent
                                    .checked_add(1)
                                    .expect("store checked the control epoch")
                            })
                            .unwrap_or_else(|| self.durable_intent.load(Ordering::SeqCst));
                        GoalCreateOutcome::Created(created)
                    }
                    Err(error) => GoalCreateOutcome::InvalidObjective(format!(
                        "goal notification failed: {error}"
                    )),
                }
            }
            Ok(Err(store::GoalInsertRejection::Unfinished(existing))) => {
                GoalCreateOutcome::Rejected(existing)
            }
            Ok(Err(store::GoalInsertRejection::GenerationChanged(_))) => {
                GoalCreateOutcome::InvalidObjective("the goal changed during creation".into())
            }
            Err(error) => {
                GoalCreateOutcome::InvalidObjective(format!("goal could not be stored: {error}"))
            }
        }
    }
    pub async fn validate_host_wire_mutation(
        &self,
        mutation: &crate::goal::service::HostGoalMutation,
    ) -> io::Result<()> {
        let _permit = self.permit().await;
        let previous = self.goal().await?;
        let creating = previous
            .as_ref()
            .is_none_or(|goal| goal.status == ThreadGoalStatus::Complete);
        let mut goal = if creating {
            ThreadGoal {
                thread_id: self.thread_id.clone(),
                goal_id: RuntimeString::default(),
                objective: mutation.objective.clone().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "supply an objective to create a goal",
                    )
                })?,
                status: ThreadGoalStatus::Active,
                token_budget: self
                    .valid_budget(mutation.token_budget.flatten())
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?,
                tokens_used: 0,
                time_used_seconds: 0,
                created_at_ms: self.now_ms(),
                updated_at_ms: self.now_ms(),
            }
        } else {
            previous.expect("existing goal checked")
        };
        if let Some(objective) = &mutation.objective {
            goal.objective = Self::validate_objective(objective)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        }
        if let Some(status) = crate::goal::service::parse_host_status(mutation.status.as_ref())? {
            goal.status = status;
        }
        if let Some(budget) = mutation.token_budget {
            goal.token_budget = self
                .valid_budget(budget)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        }
        goal.updated_at_ms = self.now_ms();
        crate::goal::tools::validate_goal_result(&goal)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))
    }
    /// Commit an explicit submitted objective and its retry receipt atomically.
    pub async fn apply_host_update_once(
        &self,
        mutation: crate::goal::service::HostGoalMutation,
        request_id: &str,
    ) -> io::Result<(GoalHostUpdate, bool)> {
        self.apply_host_update_once_for_intent(mutation, request_id, None)
            .await
    }
    pub(crate) async fn apply_host_update_once_for_intent(
        &self,
        mutation: crate::goal::service::HostGoalMutation,
        request_id: &str,
        expected_intent: Option<i64>,
    ) -> io::Result<(GoalHostUpdate, bool)> {
        let pursuit_epoch = self.resume_epoch();
        let _permit = self.permit().await;
        let fingerprint = crate::goal::service::request_fingerprint(&mutation)?;
        if let Some(receipt) =
            store::get_goal_request_receipt(&self.thread_key(), request_id).await?
        {
            if receipt.fingerprint != fingerprint {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "clientRequestId was already used for a different goal request",
                ));
            }
            return Ok((
                GoalHostUpdate {
                    goal: receipt.goal,
                    revision: receipt.revision,
                    steer: None,
                    turn_in_flight: self.turn_in_flight(),
                },
                true,
            ));
        }
        let resource_intent = mutation.objective.is_some();
        let resources = self
            .resource_update(resource_intent, mutation.owned_resources.clone())
            .await?;
        let status = crate::goal::service::parse_host_status(mutation.status.as_ref())?;
        let objective = mutation
            .objective
            .as_ref()
            .map(Self::validate_objective)
            .transpose()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        let budget = mutation
            .token_budget
            .map(|budget| self.valid_budget(budget))
            .transpose()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        let previous = self.goal().await?;
        let creating = previous
            .as_ref()
            .is_none_or(|goal| goal.status == ThreadGoalStatus::Complete);
        let request = if creating {
            let objective = objective.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "supply an objective to create a goal",
                )
            })?;
            let status = status.unwrap_or(ThreadGoalStatus::Active);
            let now = self.now_ms();
            let token_budget = self
                .valid_budget(budget.flatten())
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
            let goal = ThreadGoal {
                thread_id: self.thread_id.clone(),
                goal_id: (self.uuid)().into(),
                objective,
                status,
                token_budget,
                tokens_used: 0,
                time_used_seconds: 0,
                created_at_ms: now,
                updated_at_ms: now,
            };
            if mutation.require_safe_json {
                crate::goal::tools::validate_goal_result(&goal)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
            }
            store::GoalRequestMutation::CreateChecked {
                goal,
                expected_goal_id: mutation.expected_goal_id,
                expected_absent: mutation.expected_absent,
                expected_revision: mutation.expected_revision,
                expected_intent_revision: expected_intent,
            }
        } else {
            if mutation.expected_absent {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "a goal already exists",
                ));
            }
            store::GoalRequestMutation::Update {
                update: GoalUpdate {
                    objective,
                    status,
                    token_budget: budget,
                    expected_goal_id: mutation
                        .expected_goal_id
                        .or_else(|| previous.as_ref().map(|goal| goal.goal_id.clone())),
                    expected_revision: mutation.expected_revision,
                    expected_intent_revision: expected_intent,
                    require_safe_json: mutation.require_safe_json,
                    control_guard: mutation.control_guard,
                },
                now_ms: self.now_ms(),
            }
        };
        let control = self
            .apply_control_locked(
                store::GoalControlMutation::Set(request),
                Some(store::GoalControlReceipt {
                    request_id: request_id.to_owned(),
                    fingerprint,
                }),
                resources,
            )
            .await?;
        let outcome = control
            .receipt
            .ok_or_else(|| io::Error::other("goal submission receipt is missing"))?;
        let goal = outcome.goal;
        if outcome.replayed {
            return Ok((
                GoalHostUpdate {
                    goal,
                    revision: outcome.revision,
                    steer: None,
                    turn_in_flight: self.turn_in_flight(),
                },
                true,
            ));
        }
        if previous.as_ref().is_none_or(|old| {
            resource_intent || old.objective != goal.objective || old.status != goal.status
        }) {
            self.intent.fetch_add(1, Ordering::SeqCst);
        }
        if goal.status.is_active() && (creating || status == Some(ThreadGoalStatus::Active)) {
            self.resume_pursuit(pursuit_epoch);
        }
        if creating {
            self.intent.fetch_add(1, Ordering::SeqCst);
            self.callback_epoch.fetch_add(1, Ordering::SeqCst);
        }
        let in_flight = self.turn_in_flight();
        let deferred = store::get_record(&self.thread_key())
            .await?
            .continuation_deferred;
        if self.enabled()
            && !self.continuation_suppressed()
            && !deferred
            && self.execution_enabled.load(Ordering::SeqCst)
            && goal.status.is_active()
            && (!creating || !in_flight)
        {
            if previous
                .as_ref()
                .is_some_and(|old| old.goal_id == goal.goal_id && old.status.is_active())
                && self.active_generation().as_ref() == Some(&goal.goal_id)
            {
                self.accounting.mark_goal_active(&goal.goal_id);
            } else {
                self.accounting
                    .mark_goal_active_from_now(&goal.goal_id, self.elapsed_ms());
            }
        } else {
            self.accounting.clear_active_goal_at(self.elapsed_ms());
        }
        {
            let mut safety = self.safety();
            safety.empty_turns = 0;
            safety.failed_execution_turns = 0;
        }
        self.publish().await?;
        let resumed = previous.as_ref().is_none_or(|old| !old.status.is_active());
        let changed = previous
            .as_ref()
            .is_some_and(|old| old.objective != goal.objective);
        let steer =
            if self.enabled() && !deferred && goal.status.is_active() && (creating || resumed) {
                Some(self.continuation_prompt(&goal))
            } else if self.enabled() && !deferred && goal.status.is_active() && changed {
                Some(objective_updated_prompt(&goal))
            } else {
                None
            };
        Ok((
            GoalHostUpdate {
                goal,
                revision: outcome.revision,
                steer,
                turn_in_flight: in_flight,
            },
            false,
        ))
    }

    pub async fn create_host_goal(
        &self,
        objective: RuntimeString,
        status: Option<ThreadGoalStatus>,
        token_budget: Option<i64>,
        expected_goal_id: Option<RuntimeString>,
    ) -> io::Result<Option<GoalHostUpdate>> {
        self.create_host_goal_guarded(objective, status, token_budget, expected_goal_id, false)
            .await
    }
    pub async fn create_host_goal_guarded(
        &self,
        objective: RuntimeString,
        status: Option<ThreadGoalStatus>,
        token_budget: Option<i64>,
        expected_goal_id: Option<RuntimeString>,
        expected_absent: bool,
    ) -> io::Result<Option<GoalHostUpdate>> {
        self.create_host_goal_checked(
            objective,
            status,
            token_budget,
            expected_goal_id,
            expected_absent,
            None,
        )
        .await
    }
    pub async fn create_host_goal_checked(
        &self,
        objective: RuntimeString,
        status: Option<ThreadGoalStatus>,
        token_budget: Option<i64>,
        expected_goal_id: Option<RuntimeString>,
        expected_absent: bool,
        expected_revision: Option<i64>,
    ) -> io::Result<Option<GoalHostUpdate>> {
        self.create_host_goal_with_resources(
            objective,
            status,
            token_budget,
            GoalCreatePreconditions {
                expected_goal_id,
                expected_absent,
                expected_revision,
            },
            None,
        )
        .await
    }
    pub async fn create_host_goal_with_resources(
        &self,
        objective: RuntimeString,
        status: Option<ThreadGoalStatus>,
        token_budget: Option<i64>,
        preconditions: GoalCreatePreconditions,
        owned_resources: Option<crate::goal::owned_resources::GoalOwnedResourcesInput>,
    ) -> io::Result<Option<GoalHostUpdate>> {
        self.create_host_goal_with_resources_checked(
            objective,
            status,
            token_budget,
            preconditions,
            owned_resources,
            false,
        )
        .await
    }
    pub async fn create_host_goal_with_resources_checked(
        &self,
        objective: RuntimeString,
        status: Option<ThreadGoalStatus>,
        token_budget: Option<i64>,
        preconditions: GoalCreatePreconditions,
        owned_resources: Option<crate::goal::owned_resources::GoalOwnedResourcesInput>,
        require_safe_json: bool,
    ) -> io::Result<Option<GoalHostUpdate>> {
        let GoalCreatePreconditions {
            expected_goal_id,
            expected_absent,
            expected_revision,
        } = preconditions;
        let pursuit_epoch = self.resume_epoch();
        let _permit = self.permit().await;
        let objective = Self::validate_objective(&objective)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        let token_budget = self
            .valid_budget(token_budget)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        let status = status.unwrap_or(ThreadGoalStatus::Active);
        let resources = self.resource_update(true, owned_resources).await?;
        let now = self.now_ms();
        let goal = ThreadGoal {
            thread_id: self.thread_id.clone(),
            goal_id: (self.uuid)().into(),
            objective,
            status,
            token_budget,
            tokens_used: 0,
            time_used_seconds: 0,
            created_at_ms: now,
            updated_at_ms: now,
        };
        if require_safe_json {
            crate::goal::tools::validate_goal_result(&goal)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        }
        let control = self
            .apply_control_locked(
                store::GoalControlMutation::Set(store::GoalRequestMutation::CreateChecked {
                    goal,
                    expected_goal_id,
                    expected_absent,
                    expected_revision,
                    expected_intent_revision: None,
                }),
                None,
                resources,
            )
            .await?;
        let goal = control
            .record
            .goal
            .ok_or_else(|| io::Error::other("goal creation produced no goal"))?;
        if goal.status.is_active() {
            self.resume_pursuit(pursuit_epoch);
        }
        self.intent.fetch_add(1, Ordering::SeqCst);
        self.callback_epoch.fetch_add(1, Ordering::SeqCst);
        let in_flight = self.safety().in_flight;
        // Host creation never rebinds an already-running model's tool scope.
        if self.enabled()
            && !self.continuation_suppressed()
            && self.execution_enabled.load(Ordering::SeqCst)
            && goal.status.is_active()
            && !in_flight
        {
            self.accounting
                .mark_goal_active_from_now(&goal.goal_id, self.elapsed_ms());
        }
        let snapshot = self.publish().await?;
        if snapshot.goal.as_ref() != Some(&goal) {
            return Err(io::Error::other(
                "the goal changed after this mutation; reload its authoritative snapshot",
            ));
        }
        Ok(Some(GoalHostUpdate {
            steer: (self.enabled() && goal.status.is_active())
                .then(|| self.continuation_prompt(&goal)),
            goal,
            turn_in_flight: in_flight,
            revision: snapshot.revision,
        }))
    }
    pub async fn update_goal_status(&self, status: ThreadGoalStatus) -> GoalUpdateOutcome {
        let expected = self.active_generation();
        self.update_goal_status_for(status, expected.as_ref()).await
    }
    pub async fn update_goal_status_for(
        &self,
        status: ThreadGoalStatus,
        expected: Option<&RuntimeString>,
    ) -> GoalUpdateOutcome {
        self.update_goal_status_checked(status, expected, None)
            .await
    }
    pub async fn update_goal_status_checked(
        &self,
        status: ThreadGoalStatus,
        expected: Option<&RuntimeString>,
        intent: Option<u64>,
    ) -> GoalUpdateOutcome {
        self.update_goal_status_with_intent(status, expected, intent, None, false, None)
            .await
    }
    pub async fn update_goal_status_for_identity(
        &self,
        status: ThreadGoalStatus,
        identity: &GoalRequestIdentity,
    ) -> GoalUpdateOutcome {
        self.update_goal_status_with_intent(
            status,
            identity.generation.as_ref(),
            Some(identity.intent),
            Some(identity.durable_intent),
            true,
            None,
        )
        .await
    }
    pub async fn update_goal_status_for_tool(
        &self,
        status: ThreadGoalStatus,
        identity: Option<&GoalRequestIdentity>,
    ) -> GoalUpdateOutcome {
        self.update_goal_status_with_intent(
            status,
            identity.and_then(|identity| identity.generation.as_ref()),
            identity.map(|identity| identity.intent),
            identity.map(|identity| identity.durable_intent),
            true,
            None,
        )
        .await
    }
    pub async fn update_goal_status_for_tool_with_signal(
        &self,
        status: ThreadGoalStatus,
        identity: Option<&GoalRequestIdentity>,
        signal: &crate::types::abort::AbortSignal,
    ) -> GoalUpdateOutcome {
        if signal.is_cancelled() {
            return GoalUpdateOutcome::Forbidden("The goal request was cancelled".into());
        }
        self.update_goal_status_with_intent(
            status,
            identity.and_then(|identity| identity.generation.as_ref()),
            identity.map(|identity| identity.intent),
            identity.map(|identity| identity.durable_intent),
            true,
            Some(self.request_control_guard(signal, identity)),
        )
        .await
    }
    async fn update_goal_status_with_intent(
        &self,
        status: ThreadGoalStatus,
        expected: Option<&RuntimeString>,
        intent: Option<u64>,
        durable_intent: Option<i64>,
        require_safe_json: bool,
        control_guard: Option<store::GoalControlGuard>,
    ) -> GoalUpdateOutcome {
        if !matches!(
            status,
            ThreadGoalStatus::Complete | ThreadGoalStatus::Blocked | ThreadGoalStatus::Paused
        ) {
            return GoalUpdateOutcome::Forbidden(format!(
                "status `{}` is controlled by the user or system",
                status.wire_name()
            ));
        }
        let _permit = self.permit().await;
        if !self.enabled() {
            return GoalUpdateOutcome::Forbidden("Goal mode is disabled".into());
        }
        if intent.is_some_and(|epoch| epoch != self.intent.load(Ordering::SeqCst)) {
            return GoalUpdateOutcome::Forbidden("The goal intent changed after this request; read the updated objective in the next request.".into());
        }
        if require_safe_json {
            match self.goal().await {
                Ok(Some(mut goal)) => {
                    goal.updated_at_ms = self.now_ms();
                    if let Err(error) = crate::goal::tools::validate_goal_result(&goal) {
                        return GoalUpdateOutcome::Forbidden(error);
                    }
                }
                Ok(None) => return GoalUpdateOutcome::Missing,
                Err(error) => {
                    return GoalUpdateOutcome::Forbidden(format!("goal could not be read: {error}"))
                }
            }
        }
        if let Err(error) = self
            .account_progress_locked(
                if status == ThreadGoalStatus::Complete {
                    GoalAccountingMode::ActiveOrComplete
                } else {
                    GoalAccountingMode::ActiveOrStopped
                },
                BudgetLimitedGoalDisposition::ClearActive,
            )
            .await
        {
            return GoalUpdateOutcome::Forbidden(format!("goal accounting failed: {error}"));
        }
        let existing = match self.goal().await {
            Ok(Some(value)) => value,
            Ok(None) => return GoalUpdateOutcome::Missing,
            Err(error) => {
                return GoalUpdateOutcome::Forbidden(format!("goal could not be read: {error}"))
            }
        };
        if require_safe_json && existing.status == ThreadGoalStatus::Complete {
            return GoalUpdateOutcome::Forbidden(
                "The goal is already complete; no completion mutation was performed".into(),
            );
        }
        if existing.status == ThreadGoalStatus::BudgetLimited
            && status != ThreadGoalStatus::Complete
        {
            return GoalUpdateOutcome::Forbidden("The token budget limit takes precedence; preserve budget_limited unless the full objective is achieved.".into());
        }
        if expected.is_some_and(|id| id != &existing.goal_id) {
            return GoalUpdateOutcome::Forbidden(
                "the goal generation changed; read get_goal again".into(),
            );
        }
        match store::update_goal(
            &self.thread_key(),
            GoalUpdate {
                status: Some(status),
                expected_goal_id: Some(existing.goal_id.clone()),
                expected_intent_revision: durable_intent,
                require_safe_json,
                control_guard,
                ..Default::default()
            },
            self.now_ms(),
        )
        .await
        {
            Ok(Ok(Some(goal))) => {
                self.accounting.clear_active_goal_at(self.elapsed_ms());
                match self.publish().await {
                    Ok(_) => GoalUpdateOutcome::Updated(goal),
                    Err(error) => {
                        GoalUpdateOutcome::Forbidden(format!("goal notification failed: {error}"))
                    }
                }
            }
            Ok(Ok(None)) => GoalUpdateOutcome::Missing,
            Ok(Err(_)) => GoalUpdateOutcome::Forbidden("goal changed during update".into()),
            Err(error) => {
                GoalUpdateOutcome::Forbidden(format!("goal could not be stored: {error}"))
            }
        }
    }
    pub async fn apply_host_update(
        &self,
        update: GoalUpdate,
    ) -> io::Result<Option<GoalHostUpdate>> {
        self.apply_host_update_resources(update, None).await
    }
    pub async fn apply_host_update_resources(
        &self,
        mut update: GoalUpdate,
        owned_resources: Option<crate::goal::owned_resources::GoalOwnedResourcesInput>,
    ) -> io::Result<Option<GoalHostUpdate>> {
        let pursuit_epoch = self.resume_epoch();
        let _permit = self.permit().await;
        if let Some(objective) = &update.objective {
            update.objective = Some(
                Self::validate_objective(objective)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?,
            );
        }
        if let Some(budget) = update.token_budget {
            update.token_budget = Some(
                self.valid_budget(budget)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?,
            );
        }
        let resume_requested = update.status == Some(ThreadGoalStatus::Active);
        let resource_intent = update.objective.is_some();
        let resources = self
            .resource_update(resource_intent, owned_resources)
            .await?;
        let Some(previous) = self.goal().await? else {
            return Ok(None);
        };
        let objective_changed = update
            .objective
            .as_ref()
            .is_some_and(|value| value != &previous.objective);
        if update.expected_goal_id.is_none() {
            update.expected_goal_id = Some(previous.goal_id.clone());
        }
        let control = self
            .apply_control_locked(
                store::GoalControlMutation::Set(store::GoalRequestMutation::Update {
                    update,
                    now_ms: self.now_ms(),
                }),
                None,
                resources,
            )
            .await?;
        let Some(goal) = control.record.goal else {
            return Ok(None);
        };
        if goal.status.is_active() && resume_requested {
            self.resume_pursuit(pursuit_epoch);
        }
        let deferred = store::get_record(&self.thread_key())
            .await?
            .continuation_deferred;
        if self.enabled()
            && !self.continuation_suppressed()
            && !deferred
            && self.execution_enabled.load(Ordering::SeqCst)
            && goal.status.is_active()
        {
            if previous.status.is_active()
                && self.active_generation().as_ref() == Some(&goal.goal_id)
            {
                self.accounting.mark_goal_active(&goal.goal_id);
            } else {
                self.accounting
                    .mark_goal_active_from_now(&goal.goal_id, self.elapsed_ms());
            }
        } else {
            self.accounting.clear_active_goal_at(self.elapsed_ms());
        }
        if resource_intent || previous.objective != goal.objective || previous.status != goal.status
        {
            self.intent.fetch_add(1, Ordering::SeqCst);
        }
        let in_flight = {
            let mut safety = self.safety();
            safety.empty_turns = 0;
            safety.failed_execution_turns = 0;
            safety.in_flight
        };
        let snapshot = self.publish().await?;
        if snapshot.goal.as_ref() != Some(&goal) {
            return Err(io::Error::other(
                "the goal changed after this mutation; reload its authoritative snapshot",
            ));
        }
        let steer = if self.enabled()
            && !deferred
            && goal.status.is_active()
            && !previous.status.is_active()
        {
            Some(self.continuation_prompt(&goal))
        } else if self.enabled() && !deferred && objective_changed && goal.status.is_active() {
            Some(objective_updated_prompt(&goal))
        } else {
            None
        };
        Ok(Some(GoalHostUpdate {
            goal,
            steer,
            turn_in_flight: in_flight,
            revision: snapshot.revision,
        }))
    }
    pub async fn clear_goal(&self) -> io::Result<Option<ThreadGoal>> {
        self.clear_goal_if(None)
            .await?
            .map_err(|_| io::Error::other("goal changed before clear"))
    }
    pub async fn clear_goal_if(
        &self,
        expected: Option<RuntimeString>,
    ) -> io::Result<Result<Option<ThreadGoal>, ThreadGoal>> {
        self.clear_goal_if_revision(expected, None).await
    }
    pub async fn clear_goal_if_revision(
        &self,
        expected: Option<RuntimeString>,
        expected_revision: Option<i64>,
    ) -> io::Result<Result<Option<ThreadGoal>, ThreadGoal>> {
        let _permit = self.permit().await;
        let control = self
            .apply_control_locked(
                store::GoalControlMutation::Clear {
                    expected_goal_id: expected,
                    expected_revision,
                    expected_intent_revision: None,
                },
                None,
                None,
            )
            .await?;
        let result: Result<Option<ThreadGoal>, ThreadGoal> = Ok(control.removed_goal);
        if result.is_ok() {
            self.intent.fetch_add(1, Ordering::SeqCst);
            self.callback_epoch.fetch_add(1, Ordering::SeqCst);
            self.accounting.clear_active_goal_at(self.elapsed_ms());
            *self.safety() = SafetyState::default();
            self.publish().await?;
        }
        Ok(result)
    }
}
/// Holds a child's root generation through nested spawning and detached work.
pub struct GoalDescendantLease {
    runtime: std::sync::Weak<GoalRuntime>,
    agent: String,
    pub generation: Option<RuntimeString>,
}
impl Drop for GoalDescendantLease {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.upgrade() {
            runtime
                .descendants
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&self.agent);
            runtime.notify_idle();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::message::Usage;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
    use std::sync::Mutex;

    use crate::test_support::CONFIG_ENV_LOCK as ENV_LOCK;

    struct EnvGuard {
        _lock: std::sync::MutexGuard<'static, ()>,
        prev: Option<String>,
        home: PathBuf,
    }

    impl EnvGuard {
        fn temp_home(tag: &str) -> Self {
            static SEQ: AtomicUsize = AtomicUsize::new(0);
            let lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            let prev = std::env::var("KLAUS_CONFIG_DIR").ok();
            let mut home = std::env::temp_dir();
            home.push(format!(
                "klaus-goal-runtime-{tag}-{}-{}",
                std::process::id(),
                SEQ.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&home).unwrap();
            crate::runtime_process::set_process_environment_var(
                "KLAUS_CONFIG_DIR",
                &*home.to_string_lossy(),
            );
            EnvGuard {
                _lock: lock,
                prev,
                home,
            }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.home);
            match &self.prev {
                Some(v) => {
                    crate::runtime_process::set_process_environment_var("KLAUS_CONFIG_DIR", v)
                }
                None => crate::runtime_process::remove_process_environment_var("KLAUS_CONFIG_DIR"),
            }
        }
    }

    struct Harness {
        runtime: Arc<GoalRuntime>,
        clock: Arc<AtomicI64>,
        published: Arc<Mutex<Vec<ThreadGoal>>>,
    }

    fn harness(thread: &str) -> Harness {
        let clock = Arc::new(AtomicI64::new(0));
        let published: Arc<Mutex<Vec<ThreadGoal>>> = Arc::new(Mutex::new(Vec::new()));
        let seq = Arc::new(AtomicUsize::new(0));
        let uuid: UuidFn = {
            let seq = seq.clone();
            Arc::new(move || format!("goal-{}", seq.fetch_add(1, Ordering::Relaxed)))
        };
        let now: Arc<dyn Fn() -> i64 + Send + Sync> = {
            let clock = clock.clone();
            Arc::new(move || clock.load(Ordering::Relaxed))
        };
        let observer: GoalObserver = {
            let published = published.clone();
            Arc::new(move |snapshot| {
                if let Some(goal) = snapshot.goal {
                    published.lock().unwrap().push(goal);
                }
            })
        };
        Harness {
            runtime: Arc::new(GoalRuntime::new(thread.into(), uuid, now, Some(observer))),
            clock,
            published,
        }
    }

    fn usage(input: u64, output: u64) -> Usage {
        Usage {
            input_tokens: input,
            output_tokens: output,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
            total_tokens: None,
        }
    }

    #[tokio::test]
    async fn budget_delivery_revisions_are_published_and_repeated_checks_are_noops() {
        let _g = EnvGuard::temp_home("wrapup-revision");
        let published = Arc::new(Mutex::new(Vec::<GoalSnapshot>::new()));
        let runtime = GoalRuntime::new(
            "wrapup-revision".into(),
            Arc::new(|| "wrapup-generation".to_owned()),
            Arc::new(|| 0),
            Some(Arc::new({
                let published = published.clone();
                move |snapshot| {
                    let mut published = published.lock().unwrap();
                    assert!(published.len() < 32);
                    published.push(snapshot);
                }
            })),
        );
        let created = runtime
            .create_host_goal(
                "Deliver the budget report".into(),
                Some(ThreadGoalStatus::BudgetLimited),
                Some(10),
                None,
            )
            .await
            .unwrap()
            .unwrap();
        // Current writers create delivery metadata atomically. Reconstruct a
        // saved budget stop whose older delivery metadata is absent.
        let removed = rusqlite::Connection::open(_g.home.join("goals.sqlite3"))
            .unwrap()
            .execute(
                "DELETE FROM goal_budget_wrapups WHERE thread_id = ?1",
                ["wrapup-revision"],
            )
            .unwrap();
        assert_eq!(
            removed, 1,
            "host creation already installed the pending record"
        );
        assert!(matches!(
            runtime.continuation_if_idle(false).await.unwrap(),
            GoalContinuation::Continue { .. }
        ));
        let pending = runtime.snapshot().await.unwrap();
        assert_eq!(pending.revision, created.revision + 1);
        assert_eq!(published.lock().unwrap().last(), Some(&pending));
        let count = published.lock().unwrap().len();
        assert!(matches!(
            runtime.continuation_if_idle(false).await.unwrap(),
            GoalContinuation::Continue { .. }
        ));
        assert_eq!(runtime.snapshot().await.unwrap(), pending);
        assert_eq!(published.lock().unwrap().len(), count);

        runtime
            .on_turn_start_for(true, Some(&created.goal.goal_id), true)
            .await
            .unwrap();
        let id = RuntimeString::from("wrapup-context");
        runtime.register_context_input(id.clone(), true);
        let messages = [crate::schemas::message::Message::User(
            crate::schemas::message::UserMessage {
                uuid: id,
                content: Vec::new(),
                is_meta: Some(true),
                question_answer: None,
            },
        )];
        let identity = Mutex::new(runtime.request_identity());
        let lease = runtime
            .prepare_provider_request(&messages, &identity, None, None)
            .await
            .unwrap();
        let claimed = runtime.snapshot().await.unwrap();
        assert_eq!(claimed.revision, pending.revision + 1);
        lease.accepted(&runtime).await.unwrap();
        let accepted = runtime.snapshot().await.unwrap();
        assert_eq!(accepted.revision, claimed.revision + 1);
        assert_eq!(published.lock().unwrap().last(), Some(&accepted));
        let count = published.lock().unwrap().len();
        runtime
            .prepare_provider_request(&messages, &identity, None, None)
            .await
            .unwrap()
            .accepted(&runtime)
            .await
            .unwrap();
        assert_eq!(runtime.snapshot().await.unwrap(), accepted);
        assert_eq!(
            published.lock().unwrap().len(),
            count,
            "retained claims and delivered acknowledgements do not publish no-ops"
        );
        runtime.on_turn_aborted().await.unwrap();
        let released = runtime.snapshot().await.unwrap();
        assert_eq!(released.revision, accepted.revision + 1);
        assert_eq!(published.lock().unwrap().last(), Some(&released));
        let count = published.lock().unwrap().len();
        runtime.on_turn_aborted().await.unwrap();
        assert_eq!(runtime.snapshot().await.unwrap(), released);
        assert_eq!(
            published.lock().unwrap().len(),
            count,
            "a repeated release is not a mutation"
        );
    }

    #[tokio::test]
    async fn an_explicit_turn_publishes_the_saved_deferral_release_revision() {
        let _g = EnvGuard::temp_home("deferral-revision");
        let published = Arc::new(Mutex::new(Vec::<GoalSnapshot>::new()));
        let runtime = GoalRuntime::new(
            "deferral-revision".into(),
            Arc::new(|| "deferred-generation".to_owned()),
            Arc::new(|| 0),
            Some(Arc::new({
                let published = published.clone();
                move |snapshot| {
                    let mut published = published.lock().unwrap();
                    assert!(published.len() < 32);
                    published.push(snapshot);
                }
            })),
        );
        let created = runtime
            .create_host_goal(
                "Resume a saved deferred goal".into(),
                Some(ThreadGoalStatus::Paused),
                None,
                None,
            )
            .await
            .unwrap()
            .unwrap();
        store::set_continuation_deferral_if(
            "deferral-revision",
            true,
            Some(created.goal.goal_id.clone()),
        )
        .await
        .unwrap()
        .unwrap();
        let deferred = runtime.snapshot().await.unwrap();
        runtime.on_turn_start_for(false, None, false).await.unwrap();
        let released = runtime.snapshot().await.unwrap();
        assert_eq!(released.revision, deferred.revision + 1);
        assert_eq!(released.goal, deferred.goal);
        assert!(!store::has_continuation_deferral("deferral-revision")
            .await
            .unwrap());
        assert_eq!(published.lock().unwrap().last(), Some(&released));
        runtime.on_turn_aborted().await.unwrap();
        let count = published.lock().unwrap().len();
        runtime.on_turn_start_for(false, None, false).await.unwrap();
        assert_eq!(runtime.snapshot().await.unwrap(), released);
        assert_eq!(
            published.lock().unwrap().len(),
            count,
            "an already released deferral does not publish a no-op"
        );
        runtime.on_turn_aborted().await.unwrap();
    }

    #[tokio::test]
    async fn create_then_continue_until_the_model_completes() {
        let _g = EnvGuard::temp_home("continue");
        let h = harness("t1");

        h.runtime.on_turn_start().await.unwrap();
        let created = match h.runtime.create_goal("ship it".into(), None).await {
            GoalCreateOutcome::Created(goal) => goal,
            other => panic!("expected a create, got {other:?}"),
        };
        assert_eq!(created.status, ThreadGoalStatus::Active);

        h.runtime.on_token_usage(&usage(100, 50));
        h.clock.store(3_000, Ordering::Relaxed);
        match h
            .runtime
            .on_turn_end(/*pending_user_input*/ false)
            .await
            .unwrap()
        {
            GoalContinuation::Continue { goal, prompt } => {
                assert_eq!(goal.tokens_used, 150);
                assert_eq!(goal.time_used_seconds, 3);
                assert!(prompt.contains("Continue working toward the active thread goal."));
            }
            GoalContinuation::Stop => panic!("an active goal must continue"),
        }

        // The continuation turn completes the goal.
        h.runtime.on_turn_start().await.unwrap();
        h.runtime.on_token_usage(&usage(20, 10));
        h.clock.store(4_000, Ordering::Relaxed);
        match h
            .runtime
            .update_goal_status(ThreadGoalStatus::Complete)
            .await
        {
            GoalUpdateOutcome::Updated(goal) => {
                assert_eq!(goal.status, ThreadGoalStatus::Complete);
                assert_eq!(goal.tokens_used, 180, "the completing turn is charged too");
            }
            other => panic!("expected an update, got {other:?}"),
        }
        assert_eq!(
            h.runtime.on_turn_end(false).await.unwrap(),
            GoalContinuation::Stop,
            "a complete goal does not continue"
        );
    }

    #[tokio::test]
    async fn a_pending_user_turn_outranks_the_automatic_one() {
        let _g = EnvGuard::temp_home("pending");
        let h = harness("t1");
        h.runtime.on_turn_start().await.unwrap();
        h.runtime.create_goal("ship it".into(), None).await;
        assert_eq!(
            h.runtime
                .on_turn_end(/*pending_user_input*/ true)
                .await
                .unwrap(),
            GoalContinuation::Stop
        );
    }

    #[tokio::test]
    async fn the_model_may_not_resume_or_set_system_limits() {
        let _g = EnvGuard::temp_home("forbidden");
        let h = harness("t1");
        h.runtime.on_turn_start().await.unwrap();
        h.runtime.create_goal("ship it".into(), None).await;
        for forbidden in [
            ThreadGoalStatus::Active,
            ThreadGoalStatus::BudgetLimited,
            ThreadGoalStatus::UsageLimited,
        ] {
            match h.runtime.update_goal_status(forbidden).await {
                GoalUpdateOutcome::Forbidden(message) => {
                    assert!(message.contains(forbidden.wire_name()), "{message}");
                }
                other => panic!("expected a refusal for {forbidden:?}, got {other:?}"),
            }
        }
        assert_eq!(
            h.runtime.goal().await.unwrap().unwrap().status,
            ThreadGoalStatus::Active
        );
    }

    #[tokio::test]
    async fn stale_model_creation_cannot_restore_a_cleared_goal() {
        let _g = EnvGuard::temp_home("stale-create");
        let h = harness("t1");
        h.runtime.on_turn_start().await.unwrap();
        let captured = h.runtime.request_identity();
        h.runtime.clear_goal().await.unwrap();
        assert!(matches!(
            h.runtime
                .create_goal_checked("Stale intent".into(), None, Some(&captured))
                .await,
            GoalCreateOutcome::InvalidObjective(_)
        ));
        assert!(h.runtime.goal().await.unwrap().is_none());
        let current = h.runtime.request_identity();
        assert!(matches!(
            h.runtime
                .create_goal_checked("Current intent".into(), None, Some(&current))
                .await,
            GoalCreateOutcome::Created(_)
        ));
    }

    #[tokio::test]
    async fn a_second_goal_is_refused_while_the_first_is_unfinished() {
        let _g = EnvGuard::temp_home("second");
        let h = harness("t1");
        h.runtime.on_turn_start().await.unwrap();
        h.runtime.create_goal("first".into(), None).await;
        match h.runtime.create_goal("second".into(), None).await {
            GoalCreateOutcome::Rejected(existing) => {
                assert_eq!(existing.objective, RuntimeString::from("first"));
            }
            other => panic!("expected a rejection, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn the_budget_stop_wraps_up_once_and_ends_continuations() {
        let _g = EnvGuard::temp_home("budget");
        let h = harness("t1");
        h.runtime.on_turn_start().await.unwrap();
        h.runtime.create_goal("ship it".into(), Some(100)).await;

        h.runtime.on_token_usage(&usage(80, 40));
        let first = h
            .runtime
            .account_progress(
                GoalAccountingMode::ActiveOnly,
                BudgetLimitedGoalDisposition::KeepActive,
            )
            .await
            .unwrap();
        let wrap_up = first.expect("the budget stop is announced");
        assert!(wrap_up.contains("reached its token budget"));

        store::acknowledge_budget_wrapup("t1", &h.runtime.goal().await.unwrap().unwrap().goal_id)
            .await
            .unwrap();
        // A second pass after provider acceptance does not repeat the announcement.
        h.runtime.on_token_usage(&usage(200, 40));
        let second = h
            .runtime
            .account_progress(
                GoalAccountingMode::ActiveOnly,
                BudgetLimitedGoalDisposition::KeepActive,
            )
            .await
            .unwrap();
        assert!(second.is_none(), "the wrap-up must not repeat");

        assert_eq!(
            h.runtime.on_turn_end(false).await.unwrap(),
            GoalContinuation::Stop,
            "a budget-limited goal does not continue"
        );
        assert_eq!(
            h.runtime.goal().await.unwrap().unwrap().status,
            ThreadGoalStatus::BudgetLimited
        );
    }

    #[tokio::test]
    async fn a_failed_turn_blocks_the_goal_and_a_usage_limit_marks_it() {
        let _g = EnvGuard::temp_home("failed");
        let h = harness("t1");
        h.runtime.on_turn_start().await.unwrap();
        h.runtime.create_goal("ship it".into(), None).await;
        let blocked = h
            .runtime
            .on_turn_failed(GoalStopReason::TurnError)
            .await
            .unwrap()
            .expect("a goal to stop");
        assert_eq!(blocked.status, ThreadGoalStatus::Blocked);

        // A resume, then a usage-limit failure.
        h.runtime
            .apply_host_update(GoalUpdate {
                status: Some(ThreadGoalStatus::Active),
                ..Default::default()
            })
            .await
            .unwrap()
            .expect("resume applies");
        assert_eq!(
            h.runtime
                .on_turn_failed(GoalStopReason::UsageLimit)
                .await
                .unwrap()
                .unwrap()
                .status,
            ThreadGoalStatus::Active,
            "the previous turn cannot stop a newly resumed intent"
        );
        h.runtime.on_turn_start().await.unwrap();
        let limited = h
            .runtime
            .on_turn_failed(GoalStopReason::UsageLimit)
            .await
            .unwrap()
            .expect("a goal to stop");
        assert_eq!(limited.status, ThreadGoalStatus::UsageLimited);
    }

    // A usage limit outranks the goal's own budget: the remedy is different
    // (wait out the quota window vs raise the budget), so the reason the user is
    // shown has to be the one that is actually blocking them.
    #[tokio::test]
    async fn a_usage_limit_overrides_a_budget_stop_but_a_turn_error_does_not() {
        let _g = EnvGuard::temp_home("override");
        let h = harness("t1");
        h.runtime.on_turn_start().await.unwrap();
        h.runtime.create_goal("ship it".into(), Some(100)).await;
        h.runtime.on_token_usage(&usage(120, 40));
        h.runtime
            .account_progress(
                GoalAccountingMode::ActiveOnly,
                BudgetLimitedGoalDisposition::KeepActive,
            )
            .await
            .unwrap();
        assert_eq!(
            h.runtime.goal().await.unwrap().unwrap().status,
            ThreadGoalStatus::BudgetLimited
        );

        // A plain turn error leaves the budget stop standing.
        let after_error = h
            .runtime
            .on_turn_failed(GoalStopReason::TurnError)
            .await
            .unwrap()
            .expect("a goal to report");
        assert_eq!(after_error.status, ThreadGoalStatus::BudgetLimited);

        let after_limit = h
            .runtime
            .on_turn_failed(GoalStopReason::UsageLimit)
            .await
            .unwrap()
            .expect("a goal to report");
        assert_eq!(after_limit.status, ThreadGoalStatus::UsageLimited);
    }

    // The turn that crossed the budget keeps running to wrap up, and its
    // remaining tokens still belong to the goal.
    #[tokio::test]
    async fn a_budget_limited_goal_is_still_charged_for_the_rest_of_its_turn() {
        let _g = EnvGuard::temp_home("charge-after-budget");
        let h = harness("t1");
        h.runtime.on_turn_start().await.unwrap();
        h.runtime.create_goal("ship it".into(), Some(100)).await;
        h.runtime.on_token_usage(&usage(90, 20));
        h.runtime
            .account_progress(
                GoalAccountingMode::ActiveOnly,
                BudgetLimitedGoalDisposition::KeepActive,
            )
            .await
            .unwrap();
        let at_stop = h.runtime.goal().await.unwrap().unwrap();
        assert_eq!(at_stop.status, ThreadGoalStatus::BudgetLimited);
        assert_eq!(at_stop.tokens_used, 110);

        h.runtime.on_token_usage(&usage(90, 60));
        h.runtime
            .account_progress(
                GoalAccountingMode::ActiveOnly,
                BudgetLimitedGoalDisposition::KeepActive,
            )
            .await
            .unwrap();
        // Each response reports its own usage, so the wrap-up response adds its
        // own 150 on top of the 110 already charged.
        let after_wrap_up = h.runtime.goal().await.unwrap().unwrap();
        assert_eq!(after_wrap_up.tokens_used, 260);
        assert_eq!(after_wrap_up.status, ThreadGoalStatus::BudgetLimited);
    }

    #[tokio::test]
    async fn editing_the_objective_steers_the_running_turn() {
        let _g = EnvGuard::temp_home("edit");
        let h = harness("t1");
        h.runtime.on_turn_start().await.unwrap();
        h.runtime.create_goal("old objective".into(), None).await;
        let (goal, steer) = {
            let applied = h
                .runtime
                .apply_host_update(GoalUpdate {
                    objective: Some("new objective".into()),
                    ..Default::default()
                })
                .await
                .unwrap()
                .expect("edit applies");
            (applied.goal, applied.steer)
        };
        assert_eq!(goal.objective, RuntimeString::from("new objective"));
        let steer = steer.expect("an edited objective steers the turn");
        assert!(steer.contains("new objective"));
        assert!(steer.contains("supersedes any previous thread goal objective"));

        // Re-applying the same objective does not steer again.
        let applied = h
            .runtime
            .apply_host_update(GoalUpdate {
                objective: Some("new objective".into()),
                ..Default::default()
            })
            .await
            .unwrap()
            .expect("edit applies");
        assert!(applied.steer.is_none());
    }

    #[tokio::test]
    async fn resuming_a_stopped_goal_hands_back_the_continuation_prompt() {
        let _g = EnvGuard::temp_home("resume");
        let h = harness("t1");
        h.runtime.on_turn_start().await.unwrap();
        h.runtime.create_goal("ship it".into(), None).await;
        h.runtime
            .on_turn_failed(GoalStopReason::TurnError)
            .await
            .unwrap();

        let applied = h
            .runtime
            .apply_host_update(GoalUpdate {
                status: Some(ThreadGoalStatus::Active),
                ..Default::default()
            })
            .await
            .unwrap()
            .expect("resume applies");
        assert_eq!(applied.goal.status, ThreadGoalStatus::Active);
        let steer = applied.steer.expect("a resumed goal needs a turn");
        assert!(steer.contains("Continue working toward the active thread goal."));
        assert!(
            !applied.turn_in_flight,
            "no turn is running, so the host must start one"
        );

        // A pause needs no instruction at all.
        let applied = h
            .runtime
            .apply_host_update(GoalUpdate {
                status: Some(ThreadGoalStatus::Paused),
                ..Default::default()
            })
            .await
            .unwrap()
            .expect("pause applies");
        assert!(applied.steer.is_none());
    }

    #[tokio::test]
    async fn every_change_is_published_to_the_observer() {
        let _g = EnvGuard::temp_home("observer");
        let h = harness("t1");
        h.runtime.on_turn_start().await.unwrap();
        h.runtime.create_goal("ship it".into(), None).await;
        h.runtime.on_token_usage(&usage(10, 5));
        h.clock.store(1_000, Ordering::Relaxed);
        h.runtime.on_turn_end(false).await.unwrap();
        h.runtime
            .update_goal_status(ThreadGoalStatus::Complete)
            .await;
        let published = h.published.lock().unwrap();
        assert!(
            published.len() >= 3,
            "create, charge and complete are all published: {published:?}"
        );
        assert_eq!(published.first().unwrap().status, ThreadGoalStatus::Active);
        assert_eq!(published.last().unwrap().status, ThreadGoalStatus::Complete);
    }

    #[tokio::test]
    async fn clearing_drops_the_goal_and_stops_continuations() {
        let _g = EnvGuard::temp_home("clear");
        let h = harness("t1");
        h.runtime.on_turn_start().await.unwrap();
        h.runtime.create_goal("ship it".into(), None).await;
        assert!(h.runtime.clear_goal().await.unwrap().is_some());
        assert!(h.runtime.goal().await.unwrap().is_none());
        assert_eq!(
            h.runtime.on_turn_end(false).await.unwrap(),
            GoalContinuation::Stop
        );
    }

    #[tokio::test]
    async fn a_turn_that_starts_before_the_goal_charges_only_post_create_work() {
        let _g = EnvGuard::temp_home("baseline");
        let h = harness("t1");
        h.runtime.on_turn_start().await.unwrap();
        // Work done before the goal exists.
        h.runtime.on_token_usage(&usage(900, 100));
        h.clock.store(2_000, Ordering::Relaxed);
        h.runtime.create_goal("ship it".into(), None).await;
        h.runtime.on_token_usage(&usage(100, 50));
        h.clock.store(5_000, Ordering::Relaxed);
        match h.runtime.on_turn_end(false).await.unwrap() {
            GoalContinuation::Continue { goal, .. } => {
                assert_eq!(
                    goal.tokens_used, 150,
                    "only the tokens after create_goal are the goal's"
                );
                assert_eq!(goal.time_used_seconds, 3);
            }
            GoalContinuation::Stop => panic!("an active goal must continue"),
        }
    }
    #[tokio::test]
    async fn stale_request_intent_cannot_complete_a_same_generation_edit() {
        let _g = EnvGuard::temp_home("intent");
        let h = harness("intent");
        h.runtime.on_turn_start().await.unwrap();
        let created = match h
            .runtime
            .create_goal("Original objective".into(), None)
            .await
        {
            GoalCreateOutcome::Created(goal) => goal,
            other => panic!("{other:?}"),
        };
        let captured = h.runtime.request_identity();
        h.runtime
            .apply_host_update(GoalUpdate {
                objective: Some("Edited objective".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(matches!(
            h.runtime
                .update_goal_status_checked(
                    ThreadGoalStatus::Complete,
                    Some(&created.goal_id),
                    Some(captured.intent)
                )
                .await,
            GoalUpdateOutcome::Forbidden(_)
        ));
        let current = h.runtime.goal().await.unwrap().unwrap();
        assert_eq!(current.goal_id, created.goal_id);
        assert_eq!(current.objective, RuntimeString::from("Edited objective"));
        assert_eq!(current.status, ThreadGoalStatus::Active);
    }
    #[tokio::test]
    async fn three_qualifying_executed_failures_block_but_results_and_success_reset() {
        let _g = EnvGuard::temp_home("executed");
        let h = harness("executed");
        h.runtime.create_goal("Finish".into(), None).await;
        for _ in 0..2 {
            h.runtime.on_turn_start().await.unwrap();
            h.runtime.note_tool_outcome(true, false);
            h.runtime.on_turn_end(false).await.unwrap();
        }
        h.runtime.on_turn_start().await.unwrap();
        h.runtime.note_tool_outcome(false, true);
        h.runtime.on_turn_end(false).await.unwrap();
        for _ in 0..2 {
            h.runtime.on_turn_start().await.unwrap();
            h.runtime.note_tool_outcome(true, false);
            h.runtime.on_turn_end(false).await.unwrap();
            assert_eq!(
                h.runtime.goal().await.unwrap().unwrap().status,
                ThreadGoalStatus::Active
            );
        }
        h.runtime.on_turn_start().await.unwrap();
        h.runtime.note_tool_outcome(true, false);
        h.runtime.on_turn_end(false).await.unwrap();
        assert_eq!(
            h.runtime.goal().await.unwrap().unwrap().status,
            ThreadGoalStatus::Blocked
        );
    }
    #[tokio::test]
    async fn empty_final_and_interruption_audit_resets_on_nonblank_activity() {
        let _g = EnvGuard::temp_home("empty");
        let h = harness("empty");
        let goal = match h.runtime.create_goal("Finish".into(), None).await {
            GoalCreateOutcome::Created(goal) => goal,
            other => panic!("{other:?}"),
        };
        for _ in 0..2 {
            h.runtime
                .on_turn_start_for(true, Some(&goal.goal_id), true)
                .await
                .unwrap();
            let identity = Mutex::new(h.runtime.request_identity());
            h.runtime
                .prepare_provider_request(&[], &identity, None, None)
                .await
                .unwrap()
                .accepted(&h.runtime)
                .await
                .unwrap();
            h.runtime.note_empty_final();
            h.runtime.on_turn_end(false).await.unwrap();
        }
        h.runtime
            .on_turn_start_for(true, Some(&goal.goal_id), true)
            .await
            .unwrap();
        let identity = Mutex::new(h.runtime.request_identity());
        h.runtime
            .prepare_provider_request(&[], &identity, None, None)
            .await
            .unwrap()
            .accepted(&h.runtime)
            .await
            .unwrap();
        h.runtime.note_activity();
        h.runtime.on_turn_end(false).await.unwrap();
        for _ in 0..2 {
            h.runtime
                .on_turn_start_for(true, Some(&goal.goal_id), true)
                .await
                .unwrap();
            let identity = Mutex::new(h.runtime.request_identity());
            h.runtime
                .prepare_provider_request(&[], &identity, None, None)
                .await
                .unwrap()
                .accepted(&h.runtime)
                .await
                .unwrap();
            h.runtime.on_turn_aborted().await.unwrap();
            assert_eq!(
                h.runtime.goal().await.unwrap().unwrap().status,
                ThreadGoalStatus::Active
            );
        }
        h.runtime
            .on_turn_start_for(true, Some(&goal.goal_id), true)
            .await
            .unwrap();
        let identity = Mutex::new(h.runtime.request_identity());
        h.runtime
            .prepare_provider_request(&[], &identity, None, None)
            .await
            .unwrap()
            .accepted(&h.runtime)
            .await
            .unwrap();
        h.runtime.on_turn_aborted().await.unwrap();
        assert_eq!(
            h.runtime.goal().await.unwrap().unwrap().status,
            ThreadGoalStatus::Blocked
        );
    }
    #[tokio::test]
    async fn a_later_explicit_resume_revokes_an_older_stop_without_erasing_a_newer_one() {
        let _g = EnvGuard::temp_home("stop-authority");
        let h = harness("stop-authority");
        h.runtime
            .create_host_goal("Finish".into(), None, None, None)
            .await
            .unwrap()
            .unwrap();
        let delayed_stop = h.runtime.begin_stop();
        h.runtime
            .apply_host_update(GoalUpdate {
                status: Some(ThreadGoalStatus::Active),
                ..Default::default()
            })
            .await
            .unwrap()
            .unwrap();
        assert!(!h.runtime.suppress_continuation_for(delayed_stop));
        assert!(!h.runtime.continuation_suppressed());

        let older_resume = h.runtime.resume_epoch();
        let newer_stop = h.runtime.begin_stop();
        assert!(h.runtime.suppress_continuation_for(newer_stop));
        h.runtime.resume_pursuit(older_resume);
        assert!(h.runtime.continuation_suppressed());
    }

    #[tokio::test]
    async fn a_fresh_authorized_model_goal_can_replace_a_stop_but_an_old_request_cannot() {
        let _g = EnvGuard::temp_home("stop-create");
        let h = harness("stop-create");
        let old = h.runtime.request_identity();
        h.runtime.suppress_continuation();
        assert!(matches!(
            h.runtime
                .create_goal_checked("Old work".into(), None, Some(&old))
                .await,
            GoalCreateOutcome::InvalidObjective(_)
        ));
        assert!(h.runtime.goal().await.unwrap().is_none());
        assert!(h.runtime.continuation_suppressed());
        let fresh = h.runtime.request_identity();
        assert!(matches!(
            h.runtime
                .create_goal_checked("New authorized work".into(), None, Some(&fresh))
                .await,
            GoalCreateOutcome::Created(_)
        ));
        assert!(!h.runtime.continuation_suppressed());
    }

    #[tokio::test]
    async fn a_stop_latch_survives_mode_feature_and_state_only_edits_until_explicit_resume() {
        let _g = EnvGuard::temp_home("stop-latch");
        let h = harness("stop-latch");
        let created = h
            .runtime
            .create_host_goal("Finish".into(), None, None, None)
            .await
            .unwrap()
            .unwrap()
            .goal;
        h.runtime.suppress_continuation();
        h.runtime.flush_stopped_progress().await.unwrap();
        h.clock.store(10_000, Ordering::SeqCst);
        h.runtime.synchronize_execution_mode(false).await.unwrap();
        h.runtime.synchronize_execution_mode(true).await.unwrap();
        h.runtime.set_enabled(false, true).await.unwrap();
        h.runtime.set_enabled(true, true).await.unwrap();
        h.runtime
            .apply_host_update(GoalUpdate {
                objective: Some("Edited without resuming".into()),
                token_budget: Some(Some(100)),
                ..Default::default()
            })
            .await
            .unwrap()
            .unwrap();
        h.clock.store(20_000, Ordering::SeqCst);
        h.runtime.flush_progress().await.unwrap();
        assert!(h.runtime.continuation_suppressed());
        let stopped = h.runtime.goal().await.unwrap().unwrap();
        assert_eq!(stopped.goal_id, created.goal_id);
        assert_eq!(stopped.status, ThreadGoalStatus::Active);
        assert_eq!(stopped.time_used_seconds, 0);
        assert_eq!(
            h.runtime.continuation_if_idle(false).await.unwrap(),
            GoalContinuation::Stop
        );
        h.runtime
            .apply_host_update(GoalUpdate {
                status: Some(ThreadGoalStatus::Active),
                ..Default::default()
            })
            .await
            .unwrap()
            .unwrap();
        assert!(!h.runtime.continuation_suppressed());
        h.clock.store(21_000, Ordering::SeqCst);
        h.runtime.flush_progress().await.unwrap();
        assert_eq!(
            h.runtime.goal().await.unwrap().unwrap().time_used_seconds,
            1
        );
    }

    #[tokio::test]
    async fn planning_time_is_excluded_and_active_idle_fraction_is_preserved() {
        let _g = EnvGuard::temp_home("clock");
        let h = harness("clock");
        h.runtime.synchronize_execution_mode(false).await.unwrap();
        h.runtime
            .create_host_goal("Finish".into(), None, None, None)
            .await
            .unwrap();
        h.clock.store(10_000, Ordering::SeqCst);
        h.runtime
            .apply_host_update(GoalUpdate {
                status: Some(ThreadGoalStatus::Paused),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            h.runtime.goal().await.unwrap().unwrap().time_used_seconds,
            0
        );
        h.runtime.synchronize_execution_mode(true).await.unwrap();
        h.runtime
            .apply_host_update(GoalUpdate {
                status: Some(ThreadGoalStatus::Active),
                ..Default::default()
            })
            .await
            .unwrap();
        h.clock.store(10_600, Ordering::SeqCst);
        h.runtime
            .apply_host_update(GoalUpdate {
                status: Some(ThreadGoalStatus::Paused),
                ..Default::default()
            })
            .await
            .unwrap();
        h.clock.store(90_000, Ordering::SeqCst);
        h.runtime
            .apply_host_update(GoalUpdate {
                status: Some(ThreadGoalStatus::Active),
                ..Default::default()
            })
            .await
            .unwrap();
        h.clock.store(90_400, Ordering::SeqCst);
        h.runtime.flush_progress().await.unwrap();
        assert_eq!(
            h.runtime.goal().await.unwrap().unwrap().time_used_seconds,
            1
        );
    }

    #[tokio::test]
    async fn active_goal_fraction_and_accepted_usage_survive_a_planning_interval() {
        let _g = EnvGuard::temp_home("clock-plan-fraction");
        let h = harness("clock-plan-fraction");
        let created = h
            .runtime
            .create_host_goal("Finish".into(), None, None, None)
            .await
            .unwrap()
            .unwrap()
            .goal;
        h.runtime.on_turn_start().await.unwrap();
        let identity = Mutex::new(h.runtime.request_identity());
        h.runtime
            .prepare_provider_request(&[], &identity, None, None)
            .await
            .unwrap()
            .accepted(&h.runtime)
            .await
            .unwrap();
        h.runtime.on_token_usage(&usage(8, 2));
        h.clock.store(600, Ordering::SeqCst);
        h.runtime.on_turn_end(true).await.unwrap();
        assert!(!h.runtime.turn_in_flight());
        h.runtime.synchronize_execution_mode(false).await.unwrap();
        let planning = h.runtime.goal().await.unwrap().unwrap();
        assert_eq!(planning.goal_id, created.goal_id);
        assert_eq!(planning.status, ThreadGoalStatus::Active);
        assert_eq!(planning.tokens_used, 10);
        assert_eq!(planning.time_used_seconds, 0);

        h.clock.store(60_600, Ordering::SeqCst);
        h.runtime.flush_progress().await.unwrap();
        assert_eq!(h.runtime.goal().await.unwrap().unwrap(), planning);

        h.runtime.synchronize_execution_mode(true).await.unwrap();
        h.clock.store(61_000, Ordering::SeqCst);
        h.runtime.flush_progress().await.unwrap();
        let resumed = h.runtime.goal().await.unwrap().unwrap();
        assert_eq!(resumed.goal_id, created.goal_id);
        assert_eq!(resumed.status, ThreadGoalStatus::Active);
        assert_eq!(resumed.tokens_used, 10);
        assert_eq!(resumed.time_used_seconds, 1);
    }
    #[tokio::test]
    async fn terminal_sample_charges_paused_same_generation_and_rejects_replacement() {
        let _g = EnvGuard::temp_home("terminal");
        let h = harness("terminal");
        let created = h
            .runtime
            .create_host_goal("First".into(), None, None, None)
            .await
            .unwrap()
            .unwrap()
            .goal;
        h.runtime
            .apply_host_update(GoalUpdate {
                status: Some(ThreadGoalStatus::Paused),
                ..Default::default()
            })
            .await
            .unwrap();
        h.runtime
            .on_token_usage_for(Some(&created.goal_id), &usage(40, 1));
        h.runtime
            .account_progress(
                GoalAccountingMode::ActiveOrStopped,
                BudgetLimitedGoalDisposition::ClearActive,
            )
            .await
            .unwrap();
        assert_eq!(h.runtime.goal().await.unwrap().unwrap().tokens_used, 41);
        h.runtime.clear_goal().await.unwrap();
        h.runtime
            .create_host_goal("Second".into(), None, None, None)
            .await
            .unwrap();
        h.runtime
            .on_token_usage_for(Some(&created.goal_id), &usage(999, 999));
        h.runtime.flush_progress().await.unwrap();
        assert_eq!(h.runtime.goal().await.unwrap().unwrap().tokens_used, 0);
    }
    #[tokio::test]
    async fn control_flush_preserves_pending_wrapup_until_provider_acceptance() {
        let _g = EnvGuard::temp_home("pending-wrapup");
        let h = harness("pending-wrapup");
        h.runtime
            .create_host_goal("Finish".into(), None, Some(10), None)
            .await
            .unwrap();
        h.runtime.on_turn_start().await.unwrap();
        h.runtime.on_token_usage(&usage(9, 2));
        h.runtime.flush_progress().await.unwrap();
        h.runtime.on_turn_end(false).await.unwrap();
        assert!(matches!(
            h.runtime.continuation_if_idle(false).await.unwrap(),
            GoalContinuation::Continue { .. }
        ));
        let goal = h.runtime.goal().await.unwrap().unwrap();
        assert!(
            store::pending_budget_wrapup("pending-wrapup", &goal.goal_id)
                .await
                .unwrap()
        );
    }
    #[tokio::test]
    async fn configured_maximum_budget_preserves_omission_and_applies_to_explicit_null() {
        let _g = EnvGuard::temp_home("maximum");
        let clock: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(|| 0);
        let runtime = GoalRuntime::with_clock(
            "maximum".into(),
            Arc::new(|| "capped-goal".into()),
            clock.clone(),
            clock.clone(),
            None,
            Some(100),
        );
        assert!(matches!(
            runtime.create_goal("Finish".into(), Some(101)).await,
            GoalCreateOutcome::InvalidObjective(_)
        ));
        let created = runtime
            .create_host_goal("Finish".into(), None, None, None)
            .await
            .unwrap()
            .unwrap()
            .goal;
        assert_eq!(created.token_budget, Some(100));
        runtime
            .apply_host_update(GoalUpdate {
                token_budget: Some(Some(80)),
                ..Default::default()
            })
            .await
            .unwrap();
        runtime
            .apply_host_update(GoalUpdate {
                objective: Some("Edited".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            runtime.goal().await.unwrap().unwrap().token_budget,
            Some(80)
        );
        runtime
            .apply_host_update(GoalUpdate {
                token_budget: Some(None),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            runtime.goal().await.unwrap().unwrap().token_budget,
            Some(100)
        );
        assert!(runtime
            .apply_host_update(GoalUpdate {
                token_budget: Some(Some(101)),
                ..Default::default()
            })
            .await
            .is_err());
        let uncapped = GoalRuntime::with_clock(
            "uncapped".into(),
            Arc::new(|| "uncapped-goal".into()),
            clock.clone(),
            clock.clone(),
            None,
            None,
        );
        uncapped
            .create_host_goal("Finish".into(), None, Some(10), None)
            .await
            .unwrap();
        uncapped
            .apply_host_update(GoalUpdate {
                token_budget: Some(None),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(uncapped.goal().await.unwrap().unwrap().token_budget, None);
        let invalid = GoalRuntime::with_clock(
            "invalid".into(),
            Arc::new(|| "invalid-goal".into()),
            clock.clone(),
            clock,
            None,
            Some(0),
        );
        assert!(matches!(
            invalid.create_goal("Finish".into(), None).await,
            GoalCreateOutcome::InvalidObjective(_)
        ));
    }
    #[tokio::test]
    async fn checked_pause_commits_its_own_pending_accounting_without_self_conflict() {
        let _g = EnvGuard::temp_home("control-cas");
        let h = harness("control-cas");
        let created = h
            .runtime
            .create_host_goal("Finish".into(), None, None, None)
            .await
            .unwrap()
            .unwrap();
        h.runtime.on_token_usage(&usage(4, 3));
        h.clock.store(1_000, Ordering::SeqCst);
        let paused = h
            .runtime
            .apply_host_update(GoalUpdate {
                status: Some(ThreadGoalStatus::Paused),
                expected_goal_id: Some(created.goal.goal_id),
                expected_revision: Some(created.revision),
                ..Default::default()
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(paused.goal.tokens_used, 7);
        assert_eq!(paused.goal.time_used_seconds, 1);
        assert_eq!(paused.goal.status, ThreadGoalStatus::Paused);
    }
}
