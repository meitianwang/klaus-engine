//! The Session's side of goal mode: the runtime handle, the host-facing
//! set/clear surface, and the observer slot goal changes are published through.
//!
//! The turn-loop side (adopt the goal, charge usage, decide whether to run
//! another turn) lives in `session_run.rs`, next to the turn it belongs to.

use std::sync::Arc;

use super::Session;
use super::TurnOptions;
use crate::goal::runtime::{GoalHostUpdate, GoalObserver, GoalRuntime, GoalSnapshot};
use crate::goal::types::{ThreadGoal, ThreadGoalStatus};
use crate::observability::otel::OtelObserver;
use crate::runtime_values::RuntimeString;

/// Where a host installs its goal sink. Replacing is allowed: a reconnected
/// host installs a fresh sink and the stale one must stop receiving.
#[derive(Default)]
pub struct GoalObserverSlot {
    inner: std::sync::Mutex<Option<GoalObserver>>,
}

impl GoalObserverSlot {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&self, sink: Option<GoalObserver>) {
        *self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = sink;
    }

    /// Deliver one change. The sink is cloned out before the call so the lock is
    /// never held while host code runs.
    pub fn notify(&self, goal: GoalSnapshot) {
        let sink = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(sink) = sink {
            sink(goal);
        }
    }
}

impl std::fmt::Debug for GoalObserverSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GoalObserverSlot").finish_non_exhaustive()
    }
}

/// The metrics half of the publish funnel: every goal change passes through
/// here on its way to the host sink, which is the only place that sees ALL of
/// them (a create, a resume, a terminal stop, a usage-only bump).
///
/// It has to remember the last snapshot itself: the store hands out the goal as
/// it now is, and a counter for "created" or "resumed" is only meaningful
/// against what came before. A new `goalId` is a new generation, so an unseen id
/// is a create.
struct GoalMetrics {
    otel: Arc<std::sync::Mutex<OtelObserver>>,
    last: std::sync::Mutex<Option<(RuntimeString, ThreadGoalStatus)>>,
}

/// The status this goal carried in the previous snapshot, and remember this one.
/// `None` means "not seen before", which is what a create looks like: a new
/// generation gets a new `goalId`, so a re-created goal is a create again.
fn previous_status(
    last: &mut Option<(RuntimeString, ThreadGoalStatus)>,
    goal: &ThreadGoal,
) -> Option<ThreadGoalStatus> {
    let previous = match last.as_ref() {
        Some((goal_id, status)) if *goal_id == goal.goal_id => Some(*status),
        _ => None,
    };
    *last = Some((goal.goal_id.clone(), goal.status));
    previous
}

impl GoalMetrics {
    fn record(&self, goal: &ThreadGoal) {
        let previous = {
            let mut last = self
                .last
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            previous_status(&mut last, goal)
        };
        // Both locks are released before anything else runs: the observer lock is
        // shared with the turn path, and holding it across the host sink would
        // let a slow host stall the next turn's metrics.
        if let Ok(otel) = self.otel.lock() {
            otel.record_goal_change(previous, goal);
        }
    }
}

/// Build the session's goal runtime, or `None` when goal mode is off.
pub(crate) fn build_goal_runtime(
    thread_id: RuntimeString,
    enabled: bool,
    maximum_budget: Option<i64>,
    uuid: crate::types::tool::UuidFn,
    now: Arc<dyn Fn() -> i64 + Send + Sync>,
    observer_slot: Arc<GoalObserverSlot>,
    otel: Option<Arc<std::sync::Mutex<OtelObserver>>>,
) -> Option<Arc<GoalRuntime>> {
    if !enabled {
        return None;
    }
    let metrics = otel.map(|otel| GoalMetrics {
        otel,
        last: std::sync::Mutex::new(None),
    });
    let observer: GoalObserver = Arc::new(move |snapshot: GoalSnapshot| {
        if let (Some(metrics), Some(goal)) = (&metrics, &snapshot.goal) {
            metrics.record(goal);
        }
        observer_slot.notify(snapshot)
    });
    let started = std::time::Instant::now();
    let elapsed: Arc<dyn Fn() -> i64 + Send + Sync> =
        Arc::new(move || i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX));
    Some(Arc::new(GoalRuntime::with_clock(
        thread_id,
        uuid,
        now,
        elapsed,
        Some(observer),
        maximum_budget,
    )))
}

/// The options a goal CONTINUATION turn inherits from the turn that preceded
/// it.
///
/// Everything the host chose for the run carries over, with two deliberate
/// exceptions:
///   - `output_schema` is dropped. A schema is a demand on the answer the host
///     asked for; a continuation is more work, not another answer in that
///     shape, and re-imposing it would force the model to emit JSON instead of
///     working.
///   - `meta_input` is set. The continuation text is an instruction the engine
///     wrote to itself, so it must not be rendered as the user's words.
pub(crate) fn continuation_turn_options(previous: &TurnOptions) -> TurnOptions {
    TurnOptions {
        signal: previous.signal.clone(),
        model: previous.model.clone(),
        // Inherited with `model`: a per-turn override that a goal continuation
        // dropped would silently re-tune the remaining work mid-task.
        effort: previous.effort.clone(),
        ultra_mode: previous.ultra_mode,
        output_schema: None,
        permission_mode_override: previous.permission_mode_override,
        approval_handler: previous.approval_handler.clone(),
        approval_reviewer: previous.approval_reviewer.clone(),
        system_prompt_append: previous.system_prompt_append.clone(),
        token_budget: previous.token_budget,
        selected_capability_roots: previous.selected_capability_roots.clone(),
        display_metadata: None,
        meta_input: true,
        goal_pending_input: previous.goal_pending_input.clone(),
        on_provider_request_admitted: previous.on_provider_request_admitted.clone(),
    }
}

impl Session {
    /// Whether a real user message is already waiting in the queue.
    ///
    /// A queued human turn outranks an automatic one: continuing the goal first
    /// would answer the user's message inside a turn they did not ask for, and
    /// after work they have not seen yet.
    pub(crate) fn has_pending_user_prompt(&self) -> bool {
        self.conversation
            .queue()
            .peek(Some(
                &|cmd: &crate::engine::message_queue::QueuedMessage| {
                    cmd.agent_id.is_none()
                        && cmd.is_meta != Some(true)
                        && !crate::engine::message_queue::MessageQueue::is_slash_command(&cmd.value)
                        && crate::engine::message_queue::queued_value_has_model_content(&cmd.value)
                },
            ))
            .is_some()
    }

    /// The thread's goal runtime, when goal mode is on.
    pub fn goal_runtime(&self) -> Option<Arc<GoalRuntime>> {
        self.goal.as_ref().filter(|goal| goal.enabled()).cloned()
    }

    /// Whether this session may own goal pursuit, independent of feature state.
    pub fn goal_pursuit_eligible(&self) -> bool {
        self.persistence.store.is_some()
            && self.persistence.rollout.is_some()
            && self.meta.parent_thread_id.is_none()
            && matches!(
                self.meta.role,
                None | Some(crate::schemas::rollout::SessionRole::Main)
            )
            && self.config.task_resource_scope.is_none()
    }
    pub fn goal_enabled(&self) -> bool {
        self.goal_runtime().is_some()
    }
    pub fn mark_goal_submission_pending(&self, id: &RuntimeString) {
        if let Some(goal) = &self.goal {
            goal.mark_submission_pending(id);
        }
    }
    pub async fn set_goal_enabled(&self, enabled: bool) -> Result<(), RuntimeString> {
        if enabled && self.config.task_resource_scope.is_some() {
            return Err("Goals are unavailable in a task resource scope".into());
        }
        let runtime = self.goal.as_ref().ok_or_else(|| {
            RuntimeString::from("Goals require a persistent ordinary main thread")
        })?;
        if enabled {
            runtime
                .recover_interrupted_run()
                .await
                .map_err(|error| RuntimeString::from(error.to_string()))?;
        }
        runtime
            .set_enabled(
                enabled,
                self.collaboration_mode() != crate::planning::CollaborationMode::Plan,
            )
            .await
            .map_err(|error| RuntimeString::from(error.to_string()))
    }
    /// Observation stays installed while the feature is disabled so later enablement can resume the same owner.
    pub fn goal_observation_runtime(&self) -> Option<Arc<GoalRuntime>> {
        self.goal.clone()
    }

    /// Install (or clear) the host's goal sink.
    pub fn set_goal_observer(&self, sink: Option<GoalObserver>) {
        let installed = sink.is_some();
        self.goal_observer.set(sink);
        if installed {
            if let Some(goal) = &self.goal {
                goal.watch_external_changes();
            }
        }
    }

    /// Read the authoritative snapshot without starting inference.
    pub async fn thread_goal(&self) -> std::io::Result<Option<ThreadGoal>> {
        match &self.goal {
            Some(runtime) => runtime.goal().await,
            None => Ok(None),
        }
    }
    pub async fn goal_snapshot(&self) -> std::io::Result<GoalSnapshot> {
        if self.persistence.store.is_some() && self.persistence.rollout.is_some() {
            let thread = self.id.try_to_utf8().map_err(|error| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, error.to_string())
            })?;
            crate::goal::store::set_thread_goal_eligibility(&thread, self.goal_pursuit_eligible())
                .await?;
        }
        match &self.goal {
            Some(runtime) => runtime.snapshot().await,
            None => Ok(GoalSnapshot {
                goal: None,
                revision: 0,
            }),
        }
    }
    pub async fn synchronize_goal_mode(&self) -> Result<(), RuntimeString> {
        if let Some(goal) = &self.goal {
            goal.synchronize_execution_mode(
                self.collaboration_mode() != crate::planning::CollaborationMode::Plan,
            )
            .await
            .map_err(|error| RuntimeString::from(error.to_string()))?;
        }
        Ok(())
    }
    pub async fn pause_goal_for_shutdown(&self) -> Result<(), RuntimeString> {
        if let Some(goal) = &self.goal {
            goal.pause_for_shutdown()
                .await
                .map_err(|error| RuntimeString::from(error.to_string()))?;
        }
        Ok(())
    }
    pub async fn pause_goal_for_shutdown_guarded(
        &self,
        guard: Option<crate::goal::store::GoalControlGuard>,
    ) -> Result<(), RuntimeString> {
        if let Some(goal) = &self.goal {
            goal.pause_for_shutdown_guarded(guard)
                .await
                .map_err(|error| RuntimeString::from(error.to_string()))?;
        }
        Ok(())
    }
    pub async fn flush_stopped_goal_progress(&self) -> Result<(), RuntimeString> {
        if let Some(goal) = &self.goal {
            goal.flush_stopped_progress()
                .await
                .map_err(|error| RuntimeString::from(error.to_string()))?;
        }
        Ok(())
    }
    pub async fn flush_goal_progress(&self) -> Result<(), RuntimeString> {
        if let Some(runtime) = &self.goal {
            runtime
                .flush_progress()
                .await
                .map_err(|error| RuntimeString::from(error.to_string()))?;
        }
        Ok(())
    }
    pub async fn set_thread_goal(
        &self,
        objective: Option<RuntimeString>,
        status: Option<RuntimeString>,
        token_budget: Option<Option<i64>>,
    ) -> Result<GoalHostUpdate, RuntimeString> {
        self.set_thread_goal_conditional(objective, status, token_budget, None)
            .await
    }
    pub async fn set_thread_goal_conditional(
        &self,
        objective: Option<RuntimeString>,
        status: Option<RuntimeString>,
        token_budget: Option<Option<i64>>,
        expected_goal_id: Option<RuntimeString>,
    ) -> Result<GoalHostUpdate, RuntimeString> {
        self.set_thread_goal_guarded(objective, status, token_budget, expected_goal_id, false)
            .await
    }
    pub async fn set_thread_goal_guarded(
        &self,
        objective: Option<RuntimeString>,
        status: Option<RuntimeString>,
        token_budget: Option<Option<i64>>,
        expected_goal_id: Option<RuntimeString>,
        expected_absent: bool,
    ) -> Result<GoalHostUpdate, RuntimeString> {
        self.set_thread_goal_checked(crate::goal::service::HostGoalMutation {
            objective,
            status,
            token_budget,
            expected_goal_id,
            expected_absent,
            expected_revision: None,
            owned_resources: None,
            require_safe_json: false,
            control_guard: None,
        })
        .await
    }
    fn validate_goal_host_eligibility(
        &self,
        mutation: &crate::goal::service::HostGoalMutation,
    ) -> Result<(), RuntimeString> {
        if self.config.task_resource_scope.is_some()
            && (mutation.objective.is_some()
                || mutation.token_budget.is_some()
                || mutation.owned_resources.is_some()
                || mutation
                    .status
                    .as_ref()
                    .is_none_or(|status| *status != "paused"))
        {
            return Err("Goals are unavailable in a task resource scope; saved goals may only be read, paused, or cleared".into());
        }
        Ok(())
    }
    /// Validate a named JSON mutation before recording user authorization.
    pub async fn validate_goal_wire_mutation(
        &self,
        mutation: &crate::goal::service::HostGoalMutation,
    ) -> Result<(), RuntimeString> {
        self.validate_goal_host_eligibility(mutation)?;
        let runtime = self.goal.as_ref().ok_or_else(|| {
            RuntimeString::from(
                "Goals require a persistent ordinary main thread without a task resource scope",
            )
        })?;
        runtime
            .validate_host_wire_mutation(mutation)
            .await
            .map_err(|error| error.to_string().into())
    }
    pub async fn set_thread_goal_checked(
        &self,
        mutation: crate::goal::service::HostGoalMutation,
    ) -> Result<GoalHostUpdate, RuntimeString> {
        self.validate_goal_host_eligibility(&mutation)?;
        let runtime = self.goal.as_ref().ok_or_else(|| {
            RuntimeString::from("Goals require a persistent ordinary main thread")
        })?;
        self.synchronize_goal_mode().await?;
        crate::goal::service::apply_host_update(runtime, mutation)
            .await
            .map_err(|error| error.to_string().into())
    }
    pub async fn clear_thread_goal_checked(
        &self,
        expected: Option<RuntimeString>,
        revision: Option<i64>,
    ) -> Result<bool, RuntimeString> {
        let runtime = self.goal.as_ref().ok_or_else(|| {
            RuntimeString::from("Goals require a persistent ordinary main thread")
        })?;
        runtime
            .clear_goal_if_revision(expected, revision)
            .await
            .map_err(|error| RuntimeString::from(error.to_string()))?
            .map(|goal| goal.is_some())
            .map_err(|_| RuntimeString::from("GOAL_CONFLICT: the goal changed before clear"))
    }
    pub async fn set_thread_goal_once(
        &self,
        mutation: crate::goal::service::HostGoalMutation,
        request_id: RuntimeString,
    ) -> Result<(GoalHostUpdate, bool), RuntimeString> {
        self.validate_goal_host_eligibility(&mutation)?;
        let runtime = self
            .goal
            .as_ref()
            .ok_or_else(|| RuntimeString::from("goal mode is not enabled for this session"))?;
        let request_id = request_id
            .try_to_utf8()
            .map_err(|error| RuntimeString::from(error.to_string()))?;
        self.synchronize_goal_mode().await?;
        runtime
            .apply_host_update_once(mutation, &request_id)
            .await
            .map_err(|error| RuntimeString::from(error.to_string()))
    }
    pub async fn clear_thread_goal(&self) -> Result<bool, RuntimeString> {
        self.clear_thread_goal_conditional(None).await
    }
    pub async fn clear_thread_goal_conditional(
        &self,
        expected: Option<RuntimeString>,
    ) -> Result<bool, RuntimeString> {
        let runtime = self
            .goal
            .as_ref()
            .ok_or_else(|| RuntimeString::from("goal mode is not enabled for this session"))?;
        runtime
            .clear_goal_if(expected)
            .await
            .map_err(|error| RuntimeString::from(error.to_string()))?
            .map(|goal| goal.is_some())
            .map_err(|_| RuntimeString::from("the goal generation changed before clear"))
    }

    /// Deliver a goal instruction into the RUNNING turn.
    ///
    /// Queued as a meta `task-notification`, which the turn loop drains inline:
    /// the model sees the change during the work it is doing, and the transcript
    /// does not gain a message the user never wrote.
    pub async fn steer_goal_instruction(&self, text: String) -> Result<(), RuntimeString> {
        if self.try_steer_goal_instruction(text)? {
            Ok(())
        } else {
            Err("no foreground turn can accept this goal instruction".into())
        }
    }
    pub fn try_steer_goal_instruction(&self, text: String) -> Result<bool, RuntimeString> {
        if !self.turn.is_active() {
            return Ok(false);
        }
        let input_id: RuntimeString = (self.deps.uuid)().into();
        if let Some(goal) = &self.goal {
            goal.register_context_input(input_id.clone(), false);
        }
        self.conversation.queue().enqueue_pending_notification(
            crate::engine::message_queue::QueuedMessage {
                trigger_turn: Some(false),
                value: crate::engine::message_queue::QueuedValue::Text(RuntimeString::from(text)),
                mode: "task-notification".into(),
                is_meta: Some(true),
                origin: Some("goal".into()),
                priority: Some(crate::engine::message_queue::QueuePriority::Next),
                uuid: Some(input_id),
                session_id: Some(self.id.clone()),
                agent_id: None,
                correlation_id: None,
                pdf_bytes: 0,
                pdf_documents: 0,
                delivery: None,
                display_metadata: None,
            },
        )?;
        Ok(true)
    }
}

impl Session {
    pub fn maximum_goal_token_budget(&self) -> Option<i64> {
        self.goal.as_ref().and_then(|goal| goal.maximum_budget())
    }
    pub async fn set_maximum_goal_token_budget(
        &self,
        maximum: Option<i64>,
    ) -> Result<(), RuntimeString> {
        let goal = self
            .goal
            .as_ref()
            .ok_or_else(|| RuntimeString::from("This session cannot own a persistent goal"))?;
        goal.set_maximum_budget(maximum)
            .await
            .map_err(|error| RuntimeString::from(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_observer_slot_delivers_to_the_installed_sink_only() {
        let slot = GoalObserverSlot::new();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink: GoalObserver = {
            let seen = seen.clone();
            Arc::new(move |snapshot: GoalSnapshot| {
                if let Some(goal) = snapshot.goal {
                    seen.lock().unwrap().push(goal.goal_id);
                }
            })
        };
        let goal = ThreadGoal {
            thread_id: "t1".into(),
            goal_id: "g1".into(),
            objective: "x".into(),
            status: ThreadGoalStatus::Active,
            token_budget: None,
            tokens_used: 0,
            time_used_seconds: 0,
            created_at_ms: 0,
            updated_at_ms: 0,
        };
        // No sink installed: notify is a no-op rather than a panic.
        slot.notify(GoalSnapshot {
            goal: Some(goal.clone()),
            revision: 1,
        });
        slot.set(Some(sink));
        slot.notify(GoalSnapshot {
            goal: Some(goal.clone()),
            revision: 1,
        });
        slot.set(None);
        slot.notify(GoalSnapshot {
            goal: Some(goal),
            revision: 1,
        });
        assert_eq!(*seen.lock().unwrap(), vec![RuntimeString::from("g1")]);
    }

    #[test]
    fn goal_mode_off_means_no_runtime_is_built() {
        let uuid: crate::types::tool::UuidFn = Arc::new(|| "id".to_string());
        let now: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(|| 0);
        assert!(build_goal_runtime(
            "t1".into(),
            false,
            None,
            uuid.clone(),
            now.clone(),
            Arc::new(GoalObserverSlot::new()),
            None
        )
        .is_none());
        assert!(build_goal_runtime(
            "t1".into(),
            true,
            None,
            uuid,
            now,
            Arc::new(GoalObserverSlot::new()),
            None
        )
        .is_some());
    }

    #[test]
    fn a_metrics_snapshot_is_a_create_only_the_first_time_its_generation_is_seen() {
        let goal = |goal_id: &str, status: ThreadGoalStatus| ThreadGoal {
            thread_id: "t1".into(),
            goal_id: goal_id.into(),
            objective: "x".into(),
            status,
            token_budget: None,
            tokens_used: 0,
            time_used_seconds: 0,
            created_at_ms: 0,
            updated_at_ms: 0,
        };
        let mut last = None;
        // First sight of a generation: no previous status, so it counts as a create.
        assert_eq!(
            previous_status(&mut last, &goal("g1", ThreadGoalStatus::Active)),
            None
        );
        assert_eq!(
            previous_status(&mut last, &goal("g1", ThreadGoalStatus::Paused)),
            Some(ThreadGoalStatus::Active)
        );
        assert_eq!(
            previous_status(&mut last, &goal("g1", ThreadGoalStatus::Active)),
            Some(ThreadGoalStatus::Paused)
        );
        // A replacement goal is a NEW generation, so it is a create again rather
        // than a transition out of the finished one.
        assert_eq!(
            previous_status(&mut last, &goal("g2", ThreadGoalStatus::Active)),
            None
        );
    }
}
