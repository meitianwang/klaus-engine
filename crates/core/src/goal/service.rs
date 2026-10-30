//! Shared host goal transitions for live and saved threads.

use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex, OnceLock, Weak};

use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

use super::runtime::{GoalCreatePreconditions, GoalHostUpdate, GoalRuntime, GoalSnapshot};
use super::types::{GoalUpdate, ThreadGoalStatus};
use crate::runtime_values::RuntimeString;
use crate::store::session_store::SessionStore;

/// Stable internal carrier retained by the existing string error boundary.
/// Only definitive conditional-write rejection carries this marker.
pub const GOAL_CONFLICT_PREFIX: &str = "GOAL_CONFLICT: ";

pub fn conflict(message: impl Into<String>) -> io::Error {
    io::Error::new(
        io::ErrorKind::AlreadyExists,
        format!("{GOAL_CONFLICT_PREFIX}{}", message.into()),
    )
}

#[derive(Debug, Clone, Default)]
pub struct HostGoalMutation {
    pub owned_resources: Option<super::owned_resources::GoalOwnedResourcesInput>,
    pub objective: Option<RuntimeString>,
    pub status: Option<RuntimeString>,
    pub token_budget: Option<Option<i64>>,
    pub expected_goal_id: Option<RuntimeString>,
    pub expected_absent: bool,
    pub expected_revision: Option<i64>,
    /// Named JSON callers require an exactly representable structured result.
    pub require_safe_json: bool,
    /// Internal ownership guard; this is never a client-supplied capability.
    pub control_guard: Option<super::store::GoalControlGuard>,
}

/// Serialize provenance and mutations together, independently for each thread.
/// Weak entries are removed as owners leave so unrelated threads never accumulate.
pub async fn thread_mutation_guard(thread: &str) -> OwnedMutexGuard<()> {
    static LOCKS: OnceLock<Mutex<HashMap<String, Weak<AsyncMutex<()>>>>> = OnceLock::new();
    let lock = {
        let mut locks = LOCKS
            .get_or_init(Mutex::default)
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        locks.retain(|_, lock| lock.strong_count() > 0);
        let lock = locks
            .get(thread)
            .and_then(Weak::upgrade)
            .unwrap_or_else(|| {
                let lock = Arc::new(AsyncMutex::new(()));
                locks.insert(thread.to_owned(), Arc::downgrade(&lock));
                lock
            });
        lock
    };
    lock.lock_owned().await
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

pub fn parse_host_status(status: Option<&RuntimeString>) -> io::Result<Option<ThreadGoalStatus>> {
    status
        .map(|status| {
            let status = status
                .try_to_utf8()
                .map_err(|error| invalid(error.to_string()))?;
            match status.as_str() {
                "active" => Ok(ThreadGoalStatus::Active),
                "paused" => Ok(ThreadGoalStatus::Paused),
                "usage_limited" => Ok(ThreadGoalStatus::UsageLimited),
                "budget_limited" => Ok(ThreadGoalStatus::BudgetLimited),
                "complete" => Ok(ThreadGoalStatus::Complete),
                "blocked" => Ok(ThreadGoalStatus::Blocked),
                _ => Err(invalid(format!("unknown goal status `{status}`"))),
            }
        })
        .transpose()
}

pub async fn apply_host_update(
    runtime: &GoalRuntime,
    mutation: HostGoalMutation,
) -> io::Result<GoalHostUpdate> {
    if mutation
        .expected_revision
        .is_some_and(|revision| !(0..=9_007_199_254_740_991).contains(&revision))
    {
        return Err(invalid(
            "expectedGoalRevision must be a nonnegative safe integer",
        ));
    }
    let status = parse_host_status(mutation.status.as_ref())?;
    if mutation
        .token_budget
        .flatten()
        .is_some_and(|budget| budget <= 0)
    {
        return Err(invalid("tokenBudget must be a positive integer or null"));
    }
    let snapshot = runtime.snapshot().await?;
    if mutation
        .expected_revision
        .is_some_and(|expected| expected != snapshot.revision)
        || mutation.expected_goal_id.as_ref().is_some_and(|expected| {
            snapshot
                .goal
                .as_ref()
                .is_none_or(|goal| &goal.goal_id != expected)
        })
        || (mutation.expected_absent && snapshot.goal.is_some())
    {
        return Err(conflict("the goal changed before this update was accepted"));
    }
    let previous = snapshot.goal;
    if previous
        .as_ref()
        .is_none_or(|goal| goal.status == ThreadGoalStatus::Complete)
    {
        let objective = mutation.objective.ok_or_else(|| {
            invalid(if previous.is_some() {
                "a completed goal cannot be resumed; supply an objective to create a new one"
            } else {
                "no goal is set for this thread; supply an objective to create one"
            })
        })?;
        runtime
            .create_host_goal_with_resources_checked(
                objective,
                status,
                mutation.token_budget.flatten(),
                GoalCreatePreconditions {
                    expected_goal_id: mutation.expected_goal_id,
                    expected_absent: mutation.expected_absent,
                    expected_revision: mutation.expected_revision,
                },
                mutation.owned_resources,
                mutation.require_safe_json,
            )
            .await?
            .ok_or_else(|| conflict("the goal changed before this update was accepted"))
    } else {
        runtime
            .apply_host_update_resources(
                GoalUpdate {
                    objective: mutation.objective,
                    status,
                    token_budget: mutation.token_budget,
                    expected_goal_id: mutation.expected_goal_id,
                    expected_revision: mutation.expected_revision,
                    expected_intent_revision: None,
                    require_safe_json: mutation.require_safe_json,
                    control_guard: mutation.control_guard,
                },
                mutation.owned_resources,
            )
            .await?
            .ok_or_else(|| conflict("the goal changed before this update was accepted"))
    }
}

#[derive(Debug, Clone)]
pub enum SavedGoalRequest {
    Read,
    Pause {
        captured: super::store::GoalRecord,
        user_origin: bool,
        foreground_submission_id: Option<RuntimeString>,
        control_guard: Option<super::store::GoalControlGuard>,
    },
    Set {
        mutation: HostGoalMutation,
        user_origin: bool,
    },
    Clear {
        expected_goal_id: Option<RuntimeString>,
        expected_revision: Option<i64>,
        user_origin: bool,
        automatic: bool,
    },
}

/// Run a saved-thread operation while retaining the writer through cancellation.
/// No Session, inference target or provider credential is constructed here.
pub async fn saved_goal_request(
    store: Arc<dyn SessionStore>,
    thread_id: String,
    request: SavedGoalRequest,
    uuid: crate::types::tool::UuidFn,
    now: Arc<dyn Fn() -> i64 + Send + Sync>,
) -> io::Result<GoalSnapshot> {
    tokio::spawn(async move {
        let _guard = thread_mutation_guard(&thread_id).await;
        let runtime = GoalRuntime::new(thread_id.clone().into(), uuid.clone(), now, None);
        if matches!(request, SavedGoalRequest::Read) {
            return runtime.snapshot().await;
        }
        if let SavedGoalRequest::Pause { captured, .. } = &request {
            if captured
                .goal
                .as_ref()
                .is_none_or(|goal| !goal.status.is_active())
            {
                return Ok(GoalSnapshot {
                    goal: captured.goal.clone(),
                    revision: captured.revision,
                });
            }
        }
        // The lease remains owned by this detached task until append, state mutation,
        // flush and shutdown finish, even when the request receiver disappears.
        let acquired = store.resume_for_write(&thread_id).await?;
        let rollout = acquired.rollout;
        let result = async {
            match request {
                SavedGoalRequest::Read => unreachable!(),
                SavedGoalRequest::Pause {
                    captured,
                    user_origin,
                    foreground_submission_id,
                    control_guard,
                } => {
                    if user_origin {
                        if let Some(goal) = captured.goal.filter(|goal| goal.status.is_active()) {
                            let mutation = HostGoalMutation {
                                status: Some("paused".into()),
                                expected_goal_id: Some(goal.goal_id),
                                expected_revision: Some(captured.revision),
                                control_guard,
                                ..Default::default()
                            };
                            let attempt = super::retained_context::pause_control_attempt(
                                format!("goal-control-{}", (uuid)()).into(),
                                foreground_submission_id,
                                &mutation,
                            )?;
                            super::retained_context::append_control_attempt(
                                rollout.as_ref(),
                                &attempt,
                            )
                            .await?;
                            let request_id = attempt
                                .request_id
                                .try_to_utf8()
                                .map_err(|error| invalid(error.to_string()))?;
                            let applied = runtime
                                .apply_host_update_once_for_intent(
                                    mutation,
                                    &request_id,
                                    Some(captured.intent_revision),
                                )
                                .await
                                .map(|_| ());
                            let accepted = super::retained_context::control_attempt_was_applied(
                                &thread_id, &attempt,
                            )
                            .await?;
                            let failure = applied
                                .as_ref()
                                .err()
                                .map(|error| RuntimeString::from(error.to_string()));
                            super::retained_context::append_control_disposition(
                                rollout.as_ref(),
                                &attempt,
                                accepted,
                                failure.as_ref(),
                            )
                            .await?;
                            if !accepted && applied.is_ok() {
                                return Err(invalid(
                                    "goal control committed without an exact receipt",
                                ));
                            }
                            applied?;
                        }
                    } else if let Some(goal) = captured.goal.filter(|goal| goal.status.is_active())
                    {
                        runtime
                            .apply_host_update(super::types::GoalUpdate {
                                status: Some(ThreadGoalStatus::Paused),
                                expected_goal_id: Some(goal.goal_id),
                                expected_revision: Some(captured.revision),
                                expected_intent_revision: Some(captured.intent_revision),
                                control_guard,
                                ..Default::default()
                            })
                            .await?;
                    }
                }
                SavedGoalRequest::Set {
                    mutation,
                    user_origin,
                } => {
                    if mutation.token_budget.is_some() {
                        return Err(invalid("load the thread before changing its goal budget"));
                    }
                    if mutation
                        .status
                        .as_ref()
                        .is_some_and(|status| *status != "paused")
                        || mutation.objective.is_some()
                    {
                        return Err(invalid(
                            "load the thread before creating, editing or resuming its goal",
                        ));
                    }
                    if mutation.require_safe_json {
                        runtime.validate_host_wire_mutation(&mutation).await?;
                    }
                    if user_origin {
                        if let Some(instruction) = super::retained_context::instruction_for_set(
                            (uuid)().into(),
                            mutation.objective.as_ref(),
                            mutation.status.as_ref(),
                        )? {
                            super::retained_context::append_instruction(
                                rollout.as_ref(),
                                &instruction,
                            )
                            .await?;
                        }
                    }
                    apply_host_update(&runtime, mutation).await?;
                }
                SavedGoalRequest::Clear {
                    expected_goal_id,
                    expected_revision,
                    user_origin,
                    automatic,
                } => {
                    if automatic {
                        let current = runtime.goal().await?;
                        if current.as_ref().is_none_or(|goal| {
                            goal.status != ThreadGoalStatus::Complete
                                || expected_goal_id.as_ref() != Some(&goal.goal_id)
                        }) {
                            return Err(invalid(
                                "automatic clear requires the current completed goal identity",
                            ));
                        }
                        let snapshot = runtime.snapshot().await?;
                        super::retained_context::append_completion(
                            rollout.as_ref(),
                            &super::retained_context::GoalCompletion {
                                goal: current.expect("automatic clear checked completion"),
                                goal_revision: snapshot.revision,
                                source_turn_id: acquired
                                    .resume
                                    .last_turn_context
                                    .as_ref()
                                    .and_then(|context| context.turn_id.clone()),
                            },
                        )
                        .await?;
                    } else if user_origin {
                        let instruction =
                            super::retained_context::clear_instruction((uuid)().into());
                        super::retained_context::append_instruction(rollout.as_ref(), &instruction)
                            .await?;
                    }
                    if runtime
                        .clear_goal_if_revision(expected_goal_id, expected_revision)
                        .await?
                        .is_err()
                    {
                        return Err(conflict("the goal changed before clear was accepted"));
                    }
                }
            }
            rollout.flush().await?;
            runtime.snapshot().await
        }
        .await;
        let closed = rollout.shutdown().await;
        match (result, closed) {
            (Err(error), _) | (Ok(_), Err(error)) => Err(error),
            (Ok(snapshot), Ok(())) => Ok(snapshot),
        }
    })
    .await
    .map_err(|error| io::Error::other(format!("saved goal writer failed: {error}")))?
}

/// Bind an idempotency receipt to the exact user mutation, independent of generated IDs.
pub fn request_fingerprint(mutation: &HostGoalMutation) -> io::Result<String> {
    use crate::runtime_values::{runtime_json, RuntimeJson, RuntimeObject};
    use sha2::{Digest, Sha256};
    let mut input = RuntimeObject::new();
    if let Some(objective) = &mutation.objective {
        input.insert("objective", objective.clone().into());
    }
    if let Some(status) = &mutation.status {
        input.insert("status", status.clone().into());
    }
    if let Some(budget) = mutation.token_budget {
        input.insert(
            "tokenBudget",
            budget
                .map(|budget| RuntimeJson::String(budget.to_string().into()))
                .unwrap_or(RuntimeJson::Null),
        );
    }
    if let Some(expected) = &mutation.expected_goal_id {
        input.insert("expectedGoalId", expected.clone().into());
    }
    if mutation.expected_absent {
        input.insert("expectedGoalId", RuntimeJson::Null);
    }
    if let Some(revision) = mutation.expected_revision {
        input.insert(
            "expectedGoalRevision",
            RuntimeJson::String(revision.to_string().into()),
        );
    }
    if let Some(resources) = &mutation.owned_resources {
        let mut canonical = resources.clone();
        canonical
            .files
            .sort_by(|left, right| left.name.cmp(&right.name));
        let encoded = crate::runtime_values::runtime_codec::encode_value(&canonical)
            .map_err(|error| invalid(error.to_string()))?;
        input.insert("ownedResources", encoded);
    }
    let bytes = runtime_json::stringify(&RuntimeJson::Object(input)).to_node_utf8();
    Ok(format!("{:x}", Sha256::digest(bytes)))
}
