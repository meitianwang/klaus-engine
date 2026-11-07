//! Atomic foreground submission shared by facade clients and raw admission.

use super::*;
use klaus_agent_core::engine::message_queue::{QueuePriority, SteerDelivery};
use klaus_agent_core::engine::session::UserContentPart;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnSubmissionMode {
    StartIfIdle,
    ContinueIfIdle,
    Steer,
    StartOrSteer,
}

pub struct TurnSubmission {
    pub submission_id: String,
    pub input: Input,
    pub options: Option<RunOpOptions>,
    pub mode: TurnSubmissionMode,
    pub expected_turn_id: Option<String>,
    pub queue_message_id: Option<String>,
    pub delivery: Option<SteerDelivery>,
    pub display_metadata: Option<RuntimeJson>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnSubmissionRejection {
    NoActiveTurn,
    ExpectedTurnMismatch,
    NotIdle,
    SessionClosing,
    ActiveTurnNotSteerable,
    EmptyInput,
    InvalidInput,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnSubmissionKind {
    Review,
    Compact,
}

pub(super) struct NonSteerableReservation {
    pub(super) submission_id: String,
    pub(super) kind: TurnSubmissionKind,
    pub(super) active: bool,
}

#[derive(Debug)]
pub enum TurnSubmissionOutcome {
    Started {
        turn_id: String,
    },
    Steered {
        turn_id: String,
    },
    NotSubmitted {
        code: TurnSubmissionRejection,
        message: String,
        active_turn_id: Option<String>,
        turn_kind: Option<TurnSubmissionKind>,
    },
}

fn rejected(
    code: TurnSubmissionRejection,
    message: impl Into<String>,
    active: Option<String>,
) -> TurnSubmissionOutcome {
    rejected_for_kind(code, message, active, None)
}

fn rejected_for_kind(
    code: TurnSubmissionRejection,
    message: impl Into<String>,
    active: Option<String>,
    turn_kind: Option<TurnSubmissionKind>,
) -> TurnSubmissionOutcome {
    TurnSubmissionOutcome::NotSubmitted {
        code,
        message: message.into(),
        active_turn_id: active,
        turn_kind,
    }
}

fn blank(text: &RuntimeString) -> bool {
    text.as_utf16()
        .iter()
        .all(|unit| char::from_u32(u32::from(*unit)).is_some_and(char::is_whitespace))
}

fn empty_input(input: &Input) -> bool {
    match input {
        Input::Text(text) => blank(text),
        Input::Parts(parts) => parts
            .iter()
            .all(|part| matches!(part, UserContentPart::Text { text } if blank(text))),
    }
}

/// Review runs beside the serial chain; compact runs on it. Both retain this
/// lease from raw admission until their actual worker ends, including errors.
pub(super) struct BusySubmissionGuard {
    pub(super) inner: Weak<ServerInner>,
    pub(super) token: uuid::Uuid,
}

impl BusySubmissionGuard {
    pub(super) fn activate(&self) {
        if let Some(inner) = self.inner.upgrade() {
            if let Some(owner) = inner
                .run_admission
                .lock()
                .unwrap()
                .non_steerable
                .get_mut(&self.token)
            {
                owner.active = true;
            }
        }
    }
}

impl Drop for BusySubmissionGuard {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            inner
                .run_admission
                .lock()
                .unwrap()
                .non_steerable
                .remove(&self.token);
            inner.schedule_goal_idle();
        }
    }
}

impl ProtocolServer {
    /// The facade owns durable future-turn input. Commit its pause only while
    /// the captured interrupt still owns the foreground admission gate, before
    /// canceling that turn. A stale target never invokes the persistence hook.
    pub fn register_interrupt_commit(
        &self,
        submission_id: String,
        before_cancel: InterruptPreparation,
    ) -> Result<(), String> {
        let mut commits = self.inner.interrupt_commits.lock().unwrap();
        if self.is_closing() || commits.contains_key(&submission_id) {
            return Err("interrupt is closing or already registered".into());
        }
        commits.insert(submission_id, before_cancel);
        Ok(())
    }

    pub fn unregister_interrupt_commit(&self, submission_id: &str) {
        self.inner
            .interrupt_commits
            .lock()
            .unwrap()
            .remove(submission_id);
    }

    /// Internal facade adapter. The state check, queue insertion and foreground
    /// reservation share the same lock as raw runs, goal continuations and Stop.
    pub async fn submit_turn(&self, request: TurnSubmission) -> TurnSubmissionOutcome {
        use TurnSubmissionRejection as Reason;
        if request.mode == TurnSubmissionMode::Steer
            && request
                .expected_turn_id
                .as_ref()
                .is_none_or(String::is_empty)
        {
            return rejected(
                Reason::InvalidInput,
                "steer requires a nonempty expectedTurnId",
                None,
            );
        }
        if request.mode == TurnSubmissionMode::ContinueIfIdle {
            // Continuation reuses history without adding or replaying a user message.
            if !matches!(&request.input, Input::Parts(parts) if parts.is_empty()) {
                return rejected(
                    Reason::InvalidInput,
                    "continuation requires an empty input list",
                    None,
                );
            }
        } else if empty_input(&request.input) {
            return rejected(Reason::EmptyInput, "input must not be empty", None);
        }
        if request.submission_id.is_empty()
            || request
                .expected_turn_id
                .as_ref()
                .is_some_and(String::is_empty)
            || request.queue_message_id.as_ref().is_some_and(|id| {
                !uuid::Uuid::parse_str(id).is_ok_and(|uuid| uuid.to_string() == *id)
            })
        {
            return rejected(
                Reason::InvalidInput,
                "invalid submission, expected turn or queue message identity",
                None,
            );
        }
        // Refusals with an already-known admission state do not even prepare a
        // lazy core. The authoritative check is repeated after preparation.
        {
            let admission = self.inner.run_admission.lock().unwrap();
            if let Err(outcome) = self.inner.submission_target(&admission, &request) {
                return outcome;
            }
        }
        let session = match self.prepare_session().await {
            Ok(session) => session,
            Err(error) => {
                return rejected(
                    if self.is_closing() {
                        Reason::SessionClosing
                    } else {
                        Reason::InvalidInput
                    },
                    error,
                    None,
                )
            }
        };
        self.inner.submit_turn_locked(session, request)
    }
}

impl ServerInner {
    /// `None` selects a start; `Some` names the exact steer recipient.
    fn submission_target(
        &self,
        admission: &RunAdmission,
        request: &TurnSubmission,
    ) -> Result<Option<String>, TurnSubmissionOutcome> {
        use TurnSubmissionRejection as Reason;
        let foreground = admission.foreground.front();
        let compact = admission
            .non_steerable
            .values()
            .find(|owner| owner.kind == TurnSubmissionKind::Compact && owner.active);
        // Waiting compaction must not steal a live regular turn's recipient.
        // Review has an independent delegate; an existing main turn remains
        // steerable while that delegate runs.
        let operation = compact.or_else(|| {
            foreground
                .is_none()
                .then(|| admission.non_steerable.values().next())
                .flatten()
        });
        let active = operation
            .map(|owner| owner.submission_id.clone())
            .or_else(|| foreground.map(|owner| owner.submission_id.clone()));
        let turn_kind = operation.map(|owner| owner.kind);
        let refuse =
            |code, message: &str| Err(rejected_for_kind(code, message, active.clone(), turn_kind));
        if admission.signal.is_cancelled()
            || self.stopped.load(Ordering::SeqCst)
            || self.session_closing.load(Ordering::SeqCst)
        {
            return refuse(Reason::SessionClosing, "session is closing");
        }
        let busy = !admission.foreground.is_empty() || !admission.non_steerable.is_empty();
        if !busy && request.mode == TurnSubmissionMode::Steer {
            return refuse(Reason::NoActiveTurn, "session has no active turn");
        }
        if let Some(expected) = request.expected_turn_id.as_ref() {
            if active.as_ref() != Some(expected) {
                return refuse(
                    Reason::ExpectedTurnMismatch,
                    "the expected turn is no longer current",
                );
            }
        }
        if busy {
            if matches!(
                request.mode,
                TurnSubmissionMode::StartIfIdle | TurnSubmissionMode::ContinueIfIdle
            ) {
                return refuse(Reason::NotIdle, "session already has admitted work");
            }
            let Some(target) = admission
                .steer_turn_id
                .clone()
                .filter(|_| compact.is_none())
            else {
                return refuse(
                    Reason::ActiveTurnNotSteerable,
                    "the current operation cannot accept steering",
                );
            };
            return Ok(Some(target));
        }
        Ok(None)
    }

    fn submit_turn_locked(
        self: &Arc<Self>,
        session: Arc<Session>,
        request: TurnSubmission,
    ) -> TurnSubmissionOutcome {
        use TurnSubmissionRejection as Reason;
        let mut admission = self.run_admission.lock().unwrap();
        let target = match self.submission_target(&admission, &request) {
            Ok(target) => target,
            Err(outcome) => return outcome,
        };
        if let Some(target) = target {
            // Turn options belong to the start branch only. Steering validates
            // against the captured foreground model and never changes settings.
            return match session.enqueue(
                request.input,
                EnqueueOptions {
                    priority: Some(QueuePriority::Now),
                    correlation_id: Some(request.submission_id.into()),
                    queue_message_id: request.queue_message_id.map(Into::into),
                    delivery: request.delivery,
                    display_metadata: request.display_metadata,
                    ..Default::default()
                },
            ) {
                Ok(()) => TurnSubmissionOutcome::Steered { turn_id: target },
                Err(error) => rejected(Reason::InvalidInput, error.message, Some(target)),
            };
        }
        let mut options = request.options.unwrap_or_default();
        if let Some(metadata) = request.display_metadata {
            options.display_metadata = Some(metadata);
        }
        if options.permission_mode.is_some() {
            return rejected(
                Reason::InvalidInput,
                "per-turn permissionMode is retired; use session/setRuntimePolicy",
                None,
            );
        }
        if let Some(schema) = options
            .output_schema
            .as_ref()
            .filter(|schema| runtime_json_truthy(schema))
        {
            if let Err(error) = scalar_json_value(schema, "run outputSchema") {
                return rejected(Reason::InvalidInput, error, None);
            }
        }
        let validation_options = TurnOptions {
            model: options.model.clone().filter(|model| !model.is_empty()),
            effort: options.effort.clone(),
            ultra_mode: options.ultra_mode,
            ..Default::default()
        };
        if let Err(error) =
            session.validate_input_for_turn(request.input.clone(), &validation_options)
        {
            return rejected(Reason::InvalidInput, error.message, None);
        }
        // Keep chain availability and reservation insertion inside admission.
        // A failed send retains its guard until after the lock is released.
        let chain = self.chain_tx.lock().unwrap();
        let Some(tx) = chain.as_ref().filter(|tx| !tx.is_closed()) else {
            return rejected(
                Reason::SessionClosing,
                "session execution queue is closed",
                None,
            );
        };
        let signal = admission.child_token();
        let token = uuid::Uuid::new_v4();
        let turn_id = request.submission_id;
        admission.foreground.push_back(ForegroundReservation {
            token,
            submission_id: turn_id.clone(),
            early_steer: true,
            signal: signal.clone(),
        });
        admission.activate_front();
        admission.refresh_early_steer(false);
        let guard = CurrentSubmissionGuard {
            inner: Arc::downgrade(self),
            submission_id: turn_id.clone(),
            reservation_token: token,
            goal_reschedule: true,
            goal_decision: None,
        };
        let inner = self.clone();
        let sid = turn_id.clone();
        let job = ChainJob {
            submission_id: turn_id.clone(),
            run: Box::new(move || {
                Box::pin(async move {
                    inner
                        .run_turn(
                            sid,
                            Some(request.input),
                            Some(options),
                            signal,
                            guard,
                            false,
                        )
                        .await
                })
            }),
        };
        let result = tx.send(job);
        drop(chain);
        drop(admission);
        match result {
            Ok(()) => TurnSubmissionOutcome::Started { turn_id },
            Err(job) => {
                drop(job);
                rejected(
                    Reason::SessionClosing,
                    "session execution queue is closed",
                    None,
                )
            }
        }
    }
}
