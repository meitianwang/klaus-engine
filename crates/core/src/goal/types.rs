//! The persisted thread-goal record and its wire codecs.
//!
//! One thread owns at most one goal. The record is versioned by `goal_id` so a
//! writer that read an older generation cannot clobber a replacement (see
//! [`GoalUpdate::expected_goal_id`]).

use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};

/// Objective length cap. Beyond this the tool layer rejects the create.
pub const GOAL_OBJECTIVE_MAX_LENGTH: usize = 4_000;

fn decode_unit_enum<T: Copy>(
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

/// The lifecycle state of a thread goal.
///
/// Who may set what is not symmetric, and that asymmetry is the point:
///   - the MODEL may claim completion, a repeated blocking condition, or a pause explicitly requested by the user;
///   - the USER/host may explicitly set any lifecycle state;
///   - the ENGINE also sets [`Self::BudgetLimited`] (token budget exhausted)
///     and [`Self::UsageLimited`] (provider usage limit hit mid-turn).
///
/// A model may not pause on its own initiative; that semantic restriction is
/// carried by the tool instructions while the runtime enforces status authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadGoalStatus {
    Active,
    Paused,
    Blocked,
    UsageLimited,
    BudgetLimited,
    Complete,
}

impl ThreadGoalStatus {
    /// The wire tag, which is also what the goal tools echo back to the model.
    pub fn wire_name(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Blocked => "blocked",
            Self::UsageLimited => "usage_limited",
            Self::BudgetLimited => "budget_limited",
            Self::Complete => "complete",
        }
    }

    /// Whether the goal is still being pursued. Only an active goal drives
    /// automatic continuations and only an active goal accrues usage.
    pub fn is_active(self) -> bool {
        matches!(self, Self::Active)
    }

    /// Whether the goal has stopped for a reason that a resume can undo.
    pub fn is_resumable(self) -> bool {
        matches!(self, Self::Paused | Self::Blocked | Self::UsageLimited)
    }

    /// Whether a NEW goal may replace this one without an explicit clear.
    /// Only a finished goal may be replaced; an unfinished one must be
    /// cleared or completed first, so a model cannot silently swap objectives.
    pub fn allows_replacement(self) -> bool {
        matches!(self, Self::Complete)
    }
}

impl RuntimeEncode for ThreadGoalStatus {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(out, self.wire_name())
    }
}

impl RuntimeDecode for ThreadGoalStatus {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("active", &[]),
                RuntimeEnumVariant::new("paused", &[]),
                RuntimeEnumVariant::new("blocked", &[]),
                RuntimeEnumVariant::new("usage_limited", &[]),
                RuntimeEnumVariant::new("budget_limited", &[]),
                RuntimeEnumVariant::new("complete", &[]),
            ],
            &[
                Self::Active,
                Self::Paused,
                Self::Blocked,
                Self::UsageLimited,
                Self::BudgetLimited,
                Self::Complete,
            ],
        )
    }
}

/// The persisted goal of one thread.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadGoal {
    pub thread_id: RuntimeString,
    /// Generation id. A fresh goal gets a fresh id, so a stale writer's
    /// `expected_goal_id` no longer matches and its update is refused.
    pub goal_id: RuntimeString,
    pub objective: RuntimeString,
    pub status: ThreadGoalStatus,
    /// Optional token ceiling. `None` means "no budget", which is also the
    /// only shape with no automatic stop.
    pub token_budget: Option<i64>,
    pub tokens_used: i64,
    pub time_used_seconds: i64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

impl ThreadGoal {
    /// Tokens left before the budget stops the goal. `None` without a budget.
    pub fn remaining_tokens(&self) -> Option<i64> {
        self.token_budget
            .map(|budget| budget.saturating_sub(self.tokens_used).max(0))
    }

    /// Whether accrued usage has reached the budget.
    pub fn budget_exhausted(&self) -> bool {
        match self.token_budget {
            Some(budget) if budget > 0 => self.tokens_used >= budget,
            _ => false,
        }
    }
}

impl RuntimeEncode for ThreadGoal {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        crate::goal::tools::validate_goal_result(self)
            .map_err(|error| RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(error.into())))?;
        let Self {
            thread_id,
            goal_id,
            objective,
            status,
            token_budget,
            tokens_used,
            time_used_seconds,
            created_at_ms,
            updated_at_ms,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("threadId", thread_id)?;
        object.field("goalId", goal_id)?;
        object.field("objective", objective)?;
        object.field("status", status)?;
        if let Some(value) = token_budget {
            object.field("tokenBudget", value)?;
        }
        object.field("tokensUsed", tokens_used)?;
        object.field("timeUsedSeconds", time_used_seconds)?;
        object.field("createdAtMs", created_at_ms)?;
        object.field("updatedAtMs", updated_at_ms)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ThreadGoal {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                thread_id: object.required("threadId", &[])?,
                goal_id: object.required("goalId", &[])?,
                objective: object.required("objective", &[])?,
                status: object.required("status", &[])?,
                token_budget: object.optional("tokenBudget", &[])?,
                tokens_used: object.required("tokensUsed", &[])?,
                time_used_seconds: object.required("timeUsedSeconds", &[])?,
                created_at_ms: object.required("createdAtMs", &[])?,
                updated_at_ms: object.required("updatedAtMs", &[])?,
            })
        })
    }
}

/// A partial mutation of an existing goal. Absent fields are left alone, so a
/// status change never silently rewrites the objective.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GoalUpdate {
    pub objective: Option<RuntimeString>,
    pub status: Option<ThreadGoalStatus>,
    /// Two levels of optional on purpose: outer `None` leaves the budget as it
    /// is, `Some(None)` REMOVES it.
    pub token_budget: Option<Option<i64>>,
    /// When set, the update applies only if the stored generation matches.
    pub expected_goal_id: Option<RuntimeString>,
    /// When set, the update also compares the observed thread revision.
    pub expected_revision: Option<i64>,
    /// Private control epoch: accounting updates do not advance this value.
    pub expected_intent_revision: Option<i64>,
    /// Refuse a mutation whose structured JSON result cannot be represented exactly.
    pub require_safe_json: bool,
    /// Engine-owned admission guard evaluated within the authoritative transaction.
    pub control_guard: Option<crate::goal::store::GoalControlGuard>,
}

/// Which statuses an accounting pass is allowed to charge.
///
/// Usage is only ever charged to a goal the caller believes is still running;
/// a stopped goal must not keep absorbing tokens from unrelated later work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GoalAccountingMode {
    /// Charge while the goal is still being pursued. `budget_limited` counts:
    /// the turn that crossed the budget keeps running to wrap up, and those
    /// tokens were spent on this goal, so freezing the counters at the crossing
    /// would under-report what the goal actually cost.
    ActiveOnly,
    /// Also charge on the pass that observes completion, so the final tokens of
    /// the completing turn are not lost.
    ActiveOrComplete,
    /// Charge in every non-finished state, including a stop the goal is already
    /// sitting in. This is the mode for the write that declares a goal blocked:
    /// the work of the turn that gave up still belongs to the goal.
    ActiveOrStopped,
}

impl GoalAccountingMode {
    fn admits(self, status: ThreadGoalStatus) -> bool {
        match self {
            Self::ActiveOnly => {
                status.is_active() || matches!(status, ThreadGoalStatus::BudgetLimited)
            }
            Self::ActiveOrComplete => {
                status.is_active()
                    || matches!(
                        status,
                        ThreadGoalStatus::BudgetLimited | ThreadGoalStatus::Complete
                    )
            }
            Self::ActiveOrStopped => status != ThreadGoalStatus::Complete,
        }
    }

    /// Whether the stored goal may be charged under this mode.
    pub fn admits_status(self, status: ThreadGoalStatus) -> bool {
        self.admits(status)
    }
}

/// The result of an accounting pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GoalAccountingOutcome {
    /// Nothing was charged. Carries the stored goal when there was one, so a
    /// caller can still see the status that refused the charge.
    Unchanged(Option<ThreadGoal>),
    /// Usage was charged; carries the goal AFTER the write, including a status
    /// the write itself flipped to `budget_limited`.
    Updated(ThreadGoal),
}

impl GoalAccountingOutcome {
    /// The stored goal, charged or not.
    pub fn goal(&self) -> Option<&ThreadGoal> {
        match self {
            Self::Unchanged(goal) => goal.as_ref(),
            Self::Updated(goal) => Some(goal),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn goal() -> ThreadGoal {
        ThreadGoal {
            thread_id: "sess_1".into(),
            goal_id: "goal_1".into(),
            objective: "ship it".into(),
            status: ThreadGoalStatus::Active,
            token_budget: Some(1_000),
            tokens_used: 400,
            time_used_seconds: 12,
            created_at_ms: 1,
            updated_at_ms: 2,
        }
    }

    #[test]
    fn status_wire_names_round_trip() {
        for status in [
            ThreadGoalStatus::Active,
            ThreadGoalStatus::Paused,
            ThreadGoalStatus::Blocked,
            ThreadGoalStatus::UsageLimited,
            ThreadGoalStatus::BudgetLimited,
            ThreadGoalStatus::Complete,
        ] {
            let encoded = runtime_codec::encode_value(&status).expect("status encodes");
            let decoded = ThreadGoalStatus::decode_runtime(&encoded).expect("status decodes");
            assert_eq!(decoded, status);
        }
    }

    #[test]
    fn goal_round_trips_including_absent_budget() {
        let encoded = runtime_codec::encode_value(&goal()).expect("goal encodes");
        assert_eq!(ThreadGoal::decode_runtime(&encoded).unwrap(), goal());

        let mut budgetless = goal();
        budgetless.token_budget = None;
        let encoded = runtime_codec::encode_value(&budgetless).expect("goal encodes");
        assert!(encoded.get("tokenBudget").is_none());
        assert_eq!(ThreadGoal::decode_runtime(&encoded).unwrap(), budgetless);
    }

    #[test]
    fn remaining_tokens_floors_at_zero() {
        let mut over = goal();
        over.tokens_used = 5_000;
        assert_eq!(over.remaining_tokens(), Some(0));
        assert!(over.budget_exhausted());

        let mut budgetless = goal();
        budgetless.token_budget = None;
        assert_eq!(budgetless.remaining_tokens(), None);
        assert!(!budgetless.budget_exhausted());
    }

    #[test]
    fn only_a_complete_goal_may_be_replaced() {
        assert!(ThreadGoalStatus::Complete.allows_replacement());
        for status in [
            ThreadGoalStatus::Active,
            ThreadGoalStatus::Paused,
            ThreadGoalStatus::Blocked,
            ThreadGoalStatus::UsageLimited,
            ThreadGoalStatus::BudgetLimited,
        ] {
            assert!(!status.allows_replacement(), "{status:?}");
        }
    }

    #[test]
    fn accounting_modes_gate_by_status() {
        assert!(GoalAccountingMode::ActiveOnly.admits_status(ThreadGoalStatus::Active));
        // The turn that crossed the budget is still charged for its wrap-up.
        assert!(GoalAccountingMode::ActiveOnly.admits_status(ThreadGoalStatus::BudgetLimited));
        assert!(!GoalAccountingMode::ActiveOnly.admits_status(ThreadGoalStatus::Complete));
        assert!(!GoalAccountingMode::ActiveOnly.admits_status(ThreadGoalStatus::Paused));
        assert!(GoalAccountingMode::ActiveOrComplete.admits_status(ThreadGoalStatus::Complete));
        assert!(!GoalAccountingMode::ActiveOrComplete.admits_status(ThreadGoalStatus::Blocked));
        assert!(GoalAccountingMode::ActiveOrStopped.admits_status(ThreadGoalStatus::Blocked));
        assert!(GoalAccountingMode::ActiveOrStopped.admits_status(ThreadGoalStatus::Paused));
        assert!(!GoalAccountingMode::ActiveOrStopped.admits_status(ThreadGoalStatus::Complete));
    }
}
