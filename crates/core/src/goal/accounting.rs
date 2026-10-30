//! Per-turn goal accounting: what to charge the goal, and once only.
//!
//! The engine reports CUMULATIVE usage for a turn, so charging a goal means
//! diffing against the last charge rather than adding the report. Two things
//! make that non-trivial and are handled here:
//!
//!   - a turn issues several usage reports (one per provider response) and
//!     several accounting passes (each tool finish, plus the turn's end), so
//!     the baseline has to advance with every charge or the same tokens get
//!     billed repeatedly;
//!   - only the work of a turn that is actually pursuing the goal counts, so a
//!     turn that starts before the goal exists contributes nothing before the
//!     `create_goal` call that adopts it.

use crate::runtime_values::RuntimeString;
use crate::schemas::message::Usage;
use crate::sync_ext::MutexExt;
use std::sync::Mutex;

/// What the caller intends to do with the active-goal marker after a charge
/// that stopped the goal on its budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetLimitedGoalDisposition {
    /// Keep the marker: the turn is still running and later passes in the same
    /// turn must still be able to observe the goal they belong to.
    KeepActive,
    /// Drop the marker: the turn is over (or failed), so nothing more may be
    /// charged to this goal.
    ClearActive,
}

/// Tokens one usage report contributes to a goal.
///
/// Provider normalization has already removed cache reads from input tokens.
/// Goal spending adds the disjoint uncached input and output fields.
pub fn goal_token_delta_for_usage(usage: &Usage) -> i64 {
    // Usage fields are disjoint: input_tokens is already uncached input.
    // This equals max(raw input - cached input, 0) + raw output at the
    // provider normalization boundary. Cache and total fields are metadata.
    let input = i64::try_from(usage.input_tokens).unwrap_or(i64::MAX);
    let output = i64::try_from(usage.output_tokens).unwrap_or(i64::MAX);
    input.saturating_add(output)
}

/// A charge to apply: the token delta since the last charge plus the wall time
/// since the last charge, bound to the goal generation that owns them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoalProgressSnapshot {
    pub goal_id: RuntimeString,
    pub token_delta: i64,
    pub time_delta_seconds: i64,
    /// The cumulative usage this snapshot advances the baseline to, once the
    /// charge lands.
    pub accounted_tokens: i64,
}

#[derive(Debug, Default)]
struct AccountingInner {
    /// The goal generation the CURRENT turn is pursuing, when any.
    active_goal_id: Option<RuntimeString>,
    /// Cumulative goal-relevant tokens as of the last charge.
    last_accounted_tokens: i64,
    /// Cumulative goal-relevant tokens as of the latest usage report.
    observed_tokens: i64,
    /// Epoch ms the last charge (or the turn start) was taken at.
    last_accounted_at_ms: i64,
    /// Active milliseconds not yet large enough to persist as a whole second.
    fractional_millis: i64,
    clock_running: bool,
    clock_generation: Option<RuntimeString>,
}

/// The turn-scoped accounting state of one thread.
#[derive(Debug, Default)]
pub struct GoalAccountingState {
    inner: Mutex<AccountingInner>,
}

impl GoalAccountingState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Begin a turn: usage baselines reset to zero because the engine's report
    /// is per-turn cumulative, and the clock baseline becomes `now_ms`.
    ///
    /// The active-goal marker is NOT set here. A turn only becomes a goal turn
    /// when the goal is observed, which for a `create_goal` happens mid-turn.
    pub fn start_turn(&self, now_ms: i64) {
        let mut inner = self.inner.lock_recover();
        inner.last_accounted_tokens = 0;
        inner.observed_tokens = 0;
        if !inner.clock_running {
            inner.last_accounted_at_ms = now_ms;
        }
    }

    pub fn mark_goal_active(&self, goal_id: &RuntimeString) {
        let mut inner = self.inner.lock_recover();
        if inner.clock_generation.as_ref() != Some(goal_id) {
            inner.fractional_millis = 0;
            inner.clock_generation = Some(goal_id.clone());
        }
        inner.active_goal_id = Some(goal_id.clone());
        inner.clock_running = true;
    }

    pub fn mark_goal_active_from_now(&self, goal_id: &RuntimeString, now_ms: i64) {
        let mut inner = self.inner.lock_recover();
        if inner.clock_generation.as_ref() != Some(goal_id) {
            inner.fractional_millis = 0;
            inner.clock_generation = Some(goal_id.clone());
        }
        inner.active_goal_id = Some(goal_id.clone());
        inner.last_accounted_tokens = inner.observed_tokens;
        inner.last_accounted_at_ms = now_ms;
        inner.clock_running = true;
    }

    pub fn clear_active_goal(&self) {
        let mut inner = self.inner.lock_recover();
        inner.active_goal_id = None;
        inner.clock_running = false;
    }

    /// Freeze active time at a mode boundary without discarding usage still in flight.
    pub fn pause_clock(&self, now_ms: i64) {
        let mut inner = self.inner.lock_recover();
        if inner.clock_running {
            inner.fractional_millis = inner
                .fractional_millis
                .saturating_add(now_ms.saturating_sub(inner.last_accounted_at_ms).max(0));
        }
        inner.last_accounted_at_ms = now_ms;
        inner.clock_running = false;
    }

    pub fn clear_active_goal_at(&self, now_ms: i64) {
        self.pause_clock(now_ms);
        self.inner.lock_recover().active_goal_id = None;
    }

    pub fn active_goal_id(&self) -> Option<RuntimeString> {
        self.inner.lock_recover().active_goal_id.clone()
    }

    /// Record one provider response's usage.
    ///
    /// The engine reports usage PER RESPONSE, and a turn makes several calls, so
    /// this accumulates. Taking the largest report instead would silently drop
    /// every response after the biggest one.
    pub fn record_token_usage(&self, usage: &Usage) {
        let mut inner = self.inner.lock_recover();
        let delta = goal_token_delta_for_usage(usage);
        inner.observed_tokens = inner.observed_tokens.saturating_add(delta.max(0));
    }

    /// Compare ownership and add usage under one lock, including replacement races.
    pub fn record_token_usage_for(&self, goal_id: &RuntimeString, usage: &Usage) {
        let mut inner = self.inner.lock_recover();
        if inner.clock_generation.as_ref() == Some(goal_id) {
            inner.active_goal_id = Some(goal_id.clone());
            inner.observed_tokens = inner
                .observed_tokens
                .saturating_add(goal_token_delta_for_usage(usage).max(0));
        }
    }

    /// The charge owed right now, or `None` when this turn is not pursuing a
    /// goal or nothing has accrued.
    pub fn progress_snapshot(&self, now_ms: i64) -> Option<GoalProgressSnapshot> {
        let inner = self.inner.lock_recover();
        let goal_id = inner.active_goal_id.clone()?;
        let token_delta = inner
            .observed_tokens
            .saturating_sub(inner.last_accounted_tokens);
        let elapsed = if inner.clock_running {
            now_ms.saturating_sub(inner.last_accounted_at_ms).max(0)
        } else {
            0
        };
        let time_delta_seconds = inner.fractional_millis.saturating_add(elapsed) / 1_000;
        if token_delta <= 0 && time_delta_seconds <= 0 {
            return None;
        }
        Some(GoalProgressSnapshot {
            goal_id,
            token_delta: token_delta.max(0),
            time_delta_seconds: time_delta_seconds.max(0),
            accounted_tokens: inner.observed_tokens,
        })
    }

    /// Advance the baselines after a charge landed, and drop the active-goal
    /// marker when the caller says the goal is done accruing.
    pub fn mark_progress_accounted(
        &self,
        snapshot: &GoalProgressSnapshot,
        now_ms: i64,
        clear_active_goal: bool,
    ) {
        let mut inner = self.inner.lock_recover();
        inner.last_accounted_tokens = snapshot.accounted_tokens;
        let elapsed = if inner.clock_running {
            now_ms.saturating_sub(inner.last_accounted_at_ms).max(0)
        } else {
            0
        };
        inner.fractional_millis = inner
            .fractional_millis
            .saturating_add(elapsed)
            .saturating_sub(snapshot.time_delta_seconds.saturating_mul(1_000))
            .max(0);
        inner.last_accounted_at_ms = now_ms;
        if clear_active_goal {
            inner.active_goal_id = None;
            inner.clock_running = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn usage(input: u64, cached: u64, output: u64) -> Usage {
        Usage {
            input_tokens: input,
            output_tokens: output,
            cache_read_input_tokens: cached,
            cache_creation_input_tokens: 0,
            total_tokens: None,
        }
    }

    #[test]
    fn token_delta_uses_disjoint_uncached_input_and_output() {
        assert_eq!(goal_token_delta_for_usage(&usage(600, 400, 250)), 850);
        // Input and cache counters are disjoint, so metadata does not reduce uncached input.
        assert_eq!(goal_token_delta_for_usage(&usage(0, 500, 50)), 50);
    }

    #[test]
    fn charges_are_deltas_against_the_last_charge() {
        let state = GoalAccountingState::new();
        state.start_turn(0);
        state.mark_goal_active(&"g1".into());
        state.record_token_usage(&usage(100, 0, 50));

        let first = state.progress_snapshot(2_000).expect("a charge is owed");
        assert_eq!(first.token_delta, 150);
        assert_eq!(first.time_delta_seconds, 2);
        state.mark_progress_accounted(&first, 2_000, false);

        // No new usage: nothing owed at the same instant.
        assert!(state.progress_snapshot(2_000).is_none());

        state.record_token_usage(&usage(300, 0, 90));
        let second = state.progress_snapshot(5_000).expect("a charge is owed");
        assert_eq!(
            second.token_delta, 390,
            "only the tokens since the previous charge"
        );
        assert_eq!(second.time_delta_seconds, 3);
    }

    #[test]
    fn several_responses_in_one_turn_accumulate() {
        let state = GoalAccountingState::new();
        state.start_turn(0);
        state.mark_goal_active(&"g1".into());
        state.record_token_usage(&usage(500, 0, 100));
        state.record_token_usage(&usage(10, 0, 5));
        assert_eq!(
            state.progress_snapshot(1_000).unwrap().token_delta,
            615,
            "per-response reports are summed, not maxed"
        );
    }

    #[test]
    fn a_goal_created_mid_turn_does_not_inherit_earlier_work() {
        let state = GoalAccountingState::new();
        state.start_turn(0);
        state.record_token_usage(&usage(900, 0, 100));
        // Nothing is owed before a goal is adopted.
        assert!(state.progress_snapshot(1_000).is_none());

        state.mark_goal_active_from_now(&"g1".into(), 1_000);
        assert!(
            state.progress_snapshot(1_000).is_none(),
            "the pre-goal 1000 tokens belong to no goal"
        );
        state.record_token_usage(&usage(200, 0, 50));
        let snapshot = state.progress_snapshot(3_000).expect("a charge is owed");
        assert_eq!(snapshot.token_delta, 250);
        assert_eq!(snapshot.time_delta_seconds, 2);
    }

    #[test]
    fn clearing_the_active_goal_stops_further_charges() {
        let state = GoalAccountingState::new();
        state.start_turn(0);
        state.mark_goal_active(&"g1".into());
        state.record_token_usage(&usage(100, 0, 0));
        let snapshot = state.progress_snapshot(1_000).unwrap();
        state.mark_progress_accounted(&snapshot, 1_000, /*clear_active_goal*/ true);
        state.record_token_usage(&usage(500, 0, 0));
        assert!(state.progress_snapshot(9_000).is_none());
        assert!(state.active_goal_id().is_none());
    }

    #[test]
    fn a_new_turn_resets_the_usage_baseline() {
        let state = GoalAccountingState::new();
        state.start_turn(0);
        state.mark_goal_active(&"g1".into());
        state.record_token_usage(&usage(400, 0, 0));
        let snapshot = state.progress_snapshot(1_000).unwrap();
        state.mark_progress_accounted(&snapshot, 1_000, false);

        // The next turn reports from zero again; the first charge of that turn
        // must be its own tokens, not a negative diff.
        state.start_turn(10_000);
        state.mark_goal_active(&"g1".into());
        state.record_token_usage(&usage(70, 0, 30));
        let next = state.progress_snapshot(11_000).expect("a charge is owed");
        assert_eq!(next.token_delta, 100);
        assert_eq!(
            next.time_delta_seconds, 10,
            "the active clock includes the idle interval between turns"
        );
    }
}
