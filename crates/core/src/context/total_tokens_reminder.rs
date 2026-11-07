//! The remaining-context line handed to the model each turn.
//!
//! The model cannot see the window it is filling. Left unsignalled it writes as
//! if space were unbounded and only discovers otherwise when the harness compacts
//! underneath it — losing the chance to shorten its own output or shed history it
//! no longer needs. One short line per turn closes that gap.
//!
//! The line is deliberately volatile: it changes nearly every turn, so callers
//! must place it as the LAST system part, after every cache breakpoint, or it
//! would invalidate the cached prefix on every single request.

use crate::runtime_process::RuntimeEnv;
use crate::runtime_values::RuntimeString;
use crate::schemas::config::{ResolvedConfig, TotalTokensReminderMode, FIXED_TOTAL_TOKENS};

use super::compact::window::{actual_model_window, environment_text, parse_environment_integer};

pub const REMINDER_MODE_ENV: &str = "KLAUS_TOTAL_TOKENS_REMINDER";
pub const REMINDER_BUDGET_ENV: &str = "KLAUS_TOTAL_TOKENS_REMINDER_BUDGET";
pub const REMINDER_AFTER_USER_ENV: &str = "KLAUS_TOTAL_TOKENS_REMINDER_AFTER_USER_TURN";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReminderPolicy {
    pub mode: TotalTokensReminderMode,
    pub budget: u64,
    pub after_user_turn: bool,
}

pub fn resolve_reminder_policy(config: &ResolvedConfig) -> ReminderPolicy {
    ReminderPolicy {
        mode: config.total_tokens_reminder,
        budget: config.total_tokens_reminder_budget,
        after_user_turn: config
            .total_tokens_reminder_after_user_turn
            .unwrap_or(false),
    }
}

pub(crate) fn bind_reminder_environment(config: &mut ResolvedConfig, env: &RuntimeEnv) {
    if let Some(mode) = env
        .get(&RuntimeString::from(REMINDER_MODE_ENV))
        .and_then(environment_text)
        .and_then(|mode| match mode.as_str() {
            "off" => Some(TotalTokensReminderMode::Off),
            "infinite" => Some(TotalTokensReminderMode::Infinite),
            "fixed" => Some(TotalTokensReminderMode::Fixed),
            "countdown" => Some(TotalTokensReminderMode::Countdown),
            "padded-countdown" => Some(TotalTokensReminderMode::PaddedCountdown),
            _ => None,
        })
    {
        config.total_tokens_reminder = mode;
    }
    if let Some(budget) = env
        .get(&RuntimeString::from(REMINDER_BUDGET_ENV))
        .and_then(parse_environment_integer)
        .filter(|budget| budget.is_finite() && *budget > 0.0 && *budget < u64::MAX as f64)
    {
        config.total_tokens_reminder_budget = budget as u64;
    }
    if let Some(enabled) = env
        .get(&RuntimeString::from(REMINDER_AFTER_USER_ENV))
        .and_then(environment_text)
        .and_then(|value| match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
    {
        config.total_tokens_reminder_after_user_turn = Some(enabled);
    }
}

/// The long-horizon spend that a padded countdown measures against its budget.
///
/// Monotonic by construction: `base` carries the tokens already dropped by earlier
/// compactions and `current` is the live occupancy, so summarizing history away
/// cannot make the reported remainder rewind upward. `high_water` pins that
/// guarantee even when the live occupancy shrinks faster than `base` grows.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PaddedCountdownSpend {
    /// Tokens retired by earlier compactions: spent, and never refunded.
    pub base_tokens: u64,
    /// The highest total this session has ever reported.
    pub high_water_tokens: u64,
}

impl PaddedCountdownSpend {
    /// Fold the live occupancy into the spend and return the monotonic total.
    pub fn total_with(&self, current_tokens: u64) -> u64 {
        self.high_water_tokens
            .max(self.base_tokens.saturating_add(current_tokens))
    }
}

/// Per-agent task accounting. Persist this across automatic continuations, and
/// call `reanchor` only when a fresh ordinary user request starts a new task.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TaskCountdownAnchor {
    anchor_total: Option<u64>,
    high_water_tokens: u64,
}

impl TaskCountdownAnchor {
    pub fn reanchor(&mut self, spend: PaddedCountdownSpend, current_tokens: u64) {
        self.anchor_total = Some(spend.base_tokens.saturating_add(current_tokens));
        self.high_water_tokens = 0;
    }

    pub fn is_anchored(&self) -> bool {
        self.anchor_total.is_some()
    }

    pub fn remaining(
        &mut self,
        budget: u64,
        spend: PaddedCountdownSpend,
        current_tokens: u64,
    ) -> u64 {
        let spent = self.anchor_total.map_or_else(
            || spend.total_with(current_tokens),
            |anchor| {
                spend
                    .base_tokens
                    .saturating_add(current_tokens)
                    .saturating_sub(anchor)
            },
        );
        self.high_water_tokens = self.high_water_tokens.max(spent);
        budget.saturating_sub(self.high_water_tokens)
    }
}

/// Only the occupancy actually removed remains spent after a rewrite.
pub fn retired_context_tokens(before_tokens: u64, after_tokens: u64) -> u64 {
    before_tokens.saturating_sub(after_tokens)
}

/// Render from current agent-owned accounting. Placement is request-only: the
/// caller appends this after its cache breakpoints or selected content carrier.
pub fn render_total_tokens_reminder(
    config: &ResolvedConfig,
    used_tokens: u64,
    spend: PaddedCountdownSpend,
    task_anchor: Option<&mut TaskCountdownAnchor>,
) -> Option<RuntimeString> {
    let policy = resolve_reminder_policy(config);
    let remaining =
        if policy.mode == TotalTokensReminderMode::PaddedCountdown && policy.after_user_turn {
            match task_anchor {
                Some(anchor) => anchor.remaining(policy.budget, spend, used_tokens),
                None => remaining_tokens(
                    policy.mode,
                    actual_model_window(config),
                    used_tokens,
                    policy.budget,
                    spend,
                ),
            }
        } else {
            remaining_tokens(
                policy.mode,
                actual_model_window(config),
                used_tokens,
                policy.budget,
                spend,
            )
        };
    format_total_tokens_reminder(policy.mode, remaining)
}

/// The remaining-token figure for `mode`.
///
/// `Off` and `Infinite` have no numeric answer — the caller renders those without
/// consulting this. `Fixed` reports a constant so a run can be compared against
/// one that never appears to shrink.
pub fn remaining_tokens(
    mode: TotalTokensReminderMode,
    context_window: u64,
    used_tokens: u64,
    padded_budget: u64,
    padded_spend: PaddedCountdownSpend,
) -> u64 {
    match mode {
        TotalTokensReminderMode::Off | TotalTokensReminderMode::Infinite => 0,
        TotalTokensReminderMode::Fixed => FIXED_TOTAL_TOKENS,
        TotalTokensReminderMode::Countdown => context_window.saturating_sub(used_tokens),
        TotalTokensReminderMode::PaddedCountdown => {
            padded_budget.saturating_sub(padded_spend.total_with(used_tokens))
        }
    }
}

/// Render the reminder line, or `None` when the mode says nothing.
pub fn format_total_tokens_reminder(
    mode: TotalTokensReminderMode,
    remaining_tokens: u64,
) -> Option<RuntimeString> {
    let body = match mode {
        TotalTokensReminderMode::Off => return None,
        TotalTokensReminderMode::Infinite => "Infinite".to_string(),
        _ => remaining_tokens.to_string(),
    };
    Some(RuntimeString::from(
        format!("<total_tokens>{body} tokens left</total_tokens>").as_str(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn off_says_nothing_at_all() {
        assert_eq!(
            format_total_tokens_reminder(TotalTokensReminderMode::Off, 1_000),
            None
        );
    }

    #[test]
    fn a_countdown_reports_the_room_actually_left() {
        let remaining = remaining_tokens(
            TotalTokensReminderMode::Countdown,
            200_000,
            150_000,
            0,
            PaddedCountdownSpend::default(),
        );
        assert_eq!(remaining, 50_000);
        assert_eq!(
            format_total_tokens_reminder(TotalTokensReminderMode::Countdown, remaining),
            Some(RuntimeString::from(
                "<total_tokens>50000 tokens left</total_tokens>"
            ))
        );
    }

    #[test]
    fn a_countdown_floors_at_zero_instead_of_underflowing() {
        assert_eq!(
            remaining_tokens(
                TotalTokensReminderMode::Countdown,
                200_000,
                250_000,
                0,
                PaddedCountdownSpend::default(),
            ),
            0
        );
    }

    #[test]
    fn infinite_and_fixed_report_a_budget_that_never_shrinks() {
        assert_eq!(
            format_total_tokens_reminder(TotalTokensReminderMode::Infinite, 7),
            Some(RuntimeString::from(
                "<total_tokens>Infinite tokens left</total_tokens>"
            ))
        );
        let fixed = remaining_tokens(
            TotalTokensReminderMode::Fixed,
            200_000,
            190_000,
            0,
            PaddedCountdownSpend::default(),
        );
        assert_eq!(fixed, FIXED_TOTAL_TOKENS);
    }

    #[test]
    fn a_padded_countdown_keeps_descending_across_a_compaction() {
        let budget = 1_000_000;
        // Before compacting: 400k occupied, nothing retired yet.
        let before = remaining_tokens(
            TotalTokensReminderMode::PaddedCountdown,
            200_000,
            400_000,
            budget,
            PaddedCountdownSpend::default(),
        );
        assert_eq!(before, 600_000);

        // Compaction retires those 400k and leaves 20k of summary behind. A plain
        // countdown would jump back up; the padded one must not.
        let spend = PaddedCountdownSpend {
            base_tokens: 400_000,
            high_water_tokens: 400_000,
        };
        let after = remaining_tokens(
            TotalTokensReminderMode::PaddedCountdown,
            200_000,
            20_000,
            budget,
            spend,
        );
        assert_eq!(after, 580_000);
        assert!(after < before, "the budget must keep descending");
    }

    #[test]
    fn a_padded_countdown_never_rewinds_when_occupancy_drops() {
        let spend = PaddedCountdownSpend {
            base_tokens: 100_000,
            high_water_tokens: 500_000,
        };
        // Live occupancy fell, but the high-water mark holds the spend.
        assert_eq!(spend.total_with(1_000), 500_000);
        assert_eq!(
            remaining_tokens(
                TotalTokensReminderMode::PaddedCountdown,
                200_000,
                1_000,
                1_000_000,
                spend
            ),
            500_000
        );
    }
}
