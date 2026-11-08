//! Token-window policy for automatic and completed-turn history compaction.

use crate::schemas::config::{AutoCompactScope, ResolvedConfig};

const USABLE_WINDOW_PERCENT: u64 = 95;
const AUTO_COMPACT_WINDOW_PERCENT: u64 = 90;
/// Share of the automatic-compaction boundary held back as the precompute lead.
const PRECOMPUTE_BUFFER_FRACTION_PERCENT: u64 = 20;
/// Absolute token reserve before the automatic-compaction boundary.
const PRECOMPUTE_RESERVE_TOKENS: u64 = 13_000;

fn percent_of(tokens: u64, percent: u64) -> u64 {
    (u128::from(tokens) * u128::from(percent) / 100) as u64
}

/// Scope and full-window observations share one resolved policy boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContextWindowTokenStatus {
    pub active_context_tokens: u64,
    pub auto_compact_scope_tokens: u64,
    pub auto_compact_scope_limit: u64,
    pub full_context_window_limit: u64,
    pub base_window_tokens_remaining: u64,
    pub auto_compact_window_prefill_tokens: Option<u64>,
    pub full_context_window_limit_reached: bool,
    pub token_limit_reached: bool,
    pub turn_end_compaction_threshold_reached: bool,
}

/// The configured token limit applies either to total input or to growth after
/// the current window's prefill. The complete request retains its own hard cap.
pub fn context_window_status(
    config: &ResolvedConfig,
    active_context_tokens: u64,
    prefill_input_tokens: Option<u64>,
) -> ContextWindowTokenStatus {
    let full_context_window_limit = usable_context_window(config);
    let auto_compact_scope_limit = auto_compact_token_limit(config);
    let (auto_compact_scope_tokens, auto_compact_window_prefill_tokens) =
        match config.auto_compact_scope {
            AutoCompactScope::Total => (active_context_tokens, None),
            AutoCompactScope::BodyAfterPrefix => (
                active_context_tokens
                    .saturating_sub(prefill_input_tokens.unwrap_or(active_context_tokens)),
                prefill_input_tokens,
            ),
        };
    let full_context_window_limit_reached = active_context_tokens >= full_context_window_limit;
    let token_limit_reached =
        auto_compact_scope_tokens >= auto_compact_scope_limit || full_context_window_limit_reached;
    let base_window_tokens_remaining = auto_compact_scope_limit
        .saturating_sub(auto_compact_scope_tokens)
        .min(full_context_window_limit.saturating_sub(active_context_tokens));
    let post_turn_percent = config.post_turn_compact_threshold_percent;
    let turn_end_compaction_threshold_reached = post_turn_percent > 0
        && (token_limit_reached
            || u128::from(active_context_tokens) * 100
                >= u128::from(full_context_window_limit) * u128::from(post_turn_percent));
    ContextWindowTokenStatus {
        active_context_tokens,
        auto_compact_scope_tokens,
        auto_compact_scope_limit,
        full_context_window_limit,
        base_window_tokens_remaining,
        auto_compact_window_prefill_tokens,
        full_context_window_limit_reached,
        token_limit_reached,
        turn_end_compaction_threshold_reached,
    }
}

/// Capacity usable by a complete active request, with model-proportional headroom.
pub fn usable_context_window(config: &ResolvedConfig) -> u64 {
    percent_of(
        super::window::actual_model_window(config),
        config
            .model_effective_context_window_percent
            .map_or(USABLE_WINDOW_PERCENT, |percent| {
                u64::from(percent.clamp(1, 100))
            }),
    )
}

/// The model boundary before an earlier session limit or body scope is applied.
pub fn model_auto_compact_token_limit(config: &ResolvedConfig) -> u64 {
    let capacity_limit = percent_of(
        super::window::actual_model_window(config),
        AUTO_COMPACT_WINDOW_PERCENT,
    );
    config
        .model_auto_compact_token_limit
        .map_or(capacity_limit, |limit| limit.min(capacity_limit))
}

/// An explicit total-input limit may advance compaction, never postpone the
/// default boundary. A body-only limit excludes its persisted prefill baseline.
pub fn auto_compact_token_limit(config: &ResolvedConfig) -> u64 {
    let default = model_auto_compact_token_limit(config);
    match config.auto_compact_scope {
        AutoCompactScope::Total => config
            .auto_compact_window
            .map_or(default, |limit| limit.min(default)),
        AutoCompactScope::BodyAfterPrefix => config.auto_compact_window.unwrap_or(default),
    }
}

/// Retained diagnostic name for proportional model headroom. Historical output
/// reservation settings no longer alter either the automatic or usable window.
pub fn output_reservation(config: &ResolvedConfig) -> u64 {
    super::window::actual_model_window(config).saturating_sub(usable_context_window(config))
}

pub fn actual_input_window(config: &ResolvedConfig) -> f64 {
    usable_context_window(config) as f64
}

pub fn effective_window(config: &ResolvedConfig) -> f64 {
    actual_input_window(config)
}

pub fn trigger_threshold(config: &ResolvedConfig) -> f64 {
    auto_compact_token_limit(config) as f64
}

pub fn warning_threshold(config: &ResolvedConfig, automatic_compaction: bool) -> f64 {
    if automatic_compaction {
        trigger_threshold(config).min(actual_input_window(config))
    } else {
        actual_input_window(config)
    }
}

pub fn hard_block_threshold(config: &ResolvedConfig) -> f64 {
    actual_input_window(config)
}

pub fn preflight_threshold(config: &ResolvedConfig) -> f64 {
    trigger_threshold(config).min(hard_block_threshold(config))
}

/// The scope-token count at which background precomputation of the compaction
/// summary arms: the earlier of a proportional buffer and an absolute reserve
/// below the automatic-compaction boundary. Boundaries at or below the reserve
/// leave no room for a lead, so they never arm.
pub fn precompute_arm_threshold(auto_compact_scope_limit: u64) -> Option<u64> {
    if auto_compact_scope_limit <= PRECOMPUTE_RESERVE_TOKENS {
        return None;
    }
    // Round-half-up, matching the proportional buffer's reference semantics.
    let buffered = auto_compact_scope_limit
        - (auto_compact_scope_limit * PRECOMPUTE_BUFFER_FRACTION_PERCENT + 50) / 100;
    Some(buffered.min(auto_compact_scope_limit - PRECOMPUTE_RESERVE_TOKENS))
}

#[cfg(test)]
mod precompute_threshold_tests {
    use super::precompute_arm_threshold;

    #[test]
    fn proportional_buffer_wins_on_large_windows() {
        // 180k boundary: min(180k - 36k, 180k - 13k) = 144k.
        assert_eq!(precompute_arm_threshold(180_000), Some(144_000));
        // 20k boundary: min(20k - 4k, 20k - 13k) = 7k (reserve wins).
        assert_eq!(precompute_arm_threshold(20_000), Some(7_000));
    }

    #[test]
    fn small_boundaries_never_arm() {
        assert_eq!(precompute_arm_threshold(13_000), None);
        assert_eq!(precompute_arm_threshold(0), None);
    }

    #[test]
    fn rounding_is_half_up() {
        // 16_385 * 20% = 3277.0 -> buffered = 13_108; reserve = 3_385.
        assert_eq!(precompute_arm_threshold(16_385), Some(3_385));
        // 65_125 * 20% = 13_025 exactly.
        assert_eq!(precompute_arm_threshold(65_125), Some(52_100));
    }
}
