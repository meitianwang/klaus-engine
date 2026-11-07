//! Defect: fixed output reservations changed automatic context boundaries.
//! Root cause: input admission depended on a response budget and absolute buffers.
//! Work item: 01a1588f pin proportional windows and independent prefix scopes.

use klaus_agent_core::context::compact::policy::{
    actual_input_window, auto_compact_token_limit, context_window_status, effective_window,
    hard_block_threshold, output_reservation, trigger_threshold, warning_threshold,
};
use klaus_agent_core::engine::session::resolve_config;
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeString};
use klaus_agent_core::schemas::config::{AutoCompactScope, EngineConfig};
use serde::Deserialize;
use serde_json::json;

#[test]
fn complete_input_and_automatic_limits_scale_with_actual_model_capacity() {
    for (actual, automatic, usable, trigger) in [
        (200_000, None, 190_000, 180_000),
        (1_000_000, None, 950_000, 900_000),
        (1_000_000, Some(200_000), 950_000, 200_000),
        (200_000, Some(1_000_000), 190_000, 180_000),
        (1_000, None, 950, 900),
    ] {
        let config = EngineConfig {
            context_window: actual,
            auto_compact_window: automatic,
            ..EngineConfig::default()
        };
        assert_eq!(actual_input_window(&config), usable as f64);
        assert_eq!(effective_window(&config), usable as f64);
        assert_eq!(hard_block_threshold(&config), usable as f64);
        assert_eq!(output_reservation(&config), actual - usable);
        assert_eq!(trigger_threshold(&config), trigger as f64);
        assert_eq!(warning_threshold(&config, true), trigger as f64);
        assert_eq!(warning_threshold(&config, false), usable as f64);
    }
}

#[test]
fn model_output_budgets_and_retired_reservations_do_not_change_input_policy() {
    let mut config = EngineConfig {
        context_window: 200_000,
        ..EngineConfig::default()
    };
    for output in [1_024, 8_192, 64_000] {
        config.max_output_tokens = Some(output);
        config.model_max_output_tokens = Some(output);
        for fraction in [None, Some(0.25), Some(0.99)] {
            config.output_reserve_tokens = output;
            config.compact_buffer_tokens = 200_000;
            config.compact_threshold = fraction;
            config.precompute_buffer_fraction = Some(0.25);
            assert_eq!(effective_window(&config), 190_000.0);
            assert_eq!(trigger_threshold(&config), 180_000.0);
            assert_eq!(hard_block_threshold(&config), 190_000.0);
        }
    }
}

#[test]
fn captured_model_policy_defaults_and_catalog_limits_keep_independent_boundaries() {
    let mut config = EngineConfig {
        context_window: 1_000_000,
        model_context_window: Some(200_000),
        model_effective_context_window_percent: Some(80),
        model_auto_compact_token_limit: Some(150_000),
        ..EngineConfig::default()
    };
    assert_eq!(hard_block_threshold(&config), 160_000.0);
    assert_eq!(trigger_threshold(&config), 150_000.0);
    config.auto_compact_window = Some(100_000);
    assert_eq!(trigger_threshold(&config), 100_000.0);
    config.auto_compact_window = None;
    config.model_auto_compact_token_limit = Some(250_000);
    assert_eq!(trigger_threshold(&config), 180_000.0);
    config.model_effective_context_window_percent = Some(0);
    assert_eq!(hard_block_threshold(&config), 2_000.0);
    config.model_effective_context_window_percent = Some(255);
    assert_eq!(hard_block_threshold(&config), 200_000.0);
    config.model_effective_context_window_percent = None;
    assert_eq!(hard_block_threshold(&config), 190_000.0);
    let serialized = serde_json::to_value(&config).unwrap();
    assert!(serialized
        .get("modelEffectiveContextWindowPercent")
        .is_none());
    assert!(serialized.get("modelAutoCompactTokenLimit").is_none());
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StatusVector {
    name: String,
    config: serde_json::Value,
    active: u64,
    prefill: Option<u64>,
    scope_tokens: u64,
    scope_limit: u64,
    usable: u64,
    remaining: u64,
    full: bool,
    compact: bool,
    post_turn: bool,
}

#[test]
fn context_status_vectors_pin_prefix_growth_and_post_turn_trigger() {
    let vectors: Vec<StatusVector> = serde_json::from_str(include_str!(
        "../fixtures/context_window_policy_vectors.json"
    ))
    .unwrap();
    for vector in vectors {
        let config = resolve_config(vector.config).unwrap();
        let status = context_window_status(&config, vector.active, vector.prefill);
        assert_eq!(
            status.active_context_tokens, vector.active,
            "{}",
            vector.name
        );
        assert_eq!(
            status.auto_compact_scope_tokens, vector.scope_tokens,
            "{}",
            vector.name
        );
        assert_eq!(
            status.auto_compact_scope_limit, vector.scope_limit,
            "{}",
            vector.name
        );
        assert_eq!(
            status.full_context_window_limit, vector.usable,
            "{}",
            vector.name
        );
        assert_eq!(
            status.base_window_tokens_remaining, vector.remaining,
            "{}",
            vector.name
        );
        assert_eq!(
            status.full_context_window_limit_reached, vector.full,
            "{}",
            vector.name
        );
        assert_eq!(
            status.token_limit_reached, vector.compact,
            "{}",
            vector.name
        );
        assert_eq!(
            status.turn_end_compaction_threshold_reached, vector.post_turn,
            "{}",
            vector.name
        );
        assert_eq!(
            auto_compact_token_limit(&config),
            vector.scope_limit,
            "{}",
            vector.name
        );
    }
}

#[test]
fn active_preferences_round_trip_and_retired_settings_are_read_only_compatibility() {
    let mut config = resolve_config(json!({
        "contextWindow": 1_000_000,
        "autoCompactWindow": 200_000,
        "autoCompactScope": "body-after-prefix",
        "postTurnCompactThresholdPercent": 75,
        "compactionStreamMaxRetries": 7,
        "compactPrompt": "Preserve unfinished tasks and their constraints.",
        "precomputeBufferFraction": 0.25,
        "outputReserveTokens": 20_000,
        "compactBufferTokens": 13_000,
        "compactThreshold": 0.85
    }))
    .unwrap();
    config.model_max_output_tokens = Some(64_000);
    let serialized = serde_json::to_value(&config).unwrap();
    assert_eq!(serialized["autoCompactWindow"], 200_000);
    assert_eq!(serialized["autoCompactScope"], "body-after-prefix");
    assert_eq!(serialized["postTurnCompactThresholdPercent"], 75);
    assert_eq!(serialized["compactionStreamMaxRetries"], 7);
    for retired in [
        "outputReserveTokens",
        "compactBufferTokens",
        "compactThreshold",
        "precomputeBufferFraction",
    ] {
        assert!(
            serialized.get(retired).is_none(),
            "retired preference {retired}"
        );
    }
    assert!(serialized.get("modelMaxOutputTokens").is_none());
    let restored = resolve_config(serialized).unwrap();
    assert_eq!(
        restored.auto_compact_scope,
        AutoCompactScope::BodyAfterPrefix
    );
    assert_eq!(restored.compact_prompt, config.compact_prompt);
    assert_eq!(restored.model_max_output_tokens, None);
    let runtime = runtime_codec::encode_text(&config).unwrap();
    let runtime_restored: EngineConfig = runtime_codec::decode_text(&runtime).unwrap();
    assert_eq!(
        runtime_restored.auto_compact_scope,
        AutoCompactScope::BodyAfterPrefix
    );
    assert_eq!(
        runtime_restored.compact_prompt,
        Some(RuntimeString::from(
            "Preserve unfinished tasks and their constraints."
        ))
    );
    for invalid in [
        json!({"modelMaxOutputTokens": 64_000}),
        json!({"autoCompactWindow": 0}),
        json!({"autoCompactScope": "unknown"}),
        json!({"postTurnCompactThresholdPercent": 101}),
        json!({"compactionStreamMaxRetries": 101}),
    ] {
        assert!(resolve_config(invalid).is_err());
    }
}
