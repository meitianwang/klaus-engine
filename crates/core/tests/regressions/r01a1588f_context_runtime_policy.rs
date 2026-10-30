//! Defect: runtime context overrides lost their source, and task countdowns
//! could charge retained context twice or rewind across compaction.
//! Root cause: actual and automatic windows shared one number, while reminder
//! policy lacked an explicit per-task anchor and retirement delta.
//! Work item 01a1588f: keep model capacity and agent-owned budgets separate.

use klaus_agent_core::context::compact::policy::{
    actual_input_window, effective_window, trigger_threshold,
};
use klaus_agent_core::context::compact::window::{
    bind_context_runtime_environment, resolve_auto_window, AutoWindowSource, AUTO_WINDOW_ENV,
};
use klaus_agent_core::context::total_tokens_reminder::{
    render_total_tokens_reminder, resolve_reminder_policy, retired_context_tokens,
    PaddedCountdownSpend, TaskCountdownAnchor, REMINDER_AFTER_USER_ENV, REMINDER_BUDGET_ENV,
    REMINDER_MODE_ENV,
};
use klaus_agent_core::runtime_process::RuntimeEnv;
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeString};
use klaus_agent_core::schemas::config::{default_config, TotalTokensReminderMode};

fn env(entries: &[(&str, &str)]) -> RuntimeEnv {
    let mut env = RuntimeEnv::new();
    for (key, value) in entries {
        env.insert(RuntimeString::from(*key), RuntimeString::from(*value));
    }
    env
}

#[test]
fn actual_model_capacity_and_automatic_limits_have_independent_sources() {
    let mut config = default_config();
    config.context_window = 800_000;
    config.model_context_window = Some(200_000);
    config.output_reserve_tokens = 20_000;
    config.auto_compact_window = Some(350_000);
    let selection = resolve_auto_window(&config);
    assert_eq!(selection.actual, 200_000);
    assert_eq!(selection.selected, 200_000);
    assert_eq!(selection.configured, Some(350_000));
    assert_eq!(selection.source, AutoWindowSource::Configuration);
    assert_eq!(actual_input_window(&config), 190_000.0);
    assert_eq!(effective_window(&config), 190_000.0);
    assert_eq!(trigger_threshold(&config), 180_000.0);
    config.auto_compact_window = Some(50_000);
    assert_eq!(effective_window(&config), 190_000.0);
    assert_eq!(trigger_threshold(&config), 50_000.0);
    assert_eq!(actual_input_window(&config), 190_000.0);
    config.auto_compact_window = None;
    assert_eq!(
        resolve_auto_window(&config).source,
        AutoWindowSource::Automatic
    );
    config.model_context_window = None;
    assert_eq!(resolve_auto_window(&config).actual, 800_000);
}

#[test]
fn environment_window_precedence_clamps_only_the_override_not_actual_capacity() {
    for (raw, configured, selected) in [
        ("1000", 100_000, 100_000),
        ("9_000_000", 1_000_000, 200_000),
        ("1e5", 100_000, 100_000),
        ("250000 trailing", 250_000, 200_000),
        ("\u{feff}150000\u{feff}", 150_000, 150_000),
    ] {
        let mut config = default_config();
        config.model_context_window = Some(200_000);
        config.auto_compact_window = Some(50_000);
        bind_context_runtime_environment(&mut config, &env(&[(AUTO_WINDOW_ENV, raw)]));
        let selection = resolve_auto_window(&config);
        assert_eq!(selection.actual, 200_000, "{raw}");
        assert_eq!(selection.configured, Some(configured), "{raw}");
        assert_eq!(selection.selected, selected, "{raw}");
        assert_eq!(selection.source, AutoWindowSource::Environment, "{raw}");
    }
    for raw in [
        "",
        "auto",
        "0",
        "-2",
        "NaN",
        "Infinity",
        "1e-1",
        "\u{85}150000",
    ] {
        let mut config = default_config();
        config.auto_compact_window = Some(50_000);
        bind_context_runtime_environment(&mut config, &env(&[(AUTO_WINDOW_ENV, raw)]));
        assert_eq!(config.auto_compact_window, Some(50_000), "{raw}");
        assert_eq!(
            resolve_auto_window(&config).source,
            AutoWindowSource::Configuration
        );
    }
}

#[test]
fn default_wire_data_omits_runtime_identity_and_the_optional_reminder_flag() {
    let mut config = default_config();
    assert!(!resolve_reminder_policy(&config).after_user_turn);
    config.model_context_window = Some(32_000);
    config.auto_compact_window_source = Some(AutoWindowSource::Environment);
    config.model_max_output_tokens = Some(64_000);
    config.compaction_disabled = true;
    let runtime = runtime_codec::encode_value(&config).unwrap();
    let object = runtime.as_object().unwrap();
    for (key, value) in [
        ("modelMaxOutputTokens", "1000000"),
        ("modelContextWindow", "9000000"),
        ("autoCompactWindowSource", "\"environment\""),
        ("compactionDisabled", "true"),
    ] {
        assert!(
            object.get(RuntimeString::from(key)).is_none(),
            "runtime-only state must not be emitted: {key}"
        );
        let forged =
            runtime_json::parse(&RuntimeString::from(format!(r#"{{"{key}":{value}}}"#))).unwrap();
        let error =
            runtime_codec::decode_value::<klaus_agent_core::schemas::config::EngineConfig>(&forged)
                .expect_err(
                    "runtime-only fields must be rejected at the public configuration boundary",
                );
        assert!(
            error.to_string().contains("unknown field"),
            "{key}: {error}"
        );
    }
    let decoded =
        runtime_codec::decode_value::<klaus_agent_core::schemas::config::EngineConfig>(&runtime)
            .unwrap();
    assert_eq!(decoded.model_context_window, None);
    assert_eq!(decoded.model_max_output_tokens, None);
    assert_eq!(decoded.auto_compact_window_source, None);
    assert!(!decoded.compaction_disabled);
    let wire = serde_json::to_value(&config).unwrap();
    assert!(wire.get("modelContextWindow").is_none());
    assert!(wire.get("autoCompactWindowSource").is_none());
    assert!(wire.get("totalTokensReminderAfterUserTurn").is_none());
    config.total_tokens_reminder_after_user_turn = Some(true);
    let wire = serde_json::to_value(&config).unwrap();
    assert_eq!(wire["totalTokensReminderAfterUserTurn"], true);
    config.total_tokens_reminder_budget = 0;
    assert_eq!(
        config.validate().unwrap_err(),
        "totalTokensReminderBudget must be a positive integer"
    );
}

#[test]
fn reminder_environment_overrides_valid_values_without_reviving_disabled_defaults() {
    let mut config = default_config();
    assert!(render_total_tokens_reminder(&config, 1_000, Default::default(), None).is_none());
    bind_context_runtime_environment(
        &mut config,
        &env(&[
            (REMINDER_MODE_ENV, " padded-countdown "),
            (REMINDER_BUDGET_ENV, "20_000_000"),
            (REMINDER_AFTER_USER_ENV, " YES "),
        ]),
    );
    let policy = resolve_reminder_policy(&config);
    assert_eq!(policy.mode, TotalTokensReminderMode::PaddedCountdown);
    assert_eq!(policy.budget, 20_000_000);
    assert!(policy.after_user_turn);
    bind_context_runtime_environment(
        &mut config,
        &env(&[
            (REMINDER_MODE_ENV, "COUNTDOWN"),
            (REMINDER_BUDGET_ENV, "-1"),
            (REMINDER_AFTER_USER_ENV, "maybe"),
        ]),
    );
    assert_eq!(resolve_reminder_policy(&config), policy);
    bind_context_runtime_environment(&mut config, &env(&[(REMINDER_AFTER_USER_ENV, "off")]));
    assert!(!resolve_reminder_policy(&config).after_user_turn);
}

#[test]
fn fixed_and_live_reminders_keep_their_distinct_capacity_semantics() {
    let mut config = default_config();
    config.context_window = 300_000;
    config.model_context_window = Some(160_000);
    config.auto_compact_window = Some(100_000);
    config.total_tokens_reminder = TotalTokensReminderMode::Fixed;
    assert_eq!(
        render_total_tokens_reminder(&config, 31_000, Default::default(), None),
        Some("<total_tokens>5000000 tokens left</total_tokens>".into())
    );
    config.total_tokens_reminder = TotalTokensReminderMode::Countdown;
    assert_eq!(
        render_total_tokens_reminder(&config, 31_000, Default::default(), None),
        Some("<total_tokens>129000 tokens left</total_tokens>".into())
    );
    config.total_tokens_reminder = TotalTokensReminderMode::Infinite;
    assert_eq!(
        render_total_tokens_reminder(&config, u64::MAX, Default::default(), None),
        Some("<total_tokens>Infinite tokens left</total_tokens>".into())
    );
}

#[test]
fn padded_session_countdown_charges_only_removed_occupancy_across_compaction() {
    let mut config = default_config();
    config.total_tokens_reminder = TotalTokensReminderMode::PaddedCountdown;
    config.total_tokens_reminder_budget = 10_000;
    let before = render_total_tokens_reminder(&config, 1_000, Default::default(), None);
    let spend = PaddedCountdownSpend {
        base_tokens: retired_context_tokens(1_000, 250),
        high_water_tokens: 1_000,
    };
    assert_eq!(
        before,
        render_total_tokens_reminder(&config, 250, spend, None)
    );
    assert_eq!(retired_context_tokens(250, 300), 0);
    assert_eq!(
        render_total_tokens_reminder(&config, 500, spend, None),
        Some("<total_tokens>8750 tokens left</total_tokens>".into())
    );
}

#[test]
fn task_anchor_survives_continuations_and_compaction_until_a_new_user_request() {
    let mut config = default_config();
    config.total_tokens_reminder = TotalTokensReminderMode::PaddedCountdown;
    config.total_tokens_reminder_budget = 10_000;
    config.total_tokens_reminder_after_user_turn = Some(true);
    let mut anchor = TaskCountdownAnchor::default();
    let spend = PaddedCountdownSpend {
        base_tokens: 500,
        high_water_tokens: 20_000,
    };
    anchor.reanchor(spend, 1_000);
    assert!(anchor.is_anchored());
    assert_eq!(anchor.remaining(10_000, spend, 1_000), 10_000);
    assert_eq!(anchor.remaining(10_000, spend, 1_200), 9_800);
    let after = PaddedCountdownSpend {
        base_tokens: spend.base_tokens + retired_context_tokens(1_200, 200),
        high_water_tokens: spend.high_water_tokens,
    };
    let mut continuation = anchor;
    assert_eq!(continuation.remaining(10_000, after, 200), 9_800);
    assert_eq!(continuation.remaining(10_000, after, 500), 9_500);
    assert_eq!(continuation.remaining(10_000, after, 100), 9_500);
    assert_eq!(
        render_total_tokens_reminder(&config, 100, after, Some(&mut continuation)),
        Some("<total_tokens>9500 tokens left</total_tokens>".into())
    );
    continuation.reanchor(after, 100);
    assert_eq!(continuation.remaining(10_000, after, 100), 10_000);
    assert_eq!(anchor.remaining(10_000, spend, 1_000), 9_800);
}
