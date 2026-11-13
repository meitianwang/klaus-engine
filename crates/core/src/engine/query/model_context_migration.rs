//! Adapt oversized conversation context on the newly selected model without
//! calling the deselected model source.

use super::*;
use crate::context::compact::policy::{model_auto_compact_token_limit, usable_context_window};
use crate::schemas::config::{AutoCompactScope, ResolvedConfig};

#[allow(clippy::too_many_arguments)]
pub(super) async fn compact(
    shared: &Arc<Shared>,
    cwd: &RuntimePath,
    _thread_id: &RuntimeString,
    turn_user_uuid: &Option<RuntimeString>,
    system_prompt: &mut Vec<SystemPromptPart>,
    tool_specs: &[ToolSpec],
    turn_override: &TurnOverride,
    effective_max_output: u64,
    messages: &mut Vec<Message>,
    prepared: &mut PreparedModelRequest,
) -> Result<bool, CompactionFailure> {
    if super::portable_migration::recover(
        shared,
        cwd,
        turn_user_uuid,
        system_prompt,
        tool_specs,
        turn_override,
        effective_max_output,
        messages,
        prepared,
    )
    .await?
    {
        return Ok(true);
    }

    let Some(target) = shared.bundle() else {
        return Ok(false);
    };
    let config = shared.config();
    let rendered = super::super::request::rendered_context_tokens(&prepared.request);
    let active = shared
        .params
        .context_window
        .as_ref()
        .map_or(rendered, |state| state.sample(rendered));

    if let Some(downshift) = target.context_downshift.clone() {
        if let Some(previous_usable) = downshift.previous_usable_window() {
            if !downshift_needed(previous_usable, &config, active) {
                downshift.complete();
                return Ok(false);
            }
            return super::portable_migration::downshift_on_selected(
                shared,
                &target,
                cwd,
                turn_user_uuid,
                system_prompt,
                tool_specs,
                turn_override,
                effective_max_output,
                messages,
                prepared,
            )
            .await;
        }
    }

    Ok(false)
}

/// Trigger model-switch downshift compaction when the smaller target model's
/// own boundary is exceeded. Earlier session-level budgets still flow through
/// the ordinary preflight compaction check on the selected model.
fn downshift_needed(previous_usable_window: u64, target: &ResolvedConfig, active: u64) -> bool {
    let target_window = usable_context_window(target);
    if previous_usable_window <= target_window {
        return false;
    }
    if active >= target_window {
        return true;
    }
    target.auto_compaction
        && !target.compaction_disabled
        && target.auto_compact_scope == AutoCompactScope::Total
        && active > model_auto_compact_token_limit(target)
}

#[cfg(test)]
mod migration_budget_contracts {
    use super::*;

    fn config(window: u64, percent: u8) -> ResolvedConfig {
        ResolvedConfig {
            context_window: window,
            model_context_window: Some(window),
            model_effective_context_window_percent: Some(percent),
            ..Default::default()
        }
    }

    #[test]
    fn total_budget_uses_a_strict_model_boundary_and_inclusive_complete_window() {
        let source = usable_context_window(&config(500_000, 95));
        let target = config(256_000, 95);
        for (active, expected) in [
            (230_399, false),
            (230_400, false),
            (230_401, true),
            (243_199, true),
            (243_200, true),
        ] {
            assert_eq!(downshift_needed(source, &target, active), expected);
        }
    }

    #[test]
    fn an_earlier_session_budget_keeps_compaction_on_the_ordinary_preflight_path() {
        let source = usable_context_window(&config(500_000, 95));
        let mut target = config(256_000, 95);
        target.auto_compact_window = Some(100_000);
        assert!(
            crate::context::compact::policy::context_window_status(&target, 200_000, None)
                .token_limit_reached
        );
        assert!(!downshift_needed(source, &target, 200_000));
    }

    #[test]
    fn body_growth_does_not_force_downshift_below_the_complete_window() {
        let source = usable_context_window(&config(500_000, 95));
        let mut target = config(256_000, 95);
        target.auto_compact_scope = AutoCompactScope::BodyAfterPrefix;
        target.auto_compact_window = Some(10_000);
        assert!(crate::context::compact::policy::context_window_status(
            &target,
            240_000,
            Some(100_000),
        )
        .token_limit_reached);
        assert!(!downshift_needed(source, &target, 240_000));
        assert!(downshift_needed(source, &target, 243_200));
    }

    #[test]
    fn metadata_limits_are_capped_before_the_strict_comparison() {
        let source = usable_context_window(&config(500_000, 95));
        let mut target = config(256_000, 95);
        target.model_auto_compact_token_limit = Some(200_000);
        assert!(!downshift_needed(source, &target, 200_000));
        assert!(downshift_needed(source, &target, 200_001));
        target.model_auto_compact_token_limit = Some(300_000);
        assert!(!downshift_needed(source, &target, 230_400));
        assert!(downshift_needed(source, &target, 230_401));
    }

    #[test]
    fn effective_windows_determine_whether_selected_capacity_is_smaller() {
        assert!(!downshift_needed(
            usable_context_window(&config(500_000, 40)),
            &config(256_000, 95),
            250_000,
        ));
        assert!(downshift_needed(
            usable_context_window(&config(200_000, 100)),
            &config(256_000, 50),
            150_000,
        ));
    }

    #[test]
    fn disabled_automatic_compaction_only_intercepts_a_complete_window_downshift() {
        let source = usable_context_window(&config(500_000, 95));
        for globally_disabled in [false, true] {
            let mut target = config(256_000, 95);
            target.auto_compaction = globally_disabled;
            target.compaction_disabled = globally_disabled;
            assert!(!downshift_needed(source, &target, 240_000));
            assert!(downshift_needed(source, &target, 243_200));
        }
    }
}
