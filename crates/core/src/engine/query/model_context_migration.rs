//! Rebuild conversation context with its previous transport before publication.

use super::*;
use crate::context::compact::policy::{model_auto_compact_token_limit, usable_context_window};
use crate::schemas::config::{AutoCompactScope, ResolvedConfig};
use crate::types::provider::{RemoteCompactionSupport, ThinkingConfig};

#[allow(clippy::too_many_arguments)]
pub(super) async fn compact(
    shared: &Arc<Shared>,
    cwd: &RuntimePath,
    thread_id: &RuntimeString,
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

    if shared
        .inference
        .as_ref()
        .is_none_or(|capture| capture.next.is_none())
    {
        return Ok(false);
    }
    let Some(target) = shared.bundle() else {
        return Ok(false);
    };
    let Some(migration) = target.context_migration.clone() else {
        return Ok(false);
    };
    let Some(source) = migration.source() else {
        return Ok(false);
    };
    let config = shared.config();
    let source_support = source
        .provider
        .remote_compaction_support(&source.config.model);
    let target_support = target.provider.remote_compaction_support(&config.model);
    let mut native_to_local = false;
    for message in messages.iter() {
        let Message::CompactBoundary(boundary) = message else {
            continue;
        };
        let Some(checkpoint) = &boundary.native_compaction else {
            continue;
        };
        let compatible = tokio::select! {
            biased;
            _ = shared.params.signal.cancelled() => return Err(CompactionFailure::Aborted),
            result = target.provider.native_checkpoint_compatibility(checkpoint, &config.model, &shared.params.signal) => result,
        }.map_err(|error| CompactionFailure::Service(EngineError::from_provider(error, "context-checkpoint-compatibility")))?;
        native_to_local |= !compatible;
    }
    let hash_changed = source
        .provider
        .compaction_compatibility_hash(&source.config.model)
        .zip(target.provider.compaction_compatibility_hash(&config.model))
        .is_some_and(|(previous, current)| previous != current);
    let estimate = super::super::request::rendered_context_tokens(&prepared.request);
    let active = shared
        .params
        .context_window
        .as_ref()
        .map_or(estimate, |state| state.sample(estimate));
    let downshift = previous_model_needed(&source.config, &config, active);
    if !native_to_local && !hash_changed && !downshift {
        migration.complete();
        return Ok(false);
    }
    if config.compaction_disabled || !config.auto_compaction {
        return Err("Automatic context compaction is disabled for the selected model.".into());
    }
    let mut instructions = None;
    if let Some(bus) = &shared.params.hook_bus {
        if bus.has(HookEvent::PreCompact) {
            let mut input = base_hook_input(HookEvent::PreCompact, cwd, thread_id);
            input.trigger = Some(HookTrigger::Auto);
            let outcome = tokio::select! {
                biased;
                _ = shared.params.signal.cancelled() => return Err(CompactionFailure::Aborted),
                outcome = bus.dispatch(HookEvent::PreCompact, &input) => outcome,
            };
            for message in outcome.system_messages {
                notify(&shared.q, NotificationLevel::Info, message);
            }
            if outcome.aborted || outcome.blocked {
                return Err(CompactionFailure::Aborted);
            }
            if !outcome.additional_context.is_empty() {
                instructions = Some(join_runtime_strings(&outcome.additional_context, "\n"));
            }
        }
    }
    let source_override = TurnOverride {
        model: source.config.model.clone(),
        effort: source.config.effort.clone(),
        ultra_mode: source.config.ultra_mode,
    };
    let source_request = super::super::request::prepare_with_inference(
        messages,
        &shared.params,
        super::super::request::RequestInference {
            config: &source.config,
            provider_info: Some(source.provider.info()),
            thinking: source.config.thinking.then_some(ThinkingConfig::Adaptive),
        },
        system_prompt,
        source
            .config
            .max_output_tokens
            .unwrap_or(DEFAULT_TURN_OUTPUT_TOKENS),
        tool_specs,
        Some(&source_override),
    );
    let source_deps = source.deps.to_engine_deps();
    let mut used_compactor = source_deps.compactor.clone();
    used_compactor.prepare_request(&source_request.request, &shared.params.signal);
    used_compactor.bind_output_observer(shared.params.compaction_output.clone());
    let (compactable, protected) =
        split_compaction_protected(messages, &shared.params.compaction_protected_message_ids);
    let before = crate::context::tokens::estimate_context_tokens(messages);
    let source_ids = messages
        .iter()
        .map(super::super::query_message_uuid)
        .cloned()
        .collect();
    let mut summary_config = source.config.clone();
    summary_config.compact_prompt = config.compact_prompt.clone();
    shared.q.push(EngineEvent::CompactionProgress {
        phase: "started".into(),
    });
    let mut result = tokio::select! {
        biased;
        _ = shared.params.signal.cancelled() => return Err(CompactionFailure::Aborted),
        result = async {
            if native_to_local {
                used_compactor.local_compact(&compactable, &summary_config, instructions.as_ref()).await
            } else {
                used_compactor.threshold_compact(&compactable, &summary_config, instructions.as_ref()).await
            }
        } => result,
    };
    // A model migration may retry a failed native request only on an explicitly
    // supported target transport. A local summary never rotates providers.
    let distinct_target = source.config.model != config.model || source.source != target.source;
    let quota_or_budget = result
        .diagnostics
        .iter()
        .filter_map(|item| item.provider_error.as_ref())
        .any(|error| {
            error.kind == ProviderErrorKind::Quota
                || error.code.as_ref().is_some_and(|code| {
                    code == "session_budget_exceeded" || code == "budget_exceeded"
                })
        });
    if !result.compacted
        && !native_to_local
        && source_support == RemoteCompactionSupport::ResponsesV2
        && distinct_target
        && target_support == RemoteCompactionSupport::ResponsesV2
        && target.provider.supports_compaction_model_fallback()
        && !quota_or_budget
        && !shared.params.signal.is_cancelled()
    {
        let target_deps = target.deps.to_engine_deps();
        used_compactor = target_deps.compactor.clone();
        used_compactor.prepare_request(&prepared.request, &shared.params.signal);
        used_compactor.bind_output_observer(shared.params.compaction_output.clone());
        result = tokio::select! {
            biased;
            _ = shared.params.signal.cancelled() => return Err(CompactionFailure::Aborted),
            result = used_compactor.threshold_compact(&compactable, &config, instructions.as_ref()) => result,
        };
    }
    shared.q.push(EngineEvent::CompactionProgress {
        phase: "completed".into(),
    });
    if shared.params.signal.is_cancelled() {
        return Err(CompactionFailure::Aborted);
    }
    if !result.compacted {
        if result.diagnostics.iter().any(|item| {
            item.kind == crate::context::compact::outcome::CompactionDiagnosticKind::PromptTooLong
        }) {
            if let Some(window) = &shared.params.context_window {
                window
                    .mark_full(crate::context::compact::policy::usable_context_window(
                        &config,
                    ))
                    .await;
            }
        }
        let diagnostics = result
            .diagnostics
            .iter()
            .filter_map(|item| item.message.clone())
            .collect::<Vec<_>>();
        return Err(if diagnostics.is_empty() {
            "The previous model did not return a completed context checkpoint.".into()
        } else {
            join_runtime_strings(&diagnostics, "; ").into()
        });
    }
    if !result.has_new_checkpoint(&compactable) {
        return Err("The summary operation did not return a new context checkpoint.".into());
    }
    let authoritative = shared
        .params
        .compaction_history
        .as_ref()
        .map_or_else(|| compactable.clone(), |snapshot| snapshot());
    let authoritative: Vec<_> = authoritative
        .into_iter()
        .filter(|message| {
            !shared
                .params
                .compaction_protected_message_ids
                .contains(super::super::query_message_uuid(message))
        })
        .collect();
    let replacement = crate::context::compact::full_compact::refresh_local_user_history(
        &authoritative,
        result.messages,
    );
    let mut candidate =
        restore_compaction_protected(replacement, protected, turn_user_uuid.as_ref());
    invalidate_context_usage(&mut candidate);
    candidate = inject_running_agent_reminder(
        candidate,
        shared.background_tasks.as_ref(),
        &|| (shared.deps.uuid)(),
        &prepared.profile,
    );
    {
        let surfaced = shared.surfaced.lock_recover();
        let tools = shared.tools.lock_recover().clone();
        candidate = inject_surfaced_tools_reminder(
            candidate,
            &tools,
            &surfaced,
            &|| (shared.deps.uuid)(),
            &prepared.profile,
        );
    }
    let has_task_tool = shared.params.tools.iter().any(|tool| tool.name() == "task");
    tokio::select! {
        biased;
        _ = shared.params.signal.cancelled() => return Err(CompactionFailure::Aborted),
        refreshed = async {
            super::refresh_retained_context_inner(shared, &mut candidate, cwd, has_task_tool, false).await;
            super::refresh_captured_system_prompt(shared, system_prompt).await
        } => {
            if !refreshed {
                return Err(CompactionFailure::Aborted);
            }
        },
    }
    super::place_retained_context(&mut candidate);
    if shared.params.signal.is_cancelled() {
        return Err(CompactionFailure::Aborted);
    }
    super::super::compaction::persist_compaction_from(
        shared,
        &mut candidate,
        source_ids,
        before,
        used_compactor.as_ref(),
    )
    .await;
    let next = shared.prepare(
        &candidate,
        system_prompt,
        effective_max_output,
        tool_specs,
        Some(turn_override),
    );
    if shared.params.on_compaction.is_none() {
        if let Some(window) = &shared.params.context_window {
            window
                .reset(estimate_context_tokens_with_prefix(
                    &candidate,
                    fixed_prefix_tokens(&next.request),
                ))
                .await;
        }
    }
    *messages = candidate;
    *prepared = next;
    migration.complete();
    shared.q.push(EngineEvent::Compaction {
        mode: CompactionMode::Full,
        freed_tokens: result.freed_tokens,
    });
    if let Some(bus) = &shared.params.hook_bus {
        if bus.has(HookEvent::PostCompact) {
            let mut input = base_hook_input(HookEvent::PostCompact, cwd, thread_id);
            input.trigger = Some(HookTrigger::Auto);
            input.message = Some(compaction_summary_of(messages));
            let outcome = tokio::select! {
                biased;
                _ = shared.params.signal.cancelled() => return Err(CompactionFailure::Aborted),
                outcome = bus.dispatch(HookEvent::PostCompact, &input) => outcome,
            };
            for message in outcome.system_messages {
                notify(&shared.q, NotificationLevel::Info, message);
            }
            if outcome.aborted || outcome.blocked {
                return Err(CompactionFailure::Aborted);
            }
        }
    }
    Ok(true)
}

/// Choose the previous transport only when the smaller model's own boundary
/// requires it. Earlier session budgets still compact on the selected transport.
fn previous_model_needed(source: &ResolvedConfig, target: &ResolvedConfig, active: u64) -> bool {
    let target_window = usable_context_window(target);
    if usable_context_window(source) <= target_window {
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
        let source = config(500_000, 95);
        let target = config(256_000, 95);
        for (active, expected) in [
            (230_399, false),
            (230_400, false),
            (230_401, true),
            (243_199, true),
            (243_200, true),
        ] {
            assert_eq!(previous_model_needed(&source, &target, active), expected);
        }
    }

    #[test]
    fn an_earlier_session_budget_keeps_compaction_on_the_selected_transport() {
        let source = config(500_000, 95);
        let mut target = config(256_000, 95);
        target.auto_compact_window = Some(100_000);
        assert!(
            crate::context::compact::policy::context_window_status(&target, 200_000, None,)
                .token_limit_reached
        );
        assert!(!previous_model_needed(&source, &target, 200_000));
    }

    #[test]
    fn body_growth_does_not_select_the_previous_transport_below_the_complete_window() {
        let source = config(500_000, 95);
        let mut target = config(256_000, 95);
        target.auto_compact_scope = AutoCompactScope::BodyAfterPrefix;
        target.auto_compact_window = Some(10_000);
        assert!(crate::context::compact::policy::context_window_status(
            &target, 240_000, Some(100_000),
        ).token_limit_reached);
        assert!(!previous_model_needed(&source, &target, 240_000));
        assert!(previous_model_needed(&source, &target, 243_200));
    }

    #[test]
    fn metadata_limits_are_capped_before_the_strict_comparison() {
        let source = config(500_000, 95);
        let mut target = config(256_000, 95);
        target.model_auto_compact_token_limit = Some(200_000);
        assert!(!previous_model_needed(&source, &target, 200_000));
        assert!(previous_model_needed(&source, &target, 200_001));
        target.model_auto_compact_token_limit = Some(300_000);
        assert!(!previous_model_needed(&source, &target, 230_400));
        assert!(previous_model_needed(&source, &target, 230_401));
    }

    #[test]
    fn effective_windows_determine_whether_selected_capacity_is_smaller() {
        assert!(!previous_model_needed(
            &config(500_000, 40),
            &config(256_000, 95),
            250_000,
        ));
        assert!(previous_model_needed(
            &config(200_000, 100),
            &config(256_000, 50),
            150_000,
        ));
    }

    #[test]
    fn disabled_automatic_compaction_only_intercepts_a_complete_window_downshift() {
        let source = config(500_000, 95);
        for globally_disabled in [false, true] {
            let mut target = config(256_000, 95);
            target.auto_compaction = globally_disabled;
            target.compaction_disabled = globally_disabled;
            assert!(!previous_model_needed(&source, &target, 240_000));
            assert!(previous_model_needed(&source, &target, 243_200));
        }
    }
}
