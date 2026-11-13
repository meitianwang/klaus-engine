//! Restore incompatible opaque context and downshift oversized histories
//! exclusively on the currently selected model.

use super::*;
use crate::context::compact::policy::{model_auto_compact_token_limit, usable_context_window};
use crate::schemas::content::{ContentBlock, OutputBlock, TextBlock};
use crate::types::provider::ModelRequest;
use std::collections::BTreeSet;

/// Keep recent tool outputs intact while capping aged historical outputs so a
/// smaller target model can summarize without spending its window on old logs.
const PRESERVED_RECENT_MESSAGES: usize = 6;
const AGED_TOOL_OUTPUT_MAX_CHARS: usize = 2_000;
const MAX_TRACKED_FILE_PATHS: usize = 40;

#[allow(clippy::too_many_arguments)]
pub(super) async fn recover(
    shared: &Arc<Shared>,
    cwd: &RuntimePath,
    turn_user_uuid: &Option<RuntimeString>,
    system_prompt: &mut Vec<SystemPromptPart>,
    tool_specs: &[ToolSpec],
    turn_override: &TurnOverride,
    effective_max_output: u64,
    messages: &mut Vec<Message>,
    prepared: &mut PreparedModelRequest,
) -> Result<bool, CompactionFailure> {
    if !crate::provider::native_compaction::contains_native_checkpoint(messages) {
        return Ok(false);
    }
    let Some(target) = shared.bundle() else {
        return Ok(false);
    };
    let config = shared.config();
    let mut incompatible = false;
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
        incompatible |= !compatible;
    }
    if !incompatible {
        return Ok(false);
    }
    let Some(recover) = &shared.params.recover_compaction_history else {
        return Err(
            "Original local history is unavailable for migrating this encrypted context.".into(),
        );
    };
    let restored = tokio::select! {
        biased;
        _ = shared.params.signal.cancelled() => return Err(CompactionFailure::Aborted),
        result = recover(messages.clone()) => result,
    }
    .map_err(CompactionFailure::from)?;
    let restored = crate::engine::resume_repair::normalize_tool_history_owned(restored);
    if crate::provider::native_compaction::contains_native_checkpoint(&restored) {
        return Err(
            "The restored local history still contains incompatible encrypted context.".into(),
        );
    }
    apply_target_context_adaptation(
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
        restored,
        false,
        "The restored history exceeds the selected model's context window and automatic compaction is disabled.",
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn downshift_on_selected(
    shared: &Arc<Shared>,
    target: &Arc<crate::engine::session::InferenceBundle>,
    cwd: &RuntimePath,
    turn_user_uuid: &Option<RuntimeString>,
    system_prompt: &mut Vec<SystemPromptPart>,
    tool_specs: &[ToolSpec],
    turn_override: &TurnOverride,
    effective_max_output: u64,
    messages: &mut Vec<Message>,
    prepared: &mut PreparedModelRequest,
) -> Result<bool, CompactionFailure> {
    let candidate = messages.clone();
    apply_target_context_adaptation(
        shared,
        target,
        cwd,
        turn_user_uuid,
        system_prompt,
        tool_specs,
        turn_override,
        effective_max_output,
        messages,
        prepared,
        candidate,
        true,
        "Automatic context compaction is disabled for the selected model.",
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn apply_target_context_adaptation(
    shared: &Arc<Shared>,
    target: &Arc<crate::engine::session::InferenceBundle>,
    cwd: &RuntimePath,
    turn_user_uuid: &Option<RuntimeString>,
    system_prompt: &mut Vec<SystemPromptPart>,
    tool_specs: &[ToolSpec],
    turn_override: &TurnOverride,
    effective_max_output: u64,
    messages: &mut Vec<Message>,
    prepared: &mut PreparedModelRequest,
    mut candidate: Vec<Message>,
    force_summarize: bool,
    disabled_error: &'static str,
) -> Result<bool, CompactionFailure> {
    let config = shared.config();
    let before = crate::context::tokens::estimate_context_tokens(messages);
    let source_ids: Vec<RuntimeString> = messages
        .iter()
        .map(super::super::query_message_uuid)
        .cloned()
        .collect();
    let mut next = shared.prepare(
        &candidate,
        system_prompt,
        effective_max_output,
        tool_specs,
        Some(turn_override),
    );
    validate_restored_media(shared, &next.request)?;
    let model_limit = model_auto_compact_token_limit(&config);
    let usable = usable_context_window(&config);
    let limit = if model_limit == 0 {
        usable
    } else {
        model_limit.min(usable)
    };
    let mut summarized = false;
    let mut single_pass_summary = false;
    let needs_summary =
        force_summarize || super::super::request::rendered_context_tokens(&next.request) >= limit;
    if needs_summary {
        if config.compaction_disabled || !config.auto_compaction {
            return Err(disabled_error.into());
        }
        let deps = target.deps.to_engine_deps();
        let compactor = deps.compactor;
        let instructions = compaction_hook(shared, cwd, HookEvent::PreCompact, None).await?;
        let previous_observer = shared.params.compaction_output.clone();
        compactor.bind_output_observer(None);
        shared.q.push(EngineEvent::CompactionProgress {
            phase: "started".into(),
        });
        let result = summarize_for_target(
            shared,
            &compactor,
            &config,
            &candidate,
            turn_user_uuid,
            system_prompt,
            tool_specs,
            turn_override,
            effective_max_output,
            limit,
            force_summarize,
            instructions.as_ref(),
        )
        .await;
        shared.q.push(EngineEvent::CompactionProgress {
            phase: "completed".into(),
        });
        compactor.bind_output_observer(previous_observer);
        let (next_candidate, single_pass) = result?;
        candidate = next_candidate;
        single_pass_summary = single_pass;
        summarized = true;
    }
    if shared.params.signal.is_cancelled() {
        return Err(CompactionFailure::Aborted);
    }
    // Re-inject only the retained reminder fragments this rewrite dropped;
    // fragments still visible to the model contribute zero bytes.
    {
        let profile = crate::provider::behavior::resolve_provider_behavior_profile(
            shared.params.provider_info.as_ref(),
            &turn_override.model,
        );
        reinject_query_retained_fragments(shared, &mut candidate, &profile);
    }
    // Refresh current instructions before the atomic context replacement.
    let has_task_tool = shared.params.tools.iter().any(|tool| tool.name() == "task");
    super::super::turn::refresh_retained_context_inner(
        shared,
        &mut candidate,
        cwd,
        has_task_tool,
        false,
    )
    .await;
    if !super::super::turn::refresh_captured_system_prompt(shared, system_prompt).await {
        return Err(CompactionFailure::Aborted);
    }
    super::super::turn::place_retained_context(&mut candidate);
    crate::context::tokens::invalidate_context_usage(&mut candidate);
    next = shared.prepare(
        &candidate,
        system_prompt,
        effective_max_output,
        tool_specs,
        Some(turn_override),
    );
    validate_restored_media(shared, &next.request)?;
    if !single_pass_summary
        && super::super::request::rendered_context_tokens(&next.request) >= usable
    {
        // When raw recovery without a summary (or multi-chunk synthesis) still
        // exceeds the usable window, drop the oldest unprotected turns so the
        // conversation remains usable. A completed single-pass summary is
        // committed verbatim so the provider makes the final admission decision.
        candidate = fit_restored_to_window(
            shared,
            candidate,
            system_prompt,
            tool_specs,
            turn_override,
            effective_max_output,
            turn_user_uuid,
            usable,
        )?;
        next = shared.prepare(
            &candidate,
            system_prompt,
            effective_max_output,
            tool_specs,
            Some(turn_override),
        );
        validate_restored_media(shared, &next.request)?;
        if super::super::request::rendered_context_tokens(&next.request) >= usable {
            return Err(
                "The selected model has no room for this conversation even after dropping restored history."
                    .into(),
            );
        }
    }
    if shared.params.signal.is_cancelled() {
        return Err(CompactionFailure::Aborted);
    }
    super::super::compaction::persist_compaction(shared, &mut candidate, source_ids, before).await;
    if shared.params.signal.is_cancelled() {
        return Err(CompactionFailure::Aborted);
    }
    // Persistence may merge answers admitted while the adaptation was running.
    next = shared.prepare(
        &candidate,
        system_prompt,
        effective_max_output,
        tool_specs,
        Some(turn_override),
    );
    validate_restored_media(shared, &next.request)?;
    if let Some(downshift) = &target.context_downshift {
        downshift.complete();
    }
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
    if summarized {
        shared.q.push(EngineEvent::Compaction {
            mode: CompactionMode::Full,
            freed_tokens: before
                .saturating_sub(crate::context::tokens::estimate_context_tokens(messages)),
        });
        compaction_hook(
            shared,
            cwd,
            HookEvent::PostCompact,
            Some(compaction_summary_of(messages)),
        )
        .await?;
    }
    Ok(true)
}

async fn compaction_hook(
    shared: &Shared,
    cwd: &RuntimePath,
    event: HookEvent,
    message: Option<RuntimeString>,
) -> Result<Option<RuntimeString>, CompactionFailure> {
    let Some(bus) = &shared.params.hook_bus else {
        return Ok(None);
    };
    if !bus.has(event) {
        return Ok(None);
    }
    let mut input = base_hook_input(event, cwd, &shared.params.thread_id);
    input.trigger = Some(HookTrigger::Auto);
    input.message = message;
    let outcome = tokio::select! {
        biased;
        _ = shared.params.signal.cancelled() => return Err(CompactionFailure::Aborted),
        outcome = bus.dispatch(event, &input) => outcome,
    };
    for message in outcome.system_messages {
        notify(&shared.q, NotificationLevel::Info, message);
    }
    if outcome.aborted || outcome.blocked {
        return Err(CompactionFailure::Aborted);
    }
    Ok((!outcome.additional_context.is_empty())
        .then(|| join_runtime_strings(&outcome.additional_context, "\n")))
}

fn validate_restored_media(
    shared: &Shared,
    request: &ModelRequest,
) -> Result<(), CompactionFailure> {
    shared
        .validate_media(request)
        .map_err(|error| CompactionFailure::from(RuntimeString::from(error.message.clone())))?;
    if request_contains_document(request) {
        shared
            .validate_pdf(&request.model)
            .map_err(|error| CompactionFailure::from(RuntimeString::from(error.message.clone())))?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn summarize_for_target(
    shared: &Arc<Shared>,
    compactor: &Arc<dyn crate::engine::deps::Compactor>,
    config: &crate::schemas::config::ResolvedConfig,
    restored: &[Message],
    turn_user_uuid: &Option<RuntimeString>,
    system_prompt: &[SystemPromptPart],
    tool_specs: &[ToolSpec],
    turn_override: &TurnOverride,
    effective_max_output: u64,
    limit: u64,
    force_at_least_one_pass: bool,
    instructions: Option<&RuntimeString>,
) -> Result<(Vec<Message>, bool), CompactionFailure> {
    let mut protected_ids = shared.params.compaction_protected_message_ids.clone();
    if !force_at_least_one_pass {
        if let Some(uuid) = turn_user_uuid {
            protected_ids.insert(uuid.clone());
        }
    }
    let (raw_body, protected) = split_compaction_protected(restored, &protected_ids);
    let file_note = collect_file_operations_note(&raw_body);
    let combined_instructions = match (instructions, file_note) {
        (Some(base), Some(note)) if !base.is_empty() => {
            Some(base.concat(&RuntimeString::from("\n\n")).concat(&note))
        }
        (Some(base), None) => Some(base.clone()),
        (None, Some(note)) | (Some(_), Some(note)) => Some(note),
        (None, None) => None,
    };
    let mut body = pre_prune_summary_body(&raw_body);
    let usable = usable_context_window(config);

    // Fast path: if the pre-pruned body fits in one summary call on the
    // selected model, run a single pass (allowing native remote compaction or
    // zero-request session-memory compaction when available).
    let lean_single =
        prepare_lean_summary_request(shared, &body, turn_override, effective_max_output);
    let single_budget = if compactor.native_summary_active() {
        usable
    } else {
        let reserve = (usable / 10).clamp(1_024, 8_192);
        usable.saturating_sub(reserve)
    };
    if !body.is_empty()
        && single_budget > 0
        && super::super::request::rendered_context_tokens(&lean_single.request) <= single_budget
    {
        validate_restored_media(shared, &lean_single.request)?;
        compactor.prepare_request(&lean_single.request, &shared.params.signal);
        let result = tokio::select! {
            biased;
            _ = shared.params.signal.cancelled() => return Err(CompactionFailure::Aborted),
            result = compactor.threshold_compact(&body, config, combined_instructions.as_ref()) => result,
        };
        if shared.params.signal.is_cancelled() {
            return Err(CompactionFailure::Aborted);
        }
        if result.compacted && result.has_new_checkpoint(&body) {
            let authoritative = authoritative_unprotected(shared, &raw_body, &protected_ids);
            let refreshed = crate::context::compact::full_compact::refresh_local_user_history(
                &authoritative,
                result.messages,
            );
            let candidate =
                restore_compaction_protected(refreshed, protected.clone(), turn_user_uuid.as_ref());
            return Ok((candidate, true));
        } else if !result.compacted {
            // Surface deterministic provider errors (such as target auth/quota
            // failures) immediately rather than repeating identical calls;
            // fall through to chunked summarization only on size rejections.
            let is_overflow = result.diagnostics.iter().any(|item| {
                item.kind
                    == crate::context::compact::outcome::CompactionDiagnosticKind::PromptTooLong
            });
            if !is_overflow {
                return Err(summary_failure_from_diagnostics(
                    &result.diagnostics,
                    "The selected model did not return a completed context checkpoint.",
                ));
            }
        }
    }

    let mut completed_pass = false;
    for _ in 0..8 {
        let candidate =
            restore_compaction_protected(body.clone(), protected.clone(), turn_user_uuid.as_ref());
        let prepared = shared.prepare(
            &candidate,
            system_prompt,
            effective_max_output,
            tool_specs,
            Some(turn_override),
        );
        if (!force_at_least_one_pass || completed_pass)
            && super::super::request::rendered_context_tokens(&prepared.request) < limit
        {
            return Ok((candidate, false));
        }
        let empty_lean =
            prepare_lean_summary_request(shared, &[], turn_override, effective_max_output);
        let fixed = fixed_prefix_tokens(&empty_lean.request);
        let chunk_ceiling = limit.max(usable);
        let chunk_reserve = (chunk_ceiling / 4).min(8_192);
        let budget = chunk_ceiling
            .saturating_sub(fixed)
            .saturating_sub(chunk_reserve);
        if budget == 0 {
            return Err(
                "The selected model has no room for a complete migration summary request.".into(),
            );
        }
        let before = crate::context::tokens::estimate_context_tokens(&body);
        let chunks = split_chunks(&body, budget)?;
        let mut summaries = Vec::new();
        for chunk in chunks {
            let chunk_summaries = summarize_chunk_with_split_fallback(
                shared,
                compactor,
                config,
                chunk,
                turn_override,
                effective_max_output,
                combined_instructions.as_ref(),
            )
            .await?;
            summaries.extend(chunk_summaries);
        }
        completed_pass = true;
        if summaries.len() <= 1
            && crate::context::tokens::estimate_context_tokens(&summaries) >= before
        {
            return Err(
                "The selected model's migration summaries did not reduce the restored context."
                    .into(),
            );
        }
        let authoritative = authoritative_unprotected(shared, &raw_body, &protected_ids);
        body = crate::context::compact::full_compact::refresh_local_user_history(
            &authoritative,
            summaries,
        );
    }
    Err(
        "The conversation history could not fit the selected model within the summary pass limit."
            .into(),
    )
}

async fn summarize_chunk_with_split_fallback(
    shared: &Arc<Shared>,
    compactor: &Arc<dyn crate::engine::deps::Compactor>,
    config: &crate::schemas::config::ResolvedConfig,
    chunk: Vec<Message>,
    turn_override: &TurnOverride,
    effective_max_output: u64,
    instructions: Option<&RuntimeString>,
) -> Result<Vec<Message>, CompactionFailure> {
    let mut work_queue = vec![(chunk, 0u8)];
    let mut outputs = Vec::new();
    while let Some((piece, depth)) = work_queue.pop() {
        let request =
            prepare_lean_summary_request(shared, &piece, turn_override, effective_max_output);
        validate_restored_media(shared, &request.request)?;
        compactor.prepare_request(&request.request, &shared.params.signal);
        let result = tokio::select! {
            biased;
            _ = shared.params.signal.cancelled() => return Err(CompactionFailure::Aborted),
            result = compactor.local_compact_exact(&piece, config, instructions) => result,
        };
        if shared.params.signal.is_cancelled() {
            return Err(CompactionFailure::Aborted);
        }
        if result.compacted && result.has_new_checkpoint(&piece) {
            if crate::provider::native_compaction::contains_native_checkpoint(&result.messages) {
                return Err(
                    "The portable context summary unexpectedly contains encrypted provider state."
                        .into(),
                );
            }
            outputs.extend(result.messages);
            continue;
        }
        let overflow = result.diagnostics.iter().any(|item| {
            item.kind == crate::context::compact::outcome::CompactionDiagnosticKind::PromptTooLong
        });
        if overflow && depth < 4 {
            if let Some((left, right)) = bisect_closed_messages(&piece) {
                // Push right first so LIFO pops left first, preserving chronological order.
                work_queue.push((right, depth + 1));
                work_queue.push((left, depth + 1));
                continue;
            }
            let current_tokens = crate::context::tokens::estimate_context_tokens(&piece);
            if current_tokens > 256 {
                let mut shrunk = piece;
                shrink_piece_to_budget(&mut shrunk, (current_tokens / 2).max(128));
                work_queue.push((shrunk, depth + 1));
                continue;
            }
        }
        return Err(summary_failure_from_diagnostics(
            &result.diagnostics,
            "The selected model did not complete the context summary.",
        ));
    }
    Ok(outputs)
}

fn prepare_lean_summary_request(
    shared: &Arc<Shared>,
    messages: &[Message],
    turn_override: &TurnOverride,
    effective_max_output: u64,
) -> PreparedModelRequest {
    // Summarization neither calls tools nor benefits from a cross-model system
    // prompt prefix, so omitting both reserves the new model's window for history.
    shared.prepare(
        messages,
        &[],
        effective_max_output,
        &[],
        Some(turn_override),
    )
}

fn authoritative_unprotected(
    shared: &Arc<Shared>,
    fallback: &[Message],
    protected_ids: &std::collections::HashSet<RuntimeString>,
) -> Vec<Message> {
    let source = shared
        .params
        .compaction_history
        .as_ref()
        .map_or_else(|| fallback.to_vec(), |snapshot| snapshot());
    source
        .into_iter()
        .filter(|message| !protected_ids.contains(super::super::query_message_uuid(message)))
        .collect()
}

fn summary_failure_from_diagnostics(
    diagnostics: &[crate::context::compact::outcome::CompactionDiagnostic],
    fallback: &'static str,
) -> CompactionFailure {
    if let Some(error) = diagnostics
        .iter()
        .find_map(|item| item.provider_error.clone())
    {
        return CompactionFailure::Service(EngineError::from_provider(
            error,
            "context-downshift-compaction",
        ));
    }
    let messages: Vec<_> = diagnostics
        .iter()
        .filter_map(|item| item.message.clone())
        .collect();
    if messages.is_empty() {
        RuntimeString::from(fallback).into()
    } else {
        join_runtime_strings(&messages, "; ").into()
    }
}

/// Strip prior-model reasoning, replace binary attachments with lightweight
/// markers, and cap aged tool outputs on the summary copy while leaving the
/// durable rollout history untouched.
fn pre_prune_summary_body(messages: &[Message]) -> Vec<Message> {
    let recent_cutoff = messages.len().saturating_sub(PRESERVED_RECENT_MESSAGES);
    let mut pruned = Vec::with_capacity(messages.len());
    for (index, message) in messages.iter().enumerate() {
        let mut copy = message.clone();
        let is_aged = index < recent_cutoff;
        match &mut copy {
            Message::User(user) => {
                scrub_summary_blocks(&mut user.content, is_aged);
                if user.content.is_empty() && user.question_answer.is_none() {
                    continue;
                }
            }
            Message::Assistant(assistant) => {
                assistant.usage = None;
                assistant.content.retain(|block| {
                    !matches!(
                        block,
                        ContentBlock::Thinking(_) | ContentBlock::RedactedThinking(_)
                    )
                });
                scrub_summary_blocks(&mut assistant.content, false);
                if assistant.content.is_empty() {
                    continue;
                }
            }
            Message::ToolResult(result) => {
                result.native_tool_output = None;
                scrub_summary_outputs(&mut result.content, is_aged);
                if let Some(extra) = result.extra_user_blocks.as_mut() {
                    scrub_summary_blocks(extra, is_aged);
                }
            }
            Message::System(_) | Message::CompactBoundary(_) => {}
        }
        pruned.push(copy);
    }
    crate::engine::resume_repair::normalize_tool_history_owned(pruned)
}

fn scrub_summary_blocks(blocks: &mut [ContentBlock], cap_tool_outputs: bool) {
    for block in blocks.iter_mut() {
        match block {
            ContentBlock::Image(_) => {
                *block = ContentBlock::Text(plain_text("[image attachment]"));
            }
            ContentBlock::Document(doc) => {
                let label = doc
                    .filename
                    .as_ref()
                    .and_then(|name| name.try_to_utf8().ok())
                    .filter(|name| !name.is_empty())
                    .map_or_else(
                        || "[PDF document]".to_string(),
                        |name| format!("[PDF document: {name}]"),
                    );
                *block = ContentBlock::Text(plain_text(&label));
            }
            ContentBlock::Media(media) => {
                let label = media.mime_type.try_to_utf8().map_or_else(
                    |_| "[media attachment]".to_string(),
                    |mime| format!("[media: {mime}]"),
                );
                *block = ContentBlock::Text(plain_text(&label));
            }
            ContentBlock::ToolResult(result) => {
                scrub_summary_outputs(&mut result.content, cap_tool_outputs);
            }
            ContentBlock::Thinking(thinking) => {
                thinking.encrypted_content = None;
                thinking.signature = None;
            }
            _ => {}
        }
    }
}

fn scrub_summary_outputs(outputs: &mut [OutputBlock], cap_text: bool) {
    for output in outputs.iter_mut() {
        match output {
            OutputBlock::Image(_) => {
                *output = OutputBlock::Text(plain_text("[tool image output]"));
            }
            OutputBlock::Text(text) if cap_text => {
                text.text = truncate_head_tail(&text.text, AGED_TOOL_OUTPUT_MAX_CHARS);
            }
            _ => {}
        }
    }
}

fn plain_text(value: &str) -> TextBlock {
    TextBlock {
        text: RuntimeString::from(value),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    }
}

fn truncate_head_tail(text: &RuntimeString, max_chars: usize) -> RuntimeString {
    let Ok(owned) = text.try_to_utf8() else {
        return text.clone();
    };
    let total = owned.chars().count();
    if total <= max_chars {
        return text.clone();
    }
    let head_len = (max_chars * 3) / 4;
    let tail_len = max_chars.saturating_sub(head_len);
    let head: String = owned.chars().take(head_len).collect();
    let tail: String = owned.chars().skip(total.saturating_sub(tail_len)).collect();
    let omitted = total.saturating_sub(head_len + tail_len);
    RuntimeString::from(format!(
        "{head}\n…[{omitted} chars of historical tool output truncated for summary]…\n{tail}"
    ))
}

fn collect_file_operations_note(messages: &[Message]) -> Option<RuntimeString> {
    let mut read_files = BTreeSet::new();
    let mut modified_files = BTreeSet::new();
    for message in messages {
        let Message::Assistant(assistant) = message else {
            continue;
        };
        for block in &assistant.content {
            let ContentBlock::ToolUse(call) = block else {
                continue;
            };
            let Ok(name) = call.name.try_to_utf8() else {
                continue;
            };
            let Some(path) = extract_tool_path(&call.input) else {
                continue;
            };
            match name.as_str() {
                "read" | "Read" => {
                    if read_files.len() < MAX_TRACKED_FILE_PATHS {
                        read_files.insert(path);
                    }
                }
                "write" | "Write" | "edit" | "Edit" | "multi_edit" | "MultiEdit"
                | "notebook_edit" => {
                    if modified_files.len() < MAX_TRACKED_FILE_PATHS {
                        modified_files.insert(path);
                    }
                }
                _ => {}
            }
        }
    }
    for modified in &modified_files {
        read_files.remove(modified);
    }
    if read_files.is_empty() && modified_files.is_empty() {
        return None;
    }
    let mut sections = Vec::new();
    if !modified_files.is_empty() {
        sections.push(format!(
            "Modified files in this segment: {}",
            modified_files.into_iter().collect::<Vec<_>>().join(", ")
        ));
    }
    if !read_files.is_empty() {
        sections.push(format!(
            "Read files in this segment: {}",
            read_files.into_iter().collect::<Vec<_>>().join(", ")
        ));
    }
    Some(RuntimeString::from(format!(
        "Preserve these exact file references in the handoff summary when relevant:\n{}",
        sections.join("\n")
    )))
}

fn extract_tool_path(input: &crate::runtime_values::RuntimeJson) -> Option<String> {
    let crate::runtime_values::RuntimeJson::Object(entries) = input else {
        return None;
    };
    for key in ["path", "file_path", "notebook_path"] {
        for (entry_key, value) in entries {
            if entry_key == key {
                if let crate::runtime_values::RuntimeJson::String(raw) = value {
                    if let Ok(text) = raw.try_to_utf8() {
                        if !text.trim().is_empty() {
                            return Some(text);
                        }
                    }
                }
            }
        }
    }
    None
}

fn bisect_closed_messages(messages: &[Message]) -> Option<(Vec<Message>, Vec<Message>)> {
    if messages.len() < 2 {
        return None;
    }
    let mut boundaries = Vec::new();
    let mut open = std::collections::HashSet::new();
    for (index, message) in messages.iter().enumerate() {
        match message {
            Message::Assistant(assistant) => {
                for block in &assistant.content {
                    if let ContentBlock::ToolUse(call) = block {
                        open.insert(call.id.clone());
                    }
                }
            }
            Message::ToolResult(result) => {
                open.remove(&result.tool_use_id);
            }
            _ => {}
        }
        let cut = index + 1;
        if open.is_empty() && cut < messages.len() {
            boundaries.push(cut);
        }
    }
    let mid = messages.len() / 2;
    let split = boundaries
        .into_iter()
        .min_by_key(|candidate| candidate.abs_diff(mid))?;
    Some((messages[..split].to_vec(), messages[split..].to_vec()))
}

fn split_chunks(messages: &[Message], budget: u64) -> Result<Vec<Vec<Message>>, CompactionFailure> {
    let mut chunks = Vec::new();
    let mut start = 0;
    while start < messages.len() {
        let mut tokens: u64 = 0;
        let mut end = start;
        let mut safe_end = start;
        let mut open = std::collections::HashSet::new();
        while end < messages.len() {
            let cost = crate::context::tokens::estimate_context_tokens(&messages[end..end + 1]);
            if tokens.saturating_add(cost) > budget {
                break;
            }
            tokens += cost;
            match &messages[end] {
                Message::Assistant(assistant) => {
                    for block in &assistant.content {
                        if let ContentBlock::ToolUse(call) = block {
                            open.insert(call.id.clone());
                        }
                    }
                }
                Message::ToolResult(result) => {
                    open.remove(&result.tool_use_id);
                }
                _ => {}
            }
            end += 1;
            if open.is_empty() {
                safe_end = end;
            }
        }
        if safe_end == start {
            // One exchange is larger than the summary budget. Truncate its text
            // and continue; refusing here aborts a model switch outright.
            let exchange_end = closed_exchange_end(messages, start);
            let mut piece = messages[start..exchange_end].to_vec();
            shrink_piece_to_budget(&mut piece, budget);
            chunks.push(piece);
            start = exchange_end;
            continue;
        }
        chunks.push(messages[start..safe_end].to_vec());
        start = safe_end;
    }
    Ok(chunks)
}

fn closed_exchange_end(messages: &[Message], start: usize) -> usize {
    if start >= messages.len() {
        return start;
    }
    let mut open = std::collections::HashSet::new();
    let mut end = start;
    while end < messages.len() {
        match &messages[end] {
            Message::Assistant(assistant) => {
                for block in &assistant.content {
                    if let ContentBlock::ToolUse(call) = block {
                        open.insert(call.id.clone());
                    }
                }
            }
            Message::ToolResult(result) => {
                open.remove(&result.tool_use_id);
            }
            _ => {}
        }
        end += 1;
        if open.is_empty() || end > start.saturating_add(8) {
            return end;
        }
    }
    end
}

fn shrink_piece_to_budget(piece: &mut [Message], budget: u64) {
    let mut cap = 8_000usize;
    for _ in 0..16 {
        if budget == 0 || crate::context::tokens::estimate_context_tokens(piece) <= budget {
            return;
        }
        for message in piece.iter_mut() {
            shrink_message(message, cap);
        }
        cap = (cap / 2).max(64);
    }
}

fn clip_runtime(text: &RuntimeString, max_chars: usize) -> RuntimeString {
    let Ok(owned) = text.try_to_utf8() else {
        return text.clone();
    };
    if owned.chars().count() <= max_chars {
        return text.clone();
    }
    let clipped: String = owned.chars().take(max_chars).collect();
    RuntimeString::from(format!(
        "{clipped}\n…truncated so this conversation can continue on the selected model…"
    ))
}

fn shrink_blocks(blocks: &mut [ContentBlock], max_chars: usize) {
    for block in blocks {
        match block {
            ContentBlock::Text(text) => text.text = clip_runtime(&text.text, max_chars),
            ContentBlock::Thinking(thinking) => {
                thinking.text = clip_runtime(&thinking.text, max_chars);
                thinking.encrypted_content = None;
                thinking.signature = None;
            }
            ContentBlock::RedactedThinking(thinking) => {
                thinking.data = clip_runtime(&thinking.data, max_chars);
            }
            ContentBlock::ToolResult(result) => {
                for output in &mut result.content {
                    if let OutputBlock::Text(text) = output {
                        text.text = clip_runtime(&text.text, max_chars);
                    }
                }
            }
            _ => {}
        }
    }
}

fn shrink_message(message: &mut Message, max_chars: usize) {
    match message {
        Message::User(user) => shrink_blocks(&mut user.content, max_chars),
        Message::Assistant(assistant) => {
            assistant.usage = None;
            shrink_blocks(&mut assistant.content, max_chars);
        }
        Message::ToolResult(result) => {
            for output in &mut result.content {
                if let OutputBlock::Text(text) = output {
                    text.text = clip_runtime(&text.text, max_chars);
                }
            }
            if let Some(extra) = result.extra_user_blocks.as_mut() {
                shrink_blocks(extra, max_chars);
            }
            result.native_tool_output = None;
        }
        Message::System(system) => system.text = clip_runtime(&system.text, max_chars),
        Message::CompactBoundary(boundary) => {
            boundary.summary = clip_runtime(&boundary.summary, max_chars);
            boundary.native_compaction = None;
        }
    }
}

fn drop_leading_unprotected(
    messages: &mut Vec<Message>,
    protected: &std::collections::HashSet<RuntimeString>,
) -> bool {
    if messages.is_empty() || protected.contains(super::super::query_message_uuid(&messages[0])) {
        return false;
    }
    let end = closed_exchange_end(messages, 0);
    if end == 0
        || messages[..end]
            .iter()
            .any(|message| protected.contains(super::super::query_message_uuid(message)))
    {
        return false;
    }
    messages.drain(0..end);
    true
}

#[allow(clippy::too_many_arguments)]
fn fit_restored_to_window(
    shared: &Arc<Shared>,
    mut candidate: Vec<Message>,
    system_prompt: &[SystemPromptPart],
    tool_specs: &[ToolSpec],
    turn_override: &TurnOverride,
    effective_max_output: u64,
    turn_user_uuid: &Option<RuntimeString>,
    limit: u64,
) -> Result<Vec<Message>, CompactionFailure> {
    let mut protected = shared.params.compaction_protected_message_ids.clone();
    if let Some(uuid) = turn_user_uuid {
        protected.insert(uuid.clone());
    }
    for message in &mut candidate {
        if let Message::Assistant(assistant) = message {
            assistant.usage = None;
        }
    }
    for _ in 0..candidate.len().saturating_add(4) {
        let prepared = shared.prepare(
            &candidate,
            system_prompt,
            effective_max_output,
            tool_specs,
            Some(turn_override),
        );
        if super::super::request::rendered_context_tokens(&prepared.request) < limit {
            return Ok(candidate);
        }
        if drop_leading_unprotected(&mut candidate, &protected) {
            continue;
        }
        for message in &mut candidate {
            shrink_message(message, 512);
        }
        let prepared = shared.prepare(
            &candidate,
            system_prompt,
            effective_max_output,
            tool_specs,
            Some(turn_override),
        );
        if super::super::request::rendered_context_tokens(&prepared.request) < limit {
            return Ok(candidate);
        }
        break;
    }
    Err(
        "The selected model has no room for this conversation even after dropping restored history."
            .into(),
    )
}
