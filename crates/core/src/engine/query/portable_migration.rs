//! Restore incompatible opaque context using local history and the selected transport.

use super::*;
use crate::context::compact::policy::{model_auto_compact_token_limit, usable_context_window};
use crate::types::provider::ModelRequest;

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
        // Non-persistent internal queries may still have their original transport.
        if target
            .context_migration
            .as_ref()
            .and_then(|migration| migration.source())
            .is_some()
        {
            return Ok(false);
        }
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
    let before = crate::context::tokens::estimate_context_tokens(messages);
    let source_ids = messages
        .iter()
        .map(super::super::query_message_uuid)
        .cloned()
        .collect();
    let mut candidate = restored;
    let mut next = shared.prepare(
        &candidate,
        system_prompt,
        effective_max_output,
        tool_specs,
        Some(turn_override),
    );
    validate_restored_media(shared, &next.request)?;
    let model_limit = model_auto_compact_token_limit(&config);
    let limit = if model_limit == 0 {
        usable_context_window(&config)
    } else {
        model_limit.min(usable_context_window(&config))
    };
    let mut summarized = false;
    if super::super::request::rendered_context_tokens(&next.request) >= limit {
        if config.compaction_disabled || !config.auto_compaction {
            return Err("The restored history exceeds the selected model's context window and automatic compaction is disabled.".into());
        }
        let deps = target.deps.to_engine_deps();
        let compactor = deps.compactor;
        let instructions = compaction_hook(shared, cwd, HookEvent::PreCompact, None).await?;
        // Partial summaries remain private until the whole migration is ready.
        compactor.bind_output_observer(None);
        shared.q.push(EngineEvent::CompactionProgress {
            phase: "started".into(),
        });
        let result = summarize_bounded(
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
            instructions.as_ref(),
        )
        .await;
        shared.q.push(EngineEvent::CompactionProgress {
            phase: "completed".into(),
        });
        compactor.bind_output_observer(shared.params.compaction_output.clone());
        candidate = result?;
        summarized = true;
    }
    if shared.params.signal.is_cancelled() {
        return Err(CompactionFailure::Aborted);
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
    if super::super::request::rendered_context_tokens(&next.request)
        >= usable_context_window(&config)
    {
        // A model switch has to keep the conversation usable. Drop the oldest
        // restored turns instead of refusing the message the user just sent.
        candidate = fit_restored_to_window(
            shared,
            candidate,
            system_prompt,
            tool_specs,
            turn_override,
            effective_max_output,
            turn_user_uuid,
            usable_context_window(&config),
        )?;
        next = shared.prepare(
            &candidate,
            system_prompt,
            effective_max_output,
            tool_specs,
            Some(turn_override),
        );
        validate_restored_media(shared, &next.request)?;
        if super::super::request::rendered_context_tokens(&next.request)
            >= usable_context_window(&config)
        {
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
    // Persistence may merge answers admitted while the recovery was running.
    next = shared.prepare(
        &candidate,
        system_prompt,
        effective_max_output,
        tool_specs,
        Some(turn_override),
    );
    validate_restored_media(shared, &next.request)?;
    if let Some(migration) = &target.context_migration {
        migration.complete();
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
async fn summarize_bounded(
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
    instructions: Option<&RuntimeString>,
) -> Result<Vec<Message>, CompactionFailure> {
    let mut protected_ids = shared.params.compaction_protected_message_ids.clone();
    if let Some(uuid) = turn_user_uuid {
        protected_ids.insert(uuid.clone());
    }
    let (mut body, protected) = split_compaction_protected(restored, &protected_ids);
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
        if super::super::request::rendered_context_tokens(&prepared.request) < limit {
            return Ok(candidate);
        }
        let fixed = fixed_prefix_tokens(&prepared.request);
        let reserve = (limit / 4).min(8192);
        let budget = limit.saturating_sub(fixed).saturating_sub(reserve);
        if budget == 0 {
            return Err(
                "The selected model has no room for a complete migration summary request.".into(),
            );
        }
        let before = crate::context::tokens::estimate_context_tokens(&body);
        let chunks = split_chunks(&body, budget)?;
        let mut summaries = Vec::new();
        for chunk in chunks {
            let request = shared.prepare(
                &chunk,
                system_prompt,
                effective_max_output,
                tool_specs,
                Some(turn_override),
            );
            validate_restored_media(shared, &request.request)?;
            compactor.prepare_request(&request.request, &shared.params.signal);
            let result = tokio::select! {
                biased;
                _ = shared.params.signal.cancelled() => return Err(CompactionFailure::Aborted),
                result = compactor.local_compact_exact(&chunk, config, instructions) => result,
            };
            if !result.compacted || !result.has_new_checkpoint(&chunk) {
                let messages: Vec<_> = result
                    .diagnostics
                    .iter()
                    .filter_map(|item| item.message.clone())
                    .collect();
                return Err(if messages.is_empty() {
                    RuntimeString::from(
                        "The selected model did not complete the portable context summary.",
                    )
                } else {
                    join_runtime_strings(&messages, "; ")
                }
                .into());
            }
            if crate::provider::native_compaction::contains_native_checkpoint(&result.messages) {
                return Err(
                    "The portable context summary unexpectedly contains encrypted provider state."
                        .into(),
                );
            }
            summaries.extend(result.messages);
        }
        if crate::context::tokens::estimate_context_tokens(&summaries) >= before {
            return Err(
                "The selected model's migration summaries did not reduce the restored context."
                    .into(),
            );
        }
        body = summaries;
    }
    Err(
        "The restored history could not fit the selected model within the migration summary limit."
            .into(),
    )
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
                    if let crate::schemas::content::OutputBlock::Text(text) = output {
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
                if let crate::schemas::content::OutputBlock::Text(text) = output {
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
