//! Compaction persistence wiring and protected-message reinsertion, including
//! the running-agent / surfaced-tools reminder fragments.

use super::join_runtime_strings;
/// The latest checkpoint summary text, or an empty string for opaque checkpoints.
use super::CompactionRecord;
use super::QueryParams;
use crate::context::manager::ContextFragment;
use crate::context::manager::ContextManager;
use crate::provider::behavior::ProviderBehaviorProfile;
use crate::runtime_values::RuntimeString;
use crate::schemas::message::Message;
use crate::types::tool::BackgroundTaskRegistry;
use crate::types::tool::BackgroundTaskStatus;
use crate::types::tool::Tool;
use crate::types::tool::ToolExposure;
use std::collections::HashSet;
use std::sync::Arc;
pub fn compaction_summary_of(messages: &[Message]) -> RuntimeString {
    for m in messages.iter().rev() {
        if let Message::CompactBoundary(c) = m {
            return c.summary.clone();
        }
    }
    RuntimeString::default()
}

pub(super) async fn persist_compaction(
    shared: &super::Shared,
    snapshot: &mut Vec<Message>,
    source_message_ids: Vec<RuntimeString>,
    before_tokens: u64,
) {
    persist_compaction_from(
        shared,
        snapshot,
        source_message_ids,
        before_tokens,
        shared.deps().compactor.as_ref(),
    )
    .await;
}

pub(super) async fn persist_compaction_from(
    shared: &super::Shared,
    snapshot: &mut Vec<Message>,
    source_message_ids: Vec<RuntimeString>,
    before_tokens: u64,
    _compactor: &dyn crate::engine::deps::Compactor,
) {
    merge_fresh_answers(&shared.params, snapshot, &source_message_ids);
    let after_tokens = crate::context::tokens::estimate_context_tokens(snapshot);
    {
        let mut spend = shared.reminder_spend.lock().unwrap();
        spend.high_water_tokens = spend.total_with(before_tokens);
        spend.base_tokens = spend
            .base_tokens
            .saturating_add(before_tokens.saturating_sub(after_tokens));
    }
    persist_checkpoint(&shared.params, snapshot, source_message_ids.clone(), true).await;
    merge_fresh_answers(&shared.params, snapshot, &source_message_ids);
}

fn merge_fresh_answers(
    params: &QueryParams,
    snapshot: &mut Vec<Message>,
    source_message_ids: &[RuntimeString],
) {
    if let Some(history) = &params.compaction_history {
        *snapshot = crate::store::projection::preserve_uncompacted_question_answers(
            std::mem::take(snapshot),
            &history(),
            source_message_ids,
        );
        crate::context::compact::full_compact::place_retained_context(snapshot);
    }
}

pub(super) async fn persist_history_repair(
    params: &QueryParams,
    snapshot: &[Message],
    source_message_ids: Vec<RuntimeString>,
) {
    persist_checkpoint(params, snapshot, source_message_ids, false).await;
}

async fn persist_checkpoint(
    params: &QueryParams,
    snapshot: &[Message],
    source_message_ids: Vec<RuntimeString>,
    retires_context: bool,
) {
    if let Some(cb) = &params.on_compaction {
        if let Err(error) = cb(CompactionRecord {
            retires_context,
            summary: compaction_summary_of(snapshot),
            replaced_range: (0, source_message_ids.len()),
            replacement_history: snapshot.to_vec(),
            source_message_ids,
        })
        .await
        {
            // The query-owned callback has already closed success publication.
            tracing::debug!(?error, "Compaction persistence callback rejected");
        }
    }
}

pub(super) fn query_message_uuid(message: &Message) -> &RuntimeString {
    match message {
        Message::User(message) => &message.uuid,
        Message::Assistant(message) => &message.uuid,
        Message::ToolResult(message) => &message.uuid,
        Message::System(message) => &message.uuid,
        Message::CompactBoundary(message) => &message.uuid,
    }
}

pub(super) fn split_compaction_protected(
    messages: &[Message],
    protected_ids: &HashSet<RuntimeString>,
) -> (Vec<Message>, Vec<Message>) {
    messages
        .iter()
        .cloned()
        .partition(|message| !protected_ids.contains(query_message_uuid(message)))
}

pub(super) fn restore_compaction_protected(
    mut compacted: Vec<Message>,
    protected: Vec<Message>,
    turn_user_uuid: Option<&RuntimeString>,
) -> Vec<Message> {
    if protected.is_empty() {
        return compacted;
    }
    let insertion = turn_user_uuid
        .and_then(|uuid| {
            compacted
                .iter()
                .position(|message| query_message_uuid(message) == uuid)
        })
        .unwrap_or(compacted.len());
    compacted.splice(insertion..insertion, protected);
    compacted
}

fn render_reinsertion_fragment(
    messages: Vec<Message>,
    id: &str,
    order: i32,
    text: RuntimeString,
    uuid: &dyn Fn() -> String,
    profile: &ProviderBehaviorProfile,
) -> Vec<Message> {
    let mut manager = ContextManager::new();
    manager.upsert(ContextFragment::user_reinsertion(id, order, text));
    manager.render_messages(messages, uuid, profile)
}

/// Re-surface running background agents as an ephemeral reminder after a
/// compaction dropped the tool_use that spawned them.
pub fn inject_running_agent_reminder(
    messages: Vec<Message>,
    background_tasks: Option<&Arc<dyn BackgroundTaskRegistry>>,
    uuid: &dyn Fn() -> String,
    profile: &ProviderBehaviorProfile,
) -> Vec<Message> {
    let running: Vec<_> = background_tasks
        .map(|b| b.list())
        .unwrap_or_default()
        .into_iter()
        .filter(|t| t.status == BackgroundTaskStatus::Running)
        .collect();
    if running.is_empty() {
        return messages;
    }
    let lines: Vec<RuntimeString> = running
        .iter()
        .map(|t| {
            let description = t
                .description
                .as_ref()
                .map(|description| RuntimeString::from(" — ").concat(description))
                .unwrap_or_default();
            RuntimeString::from("- ")
                .concat(&t.agent_id)
                .concat(&description)
                .concat(&RuntimeString::from(" (output: "))
                .concat(t.output_file.as_runtime_string())
                .concat(&RuntimeString::from(")"))
        })
        .collect();
    let text = RuntimeString::from(
        "These background tasks are still running after the context was compacted. Do NOT re-spawn them; read their results with task_output:\n",
    )
    .concat(&join_runtime_strings(&lines, "\n"));
    render_reinsertion_fragment(
        messages,
        "running-background-tasks",
        10,
        text,
        uuid,
        profile,
    )
}

/// Re-announce the deferred tools surfaced via `tool_search` after a compaction
/// dropped the search call that revealed them.
pub fn inject_surfaced_tools_reminder(
    messages: Vec<Message>,
    tools: &[Arc<dyn Tool>],
    surfaced: &HashSet<RuntimeString>,
    uuid: &dyn Fn() -> String,
    profile: &ProviderBehaviorProfile,
) -> Vec<Message> {
    if surfaced.is_empty() {
        return messages;
    }
    let available: Vec<&Arc<dyn Tool>> = tools
        .iter()
        .filter(|t| t.exposure() == ToolExposure::Deferred && surfaced.contains(&t.name()))
        .collect();
    if available.is_empty() {
        return messages;
    }
    let lines: Vec<RuntimeString> = available
        .iter()
        .map(|t| {
            RuntimeString::from("- ")
                .concat(&t.name())
                .concat(&RuntimeString::from(": "))
                .concat(&t.description())
        })
        .collect();
    let text = RuntimeString::from(
        "These tools you previously found via tool_search remain available — call them directly without searching again:\n",
    )
    .concat(&join_runtime_strings(&lines, "\n"));
    render_reinsertion_fragment(messages, "surfaced-tools", 20, text, uuid, profile)
}
