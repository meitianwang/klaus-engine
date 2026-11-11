//! Compaction persistence wiring, protected-message reinsertion, and
//! liveness-based re-injection of the retained reminder fragments.

/// The latest checkpoint summary text, or an empty string for opaque checkpoints.
use super::CompactionRecord;
use super::QueryParams;
use crate::context::retained_fragments::reinject_retained_fragments;
use crate::context::retained_fragments::RunningTasksSection;
use crate::context::retained_fragments::SurfacedToolsSection;
use crate::provider::behavior::ProviderBehaviorProfile;
use crate::runtime_values::RuntimeString;
use crate::schemas::message::Message;
use crate::sync_ext::MutexExt;
use std::collections::HashSet;
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

/// Detect which retained reminder fragments a history rewrite dropped and
/// re-inject only those, in registration order. A fragment that is still
/// visible to the model contributes zero bytes; an unverifiable surviving
/// copy is replaced instead of duplicated.
pub(super) fn reinject_query_retained_fragments(
    shared: &super::Shared,
    messages: &mut Vec<Message>,
    profile: &ProviderBehaviorProfile,
) {
    let running = RunningTasksSection::new(shared.background_tasks.as_ref());
    let surfaced = {
        let surfaced_tools = shared.surfaced.lock_recover();
        let tools = shared.tools.lock_recover().clone();
        SurfacedToolsSection::new(&tools, &surfaced_tools)
    };
    reinject_retained_fragments(
        messages,
        &[&running, &surfaced],
        &|| (shared.deps.uuid)(),
        profile,
    );
}
