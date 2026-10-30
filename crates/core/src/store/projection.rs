//! Rollout projection: rebuild the model-visible history (and the rewind
//! checkpoint ledger) from a rollout's raw item stream.

use crate::runtime_values::RuntimeString;
use crate::schemas::message::{CompactBoundaryMessage, Message};
use crate::schemas::rollout::{CompactedItem, RolloutItem};
use crate::store::history_rewind::drop_last_n_user_turns;
use std::collections::HashSet;

/// Reconstruct the model-visible message list from a rollout's items, applying
/// compaction records along the way.
///
/// The projection is produced by walking the rollout in order:
/// the raw on-disk history is replayed and
/// compaction is *projected away* so resume reconstructs the COMPACTED view the
/// model last saw — not the full untrimmed transcript.
///
/// Projection rules, applied as items are replayed oldest-to-newest:
///
///  - `message`     → appended to the running projection.
///  - `compacted`   → compaction. Two flavors:
///    • If `replacement_history` is present, it is an exact post-compaction
///    snapshot of the message list; the running projection is *replaced*
///    wholesale by it (the `replacement_history` path). Source-aware snapshots
///    preserve typed question answers that were admitted outside their source.
///    • Otherwise the half-open `replaced_range` `[start, end)` is interpreted
///    as indices into the running projection: those messages are dropped and
///    a single synthetic `compact_boundary` summary message is substituted in
///    their place (drop-and-substitute).
///  - `rollback`    → conversation rewind (`ThreadRolledBack` replay): the
///    last `num_turns` user turns are dropped from the running projection, so a
///    resumed session reconstructs the same truncated history the live rollback
///    produced.
///  - everything else (`session_meta`, `turn_context`, `event`) → ignored; these
///    are metadata/trace records, not model-visible content.
///
/// The function is total and never panics on malformed compaction ranges: an
/// out-of-bounds or inverted range is clamped, so a corrupt record degrades to a
/// best-effort projection rather than a crash.
pub fn project_for_prompt(items: &[RolloutItem]) -> Vec<Message> {
    project_for_prompt_with_initial_history(items, Vec::new())
}

/// Replay a child's own fork context before its local records. Compaction and
/// rollback operate on that entire context, so retired inherited history stays
/// retired across a restart.
pub(crate) fn project_for_prompt_with_initial_history(
    items: &[RolloutItem],
    mut history: Vec<Message>,
) -> Vec<Message> {
    for item in items {
        match item {
            RolloutItem::Message { message } => history.push(message.clone()),
            RolloutItem::Compacted(compacted) => {
                history = apply_compaction(history, compacted);
            }
            RolloutItem::Rollback(rollback) => {
                // Conversation rewind: drop the last N user turns from the running
                // projection (ThreadRolledBack forward replay).
                history = drop_last_n_user_turns(&history, f64::from(rollback.num_turns));
            }
            RolloutItem::SessionMeta { .. }
            | RolloutItem::TurnContext { .. }
            | RolloutItem::Event(_)
            | RolloutItem::Checkpoint(_)
            | RolloutItem::UsageLedger(_)
            | RolloutItem::SubagentLifecycle(_)
            | RolloutItem::BackgroundProcessLifecycle(_) => {
                // Metadata / trace records are not part of the model-visible projection.
                // (`checkpoint` is the rewind file-ledger; reconstruct it with
                // `project_checkpoints`.)
            }
        }
    }

    crate::goal::retained_context::preserve_instructions(
        history,
        &crate::goal::retained_context::instructions_from_rollout(items),
    )
}

/// Reconstruct the per-user-turn working-tree checkpoint ledger from a rollout — the resume-side
/// replay of the rewind controller's in-memory ledger. It mirrors the live ledger operations
/// EXACTLY: each `checkpoint` item pushes its hash (one per turn), and each `rollback` truncates the
/// last N entries in lockstep with the N user turns it drops. Compaction does not touch it (the live
/// ledger doesn't either). So a resumed session's file rewind targets the same checkpoints the live
/// session would have — instead of losing them and seeding nulls.
pub fn project_checkpoints(items: &[RolloutItem]) -> Vec<Option<String>> {
    let mut ledger: Vec<Option<String>> = Vec::new();
    for item in items {
        match item {
            RolloutItem::Checkpoint(checkpoint) => ledger.push(
                checkpoint
                    .hash
                    .as_ref()
                    .and_then(|hash| hash.try_to_utf8().ok()),
            ),
            RolloutItem::Rollback(rollback) => {
                // Never a negative length: an over-large rollback clamps to empty.
                let kept = ledger.len().saturating_sub(rollback.num_turns as usize);
                ledger.truncate(kept);
            }
            _ => {}
        }
    }
    ledger
}

/// Rebuild the per-message tool-result budget replacement records from a
/// rollout. Each `tool_result_budget` event carries this turn's fresh records;
/// later records for the same id win (a re-persist can only refine, never
/// contradict, an earlier body). Records whose turn a rollback dropped stay in
/// the set: their tool_use ids no longer appear in the projected history, so a
/// re-apply lookup cannot find them — harmless, exactly like a stale entry.
pub fn project_tool_result_budget_records(
    items: &[RolloutItem],
) -> Vec<(
    crate::runtime_values::RuntimeString,
    crate::runtime_values::RuntimeString,
)> {
    use std::collections::BTreeMap;
    let mut records: BTreeMap<
        crate::runtime_values::RuntimeString,
        crate::runtime_values::RuntimeString,
    > = BTreeMap::new();
    for item in items {
        let RolloutItem::Event(event) = item else {
            continue;
        };
        if event.event != "tool_result_budget" {
            continue;
        }
        let Some(crate::runtime_values::RuntimeJson::Object(payload)) = &event.payload else {
            continue;
        };
        let Some(crate::runtime_values::RuntimeJson::Array(entries)) = payload.get("records")
        else {
            continue;
        };
        for entry in entries {
            let crate::runtime_values::RuntimeJson::Array(pair) = entry else {
                continue;
            };
            if pair.len() != 2 {
                continue;
            }
            let (
                crate::runtime_values::RuntimeJson::String(id),
                crate::runtime_values::RuntimeJson::String(body),
            ) = (&pair[0], &pair[1])
            else {
                continue;
            };
            records.insert(id.clone(), body.clone());
        }
    }
    records.into_iter().collect()
}

/// Apply a single compaction record to the running projection.
///
/// Source-aware snapshots keep typed answers not covered by the summarized source.
/// Legacy exact snapshots (`replacement_history`) win; otherwise the `replaced_range` is
/// dropped and the summary is substituted as a `compact_boundary` message at the
/// range's start position. Indices are clamped so a malformed range can never
/// panic or splice outside the array.
fn apply_compaction(history: Vec<Message>, item: &CompactedItem) -> Vec<Message> {
    if let Some(replacement) = &item.replacement_history {
        return match &item.source_message_ids {
            Some(source) => {
                preserve_uncompacted_question_answers(replacement.clone(), &history, source)
            }
            None => replacement.clone(),
        };
    }

    let (raw_start, raw_end) = item.replaced_range;
    let start = clamp_index(raw_start, 0, history.len());
    let end = clamp_index(raw_end, start, history.len());

    let summary_message = Message::CompactBoundary(CompactBoundaryMessage {
        native_compaction: None,
        uuid: compaction_uuid(item),
        summary: item.summary.clone(),
    });

    let mut next: Vec<Message> = history[..start].to_vec();
    next.push(summary_message);
    next.extend_from_slice(&history[end..]);
    next
}

/// Keep only typed answers that were not part of the compacted source. An answer
/// can be enqueued durably before it reaches live history, so both live history
/// replacement and rollout projection must apply this same UUID watermark.
pub(crate) fn preserve_uncompacted_question_answers(
    mut replacement: Vec<Message>,
    previous: &[Message],
    source_message_ids: &[RuntimeString],
) -> Vec<Message> {
    let source: HashSet<_> = source_message_ids.iter().collect();
    let mut retained: HashSet<_> = replacement
        .iter()
        .map(|message| match message {
            Message::User(message) => message.uuid.clone(),
            Message::Assistant(message) => message.uuid.clone(),
            Message::ToolResult(message) => message.uuid.clone(),
            Message::System(message) => message.uuid.clone(),
            Message::CompactBoundary(message) => message.uuid.clone(),
        })
        .collect();
    for message in previous {
        if let Message::User(user) = message {
            if user.question_answer.is_some()
                && !source.contains(&user.uuid)
                && retained.insert(user.uuid.clone())
            {
                replacement.push(message.clone());
            }
        }
    }
    replacement
}

/// Derive a stable synthetic uuid for the substituted summary message. Resume is
/// deterministic, so the same compaction record always projects to the same
/// boundary message id (handy for diffing / idempotent re-projection). The RAW
/// (unclamped) range is used, so the id reflects the record as written.
fn compaction_uuid(item: &CompactedItem) -> crate::runtime_values::RuntimeString {
    let (start, end) = item.replaced_range;
    format!("compact-{start}-{end}").into()
}

/// Clamp a stored range endpoint into `[lo, hi]`. The persisted value is
/// already integral and non-negative (the schema enforces it); clamping keeps a
/// malformed range from splicing outside the projection.
fn clamp_index(value: u64, lo: usize, hi: usize) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX).clamp(lo, hi)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::RuntimeString;
    use crate::schemas::content::{ContentBlock, TextBlock};
    use crate::schemas::message::{AssistantMessage, UserMessage};
    use crate::schemas::rollout::{CheckpointItem, EventItem, RollbackItem};

    fn cp(hash: Option<&str>) -> RolloutItem {
        RolloutItem::Checkpoint(CheckpointItem {
            hash: hash.map(RuntimeString::from),
        })
    }

    fn rb(num_turns: u32) -> RolloutItem {
        RolloutItem::Rollback(RollbackItem { num_turns })
    }

    fn user(id: &str) -> Message {
        Message::User(UserMessage {
            question_answer: None,
            uuid: id.into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: "x".into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_meta: None,
        })
    }

    fn asst(id: &str) -> Message {
        Message::Assistant(AssistantMessage {
            uuid: id.into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: "x".into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            model: "m".into(),
            usage: None,
        })
    }

    fn item(message: Message) -> RolloutItem {
        RolloutItem::Message { message }
    }

    fn msg() -> RolloutItem {
        item(user("u"))
    }

    /// The uuid of any message variant.
    fn uuid(m: &Message) -> &crate::runtime_values::RuntimeString {
        match m {
            Message::User(x) => &x.uuid,
            Message::Assistant(x) => &x.uuid,
            Message::ToolResult(x) => &x.uuid,
            Message::System(x) => &x.uuid,
            Message::CompactBoundary(x) => &x.uuid,
        }
    }

    fn uuids(msgs: &[Message]) -> Vec<crate::runtime_values::RuntimeString> {
        msgs.iter().map(|m| uuid(m).clone()).collect()
    }

    // project_checkpoints (rewind ledger reconstruction)

    #[test]
    fn replays_checkpoint_items_in_order_ignoring_non_checkpoint_records() {
        assert_eq!(
            project_checkpoints(&[cp(Some("a")), msg(), cp(Some("b")), msg(), cp(None)]),
            vec![Some("a".to_string()), Some("b".to_string()), None]
        );
    }

    #[test]
    fn truncates_the_last_n_entries_on_a_rollback_in_lockstep_with_its_turns() {
        // Three checkpoints, then a rollback of 2 → only the first survives.
        assert_eq!(
            project_checkpoints(&[cp(Some("a")), cp(Some("b")), cp(Some("c")), rb(2)]),
            vec![Some("a".to_string())]
        );
    }

    #[test]
    fn clamps_an_over_large_rollback_to_empty_never_negative_length() {
        assert_eq!(
            project_checkpoints(&[cp(Some("a")), rb(5)]),
            Vec::<Option<String>>::new()
        );
    }

    #[test]
    fn returns_an_empty_ledger_for_a_rollout_that_never_checkpointed() {
        assert_eq!(
            project_checkpoints(&[msg(), msg()]),
            Vec::<Option<String>>::new()
        );
    }

    // project_for_prompt (model-visible history reconstruction)

    #[test]
    fn projects_messages_and_ignores_metadata_records() {
        let items = vec![
            RolloutItem::Event(EventItem {
                event: "turn_started".into(),
                turn_id: None,
                payload: None,
            }),
            item(user("u1")),
            cp(Some("h")),
            item(user("u2")),
        ];
        assert_eq!(
            uuids(&project_for_prompt(&items)),
            vec![RuntimeString::from("u1"), RuntimeString::from("u2")]
        );
    }

    #[test]
    fn compaction_substitutes_a_boundary_summary_for_the_replaced_range() {
        let items = vec![
            item(user("u1")),
            item(user("u2")),
            item(user("u3")),
            RolloutItem::Compacted(CompactedItem {
                approval_parent_history: None,
                summary: "s".into(),
                replaced_range: (0, 2),
                source_message_ids: None,
                replacement_history: None,
            }),
        ];
        let projected = project_for_prompt(&items);
        assert_eq!(
            uuids(&projected),
            vec![
                RuntimeString::from("compact-0-2"),
                RuntimeString::from("u3")
            ]
        );
        let Message::CompactBoundary(boundary) = &projected[0] else {
            panic!("expected a compact_boundary message");
        };
        assert_eq!(boundary.summary, "s");
    }

    #[test]
    fn replacement_history_snapshot_wins_over_the_range() {
        let items = vec![
            item(user("u1")),
            item(user("u2")),
            RolloutItem::Compacted(CompactedItem {
                approval_parent_history: None,
                summary: "s".into(),
                replaced_range: (0, 1),
                source_message_ids: None,
                replacement_history: Some(vec![user("r1")]),
            }),
        ];
        assert_eq!(
            uuids(&project_for_prompt(&items)),
            vec![RuntimeString::from("r1")]
        );
    }

    #[test]
    fn malformed_compaction_ranges_are_clamped_not_fatal() {
        // An out-of-bounds range degrades to a best-effort projection: nothing is
        // dropped, the boundary is appended, and its uuid keeps the RAW range.
        let items = vec![
            item(user("u1")),
            item(user("u2")),
            RolloutItem::Compacted(CompactedItem {
                approval_parent_history: None,
                summary: "s".into(),
                replaced_range: (5, 99),
                source_message_ids: None,
                replacement_history: None,
            }),
        ];
        assert_eq!(
            uuids(&project_for_prompt(&items)),
            vec![
                RuntimeString::from("u1"),
                RuntimeString::from("u2"),
                RuntimeString::from("compact-5-99"),
            ]
        );
    }

    #[test]
    fn rollback_drops_the_last_user_turns_on_replay() {
        let items = vec![
            item(user("u1")),
            item(asst("a1")),
            item(user("u2")),
            item(asst("a2")),
            rb(1),
        ];
        assert_eq!(
            uuids(&project_for_prompt(&items)),
            vec![RuntimeString::from("u1"), RuntimeString::from("a1")]
        );
    }
}
