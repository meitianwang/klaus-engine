//! JSONL (de)serialization for rollout records, plus the persistence filter
//! ([`is_persisted_rollout_item`]) deciding which items are written to disk.

use crate::runtime_values::{runtime_codec, runtime_json, RuntimeString};
use crate::schemas::rollout::RolloutItem;

/// Serialize a rollout item to a single JSONL line (no embedded newlines).
pub fn serialize_rollout_item(item: &RolloutItem) -> String {
    let value = runtime_codec::encode_value(item).expect("RolloutItem runtime-encodes");
    runtime_json::stringify(&value)
        .try_to_utf8()
        .expect("JSON text contains only Unicode scalar code points")
}

/// Parse a single JSONL line into a [`RolloutItem`]. Tolerant by design: a
/// corrupt, truncated, or otherwise invalid line returns `None` instead of
/// erroring, so a `kill -9` mid-append never breaks resume.
pub fn parse_rollout_line(line: &str) -> Option<RolloutItem> {
    runtime_json::parse(&RuntimeString::from(line))
        .ok()
        .and_then(|value| runtime_codec::decode_value(&value).ok())
}

/// Filter seam: whether a rollout item should be written to disk. The persisted
/// set covers every kind except `checkpoint` (ThreadRolledBack is persisted
/// too); the exhaustive match forces a review if a new kind is added.
pub fn is_persisted_rollout_item(item: &RolloutItem) -> bool {
    match item {
        RolloutItem::SessionMeta { .. }
        | RolloutItem::TurnContext { .. }
        | RolloutItem::Message { .. }
        | RolloutItem::Compacted(_)
        | RolloutItem::Event(_)
        | RolloutItem::Rollback(_)
        | RolloutItem::UsageLedger(_)
        | RolloutItem::SubagentLifecycle(_)
        | RolloutItem::BackgroundProcessLifecycle(_) => true,
        RolloutItem::Checkpoint(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::content::{ContentBlock, TextBlock};
    use crate::schemas::message::{Message, UserMessage};
    use crate::schemas::rollout::{
        CompactedItem, EventItem, RollbackItem, SessionMeta, SubagentLifecycleItem,
        TurnContextSnapshot, UsageLedgerItem,
    };
    use crate::schemas::subagent_lifecycle::{
        SubagentContext, SubagentKind, SubagentLifecycleRecord, SubagentStatus,
    };
    use crate::types::permission::ExternalPermissionMode;

    fn parse_value(source: &str) -> crate::runtime_values::RuntimeJson {
        runtime_json::parse(&RuntimeString::from(source)).unwrap()
    }

    fn samples() -> Vec<RolloutItem> {
        vec![
            RolloutItem::SessionMeta {
                schema_version: 2,
                meta: SessionMeta {
                    id: "x".into(),
                    created_at: 1,
                    cwd: "/".into(),
                    model: "m".into(),
                    title: None,
                    originator: None,
                    cli_version: None,
                    source: None,
                    role: None,
                    parent_thread_id: None,
                    forked_from_id: None,
                    git: None,
                    host_metadata: None,
                },
            },
            RolloutItem::TurnContext {
                ctx: TurnContextSnapshot {
                    cwd: "/".into(),
                    model: "m".into(),
                    permission_mode: ExternalPermissionMode::Default,
                    turn_id: None,
                    started_at_ms: None,
                    approval_policy: None,
                    sandbox_mode: None,
                    approvals_reviewer: None,
                    network_allowed: None,
                    writable_roots: None,
                },
            },
            RolloutItem::Message {
                message: Message::User(UserMessage {
                    question_answer: None,
                    uuid: "u".into(),
                    content: vec![ContentBlock::Text(TextBlock {
                        text: "hi".into(),
                        citations: None,
                        harness_envelope: None,
                        message_id: None,
                        phase: None,
                    })],
                    is_meta: None,
                }),
            },
            RolloutItem::Compacted(CompactedItem {
                approval_parent_history: None,
                summary: "s".into(),
                replaced_range: (0, 1),
                source_message_ids: None,
                replacement_history: None,
            }),
            RolloutItem::Event(EventItem {
                event: "token_count".into(),
                turn_id: None,
                payload: None,
            }),
            RolloutItem::Rollback(RollbackItem { num_turns: 1 }),
            RolloutItem::UsageLedger(UsageLedgerItem {
                inference: None,
                request_id: "req-1".into(),
                assistant_message_id: "assistant-1".into(),
                model: "m".into(),
                usage: crate::schemas::message::Usage {
                    input_tokens: 1,
                    output_tokens: 2,
                    cache_read_input_tokens: 3,
                    cache_creation_input_tokens: 4,
                    total_tokens: None,
                },
                cost_usd: 0.5,
            }),
            RolloutItem::SubagentLifecycle(SubagentLifecycleItem {
                record: SubagentLifecycleRecord {
                    agent_id: "child-1".into(),
                    kind: SubagentKind::Foreground,
                    status: SubagentStatus::Running,
                    context: SubagentContext {
                        session_id: "session-1".into(),
                        parent_agent_id: None,
                        parent_thread_id: None,
                        agent_type: None,
                        cwd: "/".into(),
                        description: "work".into(),
                        depth: 1,
                        resume_from: None,
                        spawn_turn_id: None,
                    },
                    started_at_ms: 1,
                    updated_at_ms: 2,
                    completed_at_ms: None,
                    last_result: None,
                    last_error: None,
                    cancellation_requested: false,
                    model: None,
                    model_source: None,
                },
            }),
        ]
    }

    #[test]
    fn every_kind_is_persisted() {
        for item in samples() {
            assert!(is_persisted_rollout_item(&item));
            assert_eq!(
                parse_rollout_line(&serialize_rollout_item(&item)),
                Some(item)
            );
        }
    }

    #[test]
    fn round_trips_rollback_marker() {
        let item = RolloutItem::Rollback(RollbackItem { num_turns: 3 });
        let parsed = parse_rollout_line(&serialize_rollout_item(&item));
        assert_eq!(parsed, Some(item));
    }

    #[test]
    fn rejects_fractional_or_negative_num_turns() {
        assert_eq!(
            parse_rollout_line(r#"{"kind":"rollback","numTurns":1.5}"#),
            None
        );
        assert_eq!(
            parse_rollout_line(r#"{"kind":"rollback","numTurns":-1}"#),
            None
        );
    }

    #[test]
    fn rollout_parse_applies_javascript_number_semantics_before_typed_decode() {
        assert_eq!(
            parse_rollout_line(
                r#"{"kind":"event","event":"wide","payload":{"wide":9007199254740993}}"#
            ),
            Some(RolloutItem::Event(EventItem {
                event: "wide".into(),
                turn_id: None,
                payload: Some(parse_value(r#"{"wide":9007199254740992}"#)),
            }))
        );
    }

    #[test]
    fn rollout_serialization_projects_javascript_json_numbers_and_round_trips_siblings() {
        let payload = parse_value(
            r#"{
                "unsafeInteger": 9007199254740993,
                "negativeZero": -0,
                "positiveNonfinite": 1e400,
                "negativeNonfinite": -1e400,
                "sibling": {"safe": 42, "text": "unchanged", "flag": true}
            }"#,
        );
        let item = RolloutItem::Event(EventItem {
            event: "number_projection".into(),
            turn_id: Some("turn-1".into()),
            payload: Some(payload),
        });

        let line = serialize_rollout_item(&item);
        assert_eq!(
            line,
            r#"{"kind":"event","event":"number_projection","turnId":"turn-1","payload":{"unsafeInteger":9007199254740992,"negativeZero":0,"positiveNonfinite":null,"negativeNonfinite":null,"sibling":{"safe":42,"text":"unchanged","flag":true}}}"#
        );
        let reparsed = parse_rollout_line(&line).unwrap();
        assert_eq!(serialize_rollout_item(&reparsed), line);
    }

    #[test]
    fn rollout_preserves_surrogate_keys_replacement_and_literal_marker_text() {
        let source = r#"{"kind":"event","event":"\uFDD0","payload":{"\uD800":"high","\uDC00":"low","\uFFFD":"replacement","\uFDD0ud800":"marker"}}"#;
        let item = parse_rollout_line(source).expect("valid rollout event");
        let RolloutItem::Event(event) = &item else {
            panic!("expected event rollout item");
        };
        assert_eq!(
            event.payload.as_ref().unwrap().as_object().unwrap().len(),
            4
        );

        let first_wire = serialize_rollout_item(&item);
        assert_eq!(
            first_wire,
            format!(
                "{{\"kind\":\"event\",\"event\":\"﷐\",\"payload\":{{\"\\ud800\":\"high\",\"\\udc00\":\"low\",\"�\":\"replacement\",\"﷐ud800\":\"marker\"}}}}"
            )
        );
        let reparsed = parse_rollout_line(&first_wire).expect("serialized rollout reparses");
        assert_eq!(serialize_rollout_item(&reparsed), first_wire);
    }
}
