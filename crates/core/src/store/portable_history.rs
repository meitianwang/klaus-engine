//! Recover portable context from the source of surviving native checkpoints.

use std::collections::{HashMap, HashSet};

use crate::runtime_values::RuntimeString;
use crate::schemas::content::{ContentBlock, OutputBlock, TextBlock};
use crate::schemas::message::{CompactBoundaryMessage, Message};
use crate::schemas::rollout::RolloutItem;
use crate::store::projection::project_for_prompt;

const MAX_NATIVE_DEPTH: usize = 128;

/// Expand only native boundaries in the effective history supplied by the caller.
/// Snapshots contain complete raw rollouts, current first and then bounded ancestors.
/// Ancestors resolve referenced checkpoints; their latest suffix is never appended.
/// Ordinary text summaries remain summaries, and the caller's effective projection
/// remains authoritative for rollback and messages admitted after compaction.
///
/// A native display summary is not its source. Missing or cyclic source evidence
/// fails explicitly instead of silently passing an incomplete transcript onward.
/// Tool pairing is deliberately left to the ordinary request normalization path.
pub(crate) fn recover_portable_history(
    messages: &[Message],
    snapshots: &[Vec<RolloutItem>],
) -> Result<Vec<Message>, RuntimeString> {
    let mut recovery = Recovery {
        snapshots,
        visiting: HashSet::new(),
        recovered: HashMap::new(),
    };
    let expanded = recovery.expand(messages)?;
    let mut portable = Vec::with_capacity(expanded.len());
    for mut message in expanded {
        if sanitize_message(&mut message) {
            portable.push(message);
        }
    }
    if !messages.is_empty() && portable.is_empty() {
        return Err("The stored context contains only opaque provider state; portable source messages are unavailable.".into());
    }
    Ok(portable)
}

struct Recovery<'a> {
    snapshots: &'a [Vec<RolloutItem>],
    visiting: HashSet<RuntimeString>,
    recovered: HashMap<RuntimeString, Vec<Message>>,
}

impl Recovery<'_> {
    fn expand(&mut self, messages: &[Message]) -> Result<Vec<Message>, RuntimeString> {
        let mut replacements = HashMap::new();
        let mut restored = HashSet::new();
        for (index, message) in messages.iter().enumerate() {
            let Message::CompactBoundary(boundary) = message else {
                continue;
            };
            if boundary.native_compaction.is_none() {
                continue;
            }
            let source = self.boundary_source(boundary)?;
            for original in &source {
                restored.insert(identity(original));
            }
            replacements.insert(index, source);
        }

        let mut result = Vec::new();
        let mut emitted = HashSet::new();
        for (index, message) in messages.iter().enumerate() {
            if let Some(source) = replacements.remove(&index) {
                for original in source {
                    if emitted.insert(identity(&original)) {
                        result.push(original);
                    }
                }
            } else if !restored.contains(&identity(message)) {
                // Retained user copies can precede the boundary and can contain
                // truncated text. Restore the originals at the boundary, in source
                // order, rather than keeping the first copy encountered here.
                result.push(message.clone());
            }
        }
        Ok(result)
    }

    fn boundary_source(
        &mut self,
        boundary: &CompactBoundaryMessage,
    ) -> Result<Vec<Message>, RuntimeString> {
        if self.visiting.contains(&boundary.uuid) {
            return Err(
                "Native checkpoint recovery found a cycle in the stored source history.".into(),
            );
        }
        if let Some(messages) = self.recovered.get(&boundary.uuid) {
            return Ok(messages.clone());
        }
        if self.visiting.len() >= MAX_NATIVE_DEPTH {
            return Err(
                "Native checkpoint source history exceeds the portable recovery depth limit."
                    .into(),
            );
        }
        self.visiting.insert(boundary.uuid.clone());
        let result = self
            .source_before_record(boundary)
            .and_then(|source| self.expand(&source));
        self.visiting.remove(&boundary.uuid);
        if let Ok(messages) = &result {
            self.recovered
                .insert(boundary.uuid.clone(), messages.clone());
        }
        result
    }

    fn source_before_record(
        &self,
        boundary: &CompactBoundaryMessage,
    ) -> Result<Vec<Message>, RuntimeString> {
        for snapshot in self.snapshots {
            for (index, item) in snapshot.iter().enumerate() {
                let RolloutItem::Compacted(compacted) = item else {
                    continue;
                };
                let Some(replacement) = &compacted.replacement_history else {
                    continue;
                };
                if !replacement.iter().any(|message| {
                    matches!(message, Message::CompactBoundary(candidate) if candidate == boundary)
                }) {
                    continue;
                }
                let Some(source_ids) = &compacted.source_message_ids else {
                    continue;
                };
                if source_ids.contains(&boundary.uuid) {
                    // Later repair snapshots may retain this same checkpoint.
                    // They are not the record which created its source boundary.
                    continue;
                }
                if source_ids.is_empty() {
                    continue;
                }
                let source: HashSet<_> = source_ids.iter().cloned().collect();
                let previous = project_for_prompt(&snapshot[..index]);
                let selected: Vec<_> = previous
                    .into_iter()
                    .filter(|message| source.contains(message_uuid(message)))
                    .collect();
                let found: HashSet<_> = selected.iter().map(message_uuid).cloned().collect();
                if source.iter().all(|id| found.contains(id)) {
                    return Ok(selected);
                }
                // An inherited checkpoint may appear in a child's snapshot
                // without its original prefix. Only a complete originating
                // record in an ancestor can supply that missing evidence.
            }
        }
        Err("The native checkpoint's complete source messages are unavailable in the stored rollout history; its display summary cannot be used as portable context.".into())
    }
}

fn message_uuid(message: &Message) -> &RuntimeString {
    match message {
        Message::User(message) => &message.uuid,
        Message::Assistant(message) => &message.uuid,
        Message::ToolResult(message) => &message.uuid,
        Message::System(message) => &message.uuid,
        Message::CompactBoundary(message) => &message.uuid,
    }
}

// User display records intentionally share a UUID with their user message.
fn identity(message: &Message) -> (u8, RuntimeString) {
    let kind = match message {
        Message::User(_) => 0,
        Message::Assistant(_) => 1,
        Message::ToolResult(_) => 2,
        Message::System(_) => 3,
        Message::CompactBoundary(_) => 4,
    };
    (kind, message_uuid(message).clone())
}

fn sanitize_message(message: &mut Message) -> bool {
    match message {
        Message::User(user) => {
            sanitize_blocks(&mut user.content);
            !user.content.is_empty() || user.question_answer.is_some()
        }
        Message::Assistant(assistant) => {
            sanitize_blocks(&mut assistant.content);
            !assistant.content.is_empty()
        }
        Message::ToolResult(result) => {
            sanitize_output(&mut result.content);
            result.native_tool_output = None;
            result.tool_kind = None;
            if let Some(blocks) = &mut result.extra_user_blocks {
                sanitize_blocks(blocks);
            }
            true
        }
        Message::System(_) | Message::CompactBoundary(_) => true,
    }
}

fn sanitize_blocks(blocks: &mut Vec<ContentBlock>) {
    blocks.retain_mut(|block| match block {
        ContentBlock::Text(text) => {
            sanitize_text(text);
            true
        }
        ContentBlock::Thinking(thinking) => {
            if thinking.text.is_empty() {
                return false;
            }
            *block = ContentBlock::Text(TextBlock {
                text: thinking.text.clone(),
                citations: None,
                harness_envelope: None,
                message_id: thinking.message_id.clone(),
                phase: None,
            });
            true
        }
        ContentBlock::RedactedThinking(_) => false,
        ContentBlock::ToolUse(call) => {
            call.thought_signature = None;
            call.tool_kind = None;
            true
        }
        ContentBlock::ToolResult(result) => {
            sanitize_output(&mut result.content);
            true
        }
        ContentBlock::WebSearchCall(search) => {
            let mut text = RuntimeString::from("Web search: ").concat(&search.query);
            for result in &search.results {
                text = text
                    .concat(&RuntimeString::from("\n"))
                    .concat(&result.title)
                    .concat(&RuntimeString::from(" — "))
                    .concat(&result.url);
            }
            if let Some(error) = &search.error {
                text = text.concat(&RuntimeString::from("\n")).concat(error);
            }
            *block = ContentBlock::Text(TextBlock {
                text,
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            });
            true
        }
        ContentBlock::Image(_) | ContentBlock::Document(_) | ContentBlock::Media(_) => true,
    });
}

fn sanitize_text(text: &mut TextBlock) {
    if let Some(citations) = &mut text.citations {
        for citation in citations {
            citation.encrypted_index = None;
        }
    }
}

fn sanitize_output(blocks: &mut [OutputBlock]) {
    for block in blocks {
        if let OutputBlock::Text(text) = block {
            sanitize_text(text);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::questions::QuestionAnswer;
    use crate::runtime_values::{RuntimeJson, RuntimeMap};
    use crate::schemas::content::{
        Citation, NativeSearchResult, RedactedThinkingBlock, ThinkingBlock, ToolResultBlock,
        ToolUseBlock, WebSearchCallBlock,
    };
    use crate::schemas::message::{
        AssistantMessage, NativeCompactionCheckpoint, SystemMessage, SystemSubtype,
        ToolResultMessage, UserMessage,
    };
    use crate::schemas::rollout::{CompactedItem, RollbackItem};

    fn text(value: &str) -> TextBlock {
        TextBlock {
            text: value.into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        }
    }

    fn user(id: &str) -> Message {
        Message::User(UserMessage {
            uuid: id.into(),
            question_answer: None,
            content: vec![ContentBlock::Text(text(id))],
            is_meta: None,
        })
    }

    fn assistant(id: &str) -> Message {
        Message::Assistant(AssistantMessage {
            uuid: id.into(),
            content: vec![ContentBlock::Text(text(id))],
            model: "test-model".into(),
            usage: None,
        })
    }

    fn native(id: &str) -> Message {
        Message::CompactBoundary(CompactBoundaryMessage {
            uuid: id.into(),
            summary: "Display label, not a complete source transcript".into(),
            native_compaction: Some(NativeCompactionCheckpoint {
                source: "test-source".into(),
                auth_scope: "test-scope".into(),
                model: "test-model".into(),
                comp_hash: None,
                response_id: id.into(),
                encrypted_content: "opaque-checkpoint".into(),
                retained_input: Vec::new(),
                retained_input_metadata: Vec::new(),
            }),
        })
    }

    fn row(message: Message) -> RolloutItem {
        RolloutItem::Message { message }
    }

    fn compact(source: &[Message], replacement: Vec<Message>) -> RolloutItem {
        RolloutItem::Compacted(CompactedItem {
            summary: "Display summary".into(),
            replaced_range: (0, source.len() as u64),
            replacement_history: Some(replacement),
            source_message_ids: Some(source.iter().map(message_uuid).cloned().collect()),
            approval_parent_history: None,
        })
    }

    fn ids(messages: &[Message]) -> Vec<RuntimeString> {
        messages.iter().map(message_uuid).cloned().collect()
    }

    fn answer(id: &str) -> Message {
        let Message::User(mut message) = user(id) else {
            unreachable!()
        };
        message.question_answer = Some(Box::new(QuestionAnswer {
            request_id: "question".into(),
            turn_id: "turn".into(),
            tool_call_id: "question-call".into(),
            client_response_id: "response".into(),
            questions: Vec::new(),
            answers: RuntimeMap::default(),
        }));
        Message::User(message)
    }

    #[test]
    fn recursively_recovers_nested_native_sources_in_original_order() {
        let first = vec![user("u1"), assistant("a1")];
        let n1 = native("n1");
        let second = vec![n1.clone(), user("u2"), assistant("a2")];
        let n2 = native("n2");
        let snapshot = vec![
            row(first[0].clone()),
            row(first[1].clone()),
            compact(&first, vec![n1]),
            row(second[1].clone()),
            row(second[2].clone()),
            compact(&second, vec![n2]),
            row(user("fresh")),
        ];
        let effective = project_for_prompt(&snapshot);
        let actual = recover_portable_history(&effective, &[snapshot]).unwrap();
        assert_eq!(
            ids(&actual),
            ["u1", "a1", "u2", "a2", "fresh"].map(RuntimeString::from)
        );
    }

    #[test]
    fn ordinary_text_compaction_remains_a_summary_inside_native_source() {
        let summary = Message::CompactBoundary(CompactBoundaryMessage {
            uuid: "text-boundary".into(),
            summary: "Earlier work was summarized here.".into(),
            native_compaction: None,
        });
        let source = vec![summary.clone(), user("u2"), assistant("a2")];
        let snapshot = vec![
            row(user("retired")),
            compact(&[user("retired")], vec![summary]),
            row(source[1].clone()),
            row(source[2].clone()),
            compact(&source, vec![native("native")]),
        ];
        assert_eq!(
            recover_portable_history(&project_for_prompt(&snapshot), &[snapshot]).unwrap(),
            source
        );
    }

    #[test]
    fn rollback_before_source_capture_and_after_checkpoint_keeps_deletions() {
        let source = vec![user("kept"), assistant("kept-reply")];
        let snapshot = vec![
            row(source[0].clone()),
            row(source[1].clone()),
            row(user("deleted-before")),
            row(assistant("deleted-before-reply")),
            RolloutItem::Rollback(RollbackItem { num_turns: 1 }),
            compact(&source, vec![native("native")]),
            row(user("deleted-after")),
            row(assistant("deleted-after-reply")),
            RolloutItem::Rollback(RollbackItem { num_turns: 1 }),
            row(user("fresh")),
        ];
        let actual = recover_portable_history(&project_for_prompt(&snapshot), &[snapshot]).unwrap();
        assert_eq!(
            ids(&actual),
            ["kept", "kept-reply", "fresh"].map(RuntimeString::from)
        );
    }

    #[test]
    fn removed_native_boundary_is_not_recovered_from_raw_records() {
        let source = vec![user("retired")];
        let snapshot = vec![
            row(source[0].clone()),
            compact(&source, vec![user("retired"), native("native")]),
            RolloutItem::Rollback(RollbackItem { num_turns: 1 }),
            row(user("fresh")),
        ];
        let effective = project_for_prompt(&snapshot);
        assert_eq!(effective, vec![user("fresh")]);
        assert_eq!(
            recover_portable_history(&effective, &[snapshot]).unwrap(),
            effective
        );
    }

    #[test]
    fn retained_user_copies_do_not_duplicate_or_reorder_recovered_turns() {
        let source = vec![user("u1"), assistant("a1"), user("u2"), assistant("a2")];
        let Message::User(mut truncated) = user("u1") else {
            unreachable!()
        };
        truncated.content = vec![ContentBlock::Text(text("truncated copy"))];
        let snapshot = vec![
            row(source[0].clone()),
            row(source[1].clone()),
            row(source[2].clone()),
            row(source[3].clone()),
            compact(
                &source,
                vec![Message::User(truncated), user("u2"), native("native")],
            ),
            row(user("fresh")),
        ];
        let actual = recover_portable_history(&project_for_prompt(&snapshot), &[snapshot]).unwrap();
        let mut expected = source;
        expected.push(user("fresh"));
        assert_eq!(actual, expected);
    }

    #[test]
    fn fresh_typed_answer_outside_source_survives_exactly_once() {
        let source = vec![user("u1"), assistant("a1")];
        let fresh_answer = answer("answer");
        let snapshot = vec![
            row(source[0].clone()),
            row(source[1].clone()),
            row(fresh_answer.clone()),
            compact(&source, vec![native("native")]),
            row(user("fresh")),
        ];
        let actual = recover_portable_history(&project_for_prompt(&snapshot), &[snapshot]).unwrap();
        assert_eq!(
            actual,
            vec![
                source[0].clone(),
                source[1].clone(),
                fresh_answer,
                user("fresh")
            ]
        );
    }

    #[test]
    fn ancestor_resolves_inherited_boundary_without_appending_its_latest_suffix() {
        let source = vec![user("parent-input"), assistant("parent-reply")];
        let boundary = native("parent-checkpoint");
        let parent = vec![
            row(source[0].clone()),
            row(source[1].clone()),
            compact(&source, vec![boundary.clone()]),
            row(user("parent-later-input")),
            row(assistant("parent-later-reply")),
        ];
        let child = vec![row(boundary), row(user("child-input"))];
        let actual =
            recover_portable_history(&project_for_prompt(&child), &[child, parent]).unwrap();
        assert_eq!(
            ids(&actual),
            ["parent-input", "parent-reply", "child-input"].map(RuntimeString::from)
        );
    }

    #[test]
    fn later_repair_record_does_not_replace_checkpoint_origin() {
        let source = vec![user("u1"), assistant("a1")];
        let boundary = native("native");
        let snapshot = vec![
            row(source[0].clone()),
            row(source[1].clone()),
            compact(&source, vec![boundary.clone()]),
            compact(std::slice::from_ref(&boundary), vec![boundary.clone()]),
        ];
        assert_eq!(
            recover_portable_history(&project_for_prompt(&snapshot), &[snapshot]).unwrap(),
            source
        );
    }

    #[test]
    fn missing_source_id_and_opaque_only_import_fail_explicitly() {
        let boundary = native("native");
        let missing = vec![compact(&[user("missing")], vec![boundary.clone()])];
        assert!(recover_portable_history(std::slice::from_ref(&boundary), &[missing]).is_err());
        assert!(recover_portable_history(
            std::slice::from_ref(&boundary),
            &[vec![row(boundary.clone())]]
        )
        .is_err());
    }

    #[test]
    fn missing_watermark_does_not_substitute_display_summary() {
        let boundary = native("native");
        let snapshot = vec![
            row(user("original")),
            RolloutItem::Compacted(CompactedItem {
                summary: "Display label".into(),
                replaced_range: (0, 1),
                replacement_history: Some(vec![boundary.clone()]),
                source_message_ids: None,
                approval_parent_history: None,
            }),
        ];
        assert!(recover_portable_history(&[boundary], &[snapshot]).is_err());
    }

    #[test]
    fn cyclic_checkpoint_dependencies_fail_explicitly() {
        let a = native("a");
        let b = native("b");
        let snapshot = vec![
            row(b.clone()),
            compact(std::slice::from_ref(&b), vec![a.clone()]),
            compact(std::slice::from_ref(&a), vec![b.clone()]),
        ];
        let error = recover_portable_history(&[a], &[snapshot]).unwrap_err();
        assert!(error.try_to_utf8().unwrap().contains("cycle"));
    }

    #[test]
    fn user_display_record_sharing_uuid_is_not_deduplicated_with_user() {
        let source = vec![
            Message::System(SystemMessage {
                uuid: "u1".into(),
                text: "Display input".into(),
                subtype: SystemSubtype::UserDisplay,
            }),
            user("u1"),
            assistant("a1"),
        ];
        let snapshot = vec![
            row(source[0].clone()),
            row(source[1].clone()),
            row(source[2].clone()),
            compact(&source, vec![native("native")]),
        ];
        assert_eq!(
            recover_portable_history(&project_for_prompt(&snapshot), &[snapshot]).unwrap(),
            source
        );
    }

    fn cited_text() -> TextBlock {
        let mut block = text("Readable finding");
        block.citations = Some(vec![Citation {
            url: "https://example.test/source".into(),
            title: Some("Source title".into()),
            cited_text: Some("Source excerpt".into()),
            start_index: Some(0),
            end_index: Some(8),
            encrypted_index: Some("opaque-citation".into()),
        }]);
        block
    }

    #[test]
    fn sanitizes_opaque_tokens_and_preserves_readable_content_and_tool_ids() {
        let mut blocks = vec![
            ContentBlock::Thinking(ThinkingBlock {
                text: "Readable reasoning".into(),
                message_id: Some("thinking-item".into()),
                signature: Some("opaque-signature".into()),
                encrypted_content: Some("opaque-reasoning".into()),
            }),
            ContentBlock::RedactedThinking(RedactedThinkingBlock {
                data: "opaque".into(),
            }),
            ContentBlock::ToolUse(ToolUseBlock {
                id: "call-id".into(),
                name: "read".into(),
                input: RuntimeJson::Null,
                tool_kind: None,
                thought_signature: Some("opaque-tool-signature".into()),
            }),
            ContentBlock::WebSearchCall(WebSearchCallBlock {
                id: "search-id".into(),
                query: "query".into(),
                action: None,
                results: vec![NativeSearchResult {
                    title: "Readable hit".into(),
                    url: "https://example.test/source".into(),
                    encrypted_token: Some("opaque-search".into()),
                    page_age: Some("today".into()),
                }],
                error: None,
            }),
            ContentBlock::Text(cited_text()),
            ContentBlock::ToolResult(ToolResultBlock {
                tool_use_id: "nested-id".into(),
                content: vec![OutputBlock::Text(cited_text())],
                is_error: None,
            }),
        ];
        let original = Message::Assistant(AssistantMessage {
            uuid: "assistant".into(),
            content: blocks.clone(),
            model: "test-model".into(),
            usage: None,
        });
        blocks.remove(1);
        let mut reasoning = text("Readable reasoning");
        reasoning.message_id = Some("thinking-item".into());
        blocks[0] = ContentBlock::Text(reasoning);
        let ContentBlock::ToolUse(call) = &mut blocks[1] else {
            unreachable!()
        };
        call.thought_signature = None;
        blocks[2] = ContentBlock::Text(text(
            "Web search: query\nReadable hit — https://example.test/source",
        ));
        let ContentBlock::Text(text) = &mut blocks[3] else {
            unreachable!()
        };
        text.citations.as_mut().unwrap()[0].encrypted_index = None;
        let ContentBlock::ToolResult(nested) = &mut blocks[4] else {
            unreachable!()
        };
        let OutputBlock::Text(text) = &mut nested.content[0] else {
            unreachable!()
        };
        text.citations.as_mut().unwrap()[0].encrypted_index = None;
        let result = Message::ToolResult(ToolResultMessage {
            uuid: "result-id".into(),
            tool_use_id: "call-id".into(),
            content: vec![OutputBlock::Text(cited_text())],
            is_error: false,
            tool_kind: None,
            native_tool_output: Some(RuntimeJson::String("opaque-output".into())),
            output: None,
            extra_user_blocks: Some(vec![ContentBlock::Text(cited_text())]),
        });
        let portable = recover_portable_history(&[original, result], &[]).unwrap();
        let Message::Assistant(actual) = &portable[0] else {
            unreachable!()
        };
        assert_eq!(actual.content, blocks);
        let Message::ToolResult(actual) = &portable[1] else {
            unreachable!()
        };
        assert_eq!(actual.uuid, "result-id");
        assert_eq!(actual.tool_use_id, "call-id");
        assert!(actual.native_tool_output.is_none());
        let OutputBlock::Text(output) = &actual.content[0] else {
            unreachable!()
        };
        assert_eq!(output.text, "Readable finding");
        assert!(output.citations.as_ref().unwrap()[0]
            .encrypted_index
            .is_none());
        let ContentBlock::Text(extra) = &actual.extra_user_blocks.as_ref().unwrap()[0] else {
            unreachable!()
        };
        assert!(extra.citations.as_ref().unwrap()[0]
            .encrypted_index
            .is_none());
    }

    #[test]
    fn opaque_only_reasoning_does_not_become_portable_text() {
        let message = Message::Assistant(AssistantMessage {
            uuid: "opaque".into(),
            model: "test-model".into(),
            usage: None,
            content: vec![
                ContentBlock::Thinking(ThinkingBlock {
                    text: RuntimeString::default(),
                    message_id: None,
                    signature: None,
                    encrypted_content: Some("opaque-reasoning".into()),
                }),
                ContentBlock::RedactedThinking(RedactedThinkingBlock {
                    data: "opaque".into(),
                }),
            ],
        });
        assert!(recover_portable_history(&[message], &[]).is_err());
        assert!(recover_portable_history(&[], &[]).unwrap().is_empty());
    }
}
