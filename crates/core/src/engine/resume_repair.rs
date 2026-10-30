//! Repair a transcript loaded for resume so it satisfies the provider's
//! tool-call invariant. A sub-agent killed mid-tool (or `kill -9` before the
//! tool_result was synthesized) persists a transcript whose last assistant
//! message carries a `tool_use` with no matching `tool_result`. Sending that
//! verbatim makes the next API call 400, so the orphaned tool_use is stripped
//! before re-running.

use crate::runtime_values::RuntimeString;
use crate::schemas::content::{ContentBlock, OutputBlock, TextBlock};
use crate::schemas::message::{AssistantMessage, Message, ToolResultMessage};
use std::collections::HashSet;

/// Build a provider-only history view. Missing executions are explicitly aborted;
/// real results and the source transcript are never rewritten or re-executed.
pub fn normalize_tool_history(messages: &[Message]) -> Vec<Message> {
    normalize_tool_history_owned(messages.to_vec())
}

pub(crate) fn normalize_tool_history_owned(messages: Vec<Message>) -> Vec<Message> {
    // Embedded user tool-result blocks are typed input attachments, such as
    // mention expansion. Preserve their body, order and authority verbatim;
    // executable call outputs use standalone ToolResult messages.
    let expanded = messages;
    let calls: HashSet<_> = expanded
        .iter()
        .filter_map(|message| match message {
            Message::Assistant(assistant) => Some(&assistant.content),
            _ => None,
        })
        .flatten()
        .filter_map(|block| match block {
            ContentBlock::ToolUse(call) => Some(call.id.clone()),
            _ => None,
        })
        .collect();
    let results: HashSet<_> = expanded
        .iter()
        .filter_map(|message| match message {
            Message::ToolResult(result) => Some(result.tool_use_id.clone()),
            _ => None,
        })
        .collect();
    let mut out = Vec::with_capacity(expanded.len());
    for message in expanded {
        match &message {
            Message::ToolResult(result) if !calls.contains(&result.tool_use_id) => continue,
            Message::Assistant(assistant) => {
                let mut missing = Vec::new();
                for block in &assistant.content {
                    let ContentBlock::ToolUse(call) = block else {
                        continue;
                    };
                    if results.contains(&call.id) {
                        continue;
                    }
                    missing.push(Message::ToolResult(ToolResultMessage {
                        uuid: assistant
                            .uuid
                            .concat(&"::aborted-tool-result:".into())
                            .concat(&call.id),
                        tool_use_id: call.id.clone(),
                        content: vec![OutputBlock::Text(TextBlock {
                            text: "aborted".into(),
                            citations: None,
                            harness_envelope: None,
                            message_id: None,
                            phase: None,
                        })],
                        is_error: true,
                        tool_kind: call.tool_kind,
                        native_tool_output: None,
                        output: None,
                        extra_user_blocks: None,
                    }));
                }
                out.push(message);
                out.extend(missing);
                continue;
            }
            _ => {}
        }
        out.push(message);
    }
    out
}

/// Drop `tool_use` blocks that have no matching `tool_result` message anywhere in
/// the list (so every surviving tool_use is resolved). An assistant message left
/// with no blocks is removed entirely. Resolved tool_uses — and their results —
/// are preserved, so no orphaned `tool_result` is ever created.
pub fn filter_unresolved_tool_uses(messages: &[Message]) -> Vec<Message> {
    let mut resolved: HashSet<RuntimeString> = HashSet::new();
    for m in messages {
        if let Message::ToolResult(tr) = m {
            resolved.insert(tr.tool_use_id.clone());
        }
    }

    let mut out: Vec<Message> = Vec::new();
    for m in messages {
        let Message::Assistant(a) = m else {
            out.push(m.clone());
            continue;
        };
        let kept: Vec<ContentBlock> = a
            .content
            .iter()
            .filter(|b| match b {
                ContentBlock::ToolUse(tu) => resolved.contains(&tu.id),
                _ => true,
            })
            .cloned()
            .collect();
        if kept.is_empty() {
            continue; // had only unresolved tool_uses → drop the message
        }
        if kept.len() == a.content.len() {
            out.push(m.clone());
        } else {
            out.push(Message::Assistant(AssistantMessage {
                content: kept,
                ..a.clone()
            }));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::{RuntimeJson, RuntimeObject};
    use crate::schemas::content::{OutputBlock, TextBlock, ToolUseBlock};
    use crate::schemas::message::{ToolResultMessage, UserMessage};

    fn user(id: &str) -> Message {
        Message::User(UserMessage {
            question_answer: None,
            uuid: id.into(),
            content: vec![text_block(id)],
            is_meta: None,
        })
    }

    fn text_block(t: &str) -> ContentBlock {
        ContentBlock::Text(TextBlock {
            text: t.into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })
    }

    fn tool_use_block(id: &str) -> ContentBlock {
        ContentBlock::ToolUse(ToolUseBlock {
            id: id.into(),
            name: "bash".into(),
            input: RuntimeJson::Object(RuntimeObject::new()),
            tool_kind: None,
            thought_signature: None,
        })
    }

    fn assistant(uuid: &str, content: Vec<ContentBlock>) -> Message {
        Message::Assistant(AssistantMessage {
            uuid: uuid.into(),
            content,
            model: "m".into(),
            usage: None,
        })
    }

    fn tool_result(tool_use_id: &str) -> Message {
        Message::ToolResult(ToolResultMessage {
            uuid: "tr".into(),
            tool_use_id: tool_use_id.into(),
            content: vec![OutputBlock::Text(TextBlock {
                text: "ok".into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_error: false,
            tool_kind: None,
            native_tool_output: None,
            output: None,
            extra_user_blocks: None,
        })
    }

    #[test]
    fn drops_an_orphaned_tool_use_and_removes_an_emptied_assistant_message() {
        let msgs = vec![user("u1"), assistant("a1", vec![tool_use_block("t1")])];
        // t1 has no tool_result → the assistant message (only that block) is dropped whole.
        let out = filter_unresolved_tool_uses(&msgs);
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], Message::User(_)));
    }

    #[test]
    fn keeps_a_resolved_tool_use_and_its_result_unchanged() {
        let msgs = vec![
            assistant("a1", vec![tool_use_block("t1")]),
            tool_result("t1"),
        ];
        let out = filter_unresolved_tool_uses(&msgs);
        assert_eq!(out.len(), 2);
        assert_eq!(out, msgs);
    }

    #[test]
    fn strips_only_the_unresolved_block_keeping_the_rest_of_the_message() {
        let msgs = vec![
            assistant(
                "a1",
                vec![
                    text_block("thinking out loud"),
                    tool_use_block("t1"),
                    tool_use_block("t2"),
                ],
            ),
            tool_result("t1"), // resolves t1 only
        ];
        let out = filter_unresolved_tool_uses(&msgs);
        assert_eq!(out.len(), 2);
        match &out[0] {
            Message::Assistant(a) => {
                assert_eq!(a.content.len(), 2); // text + t1 kept; t2 dropped
                assert!(matches!(&a.content[0], ContentBlock::Text(_)));
                match &a.content[1] {
                    ContentBlock::ToolUse(tu) => assert_eq!(tu.id, "t1"),
                    other => panic!("expected tool_use t1, got {other:?}"),
                }
            }
            other => panic!("expected assistant message, got {other:?}"),
        }
    }

    #[test]
    fn preserves_non_assistant_messages_verbatim() {
        // Only tool_use blocks are filtered; a pre-existing tool_result is left as-is.
        let msgs = vec![user("u1"), tool_result("never-matched"), user("u2")];
        let out = filter_unresolved_tool_uses(&msgs);
        assert_eq!(out, msgs);
    }

    #[test]
    fn request_history_closes_missing_calls_without_changing_real_outputs() {
        let source = vec![
            assistant(
                "parent-response",
                vec![tool_use_block("pending"), tool_use_block("complete")],
            ),
            tool_result("complete"),
            user("runtime-guidance"),
            tool_result("orphan"),
        ];
        let before = source.clone();
        let projected = normalize_tool_history(&source);
        assert_eq!(source, before);
        assert_eq!(projected[0], source[0]);
        let Message::ToolResult(aborted) = &projected[1] else {
            panic!("missing call must receive an explicit result");
        };
        assert_eq!(aborted.tool_use_id, "pending");
        assert!(aborted.is_error);
        assert!(matches!(&aborted.content[0], OutputBlock::Text(text) if text.text == "aborted"));
        assert_eq!(projected[2], source[1]);
        assert_eq!(projected[3], source[2]);
        assert_eq!(projected.len(), 4);
        assert_eq!(normalize_tool_history(&source), projected);
        assert_eq!(normalize_tool_history(&projected), projected);
    }

    #[test]
    fn request_history_preserves_embedded_results_and_surrounding_user_text() {
        let source = vec![
            assistant("a", vec![tool_use_block("t")]),
            Message::User(UserMessage {
                question_answer: None,
                uuid: "u".into(),
                content: vec![
                    text_block("before"),
                    ContentBlock::ToolResult(crate::schemas::content::ToolResultBlock {
                        tool_use_id: "embedded-reader".into(),
                        content: vec![OutputBlock::Text(TextBlock {
                            text: "actual result".into(),
                            citations: None,
                            harness_envelope: None,
                            message_id: None,
                            phase: None,
                        })],
                        is_error: Some(false),
                    }),
                    text_block("after"),
                ],
                is_meta: Some(true),
            }),
        ];
        let projected = normalize_tool_history(&source);
        assert_eq!(projected.len(), 3);
        assert_eq!(projected[0], source[0]);
        assert!(
            matches!(&projected[1], Message::ToolResult(result) if result.tool_use_id == "t" && result.is_error)
        );
        assert_eq!(projected[2], source[1]);
        assert_eq!(normalize_tool_history(&projected), projected);
    }
}
