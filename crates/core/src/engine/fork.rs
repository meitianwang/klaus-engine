//! Builds full-history child seeds and identifies their historical marker.
//!
//! Children inherit the initiating step's conversation, system parts and actual
//! tool pool. Nested delegation follows the current tools, permissions and mode;
//! the inherited marker identifies history without restricting delegation.

use crate::runtime_values::RuntimeString;
use crate::schemas::content::{ContentBlock, OutputBlock, TextBlock};
use crate::schemas::message::{Message, ToolResultMessage, UserMessage};
use std::collections::HashSet;

/// Synthetic agent-type label recorded for a fork child.
pub const FORK_AGENT_TYPE: &str = "fork";

/// Historical explicit budget retained for callers that select it themselves.
pub const FORK_MAX_TURNS: u32 = 200;

/// Tag wrapping the fork directive so history and renderers can identify it.
pub const FORK_BOILERPLATE_TAG: &str = "fork-worker";

/// Identical placeholder text for every synthesized tool_result in the fork seed,
/// so all fork children off the same turn share a byte-identical request prefix
/// (only the per-child directive differs) — maximizing prompt-cache hits.
pub(crate) const FORK_PLACEHOLDER_RESULT: &str =
    "Forked — the parent handled this call; continue with your directive.";

/// True when the conversation already contains a fork directive (a user text block
/// carrying the boilerplate tag) — i.e. the current agent is itself a fork child.
/// This is history metadata and does not limit subsequent delegation.
pub fn is_in_fork_child(messages: &[Message]) -> bool {
    let needle = format!("<{FORK_BOILERPLATE_TAG}>");
    messages.iter().any(|m| match m {
        Message::User(u) => u.content.iter().any(|b| match b {
            ContentBlock::Text(t) => t.text.contains(&needle),
            _ => false,
        }),
        _ => false,
    })
}

/// The directive message body for a fork child: original instructions telling the
/// worker it inherited the context and must finish autonomously, wrapped in the
/// historical fork marker, followed by the directive.
pub fn build_fork_boilerplate(directive: &str) -> String {
    build_fork_boilerplate_runtime(RuntimeString::from(directive))
        .try_to_utf8()
        .expect("scalar fork directive produces a scalar boilerplate")
}

fn build_fork_boilerplate_runtime(directive: RuntimeString) -> RuntimeString {
    let tag = FORK_BOILERPLATE_TAG;
    // Preserve blank lines and the exact boundary before the assigned directive.
    let lines = [
        format!("<{tag}>"),
        "You are a forked worker. The full conversation above was inherited from a parent".to_string(),
        "agent. Continue as a child agent within the assigned scope.".to_string(),
        String::new(),
        "Complete the assigned directive within the inherited constraints:".to_string(),
        "- Work within its scope and the tools and permissions actually available.".to_string(),
        "- Delegate through the tools actually available and the current collaboration mode.".to_string(),
        "- Resolve discoverable facts yourself. If a consequential decision cannot be resolved, report the blocker to the parent instead of inventing authority.".to_string(),
        "- Do not commit changes unless the user's instructions authorize a commit.".to_string(),
        String::new(),
        "Return a concise, self-contained result with the evidence and remaining uncertainty needed by the parent.".to_string(),
        format!("</{tag}>"),
        String::new(),
        "Directive: ".to_string(),
    ];
    RuntimeString::from(lines.join("\n")).concat(&directive)
}

/// Build the seed transcript for a fork child: the inherited conversation, then a
/// synthesized placeholder tool_result for every UNRESOLVED tool_use in it (so the
/// spawning turn's dangling tool calls — including the fork's own — are closed
/// without dropping or duplicating any), then the directive message.
///
/// `tool_result` is a standalone message in this engine, so placeholders are
/// appended as their own messages rather than folded into a user block; the net
/// resolved-tool-use prefix is equivalent.
pub fn build_fork_transcript<D: Into<RuntimeString>>(
    conversation: &[Message],
    directive: D,
    uuid: impl Fn() -> String,
) -> Vec<Message> {
    let directive = directive.into();
    let mut resolved: HashSet<&RuntimeString> = HashSet::new();
    for m in conversation {
        if let Message::ToolResult(tr) = m {
            resolved.insert(&tr.tool_use_id);
        }
    }
    let mut unresolved_ids: Vec<RuntimeString> = Vec::new();
    for m in conversation {
        if let Message::Assistant(a) = m {
            for block in &a.content {
                if let ContentBlock::ToolUse(tu) = block {
                    if !resolved.contains(&tu.id) {
                        unresolved_ids.push(tu.id.clone());
                    }
                }
            }
        }
    }
    // Preserve the inherited transcript, then close each dangling tool_use with an
    // identical placeholder (uuid() called once per placeholder, in order), and
    // finally append the directive message (its uuid() drawn last).
    let mut out: Vec<Message> = conversation.to_vec();
    for tool_use_id in unresolved_ids {
        out.push(Message::ToolResult(ToolResultMessage {
            uuid: uuid().into(),
            tool_use_id,
            content: vec![OutputBlock::Text(TextBlock {
                text: FORK_PLACEHOLDER_RESULT.into(),
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
        }));
    }
    out.push(Message::User(UserMessage {
        question_answer: None,
        uuid: uuid().into(),
        content: vec![ContentBlock::Text(TextBlock {
            text: build_fork_boilerplate_runtime(directive),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })],
        is_meta: None,
    }));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::content::ToolUseBlock;
    use crate::schemas::message::AssistantMessage;
    use std::cell::Cell;

    fn user_text(text: &str) -> Message {
        Message::User(UserMessage {
            question_answer: None,
            uuid: "u".into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: text.into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_meta: None,
        })
    }

    fn asst_tool_use(id: &str) -> Message {
        Message::Assistant(AssistantMessage {
            uuid: "a".into(),
            content: vec![ContentBlock::ToolUse(ToolUseBlock {
                id: id.into(),
                name: "bash".into(),
                input: crate::runtime_values::RuntimeJson::Object(
                    crate::runtime_values::RuntimeObject::new(),
                ),
                tool_kind: None,
                thought_signature: None,
            })],
            model: "m".into(),
            usage: None,
        })
    }

    fn tool_result(tool_use_id: &str) -> Message {
        Message::ToolResult(ToolResultMessage {
            uuid: "t".into(),
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
    fn is_in_fork_child_detects_the_boilerplate_tag() {
        assert!(!is_in_fork_child(&[user_text("hello")]));
        // A user text block carrying the directive boilerplate trips the guard.
        assert!(is_in_fork_child(&[user_text(&build_fork_boilerplate(
            "do the thing"
        ))]));
        // Only USER text blocks count: the tag inside an assistant message must not trip it.
        assert!(!is_in_fork_child(&[Message::Assistant(AssistantMessage {
            uuid: "a".into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: "<fork-worker>".into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            model: "m".into(),
            usage: None,
        })]));
    }

    #[test]
    fn boilerplate_wraps_the_directive_in_the_guard_tag() {
        let b = build_fork_boilerplate("ship it");
        assert!(b.starts_with("<fork-worker>\n"));
        assert!(b.contains("</fork-worker>"));
        assert!(b.ends_with("Directive: ship it"));
        assert!(b.contains("You are a forked worker."));
        // The report template keeps its two-space indentation.
        assert!(b.contains("Return a concise, self-contained result"));
    }

    #[test]
    fn transcript_appends_placeholders_for_unresolved_tool_uses_then_directive() {
        let counter = Cell::new(0u32);
        let uuid = || {
            let n = counter.get() + 1;
            counter.set(n);
            format!("id{n}")
        };
        // A turn where the assistant makes two calls; only the first is resolved.
        let convo = vec![
            user_text("go"),
            asst_tool_use("call-a"),
            tool_result("call-a"),
            asst_tool_use("call-b"), // dangling
        ];
        let out = build_fork_transcript(&convo, "finish up", uuid);
        // original 4 + 1 placeholder (only call-b) + 1 directive
        assert_eq!(out.len(), 6);
        match &out[4] {
            Message::ToolResult(tr) => {
                assert_eq!(tr.tool_use_id, "call-b");
                assert_eq!(tr.uuid, "id1");
                assert!(!tr.is_error);
                assert_eq!(
                    tr.content,
                    vec![OutputBlock::Text(TextBlock {
                        text:
                            "Forked — the parent handled this call; continue with your directive."
                                .into(),
                        citations: None,
                        harness_envelope: None,
                        message_id: None,
                        phase: None,
                    })]
                );
            }
            other => panic!("expected tool_result placeholder, got {other:?}"),
        }
        match &out[5] {
            Message::User(u) => {
                assert_eq!(u.uuid, "id2"); // directive uuid drawn after the placeholder
                match &u.content[0] {
                    ContentBlock::Text(t) => {
                        assert!(t.text.starts_with("<fork-worker>"));
                        assert!(t.text.ends_with("Directive: finish up"));
                    }
                    other => panic!("expected text block, got {other:?}"),
                }
            }
            other => panic!("expected directive user message, got {other:?}"),
        }
    }

    #[test]
    fn transcript_with_no_dangling_calls_just_appends_the_directive() {
        let uuid = || "x".to_string();
        let convo = vec![user_text("hi"), asst_tool_use("c"), tool_result("c")];
        let out = build_fork_transcript(&convo, "d", uuid);
        // No placeholders — every tool_use is resolved — only the directive is appended.
        assert_eq!(out.len(), 4);
        assert!(matches!(out[3], Message::User(_)));
    }
}
