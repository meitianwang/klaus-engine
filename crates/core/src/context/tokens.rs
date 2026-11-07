//! Context-size accounting (no local
//! tokenizer, no `count_tokens` call on the hot path).
//!
//! The compaction trigger needs one number: "how many tokens are in the context
//! right now". The source of truth is the authoritative `usage` block the
//! provider returned on the most recent response. [`estimate_context_tokens`]
//! anchors on it and only ROUGH-ESTIMATES the few messages appended since, so
//! the expensive prefix is never re-estimated and multimodal content is not
//! mis-measured by counting base64 characters.

use crate::runtime_values::{runtime_codec, runtime_json, RuntimeEncode, RuntimeString};
use crate::schemas::content::{ContentBlock, OutputBlock};
use crate::schemas::message::{Message, Usage};
use crate::types::provider::{SystemPromptPart, ToolSpec};

/// Approximate UTF-8 bytes per token when provider usage is unavailable.
const BYTES_PER_TOKEN: u64 = 4;
/// Native media uses a stable estimate until provider usage supplies an anchor.
pub(crate) const MEDIA_TOKENS: u64 = 2_000;

/// Authoritative context size from a usage report, honoring each protocol's
/// native convention:
///
///  - OpenAI: trust the server's `total_tokens` directly.
///  - Anthropic: no server total; the four fields are disjoint, so sum all
///    four.
pub fn tokens_from_usage(u: &Usage) -> u64 {
    if let Some(total) = u.total_tokens {
        return total;
    }
    u.input_tokens + u.output_tokens + u.cache_read_input_tokens + u.cache_creation_input_tokens
}

fn rounded_tokens(len: usize) -> u64 {
    (len as u64).div_ceil(BYTES_PER_TOKEN)
}

fn estimate_text(s: &RuntimeString) -> u64 {
    rounded_tokens(s.to_node_utf8().len())
}

fn estimate_structured<T: RuntimeEncode + ?Sized>(v: &T) -> u64 {
    let length = runtime_codec::encode_value(v)
        .map(|value| runtime_json::stringify(&value).to_node_utf8().len())
        .unwrap_or_default();
    rounded_tokens(length)
}

/// Rough token estimate for one tool_result output block.
fn estimate_output_block(b: &OutputBlock) -> u64 {
    match b {
        OutputBlock::Image(_) => MEDIA_TOKENS,
        OutputBlock::Text(t) => estimate_text(&t.text),
    }
}

/// Rough token estimate for one content block (structure-aware).
fn estimate_content_block(b: &ContentBlock) -> u64 {
    match b {
        ContentBlock::Text(t) => estimate_text(&t.text),
        ContentBlock::Thinking(t) => {
            estimate_text(&t.text)
                + t.encrypted_content
                    .as_ref()
                    .map(estimate_text)
                    .unwrap_or_default()
        }
        ContentBlock::RedactedThinking(t) => estimate_text(&t.data),
        ContentBlock::Image(_) | ContentBlock::Document(_) | ContentBlock::Media(_) => MEDIA_TOKENS,
        ContentBlock::ToolUse(tu) => {
            let input_length = runtime_json::stringify(&tu.input).to_node_utf8().len();
            rounded_tokens(tu.name.to_node_utf8().len().saturating_add(input_length))
        }
        ContentBlock::ToolResult(tr) => tr.content.iter().map(estimate_output_block).sum(),
        other => estimate_structured(other),
    }
}

/// Rough token estimate for a single message (structure-aware, no tokenizer).
pub fn estimate_message_tokens(m: &Message) -> u64 {
    match m {
        Message::User(u) => u.content.iter().map(estimate_content_block).sum(),
        Message::Assistant(a) => a.content.iter().map(estimate_content_block).sum(),
        Message::ToolResult(tr) => {
            // Count the tool_result content AND any sibling media (a native PDF
            // on extra_user_blocks) — both land on the same user turn.
            let body: u64 = tr.content.iter().map(estimate_output_block).sum();
            let extra: u64 = tr
                .extra_user_blocks
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .map(estimate_content_block)
                .sum();
            body + extra
        }
        Message::System(s) if s.subtype == crate::schemas::message::SystemSubtype::UserDisplay => 0,
        Message::System(s) => estimate_text(&s.text),
        Message::CompactBoundary(c) => c
            .native_compaction
            .as_ref()
            .map(|checkpoint| checkpoint.estimated_context_tokens())
            .unwrap_or_else(|| estimate_text(&c.summary)),
    }
}

/// Rough token estimate for a list of messages (purely structural).
pub fn estimate_messages_tokens(messages: &[Message]) -> u64 {
    messages.iter().map(estimate_message_tokens).sum()
}

/// Current context size. Walk back to the most recent assistant
/// message carrying authoritative `usage` and add a rough estimate for ONLY the
/// messages appended since. Falls back to a full structural estimate when no
/// response has landed yet.
///
/// This form knows nothing about the request's fixed prefix, so it is only honest
/// while a usage anchor exists. Callers deciding whether to compact must use
/// [`estimate_context_tokens_with_prefix`].
pub fn estimate_context_tokens(messages: &[Message]) -> u64 {
    estimate_context_tokens_with_prefix(messages, 0)
}

/// Current context size, including the request's FIXED PREFIX when there is no
/// usage anchor to account for it.
///
/// An authoritative `usage` already covers the entire request that produced it —
/// system prompt and tool schemas included — so when an anchor exists the prefix
/// must NOT be added again. With no anchor the estimate is messages-only, and the
/// prefix is invisible: on a large tool surface that is tens of thousands of
/// tokens missing. That gap opens exactly when it is most dangerous, because
/// compaction clears every anchor ([`invalidate_context_usage`]) — so the turn
/// right after compacting, when the context is closest to the ceiling, would
/// otherwise measure itself without its own prefix and decide it has room.
pub fn estimate_context_tokens_with_prefix(messages: &[Message], prefix_tokens: u64) -> u64 {
    for i in (0..messages.len()).rev() {
        if let Message::Assistant(a) = &messages[i] {
            if let Some(usage) = &a.usage {
                return tokens_from_usage(usage) + estimate_messages_tokens(&messages[i + 1..]);
            }
        }
    }
    prefix_tokens + estimate_messages_tokens(messages)
}

/// Rough token cost of the request's fixed prefix: the rendered system prompt and
/// the tool schemas the model is shown. Both are re-sent on every request and both
/// are counted by the provider, so an estimate that omits them understates the
/// context by their whole size.
pub fn estimate_prefix_tokens(system: &[SystemPromptPart], tools: &[ToolSpec]) -> u64 {
    let system_tokens: u64 = system.iter().map(|part| estimate_text(&part.text)).sum();
    let tool_tokens: u64 = tools
        .iter()
        .map(|tool| {
            estimate_text(&tool.name)
                + estimate_text(&tool.description)
                + estimate_structured(&tool.input_schema)
        })
        .sum();
    system_tokens + tool_tokens
}

/// Provider usage describes the exact request that produced an assistant message.
/// Once compaction rewrites that request history, those anchors are stale; clear
/// them until the next real model response supplies a new authoritative usage.
pub fn invalidate_context_usage(messages: &mut [Message]) {
    for message in messages {
        if let Message::Assistant(assistant) = message {
            assistant.usage = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::{RuntimeJson, RuntimeObject};
    use crate::schemas::content::{ImageBlock, TextBlock, ToolUseBlock};
    use crate::schemas::message::{AssistantMessage, ToolResultMessage, UserMessage};

    fn usage(input: u64, output: u64, total: Option<u64>) -> Usage {
        Usage {
            input_tokens: input,
            output_tokens: output,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
            total_tokens: total,
        }
    }

    fn user(text: impl Into<RuntimeString>) -> Message {
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

    fn assistant(text: &str, u: Option<Usage>) -> Message {
        Message::Assistant(AssistantMessage {
            uuid: "a".into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: text.into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            model: "m".into(),
            usage: u,
        })
    }

    #[test]
    fn tokens_from_usage_sums_or_trusts_total() {
        let u = Usage {
            input_tokens: 1000,
            output_tokens: 200,
            cache_read_input_tokens: 50,
            cache_creation_input_tokens: 30,
            total_tokens: None,
        };
        assert_eq!(tokens_from_usage(&u), 1280);
        let u = usage(994, 200, Some(1200));
        assert_eq!(tokens_from_usage(&u), 1200);
    }

    #[test]
    fn text_counted_by_chars_over_four() {
        assert_eq!(estimate_message_tokens(&user("x".repeat(40))), 10);
    }

    #[test]
    fn text_estimator_counts_encoded_bytes_for_astral_text() {
        assert_eq!(estimate_message_tokens(&user("😀😀😀")), 3);
    }

    #[test]
    fn text_estimator_counts_encoded_replacement_bytes_for_lone_surrogates() {
        let value =
            runtime_json::parse(&RuntimeString::from(r#""\ud800\ud800\ud800\ud800\ud800""#))
                .expect("exact runtime JSON parser must preserve lone surrogates");
        let text = value.as_str().expect("fixture is a JSON string");

        assert_eq!(estimate_message_tokens(&user(text.clone())), 4);
    }

    #[test]
    fn structured_estimator_counts_encoded_json_bytes() {
        let mut object = RuntimeObject::new();
        object.insert("emoji", RuntimeJson::from("😀😀"));
        assert_eq!(estimate_structured(&RuntimeJson::Object(object)), 5);
    }

    #[test]
    fn image_flat_cost_not_base64_length() {
        let m = Message::User(UserMessage {
            question_answer: None,
            uuid: "u1".into(),
            content: vec![ContentBlock::Image(ImageBlock {
                mime_type: "image/png".into(),
                base64: "A".repeat(500_000).into(),
                detail: None,
                source_path: None,
            })],
            is_meta: None,
        });
        let n = estimate_message_tokens(&m);
        assert_eq!(n, MEDIA_TOKENS);
    }

    #[test]
    fn tool_use_counts_name_and_input() {
        let mut input = RuntimeObject::new();
        input.insert("file", RuntimeJson::from("/x".repeat(30)));
        let m = Message::Assistant(AssistantMessage {
            uuid: "a1".into(),
            content: vec![ContentBlock::ToolUse(ToolUseBlock {
                id: "t1".into(),
                name: "read".into(),
                input: RuntimeJson::Object(input),
                tool_kind: None,
                thought_signature: None,
            })],
            model: "m".into(),
            usage: None,
        });
        assert!(estimate_message_tokens(&m) > 0);
    }

    #[test]
    fn tool_result_body_counted() {
        let m = Message::ToolResult(ToolResultMessage {
            uuid: "r1".into(),
            tool_use_id: "t1".into(),
            content: vec![OutputBlock::Text(TextBlock {
                text: "y".repeat(80).into(),
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
        });
        assert_eq!(estimate_message_tokens(&m), 20);
    }

    fn tool_result(id: &str, text: &str) -> Message {
        Message::ToolResult(ToolResultMessage {
            uuid: format!("r-{id}").into(),
            tool_use_id: id.into(),
            content: vec![OutputBlock::Text(TextBlock {
                text: text.into(),
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

    fn system_part(name: &str, text: &str) -> SystemPromptPart {
        SystemPromptPart {
            name: name.into(),
            text: text.into(),
            cacheable: None,
            cache_scope: None,
        }
    }

    fn tool_spec(name: &str, description: &str) -> ToolSpec {
        ToolSpec {
            name: name.into(),
            description: description.into(),
            input_schema: RuntimeObject::new().into(),
            kind: crate::types::provider::ToolSpecKind::Function,
            format: None,
        }
    }

    #[test]
    fn the_prefix_is_counted_when_no_usage_anchor_accounts_for_it() {
        // Compaction clears every anchor, so this is the state the turn right after
        // compacting measures itself in: without the prefix it would look far
        // emptier than the request actually is.
        let messages = vec![user("a".repeat(40))];
        let bare = estimate_context_tokens(&messages);
        assert_eq!(
            estimate_context_tokens_with_prefix(&messages, 7_000),
            bare + 7_000
        );
    }

    #[test]
    fn the_prefix_is_not_counted_twice_on_top_of_a_usage_anchor() {
        // An authoritative usage already covers the whole request, prefix included.
        let messages = vec![
            user("a".repeat(40)),
            assistant("b", Some(usage(5_000, 100, None))),
        ];
        assert_eq!(
            estimate_context_tokens_with_prefix(&messages, 7_000),
            estimate_context_tokens(&messages)
        );
    }

    #[test]
    fn the_prefix_estimate_covers_the_system_prompt_and_every_tool_schema() {
        let system = vec![system_part("identity", &"s".repeat(40))];
        let tools = vec![tool_spec("read", &"d".repeat(40))];
        // 40/4 system + 4/4 name + 40/4 description + "{}" rounded at 4 chars/token.
        assert_eq!(estimate_prefix_tokens(&system, &tools), 10 + 1 + 10 + 1);
        assert_eq!(estimate_prefix_tokens(&[], &[]), 0);
    }

    #[test]
    fn every_result_of_a_parallel_tool_round_lands_in_the_tail_estimate() {
        // One response carries all parallel tool_use blocks in a SINGLE assistant
        // record, so the anchored tail is every result that followed it. Nothing may
        // fall between the anchor and the tail.
        let messages = vec![
            user("go"),
            Message::Assistant(AssistantMessage {
                uuid: "a1".into(),
                content: vec![
                    ContentBlock::ToolUse(ToolUseBlock {
                        id: "t1".into(),
                        name: "read".into(),
                        input: RuntimeObject::new().into(),
                        tool_kind: None,
                        thought_signature: None,
                    }),
                    ContentBlock::ToolUse(ToolUseBlock {
                        id: "t2".into(),
                        name: "read".into(),
                        input: RuntimeObject::new().into(),
                        tool_kind: None,
                        thought_signature: None,
                    }),
                ],
                model: "m".into(),
                usage: Some(usage(1_000, 10, None)),
            }),
            tool_result("t1", &"x".repeat(400)),
            tool_result("t2", &"y".repeat(800)),
        ];
        // anchor 1010 + 400/4 + 800/4
        assert_eq!(estimate_context_tokens(&messages), 1010 + 100 + 200);
    }

    #[test]
    fn an_undecodable_image_has_a_stable_media_estimate() {
        let m = Message::User(UserMessage {
            question_answer: None,
            uuid: "u".into(),
            content: vec![ContentBlock::Image(ImageBlock {
                mime_type: "image/png".into(),
                base64: "Z".repeat(200_000).into(),
                detail: None,
                source_path: None,
            })],
            is_meta: None,
        });
        assert_eq!(estimate_message_tokens(&m), MEDIA_TOKENS);
    }

    #[test]
    fn context_falls_back_to_full_estimate_without_usage() {
        let messages = vec![user("a".repeat(40)), assistant(&"b".repeat(40), None)];
        assert_eq!(
            estimate_context_tokens(&messages),
            estimate_messages_tokens(&messages)
        );
    }

    #[test]
    fn context_anchors_on_last_usage_plus_tail() {
        let messages = vec![
            user("old".repeat(100)),
            assistant("answer", Some(usage(5000, 100, None))),
            Message::ToolResult(ToolResultMessage {
                uuid: "r1".into(),
                tool_use_id: "t1".into(),
                content: vec![OutputBlock::Text(TextBlock {
                    text: "z".repeat(40).into(),
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
            }),
        ];
        assert_eq!(estimate_context_tokens(&messages), 5110);
    }

    #[test]
    fn context_uses_most_recent_usage() {
        let messages = vec![
            assistant("first", Some(usage(1000, 0, None))),
            user("more"),
            assistant("second", Some(usage(9000, 50, None))),
        ];
        assert_eq!(estimate_context_tokens(&messages), 9050);
    }
}
