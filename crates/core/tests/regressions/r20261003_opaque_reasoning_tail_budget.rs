//! Defect: retained encrypted reasoning was absent from local context estimates.
//! Root cause: reasoning signatures and encrypted bodies used the same accounting rule.
//! Work item: 20261003 count opaque reasoning while preserving authoritative usage anchors.

use klaus_agent_core::context::tokens::{
    estimate_context_tokens_with_prefix, estimate_message_tokens, invalidate_context_usage,
};
use klaus_agent_core::schemas::content::{ContentBlock, ThinkingBlock};
use klaus_agent_core::schemas::message::{AssistantMessage, Message, Usage};

fn reasoning(signature: Option<&str>, encrypted: Option<&str>) -> ContentBlock {
    ContentBlock::Thinking(ThinkingBlock {
        message_id: None,
        text: "brief".into(),
        signature: signature.map(Into::into),
        encrypted_content: encrypted.map(Into::into),
    })
}

fn assistant(id: &str, content: Vec<ContentBlock>, usage: Option<Usage>) -> Message {
    Message::Assistant(AssistantMessage {
        uuid: id.into(),
        content,
        model: "opaque-budget".into(),
        usage,
    })
}

fn usage() -> Usage {
    Usage {
        input_tokens: 900,
        output_tokens: 100,
        cache_read_input_tokens: 0,
        cache_creation_input_tokens: 0,
        total_tokens: None,
    }
}

#[test]
fn both_replayed_reasoning_formats_are_accounted_without_phantom_empty_cost() {
    let visible = assistant("plain", vec![reasoning(None, None)], None);
    let signed = assistant(
        "signed",
        vec![reasoning(Some(&"S".repeat(3_000)), None)],
        None,
    );
    let encrypted = assistant(
        "encrypted",
        vec![reasoning(None, Some(&"E".repeat(3_000)))],
        None,
    );
    let empty = assistant("empty", vec![reasoning(Some(""), Some(""))], None);
    assert_eq!(estimate_message_tokens(&visible), 2);
    assert_eq!(estimate_message_tokens(&signed), 2);
    assert_eq!(estimate_message_tokens(&encrypted), 752);
    assert_eq!(
        estimate_message_tokens(&empty),
        estimate_message_tokens(&visible)
    );
    for (signature, encrypted_content) in [
        (None, None),
        (Some(""), None),
        (None, Some("")),
        (Some(""), Some("")),
    ] {
        let empty = assistant(
            "empty-body",
            vec![ContentBlock::Thinking(ThinkingBlock {
                message_id: None,
                text: "".into(),
                signature: signature.map(Into::into),
                encrypted_content: encrypted_content.map(Into::into),
            })],
            None,
        );
        assert_eq!(estimate_message_tokens(&empty), 0);
    }
}

#[test]
fn usage_anchors_cover_old_payloads_and_new_reasoning_grows_the_context() {
    let old = assistant(
        "old",
        vec![reasoning(None, Some(&"E".repeat(30_000)))],
        Some(usage()),
    );
    let mut history = vec![old];
    assert_eq!(estimate_context_tokens_with_prefix(&history, 500), 1_000);
    let next = assistant(
        "new",
        vec![reasoning(
            Some(&"S".repeat(3_000)),
            Some(&"E".repeat(3_000)),
        )],
        None,
    );
    assert_eq!(estimate_message_tokens(&next), 752);
    history.push(next.clone());
    assert_eq!(
        estimate_context_tokens_with_prefix(&history, 500),
        1_000 + estimate_message_tokens(&next)
    );
    assert_eq!(estimate_context_tokens_with_prefix(&history, 500), 1_752);
    invalidate_context_usage(&mut history);
    assert_eq!(
        estimate_context_tokens_with_prefix(&history, 500),
        8_754,
        "clearing the usage anchor restores the prefix and both encrypted payloads"
    );
}
