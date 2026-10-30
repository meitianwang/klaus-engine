//! Defect: the context-size estimate that gates compaction counted messages
//! only, so the request's fixed prefix — the rendered system prompt plus every
//! tool schema — was invisible to it. On a large tool surface that is tens of
//! thousands of tokens missing, and the blind spot opened at the worst possible
//! moment: compaction clears every `usage` anchor, so the turn immediately after
//! compacting measured itself without its own prefix, concluded it had room, and
//! sent a request that could overflow the window.
//! Root cause: `estimate_context_tokens` fell back to a messages-only structural
//! estimate whenever no assistant message carried authoritative `usage`, and
//! nothing folded the per-request prefix back in for that case.
//! Fixed by: commit ca43230 (prefix-aware context estimate).

use klaus_agent_core::context::compact::policy::trigger_threshold;
use klaus_agent_core::context::tokens::{
    estimate_context_tokens, estimate_context_tokens_with_prefix, estimate_prefix_tokens,
    invalidate_context_usage,
};
use klaus_agent_core::runtime_values::{RuntimeObject, RuntimeString};
use klaus_agent_core::schemas::config::default_config;
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{AssistantMessage, Message, Usage, UserMessage};
use klaus_agent_core::types::provider::{SystemPromptPart, ToolSpec, ToolSpecKind};

fn user(text: &str) -> Message {
    Message::User(UserMessage {
        question_answer: None,
        uuid: "u1".into(),
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

fn assistant_with_usage(input_tokens: u64) -> Message {
    Message::Assistant(AssistantMessage {
        uuid: "a1".into(),
        content: vec![ContentBlock::Text(TextBlock {
            text: "done".into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })],
        model: "m".into(),
        usage: Some(Usage {
            input_tokens,
            output_tokens: 0,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
            total_tokens: None,
        }),
    })
}

/// A system prompt and tool surface whose combined cost dwarfs the conversation.
fn fat_prefix() -> (Vec<SystemPromptPart>, Vec<ToolSpec>) {
    let system = vec![SystemPromptPart {
        name: "identity".into(),
        text: RuntimeString::from("s".repeat(40_000).as_str()),
        cacheable: None,
        cache_scope: None,
    }];
    let tools = (0..20)
        .map(|index| ToolSpec {
            name: format!("tool_{index}").into(),
            description: RuntimeString::from("d".repeat(2_000).as_str()),
            input_schema: RuntimeObject::new().into(),
            kind: ToolSpecKind::Function,
            format: None,
        })
        .collect();
    (system, tools)
}

#[test]
fn the_estimate_counts_the_request_prefix_once_the_compaction_anchor_is_gone() {
    let (system, tools) = fat_prefix();
    let prefix = estimate_prefix_tokens(&system, &tools);
    assert!(
        prefix > 20_000,
        "the fixture prefix must be big enough to matter: {prefix}"
    );

    // The state compaction leaves behind: a rewritten history whose `usage`
    // anchors have all been cleared.
    let mut history = vec![user(&"c".repeat(400)), assistant_with_usage(150_000)];
    invalidate_context_usage(&mut history);

    let messages_only = estimate_context_tokens(&history);
    let with_prefix = estimate_context_tokens_with_prefix(&history, prefix);

    // Pre-fix the gating estimate was `messages_only`, which omits the entire
    // prefix — the defect.
    assert_eq!(
        with_prefix,
        messages_only + prefix,
        "an anchorless estimate must carry the prefix it is about to re-send"
    );
    assert!(
        with_prefix > messages_only,
        "the prefix must move the number the compaction gate reads"
    );
}

#[test]
fn a_live_usage_anchor_still_accounts_for_the_prefix_exactly_once() {
    // The complement of the fix: an authoritative usage already covers the whole
    // request, so folding the prefix in again would over-count and compact early.
    let (system, tools) = fat_prefix();
    let prefix = estimate_prefix_tokens(&system, &tools);
    let history = vec![user(&"c".repeat(400)), assistant_with_usage(150_000)];

    assert_eq!(
        estimate_context_tokens_with_prefix(&history, prefix),
        estimate_context_tokens(&history),
        "a usage anchor must not have the prefix added on top of it"
    );
}

#[test]
fn the_prefix_is_what_decides_whether_the_compaction_gate_fires() {
    // The behavioural consequence of the defect: with a window sized so the trigger
    // sits between the two estimates, the messages-only number reports headroom and
    // compaction never runs, while the prefix-aware number crosses the line.
    let (system, tools) = fat_prefix();
    let prefix = estimate_prefix_tokens(&system, &tools);
    let mut history = vec![user(&"c".repeat(4_000))];
    history.push(assistant_with_usage(1));
    invalidate_context_usage(&mut history);

    let messages_only = estimate_context_tokens(&history);
    let with_prefix = estimate_context_tokens_with_prefix(&history, prefix);

    let mut config = default_config();
    config.output_reserve_tokens = 0;
    config.max_output_tokens = Some(0);
    config.compact_threshold = None;
    // Place the trigger strictly between the two estimates.
    let target_trigger = (messages_only + with_prefix) / 2;
    config.context_window = target_trigger + 1_000;
    config.compact_buffer_tokens = 1_000;

    let trigger = trigger_threshold(&config);
    assert!(
        (messages_only as f64) <= trigger,
        "messages-only must look like it still has headroom: {messages_only} vs {trigger}"
    );
    assert!(
        (with_prefix as f64) > trigger,
        "the prefix-aware estimate must trip the gate: {with_prefix} vs {trigger}"
    );
}
