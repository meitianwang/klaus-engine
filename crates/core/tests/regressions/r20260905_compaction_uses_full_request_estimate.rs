//! Defect: proactive compaction repeatedly accepted tiny or empty microcompaction
//! passes even though the actual request remained near the context limit. Tool-
//! heavy sessions then refilled immediately and hit the rapid-refill breaker,
//! leaving the conversation unable to continue.
//! Root cause: the compactor ignored the caller's prefix-aware full-request
//! estimate and compared a messages-only estimate against the global context
//! ceiling, omitting system prompts, tool schemas, and protected messages.
//! Fixed by: 2026-09-05 compaction recovery work item.

use klaus_agent_core::context::compact::engine::Compactor;
use klaus_agent_core::context::compact::policy::preflight_threshold;
use klaus_agent_core::context::compact::{create_compactor, CompactorOptions};
use klaus_agent_core::context::tokens::estimate_messages_tokens;
use klaus_agent_core::runtime_values::{RuntimeJson, RuntimeObject, RuntimeString};
use klaus_agent_core::schemas::config::default_config;
use klaus_agent_core::schemas::content::{ContentBlock, OutputBlock, TextBlock, ToolUseBlock};
use klaus_agent_core::schemas::message::{
    AssistantMessage, Message, ToolResultMessage, Usage, UserMessage,
};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    CallModel, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn user(uuid: &str, text: &str) -> Message {
    Message::User(UserMessage {
        question_answer: None,
        uuid: uuid.into(),
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

fn assistant_text(uuid: &str, text: &str) -> Message {
    Message::Assistant(AssistantMessage {
        uuid: uuid.into(),
        content: vec![ContentBlock::Text(TextBlock {
            text: text.into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })],
        model: "m".into(),
        usage: None,
    })
}

fn task_call(uuid: &str) -> Message {
    Message::Assistant(AssistantMessage {
        uuid: uuid.into(),
        content: vec![ContentBlock::ToolUse(ToolUseBlock {
            id: format!("tu-{uuid}").into(),
            name: "task".into(),
            input: RuntimeJson::Object(RuntimeObject::new()),
            tool_kind: None,
            thought_signature: None,
        })],
        model: "m".into(),
        usage: None,
    })
}

fn task_result(uuid: &str, tool_use_id: &str, text: &str) -> Message {
    Message::ToolResult(ToolResultMessage {
        uuid: uuid.into(),
        tool_use_id: tool_use_id.into(),
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

fn summary_model(calls: Arc<AtomicUsize>) -> CallModel {
    Box::new(
        move |_request: ModelRequest, _signal: AbortSignal| -> ModelStream {
            calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(futures::stream::iter(vec![
                ModelStreamEvent::TextDelta {
                    text: "## Goals\nContinue the task.".into(),
                },
                ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::EndTurn,
                    usage: Usage {
                        input_tokens: 1,
                        output_tokens: 1,
                        cache_read_input_tokens: 0,
                        cache_creation_input_tokens: 0,
                        total_tokens: None,
                    },
                },
            ]))
        },
    )
}

#[tokio::test]
async fn prefix_pressure_requires_a_complete_summary() {
    let messages = vec![
        user("u0", "start"),
        task_call("a0"),
        task_result("r0", "tu-a0", &"x".repeat(1_200)),
        assistant_text("a1", &"y".repeat(1_200)),
        user("u1", "next"),
        assistant_text("a2", "one"),
        user("u2", "continue"),
        assistant_text("a3", "two"),
        user("u3", "finish"),
        assistant_text("a4", "three"),
    ];

    let mut config = default_config();
    config.context_window = 1_000;
    config.output_reserve_tokens = 0;
    config.max_output_tokens = Some(100);
    config.compact_buffer_tokens = 150;
    config.compact_threshold = Some(0.85);

    let messages_only = estimate_messages_tokens(&messages);
    let ceiling = preflight_threshold(&config);
    assert!(
        (messages_only as f64) <= ceiling,
        "the message body alone must be below the automatic boundary: {messages_only} vs {ceiling}"
    );

    let request_estimate = ceiling as u64 + 1;
    let calls = Arc::new(AtomicUsize::new(0));
    let compactor = create_compactor(CompactorOptions {
        call_model: summary_model(Arc::clone(&calls)),
        model: RuntimeString::from("m"),
        max_output_tokens: None,
        uuid: Box::new(|| "fixed-id".to_string()),
    });

    let result = compactor
        .maybe_compact(messages, request_estimate, &config, None)
        .await;

    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the full-request estimate must trigger the summary even when the body is smaller"
    );
    assert!(result.compacted);
    assert!(matches!(
        result.messages.last(),
        Some(Message::CompactBoundary(_))
    ));
}
