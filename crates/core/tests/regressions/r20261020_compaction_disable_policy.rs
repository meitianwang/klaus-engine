//! Defect: runtime disable switches did not cover every compaction entry point.
//! Root cause: manual, overflow and background paths lacked one shared runtime gate.
//! Work item: keep global disable separate from automatic-only policy without stored knobs.

use klaus_agent_core::context::compact::engine::Compactor as NativeCompactor;
use klaus_agent_core::context::compact::full_compact::PartialCompactionDirection;
use klaus_agent_core::context::compact::window::{
    bind_context_runtime_environment, DISABLE_AUTO_COMPACT_ENV, DISABLE_COMPACT_ENV,
};
use klaus_agent_core::context::compact::{
    create_compactor, CompactionNoopReason, CompactorOptions,
};
use klaus_agent_core::runtime_process::RuntimeEnv;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::config::{default_config, ResolvedConfig};
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{AssistantMessage, Message, Usage, UserMessage};
use klaus_agent_core::types::provider::{CallModel, ModelStopReason, ModelStreamEvent};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn environment(key: &str, value: &str) -> RuntimeEnv {
    let mut env = RuntimeEnv::new();
    env.insert(key.into(), value.into());
    env
}
fn text(value: impl Into<RuntimeString>) -> ContentBlock {
    ContentBlock::Text(TextBlock {
        text: value.into(),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    })
}
fn history() -> Vec<Message> {
    vec![
        Message::User(UserMessage {
            uuid: "task".into(),
            content: vec![text("Keep the active task.")],
            is_meta: None,
            question_answer: None,
        }),
        Message::Assistant(AssistantMessage {
            uuid: "old".into(),
            content: vec![text("x".repeat(160_000))],
            model: "fake-model".into(),
            usage: None,
        }),
        Message::User(UserMessage {
            uuid: "followup".into(),
            content: vec![text("Continue.")],
            is_meta: None,
            question_answer: None,
        }),
        Message::Assistant(AssistantMessage {
            uuid: "new".into(),
            content: vec![text("y".repeat(160_000))],
            model: "fake-model".into(),
            usage: None,
        }),
    ]
}
fn config() -> ResolvedConfig {
    let mut config = default_config();
    config.model = "fake-model".into();
    config.context_window = 200_000;
    config.auto_compact_window = Some(100_000);
    config.max_output_tokens = Some(1_000);
    config.model_max_output_tokens = Some(40_000);
    config.output_reserve_tokens = 20_000;
    config.speculative_compaction = true;
    config.early_microcompact = true;
    config.idle_microcompact = true;
    config.loop_detection = false;
    config.max_retries = 0;
    config
}
#[test]
fn disable_environment_is_truthy_scoped_and_never_a_persisted_preference() {
    assert!(!default_config().compaction_disabled);
    for raw in ["1", "TRUE", " yes ", "On"] {
        let mut global = config();
        bind_context_runtime_environment(&mut global, &environment(DISABLE_COMPACT_ENV, raw));
        assert!(global.compaction_disabled);
        assert!(!global.auto_compaction);
        assert!(serde_json::to_value(&global)
            .unwrap()
            .get("compactionDisabled")
            .is_none());
        let mut automatic = config();
        bind_context_runtime_environment(
            &mut automatic,
            &environment(DISABLE_AUTO_COMPACT_ENV, raw),
        );
        assert!(!automatic.compaction_disabled);
        assert!(!automatic.auto_compaction);
    }
    for key in [DISABLE_COMPACT_ENV, DISABLE_AUTO_COMPACT_ENV] {
        for raw in ["0", "false", "no", "off", "", "maybe"] {
            let mut enabled = config();
            bind_context_runtime_environment(&mut enabled, &environment(key, raw));
            assert!(!enabled.compaction_disabled);
            assert!(enabled.auto_compaction);
            enabled.auto_compaction = false;
            bind_context_runtime_environment(&mut enabled, &environment(key, raw));
            assert!(
                !enabled.auto_compaction,
                "a false disable flag must not enable automatic compaction"
            );
        }
    }
}
fn summary_model(calls: Arc<AtomicUsize>) -> CallModel {
    Box::new(move |_, _| {
        calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(futures::stream::iter(vec![
            ModelStreamEvent::TextDelta {
                text: "Keep the active task and pending validation.".into(),
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
    })
}
#[tokio::test]
async fn global_disable_preserves_every_native_entry_while_auto_only_allows_manual() {
    let calls = Arc::new(AtomicUsize::new(0));
    let compactor = create_compactor(CompactorOptions {
        call_model: summary_model(calls.clone()),
        model: "fake-model".into(),
        max_output_tokens: Some(1_000),
        uuid: Box::new(|| uuid::Uuid::new_v4().to_string()),
    });
    let mut disabled = config();
    bind_context_runtime_environment(&mut disabled, &environment(DISABLE_COMPACT_ENV, "1"));
    for result in [
        compactor.manual_compact(history(), &disabled, None).await,
        compactor
            .partial_compact(
                history(),
                2,
                PartialCompactionDirection::From,
                &disabled,
                None,
            )
            .await,
        compactor
            .maybe_compact(history(), 80_000, &disabled, None)
            .await,
        compactor
            .threshold_compact(history(), &disabled, None)
            .await,
        compactor.reactive_compact(history(), &disabled, None).await,
    ] {
        assert!(!result.compacted);
        assert_eq!(result.reason, Some(CompactionNoopReason::CompactorDisabled));
        assert_eq!(result.messages, history());
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let mut automatic = config();
    bind_context_runtime_environment(&mut automatic, &environment(DISABLE_AUTO_COMPACT_ENV, "1"));
    let manual = compactor.manual_compact(history(), &automatic, None).await;
    assert!(
        manual.compacted,
        "automatic-only disable leaves manual compaction available"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
