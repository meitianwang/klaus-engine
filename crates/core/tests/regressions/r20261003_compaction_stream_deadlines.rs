//! Defect: a silent manual summary did not settle at its configured idle boundary.
//! Root cause: the public manual entry point failed to use current stream settings.
//! Work item: 20261003 retain source history when the summary stream is idle.

use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{AssistantMessage, Message, UserMessage};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::ModelRequest;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

struct SilentProvider {
    info: klaus_agent_core::types::provider::ProviderInfo,
    calls: Arc<AtomicUsize>,
}

impl klaus_agent_core::types::provider::ModelProvider for SilentProvider {
    fn info(&self) -> &klaus_agent_core::types::provider::ProviderInfo {
        &self.info
    }

    fn capabilities(
        &self,
        _: &klaus_agent_core::runtime_values::RuntimeString,
    ) -> klaus_agent_core::types::provider::ProviderCapabilities {
        klaus_agent_core::types::provider::ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: false,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: 200_000,
            max_output_tokens: 8192,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn stream(
        &self,
        _: ModelRequest,
        _: AbortSignal,
    ) -> klaus_agent_core::types::provider::ModelStream {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(futures::stream::pending())
    }
}

#[tokio::test]
async fn public_session_manual_compaction_obeys_current_config_and_keeps_history() {
    use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
    use klaus_agent_core::runtime_process::RuntimeProcessContext;

    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let root =
        std::env::temp_dir().join(format!("klaus-summary-deadline-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
    options.cwd = Some(root.to_string_lossy().into_owned());
    let calls = Arc::new(AtomicUsize::new(0));
    options.provider = Arc::new(SilentProvider {
        calls: calls.clone(),
        info: klaus_agent_core::types::provider::ProviderInfo {
            name: "silent-summary".into(),
            base_url: None,
            env_key: None,
            default_model: "silent-summary".into(),
        },
    });
    let config = options.config.as_mut().unwrap();
    config["model"] = serde_json::json!("silent-summary");
    config["streamIdleTimeoutMs"] = serde_json::json!(20);
    config["compactionStreamMaxRetries"] = serde_json::json!(0);
    config["autoMemory"] = serde_json::json!(false);
    config["agentMemory"] = serde_json::json!(false);
    let mut original = history();
    original.push(Message::Assistant(AssistantMessage {
        uuid: "previous-assistant".into(),
        model: "silent-summary".into(),
        usage: None,
        content: vec![ContentBlock::Text(TextBlock {
            text: "The earlier task is still in progress.".into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })],
    }));
    original.push(Message::User(UserMessage {
        uuid: "follow-up-user".into(),
        content: vec![ContentBlock::Text(TextBlock {
            text: "Continue the task.".into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })],
        is_meta: None,
        question_answer: None,
    }));
    original.push(Message::Assistant(AssistantMessage {
        uuid: "latest-assistant".into(),
        model: "silent-summary".into(),
        usage: None,
        content: vec![ContentBlock::Text(TextBlock {
            text: "Preserve this newest round while summarizing the earlier exchange.".into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })],
    }));
    let session = Agent::new(options).start_session(StartSessionOptions {
        user_config_dir: Some(root.to_string_lossy().into_owned().into()),
        initial_history: Some(original.clone()),
        ..Default::default()
    });
    let result = tokio::time::timeout(Duration::from_secs(3), session.compact(None)).await;
    let retained = session.history();
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
    let result = result
        .expect("manual compaction must settle, not wait forever")
        .unwrap();
    assert!(!result.compacted);
    assert_eq!(retained, original);
    assert!(
        calls.load(Ordering::SeqCst) > 0,
        "the eligible manual operation must invoke the summary provider"
    );
    assert!(
        result.diagnostics[0]
            .message
            .as_ref()
            .unwrap()
            .contains("idle timeout"),
        "an admitted summary must use the configured idle deadline: {:?}",
        result.diagnostics
    );
}

fn history() -> Vec<Message> {
    vec![Message::User(UserMessage {
        uuid: "original-task".into(),
        content: vec![ContentBlock::Text(TextBlock {
            text: "Preserve the original task constraints.".into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })],
        is_meta: None,
        question_answer: None,
    })]
}
