//! Defect: a model stream that emitted frequent events could run forever.
//! Root cause: streamTotalTimeoutMs was configured but only the per-event idle timeout was enforced.
//! Fixed by: 2026-09-07 local-model runtime correctness work item.

use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStream, ModelStreamEvent, ProviderCapabilities, ProviderInfo,
};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

struct HeartbeatProvider {
    info: ProviderInfo,
    calls: Arc<AtomicUsize>,
    events: Arc<AtomicUsize>,
}

impl ModelProvider for HeartbeatProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }
    fn capabilities(&self, _model: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: false,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: 200_000,
            max_output_tokens: 512,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }
    fn stream(&self, _request: ModelRequest, _signal: AbortSignal) -> ModelStream {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let events = self.events.clone();
        Box::pin(
            futures::stream::once(async {
                ModelStreamEvent::MessageStart {
                    model: "heartbeat".into(),
                }
            })
            .chain(futures::stream::unfold(
                (0u64, events),
                |(index, events)| async move {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    events.fetch_add(1, Ordering::SeqCst);
                    Some((
                        ModelStreamEvent::TextDelta {
                            text: format!("{index}").into(),
                        },
                        (index + 1, events),
                    ))
                },
            )),
        )
    }
}

#[tokio::test]
async fn public_session_enforces_total_timeout_even_while_events_keep_arriving() {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let root = std::env::temp_dir().join(format!(
        "klaus-stream-total-timeout-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
    options.cwd = Some(root.to_string_lossy().into_owned());
    options.config.as_mut().unwrap()["model"] = json!("heartbeat");
    options.config.as_mut().unwrap()["streamIdleTimeoutMs"] = json!(100);
    options.config.as_mut().unwrap()["streamTotalTimeoutMs"] = json!(40);
    options.config.as_mut().unwrap()["maxRetries"] = json!(0);
    options.config.as_mut().unwrap()["compactBufferTokens"] = json!(512);
    options.config.as_mut().unwrap()["agentMemory"] = json!(false);
    options.config.as_mut().unwrap()["autoMemory"] = json!(false);
    let resolved =
        klaus_agent_core::engine::session::resolve_config(options.config.clone().unwrap()).unwrap();
    assert_eq!(resolved.stream_total_timeout_ms, 40);
    let calls = Arc::new(AtomicUsize::new(0));
    let events = Arc::new(AtomicUsize::new(0));
    options.provider = Arc::new(HeartbeatProvider {
        info: ProviderInfo {
            name: "heartbeat".into(),
            base_url: None,
            env_key: None,
            default_model: "heartbeat".into(),
        },
        calls: calls.clone(),
        events: events.clone(),
    });

    let session = Agent::new(options).start_session(StartSessionOptions {
        config: Some(json!({
            "streamIdleTimeoutMs": 100,
            "streamTotalTimeoutMs": 40,
            "maxRetries": 0,
            "compactBufferTokens": 512,
        })),
        ..StartSessionOptions::default()
    });
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        session.run(
            Input::Text("Never stop streaming.".into()),
            TurnOptions::default(),
        ),
    )
    .await
    .unwrap_or_else(|_| {
        panic!(
            "the total timeout must terminate the public turn; provider calls={}, events={}",
            calls.load(Ordering::SeqCst),
            events.load(Ordering::SeqCst)
        )
    });
    let error = result.error.expect("an endless stream must fail");
    assert!(
        error.message.contains("stream total timeout"),
        "{:?}",
        error.message
    );
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
