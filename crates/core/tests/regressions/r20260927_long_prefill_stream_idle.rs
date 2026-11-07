//! Defect: long prompts could be canceled before the model emitted its first event.
//! Root cause: the default stream idle timeout was shorter than long-prompt processing.
//! Fixed by: 2026-09-27 long-prefill stream timeout work item.

use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{resolve_config, Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::message::Usage;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

struct SlowFirstEventProvider {
    info: ProviderInfo,
    delay: Option<Duration>,
    calls: Arc<AtomicUsize>,
}

impl ModelProvider for SlowFirstEventProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
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

    fn stream(&self, _: ModelRequest, _: AbortSignal) -> ModelStream {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let Some(delay) = self.delay else {
            return Box::pin(
                futures::stream::iter(vec![ModelStreamEvent::MessageStart {
                    model: "slow-first-event".into(),
                }])
                .chain(futures::stream::pending()),
            );
        };
        Box::pin(
            futures::stream::once(async move {
                tokio::time::sleep(delay).await;
                ModelStreamEvent::MessageStart {
                    model: "slow-first-event".into(),
                }
            })
            .chain(futures::stream::iter(vec![
                ModelStreamEvent::TextDelta {
                    text: "Done".into(),
                },
                ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::EndTurn,
                    usage: Usage {
                        input_tokens: 10,
                        output_tokens: 1,
                        cache_read_input_tokens: 0,
                        cache_creation_input_tokens: 0,
                        total_tokens: None,
                    },
                },
            ])),
        )
    }
}

async fn run_with_first_event(delay: Option<Duration>, idle_ms: u64) -> Option<String> {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let root =
        std::env::temp_dir().join(format!("klaus-slow-first-event-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
    options.cwd = Some(root.to_string_lossy().into_owned());
    options.config.as_mut().unwrap()["model"] = json!("slow-first-event");
    options.config.as_mut().unwrap()["streamIdleTimeoutMs"] = json!(idle_ms);
    options.config.as_mut().unwrap()["streamTotalTimeoutMs"] = json!(1_000);
    options.config.as_mut().unwrap()["maxRetries"] = json!(0);
    options.config.as_mut().unwrap()["agentMemory"] = json!(false);
    options.config.as_mut().unwrap()["autoMemory"] = json!(false);
    let calls = Arc::new(AtomicUsize::new(0));
    options.provider = Arc::new(SlowFirstEventProvider {
        info: ProviderInfo {
            name: "slow-first-event".into(),
            base_url: None,
            env_key: None,
            default_model: "slow-first-event".into(),
        },
        delay,
        calls: calls.clone(),
    });
    let session = Agent::new(options).start_session(StartSessionOptions {
        config: Some(json!({
            "streamIdleTimeoutMs": idle_ms,
            "streamTotalTimeoutMs": 1_000,
            "maxRetries": 0,
        })),
        ..StartSessionOptions::default()
    });
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        session.run(Input::Text("Reply once.".into()), TurnOptions::default()),
    )
    .await
    .unwrap_or_else(|_| {
        panic!(
            "the public turn must settle; calls={}",
            calls.load(Ordering::SeqCst)
        )
    });
    let error = result
        .error
        .map(|error| error.message.try_to_utf8().unwrap().to_owned());
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
    error
}

#[test]
fn new_sessions_allow_five_minutes_without_a_model_event() {
    let config = resolve_config(json!({})).unwrap();
    assert_eq!(config.stream_idle_timeout_ms, 300_000);
    assert_eq!(config.stream_total_timeout_ms, 1_200_000);
    // The idle bound must stay STRICTLY below the total bound. At equality the
    // stream loop's `remaining_total <= idle_wait` test classifies every timeout
    // as the (non-retryable) total deadline, so a recoverable silent stream
    // would stop being retried as a network error.
    assert!(
        config.stream_idle_timeout_ms < config.stream_total_timeout_ms,
        "idle {} must stay below total {} so idle timeouts remain retryable",
        config.stream_idle_timeout_ms,
        config.stream_total_timeout_ms
    );
}

#[test]
fn explicit_stream_idle_timeout_still_takes_precedence() {
    let config = resolve_config(json!({ "streamIdleTimeoutMs": 60_000 })).unwrap();
    assert_eq!(config.stream_idle_timeout_ms, 60_000);
}

#[tokio::test]
async fn public_session_accepts_a_slow_first_event_within_the_idle_limit() {
    let error = run_with_first_event(Some(Duration::from_millis(150)), 500).await;
    assert_eq!(error, None);
}

#[tokio::test]
async fn public_session_still_fails_when_a_started_stream_goes_silent() {
    let error = run_with_first_event(None, 50).await;
    assert!(
        error
            .as_deref()
            .unwrap_or_default()
            .contains("stream idle timeout"),
        "{error:?}"
    );
}
