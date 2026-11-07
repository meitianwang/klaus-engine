//! A response that stops normally after emitting ONLY internal reasoning is no
//! longer settled as a failed "no content" turn on the first attempt. It walks
//! the empty-closing retry once with a direct-answer reminder; if the retry
//! still carries no visible output the turn fails with the standard wording.

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};
use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::message::Usage;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::EngineErrorCode;
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use serde_json::json;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

enum Reply {
    /// Internal reasoning only, normal stop: the regression shape.
    ThinkingOnly,
    /// A visible answer, normal stop.
    Text(&'static str),
}

struct FixtureProvider {
    info: ProviderInfo,
    replies: Mutex<VecDeque<Reply>>,
    requests: Mutex<Vec<ModelRequest>>,
}

impl ModelProvider for FixtureProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: false,
            thinking: true,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: 200_000,
            max_output_tokens: 8_192,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn stream(&self, request: ModelRequest, _signal: AbortSignal) -> ModelStream {
        self.requests.lock().unwrap().push(request);
        let reply = self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("each request needs a scripted reply");
        let mut events = vec![ModelStreamEvent::MessageStart {
            model: "thinking-fixture".into(),
        }];
        let stop_reason = match reply {
            Reply::ThinkingOnly => {
                events.push(ModelStreamEvent::ThinkingDelta {
                    text: "I reasoned internally but never wrote an answer.".into(),
                });
                ModelStopReason::EndTurn
            }
            Reply::Text(text) => {
                events.push(ModelStreamEvent::TextDelta { text: text.into() });
                ModelStopReason::EndTurn
            }
        };
        events.push(ModelStreamEvent::MessageStop {
            stop_reason,
            usage: Usage {
                input_tokens: 10,
                output_tokens: 20,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
                total_tokens: None,
            },
        });
        Box::pin(futures::stream::iter(events))
    }
}

async fn run(replies: Vec<Reply>) -> (usize, Vec<EngineEvent>) {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let root = tempfile::tempdir().unwrap();
    let mut options = agent_options(process.clone());
    let provider = Arc::new(FixtureProvider {
        info: ProviderInfo {
            name: "thinking-fixture".into(),
            default_model: "thinking-fixture".into(),
            base_url: None,
            env_key: None,
        },
        replies: Mutex::new(replies.into()),
        requests: Mutex::new(Vec::new()),
    });
    options.provider = provider.clone();
    options.tools = Some(Vec::new());
    options.config = Some(json!({
        "model": "thinking-fixture", "prewarm": false,
        "goal": false, "autoMemory": false, "autoDream": false,
        "approval_policy": "never", "sandbox_mode": "danger-full-access"
    }));
    let directory = root.path().to_string_lossy().into_owned();
    let session = Agent::new(options).start_session(StartSessionOptions {
        cwd: Some(directory.clone().into()),
        user_config_dir: Some(directory.into()),
        ..Default::default()
    });
    let events: Vec<_> = session
        .run_streamed(
            Input::Text("Answer the user's question".into()),
            TurnOptions::default(),
        )
        .events
        .collect()
        .await;
    let requests = provider.requests.lock().unwrap().len();
    (requests, events)
}

#[tokio::test]
async fn thinking_only_response_is_retried_then_completed() {
    let (requests, events) = run(vec![
        Reply::ThinkingOnly,
        Reply::Text("Here is the answer."),
    ])
    .await;
    assert_eq!(requests, 2, "the thinking-only response gets one retry");
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "the retry must recover the turn: {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnCompleted { .. })),
        "the recovered turn must complete: {events:?}"
    );
    assert!(
        events.iter().any(
            |event| matches!(event, EngineEvent::Notification { level: _, message }
                if message.contains("empty response"))
        ),
        "the retry path must announce itself: {events:?}"
    );
}

#[tokio::test]
async fn thinking_only_response_fails_after_the_retry_comes_back_empty() {
    let (requests, events) = run(vec![Reply::ThinkingOnly, Reply::ThinkingOnly]).await;
    assert_eq!(requests, 2, "the retry is attempted exactly once");
    let failures: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            EngineEvent::TurnFailed { error, .. } => Some(error),
            _ => None,
        })
        .collect();
    assert_eq!(failures.len(), 1, "{events:?}");
    assert_eq!(failures[0].code, EngineErrorCode::ProviderError);
    assert_eq!(
        failures[0].message.try_to_utf8().unwrap(),
        "the model returned no content for this turn"
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnCompleted { .. })),
        "an unrecovered empty turn must not complete: {events:?}"
    );
}
