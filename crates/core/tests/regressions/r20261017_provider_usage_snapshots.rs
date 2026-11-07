//! Defect: known provider usage disappeared when a response was cancelled or failed.
//! Root cause: adapters retained usage privately until a successful terminal event.
//! Work item 20261017: preserve actual cumulative request usage before termination.

use futures::StreamExt;
use klaus_agent_core::provider::anthropic::{AnthropicProvider, AnthropicProviderOptions};
use klaus_agent_core::provider::gemini::{GeminiProvider, GeminiProviderOptions};
use klaus_agent_core::provider::openai::{OpenAIProvider, OpenAIProviderOptions, OpenAiApi};
use klaus_agent_core::provider::retry::RetryOptions;
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeString};
use klaus_agent_core::schemas::message::Usage;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{ModelProvider, ModelRequest, ModelStreamEvent};
use serde_json::json;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

#[derive(Clone, Copy, Debug)]
enum Protocol {
    Anthropic,
    Chat,
    Responses,
    Gemini,
}

const PROTOCOLS: [Protocol; 4] = [
    Protocol::Anthropic,
    Protocol::Chat,
    Protocol::Responses,
    Protocol::Gemini,
];

fn event(payload: serde_json::Value) -> String {
    format!("data: {payload}\n\n")
}

impl Protocol {
    fn provider(self, base_url: String) -> Box<dyn ModelProvider> {
        let retry = Some(RetryOptions {
            max_attempts: Some(1),
            ..Default::default()
        });
        match self {
            Self::Anthropic => Box::new(AnthropicProvider::new(AnthropicProviderOptions {
                api_key: Some("usage-fixture-key".into()),
                base_url: Some(base_url),
                retry,
                ..Default::default()
            })),
            Self::Chat | Self::Responses => Box::new(OpenAIProvider::new(OpenAIProviderOptions {
                api_key: Some("usage-fixture-key".into()),
                base_url: Some(base_url),
                retry,
                api: Some(if matches!(self, Self::Chat) {
                    OpenAiApi::Chat
                } else {
                    OpenAiApi::Responses
                }),
                ..Default::default()
            })),
            Self::Gemini => Box::new(GeminiProvider::new(GeminiProviderOptions {
                api_key: Some("usage-fixture-key".into()),
                base_url: Some(base_url),
                retry,
                ..Default::default()
            })),
        }
    }

    fn prefix(self) -> String {
        match self {
            // This protocol's input count is already uncached. Other protocols
            // report the full prompt and the cached subset separately.
            Self::Anthropic => event(
                json!({"type":"message_start","message":{"model":"usage-fixture","usage":{"input_tokens":30,"output_tokens":5,"cache_read_input_tokens":10,"cache_creation_input_tokens":3}}}),
            ),
            Self::Chat => event(
                json!({"model":"usage-fixture","choices":[],"usage":{"prompt_tokens":40,"completion_tokens":5,"prompt_tokens_details":{"cached_tokens":10}}}),
            ),
            Self::Responses => event(
                json!({"type":"response.in_progress","response":{"usage":{"input_tokens":40,"output_tokens":5,"input_tokens_details":{"cached_tokens":10}}}}),
            ),
            Self::Gemini => event(
                json!({"modelVersion":"usage-fixture","usageMetadata":{"promptTokenCount":40,"candidatesTokenCount":3,"thoughtsTokenCount":2,"cachedContentTokenCount":10}}),
            ),
        }
    }

    fn suffix(self) -> String {
        match self {
            // Omitted input/cache fields must preserve earlier reported counts.
            Self::Anthropic => {
                event(
                    json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":16}}),
                ) + &event(json!({"type":"message_stop"}))
            }
            Self::Chat => {
                event(
                    json!({"choices":[{"delta":{"content":"done"},"finish_reason":"stop"}],"usage":{"completion_tokens":16}}),
                ) + "data: [DONE]\n\n"
            }
            Self::Responses => event(
                json!({"type":"response.completed","response":{"model":"usage-fixture","output":[],"usage":{"output_tokens":16}}}),
            ),
            Self::Gemini => event(
                json!({"candidates":[{"content":{"parts":[{"text":"done"}]},"finishReason":"STOP"}],"usageMetadata":{"candidatesTokenCount":14}}),
            ),
        }
    }
}

struct HttpFixture {
    base_url: String,
    release: Option<oneshot::Sender<()>>,
    server: tokio::task::JoinHandle<()>,
    requests: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl Drop for HttpFixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl HttpFixture {
    async fn new(prefix: String, suffix: Option<String>, hold: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let (release, released) = oneshot::channel();
        let requests = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let received = requests.clone();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            received.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut request = Vec::new();
            loop {
                let mut chunk = [0_u8; 4096];
                let read = socket.read(&mut chunk).await.unwrap();
                assert_ne!(read, 0, "the provider must send its complete request");
                request.extend_from_slice(&chunk[..read]);
                assert!(request.len() <= 1024 * 1024, "fixture requests are bounded");
                if let Some(end) = request
                    .windows(4)
                    .position(|part| part == b"\r\n\r\n")
                    .map(|end| end + 4)
                {
                    let headers = std::str::from_utf8(&request[..end]).unwrap();
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    if request.len() >= end + length {
                        break;
                    }
                }
            }
            // An incomplete declared body forces a real transport error rather
            // than a provider-specific synthetic terminal or fabricated usage.
            let body_length = prefix.len() + suffix.as_ref().map_or(100, String::len);
            let headers = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {body_length}\r\nConnection: close\r\n\r\n");
            socket.write_all(headers.as_bytes()).await.unwrap();
            socket.write_all(prefix.as_bytes()).await.unwrap();
            socket.flush().await.unwrap();
            if hold {
                let _ = released.await;
            }
            if let Some(suffix) = suffix {
                socket.write_all(suffix.as_bytes()).await.unwrap();
            }
            socket.shutdown().await.unwrap();
        });
        Self {
            base_url,
            release: Some(release),
            server,
            requests,
        }
    }

    async fn finish(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
        tokio::time::timeout(Duration::from_secs(5), &mut self.server)
            .await
            .unwrap()
            .unwrap();
    }
}

fn request() -> ModelRequest {
    runtime_codec::decode_value(&runtime_json::parse(&RuntimeString::from(
        r#"{"model":"usage-fixture","system":[],"messages":[],"tools":[],"maxOutputTokens":100}"#,
    )).unwrap()).unwrap()
}

fn snapshots(events: &[ModelStreamEvent]) -> Vec<Usage> {
    events
        .iter()
        .filter_map(|event| match event {
            ModelStreamEvent::UsageSnapshot { usage } => Some(usage.clone()),
            _ => None,
        })
        .collect()
}

fn assert_uncached_usage(usage: &Usage, output: u64) {
    assert_eq!(
        usage.input_tokens, 30,
        "input tokens are already disjoint from cached tokens"
    );
    assert_eq!(usage.cache_read_input_tokens, 10);
    assert_eq!(usage.output_tokens, output);
    assert_eq!(
        usage.input_tokens.saturating_add(usage.output_tokens),
        30 + output
    );
}

#[tokio::test]
async fn actual_http_provider_usage_is_delivered_before_cancellation_without_a_terminal() {
    for protocol in PROTOCOLS {
        let mut fixture = HttpFixture::new(protocol.prefix(), None, true).await;
        let provider = protocol.provider(fixture.base_url.clone());
        let signal = AbortSignal::new();
        let mut stream = provider.stream(request(), signal.clone());
        let first = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .unwrap()
            .unwrap();
        let ModelStreamEvent::UsageSnapshot { usage } = first else {
            panic!("known usage must precede content: {protocol:?} {first:?}");
        };
        assert_uncached_usage(&usage, 5);
        signal.cancel();
        // Cancellation must wake the pending network read without requiring the
        // peer to finish or close its held response body.
        let remaining = tokio::time::timeout(Duration::from_secs(5), stream.collect::<Vec<_>>())
            .await
            .unwrap();
        assert!(
            !remaining.iter().any(|event| matches!(
                event,
                ModelStreamEvent::MessageStop { .. } | ModelStreamEvent::Error { .. }
            )),
            "cancelled stream settled as a terminal: {protocol:?} {remaining:?}"
        );
        fixture.finish().await;
    }
}

#[tokio::test]
async fn actual_http_provider_transport_errors_keep_known_usage_without_a_success_terminal() {
    for protocol in PROTOCOLS {
        let mut fixture = HttpFixture::new(protocol.prefix(), None, false).await;
        let provider = protocol.provider(fixture.base_url.clone());
        let events = tokio::time::timeout(
            Duration::from_secs(5),
            provider
                .stream(request(), AbortSignal::new())
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
        fixture.finish().await;
        let usage = snapshots(&events);
        assert_eq!(
            usage.len(),
            1,
            "one actual usage report: {protocol:?} {events:?}"
        );
        assert_uncached_usage(&usage[0], 5);
        assert!(
            matches!(events.last(), Some(ModelStreamEvent::Error { .. })),
            "transport failure must stay a failure: {protocol:?} {events:?}"
        );
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, ModelStreamEvent::MessageStop { .. })),
            "failed stream must not synthesize success: {protocol:?}"
        );
    }
}

#[tokio::test]
async fn actual_http_provider_final_usage_repeats_the_cumulative_snapshot_without_losing_fields() {
    for protocol in PROTOCOLS {
        let mut fixture = HttpFixture::new(protocol.prefix(), Some(protocol.suffix()), false).await;
        let provider = protocol.provider(fixture.base_url.clone());
        let events = tokio::time::timeout(
            Duration::from_secs(5),
            provider
                .stream(request(), AbortSignal::new())
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
        fixture.finish().await;
        let usage = snapshots(&events);
        assert_eq!(
            usage.len(),
            2,
            "each wire report has its own snapshot: {protocol:?} {events:?}"
        );
        assert_uncached_usage(&usage[0], 5);
        assert_uncached_usage(&usage[1], 16);
        let Some(ModelStreamEvent::MessageStop {
            usage: terminal, ..
        }) = events.last()
        else {
            panic!("successful response must retain its terminal: {protocol:?} {events:?}");
        };
        assert_eq!(
            terminal, &usage[1],
            "terminal usage is the same cumulative report"
        );
        assert_eq!(
            terminal.input_tokens.saturating_add(terminal.output_tokens),
            46
        );
    }
}

#[tokio::test]
async fn actual_http_openai_late_cache_reports_correct_the_cumulative_uncached_usage() {
    for protocol in [Protocol::Chat, Protocol::Responses] {
        let (prefix, suffix) = match protocol {
            Protocol::Chat => (
                event(json!({"choices":[],"usage":{"prompt_tokens":40,"completion_tokens":5}})),
                event(
                    json!({"choices":[{"delta":{"content":"done"},"finish_reason":"stop"}],"usage":{"completion_tokens":6,"prompt_tokens_details":{"cached_tokens":10}}}),
                ) + "data: [DONE]\n\n",
            ),
            Protocol::Responses => (
                event(
                    json!({"type":"response.in_progress","response":{"usage":{"input_tokens":40,"output_tokens":5}}}),
                ),
                event(
                    json!({"type":"response.completed","response":{"output":[],"usage":{"output_tokens":6,"input_tokens_details":{"cached_tokens":10}}}}),
                ),
            ),
            _ => unreachable!(),
        };
        let mut fixture = HttpFixture::new(prefix, Some(suffix), false).await;
        let events = tokio::time::timeout(
            Duration::from_secs(5),
            protocol
                .provider(fixture.base_url.clone())
                .stream(request(), AbortSignal::new())
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
        fixture.finish().await;
        let usage = snapshots(&events);
        assert_eq!(usage.len(), 2);
        assert_eq!((usage[0].input_tokens, usage[0].output_tokens), (40, 5));
        assert_eq!(
            (
                usage[1].input_tokens,
                usage[1].output_tokens,
                usage[1].cache_read_input_tokens
            ),
            (30, 6, 10)
        );
        let Some(ModelStreamEvent::MessageStop {
            usage: terminal, ..
        }) = events.last()
        else {
            panic!("missing terminal: {events:?}");
        };
        assert_eq!(terminal, &usage[1]);
        assert_eq!(
            terminal.input_tokens.saturating_add(terminal.output_tokens),
            36,
            "a late cache report can lower actual cumulative uncached usage"
        );
    }
}

#[tokio::test]
async fn actual_http_responses_rejected_terminals_deliver_usage_before_the_error() {
    for terminal in ["response.failed", "response.incomplete"] {
        let suffix = event(
            json!({"type":terminal,"response":{"usage":{"output_tokens":16},"incomplete_details":{"reason":"max_output_tokens"},"error":{"message":"fixture failure","code":"server_error"}}}),
        );
        let mut fixture = HttpFixture::new(Protocol::Responses.prefix(), Some(suffix), false).await;
        let events = tokio::time::timeout(
            Duration::from_secs(5),
            Protocol::Responses
                .provider(fixture.base_url.clone())
                .stream(request(), AbortSignal::new())
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
        fixture.finish().await;
        let usage = snapshots(&events);
        assert_eq!(usage.len(), 2);
        assert_uncached_usage(&usage[1], 16);
        assert!(matches!(
            events.last(),
            Some(ModelStreamEvent::Error { .. })
        ));
        assert!(!events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::MessageStop { .. })));
    }
}

struct GoalHome {
    path: std::path::PathBuf,
    previous: Option<String>,
}

impl GoalHome {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("klaus-goal-http-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        let previous = std::env::var("KLAUS_CONFIG_DIR").ok();
        klaus_agent_core::runtime_process::set_process_environment_var(
            "KLAUS_CONFIG_DIR",
            &*path.to_string_lossy(),
        );
        Self { path, previous }
    }
}

impl Drop for GoalHome {
    fn drop(&mut self) {
        match &self.previous {
            Some(value) => klaus_agent_core::runtime_process::set_process_environment_var(
                "KLAUS_CONFIG_DIR",
                value,
            ),
            None => klaus_agent_core::runtime_process::remove_process_environment_var(
                "KLAUS_CONFIG_DIR",
            ),
        }
        if std::thread::panicking() {
            eprintln!("Retained HTTP goal fixture: {}", self.path.display());
        } else {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

struct GoalSession {
    session: std::sync::Arc<klaus_agent_core::engine::session::Session>,
    process: klaus_agent_core::runtime_process::RuntimeProcessContext,
    _agent: klaus_agent_core::engine::agent::Agent,
    store: std::sync::Arc<dyn klaus_agent_core::store::session_store::SessionStore>,
    phase_origin: std::time::Instant,
    phases: std::sync::Arc<std::sync::Mutex<Vec<(&'static str, u128)>>>,
}

impl GoalSession {
    async fn new(home: &GoalHome, protocol: Protocol, base_url: String) -> Self {
        Self::new_with_tools(home, protocol, base_url, Vec::new()).await
    }

    async fn new_with_tools(
        home: &GoalHome,
        protocol: Protocol,
        base_url: String,
        tools: Vec<std::sync::Arc<dyn klaus_agent_core::types::tool::Tool>>,
    ) -> Self {
        use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
        use klaus_agent_core::engine::deps::NoopCompactor;
        use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
        use klaus_agent_core::store::session_store::SessionStore;
        use std::sync::Arc;
        let process = klaus_agent_core::runtime_process::RuntimeProcessContext::initialize(
            super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
        )
        .unwrap();
        let mut options =
            super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
        options.provider = Arc::from(protocol.provider(base_url));
        options.tools = Some(tools);
        options.compactor = Some(Arc::new(NoopCompactor));
        options.cwd = Some(home.path.to_string_lossy().into_owned());
        let agent = Agent::new(options);
        let store: Arc<dyn SessionStore> =
            Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
                dir: Some(
                    home.path
                        .join(format!("sessions-{protocol:?}"))
                        .to_string_lossy()
                        .into_owned(),
                ),
                archive_dir: None,
            }));
        let session = agent.open_session(store.clone(), StartSessionOptions {
            cwd: Some(home.path.to_string_lossy().to_string().into()),
            user_config_dir: Some(home.path.to_string_lossy().to_string().into()),
            config: Some(json!({"model":"usage-fixture","goal":true,"prewarm":false,"autoMemory":false,"agentMemory":false,"maxRetries":0})),
            ..Default::default()
        }).await.unwrap();
        let phase_origin = std::time::Instant::now();
        let phases = Arc::new(std::sync::Mutex::new(Vec::new()));
        #[cfg(feature = "testing")]
        {
            let phases = phases.clone();
            session
                .goal_runtime()
                .expect("persistent fixture has goal runtime")
                .testing_set_phase_observer(Some(Arc::new(move |phase| {
                    let mut trace = phases.lock().unwrap();
                    if trace.len() == 256 {
                        trace.remove(0);
                    }
                    trace.push((phase, phase_origin.elapsed().as_millis()));
                })));
        }
        Self {
            session,
            process,
            _agent: agent,
            store,
            phase_origin,
            phases,
        }
    }

    async fn close(self) {
        self.session.dispose().await;
        self.process.shutdown().await.unwrap();
    }
}

fn partial_text(protocol: Protocol) -> String {
    match protocol {
        Protocol::Anthropic => {
            event(
                json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
            ) + &event(
                json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"partial-ready"}}),
            )
        }
        Protocol::Chat => event(
            json!({"model":"usage-fixture","choices":[{"delta":{"content":"partial-ready"},"finish_reason":null}]}),
        ),
        Protocol::Responses => {
            event(json!({"type":"response.created","response":{"model":"usage-fixture"}}))
                + &event(json!({"type":"response.output_text.delta","delta":"partial-ready"}))
        }
        Protocol::Gemini => event(
            json!({"modelVersion":"usage-fixture","candidates":[{"content":{"parts":[{"text":"partial-ready"}]}}]}),
        ),
    }
}

async fn persisted_goal(thread_id: &RuntimeString) -> klaus_agent_core::goal::types::ThreadGoal {
    klaus_agent_core::goal::store::get_goal(&thread_id.try_to_utf8().unwrap())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn persistent_agent_goal_bills_actual_http_partial_usage_after_pause_and_interrupt() {
    use klaus_agent_core::engine::session::{Input, TurnOptions};
    use klaus_agent_core::goal::types::ThreadGoalStatus;
    use klaus_agent_core::types::events::EngineEvent;
    let home = GoalHome::new();
    for protocol in PROTOCOLS {
        let mut fixture =
            HttpFixture::new(protocol.prefix() + &partial_text(protocol), None, true).await;
        let agent = GoalSession::new(&home, protocol, fixture.base_url.clone()).await;
        let created = agent
            .session
            .set_thread_goal(
                Some("Retain every measured usage report.".into()),
                None,
                Some(Some(100)),
            )
            .await
            .unwrap();
        let mut events = agent
            .session
            .run_streamed(
                Input::Text("Measure the held response.".into()),
                TurnOptions::default(),
            )
            .events;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let event = events.next().await.expect("partial provider content must reach the session");
                if matches!(event, EngineEvent::TextDelta { ref text, .. } if text.contains("partial-ready")) { break; }
            }
        }).await.unwrap();
        agent
            .session
            .set_thread_goal(None, Some("paused".into()), None)
            .await
            .unwrap();
        agent.session.interrupt_by_user();
        tokio::time::timeout(Duration::from_secs(5), events.collect::<Vec<_>>())
            .await
            .unwrap();
        // The peer is still held: billing must settle from actual snapshots on
        // interruption, without waiting for a terminal usage report.
        let goal = persisted_goal(&created.goal.thread_id).await;
        assert_eq!(goal.goal_id, created.goal.goal_id);
        assert_eq!(goal.status, ThreadGoalStatus::Paused);
        assert_eq!(
            goal.tokens_used, 35,
            "normalized uncached input plus output exactly once: {protocol:?}"
        );
        fixture.finish().await;
        agent.close().await;
        assert_eq!(
            persisted_goal(&created.goal.thread_id).await.tokens_used,
            35,
            "shutdown must not debit the response a second time"
        );
    }
}

#[tokio::test]
async fn persistent_agent_goal_bills_actual_http_partial_usage_on_final_transport_error() {
    use klaus_agent_core::engine::session::{Input, TurnOptions};
    use klaus_agent_core::goal::types::ThreadGoalStatus;
    use klaus_agent_core::types::events::EngineEvent;
    let home = GoalHome::new();
    for protocol in PROTOCOLS {
        let mut fixture =
            HttpFixture::new(protocol.prefix() + &partial_text(protocol), None, false).await;
        let agent = GoalSession::new(&home, protocol, fixture.base_url.clone()).await;
        let created = agent
            .session
            .set_thread_goal(Some("Account for the failed request.".into()), None, None)
            .await
            .unwrap();
        let events = tokio::time::timeout(
            Duration::from_secs(5),
            agent
                .session
                .run_streamed(
                    Input::Text("Measure the failing response.".into()),
                    TurnOptions::default(),
                )
                .events
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap_or_else(|error| {
            let index = agent.session.turn_index_snapshot();
            panic!("HTTP goal turn stalled: protocol={protocol:?}, elapsed_ms={}, phases={:?}, actual_requests={}, started_turns={}, goal_metadata={}, lifecycle={:?}, timeout={error:?}",
                agent.phase_origin.elapsed().as_millis(),
                agent.phases.lock().unwrap(),
                fixture.requests.load(std::sync::atomic::Ordering::SeqCst),
                index.turn_started_at_ms.len(),
                index.turn_goal_metadata.len(),
                agent.session.subagent_lifecycle_snapshot());
        });
        fixture.finish().await;
        assert!(
            events
                .iter()
                .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
            "the transport error must remain a failed turn: {protocol:?} {events:?}"
        );
        let goal = persisted_goal(&created.goal.thread_id).await;
        assert_eq!(goal.goal_id, created.goal.goal_id);
        assert_eq!(
            goal.tokens_used, 35,
            "failed request usage is retained exactly once: {protocol:?}"
        );
        assert_eq!(goal.status, ThreadGoalStatus::Blocked);
        agent.close().await;
        assert_eq!(
            persisted_goal(&created.goal.thread_id).await.tokens_used,
            35
        );
    }
}

#[tokio::test]
async fn persistent_agent_goal_settles_successful_actual_http_usage_once() {
    use klaus_agent_core::engine::session::{Input, TurnOptions};
    use klaus_agent_core::goal::types::ThreadGoalStatus;
    let home = GoalHome::new();
    for protocol in PROTOCOLS {
        let mut fixture = HttpFixture::new(
            protocol.prefix() + &partial_text(protocol),
            Some(protocol.suffix()),
            false,
        )
        .await;
        let agent = GoalSession::new(&home, protocol, fixture.base_url.clone()).await;
        let created = agent
            .session
            .set_thread_goal(
                Some("Retain final measured usage.".into()),
                None,
                Some(Some(100)),
            )
            .await
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            agent
                .session
                .run_streamed(
                    Input::Text("Finish this response.".into()),
                    TurnOptions::default(),
                )
                .events
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap_or_else(|error| {
            let index = agent.session.turn_index_snapshot();
            panic!("HTTP goal turn stalled: protocol={protocol:?}, elapsed_ms={}, phases={:?}, actual_requests={}, started_turns={}, goal_metadata={}, lifecycle={:?}, timeout={error:?}",
                agent.phase_origin.elapsed().as_millis(),
                agent.phases.lock().unwrap(),
                fixture.requests.load(std::sync::atomic::Ordering::SeqCst),
                index.turn_started_at_ms.len(),
                index.turn_goal_metadata.len(),
                agent.session.subagent_lifecycle_snapshot());
        });
        fixture.finish().await;
        let goal = persisted_goal(&created.goal.thread_id).await;
        assert_eq!(goal.status, ThreadGoalStatus::Active);
        assert_eq!(
            goal.tokens_used, 46,
            "cumulative snapshots and terminal must settle once: {protocol:?}"
        );
        agent.close().await;
        assert_eq!(
            persisted_goal(&created.goal.thread_id).await.tokens_used,
            46
        );
    }
}

#[tokio::test]
async fn persistent_agent_goal_late_cache_correction_avoids_a_false_budget_limit() {
    use klaus_agent_core::engine::session::{Input, TurnOptions};
    use klaus_agent_core::goal::types::ThreadGoalStatus;
    let home = GoalHome::new();
    for protocol in [Protocol::Chat, Protocol::Responses] {
        let (prefix, suffix) = match protocol {
            Protocol::Chat => (
                event(json!({"choices":[],"usage":{"prompt_tokens":40,"completion_tokens":5}})),
                event(
                    json!({"choices":[{"delta":{"content":"done"},"finish_reason":"stop"}],"usage":{"completion_tokens":6,"prompt_tokens_details":{"cached_tokens":10}}}),
                ) + "data: [DONE]\n\n",
            ),
            Protocol::Responses => (
                event(
                    json!({"type":"response.in_progress","response":{"usage":{"input_tokens":40,"output_tokens":5}}}),
                ),
                event(
                    json!({"type":"response.completed","response":{"output":[],"usage":{"output_tokens":6,"input_tokens_details":{"cached_tokens":10}}}}),
                ),
            ),
            _ => unreachable!(),
        };
        let mut fixture =
            HttpFixture::new(prefix + &partial_text(protocol), Some(suffix), false).await;
        let agent = GoalSession::new(&home, protocol, fixture.base_url.clone()).await;
        let created = agent
            .session
            .set_thread_goal(
                Some("Respect the actual uncached usage.".into()),
                None,
                Some(Some(40)),
            )
            .await
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            agent
                .session
                .run_streamed(
                    Input::Text("Finish with the final cache report.".into()),
                    TurnOptions::default(),
                )
                .events
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
        fixture.finish().await;
        let goal = persisted_goal(&created.goal.thread_id).await;
        assert_eq!(goal.goal_id, created.goal.goal_id);
        assert_eq!(
            goal.tokens_used, 36,
            "final normalized usage replaces the earlier 45-token observation"
        );
        assert_eq!(goal.token_budget, Some(40));
        assert_eq!(
            goal.status,
            ThreadGoalStatus::Active,
            "the provisional usage must not trigger a false limit"
        );
        agent.close().await;
        assert_eq!(
            persisted_goal(&created.goal.thread_id).await.tokens_used,
            36
        );
    }
}

struct HoldMeasuredUsage {
    schema: klaus_agent_core::types::json::JsonSchema,
    entered: std::sync::Arc<tokio::sync::Semaphore>,
}

#[async_trait::async_trait]
impl klaus_agent_core::types::tool::Tool for HoldMeasuredUsage {
    fn static_name(&self) -> &str {
        "hold_measured_usage"
    }
    fn static_description(&self) -> &str {
        "Hold the current tool until the owning turn is interrupted."
    }
    fn static_input_schema(&self) -> &klaus_agent_core::types::json::JsonSchema {
        &self.schema
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _: &klaus_agent_core::types::json::Json) -> bool {
        false
    }
    fn is_read_only(&self, _: &klaus_agent_core::types::json::Json) -> bool {
        false
    }
    async fn call(
        &self,
        _: klaus_agent_core::types::json::Json,
        context: &klaus_agent_core::types::tool::ToolContext,
        _: Option<klaus_agent_core::types::tool::ToolProgress>,
    ) -> Result<klaus_agent_core::types::tool::ToolResult, klaus_agent_core::types::errors::ToolError>
    {
        self.entered.add_permits(1);
        context.signal.cancelled().await;
        Err(klaus_agent_core::types::errors::ToolError::new(
            "The measured tool was interrupted.",
        ))
    }
}

#[tokio::test]
async fn actual_http_usage_flushes_before_a_goal_fork_and_deferral_survives_reopen_and_controls() {
    use klaus_agent_core::engine::agent::StartSessionOptions;
    use klaus_agent_core::engine::session::{Input, TurnOptions};
    use klaus_agent_core::goal::types::ThreadGoalStatus;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    let home = GoalHome::new();
    let source_body = event(
        json!({"model":"usage-fixture","choices":[{"delta":{"tool_calls":[{"index":0,"id":"hold-real-usage","type":"function","function":{"name":"hold_measured_usage","arguments":"{}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":40,"completion_tokens":16,"prompt_tokens_details":{"cached_tokens":10}}}),
    ) + "data: [DONE]\n\n";
    let mut source_http = HttpFixture::new(source_body, Some(String::new()), false).await;
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let source = GoalSession::new_with_tools(
        &home,
        Protocol::Chat,
        source_http.base_url.clone(),
        vec![Arc::new(HoldMeasuredUsage {
            schema: json!({"type":"object","properties":{}})
                .as_object()
                .unwrap()
                .clone(),
            entered: entered.clone(),
        })],
    )
    .await;
    let created = source
        .session
        .set_thread_goal(
            Some("Retain measured work in an explicitly inherited fork.".into()),
            None,
            Some(Some(500)),
        )
        .await
        .unwrap();
    let mut source_events = source
        .session
        .run_streamed(
            Input::Text("Measure one request and hold its tool.".into()),
            TurnOptions::default(),
        )
        .events;
    let drain = tokio::spawn(async move {
        let mut count = 0;
        while source_events.next().await.is_some() {
            count += 1;
            assert!(count <= 256, "bounded held-source event fixture");
        }
    });
    tokio::time::timeout(Duration::from_secs(5), entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    source_http.finish().await;
    assert_eq!(source_http.requests.load(Ordering::SeqCst), 1);
    // The real provider terminal has settled into the live request accumulator,
    // but the unfinished root turn has not flushed that accumulator to SQLite.
    assert_eq!(
        persisted_goal(&created.goal.thread_id).await.tokens_used,
        0,
        "this fixture must exercise pending measured usage rather than an already-flushed turn"
    );
    let running_snapshot = source.session.persistent_goal_fork_snapshot(None).await;
    assert!(
        matches!(running_snapshot, Err(ref error) if error == "cannot fork a running live session"),
        "a measured live turn still owns the transcript admission boundary"
    );
    assert_eq!(
        persisted_goal(&created.goal.thread_id).await.tokens_used,
        46,
        "fork preparation must flush actual normalized usage before checking transcript admission"
    );
    // End the held tool through the ordinary internal cancellation path. The
    // source goal remains active and its complete idle transcript can be forked.
    source.session.interrupt();
    tokio::time::timeout(Duration::from_secs(5), drain)
        .await
        .unwrap()
        .unwrap();
    let snapshot = source
        .session
        .persistent_goal_fork_snapshot(None)
        .await
        .unwrap();
    let captured = persisted_goal(&created.goal.thread_id).await;
    assert_eq!(
        captured.tokens_used, 46,
        "the admitted idle fork retains the previously measured usage exactly once"
    );
    assert_eq!(
        persisted_goal(&created.goal.thread_id).await.tokens_used,
        46
    );
    assert_eq!(captured.goal_id, created.goal.goal_id);
    assert_eq!(captured.token_budget, Some(500));
    assert_eq!(captured.created_at_ms, created.goal.created_at_ms);
    assert_eq!(captured.status, ThreadGoalStatus::Active);

    let mut child_http = HttpFixture::new(
        Protocol::Chat.prefix() + &partial_text(Protocol::Chat),
        Some(Protocol::Chat.suffix()),
        false,
    )
    .await;
    let child_provider: Arc<dyn ModelProvider> =
        Arc::from(Protocol::Chat.provider(child_http.base_url.clone()));
    let child_options = || StartSessionOptions {
        id: Some("measured-goal-fork-target".into()),
        cwd: Some(home.path.to_string_lossy().to_string().into()),
        user_config_dir: Some(home.path.to_string_lossy().to_string().into()),
        provider: Some(child_provider.clone()),
        config: Some(
            json!({"model":"usage-fixture","goal":true,"prewarm":false,"autoMemory":false,"agentMemory":false,"maxRetries":0}),
        ),
        ..Default::default()
    };
    let child = source
        ._agent
        .fork_live_session(source.store.clone(), &snapshot, child_options())
        .await
        .unwrap();
    let child_thread: RuntimeString = "measured-goal-fork-target".into();
    let mut expected = captured.clone();
    expected.thread_id = child_thread.clone();
    assert_eq!(
        persisted_goal(&child_thread).await,
        expected,
        "explicit inheritance copies every captured goal field"
    );
    assert!(
        klaus_agent_core::goal::store::has_continuation_deferral("measured-goal-fork-target")
            .await
            .unwrap()
    );
    assert!(child
        .run_goal_idle_streamed(TurnOptions::default())
        .await
        .unwrap()
        .is_none());
    assert_eq!(child_http.requests.load(Ordering::SeqCst), 0);
    child.dispose().await;
    drop(child);

    let reopened = source
        ._agent
        .resume_session(
            source.store.clone(),
            "measured-goal-fork-target",
            child_options(),
        )
        .await
        .unwrap();
    assert!(
        klaus_agent_core::goal::store::has_continuation_deferral("measured-goal-fork-target")
            .await
            .unwrap()
    );
    reopened
        .set_thread_goal(
            Some("Edit the inherited objective before explicit work.".into()),
            Some("paused".into()),
            None,
        )
        .await
        .unwrap();
    assert!(reopened
        .run_goal_idle_streamed(TurnOptions::default())
        .await
        .unwrap()
        .is_none());
    reopened
        .set_thread_goal(None, Some("active".into()), None)
        .await
        .unwrap();
    let edited = persisted_goal(&child_thread).await;
    assert_eq!(edited.goal_id, captured.goal_id);
    assert_eq!(edited.created_at_ms, captured.created_at_ms);
    assert_eq!(edited.tokens_used, 46);
    assert_eq!(edited.token_budget, Some(500));
    assert_eq!(edited.status, ThreadGoalStatus::Active);
    assert!(
        klaus_agent_core::goal::store::has_continuation_deferral("measured-goal-fork-target")
            .await
            .unwrap(),
        "editing and resuming must preserve durable continuation deferral"
    );
    assert!(reopened
        .run_goal_idle_streamed(TurnOptions::default())
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        child_http.requests.load(Ordering::SeqCst),
        0,
        "fork, reopen, edit and resume must not invoke inference"
    );
    assert_eq!(source_http.requests.load(Ordering::SeqCst), 1);

    tokio::time::timeout(
        Duration::from_secs(5),
        reopened
            .run_streamed(
                Input::Text("Start explicit work on this inherited goal.".into()),
                TurnOptions::default(),
            )
            .events
            .collect::<Vec<_>>(),
    )
    .await
    .unwrap();
    child_http.finish().await;
    assert_eq!(child_http.requests.load(Ordering::SeqCst), 1);
    assert!(
        !klaus_agent_core::goal::store::has_continuation_deferral("measured-goal-fork-target")
            .await
            .unwrap(),
        "the first explicit turn releases the deferral"
    );
    assert_eq!(
        persisted_goal(&child_thread).await.tokens_used,
        92,
        "the child independently adds one actual46-token response"
    );
    assert_eq!(
        persisted_goal(&created.goal.thread_id).await.tokens_used,
        46,
        "child accounting must not write back into its fork source"
    );
    reopened.dispose().await;
    source
        .session
        .set_thread_goal(None, Some("paused".into()), None)
        .await
        .unwrap();
    source.close().await;
}
