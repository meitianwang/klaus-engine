//! Defect: unfinished assistant text reset the automatic empty-turn audit.
//! Root cause: provisional stream events were treated as completed activity.
//! Work item: 20261017 completed activity at public HTTP turn boundaries.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::deps::NoopCompactor;
use klaus_agent_core::engine::session::{Session, TurnOptions};
use klaus_agent_core::goal::ThreadGoalStatus;
use klaus_agent_core::provider::openai::{OpenAIProvider, OpenAIProviderOptions, OpenAiApi};
use klaus_agent_core::provider::retry::RetryOptions;
use klaus_agent_core::runtime_process::{
    remove_process_environment_var, set_process_environment_var, RuntimeProcessContext,
};
use klaus_agent_core::runtime_values::RuntimeJson;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::tools::build_tool::text_result;
use klaus_agent_core::types::errors::{TerminalReason, ToolError};
use klaus_agent_core::types::events::{EngineEvent, ItemStatus, ThreadItem};
use klaus_agent_core::types::json::{Json, JsonSchema};
use klaus_agent_core::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

const WAIT: Duration = Duration::from_secs(15);
const PARTIAL: &str = "This provisional assistant text has no completed item.";
const COMPLETE: &str = "The assistant completed this useful response.";

struct Home {
    path: PathBuf,
    previous: Option<std::ffi::OsString>,
}

impl Home {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("klaus-goal-activity-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        let previous = std::env::var_os("KLAUS_CONFIG_DIR");
        set_process_environment_var("KLAUS_CONFIG_DIR", &path);
        Self { path, previous }
    }
}

impl Drop for Home {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(value) => set_process_environment_var("KLAUS_CONFIG_DIR", value),
            None => remove_process_environment_var("KLAUS_CONFIG_DIR"),
        }
        if std::thread::panicking() {
            eprintln!(
                "Retained completed-activity fixture: {}",
                self.path.display()
            );
        } else {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

#[derive(Clone, Copy)]
enum Reply {
    Empty,
    Complete,
    Partial,
    Tool,
}

impl Reply {
    fn body(self) -> String {
        let (delta, reason) = match self {
            Self::Empty => (json!({"role":"assistant","content":" \n"}), Some("stop")),
            Self::Complete => (json!({"role":"assistant","content":COMPLETE}), Some("stop")),
            Self::Partial => (json!({"role":"assistant","content":PARTIAL}), None),
            Self::Tool => (
                json!({"role":"assistant","tool_calls":[{
                    "index":0,"id":"activity-tool-call","type":"function",
                    "function":{"name":"observe_completion","arguments":"{}"}
                }]}),
                Some("tool_calls"),
            ),
        };
        let payload = json!({
            "id":"activity-response","model":"activity-fixture",
            "choices":[{"index":0,"delta":delta,"finish_reason":reason}],
            "usage":{"prompt_tokens":1,"completion_tokens":1}
        });
        let mut body = format!("data: {payload}\n\n");
        if !matches!(self, Self::Partial) {
            body.push_str("data: [DONE]\n\n");
        }
        body
    }
}

async fn read_request(socket: &mut TcpStream) -> Value {
    let mut bytes = Vec::new();
    loop {
        let mut chunk = [0_u8; 4096];
        let read = socket.read(&mut chunk).await.unwrap();
        assert_ne!(read, 0, "the provider must send its complete HTTP request");
        bytes.extend_from_slice(&chunk[..read]);
        assert!(
            bytes.len() <= 1024 * 1024,
            "HTTP fixture requests are bounded"
        );
        let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") else {
            continue;
        };
        let end = end + 4;
        let headers = std::str::from_utf8(&bytes[..end]).unwrap();
        let length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .expect("the actual provider sends a bounded JSON request");
        assert!(length <= 1024 * 1024);
        if bytes.len() >= end + length {
            return serde_json::from_slice(&bytes[end..end + length]).unwrap();
        }
    }
}

struct HttpFixture {
    base_url: String,
    requests: Arc<Mutex<Vec<Value>>>,
    release: Option<oneshot::Sender<()>>,
    server: tokio::task::JoinHandle<()>,
}

impl HttpFixture {
    async fn new(replies: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let (release, released) = oneshot::channel();
        let mut released = Some(released);
        let server = tokio::spawn(async move {
            let count = replies.len();
            for reply in replies {
                let (mut socket, _) = listener.accept().await.unwrap();
                let request = read_request(&mut socket).await;
                {
                    let mut requests = captured.lock().unwrap();
                    assert!(requests.len() < count, "provider requests remain bounded");
                    requests.push(request);
                }
                let body = reply.body();
                let length = if matches!(reply, Reply::Partial) {
                    body.len() + 1024
                } else {
                    body.len()
                };
                let headers = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n");
                socket.write_all(headers.as_bytes()).await.unwrap();
                socket.write_all(body.as_bytes()).await.unwrap();
                socket.flush().await.unwrap();
                if matches!(reply, Reply::Partial) {
                    // Hold the real network body without any successful terminal.
                    let _ = released.take().expect("one held response").await;
                }
                let _ = socket.shutdown().await;
            }
        });
        Self {
            base_url,
            requests,
            release: Some(release),
            server,
        }
    }

    fn release_partial(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }

    async fn finish(&mut self) {
        self.release_partial();
        tokio::time::timeout(WAIT, &mut self.server)
            .await
            .expect("the scripted HTTP server must settle")
            .unwrap();
    }
}

impl Drop for HttpFixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

struct CompletionTool {
    schema: JsonSchema,
    calls: Arc<AtomicUsize>,
    fails: bool,
}

#[async_trait::async_trait]
impl Tool for CompletionTool {
    fn static_name(&self) -> &str {
        "observe_completion"
    }
    fn static_description(&self) -> &str {
        "Observe a completed fixture operation."
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }

    async fn call(
        &self,
        _input: Json,
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(text_result(
            RuntimeJson::Null,
            if self.fails {
                "The executed operation returned an error."
            } else {
                "The operation completed."
            },
            self.fails,
        ))
    }
}

struct Fixture {
    session: Arc<Session>,
    http: HttpFixture,
    calls: Arc<AtomicUsize>,
    process: RuntimeProcessContext,
    agent: Agent,
    home: Home,
}

impl Fixture {
    async fn new(replies: Vec<Reply>, failed_tool: bool) -> Self {
        let home = Home::new();
        let http = HttpFixture::new(replies).await;
        let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut options = agent_options(process.clone());
        options.provider = Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
            api_key: Some("activity-fixture-key".into()),
            base_url: Some(http.base_url.clone()),
            api: Some(OpenAiApi::Chat),
            retry: Some(RetryOptions {
                max_attempts: Some(1),
                ..Default::default()
            }),
            ..Default::default()
        }));
        options.tools = Some(vec![Arc::new(CompletionTool {
            schema: json!({"type":"object","properties":{},"additionalProperties":false})
                .as_object()
                .unwrap()
                .clone(),
            calls: calls.clone(),
            fails: failed_tool,
        })]);
        options.include_default_agents = Some(false);
        options.compactor = Some(Arc::new(NoopCompactor));
        options.cwd = Some(home.path.to_string_lossy().into_owned());
        let agent = Agent::new(options);
        let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(home.path.join("sessions").to_string_lossy().into_owned()),
            archive_dir: None,
        }));
        let session = agent
            .open_session(
                store,
                StartSessionOptions {
                    cwd: Some(home.path.to_string_lossy().into_owned().into()),
                    user_config_dir: Some(home.path.to_string_lossy().into_owned().into()),
                    config: Some(json!({
                        "model":"activity-fixture","goal":true,"prewarm":false,
                        "autoMemory":false,"autoDream":false,"agentMemory":false,
                        "memoryRecall":false,"memory":{"enabled":false},
                        "idleMicrocompact":false,"earlyMicrocompact":false,
                        "speculativeCompaction":false,"loopDetection":false,"maxRetries":0
                    })),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        session
            .set_thread_goal(
                Some("Finish only through completed activity.".into()),
                None,
                None,
            )
            .await
            .unwrap();
        Self {
            session,
            http,
            calls,
            process,
            agent,
            home,
        }
    }

    async fn idle(&self) -> Vec<EngineEvent> {
        let turn = tokio::time::timeout(
            WAIT,
            self.session.run_goal_idle_streamed(TurnOptions::default()),
        )
        .await
        .unwrap()
        .unwrap()
        .expect("the active goal admits one real automatic turn");
        tokio::time::timeout(WAIT, turn.events.collect::<Vec<_>>())
            .await
            .unwrap()
    }

    async fn status(&self, expected: ThreadGoalStatus) {
        assert_eq!(
            self.session.thread_goal().await.unwrap().unwrap().status,
            expected
        );
    }

    async fn two_empty(&self) {
        for _ in 0..2 {
            let events = self.idle().await;
            assert!(events.iter().any(|event| matches!(
                event,
                EngineEvent::TurnCompleted {
                    stop_reason: TerminalReason::Completed,
                    ..
                }
            )));
            self.status(ThreadGoalStatus::Active).await;
        }
    }

    async fn assert_blocked(&self, requests: usize) {
        self.status(ThreadGoalStatus::Blocked).await;
        assert!(self
            .session
            .run_goal_idle_streamed(TurnOptions::default())
            .await
            .unwrap()
            .is_none());
        assert_eq!(self.http.requests.lock().unwrap().len(), requests);
    }

    async fn finish(mut self) {
        self.http.finish().await;
        self.session.shutdown_and_wait().await.unwrap();
        self.process.shutdown().await.unwrap();
        drop(self.agent);
        drop(self.home);
    }
}

#[tokio::test]
async fn partial_http_assistant_text_interrupted_before_completion_is_the_third_empty_turn() {
    let mut fixture = Fixture::new(vec![Reply::Empty, Reply::Empty, Reply::Partial], false).await;
    fixture.two_empty().await;
    let turn = fixture
        .session
        .run_goal_idle_streamed(TurnOptions::default())
        .await
        .unwrap()
        .unwrap();
    let mut stream = turn.events;
    let mut observed = Vec::new();
    tokio::time::timeout(WAIT, async {
        loop {
            let event = stream
                .next()
                .await
                .expect("the actual HTTP stream must yield partial text");
            let partial =
                matches!(&event, EngineEvent::TextDelta { text, .. } if text.contains(PARTIAL));
            assert!(
                observed.len() < 128,
                "the provisional event trace remains bounded"
            );
            observed.push(event);
            if partial {
                break;
            }
        }
    })
    .await
    .unwrap();
    fixture.session.interrupt();
    observed.extend(
        tokio::time::timeout(WAIT, stream.collect::<Vec<_>>())
            .await
            .unwrap(),
    );
    assert!(
        observed.iter().any(|event| matches!(event,
        EngineEvent::TextDelta { text, .. } if text.contains(PARTIAL))),
        "the actual streamed partial assistant text must be observed: {observed:?}"
    );
    assert!(observed.iter().any(|event| matches!(
        event,
        EngineEvent::TurnCompleted {
            stop_reason: TerminalReason::Aborted,
            ..
        }
    )));
    assert!(
        !observed
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "the third empty turn must block through abort auditing, not a transport failure: {observed:?}"
    );
    assert!(
        !observed.iter().any(|event| matches!(event,
        EngineEvent::ItemCompleted { item: ThreadItem::AgentMessage { text, .. }, .. }
            if text.contains(PARTIAL))),
        "the held response must not manufacture completed activity: {observed:?}"
    );
    fixture.assert_blocked(3).await;
    fixture.http.release_partial();
    fixture.finish().await;
}

#[tokio::test]
async fn a_completed_http_assistant_item_resets_two_empty_automatic_turns() {
    let fixture = Fixture::new(
        vec![
            Reply::Empty,
            Reply::Empty,
            Reply::Complete,
            Reply::Empty,
            Reply::Empty,
            Reply::Empty,
        ],
        false,
    )
    .await;
    fixture.two_empty().await;
    let completed = fixture.idle().await;
    assert!(
        completed.iter().any(|event| matches!(event,
        EngineEvent::ItemCompleted { item: ThreadItem::AgentMessage { text, .. }, .. }
            if text.contains(COMPLETE))),
        "{completed:?}"
    );
    fixture.status(ThreadGoalStatus::Active).await;
    fixture.two_empty().await;
    fixture.idle().await;
    fixture.assert_blocked(6).await;
    fixture.finish().await;
}

#[tokio::test]
async fn executed_http_tool_results_still_reset_the_empty_audit_for_completed_and_failed_items() {
    for fails in [false, true] {
        let fixture = Fixture::new(
            vec![
                Reply::Empty,
                Reply::Empty,
                Reply::Tool,
                Reply::Empty,
                Reply::Empty,
                Reply::Empty,
                Reply::Empty,
            ],
            fails,
        )
        .await;
        fixture.two_empty().await;
        let completed = fixture.idle().await;
        assert_eq!(
            fixture.calls.load(Ordering::SeqCst),
            1,
            "the real handler must execute"
        );
        assert!(
            completed.iter().any(|event| match event {
                EngineEvent::ItemCompleted {
                    item:
                        ThreadItem::ToolCall {
                            status: ItemStatus::Completed,
                            result: Some(_),
                            ..
                        },
                    ..
                } => !fails,
                EngineEvent::ItemUpdated {
                    item:
                        ThreadItem::ToolCall {
                            status: ItemStatus::Failed,
                            result: Some(_),
                            ..
                        },
                    ..
                } => fails,
                _ => false,
            }),
            "actual terminal tool item missing: failed={fails}: {completed:?}"
        );
        fixture.status(ThreadGoalStatus::Active).await;
        fixture.two_empty().await;
        fixture.idle().await;
        fixture.assert_blocked(7).await;
        fixture.finish().await;
    }
}
