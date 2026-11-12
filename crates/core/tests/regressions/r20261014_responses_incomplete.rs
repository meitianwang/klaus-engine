//! Defect: incomplete Responses replies were settled as successful partial answers.
//! Root cause: incomplete reasons entered the output-limit continuation ladder.
//! Work item 20261014: fail incomplete attempts, bound retries, and commit tools only
//! after an accepted response terminal.

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};
use async_trait::async_trait;
use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::deps::NoopCompactor;
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::provider::openai::{OpenAIProvider, OpenAIProviderOptions, OpenAiApi};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::schemas::message::Message;
use klaus_agent_core::tools::build_tool::text_result;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{ProviderErrorKind, ToolError};
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::json::{Json, JsonSchema};
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStreamEvent,
};
use klaus_agent_core::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn event(value: Value) -> String {
    format!("data: {value}\n\n")
}

fn terminal(reason: Option<&str>, tool: bool) -> String {
    let output = if tool {
        json!([{"type":"function_call","id":"item-call","call_id":"call-probe","name":"probe","arguments":"{}"}])
    } else {
        json!([])
    };
    let mut response = json!({"model":"fixture","output":output,"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}});
    let kind = if reason.is_some() {
        "response.incomplete"
    } else {
        "response.completed"
    };
    if let Some(reason) = reason {
        response["incomplete_details"] = json!({"reason":reason});
    }
    event(json!({"type":kind,"response":response}))
}

fn text_body(text: &str, reason: Option<&str>) -> String {
    event(json!({"type":"response.created","response":{"model":"fixture"}}))
        + &event(json!({"type":"response.output_text.delta","delta":text}))
        + &terminal(reason, false)
}

fn tool_body(reason: Option<&str>) -> String {
    event(json!({"type":"response.created","response":{"model":"fixture"}}))
        + &event(
            json!({"type":"response.output_item.added","item":{"type":"function_call","id":"item-call","call_id":"call-probe","name":"probe","arguments":""}}),
        )
        + &event(
            json!({"type":"response.output_item.done","item":{"type":"function_call","id":"item-call","call_id":"call-probe","name":"probe","arguments":"{}"}}),
        )
        + &terminal(reason, true)
}

async fn request(socket: &mut TcpStream) -> Value {
    let mut bytes = Vec::new();
    loop {
        let mut chunk = [0_u8; 4096];
        let read = socket.read(&mut chunk).await.unwrap();
        assert_ne!(read, 0, "fixture request must include a complete HTTP body");
        bytes.extend_from_slice(&chunk[..read]);
        if let Some(end) = bytes
            .windows(4)
            .position(|part| part == b"\r\n\r\n")
            .map(|i| i + 4)
        {
            let headers = std::str::from_utf8(&bytes[..end]).unwrap();
            let length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            if bytes.len() >= end + length {
                return serde_json::from_slice(&bytes[end..end + length]).unwrap();
            }
        }
    }
}

struct Fixture {
    provider: Arc<OpenAIProvider>,
    requests: Arc<Mutex<Vec<Value>>>,
    server: tokio::task::JoinHandle<()>,
}

impl Fixture {
    async fn new(bodies: Vec<String>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let observed = requests.clone();
        let server = tokio::spawn(async move {
            let mut index = 0;
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let body = &bodies[index.min(bodies.len() - 1)];
                let captured = request(&mut socket).await;
                observed.lock().unwrap().push(captured);
                index += 1;
                let response = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        Self {
            provider: Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
                api: Some(OpenAiApi::Responses),
                api_key: Some("fixture-token".into()),
                default_model: Some("fixture".into()),
                base_url: Some(format!("http://{address}/v1")),
                ..Default::default()
            })),
            requests,
            server,
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

fn model_request() -> ModelRequest {
    ModelRequest {
        supports_image_detail_original: false,
        model_verbosity: None,
        reasoning_summary: Default::default(),
        model: "fixture".into(),
        image_limits: None,
        max_media_blocks: None,
        system: vec![],
        messages: vec![],
        tools: vec![],
        native_deferred_tools: None,
        deferred_tools: Vec::new(),
        tool_choice: None,
        max_output_tokens: 1_000,
        task_budget: None,
        output_schema: None,
        web_search: None,
        thinking: None,
        effort: None,
        skip_cache_write: None,
        cache_ttl: None,
        cache_domain: None,
        cache_key: None,
        context_management: None,
        context_hints: None,
        thinking_context: None,
    }
}

#[tokio::test]
async fn public_responses_provider_rejects_incomplete_reasons_before_tool_execution() {
    let mut bodies = vec![
        tool_body(Some("max_output_tokens")),
        tool_body(Some("content_filter")),
        tool_body(Some("unknown_reason")),
    ];
    bodies.push(event(json!({"type":"response.incomplete","response":{}})));
    bodies.push(tool_body(None).rsplit_once("data: ").unwrap().0.to_string());
    for body in bodies {
        let fixture = Fixture::new(vec![body]).await;
        let events: Vec<_> = fixture
            .provider
            .stream(model_request(), AbortSignal::new())
            .collect()
            .await;
        assert!(
            !events.iter().any(|event| matches!(
                event,
                ModelStreamEvent::ToolUseStop { .. } | ModelStreamEvent::MessageStop { .. }
            )),
            "rejected attempts must not commit tools or completion: {events:?}"
        );
        let Some(ModelStreamEvent::Error { error }) = events.last() else {
            panic!("incomplete attempt must end in a typed error: {events:?}")
        };
        assert_eq!(error.kind, ProviderErrorKind::Network);
        assert_eq!(error.code.as_ref().unwrap(), "response_incomplete");
    }
}

#[tokio::test]
async fn public_responses_provider_accepts_interrupted_and_completed_terminals() {
    for reason in [None, Some("interrupted")] {
        let fixture = Fixture::new(vec![text_body("accepted", reason)]).await;
        let events: Vec<_> = fixture
            .provider
            .stream(model_request(), AbortSignal::new())
            .collect()
            .await;
        assert!(matches!(
            events.last(),
            Some(ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                ..
            })
        ));
        assert!(!events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::Error { .. })));
    }
}

struct Probe {
    schema: JsonSchema,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Tool for Probe {
    fn static_name(&self) -> &str {
        "probe"
    }
    fn static_description(&self) -> &str {
        "Count execution of an accepted call."
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _: &Json) -> bool {
        true
    }
    fn is_read_only(&self, _: &Json) -> bool {
        true
    }
    async fn call(
        &self,
        input: Json,
        _: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(text_result(input, "accepted tool result", false))
    }
}

async fn run_session(
    fixture: &Fixture,
    retries: u32,
    calls: Arc<AtomicUsize>,
) -> (Vec<EngineEvent>, Vec<Message>) {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let root =
        std::env::temp_dir().join(format!("klaus-responses-terminal-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let mut options = agent_options(process.clone());
    options.cwd = Some(root.to_string_lossy().into_owned());
    options.provider = fixture.provider.clone();
    options.compactor = Some(Arc::new(NoopCompactor));
    options.tools = Some(vec![Arc::new(Probe {
        schema: json!({"type":"object","properties":{}})
            .as_object()
            .unwrap()
            .clone(),
        calls,
    })]);
    options.config.as_mut().unwrap().as_object_mut().unwrap().extend(json!({"model":"fixture","contextWindow":1_000_000,"maxOutputTokens":1000,"maxRetries":retries,"prewarm":false,"autoMemory":false,"agentMemory":false,"idleMicrocompact":false,"earlyMicrocompact":false,"speculativeCompaction":false,"loopDetection":false,"goal":false}).as_object().unwrap().clone());
    let session = Agent::new(options).start_session(StartSessionOptions {
        user_config_dir: Some(root.to_string_lossy().into_owned().into()),
        ..Default::default()
    });
    let events = tokio::time::timeout(
        Duration::from_secs(5),
        session
            .run_streamed(
                Input::Text("Complete the fixture request.".into()),
                TurnOptions::default(),
            )
            .events
            .collect::<Vec<_>>(),
    )
    .await;
    let history = session.history();
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
    (
        events.expect("finite response retries must settle"),
        history,
    )
}

#[tokio::test]
async fn public_session_responses_incomplete_retries_are_finite_and_never_continue_a_tail() {
    for reason in ["max_output_tokens", "content_filter", "unknown_reason"] {
        let fixture = Fixture::new(vec![text_body("partial answer", Some(reason))]).await;
        let calls = Arc::new(AtomicUsize::new(0));
        let (events, history) = run_session(&fixture, 2, calls).await;
        let requests = fixture.requests.lock().unwrap();
        assert_eq!(
            requests.len(),
            3,
            "initial attempt plus exactly two stream retries"
        );
        for retried in requests.iter().skip(1) {
            assert_eq!(
                retried["input"], requests[0]["input"],
                "retry must reissue the request without partial output or continuation prompts"
            );
            assert_eq!(
                retried["max_output_tokens"],
                requests[0]["max_output_tokens"]
            );
        }
        assert!(events.iter().any(|event| matches!(event, EngineEvent::TurnFailed { error, .. } if error.provider_kind==Some(ProviderErrorKind::Network) && error.message.contains(reason))));
        assert!(!events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnCompleted { .. })));
        assert!(
            !history
                .iter()
                .any(|message| matches!(message, Message::Assistant(_))),
            "failed partial output must not become settled assistant history"
        );
    }
}

#[tokio::test]
async fn public_session_responses_retry_executes_only_the_accepted_tool_attempt() {
    let fixture = Fixture::new(vec![
        tool_body(Some("max_output_tokens")),
        tool_body(None),
        text_body("finished", None),
    ])
    .await;
    let calls = Arc::new(AtomicUsize::new(0));
    let (events, _) = run_session(&fixture, 1, calls.clone()).await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "a rejected response must not execute its completed tool item"
    );
    let requests = fixture.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0]["input"], requests[1]["input"]);
    assert!(events
        .iter()
        .any(|event| matches!(event, EngineEvent::TurnCompleted { .. })));
    assert!(!events
        .iter()
        .any(|event| matches!(event, EngineEvent::TurnFailed { .. })));
}
