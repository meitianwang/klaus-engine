//! Defect: structured approval assessment transport discarded its inspection tools.
//! Root cause: forced result-tool emulation replaced the complete review tool registry.
//! Work item: 20261019 private review tool-loop structured result transport.

#![cfg(unix)]

use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::provider::anthropic::{AnthropicProvider, AnthropicProviderOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeString};
use klaus_agent_core::tools::builtins::write::create_write_tool;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::approval_review::ApprovalReviewStatus;
use klaus_agent_core::types::provider::{
    ModelCapabilities, ModelProvider, ModelRequest, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::r7c876d49_spec_restores_latest_permission_policy::agent_options;

const MODEL: &str = "inspection-review-model";
const SENTINEL: &str = "ACTUAL_REVIEW_INSPECTION_EVIDENCE";

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("klaus-review-transport-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(path.join("workspace")).unwrap();
        std::fs::create_dir_all(path.join("outside")).unwrap();
        Self(path.canonicalize().unwrap())
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct RecordedProvider {
    inner: AnthropicProvider,
    reviews: Mutex<Vec<(ModelRequest, ModelCapabilities, usize)>>,
}
impl ModelProvider for RecordedProvider {
    fn info(&self) -> &ProviderInfo {
        self.inner.info()
    }
    fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
        self.inner.capabilities(model)
    }
    fn model_metadata(&self, model: &RuntimeString) -> ModelCapabilities {
        self.inner.model_metadata(model)
    }
    fn offline_model_catalog(&self) -> Vec<(RuntimeString, ModelCapabilities)> {
        self.inner.offline_model_catalog()
    }
    fn approval_review_preferred_model(&self) -> RuntimeString {
        self.inner.approval_review_preferred_model()
    }
    fn approval_request_tokens_with_metadata(
        &self,
        req: &ModelRequest,
        metadata: ModelCapabilities,
        effort: Option<RuntimeString>,
    ) -> Result<usize, RuntimeString> {
        self.inner
            .approval_request_tokens_with_metadata(req, metadata, effort)
    }
    fn stream(&self, req: ModelRequest, signal: AbortSignal) -> ModelStream {
        self.inner.stream(req, signal)
    }
    fn stream_with_model_metadata(
        &self,
        req: ModelRequest,
        metadata: ModelCapabilities,
        effort: Option<RuntimeString>,
        attempts: u32,
        signal: AbortSignal,
    ) -> ModelStream {
        let tokens = self
            .inner
            .approval_request_tokens_with_metadata(&req, metadata.clone(), effort.clone())
            .unwrap();
        self.reviews
            .lock()
            .unwrap()
            .push((req.clone(), metadata.clone(), tokens));
        self.inner
            .stream_with_model_metadata(req, metadata, effort, attempts, signal)
    }
}

async fn read_request(socket: &mut TcpStream) -> Value {
    let mut bytes = Vec::new();
    loop {
        let mut chunk = [0u8; 8192];
        let count = socket.read(&mut chunk).await.unwrap();
        assert_ne!(count, 0);
        assert!(bytes.len() + count <= 512 * 1024);
        bytes.extend_from_slice(&chunk[..count]);
        if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&bytes[..end]);
            let length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            if bytes.len() >= end + 4 + length {
                return serde_json::from_slice(&bytes[end + 4..end + 4 + length]).unwrap();
            }
        }
    }
}
fn event(value: Value) -> String {
    format!("data: {value}\n\n")
}
fn reply_tool(name: &str, id: &str, input: Value) -> String {
    event(
        json!({"type":"message_start","message":{"model":MODEL,"usage":{"input_tokens":20,"output_tokens":0}}}),
    ) + &event(
        json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":id,"name":name,"input":{}}}),
    ) + &event(
        json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":input.to_string()}}),
    ) + &event(json!({"type":"content_block_stop","index":0}))
        + &event(
            json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":8}}),
        )
        + &event(json!({"type":"message_stop"}))
}
fn reply_done() -> String {
    event(
        json!({"type":"message_start","message":{"model":MODEL,"usage":{"input_tokens":20,"output_tokens":0}}}),
    ) + &event(
        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
    ) + &event(
        json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"The requested file is ready."}}),
    ) + &event(json!({"type":"content_block_stop","index":0}))
        + &event(
            json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":8}}),
        )
        + &event(json!({"type":"message_stop"}))
}
async fn respond(socket: &mut TcpStream, body: &str) {
    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
}
fn wire_tokens(body: &Value) -> usize {
    let mut input = serde_json::Map::new();
    for field in [
        "system",
        "messages",
        "tools",
        "tool_choice",
        "output_config",
        "thinking",
    ] {
        if let Some(value) = body.get(field) {
            input.insert(field.into(), value.clone());
        }
    }
    serde_json::to_vec(&input).unwrap().len().div_ceil(4)
}
fn metadata() -> ModelCapabilities {
    runtime_codec::decode_text(&RuntimeString::from(
        json!({
            "tools":true,"vision":true,"contextWindow":200000,"maxOutputTokens":8192,
            "effort":{"levels":["low"],"default":"low"},
            "thinking":{"adaptive":true,"interleaved":true},"structuredOutput":false
        })
        .to_string(),
    ))
    .unwrap()
}

#[tokio::test]
async fn actual_review_inspects_before_internal_result_and_uses_complete_wire_budget() {
    let directory = Directory::new();
    let sentinel = directory.0.join("workspace/sentinel.txt");
    let output = directory.0.join("outside/result.txt");
    std::fs::write(&sentinel, SENTINEL).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let provider = Arc::new(RecordedProvider {
        inner: AnthropicProvider::new(AnthropicProviderOptions {
            api_key: Some("fixture-only".into()),
            base_url: Some(format!("http://{}", listener.local_addr().unwrap())),
            default_model: Some(MODEL.into()),
            capabilities: Some(metadata()),
            ..Default::default()
        }),
        reviews: Mutex::new(Vec::new()),
    });
    let output_for_server = output.clone();
    let server = tokio::spawn(async move {
        let mut bodies = Vec::new();
        for round in 0..4 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let body = read_request(&mut socket).await;
            let reply = match round {
                0 => reply_tool(
                    "write",
                    "parent-write",
                    json!({"path":output_for_server,"content":"approved"}),
                ),
                1 => {
                    let names = body["tools"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|tool| tool["name"].as_str().unwrap())
                        .collect::<Vec<_>>();
                    assert_eq!(names.len(), 6);
                    for expected in [
                        "exec_command",
                        "write_stdin",
                        "view_image",
                        "exec",
                        "wait",
                        "emit_result",
                    ] {
                        assert!(names.contains(&expected), "{body}");
                    }
                    assert_eq!(body["tool_choice"], json!({"type":"auto"}));
                    assert_eq!(body["thinking"]["type"], json!("adaptive"));
                    reply_tool(
                        "exec_command",
                        "review-inspection",
                        json!({"command":"/bin/cat sentinel.txt","yieldMs":1000}),
                    )
                }
                2 => {
                    assert!(body["messages"].to_string().contains(SENTINEL), "{body}");
                    reply_tool(
                        "emit_result",
                        "internal-assessment",
                        json!({"outcome":"allow","reason":"The inspection confirms the requested file operation."}),
                    )
                }
                _ => reply_done(),
            };
            bodies.push(body);
            respond(&mut socket, &reply).await;
        }
        bodies
    });
    let supervisor = klaus_agent_core::testing::shared_test_process_supervisor();
    let process = RuntimeProcessContext::initialize(supervisor.clone()).unwrap();
    let mut options = agent_options(process.clone());
    options.provider = provider.clone();
    options.tools = Some(vec![create_write_tool()]);
    options.include_default_agents = Some(false);
    options.cwd = Some(directory.0.join("workspace").to_string_lossy().into_owned());
    options.config = Some(json!({
        "model":MODEL,"effort":"low","thinking":true,"permissionMode":"default",
        "approvalPolicy":"on-request","sandboxMode":"workspace-write","approvalsReviewer":"automatic","approvalReview":false,
        "sandbox_workspace_write":{"exclude_tmpdir_env_var":true,"exclude_slash_tmp":true},
        "sandbox":"auto","requireRealSandbox":true,"sandboxNetworkAllowed":false,"sandboxRetryOnDenial":true,
        "maxOutputTokens":4096,"useStoredPermissionRules":false,"prewarm":false,
        "goal":false,"autoMemory":false,"agentMemory":false,"autoDream":false,
        "memoryRecall":false,"memory":{"enabled":false},"loopDetection":false,"codeModeHost":false
    }));
    let session = Agent::new(options).start_session(StartSessionOptions {
        user_config_dir: Some(
            directory
                .0
                .join("config")
                .to_string_lossy()
                .into_owned()
                .into(),
        ),
        ..Default::default()
    });
    tokio::time::timeout(
        Duration::from_secs(20),
        session.run(
            Input::Text(
                "Create the requested output file and inspect the workspace sentinel first.".into(),
            ),
            TurnOptions::default(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(std::fs::read_to_string(&output).unwrap(), "approved");
    let events = session.approval_review_history();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].status, ApprovalReviewStatus::Approved);
    let bodies = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
    {
        let reviews = provider.reviews.lock().unwrap();
        assert_eq!(reviews.len(), 2);
        for ((request, metadata, counted), body) in reviews.iter().zip(&bodies[1..3]) {
            assert_eq!(*counted, wire_tokens(body));
            let mut no_result = request.clone();
            no_result.output_schema = None;
            let without = provider
                .inner
                .approval_request_tokens_with_metadata(
                    &no_result,
                    metadata.clone(),
                    Some("low".into()),
                )
                .unwrap();
            assert!(
                *counted > without,
                "the internal result tool schema consumes input budget"
            );
        }
    }
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn ordinary_structured_fallback_keeps_its_forced_result_contract() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut capabilities = metadata();
    capabilities.thinking.as_mut().unwrap().can_disable_thinking = true;
    let provider = AnthropicProvider::new(AnthropicProviderOptions {
        api_key: Some("fixture-only".into()),
        base_url: Some(format!("http://{}", listener.local_addr().unwrap())),
        capabilities: Some(capabilities),
        ..Default::default()
    });
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let body = read_request(&mut socket).await;
        respond(
            &mut socket,
            &reply_tool("emit_result", "result", json!({"outcome":"allow"})),
        )
        .await;
        body
    });
    let request:ModelRequest=runtime_codec::decode_text(&RuntimeString::from(json!({
        "model":MODEL,"system":[],"messages":[],"tools":[{"name":"exec_command","description":"Inspect local evidence.","inputSchema":{"type":"object","properties":{}}}],
        "maxOutputTokens":1024,"thinking":{"type":"adaptive"},"outputSchema":{"type":"object","properties":{"outcome":{"type":"string"}}}
    }).to_string())).unwrap();
    let counted = provider.approval_request_tokens(&request).unwrap();
    let events = provider
        .stream(request, AbortSignal::new())
        .collect::<Vec<_>>()
        .await;
    assert!(events
        .iter()
        .any(|event| matches!(event,ModelStreamEvent::TextDelta{text} if text.contains("allow"))));
    assert!(!events
        .iter()
        .any(|event| matches!(event, ModelStreamEvent::ToolUseStart { .. })));
    let body = server.await.unwrap();
    assert_eq!(body["tools"].as_array().unwrap().len(), 1);
    assert_eq!(body["tools"][0]["name"], json!("emit_result"));
    assert_eq!(
        body["tool_choice"],
        json!({"type":"tool","name":"emit_result"})
    );
    assert_eq!(body["thinking"]["type"], json!("disabled"));
    assert_eq!(counted, wire_tokens(&body));
}
