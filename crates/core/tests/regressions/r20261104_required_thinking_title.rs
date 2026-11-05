//! Defect: required-thinking gateways rejected every structured title request.
//! Root cause: the result tool was forced while thinking could not be disabled.
//! Fix: 2026-11-04; select the internal result tool automatically and isolate its JSON.

use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::provider::anthropic::{AnthropicProvider, AnthropicProviderOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::types::provider::{ModelCapabilities, ThinkingCapability};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const MODEL: &str = "required-thinking-model";
const TITLE: &str = "Repair title generation";

async fn request_body(socket: &mut TcpStream) -> Value {
    let mut bytes = Vec::new();
    loop {
        let mut chunk = [0u8; 4096];
        let count = socket.read(&mut chunk).await.unwrap();
        assert!(count > 0, "the complete title request reaches the fixture");
        assert!(bytes.len() + count <= 1024 * 1024);
        bytes.extend_from_slice(&chunk[..count]);
        if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&bytes[..end]);
            let length = headers
                .lines()
                .filter_map(|line| line.split_once(':'))
                .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                .map(|(_, value)| value.trim().parse::<usize>().unwrap())
                .unwrap();
            if bytes.len() >= end + 4 + length {
                return serde_json::from_slice(&bytes[end + 4..end + 4 + length]).unwrap();
            }
        }
    }
}

fn title_response(result_tool: bool) -> String {
    let mut events = vec![
        json!({"type":"message_start","message":{"model":MODEL,"usage":{"input_tokens":20,"output_tokens":0}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"Choose a short task title."}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}),
        json!({"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":if result_tool { "Here is the title.".to_string() } else { json!({"title":TITLE}).to_string() }}}),
        json!({"type":"content_block_stop","index":1}),
    ];
    if result_tool {
        events.extend([
            json!({"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"title-result","name":"emit_result","input":{}}}),
            json!({"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":json!({"title":TITLE}).to_string()}}),
            json!({"type":"content_block_stop","index":2}),
        ]);
    }
    events.extend([
        json!({"type":"message_delta","delta":{"stop_reason":if result_tool { "tool_use" } else { "end_turn" }},"usage":{"output_tokens":30}}),
        json!({"type":"message_stop"}),
    ]);
    events
        .into_iter()
        .map(|event| format!("data: {event}\n\n"))
        .collect()
}

async fn generate_title(adaptive: bool, result_tool: bool) -> Value {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().to_string_lossy().into_owned();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let provider = Arc::new(AnthropicProvider::new(AnthropicProviderOptions {
        api_key: Some("fixture-only".into()),
        base_url: Some(format!("http://{}", listener.local_addr().unwrap())),
        default_model: Some(MODEL.into()),
        capabilities: Some(ModelCapabilities {
            forced_tool_choice_with_thinking: Some(false),
            thinking: Some(ThinkingCapability {
                adaptive,
                interleaved: false,
                can_disable_thinking: false,
            }),
            tools: Some(true),
            ..Default::default()
        }),
        retry: Some(super::rbd0a6abc_empty_tool_choice::single_attempt()),
        ..Default::default()
    }));
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let body = request_body(&mut socket).await;
        let (status, content_type, response) = if body["tool_choice"]["type"] == "tool" {
            (
                "400 Bad Request",
                "application/json",
                json!({"error":{"type":"invalid_request_error","message":"tool_choice 'specified' is incompatible with thinking enabled"}}).to_string(),
            )
        } else {
            ("200 OK", "text/event-stream", title_response(result_tool))
        };
        socket.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).as_bytes()).await.unwrap();
        socket.shutdown().await.unwrap();
        body
    });
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
    options.provider = provider;
    options.cwd = Some(root.clone());
    options.tools = Some(Vec::new());
    options.config.as_mut().unwrap()["model"] = json!(MODEL);
    options.config.as_mut().unwrap()["prewarm"] = json!(false);
    let session = Agent::new(options).start_session(StartSessionOptions {
        cwd: Some(root.clone().into()),
        user_config_dir: Some(root.into()),
        ..Default::default()
    });
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        session.generate_title(None, Some("Repair title generation".into()), false),
    )
    .await
    .unwrap();
    let body = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    let result = result.expect("a required-thinking gateway must accept the title request");
    assert_eq!(result.title, Some(RuntimeString::from(TITLE)));
    assert!(!result.persisted);
    assert_eq!(
        result.raw,
        RuntimeString::from(json!({"title":TITLE}).to_string()),
        "prose must not contaminate the result JSON"
    );
    assert_eq!(body["tools"].as_array().unwrap().len(), 1);
    assert_eq!(body["tools"][0]["name"], json!("emit_result"));
    assert_eq!(body["tool_choice"], json!({"type":"auto"}));
    assert!(
        !body["system"].to_string().contains("emit_result"),
        "result-tool selection must not inject system instructions"
    );
    body
}

#[tokio::test]
async fn required_thinking_title_accepts_prose_then_result_tool() {
    let body = generate_title(true, true).await;
    assert_eq!(body["thinking"]["type"], json!("adaptive"));
}

#[tokio::test]
async fn required_thinking_title_supports_budgeted_reasoning() {
    let body = generate_title(false, true).await;
    assert_eq!(body["thinking"]["type"], json!("enabled"));
    assert!(
        body["thinking"]["budget_tokens"].as_u64().unwrap() < body["max_tokens"].as_u64().unwrap()
    );
}

#[tokio::test]
async fn required_thinking_title_preserves_text_only_json() {
    generate_title(true, false).await;
}
