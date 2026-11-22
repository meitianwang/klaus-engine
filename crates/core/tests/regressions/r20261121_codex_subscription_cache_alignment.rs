//! Defect: high token burn and degraded prompt-cache hit rates on Codex subscription sessions.
//! Root cause: requests used stateless HTTP SSE without load-balancer affinity cookies, per-turn
//! state tokens, responses-lite headers, or WebSocket `previous_response_id` incremental
//! continuation; dynamic runtime reminders rewrote prior turn prefixes; fork sub-agents stripped
//! `prompt_cache_key` via `skip_cache_write` while heterogeneous sub-agents replayed full uncached
//! parent history; and model-level `tool_output_token_limit` was not enforced at tool-output creation.
//! Work item: 20261121 Codex subscription token and cache hit rate alignment.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::engine::BUILTIN_AGENTS;
use klaus_agent_core::provider::auth::AuthSource;
use klaus_agent_core::provider::openai::{
    OpenAICompatibilityOptions, OpenAIProvider, OpenAIProviderOptions, OpenAiApi,
};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimePath, RuntimeString};
use klaus_agent_core::schemas::effort::EffortLevel;
use klaus_agent_core::tools::build_tool::{
    text_result, DEFAULT_IS_CONCURRENCY_SAFE, DEFAULT_IS_DESTRUCTIVE, DEFAULT_IS_ENABLED,
    DEFAULT_IS_READ_ONLY,
};
use klaus_agent_core::tools::builtins::task::{
    create_task_tool, AgentDefinition as TaskAgentDefinition, TaskToolOptions,
};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{ProviderError, ToolError};
use klaus_agent_core::types::json::Json;
use klaus_agent_core::types::permission::ExternalPermissionMode;
use klaus_agent_core::types::provider::{
    EffortCapability, ModelCapabilities, ModelProvider, ReasoningSummary, RemoteCompactionSupport,
    ThinkingCapability,
};
use klaus_agent_core::types::tool::{Tool, ToolContext, ToolExposure, ToolProgress, ToolResult};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::{Message as WsMessage, Role};
use tokio_tungstenite::WebSocketStream;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

struct SyntheticCodexOAuth {
    base_url: String,
    account_id: String,
}

#[async_trait]
impl AuthSource for SyntheticCodexOAuth {
    async fn get_headers(
        &self,
        _signal: &AbortSignal,
    ) -> Result<HashMap<String, String>, ProviderError> {
        let mut headers = HashMap::new();
        headers.insert(
            "authorization".to_string(),
            "Bearer synthetic-oauth-token".to_string(),
        );
        headers.insert("chatgpt-account-id".to_string(), self.account_id.clone());
        Ok(headers)
    }

    fn base_url(&self) -> Option<&str> {
        Some(&self.base_url)
    }

    fn is_subscription_oauth(&self) -> bool {
        true
    }

    fn credential_account_id(&self) -> Option<String> {
        Some(self.account_id.clone())
    }
}

struct EchoProbeTool {
    payload_chars: usize,
}

#[async_trait]
impl Tool for EchoProbeTool {
    fn name(&self) -> RuntimeString {
        RuntimeString::from("echo_probe")
    }

    fn description(&self) -> RuntimeString {
        "Return a deterministic probe payload.".into()
    }

    fn input_schema(&self) -> Json {
        runtime_json::parse(&RuntimeString::from(
            r#"{"type":"object","properties":{"tag":{"type":"string"}},"additionalProperties":false}"#,
        ))
        .expect("valid schema")
    }

    fn exposure(&self) -> ToolExposure {
        ToolExposure::Direct
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        DEFAULT_IS_CONCURRENCY_SAFE
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        DEFAULT_IS_READ_ONLY
    }

    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        DEFAULT_IS_DESTRUCTIVE
    }

    fn is_enabled(&self) -> bool {
        DEFAULT_IS_ENABLED
    }

    async fn call(
        &self,
        input: Json,
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let tag = input
            .get("tag")
            .and_then(Json::as_str)
            .and_then(|s| s.try_to_utf8().ok())
            .unwrap_or_else(|| "probe".to_string());
        let body = format!("{tag}:{}", "x".repeat(self.payload_chars));
        Ok(text_result(
            RuntimeJson::Null,
            RuntimeString::from_utf8(&body),
            false,
        ))
    }
}

fn codex_model_capabilities() -> ModelCapabilities {
    ModelCapabilities {
        context_window: Some(272_000),
        effective_context_window_percent: Some(95),
        max_output_tokens: Some(128_000),
        thinking: Some(ThinkingCapability::default()),
        effort: Some(EffortCapability {
            levels: vec![EffortLevel::Low, EffortLevel::Medium, EffortLevel::High],
            default: Some(EffortLevel::Medium),
        }),
        supported_reasoning_efforts: vec!["low".into(), "medium".into(), "high".into()],
        default_reasoning_summary: Some(ReasoningSummary::None),
        supports_reasoning_summary_parameter: Some(true),
        responses_lite: Some(true),
        additional_tools: Some(true),
        mid_convo_system_messages: Some(true),
        tool_output_token_limit: Some(10_000),
        ..Default::default()
    }
}

fn build_codex_provider(base_url: &str, force_ws: Option<bool>) -> Arc<dyn ModelProvider> {
    let caps = codex_model_capabilities();
    let mut by_model = HashMap::new();
    by_model.insert("gpt-5.4".to_string(), caps.clone());
    let mut extra = HashMap::new();
    extra.insert("originator".to_string(), "codex_cli_rs".to_string());
    let compat = OpenAICompatibilityOptions::try_new(extra, None).expect("valid compat headers");
    let provider = OpenAIProvider::new(OpenAIProviderOptions {
        remote_compaction: RemoteCompactionSupport::ResponsesV1,
        api_key: None,
        base_url: Some(base_url.to_string()),
        default_model: Some("gpt-5.4".to_string()),
        api: Some(OpenAiApi::Responses),
        retry: None,
        auth_source: Some(Arc::new(SyntheticCodexOAuth {
            base_url: base_url.to_string(),
            account_id: "acct_test_codex_1".to_string(),
        })),
        http_client: None,
        classifier_http_client_factory: None,
        compatibility: Some(compat),
        interceptor: None,
        capabilities: Some(caps),
        capabilities_by_model: Some(by_model),
        reasoning_dialect: None,
        fast_mode: false,
        bridge_fast_mode: None,
    })
    .with_responses_websocket(force_ws);
    Arc::new(provider)
}

fn sse_tool_call(resp_id: &str, item_id: &str, call_id: &str, name: &str, args: &Value) -> String {
    let args_str = args.to_string();
    let events = [
        json!({"type": "response.created", "response": {"id": resp_id}}),
        json!({
            "type": "response.output_item.added",
            "item": {
                "type": "function_call",
                "id": item_id,
                "call_id": call_id,
                "name": name
            }
        }),
        json!({
            "type": "response.output_item.done",
            "item": {
                "type": "function_call",
                "id": item_id,
                "call_id": call_id,
                "name": name,
                "arguments": args_str
            }
        }),
        json!({
            "type": "response.completed",
            "response": {
                "id": resp_id,
                "status": "completed",
                "usage": {
                    "input_tokens": 800,
                    "output_tokens": 20,
                    "total_tokens": 820,
                    "input_tokens_details": {"cached_tokens": 640}
                }
            }
        }),
    ];
    events
        .iter()
        .map(|ev| format!("data: {ev}\n\n"))
        .collect::<String>()
}

fn sse_text_reply(resp_id: &str, msg_id: &str, text: &str) -> String {
    let events = [
        json!({"type": "response.created", "response": {"id": resp_id}}),
        json!({
            "type": "response.output_item.added",
            "item": {"id": msg_id, "type": "message", "role": "assistant"}
        }),
        json!({
            "type": "response.output_text.delta",
            "item_id": msg_id,
            "delta": text
        }),
        json!({
            "type": "response.output_item.done",
            "item": {
                "id": msg_id,
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": text}]
            }
        }),
        json!({
            "type": "response.completed",
            "response": {
                "id": resp_id,
                "status": "completed",
                "usage": {
                    "input_tokens": 900,
                    "output_tokens": 15,
                    "total_tokens": 915,
                    "input_tokens_details": {"cached_tokens": 768}
                }
            }
        }),
    ];
    events
        .iter()
        .map(|ev| format!("data: {ev}\n\n"))
        .collect::<String>()
}

async fn read_http_request(
    stream: &mut tokio::net::TcpStream,
) -> (String, HashMap<String, String>, Vec<u8>) {
    let mut buf = Vec::new();
    let mut header_end = None;
    let mut chunk = [0u8; 4096];
    while header_end.is_none() {
        let n = stream.read(&mut chunk).await.expect("read request headers");
        assert!(n > 0, "unexpected EOF reading HTTP headers");
        buf.extend_from_slice(&chunk[..n]);
        header_end = buf
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .map(|idx| idx + 4);
    }
    let end = header_end.unwrap();
    let header_text = String::from_utf8_lossy(&buf[..end]).to_string();
    let mut lines = header_text.split("\r\n");
    let request_line = lines.next().unwrap_or_default().to_string();
    let mut headers = HashMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }
    let content_len = headers
        .get("content-length")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    let mut body = buf[end..].to_vec();
    while body.len() < content_len {
        let n = stream.read(&mut chunk).await.expect("read request body");
        if n == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..n]);
    }
    body.truncate(content_len);
    (request_line, headers, body)
}

fn codex_config_json() -> Value {
    json!({
        "approval_policy": "never",
        "sandbox_mode": "danger-full-access",
        "model": "gpt-5.4",
        "thinking": true,
        "modelSource": {
            "source": "codex",
            "modelId": "gpt-5.4"
        },
        "prewarm": false,
        "autoMemory": false,
        "autoDream": false,
        "memoryRecall": false,
        "checkpointing": false
    })
}

#[tokio::test]
async fn codex_websocket_incremental_continuation_and_sticky_affinity_headers() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("local addr");
    let handshake_headers_log: Arc<Mutex<Vec<HashMap<String, String>>>> =
        Arc::new(Mutex::new(Vec::new()));
    let ws_frames_log: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let hs_clone = handshake_headers_log.clone();
    let frames_clone = ws_frames_log.clone();

    let server = tokio::spawn(async move {
        let (mut tcp, _) = listener
            .accept()
            .await
            .expect("accept single persistent ws");
        let (_req_line, headers, _body) = read_http_request(&mut tcp).await;
        hs_clone.lock().unwrap().push(headers.clone());
        let ws_key = headers.get("sec-websocket-key").expect("sec-websocket-key");
        let accept = derive_accept_key(ws_key.as_bytes());
        let upgrade_resp = format!(
            "HTTP/1.1 101 Switching Protocols\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             Sec-WebSocket-Accept: {accept}\r\n\
             Set-Cookie: __oailb=lb-shard-42; Path=/; Max-Age=3600\r\n\
             Set-Cookie: __cflb=cf-edge-99; Path=/; Max-Age=3600\r\n\
             x-codex-turn-state: turn-sticky-token-1\r\n\r\n"
        );
        tcp.write_all(upgrade_resp.as_bytes())
            .await
            .expect("write 101");

        let mut ws = WebSocketStream::from_raw_socket(tcp, Role::Server, None).await;

        // Frame 1: Turn 1 Step 1 (tool call)
        let msg1 = ws.next().await.expect("frame 1").expect("ws frame 1");
        let req1: Value = serde_json::from_str(msg1.to_text().unwrap()).expect("json 1");
        frames_clone.lock().unwrap().push(req1);
        for ev in [
            json!({"type": "response.created", "response": {"id": "resp_step_1"}}),
            json!({
                "type": "response.output_item.done",
                "item": {
                    "type": "reasoning",
                    "id": "rs_1",
                    "summary": [],
                    "encrypted_content": "enc_blob_step_1"
                }
            }),
            json!({
                "type": "response.output_item.added",
                "item": {
                    "type": "function_call",
                    "id": "fc_item_1",
                    "call_id": "call_probe_1",
                    "name": "echo_probe"
                }
            }),
            json!({
                "type": "response.output_item.done",
                "item": {
                    "type": "function_call",
                    "id": "fc_item_1",
                    "call_id": "call_probe_1",
                    "name": "echo_probe",
                    "arguments": "{\"tag\":\"step1\"}"
                }
            }),
            json!({
                "type": "response.completed",
                "response": {
                    "id": "resp_step_1",
                    "status": "completed",
                    "usage": {
                        "input_tokens": 1200,
                        "output_tokens": 45,
                        "total_tokens": 1245,
                        "input_tokens_details": {"cached_tokens": 1024}
                    }
                }
            }),
        ] {
            ws.send(WsMessage::Text(ev.to_string()))
                .await
                .expect("send step 1 event");
        }

        // Frame 2: Turn 1 Step 2 (incremental tool output continuation)
        let msg2 = ws.next().await.expect("frame 2").expect("ws frame 2");
        let req2: Value = serde_json::from_str(msg2.to_text().unwrap()).expect("json 2");
        frames_clone.lock().unwrap().push(req2);
        for ev in [
            json!({"type": "response.created", "response": {"id": "resp_step_2"}}),
            json!({
                "type": "response.output_item.added",
                "item": {"id": "msg_2", "type": "message", "role": "assistant"}
            }),
            json!({
                "type": "response.output_text.delta",
                "item_id": "msg_2",
                "delta": "Step 1 completed."
            }),
            json!({
                "type": "response.output_item.done",
                "item": {
                    "id": "msg_2",
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "Step 1 completed."}]
                }
            }),
            json!({
                "type": "response.completed",
                "response": {
                    "id": "resp_step_2",
                    "status": "completed",
                    "usage": {
                        "input_tokens": 1400,
                        "output_tokens": 20,
                        "total_tokens": 1420,
                        "input_tokens_details": {"cached_tokens": 1280}
                    }
                }
            }),
        ] {
            ws.send(WsMessage::Text(ev.to_string()))
                .await
                .expect("send step 2 event");
        }

        // Frame 3: Turn 2 (incremental user follow-up on the same WebSocket!)
        let msg3 = ws.next().await.expect("frame 3").expect("ws frame 3");
        let req3: Value = serde_json::from_str(msg3.to_text().unwrap()).expect("json 3");
        frames_clone.lock().unwrap().push(req3);
        for ev in [
            json!({"type": "response.created", "response": {"id": "resp_turn_2"}}),
            json!({
                "type": "response.output_item.added",
                "item": {"id": "msg_3", "type": "message", "role": "assistant"}
            }),
            json!({
                "type": "response.output_text.delta",
                "item_id": "msg_3",
                "delta": "Turn 2 completed."
            }),
            json!({
                "type": "response.output_item.done",
                "item": {
                    "id": "msg_3",
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "Turn 2 completed."}]
                }
            }),
            json!({
                "type": "response.completed",
                "response": {
                    "id": "resp_turn_2",
                    "status": "completed",
                    "usage": {
                        "input_tokens": 1500,
                        "output_tokens": 18,
                        "total_tokens": 1518,
                        "input_tokens_details": {"cached_tokens": 1408}
                    }
                }
            }),
        ] {
            ws.send(WsMessage::Text(ev.to_string()))
                .await
                .expect("send turn 2 event");
        }
    });

    let dir = tempfile::tempdir().expect("tempdir");
    let cfg_dir = tempfile::tempdir().expect("cfgdir");
    let base_url = format!("http://{addr}/backend-api/codex");
    let provider = build_codex_provider(&base_url, Some(true));
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut opts = agent_options(process);
    opts.provider = provider;
    opts.config = Some(codex_config_json());
    opts.tools = Some(vec![Arc::new(EchoProbeTool { payload_chars: 32 })]);
    let agent = Agent::new(opts);
    let session = agent
        .try_start_session_async(StartSessionOptions {
            cwd: Some(RuntimePath::from(dir.path().to_string_lossy().as_ref())),
            user_config_dir: Some(RuntimePath::from(cfg_dir.path().to_string_lossy().as_ref())),
            ..Default::default()
        })
        .await
        .expect("start session");

    let out1 = session
        .run(Input::Text("Run probe".into()), TurnOptions::default())
        .await;
    assert!(out1.error.is_none(), "turn 1 failed: {:?}", out1.error);

    let out2 = session
        .run(Input::Text("Follow up".into()), TurnOptions::default())
        .await;
    assert!(out2.error.is_none(), "turn 2 failed: {:?}", out2.error);

    server.await.expect("server task");

    let handshakes = handshake_headers_log.lock().unwrap();
    assert_eq!(
        handshakes.len(),
        1,
        "all steps and turns must reuse one persistent WebSocket"
    );
    let hs = &handshakes[0];
    assert_eq!(
        hs.get("openai-beta").map(String::as_str),
        Some("responses_websockets=2026-02-06")
    );
    assert_eq!(
        hs.get("originator").map(String::as_str),
        Some("codex_cli_rs")
    );
    assert_eq!(
        hs.get("x-openai-internal-codex-responses-lite")
            .map(String::as_str),
        Some("true")
    );
    assert_eq!(
        hs.get("x-codex-routing-hint").map(String::as_str),
        Some("model=gpt-5.4")
    );
    let sid = session.id.try_to_utf8().unwrap();
    assert_eq!(hs.get("session-id"), Some(&sid));
    assert_eq!(hs.get("thread-id"), Some(&sid));
    assert_eq!(hs.get("x-client-request-id"), Some(&sid));

    let frames = ws_frames_log.lock().unwrap();
    assert_eq!(frames.len(), 3);

    // Frame 1: initial full request, responses-lite reasoning context, no summary by default.
    assert_eq!(frames[0]["type"], "response.create");
    assert!(frames[0].get("previous_response_id").is_none());
    assert_eq!(frames[0]["prompt_cache_key"], sid);
    assert_eq!(frames[0]["parallel_tool_calls"], false);
    assert_eq!(
        frames[0]["client_metadata"]["ws_request_header_x_openai_internal_codex_responses_lite"],
        "true"
    );
    assert_eq!(frames[0]["reasoning"]["context"], "all_turns");
    assert!(
        frames[0]["reasoning"].get("summary").is_none(),
        "default_reasoning_summary=none must omit reasoning.summary unless explicitly requested"
    );

    // Frame 2: incremental continuation within Turn 1 (carries turn-sticky token and ONLY delta input!).
    assert_eq!(frames[1]["previous_response_id"], "resp_step_1");
    assert_eq!(
        frames[1]["client_metadata"]["x-codex-turn-state"],
        "turn-sticky-token-1"
    );
    let step2_input = frames[1]["input"].as_array().expect("step 2 input array");
    assert_eq!(
        step2_input.len(),
        1,
        "incremental step 2 must send only the new function_call_output item: {step2_input:?}"
    );
    assert_eq!(step2_input[0]["type"], "function_call_output");
    assert_eq!(step2_input[0]["call_id"], "call_probe_1");

    // Frame 3: Turn 2 incremental continuation (new user turn resets prior turn's x-codex-turn-state!).
    assert_eq!(frames[2]["previous_response_id"], "resp_step_2");
    assert!(
        frames[2]["client_metadata"]
            .get("x-codex-turn-state")
            .is_none(),
        "a new user turn must reset the previous turn's x-codex-turn-state token"
    );
    let turn2_input = frames[2]["input"].as_array().expect("turn 2 input array");
    assert!(
        turn2_input
            .iter()
            .all(|item| item.get("type") != Some(&json!("function_call"))),
        "turn 2 incremental payload must not re-transmit prior turn history"
    );
}

#[tokio::test]
async fn codex_websocket_previous_response_not_found_resets_anchor_and_runtime_reminders_preserve_prefix(
) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("local addr");
    let ws_frames_log: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let frames_clone = ws_frames_log.clone();

    let server = tokio::spawn(async move {
        let (mut tcp, _) = listener.accept().await.expect("accept ws");
        let (_req_line, headers, _body) = read_http_request(&mut tcp).await;
        let ws_key = headers.get("sec-websocket-key").expect("sec-websocket-key");
        let accept = derive_accept_key(ws_key.as_bytes());
        let upgrade_resp = format!(
            "HTTP/1.1 101 Switching Protocols\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             Sec-WebSocket-Accept: {accept}\r\n\r\n"
        );
        tcp.write_all(upgrade_resp.as_bytes()).await.unwrap();
        let mut ws = WebSocketStream::from_raw_socket(tcp, Role::Server, None).await;

        // Frame 1: Turn 1 initial request -> completed
        let msg1 = ws.next().await.unwrap().unwrap();
        frames_clone
            .lock()
            .unwrap()
            .push(serde_json::from_str(msg1.to_text().unwrap()).unwrap());
        for ev in [
            json!({"type": "response.created", "response": {"id": "resp_ephemeral_1"}}),
            json!({"type": "response.output_item.added", "item": {"id": "m1", "type": "message", "role": "assistant"}}),
            json!({"type": "response.output_text.delta", "item_id": "m1", "delta": "Turn 1 ok."}),
            json!({"type": "response.output_item.done", "item": {"id": "m1", "type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "Turn 1 ok."}]}}),
            json!({"type": "response.completed", "response": {"id": "resp_ephemeral_1", "status": "completed", "usage": {"input_tokens": 500, "output_tokens": 10, "total_tokens": 510}}}),
        ] {
            ws.send(WsMessage::Text(ev.to_string())).await.unwrap();
        }

        // Frame 2: Turn 2 incremental request -> server returns previous_response_not_found!
        let msg2 = ws.next().await.unwrap().unwrap();
        frames_clone
            .lock()
            .unwrap()
            .push(serde_json::from_str(msg2.to_text().unwrap()).unwrap());
        let failed = json!({
            "type": "response.failed",
            "response": {
                "id": "resp_err",
                "status": "failed",
                "error": {
                    "code": "previous_response_not_found",
                    "message": "Previous response resp_ephemeral_1 expired."
                }
            }
        });
        ws.send(WsMessage::Text(failed.to_string())).await.unwrap();

        // Frame 3: Client must automatically retry with full input (no previous_response_id) on the same socket!
        let msg3 = ws.next().await.unwrap().unwrap();
        frames_clone
            .lock()
            .unwrap()
            .push(serde_json::from_str(msg3.to_text().unwrap()).unwrap());
        for ev in [
            json!({"type": "response.created", "response": {"id": "resp_recovered_2"}}),
            json!({"type": "response.output_item.added", "item": {"id": "m2", "type": "message", "role": "assistant"}}),
            json!({"type": "response.output_text.delta", "item_id": "m2", "delta": "Turn 2 recovered."}),
            json!({"type": "response.output_item.done", "item": {"id": "m2", "type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "Turn 2 recovered."}]}}),
            json!({"type": "response.completed", "response": {"id": "resp_recovered_2", "status": "completed", "usage": {"input_tokens": 700, "output_tokens": 10, "total_tokens": 710}}}),
        ] {
            ws.send(WsMessage::Text(ev.to_string())).await.unwrap();
        }
    });

    let dir = tempfile::tempdir().expect("tempdir");
    let cfg_dir = tempfile::tempdir().expect("cfgdir");
    let base_url = format!("http://{addr}/backend-api/codex");
    let provider = build_codex_provider(&base_url, Some(true));
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut opts = agent_options(process);
    opts.provider = provider;
    opts.config = Some(codex_config_json());
    opts.tools = Some(vec![Arc::new(EchoProbeTool { payload_chars: 16 })]);
    let agent = Agent::new(opts);
    let session = agent
        .try_start_session_async(StartSessionOptions {
            cwd: Some(RuntimePath::from(dir.path().to_string_lossy().as_ref())),
            user_config_dir: Some(RuntimePath::from(cfg_dir.path().to_string_lossy().as_ref())),
            permission_mode: Some(ExternalPermissionMode::Default),
            ..Default::default()
        })
        .await
        .expect("start session");

    let out1 = session
        .run(
            Input::Text("TURN1_IMMUTABLE_PREFIX".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(out1.error.is_none(), "turn 1 failed: {:?}", out1.error);

    // Mutate runtime permission mode between turns: on `codex`, runtime reminders must
    // append to Turn 2 rather than rewriting Turn 1's prefix items!
    let _ = session.set_permission_mode(ExternalPermissionMode::AcceptEdits);

    let out2 = session
        .run(Input::Text("Turn 2 prompt".into()), TurnOptions::default())
        .await;
    assert!(
        out2.error.is_none(),
        "turn 2 recovery failed: {:?}",
        out2.error
    );

    server.await.expect("server");
    let frames = ws_frames_log.lock().unwrap();
    assert_eq!(frames.len(), 3);

    // Frame 2 attempted incremental continuation first (demonstrating Turn 1 prefix was NOT rewritten!).
    assert_eq!(frames[1]["previous_response_id"], "resp_ephemeral_1");

    // Frame 3 automatically reset anchor and resent full input with Turn 1 prefix byte-identical!
    assert!(frames[2].get("previous_response_id").is_none());
    let input1 = frames[0]["input"].as_array().unwrap();
    let input3 = frames[2]["input"].as_array().unwrap();
    assert!(input3.len() > input1.len());
    assert_eq!(
        &input3[..input1.len()],
        input1.as_slice(),
        "Turn 1 prefix items must remain byte-identical even after a runtime permission-mode update"
    );
}

#[tokio::test]
async fn codex_websocket_upgrade_rejection_falls_back_to_http_sse_with_affinity_cookies() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("local addr");
    let seen_http_headers: Arc<Mutex<Vec<HashMap<String, String>>>> =
        Arc::new(Mutex::new(Vec::new()));
    let seen_clone = seen_http_headers.clone();

    let server = tokio::spawn(async move {
        // Connection 1: WebSocket GET upgrade -> reject with 426 + Set-Cookie
        let (mut tcp1, _) = listener.accept().await.expect("accept ws upgrade");
        let (req_line1, _h1, _) = read_http_request(&mut tcp1).await;
        assert!(req_line1.starts_with("GET "));
        let reject = "HTTP/1.1 426 Upgrade Required\r\n\
                      Set-Cookie: __oailb=http-fallback-lb; Path=/; Max-Age=3600\r\n\
                      Content-Length: 16\r\n\r\nupgrade rejected";
        tcp1.write_all(reject.as_bytes()).await.unwrap();
        drop(tcp1);

        // Connection 2: Automatic HTTP POST SSE fallback (Step 1: returns tool call + x-codex-turn-state)
        let (mut tcp2, _) = listener.accept().await.expect("accept http step 1");
        let (req_line2, h2, _) = read_http_request(&mut tcp2).await;
        assert!(req_line2.starts_with("POST "));
        seen_clone.lock().unwrap().push(h2);
        let sse1 = sse_tool_call(
            "resp_http_1",
            "fc_http_1",
            "call_http_1",
            "echo_probe",
            &json!({"tag": "http"}),
        );
        let resp2 = format!(
            "HTTP/1.1 200 OK\r\n\
             Content-Type: text/event-stream\r\n\
             x-codex-turn-state: http-turn-token-77\r\n\
             Content-Length: {}\r\n\r\n{}",
            sse1.len(),
            sse1
        );
        tcp2.write_all(resp2.as_bytes()).await.unwrap();
        drop(tcp2);

        // Connection 3: Step 2 HTTP POST SSE (must carry both __oailb cookie and x-codex-turn-state!)
        let (mut tcp3, _) = listener.accept().await.expect("accept http step 2");
        let (_req_line3, h3, _) = read_http_request(&mut tcp3).await;
        seen_clone.lock().unwrap().push(h3);
        let sse2 = sse_text_reply("resp_http_2", "m_http", "HTTP fallback ok.");
        let resp3 = format!(
            "HTTP/1.1 200 OK\r\n\
             Content-Type: text/event-stream\r\n\
             Content-Length: {}\r\n\r\n{}",
            sse2.len(),
            sse2
        );
        tcp3.write_all(resp3.as_bytes()).await.unwrap();
    });

    let dir = tempfile::tempdir().expect("tempdir");
    let cfg_dir = tempfile::tempdir().expect("cfgdir");
    let base_url = format!("http://{addr}/backend-api/codex");
    let provider = build_codex_provider(&base_url, Some(true));
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut opts = agent_options(process);
    opts.provider = provider;
    opts.config = Some(codex_config_json());
    opts.tools = Some(vec![Arc::new(EchoProbeTool { payload_chars: 16 })]);
    let agent = Agent::new(opts);
    let session = agent
        .try_start_session_async(StartSessionOptions {
            cwd: Some(RuntimePath::from(dir.path().to_string_lossy().as_ref())),
            user_config_dir: Some(RuntimePath::from(cfg_dir.path().to_string_lossy().as_ref())),
            ..Default::default()
        })
        .await
        .expect("start session");

    let out = session
        .run(Input::Text("Fallback test".into()), TurnOptions::default())
        .await;
    assert!(out.error.is_none(), "fallback turn failed: {:?}", out.error);

    server.await.expect("server");
    let http_headers = seen_http_headers.lock().unwrap();
    assert_eq!(http_headers.len(), 2);
    assert_eq!(
        http_headers[0].get("cookie").map(String::as_str),
        Some("__oailb=http-fallback-lb"),
        "Set-Cookie from rejected WS upgrade must immediately stick on HTTP SSE fallback"
    );
    assert_eq!(
        http_headers[1]
            .get("x-codex-turn-state")
            .map(String::as_str),
        Some("http-turn-token-77"),
        "Step 2 HTTP SSE request must forward x-codex-turn-state captured from Step 1"
    );
}

#[tokio::test]
async fn codex_subagents_keep_parent_cache_key_for_same_model_and_isolate_specialized_roles_and_spill_oversized_outputs(
) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("local addr");
    let request_bodies: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let bodies_clone = request_bodies.clone();
    let call_idx = Arc::new(AtomicUsize::new(0));
    let idx_clone = call_idx.clone();

    let server = tokio::spawn(async move {
        loop {
            let Ok((mut tcp, _)) = listener.accept().await else {
                break;
            };
            let bodies_inner = bodies_clone.clone();
            let step = idx_clone.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let (_req_line, _headers, body_bytes) = read_http_request(&mut tcp).await;
                let parsed: Value = serde_json::from_slice(&body_bytes).expect("json body");
                bodies_inner.lock().unwrap().push(parsed);

                let sse = match step {
                    // Step 0: parent invokes oversized probe tool (60,000 chars > 40,000-char 10k-token cap)
                    0 => sse_tool_call(
                        "r0",
                        "fc_big",
                        "call_big",
                        "echo_probe",
                        &json!({"tag": "oversized"}),
                    ),
                    // Step 1: parent spawns same-model general-purpose sub-agent synchronously
                    1 => sse_tool_call(
                        "r1",
                        "fc_sub_gp",
                        "call_sub_gp",
                        "spawn_agent",
                        &json!({
                            "task_name": "check",
                            "message": "Subagent same-model check",
                            "model": "high",
                            "run_in_background": false
                        }),
                    ),
                    // Step 2: general-purpose sub-agent replies
                    2 => sse_text_reply("r2", "m2", "GP done."),
                    // Step 3: parent spawns Explore sub-agent synchronously without explicit fork_turns
                    3 => sse_tool_call(
                        "r3",
                        "fc_sub_ex",
                        "call_sub_ex",
                        "spawn_agent",
                        &json!({
                            "task_name": "locate",
                            "message": "Explore isolated check",
                            "agent_type": "Explore",
                            "model": "high",
                            "run_in_background": false
                        }),
                    ),
                    // Step 4+: Explore replies, then parent finishes
                    _ => sse_text_reply("r_end", "m_end", "All checks complete."),
                };
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\n\r\n{}",
                    sse.len(),
                    sse
                );
                let _ = tcp.write_all(resp.as_bytes()).await;
            });
        }
    });

    let dir = tempfile::tempdir().expect("tempdir");
    let cfg_dir = tempfile::tempdir().expect("cfgdir");
    let base_url = format!("http://{addr}/backend-api/codex");
    // Use HTTP SSE here (`Some(false)`) so every request sends its full body for direct inspection.
    let provider = build_codex_provider(&base_url, Some(false));
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut opts = agent_options(process);
    opts.provider = provider;
    opts.config = Some(codex_config_json());
    opts.agents = Some(BUILTIN_AGENTS.clone());
    let task_agents = BUILTIN_AGENTS
        .iter()
        .map(|def| TaskAgentDefinition {
            name: def.name.clone(),
            description: def.description.clone(),
            isolation: def.isolation,
            background: def.background,
        })
        .collect();
    opts.tools = Some(vec![
        Arc::new(EchoProbeTool {
            payload_chars: 60_000,
        }),
        create_task_tool(TaskToolOptions {
            model_catalog: false,
            model_options: Vec::new(),
            agents: task_agents,
        }),
    ]);
    let agent = Agent::new(opts);
    let session = agent
        .try_start_session_async(StartSessionOptions {
            cwd: Some(RuntimePath::from(dir.path().to_string_lossy().as_ref())),
            user_config_dir: Some(RuntimePath::from(cfg_dir.path().to_string_lossy().as_ref())),
            ..Default::default()
        })
        .await
        .expect("start session");
    let parent_sid = session.id.try_to_utf8().unwrap();

    let out = session
        .run(
            Input::Text("PARENT_SENTINEL_MARKER_9911".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(out.error.is_none(), "parent turn failed: {:?}", out.error);
    server.abort();

    let bodies = request_bodies.lock().unwrap();
    assert!(
        bodies.len() >= 5,
        "expected parent + two sub-agent requests, got {}",
        bodies.len()
    );

    // 1. Check 10,000-token (~40,000-char) creation-time tool output spill on Step 1:
    let step1_input_str = bodies[1]["input"].to_string();
    assert!(
        step1_input_str.len() < 45_000,
        "60,000-char tool output must be spilled under the 10,000-token model cap, got len={}",
        step1_input_str.len()
    );
    assert!(
        step1_input_str.contains("tool-results"),
        "spilled tool output must reference on-disk artifact path"
    );

    // 2. Same-model general-purpose fork sub-agent (bodies[2]) MUST keep parent's prompt_cache_key,
    // byte-identical input[0..2] (additional_tools + base_instructions), AND parent history!
    assert_eq!(
        bodies[2]["prompt_cache_key"], parent_sid,
        "same-model fork sub-agent must preserve parent prompt_cache_key instead of stripping it via skip_cache_write"
    );
    assert_eq!(
        bodies[2]["input"][0], bodies[0]["input"][0],
        "same-model fork sub-agent input[0] (additional_tools) must be byte-identical to parent"
    );
    assert_eq!(
        bodies[2]["input"][1], bodies[0]["input"][1],
        "same-model fork sub-agent input[1] (base_instructions) must be byte-identical to parent"
    );
    assert!(
        bodies[2]["input"]
            .to_string()
            .contains("PARENT_SENTINEL_MARKER_9911"),
        "same-model general-purpose fork sub-agent inherits parent prefix for cache reuse"
    );

    // 3. Specialized Explore sub-agent without explicit fork_turns (bodies[4]) MUST NOT replay parent history!
    assert!(
        !bodies[4]["input"]
            .to_string()
            .contains("PARENT_SENTINEL_MARKER_9911"),
        "specialized Explore sub-agent must not replay uncached parent history unless fork_turns is explicit"
    );
}
