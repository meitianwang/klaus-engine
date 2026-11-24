//! Defect: elevated quota consumption on Claude subscription sessions across
//! context editing, prompt cache TTL, deferred-tool directory reminders,
//! subagent context inheritance/model selection, and one-shot auxiliary calls.
//! Root cause: default `context_management` emitted unthresholded
//! `clear_tool_uses_20250919` on every request; Claude subscription sessions
//! defaulted to 5-minute cache TTL instead of 1-hour; deferred-tool directory
//! reminders were appended onto the uncached tail user message on every step;
//! Claude subscription subagents defaulted to full parent history inheritance
//! and caller-selected `Explore` model tiers; and one-shot selector/review
//! queries wrote ephemeral prompt caches unnecessarily.
//! Work item: 20261121 Claude subscription cache stability and subagent budget.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::stream;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::memory::agent_memory::{
    AgentMemoryHost, AgentMemoryLayout, AgentMemoryScope,
};
use klaus_agent_core::memory::store::{
    NewMemoryRecord, RecallSemanticOptions, TypedMemoryStore, TypedMemoryStoreOptions,
};
use klaus_agent_core::memory::types::MemoryType;
use klaus_agent_core::provider::anthropic::{AnthropicProvider, AnthropicProviderOptions};
use klaus_agent_core::provider::auth::AuthSource;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimePath, RuntimeString};
use klaus_agent_core::schemas::config::EngineConfig;
use klaus_agent_core::schemas::effort::EffortLevel;
use klaus_agent_core::tools::build_tool::{
    text_result, DEFAULT_IS_CONCURRENCY_SAFE, DEFAULT_IS_DESTRUCTIVE, DEFAULT_IS_ENABLED,
    DEFAULT_IS_READ_ONLY,
};
use klaus_agent_core::tools::builtins::read::create_read_tool;
use klaus_agent_core::tools::builtins::web_fetch::{
    create_web_fetch_tool, PageFetcher, PageResult, WebFetchToolOptions,
};
use klaus_agent_core::tools::builtins::web_fetch_redirect::FetchError;
use klaus_agent_core::tools::builtins::write::create_write_tool;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{ProviderError, ToolError};
use klaus_agent_core::types::json::Json;
use klaus_agent_core::types::provider::{
    CallModel, EffortCapability, ModelCapabilities, ModelProvider, ModelRequest, ModelStreamEvent,
    ThinkingCapability,
};
use klaus_agent_core::types::tool::{Tool, ToolContext, ToolExposure, ToolProgress, ToolResult};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

struct SyntheticClaudeOAuth {
    base_url: String,
}

#[async_trait]
impl AuthSource for SyntheticClaudeOAuth {
    async fn get_headers(
        &self,
        _signal: &AbortSignal,
    ) -> Result<HashMap<String, String>, ProviderError> {
        let mut headers = HashMap::new();
        headers.insert(
            "authorization".to_string(),
            "Bearer sk-ant-oat01-synthetic".to_string(),
        );
        headers.insert("anthropic-beta".to_string(), "oauth-2025-04-20".to_string());
        Ok(headers)
    }

    fn base_url(&self) -> Option<&str> {
        Some(&self.base_url)
    }

    fn is_subscription_oauth(&self) -> bool {
        true
    }
}

fn claude_capabilities() -> ModelCapabilities {
    ModelCapabilities {
        context_window: Some(200_000),
        max_output_tokens: Some(32_000),
        thinking: Some(ThinkingCapability {
            adaptive: true,
            interleaved: true,
            can_disable_thinking: true,
        }),
        effort: Some(EffortCapability {
            levels: vec![EffortLevel::Low, EffortLevel::Medium, EffortLevel::High],
            default: Some(EffortLevel::High),
        }),
        structured_output: true,
        vision: true,
        native_pdf: true,
        native_deferred_tools: None,
        ..Default::default()
    }
}

fn build_first_party_claude_provider(
    base_url: &str,
    default_model: &str,
) -> Arc<dyn ModelProvider> {
    let caps = claude_capabilities();
    let mut by_model = HashMap::new();
    by_model.insert("claude-opus-4-6".to_string(), caps.clone());
    by_model.insert("claude-sonnet-4-6".to_string(), caps.clone());
    by_model.insert("claude-haiku-4-5".to_string(), caps.clone());
    let mut provider = AnthropicProvider::new(AnthropicProviderOptions {
        default_model: Some(default_model.to_string()),
        auth_source: Some(Arc::new(SyntheticClaudeOAuth {
            base_url: base_url.to_string(),
        })),
        capabilities: Some(caps),
        capabilities_by_model: Some(by_model),
        ..Default::default()
    });
    provider.testing_set_first_party(true);
    Arc::new(provider)
}

#[derive(Clone)]
struct CapturedAnthropicCall {
    body: Value,
}

async fn read_one_http_json_request(
    stream: &mut tokio::net::TcpStream,
) -> Option<(HashMap<String, String>, Value)> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 8192];
    let header_end = loop {
        let n = stream.read(&mut tmp).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
    };
    let header_text = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut headers = HashMap::new();
    let mut content_length = 0usize;
    for line in header_text.lines().skip(1) {
        if let Some((k, v)) = line.split_once(':') {
            let key = k.trim().to_ascii_lowercase();
            let val = v.trim().to_string();
            if key == "content-length" {
                content_length = val.parse().unwrap_or(0);
            }
            headers.insert(key, val);
        }
    }
    while buf.len() < header_end + content_length {
        let n = stream.read(&mut tmp).await.ok()?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
    }
    let body: Value = serde_json::from_slice(&buf[header_end..header_end + content_length]).ok()?;
    Some((headers, body))
}

fn anthropic_sse_text_reply(model: &str, text: &str) -> String {
    let start = json!({
        "type": "message_start",
        "message": {
            "id": "msg_test",
            "type": "message",
            "role": "assistant",
            "model": model,
            "content": [],
            "stop_reason": Value::Null,
            "usage": {
                "input_tokens": 120,
                "cache_creation_input_tokens": 1000,
                "cache_read_input_tokens": 0,
                "output_tokens": 12
            }
        }
    });
    let block_start = json!({
        "type": "content_block_start",
        "index": 0,
        "content_block": { "type": "text", "text": "" }
    });
    let delta = json!({
        "type": "content_block_delta",
        "index": 0,
        "delta": { "type": "text_delta", "text": text }
    });
    let block_stop = json!({ "type": "content_block_stop", "index": 0 });
    let msg_delta = json!({
        "type": "message_delta",
        "delta": { "stop_reason": "end_turn" },
        "usage": { "output_tokens": 12 }
    });
    format!(
        "event: message_start\ndata: {start}\n\n\
         event: content_block_start\ndata: {block_start}\n\n\
         event: content_block_delta\ndata: {delta}\n\n\
         event: content_block_stop\ndata: {block_stop}\n\n\
         event: message_delta\ndata: {msg_delta}\n\n\
         event: message_stop\ndata: {{\"type\":\"message_stop\"}}\n\n"
    )
}

fn anthropic_sse_tool_call(model: &str, id: &str, name: &str, args: &Value) -> String {
    let start = json!({
        "type": "message_start",
        "message": {
            "id": "msg_tool",
            "type": "message",
            "role": "assistant",
            "model": model,
            "content": [],
            "stop_reason": Value::Null,
            "usage": {
                "input_tokens": 120,
                "cache_creation_input_tokens": 0,
                "cache_read_input_tokens": 1000,
                "output_tokens": 24
            }
        }
    });
    let block_start = json!({
        "type": "content_block_start",
        "index": 0,
        "content_block": {
            "type": "tool_use",
            "id": id,
            "name": name,
            "input": {}
        }
    });
    let delta = json!({
        "type": "content_block_delta",
        "index": 0,
        "delta": {
            "type": "input_json_delta",
            "partial_json": args.to_string()
        }
    });
    let block_stop = json!({ "type": "content_block_stop", "index": 0 });
    let msg_delta = json!({
        "type": "message_delta",
        "delta": { "stop_reason": "tool_use" },
        "usage": { "output_tokens": 24 }
    });
    format!(
        "event: message_start\ndata: {start}\n\n\
         event: content_block_start\ndata: {block_start}\n\n\
         event: content_block_delta\ndata: {delta}\n\n\
         event: content_block_stop\ndata: {block_stop}\n\n\
         event: message_delta\ndata: {msg_delta}\n\n\
         event: message_stop\ndata: {{\"type\":\"message_stop\"}}\n\n"
    )
}

async fn write_sse_ok(stream: &mut tokio::net::TcpStream, sse: &str) {
    let resp = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        sse.len(),
        sse
    );
    let _ = stream.write_all(resp.as_bytes()).await;
}

struct ToggleDeferredTool {
    name: &'static str,
    enabled: Arc<AtomicBool>,
}

#[async_trait]
impl Tool for ToggleDeferredTool {
    fn name(&self) -> RuntimeString {
        RuntimeString::from(self.name)
    }

    fn description(&self) -> RuntimeString {
        RuntimeString::from("Deferred test lookup tool")
    }

    fn input_schema(&self) -> Json {
        runtime_json::parse(&RuntimeString::from(
            r#"{"type":"object","properties":{"q":{"type":"string"}},"additionalProperties":false}"#,
        ))
        .expect("valid schema")
    }

    fn exposure(&self) -> ToolExposure {
        ToolExposure::Deferred
    }

    fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::SeqCst)
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        DEFAULT_IS_READ_ONLY
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        DEFAULT_IS_CONCURRENCY_SAFE
    }

    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        DEFAULT_IS_DESTRUCTIVE
    }

    async fn call(
        &self,
        _input: Json,
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        Ok(text_result(
            RuntimeJson::Null,
            RuntimeString::from("deferred-ok"),
            false,
        ))
    }
}

struct SimpleActiveTool;

#[async_trait]
impl Tool for SimpleActiveTool {
    fn name(&self) -> RuntimeString {
        RuntimeString::from("echo_probe")
    }

    fn description(&self) -> RuntimeString {
        RuntimeString::from("Active probe tool")
    }

    fn input_schema(&self) -> Json {
        runtime_json::parse(&RuntimeString::from(
            r#"{"type":"object","properties":{},"additionalProperties":false}"#,
        ))
        .expect("valid schema")
    }

    fn exposure(&self) -> ToolExposure {
        ToolExposure::Direct
    }

    fn is_enabled(&self) -> bool {
        DEFAULT_IS_ENABLED
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }

    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        Some(false)
    }

    async fn call(
        &self,
        _input: Json,
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        Ok(text_result(
            RuntimeJson::Null,
            RuntimeString::from("probe-result"),
            false,
        ))
    }
}

fn claude_subscription_config(model: &str) -> Value {
    json!({
        "model": model,
        "thinking": true,
        "effort": "high",
        "permissionMode": "bypassPermissions",
        "autoMemory": false,
        "autoDream": false,
        "modelTiers": {
            "low": "claude-haiku-4-5",
            "medium": "claude-sonnet-4-6",
            "high": "claude-opus-4-6"
        },
        "modelSource": {
            "source": "subscription",
            "modelId": model
        }
    })
}

#[tokio::test]
async fn claude_subscription_session_uses_1h_cache_omits_default_tool_clearing_and_freezes_deferred_directory(
) {
    let dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");

    let captured: Arc<Mutex<Vec<CapturedAnthropicCall>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_clone = captured.clone();
    let step = Arc::new(AtomicUsize::new(0));
    let step_clone = step.clone();

    let server = tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let Some((_headers, body)) = read_one_http_json_request(&mut sock).await else {
                continue;
            };
            let model = body["model"]
                .as_str()
                .unwrap_or("claude-opus-4-6")
                .to_string();
            captured_clone
                .lock()
                .unwrap()
                .push(CapturedAnthropicCall { body });
            let idx = step_clone.fetch_add(1, Ordering::SeqCst);
            let sse = match idx {
                0 => anthropic_sse_tool_call(
                    &model,
                    "toolu_search_1",
                    "tool_search",
                    &json!({ "query": "select:mcp__demo__lookup" }),
                ),
                1 => anthropic_sse_tool_call(&model, "toolu_probe_2", "echo_probe", &json!({})),
                2 => anthropic_sse_text_reply(&model, "Turn one finished."),
                _ => anthropic_sse_text_reply(&model, "Turn two finished."),
            };
            write_sse_ok(&mut sock, &sse).await;
        }
    });

    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut opts = agent_options(process);
    opts.provider = build_first_party_claude_provider(&base_url, "claude-opus-4-6");
    opts.config = Some(claude_subscription_config("claude-opus-4-6"));
    let lookup_enabled = Arc::new(AtomicBool::new(true));
    opts.tools = Some(vec![
        Arc::new(SimpleActiveTool),
        Arc::new(ToggleDeferredTool {
            name: "mcp__demo__lookup",
            enabled: lookup_enabled,
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
        .expect("start claude subscription session");

    let turn1 = session
        .run(
            Input::Text("first turn prompt".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(turn1.error.is_none(), "turn1 error: {:?}", turn1.error);

    let turn2 = session
        .run(
            Input::Text("second turn prompt".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(turn2.error.is_none(), "turn2 error: {:?}", turn2.error);
    server.abort();

    let calls = captured.lock().unwrap();
    assert_eq!(
        calls.len(),
        4,
        "expected 3 steps in turn1 + 1 step in turn2"
    );

    // 1. Default context_management omits clear_tool_uses_20250919 while retaining clear_thinking_20251015.
    for (idx, call) in calls.iter().enumerate() {
        let edits = call.body["context_management"]["edits"]
            .as_array()
            .expect("thinking context editing remains enabled");
        assert!(
            edits
                .iter()
                .all(|edit| edit["type"] != "clear_tool_uses_20250919"),
            "call {idx} must not emit unthresholded clear_tool_uses_20250919: {edits:?}"
        );
        assert!(
            edits
                .iter()
                .any(|edit| edit["type"] == "clear_thinking_20251015"),
            "call {idx} must retain clear_thinking_20251015: {edits:?}"
        );

        // 2. Claude subscription main session requests use 1h prompt cache TTL.
        let sys_blocks = call.body["system"].as_array().unwrap();
        assert!(
            sys_blocks
                .iter()
                .any(|block| block["cache_control"]["ttl"] == "1h"),
            "call {idx} system blocks must carry ttl=1h: {sys_blocks:?}"
        );
    }

    // 3. Deferred directory sits inside the cached prefix on messages[0], never mutates on tool_search
    // unlock, and is NOT repeated on trailing tool-result or follow-up user messages.
    let first_msg_step0 = &calls[0].body["messages"][0];
    let first_msg_blocks0 = first_msg_step0["content"].as_array().unwrap();
    let baseline_block0 = first_msg_blocks0
        .iter()
        .find(|block| {
            block["text"].as_str().is_some_and(|text| {
                text.contains("The following deferred tools are available via tool_search")
            })
        })
        .expect("step 0 messages[0] must carry the deferred directory block");
    assert!(
        baseline_block0["text"]
            .as_str()
            .unwrap()
            .contains("mcp__demo__lookup"),
        "step 0 deferred directory must list mcp__demo__lookup: {baseline_block0:?}"
    );
    assert_eq!(
        first_msg_blocks0.last().unwrap()["cache_control"]["ttl"],
        "1h",
        "step 0 messages[0] must place cache_control AFTER the deferred directory so it is cached"
    );

    // After tool_search unlocked mcp__demo__lookup on step 1, messages[0] text blocks stay byte-identical
    // (only Anthropic's rolling message-level cache_control marker advances onto the newest message).
    let strip_cache_control = |blocks: &Value| -> Value {
        Value::Array(
            blocks
                .as_array()
                .unwrap()
                .iter()
                .map(|b| {
                    let mut cloned = b.clone();
                    if let Some(obj) = cloned.as_object_mut() {
                        obj.remove("cache_control");
                    }
                    cloned
                })
                .collect(),
        )
    };
    let expected_msg0 = strip_cache_control(&calls[0].body["messages"][0]["content"]);
    for idx in [1usize, 2, 3] {
        assert_eq!(
            strip_cache_control(&calls[idx].body["messages"][0]["content"]),
            expected_msg0,
            "call {idx} messages[0] deferred directory must stay frozen after tool_search unlock"
        );
        let tail_msg = calls[idx].body["messages"]
            .as_array()
            .unwrap()
            .last()
            .unwrap();
        let tail_str = tail_msg.to_string();
        assert!(
            !tail_str.contains("available via tool_search"),
            "call {idx} trailing user message must not re-inject uncached deferred directory: {tail_str}"
        );
    }
}

#[tokio::test]
async fn deferred_directory_emits_compact_delta_only_when_session_tools_change() {
    let dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");

    let captured: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_clone = captured.clone();
    let server = tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let Some((_headers, body)) = read_one_http_json_request(&mut sock).await else {
                continue;
            };
            let model = body["model"]
                .as_str()
                .unwrap_or("claude-opus-4-6")
                .to_string();
            captured_clone.lock().unwrap().push(body);
            write_sse_ok(&mut sock, &anthropic_sse_text_reply(&model, "ok")).await;
        }
    });

    let alpha_enabled = Arc::new(AtomicBool::new(true));
    let beta_enabled = Arc::new(AtomicBool::new(false));

    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut opts = agent_options(process);
    opts.provider = build_first_party_claude_provider(&base_url, "claude-opus-4-6");
    opts.config = Some(claude_subscription_config("claude-opus-4-6"));
    opts.tools = Some(vec![
        Arc::new(SimpleActiveTool),
        Arc::new(ToggleDeferredTool {
            name: "mcp__demo__alpha",
            enabled: alpha_enabled.clone(),
        }),
        Arc::new(ToggleDeferredTool {
            name: "mcp__demo__beta",
            enabled: beta_enabled.clone(),
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
        .unwrap();

    session
        .run(Input::Text("turn 1".into()), TurnOptions::default())
        .await;
    // Dynamically enable mcp__demo__beta before Turn 2.
    beta_enabled.store(true, Ordering::SeqCst);
    session
        .run(Input::Text("turn 2".into()), TurnOptions::default())
        .await;
    // Turn 3 with no further change.
    session
        .run(Input::Text("turn 3".into()), TurnOptions::default())
        .await;
    server.abort();

    let bodies = captured.lock().unwrap();
    assert_eq!(bodies.len(), 3);

    // Turn 1 baseline stays frozen on messages[0] across all 3 turns.
    let strip_cc = |blocks: &Value| -> Value {
        Value::Array(
            blocks
                .as_array()
                .unwrap()
                .iter()
                .map(|b| {
                    let mut cloned = b.clone();
                    if let Some(obj) = cloned.as_object_mut() {
                        obj.remove("cache_control");
                    }
                    cloned
                })
                .collect(),
        )
    };
    let turn1_msg0 = strip_cc(&bodies[0]["messages"][0]["content"]);
    let turn1_msg0_str = turn1_msg0.to_string();
    assert!(turn1_msg0_str.contains("mcp__demo__alpha"));
    assert!(!turn1_msg0_str.contains("mcp__demo__beta"));
    assert_eq!(strip_cc(&bodies[1]["messages"][0]["content"]), turn1_msg0);
    assert_eq!(strip_cc(&bodies[2]["messages"][0]["content"]), turn1_msg0);

    // Turn 2 attaches ONLY the added delta onto Turn 2's user message inside the cached prefix.
    let turn2_last = bodies[1]["messages"].as_array().unwrap().last().unwrap();
    let turn2_text = turn2_last.to_string();
    assert!(
        turn2_text.contains("Deferred tools added") && turn2_text.contains("mcp__demo__beta"),
        "turn 2 must carry compact added delta: {turn2_text}"
    );
    assert_eq!(
        turn2_last["content"].as_array().unwrap().last().unwrap()["cache_control"]["ttl"],
        "1h",
        "turn 2 delta must sit inside the cached user block"
    );

    // Turn 3 has no inventory change, so Turn 3's user message carries zero deferred reminders.
    let turn3_last = bodies[2]["messages"].as_array().unwrap().last().unwrap();
    assert!(
        !turn3_last.to_string().contains("tool_search"),
        "turn 3 must not repeat delta when inventory is unchanged: {turn3_last}"
    );
}

#[tokio::test]
async fn claude_subscription_subagents_default_to_isolated_context_and_route_builtin_explore_to_low_tier(
) {
    let dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");

    let captured: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_clone = captured.clone();
    let parent_step = Arc::new(AtomicUsize::new(0));
    let parent_step_clone = parent_step.clone();

    let server = tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let Some((_headers, body)) = read_one_http_json_request(&mut sock).await else {
                continue;
            };
            let model = body["model"]
                .as_str()
                .unwrap_or("claude-opus-4-6")
                .to_string();
            let is_child = body["messages"]
                .as_array()
                .and_then(|msgs| msgs.last())
                .is_some_and(|last| last.to_string().contains("CHILD_"));
            let is_parent = !is_child;
            captured_clone.lock().unwrap().push(body);
            let sse = if is_parent {
                match parent_step_clone.fetch_add(1, Ordering::SeqCst) {
                    0 => anthropic_sse_tool_call(
                        &model,
                        "toolu_gp_default",
                        "task",
                        &json!({
                            "description": "isolated worker",
                            "prompt": "CHILD_DEFAULT_GP_PROMPT",
                            "subagent_type": "general-purpose",
                            "model": "medium",
                            "run_in_background": false
                        }),
                    ),
                    1 => anthropic_sse_tool_call(
                        &model,
                        "toolu_explore",
                        "task",
                        &json!({
                            "description": "explore repo",
                            "prompt": "CHILD_EXPLORE_PROMPT",
                            "subagent_type": "Explore",
                            "model": "high",
                            "run_in_background": false
                        }),
                    ),
                    2 => anthropic_sse_tool_call(
                        &model,
                        "toolu_gp_fork",
                        "task",
                        &json!({
                            "description": "explicit fork worker",
                            "prompt": "CHILD_EXPLICIT_FORK_PROMPT",
                            "subagent_type": "general-purpose",
                            "model": "high",
                            "fork_turns": "all",
                            "run_in_background": false
                        }),
                    ),
                    _ => anthropic_sse_text_reply(&model, "All subagents completed."),
                }
            } else {
                anthropic_sse_text_reply(&model, "Child completed.")
            };
            write_sse_ok(&mut sock, &sse).await;
        }
    });

    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut opts = agent_options(process);
    opts.tools = None;
    opts.provider = build_first_party_claude_provider(&base_url, "claude-opus-4-6");
    opts.config = Some(claude_subscription_config("claude-opus-4-6"));
    opts.agents = None;

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
        .run(
            Input::Text("PARENT_CLAUDE_SENTINEL_4488".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(out.error.is_none(), "parent turn failed: {:?}", out.error);
    server.abort();

    let bodies = captured.lock().unwrap();
    // Verify parent task tool schema advertises `none (default)` on Claude subscription.
    let task_tool = bodies[0]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| {
            tool["name"].as_str().is_some_and(|name| {
                name.eq_ignore_ascii_case("task") || name.eq_ignore_ascii_case("agent")
            })
        })
        .expect("task tool present");
    assert!(
        task_tool["input_schema"]["properties"]["fork_turns"]["description"]
            .as_str()
            .unwrap()
            .contains("none (default)"),
        "Claude subscription task schema must advertise none (default)"
    );

    // Child 1: general-purpose with omitted fork_turns -> isolated (no parent sentinel), 5m cache (no ttl=1h).
    let gp_default = bodies
        .iter()
        .find(|b| {
            b["messages"]
                .to_string()
                .contains("CHILD_DEFAULT_GP_PROMPT")
                && b["messages"].as_array().unwrap().len() == 1
        })
        .expect("isolated general-purpose child request");
    assert_eq!(gp_default["model"], "claude-sonnet-4-6");
    assert!(
        !gp_default["messages"]
            .to_string()
            .contains("PARENT_CLAUDE_SENTINEL_4488"),
        "Claude subscription subagent with omitted fork_turns must NOT inherit parent history"
    );
    assert!(
        !gp_default["system"].to_string().contains("\"1h\""),
        "isolated subagent must use standard 5m cache rather than 2x 1h cache write"
    );

    // Child 2: built-in Explore even when caller passed model="high" -> routed to low tier (claude-haiku-4-5) & isolated!
    let explore_child = bodies
        .iter()
        .find(|b| {
            b["messages"].to_string().contains("CHILD_EXPLORE_PROMPT")
                && b["messages"].as_array().unwrap().len() == 1
        })
        .expect("isolated Explore child request");
    assert_eq!(
        explore_child["model"], "claude-haiku-4-5",
        "built-in Explore on Claude subscription must route to low tier (claude-haiku-4-5)"
    );
    assert!(
        !explore_child["messages"]
            .to_string()
            .contains("PARENT_CLAUDE_SENTINEL_4488"),
        "Explore subagent must start in an isolated context"
    );

    // Child 3: explicit fork_turns="all" on same model -> inherits parent history, skips conversation cache write, keeps 1h system TTL!
    let explicit_fork = bodies
        .iter()
        .find(|b| {
            let last_is_child = b["messages"]
                .as_array()
                .and_then(|msgs| msgs.last())
                .is_some_and(|last| last.to_string().contains("CHILD_EXPLICIT_FORK_PROMPT"));
            last_is_child
                && b["messages"]
                    .to_string()
                    .contains("PARENT_CLAUDE_SENTINEL_4488")
        })
        .expect("explicit fork_turns=all child must inherit parent history");
    assert_eq!(explicit_fork["model"], "claude-opus-4-6");
    assert!(
        explicit_fork["system"].to_string().contains("\"1h\""),
        "explicit fork child must preserve parent 1h TTL on system blocks to hit parent warm cache"
    );
    let fork_msgs = explicit_fork["messages"].as_array().unwrap();
    assert!(
        !fork_msgs
            .last()
            .unwrap()
            .to_string()
            .contains("cache_control"),
        "explicit fork child must skip cache_control on its ephemeral tail message"
    );
}

#[tokio::test]
async fn non_claude_sources_preserve_5m_ttl_and_caller_selected_explore_tier() {
    let dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");

    let captured: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_clone = captured.clone();
    let parent_step = Arc::new(AtomicUsize::new(0));
    let parent_step_clone = parent_step.clone();

    let server = tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let Some((_headers, body)) = read_one_http_json_request(&mut sock).await else {
                continue;
            };
            let model = body["model"]
                .as_str()
                .unwrap_or("claude-opus-4-6")
                .to_string();
            let is_child = body["messages"]
                .as_array()
                .and_then(|msgs| msgs.last())
                .is_some_and(|last| {
                    let s = last.to_string();
                    s.contains("NON_CLAUDE_GP_CHILD") || s.contains("NON_CLAUDE_EXPLORE_CHILD")
                });
            let is_parent = !is_child;
            captured_clone.lock().unwrap().push(body);
            let sse = if is_parent {
                match parent_step_clone.fetch_add(1, Ordering::SeqCst) {
                    0 => anthropic_sse_tool_call(
                        &model,
                        "toolu_non_claude_gp",
                        "task",
                        &json!({
                            "description": "default fork worker",
                            "prompt": "NON_CLAUDE_GP_CHILD",
                            "subagent_type": "general-purpose",
                            "model": "high",
                            "run_in_background": false
                        }),
                    ),
                    1 => anthropic_sse_tool_call(
                        &model,
                        "toolu_non_claude_explore",
                        "task",
                        &json!({
                            "description": "high tier explore",
                            "prompt": "NON_CLAUDE_EXPLORE_CHILD",
                            "subagent_type": "Explore",
                            "model": "high",
                            "run_in_background": false
                        }),
                    ),
                    _ => anthropic_sse_text_reply(&model, "Done."),
                }
            } else {
                anthropic_sse_text_reply(&model, "Child done.")
            };
            write_sse_ok(&mut sock, &sse).await;
        }
    });

    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut opts = agent_options(process);
    opts.tools = None;
    opts.provider = build_first_party_claude_provider(&base_url, "claude-opus-4-6");
    // Custom API key / non-subscription source (`source: "custom"`): must NOT alter subagent defaults or TTL!
    opts.config = Some(json!({
        "model": "claude-opus-4-6",
        "thinking": true,
        "effort": "high",
        "permissionMode": "bypassPermissions",
        "autoMemory": false,
        "autoDream": false,
        "modelTiers": {
            "low": "claude-haiku-4-5",
            "medium": "claude-sonnet-4-6",
            "high": "claude-opus-4-6"
        },
        "modelSource": {
            "source": "custom",
            "modelId": "claude-opus-4-6"
        }
    }));
    opts.agents = None;

    let agent = Agent::new(opts);
    let session = agent
        .try_start_session_async(StartSessionOptions {
            cwd: Some(RuntimePath::from(dir.path().to_string_lossy().as_ref())),
            user_config_dir: Some(RuntimePath::from(cfg_dir.path().to_string_lossy().as_ref())),
            ..Default::default()
        })
        .await
        .unwrap();

    let out = session
        .run(
            Input::Text("PARENT_NON_CLAUDE_SENTINEL_7722".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(
        out.error.is_none(),
        "non-claude turn failed: {:?}",
        out.error
    );
    server.abort();

    let bodies = captured.lock().unwrap();
    // 1. Non-subscription session keeps standard 5m cache TTL (no ttl="1h") and
    //    defaults to isolated subagents (`none (default)`), since only Codex
    //    shares byte-identical prefix caches across default forks.
    assert!(
        !bodies[0]["system"].to_string().contains("\"1h\""),
        "non-subscription session must not force 1h cache TTL"
    );
    let task_tool = bodies[0]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| {
            tool["name"].as_str().is_some_and(|name| {
                name.eq_ignore_ascii_case("task") || name.eq_ignore_ascii_case("agent")
            })
        })
        .unwrap();
    assert!(
        task_tool["input_schema"]["properties"]["fork_turns"]["description"]
            .as_str()
            .unwrap()
            .contains("none (default)"),
        "configured non-Codex task schema must advertise none (default)"
    );

    // 2. Non-Codex general-purpose with omitted fork_turns starts isolated.
    let gp_child = bodies
        .iter()
        .find(|b| {
            b["messages"]
                .as_array()
                .and_then(|msgs| msgs.last())
                .is_some_and(|last| last.to_string().contains("NON_CLAUDE_GP_CHILD"))
        })
        .expect("non-claude general-purpose child");
    assert!(
        !gp_child["messages"]
            .to_string()
            .contains("PARENT_NON_CLAUDE_SENTINEL_7722"),
        "configured non-Codex subagent with omitted fork_turns must start isolated"
    );

    // 3. Non-subscription Explore with model="high" STILL uses caller's "high" model (claude-opus-4-6), NOT low!
    let explore_child = bodies
        .iter()
        .find(|b| {
            b["messages"]
                .as_array()
                .and_then(|msgs| msgs.last())
                .is_some_and(|last| last.to_string().contains("NON_CLAUDE_EXPLORE_CHILD"))
        })
        .expect("non-claude Explore child");
    assert_eq!(
        explore_child["model"], "claude-opus-4-6",
        "non-subscription Explore must respect the caller-selected model tier instead of forcing low"
    );
}

#[tokio::test]
async fn one_shot_memory_recall_skips_cache_write_and_claude_subscription_aggregates_tool_results()
{
    let tmp = tempfile::tempdir().unwrap();
    let store = TypedMemoryStore::new(TypedMemoryStoreOptions {
        dir: Some(RuntimePath::from(tmp.path().to_string_lossy().as_ref())),
        redact_secrets: Some(false),
    });
    store
        .write(&NewMemoryRecord {
            name: "build-guide".into(),
            description: "Workspace build commands".into(),
            memory_type: MemoryType::Project,
            body: "Run targeted tests first.".into(),
        })
        .await
        .unwrap();

    let captured_skip: Arc<Mutex<Option<Option<bool>>>> = Arc::new(Mutex::new(None));
    let captured_skip_clone = captured_skip.clone();
    let call_model: CallModel = Box::new(move |req: ModelRequest, _sig: AbortSignal| {
        *captured_skip_clone.lock().unwrap() = Some(req.skip_cache_write);
        Box::pin(stream::iter(vec![ModelStreamEvent::TextDelta {
            text: "build-guide".into(),
        }]))
    });

    let recalled = store
        .recall_semantic(
            &RuntimeString::from("how do I build"),
            RecallSemanticOptions {
                call_model: &call_model,
                model: RuntimeString::from("claude-haiku-4-5"),
                signal: None,
                limit: Some(3),
                recent_tools: None,
                exclude: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(recalled.len(), 1);
    assert_eq!(
        *captured_skip.lock().unwrap(),
        Some(Some(true)),
        "one-shot memory recall selector must set skip_cache_write=Some(true)"
    );

    let claude_cfg: EngineConfig = serde_json::from_value(json!({
        "modelSource": { "source": "subscription" }
    }))
    .unwrap();
    assert!(claude_cfg.is_claude_subscription_source());
    assert!(claude_cfg.effective_aggregate_tool_results());

    let codex_cfg: EngineConfig = serde_json::from_value(json!({
        "modelSource": { "source": "codex" }
    }))
    .unwrap();
    assert!(!codex_cfg.is_claude_subscription_source());
    assert!(!codex_cfg.effective_aggregate_tool_results());

    let unset_cfg = EngineConfig::default();
    assert!(!unset_cfg.is_claude_subscription_source());
    assert!(!unset_cfg.effective_aggregate_tool_results());
}

struct StaticPageFetcher;

#[async_trait]
impl PageFetcher for StaticPageFetcher {
    async fn fetch_page(
        &self,
        url: &RuntimeString,
        _signal: &AbortSignal,
        _max_chars: usize,
    ) -> Result<PageResult, FetchError> {
        Ok(PageResult {
            text: RuntimeString::from("# Release Notes\n\nVersion 3.1.0 ships today."),
            final_url: Some(url.clone()),
            status: Some(200),
            truncated: Some(false),
        })
    }
}

struct TempMemoryHost {
    dir: String,
}

impl AgentMemoryLayout for TempMemoryHost {
    fn dir_for(&self, _agent_type: &str, _scope: AgentMemoryScope) -> String {
        self.dir.clone()
    }

    fn snapshot_dir_for(&self, _agent_type: &str) -> String {
        self.dir.clone()
    }
}

impl AgentMemoryHost for TempMemoryHost {
    fn layout(&self) -> &dyn AgentMemoryLayout {
        self
    }
}

#[tokio::test]
async fn multi_step_memory_extraction_keeps_ephemeral_cache_while_web_fetch_and_lean_compaction_shed_dead_tokens(
) {
    let dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let mem_dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("AGENTS.md"),
        "# Workspace Policy\n\nKeep code concise.\n",
    )
    .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");

    let captured: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_clone = captured.clone();
    let parent_step = Arc::new(AtomicUsize::new(0));
    let parent_step_clone = parent_step.clone();
    let mem_step = Arc::new(AtomicUsize::new(0));
    let mem_step_clone = mem_step.clone();
    let compact_step = Arc::new(AtomicUsize::new(0));
    let compact_step_clone = compact_step.clone();
    let mem_root_for_server = mem_dir.path().to_path_buf();

    let server = tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let Some((_headers, body)) = read_one_http_json_request(&mut sock).await else {
                continue;
            };
            let model = body["model"]
                .as_str()
                .unwrap_or("claude-opus-4-6")
                .to_string();
            let last_msg_str = body["messages"]
                .as_array()
                .and_then(|msgs| msgs.last())
                .map(|m| m.to_string())
                .unwrap_or_default();
            let tools_len = body["tools"].as_array().map(Vec::len).unwrap_or(0);
            captured_clone.lock().unwrap().push(body.clone());

            let sse = if last_msg_str.contains("Answer the user's prompt using only the Markdown") {
                // One-shot web_fetch prompt processor (zero tools).
                anthropic_sse_text_reply(&model, "Version 3.1.0 ships today.")
            } else if last_msg_str.contains("session-memory")
                || last_msg_str.contains("session_memory")
                || body["messages"].to_string().contains("session-memory")
            {
                // Multi-step session memory update fork: step 1 writes notes file, step 2 finishes.
                match mem_step_clone.fetch_add(1, Ordering::SeqCst) {
                    0 => {
                        let sm_dir = mem_root_for_server.join("session-memory");
                        let notes_path = std::fs::read_dir(&sm_dir)
                            .ok()
                            .and_then(|mut it| it.next())
                            .and_then(|entry| entry.ok())
                            .map(|entry| entry.path().join("summary.md"))
                            .unwrap_or_else(|| mem_root_for_server.join("MEMORY.md"))
                            .to_string_lossy()
                            .into_owned();
                        anthropic_sse_tool_call(
                            &model,
                            "toolu_mem_write",
                            "write",
                            &json!({
                                "path": notes_path,
                                "content": "# Session Notes\n\n- Verified release 3.1.0.\n"
                            }),
                        )
                    }
                    _ => anthropic_sse_text_reply(&model, "Updated session notes."),
                }
            } else if last_msg_str.contains("CUSTOM_COMPACT_SENTINEL_8899") {
                // Manual compaction: Stage 1 stops on a tool_use to force Stage 2 lean fallback.
                match compact_step_clone.fetch_add(1, Ordering::SeqCst) {
                    0 if tools_len > 0 => anthropic_sse_tool_call(
                        &model,
                        "toolu_unexpected_in_compact",
                        "echo_probe",
                        &json!({}),
                    ),
                    _ => anthropic_sse_text_reply(
                        &model,
                        "<summary>Compacted conversation summary for release 3.1.0.</summary>",
                    ),
                }
            } else {
                match parent_step_clone.fetch_add(1, Ordering::SeqCst) {
                    0 => anthropic_sse_tool_call(
                        &model,
                        "toolu_webfetch",
                        "web_fetch",
                        &json!({
                            "url": "https://docs.example/releases",
                            "prompt": "What version ships today?"
                        }),
                    ),
                    _ => anthropic_sse_text_reply(&model, "Confirmed release 3.1.0."),
                }
            };
            write_sse_ok(&mut sock, &sse).await;
        }
    });

    let alpha_enabled = Arc::new(AtomicBool::new(true));
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut opts = agent_options(process);
    opts.provider = build_first_party_claude_provider(&base_url, "claude-opus-4-6");
    let mut cfg = claude_subscription_config("claude-opus-4-6");
    cfg["agentMemory"] = json!(true);
    opts.config = Some(cfg);
    opts.agent_memory = Some(Arc::new(TempMemoryHost {
        dir: mem_dir.path().to_string_lossy().into_owned(),
    }));
    opts.tools = Some(vec![
        Arc::new(SimpleActiveTool),
        create_read_tool(),
        create_write_tool(),
        create_web_fetch_tool(WebFetchToolOptions {
            fetch_page: Some(Arc::new(StaticPageFetcher)),
            ..Default::default()
        }),
        Arc::new(ToggleDeferredTool {
            name: "mcp__demo__alpha",
            enabled: alpha_enabled,
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

    // Turn 1 triggers web_fetch (which spawns a one-shot zero-tool helper).
    let turn1 = session
        .run(
            Input::Text("TURN1_PERSISTED_SENTINEL_8811".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(turn1.error.is_none(), "turn 1 failed: {:?}", turn1.error);

    // Turn 2 verifies messages[0] with AGENTS.md + persisted user anchor stays byte-identical.
    let turn2 = session
        .run(
            Input::Text("TURN2_FOLLOWUP_8812".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(turn2.error.is_none(), "turn 2 failed: {:?}", turn2.error);

    // Manual compaction first (before session-memory fusion summarizes history):
    // Stage 1 hits tool_use stop -> Stage 2 lean fallback runs tool-less.
    let compact_res = session
        .compact(Some("CUSTOM_COMPACT_SENTINEL_8899".into()))
        .await;
    assert!(
        compact_res.is_ok() && compact_res.as_ref().unwrap().compacted,
        "manual compact failed: {compact_res:?}"
    );

    // Multi-step session memory update must retain 5-minute intra-run caching on Step 1.
    let notes_res = session.extract_session_memory_now().await;
    assert!(
        notes_res.is_ok(),
        "extract_session_memory_now failed: {notes_res:?}"
    );
    server.abort();

    let bodies = captured.lock().unwrap();

    // 1. Workspace instructions + persisted Turn 1 user message + deferred baseline order is
    //    `[AGENTS.md, TURN1_PERSISTED_SENTINEL_8811, <system-reminder>Deferred tools...]`
    //    and remains byte-identical (ignoring cache_control) across Turn 1 and Turn 2.
    let strip_cc = |blocks: &Value| -> Value {
        Value::Array(
            blocks
                .as_array()
                .unwrap()
                .iter()
                .map(|b| {
                    let mut cloned = b.clone();
                    if let Some(obj) = cloned.as_object_mut() {
                        obj.remove("cache_control");
                    }
                    cloned
                })
                .collect(),
        )
    };
    let parent_turn1 = &bodies[0];
    let msg0_blocks = parent_turn1["messages"][0]["content"].as_array().unwrap();
    let workspace_pos = msg0_blocks
        .iter()
        .position(|b| {
            b["text"]
                .as_str()
                .is_some_and(|t| t.contains("Workspace Policy"))
        })
        .expect("AGENTS.md projection block in messages[0]");
    let user1_pos = msg0_blocks
        .iter()
        .position(|b| {
            b["text"]
                .as_str()
                .is_some_and(|t| t.contains("TURN1_PERSISTED_SENTINEL_8811"))
        })
        .expect("persisted Turn 1 user block in messages[0]");
    let deferred_pos = msg0_blocks
        .iter()
        .position(|b| {
            b["text"]
                .as_str()
                .is_some_and(|t| t.contains("mcp__demo__alpha"))
        })
        .expect("deferred baseline block in messages[0]");
    assert!(
        workspace_pos < user1_pos && user1_pos < deferred_pos,
        "deferred baseline must anchor after the persisted user block rather than before it: workspace={workspace_pos}, user1={user1_pos}, deferred={deferred_pos}"
    );
    let parent_turn2 = bodies
        .iter()
        .find(|b| {
            b["messages"]
                .as_array()
                .and_then(|m| m.last())
                .is_some_and(|last| last.to_string().contains("TURN2_FOLLOWUP_8812"))
        })
        .expect("parent turn 2 request");
    assert_eq!(
        strip_cc(&parent_turn2["messages"][0]["content"]),
        strip_cc(&parent_turn1["messages"][0]["content"]),
        "messages[0] content must stay byte-identical across turns"
    );

    // 2. One-shot web_fetch prompt helper must skip trailing message cache_control write
    //    and must not inherit parent conversation history.
    let web_fetch_req = bodies
        .iter()
        .find(|b| {
            b["messages"]
                .to_string()
                .contains("Answer the user's prompt using only the Markdown")
        })
        .expect("web_fetch helper request");
    assert!(
        !web_fetch_req["messages"]
            .to_string()
            .contains("TURN1_PERSISTED_SENTINEL_8811"),
        "web_fetch one-shot helper must not inherit parent history"
    );
    let wf_last_block = web_fetch_req["messages"]
        .as_array()
        .unwrap()
        .last()
        .unwrap()["content"]
        .as_array()
        .unwrap()
        .last()
        .unwrap();
    assert!(
        wf_last_block.get("cache_control").is_none(),
        "one-shot web_fetch helper must set skip_cache_write so its ephemeral tail is not cached: {wf_last_block}"
    );

    // 3. Multi-step session memory update must write a 5m message cache breakpoint on Step 1
    //    so Step 2+ reuses the prefix instead of re-prefilling uncached on every step.
    let mem_step1 = bodies
        .iter()
        .find(|b| {
            b["messages"].as_array().is_some_and(|msgs| {
                msgs.len() == 1 && b["messages"].to_string().contains("session-memory")
            })
        })
        .expect("session memory step 1 request");
    let mem_step1_last_block = mem_step1["messages"][0]["content"]
        .as_array()
        .unwrap()
        .last()
        .unwrap();
    assert_eq!(
        mem_step1_last_block["cache_control"]["type"], "ephemeral",
        "multi-step memory fork must keep 5m intra-run prompt caching enabled on step 1: {mem_step1_last_block}"
    );
    assert!(
        mem_step1_last_block["cache_control"].get("ttl").is_none(),
        "isolated memory fork must use 5m cache rather than 1h cache: {mem_step1_last_block}"
    );

    // 4. Stage 1 compaction preserves deferred announcements for prefix reuse, while
    //    Stage 2 lean compaction (tools == []) strips `<system-reminder>Deferred tools...`.
    let stage1_compact_req = bodies
        .iter()
        .find(|b| {
            b.get("tools")
                .and_then(Value::as_array)
                .is_some_and(|t| !t.is_empty())
                && b["messages"]
                    .to_string()
                    .contains("CUSTOM_COMPACT_SENTINEL_8899")
        })
        .expect("stage 1 cache-preserving compaction request");
    assert!(
        stage1_compact_req["messages"]
            .to_string()
            .contains("mcp__demo__alpha"),
        "Stage 1 compaction must retain deferred-tool baseline for 1h prefix cache hit"
    );
    let lean_compact_req = bodies
        .iter()
        .find(|b| {
            b.get("tools")
                .is_none_or(|t| t.as_array().is_some_and(Vec::is_empty))
                && b["messages"]
                    .to_string()
                    .contains("CUSTOM_COMPACT_SENTINEL_8899")
        })
        .expect("stage 2 tool-less lean compaction request");
    assert!(
        !lean_compact_req["messages"]
            .to_string()
            .contains("mcp__demo__alpha"),
        "tool-less Stage 2 lean compaction must strip deferred-tool directory announcements: {lean_compact_req}"
    );
}
