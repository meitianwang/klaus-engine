//! Defect: subagents spawned on model sources outside Claude subscription,
//! Codex subscription, and Antigravity suffered prompt-cache invalidation and
//! full uncached parent-history replay:
//! (1) `subagents_default_isolated_context()` only covered `subscription` and
//!     `antigravity`, while `diverges_from_parent_prefix` auto-downgrade only
//!     covered `codex`, so every other source replayed full parent history even
//!     when changing models or narrowing tools;
//! (2) `fork_trailing_role` only covered `codex`, so explicit forks on OpenAI
//!     and prefix-cached Anthropic-compatible sources mutated the leading system
//!     prefix with the child role block;
//! (3) forked OpenAI-wire subagents inherited `skip_cache_write: Some(true)`,
//!     stripping `prompt_cache_key` and emitting `prompt_cache_options: {"mode":"explicit"}`
//!     on write-billing models across multi-step tool loops.
//! Root cause: default subagent isolation, prefix-divergence auto-downgrade,
//! trailing role placement, and the OpenAI `skip_cache_write` guard were scoped
//! too narrowly by source name.
//! Work item: 20261124 non-Codex subagent prompt-cache guards.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::agent_definitions::AgentDefinition;
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::provider::anthropic::{AnthropicProvider, AnthropicProviderOptions};
use klaus_agent_core::provider::openai::{OpenAIProvider, OpenAIProviderOptions, OpenAiApi};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimePath;
use klaus_agent_core::schemas::config::EngineConfig;
use klaus_agent_core::types::provider::{ModelCapabilities, ModelProvider};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

async fn read_one_http_json_request(
    stream: &mut tokio::net::TcpStream,
) -> Option<(HashMap<String, String>, Value)> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
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
    let header_text = String::from_utf8_lossy(&buf[..header_end]);
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

async fn write_sse_ok(stream: &mut tokio::net::TcpStream, body: &str) {
    let response = format!(
        "HTTP/1.1 200 OK\r\n\
         content-type: text/event-stream\r\n\
         cache-control: no-cache\r\n\
         content-length: {}\r\n\
         connection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.shutdown().await;
}

fn openai_sse_function_call(resp_id: &str, call_id: &str, name: &str, args: &Value) -> String {
    let args_str = args.to_string();
    let events = [
        json!({"type": "response.created", "response": {"id": resp_id}}),
        json!({
            "type": "response.output_item.added",
            "item": {"id": format!("fc_{call_id}"), "type": "function_call", "call_id": call_id, "name": name}
        }),
        json!({
            "type": "response.function_call_arguments.delta",
            "item_id": format!("fc_{call_id}"),
            "call_id": call_id,
            "delta": args_str
        }),
        json!({
            "type": "response.output_item.done",
            "item": {
                "id": format!("fc_{call_id}"),
                "type": "function_call",
                "call_id": call_id,
                "name": name,
                "arguments": args_str,
                "status": "completed"
            }
        }),
        json!({
            "type": "response.completed",
            "response": {
                "id": resp_id,
                "status": "completed",
                "usage": {"input_tokens": 500, "output_tokens": 20, "total_tokens": 520}
            }
        }),
    ];
    events.iter().map(|ev| format!("data: {ev}\n\n")).collect()
}

fn openai_sse_text_reply(resp_id: &str, msg_id: &str, text: &str) -> String {
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
                "usage": {"input_tokens": 500, "output_tokens": 12, "total_tokens": 512}
            }
        }),
    ];
    events.iter().map(|ev| format!("data: {ev}\n\n")).collect()
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
                "cache_creation_input_tokens": 800,
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
                "cache_read_input_tokens": 800,
                "output_tokens": 24
            }
        }
    });
    let block_start = json!({
        "type": "content_block_start",
        "index": 0,
        "content_block": { "type": "tool_use", "id": id, "name": name, "input": {} }
    });
    let delta = json!({
        "type": "content_block_delta",
        "index": 0,
        "delta": { "type": "input_json_delta", "partial_json": args.to_string() }
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

#[test]
fn config_subagent_isolation_defaults_cover_all_non_codex_sources() {
    for source in [
        "subscription",
        "antigravity",
        "kimi",
        "bailian",
        "minimax",
        "zhipu",
        "ark",
        "qoder",
        "workbuddy",
        "cursor",
        "trae",
        "kiro",
        "openai",
        "anthropic",
        "openrouter",
        "gemini",
        "custom",
    ] {
        let cfg: EngineConfig = serde_json::from_value(json!({
            "modelSource": { "source": source }
        }))
        .unwrap();
        assert!(
            cfg.subagents_default_isolated_context(),
            "source `{source}` must default subagents to isolated context"
        );
        let expected_explore_low = matches!(source, "subscription" | "antigravity");
        assert_eq!(
            cfg.force_explore_low_tier(),
            expected_explore_low,
            "source `{source}` explore low-tier pinning mismatch"
        );
    }

    let codex: EngineConfig = serde_json::from_value(json!({
        "modelSource": { "source": "codex" }
    }))
    .unwrap();
    assert!(codex.is_codex_subscription_source());
    assert!(!codex.subagents_default_isolated_context());
    assert!(!codex.force_explore_low_tier());

    let unset = EngineConfig::default();
    assert!(!unset.is_codex_subscription_source());
    assert!(!unset.subagents_default_isolated_context());
    assert!(!unset.force_explore_low_tier());
}

#[tokio::test]
async fn openai_wire_forked_subagent_preserves_prompt_cache_key_and_trailing_role_without_explicit_mode(
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
        let mut seq = 0usize;
        while let Ok((mut sock, _)) = listener.accept().await {
            let Some((_headers, body)) = read_one_http_json_request(&mut sock).await else {
                continue;
            };
            seq += 1;
            let is_child = body["input"].as_array().is_some_and(|items| {
                items.iter().any(|item| {
                    item["role"] == "user"
                        && (item.to_string().contains("OAI_DEFAULT_CHILD_PROMPT")
                            || item.to_string().contains("OAI_FORK_CHILD_PROMPT"))
                })
            });
            captured_clone.lock().unwrap().push(body);
            let sse = if !is_child {
                match parent_step_clone.fetch_add(1, Ordering::SeqCst) {
                    0 => openai_sse_function_call(
                        &format!("resp_p{seq}"),
                        "call_oai_default",
                        "spawn_agent",
                        &json!({
                            "description": "default isolated child",
                            "prompt": "OAI_DEFAULT_CHILD_PROMPT",
                            "subagent_type": "general-purpose",
                            "model": "high",
                            "run_in_background": false
                        }),
                    ),
                    1 => openai_sse_function_call(
                        &format!("resp_p{seq}"),
                        "call_oai_fork",
                        "spawn_agent",
                        &json!({
                            "description": "explicit fork reviewer",
                            "prompt": "OAI_FORK_CHILD_PROMPT",
                            "subagent_type": "cache-auditor",
                            "model": "high",
                            "fork_turns": "all",
                            "run_in_background": false
                        }),
                    ),
                    _ => openai_sse_text_reply(
                        &format!("resp_p{seq}"),
                        &format!("msg_p{seq}"),
                        "Parent finished.",
                    ),
                }
            } else {
                openai_sse_text_reply(
                    &format!("resp_c{seq}"),
                    &format!("msg_c{seq}"),
                    "Child finished.",
                )
            };
            write_sse_ok(&mut sock, &sse).await;
        }
    });

    let caps = ModelCapabilities {
        context_window: Some(200_000),
        max_output_tokens: Some(16_000),
        explicit_prompt_cache_mode: Some(true),
        ..Default::default()
    };
    let mut by_model = HashMap::new();
    by_model.insert("gpt-5.6-sol".to_string(), caps);
    let provider: Arc<dyn ModelProvider> = Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
        api_key: Some("sk-synthetic".into()),
        base_url: Some(base_url),
        default_model: Some("gpt-5.6-sol".into()),
        api: Some(OpenAiApi::Responses),
        capabilities_by_model: Some(by_model),
        ..Default::default()
    }));

    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut opts = agent_options(process);
    opts.tools = None;
    opts.provider = provider;
    opts.agents = Some(vec![
        AgentDefinition {
            name: "general-purpose".into(),
            description: "General worker".into(),
            prompt: Some("GENERAL_PURPOSE_SYSTEM".into()),
            ..Default::default()
        },
        AgentDefinition {
            name: "cache-auditor".into(),
            description: "Audit prompt cache behavior".into(),
            prompt: Some("UNIQUE_AUDITOR_ROLE_SENTINEL_8841".into()),
            ..Default::default()
        },
    ]);
    opts.config = Some(json!({
        "model": "gpt-5.6-sol",
        "permissionMode": "bypassPermissions",
        "autoMemory": false,
        "autoDream": false,
        "modelTiers": {
            "low": "gpt-5.6-sol",
            "medium": "gpt-5.6-sol",
            "high": "gpt-5.6-sol"
        },
        "modelSource": {
            "source": "openai",
            "modelId": "gpt-5.6-sol"
        }
    }));

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
            Input::Text("PARENT_OPENAI_SENTINEL_6012".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(out.error.is_none(), "parent turn failed: {:?}", out.error);
    server.abort();

    let bodies = captured.lock().unwrap();
    let parent_req = &bodies[0];
    let parent_cache_key = parent_req["prompt_cache_key"]
        .as_str()
        .expect("parent OpenAI Responses request must carry prompt_cache_key");

    // 1. Default omitted fork_turns on source="openai" starts isolated.
    let default_child = bodies
        .iter()
        .find(|b| {
            b["input"].as_array().is_some_and(|items| {
                items.iter().any(|item| {
                    item["role"] == "user" && item.to_string().contains("OAI_DEFAULT_CHILD_PROMPT")
                })
            })
        })
        .expect("default isolated child request");
    assert!(
        !default_child["input"]
            .to_string()
            .contains("PARENT_OPENAI_SENTINEL_6012"),
        "default child on source=openai must not replay parent conversation"
    );

    // 2. Explicit fork_turns="all" on OpenAI wire keeps parent prompt_cache_key,
    //    omits `prompt_cache_options: {"mode":"explicit"}`, and emits child role
    //    instructions AFTER the inherited parent history rather than in leading instructions.
    let fork_child = bodies
        .iter()
        .find(|b| {
            b["input"].as_array().is_some_and(|items| {
                items.iter().any(|item| {
                    item["role"] == "user" && item.to_string().contains("OAI_FORK_CHILD_PROMPT")
                })
            })
        })
        .expect("explicit fork child request");
    assert!(
        fork_child["input"]
            .to_string()
            .contains("PARENT_OPENAI_SENTINEL_6012"),
        "explicit fork_turns=all child must inherit parent history"
    );
    assert_eq!(
        fork_child["prompt_cache_key"].as_str(),
        Some(parent_cache_key),
        "OpenAI-wire forked subagent must preserve parent prompt_cache_key"
    );
    assert!(
        fork_child.get("prompt_cache_options").is_none(),
        "OpenAI-wire forked subagent must not emit prompt_cache_options={{mode:explicit}}: {:?}",
        fork_child.get("prompt_cache_options")
    );
    assert!(
        !fork_child["instructions"]
            .as_str()
            .unwrap_or_default()
            .contains("UNIQUE_AUDITOR_ROLE_SENTINEL_8841"),
        "child role prompt must not mutate leading instructions on an OpenAI fork"
    );
    let fork_input = fork_child["input"].as_array().expect("input array");
    let parent_pos = fork_input
        .iter()
        .position(|item| item.to_string().contains("PARENT_OPENAI_SENTINEL_6012"))
        .expect("parent message in fork input");
    let role_pos = fork_input
        .iter()
        .position(|item| {
            item.to_string()
                .contains("UNIQUE_AUDITOR_ROLE_SENTINEL_8841")
        })
        .expect("trailing child role guidance in fork input");
    assert!(
        role_pos > parent_pos,
        "child role guidance (idx {role_pos}) must appear after inherited parent history (idx {parent_pos})"
    );
}

#[tokio::test]
async fn anthropic_compat_source_isolates_by_default_and_moves_fork_role_out_of_system_prefix() {
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
                .unwrap_or("kimi-for-coding")
                .to_string();
            let is_child = body["messages"].as_array().is_some_and(|msgs| {
                msgs.iter().any(|msg| {
                    msg["role"] == "user"
                        && (msg.to_string().contains("KIMI_DEFAULT_CHILD_PROMPT")
                            || msg.to_string().contains("KIMI_FORK_CHILD_PROMPT"))
                })
            });
            captured_clone.lock().unwrap().push(body);
            let sse = if !is_child {
                match parent_step_clone.fetch_add(1, Ordering::SeqCst) {
                    0 => anthropic_sse_tool_call(
                        &model,
                        "toolu_kimi_default",
                        "task",
                        &json!({
                            "description": "default isolated child",
                            "prompt": "KIMI_DEFAULT_CHILD_PROMPT",
                            "subagent_type": "general-purpose",
                            "model": "high",
                            "run_in_background": false
                        }),
                    ),
                    1 => anthropic_sse_tool_call(
                        &model,
                        "toolu_kimi_fork",
                        "task",
                        &json!({
                            "description": "explicit fork specialist",
                            "prompt": "KIMI_FORK_CHILD_PROMPT",
                            "subagent_type": "kimi-specialist",
                            "model": "high",
                            "fork_turns": "all",
                            "run_in_background": false
                        }),
                    ),
                    _ => anthropic_sse_text_reply(&model, "Parent done."),
                }
            } else {
                anthropic_sse_text_reply(&model, "Child done.")
            };
            write_sse_ok(&mut sock, &sse).await;
        }
    });

    let provider: Arc<dyn ModelProvider> =
        Arc::new(AnthropicProvider::new(AnthropicProviderOptions {
            api_key: Some("sk-kimi-synthetic".into()),
            base_url: Some(base_url),
            default_model: Some("kimi-for-coding".into()),
            ..Default::default()
        }));

    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut opts = agent_options(process);
    opts.tools = None;
    opts.provider = provider;
    opts.agents = Some(vec![
        AgentDefinition {
            name: "general-purpose".into(),
            description: "General worker".into(),
            prompt: Some("GENERAL_PURPOSE_SYSTEM".into()),
            ..Default::default()
        },
        AgentDefinition {
            name: "kimi-specialist".into(),
            description: "Specialist role".into(),
            prompt: Some("KIMI_SPECIALIST_ROLE_SENTINEL_7331".into()),
            ..Default::default()
        },
    ]);
    opts.config = Some(json!({
        "model": "kimi-for-coding",
        "permissionMode": "bypassPermissions",
        "autoMemory": false,
        "autoDream": false,
        "modelTiers": {
            "low": "kimi-for-coding",
            "medium": "kimi-for-coding",
            "high": "kimi-for-coding"
        },
        "modelSource": {
            "source": "kimi",
            "modelId": "kimi-for-coding"
        }
    }));

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
            Input::Text("PARENT_KIMI_SENTINEL_5519".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(out.error.is_none(), "parent turn failed: {:?}", out.error);
    server.abort();

    let bodies = captured.lock().unwrap();

    // 1. Default omitted fork_turns on source="kimi" starts isolated.
    let default_child = bodies
        .iter()
        .find(|b| {
            b["messages"].as_array().is_some_and(|msgs| {
                msgs.iter().any(|msg| {
                    msg["role"] == "user" && msg.to_string().contains("KIMI_DEFAULT_CHILD_PROMPT")
                })
            })
        })
        .expect("default isolated kimi child");
    assert!(
        !default_child["messages"]
            .to_string()
            .contains("PARENT_KIMI_SENTINEL_5519"),
        "default child on source=kimi must not replay parent conversation"
    );

    // 2. Explicit fork_turns="all" on source="kimi" keeps system[] identical to parent's
    //    stable prefix and delivers child role instructions in the trailing message stream.
    let fork_child = bodies
        .iter()
        .find(|b| {
            b["messages"].as_array().is_some_and(|msgs| {
                msgs.iter().any(|msg| {
                    msg["role"] == "user" && msg.to_string().contains("KIMI_FORK_CHILD_PROMPT")
                })
            })
        })
        .expect("explicit fork kimi child");
    assert!(
        fork_child["messages"]
            .to_string()
            .contains("PARENT_KIMI_SENTINEL_5519"),
        "explicit fork child must inherit parent history"
    );
    assert!(
        !fork_child["system"]
            .to_string()
            .contains("KIMI_SPECIALIST_ROLE_SENTINEL_7331"),
        "fork child role prompt must not mutate leading system[] on prefix-cached compat source"
    );
    assert!(
        fork_child["messages"]
            .to_string()
            .contains("KIMI_SPECIALIST_ROLE_SENTINEL_7331"),
        "fork child role prompt must be delivered in the trailing runtime-reminder snapshot"
    );
}

#[tokio::test]
async fn unset_model_source_auto_downgrades_divergent_subagent_prefix() {
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
            let msgs_str = body["messages"].to_string();
            let is_child = msgs_str.contains("UNSET_DIVERGENT_CHILD_PROMPT");
            captured_clone.lock().unwrap().push(body);
            let sse = if !is_child {
                match parent_step_clone.fetch_add(1, Ordering::SeqCst) {
                    0 => anthropic_sse_tool_call(
                        &model,
                        "toolu_unset_divergent",
                        "task",
                        &json!({
                            "description": "cross-model divergent child",
                            "prompt": "UNSET_DIVERGENT_CHILD_PROMPT",
                            "subagent_type": "Explore",
                            "model": "low",
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

    let provider: Arc<dyn ModelProvider> =
        Arc::new(AnthropicProvider::new(AnthropicProviderOptions {
            api_key: Some("sk-ant-synthetic".into()),
            base_url: Some(base_url),
            default_model: Some("claude-opus-4-6".into()),
            ..Default::default()
        }));

    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut opts = agent_options(process);
    opts.tools = None;
    opts.provider = provider;
    opts.agents = None;
    // No modelSource configured: default fork_turns is "all", but cross-model / Explore
    // diverges from parent prefix and must auto-downgrade instead of replaying full history.
    opts.config = Some(json!({
        "model": "claude-opus-4-6",
        "permissionMode": "bypassPermissions",
        "autoMemory": false,
        "autoDream": false,
        "modelTiers": {
            "low": "claude-haiku-4-5",
            "medium": "claude-sonnet-4-6",
            "high": "claude-opus-4-6"
        }
    }));

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
            Input::Text("PARENT_UNSET_SENTINEL_3141".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(out.error.is_none(), "parent turn failed: {:?}", out.error);
    server.abort();

    let bodies = captured.lock().unwrap();
    let child = bodies
        .iter()
        .find(|b| {
            b["messages"]
                .to_string()
                .contains("UNSET_DIVERGENT_CHILD_PROMPT")
        })
        .expect("divergent child request");
    assert_eq!(child["model"], "claude-haiku-4-5");
    assert!(
        !child["messages"]
            .to_string()
            .contains("PARENT_UNSET_SENTINEL_3141"),
        "divergent child without explicit fork_turns must auto-downgrade to isolated context"
    );
}
