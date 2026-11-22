//! Defect: Invoking a deferred MCP tool through the static `call_mcp_tool`
//! bridge emitted two full `ToolCall` stream items and two `tool_lifecycle`
//! sequences (one for the outer `call_mcp_tool` call ID and a second synthetic
//! UUID for the inner `invoke_tool` target), causing the live transcript UI to
//! render every indirect MCP action twice once `call_mcp_tool` rows were
//! unwrapped to their underlying target tool.
//! Root cause: `ToolContext::invoke_tool` unconditionally recorded a nested
//! `Requested`/`Started`/`Completed` `tool_lifecycle` and pushed a second
//! `EngineEvent::ItemStarted`/`ItemCompleted` `ThreadItem::ToolCall`, even when
//! the caller was the 1:1 `call_mcp_tool` wire bridge rather than a multi-call
//! script host.
//! Work item: 20261121 single-lifecycle indirect MCP dispatch.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, Session, TurnOptions};
use klaus_agent_core::mcp::tool_wrapper::{wrap_mcp_tool, McpToolCall, WrapMcpToolOptions};
use klaus_agent_core::mcp::types::{McpCallResult, McpContentBlock, McpToolDescriptor};
use klaus_agent_core::provider::gemini::{
    generate_content_request, GeminiAdapterPolicy, GeminiApiError, GeminiChunkStream, GeminiClient,
    GeminiProvider, GeminiProviderOptions, GenerateContentParameters, GenerateContentResponse,
};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeString};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::tools::builtins::read::create_read_tool;
use klaus_agent_core::tools::builtins::tool_search::create_tool_search_tool;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::events::{EngineEvent, ThreadItem, ToolDomainEvent};
use klaus_agent_core::types::provider::{AuthHeaders, ModelCapabilities};
use klaus_agent_core::types::tool::Tool;
use serde_json::{json, Value};

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

const FIXTURE_MODEL: &str = "gemini-indirect-mcp-single-lifecycle";

#[derive(Clone)]
enum ScriptedReply {
    Text(&'static str),
    ToolCall {
        id: &'static str,
        name: &'static str,
        args: Value,
    },
}

struct ScriptedGeminiClient {
    replies: Mutex<VecDeque<ScriptedReply>>,
}

impl ScriptedGeminiClient {
    fn new(replies: Vec<ScriptedReply>) -> Arc<Self> {
        Arc::new(Self {
            replies: Mutex::new(replies.into()),
        })
    }
}

#[async_trait::async_trait]
impl GeminiClient for ScriptedGeminiClient {
    async fn generate_content_stream(
        &self,
        params: GenerateContentParameters,
        _: Option<AuthHeaders>,
        _: Vec<(String, String)>,
        _: AbortSignal,
    ) -> Result<GeminiChunkStream, GeminiApiError> {
        let _ = generate_content_request(&params);
        let reply = self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(ScriptedReply::Text("done"));
        let parts = match reply {
            ScriptedReply::Text(text) => json!([{ "text": text }]),
            ScriptedReply::ToolCall { id, name, args } => json!([{
                "functionCall": {
                    "id": id,
                    "name": name,
                    "args": args,
                }
            }]),
        };
        let response: GenerateContentResponse = runtime_codec::decode_text(&RuntimeString::from(
            json!({
                "modelVersion": params.model.try_to_utf8().unwrap(),
                "candidates": [{
                    "content": { "role": "model", "parts": parts },
                    "finishReason": "STOP"
                }],
                "usageMetadata": {
                    "promptTokenCount": 128,
                    "candidatesTokenCount": 8,
                    "totalTokenCount": 136
                }
            })
            .to_string(),
        ))
        .unwrap();
        Ok(Box::pin(futures::stream::iter(vec![Ok(response)])))
    }
}

fn demo_mcp_tool(server: &str, name: &str, fail: bool) -> Arc<dyn Tool> {
    let label = format!("{server}:{name}");
    let call_label = label.clone();
    let call: McpToolCall = Arc::new(move |_tool, args, progress, _sig| {
        let label = call_label.clone();
        Box::pin(async move {
            if let Some(cb) = &progress {
                cb(ToolDomainEvent::Status {
                    message: format!("progress:{label}").into(),
                })
                .await;
            }
            let title = args
                .get("title")
                .and_then(|v| v.as_str())
                .and_then(|s| s.try_to_utf8().ok())
                .unwrap_or_default();
            Ok(McpCallResult {
                content: vec![McpContentBlock {
                    block_type: "text".into(),
                    text: Some(
                        json!({ "workspace": label, "title": title })
                            .to_string()
                            .into(),
                    ),
                    ..Default::default()
                }],
                is_error: Some(fail),
                structured_content: None,
                meta: None,
            })
        })
    });
    wrap_mcp_tool(
        server,
        McpToolDescriptor {
            name: name.into(),
            description: Some(format!("Demo tool {label}").into()),
            input_schema: Some(
                runtime_json::parse(&RuntimeString::from(
                    json!({
                        "type": "object",
                        "properties": {
                            "title": { "type": "string" }
                        },
                        "required": ["title"]
                    })
                    .to_string(),
                ))
                .unwrap(),
            ),
            ..Default::default()
        },
        call,
        Some(WrapMcpToolOptions::default()),
    )
}

async fn start_session(
    client: Arc<ScriptedGeminiClient>,
    tools: Vec<Arc<dyn Tool>>,
) -> (tempfile::TempDir, Arc<Session>) {
    let directory = tempfile::tempdir().unwrap();
    let provider = GeminiProvider::new(GeminiProviderOptions {
        default_model: Some(FIXTURE_MODEL.into()),
        client: Some(client),
        capabilities: Some(ModelCapabilities {
            context_window: Some(200_000),
            max_output_tokens: Some(1_024),
            tools: Some(true),
            ..Default::default()
        }),
        adapter_policy: GeminiAdapterPolicy {
            replay_signed_parts: true,
            scope_signatures_to_model: true,
            honor_tools_capability: true,
            model_tool_budget: true,
        },
        ..Default::default()
    });
    let mut options =
        agent_options(RuntimeProcessContext::initialize(process_supervisor()).unwrap());
    options.provider = Arc::new(provider);
    options.config = Some(json!({
        "model": FIXTURE_MODEL,
        "modelSource": {
            "source": "antigravity",
            "selection": format!("antigravity:{FIXTURE_MODEL}")
        },
        "prewarm": false,
        "autoMemory": false,
        "autoDream": false,
        "memoryRecall": false,
        "memory": { "enabled": false },
        "checkpointing": false,
        "speculativeCompaction": false,
        "earlyMicrocompact": false,
        "idleMicrocompact": false,
        "compactionSelfCheck": false,
        "loopDetection": false,
        "autoCompaction": true,
        "contextWindow": 200_000,
        "maxRetries": 1,
        "unboundedNetworkRetries": false,
        "totalTokensReminder": "off",
        "approval_policy": "never",
        "sandbox_mode": "danger-full-access"
    }));
    let sessions_dir = directory.path().join("sessions");
    std::fs::create_dir_all(&sessions_dir).unwrap();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(sessions_dir.to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    options.tools = Some(tools);
    options.include_default_agents = Some(false);
    let root = directory.path().to_string_lossy().into_owned();
    let session = Agent::new(options)
        .open_session(
            store,
            StartSessionOptions {
                cwd: Some(root.clone().into()),
                user_config_dir: Some(root.into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    (directory, session)
}

#[tokio::test]
async fn call_mcp_tool_emits_single_stream_item_and_lifecycle_per_invocation() {
    let client = ScriptedGeminiClient::new(vec![
        ScriptedReply::ToolCall {
            id: "search-1",
            name: "tool_search",
            args: json!({ "query": "select:mcp__demo__open,mcp__demo__fail" }),
        },
        ScriptedReply::ToolCall {
            id: "mcp-open-1",
            name: "call_mcp_tool",
            args: json!({
                "name": "mcp__demo__open",
                "arguments": { "title": "Retouch Corner" }
            }),
        },
        ScriptedReply::ToolCall {
            id: "mcp-fail-1",
            name: "call_mcp_tool",
            args: json!({
                "name": "mcp__demo__fail",
                "arguments": { "title": "Broken Step" }
            }),
        },
        ScriptedReply::Text("finished"),
    ]);

    let (dir, session) = start_session(
        client,
        vec![
            create_read_tool(),
            create_tool_search_tool(None),
            demo_mcp_tool("demo", "open", false),
            demo_mcp_tool("demo", "fail", true),
        ],
    )
    .await;

    let events = tokio::time::timeout(
        Duration::from_secs(15),
        session
            .run_streamed(
                Input::Text("Open workspace.".into()),
                TurnOptions::default(),
            )
            .events
            .collect::<Vec<_>>(),
    )
    .await
    .expect("turn completes within timeout");
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
    assert!(events
        .iter()
        .any(|event| matches!(event, EngineEvent::TurnCompleted { .. })));

    let mut started_items: Vec<(String, String)> = Vec::new();
    let mut terminal_items: Vec<(String, String, bool)> = Vec::new();
    let mut progress_ids: Vec<String> = Vec::new();
    let mut open_result: Option<Value> = None;
    for event in &events {
        match event {
            EngineEvent::ItemStarted {
                item: ThreadItem::ToolCall { id, tool_name, .. },
                ..
            } => {
                started_items.push((id.try_to_utf8().unwrap(), tool_name.try_to_utf8().unwrap()));
            }
            EngineEvent::ItemCompleted {
                item:
                    ThreadItem::ToolCall {
                        id,
                        tool_name,
                        result,
                        ..
                    },
                ..
            } => {
                let id_str = id.try_to_utf8().unwrap();
                if id_str == "mcp-open-1" {
                    open_result = result.as_ref().map(|r| {
                        serde_json::from_str(&runtime_json::stringify(r).try_to_utf8().unwrap())
                            .unwrap()
                    });
                }
                terminal_items.push((id_str, tool_name.try_to_utf8().unwrap(), false));
            }
            EngineEvent::ItemUpdated {
                item:
                    ThreadItem::ToolCall {
                        id,
                        tool_name,
                        error,
                        ..
                    },
                ..
            } if error.is_some() => {
                terminal_items.push((
                    id.try_to_utf8().unwrap(),
                    tool_name.try_to_utf8().unwrap(),
                    true,
                ));
            }
            EngineEvent::ToolEvent { tool_use_id, .. } => {
                progress_ids.push(tool_use_id.try_to_utf8().unwrap());
            }
            _ => {}
        }
    }

    assert_eq!(
        started_items,
        vec![
            ("search-1".to_string(), "tool_search".to_string()),
            ("mcp-open-1".to_string(), "call_mcp_tool".to_string()),
            ("mcp-fail-1".to_string(), "call_mcp_tool".to_string()),
        ],
        "indirect MCP bridge must not emit a duplicate inner ItemStarted"
    );
    assert_eq!(
        terminal_items,
        vec![
            ("search-1".to_string(), "tool_search".to_string(), false),
            ("mcp-open-1".to_string(), "call_mcp_tool".to_string(), false),
            ("mcp-fail-1".to_string(), "call_mcp_tool".to_string(), true),
        ],
        "indirect MCP bridge must emit exactly one terminal ToolCall item per model call"
    );
    assert_eq!(
        progress_ids,
        vec!["mcp-open-1".to_string(), "mcp-fail-1".to_string()],
        "nested MCP progress must attach to the outer call_mcp_tool toolUseId"
    );
    let open_result = open_result.expect("outer call_mcp_tool carries structured inner result");
    assert!(
        open_result.to_string().contains("Retouch Corner"),
        "outer call_mcp_tool result preserves inner MCP payload: {open_result}"
    );

    session.shutdown_and_wait().await.unwrap();

    let sessions_dir = dir.path().join("sessions");
    let jsonl_path = std::fs::read_dir(&sessions_dir)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| path.extension().and_then(|ext| ext.to_str()) == Some("jsonl"))
        .expect("session rollout jsonl written");
    let raw = std::fs::read_to_string(&jsonl_path).unwrap();
    let mut lifecycle_entries: Vec<(String, String, String)> = Vec::new();
    for line in raw.lines() {
        let parsed: Value = serde_json::from_str(line).unwrap();
        if parsed.get("event").and_then(Value::as_str) == Some("tool_lifecycle") {
            let payload = parsed.get("payload").unwrap();
            let id = payload["toolUseId"].as_str().unwrap().to_string();
            let name = payload["toolName"].as_str().unwrap().to_string();
            let phase = payload["phase"].as_str().unwrap().to_string();
            lifecycle_entries.push((id, name, phase));
        }
    }
    assert!(
        lifecycle_entries
            .iter()
            .all(|(id, name, _)| matches!(id.as_str(), "search-1" | "mcp-open-1" | "mcp-fail-1")
                && !name.starts_with("mcp__demo__")),
        "rollout tool_lifecycle must not contain duplicate synthetic inner MCP entries: {lifecycle_entries:?}"
    );
    assert!(
        lifecycle_entries
            .iter()
            .any(|(id, _, phase)| id == "mcp-open-1" && phase == "progress"),
        "progress lifecycle must bind to outer mcp-open-1: {lifecycle_entries:?}"
    );
}
