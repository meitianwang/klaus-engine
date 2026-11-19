//! Defect: Antigravity Gemini sessions suffered recurring full-prefix prompt
//! cache misses from three client-side invalidation paths:
//! (1) `:sub:` sub-agents (including default fork sub-agents) and `:readonly:`
//!     side queries reused the parent conversation's `cache_key` / wire
//!     `sessionId`, overwriting the parent's upstream KV cache slot;
//! (2) non-cacheable (`cacheable == Some(false)`) system blocks such as
//!     `collaboration-mode`, `permissions`, and `multi_agent` stayed inside
//!     `systemInstruction` (or were re-injected after reminder projection);
//! (3) `Deferred` MCP/runtime tools hid behind `tool_search` and appended onto
//!     the leading `tools` array mid-conversation, invalidating everything
//!     behind `tools`.
//! Root cause: `GeminiProvider` dropped `ModelRequest.cache_key`, `request.rs`
//! inherited parent `cache_key` on Gemini sub-threads and re-applied
//! `request_policy_with_model_messages` after `runtime_reminders::project`,
//! `runtime_reminders::is_dynamic` only matched `reminder`/`hook-context`, and
//! `visible_tool_specs_for_step` deferred tools even when the full inventory
//! fit comfortably inside the Gemini input window.
//! Work item: 20261117 Antigravity Gemini prompt-cache stabilization.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, RuntimePolicyUpdate, Session, TurnOptions};
use klaus_agent_core::mcp::tool_wrapper::{wrap_mcp_tool, McpToolCall, WrapMcpToolOptions};
use klaus_agent_core::mcp::types::{McpCallResult, McpToolDescriptor};
use klaus_agent_core::planning::CollaborationMode;
use klaus_agent_core::provider::gemini::{
    generate_content_request, GeminiAdapterPolicy, GeminiApiError, GeminiChunkStream, GeminiClient,
    GeminiProvider, GeminiProviderOptions, GenerateContentParameters, GenerateContentResponse,
};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeString};
use klaus_agent_core::tools::builtins::read::create_read_tool;
use klaus_agent_core::tools::builtins::task::{create_task_tool, TaskToolOptions};
use klaus_agent_core::tools::builtins::tool_search::create_tool_search_tool;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::permission::{ApprovalPolicy, SandboxMode};
use klaus_agent_core::types::provider::{AuthHeaders, ModelCapabilities};
use klaus_agent_core::types::tool::Tool;
use serde_json::{json, Value};

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

const GEMINI_MODEL: &str = "gemini-cache-stability-fixture";

#[derive(Clone)]
enum ScriptedReply {
    Text(&'static str),
    ToolCall {
        id: &'static str,
        name: &'static str,
        args: Value,
    },
}

#[derive(Clone, Debug)]
struct CapturedGeminiCall {
    cache_key: Option<String>,
    wire: Value,
}

struct ScriptedGeminiClient {
    calls: Mutex<Vec<CapturedGeminiCall>>,
    replies: Mutex<VecDeque<ScriptedReply>>,
}

impl ScriptedGeminiClient {
    fn new(replies: Vec<ScriptedReply>) -> Arc<Self> {
        Arc::new(Self {
            calls: Mutex::new(Vec::new()),
            replies: Mutex::new(replies.into()),
        })
    }

    fn taken(&self) -> Vec<CapturedGeminiCall> {
        self.calls.lock().unwrap().clone()
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
        let wire_json = generate_content_request(&params);
        let wire: Value =
            serde_json::from_str(&runtime_json::stringify(&wire_json).try_to_utf8().unwrap())
                .unwrap();
        self.calls.lock().unwrap().push(CapturedGeminiCall {
            cache_key: params
                .routing_session_key
                .as_ref()
                .and_then(|key| key.try_to_utf8().ok()),
            wire,
        });
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
                    "promptTokenCount": 256,
                    "candidatesTokenCount": 8
                }
            })
            .to_string(),
        ))
        .unwrap();
        Ok(Box::pin(futures::stream::iter(vec![Ok(response)])))
    }
}

fn start_antigravity_gemini_session(
    client: Arc<ScriptedGeminiClient>,
    tools: Vec<Arc<dyn Tool>>,
    context_window: u64,
    model_messages: Option<Value>,
) -> (tempfile::TempDir, Arc<Session>) {
    let directory = tempfile::tempdir().unwrap();
    let provider = GeminiProvider::new(GeminiProviderOptions {
        default_model: Some(GEMINI_MODEL.into()),
        client: Some(client),
        capabilities: Some(ModelCapabilities {
            context_window: Some(context_window),
            max_output_tokens: Some(1_024),
            tools: Some(true),
            model_messages: model_messages
                .map(|value| runtime_json::parse(&RuntimeString::from(value.to_string())).unwrap()),
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
        "model": GEMINI_MODEL,
        "modelSource": {
            "source": "antigravity",
            "selection": format!("antigravity:{GEMINI_MODEL}")
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
        "autoCompaction": context_window >= 10_000,
        "contextWindow": context_window,
        "maxRetries": 1,
        "unboundedNetworkRetries": false,
        "totalTokensReminder": "off",
        "approval_policy": "never",
        "sandbox_mode": "danger-full-access"
    }));
    options.tools = Some(tools);
    options.include_default_agents = Some(true);
    let root = directory.path().to_string_lossy().into_owned();
    let session = Agent::new(options).start_session(StartSessionOptions {
        cwd: Some(root.clone().into()),
        user_config_dir: Some(root.into()),
        ..Default::default()
    });
    (directory, session)
}

fn demo_mcp_tool_tracked(
    server: &str,
    name: &str,
    description: &str,
    invocations: Arc<Mutex<Vec<String>>>,
) -> Arc<dyn Tool> {
    let label = format!("{server}:{name}");
    let call: McpToolCall = Arc::new(move |_tool, args, _progress, _sig| {
        let invocations = Arc::clone(&invocations);
        let label = label.clone();
        Box::pin(async move {
            let q = args
                .get("q")
                .and_then(|v| v.as_str())
                .and_then(|s| s.try_to_utf8().ok())
                .unwrap_or_default();
            invocations.lock().unwrap().push(format!("{label}={q}"));
            Ok(McpCallResult {
                content: vec![klaus_agent_core::mcp::types::McpContentBlock {
                    block_type: "text".into(),
                    text: Some(format!("ack:{label}:{q}").into()),
                    ..Default::default()
                }],
                is_error: Some(false),
                structured_content: None,
                meta: None,
            })
        })
    });
    wrap_mcp_tool(
        server,
        McpToolDescriptor {
            name: name.into(),
            description: Some(description.into()),
            input_schema: Some(
                runtime_json::parse(&RuntimeString::from(
                    json!({
                        "type": "object",
                        "properties": {
                            "q": { "type": "string", "description": description }
                        },
                        "required": ["q"]
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

fn demo_mcp_tool(server: &str, name: &str, description: &str) -> Arc<dyn Tool> {
    let call: McpToolCall = Arc::new(|_, _, _, _| Box::pin(async { Ok(McpCallResult::default()) }));
    wrap_mcp_tool(
        server,
        McpToolDescriptor {
            name: name.into(),
            description: Some(description.into()),
            input_schema: Some(
                runtime_json::parse(&RuntimeString::from(
                    json!({
                        "type": "object",
                        "properties": {
                            "q": { "type": "string", "description": description }
                        }
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

#[tokio::test]
async fn subagents_and_readonly_queries_isolate_gemini_cache_keys_from_parent() {
    let file_target = "note.txt";
    let client = ScriptedGeminiClient::new(vec![
        // Parent step 1 spawns a default (`fork_turns: "all"`) sub-agent.
        ScriptedReply::ToolCall {
            id: "call-fork-sub",
            name: "task",
            args: json!({
                "description": "fork child check",
                "prompt": "Read note.txt and report back.",
                "model": "low",
                "run_in_background": false
            }),
        },
        // Fork sub-agent step 1 calls `read`, step 2 finishes (verifying child stability).
        ScriptedReply::ToolCall {
            id: "sub-read-1",
            name: "read",
            args: json!({ "path": file_target }),
        },
        ScriptedReply::Text("fork child done"),
        // Parent step 2 spawns a `fork_turns: "none"` sub-agent.
        ScriptedReply::ToolCall {
            id: "call-clean-sub",
            name: "task",
            args: json!({
                "description": "isolated child check",
                "prompt": "Confirm isolated child key.",
                "model": "low",
                "fork_turns": "none",
                "run_in_background": false
            }),
        },
        // Non-fork sub-agent finishes.
        ScriptedReply::Text("clean child done"),
        // Parent step 3 finishes.
        ScriptedReply::Text("parent done"),
        // Read-only side question (`:readonly:`).
        ScriptedReply::Text("side answer"),
    ]);

    let (dir, session) = start_antigravity_gemini_session(
        client.clone(),
        vec![
            create_read_tool(),
            create_task_tool(TaskToolOptions::default()),
        ],
        200_000,
        None,
    );
    std::fs::write(dir.path().join(file_target), "hello\n").unwrap();

    let result = session
        .run(
            Input::Text("Coordinate sub-agents.".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    assert_eq!(result.final_text, "parent done");

    let side = session
        .ask_side_question_input_with_signal(
            Input::Text("Quick side question?".into()),
            AbortSignal::new(),
        )
        .await
        .unwrap();
    assert_eq!(side.response, "side answer");

    let calls = client.taken();
    assert_eq!(calls.len(), 7);
    let parent_id = session.id.try_to_utf8().unwrap();

    // Parent steps 1, 2, and 3 keep the exact parent conversation key.
    assert_eq!(calls[0].cache_key.as_deref(), Some(parent_id.as_str()));
    assert_eq!(calls[3].cache_key.as_deref(), Some(parent_id.as_str()));
    assert_eq!(calls[5].cache_key.as_deref(), Some(parent_id.as_str()));

    // Default fork sub-agent gets its own `:sub:` key (never the parent key) and
    // keeps that same `:sub:` key across both of its steps.
    let fork_sub_key = calls[1].cache_key.as_deref().expect("fork sub cache_key");
    assert!(
        fork_sub_key.starts_with(&format!("{parent_id}:sub:")),
        "expected `:sub:` key for fork child, got {fork_sub_key}"
    );
    assert_eq!(calls[2].cache_key.as_deref(), Some(fork_sub_key));

    // Non-fork sub-agent gets a distinct `:sub:` key from both parent and fork sibling.
    let clean_sub_key = calls[4].cache_key.as_deref().expect("clean sub cache_key");
    assert!(
        clean_sub_key.starts_with(&format!("{parent_id}:sub:")),
        "expected `:sub:` key for non-fork child, got {clean_sub_key}"
    );
    assert_ne!(clean_sub_key, fork_sub_key);

    // Read-only side query gets a `:readonly:` key and never touches the parent slot.
    let readonly_key = calls[6].cache_key.as_deref().expect("readonly cache_key");
    assert!(
        readonly_key.starts_with(&format!("{parent_id}:readonly:")),
        "expected `:readonly:` key for side question, got {readonly_key}"
    );

    session.dispose().await;
}

#[tokio::test]
async fn non_cacheable_mode_permission_and_orchestration_blocks_stay_out_of_gemini_system_instruction(
) {
    let client = ScriptedGeminiClient::new(vec![
        ScriptedReply::Text("turn-1"),
        ScriptedReply::Text("turn-2"),
        ScriptedReply::Text("turn-3"),
        ScriptedReply::Text("turn-4"),
    ]);
    let (_dir, session) = start_antigravity_gemini_session(
        client.clone(),
        vec![
            create_read_tool(),
            create_task_tool(TaskToolOptions::default()),
        ],
        200_000,
        Some(json!({
            "multi_agent": {
                "mode": {
                    "proactive": "Delegate independent checks when helpful."
                }
            }
        })),
    );

    // Turn 1: Default collaboration mode, danger-full-access, ultra_mode false.
    let r1 = session
        .run(
            Input::Text("Start in default mode.".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(r1.error.is_none(), "{:?}", r1.error);

    // Turn 2: Keep Default mode so the tool pool does not gain Plan-only tools,
    // while tightening runtime permissions and enabling `ultra_mode`.
    session
        .set_runtime_policy(RuntimePolicyUpdate {
            approval_policy: Some(ApprovalPolicy::OnRequest),
            sandbox_mode: Some(SandboxMode::ReadOnly),
            approvals_reviewer: None,
            network_allowed: Some(false),
            sandbox_workspace_write: None,
        })
        .unwrap();
    let r2 = session
        .run(
            Input::Text("Tighten policy + ultra in default mode.".into()),
            TurnOptions {
                ultra_mode: Some(true),
                ..Default::default()
            },
        )
        .await;
    assert!(r2.error.is_none(), "{:?}", r2.error);

    // Turn 3: Switch collaboration mode to Plan.
    session
        .planning_runtime()
        .set_mode(CollaborationMode::Plan, None)
        .unwrap();
    let r3 = session
        .run(
            Input::Text("Switch to plan mode.".into()),
            TurnOptions {
                ultra_mode: Some(true),
                ..Default::default()
            },
        )
        .await;
    assert!(r3.error.is_none(), "{:?}", r3.error);

    // Turn 4: Return to Default mode and full access.
    session
        .set_runtime_policy(RuntimePolicyUpdate {
            approval_policy: Some(ApprovalPolicy::Never),
            sandbox_mode: Some(SandboxMode::DangerFullAccess),
            approvals_reviewer: None,
            network_allowed: Some(true),
            sandbox_workspace_write: None,
        })
        .unwrap();
    session
        .planning_runtime()
        .set_mode(CollaborationMode::Default, None)
        .unwrap();
    let r4 = session
        .run(
            Input::Text("Back to default execution.".into()),
            TurnOptions {
                ultra_mode: Some(false),
                ..Default::default()
            },
        )
        .await;
    assert!(r4.error.is_none(), "{:?}", r4.error);

    let calls = client.taken();
    assert_eq!(calls.len(), 4);
    assert_eq!(
        calls[0].wire["tools"], calls[1].wire["tools"],
        "permission and ultra_mode updates within Default mode keep tools byte-identical"
    );
    assert_eq!(calls[0].wire["tools"], calls[3].wire["tools"]);
    for pair in calls.windows(2) {
        assert_eq!(
            pair[0].wire["systemInstruction"], pair[1].wire["systemInstruction"],
            "systemInstruction must remain byte-identical across mode, permission, and ultra transitions"
        );
        assert!(
            pair[1].wire["contents"]
                .as_array()
                .unwrap()
                .starts_with(pair[0].wire["contents"].as_array().unwrap()),
            "contents must grow strictly by append-only prefix"
        );
    }

    let system_text = calls[2].wire["systemInstruction"].to_string();
    assert!(!system_text.contains("# Collaboration Mode:"));
    assert!(!system_text.contains("# Current permissions"));
    assert!(!system_text.contains("Delegate independent checks when helpful."));

    let contents_text = calls[3].wire["contents"].to_string();
    assert!(contents_text.contains("[collaboration-mode]"));
    assert!(contents_text.contains("[permissions]"));
    assert!(contents_text.contains("[orchestration-mode]"));
    assert!(contents_text.contains("Delegate independent checks when helpful."));

    session.dispose().await;
}

#[tokio::test]
async fn antigravity_gemini_keeps_deferred_tools_out_of_declarations_and_invokes_via_call_mcp_tool()
{
    // 1. Normal window: `Deferred` MCP tools stay out of `functionDeclarations`
    //    at step 0, `tool_search` returns their `inputSchema` inside `contents`
    //    without mutating `wire["tools"]`, and `call_mcp_tool` invokes the
    //    discovered MCP tools while keeping `wire["tools"]` byte-identical.
    let invocations = Arc::new(Mutex::new(Vec::new()));
    let normal_client = ScriptedGeminiClient::new(vec![
        ScriptedReply::ToolCall {
            id: "search-1",
            name: "tool_search",
            args: json!({ "query": "select:mcp__demo__beta" }),
        },
        ScriptedReply::ToolCall {
            id: "mcp-1",
            name: "call_mcp_tool",
            args: json!({ "name": "mcp__demo__beta", "arguments": { "q": "ping" } }),
        },
        ScriptedReply::ToolCall {
            id: "mcp-2",
            name: "call_mcp_tool",
            args: json!({ "server": "demo", "name": "alpha", "arguments": { "q": "pong" } }),
        },
        ScriptedReply::Text("done"),
    ]);
    let (_dir, normal_session) = start_antigravity_gemini_session(
        normal_client.clone(),
        vec![
            create_read_tool(),
            create_tool_search_tool(None),
            demo_mcp_tool_tracked("demo", "alpha", "Alpha capability", invocations.clone()),
            demo_mcp_tool_tracked("demo", "beta", "Beta capability", invocations.clone()),
        ],
        200_000,
        None,
    );
    let normal_res = normal_session
        .run(Input::Text("Use beta tool.".into()), TurnOptions::default())
        .await;
    assert!(normal_res.error.is_none(), "{:?}", normal_res.error);
    assert_eq!(normal_res.final_text, "done");
    assert_eq!(
        *invocations.lock().unwrap(),
        vec!["demo:beta=ping".to_string(), "demo:alpha=pong".to_string()]
    );

    let normal_calls = normal_client.taken();
    assert_eq!(normal_calls.len(), 4);
    let step0_tool_names: Vec<&str> = normal_calls[0].wire["tools"][0]["functionDeclarations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|decl| decl["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        step0_tool_names,
        vec!["call_mcp_tool", "read", "tool_search", "update_plan"],
        "deferred MCP tools must never bloat functionDeclarations"
    );
    assert_eq!(normal_calls[0].wire["tools"], normal_calls[1].wire["tools"]);
    assert_eq!(normal_calls[1].wire["tools"], normal_calls[2].wire["tools"]);
    assert_eq!(normal_calls[2].wire["tools"], normal_calls[3].wire["tools"]);

    // Verify `tool_search` response on Gemini carries the matched `inputSchema`
    // and `call_mcp_tool` guidance at the append-only tail of `contents`.
    let step1_contents = normal_calls[1].wire["contents"].to_string();
    assert!(step1_contents.contains("\"mcp__demo__beta\""));
    assert!(step1_contents.contains("\"inputSchema\""));
    assert!(step1_contents.contains("call_mcp_tool"));
    let step2_contents = normal_calls[2].wire["contents"].to_string();
    assert!(step2_contents.contains("ack:demo:beta:ping"));
    normal_session.dispose().await;

    // 2. Oversized direct inventory (`fixed_prefix_tokens > input_window`):
    //    defers `Direct` tools behind `tool_search` + `call_mcp_tool` as well
    //    and keeps `wire["tools"]` invariant across `tool_search`.
    let pad = "Long schema description padding ".repeat(80);
    let tight_client = ScriptedGeminiClient::new(vec![
        ScriptedReply::ToolCall {
            id: "search-tight",
            name: "tool_search",
            args: json!({ "query": "select:mcp__demo__alpha" }),
        },
        ScriptedReply::Text("tight done"),
    ]);
    let (_tight_dir, tight_session) = start_antigravity_gemini_session(
        tight_client.clone(),
        vec![
            create_read_tool(),
            create_tool_search_tool(None),
            demo_mcp_tool("demo", "alpha", &pad),
            demo_mcp_tool("demo", "beta", &pad),
        ],
        1_200,
        None,
    );
    let tight_res = tight_session
        .run(
            Input::Text("Activate demo server.".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(tight_res.error.is_none(), "{:?}", tight_res.error);
    let tight_calls = tight_client.taken();
    assert_eq!(tight_calls.len(), 2);
    let initial_decls: Vec<&str> = tight_calls[0].wire["tools"][0]["functionDeclarations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|decl| decl["name"].as_str().unwrap())
        .collect();
    assert_eq!(initial_decls, vec!["call_mcp_tool", "tool_search"]);
    assert_eq!(
        tight_calls[0].wire["tools"], tight_calls[1].wire["tools"],
        "tools prefix must remain byte-identical after tool_search under tight windows too"
    );
    tight_session.dispose().await;
}

#[tokio::test]
async fn large_deferred_catalog_stays_deferred_without_mutating_declarations() {
    let bulk_desc = "Plugin schema description payload ".repeat(160);
    let mut tools: Vec<Arc<dyn Tool>> = vec![
        create_read_tool(),
        create_tool_search_tool(None),
        demo_mcp_tool("design", "open", "Open design canvas"),
        demo_mcp_tool("design", "publish", "Publish design artboard"),
    ];
    for idx in 0..24 {
        tools.push(demo_mcp_tool(
            "unused_catalog",
            &format!("bulk_{idx:02}"),
            &bulk_desc,
        ));
    }

    let client = ScriptedGeminiClient::new(vec![
        ScriptedReply::ToolCall {
            id: "search-design",
            name: "tool_search",
            args: json!({ "query": "select:mcp__design__open" }),
        },
        ScriptedReply::Text("design ready"),
    ]);
    let (_dir, session) = start_antigravity_gemini_session(client.clone(), tools, 1_048_576, None);
    let res = session
        .run(
            Input::Text("Open the design canvas.".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(res.error.is_none(), "{:?}", res.error);
    let calls = client.taken();
    assert_eq!(calls.len(), 2);

    let step0_decls: Vec<&str> = calls[0].wire["tools"][0]["functionDeclarations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|decl| decl["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        step0_decls,
        vec!["call_mcp_tool", "read", "tool_search", "update_plan"],
        "direct tools and call_mcp_tool must stay visible while deferred catalogs remain out of functionDeclarations"
    );
    assert_eq!(
        calls[0].wire["tools"], calls[1].wire["tools"],
        "activating deferred tools via tool_search must not mutate functionDeclarations"
    );
    session.dispose().await;
}
