//! Defect: Antigravity Gemini sessions failed with HTTP `400 INVALID_ARGUMENT`
//! (`The referenced name '#/$defs/__schema2' in function_response.response does
//! not match to a display_name in the function_response.parts.`) as soon as
//! `tool_search` matched any deferred MCP tool whose JSON Schema used `$defs`
//! and `{"$ref": "..."}`, and every subsequent turn in the same session kept
//! failing when replaying the persisted `tool_search` result from history.
//! Root cause: `provider::gemini::to_contents_with_profile` embedded the raw
//! `native_tool_output.tools` JSON objects inside `functionResponse.response`,
//! where Gemini's `GenerateContent` validator interprets nested `{"$ref": ...}`
//! keys as multimodal `FunctionResponsePart` display-name references.
//! Work item: 20261122 Gemini tool_search `$ref` schema projection fix.

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::mcp::tool_wrapper::{wrap_mcp_tool, McpToolCall, WrapMcpToolOptions};
use klaus_agent_core::mcp::types::{McpCallResult, McpContentBlock, McpToolDescriptor};
use klaus_agent_core::provider::gemini::{
    generate_content_request, GeminiAdapterPolicy, GeminiApiError, GeminiChunkStream, GeminiClient,
    GeminiProvider, GeminiProviderOptions, GenerateContentParameters, GenerateContentResponse,
};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimePath, RuntimeString};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::tools::builtins::read::create_read_tool;
use klaus_agent_core::tools::builtins::tool_search::create_tool_search_tool;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{AuthHeaders, ModelCapabilities};
use klaus_agent_core::types::tool::Tool;
use serde_json::{json, Value};

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

const GEMINI_MODEL: &str = "gemini-4-carbon-high";

#[derive(Clone)]
enum ScriptedReply {
    Text(&'static str),
    ToolCall {
        id: &'static str,
        name: &'static str,
        args: Value,
    },
}

struct ValidatingGeminiClient {
    wires: Mutex<Vec<Value>>,
    replies: Mutex<VecDeque<ScriptedReply>>,
}

impl ValidatingGeminiClient {
    fn new(replies: Vec<ScriptedReply>) -> Arc<Self> {
        Arc::new(Self {
            wires: Mutex::new(Vec::new()),
            replies: Mutex::new(replies.into()),
        })
    }

    fn taken_wires(&self) -> Vec<Value> {
        self.wires.lock().unwrap().clone()
    }
}

fn find_unmatched_function_response_ref(
    node: &Value,
    allowed_display_names: &HashSet<&str>,
) -> Option<String> {
    match node {
        Value::Object(map) => {
            if let Some(ref_name) = map.get("$ref").and_then(Value::as_str) {
                if !allowed_display_names.contains(ref_name) {
                    return Some(ref_name.to_string());
                }
            }
            for value in map.values() {
                if let Some(unmatched) =
                    find_unmatched_function_response_ref(value, allowed_display_names)
                {
                    return Some(unmatched);
                }
            }
            None
        }
        Value::Array(items) => items
            .iter()
            .find_map(|item| find_unmatched_function_response_ref(item, allowed_display_names)),
        _ => None,
    }
}

fn validate_wire_function_responses(wire: &Value) -> Result<(), GeminiApiError> {
    let Some(contents) = wire.get("contents").and_then(Value::as_array) else {
        return Ok(());
    };
    for content in contents {
        let Some(parts) = content.get("parts").and_then(Value::as_array) else {
            continue;
        };
        for part in parts {
            let Some(fn_resp) = part.get("functionResponse") else {
                continue;
            };
            let mut display_names = HashSet::new();
            if let Some(resp_parts) = fn_resp.get("parts").and_then(Value::as_array) {
                for rp in resp_parts {
                    if let Some(name) = rp
                        .get("inlineData")
                        .and_then(|d| d.get("displayName"))
                        .and_then(Value::as_str)
                    {
                        display_names.insert(name);
                    }
                }
            }
            if let Some(response_obj) = fn_resp.get("response") {
                if let Some(unmatched) =
                    find_unmatched_function_response_ref(response_obj, &display_names)
                {
                    return Err(GeminiApiError {
                        status: Some(400),
                        message: format!(
                            "antigravity_request_rejected [INVALID_ARGUMENT]: The referenced name `{unmatched}` in function_response.response does not match to a display_name in the function_response.parts."
                        ),
                        headers: None,
                    });
                }
            }
        }
    }
    Ok(())
}

#[async_trait::async_trait]
impl GeminiClient for ValidatingGeminiClient {
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
        validate_wire_function_responses(&wire)?;
        self.wires.lock().unwrap().push(wire);

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
                    "candidatesTokenCount": 16,
                    "totalTokenCount": 272
                }
            })
            .to_string(),
        ))
        .unwrap();
        Ok(Box::pin(futures::stream::iter(vec![Ok(response)])))
    }
}

fn mcp_tool_with_defs_ref_schema(invocations: Arc<Mutex<Vec<String>>>) -> Arc<dyn Tool> {
    let call: McpToolCall = Arc::new(move |_tool, args, _progress, _sig| {
        let invocations = Arc::clone(&invocations);
        Box::pin(async move {
            let endpoint = args
                .get("cdp_endpoint")
                .and_then(|v| v.as_str())
                .and_then(|s| s.try_to_utf8().ok())
                .unwrap_or_default();
            invocations.lock().unwrap().push(endpoint);
            Ok(McpCallResult {
                content: vec![McpContentBlock {
                    block_type: "text".into(),
                    text: Some("captured viewport".into()),
                    ..Default::default()
                }],
                is_error: Some(false),
                structured_content: None,
                meta: None,
            })
        })
    });
    wrap_mcp_tool(
        "inspector",
        McpToolDescriptor {
            name: "capture_web_screenshot".into(),
            description: Some(
                "Capture the visible viewport of one configured browser page.".into(),
            ),
            input_schema: Some(
                runtime_json::parse(&RuntimeString::from(
                    json!({
                        "type": "object",
                        "properties": {
                            "cdp_endpoint": {
                                "$ref": "#/$defs/__schema0",
                                "description": "Loopback Chrome DevTools Protocol endpoint."
                            },
                            "allowed_origins": {
                                "$ref": "#/$defs/__schema1"
                            },
                            "selector": {
                                "$ref": "#/$defs/__schema2"
                            }
                        },
                        "required": ["cdp_endpoint"],
                        "$defs": {
                            "__schema0": { "type": "string" },
                            "__schema1": { "type": "array", "items": { "type": "string" } },
                            "__schema2": { "type": "string" }
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

fn build_gemini_agent(client: Arc<ValidatingGeminiClient>, tools: Vec<Arc<dyn Tool>>) -> Agent {
    let provider = GeminiProvider::new(GeminiProviderOptions {
        default_model: Some(GEMINI_MODEL.into()),
        client: Some(client),
        capabilities: Some(ModelCapabilities {
            context_window: Some(1_048_576),
            max_output_tokens: Some(8_192),
            auto_compact_token_limit: Some(256_000),
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
        "autoCompaction": true,
        "contextWindow": 1_048_576,
        "maxRetries": 0,
        "unboundedNetworkRetries": false,
        "totalTokensReminder": "off",
        "approval_policy": "never",
        "sandbox_mode": "danger-full-access"
    }));
    options.tools = Some(tools);
    Agent::new(options)
}

#[tokio::test]
async fn gemini_tool_search_with_json_schema_ref_does_not_trip_multimodal_part_ref_validation_and_replays_cleanly(
) {
    // Sanity-check that the validator rejects the pre-fix structured response shape.
    let pre_fix_wire = json!({
        "contents": [{
            "role": "user",
            "parts": [{
                "functionResponse": {
                    "id": "call_1",
                    "name": "tool_search",
                    "response": {
                        "output": "These tools are now available",
                        "tools": [{
                            "name": "mcp__inspector__capture_web_screenshot",
                            "inputSchema": {
                                "properties": {
                                    "selector": { "$ref": "#/$defs/__schema2" }
                                }
                            }
                        }]
                    }
                }
            }]
        }]
    });
    let err = validate_wire_function_responses(&pre_fix_wire).unwrap_err();
    assert!(err.message.contains("#/$defs/__schema2"), "{}", err.message);

    let temp = tempfile::tempdir().unwrap();
    let store_dir = temp.path().join("sessions");
    let cfg_dir = temp.path().join("config");
    std::fs::create_dir_all(&store_dir).unwrap();
    std::fs::create_dir_all(&cfg_dir).unwrap();
    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(store_dir.to_string_lossy().into_owned()),
        archive_dir: None,
    }));

    let invocations = Arc::new(Mutex::new(Vec::new()));
    let client = ValidatingGeminiClient::new(vec![
        ScriptedReply::ToolCall {
            id: "search-ref-1",
            name: "tool_search",
            args: json!({ "query": "capture web screenshot" }),
        },
        ScriptedReply::ToolCall {
            id: "call-ref-1",
            name: "call_mcp_tool",
            args: json!({
                "name": "mcp__inspector__capture_web_screenshot",
                "arguments": { "cdp_endpoint": "http://127.0.0.1:9222" }
            }),
        },
        ScriptedReply::Text("captured successfully"),
        ScriptedReply::Text("resumed follow-up ok"),
    ]);

    let tools: Vec<Arc<dyn Tool>> = vec![
        create_read_tool(),
        create_tool_search_tool(None),
        mcp_tool_with_defs_ref_schema(invocations.clone()),
    ];
    let agent = build_gemini_agent(client.clone(), tools);

    let session = agent
        .open_session(
            store.clone(),
            StartSessionOptions {
                cwd: Some(RuntimePath::from(temp.path().to_string_lossy().as_ref())),
                user_config_dir: Some(RuntimePath::from(cfg_dir.to_string_lossy().as_ref())),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let session_id = session.id.try_to_utf8().unwrap();

    let turn1 = session
        .run(
            Input::Text("Inspect the page screenshot.".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(turn1.error.is_none(), "turn 1 failed: {:?}", turn1.error);
    assert_eq!(turn1.final_text, "captured successfully");
    assert_eq!(
        *invocations.lock().unwrap(),
        vec!["http://127.0.0.1:9222".to_string()]
    );
    session.dispose().await;

    // Reopen from persisted JSONL history and run a follow-up turn to ensure
    // replaying `nativeToolOutput.tools` from disk also projects cleanly.
    let resumed = agent
        .resume_session(
            store,
            &session_id,
            StartSessionOptions {
                cwd: Some(RuntimePath::from(temp.path().to_string_lossy().as_ref())),
                user_config_dir: Some(RuntimePath::from(cfg_dir.to_string_lossy().as_ref())),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let turn2 = resumed
        .run(
            Input::Text("Follow up after tool search.".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(turn2.error.is_none(), "turn 2 failed: {:?}", turn2.error);
    assert_eq!(turn2.final_text, "resumed follow-up ok");
    resumed.dispose().await;

    let wires = client.taken_wires();
    assert_eq!(wires.len(), 4);
    let step1_fn_resp = wires[1]["contents"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|c| c["parts"].as_array().unwrap().iter())
        .find_map(|p| p.get("functionResponse"))
        .expect("step 1 must carry tool_search functionResponse");
    let response_map = step1_fn_resp["response"].as_object().unwrap();
    assert_eq!(
        response_map.keys().collect::<Vec<_>>(),
        vec!["output"],
        "functionResponse.response must only carry the string output field"
    );
    let output_text = response_map["output"].as_str().unwrap();
    assert!(output_text.contains("mcp__inspector__capture_web_screenshot"));
    assert!(output_text.contains("call_mcp_tool"));
    assert!(output_text.contains("Input schema:"));
    assert!(output_text.contains("#/$defs/__schema2"));
}
