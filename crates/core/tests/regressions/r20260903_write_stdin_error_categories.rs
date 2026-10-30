//! Defect: every `write_stdin` failure was reported as "No session with id",
//! so the model could not tell a terminal session from a transient I/O failure
//! and often repeated the same call until loop detection stopped the turn.
//! Root cause: the interactive-shell adapter collapsed process status and all
//! registry errors into one `found: false` result. Fixed by work item 2026-09-03:
//! preserve stable error categories and tell the model to prefer background
//! `bash` unless the same live process must receive interactive input.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use klaus_agent_core::engine::agent::{Agent, AgentOptions, StartSessionOptions};
use klaus_agent_core::engine::deps::EngineDepsOverrides;
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimeObject, RuntimeString};
use klaus_agent_core::schemas::content::OutputBlock;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::permission::ExternalPermissionMode;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};

struct WriteStdinProvider {
    info: ProviderInfo,
    calls: AtomicUsize,
}

impl WriteStdinProvider {
    fn new() -> Self {
        Self {
            info: ProviderInfo {
                name: "write-stdin-regression".into(),
                base_url: None,
                env_key: None,
                default_model: "write-stdin-regression".into(),
            },
            calls: AtomicUsize::new(0),
        }
    }
}

impl ModelProvider for WriteStdinProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _model: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: true,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: 200_000,
            max_output_tokens: 8_192,
            native_pdf: true,
            vision: true,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn stream(&self, request: ModelRequest, _signal: AbortSignal) -> ModelStream {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let events = match call {
            0 => {
                assert!(request.tools.iter().any(|tool| tool.name == "tool_search"));
                assert!(!request.tools.iter().any(|tool| tool.name == "exec_command"));
                assert!(!request.tools.iter().any(|tool| tool.name == "write_stdin"));

                let mut input = RuntimeObject::new();
                input.insert("query", RuntimeJson::String("select:exec_command".into()));
                vec![
                    ModelStreamEvent::MessageStart {
                        model: "write-stdin-regression".into(),
                    },
                    ModelStreamEvent::ToolUseStart {
                        id: "search-1".into(),
                        name: "tool_search".into(),
                        thought_signature: None,
                        tool_kind: None,
                        input_format: None,
                    },
                    ModelStreamEvent::ToolUseInputDelta {
                        id: "search-1".into(),
                        partial_json: runtime_json::stringify(&RuntimeJson::Object(input)),
                    },
                    ModelStreamEvent::ToolUseStop {
                        id: "search-1".into(),
                    },
                    stop(ModelStopReason::ToolUse),
                ]
            }
            1 => {
                let search_result = request
                    .messages
                    .iter()
                    .find_map(|message| match message {
                        Message::ToolResult(result) if result.tool_use_id == "search-1" => {
                            Some(result)
                        }
                        _ => None,
                    })
                    .expect("tool_search result must return to the model");
                assert!(!search_result.is_error);
                assert_eq!(
                    search_result
                        .output
                        .as_ref()
                        .and_then(|output| output.get("matched")),
                    Some(&RuntimeJson::Array(vec![
                        RuntimeJson::String("exec_command".into()),
                        RuntimeJson::String("write_stdin".into()),
                    ]))
                );
                let write_description = request
                    .tools
                    .iter()
                    .find(|tool| tool.name == "write_stdin")
                    .expect("write_stdin must remain available for interactive sessions")
                    .description
                    .clone();
                assert!(write_description
                    .contains("Prefer bash in background mode with bash_output and kill_shell"));
                assert!(write_description
                    .contains("only when the same live process must receive interactive input"));
                let exec_description = request
                    .tools
                    .iter()
                    .find(|tool| tool.name == "exec_command")
                    .expect("exec_command must remain available for interactive sessions")
                    .description
                    .clone();
                assert!(exec_description
                    .contains("Prefer bash in background mode with bash_output and kill_shell"));
                assert!(exec_description
                    .contains("only when the same live process must receive later input"));

                let mut input = RuntimeObject::new();
                input.insert("sessionId", RuntimeJson::String("missing-session".into()));
                input.insert("input", RuntimeJson::String("yes\n".into()));
                vec![
                    ModelStreamEvent::MessageStart {
                        model: "write-stdin-regression".into(),
                    },
                    ModelStreamEvent::ToolUseStart {
                        id: "write-1".into(),
                        name: "write_stdin".into(),
                        thought_signature: None,
                        tool_kind: None,
                        input_format: None,
                    },
                    ModelStreamEvent::ToolUseInputDelta {
                        id: "write-1".into(),
                        partial_json: runtime_json::stringify(&RuntimeJson::Object(input)),
                    },
                    ModelStreamEvent::ToolUseStop {
                        id: "write-1".into(),
                    },
                    stop(ModelStopReason::ToolUse),
                ]
            }
            2 => {
                let result = request
                    .messages
                    .iter()
                    .find_map(|message| match message {
                        Message::ToolResult(result) if result.tool_use_id == "write-1" => {
                            Some(result)
                        }
                        _ => None,
                    })
                    .expect("the classified tool failure must return to the model");
                assert!(result.is_error);
                let output = result.output.as_ref().expect("structured tool output");
                assert_eq!(
                    output.get("errorCode").and_then(RuntimeJson::as_str),
                    Some(&RuntimeString::from("session_not_found"))
                );
                assert_eq!(
                    output.get("errorClass").and_then(RuntimeJson::as_str),
                    Some(&RuntimeString::from("terminal"))
                );
                assert_eq!(
                    output.get("retryAllowed").and_then(RuntimeJson::as_bool),
                    Some(false)
                );
                assert_eq!(
                    output.get("retriesRemaining").and_then(RuntimeJson::as_u64),
                    Some(0)
                );
                assert_eq!(
                    output.get("sessionBlocked").and_then(RuntimeJson::as_bool),
                    Some(true)
                );
                assert_eq!(
                    output
                        .get("attemptPrevented")
                        .and_then(RuntimeJson::as_bool),
                    Some(true)
                );
                let mut message = RuntimeString::default();
                for block in &result.content {
                    if let OutputBlock::Text(text) = block {
                        message = message.concat(&text.text);
                    }
                }
                assert!(message.contains("terminal failures have zero retry budget"));
                assert!(message.contains("Do not retry this sessionId"));

                let mut input = RuntimeObject::new();
                input.insert("sessionId", RuntimeJson::String("missing-session".into()));
                input.insert("input", RuntimeJson::String("yes\n".into()));
                vec![
                    ModelStreamEvent::MessageStart {
                        model: "write-stdin-regression".into(),
                    },
                    ModelStreamEvent::ToolUseStart {
                        id: "write-2".into(),
                        name: "write_stdin".into(),
                        thought_signature: None,
                        tool_kind: None,
                        input_format: None,
                    },
                    ModelStreamEvent::ToolUseInputDelta {
                        id: "write-2".into(),
                        partial_json: runtime_json::stringify(&RuntimeJson::Object(input)),
                    },
                    ModelStreamEvent::ToolUseStop {
                        id: "write-2".into(),
                    },
                    stop(ModelStopReason::ToolUse),
                ]
            }
            3 => {
                let result = request
                    .messages
                    .iter()
                    .find_map(|message| match message {
                        Message::ToolResult(result) if result.tool_use_id == "write-2" => {
                            Some(result)
                        }
                        _ => None,
                    })
                    .expect("the engine-blocked retry must return to the model");
                let output = result.output.as_ref().expect("structured tool output");
                assert_eq!(
                    output
                        .get("attemptPrevented")
                        .and_then(RuntimeJson::as_bool),
                    Some(true)
                );
                assert_eq!(
                    output.get("sessionBlocked").and_then(RuntimeJson::as_bool),
                    Some(true)
                );
                assert_eq!(
                    output.get("retryAllowed").and_then(RuntimeJson::as_bool),
                    Some(false)
                );
                let mut message = RuntimeString::default();
                for block in &result.content {
                    if let OutputBlock::Text(text) = block {
                        message = message.concat(&text.text);
                    }
                }
                assert!(message.contains("Blocked by the engine before process I/O"));
                vec![
                    ModelStreamEvent::MessageStart {
                        model: "write-stdin-regression".into(),
                    },
                    ModelStreamEvent::TextDelta {
                        text: "blocked-and-recovered".into(),
                    },
                    stop(ModelStopReason::EndTurn),
                ]
            }
            _ => panic!("unexpected extra model request {call}"),
        };
        Box::pin(futures::stream::iter(events))
    }
}

fn stop(reason: ModelStopReason) -> ModelStreamEvent {
    ModelStreamEvent::MessageStop {
        stop_reason: reason,
        usage: Usage {
            input_tokens: 10,
            output_tokens: 5,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
            total_tokens: None,
        },
    }
}

fn agent_options(
    process_context: RuntimeProcessContext,
    provider: Arc<WriteStdinProvider>,
) -> AgentOptions {
    AgentOptions {
        process_context,
        provider,
        config: None,
        tools: None,
        web_fetch_page: None,
        browser_runtime: None,
        capability_roots: Vec::new(),
        cwd: Some(std::env::temp_dir().to_string_lossy().into_owned()),
        deps: EngineDepsOverrides::default(),
        compactor: None,
        approval_handler: None,
        approval_reviewer: None,
        elicit_handler: None,
        credential_store: None,
        hooks: Vec::new(),
        observer: Some(None),
        logger: None,
        pricing: None,
        agents: None,
        include_default_agents: None,
        skills: None,
        output_styles: None,
        workflows: None,
        mcp_catalog: None,
        plugin_catalog: None,
        lsp_servers: None,
        agent_memory: None,
        mcp_clients: None,
        mcp_server_configs: Vec::new(),
        mcp_tools: None,
        refresh_tools: None,
        system_prompt_provider: None,
        plugins: Vec::new(),
        plugin_load_report: None,
    }
}

#[tokio::test]
async fn model_sees_classified_error_and_background_bash_guidance() {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .expect("process context");
    let provider = Arc::new(WriteStdinProvider::new());
    let agent = Agent::new(agent_options(process.clone(), provider.clone()));
    let session = agent.start_session(StartSessionOptions {
        permission_mode: Some(ExternalPermissionMode::BypassPermissions),
        user_config_dir: Some(std::env::temp_dir().to_string_lossy().into_owned().into()),
        config: Some(serde_json::json!({"allow": ["write_stdin"]})),
        ..StartSessionOptions::default()
    });

    let result = session
        .run(
            Input::Text("use the missing interactive session".into()),
            TurnOptions::default(),
        )
        .await;

    assert_eq!(
        result.final_text,
        RuntimeString::from("blocked-and-recovered")
    );
    assert!(result.error.is_none(), "turn error: {:?}", result.error);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 4);

    session.shutdown_and_wait().await.expect("session shutdown");
    process.shutdown().await.expect("process shutdown");
}
