//! Defect: parallel tool output was cut without preserving omitted text, while
//! tools declaring a large result cap bypassed the shared persistence threshold.
//! Root cause: the live aggregate path used a one-pass 30 percent trim and
//! persisted no artifact; the per-tool threshold did not clamp declarations.
//! Fixed by: 2026-09-05 context tool-result budget alignment work item.

use async_trait::async_trait;
use klaus_agent_core::engine::agent::{Agent, AgentOptions, StartSessionOptions};
use klaus_agent_core::engine::deps::EngineDepsOverrides;
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::message::{Message, ToolResultMessage, Usage};
use klaus_agent_core::tools::build_tool::text_result;
use klaus_agent_core::tools::result_storage::output_blocks_text;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::ToolError;
use klaus_agent_core::types::json::{Json, JsonSchema};
use klaus_agent_core::types::permission::ExternalPermissionMode;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

struct ReportTool {
    schema: JsonSchema,
}
#[async_trait]
impl Tool for ReportTool {
    fn static_name(&self) -> &str {
        "report"
    }
    fn static_description(&self) -> &str {
        "Return a bounded report."
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
    fn max_result_size_chars(&self) -> Option<u64> {
        Some(100_000)
    }
    async fn call(
        &self,
        input: Json,
        _: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let size = input.get("size").and_then(Json::as_u64).unwrap() as usize;
        Ok(text_result(input, "x".repeat(size), false))
    }
}

struct RecordingProvider {
    info: ProviderInfo,
    dir: std::path::PathBuf,
    calls: AtomicUsize,
    first_results: Mutex<Option<Vec<ToolResultMessage>>>,
}
impl ModelProvider for RecordingProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }
    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: true,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: 1_000_000,
            max_output_tokens: 8192,
            native_pdf: true,
            vision: true,
            image_limits: None,
            max_media_blocks: None,
        }
    }
    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        // Session housekeeping can make an isolated call without user tools.
        // This regression records only the report conversation's requests.
        if !request.tools.iter().any(|tool| tool.name == "report") {
            return Box::pin(futures::stream::iter(vec![
                ModelStreamEvent::TextDelta {
                    text: "complete".into(),
                },
                stop(ModelStopReason::EndTurn),
            ]));
        }
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let mut events = vec![ModelStreamEvent::MessageStart {
            model: "budget-model".into(),
        }];
        if call == 0 {
            for index in 0..7 {
                let id = format!("budget-{index}");
                events.extend([
                    ModelStreamEvent::ToolUseStart {
                        id: id.clone().into(),
                        name: "report".into(),
                        thought_signature: None,
                        tool_kind: None,
                        input_format: None,
                    },
                    ModelStreamEvent::ToolUseInputDelta {
                        id: id.clone().into(),
                        partial_json: format!(
                            r#"{{"size":{},"index":{index}}}"#,
                            if index == 6 { 60_000 } else { 40_000 }
                        )
                        .into(),
                    },
                    ModelStreamEvent::ToolUseStop { id: id.into() },
                ]);
            }
            events.push(stop(ModelStopReason::ToolUse));
        } else {
            let results = request
                .messages
                .iter()
                .filter_map(|message| match message {
                    Message::ToolResult(result) => Some(result.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results.len(),
                7,
                "call={call} system={:?} tools={:?}",
                request
                    .system
                    .iter()
                    .map(|part| &part.name)
                    .collect::<Vec<_>>(),
                request
                    .tools
                    .iter()
                    .map(|tool| &tool.name)
                    .collect::<Vec<_>>()
            );
            if call == 1 {
                let text = results
                    .iter()
                    .map(|result| output_blocks_text(&result.content))
                    .collect::<Vec<_>>();
                let total: usize = text.iter().map(RuntimeString::len_utf16).sum();
                assert!(total <= 200_000, "model-visible batch must fit: {total}");
                assert!(text[0].contains("budget-0.aggregate.txt"));
                assert!(text[1].contains("budget-1.aggregate.txt"));
                for value in &text[2..6] {
                    assert_eq!(*value, "x".repeat(40_000));
                }
                assert!(text[6].contains("budget-6.txt"));
                for index in 0..2 {
                    assert_eq!(
                        std::fs::read_to_string(
                            self.dir
                                .join(format!(".klaus/tool-results/budget-{index}.aggregate.txt"))
                        )
                        .unwrap(),
                        "x".repeat(40_000)
                    );
                }
                assert_eq!(
                    std::fs::read_to_string(self.dir.join(".klaus/tool-results/budget-6.txt"))
                        .unwrap(),
                    "x".repeat(60_000)
                );
                for (index, result) in results.iter().enumerate() {
                    assert_eq!(
                        result
                            .output
                            .as_ref()
                            .unwrap()
                            .get("index")
                            .and_then(Json::as_u64),
                        Some(index as u64),
                        "UI output is preserved"
                    );
                }
                *self.first_results.lock().unwrap() = Some(results);
            } else {
                assert_eq!(
                    Some(results),
                    *self.first_results.lock().unwrap(),
                    "later turns replay the same persisted replacements"
                );
            }
            events.push(ModelStreamEvent::TextDelta {
                text: "complete".into(),
            });
            events.push(stop(ModelStopReason::EndTurn));
        }
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
    provider: Arc<RecordingProvider>,
) -> AgentOptions {
    AgentOptions {
        process_context,
        provider: provider.clone(),
        config: Some(serde_json::json!({"contextWindow":1_000_000,"aggregateToolResults":true})),
        tools: Some(vec![Arc::new(ReportTool {
            schema: serde_json::json!({"type":"object", "properties": {"size":{"type":"integer"}, "index":{"type":"integer"}}, "required":["size","index"]}).as_object().unwrap().clone(),
        })]),
        web_fetch_page: None,
        browser_runtime: None,
        capability_roots: Vec::new(),
        cwd: Some(provider.dir.to_string_lossy().into_owned()),
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
async fn public_session_preserves_full_parallel_results_and_replays_bounded_history() {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let dir = std::env::temp_dir().join(format!("klaus-result-budget-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let provider = Arc::new(RecordingProvider {
        info: ProviderInfo {
            name: "budget-provider".into(),
            base_url: None,
            env_key: None,
            default_model: "budget-model".into(),
        },
        dir: dir.clone(),
        calls: AtomicUsize::new(0),
        first_results: Mutex::new(None),
    });
    let agent = Agent::new(agent_options(process.clone(), provider.clone()));
    let session = agent.start_session(StartSessionOptions {
        permission_mode: Some(ExternalPermissionMode::BypassPermissions),
        user_config_dir: Some(dir.to_string_lossy().into_owned().into()),
        ..Default::default()
    });
    for input in ["make reports", "continue"] {
        let result = session
            .run(Input::Text(input.into()), TurnOptions::default())
            .await;
        assert!(result.error.is_none(), "turn error: {:?}", result.error);
        assert_eq!(result.final_text, "complete");
    }
    assert_eq!(provider.calls.load(Ordering::SeqCst), 3);
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}
