//! Defect: plain-chat local models received the complete agent tool schema.
//! Root cause: OpenAI providers reported tools=true regardless of the host-declared capability.
//! Fixed by: 2026-09-07 local-model runtime correctness work item.

use async_trait::async_trait;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::message::Usage;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::ToolError;
use klaus_agent_core::types::json::{Json, JsonSchema};
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};
use serde_json::json;
use std::sync::{Arc, Mutex};

struct PlainChatProvider {
    info: ProviderInfo,
    requests: Mutex<Vec<ModelRequest>>,
}

impl ModelProvider for PlainChatProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _model: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: false,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: 32_768,
            max_output_tokens: 512,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn stream(&self, request: ModelRequest, _signal: AbortSignal) -> ModelStream {
        self.requests.lock().unwrap().push(request);
        Box::pin(futures::stream::iter(vec![
            ModelStreamEvent::TextDelta {
                text: "plain answer".into(),
            },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: Usage {
                    input_tokens: 10,
                    output_tokens: 2,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: Some(12),
                },
            },
        ]))
    }
}

struct RegisteredTool {
    schema: JsonSchema,
}

#[async_trait]
impl Tool for RegisteredTool {
    fn static_name(&self) -> &str {
        "registered_tool"
    }
    fn static_description(&self) -> &str {
        "A tool that must stay hidden."
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }
    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }
    async fn call(
        &self,
        _input: Json,
        _context: &ToolContext,
        _progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        panic!("a tool-free model must never call a registered tool")
    }
}

#[tokio::test]
async fn public_session_hides_registered_tools_when_provider_declares_none() {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let root = std::env::temp_dir().join(format!(
        "klaus-local-tools-capability-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let provider = Arc::new(PlainChatProvider {
        info: ProviderInfo {
            name: "local-plain-chat".into(),
            base_url: None,
            env_key: None,
            default_model: "local-plain-chat".into(),
        },
        requests: Mutex::new(Vec::new()),
    });
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
    options.provider = provider.clone();
    options.cwd = Some(root.to_string_lossy().into_owned());
    options.config.as_mut().unwrap()["model"] = json!("local-plain-chat");
    options.config.as_mut().unwrap()["agentMemory"] = json!(false);
    options.config.as_mut().unwrap()["autoMemory"] = json!(false);
    options.tools = Some(vec![Arc::new(RegisteredTool {
        schema: json!({"type":"object","properties":{}})
            .as_object()
            .unwrap()
            .clone(),
    })]);

    let session = Agent::new(options).start_session(StartSessionOptions::default());
    let result = session
        .run(
            Input::Text("Answer without tools.".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    {
        let requests = provider.requests.lock().unwrap();
        assert!(!requests.is_empty());
        assert!(
            requests.iter().all(|request| request.tools.is_empty()),
            "tool schema leaked to a tool-free model"
        );
        assert!(requests.iter().all(|request| request.tool_choice.is_none()));
    }
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
