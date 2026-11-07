//! Defect: a scoped research child could keep canonical web_search but lose the
//! browser host tool it needs, making default browser search fail in that child.
//! Root cause: role allowlists removed every mapped browser dependency.
//! Fixed by: the 2026-09-19 Bing browser-search work item retains one hidden,
//! bounded tabs_content dependency after filtering the child tool set.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use klaus_agent_core::browser::{
    BrowserHostToolMapping, BrowserOperation, BrowserRuntimeRequest, BrowserRuntimeResponse,
    BrowserTarget, BrowserWaitUntil,
};
use klaus_agent_core::capabilities::RuntimeCapabilityRoot;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::agent_definitions::AgentDefinition;
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeString};
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::{Message, ToolResultMessage, Usage};
use klaus_agent_core::tools::build_tool::text_result;
use klaus_agent_core::tools::builtins::tool_search::create_tool_search_tool;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::ToolError;
use klaus_agent_core::types::json::Json;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};
use serde_json::json;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

const CHILD_PROMPT: &str = "bounded-search-child-directive";
const SEARCH_HTML: &str = r#"<ol id="b_results"><li class="b_algo"><h2><a href="https://docs.python.org/3/">Python docs</a></h2><div class="b_caption"><p>Official documentation.</p></div></li></ol>"#;

macro_rules! runtime_json {
    ($($value:tt)*) => {
        runtime_json::parse(&RuntimeString::from(json!($($value)*).to_string())).unwrap()
    };
}

fn usage() -> Usage {
    Usage {
        input_tokens: 10,
        output_tokens: 4,
        cache_read_input_tokens: 0,
        cache_creation_input_tokens: 0,
        total_tokens: None,
    }
}

fn tool_call(name: &str, input: serde_json::Value) -> ModelStream {
    let id = RuntimeString::from(format!("call-{name}"));
    Box::pin(futures::stream::iter(vec![
        ModelStreamEvent::MessageStart {
            model: "search-fixture".into(),
        },
        ModelStreamEvent::ToolUseStart {
            id: id.clone(),
            name: name.into(),
            thought_signature: None,
            tool_kind: None,
            input_format: None,
        },
        ModelStreamEvent::ToolUseInputDelta {
            id: id.clone(),
            partial_json: input.to_string().into(),
        },
        ModelStreamEvent::ToolUseStop { id },
        ModelStreamEvent::MessageStop {
            stop_reason: ModelStopReason::ToolUse,
            usage: usage(),
        },
    ]))
}

fn finish() -> ModelStream {
    Box::pin(futures::stream::iter(vec![
        ModelStreamEvent::MessageStart {
            model: "search-fixture".into(),
        },
        ModelStreamEvent::TextDelta {
            text: "Research completed.".into(),
        },
        ModelStreamEvent::MessageStop {
            stop_reason: ModelStopReason::EndTurn,
            usage: usage(),
        },
    ]))
}

fn result<'a>(request: &'a ModelRequest, id: &str) -> &'a ToolResultMessage {
    request
        .messages
        .iter()
        .find_map(|message| match message {
            Message::ToolResult(result) if result.tool_use_id == id => Some(result),
            _ => None,
        })
        .expect("scripted tool result must be visible to the next model turn")
}

struct SearchProvider {
    info: ProviderInfo,
    dependency_name: String,
    spawned: AtomicBool,
    child_step: AtomicUsize,
    completed: Arc<AtomicBool>,
}

impl ModelProvider for SearchProvider {
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
            context_window: 200_000,
            max_output_tokens: 8_192,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn stream(&self, request: ModelRequest, _signal: AbortSignal) -> ModelStream {
        let is_child = request.messages.iter().any(|message| {
            match message {
            Message::User(message) => message.content.iter().any(|block| {
                matches!(block, ContentBlock::Text(text) if text.text.contains(CHILD_PROMPT))
            }),
            _ => false,
        }
        });
        if is_child {
            assert!(
                request.tools.iter().all(|tool| {
                    !tool.name.starts_with("mcp__") && !tool.name.starts_with("browser_")
                }),
                "scoped research must not expose general browser tools"
            );
            match self.child_step.fetch_add(1, Ordering::SeqCst) {
                0 => {
                    return tool_call(
                        "tool_search",
                        json!({
                            "query": format!("select:{}", self.dependency_name),
                        }),
                    )
                }
                1 => {
                    let discovery = result(&request, "call-tool_search");
                    assert!(!discovery.is_error);
                    let output = discovery
                        .output
                        .as_ref()
                        .expect("structured discovery output");
                    assert!(output
                        .get("tools")
                        .and_then(Json::as_array)
                        .unwrap()
                        .is_empty());
                    assert!(output.get("alreadyAvailable").is_none());
                    assert_eq!(
                        output.get("missing"),
                        Some(&runtime_json!([self.dependency_name.clone()]))
                    );
                    return tool_call("web_search", json!({"query":"python"}));
                }
                2 => {
                    let search = result(&request, "call-web_search");
                    assert!(
                        !search.is_error,
                        "canonical search must reach the child's hidden browser read"
                    );
                    let output = search
                        .output
                        .as_ref()
                        .expect("structured canonical search output");
                    assert_eq!(output.get("count").and_then(Json::as_u64), Some(1));
                    let results = output.get("results").and_then(Json::as_array).unwrap();
                    assert_eq!(results[0].get("title"), Some(&runtime_json!("Python docs")));
                    assert_eq!(
                        results[0].get("url"),
                        Some(&runtime_json!("https://docs.python.org/3/"))
                    );
                    self.completed.store(true, Ordering::SeqCst);
                }
                _ => panic!("unexpected child model call"),
            }
            return finish();
        }
        if !self.spawned.swap(true, Ordering::SeqCst) {
            return tool_call(
                "task",
                json!({"model": "low",
                    "description":"Research Python documentation",
                    "prompt":CHILD_PROMPT,
                    "subagent_type":"bounded-research",
                    "run_in_background":false,
                }),
            );
        }
        finish()
    }
}

struct BrowserRead {
    name: String,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Tool for BrowserRead {
    fn static_name(&self) -> &str {
        &self.name
    }
    fn static_description(&self) -> &str {
        "Browser host fixture"
    }
    fn input_schema(&self) -> Json {
        runtime_json!({"type":"object"})
    }
    fn static_mcp_server(&self) -> Option<&str> {
        Some("selected")
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _: &Json) -> bool {
        false
    }
    fn is_read_only(&self, _: &Json) -> bool {
        true
    }

    async fn call(
        &self,
        input: Json,
        _: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let request: BrowserRuntimeRequest = runtime_codec::decode_value(&input).unwrap();
        assert_eq!(request.target, BrowserTarget::default());
        assert_eq!(request.timeout_ms, Some(20_000));
        assert_eq!(
            request.metadata.get(&RuntimeString::from("toolName")),
            Some(&runtime_json!("web_search"))
        );
        assert_eq!(
            request.operation,
            BrowserOperation::TabsContent {
                urls: vec!["https://www.bing.com/search?q=python".into()],
                content_type: Some("html".into()),
                max_chars: Some(200_000),
                wait_until: Some(BrowserWaitUntil::DomContentLoaded),
            }
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
        let response = BrowserRuntimeResponse {
            value: Some(runtime_json!({"pages":[{
                "url":"https://www.bing.com/search?q=python",
                "content":SEARCH_HTML,
                "status":200,
                "format":"html",
            }]})),
            ..Default::default()
        };
        Ok(text_result(
            runtime_codec::encode_value(&response).unwrap(),
            "Rendered Bing search",
            false,
        ))
    }
}

#[tokio::test]
async fn scoped_research_child_uses_default_canonical_search_with_a_hidden_browser_read() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mapping = BrowserHostToolMapping::for_mcp_server("selected");
    let calls = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicBool::new(false));
    let mut options = agent_options(process.clone());
    options.config.as_mut().unwrap()["prewarm"] = json!(false);
    options.tools = None;
    options.provider = Arc::new(SearchProvider {
        info: ProviderInfo {
            name: "search-fixture".into(),
            default_model: "search-fixture".into(),
            base_url: None,
            env_key: None,
        },
        dependency_name: mapping.tabs_content.clone(),
        spawned: AtomicBool::new(false),
        child_step: AtomicUsize::new(0),
        completed: completed.clone(),
    });
    options.mcp_tools = Some(vec![
        create_tool_search_tool(None),
        Arc::new(BrowserRead {
            name: mapping.tabs_content.clone(),
            calls: calls.clone(),
        }),
        Arc::new(BrowserRead {
            name: mapping.eval.clone(),
            calls: calls.clone(),
        }),
    ]);
    options.capability_roots =
        vec![RuntimeCapabilityRoot::host("selected").with_browser_tool_mapping(mapping)];
    options.include_default_agents = Some(false);
    options.agents = Some(vec![AgentDefinition {
        name: "bounded-research".into(),
        description: "A scoped research fixture".into(),
        tools: Some(vec!["web_search".into(), "tool_search".into()]),
        ..Default::default()
    }]);
    let directory =
        std::env::temp_dir().join(format!("klaus-search-child-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions {
        cwd: Some(directory.to_string_lossy().into_owned().into()),
        user_config_dir: Some(directory.to_string_lossy().into_owned().into()),
        ..Default::default()
    });
    let mut events = session
        .run_streamed(
            Input::Text("Research Python documentation with a scoped child".into()),
            TurnOptions::default(),
        )
        .events;
    while events.next().await.is_some() {}
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(directory);
    assert!(
        completed.load(Ordering::SeqCst),
        "child must receive structured browser search results"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
