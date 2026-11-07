//! The browser surface has exactly one owner per session shape. A host that
//! injects a direct browser runtime gets the engine's browser_* protocol
//! tools; a host that only declares an MCP tool mapping keeps the browser
//! surface on its own MCP tools, and the engine registers no browser_*
//! builtins for it. The mapping still backs internal consumers: a scoped
//! research child keeps its hidden bounded search dependency, and a cold
//! continuation rebuilds that dependency from the current authorized context
//! while present denials stay effective.
//! Work item: 2026-11-07 browser surface ownership.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use klaus_agent_core::browser::{
    attest_spawn_free, BrowserExecutor, BrowserHostToolMapping, BrowserOperation,
    BrowserRuntimeError, BrowserRuntimeRequest, BrowserRuntimeResponse, BrowserTarget,
    BrowserWaitUntil,
};
use klaus_agent_core::capabilities::RuntimeCapabilityRoot;
use klaus_agent_core::engine::agent::{Agent, AgentOptions, StartSessionOptions};
use klaus_agent_core::engine::agent_definitions::AgentDefinition;
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeString};
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::{Message, ToolResultMessage, Usage};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::tools::build_tool::text_result;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::ToolError;
use klaus_agent_core::types::json::Json;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::{
    SubagentMessageRequest, Tool, ToolContext, ToolProgress, ToolResult,
};
use serde_json::json;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

const WAIT: Duration = Duration::from_secs(30);
const ROOT_THREAD: &str = "surface-ownership-root";
const CHILD_PROMPT: &str = "surface-ownership-search-child";
const SEARCH_HTML: &str = r#"<ol id="b_results"><li class="b_algo"><h2><a href="https://docs.python.org/3/">Python docs</a></h2><div class="b_caption"><p>Official documentation.</p></div></li></ol>"#;

macro_rules! runtime_json {
    ($($value:tt)*) => {
        runtime_json::parse(&RuntimeString::from(json!($($value)*).to_string())).unwrap()
    };
}

fn fixture_info() -> ProviderInfo {
    ProviderInfo {
        name: "surface-fixture".into(),
        default_model: "surface-fixture".into(),
        base_url: None,
        env_key: None,
    }
}

fn fixture_capabilities() -> ProviderCapabilities {
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

fn usage() -> Usage {
    Usage {
        input_tokens: 10,
        output_tokens: 4,
        cache_read_input_tokens: 0,
        cache_creation_input_tokens: 0,
        total_tokens: None,
    }
}

fn answer(text: &str) -> ModelStream {
    Box::pin(futures::stream::iter(vec![
        ModelStreamEvent::MessageStart {
            model: "surface-fixture".into(),
        },
        ModelStreamEvent::TextDelta { text: text.into() },
        ModelStreamEvent::MessageStop {
            stop_reason: ModelStopReason::EndTurn,
            usage: usage(),
        },
    ]))
}

fn tool_call(name: &str, input: serde_json::Value) -> ModelStream {
    tool_call_with_id(&format!("call-{name}"), name, input)
}

/// Script a call under an explicit id. A cold continuation replays persisted
/// history whose reloaded tool results carry no structured output, so the cold
/// script must not share call ids with the warm phase.
fn tool_call_with_id(call_id: &str, name: &str, input: serde_json::Value) -> ModelStream {
    let id = RuntimeString::from(call_id);
    Box::pin(futures::stream::iter(vec![
        ModelStreamEvent::MessageStart {
            model: "surface-fixture".into(),
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

fn child_request(request: &ModelRequest) -> bool {
    request.messages.iter().any(|message| match message {
        Message::User(message) => message.content.iter().any(
            |block| matches!(block, ContentBlock::Text(text) if text.text.contains(CHILD_PROMPT)),
        ),
        _ => false,
    })
}

fn assert_clean_scoped_surface(request: &ModelRequest) {
    assert!(
        request
            .tools
            .iter()
            .all(|tool| !tool.name.starts_with("mcp__") && !tool.name.starts_with("browser_")),
        "scoped research must not expose general browser tools"
    );
}

struct RecordingProvider {
    info: ProviderInfo,
    requests: Mutex<Vec<ModelRequest>>,
}

impl ModelProvider for RecordingProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        fixture_capabilities()
    }

    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        self.requests.lock().unwrap().push(request);
        answer("Surface inspected.")
    }
}

struct RuntimeSurfaceProvider {
    info: ProviderInfo,
    step: AtomicUsize,
}

impl ModelProvider for RuntimeSurfaceProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        fixture_capabilities()
    }

    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        match self.step.fetch_add(1, Ordering::SeqCst) {
            0 => {
                assert!(
                    request.tools.iter().any(|tool| tool.name == "browser_tabs")
                        && request
                            .tools
                            .iter()
                            .any(|tool| tool.name == "browser_navigate"),
                    "an injected direct runtime keeps the engine browser surface registered"
                );
                tool_call("browser_tabs", json!({"action":"list"}))
            }
            1 => {
                let tabs = result(&request, "call-browser_tabs");
                assert!(
                    !tabs.is_error,
                    "direct runtime dispatch must return a successful tool result"
                );
                answer("Runtime surface inspected.")
            }
            n => panic!("unexpected runtime surface request {n}"),
        }
    }
}

struct CountingBrowser(Arc<AtomicUsize>);

// This fixture executor answers in process and has no process effects.
#[async_trait]
unsafe impl BrowserExecutor for CountingBrowser {
    async fn execute(
        &self,
        request: BrowserRuntimeRequest,
        _: AbortSignal,
        _: klaus_agent_core::browser::BrowserEventSink,
    ) -> Result<BrowserRuntimeResponse, BrowserRuntimeError> {
        assert!(matches!(request.operation, BrowserOperation::Tabs { .. }));
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(BrowserRuntimeResponse {
            message: Some("The injected runtime answered.".into()),
            ..Default::default()
        })
    }
}

struct WarmSearchProvider {
    info: ProviderInfo,
    spawned: AtomicBool,
    child_step: AtomicUsize,
    completed: Arc<AtomicBool>,
}

impl ModelProvider for WarmSearchProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        fixture_capabilities()
    }

    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        if child_request(&request) {
            return match self.child_step.fetch_add(1, Ordering::SeqCst) {
                0 => {
                    assert_clean_scoped_surface(&request);
                    tool_call("web_search", json!({"query":"python"}))
                }
                1 => {
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
                    self.completed.store(true, Ordering::SeqCst);
                    answer("Research completed.")
                }
                n => panic!("unexpected warm child request {n}"),
            };
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
        answer("Parent finished delegation.")
    }
}

struct ColdSearchProvider {
    info: ProviderInfo,
    denied: bool,
    child_step: AtomicUsize,
    completed: Arc<AtomicBool>,
    denial_observed: Arc<AtomicBool>,
}

impl ModelProvider for ColdSearchProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        fixture_capabilities()
    }

    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        assert!(
            child_request(&request),
            "the cold continuation addresses the saved child"
        );
        match self.child_step.fetch_add(1, Ordering::SeqCst) {
            0 => {
                assert_clean_scoped_surface(&request);
                tool_call_with_id(
                    "call-cold-web_search",
                    "web_search",
                    json!({"query":"python"}),
                )
            }
            1 => {
                let search = result(&request, "call-cold-web_search");
                if self.denied {
                    assert!(
                        search.is_error,
                        "a present denial must keep the hidden dependency unavailable"
                    );
                    self.denial_observed.store(true, Ordering::SeqCst);
                } else {
                    assert!(
                        !search.is_error,
                        "the rebuilt dependency must serve the cold search"
                    );
                    let output = search
                        .output
                        .as_ref()
                        .expect("structured canonical search output");
                    assert_eq!(output.get("count").and_then(Json::as_u64), Some(1));
                }
                self.completed.store(true, Ordering::SeqCst);
                answer("Cold research completed.")
            }
            n => panic!("unexpected cold child request {n}"),
        }
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

fn search_options(
    process: RuntimeProcessContext,
    provider: Arc<dyn ModelProvider>,
    mapping: &BrowserHostToolMapping,
    calls: Arc<AtomicUsize>,
    disallowed: Option<Vec<RuntimeString>>,
) -> AgentOptions {
    let mut options = agent_options(process);
    options.config.as_mut().unwrap()["prewarm"] = json!(false);
    options.tools = None;
    options.provider = provider;
    options.mcp_tools = Some(vec![
        Arc::new(BrowserRead {
            name: mapping.tabs_content.clone(),
            calls: calls.clone(),
        }),
        Arc::new(BrowserRead {
            name: mapping.eval.clone(),
            calls,
        }),
    ]);
    options.capability_roots =
        vec![RuntimeCapabilityRoot::host("selected").with_browser_tool_mapping(mapping.clone())];
    options.include_default_agents = Some(false);
    options.agents = Some(vec![AgentDefinition {
        name: "bounded-research".into(),
        description: "A scoped research fixture".into(),
        tools: Some(vec!["web_search".into(), "tool_search".into()]),
        disallowed_tools: disallowed,
        ..Default::default()
    }]);
    options
}

#[tokio::test]
async fn mapping_only_host_registers_no_engine_browser_builtins() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let provider = Arc::new(RecordingProvider {
        info: fixture_info(),
        requests: Mutex::new(Vec::new()),
    });
    let mut options = agent_options(process.clone());
    options.config.as_mut().unwrap()["prewarm"] = json!(false);
    options.tools = None;
    options.provider = provider.clone();
    options.capability_roots = vec![RuntimeCapabilityRoot::host("selected")
        .with_browser_tool_mapping(BrowserHostToolMapping::for_mcp_server("selected"))];
    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions::default());
    let result = session
        .run(
            Input::Text("Inspect the surface.".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    assert!(
        session
            .runtime_capability_snapshot_for_turn(&TurnOptions::default())
            .capabilities
            .browser_tool_mapping
            .is_some(),
        "the declared mapping is adopted for internal dispatch"
    );
    {
        let requests = provider.requests.lock().unwrap();
        let request = requests.last().unwrap();
        assert!(
            !request
                .tools
                .iter()
                .any(|tool| tool.name.starts_with("browser_")),
            "a mapping-only host keeps the browser surface on its own MCP tools"
        );
        assert!(
            request.tools.iter().any(|tool| tool.name == "web_search"),
            "the internal mapping consumer stays registered"
        );
    }
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn direct_runtime_host_keeps_engine_browser_builtins() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    // The fixture executor cannot create, control, or delegate a local process.
    let runtime = unsafe { attest_spawn_free(Arc::new(CountingBrowser(calls.clone()))) };
    let mut options = agent_options(process.clone());
    options.config.as_mut().unwrap()["prewarm"] = json!(false);
    options.tools = None;
    options.provider = Arc::new(RuntimeSurfaceProvider {
        info: fixture_info(),
        step: AtomicUsize::new(0),
    });
    options.capability_roots =
        vec![RuntimeCapabilityRoot::host("native").with_browser_runtime(runtime)];
    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions::default());
    let result = session
        .run(
            Input::Text("Inspect the runtime surface.".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the injected runtime answers the native dispatch"
    );
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn cold_reopened_research_child_rebuilds_search_dependency_from_current_context() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    for scenario in ["recover", "dependency-disallowed"] {
        eprintln!("Browser surface ownership scenario: {scenario}");
        let denied = scenario == "dependency-disallowed";
        let directory =
            std::env::temp_dir().join(format!("klaus-surface-ownership-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let store_options = JsonlSessionStoreOptions {
            dir: Some(directory.join("sessions").to_string_lossy().into_owned()),
            ..Default::default()
        };
        let start = || StartSessionOptions {
            id: Some(ROOT_THREAD.into()),
            cwd: Some(directory.to_string_lossy().into_owned().into()),
            user_config_dir: Some(directory.to_string_lossy().into_owned().into()),
            ..Default::default()
        };
        let mapping = BrowserHostToolMapping::for_mcp_server("selected");
        let warm_calls = Arc::new(AtomicUsize::new(0));
        let warm_completed = Arc::new(AtomicBool::new(false));
        let store = Arc::new(JsonlSessionStore::new(store_options.clone()));
        let warm = Agent::new(search_options(
            process.clone(),
            Arc::new(WarmSearchProvider {
                info: fixture_info(),
                spawned: AtomicBool::new(false),
                child_step: AtomicUsize::new(0),
                completed: warm_completed.clone(),
            }),
            &mapping,
            warm_calls.clone(),
            None,
        ))
        .open_session(store.clone(), start())
        .await
        .unwrap();
        let result = warm
            .run(
                Input::Text("Run the bounded search.".into()),
                TurnOptions::default(),
            )
            .await;
        assert!(result.error.is_none(), "{scenario}: {:?}", result.error);
        assert!(
            warm_completed.load(Ordering::SeqCst),
            "{scenario}: warm child must finish its hidden-dependency search"
        );
        assert_eq!(
            warm_calls.load(Ordering::SeqCst),
            1,
            "{scenario}: warm search must reach the admitted host read"
        );
        let lifecycle = warm.subagent_lifecycle_snapshot();
        assert_eq!(
            lifecycle.agents.len(),
            1,
            "{scenario}: one scoped research child"
        );
        let child = lifecycle.agents[0].agent_id.clone();
        warm.flush_rollout().await.unwrap();
        warm.shutdown_and_wait().await.unwrap();

        let current_calls = Arc::new(AtomicUsize::new(0));
        let cold_completed = Arc::new(AtomicBool::new(false));
        let denial_observed = Arc::new(AtomicBool::new(false));
        let cold = Agent::new(search_options(
            process.clone(),
            Arc::new(ColdSearchProvider {
                info: fixture_info(),
                denied,
                child_step: AtomicUsize::new(0),
                completed: cold_completed.clone(),
                denial_observed: denial_observed.clone(),
            }),
            &mapping,
            current_calls.clone(),
            denied.then(|| vec![mapping.tabs_content.clone().into()]),
        ))
        .resume_session(
            Arc::new(JsonlSessionStore::new(store_options.clone())),
            ROOT_THREAD,
            start(),
        )
        .await
        .unwrap();
        let reply = cold
            .send_subagent_message(SubagentMessageRequest {
                target: child.clone(),
                message: "Rerun the bounded search.".into(),
                summary: "Rerun the bounded search".into(),
                sender_agent_id: None,
                trigger_turn: true,
                user_initiated: true,
            })
            .await
            .expect("stored ordinary child must remain addressable")
            .expect("unchanged capability roots must restore the child");
        assert!(
            reply.resumed,
            "{scenario}: declarations restore before current dispatch policy is enforced"
        );
        tokio::time::timeout(WAIT, async {
            while !cold_completed.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            warm_calls.load(Ordering::SeqCst),
            1,
            "{scenario}: cold continuation must not reuse the previous host executor"
        );
        if denied {
            assert!(
                denial_observed.load(Ordering::SeqCst),
                "{scenario}: the present denial must surface as an explicit search error"
            );
            assert_eq!(
                current_calls.load(Ordering::SeqCst),
                0,
                "{scenario}: a denied dependency must not reach the current host executor"
            );
        } else {
            assert_eq!(
                current_calls.load(Ordering::SeqCst),
                1,
                "{scenario}: the rebuilt dependency must reach the current host executor"
            );
        }
        cold.shutdown_and_wait().await.unwrap();
        std::fs::remove_dir_all(&directory).unwrap();
    }
    process.shutdown().await.unwrap();
}
