//! Defect: small-context models received an unbounded direct tool inventory.
//! Root cause: discovery existed without model-window-aware declaration admission.
//! Work item: 20261019 bounded fixed prefixes with deferred tool activation.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use klaus_agent_core::context::tokens::{estimate_messages_tokens, estimate_prefix_tokens};
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, Session, TurnOptions};
use klaus_agent_core::provider::model_catalog::{with_model_catalog, ModelCatalog};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::tools::build_tool::text_result;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{
    EngineErrorCode, ProviderError, ProviderErrorKind, ToolError,
};
use klaus_agent_core::types::json::{Json, JsonSchema};
use klaus_agent_core::types::permission::{PermissionReason, PermissionResult};
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo, SystemPromptPart,
};
use klaus_agent_core::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};
use serde_json::json;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

type Requests = Arc<Mutex<Vec<ModelRequest>>>;

struct Probe {
    name: String,
    description: String,
    schema: JsonSchema,
    checks: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
    deny: bool,
}

#[async_trait]
impl Tool for Probe {
    fn static_name(&self) -> &str {
        &self.name
    }
    fn static_description(&self) -> &str {
        &self.description
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }
    fn persistence_identity(&self) -> Option<RuntimeString> {
        Some("window-probe-implementation".into())
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _: &Json) -> bool {
        true
    }
    fn is_read_only(&self, _: &Json) -> bool {
        false
    }
    async fn check_permissions(&self, _: &Json, _: &ToolContext) -> Option<PermissionResult> {
        self.checks.fetch_add(1, Ordering::SeqCst);
        self.deny.then(|| PermissionResult::Deny {
            message: "The probe is denied by its original permission check.".into(),
            reason: PermissionReason::ToolSelfCheck,
        })
    }
    async fn call(
        &self,
        _: Json,
        _: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(text_result(Json::Null, "Probe complete.", false))
    }
}

struct Provider {
    info: ProviderInfo,
    window: u64,
    activate: bool,
    direct_probe: bool,
    accept_estimated_pressure: bool,
    reject_oversized_inventory: bool,
    requests: Requests,
}

impl ModelProvider for Provider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }
    fn uses_model_tool_budget(&self, model: &RuntimeString) -> bool {
        model == "window-model"
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
            context_window: self.window,
            max_output_tokens: 4096,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }
    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        let index = {
            let mut requests = self.requests.lock().unwrap();
            let index = requests.len();
            requests.push(request.clone());
            index
        };
        assert!(index < 3, "unexpected provider continuation");
        let prefix_tokens = estimate_prefix_tokens(&request.system, &request.tools);
        let input_limit = self.window - request.max_output_tokens;
        if self.reject_oversized_inventory && prefix_tokens > input_limit {
            assert!(
                !request.tools.iter().any(|tool| tool.name == "tool_search"),
                "an explicitly disabled search tool cannot bypass the inventory limit"
            );
            return Box::pin(futures::stream::iter(vec![ModelStreamEvent::Error {
                error: ProviderError {
                    kind: ProviderErrorKind::PromptTooLong,
                    message: format!(
                        "prompt is too long: {prefix_tokens} tokens > {input_limit} maximum"
                    )
                    .into(),
                    status: Some(400),
                    code: Some("context_length_exceeded".into()),
                    error_type: None,
                    media_origins: None,
                    media_repair_complete: false,
                    retry_after_ms: None,
                    request_id: None,
                },
            }]));
        }
        assert!(
            self.accept_estimated_pressure || prefix_tokens <= input_limit,
            "unexpected request pressure: fixed prefix {prefix_tokens}, input limit {input_limit}"
        );
        let selected = if self.direct_probe {
            (index == 0).then_some(("budget_probe", "{}"))
        } else if self.activate {
            match index {
                0 => Some(("tool_search", r#"{"query":"select:budget_probe"}"#)),
                1 => Some(("budget_probe", "{}")),
                _ => None,
            }
        } else {
            None
        };
        let mut events = vec![ModelStreamEvent::MessageStart {
            model: request.model,
        }];
        if let Some((name, input)) = selected {
            let id = RuntimeString::from(format!("window-call-{index}"));
            events.push(ModelStreamEvent::ToolUseStart {
                id: id.clone(),
                name: name.into(),
                thought_signature: None,
                tool_kind: None,
                input_format: None,
            });
            events.push(ModelStreamEvent::ToolUseInputDelta {
                id: id.clone(),
                partial_json: input.into(),
            });
            events.push(ModelStreamEvent::ToolUseStop { id });
        } else {
            events.push(ModelStreamEvent::TextDelta {
                text: "Finished.".into(),
            });
        }
        events.push(ModelStreamEvent::MessageStop {
            stop_reason: if selected.is_some() {
                ModelStopReason::ToolUse
            } else {
                ModelStopReason::EndTurn
            },
            usage: Usage {
                input_tokens: 500,
                output_tokens: 20,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
                total_tokens: None,
            },
        });
        Box::pin(futures::stream::iter(events))
    }
}

struct NeutralProvider(Provider);

impl ModelProvider for NeutralProvider {
    fn info(&self) -> &ProviderInfo {
        self.0.info()
    }

    fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
        self.0.capabilities(model)
    }

    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        self.0.stream(request, signal)
    }
}

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("klaus-small-context-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub(super) fn assert_workspace_instructions_preserved(request: &ModelRequest) {
    let expected = RuntimeString::from("Mandatory workspace instruction. ".repeat(3000));
    let copies: usize = request
        .messages
        .iter()
        .map(|message| match message {
            Message::User(user) if user.is_meta == Some(true) => user
                .content
                .iter()
                .filter(|block| matches!(block, ContentBlock::Text(text) if text.text == expected))
                .count(),
            _ => 0,
        })
        .sum();
    assert_eq!(
        copies, 1,
        "exact workspace instructions must reach the provider once at user authority"
    );
}

pub(super) struct Fixture {
    pub(super) session: Arc<Session>,
    pub(super) requests: Requests,
    pub(super) checks: Arc<AtomicUsize>,
    pub(super) calls: Arc<AtomicUsize>,
    _directory: Directory,
}

fn fixture(
    window: u64,
    activate: bool,
    deny: bool,
    oversized_instructions: bool,
    search_disabled: bool,
) -> Fixture {
    fixture_with_tool_budget(
        window,
        activate,
        deny,
        oversized_instructions,
        search_disabled,
        true,
    )
}

pub(super) fn fixture_with_tool_budget(
    window: u64,
    activate: bool,
    deny: bool,
    oversized_instructions: bool,
    search_disabled: bool,
    model_tool_budget: bool,
) -> Fixture {
    fixture_with_tool_budget_inventory(
        window,
        activate,
        deny,
        oversized_instructions,
        search_disabled,
        model_tool_budget,
        10_000,
    )
}

pub(super) fn fixture_with_tool_budget_inventory(
    window: u64,
    activate: bool,
    deny: bool,
    oversized_instructions: bool,
    search_disabled: bool,
    model_tool_budget: bool,
    description_repetitions: usize,
) -> Fixture {
    let directory = Directory::new();
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let checks = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut options = agent_options(process);
    let provider = Provider {
        info: ProviderInfo {
            name: "window-budget-fixture".into(),
            default_model: "window-model".into(),
            base_url: None,
            env_key: None,
        },
        window,
        activate,
        direct_probe: activate && !model_tool_budget,
        accept_estimated_pressure: oversized_instructions || !model_tool_budget,
        reject_oversized_inventory: search_disabled,
        requests: requests.clone(),
    };
    let provider: Arc<dyn ModelProvider> = if model_tool_budget {
        Arc::new(provider)
    } else {
        Arc::new(NeutralProvider(provider))
    };
    options.provider = with_model_catalog(provider, ModelCatalog::empty());
    options.include_default_agents = Some(false);
    options.tools = Some(
        (0..17)
            .map(|index| {
                Arc::new(Probe {
                    name: if index == 0 {
                        "budget_probe".to_string()
                    } else {
                        format!("large_tool_{index}")
                    },
                    description: if index == 0 {
                        "Run the permission-checked fixture probe.".to_string()
                    } else {
                        "Large declaration. ".repeat(description_repetitions)
                    },
                    schema: json!({"type":"object","properties":{},"additionalProperties":false})
                        .as_object()
                        .unwrap()
                        .clone(),
                    checks: checks.clone(),
                    calls: calls.clone(),
                    deny,
                }) as Arc<dyn Tool>
            })
            .collect(),
    );
    options.system_prompt_provider = Some(Arc::new(move || {
        Box::pin(async move {
            let mut parts = vec![SystemPromptPart {
                name: "base".into(),
                text: "Use available tools to finish the requested check.".into(),
                cacheable: Some(true),
                cache_scope: None,
            }];
            if oversized_instructions {
                parts.push(SystemPromptPart {
                    name: "memory".into(),
                    text: "Mandatory workspace instruction. ".repeat(3000).into(),
                    cacheable: Some(true),
                    cache_scope: None,
                });
            }
            parts
        })
    }));
    let root = directory.path().to_string_lossy().into_owned();
    // Provider-admission fixtures isolate normal requests from automatic summaries.
    let auto_compaction = !(oversized_instructions || search_disabled);
    let session = Agent::new(options).start_session(StartSessionOptions {
        cwd: Some(root.clone().into()),
        user_config_dir: Some(root.into()),
        config: Some(
            json!({ "model":"window-model", "prewarm":false, "thinking":false,"contextWindow":window,
            "disabledTools": if search_disabled { vec!["tool_search"] } else { Vec::<&str>::new() },
            "autoCompaction": auto_compaction,
            "autoMemory":false,"autoDream":false,"memoryRecall":false,
            "speculativeCompaction":false,"earlyMicrocompact":false,"idleMicrocompact":false,
            "compactionSelfCheck":false,"loopDetection":false }),
        ),
        ..Default::default()
    });
    Fixture {
        session,
        requests,
        checks,
        calls,
        _directory: directory,
    }
}

#[tokio::test]
async fn small_model_discovers_and_activates_its_original_tool_with_permissions_intact() {
    for deny in [false, true] {
        let fixture = fixture(16_384, true, deny, false, false);
        let result = fixture
            .session
            .run(Input::Text("Run the probe.".into()), TurnOptions::default())
            .await;
        assert!(result.error.is_none(), "{:?}", result.error);
        assert_eq!(fixture.checks.load(Ordering::SeqCst), 1);
        assert_eq!(
            fixture.calls.load(Ordering::SeqCst),
            if deny { 0 } else { 1 }
        );
        {
            let requests = fixture.requests.lock().unwrap();
            assert_eq!(requests.len(), 3);
            assert_eq!(
                requests[0]
                    .tools
                    .iter()
                    .map(|tool| tool.name.clone())
                    .collect::<Vec<_>>(),
                vec![RuntimeString::from("tool_search")]
            );
            assert!(requests[1]
                .tools
                .iter()
                .any(|tool| tool.name == "budget_probe"));
            assert!(!requests[1]
                .tools
                .iter()
                .any(|tool| tool.name.starts_with("large_tool_")));
        }
        fixture.session.shutdown_and_wait().await.unwrap();
    }
}

#[tokio::test]
async fn large_model_retains_the_direct_inventory() {
    let fixture = fixture(2_000_000, false, false, false, false);
    let result = fixture
        .session
        .run(Input::Text("Reply briefly.".into()), TurnOptions::default())
        .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    {
        let requests = fixture.requests.lock().unwrap();
        let tools = &requests[0].tools;
        assert!(!tools.iter().any(|tool| tool.name == "tool_search"));
        for index in 0..17 {
            let name = if index == 0 {
                "budget_probe".to_string()
            } else {
                format!("large_tool_{index}")
            };
            assert!(
                tools.iter().any(|tool| tool.name == name.as_str()),
                "{name}"
            );
        }
    }
    fixture.session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn oversized_workspace_instructions_reach_the_provider_without_local_rejection() {
    let fixture = fixture(16_384, false, false, true, false);
    let result = fixture
        .session
        .run(Input::Text("Reply briefly.".into()), TurnOptions::default())
        .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    {
        let requests = fixture.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let request = &requests[0];
        assert_eq!(request.model, "window-model");
        assert_eq!(request.max_output_tokens, 4096);
        assert_workspace_instructions_preserved(request);
        assert!(estimate_messages_tokens(&request.messages) > 16_384);
        assert_eq!(
            request
                .tools
                .iter()
                .map(|tool| tool.name.clone())
                .collect::<Vec<_>>(),
            vec![RuntimeString::from("tool_search")]
        );
    }
    assert_eq!(fixture.checks.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    fixture.session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn a_disabled_search_tool_is_not_added_to_bypass_the_inventory_limit() {
    let fixture = fixture(16_384, false, false, false, true);
    let result = fixture
        .session
        .run(Input::Text("Reply briefly.".into()), TurnOptions::default())
        .await;
    let error = result
        .error
        .expect("the provider must reject its oversized inventory");
    assert_eq!(error.code, EngineErrorCode::PromptTooLong);
    assert_eq!(error.provider_kind, Some(ProviderErrorKind::PromptTooLong));
    assert_eq!(
        error.provider_error().unwrap().code.as_ref().unwrap(),
        "context_length_exceeded"
    );
    {
        let requests = fixture.requests.lock().unwrap();
        assert_eq!(
            requests.len(),
            1,
            "the provider rejection must stop the turn"
        );
        let request = &requests[0];
        assert_eq!(request.model, "window-model");
        assert!(!request.tools.iter().any(|tool| tool.name == "tool_search"));
        assert_eq!(request.tools.len(), 18);
        assert!(request.tools.iter().any(|tool| tool.name == "update_plan"));
        for index in 0..17 {
            let name = if index == 0 {
                "budget_probe".to_string()
            } else {
                format!("large_tool_{index}")
            };
            let tool = request
                .tools
                .iter()
                .find(|tool| tool.name == name.as_str())
                .expect("the direct inventory must remain intact");
            if index != 0 {
                assert!(
                    tool.description == "Large declaration. ".repeat(10_000),
                    "declaration changed for {name}"
                );
            }
        }
    }
    assert_eq!(fixture.checks.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    fixture.session.shutdown_and_wait().await.unwrap();
}
