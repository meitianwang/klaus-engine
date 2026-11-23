//! Defect: deferred tool loading, static fallback catalog announcements, and prefix stability gaps.
//! Root cause: server search outputs bypassed runtime surfacing, tool freeze epochs reset per turn,
//! lifecycle cuts leaked surfaced/frozen state, Anthropic reminder/beta paths drifted, OpenAI namespace
//! downgrades dropped or missed nested items, and oversized static catalog summaries oscillated across turns.
//! Work item: 20261119 deferred tool loading and prefix stability review.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use klaus_agent_core::context::compact::CompactionNoopReason;
use klaus_agent_core::engine::agent::{Agent, ForkCut, RefreshToolsThunk, StartSessionOptions};
use klaus_agent_core::engine::deps::{CompactionResult, Compactor};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::provider::anthropic::{AnthropicProvider, AnthropicProviderOptions};
use klaus_agent_core::provider::openai::{OpenAIProvider, OpenAIProviderOptions, OpenAiApi};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{
    runtime_codec, runtime_json, RuntimeJson, RuntimePath, RuntimeString,
};
use klaus_agent_core::schemas::config::ResolvedConfig;
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock, ToolUseBlock};
use klaus_agent_core::schemas::message::{
    AssistantMessage, CompactBoundaryMessage, Message, ToolResultMessage, Usage, UserMessage,
};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::tools::build_tool::{
    text_result, DEFAULT_IS_CONCURRENCY_SAFE, DEFAULT_IS_DESTRUCTIVE, DEFAULT_IS_ENABLED,
    DEFAULT_IS_READ_ONLY,
};
use klaus_agent_core::tools::builtins::tool_search::{
    create_call_mcp_tool, create_tool_search_tool,
};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::ToolError;
use klaus_agent_core::types::json::{Json, JsonSchema};
use klaus_agent_core::types::provider::{
    ModelCapabilities, ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo, ProviderToolCallKind, ToolInventoryEvent, ToolSpec,
    ToolSpecKind, WireCapabilityPolicy,
};
use klaus_agent_core::types::tool::{Tool, ToolContext, ToolExposure, ToolProgress, ToolResult};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

struct ProbeTool {
    tool_name: RuntimeString,
    description_text: RuntimeString,
    schema: JsonSchema,
    exposure_kind: ToolExposure,
    calls: Arc<AtomicUsize>,
    max_result_chars: Option<u64>,
    payload_size: usize,
}

impl ProbeTool {
    fn new(name: &str, exposure: ToolExposure) -> (Self, Arc<AtomicUsize>) {
        Self::with_description(name, &format!("Probe tool {name}"), exposure)
    }

    fn with_description(
        name: &str,
        desc: &str,
        exposure: ToolExposure,
    ) -> (Self, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let tool = Self {
            tool_name: RuntimeString::from(name),
            description_text: RuntimeString::from(desc),
            schema: json!({
                "type": "object",
                "properties": {
                    "note": { "type": "string" }
                }
            })
            .as_object()
            .cloned()
            .unwrap(),
            exposure_kind: exposure,
            calls: calls.clone(),
            max_result_chars: None,
            payload_size: 16,
        };
        (tool, calls)
    }

    fn with_large_payload(mut self, max_chars: u64, payload_size: usize) -> Self {
        self.max_result_chars = Some(max_chars);
        self.payload_size = payload_size;
        self
    }
}

#[async_trait]
impl Tool for ProbeTool {
    fn name(&self) -> RuntimeString {
        self.tool_name.clone()
    }

    fn description(&self) -> RuntimeString {
        self.description_text.clone()
    }

    fn input_schema(&self) -> Json {
        runtime_json::parse(&RuntimeString::from(
            serde_json::to_string(&Value::Object(self.schema.clone())).unwrap(),
        ))
        .unwrap()
    }

    fn exposure(&self) -> ToolExposure {
        self.exposure_kind
    }

    fn max_result_size_chars(&self) -> Option<u64> {
        self.max_result_chars
    }

    fn is_enabled(&self) -> bool {
        DEFAULT_IS_ENABLED
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        DEFAULT_IS_CONCURRENCY_SAFE
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        DEFAULT_IS_READ_ONLY
    }

    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        DEFAULT_IS_DESTRUCTIVE
    }

    async fn call(
        &self,
        _input: Json,
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let text = "x".repeat(self.payload_size);
        Ok(text_result(Json::Null, text, false))
    }
}

#[derive(Clone)]
struct FixedSummaryCompactor;

#[async_trait]
impl Compactor for FixedSummaryCompactor {
    fn rebind_model(
        &self,
        _: Arc<dyn ModelProvider>,
        _: &ResolvedConfig,
    ) -> Result<Arc<dyn Compactor>, String> {
        Ok(Arc::new(self.clone()))
    }

    async fn maybe_compact(
        &self,
        messages: &[Message],
        _: u64,
        _: &ResolvedConfig,
        _: Option<&RuntimeString>,
    ) -> CompactionResult {
        CompactionResult {
            messages: messages.to_vec(),
            freed_tokens: 0,
            compacted: false,
            reason: Some(CompactionNoopReason::NoProgress),
            diagnostics: Vec::new(),
        }
    }

    async fn reactive_compact(
        &self,
        _: &[Message],
        _: &ResolvedConfig,
        _: Option<&RuntimeString>,
    ) -> CompactionResult {
        CompactionResult {
            messages: vec![Message::CompactBoundary(CompactBoundaryMessage {
                uuid: "cb-20261119".into(),
                summary: "Compacted earlier turns.".into(),
                native_compaction: None,
            })],
            freed_tokens: 500,
            compacted: true,
            reason: None,
            diagnostics: Vec::new(),
        }
    }
}

fn ok_usage() -> Usage {
    Usage {
        input_tokens: 128,
        output_tokens: 24,
        cache_read_input_tokens: 0,
        cache_creation_input_tokens: 0,
        total_tokens: None,
    }
}

#[tokio::test]
async fn server_tool_search_surfaces_deferred_tool_in_same_turn_and_persists_across_resume() {
    struct ServerSearchProvider {
        info: ProviderInfo,
        requests: Mutex<Vec<ModelRequest>>,
        step: AtomicUsize,
    }

    impl ModelProvider for ServerSearchProvider {
        fn info(&self) -> &ProviderInfo {
            &self.info
        }

        fn native_deferred_loading_active(&self, _: &RuntimeString) -> bool {
            true
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

        fn stream(&self, req: ModelRequest, _: AbortSignal) -> ModelStream {
            self.requests.lock().unwrap().push(req.clone());
            let idx = self.step.fetch_add(1, Ordering::SeqCst);
            let events = if idx == 0 {
                let payload = runtime_json::parse(&RuntimeString::from(
                    json!({
                        "status": "completed",
                        "execution": "server",
                        "tools": [{
                            "type": "function",
                            "name": "deferred_probe",
                            "description": "Probe tool deferred_probe",
                            "inputSchema": { "type": "object", "properties": { "note": { "type": "string" } } }
                        }]
                    })
                    .to_string(),
                ))
                .unwrap();
                vec![
                    ModelStreamEvent::MessageStart {
                        model: req.model.clone(),
                    },
                    ModelStreamEvent::ToolUseStart {
                        id: "srv-search-1".into(),
                        name: "tool_search".into(),
                        thought_signature: None,
                        tool_kind: Some(ProviderToolCallKind::ServerToolSearch),
                        input_format: None,
                    },
                    ModelStreamEvent::ToolUseInputDelta {
                        id: "srv-search-1".into(),
                        partial_json: "{\"query\":\"deferred_probe\"}".into(),
                    },
                    ModelStreamEvent::ToolUseStop {
                        id: "srv-search-1".into(),
                    },
                    ModelStreamEvent::ServerToolSearchOutput {
                        id: "srv-search-1".into(),
                        payload,
                    },
                    ModelStreamEvent::ToolUseStart {
                        id: "call-probe-1".into(),
                        name: "deferred_probe".into(),
                        thought_signature: None,
                        tool_kind: None,
                        input_format: None,
                    },
                    ModelStreamEvent::ToolUseInputDelta {
                        id: "call-probe-1".into(),
                        partial_json: "{\"note\":\"same-turn\"}".into(),
                    },
                    ModelStreamEvent::ToolUseStop {
                        id: "call-probe-1".into(),
                    },
                    ModelStreamEvent::MessageStop {
                        stop_reason: ModelStopReason::ToolUse,
                        usage: ok_usage(),
                    },
                ]
            } else {
                vec![
                    ModelStreamEvent::MessageStart {
                        model: req.model.clone(),
                    },
                    ModelStreamEvent::TextDelta {
                        text: "done".into(),
                    },
                    ModelStreamEvent::MessageStop {
                        stop_reason: ModelStopReason::EndTurn,
                        usage: ok_usage(),
                    },
                ]
            };
            Box::pin(futures::stream::iter(events))
        }
    }

    let temp = tempfile::tempdir().unwrap();
    let store_dir = temp.path().join("sessions");
    let cfg_dir = temp.path().join("cfg");
    std::fs::create_dir_all(&store_dir).unwrap();
    std::fs::create_dir_all(&cfg_dir).unwrap();
    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(store_dir.to_string_lossy().into_owned()),
        archive_dir: None,
    }));

    let (probe_tool, probe_calls) = ProbeTool::new("deferred_probe", ToolExposure::Deferred);
    let provider = Arc::new(ServerSearchProvider {
        info: ProviderInfo {
            name: "openai".into(),
            base_url: Some("https://api.openai.com/v1".into()),
            env_key: None,
            default_model: "gpt-5.6-sol".into(),
        },
        requests: Mutex::new(Vec::new()),
        step: AtomicUsize::new(0),
    });

    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut opts = agent_options(process);
    opts.provider = provider.clone();
    opts.config = Some(json!({
        "model": "gpt-5.6-sol",
        "approval_policy": "never",
        "sandbox_mode": "danger-full-access",
        "prewarm": false,
        "autoMemory": false,
        "autoDream": false,
        "memoryRecall": false,
        "checkpointing": false
    }));
    opts.tools = Some(vec![create_tool_search_tool(None), Arc::new(probe_tool)]);
    let agent = Agent::new(opts);

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

    let turn = session
        .run(
            Input::Text("Search and run deferred_probe".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(turn.error.is_none(), "{:?}", turn.error);
    assert_eq!(
        probe_calls.load(Ordering::SeqCst),
        1,
        "server search output in the same stream must surface deferred_probe before dispatch"
    );

    {
        let requests = provider.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(
            !requests[0]
                .tools
                .iter()
                .any(|spec| spec.name == "deferred_probe"),
            "initial request must not pre-declare deferred_probe"
        );
        assert!(
            requests[1]
                .tools
                .iter()
                .any(|spec| spec.name == "deferred_probe"),
            "continuation request must include newly surfaced deferred_probe"
        );
        assert!(
            requests[1]
                .surfaced_deferred_names
                .contains(&RuntimeString::from("deferred_probe")),
            "continuation request must carry deferred_probe in surfaced_deferred_names"
        );
    }

    session.dispose().await;

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
            Input::Text("Check after resume".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(turn2.error.is_none(), "{:?}", turn2.error);
    {
        let requests = provider.requests.lock().unwrap();
        let last = requests.last().unwrap();
        assert!(
            last.tools.iter().any(|spec| spec.name == "deferred_probe"),
            "resumed session must preserve server-surfaced deferred_probe"
        );
        assert!(
            last.surfaced_deferred_names
                .contains(&RuntimeString::from("deferred_probe")),
            "resumed session must preserve surfaced_deferred_names"
        );
    }
    resumed.dispose().await;
}

#[tokio::test]
async fn tool_result_budget_spill_and_multi_turn_session_preserve_frozen_tools_and_wire_caps() {
    struct FrozenProbeProvider {
        info: ProviderInfo,
        requests: Mutex<Vec<ModelRequest>>,
        step: AtomicUsize,
    }

    impl ModelProvider for FrozenProbeProvider {
        fn info(&self) -> &ProviderInfo {
            &self.info
        }

        fn native_deferred_loading_active(&self, _: &RuntimeString) -> bool {
            true
        }

        fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
            ProviderCapabilities {
                supports_image_detail_original: false,
                streaming: true,
                tools: true,
                thinking: false,
                structured_output: false,
                web_search: false,
                prompt_cache_control: true,
                context_window: 200_000,
                max_output_tokens: 8_192,
                native_pdf: false,
                vision: false,
                image_limits: None,
                max_media_blocks: None,
            }
        }

        fn model_metadata(&self, _: &RuntimeString) -> ModelCapabilities {
            ModelCapabilities {
                native_deferred_tools: Some(true),
                mid_convo_system_messages: Some(true),
                mid_convo_tool_changes: Some(true),
                cache_control_on_tools: Some(true),
                cache_scope_supported: Some(true),
                cache_ttl_1h: Some(false),
                context_window: Some(200_000),
                max_output_tokens: Some(8_192),
                ..Default::default()
            }
        }

        fn stream(&self, req: ModelRequest, _: AbortSignal) -> ModelStream {
            self.requests.lock().unwrap().push(req.clone());
            let idx = self.step.fetch_add(1, Ordering::SeqCst);
            let events = if idx == 0 {
                vec![
                    ModelStreamEvent::MessageStart {
                        model: req.model.clone(),
                    },
                    ModelStreamEvent::ToolUseStart {
                        id: "call-big-1".into(),
                        name: "big_probe".into(),
                        thought_signature: None,
                        tool_kind: None,
                        input_format: None,
                    },
                    ModelStreamEvent::ToolUseInputDelta {
                        id: "call-big-1".into(),
                        partial_json: "{}".into(),
                    },
                    ModelStreamEvent::ToolUseStop {
                        id: "call-big-1".into(),
                    },
                    ModelStreamEvent::MessageStop {
                        stop_reason: ModelStopReason::ToolUse,
                        usage: ok_usage(),
                    },
                ]
            } else {
                vec![
                    ModelStreamEvent::MessageStart {
                        model: req.model.clone(),
                    },
                    ModelStreamEvent::TextDelta {
                        text: format!("ok-{idx}").into(),
                    },
                    ModelStreamEvent::MessageStop {
                        stop_reason: ModelStopReason::EndTurn,
                        usage: ok_usage(),
                    },
                ]
            };
            Box::pin(futures::stream::iter(events))
        }
    }

    let temp = tempfile::tempdir().unwrap();
    let cfg_dir = temp.path().join("cfg");
    std::fs::create_dir_all(&cfg_dir).unwrap();

    let (big_probe, big_calls) = ProbeTool::new("big_probe", ToolExposure::Direct);
    let big_probe: Arc<dyn Tool> = Arc::new(big_probe.with_large_payload(50_000, 4_000));
    let (extra_tool, _) = ProbeTool::new("extra_probe", ToolExposure::Direct);
    let extra_tool: Arc<dyn Tool> = Arc::new(extra_tool);
    let expose_extra = Arc::new(AtomicBool::new(false));
    let expose_flag = expose_extra.clone();
    let base_tool = big_probe.clone();
    let refresh: RefreshToolsThunk = Arc::new(move || {
        if expose_flag.load(Ordering::SeqCst) {
            vec![base_tool.clone(), extra_tool.clone()]
        } else {
            vec![base_tool.clone()]
        }
    });

    let provider = Arc::new(FrozenProbeProvider {
        info: ProviderInfo {
            name: "anthropic".into(),
            base_url: Some("https://api.anthropic.com".into()),
            env_key: None,
            default_model: "claude-sonnet-5-5".into(),
        },
        requests: Mutex::new(Vec::new()),
        step: AtomicUsize::new(0),
    });

    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut opts = agent_options(process);
    opts.provider = provider.clone();
    opts.config = Some(json!({
        "model": "claude-sonnet-5-5",
        "approval_policy": "never",
        "sandbox_mode": "danger-full-access",
        "aggregateToolResults": true,
        "maxToolResultsPerMessageChars": 3000,
        "prewarm": false,
        "autoMemory": false,
        "autoDream": false,
        "memoryRecall": false,
        "checkpointing": false
    }));
    opts.tools = Some(vec![big_probe]);
    opts.refresh_tools = Some(refresh);
    let agent = Agent::new(opts);

    let session = agent.start_session(StartSessionOptions {
        cwd: Some(RuntimePath::from(temp.path().to_string_lossy().as_ref())),
        user_config_dir: Some(RuntimePath::from(cfg_dir.to_string_lossy().as_ref())),
        ..Default::default()
    });

    let turn1 = session
        .run(Input::Text("Run big_probe".into()), TurnOptions::default())
        .await;
    assert!(turn1.error.is_none(), "{:?}", turn1.error);
    assert_eq!(big_calls.load(Ordering::SeqCst), 1);

    // Expose a second tool before Turn 2: session-scoped ToolFreezeState must
    // preserve Turn 1's frozen baseline and record an anchored inventory event
    // instead of rebasing frozen_tools to [big_probe, extra_probe].
    expose_extra.store(true, Ordering::SeqCst);
    let turn2 = session
        .run(
            Input::Text("Second turn after tool refresh".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(turn2.error.is_none(), "{:?}", turn2.error);

    let requests = provider.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 3);
    for (idx, req) in requests.iter().enumerate() {
        assert!(
            req.frozen_tools.is_some(),
            "request {idx} lost frozen_tools"
        );
        assert!(
            req.frozen_instructions.is_some(),
            "request {idx} lost frozen_instructions"
        );
        let caps = req
            .wire_caps
            .as_ref()
            .unwrap_or_else(|| panic!("request {idx} lost wire_caps"));
        assert_eq!(caps.cache_ttl_1h, Some(false));
        assert_eq!(caps.mid_convo_tool_changes, Some(true));
        assert_eq!(req.native_deferred_tools, Some(true));
    }

    let req0_frozen = requests[0].frozen_tools.as_ref().unwrap();
    let req1_frozen = requests[1].frozen_tools.as_ref().unwrap();
    let req2_frozen = requests[2].frozen_tools.as_ref().unwrap();
    assert!(req0_frozen.iter().any(|spec| spec.name == "big_probe"));
    assert!(!req0_frozen.iter().any(|spec| spec.name == "extra_probe"));
    assert_eq!(
        req1_frozen, req0_frozen,
        "spilled budget re-prepare changed frozen_tools"
    );
    assert_eq!(
        req2_frozen, req0_frozen,
        "turn 2 rebased frozen_tools instead of preserving turn 1 baseline"
    );
    assert!(
        requests[2]
            .tool_inventory_events
            .iter()
            .any(|event| event.added.iter().any(|spec| spec.name == "extra_probe")),
        "turn 2 must emit extra_probe as an anchored ToolInventoryEvent"
    );

    session.dispose().await;
}

#[tokio::test]
async fn rollback_fork_and_compaction_keep_surfaced_and_frozen_state_consistent() {
    struct LifecycleProvider {
        info: ProviderInfo,
        requests: Mutex<Vec<ModelRequest>>,
    }

    impl ModelProvider for LifecycleProvider {
        fn info(&self) -> &ProviderInfo {
            &self.info
        }

        fn native_deferred_loading_active(&self, _: &RuntimeString) -> bool {
            true
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

        fn model_metadata(&self, _: &RuntimeString) -> ModelCapabilities {
            ModelCapabilities {
                native_deferred_tools: Some(true),
                mid_convo_system_messages: Some(true),
                mid_convo_tool_changes: Some(true),
                ..Default::default()
            }
        }

        fn stream(&self, req: ModelRequest, _: AbortSignal) -> ModelStream {
            let count = {
                let mut guard = self.requests.lock().unwrap();
                guard.push(req.clone());
                guard.len()
            };
            let trigger_search = req
                .messages
                .last()
                .and_then(|msg| match msg {
                    Message::User(user) => user.content.iter().find_map(|block| match block {
                        ContentBlock::Text(t) => t.text.try_to_utf8().ok(),
                        _ => None,
                    }),
                    _ => None,
                })
                .is_some_and(|text| text == "unlock-alpha");

            let events = if trigger_search {
                let search_id: RuntimeString = format!("srv-alpha-{count}").into();
                let payload = runtime_json::parse(&RuntimeString::from(
                    json!({
                        "status": "completed",
                        "execution": "server",
                        "tools": [{
                            "type": "function",
                            "name": "deferred_alpha",
                            "description": "Probe tool deferred_alpha",
                            "inputSchema": { "type": "object", "properties": { "note": { "type": "string" } } }
                        }]
                    })
                    .to_string(),
                ))
                .unwrap();
                vec![
                    ModelStreamEvent::MessageStart {
                        model: req.model.clone(),
                    },
                    ModelStreamEvent::ToolUseStart {
                        id: search_id.clone(),
                        name: "tool_search".into(),
                        thought_signature: None,
                        tool_kind: Some(ProviderToolCallKind::ServerToolSearch),
                        input_format: None,
                    },
                    ModelStreamEvent::ToolUseInputDelta {
                        id: search_id.clone(),
                        partial_json: "{\"query\":\"deferred_alpha\"}".into(),
                    },
                    ModelStreamEvent::ToolUseStop {
                        id: search_id.clone(),
                    },
                    ModelStreamEvent::ServerToolSearchOutput {
                        id: search_id,
                        payload,
                    },
                    ModelStreamEvent::TextDelta {
                        text: "unlocked".into(),
                    },
                    ModelStreamEvent::MessageStop {
                        stop_reason: ModelStopReason::EndTurn,
                        usage: ok_usage(),
                    },
                ]
            } else {
                vec![
                    ModelStreamEvent::MessageStart {
                        model: req.model.clone(),
                    },
                    ModelStreamEvent::TextDelta {
                        text: format!("reply-{count}").into(),
                    },
                    ModelStreamEvent::MessageStop {
                        stop_reason: ModelStopReason::EndTurn,
                        usage: ok_usage(),
                    },
                ]
            };
            Box::pin(futures::stream::iter(events))
        }
    }

    let temp = tempfile::tempdir().unwrap();
    let store_dir = temp.path().join("sessions");
    let cfg_dir = temp.path().join("cfg");
    std::fs::create_dir_all(&store_dir).unwrap();
    std::fs::create_dir_all(&cfg_dir).unwrap();
    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(store_dir.to_string_lossy().into_owned()),
        archive_dir: None,
    }));

    let (alpha_tool, _) = ProbeTool::new("deferred_alpha", ToolExposure::Deferred);
    let provider = Arc::new(LifecycleProvider {
        info: ProviderInfo {
            name: "anthropic".into(),
            base_url: Some("https://api.anthropic.com".into()),
            env_key: None,
            default_model: "claude-sonnet-5-5".into(),
        },
        requests: Mutex::new(Vec::new()),
    });

    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut opts = agent_options(process);
    opts.provider = provider.clone();
    opts.config = Some(json!({
        "model": "claude-sonnet-5-5",
        "prewarm": false,
        "autoMemory": false,
        "autoDream": false,
        "memoryRecall": false,
        "checkpointing": false
    }));
    opts.tools = Some(vec![create_tool_search_tool(None), Arc::new(alpha_tool)]);
    opts.compactor = Some(Arc::new(FixedSummaryCompactor));
    let agent = Agent::new(opts);

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

    // Turn 1: ordinary prompt, nothing surfaced.
    let t1 = session
        .run(Input::Text("turn-one".into()), TurnOptions::default())
        .await;
    assert!(t1.error.is_none(), "{:?}", t1.error);

    // Turn 2: surface deferred_alpha via ServerToolSearchOutput.
    let t2 = session
        .run(Input::Text("unlock-alpha".into()), TurnOptions::default())
        .await;
    assert!(t2.error.is_none(), "{:?}", t2.error);

    let turn2_id = {
        let idx = session.turn_index_snapshot();
        session
            .visible_history()
            .iter()
            .rev()
            .find_map(|m| {
                let uuid = match m {
                    Message::User(u) => Some(&u.uuid),
                    _ => None,
                }?;
                idx.message_turn
                    .get(uuid)
                    .and_then(|t| t.try_to_utf8().ok())
            })
            .expect("turn 2 id in turn_index")
    };

    // Fork strictly before Turn 2 -> deferred_alpha must NOT be surfaced in fork.
    let forked = agent
        .fork_session_with_cut(
            store.clone(),
            &session_id,
            Some(ForkCut::BeforeTurn(turn2_id)),
            StartSessionOptions {
                cwd: Some(RuntimePath::from(temp.path().to_string_lossy().as_ref())),
                user_config_dir: Some(RuntimePath::from(cfg_dir.to_string_lossy().as_ref())),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let fork_turn = forked
        .run(Input::Text("check-fork".into()), TurnOptions::default())
        .await;
    assert!(fork_turn.error.is_none(), "{:?}", fork_turn.error);
    {
        let requests = provider.requests.lock().unwrap();
        let last = requests.last().unwrap();
        assert!(
            !last.tools.iter().any(|spec| spec.name == "deferred_alpha"),
            "fork before Turn 2 must exclude deferred_alpha surfaced during Turn 2"
        );
    }
    forked.dispose().await;

    // Roll back Turn 2 -> deferred_alpha must be pruned live and after resume.
    session.rollback(1).await.unwrap();
    let after_rollback = session
        .run(Input::Text("after-rollback".into()), TurnOptions::default())
        .await;
    assert!(after_rollback.error.is_none(), "{:?}", after_rollback.error);
    {
        let requests = provider.requests.lock().unwrap();
        let last = requests.last().unwrap();
        assert!(
            !last.tools.iter().any(|spec| spec.name == "deferred_alpha"),
            "live session after rollback must prune deferred_alpha"
        );
    }
    session.dispose().await;

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
    let after_resume = resumed
        .run(
            Input::Text("check-resume-after-rollback".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(after_resume.error.is_none(), "{:?}", after_resume.error);
    {
        let requests = provider.requests.lock().unwrap();
        let last = requests.last().unwrap();
        assert!(
            !last.tools.iter().any(|spec| spec.name == "deferred_alpha"),
            "resumed session must fold RolloutItem::Rollback and keep deferred_alpha pruned"
        );
    }

    // Re-surface deferred_alpha, compact past a CompactBoundary, then inspect post-compact request.
    let resurface = resumed
        .run(Input::Text("unlock-alpha".into()), TurnOptions::default())
        .await;
    assert!(resurface.error.is_none(), "{:?}", resurface.error);
    resumed.compact(None).await.unwrap();
    let post_compact = resumed
        .run(Input::Text("after-compact".into()), TurnOptions::default())
        .await;
    assert!(post_compact.error.is_none(), "{:?}", post_compact.error);
    {
        let requests = provider.requests.lock().unwrap();
        let last = requests.last().unwrap();
        assert!(
            last.tools.iter().any(|spec| spec.name == "deferred_alpha"),
            "post-compaction request must retain previously surfaced deferred_alpha in direct tools"
        );
        assert!(
            !last
                .surfaced_deferred_names
                .contains(&RuntimeString::from("deferred_alpha")),
            "post-compaction request must keep surfaced_deferred_names empty until a post-boundary search"
        );
    }
    resumed.dispose().await;
}

async fn read_http_request(socket: &mut tokio::net::TcpStream) -> (String, Value) {
    let mut bytes = Vec::new();
    loop {
        let mut chunk = [0; 4096];
        let count = socket.read(&mut chunk).await.unwrap();
        assert_ne!(count, 0);
        bytes.extend_from_slice(&chunk[..count]);
        assert!(bytes.len() < 2 * 1024 * 1024);
        if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&bytes[..end]).into_owned();
            let length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            if bytes.len() >= end + 4 + length {
                let body: Value =
                    serde_json::from_slice(&bytes[end + 4..end + 4 + length]).unwrap();
                return (headers, body);
            }
        }
    }
}

#[tokio::test]
async fn anthropic_turn1_sends_search_beta_places_reminder_after_cache_and_flips_on_400() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut captured = Vec::new();
        for idx in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            captured.push(read_http_request(&mut socket).await);
            let (status, content_type, body) = if idx == 0 {
                (
                    "400 Bad Request",
                    "application/json",
                    "{\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"header tool-search-tool-2025-10-19 rejected\"}}",
                )
            } else {
                (
                    "200 OK",
                    "text/event-stream",
                    "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-sonnet-4-6\",\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\ndata: {\"type\":\"message_stop\"}\n\n",
                )
            };
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            let _ = socket.shutdown().await;
        }
        captured
    });

    let provider = AnthropicProvider::new(AnthropicProviderOptions {
        base_url: Some(base),
        api_key: Some("fixture".into()),
        default_model: Some("claude-sonnet-4-6".into()),
        capabilities: Some(ModelCapabilities {
            native_deferred_tools: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    });
    assert!(provider.native_deferred_loading_active(&"claude-sonnet-4-6".into()));

    let mut req: ModelRequest = runtime_codec::decode_text(&RuntimeString::from(
        json!({
            "model": "claude-sonnet-4-6",
            "system": [],
            "messages": [],
            "maxOutputTokens": 128,
            "tools": [
                { "name": "tool_search", "description": "Search", "inputSchema": { "type": "object" } },
                { "name": "read", "description": "Read", "inputSchema": { "type": "object" } }
            ]
        })
        .to_string(),
    ))
    .unwrap();
    req.deferred_tools = vec![ToolSpec {
        name: "mcp__demo__ping".into(),
        description: "Ping".into(),
        input_schema: runtime_json::parse(&RuntimeString::from("{\"type\":\"object\"}")).unwrap(),
        kind: ToolSpecKind::Function,
        format: None,
        namespace: None,
    }];
    req.messages = vec![Message::User(UserMessage {
        question_answer: None,
        uuid: "u1".into(),
        content: vec![ContentBlock::Text(TextBlock {
            text: "hi".into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })],
        is_meta: None,
    })];

    let events = tokio::time::timeout(
        Duration::from_secs(10),
        provider.stream(req, AbortSignal::new()).collect::<Vec<_>>(),
    )
    .await
    .unwrap();
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::Error { .. })),
        "unexpected error events: {events:?}"
    );
    assert!(
        !provider.native_deferred_loading_active(&"claude-sonnet-4-6".into()),
        "400 refusal must flip native_deferred_loading_active to false for static fallback on the next turn"
    );
    let server_handle = server;
    drop(provider);

    let captured = tokio::time::timeout(Duration::from_secs(2), server_handle)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(captured.len(), 2);

    // Attempt 0 sends the search beta from Turn 1 (even before any tool is marked defer_loading)
    // and anchors the frozen deferred announcement inside the Turn 1 user cache prefix.
    let beta_0 = captured[0]
        .0
        .lines()
        .find(|line| line.to_ascii_lowercase().starts_with("anthropic-beta:"))
        .unwrap_or("");
    assert!(
        beta_0.contains("tool-search-tool-2025-10-19"),
        "Turn 1 must send search beta header, got: {beta_0}"
    );
    let user_blocks = captured[0].1["messages"][0]["content"].as_array().unwrap();
    assert_eq!(user_blocks.len(), 2);
    assert!(user_blocks[0].get("cache_control").is_none());
    assert!(user_blocks[1].get("cache_control").is_some());
    assert!(user_blocks[1]["text"]
        .as_str()
        .unwrap()
        .contains("<system-reminder>"));

    // Attempt 1 retries immediately without the rejected search beta header.
    let beta_1 = captured[1]
        .0
        .lines()
        .find(|line| line.to_ascii_lowercase().starts_with("anthropic-beta:"))
        .unwrap_or("");
    assert!(
        !beta_1.contains("tool-search-tool-2025-10-19"),
        "downgraded Turn 1 retry must omit rejected search beta header, got: {beta_1}"
    );
}

#[tokio::test]
async fn anthropic_defer_loading_400_downgrades_to_direct_tools_and_omits_search_beta() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut captured = Vec::new();
        for idx in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            captured.push(read_http_request(&mut socket).await);
            let (status, content_type, body) = if idx == 0 {
                (
                    "400 Bad Request",
                    "application/json",
                    "{\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"tools.1.defer_loading: Extra inputs are not permitted\"}}",
                )
            } else {
                (
                    "200 OK",
                    "text/event-stream",
                    "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-sonnet-4-6\",\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\ndata: {\"type\":\"message_stop\"}\n\n",
                )
            };
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            let _ = socket.shutdown().await;
        }
        captured
    });

    let provider = AnthropicProvider::new(AnthropicProviderOptions {
        base_url: Some(base),
        api_key: Some("fixture".into()),
        default_model: Some("claude-sonnet-4-6".into()),
        capabilities: Some(ModelCapabilities {
            native_deferred_tools: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    });

    let mut req: ModelRequest = runtime_codec::decode_text(&RuntimeString::from(
        json!({
            "model": "claude-sonnet-4-6",
            "system": [],
            "messages": [],
            "maxOutputTokens": 128,
            "tools": [
                { "name": "read", "description": "Read", "inputSchema": { "type": "object" } },
                { "name": "deferred_probe", "description": "Probe", "inputSchema": { "type": "object" } }
            ]
        })
        .to_string(),
    ))
    .unwrap();
    req.surfaced_deferred_names = vec!["deferred_probe".into()];
    req.messages = vec![
        Message::Assistant(AssistantMessage {
            uuid: "a1".into(),
            model: "claude-sonnet-4-6".into(),
            usage: None,
            content: vec![ContentBlock::ToolUse(ToolUseBlock {
                id: "ts-1".into(),
                name: "tool_search".into(),
                input: RuntimeJson::Null,
                thought_signature: None,
                tool_kind: Some(ProviderToolCallKind::ToolSearch),
            })],
        }),
        Message::ToolResult(ToolResultMessage {
            uuid: "tr-1".into(),
            tool_use_id: "ts-1".into(),
            content: vec![klaus_agent_core::schemas::content::OutputBlock::Text(
                TextBlock {
                    text: "found".into(),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                },
            )],
            is_error: false,
            tool_kind: Some(ProviderToolCallKind::ToolSearch),
            native_tool_output: Some(
                runtime_json::parse(&RuntimeString::from("{\"matched\":[\"deferred_probe\"]}"))
                    .unwrap(),
            ),
            output: None,
            extra_user_blocks: None,
        }),
    ];

    let events = tokio::time::timeout(
        Duration::from_secs(10),
        provider.stream(req, AbortSignal::new()).collect::<Vec<_>>(),
    )
    .await
    .unwrap();
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::Error { .. })),
        "unexpected error events: {events:?}"
    );
    drop(provider);

    let captured = tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(captured.len(), 2);

    let beta_0 = captured[0]
        .0
        .lines()
        .find(|line| line.to_ascii_lowercase().starts_with("anthropic-beta:"))
        .unwrap_or("");
    assert!(
        beta_0.contains("tool-search-tool-2025-10-19")
            || beta_0.contains("advanced-tool-use-2025-11-20"),
        "attempt 0 should send search beta header, got: {beta_0}"
    );
    let tools_0 = captured[0].1["tools"].as_array().unwrap();
    let probe_0 = tools_0
        .iter()
        .find(|t| t["name"] == "deferred_probe")
        .expect("deferred_probe on attempt 0");
    assert_eq!(probe_0["defer_loading"], Value::Bool(true));

    let beta_1 = captured[1]
        .0
        .lines()
        .find(|line| line.to_ascii_lowercase().starts_with("anthropic-beta:"))
        .unwrap_or("");
    assert!(
        !beta_1.contains("tool-search-tool-2025-10-19")
            && !beta_1.contains("advanced-tool-use-2025-11-20"),
        "downgraded attempt 1 must omit both deferred-tool betas, got: {beta_1}"
    );
    let tools_1 = captured[1].1["tools"].as_array().unwrap();
    let probe_1 = tools_1
        .iter()
        .find(|t| t["name"] == "deferred_probe")
        .expect("deferred_probe survives downgrade on attempt 1");
    assert!(probe_1.get("defer_loading").is_none());
    let has_tool_ref_1 = captured[1].1["messages"]
        .as_array()
        .unwrap()
        .iter()
        .any(|m| {
            m["content"].as_array().is_some_and(|blocks| {
                blocks.iter().any(|b| {
                    b["type"] == "tool_result"
                        && b["content"].as_array().is_some_and(|items| {
                            items.iter().any(|i| i["type"] == "tool_reference")
                        })
                })
            })
        });
    assert!(
        !has_tool_ref_1,
        "downgraded attempt 1 must convert tool_reference blocks to text"
    );
}

#[tokio::test]
async fn openai_responses_namespace_400_flattens_namespaces_without_dropping_batches_or_tool_search(
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut bodies = Vec::new();
        for idx in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let (_headers, body) = read_http_request(&mut socket).await;
            bodies.push(body);
            let (status, content_type, body) = if idx == 0 {
                (
                    "400 Bad Request",
                    "application/json",
                    "{\"error\":{\"message\":\"Invalid value: 'namespace'. Supported values are 'function', 'custom', and 'web_search'.\"}}",
                )
            } else {
                (
                    "200 OK",
                    "text/event-stream",
                    "data: {\"type\":\"response.completed\",\"response\":{\"output\":[]}}\n\n",
                )
            };
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            let _ = socket.shutdown().await;
        }
        bodies
    });

    let provider = OpenAIProvider::new(OpenAIProviderOptions {
        base_url: Some(base),
        api_key: Some("fixture".into()),
        api: Some(OpenAiApi::Responses),
        default_model: Some("gpt-5.6-sol".into()),
        capabilities: Some(ModelCapabilities {
            native_deferred_tools: Some(true),
            mid_convo_system_messages: Some(true),
            additional_tools: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    });

    let search_spec = ToolSpec {
        name: "tool_search".into(),
        description: "Find tools".into(),
        input_schema: runtime_json::parse(&RuntimeString::from("{\"type\":\"object\"}")).unwrap(),
        kind: ToolSpecKind::ToolSearch,
        format: None,
        namespace: None,
    };
    let read_spec = ToolSpec {
        name: "read".into(),
        description: "Read".into(),
        input_schema: runtime_json::parse(&RuntimeString::from("{\"type\":\"object\"}")).unwrap(),
        kind: ToolSpecKind::Function,
        format: None,
        namespace: None,
    };
    let ns_spec = ToolSpec {
        name: "mcp__demo__ping".into(),
        description: "Ping demo server".into(),
        input_schema: runtime_json::parse(&RuntimeString::from("{\"type\":\"object\"}")).unwrap(),
        kind: ToolSpecKind::Function,
        format: None,
        namespace: Some("mcp__demo".into()),
    };

    let mut req: ModelRequest = runtime_codec::decode_text(&RuntimeString::from(
        json!({
            "model": "gpt-5.6-sol",
            "system": [],
            "messages": [],
            "maxOutputTokens": 128,
            "tools": []
        })
        .to_string(),
    ))
    .unwrap();
    req.messages = vec![Message::User(UserMessage {
        question_answer: None,
        uuid: "u1".into(),
        content: vec![ContentBlock::Text(TextBlock {
            text: "hi".into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })],
        is_meta: None,
    })];
    req.wire_caps = Some(WireCapabilityPolicy {
        mid_convo_system_messages: Some(true),
        additional_tools: Some(true),
        ..Default::default()
    });
    req.frozen_tools = Some(vec![search_spec.clone(), read_spec.clone()]);
    req.tools = vec![search_spec, read_spec, ns_spec.clone()];
    req.tool_inventory_events = vec![ToolInventoryEvent {
        anchor_uuid: None,
        added: vec![ns_spec],
        removed: vec![],
    }];

    let events = tokio::time::timeout(
        Duration::from_secs(10),
        provider.stream(req, AbortSignal::new()).collect::<Vec<_>>(),
    )
    .await
    .unwrap();
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::Error { .. })),
        "unexpected error events: {events:?}"
    );
    drop(provider);

    let bodies = tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bodies.len(), 2);

    let collect_batch_tools = |body: &Value| -> Vec<Value> {
        body["input"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|item| item["type"] == "additional_tools")
            .flat_map(|item| item["tools"].as_array().unwrap().iter().cloned())
            .collect()
    };

    let batch_tools_0 = collect_batch_tools(&bodies[0]);
    assert!(
        batch_tools_0
            .iter()
            .any(|entry| entry["type"] == "namespace"),
        "attempt 0 should send namespace entries inside additional_tools: {batch_tools_0:?}"
    );
    assert!(
        batch_tools_0
            .iter()
            .any(|entry| entry["type"] == "tool_search"),
        "attempt 0 should send tool_search: {batch_tools_0:?}"
    );

    let batch_tools_1 = collect_batch_tools(&bodies[1]);
    assert!(
        !batch_tools_1.is_empty(),
        "attempt 1 must preserve additional_tools batches"
    );
    assert!(
        !batch_tools_1
            .iter()
            .any(|entry| entry["type"] == "namespace"),
        "attempt 1 must flatten namespace entries: {batch_tools_1:?}"
    );
    assert!(
        batch_tools_1
            .iter()
            .any(|entry| entry["type"] == "tool_search"),
        "attempt 1 must keep tool_search intact: {batch_tools_1:?}"
    );
    assert!(
        batch_tools_1
            .iter()
            .any(|entry| entry["name"] == "mcp__demo__ping"),
        "attempt 1 must expose flattened mcp__demo__ping declaration: {batch_tools_1:?}"
    );
}

#[tokio::test]
async fn static_fallback_oversized_catalog_is_idempotent_and_reannounces_after_compaction() {
    struct StaticFallbackProvider {
        info: ProviderInfo,
        requests: Mutex<Vec<ModelRequest>>,
        step: AtomicUsize,
    }

    impl ModelProvider for StaticFallbackProvider {
        fn info(&self) -> &ProviderInfo {
            &self.info
        }

        fn native_deferred_loading_active(&self, _: &RuntimeString) -> bool {
            false
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

        fn stream(&self, req: ModelRequest, _: AbortSignal) -> ModelStream {
            self.requests.lock().unwrap().push(req.clone());
            let idx = self.step.fetch_add(1, Ordering::SeqCst);
            let events = vec![
                ModelStreamEvent::MessageStart {
                    model: req.model.clone(),
                },
                ModelStreamEvent::TextDelta {
                    text: format!("ok-{idx}").into(),
                },
                ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::EndTurn,
                    usage: ok_usage(),
                },
            ];
            Box::pin(futures::stream::iter(events))
        }
    }

    let temp = tempfile::tempdir().unwrap();
    let cfg_dir = temp.path().join("cfg");
    std::fs::create_dir_all(&cfg_dir).unwrap();

    let mut tools: Vec<Arc<dyn Tool>> = vec![create_tool_search_tool(None), create_call_mcp_tool()];
    let (direct_read, _) = ProbeTool::new("read", ToolExposure::Direct);
    tools.push(Arc::new(direct_read));
    for server in 0..40 {
        for idx in 0..20 {
            let name = format!("mcp__srv{server}__tool{idx}");
            let desc = format!(
                "A lengthy capability description for tool {idx} of server {server} that pushes the catalog announcement past the notification byte budget."
            );
            let (tool, _) = ProbeTool::with_description(&name, &desc, ToolExposure::Deferred);
            tools.push(Arc::new(tool));
        }
    }

    let provider = Arc::new(StaticFallbackProvider {
        info: ProviderInfo {
            name: "openai".into(),
            base_url: Some("https://compat.example/v1".into()),
            env_key: None,
            default_model: "compat-chat".into(),
        },
        requests: Mutex::new(Vec::new()),
        step: AtomicUsize::new(0),
    });

    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut opts = agent_options(process);
    opts.provider = provider.clone();
    opts.config = Some(json!({
        "model": "compat-chat",
        "prewarm": false,
        "autoMemory": false,
        "autoDream": false,
        "memoryRecall": false,
        "checkpointing": false
    }));
    opts.tools = Some(tools);
    opts.compactor = Some(Arc::new(FixedSummaryCompactor));
    let agent = Agent::new(opts);

    let session = agent.start_session(StartSessionOptions {
        cwd: Some(RuntimePath::from(temp.path().to_string_lossy().as_ref())),
        user_config_dir: Some(RuntimePath::from(cfg_dir.to_string_lossy().as_ref())),
        ..Default::default()
    });

    let count_catalog_msgs = |req: &ModelRequest| -> usize {
        req.messages
            .iter()
            .filter(|m| {
                matches!(
                    m,
                    Message::User(u)
                        if u.content.iter().any(|b| {
                            matches!(
                                b,
                                ContentBlock::Text(t)
                                    if t.text
                                        .try_to_utf8()
                                        .is_ok_and(|s| s.contains("<deferred-tool-catalog>"))
                            )
                        })
                )
            })
            .count()
    };

    let t1 = session
        .run(Input::Text("turn-1".into()), TurnOptions::default())
        .await;
    assert!(t1.error.is_none(), "{:?}", t1.error);
    let t2 = session
        .run(Input::Text("turn-2".into()), TurnOptions::default())
        .await;
    assert!(t2.error.is_none(), "{:?}", t2.error);

    session.compact(None).await.unwrap();
    let t3 = session
        .run(Input::Text("turn-3".into()), TurnOptions::default())
        .await;
    assert!(t3.error.is_none(), "{:?}", t3.error);

    {
        let requests = provider.requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        // Static fallback keeps the wire tool list minimal and frozen across every turn.
        let tool_names = |req: &ModelRequest| -> Vec<String> {
            req.tools
                .iter()
                .map(|spec| spec.name.try_to_utf8().unwrap())
                .collect()
        };
        assert_eq!(tool_names(&requests[0]), tool_names(&requests[1]));
        assert_eq!(tool_names(&requests[1]), tool_names(&requests[2]));
        assert!(requests[0].deferred_tools.is_empty());

        // Turn 1 emits one summary catalog notification; Turn 2 adds zero duplicate announcements.
        assert_eq!(count_catalog_msgs(&requests[0]), 1);
        assert_eq!(
            count_catalog_msgs(&requests[1]),
            1,
            "oversized summary must remain idempotent across repeat turns"
        );
        // After compaction drops the pre-boundary catalog message, Turn 3 re-announces once.
        assert_eq!(
            count_catalog_msgs(&requests[2]),
            1,
            "post-compaction turn must re-announce the active catalog"
        );
    }
    session.dispose().await;
}
