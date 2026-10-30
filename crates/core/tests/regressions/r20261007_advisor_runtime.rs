//! Public Session regression for independently configured advisor consultation.
//! The query must supply live effective context, not a separately sampled transcript.
//! Work item 20261024: oversize advisor context needs a main-model summary
//! without replacing parent history or counting binary media as JSON text.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{
    InferenceTarget, InferenceTargetState, Input, Session, TurnOptions, UserContentPart,
};
use klaus_agent_core::observability::cost_tracker::ModelPricing;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeString};
use klaus_agent_core::schemas::content::{
    Citation, ContentBlock, ImageBlock, MediaBlock, NativeSearchResult, OutputBlock,
};
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::tools::build_tool::text_result;
use klaus_agent_core::tools::builtins::advisor::create_advisor_tool;
use klaus_agent_core::tools::builtins::task::{create_task_tool, TaskToolOptions};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{ProviderError, ProviderErrorKind, ToolError};
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::json::Json;
use klaus_agent_core::types::provider::{
    ModelCapabilities, ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo, SystemPromptPart,
};
use klaus_agent_core::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};
use serde_json::json;
use tokio::sync::Notify;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

const WAIT: Duration = Duration::from_secs(10);

fn usage() -> Usage {
    Usage {
        input_tokens: 100,
        output_tokens: 10,
        cache_read_input_tokens: 20,
        cache_creation_input_tokens: 3,
        total_tokens: None,
    }
}

fn completion(value: &str) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::TextDelta { text: value.into() },
        ModelStreamEvent::MessageStop {
            stop_reason: ModelStopReason::EndTurn,
            usage: usage(),
        },
    ]
}

fn tool_events(id: &str, name: &str, args: &str) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::ToolUseStart {
            id: id.into(),
            name: name.into(),
            thought_signature: Some("foreign-tool-signature".into()),
            tool_kind: None,
            input_format: None,
        },
        ModelStreamEvent::ToolUseInputDelta {
            id: id.into(),
            partial_json: args.into(),
        },
        ModelStreamEvent::ToolUseStop { id: id.into() },
    ]
}

fn consultation(id: &str) -> Vec<ModelStreamEvent> {
    let mut events = tool_events(
        id,
        "advisor",
        r#"{"question":"What should I verify next?"}"#,
    );
    events.push(ModelStreamEvent::MessageStop {
        stop_reason: ModelStopReason::ToolUse,
        usage: usage(),
    });
    events
}

struct Provider {
    info: ProviderInfo,
    scripts: Mutex<VecDeque<Vec<ModelStreamEvent>>>,
    requests: Mutex<Vec<ModelRequest>>,
    signals: Mutex<Vec<AbortSignal>>,
    caps: Mutex<ProviderCapabilities>,
    model_output_limit: Mutex<Option<(RuntimeString, u64)>>,
    media_types: Mutex<Vec<RuntimeString>>,
    on_next_request: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    estimate_request: AtomicBool,
    hang_at: Mutex<Option<usize>>,
    started: Notify,
    hang: bool,
}

impl Provider {
    fn new(name: &str, scripts: Vec<Vec<ModelStreamEvent>>, hang: bool) -> Arc<Self> {
        Arc::new(Self {
            info: ProviderInfo {
                name: name.into(),
                default_model: name.into(),
                base_url: None,
                env_key: None,
            },
            scripts: Mutex::new(scripts.into()),
            requests: Mutex::new(Vec::new()),
            signals: Mutex::new(Vec::new()),
            started: Notify::new(),
            hang,
            on_next_request: Mutex::new(None),
            estimate_request: AtomicBool::new(false),
            hang_at: Mutex::new(None),
            media_types: Mutex::new(Vec::new()),
            caps: Mutex::new(ProviderCapabilities {
                supports_image_detail_original: false,
                streaming: true,
                tools: true,
                thinking: true,
                structured_output: false,
                web_search: false,
                prompt_cache_control: true,
                context_window: 200_000,
                max_output_tokens: 8192,
                native_pdf: true,
                vision: true,
                image_limits: None,
                max_media_blocks: None,
            }),
            model_output_limit: Mutex::new(None),
        })
    }
}

impl ModelProvider for Provider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }
    fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
        let mut caps = self.caps.lock().unwrap().clone();
        if let Some((limited_model, limit)) = &*self.model_output_limit.lock().unwrap() {
            if limited_model == model {
                caps.max_output_tokens = *limit;
            }
        }
        caps
    }
    fn model_metadata(&self, _: &RuntimeString) -> ModelCapabilities {
        ModelCapabilities {
            media_input_mime_types: self.media_types.lock().unwrap().clone(),
            ..Default::default()
        }
    }
    fn estimate_tokens(&self, request: &ModelRequest) -> Option<u64> {
        Some(if self.estimate_request.load(Ordering::SeqCst) {
            klaus_agent_core::provider::anthropic::estimate_tokens_fallback(request)
        } else {
            100
        })
    }
    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        let number = {
            let mut requests = self.requests.lock().unwrap();
            requests.push(request);
            requests.len()
        };
        if let Some(callback) = self.on_next_request.lock().unwrap().take() {
            callback();
        }
        self.signals.lock().unwrap().push(signal.clone());
        self.started.notify_one();
        if self.hang || *self.hang_at.lock().unwrap() == Some(number) {
            return Box::pin(
                async_stream::stream! { signal.cancelled().await; if false { yield ModelStreamEvent::TextDelta { text: "never".into() }; } },
            );
        }
        let events = self
            .scripts
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| completion("main finished"));
        Box::pin(futures::stream::iter(events))
    }
}

struct EvidenceTool {
    native_media: bool,
}
#[async_trait]
impl Tool for EvidenceTool {
    fn is_enabled(&self) -> bool {
        true
    }
    // External tools provide runtime names and deliberately have no static name.
    fn name(&self) -> RuntimeString {
        "evidence_probe".into()
    }
    fn static_description(&self) -> &str {
        "Supply deterministic evidence"
    }
    fn input_schema(&self) -> Json {
        runtime_json::parse(&"{\"type\":\"object\"}".into()).unwrap()
    }
    fn is_read_only(&self, _: &Json) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _: &Json) -> bool {
        false
    }
    async fn call(
        &self,
        _: Json,
        _: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let mut result = text_result(Json::Null, "current-tool-evidence", false);
        if self.native_media {
            result.serializable.extra_user_blocks = Some(vec![ContentBlock::Media(MediaBlock {
                mime_type: "audio/wav".into(),
                base64: "UklGRg==".into(),
                filename: Some("current-evidence.wav".into()),
            })]);
            return Ok(result);
        }
        result
            .serializable
            .model_content
            .push(OutputBlock::Image(ImageBlock {
                mime_type: "image/png".into(),
                base64: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScLbtAAAAABJRU5ErkJggg==".into(),
                detail: None,
                source_path: None,
            }));
        Ok(result)
    }
}

struct Fixture {
    process: RuntimeProcessContext,
    session: Arc<Session>,
    main: Arc<Provider>,
    advisor: Arc<Provider>,
}

impl Fixture {
    fn new(
        main_scripts: Vec<Vec<ModelStreamEvent>>,
        advisor_scripts: Vec<Vec<ModelStreamEvent>>,
        hang: bool,
    ) -> Self {
        Self::with_native_media(main_scripts, advisor_scripts, hang, false)
    }

    fn with_native_media(
        main_scripts: Vec<Vec<ModelStreamEvent>>,
        advisor_scripts: Vec<Vec<ModelStreamEvent>>,
        hang: bool,
        native_media: bool,
    ) -> Self {
        Self::configured(main_scripts, advisor_scripts, hang, native_media, json!({}))
    }

    fn configured(
        main_scripts: Vec<Vec<ModelStreamEvent>>,
        advisor_scripts: Vec<Vec<ModelStreamEvent>>,
        hang: bool,
        native_media: bool,
        config: serde_json::Value,
    ) -> Self {
        let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
        let main = Provider::new("main-source", main_scripts, false);
        let advisor = Provider::new("independent-advisor", advisor_scripts, hang);
        if native_media {
            *main.media_types.lock().unwrap() = vec!["audio/wav".into()];
        }
        let mut options = agent_options(process.clone());
        options.provider = main.clone();
        // Keep capacity tests independent of machine-local prompt files.
        options.system_prompt_provider = Some(Arc::new(|| {
            Box::pin(async {
                vec![SystemPromptPart {
                    name: "fixture-instructions".into(),
                    text: "Preserve project constraints and evidence references.".into(),
                    cacheable: Some(true),
                    cache_scope: None,
                }]
            })
        }));
        options.tools = Some(vec![
            create_advisor_tool(),
            Arc::new(EvidenceTool { native_media }),
            create_task_tool(TaskToolOptions::default()),
        ]);
        options.include_default_agents = Some(false);
        for (key, value) in json!({ "model":"main-source", "prewarm":false, "fork":false, "autoMemory":false, "autoDream":false, "memoryRecall":false, "speculativeCompaction":false, "earlyMicrocompact":false, "idleMicrocompact":false, "compactionSelfCheck":false, "loopDetection":false, "compactBufferTokens":0, "maxOutputTokens":4096 }).as_object().unwrap() {
            options.config.as_mut().unwrap()[key] = value.clone();
        }
        for (key, value) in config.as_object().unwrap() {
            options.config.as_mut().unwrap()[key] = value.clone();
        }
        options.pricing = Some(std::collections::HashMap::from([(
            "main-source".into(),
            ModelPricing {
                input_per_m_tok: 1.0,
                output_per_m_tok: 2.0,
                cache_read_per_m_tok: 0.1,
                cache_write_per_m_tok: 1.1,
            },
        )]));
        let session = Agent::new(options).start_session(StartSessionOptions::default());
        Self {
            process,
            session,
            main,
            advisor,
        }
    }

    fn enable(&self) {
        self.session
            .install_advisor_target(Some(InferenceTargetState::Ready(InferenceTarget {
                source: "independent".into(),
                model_id: "advisor-pick".into(),
                model: "independent-advisor".into(),
                provider: self.advisor.clone(),
                review_requirements: None,
                pricing: vec![(
                    "independent-advisor".into(),
                    ModelPricing {
                        input_per_m_tok: 1.0,
                        output_per_m_tok: 2.0,
                        cache_read_per_m_tok: 0.1,
                        cache_write_per_m_tok: 1.1,
                    },
                )],
            })))
            .unwrap();
    }

    async fn run(&self) -> Vec<EngineEvent> {
        let events: Vec<_> = tokio::time::timeout(
            WAIT,
            self.session
                .run_streamed(
                    Input::Text("FULL-CONTEXT-BEGIN ".repeat(1000).into()),
                    TurnOptions::default(),
                )
                .events
                .collect(),
        )
        .await
        .unwrap();
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
            "{events:?}"
        );
        events
    }

    fn tool_error(&self) -> String {
        self.session
            .visible_history()
            .iter()
            .filter_map(|message| match message {
                Message::ToolResult(result) if result.is_error => Some(
                    result
                        .content
                        .iter()
                        .filter_map(|block| match block {
                            OutputBlock::Text(text) => text.text.try_to_utf8().ok(),
                            _ => None,
                        })
                        .collect::<String>(),
                ),
                _ => None,
            })
            .collect()
    }

    async fn close(self) {
        self.session.shutdown_and_wait().await.unwrap();
        self.process.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn advisor_projects_current_tool_media_before_session_history_persistence() {
    let mut first = tool_events("evidence", "evidence_probe", "{}");
    first.extend(consultation("consult"));
    let fixture = Fixture::with_native_media(
        vec![first, completion("main finished")],
        vec![completion("The audio was not read; verify its transcript.")],
        false,
        true,
    );
    {
        let mut caps = fixture.advisor.caps.lock().unwrap();
        caps.tools = false;
        caps.thinking = false;
        caps.vision = false;
        caps.native_pdf = false;
    }
    let observed = Arc::new(AtomicBool::new(false));
    let observation = observed.clone();
    let session = Arc::downgrade(&fixture.session);
    *fixture.advisor.on_next_request.lock().unwrap() = Some(Box::new(move || {
        let session = session.upgrade().unwrap();
        assert!(
            !session.history().iter().any(|message| matches!(message,
                Message::ToolResult(result) if result.tool_use_id == "evidence"
            )),
            "the completed tool is admitted to the current query before the batch is persisted"
        );
        observation.store(true, Ordering::SeqCst);
    }));
    fixture.enable();
    fixture.run().await;
    assert!(observed.load(Ordering::SeqCst));
    assert!(fixture.tool_error().is_empty(), "{}", fixture.tool_error());
    {
        let requests = fixture.advisor.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let result = requests[0]
            .messages
            .iter()
            .find_map(|message| match message {
                Message::ToolResult(result) if result.tool_use_id == "evidence" => Some(result),
                _ => None,
            })
            .expect("the current completed tool must reach the advisor snapshot");
        assert!(result.content.iter().any(|block| matches!(block,
            OutputBlock::Text(text) if text.text.contains("current-tool-evidence")
        )));
        let extra = result.extra_user_blocks.as_ref().unwrap();
        assert!(matches!(extra.as_slice(), [ContentBlock::Text(note)]
            if note.text.contains("omitted from this request")
                && note.text.contains("current-evidence.wav")
                && note.text.contains("Tell the user it was not read")
                && !note.text.contains("UklGRg==")));
    }
    let retained = fixture
        .session
        .history()
        .into_iter()
        .find_map(|message| match message {
            Message::ToolResult(result) if result.tool_use_id == "evidence" => Some(result),
            _ => None,
        })
        .unwrap();
    assert!(
        matches!(retained.extra_user_blocks.as_deref(), Some([ContentBlock::Media(media)])
        if media.mime_type == "audio/wav" && media.base64 == "UklGRg=="
            && media.filename.as_ref().is_some_and(|name| name == "current-evidence.wav"))
    );
    {
        let requests = fixture.main.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(
            requests[1].messages.iter().any(|message| matches!(message,
                Message::ToolResult(result) if result.uuid == retained.uuid
                    && result.extra_user_blocks == retained.extra_user_blocks
            )),
            "advisor projection must not mutate the calling query's native media snapshot"
        );
    }
    fixture.close().await;
}

#[tokio::test]
async fn advisor_uses_full_live_context_portably_and_keeps_prefix_and_billing_independent() {
    let mut first = vec![
        ModelStreamEvent::ThinkingBlock {
            text: "readable-current-reasoning".into(),
            signature: Some("foreign-signature".into()),
            encrypted_content: Some("foreign-opaque-state".into()),
            is_summary: false,
        },
        ModelStreamEvent::RedactedThinkingBlock {
            data: "foreign-redacted".into(),
        },
        ModelStreamEvent::ServerSearchQuery {
            id: "native-search".into(),
            query: "source evidence".into(),
            action: None,
        },
        ModelStreamEvent::WebSearchResult {
            id: "native-search".into(),
            results: vec![NativeSearchResult {
                title: "Source evidence".into(),
                url: "https://example.test/evidence".into(),
                encrypted_token: Some("foreign-search-token".into()),
                page_age: None,
            }],
            error: None,
        },
        ModelStreamEvent::TextDelta {
            text: "Source citation".into(),
        },
        ModelStreamEvent::Citation {
            citation: Citation {
                url: "https://example.test/evidence".into(),
                title: Some("Source evidence".into()),
                cited_text: Some("source excerpt".into()),
                start_index: None,
                end_index: None,
                encrypted_index: Some("foreign-citation-index".into()),
            },
        },
    ];
    first.extend(tool_events("evidence", "evidence_probe", "{}"));
    first.extend(consultation("consult-1"));
    let fixture = Fixture::new(
        vec![
            first,
            consultation("consult-2"),
            completion("main finished"),
        ],
        vec![completion("first advice"), completion("second advice")],
        false,
    );
    fixture.enable();
    fixture.run().await;
    {
        let requests = fixture.advisor.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        for request in requests.iter() {
            assert_eq!(request.model, "independent-advisor");
            assert!(request.tools.is_empty());
            assert!(request.tool_choice.is_none() && request.web_search.is_none());
            assert!(request.context_management.is_none() && request.thinking_context.is_none());
            assert!(request.effort.is_none() && request.thinking.is_none());
            let encoded =
                runtime_json::stringify(&runtime_codec::encode_value(&request.messages).unwrap())
                    .try_to_utf8()
                    .unwrap();
            assert!(encoded.contains(&"FULL-CONTEXT-BEGIN ".repeat(1000)));
            assert!(encoded.contains("current-tool-evidence"));
            assert!(encoded.contains("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScLbtAAAAABJRU5ErkJggg=="));
            assert!(encoded.contains("readable-current-reasoning"));
            assert!(encoded.contains("https://example.test/evidence"));
            assert!(encoded.contains("source excerpt"));
            assert_eq!(request.skip_cache_write, Some(true));
            assert!(
                !encoded.contains("foreign-signature")
                    && !encoded.contains("foreign-opaque-state")
                    && !encoded.contains("foreign-redacted")
                    && !encoded.contains("foreign-tool-signature")
                    && !encoded.contains("foreign-search-token")
                    && !encoded.contains("foreign-citation-index")
            );
            let result_ids: Vec<_> = request
                .messages
                .iter()
                .filter_map(|message| match message {
                    Message::ToolResult(result) => Some(&result.tool_use_id),
                    _ => None,
                })
                .collect();
            for message in &request.messages {
                if let Message::Assistant(assistant) = message {
                    assert!(assistant.usage.is_none());
                    for block in &assistant.content {
                        if let ContentBlock::ToolUse(call) = block {
                            assert!(result_ids.contains(&&call.id));
                        }
                    }
                }
            }
        }
        let first_history = runtime_json::stringify(
            &runtime_codec::encode_value(
                &requests[0].messages[..requests[0].messages.len() - 1].to_vec(),
            )
            .unwrap(),
        )
        .try_to_utf8()
        .unwrap();
        assert!(
            !first_history.contains("What should I verify next?"),
            "the current question is not duplicated inside the reusable history prefix"
        );
        assert_eq!(requests[0].cache_key, requests[1].cache_key);
        assert_eq!(requests[0].system, requests[1].system);
        assert!(requests[0]
            .cache_key
            .as_ref()
            .unwrap()
            .try_to_utf8()
            .unwrap()
            .contains(":advisor:"));
        let main = fixture.main.requests.lock().unwrap();
        assert_ne!(requests[0].cache_key, main[0].cache_key);
        assert_eq!(main[0].tools, main[1].tools);
        assert_eq!(main[0].system, main[1].system);
    }
    let snapshot = fixture.session.context_snapshot();
    assert!(snapshot.cost.total_cost_usd > 0.0);
    assert_eq!(snapshot.token_totals.input_tokens, 500);
    assert_eq!(snapshot.token_totals.output_tokens, 50);
    let persisted = runtime_json::stringify(
        &runtime_codec::encode_value(&fixture.session.visible_history()).unwrap(),
    )
    .try_to_utf8()
    .unwrap();
    assert!(persisted.contains("foreign-signature"));
    assert!(
        persisted.contains("foreign-search-token") && persisted.contains("foreign-citation-index")
    );
    assert!(!persisted.contains("Advisor consultation question (answer only this question)"));
    fixture.close().await;
}

#[tokio::test]
async fn advisor_is_not_advertised_to_children_without_parent_snapshot_access() {
    let mut spawn = tool_events(
        "child-probe",
        "task",
        r#"{"model":"test-model","description":"Inspect inherited tools","prompt":"Child tools check","run_in_background":false}"#,
    );
    spawn.push(ModelStreamEvent::MessageStop {
        stop_reason: ModelStopReason::ToolUse,
        usage: usage(),
    });
    let fixture = Fixture::new(
        vec![
            spawn,
            completion("child finished"),
            completion("parent finished"),
        ],
        vec![],
        false,
    );
    fixture.enable();
    fixture.run().await;
    {
        let requests = fixture.main.requests.lock().unwrap();
        assert_eq!(requests.len(), 3, "the actual child query was exercised");
        assert!(requests[0].tools.iter().any(|tool| tool.name == "advisor"));
        assert!(!requests[1].tools.iter().any(|tool| tool.name == "advisor"));
        assert!(!requests[1].system.iter().any(|part| part.name == "advisor"));
        assert!(requests[2].tools.iter().any(|tool| tool.name == "advisor"));
    }
    assert!(fixture.advisor.requests.lock().unwrap().is_empty());
    fixture.close().await;
}

#[tokio::test]
async fn advisor_disabled_requests_omit_tool_and_guidance_until_the_next_enabled_turn() {
    let mut evidence = tool_events("evidence", "evidence_probe", "{}");
    evidence.push(ModelStreamEvent::MessageStop {
        stop_reason: ModelStopReason::ToolUse,
        usage: usage(),
    });
    let fixture = Fixture::new(
        vec![
            evidence,
            completion("first turn"),
            completion("enabled turn"),
            completion("disabled again"),
        ],
        vec![],
        false,
    );
    fixture.run().await;
    fixture.enable();
    fixture.run().await;
    fixture.session.install_advisor_target(None).unwrap();
    fixture.run().await;
    {
        let requests = fixture.main.requests.lock().unwrap();
        assert_eq!(requests.len(), 4);
        for (request, enabled) in requests.iter().zip([false, false, true, false]) {
            assert_eq!(
                request.tools.iter().any(|tool| tool.name == "advisor"),
                enabled
            );
            assert_eq!(
                request.system.iter().any(|part| part.name == "advisor"),
                enabled
            );
            assert_eq!(
                request.system.iter().any(|part| part
                    .text
                    .try_to_utf8()
                    .unwrap()
                    .contains("Use advisor(question)")),
                enabled
            );
        }
        assert_eq!(requests[0].tools, requests[1].tools);
        assert_eq!(requests[0].system, requests[1].system);
    }
    assert!(fixture.advisor.requests.lock().unwrap().is_empty());
    fixture.close().await;
}

#[tokio::test]
async fn advisor_disable_midturn_keeps_the_frozen_target_schema_and_prompt_until_next_turn() {
    let fixture = Fixture::new(
        vec![
            consultation("first"),
            consultation("second"),
            completion("finished"),
            completion("next turn"),
        ],
        vec![completion("first advice"), completion("second advice")],
        false,
    );
    fixture.enable();
    let session = fixture.session.clone();
    *fixture.main.on_next_request.lock().unwrap() = Some(Box::new(move || {
        session.install_advisor_target(None).unwrap();
    }));
    fixture.run().await;
    fixture.run().await;
    {
        let requests = fixture.main.requests.lock().unwrap();
        assert_eq!(requests.len(), 4);
        assert!(requests[0].tools.iter().any(|tool| tool.name == "advisor"));
        assert!(requests[0].system.iter().any(|part| part.name == "advisor"));
        for request in &requests[1..3] {
            assert_eq!(requests[0].tools, request.tools);
            assert_eq!(requests[0].system, request.system);
        }
        assert!(!requests[3].tools.iter().any(|tool| tool.name == "advisor"));
        assert!(!requests[3].system.iter().any(|part| part.name == "advisor"));
    }
    assert_eq!(fixture.advisor.requests.lock().unwrap().len(), 2);
    fixture.close().await;
}

#[tokio::test]
async fn advisor_unavailable_is_a_tool_error_not_a_main_turn_failure() {
    let fixture = Fixture::new(
        vec![consultation("consult"), completion("main finished")],
        vec![],
        false,
    );
    fixture
        .session
        .install_advisor_target(Some(InferenceTargetState::Unavailable {
            source: "other".into(),
            model_id: "missing".into(),
            code: "AUTH_REQUIRED".into(),
            message: "sign in".into(),
        }))
        .unwrap();
    fixture.run().await;
    assert!(fixture.tool_error().contains("AUTH_REQUIRED"));
    assert!(fixture.advisor.requests.lock().unwrap().is_empty());
    fixture.close().await;
}

#[tokio::test]
async fn advisor_small_context_reduces_output_budget_without_truncating_input() {
    for window in [12_000, 14_000] {
        let fixture = Fixture::new(
            vec![consultation("consult"), completion("main finished")],
            vec![completion("advice")],
            false,
        );
        fixture.advisor.caps.lock().unwrap().context_window = window;
        fixture.enable();
        fixture.run().await;
        {
            let requests = fixture.advisor.requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            let request = &requests[0];
            assert!(request.max_output_tokens < 8192);
            assert!(request.max_output_tokens >= 256);
            let encoded =
                runtime_json::stringify(&runtime_codec::encode_value(&request.messages).unwrap())
                    .try_to_utf8()
                    .unwrap();
            assert!(encoded.contains(&"FULL-CONTEXT-BEGIN ".repeat(1000)));
            assert!(encoded.contains("What should I verify next?"));
        }
        assert!(fixture.tool_error().is_empty());
        fixture.close().await;
    }
}

#[tokio::test]
async fn advisor_rejects_unsupported_media_and_unusable_capacity_without_fallback() {
    for media in [true, false] {
        let mut first = tool_events("evidence", "evidence_probe", "{}");
        first.extend(consultation("consult"));
        let fixture = Fixture::new(vec![first, completion("main finished")], vec![], false);
        {
            let mut caps = fixture.advisor.caps.lock().unwrap();
            if media {
                caps.vision = false;
            } else {
                caps.context_window = 101;
            }
        }
        fixture.enable();
        fixture.run().await;
        assert!(fixture.tool_error().contains(if media {
            "cannot read images"
        } else {
            "cannot fit"
        }));
        assert!(fixture.advisor.requests.lock().unwrap().is_empty());
        fixture.close().await;
    }
}

#[tokio::test]
async fn advisor_provider_failure_is_a_recoverable_tool_error() {
    let fixture = Fixture::new(
        vec![consultation("consult"), completion("main finished")],
        vec![vec![ModelStreamEvent::Error {
            error: ProviderError {
                kind: ProviderErrorKind::Network,
                message: "advisor offline".into(),
                status: None,
                code: None,
                error_type: None,
                media_origins: None,
                media_repair_complete: false,
                retry_after_ms: None,
                request_id: None,
            },
        }]],
        false,
    );
    fixture.enable();
    fixture.run().await;
    assert!(fixture.tool_error().contains("advisor offline"));
    assert_eq!(fixture.advisor.requests.lock().unwrap().len(), 1);
    fixture.close().await;
}

#[tokio::test]
async fn advisor_never_executes_returned_tools_or_recurses_and_still_records_usage() {
    let fixture = Fixture::new(
        vec![consultation("consult"), completion("main finished")],
        vec![consultation("recursive-advisor")],
        false,
    );
    fixture.enable();
    fixture.run().await;
    assert!(fixture.tool_error().contains("no tool was executed"));
    assert_eq!(fixture.advisor.requests.lock().unwrap().len(), 1);
    assert_eq!(
        fixture.session.context_snapshot().token_totals.input_tokens,
        300
    );
    fixture.close().await;
}

#[tokio::test]
async fn advisor_parent_cancellation_signals_the_independent_request() {
    let fixture = Fixture::new(vec![consultation("consult")], vec![], true);
    fixture.enable();
    let signal = AbortSignal::new();
    let turn = fixture.session.run_streamed(
        Input::Text("consult then wait".into()),
        TurnOptions {
            signal: Some(signal.clone()),
            ..Default::default()
        },
    );
    let task = tokio::spawn(async move { turn.events.collect::<Vec<_>>().await });
    tokio::time::timeout(WAIT, fixture.advisor.started.notified())
        .await
        .unwrap();
    signal.cancel();
    tokio::time::timeout(WAIT, task).await.unwrap().unwrap();
    assert!(fixture.advisor.signals.lock().unwrap()[0].is_cancelled());
    fixture.close().await;
}

fn encoded_messages(request: &ModelRequest) -> String {
    runtime_json::stringify(&runtime_codec::encode_value(&request.messages).unwrap())
        .try_to_utf8()
        .unwrap()
}

#[tokio::test]
async fn advisor_overflow_uses_the_current_main_model_and_preserves_parent_context() {
    let mut first = tool_events("evidence", "evidence_probe", "{}");
    first.extend(consultation("consult"));
    let mut summary_events = vec![
        ModelStreamEvent::UsageSnapshot { usage: usage() },
        ModelStreamEvent::UsageSnapshot { usage: usage() },
    ];
    summary_events.extend(completion(
        "Summary: retain constraints and current-tool-evidence.",
    ));
    let fixture = Fixture::new(
        vec![first, summary_events, completion("main finished")],
        vec![completion("Check the evidence.")],
        false,
    );
    fixture.advisor.caps.lock().unwrap().context_window = 4096;
    *fixture.main.model_output_limit.lock().unwrap() = Some(("current-turn-model".into(), 512));
    fixture
        .advisor
        .estimate_request
        .store(true, Ordering::SeqCst);
    fixture.enable();
    let events: Vec<_> = fixture
        .session
        .run_streamed(
            Input::Text("FULL-CONTEXT-BEGIN ".repeat(1000).into()),
            TurnOptions {
                model: Some("current-turn-model".into()),
                system_prompt_append: Some("Preserve the project acceptance criteria.".into()),
                ..Default::default()
            },
        )
        .events
        .collect()
        .await;
    assert!(!events
        .iter()
        .any(|event| matches!(event, EngineEvent::TurnFailed { .. })));
    assert!(fixture.tool_error().is_empty(), "{}", fixture.tool_error());
    {
        let main = fixture.main.requests.lock().unwrap();
        assert_eq!(
            main.len(),
            3,
            "one main request, one summary, one continuation"
        );
        let summary = &main[1];
        assert_eq!(summary.model, "current-turn-model");
        assert!(summary.tools.is_empty());
        assert!(summary.tool_choice.is_none() && summary.web_search.is_none());
        assert!(summary.context_management.is_none() && summary.context_hints.is_none());
        assert!(summary.output_schema.is_none() && summary.task_budget.is_none());
        assert!(summary.system.iter().any(|part| part
            .text
            .contains("Preserve the project acceptance criteria.")));
        assert!(summary
            .system
            .iter()
            .any(|part| part.name == "advisor-summary-role"));
        let encoded = encoded_messages(summary);
        assert!(encoded.contains(&"FULL-CONTEXT-BEGIN ".repeat(1000)));
        assert!(encoded.contains("current-tool-evidence"));
        assert!(encoded.contains("What should I verify next?"));
        assert!(summary.max_output_tokens >= 256 && summary.max_output_tokens < 4096);
        assert!(
            summary.max_output_tokens <= 512,
            "the actual main model's output limit wins over the session default"
        );
        assert_ne!(summary.cache_key, main[0].cache_key);
        assert_eq!(main[0].system, main[2].system);
        assert_eq!(main[0].tools, main[2].tools);
        let continued = encoded_messages(&main[2]);
        assert!(continued.contains(&"FULL-CONTEXT-BEGIN ".repeat(1000)));
        assert!(continued.contains("current-tool-evidence"));
        assert!(continued.contains("The current main model summarized the context"));
        assert!(!main[2]
            .system
            .iter()
            .any(|part| part.name == "advisor-summary-role"));
        let advisor = fixture.advisor.requests.lock().unwrap();
        assert_eq!(advisor.len(), 1);
        assert_eq!(advisor[0].model, "independent-advisor");
        assert_eq!(advisor[0].system.len(), 1);
        let compact = encoded_messages(&advisor[0]);
        assert!(compact.contains("Summary: retain constraints and current-tool-evidence."));
        assert!(compact.contains("What should I verify next?"));
        assert!(!compact.contains("FULL-CONTEXT-BEGIN"));
        assert_ne!(advisor[0].cache_key, summary.cache_key);
    }
    let history = fixture.session.visible_history();
    assert!(history
        .iter()
        .any(|message| matches!(message, Message::User(user)
        if user.content.iter().any(|block| matches!(block, ContentBlock::Text(text)
            if text.text.contains(&"FULL-CONTEXT-BEGIN ".repeat(1000)))))));
    assert!(!history
        .iter()
        .any(|message| matches!(message, Message::CompactBoundary(_))));
    assert!(!history
        .iter()
        .any(|message| matches!(message, Message::Assistant(assistant)
        if assistant.content.iter().any(|block| matches!(block, ContentBlock::Text(text)
            if text.text.contains("Summary: retain constraints"))))));
    assert_eq!(
        fixture.session.context_snapshot().token_totals.input_tokens,
        400
    );
    fixture.close().await;
}

#[tokio::test]
async fn advisor_structured_images_do_not_trigger_base64_context_overflow() {
    use base64::Engine as _;
    let pixels = image::RgbImage::from_fn(1000, 1000, |x, y| {
        let seed = x
            .wrapping_add(y.wrapping_mul(1000))
            .wrapping_mul(747_796_405)
            .wrapping_add(2_891_336_453);
        let value = ((seed >> ((seed >> 28) + 4)) ^ seed).wrapping_mul(277_803_737);
        image::Rgb([(value >> 16) as u8, (value >> 8) as u8, value as u8])
    });
    let mut bytes = std::io::Cursor::new(Vec::new());
    image::DynamicImage::ImageRgb8(pixels)
        .write_to(&mut bytes, image::ImageFormat::Png)
        .unwrap();
    let image = base64::engine::general_purpose::STANDARD.encode(bytes.into_inner());
    let fixture = Fixture::new(
        vec![consultation("consult"), completion("main finished")],
        vec![completion("Image evidence received.")],
        false,
    );
    fixture
        .advisor
        .estimate_request
        .store(true, Ordering::SeqCst);
    fixture.advisor.caps.lock().unwrap().context_window = 32_000;
    fixture.enable();
    let events: Vec<_> = fixture
        .session
        .run_streamed(
            Input::Parts(vec![UserContentPart::Image {
                base64: image.into(),
                mime_type: "image/png".into(),
                source_path: None,
            }]),
            TurnOptions::default(),
        )
        .events
        .collect()
        .await;
    assert!(!events
        .iter()
        .any(|event| matches!(event, EngineEvent::TurnFailed { .. })));
    assert!(fixture.tool_error().is_empty(), "{}", fixture.tool_error());
    {
        let advisor = fixture.advisor.requests.lock().unwrap();
        assert_eq!(advisor.len(), 1);
        assert!(
            klaus_agent_core::provider::anthropic::estimate_tokens_fallback(&advisor[0]) > 32_000,
            "the serialized binary payload must reproduce the original false overflow"
        );
        assert!(advisor[0]
            .messages
            .iter()
            .any(|message| matches!(message, Message::User(user)
            if user.content.iter().any(|block| matches!(block, ContentBlock::Image(_))))));
    }
    assert_eq!(
        fixture.main.requests.lock().unwrap().len(),
        2,
        "no summary is needed"
    );
    fixture.close().await;
}

#[tokio::test]
async fn advisor_summary_failures_never_send_partial_context_or_execute_tools() {
    let mut failures = vec![
        vec![ModelStreamEvent::MessageStop {
            stop_reason: ModelStopReason::MaxTokens,
            usage: usage(),
        }],
        completion(""),
        vec![ModelStreamEvent::TextDelta {
            text: "incomplete summary".into(),
        }],
        vec![ModelStreamEvent::MessageStop {
            stop_reason: ModelStopReason::Refusal,
            usage: usage(),
        }],
        vec![ModelStreamEvent::Error {
            error: ProviderError {
                kind: ProviderErrorKind::Network,
                message: "summary offline".into(),
                status: None,
                code: None,
                error_type: None,
                media_origins: None,
                media_repair_complete: false,
                retry_after_ms: None,
                request_id: None,
            },
        }],
        completion(&"oversize-summary ".repeat(3000)),
    ];
    failures.push(consultation("recursive-summary"));
    for summary in failures {
        let fixture = Fixture::new(
            vec![
                consultation("consult"),
                summary,
                completion("main finished"),
            ],
            vec![],
            false,
        );
        fixture.advisor.caps.lock().unwrap().context_window = 4096;
        fixture.enable();
        fixture.run().await;
        assert!(!fixture.tool_error().is_empty());
        assert!(fixture.advisor.requests.lock().unwrap().is_empty());
        assert_eq!(
            fixture.main.requests.lock().unwrap().len(),
            3,
            "summary failures are bounded"
        );
        assert!(!fixture
            .session
            .history()
            .iter()
            .any(|message| matches!(message, Message::CompactBoundary(_))));
        fixture.close().await;
    }
}

#[tokio::test]
async fn advisor_summary_cancellation_cancels_main_request_and_skips_advisor() {
    let fixture = Fixture::new(vec![consultation("consult")], vec![], false);
    fixture.advisor.caps.lock().unwrap().context_window = 4096;
    *fixture.main.hang_at.lock().unwrap() = Some(2);
    fixture.enable();
    let signal = AbortSignal::new();
    let turn = fixture.session.run_streamed(
        Input::Text("FULL-CONTEXT-BEGIN ".repeat(1000).into()),
        TurnOptions {
            signal: Some(signal.clone()),
            ..Default::default()
        },
    );
    let task = tokio::spawn(async move { turn.events.collect::<Vec<_>>().await });
    tokio::time::timeout(WAIT, async {
        loop {
            fixture.main.started.notified().await;
            if fixture.main.requests.lock().unwrap().len() == 2 {
                break;
            }
        }
    })
    .await
    .unwrap();
    signal.cancel();
    tokio::time::timeout(WAIT, task).await.unwrap().unwrap();
    assert!(fixture.main.signals.lock().unwrap()[1].is_cancelled());
    assert!(fixture.advisor.requests.lock().unwrap().is_empty());
    fixture.close().await;
}

#[tokio::test]
async fn advisor_summary_usage_is_charged_when_the_stream_fails_before_completion() {
    let fixture = Fixture::new(
        vec![
            consultation("consult"),
            vec![
                ModelStreamEvent::UsageSnapshot { usage: usage() },
                ModelStreamEvent::UsageSnapshot { usage: usage() },
                ModelStreamEvent::Error {
                    error: ProviderError {
                        kind: ProviderErrorKind::Network,
                        message: "summary disconnected".into(),
                        status: None,
                        code: None,
                        error_type: None,
                        media_origins: None,
                        media_repair_complete: false,
                        retry_after_ms: None,
                        request_id: None,
                    },
                },
            ],
            completion("main finished"),
        ],
        vec![],
        false,
    );
    fixture.advisor.caps.lock().unwrap().context_window = 4096;
    fixture.enable();
    fixture.run().await;
    assert!(fixture.tool_error().contains("summary disconnected"));
    assert!(fixture.advisor.requests.lock().unwrap().is_empty());
    assert_eq!(
        fixture.session.context_snapshot().token_totals.input_tokens,
        300
    );
    fixture.close().await;
}

#[tokio::test]
async fn advisor_summary_timeout_is_a_recoverable_tool_error() {
    let fixture = Fixture::configured(
        vec![consultation("consult"), completion("main finished")],
        vec![],
        false,
        false,
        json!({ "streamIdleTimeoutMs": 20, "streamTotalTimeoutMs": 1000 }),
    );
    fixture.advisor.caps.lock().unwrap().context_window = 4096;
    *fixture.main.hang_at.lock().unwrap() = Some(2);
    fixture.enable();
    fixture.run().await;
    assert!(fixture
        .tool_error()
        .contains("advisor context summarization response stream timed out"));
    assert!(fixture.main.signals.lock().unwrap()[1].is_cancelled());
    assert!(fixture.advisor.requests.lock().unwrap().is_empty());
    fixture.close().await;
}

#[tokio::test]
async fn advisor_rechecks_the_spend_limit_after_paying_for_the_summary() {
    let fixture = Fixture::configured(
        vec![consultation("consult"), completion("Context summary.")],
        vec![],
        false,
        false,
        json!({ "maxBudgetUsd": 0.0002 }),
    );
    fixture.advisor.caps.lock().unwrap().context_window = 4096;
    fixture.enable();
    let _: Vec<_> = fixture
        .session
        .run_streamed(
            Input::Text("FULL-CONTEXT-BEGIN ".repeat(1000).into()),
            TurnOptions::default(),
        )
        .events
        .collect()
        .await;
    assert!(
        fixture.tool_error().contains("spend limit"),
        "{}",
        fixture.tool_error()
    );
    assert_eq!(fixture.main.requests.lock().unwrap().len(), 2);
    assert!(fixture.advisor.requests.lock().unwrap().is_empty());
    assert_eq!(
        fixture.session.context_snapshot().token_totals.input_tokens,
        200
    );
    fixture.close().await;
}

#[tokio::test]
async fn advisor_usage_keeps_configured_pricing_when_the_service_reports_a_model_alias() {
    let mut advice = vec![ModelStreamEvent::MessageStart {
        model: "service-canonical-model".into(),
    }];
    advice.extend(completion("Advice."));
    let fixture = Fixture::new(
        vec![consultation("consult"), completion("main finished")],
        vec![advice],
        false,
    );
    fixture.enable();
    fixture.run().await;
    let snapshot = fixture.session.context_snapshot();
    let advisor = snapshot
        .cost
        .models
        .iter()
        .find(|model| {
            model.totals.model.contains("independent-advisor")
                || model.totals.model.contains("advisor-pick")
        })
        .expect("the advisor is billed under its captured selection");
    assert_eq!(advisor.totals.requests, 1);
    assert!(
        advisor.totals.cost_usd > 0.0,
        "the configured model's price must remain available"
    );
    fixture.close().await;
}
