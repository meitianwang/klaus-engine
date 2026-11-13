//! Defect: a catalog refresh after an A-owned request started made its Read
//! report "cannot understand images" and changed planning limits inside work
//! that already held an immutable inference bundle.
//! Root cause: the bundle held a provider Arc, but the catalog decorator resolved
//! capabilities from shared mutable state on every call.
//! Work item: 2026-10-02 captured catalog capabilities; PUBLIC Session regression.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::deps::{CompactionResult, Compactor};
use klaus_agent_core::engine::session::{
    resolve_config, InferencePublication, InferenceTarget, InferenceTargetState, InferenceTargets,
    Input, Session, TurnOptions, UserContentPart,
};
use klaus_agent_core::observability::cost_tracker::DEFAULT_PRICING;
use klaus_agent_core::provider::model_catalog::{with_model_catalog, ModelCatalog};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeString};
use klaus_agent_core::schemas::config::ResolvedConfig;
use klaus_agent_core::schemas::content::{ContentBlock, OutputBlock};
use klaus_agent_core::schemas::message::{CompactBoundaryMessage, Message, Usage};
use klaus_agent_core::tools::builtins::read::create_read_tool;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{
    EngineErrorCode, ProviderError, ProviderErrorKind, ToolError,
};
use klaus_agent_core::types::events::{EngineEvent, ThreadItem};
use klaus_agent_core::types::json::Json;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::{
    SpawnSubAgentOptions, Tool, ToolContext, ToolProgress, ToolResult,
};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Notify;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

const WAIT: Duration = Duration::from_secs(10);
const MODEL: &str = "same-catalog-model";
type Requests = Arc<Mutex<Vec<(String, ModelRequest)>>>;
type Limits = Arc<Mutex<Vec<(u64, Option<u64>)>>>;

#[derive(Default)]
struct Gate {
    entered: Notify,
    release: Notify,
}

impl Gate {
    async fn wait(&self) {
        tokio::time::timeout(WAIT, self.entered.notified())
            .await
            .unwrap();
    }
}

#[derive(Clone, Copy)]
enum Reply {
    Tool,
    Text,
    Network,
    TooLarge,
}

struct Script {
    reply: Reply,
    gate: Option<Arc<Gate>>,
}

struct Provider {
    info: ProviderInfo,
    requests: Requests,
    scripts: Mutex<VecDeque<Script>>,
}

impl Provider {
    fn new(name: &str, requests: &Requests) -> Arc<Self> {
        Arc::new(Self {
            info: ProviderInfo {
                name: name.into(),
                default_model: MODEL.into(),
                base_url: None,
                env_key: None,
            },
            requests: requests.clone(),
            scripts: Mutex::new(VecDeque::new()),
        })
    }

    fn enqueue(&self, reply: Reply, gate: Option<Arc<Gate>>) {
        self.scripts
            .lock()
            .unwrap()
            .push_back(Script { reply, gate });
    }
}

impl ModelProvider for Provider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: true,
            thinking: true,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: 128_000,
            max_output_tokens: 8_192,
            native_pdf: true,
            vision: true,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        self.requests
            .lock()
            .unwrap()
            .push((self.info.name.try_to_utf8().unwrap(), request.clone()));
        let script = self.scripts.lock().unwrap().pop_front().unwrap_or(Script {
            reply: Reply::Text,
            gate: None,
        });
        Box::pin(async_stream::stream! {
            if let Some(gate) = script.gate {
                gate.entered.notify_one();
                tokio::select! {
                    _ = gate.release.notified() => {},
                    _ = signal.cancelled() => return,
                }
            }
            if matches!(script.reply, Reply::Network | Reply::TooLarge) {
                yield ModelStreamEvent::Error { error: ProviderError {
                    kind: if matches!(script.reply, Reply::TooLarge) { ProviderErrorKind::PromptTooLong } else { ProviderErrorKind::Network },
                    message: "controlled retry".into(),
                    status: matches!(script.reply, Reply::TooLarge).then_some(413), retry_after_ms: Some(1), request_id: None,
                    code: None, error_type: None, media_origins: None, media_repair_complete: false, credential_recovery: None,
                }};
                return;
            }
            yield ModelStreamEvent::MessageStart { model: request.model };
            if matches!(script.reply, Reply::Tool) {
                yield ModelStreamEvent::ToolUseStart {
                    id: "catalog-read".into(), name: "catalog_read_probe".into(),
                    thought_signature: None, tool_kind: None, input_format: None,
                };
                yield ModelStreamEvent::ToolUseInputDelta { id: "catalog-read".into(), partial_json: "{}".into() };
                yield ModelStreamEvent::ToolUseStop { id: "catalog-read".into() };
            } else {
                yield ModelStreamEvent::TextDelta { text: "Captured work finished.".into() };
            }
            yield ModelStreamEvent::MessageStop {
                stop_reason: if matches!(script.reply, Reply::Tool) { ModelStopReason::ToolUse } else { ModelStopReason::EndTurn },
                usage: Usage { input_tokens: 10, output_tokens: 1, cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0, total_tokens: None },
            };
        })
    }
}

#[derive(Default)]
struct RecordingCompactor {
    limits: Limits,
}

#[async_trait]
impl Compactor for RecordingCompactor {
    fn fork_isolated(
        &self,
        _: klaus_agent_core::types::provider::CallModel,
        _: &ResolvedConfig,
    ) -> Result<Arc<dyn Compactor>, String> {
        Ok(Arc::new(Self {
            limits: self.limits.clone(),
        }))
    }

    fn rebind_model(
        &self,
        _: Arc<dyn ModelProvider>,
        config: &ResolvedConfig,
    ) -> Result<Arc<dyn Compactor>, String> {
        self.limits
            .lock()
            .unwrap()
            .push((config.context_window, config.max_output_tokens));
        Ok(Arc::new(Self {
            limits: self.limits.clone(),
        }))
    }

    async fn maybe_compact(
        &self,
        messages: &[Message],
        _: u64,
        config: &ResolvedConfig,
        _: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.limits
            .lock()
            .unwrap()
            .push((config.context_window, config.max_output_tokens));
        CompactionResult {
            messages: messages.to_vec(),
            freed_tokens: 0,
            compacted: false,
            reason: None,
            diagnostics: Vec::new(),
        }
    }

    async fn reactive_compact(
        &self,
        messages: &[Message],
        config: &ResolvedConfig,
        _: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.limits
            .lock()
            .unwrap()
            .push((config.context_window, config.max_output_tokens));
        let mut retained = vec![Message::CompactBoundary(CompactBoundaryMessage {
            native_compaction: None,
            uuid: "catalog-summary".into(),
            summary: "Continue the captured step".into(),
        })];
        retained.extend(messages.iter().rev().take(1).cloned());
        CompactionResult {
            messages: retained,
            freed_tokens: 1_000,
            compacted: true,
            reason: None,
            diagnostics: Vec::new(),
        }
    }
}

type ReadObservations = Arc<Mutex<Vec<(Option<bool>, Option<bool>, bool)>>>;

struct Probe {
    path: String,
    observations: ReadObservations,
    child: Option<(RuntimeString, Arc<Probe>)>,
    child_events: Arc<Mutex<Vec<EngineEvent>>>,
}

#[async_trait]
impl Tool for Probe {
    fn static_name(&self) -> &str {
        "catalog_read_probe"
    }
    fn static_description(&self) -> &str {
        "Read the fixture with this work's actual capability context"
    }
    fn input_schema(&self) -> Json {
        runtime_json::parse(&RuntimeString::from(r#"{"type":"object"}"#)).unwrap()
    }
    fn is_enabled(&self) -> bool {
        true
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
        context: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let input =
            runtime_json::parse(&RuntimeString::from(json!({"path": self.path}).to_string()))
                .unwrap();
        let result = create_read_tool().call(input, context, None).await?;
        let image = result
            .serializable
            .model_content
            .iter()
            .any(|block| matches!(block, OutputBlock::Image(_)));
        self.observations
            .lock()
            .unwrap()
            .push((context.vision, context.native_pdf, image));
        if let Some((selection, child)) = &self.child {
            let events: Vec<_> = context.spawn_sub_agent.as_ref().unwrap()(SpawnSubAgentOptions {
                prompt: "Read the image using the captured target".into(),
                model: Some(selection.clone()),
                tools: Some(vec![child.clone()]),
                ..Default::default()
            })
            .await
            .map_err(ToolError::new)?
            .events
            .collect()
            .await;
            *self.child_events.lock().unwrap() = events;
        }
        Ok(result)
    }
}

fn selected(source: &str) -> ResolvedConfig {
    resolve_config(json!({
        "model": MODEL, "prewarm": false, "fork": false, "thinking": true, "effort": "low",
        "maxOutputTokens": 4_096, "autoMemory": false, "autoDream": false, "memoryRecall": false,
        "speculativeCompaction": false, "earlyMicrocompact": false, "idleMicrocompact": false,
        "compactionSelfCheck": false, "loopDetection": false, "compactBufferTokens": 200_000,
        "modelSource": { "source": source, "modelId": MODEL, "selection": format!("{source}:{MODEL}") }
    })).unwrap()
}

struct FixtureDirectory(std::path::PathBuf);

impl FixtureDirectory {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("captured-catalog-{}-{id}", std::process::id()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for FixtureDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Fixture {
    process: RuntimeProcessContext,
    session: Arc<Session>,
    catalog: Arc<ModelCatalog>,
    a: Arc<Provider>,
    b: Arc<Provider>,
    c: Arc<Provider>,
    wrapped_b: Arc<dyn ModelProvider>,
    wrapped_c: Arc<dyn ModelProvider>,
    requests: Requests,
    observations: ReadObservations,
    child_events: Arc<Mutex<Vec<EngineEvent>>>,
    limits: Limits,
    _dir: FixtureDirectory,
}

impl Fixture {
    fn new(child_selection: Option<&str>) -> Self {
        let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
        let catalog = ModelCatalog::empty();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let a = Provider::new("catalog-a", &requests);
        let b = Provider::new("catalog-b", &requests);
        let c = Provider::new("catalog-c", &requests);
        let wrapped_a = with_model_catalog(a.clone(), catalog.clone());
        let wrapped_b = with_model_catalog(b.clone(), catalog.clone());
        let wrapped_c = with_model_catalog(c.clone(), catalog.clone());
        let dir = FixtureDirectory::new();
        let path = dir.path().join("captured.png");
        image::RgbaImage::from_pixel(2, 2, image::Rgba([1, 2, 3, 255]))
            .save(&path)
            .unwrap();
        let path = path.to_string_lossy().into_owned();
        let observations = Arc::new(Mutex::new(Vec::new()));
        let child_events = Arc::new(Mutex::new(Vec::new()));
        let child = child_selection.map(|selection| {
            (
                selection.into(),
                Arc::new(Probe {
                    path: path.clone(),
                    observations: observations.clone(),
                    child: None,
                    child_events: child_events.clone(),
                }),
            )
        });
        let probe = Arc::new(Probe {
            path,
            observations: observations.clone(),
            child,
            child_events: child_events.clone(),
        });
        let compactor = Arc::new(RecordingCompactor::default());
        let limits = compactor.limits.clone();
        let mut options = agent_options(process.clone());
        options.provider = wrapped_a;
        options.tools = Some(vec![probe]);
        options.compactor = Some(compactor);
        options.include_default_agents = Some(false);
        let config = options.config.as_mut().unwrap();
        for (key, value) in serde_json::to_value(selected("custom"))
            .unwrap()
            .as_object()
            .unwrap()
        {
            if !matches!(
                key.as_str(),
                "permissionMode"
                    | "sandbox"
                    | "requireRealSandbox"
                    | "sandboxNetworkAllowed"
                    | "sandboxRetryOnDenial"
                    | "contextWindow"
            ) {
                config[key] = value.clone();
            }
        }
        let initial_history = ["earlier-catalog-a", "earlier-catalog-b"]
            .into_iter()
            .flat_map(|id| {
                let user = Message::User(klaus_agent_core::schemas::message::UserMessage {
                    uuid: id.into(),
                    question_answer: None,
                    content: vec![klaus_agent_core::schemas::content::ContentBlock::Text(
                        klaus_agent_core::schemas::content::TextBlock {
                            text: "Earlier complete fixture context.".into(),
                            citations: None,
                            harness_envelope: None,
                            message_id: None,
                            phase: None,
                        },
                    )],
                    is_meta: None,
                });
                let assistant =
                    Message::Assistant(klaus_agent_core::schemas::message::AssistantMessage {
                        uuid: format!("{id}-assistant").into(),
                        model: MODEL.into(),
                        content: vec![klaus_agent_core::schemas::content::ContentBlock::Text(
                            klaus_agent_core::schemas::content::TextBlock {
                                text: "Earlier completed response.".into(),
                                citations: None,
                                harness_envelope: None,
                                message_id: None,
                                phase: None,
                            },
                        )],
                        usage: None,
                    });
                [user, assistant]
            })
            .collect();
        let session = Agent::new(options).start_session(StartSessionOptions {
            // Keep model-window fixtures independent of ambient workspace instructions.
            cwd: Some(dir.path().to_string_lossy().into_owned().into()),
            user_config_dir: Some(dir.path().to_string_lossy().into_owned().into()),
            initial_history: Some(initial_history),
            ..Default::default()
        });
        Self {
            process,
            session,
            catalog,
            a,
            b,
            c,
            wrapped_b,
            wrapped_c,
            requests,
            observations,
            child_events,
            limits,
            _dir: dir,
        }
    }

    async fn refresh(&self) {
        let mut entries: Vec<_> = ["catalog-a", "catalog-b", "catalog-c"]
            .into_iter()
            .map(|provider| {
                json!({
                    "provider": provider, "model": MODEL, "match": "exact", "capabilities": {
                        "vision": false, "nativePdf": false, "tools": false, "thinking": false,
                        "contextWindow": 24_000, "maxOutputTokens": 512
                    }
                })
            })
            .collect();
        entries.push(json!({
            "provider": "catalog-a", "model": "uncaptured-model", "match": "exact",
            "capabilities": { "vision": true, "nativePdf": true, "tools": true }
        }));
        let body = json!({ "schemaVersion": 1, "entries": entries }).to_string();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/catalog", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                let byte = socket.read_u8().await.unwrap();
                request.push(byte);
            }
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
        });
        tokio::time::timeout(WAIT, self.catalog.refresh(&reqwest::Client::new(), &url))
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(WAIT, server).await.unwrap().unwrap();
        assert_eq!(self.catalog.entry_count(), 4);
    }

    fn start(&self) -> tokio::task::JoinHandle<Vec<EngineEvent>> {
        let turn = self.session.run_streamed(
            Input::Text("Read the captured fixture".into()),
            TurnOptions::default(),
        );
        tokio::spawn(async move { turn.events.collect().await })
    }

    async fn finish(&self, task: tokio::task::JoinHandle<Vec<EngineEvent>>) -> Vec<EngineEvent> {
        let events = tokio::time::timeout(WAIT, task).await.unwrap().unwrap();
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
            "{events:?}"
        );
        assert!(events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnCompleted { .. })));
        events
    }

    async fn close(self) {
        self.session.shutdown_and_wait().await.unwrap();
        self.process.shutdown().await.unwrap();
    }
}

fn image_count(request: &ModelRequest) -> usize {
    request
        .messages
        .iter()
        .map(|message| match message {
            Message::User(message) => message
                .content
                .iter()
                .filter(|block| matches!(block, ContentBlock::Image(_)))
                .count(),
            Message::ToolResult(message) => message
                .content
                .iter()
                .filter(|block| matches!(block, OutputBlock::Image(_)))
                .count(),
            _ => 0,
        })
        .sum()
}

fn has_omission(request: &ModelRequest) -> bool {
    request.messages.iter().any(|message| match message {
        Message::User(message) => message.content.iter().any(|block| matches!(block, ContentBlock::Text(text) if text.text.contains("cannot understand images"))),
        Message::ToolResult(message) => message.content.iter().any(|block| matches!(block, OutputBlock::Text(text) if text.text.contains("cannot understand images"))),
        _ => false,
    })
}

#[tokio::test]
async fn catalog_refresh_keeps_old_read_and_next_normal_step_sees_new_limits_without_publication() {
    for reply in [Reply::Tool, Reply::Network, Reply::TooLarge] {
        let fixture = Fixture::new(None);
        let gate = Arc::new(Gate::default());
        fixture.a.enqueue(reply, Some(gate.clone()));
        let retry = matches!(reply, Reply::Network);
        if retry {
            fixture.a.enqueue(Reply::Tool, None);
        } else if matches!(reply, Reply::TooLarge) {
            fixture.a.enqueue(Reply::Text, None);
        }
        let task = fixture.start();
        gate.wait().await;
        fixture.refresh().await;
        gate.release.notify_one();
        if matches!(reply, Reply::TooLarge) {
            let events = tokio::time::timeout(WAIT, task).await.unwrap().unwrap();
            assert!(
                events.iter().any(|event| matches!(event,
                    EngineEvent::TurnFailed { error, inference: Some(identity) }
                        if error.code == EngineErrorCode::PromptTooLong
                            && identity.revision == 0 && identity.model == MODEL
                )),
                "{events:?}"
            );
            assert!(
                !events.iter().any(|event| matches!(
                    event,
                    EngineEvent::Compaction { .. } | EngineEvent::TurnCompleted { .. }
                )),
                "overflow must stop the captured turn before recovery: {events:?}"
            );
            assert_eq!(fixture.requests.lock().unwrap().len(), 1);
            fixture.finish(fixture.start()).await;
            assert!(fixture.observations.lock().unwrap().is_empty());
        } else {
            fixture.finish(task).await;
            assert_eq!(
                *fixture.observations.lock().unwrap(),
                [(Some(true), Some(true), true)],
                "the real Read must retain A's captured vision and PDF facts"
            );
        }
        let requests = fixture.requests.lock().unwrap().clone();
        assert_eq!(requests.len(), if retry { 3 } else { 2 });
        let first = &requests[0].1;
        assert_eq!(first.max_output_tokens, 4_096);
        assert!(!first.tools.is_empty());
        if matches!(reply, Reply::Network) {
            assert_eq!(
                first, &requests[1].1,
                "network retry must retain all captured planning facts"
            );
        }
        if retry {
            assert_eq!(requests[1].1.max_output_tokens, 4_096);
            assert!(!requests[1].1.tools.is_empty());
        }
        let next = &requests.last().unwrap().1;
        assert!(next.tools.is_empty());
        assert_eq!(next.max_output_tokens, 512);
        assert_eq!(image_count(next), 0);
        if !matches!(reply, Reply::TooLarge) {
            assert!(
                has_omission(next),
                "new blind steps keep the existing omission notice"
            );
            assert!(fixture.session.history().iter().any(|message| matches!(message, Message::ToolResult(result) if result.content.iter().any(|block| matches!(block, OutputBlock::Image(_))))), "capability projection must not rewrite historical images");
        }
        let limits = fixture.limits.lock().unwrap().clone();
        assert!(limits.contains(&(128_000, Some(4_096))), "{limits:?}");
        assert!(
            limits.iter().any(|(window, _)| *window == 24_000),
            "new step must re-budget context from the refreshed catalog: {limits:?}"
        );
        assert_eq!(
            fixture.session.model_revision(),
            0,
            "catalog refresh is not an inference publication"
        );
        let count = fixture.requests.lock().unwrap().len();
        let error = fixture
            .session
            .ask_side_question_input(Input::Parts(vec![UserContentPart::Document {
                base64: "JVBERi0xLjQK".into(),
                mime_type: "application/pdf".into(),
                filename: None,
            }]))
            .await
            .unwrap_err();
        assert!(error.contains("cannot read PDF"), "{error}");
        assert_eq!(
            fixture.requests.lock().unwrap().len(),
            count,
            "a fresh worker must reject unsupported PDFs before sending"
        );
        fixture.close().await;
    }
}

#[tokio::test]
async fn held_a_spawns_its_captured_same_named_tier_after_b_publication_and_catalog_refresh() {
    let fixture = Fixture::new(Some("low"));
    let targets = InferenceTargets {
        low: Some(InferenceTargetState::Ready(InferenceTarget {
            source: "local".to_string(),
            model_id: MODEL.to_string(),
            model: MODEL.into(),
            provider: fixture.wrapped_c.clone(),
            pricing: DEFAULT_PRICING.clone(),
            review_requirements: None,
        })),
        ..Default::default()
    };
    fixture
        .session
        .reconfigure_inference_with_targets(
            with_model_catalog(fixture.a.clone(), fixture.catalog.clone()),
            selected("custom"),
            None,
            None,
            InferencePublication {
                expected_revision: 0,
                change_id: None,
                targets: Some(targets),
                advisor_target: None,
                orchestrator_target: None,
                orchestration_mode: false,
                feature_targets: Default::default(),
                source: Some(("local".to_string(), MODEL.to_string())),
                rates: None,
                publication_key: None,
                review_requirements: None,
            },
        )
        .unwrap();
    let gate = Arc::new(Gate::default());
    fixture.a.enqueue(Reply::Tool, Some(gate.clone()));
    fixture.c.enqueue(Reply::Tool, None);
    fixture.c.enqueue(Reply::Text, None);
    fixture.b.enqueue(Reply::Text, None);
    let task = fixture.start();
    gate.wait().await;
    fixture
        .session
        .reconfigure_inference(fixture.wrapped_b.clone(), selected("codex"), None, None, 1)
        .unwrap();
    fixture.refresh().await;
    gate.release.notify_one();
    fixture.finish(task).await;
    assert_eq!(
        *fixture.observations.lock().unwrap(),
        [
            (Some(true), Some(true), true),
            (Some(true), Some(true), true)
        ]
    );
    let child_events = std::mem::take(&mut *fixture.child_events.lock().unwrap());
    assert!(
        !child_events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{child_events:?}"
    );
    assert!(child_events.iter().any(|event| matches!(event, EngineEvent::StreamRequestStart { inference: Some(identity) } if identity.revision == 1 && identity.source.as_ref().is_some_and(|source| source == "local") && identity.tier.as_ref().is_some_and(|tier| tier == "low"))));
    let requests = fixture.requests.lock().unwrap().clone();
    assert_eq!(
        requests
            .iter()
            .map(|(provider, _)| provider.as_str())
            .collect::<Vec<_>>(),
        ["catalog-a", "catalog-c", "catalog-c", "catalog-b"]
    );
    assert!(requests.iter().all(|(_, request)| request.model == MODEL));
    assert_eq!(
        image_count(&requests[2].1),
        1,
        "an independent child keeps its startup vision across later tool rounds"
    );
    assert!(!requests[2].1.tools.is_empty());
    // 8192 is the child's provider capability as declared BEFORE the catalog
    // refresh; the refresh republishes 512. A child that consulted live state
    // would show 512 here, so the frozen startup value is what proves capture.
    assert_eq!(requests[2].1.max_output_tokens, 8_192);
    assert_eq!(image_count(&requests[3].1), 0);
    assert!(requests[3].1.tools.is_empty());
    // The next NORMAL step is a fresh capture and does see the refreshed
    // catalog: no publication was made, only the catalog changed.
    assert_eq!(requests[3].1.max_output_tokens, 512);
    fixture.close().await;
}

#[tokio::test]
async fn an_old_step_cannot_expand_its_provider_scope_with_an_uncaptured_child_model() {
    let fixture = Fixture::new(Some("uncaptured-model"));
    // Invalid concrete choices fall back only while directory selection is enabled.
    fixture
        .session
        .set_subagent_model_settings(
            Some(true),
            Some(vec![
                klaus_agent_core::schemas::config::SubagentModelOption {
                    value: format!("custom:{MODEL}").into(),
                    label: "Captured model".into(),
                },
            ]),
        )
        .unwrap();
    let gate = Arc::new(Gate::default());
    fixture.a.enqueue(Reply::Tool, Some(gate.clone()));
    let task = fixture.start();
    gate.wait().await;
    fixture.refresh().await;
    gate.release.notify_one();
    let events = fixture.finish(task).await;
    assert!(
        !events.iter().any(|event| matches!(event,
            EngineEvent::ItemUpdated { item: ThreadItem::ToolCall { id, error: Some(_), .. }, .. }
                if id == "catalog-read"
        )),
        "an unavailable selection falls back to the captured parent: {events:?}"
    );
    let child_events = std::mem::take(&mut *fixture.child_events.lock().unwrap());
    assert!(
        child_events.iter().any(|event| matches!(event,
            EngineEvent::StreamRequestStart { inference: Some(identity) }
                if identity.model == MODEL
        )),
        "the fallback child must use the captured model: {child_events:?}"
    );
    assert!(!fixture
        .requests
        .lock()
        .unwrap()
        .iter()
        .any(|(_, request)| request.model == "uncaptured-model"));
    fixture.close().await;
}
