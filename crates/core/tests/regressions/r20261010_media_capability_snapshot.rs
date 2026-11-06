//! Defect: outgoing media policy could consult live catalog facts or reuse another model's limits.
//! Root cause: captured capabilities were not bound to the internal request sent to the transport.
//! Work item: 2026-10-10 media capability snapshots; public Session regressions.

use std::num::NonZeroU32;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::deps::{CompactionResult, Compactor};
use klaus_agent_core::engine::session::{InferenceTargets, Input, Session, TurnOptions};
use klaus_agent_core::provider::model_catalog::{with_model_catalog, ModelCatalog};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeString};
use klaus_agent_core::schemas::config::ResolvedConfig;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::tools::build_tool::text_result;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{ProviderError, ProviderErrorKind, ToolError};
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::json::Json;
use klaus_agent_core::types::provider::{
    CallModel, ImageLimits, ModelProvider, ModelRequest, ModelStopReason, ModelStream,
    ModelStreamEvent, ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Notify;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

const WAIT: Duration = Duration::from_secs(10);

#[derive(Default)]
struct Gate {
    entered: Notify,
    release: Notify,
}

struct Provider {
    info: ProviderInfo,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
    calls: AtomicUsize,
    gate: Option<Arc<Gate>>,
}

impl ModelProvider for Provider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }
    fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
        let mut caps = ProviderCapabilities {
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
        };
        if model == "main" {
            caps.image_limits = Some(ImageLimits {
                max_width: NonZeroU32::new(2_000),
                max_height: NonZeroU32::new(2_000),
                max_base64_size: NonZeroU32::new(5_242_880),
                target_raw_size: None,
            });
            caps.max_media_blocks = NonZeroU32::new(600);
        } else if model == "summary" {
            caps.image_limits = Some(ImageLimits {
                max_height: NonZeroU32::new(1_200),
                max_base64_size: NonZeroU32::new(10_485_760),
                ..Default::default()
            });
            caps.max_media_blocks = NonZeroU32::new(100);
        } else if model == "blind" {
            caps.vision = false;
        }
        caps
    }
    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        self.requests.lock().unwrap().push(request.clone());
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let gate = self.gate.clone();
        Box::pin(async_stream::stream! {
            if call == 0 {
                if let Some(gate) = &gate {
                    gate.entered.notify_one();
                    tokio::select! {
                        _ = gate.release.notified() => {},
                        _ = signal.cancelled() => return,
                    }
                    yield ModelStreamEvent::Error { error: ProviderError {
                        kind: ProviderErrorKind::Network, message: "controlled fixture retry".into(),
                        status: None, retry_after_ms: Some(1), request_id: None,
                        code: None, error_type: None, media_origins: None, media_repair_complete: false, credential_recovery: None,
                    }};
                    return;
                }
            }
            yield ModelStreamEvent::MessageStart { model: request.model };
            let tool = gate.is_some() && call == 1;
            if tool {
                yield ModelStreamEvent::ToolUseStart {
                    id: "media-step".into(), name: "media_snapshot_probe".into(),
                    thought_signature: None, tool_kind: None, input_format: None,
                };
                yield ModelStreamEvent::ToolUseInputDelta { id: "media-step".into(), partial_json: "{}".into() };
                yield ModelStreamEvent::ToolUseStop { id: "media-step".into() };
            } else {
                yield ModelStreamEvent::TextDelta { text: "Fixture work completed.".into() };
            }
            yield ModelStreamEvent::MessageStop {
                stop_reason: if tool { ModelStopReason::ToolUse } else { ModelStopReason::EndTurn },
                usage: Usage { input_tokens: 10, output_tokens: 1, cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0, total_tokens: None },
            };
        })
    }
}

struct Probe;

#[async_trait]
impl Tool for Probe {
    fn static_name(&self) -> &str {
        "media_snapshot_probe"
    }
    fn static_description(&self) -> &str {
        "Advance the controlled media snapshot work step"
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
        _: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        Ok(text_result(Json::Null, "Fixture step advanced", false))
    }
}

async fn refresh(catalog: &ModelCatalog) {
    let body = json!({"schemaVersion":1,"entries":[{
        "provider":"media-fixture","model":"main","capabilities":{
            "imageLimits":{"maxWidth":1000,"maxBase64Size":10485760},"maxMediaBlocks":100
        }
    }]})
    .to_string();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/catalog", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut head = Vec::new();
        while !head.ends_with(b"\r\n\r\n") {
            head.push(socket.read_u8().await.unwrap());
        }
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()
        );
        socket.write_all(response.as_bytes()).await.unwrap();
    });
    catalog
        .refresh(&reqwest::Client::new(), &url)
        .await
        .unwrap();
    server.await.unwrap();
}

fn request() -> ModelRequest {
    runtime_codec::decode_text(&RuntimeString::from(
        r#"{"model":"main","system":[],"messages":[],"tools":[],"maxOutputTokens":100,"imageLimits":{"maxWidth":2000},"maxMediaBlocks":600}"#,
    )).unwrap()
}

fn options(
    process: RuntimeProcessContext,
    provider: Arc<dyn ModelProvider>,
) -> klaus_agent_core::engine::agent::AgentOptions {
    let mut options = agent_options(process);
    options.provider = provider;
    options.tools = Some(vec![Arc::new(Probe)]);
    options.include_default_agents = Some(false);
    let config = options.config.as_mut().unwrap();
    for (field, value) in [
        ("model", json!("main")),
        ("prewarm", json!(false)),
        ("fork", json!(false)),
        ("autoMemory", json!(false)),
        ("autoDream", json!(false)),
        ("memoryRecall", json!(false)),
        ("speculativeCompaction", json!(false)),
        ("earlyMicrocompact", json!(false)),
        ("idleMicrocompact", json!(false)),
        ("compactionSelfCheck", json!(false)),
        ("loopDetection", json!(false)),
    ] {
        config[field] = value;
    }
    options
}

fn provider(gate: Option<Arc<Gate>>, requests: Arc<Mutex<Vec<ModelRequest>>>) -> Arc<Provider> {
    Arc::new(Provider {
        info: ProviderInfo {
            name: "media-fixture".into(),
            default_model: "main".into(),
            base_url: None,
            env_key: None,
        },
        requests,
        calls: AtomicUsize::new(0),
        gate,
    })
}

fn injected_transport(requests: Arc<Mutex<Vec<ModelRequest>>>) -> CallModel {
    Box::new(move |request, _| {
        let model = request.model.clone();
        requests.lock().unwrap().push(request);
        Box::pin(futures::stream::iter([
            ModelStreamEvent::MessageStart { model },
            ModelStreamEvent::TextDelta {
                text: "Injected fixture completed.".into(),
            },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: Usage {
                    input_tokens: 10,
                    output_tokens: 1,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: None,
                },
            },
        ]))
    })
}

async fn finish(session: &Arc<Session>, input: &str) {
    let events = tokio::time::timeout(
        WAIT,
        session
            .run_streamed(Input::Text(input.into()), TurnOptions::default())
            .events
            .collect::<Vec<_>>(),
    )
    .await
    .unwrap();
    assert!(
        events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnCompleted { .. })),
        "{events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
}

#[tokio::test]
async fn catalog_refresh_keeps_retry_snapshot_and_next_work_step_adopts_new_media_facts() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let gate = Arc::new(Gate::default());
    let catalog = ModelCatalog::empty();
    let live = with_model_catalog(
        provider(Some(gate.clone()), requests.clone()),
        catalog.clone(),
    );
    let session =
        Agent::new(options(process.clone(), live)).start_session(StartSessionOptions::default());
    let task_session = session.clone();
    let task = tokio::spawn(async move { finish(&task_session, "Advance the fixture step").await });
    tokio::time::timeout(WAIT, gate.entered.notified())
        .await
        .unwrap();
    refresh(&catalog).await;
    gate.release.notify_one();
    tokio::time::timeout(WAIT, task).await.unwrap().unwrap();
    {
        let sent = requests.lock().unwrap();
        assert_eq!(sent.len(), 3);
        for request in &sent[..2] {
            let limits = request.image_limits.unwrap();
            assert_eq!(limits.max_width.unwrap().get(), 2_000);
            assert_eq!(limits.max_base64_size.unwrap().get(), 5_242_880);
            assert_eq!(limits.target_raw_size, None);
            assert_eq!(request.max_media_blocks.unwrap().get(), 600);
        }
        assert_eq!(
            sent[0], sent[1],
            "ordinary retry keeps the captured request unchanged"
        );
        let next = &sent[2];
        assert_eq!(next.image_limits.unwrap().max_width.unwrap().get(), 1_000);
        assert_eq!(next.image_limits.unwrap().max_height.unwrap().get(), 2_000);
        assert_eq!(
            next.image_limits.unwrap().max_base64_size.unwrap().get(),
            10_485_760
        );
        assert_eq!(next.image_limits.unwrap().target_raw_size, None);
        assert_eq!(next.max_media_blocks.unwrap().get(), 100);
        assert_eq!(session.model_revision(), 0);
    }
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}

struct SummaryProbe {
    provider: Arc<dyn ModelProvider>,
}

fn unchanged(messages: &[Message]) -> CompactionResult {
    CompactionResult {
        messages: messages.to_vec(),
        freed_tokens: 0,
        compacted: false,
        reason: None,
        diagnostics: Vec::new(),
    }
}

#[async_trait]
impl Compactor for SummaryProbe {
    fn rebind_model(
        &self,
        provider: Arc<dyn ModelProvider>,
        _: &ResolvedConfig,
    ) -> Result<Arc<dyn Compactor>, String> {
        Ok(Arc::new(Self { provider }))
    }
    async fn maybe_compact(
        &self,
        messages: &[Message],
        _: u64,
        _: &ResolvedConfig,
        _: Option<&RuntimeString>,
    ) -> CompactionResult {
        unchanged(messages)
    }
    async fn reactive_compact(
        &self,
        messages: &[Message],
        config: &ResolvedConfig,
        _: Option<&RuntimeString>,
    ) -> CompactionResult {
        let mut summary = summary_media_request();
        summary.model = config.model.clone();
        let events = self
            .provider
            .stream(summary, AbortSignal::new())
            .collect::<Vec<_>>()
            .await;
        assert!(!events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::Error { .. })));
        unchanged(messages)
    }
}

fn summary_media_request() -> ModelRequest {
    use base64::Engine;
    use klaus_agent_core::schemas::content::{
        ContentBlock, ImageBlock, OutputBlock, ToolResultBlock,
    };
    use klaus_agent_core::schemas::message::{ToolResultMessage, UserMessage};
    let mut bytes = Vec::new();
    image::RgbImage::from_pixel(96, 2400, image::Rgb([20, 40, 60]))
        .write_with_encoder(image::codecs::png::PngEncoder::new(&mut bytes))
        .unwrap();
    let image = ImageBlock {
        mime_type: "image/png".into(),
        base64: base64::engine::general_purpose::STANDARD
            .encode(bytes)
            .into(),
        detail: None,
        source_path: Some("/fixture/immutable-summary-image.png".into()),
    };
    let mut request = request();
    request.messages = vec![
        Message::User(UserMessage {
            uuid: "summary-media".into(),
            is_meta: None,
            question_answer: None,
            content: vec![
                ContentBlock::Image(image.clone()),
                ContentBlock::ToolResult(ToolResultBlock {
                    tool_use_id: "nested".into(),
                    is_error: Some(false),
                    content: vec![OutputBlock::Image(image.clone())],
                }),
            ],
        }),
        Message::ToolResult(ToolResultMessage {
            uuid: "summary-output".into(),
            tool_use_id: "output".into(),
            tool_kind: None,
            output: None,
            native_tool_output: None,
            is_error: false,
            content: vec![OutputBlock::Image(image.clone())],
            extra_user_blocks: Some(vec![ContentBlock::Image(image)]),
        }),
    ];
    request
}

#[tokio::test]
async fn captured_summary_limits_gate_real_images_before_the_injected_transport() {
    use klaus_agent_core::schemas::content::{ContentBlock, OutputBlock};
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let inner = provider(None, Arc::new(Mutex::new(Vec::new())));
    let mut options = options(process.clone(), inner.clone());
    options.deps.call_model = Some(injected_transport(requests.clone()));
    options.compactor = Some(Arc::new(SummaryProbe { provider: inner }));
    let session = Agent::new(options).start_session(StartSessionOptions::default());
    finish(&session, "Seed real summary image admission").await;
    session.set_model("summary").unwrap();
    session.compact(None).await.unwrap();
    session.set_model("blind").unwrap();
    session.compact(None).await.unwrap();
    {
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        let vision = &requests[1];
        let Message::User(user) = &vision.messages[0] else {
            panic!("summary media user")
        };
        let ContentBlock::Image(top) = &user.content[0] else {
            panic!("top image must survive")
        };
        let ContentBlock::ToolResult(nested) = &user.content[1] else {
            panic!("nested carrier must survive")
        };
        let OutputBlock::Image(nested) = &nested.content[0] else {
            panic!("nested image")
        };
        let Message::ToolResult(result) = &vision.messages[1] else {
            panic!("output carrier")
        };
        let OutputBlock::Image(output) = &result.content[0] else {
            panic!("output image")
        };
        let ContentBlock::Image(extra) = &result.extra_user_blocks.as_ref().unwrap()[0] else {
            panic!("extra image")
        };
        for image in [top, nested, output, extra] {
            assert_eq!(
                klaus_agent_core::context::images::image_dimensions(image)
                    .unwrap()
                    .height,
                1200
            );
            assert!(image.source_path.is_none());
        }
        let blind =
            klaus_agent_core::runtime_values::runtime_codec::encode_bytes(&requests[2]).unwrap();
        let blind = serde_json::from_slice::<serde_json::Value>(&blind).unwrap();
        assert!(!blind["messages"].to_string().contains("\"type\":\"image\""));
        assert!(blind["messages"].to_string().contains("image was omitted"));
    }
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn manual_summary_model_switch_rebinds_media_limits_instead_of_inheriting_main_limits() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let live: Arc<dyn ModelProvider> = provider(None, requests.clone());
    let mut options = options(process.clone(), live.clone());
    options.compactor = Some(Arc::new(SummaryProbe { provider: live }));
    let session = Agent::new(options).start_session(StartSessionOptions::default());
    session
        .install_inference_targets(Some(InferenceTargets::default()), None, None)
        .unwrap();
    finish(&session, "Create a short fixture exchange").await;
    session.set_model("summary").unwrap();
    session.compact(None).await.unwrap();
    session.set_model("undeclared").unwrap();
    session.compact(None).await.unwrap();
    {
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        let summary = &requests[1];
        assert_eq!(summary.model, "summary");
        assert_eq!(summary.image_limits.unwrap().max_width, None);
        assert_eq!(
            summary.image_limits.unwrap().max_height.unwrap().get(),
            1_200
        );
        assert_eq!(
            summary.image_limits.unwrap().max_base64_size.unwrap().get(),
            10_485_760
        );
        assert_eq!(summary.max_media_blocks.unwrap().get(), 100);
        assert_eq!(requests[2].model, "undeclared");
        assert_eq!(requests[2].image_limits, Some(ImageLimits::default()));
        assert_eq!(requests[2].max_media_blocks.unwrap().get(), 100);
    }
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}

struct HeldSummaryProbe {
    provider: Arc<dyn ModelProvider>,
    gate: Arc<Gate>,
    attempts: Arc<AtomicUsize>,
}

#[async_trait]
impl Compactor for HeldSummaryProbe {
    fn rebind_model(
        &self,
        provider: Arc<dyn ModelProvider>,
        _: &ResolvedConfig,
    ) -> Result<Arc<dyn Compactor>, String> {
        Ok(Arc::new(Self {
            provider,
            gate: self.gate.clone(),
            attempts: self.attempts.clone(),
        }))
    }
    async fn maybe_compact(
        &self,
        messages: &[Message],
        _: u64,
        _: &ResolvedConfig,
        _: Option<&RuntimeString>,
    ) -> CompactionResult {
        unchanged(messages)
    }
    async fn reactive_compact(
        &self,
        messages: &[Message],
        config: &ResolvedConfig,
        _: Option<&RuntimeString>,
    ) -> CompactionResult {
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            self.gate.entered.notify_one();
            self.gate.release.notified().await;
        }
        let mut summary = request();
        summary.model = config.model.clone();
        let events = self
            .provider
            .stream(summary, AbortSignal::new())
            .collect::<Vec<_>>()
            .await;
        assert!(!events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::Error { .. })));
        unchanged(messages)
    }
}

#[tokio::test]
async fn same_model_summary_capture_freezes_catalog_facts_and_next_summary_refreshes_its_binder() {
    for qualified in [false, true] {
        let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let inner = provider(None, Arc::new(Mutex::new(Vec::new())));
        let catalog = ModelCatalog::empty();
        let live = with_model_catalog(inner.clone(), catalog.clone());
        let gate = Arc::new(Gate::default());
        let mut options = options(process.clone(), live.clone());
        options.deps.call_model = Some(injected_transport(requests.clone()));
        options.compactor = Some(Arc::new(HeldSummaryProbe {
            provider: live,
            gate: gate.clone(),
            attempts: Arc::new(AtomicUsize::new(0)),
        }));
        let session = Agent::new(options).start_session(StartSessionOptions::default());
        if qualified {
            session
                .install_inference_targets(Some(InferenceTargets::default()), None, None)
                .unwrap();
        }
        finish(&session, "Seed the held summary fixture").await;
        let compact_session = session.clone();
        let compact = tokio::spawn(async move { compact_session.compact(None).await });
        tokio::time::timeout(WAIT, gate.entered.notified())
            .await
            .unwrap();
        refresh(&catalog).await;
        gate.release.notify_one();
        tokio::time::timeout(WAIT, compact)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        session.compact(None).await.unwrap();
        {
            let sent = requests.lock().unwrap();
            assert_eq!(sent.len(), 3);
            let held = &sent[1];
            assert_eq!(held.model, "main");
            assert_eq!(held.image_limits.unwrap().max_width.unwrap().get(), 2_000);
            assert_eq!(
                held.image_limits.unwrap().max_base64_size.unwrap().get(),
                5_242_880
            );
            assert_eq!(held.max_media_blocks.unwrap().get(), 600);
            let next = &sent[2];
            assert_eq!(next.model, "main");
            assert_eq!(next.image_limits.unwrap().max_width.unwrap().get(), 1_000);
            assert_eq!(next.image_limits.unwrap().max_height.unwrap().get(), 2_000);
            assert_eq!(
                next.image_limits.unwrap().max_base64_size.unwrap().get(),
                10_485_760
            );
            assert_eq!(next.image_limits.unwrap().target_raw_size, None);
            assert_eq!(next.max_media_blocks.unwrap().get(), 100);
            assert_eq!(
                inner.calls.load(Ordering::SeqCst),
                0,
                "summary must retain the injected transport"
            );
            assert_eq!(session.model_revision(), 0);
        }
        session.shutdown_and_wait().await.unwrap();
        process.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn summary_model_switch_without_tier_targets_keeps_injected_transport_and_rebinds_limits() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let inner = provider(None, Arc::new(Mutex::new(Vec::new())));
    let mut options = options(process.clone(), inner.clone());
    options.deps.call_model = Some(injected_transport(requests.clone()));
    options.compactor = Some(Arc::new(SummaryProbe {
        provider: inner.clone(),
    }));
    let session = Agent::new(options).start_session(StartSessionOptions::default());
    finish(&session, "Seed the independent summary target fixture").await;
    session.set_model("summary").unwrap();
    session.compact(None).await.unwrap();
    session.set_model("undeclared").unwrap();
    session.compact(None).await.unwrap();
    {
        let sent = requests.lock().unwrap();
        assert_eq!(sent.len(), 3);
        assert_eq!(sent[1].model, "summary");
        assert_eq!(sent[1].image_limits.unwrap().max_width, None);
        assert_eq!(
            sent[1].image_limits.unwrap().max_height.unwrap().get(),
            1_200
        );
        assert_eq!(sent[1].max_media_blocks.unwrap().get(), 100);
        assert_eq!(sent[2].model, "undeclared");
        assert_eq!(sent[2].image_limits, Some(ImageLimits::default()));
        assert_eq!(sent[2].max_media_blocks.unwrap().get(), 100);
        assert_eq!(inner.calls.load(Ordering::SeqCst), 0);
    }
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}
