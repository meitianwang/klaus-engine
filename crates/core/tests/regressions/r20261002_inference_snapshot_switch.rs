//! Defect: a live session could not publish a new model while work was active,
//! and removing that busy check alone mixed providers inside a captured request.
//! Root cause: request planning, retries, tool context and spend read session-latest
//! state instead of one immutable inference snapshot per normal main-loop step.
//! Scope: public Session API, one main turn, independent session isolation.
//! Work item: 2026-10-02 inference-snapshot publication and capture contract.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::deps::{CompactionResult, Compactor};
use klaus_agent_core::engine::session::{resolve_config, Input, Session, TurnOptions};
use klaus_agent_core::observability::cost_tracker::{ModelPricing, PricingTable};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeString};
use klaus_agent_core::schemas::config::ResolvedConfig;
use klaus_agent_core::schemas::content::{ContentBlock, HarnessEnvelope};
use klaus_agent_core::schemas::effort::{EffortLevel, EffortValue};
use klaus_agent_core::schemas::message::{CompactBoundaryMessage, Message, Usage};
use klaus_agent_core::tools::build_tool::text_result;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{
    ProviderError, ProviderErrorKind, TerminalReason, ToolError,
};
use klaus_agent_core::types::events::{EngineEvent, InferenceIdentity};
use klaus_agent_core::types::json::Json;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};
use serde_json::json;
use tokio::sync::Notify;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

const WAIT: Duration = Duration::from_secs(10);
type Requests = Arc<Mutex<Vec<(String, ModelRequest)>>>;

fn is_skill_catalog_context(message: &Message) -> bool {
    matches!(message, Message::User(user)
        if user.is_meta == Some(true)
            && matches!(user.content.as_slice(), [ContentBlock::Text(text)]
                if text.harness_envelope == Some(HarnessEnvelope::SystemReminder)
                    && text.text.starts_with("<skill-catalog:")))
}

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
    Text,
    Tool,
    Truncated,
    Network,
    TooLarge,
}

struct Script {
    reply: Reply,
    gate: Option<Arc<Gate>>,
}

struct Provider {
    info: ProviderInfo,
    label: String,
    requests: Requests,
    scripts: Mutex<VecDeque<Script>>,
    vision: bool,
}

impl Provider {
    fn new(label: &str, requests: &Requests, vision: bool) -> Arc<Self> {
        Arc::new(Self {
            info: ProviderInfo {
                name: label.into(),
                default_model: format!("snapshot-{label}").into(),
                base_url: None,
                env_key: None,
            },
            label: label.to_string(),
            requests: requests.clone(),
            scripts: Mutex::new(VecDeque::new()),
            vision,
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
            native_pdf: self.vision,
            vision: self.vision,
            image_limits: None,
            max_media_blocks: None,
            context_window: if self.vision { 200_000 } else { 100_000 },
            max_output_tokens: 8_192,
        }
    }

    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        self.requests
            .lock()
            .unwrap()
            .push((self.label.clone(), request.clone()));
        let script = self
            .scripts
            .lock()
            .unwrap()
            .pop_front()
            .expect("every provider request must be explicitly scripted");
        Box::pin(async_stream::stream! {
            let error = match script.reply {
                Reply::Network => Some(ProviderErrorKind::Network),
                Reply::TooLarge => Some(ProviderErrorKind::PromptTooLong),
                _ => None,
            };
            if error.is_none() {
                yield ModelStreamEvent::MessageStart { model: request.model.clone() };
                yield ModelStreamEvent::TextDelta { text: "Captured request remains alive.".into() };
            }
            if let Some(gate) = script.gate {
                gate.entered.notify_one();
                tokio::select! {
                    _ = gate.release.notified() => {},
                    _ = signal.cancelled() => return,
                }
            }
            if let Some(kind) = error {
                yield ModelStreamEvent::Error { error: ProviderError {
                    kind, message: "controlled provider failure".into(),
                    status: if matches!(script.reply, Reply::TooLarge) { Some(413) } else { None },
                    retry_after_ms: Some(1), request_id: None,
                    code: None, error_type: None, media_origins: None, media_repair_complete: false, credential_recovery: None,
                }};
                return;
            }
            if matches!(script.reply, Reply::Tool) {
                yield ModelStreamEvent::ToolUseStart {
                    id: "paused-tool".into(), name: "snapshot_pause".into(),
                    thought_signature: None, tool_kind: None, input_format: None,
                };
                yield ModelStreamEvent::ToolUseInputDelta { id: "paused-tool".into(), partial_json: "{}".into() };
                yield ModelStreamEvent::ToolUseStop { id: "paused-tool".into() };
            }
            yield ModelStreamEvent::MessageStop {
                stop_reason: match script.reply {
                    Reply::Tool => ModelStopReason::ToolUse,
                    Reply::Truncated => ModelStopReason::MaxTokens,
                    _ => ModelStopReason::EndTurn,
                },
                usage: Usage { input_tokens: 1_000, output_tokens: 0,
                    cache_read_input_tokens: 0, cache_creation_input_tokens: 0, total_tokens: None },
            };
        })
    }
}

type ObservedToolCapabilities = (Option<bool>, Option<bool>, AbortSignal);

struct PauseTool {
    gate: Arc<Gate>,
    contexts: Mutex<Vec<ObservedToolCapabilities>>,
}

#[async_trait]
impl Tool for PauseTool {
    fn static_name(&self) -> &str {
        "snapshot_pause"
    }
    fn static_description(&self) -> &str {
        "Pause the captured request's tool continuation"
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
        self.contexts.lock().unwrap().push((
            context.vision,
            context.native_pdf,
            context.signal.clone(),
        ));
        self.gate.entered.notify_one();
        tokio::select! {
            _ = self.gate.release.notified() => {},
            _ = context.signal.cancelled() => {},
        }
        Ok(text_result(Json::Null, "Captured tool finished", false))
    }
}

fn selected(model: &str, source: &str, b: bool) -> ResolvedConfig {
    resolve_config(json!({
        "model":model, "prewarm":false, "fork":false, "thinking":true,
        "effort":if b { "high" } else { "low" },
        "maxOutputTokens":if b { 1_536 } else { 1_024 },
        "maxOutputRetryTokens":if b { 3_072 } else { 2_048 },
        "contextWindow":if b { 200_000 } else { 100_000 },
        "modelSource":{"source":source,"modelId":model,"tier":"high","selection":format!("{source}:{model}")},
        "memory":{"enabled":false},
        "autoMemory":false, "autoDream":false, "memoryRecall":false,
        "speculativeCompaction":false, "earlyMicrocompact":false, "idleMicrocompact":false,
        "compactionSelfCheck":false, "loopDetection":false,
    })).unwrap()
}

fn prices(model: &str, rate: f64) -> PricingTable {
    vec![(
        model.to_string(),
        ModelPricing {
            input_per_m_tok: rate * 1_000.0,
            output_per_m_tok: 0.0,
            cache_read_per_m_tok: 0.0,
            cache_write_per_m_tok: 0.0,
        },
    )]
}

struct Fixture {
    process: RuntimeProcessContext,
    session: Arc<Session>,
    a: Arc<Provider>,
    b: Arc<Provider>,
    requests: Requests,
    tool: Arc<PauseTool>,
}

impl Fixture {
    fn new(compactor: Option<Arc<dyn Compactor>>) -> Self {
        let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let a = Provider::new("a", &requests, false);
        let b = Provider::new("b", &requests, true);
        let tool = Arc::new(PauseTool {
            gate: Arc::new(Gate::default()),
            contexts: Mutex::new(Vec::new()),
        });
        let mut options = agent_options(process.clone());
        options.provider = a.clone();
        options.tools = Some(vec![tool.clone()]);
        options.include_default_agents = Some(false);
        options.pricing = Some(prices("snapshot-a", 1.0).into_iter().collect());
        options.compactor = compactor;
        let config = options.config.as_mut().unwrap();
        let selected = serde_json::to_value(selected("snapshot-a", "custom", false)).unwrap();
        for (key, value) in selected.as_object().unwrap() {
            if !matches!(
                key.as_str(),
                "permissionMode"
                    | "sandbox"
                    | "requireRealSandbox"
                    | "sandboxNetworkAllowed"
                    | "sandboxRetryOnDenial"
            ) {
                config[key] = value.clone();
            }
        }
        let agent = Agent::new(options);
        let session = agent.start_session(StartSessionOptions::default());
        Self {
            process,
            session,
            a,
            b,
            requests,
            tool,
        }
    }

    fn begin(&self, id: &str) {
        assert_eq!(
            self.session
                .begin_model_update(id.to_string(), Some(0))
                .unwrap(),
            0
        );
    }

    fn publish_b(&self, id: &str) -> Result<u64, String> {
        self.session.reconfigure_inference_with_change_id(
            self.b.clone(),
            selected("snapshot-b", "local", true),
            None,
            Some(prices("snapshot-b", 10.0)),
            0,
            Some(id.to_string()),
        )
    }

    fn start(&self, signal: Option<AbortSignal>) -> tokio::task::JoinHandle<Vec<EngineEvent>> {
        let turn = self.session.run_streamed(
            Input::Text("Continue this exact turn".into()),
            TurnOptions {
                signal,
                ..Default::default()
            },
        );
        tokio::spawn(async move { turn.events.collect().await })
    }

    async fn finish(&self, task: tokio::task::JoinHandle<Vec<EngineEvent>>) -> Vec<EngineEvent> {
        tokio::time::timeout(WAIT, task).await.unwrap().unwrap()
    }

    async fn close(self) {
        self.session.shutdown_and_wait().await.unwrap();
        self.process.shutdown().await.unwrap();
    }
}

fn starts(events: &[EngineEvent]) -> Vec<InferenceIdentity> {
    events
        .iter()
        .filter_map(|event| match event {
            EngineEvent::StreamRequestStart { inference } => {
                Some(inference.clone().expect("request start needs attribution"))
            }
            _ => None,
        })
        .collect()
}

fn identity(
    identity: &InferenceIdentity,
    revision: u64,
    model: &str,
    source: &str,
    change: Option<&str>,
) {
    assert_eq!(identity.revision, revision);
    assert_eq!(identity.model, model);
    assert_eq!(identity.source.as_ref().unwrap(), source);
    assert_eq!(identity.tier.as_ref().unwrap(), "high");
    assert_eq!(
        identity.selection.as_ref().unwrap().try_to_utf8().unwrap(),
        format!("{source}:{model}")
    );
    assert_eq!(
        identity
            .change_id
            .as_ref()
            .map(|value| value.try_to_utf8().unwrap()),
        change.map(str::to_string)
    );
}

fn completed(events: &[EngineEvent]) {
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                EngineEvent::TurnCompleted {
                    stop_reason: TerminalReason::Completed,
                    ..
                }
            ))
            .count(),
        1,
        "one user input must produce exactly one completed main turn: {events:?}"
    );
}

#[tokio::test]
async fn streaming_publication_switches_the_same_turns_next_step_with_prompt_caps_effort_retry_budget_price_and_source(
) {
    let fixture = Fixture::new(None);
    let stream = Arc::new(Gate::default());
    fixture.a.enqueue(Reply::Tool, Some(stream.clone()));
    fixture.b.enqueue(Reply::Truncated, None);
    fixture.b.enqueue(Reply::Text, None);
    let task = fixture.start(None);
    stream.wait().await;
    fixture.begin("switch-b");
    assert_eq!(fixture.publish_b("switch-b").unwrap(), 1);
    assert_eq!(fixture.requests.lock().unwrap().len(), 1);
    stream.release.notify_one();
    fixture.tool.gate.wait().await;
    {
        let contexts = fixture.tool.contexts.lock().unwrap();
        assert_eq!((contexts[0].0, contexts[0].1), (Some(false), Some(false)));
        assert!(
            !contexts[0].2.is_cancelled(),
            "publishing must not cancel A's tool"
        );
    }
    fixture.tool.gate.release.notify_one();
    let events = fixture.finish(task).await;
    completed(&events);
    let sent = fixture.requests.lock().unwrap().clone();
    assert_eq!(
        sent.iter()
            .map(|(label, _)| label.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b", "b"]
    );
    assert_eq!(
        sent.iter()
            .map(|(_, request)| request.max_output_tokens)
            .collect::<Vec<_>>(),
        vec![1_024, 1_536, 3_072]
    );
    assert_eq!(sent[0].1.effort, Some(EffortValue::Level(EffortLevel::Low)));
    assert_eq!(
        sent[1].1.effort,
        Some(EffortValue::Level(EffortLevel::High))
    );
    for (index, (_, request)) in sent.iter().enumerate() {
        let model = if index == 0 {
            "snapshot-a"
        } else {
            "snapshot-b"
        };
        assert_eq!(request.model, model);
        let env = request
            .system
            .iter()
            .find(|part| part.name == "env")
            .expect("real default environment prompt");
        assert!(env.text.contains(model), "{env:?}");
        assert!(
            !env.text.contains(if index == 0 {
                "snapshot-b"
            } else {
                "snapshot-a"
            }),
            "{env:?}"
        );
    }
    let runtime = fixture.session.model_runtime();
    assert!(runtime.2.vision && runtime.2.native_pdf);
    assert_eq!(runtime.1.max_output_retry_tokens, Some(3_072));
    assert_eq!(runtime.1.context_window, 200_000);
    let requests = starts(&events);
    assert_eq!(requests.len(), 3);
    identity(&requests[0], 0, "snapshot-a", "custom", None);
    for request in &requests[1..] {
        identity(request, 1, "snapshot-b", "local", Some("switch-b"));
    }
    assert_eq!(fixture.session.cost_snapshot().total_cost_usd, 21.0);
    fixture.close().await;
}

#[tokio::test]
async fn network_retry_is_pinned_to_a_and_only_the_next_normal_step_captures_b() {
    let fixture = Fixture::new(None);
    let failed = Arc::new(Gate::default());
    fixture.a.enqueue(Reply::Network, Some(failed.clone()));
    fixture.a.enqueue(Reply::Tool, None);
    fixture.b.enqueue(Reply::Text, None);
    let task = fixture.start(None);
    failed.wait().await;
    fixture.begin("network-b");
    fixture.publish_b("network-b").unwrap();
    failed.release.notify_one();
    fixture.tool.gate.wait().await;
    fixture.tool.gate.release.notify_one();
    let events = fixture.finish(task).await;
    completed(&events);
    let sent = fixture.requests.lock().unwrap().clone();
    assert_eq!(
        sent.iter()
            .map(|(label, _)| label.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "a", "b"]
    );
    assert_eq!(
        sent[0].1, sent[1].1,
        "network retry must reuse the complete A request"
    );
    let requests = starts(&events);
    assert_eq!(requests.len(), 3);
    for request in &requests[..2] {
        identity(request, 0, "snapshot-a", "custom", None);
    }
    identity(&requests[2], 1, "snapshot-b", "local", Some("network-b"));
    assert_eq!(fixture.session.cost_snapshot().total_cost_usd, 11.0);
    fixture.close().await;
}

#[cfg(feature = "testing")]
#[tokio::test]
async fn timing_records_follow_the_captured_provider_after_switching_and_retrying() {
    use klaus_agent_core::observability::provider_diagnostics::{
        log_path_named, TIMING_LOG_FILE_NAME,
    };

    let fixture = Fixture::new(None);
    fixture.a.enqueue(Reply::Tool, None);
    fixture.b.enqueue(Reply::Network, None);
    fixture.b.enqueue(Reply::Text, None);
    let task = fixture.start(None);
    fixture.tool.gate.wait().await;
    fixture.begin("timing-b");
    fixture.publish_b("timing-b").unwrap();
    fixture.tool.gate.release.notify_one();
    completed(&fixture.finish(task).await);
    let session_id = fixture.session.id.try_to_utf8().unwrap();
    let records = std::fs::read_to_string(log_path_named(TIMING_LOG_FILE_NAME))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .filter(|record| record["sessionId"] == session_id && record["purpose"] == "normal")
        .collect::<Vec<_>>();
    let started = records
        .iter()
        .filter(|record| record["kind"] == "started")
        .map(|record| {
            (
                record["model"].as_str().unwrap(),
                record["providerId"].as_str().unwrap(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        started,
        vec![
            ("snapshot-a", "a"),
            ("snapshot-b", "b"),
            ("snapshot-b", "b")
        ]
    );
    for record in &records {
        assert_eq!(
            record["providerId"],
            if record["model"] == "snapshot-a" {
                "a"
            } else {
                "b"
            }
        );
    }
    let retry = records
        .iter()
        .filter(|record| record["kind"] == "retry")
        .collect::<Vec<_>>();
    assert_eq!(retry.len(), 1);
    assert_eq!(retry[0]["model"], "snapshot-b");
    assert_eq!(retry[0]["providerId"], "b");
    fixture.close().await;
}

#[derive(Default)]
struct RecoveryCompactor {
    observed: Arc<Mutex<Vec<ResolvedConfig>>>,
}

#[async_trait]
impl Compactor for RecoveryCompactor {
    fn rebind_model(
        &self,
        _: Arc<dyn ModelProvider>,
        _: &ResolvedConfig,
    ) -> Result<Arc<dyn Compactor>, String> {
        Ok(Arc::new(Self {
            observed: self.observed.clone(),
        }))
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
            compacted: false,
            freed_tokens: 0,
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
        self.observed.lock().unwrap().push(config.clone());
        let mut retained = vec![Message::CompactBoundary(CompactBoundaryMessage {
            native_compaction: None,
            uuid: "controlled-summary".into(),
            summary: "Continue this exact turn".into(),
        })];
        retained.extend(messages.iter().rev().take(1).cloned());
        CompactionResult {
            messages: retained,
            compacted: true,
            freed_tokens: 1_000,
            reason: None,
            diagnostics: Vec::new(),
        }
    }
}

#[tokio::test]
async fn payload_413_defers_old_source_compaction_until_the_next_turn_before_a_smaller_target() {
    let compactor = Arc::new(RecoveryCompactor::default());
    let fixture = Fixture::new(Some(compactor.clone()));
    fixture.a.enqueue(Reply::Text, None);
    completed(&fixture.finish(fixture.start(None)).await);
    fixture.requests.lock().unwrap().clear();
    let failed = Arc::new(Gate::default());
    fixture.a.enqueue(Reply::TooLarge, Some(failed.clone()));
    let task = fixture.start(None);
    failed.wait().await;
    fixture.begin("payload-b");
    let mut target = selected("snapshot-b", "local", true);
    target.context_window = 50_000;
    fixture
        .session
        .reconfigure_inference_with_change_id(
            fixture.b.clone(),
            target,
            Some(50_000),
            Some(prices("snapshot-b", 10.0)),
            0,
            Some("payload-b".to_string()),
        )
        .unwrap();
    failed.release.notify_one();
    let rejected = fixture.finish(task).await;
    assert!(
        rejected.iter().any(|event| matches!(event,
            EngineEvent::TurnFailed { error, .. }
                if error.provider_kind == Some(ProviderErrorKind::PromptTooLong)
        )),
        "{rejected:?}"
    );
    assert!(
        !rejected.iter().any(|event| matches!(
            event,
            EngineEvent::TurnCompleted {
                stop_reason: TerminalReason::Completed,
                ..
            }
        )),
        "an upstream overflow must finish the unfinished turn: {rejected:?}"
    );
    assert_eq!(fixture.requests.lock().unwrap().len(), 1);
    assert!(
        compactor.observed.lock().unwrap().is_empty(),
        "the rejected request must not trigger a same-turn summary or resend"
    );
    let first_starts = starts(&rejected);
    assert_eq!(first_starts.len(), 1);
    identity(&first_starts[0], 0, "snapshot-a", "custom", None);

    fixture.b.enqueue(Reply::Tool, None);
    fixture.b.enqueue(Reply::Text, None);
    let next = fixture.start(None);
    fixture.tool.gate.wait().await;
    assert_eq!(
        fixture
            .requests
            .lock()
            .unwrap()
            .iter()
            .map(|(label, _)| label.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b"]
    );
    fixture.tool.gate.release.notify_one();
    let events = fixture.finish(next).await;
    completed(&events);
    let sent = fixture.requests.lock().unwrap().clone();
    assert_eq!(
        sent.iter()
            .map(|(label, _)| label.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b", "b"]
    );
    let before = sent[0]
        .1
        .messages
        .iter()
        .filter(|message| !is_skill_catalog_context(message))
        .collect::<Vec<_>>();
    let resumed = sent[1]
        .1
        .messages
        .iter()
        .filter(|message| !is_skill_catalog_context(message))
        .collect::<Vec<_>>();
    let Some(Message::CompactBoundary(boundary)) = resumed.last() else {
        panic!("expected the checkpoint after retained user input, got {resumed:?}");
    };
    assert_eq!(boundary.uuid, "controlled-summary");
    assert_eq!(boundary.summary, "Continue this exact turn");
    let old_users = before
        .iter()
        .filter(|message| {
            matches!(message,
        Message::User(user) if user.is_meta != Some(true))
        })
        .collect::<Vec<_>>();
    assert!(!old_users.is_empty());
    for message in &old_users {
        assert!(
            resumed.contains(message),
            "retained user identity must survive: {message:?}"
        );
    }
    assert_eq!(
        resumed
            .iter()
            .filter(|message| matches!(message,
        Message::User(user) if user.is_meta != Some(true)))
            .count(),
        old_users.len() + 1
    );
    assert!(before
        .iter()
        .any(|message| matches!(message, Message::Assistant(_))));
    assert!(before
        .iter()
        .filter(|message| matches!(message, Message::Assistant(_)))
        .all(|message| !resumed.contains(message)));
    let catalog_contexts = sent[1]
        .1
        .messages
        .iter()
        .filter(|message| is_skill_catalog_context(message))
        .collect::<Vec<_>>();
    let [Message::User(catalog)] = catalog_contexts.as_slice() else {
        panic!("expected exactly one retained catalog context, got {catalog_contexts:?}");
    };
    let [ContentBlock::Text(catalog_text)] = catalog.content.as_slice() else {
        unreachable!()
    };
    assert!(catalog_text
        .text
        .ends_with("No skills are currently available. Previous skill listings no longer apply."));
    assert_eq!(sent[1].1.model, "snapshot-b");
    assert_eq!(sent[1].1.max_output_tokens, 1_536);
    let observed = compactor.observed.lock().unwrap().clone();
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].model, "snapshot-a");
    assert_eq!(
        observed[0].model_source.as_ref().unwrap().source.as_deref(),
        Some("custom")
    );
    let requests = starts(&events);
    assert_eq!(requests.len(), 2);
    for request in &requests {
        identity(request, 1, "snapshot-b", "local", Some("payload-b"));
    }
    fixture.close().await;
}

async fn at_tool(
    fixture: &Fixture,
    signal: Option<AbortSignal>,
) -> tokio::task::JoinHandle<Vec<EngineEvent>> {
    fixture.a.enqueue(Reply::Tool, None);
    let task = fixture.start(signal);
    fixture.tool.gate.wait().await;
    task
}

async fn assert_fenced(fixture: &Fixture, task: &tokio::task::JoinHandle<Vec<EngineEvent>>) {
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        fixture.requests.lock().unwrap().len(),
        1,
        "a pending update must never send cached A"
    );
    assert!(
        !task.is_finished(),
        "the uncaptured next step must wait for publication"
    );
}

#[tokio::test]
async fn begin_fences_the_next_same_turn_capture_until_publication_without_blocking_a_tool() {
    let fixture = Fixture::new(None);
    let task = at_tool(&fixture, None).await;
    fixture.begin("fence-b");
    fixture.tool.gate.release.notify_one();
    assert_fenced(&fixture, &task).await;
    fixture.b.enqueue(Reply::Text, None);
    fixture.publish_b("fence-b").unwrap();
    let events = fixture.finish(task).await;
    completed(&events);
    assert_eq!(fixture.requests.lock().unwrap()[1].0, "b");
    assert_eq!(
        fixture.session.model_update_status(),
        (Some("fence-b".to_string()), None, None)
    );
    fixture.close().await;
}

#[tokio::test]
async fn failed_update_unblocks_waiters_with_an_error_and_never_sends_the_old_provider() {
    let fixture = Fixture::new(None);
    let task = at_tool(&fixture, None).await;
    fixture.begin("failed-b");
    fixture.tool.gate.release.notify_one();
    assert_fenced(&fixture, &task).await;
    fixture
        .session
        .fail_model_update("failed-b", "target unavailable".to_string());
    let events = fixture.finish(task).await;
    assert!(events.iter().any(|event| matches!(event, EngineEvent::TurnFailed { error, .. } if error.message.contains("target unavailable"))), "{events:?}");
    assert_eq!(fixture.requests.lock().unwrap().len(), 1);
    assert_eq!(fixture.session.model_revision(), 0);
    assert!(fixture
        .publish_b("failed-b")
        .unwrap_err()
        .contains("target unavailable"));
    fixture.begin("recovered-b");
    fixture.b.enqueue(Reply::Text, None);
    fixture.publish_b("recovered-b").unwrap();
    completed(&fixture.finish(fixture.start(None)).await);
    assert_eq!(fixture.requests.lock().unwrap()[1].0, "b");
    fixture.close().await;
}

#[tokio::test]
async fn cancellation_and_closing_wake_fenced_captures_without_provider_requests() {
    for close in [false, true] {
        let fixture = Fixture::new(None);
        let signal = AbortSignal::new();
        let task = at_tool(&fixture, Some(signal.clone())).await;
        fixture.begin("waiting-b");
        fixture.tool.gate.release.notify_one();
        assert_fenced(&fixture, &task).await;
        if close {
            fixture.session.shutdown_and_wait().await.unwrap();
        } else {
            signal.cancel();
        }
        let events = fixture.finish(task).await;
        if !close {
            assert!(
                events.iter().any(|event| matches!(
                    event,
                    EngineEvent::TurnCompleted {
                        stop_reason: TerminalReason::Aborted,
                        ..
                    }
                )),
                "{events:?}"
            );
        }
        assert_eq!(fixture.requests.lock().unwrap().len(), 1);
        assert_eq!(fixture.session.model_revision(), 0);
        if close {
            assert!(fixture
                .publish_b("waiting-b")
                .unwrap_err()
                .contains("closing"));
            assert!(fixture
                .session
                .begin_model_update("late".to_string(), Some(0))
                .unwrap_err()
                .contains("closing"));
        }
        fixture.close().await;
    }
}

#[tokio::test]
async fn latest_selector_owns_the_fence_stale_cas_cannot_clear_it_and_same_change_is_idempotent() {
    let fixture = Fixture::new(None);
    let task = at_tool(&fixture, None).await;
    assert!(fixture
        .publish_b("unbegun")
        .unwrap_err()
        .contains("has not begun"));
    fixture.begin("old-selection");
    fixture.begin("latest-selection");
    assert!(fixture
        .publish_b("old-selection")
        .unwrap_err()
        .contains("superseded"));
    fixture
        .session
        .fail_model_update("old-selection", "stale failure".to_string());
    assert_eq!(
        fixture.session.model_update_status(),
        (None, Some("latest-selection".to_string()), None)
    );
    let stale = fixture
        .session
        .reconfigure_inference_with_change_id(
            fixture.b.clone(),
            selected("snapshot-b", "local", true),
            None,
            Some(prices("snapshot-b", 10.0)),
            99,
            Some("latest-selection".to_string()),
        )
        .unwrap_err();
    assert!(stale.contains("revision conflict"));
    fixture.tool.gate.release.notify_one();
    assert_fenced(&fixture, &task).await;
    fixture.b.enqueue(Reply::Text, None);
    assert_eq!(fixture.publish_b("latest-selection").unwrap(), 1);
    assert_eq!(
        fixture.publish_b("latest-selection").unwrap(),
        1,
        "lost-ACK replay must not increment revision"
    );
    let reused = fixture
        .session
        .reconfigure_inference_with_change_id(
            fixture.b.clone(),
            selected("different-model", "local", true),
            None,
            Some(prices("snapshot-b", 10.0)),
            0,
            Some("latest-selection".to_string()),
        )
        .unwrap_err();
    assert!(reused.contains("different configuration"));
    completed(&fixture.finish(task).await);
    assert_eq!(fixture.session.model_revision(), 1);
    assert_eq!(fixture.requests.lock().unwrap().len(), 2);
    fixture.close().await;
}

#[tokio::test]
async fn same_named_models_keep_separate_sources_prices_and_session_scope() {
    let fixture = Fixture::new(None);
    let independent = Fixture::new(None);
    let stream = Arc::new(Gate::default());
    fixture.a.enqueue(Reply::Tool, Some(stream.clone()));
    fixture.b.enqueue(Reply::Text, None);
    independent.a.enqueue(Reply::Text, None);
    let task = fixture.start(None);
    stream.wait().await;
    fixture.begin("same-name-new-source");
    assert_eq!(
        fixture
            .session
            .reconfigure_inference_with_change_id(
                fixture.b.clone(),
                selected("snapshot-a", "local", true),
                None,
                Some(prices("snapshot-a", 10.0)),
                0,
                Some("same-name-new-source".to_string()),
            )
            .unwrap(),
        1
    );
    stream.release.notify_one();
    fixture.tool.gate.wait().await;
    fixture.tool.gate.release.notify_one();
    let events = fixture.finish(task).await;
    completed(&events);
    assert_eq!(fixture.session.cost_snapshot().total_cost_usd, 11.0);
    let requests = starts(&events);
    assert_eq!(requests.len(), 2);
    identity(&requests[0], 0, "snapshot-a", "custom", None);
    identity(
        &requests[1],
        1,
        "snapshot-a",
        "local",
        Some("same-name-new-source"),
    );
    completed(&independent.finish(independent.start(None)).await);
    assert_eq!(independent.session.model_revision(), 0);
    assert_eq!(independent.requests.lock().unwrap()[0].0, "a");
    assert_eq!(independent.session.cost_snapshot().total_cost_usd, 1.0);
    independent.close().await;
    fixture.close().await;
}

#[tokio::test]
async fn explicit_model_override_keeps_the_captured_source_and_reports_its_actual_selection() {
    let fixture = Fixture::new(None);
    fixture.a.enqueue(Reply::Text, None);
    let events = fixture
        .session
        .run_streamed(
            Input::Text("Use the authorized per-turn override".into()),
            TurnOptions {
                model: Some("snapshot-a-override".into()),
                ..Default::default()
            },
        )
        .events
        .collect::<Vec<_>>()
        .await;
    completed(&events);
    let requests = starts(&events);
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].model, "snapshot-a-override");
    assert!(requests[0]
        .source
        .as_ref()
        .is_some_and(|source| source == "custom"));
    assert!(requests[0]
        .selection
        .as_ref()
        .is_some_and(|selection| selection == "custom:snapshot-a-override"));
    assert_eq!(fixture.requests.lock().unwrap()[0].0, "a");
    fixture.close().await;
}

#[tokio::test]
async fn begin_before_the_admitted_turns_first_poll_waits_for_publication_and_never_sends_a() {
    let fixture = Fixture::new(None);
    fixture.b.enqueue(Reply::Text, None);
    // Registration is synchronous; the spawned collector has not had a poll.
    let task = fixture.start(None);
    fixture.begin("before-first-poll");
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(fixture.requests.lock().unwrap().is_empty());
    assert!(
        !task.is_finished(),
        "pending capture must wait, not report a fictitious closing session"
    );
    fixture.publish_b("before-first-poll").unwrap();
    let events = fixture.finish(task).await;
    completed(&events);
    let requests = starts(&events);
    assert_eq!(requests.len(), 1);
    identity(
        &requests[0],
        1,
        "snapshot-b",
        "local",
        Some("before-first-poll"),
    );
    assert_eq!(fixture.requests.lock().unwrap()[0].0, "b");
    fixture.close().await;
}

#[tokio::test]
async fn new_work_after_an_explicit_failed_begin_surfaces_that_failure_not_a_closing_error() {
    let fixture = Fixture::new(None);
    fixture.begin("unavailable");
    fixture.session.fail_model_update(
        "unavailable",
        "selected provider is unavailable".to_string(),
    );
    let events = fixture.finish(fixture.start(None)).await;
    assert!(events.iter().any(|event| matches!(event,
        EngineEvent::TurnFailed { error, .. } if error.message.contains("selected provider is unavailable")
    )), "{events:?}");
    assert!(
        !events.iter().any(|event| matches!(event,
            EngineEvent::TurnFailed { error, .. } if error.message.contains("shutting down")
        )),
        "{events:?}"
    );
    assert!(fixture.requests.lock().unwrap().is_empty());
    assert!(!fixture.session.is_disposed());
    fixture.close().await;
}
