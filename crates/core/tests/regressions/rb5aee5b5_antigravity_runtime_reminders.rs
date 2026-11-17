//! Defect: changing runtime reminders rewrote the leading system instruction
//! throughout Antigravity tool loops and resumed conversations.
//! Root cause: request-local system fragments were concatenated ahead of history
//! instead of recording source-scoped, append-only context transitions.
//! Work item b5aee5b5: persist hidden reminder steps with ordinary history, keep
//! other sources unchanged, and exclude obsolete reminders from summaries.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use klaus_agent_core::context::compact::CompactionNoopReason;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::deps::{CompactionResult, Compactor};
use klaus_agent_core::engine::session::{resolve_config, Input, Session, TurnOptions};
use klaus_agent_core::provider::gemini::{
    generate_content_request, GeminiAdapterPolicy, GeminiApiError, GeminiChunkStream, GeminiClient,
    GeminiProvider, GeminiProviderOptions, GenerateContentParameters, GenerateContentResponse,
};
use klaus_agent_core::provider::retry::RetryOptions;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeString};
use klaus_agent_core::schemas::config::ResolvedConfig;
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::{CompactBoundaryMessage, Message, Usage};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::tools::builtins::read::create_read_tool;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{ProviderError, ProviderErrorKind};
use klaus_agent_core::types::provider::{
    ModelCapabilities, ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo, SystemPromptPart,
};
use serde_json::{json, Value};

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

const MODEL: &str = "gemini-runtime-reminder-fixture";
const PHASE: &str = "antigravity-runtime-reminders";

fn is_step(message: &Message) -> bool {
    matches!(message, Message::User(user) if user.is_meta == Some(true)
        && user.content.iter().any(|block| matches!(block, ContentBlock::Text(text)
            if text.phase.as_ref().is_some_and(|phase| phase == PHASE))))
}

fn steps(messages: &[Message]) -> Vec<Message> {
    messages
        .iter()
        .filter(|message| is_step(message))
        .cloned()
        .collect()
}

fn step_text(message: &Message) -> RuntimeString {
    let Message::User(user) = message else {
        panic!("expected a meta user step")
    };
    let ContentBlock::Text(text) = &user.content[0] else {
        panic!("expected text")
    };
    text.text.clone()
}

fn config(source: &str, countdown: bool) -> Value {
    json!({
        "model": MODEL,
        "modelSource": {"source":source,"selection":format!("{source}:{MODEL}")},
        "prewarm":false,"autoMemory":false,"autoDream":false,"memoryRecall":false,
        "memory":{"enabled":false},"checkpointing":false,"speculativeCompaction":false,
        "earlyMicrocompact":false,"idleMicrocompact":false,"compactionSelfCheck":false,
        "loopDetection":false,"autoCompaction":true,"contextWindow":200000,
        "maxRetries":1,"unboundedNetworkRetries":false,
        "totalTokensReminder":if countdown { "countdown" } else { "off" },
        "totalTokensReminderAfterUserTurn":true,
        "approval_policy":"never","sandbox_mode":"danger-full-access"
    })
}

#[derive(Clone, Copy)]
enum Reply {
    Read,
    Retry,
    Text,
}

struct FixtureProvider {
    info: ProviderInfo,
    path: String,
    replies: Mutex<VecDeque<Reply>>,
    requests: Mutex<Vec<ModelRequest>>,
}

impl FixtureProvider {
    fn new(path: String, replies: Vec<Reply>) -> Arc<Self> {
        Arc::new(Self {
            info: ProviderInfo {
                name: "gemini".into(),
                default_model: MODEL.into(),
                base_url: None,
                env_key: None,
            },
            path,
            replies: Mutex::new(replies.into()),
            requests: Mutex::new(Vec::new()),
        })
    }
}

impl ModelProvider for FixtureProvider {
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
            context_window: 200000,
            max_output_tokens: 8192,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }
    fn model_metadata(&self, _: &RuntimeString) -> ModelCapabilities {
        ModelCapabilities {
            context_window: Some(200000),
            max_output_tokens: Some(8192),
            tools: Some(true),
            ..Default::default()
        }
    }
    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        let mut requests = self.requests.lock().unwrap();
        let call = requests.len();
        requests.push(request);
        drop(requests);
        let reply = self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("scripted request");
        if matches!(reply, Reply::Retry) {
            return Box::pin(futures::stream::iter(vec![ModelStreamEvent::Error {
                error: ProviderError {
                    kind: ProviderErrorKind::Network,
                    message: "temporary fixture connection failure".into(),
                    status: None,
                    code: None,
                    error_type: None,
                    media_origins: None,
                    media_repair_complete: false,
                    credential_recovery: None,
                    retry_after_ms: Some(1),
                    request_id: None,
                },
            }]));
        }
        let mut events = vec![ModelStreamEvent::MessageStart {
            model: MODEL.into(),
        }];
        let stop_reason = if matches!(reply, Reply::Read) {
            let id: RuntimeString = format!("fixture-read-{call}").into();
            events.extend([
                ModelStreamEvent::ToolUseStart {
                    id: id.clone(),
                    name: "read".into(),
                    thought_signature: None,
                    tool_kind: None,
                    input_format: None,
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: id.clone(),
                    partial_json: json!({"path":self.path}).to_string().into(),
                },
                ModelStreamEvent::ToolUseStop { id },
            ]);
            ModelStopReason::ToolUse
        } else {
            events.push(ModelStreamEvent::TextDelta {
                text: "done".into(),
            });
            ModelStopReason::EndTurn
        };
        events.push(ModelStreamEvent::MessageStop {
            stop_reason,
            usage: Usage {
                input_tokens: 10000 + call as u64 * 1000,
                output_tokens: 20,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
                total_tokens: None,
            },
        });
        Box::pin(futures::stream::iter(events))
    }
}

#[derive(Clone, Default)]
struct SummarySpy {
    inputs: Arc<Mutex<Vec<Vec<Message>>>>,
}

#[async_trait::async_trait]
impl Compactor for SummarySpy {
    fn rebind_model(
        &self,
        _: Arc<dyn ModelProvider>,
        _: &ResolvedConfig,
    ) -> Result<Arc<dyn Compactor>, String> {
        // The spy has no transport; rebinding preserves the observed inputs.
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
        messages: &[Message],
        _: &ResolvedConfig,
        _: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.inputs.lock().unwrap().push(messages.to_vec());
        CompactionResult {
            messages: vec![Message::CompactBoundary(CompactBoundaryMessage {
                uuid: "reminder-fixture-checkpoint".into(),
                summary: "The fixture read completed.".into(),
                native_compaction: None,
            })],
            freed_tokens: 1000,
            compacted: true,
            reason: None,
            diagnostics: Vec::new(),
        }
    }
}

fn fixture_agent(
    root: &std::path::Path,
    source: &str,
    countdown: bool,
    replies: Vec<Reply>,
    reminder: Option<Arc<Mutex<Option<&'static str>>>>,
) -> (Agent, Arc<FixtureProvider>, Arc<SummarySpy>) {
    let path = root.join("fixture.txt");
    std::fs::write(&path, "fixture content\n").unwrap();
    let provider = FixtureProvider::new(path.to_string_lossy().into_owned(), replies);
    let summary = Arc::new(SummarySpy::default());
    let mut options =
        agent_options(RuntimeProcessContext::initialize(process_supervisor()).unwrap());
    options.provider = provider.clone();
    options.config = Some(config(source, countdown));
    options.tools = Some(vec![create_read_tool()]);
    options.include_default_agents = Some(false);
    options.compactor = Some(summary.clone());
    if let Some(reminder) = reminder {
        options.system_prompt_provider = Some(Arc::new(move || {
            let current = *reminder.lock().unwrap();
            Box::pin(async move {
                let mut parts = vec![SystemPromptPart {
                    name: "base".into(),
                    text: "Stable fixture instructions.".into(),
                    cacheable: Some(true),
                    cache_scope: None,
                }];
                if let Some(text) = current {
                    parts.push(SystemPromptPart {
                        name: "edited-files".into(),
                        text: text.into(),
                        cacheable: Some(false),
                        cache_scope: None,
                    });
                }
                parts
            })
        }));
    }
    (Agent::new(options), provider, summary)
}

fn session_options(root: &std::path::Path) -> StartSessionOptions {
    let root = root.to_string_lossy().into_owned();
    StartSessionOptions {
        cwd: Some(root.clone().into()),
        user_config_dir: Some(root.into()),
        ..Default::default()
    }
}

async fn run(session: &Arc<Session>, text: &str) {
    let result = session
        .run(Input::Text(text.into()), TurnOptions::default())
        .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    assert_eq!(result.final_text, "done");
}

fn select(session: &Arc<Session>, provider: Arc<FixtureProvider>, source: &str, countdown: bool) {
    session
        .reconfigure_inference(
            provider,
            resolve_config(config(source, countdown)).unwrap(),
            None,
            None,
            session.model_revision(),
        )
        .unwrap();
}

#[tokio::test]
async fn tool_rounds_and_network_retry_preserve_system_and_prior_message_prefixes() {
    let root = tempfile::tempdir().unwrap();
    let (agent, provider, _) = fixture_agent(
        root.path(),
        "antigravity",
        true,
        vec![Reply::Read, Reply::Retry, Reply::Read, Reply::Text],
        None,
    );
    let session = agent.start_session(session_options(root.path()));
    run(&session, "Read the fixture twice.").await;
    let requests = provider.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 4);
    let counts: Vec<_> = requests
        .iter()
        .map(|request| steps(&request.messages).len())
        .collect();
    assert_eq!(counts, vec![1, 2, 2, 3]);
    for request in &requests[1..] {
        assert_eq!(
            request.system, requests[0].system,
            "runtime updates must not rewrite system"
        );
        assert!(!request
            .system
            .iter()
            .any(|part| part.name == "total-tokens"));
        assert!(request.messages.starts_with(&requests[0].messages));
    }
    assert_eq!(
        requests[1].messages, requests[2].messages,
        "retry must reuse the exact admitted context"
    );
    assert_eq!(
        steps(&session.history()).len(),
        3,
        "only admitted transitions are persisted"
    );
    drop(requests);
    session.dispose().await;
}

#[tokio::test]
async fn persisted_steps_survive_resume_and_fork_clear_once_and_stay_private_to_the_source() {
    let root = tempfile::tempdir().unwrap();
    let reminder = Arc::new(Mutex::new(Some("The fixture was edited externally.")));
    let (agent, provider, summary) = fixture_agent(
        root.path(),
        "antigravity",
        false,
        vec![
            Reply::Read,
            Reply::Text,
            Reply::Text,
            Reply::Text,
            Reply::Text,
            Reply::Text,
            Reply::Text,
            Reply::Text,
        ],
        Some(reminder.clone()),
    );
    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.path().to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let session = agent
        .open_session(store.clone(), session_options(root.path()))
        .await
        .unwrap();
    run(&session, "Read the changed fixture.").await;
    assert_eq!(steps(&session.history()).len(), 1);
    let first_step = steps(&session.history())[0].clone();
    session.flush_rollout().await.unwrap();
    let id = session.id.try_to_utf8().unwrap();
    session.dispose().await;
    let resumed = agent
        .resume_session(store.clone(), &id, session_options(root.path()))
        .await
        .unwrap();
    assert_eq!(steps(&resumed.history()), vec![first_step.clone()]);
    run(&resumed, "Continue with the same reminder.").await;
    assert_eq!(
        steps(&resumed.history()),
        vec![first_step.clone()],
        "unchanged follow-up must not add a step"
    );
    *reminder.lock().unwrap() = None;
    run(&resumed, "The external edit is resolved.").await;
    let cleared = steps(&resumed.history());
    assert_eq!(cleared.len(), 2);
    assert!(step_text(&cleared[1]).contains("No runtime reminder entries are currently active."));
    select(&resumed, provider.clone(), "gemini", false);
    run(&resumed, "Continue through the regular Gemini source.").await;
    assert!(steps(&provider.requests.lock().unwrap().last().unwrap().messages).is_empty());
    assert_eq!(
        steps(&resumed.history()),
        cleared,
        "source projection must not delete canonical steps"
    );
    select(&resumed, provider.clone(), "antigravity", false);
    run(&resumed, "Return to the original source.").await;
    assert_eq!(steps(&resumed.history()), cleared);
    resumed.rollback(1).await.unwrap();
    assert_eq!(
        steps(&resumed.history()),
        cleared,
        "meta steps do not count as user turns"
    );
    resumed.flush_rollout().await.unwrap();
    let forked = agent
        .fork_session(store.clone(), &id, session_options(root.path()))
        .await
        .unwrap();
    assert_eq!(steps(&forked.history()), cleared);
    forked.dispose().await;
    *reminder.lock().unwrap() = Some("A fresh external edit needs review.");
    run(&resumed, "Review the new edit.").await;
    assert_eq!(steps(&resumed.history()).len(), 3);
    resumed.compact(None).await.unwrap();
    let inputs = summary.inputs.lock().unwrap().clone();
    assert!(!inputs.is_empty());
    assert!(
        inputs.iter().all(|messages| steps(messages).is_empty()),
        "obsolete runtime instructions must not enter a summary"
    );
    drop(inputs);
    assert!(
        steps(&resumed.history()).is_empty(),
        "compaction retires old step identities"
    );
    run(&resumed, "Continue after compaction.").await;
    assert_eq!(
        steps(&resumed.history()).len(),
        1,
        "current reminder is restored at the new tail"
    );
    resumed.dispose().await;
}

#[tokio::test]
async fn ordinary_gemini_keeps_existing_system_reminder_placement() {
    let root = tempfile::tempdir().unwrap();
    let reminder = Arc::new(Mutex::new(Some(
        "Keep this ordinary source reminder in system.",
    )));
    let (agent, provider, _) = fixture_agent(
        root.path(),
        "gemini",
        false,
        vec![Reply::Read, Reply::Text],
        Some(reminder),
    );
    let session = agent.start_session(session_options(root.path()));
    run(&session, "Read the fixture.").await;
    let requests = provider.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    assert!(requests
        .iter()
        .all(|request| steps(&request.messages).is_empty()));
    assert!(requests.iter().all(|request| request
        .system
        .iter()
        .any(|part| part.name == "edited-files")));
    assert!(steps(&session.history()).is_empty());
    drop(requests);
    session.dispose().await;
}

struct WireClient {
    path: String,
    replies: Mutex<VecDeque<Reply>>,
    requests: Mutex<Vec<Value>>,
}

#[async_trait::async_trait]
impl GeminiClient for WireClient {
    async fn generate_content_stream(
        &self,
        params: GenerateContentParameters,
        _: Option<klaus_agent_core::types::provider::AuthHeaders>,
        _: Vec<(String, String)>,
        _: AbortSignal,
    ) -> Result<GeminiChunkStream, GeminiApiError> {
        let wire = generate_content_request(&params);
        let wire: Value =
            serde_json::from_str(&runtime_json::stringify(&wire).try_to_utf8().unwrap()).unwrap();
        let mut requests = self.requests.lock().unwrap();
        let index = requests.len();
        requests.push(wire);
        drop(requests);
        let reply = self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("scripted Gemini request");
        if matches!(reply, Reply::Retry) {
            return Err(GeminiApiError {
                status: Some(503),
                message: "temporary fixture unavailable".into(),
                headers: None,
            });
        }
        let part = if matches!(reply, Reply::Read) {
            json!({"functionCall":{"id":format!("wire-read-{index}"),"name":"read","args":{"path":self.path}}})
        } else {
            json!({"text":"done"})
        };
        let value = json!({
            "modelVersion":MODEL,
            "candidates":[{"content":{"role":"model","parts":[part]},"finishReason":"STOP"}],
            "usageMetadata":{"promptTokenCount":10000+index*100,"candidatesTokenCount":20}
        });
        let response: GenerateContentResponse =
            runtime_codec::decode_text(&RuntimeString::from(value.to_string())).unwrap();
        Ok(Box::pin(futures::stream::iter(vec![Ok(response)])))
    }
}

#[tokio::test]
async fn real_gemini_encoding_keeps_prior_contents_and_system_across_updates_clear_and_retry() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("wire.txt");
    std::fs::write(&path, "wire fixture\n").unwrap();
    let reminder = Arc::new(Mutex::new(Some("The first external edit is active.")));
    let client = Arc::new(WireClient {
        path: path.to_string_lossy().into_owned(),
        replies: Mutex::new(
            vec![
                Reply::Read,
                Reply::Retry,
                Reply::Text,
                Reply::Text,
                Reply::Text,
                Reply::Text,
            ]
            .into(),
        ),
        requests: Mutex::new(Vec::new()),
    });
    let provider: Arc<dyn ModelProvider> = Arc::new(GeminiProvider::new(GeminiProviderOptions {
        default_model: Some(MODEL.into()),
        client: Some(client.clone()),
        capabilities: Some(ModelCapabilities {
            context_window: Some(200000),
            max_output_tokens: Some(8192),
            tools: Some(true),
            ..Default::default()
        }),
        retry: Some(RetryOptions {
            max_attempts: Some(2),
            base_delay_ms: Some(1),
            max_delay_ms: Some(1),
            jitter: Some(0.0),
            congestion_hold_ms: Some(0),
            unbounded_network_retries: Some(false),
            ..Default::default()
        }),
        adapter_policy: GeminiAdapterPolicy {
            replay_signed_parts: true,
            scope_signatures_to_model: true,
            honor_tools_capability: true,
            model_tool_budget: true,
        },
        ..Default::default()
    }));
    let mut options =
        agent_options(RuntimeProcessContext::initialize(process_supervisor()).unwrap());
    options.provider = provider;
    options.config = Some(config("antigravity", false));
    options.tools = Some(vec![create_read_tool()]);
    options.include_default_agents = Some(false);
    options.system_prompt_provider = Some(Arc::new({
        let reminder = reminder.clone();
        move || {
            let current = *reminder.lock().unwrap();
            Box::pin(async move {
                let mut parts = vec![SystemPromptPart {
                    name: "base".into(),
                    text: "Stable wire instructions.".into(),
                    cacheable: Some(true),
                    cache_scope: None,
                }];
                if let Some(text) = current {
                    parts.push(SystemPromptPart {
                        name: "edited-files".into(),
                        text: text.into(),
                        cacheable: Some(false),
                        cache_scope: None,
                    });
                }
                parts
            })
        }
    }));
    let session = Agent::new(options).start_session(session_options(root.path()));
    run(&session, "Read the fixture.").await;
    *reminder.lock().unwrap() = Some("The second external edit is active.");
    run(&session, "Apply the updated runtime context.").await;
    *reminder.lock().unwrap() = None;
    run(&session, "The runtime reminder is cleared.").await;
    run(&session, "Continue without a new reminder.").await;
    {
        let wires = client.requests.lock().unwrap();
        assert_eq!(wires.len(), 6);
        let counts: Vec<_> = wires
            .iter()
            .map(|wire| {
                wire["contents"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .flat_map(|content| content["parts"].as_array().unwrap())
                    .filter(|part| {
                        part["text"].as_str().is_some_and(|text| {
                            text.starts_with("<system-reminder>\nRuntime reminder update.")
                        })
                    })
                    .count()
            })
            .collect();
        assert_eq!(counts, vec![1, 1, 1, 2, 3, 3]);
        assert_eq!(
            wires[1], wires[2],
            "transport retry must send byte-equivalent JSON context"
        );
        for pair in wires.windows(2) {
            assert_eq!(pair[0]["systemInstruction"], pair[1]["systemInstruction"]);
            assert!(
                pair[1]["contents"]
                    .as_array()
                    .unwrap()
                    .starts_with(pair[0]["contents"].as_array().unwrap()),
                "historical Gemini Contents must remain an exact prefix"
            );
        }
        let system = wires[0]["systemInstruction"].to_string();
        assert!(!system.contains("external edit"));
        assert!(!system.contains("Runtime reminder update"));
    }
    assert_eq!(steps(&session.history()).len(), 3);
    session.dispose().await;
}
