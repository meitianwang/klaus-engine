//! Defect: publication could forward an incompatible checkpoint to a new model source.
//! Root cause: migration considered only smaller windows and retried arbitrary target providers.
//! Fix: 5dea2d9b; create and commit source-preserving context checkpoints.

use async_trait::async_trait;
use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{resolve_config, Input, Session, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{
    CompactBoundaryMessage, Message, NativeCompactionCheckpoint, Usage, UserMessage,
};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{ProviderError, ProviderErrorKind};
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo, RemoteCompactionResult, RemoteCompactionSupport,
};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

type Trace = Arc<Mutex<Vec<(String, &'static str, ModelRequest)>>>;

#[derive(Clone, Copy)]
pub(super) enum NativeReply {
    Success,
    Unavailable,
    Budget,
    Cancel,
}

struct Provider {
    info: ProviderInfo,
    model: &'static str,
    window: u64,
    hash: Option<&'static str>,
    remote: bool,
    eligible_fallback: bool,
    reply: NativeReply,
    native_network_failures: AtomicUsize,
    max_retries: u32,
    trace: Trace,
}

impl Provider {
    fn checkpoint(&self) -> NativeCompactionCheckpoint {
        NativeCompactionCheckpoint {
            source: "fixture-native".into(),
            auth_scope: self.info.name.clone(),
            model: self.model.into(),
            comp_hash: self.hash.map(Into::into),
            response_id: "fixture-compaction-response".into(),
            encrypted_content: "opaque-checkpoint".into(),
            retained_input: Vec::new(),
            retained_input_metadata: Vec::new(),
        }
    }
}

fn provider_error(kind: ProviderErrorKind, code: &str) -> ProviderError {
    ProviderError {
        kind,
        code: Some(code.into()),
        message: "Controlled migration service refusal".into(),
        status: Some(400),
        error_type: None,
        retry_after_ms: None,
        request_id: None,
        media_origins: None,
        media_repair_complete: false,
        credential_recovery: None,
    }
}

#[async_trait]
impl ModelProvider for Provider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }
    fn remote_compaction_support(&self, _: &RuntimeString) -> RemoteCompactionSupport {
        if self.remote {
            RemoteCompactionSupport::ResponsesV2
        } else {
            RemoteCompactionSupport::Unsupported
        }
    }
    fn compaction_compatibility_hash(&self, _: &RuntimeString) -> Option<RuntimeString> {
        self.hash.map(Into::into)
    }
    fn supports_compaction_model_fallback(&self) -> bool {
        self.eligible_fallback
    }
    fn compaction_stream_max_retries(&self) -> Option<u32> {
        Some(self.max_retries)
    }
    async fn native_checkpoint_compatibility(
        &self,
        checkpoint: &NativeCompactionCheckpoint,
        model: &RuntimeString,
        _: &AbortSignal,
    ) -> Result<bool, ProviderError> {
        Ok(self.remote && checkpoint.auth_scope == self.info.name && &checkpoint.model == model)
    }
    async fn compact_remote(
        &self,
        request: ModelRequest,
        signal: AbortSignal,
    ) -> Result<RemoteCompactionResult, ProviderError> {
        self.trace
            .lock()
            .unwrap()
            .push((self.info.name.try_to_utf8().unwrap(), "native", request));
        if self.native_network_failures.load(Ordering::SeqCst) > 0 {
            self.native_network_failures.fetch_sub(1, Ordering::SeqCst);
            return Err(provider_error(ProviderErrorKind::Network, "dns_error"));
        }
        match self.reply {
            NativeReply::Success => Ok(RemoteCompactionResult {
                checkpoint: self.checkpoint(),
                usage: None,
            }),
            NativeReply::Unavailable => Err(provider_error(
                ProviderErrorKind::InvalidRequest,
                "model_unavailable",
            )),
            NativeReply::Budget => Err(provider_error(
                ProviderErrorKind::Unknown,
                "session_budget_exceeded",
            )),
            NativeReply::Cancel => {
                signal.cancel();
                Err(provider_error(ProviderErrorKind::Network, "cancelled"))
            }
        }
    }
    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: true,
            tools: false,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: self.window,
            max_output_tokens: 2_000,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
            supports_image_detail_original: false,
        }
    }
    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        let summary = request.skip_cache_write == Some(true);
        self.trace.lock().unwrap().push((
            self.info.name.try_to_utf8().unwrap(),
            if summary { "local" } else { "normal" },
            request,
        ));
        Box::pin(futures::stream::iter([
            ModelStreamEvent::TextDelta {
                text: if summary {
                    "Preserved user requirements and progress."
                } else {
                    "Target model completed the task."
                }
                .into(),
            },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: Usage {
                    input_tokens: 100,
                    output_tokens: 20,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: None,
                },
            },
        ]))
    }
}

pub(super) struct Spec {
    pub(super) name: &'static str,
    pub(super) model: &'static str,
    pub(super) window: u64,
    pub(super) hash: Option<&'static str>,
    pub(super) remote: bool,
    pub(super) eligible: bool,
    pub(super) reply: NativeReply,
    pub(super) native_network_failures: usize,
    pub(super) max_retries: u32,
}

impl Spec {
    fn provider(&self, trace: &Trace) -> Arc<Provider> {
        Arc::new(Provider {
            info: ProviderInfo {
                name: self.name.into(),
                default_model: self.model.into(),
                base_url: None,
                env_key: None,
            },
            model: self.model,
            window: self.window,
            hash: self.hash,
            remote: self.remote,
            eligible_fallback: self.eligible,
            reply: self.reply,
            native_network_failures: AtomicUsize::new(self.native_network_failures),
            max_retries: self.max_retries,
            trace: trace.clone(),
        })
    }
}

pub(super) fn spec(name: &'static str, model: &'static str, hash: Option<&'static str>) -> Spec {
    Spec {
        name,
        model,
        window: 100_000,
        hash,
        remote: false,
        eligible: false,
        reply: NativeReply::Success,
        native_network_failures: 0,
        max_retries: 0,
    }
}

fn user() -> Message {
    Message::User(UserMessage {
        uuid: "original-requirement".into(),
        content: vec![ContentBlock::Text(TextBlock {
            text: "Keep the original requirement.".into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })],
        is_meta: None,
        question_answer: None,
    })
}

pub(super) struct Fixture {
    session: Arc<Session>,
    trace: Trace,
    process: RuntimeProcessContext,
    dir: std::path::PathBuf,
}

impl Fixture {
    pub(super) fn new(source: Spec, native_history: bool) -> Self {
        let process = RuntimeProcessContext::initialize(
            super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
        )
        .unwrap();
        let dir = std::env::temp_dir().join(format!(
            "klaus-compaction-migration-contract-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let trace = Trace::default();
        let provider = source.provider(&trace);
        let mut history = vec![user()];
        if native_history {
            history.push(Message::CompactBoundary(CompactBoundaryMessage {
                uuid: "previous-native-checkpoint".into(),
                summary: RuntimeString::default(),
                native_compaction: Some(provider.checkpoint()),
            }));
        }
        let mut options =
            super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
        options.provider = provider;
        options.cwd = Some(dir.to_string_lossy().into_owned());
        options.tools = Some(Vec::new());
        options.config = Some(config(&source));
        let session = Agent::new(options).start_session(StartSessionOptions {
            user_config_dir: Some(dir.to_string_lossy().into_owned().into()),
            base_instructions: Some("Continue the user's task.".into()),
            initial_history: Some(history),
            ..Default::default()
        });
        Self {
            session,
            trace,
            process,
            dir,
        }
    }
    pub(super) fn select(&self, target: Spec) {
        self.session
            .reconfigure_inference(
                target.provider(&self.trace),
                resolve_config(config(&target)).unwrap(),
                None,
                None,
                self.session.model_revision(),
            )
            .unwrap();
    }
    pub(super) async fn run(&self) -> Vec<EngineEvent> {
        self.session
            .run_streamed(
                Input::Text("Finish the task.".into()),
                TurnOptions::default(),
            )
            .events
            .collect()
            .await
    }
    pub(super) fn routes(&self) -> Vec<(String, &'static str)> {
        self.trace
            .lock()
            .unwrap()
            .iter()
            .map(|(name, kind, _)| (name.clone(), *kind))
            .collect()
    }
    pub(super) async fn close(self) {
        self.session.shutdown_and_wait().await.unwrap();
        self.process.shutdown().await.unwrap();
        std::fs::remove_dir_all(self.dir).unwrap();
    }
}

fn config(spec: &Spec) -> serde_json::Value {
    json!({ "model":spec.model,"contextWindow":spec.window,"prewarm":false,"thinking":false,
        "autoMemory":false,"autoDream":false,"memoryRecall":false,"loopDetection":false,
        "compactionStreamMaxRetries":0,"approval_policy":"never","sandbox_mode":"danger-full-access",
        "modelSource":{"source":spec.name,"modelId":spec.model,"selection":format!("{}:{}",spec.name,spec.model)} })
}

pub(super) fn succeeded(events: &[EngineEvent]) -> bool {
    !events
        .iter()
        .any(|event| matches!(event, EngineEvent::TurnFailed { .. }))
        && events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnCompleted { .. }))
}

#[tokio::test]
async fn compatibility_hash_change_compacts_fitting_history_without_a_downshift() {
    let fixture = Fixture::new(spec("old", "same-model", Some("v1")), false);
    fixture.select(spec("new", "same-model", Some("v2")));
    let events = fixture.run().await;
    assert!(succeeded(&events), "{events:?}");
    assert_eq!(
        fixture.routes(),
        [("old".into(), "local"), ("new".into(), "normal")]
    );
    fixture.close().await;
}

#[tokio::test]
async fn native_checkpoint_is_localized_by_its_owner_before_a_wider_unsupported_target() {
    let fixture = Fixture::new(
        Spec {
            remote: true,
            ..spec("old", "old-model", Some("v1"))
        },
        true,
    );
    fixture.select(Spec {
        window: 200_000,
        ..spec("new", "new-model", None)
    });
    let events = fixture.run().await;
    assert!(succeeded(&events), "{events:?}");
    assert_eq!(
        fixture.routes(),
        [("old".into(), "local"), ("new".into(), "normal")]
    );
    {
        let trace = fixture.trace.lock().unwrap();
        assert!(trace[0].2.messages.iter().any(|message| matches!(message, Message::CompactBoundary(boundary) if boundary.native_compaction.is_some())));
        assert!(!trace[1].2.messages.iter().any(|message| matches!(message, Message::CompactBoundary(boundary) if boundary.native_compaction.is_some())));
    }
    fixture.close().await;
}

#[tokio::test]
async fn failed_native_migration_retries_only_an_explicitly_eligible_target() {
    let fixture = Fixture::new(
        Spec {
            remote: true,
            reply: NativeReply::Unavailable,
            ..spec("old", "old-model", Some("v1"))
        },
        false,
    );
    fixture.select(Spec {
        remote: true,
        eligible: true,
        ..spec("new", "new-model", Some("v2"))
    });
    let events = fixture.run().await;
    assert!(succeeded(&events), "{events:?}");
    assert_eq!(
        fixture.routes(),
        [
            ("old".into(), "native"),
            ("new".into(), "native"),
            ("new".into(), "normal")
        ]
    );
    fixture.close().await;
}

#[tokio::test]
async fn unsupported_fallback_target_never_receives_the_failed_migration_history() {
    let fixture = Fixture::new(
        Spec {
            remote: true,
            reply: NativeReply::Unavailable,
            ..spec("old", "old-model", Some("v1"))
        },
        false,
    );
    fixture.select(Spec {
        remote: true,
        ..spec("new", "new-model", Some("v2"))
    });
    let events = fixture.run().await;
    assert!(!succeeded(&events));
    assert_eq!(fixture.routes(), [("old".into(), "native")]);
    assert!(fixture.session.history().iter().any(
        |message| matches!(message, Message::User(user) if user.uuid == "original-requirement")
    ));
    fixture.close().await;
}

#[tokio::test]
async fn session_budget_refusal_cannot_rotate_to_another_model() {
    let fixture = Fixture::new(
        Spec {
            remote: true,
            reply: NativeReply::Budget,
            ..spec("old", "old-model", Some("v1"))
        },
        false,
    );
    fixture.select(Spec {
        remote: true,
        eligible: true,
        ..spec("new", "new-model", Some("v2"))
    });
    let events = fixture.run().await;
    assert!(!succeeded(&events));
    assert_eq!(fixture.routes(), [("old".into(), "native")]);
    fixture.close().await;
}

#[tokio::test]
async fn cancelled_native_migration_preserves_history_without_target_fallback() {
    let fixture = Fixture::new(
        Spec {
            remote: true,
            reply: NativeReply::Cancel,
            ..spec("old", "old-model", Some("v1"))
        },
        false,
    );
    fixture.select(Spec {
        remote: true,
        eligible: true,
        ..spec("new", "new-model", Some("v2"))
    });
    let events = fixture.run().await;
    assert_eq!(fixture.routes(), [("old".into(), "native")]);
    assert!(events
        .iter()
        .any(|event| matches!(event, EngineEvent::TurnCompleted { .. })));
    assert!(!fixture
        .session
        .history()
        .iter()
        .any(|message| matches!(message, Message::CompactBoundary(_))));
    assert!(fixture.session.history().iter().any(
        |message| matches!(message, Message::User(user) if user.uuid == "original-requirement")
    ));
    fixture.close().await;
}
