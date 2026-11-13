//! Defect: side tasks inherit the parent's projected history, which can carry native
//! checkpoints only the originating source can decrypt. Before the first request on a
//! different source the engine must restore portable history from the rollout that
//! archives the checkpoint sources, but the ancestry walk followed fork links only.
//! Side tasks link to their origin through the parent thread, so their first turn
//! failed before any model request with a portable-history error.
//! Work item: 20261113 side task lineage resolves native checkpoint sources.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeString};
use klaus_agent_core::schemas::message::{Message, NativeCompactionCheckpoint, Usage};
use klaus_agent_core::schemas::rollout::{RolloutItem, SessionRole};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::projection::project_for_prompt;
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{ProviderError, ProviderErrorKind};
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo, RemoteCompactionResult, RemoteCompactionSupport,
};
use serde_json::json;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

const MODEL: &str = "shared-model-id";
const LATEST: &str = "LATEST: continue the inherited evidence task.";
const SUMMARY: &str = "Recovered task: preserve the original requirement and TOOL_EVIDENCE.";
const LARGE_WINDOW: u64 = 200_000;

struct Calls {
    source_streams: AtomicUsize,
    source_compactions: AtomicUsize,
    source_unavailable: AtomicBool,
    target: Mutex<Vec<ModelRequest>>,
}

impl Calls {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            source_streams: AtomicUsize::new(0),
            source_compactions: AtomicUsize::new(0),
            source_unavailable: AtomicBool::new(false),
            target: Mutex::new(Vec::new()),
        })
    }
}

struct Provider {
    info: ProviderInfo,
    source: bool,
    calls: Arc<Calls>,
}

fn error(kind: ProviderErrorKind, message: &str) -> ProviderError {
    ProviderError {
        kind,
        message: message.into(),
        status: None,
        code: None,
        error_type: None,
        request_id: None,
        media_origins: None,
        media_repair_complete: false,
        credential_recovery: None,
        retry_after_ms: None,
    }
}

fn stop() -> ModelStreamEvent {
    ModelStreamEvent::MessageStop {
        stop_reason: ModelStopReason::EndTurn,
        usage: Usage {
            input_tokens: 100,
            output_tokens: 20,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
            total_tokens: None,
        },
    }
}

#[async_trait]
impl ModelProvider for Provider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: true,
            tools: true,
            thinking: true,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: LARGE_WINDOW,
            max_output_tokens: 1_000,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
            supports_image_detail_original: false,
        }
    }

    fn remote_compaction_support(&self, _: &RuntimeString) -> RemoteCompactionSupport {
        if self.source {
            RemoteCompactionSupport::ResponsesV2
        } else {
            RemoteCompactionSupport::Unsupported
        }
    }

    async fn native_checkpoint_compatibility(
        &self,
        checkpoint: &NativeCompactionCheckpoint,
        _: &RuntimeString,
        _: &AbortSignal,
    ) -> Result<bool, ProviderError> {
        Ok(self.source && checkpoint.source == "checkpoint-source")
    }

    async fn compact_remote(
        &self,
        request: ModelRequest,
        _: AbortSignal,
    ) -> Result<RemoteCompactionResult, ProviderError> {
        assert!(
            self.source,
            "Recovery must never request another native checkpoint"
        );
        self.calls.source_compactions.fetch_add(1, Ordering::SeqCst);
        if self.calls.source_unavailable.load(Ordering::SeqCst) {
            return Err(error(
                ProviderErrorKind::Quota,
                "The old source is unavailable",
            ));
        }
        assert!(encoded(&request.messages).contains("ORIGINAL_REQUIREMENT"));
        Ok(RemoteCompactionResult {
            checkpoint: NativeCompactionCheckpoint {
                source: "checkpoint-source".into(),
                auth_scope: "old-auth-scope".into(),
                model: MODEL.into(),
                comp_hash: None,
                response_id: "old-response-id".into(),
                encrypted_content: "OPAQUE_CHECKPOINT_ONLY_OLD_SOURCE_CAN_READ".into(),
                retained_input: Vec::new(),
                retained_input_metadata: Vec::new(),
            },
            usage: None,
        })
    }

    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        if self.source {
            self.calls.source_streams.fetch_add(1, Ordering::SeqCst);
            return Box::pin(futures::stream::iter([ModelStreamEvent::Error {
                error: error(ProviderErrorKind::Quota, "The old source is unavailable"),
            }]));
        }
        let mut requests = self.calls.target.lock().unwrap();
        requests.push(request);
        Box::pin(futures::stream::iter([
            ModelStreamEvent::TextDelta {
                text: SUMMARY.into(),
            },
            stop(),
        ]))
    }
}

fn provider(source: bool, calls: &Arc<Calls>) -> Arc<dyn ModelProvider> {
    Arc::new(Provider {
        info: ProviderInfo {
            name: if source {
                "checkpoint-source"
            } else {
                "recovery-target"
            }
            .into(),
            base_url: None,
            env_key: None,
            default_model: MODEL.into(),
        },
        source,
        calls: calls.clone(),
    })
}

fn config(source: bool) -> serde_json::Value {
    let source = if source {
        "checkpoint-source"
    } else {
        "recovery-target"
    };
    json!({
        "model": MODEL, "contextWindow": LARGE_WINDOW, "maxOutputTokens": 1_000,
        "autoCompaction": true, "prewarm": false, "thinking": false,
        "loopDetection": false, "autoMemory": false, "autoDream": false,
        "memoryRecall": false, "maxRetries": 0, "compactionStreamMaxRetries": 0,
        "approval_policy": "never", "sandbox_mode": "danger-full-access",
        "compactPrompt": "Summarize this recovered task without dropping its requirement or tool evidence.",
        "modelSource": {"source": source, "modelId": MODEL, "selection": format!("{source}:{MODEL}")}
    })
}

fn message(value: serde_json::Value) -> Message {
    runtime_codec::decode_text(&value.to_string().into()).unwrap()
}

fn inherited_history() -> Vec<Message> {
    vec![
        message(
            json!({"type":"user","uuid":"original-user","content":[{"type":"text","text":"ORIGINAL_REQUIREMENT: preserve the exact evidence."}]}),
        ),
        message(
            json!({"type":"assistant","uuid":"tool-call-message","model":MODEL,"content":[
                {"type":"thinking","text":"Readable reasoning","encryptedContent":"OLD_ENCRYPTED_REASONING"},
                {"type":"tool_use","id":"recorded-read","name":"read","input":{"path":"evidence.txt"},"thoughtSignature":"OLD_TOOL_SIGNATURE"}
            ]}),
        ),
        message(
            json!({"type":"tool_result","uuid":"tool-result-message","toolUseId":"recorded-read","content":[{"type":"text","text":"TOOL_EVIDENCE: the durable result is 42."}],"isError":false}),
        ),
        message(
            json!({"type":"user","uuid":"second-user","content":[{"type":"text","text":"SECOND_REQUIREMENT: keep the recorded result in order."}]}),
        ),
        message(
            json!({"type":"assistant","uuid":"original-answer","model":MODEL,"content":[{"type":"text","text":"The original evidence is ready."}]}),
        ),
    ]
}

fn encoded(messages: &[Message]) -> String {
    runtime_codec::encode_text(&messages.to_vec())
        .unwrap()
        .try_to_utf8()
        .unwrap()
}

fn agent(
    process: &RuntimeProcessContext,
    root: &std::path::Path,
    source: bool,
    calls: &Arc<Calls>,
) -> Agent {
    let mut options = agent_options(process.clone());
    options.provider = provider(source, calls);
    options.config = Some(config(source));
    options.cwd = Some(root.to_string_lossy().into_owned());
    options.include_default_agents = Some(false);
    Agent::new(options)
}

fn assert_success(events: &[EngineEvent]) {
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
}

#[tokio::test]
async fn a_side_task_recovers_inherited_checkpoint_through_its_parent_lineage() {
    let dir = tempfile::tempdir().unwrap();
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let calls = Calls::new();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.path().join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let config_dir = dir.path().to_string_lossy().into_owned();

    // The origin session compacts on the checkpoint source and persists the archive.
    let origin = agent(&process, dir.path(), true, &calls)
        .open_session(
            store.clone(),
            StartSessionOptions {
                initial_history: Some(inherited_history()),
                user_config_dir: Some(config_dir.clone().into()),
                base_instructions: Some("Continue the durable evidence task.".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let outcome = origin.compact(None).await.unwrap();
    assert!(
        outcome.compacted,
        "The fixture must create a real native checkpoint"
    );
    origin.flush_rollout().await.unwrap();
    let origin_id = origin.id.try_to_utf8().unwrap();
    let origin_snapshot = store.read_thread(&origin_id).await.unwrap();
    assert!(
        origin_snapshot.raw_items.iter().any(
            |item| matches!(item, RolloutItem::Compacted(compacted) if compacted
                .replacement_history
                .as_ref()
                .is_some_and(|history| history
                    .iter()
                    .any(|message| matches!(message, Message::CompactBoundary(boundary)
                        if boundary.native_compaction.is_some()))))
        ),
        "The origin rollout must archive the native checkpoint"
    );
    origin.shutdown_and_wait().await.unwrap();
    calls.source_unavailable.store(true, Ordering::SeqCst);

    // A side task inherits the projected history: the checkpoint travels, the archive does not.
    let inherited = project_for_prompt(&origin_snapshot.raw_items);
    assert!(
        inherited.iter().any(
            |message| matches!(message, Message::CompactBoundary(boundary)
                if boundary.native_compaction.is_some())
        ),
        "The inherited history must carry the native checkpoint"
    );
    let side = agent(&process, dir.path(), false, &calls)
        .open_session(
            store.clone(),
            StartSessionOptions {
                initial_history: Some(inherited),
                session_role: Some(SessionRole::SideTask),
                parent_thread_id: Some(RuntimeString::from(origin_id.clone())),
                user_config_dir: Some(config_dir.clone().into()),
                base_instructions: Some("Continue the durable evidence task.".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    side.flush_rollout().await.unwrap();
    let side_id = side.id.try_to_utf8().unwrap();
    let side_snapshot = store.read_thread(&side_id).await.unwrap();
    assert_eq!(
        side_snapshot
            .meta
            .parent_thread_id
            .as_ref()
            .and_then(|id| id.try_to_utf8().ok()),
        Some(origin_id.clone()),
        "The side task must keep its parent lineage"
    );
    assert!(
        side_snapshot.meta.forked_from_id.is_none(),
        "The side task lineage is not a fork link"
    );
    assert!(
        !side_snapshot
            .raw_items
            .iter()
            .any(|item| matches!(item, RolloutItem::Compacted(_))),
        "The archive stays in the origin rollout"
    );

    // The first turn on the other source must recover through the parent lineage.
    let events = tokio::time::timeout(
        Duration::from_secs(20),
        side.run_streamed(Input::Text(LATEST.into()), TurnOptions::default())
            .events
            .collect::<Vec<EngineEvent>>(),
    )
    .await
    .expect("Recovery must settle within its bounded fixture");
    assert_success(&events);
    side.shutdown_and_wait().await.unwrap();

    let text = {
        let requests = calls.target.lock().unwrap();
        assert_eq!(
            requests.len(),
            1,
            "Exactly one recovered request may reach the target"
        );
        encoded(&requests[0].messages)
    };
    for forbidden in [
        "OPAQUE_CHECKPOINT_ONLY_OLD_SOURCE_CAN_READ",
        "OLD_ENCRYPTED_REASONING",
        "OLD_TOOL_SIGNATURE",
    ] {
        assert!(
            !text.contains(forbidden),
            "The target received source-owned opaque content: {forbidden}"
        );
    }
    for expected in [
        "ORIGINAL_REQUIREMENT",
        "TOOL_EVIDENCE",
        "SECOND_REQUIREMENT",
        LATEST,
    ] {
        assert!(
            text.contains(expected),
            "Recovered history lost {expected}: {text}"
        );
    }
    let original = text.find("ORIGINAL_REQUIREMENT").unwrap();
    let evidence = text.find("TOOL_EVIDENCE").unwrap();
    let second = text.find("SECOND_REQUIREMENT").unwrap();
    let latest = text.find(LATEST).unwrap();
    assert!(
        original < evidence && evidence < second && second < latest,
        "Recovered order changed: {text}"
    );
    assert_eq!(
        calls.source_streams.load(Ordering::SeqCst),
        0,
        "No request may return to the unavailable source"
    );
    assert_eq!(
        calls.source_compactions.load(Ordering::SeqCst),
        1,
        "Only fixture creation may compact on the old source"
    );
    process.shutdown().await.unwrap();
}
