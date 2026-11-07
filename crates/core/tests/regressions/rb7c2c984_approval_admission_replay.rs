//! Defect: every admitted message appended the entire bounded approval history.
//! Root cause: ordinary capture used the logical checkpoint writer.
//! Native work item: b7c2c984-235c-4169-a129-beba4ca4ef77.

use std::sync::Arc;

use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::approval_capture::{
    ReviewParentHistory, APPROVAL_MANUAL_ACTION_EVENT, APPROVAL_MESSAGE_ADMITTED_EVENT,
    APPROVAL_PARENT_HISTORY_EVENT,
};
use klaus_agent_core::engine::approval_context::model::ResponseItem;
use klaus_agent_core::engine::session::{EnqueueOptions, Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeString};
use klaus_agent_core::schemas::message::Usage;
use klaus_agent_core::schemas::rollout::RolloutItem;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::approval_review::{
    ApprovalReviewAction, ApprovalReviewCommandSource, ApprovalReviewEvent, ApprovalReviewStatus,
};
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

struct TempDir(std::path::PathBuf);
impl TempDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "klaus-approval-incremental-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Provider {
    info: ProviderInfo,
}
impl ModelProvider for Provider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: false,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: 200_000,
            max_output_tokens: 8192,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn stream(&self, _: ModelRequest, _: AbortSignal) -> ModelStream {
        Box::pin(futures::stream::iter(vec![
            ModelStreamEvent::MessageStart {
                model: "incremental-fixture".into(),
            },
            ModelStreamEvent::TextDelta {
                text: format!("Original assistant evidence. {}", "a".repeat(1800)).into(),
            },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: Usage {
                    input_tokens: 100,
                    output_tokens: 500,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: Some(600),
                },
            },
        ]))
    }
}

fn agent(process: RuntimeProcessContext, directory: &TempDir) -> Agent {
    let mut options = agent_options(process);
    options.cwd = Some(directory.0.to_string_lossy().into_owned());
    options.provider = Arc::new(Provider {
        info: ProviderInfo {
            name: "incremental-fixture".into(),
            base_url: None,
            env_key: None,
            default_model: "incremental-fixture".into(),
        },
    });
    options.include_default_agents = Some(false);
    options.config = Some(serde_json::json!({
        "model": "incremental-fixture",
        "approval_policy": "never",
        "sandbox_mode": "danger-full-access",
        "prewarm": false,
        "goal": false,
        "autoMemory": false,
        "agentMemory": false,
        "autoDream": false,
        "memoryRecall": false,
        "memory": {"enabled": false},
        "loopDetection": false
    }));
    Agent::new(options)
}

fn start_options(directory: &TempDir) -> StartSessionOptions {
    StartSessionOptions {
        user_config_dir: Some(
            directory
                .0
                .join("config")
                .to_string_lossy()
                .into_owned()
                .into(),
        ),
        ..Default::default()
    }
}

#[tokio::test]
async fn ordinary_turns_append_body_free_admissions_and_one_manual_action() {
    let directory = TempDir::new();
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(directory.0.join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let agent = agent(process.clone(), &directory);
    let session = agent
        .open_session(store.clone(), start_options(&directory))
        .await
        .unwrap();
    for index in 0..24 {
        let result = session
            .run(
                Input::Text(format!("Original user evidence {index}. {}", "u".repeat(1800)).into()),
                TurnOptions::default(),
            )
            .await;
        assert!(result.error.is_none(), "{:?}", result.error);
    }
    session
        .approve_approval_review_denial(ApprovalReviewEvent {
            id: "manual-review".into(),
            turn_id: "manual-turn".into(),
            target_item_id: Some("manual-call".into()),
            plugin_id: None,
            script_path: None,
            started_at_ms: 1,
            completed_at_ms: Some(2),
            status: ApprovalReviewStatus::Denied,
            risk_level: None,
            user_authorization: None,
            rationale: None,
            decision_source: None,
            model_context: None,
            review_reason: None,
            action: ApprovalReviewAction::Command {
                source: ApprovalReviewCommandSource::UnifiedExec,
                command: "git status --short".into(),
                cwd: directory.0.to_string_lossy().into_owned().into(),
            },
        })
        .await
        .unwrap();
    let id = session.id.try_to_utf8().unwrap();
    session.shutdown_and_wait().await.unwrap();
    let saved = store.resume(&id).await.unwrap();
    let mut admissions = 0;
    let mut manual_actions = 0;
    for item in &saved.raw_items {
        if let RolloutItem::Event(event) = item {
            assert_ne!(
                event.event, APPROVAL_PARENT_HISTORY_EVENT,
                "ordinary messages and manual approvals must not append full history"
            );
            if event.event == APPROVAL_MESSAGE_ADMITTED_EVENT {
                admissions += 1;
                let payload = event.payload.as_ref().unwrap();
                let encoded = runtime_json::stringify(payload).try_to_utf8().unwrap();
                assert!(
                    encoded.len() < 1024,
                    "admission grew to {} bytes",
                    encoded.len()
                );
                assert!(!encoded.contains("Original user evidence"));
                assert!(!encoded.contains("Original assistant evidence"));
            } else if event.event == APPROVAL_MANUAL_ACTION_EVENT {
                manual_actions += 1;
            }
        }
    }
    assert_eq!(admissions, 48);
    assert_eq!(manual_actions, 1);
    let reconstructed = ReviewParentHistory::from_rollout(&saved.raw_items).unwrap();
    let snapshot = reconstructed.snapshot();
    assert_eq!(snapshot.originals.len(), 48);
    assert!(
        snapshot.retained.has_missing_user_messages(),
        "bounded omissions remain explicit"
    );
    assert!(snapshot.items.iter().any(|item| matches!(&item.item,
        ResponseItem::Message { role, .. } if role == "developer")));
    for (order, original) in snapshot.originals.iter().enumerate() {
        assert_eq!(original.acceptance_order, order as u64);
        assert!(!original.turn_id.is_empty());
        let envelope = snapshot
            .items
            .iter()
            .find(|item| {
                item.id()
                    .is_some_and(|id| id.as_str() == original.message_id)
            })
            .unwrap();
        let source = envelope.metadata.as_ref().unwrap();
        assert_eq!(source.user_input_order, Some(original.acceptance_order));
        assert_eq!(envelope.turn_id(), Some(original.turn_id.as_str()));
        let raw = saved
            .raw_items
            .iter()
            .find_map(|item| match item {
                RolloutItem::Message { message } => match message {
                    klaus_agent_core::schemas::message::Message::User(user)
                        if user.uuid.try_to_utf8().unwrap() == original.message_id =>
                    {
                        Some(&user.content)
                    }
                    klaus_agent_core::schemas::message::Message::Assistant(assistant)
                        if assistant.uuid.try_to_utf8().unwrap() == original.message_id =>
                    {
                        Some(&assistant.content)
                    }
                    _ => None,
                },
                _ => None,
            })
            .unwrap();
        let klaus_agent_core::schemas::content::ContentBlock::Text(text) = &raw[0] else {
            panic!("fixture original is text");
        };
        let actual = serde_json::to_value(&envelope.item).unwrap();
        assert_eq!(
            actual["content"][0]["text"],
            text.text.try_to_utf8().unwrap()
        );
    }
    // Simulate a checkpoint between admission and the raw writer. The source
    // prefix may already have been skipped by the continuation loader.
    let last_original = saved
        .raw_items
        .iter()
        .rev()
        .find_map(|item| match item {
            RolloutItem::Message {
                message: klaus_agent_core::schemas::message::Message::Assistant(_),
            } => Some(item.clone()),
            _ => None,
        })
        .unwrap();
    let compacted = RolloutItem::Compacted(klaus_agent_core::schemas::rollout::CompactedItem {
        summary: "Checkpoint".into(),
        replaced_range: (0, 48),
        replacement_history: Some(Vec::new()),
        source_message_ids: Some(Vec::new()),
        approval_parent_history: Some(reconstructed.checkpoint().to_runtime_value().unwrap()),
    });
    assert_eq!(
        ReviewParentHistory::from_rollout(&[compacted, last_original])
            .unwrap()
            .checkpoint(),
        reconstructed.checkpoint(),
        "a raw suffix already captured by the checkpoint must preserve exact evidence"
    );
    let checkpoint =
        runtime_json::stringify(&reconstructed.checkpoint().to_runtime_value().unwrap());
    let resumed = agent
        .resume_session(store.clone(), &id, start_options(&directory))
        .await
        .unwrap();
    resumed.shutdown_and_wait().await.unwrap();
    let saved_again = store.resume(&id).await.unwrap();
    assert_eq!(
        runtime_json::stringify(
            &ReviewParentHistory::from_rollout(&saved_again.raw_items)
                .unwrap()
                .checkpoint()
                .to_runtime_value()
                .unwrap()
        ),
        checkpoint
    );
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn admitted_undelivered_human_input_replays_from_its_typed_queue_original() {
    let directory = TempDir::new();
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(directory.0.join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let agent = agent(process.clone(), &directory);
    let session = agent
        .open_session(store.clone(), start_options(&directory))
        .await
        .unwrap();
    session
        .enqueue(
            Input::Text("Inspect only; never publish.".into()),
            EnqueueOptions::default(),
        )
        .unwrap();
    session.await_queue_durability().await.unwrap();
    let uuid = session.queue_snapshot().items[0]
        .message
        .uuid
        .clone()
        .unwrap();
    let id = session.id.try_to_utf8().unwrap();
    session.shutdown_and_wait().await.unwrap();
    let saved = store.resume(&id).await.unwrap();
    assert!(!saved.raw_items.iter().any(|item| matches!(item,
        RolloutItem::Event(event) if event.event == APPROVAL_PARENT_HISTORY_EVENT)));
    let history = ReviewParentHistory::from_rollout(&saved.raw_items).unwrap();
    let snapshot = history.snapshot();
    assert_eq!(snapshot.originals.len(), 1);
    assert_eq!(
        snapshot.originals[0].message_id,
        uuid.try_to_utf8().unwrap()
    );
    assert!(snapshot.retained.user_messages_complete());
    snapshot.ensure_representable().unwrap();
    let encoded = runtime_codec::encode_bytes(&saved.messages).unwrap();
    assert!(!String::from_utf8(encoded).unwrap().contains("Inspect only"));
    process.shutdown().await.unwrap();
}

#[test]
fn precheckpoint_typed_answer_original_requires_its_postcheckpoint_admission() {
    let message: klaus_agent_core::schemas::message::Message = runtime_codec::decode_value(
        &runtime_json::parse(&RuntimeString::from(
            serde_json::json!({
                "type": "user",
                "uuid": "typed-answer-original",
                "content": [{"type":"text","text":"Accepted response"}],
                "questionAnswer": {
                    "requestId": "request",
                    "turnId": "origin-turn",
                    "toolCallId": "question-call",
                    "clientResponseId": "accepted-response",
                    "questions": [{"id":"scope","question":"What may be changed?"}],
                    "answers": {"scope":"Read only. Do not publish."}
                }
            })
            .to_string(),
        ))
        .unwrap(),
    )
    .unwrap();
    let base = ReviewParentHistory::default();
    let mut accepted = base.clone();
    accepted
        .accept_at_order(&message, "origin-turn", 7)
        .unwrap();
    let accepted_checkpoint = accepted.checkpoint();
    let encoded: serde_json::Value = serde_json::from_str(
        &runtime_json::stringify(&accepted_checkpoint.to_runtime_value().unwrap())
            .try_to_utf8()
            .unwrap(),
    )
    .unwrap();
    let record = &encoded["originals"][0];
    let snapshot = accepted.snapshot();
    let retained_source = snapshot
        .retained
        .ordered_entries()
        .find_map(|(_, entry)| snapshot.retained.source(entry))
        .unwrap();
    let payload = serde_json::json!({
        "message_id": record["message_id"],
        "fingerprint": record["fingerprint"],
        "turn_id": record["turn_id"],
        "acceptance_order": record["acceptance_order"],
        "role": "user",
        "retained_source": retained_source
    });
    let items = vec![
        RolloutItem::Message { message },
        RolloutItem::Compacted(klaus_agent_core::schemas::rollout::CompactedItem {
            summary: "Summary".into(),
            replaced_range: (0, 0),
            source_message_ids: Some(Vec::new()),
            replacement_history: Some(Vec::new()),
            approval_parent_history: Some(base.checkpoint().to_runtime_value().unwrap()),
        }),
        RolloutItem::Event(klaus_agent_core::schemas::rollout::EventItem {
            event: APPROVAL_MESSAGE_ADMITTED_EVENT.into(),
            turn_id: Some("origin-turn".into()),
            payload: Some(runtime_json::parse(&payload.to_string().into()).unwrap()),
        }),
    ];
    let without_admission = ReviewParentHistory::from_rollout(&items[..2]).unwrap();
    assert_eq!(
        without_admission
            .snapshot()
            .retained
            .verified_answers()
            .count(),
        0
    );
    let recovered = ReviewParentHistory::from_rollout(&items).unwrap();
    let recovered = recovered.snapshot();
    assert_eq!(recovered.items, snapshot.items);
    assert_eq!(recovered.retained, snapshot.retained);
    assert_eq!(recovered.originals, snapshot.originals);
    assert_eq!(recovered.originals[0].acceptance_order, 7);
    assert!(recovered.retained.verified_answers_complete());
    assert_eq!(
        recovered
            .retained
            .verified_answers()
            .next()
            .unwrap()
            .questions[0]
            .answer,
        "Read only. Do not publish."
    );
}
