//! Defect: a question answer accepted during compaction disappeared from live and
//! resumed history when a successful checkpoint immediately failed the turn.
//! Root cause: exact replacement discarded typed answers outside its source,
//! including answers appended durably before their live-history push.
//! Fixed for work item 20261003: source-aware checkpoints preserve those answers.

use async_trait::async_trait;
use futures::StreamExt;
use klaus_agent_core::context::tokens::estimate_messages_tokens;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::deps::{CompactionResult, Compactor};
use klaus_agent_core::engine::questions::QuestionAnswer;
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{RuntimeMap, RuntimeString};
use klaus_agent_core::schemas::config::ResolvedConfig;
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{CompactBoundaryMessage, Message, Usage, UserMessage};
use klaus_agent_core::schemas::rollout::{CompactedItem, RolloutItem};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::projection::project_for_prompt;
use klaus_agent_core::store::rollout::{parse_rollout_line, serialize_rollout_item};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::tools::builtins::ask_user::create_ask_user_async_tool;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{EngineErrorCode, ProviderError, ProviderErrorKind};
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::permission::ExternalPermissionMode;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

const QUESTION: &str = r#"{"questions":[{"question":"Which layout?","header":"Layout","options":[{"label":"Dense","description":"More rows"},{"label":"Spacious","description":"More space"}],"multiSelect":false}]}"#;

fn text(value: &str) -> TextBlock {
    TextBlock {
        text: value.into(),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    }
}

fn user(uuid: &str, value: &str, typed: bool) -> Message {
    Message::User(UserMessage {
        uuid: uuid.into(),
        content: vec![ContentBlock::Text(text(value))],
        is_meta: None,
        question_answer: typed.then(|| {
            Box::new(QuestionAnswer {
                request_id: "question".into(),
                turn_id: "turn".into(),
                tool_call_id: "question-call".into(),
                client_response_id: uuid.into(),
                questions: Vec::new(),
                answers: RuntimeMap::new(),
            })
        }),
    })
}

fn boundary(uuid: &str) -> Message {
    Message::CompactBoundary(CompactBoundaryMessage {
        native_compaction: None,
        uuid: uuid.into(),
        summary: "Preserved task summary.".into(),
    })
}

fn contains_answer(messages: &[Message], uuid: &RuntimeString) -> bool {
    messages.iter().any(|message| {
        matches!(message, Message::User(user) if &user.uuid == uuid && user.question_answer.is_some())
    })
}

struct PausedReducingCompactor {
    calls: AtomicUsize,
    started: Arc<Notify>,
    release: Arc<Notify>,
}

struct ToolProvider(ProviderInfo);

impl ModelProvider for ToolProvider {
    fn info(&self) -> &ProviderInfo {
        &self.0
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
            context_window: 40_000,
            max_output_tokens: 1_000,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn stream(&self, _: ModelRequest, _: AbortSignal) -> ModelStream {
        panic!("the regression injects its deterministic call_model stream");
    }
}

#[async_trait]
impl Compactor for PausedReducingCompactor {
    fn reactive_enabled(&self) -> bool {
        false
    }

    async fn maybe_compact(
        &self,
        messages: &[Message],
        _estimated_tokens: u64,
        _config: &ResolvedConfig,
        _custom_instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call == 2 {
            self.started.notify_one();
            self.release.notified().await;
        }
        let next = vec![boundary(&format!("question-checkpoint-{call}"))];
        CompactionResult {
            freed_tokens: estimate_messages_tokens(messages)
                .saturating_sub(estimate_messages_tokens(&next)),
            messages: next,
            compacted: true,
            reason: None,
            diagnostics: Vec::new(),
        }
    }

    async fn reactive_compact(
        &self,
        _messages: &[Message],
        _config: &ResolvedConfig,
        _custom_instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        panic!("the provider fixture does not reject requests");
    }
}

#[tokio::test]
async fn accepted_answer_survives_checkpoint_followed_by_provider_failure() {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let root = std::env::temp_dir().join(format!(
        "klaus-question-compaction-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let compactor = Arc::new(PausedReducingCompactor {
        calls: AtomicUsize::new(0),
        started: started.clone(),
        release: release.clone(),
    });
    let requests = Arc::new(AtomicUsize::new(0));
    let seen = requests.clone();
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
    options.compactor = Some(compactor.clone());
    options.provider = Arc::new(ToolProvider(ProviderInfo {
        name: "question-compaction-provider".into(),
        base_url: None,
        env_key: None,
        default_model: "regression-noop".into(),
    }));
    options.tools = Some(vec![create_ask_user_async_tool()]);
    options.cwd = Some(root.to_string_lossy().into_owned());
    options
        .config
        .as_mut()
        .unwrap()
        .as_object_mut()
        .unwrap()
        .extend(
        serde_json::json!({
            "model": "regression-noop", "contextWindow": 40_000, "autoCompactWindow": 20_000, "maxOutputTokens": 1_000,
            "outputReserveTokens": 0, "compactBufferTokens": 1_000, "compactThreshold": null,
            "earlyMicrocompact": false, "idleMicrocompact": false, "speculativeCompaction": false,
            "loopDetection": false, "autoMemory": false, "agentMemory": false,
            "prewarm": false, "goal": false, "maxRetries": 0
        })
        .as_object()
        .unwrap()
        .clone(),
    );
    options.deps.call_model = Some(Box::new(move |_, _| {
        let call = seen.fetch_add(1, Ordering::SeqCst);
        let mut events = Vec::new();
        if call == 0 {
            events.extend([
                ModelStreamEvent::ToolUseStart {
                    id: "optional-question-call".into(),
                    name: "ask_user_async".into(),
                    thought_signature: None,
                    tool_kind: None,
                    input_format: None,
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: "optional-question-call".into(),
                    partial_json: QUESTION.into(),
                },
                ModelStreamEvent::ToolUseStop {
                    id: "optional-question-call".into(),
                },
            ]);
        } else {
            return Box::pin(futures::stream::iter(vec![ModelStreamEvent::Error {
                error: ProviderError {
                    kind: ProviderErrorKind::Network,
                    message: "Controlled failure after a committed checkpoint.".into(),
                    status: None,
                    code: None,
                    error_type: None,
                    media_origins: None,
                    media_repair_complete: false,
                    credential_recovery: None,
                    retry_after_ms: None,
                    request_id: None,
                },
            }]));
        }
        events.push(ModelStreamEvent::MessageStop {
            stop_reason: if call == 0 {
                ModelStopReason::ToolUse
            } else {
                ModelStopReason::EndTurn
            },
            usage: Usage {
                input_tokens: 19_500,
                output_tokens: 500,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
                total_tokens: None,
            },
        });
        Box::pin(futures::stream::iter(events))
    }));
    let session = Agent::new(options)
        .open_session(
            store.clone(),
            StartSessionOptions {
                cwd: Some(root.to_string_lossy().into_owned().into()),
                user_config_dir: Some(root.to_string_lossy().into_owned().into()),
                initial_history: Some(vec![user(
                    "old-task",
                    &"task detail ".repeat(10_000),
                    false,
                )]),
                permission_mode: Some(ExternalPermissionMode::BypassPermissions),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let running = {
        let session = session.clone();
        tokio::spawn(async move {
            session
                .run_streamed(
                    Input::Text("Continue the task.".into()),
                    TurnOptions {
                        token_budget: Some(10_000),
                        ..Default::default()
                    },
                )
                .events
                .collect::<Vec<_>>()
                .await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(10), started.notified())
        .await
        .expect("the final reduced summary must pause before its checkpoint");
    let pending = session.pending_questions();
    assert_eq!(pending.len(), 1);
    let question = &pending[0];
    let mut answers = RuntimeMap::new();
    answers.insert("Which layout?".into(), "Dense".into());
    let receipt = session
        .answer_question(
            question.turn_id.clone(),
            question.request_id.clone(),
            "during-compaction".into(),
            answers,
            None,
        )
        .await
        .unwrap();
    assert!(receipt.accepted);
    assert!(contains_answer(
        &session.history(),
        &receipt.user_message_id
    ));
    release.notify_one();
    let events = running.await.unwrap();
    session.flush_rollout().await.unwrap();
    let retained = session.history();
    let durable = store
        .read_thread(&session.id.try_to_utf8().unwrap())
        .await
        .unwrap();
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();

    assert!(events.iter().any(|event| {
        matches!(event, EngineEvent::TurnFailed {error, ..} if error.code == EngineErrorCode::ProviderError && error.provider_kind == Some(ProviderErrorKind::Network) && error.message == "Controlled failure after a committed checkpoint.")
    }));
    assert_eq!(compactor.calls.load(Ordering::SeqCst), 2);
    assert_eq!(requests.load(Ordering::SeqCst), 2);
    assert!(contains_answer(&retained, &receipt.user_message_id));
    let projected = project_for_prompt(&durable.raw_items);
    assert!(contains_answer(&projected, &receipt.user_message_id));
    assert_eq!(projected, retained);
    assert_eq!(
        projected
            .iter()
            .filter(|message| matches!(message, Message::User(user) if user.uuid == receipt.user_message_id))
            .count(),
        1,
        "the answer is retained once without a second durable append"
    );
    let checkpoint = durable
        .raw_items
        .iter()
        .rev()
        .find_map(|item| match item {
            RolloutItem::Compacted(item) => Some(item),
            _ => None,
        })
        .unwrap();
    assert!(!checkpoint
        .source_message_ids
        .as_ref()
        .unwrap()
        .contains(&receipt.user_message_id));
}

#[test]
fn answer_enqueued_before_checkpoint_but_not_in_live_snapshot_survives_resume() {
    let fresh = user("fresh-answer", "Dense", true);
    let stale = user("source-answer", "Previous preference", true);
    let checkpoint = RolloutItem::Compacted(CompactedItem {
        approval_parent_history: None,
        summary: "summary".into(),
        replaced_range: (0, 2),
        replacement_history: Some(vec![boundary("checkpoint")]),
        source_message_ids: Some(vec!["task".into(), "source-answer".into()]),
    });
    let items = [
        RolloutItem::Message {
            message: user("task", "Work", false),
        },
        RolloutItem::Message { message: stale },
        // append_message precedes answer_question's flush and live-history push.
        RolloutItem::Message {
            message: fresh.clone(),
        },
        RolloutItem::Message {
            message: user("unrelated", "Ordinary metadata is not protected", false),
        },
        checkpoint,
    ];
    let restored = items
        .iter()
        .map(|item| parse_rollout_line(&serialize_rollout_item(item)).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        project_for_prompt(&restored),
        vec![boundary("checkpoint"), fresh],
        "retain only the typed answer outside the actual compacted source"
    );
}

#[test]
fn source_watermark_deduplicates_fresh_answers_and_allows_later_summarization() {
    let fresh = user("fresh-answer", "Dense", true);
    let mut items = vec![
        RolloutItem::Message {
            message: fresh.clone(),
        },
        RolloutItem::Compacted(CompactedItem {
            approval_parent_history: None,
            summary: "first".into(),
            replaced_range: (0, 0),
            replacement_history: Some(vec![boundary("first"), fresh.clone()]),
            source_message_ids: Some(Vec::new()),
        }),
    ];
    assert_eq!(project_for_prompt(&items), vec![boundary("first"), fresh]);
    items.push(RolloutItem::Compacted(CompactedItem {
        approval_parent_history: None,
        summary: "second".into(),
        replaced_range: (0, 2),
        replacement_history: Some(vec![boundary("second")]),
        source_message_ids: Some(vec!["first".into(), "fresh-answer".into()]),
    }));
    assert_eq!(project_for_prompt(&items), vec![boundary("second")]);
}

#[test]
fn optional_watermark_wire_vector_keeps_legacy_exact_checkpoint_semantics() {
    let checkpoint = RolloutItem::Compacted(CompactedItem {
        approval_parent_history: None,
        summary: "summary".into(),
        replaced_range: (0, 1),
        replacement_history: Some(Vec::new()),
        source_message_ids: Some(vec!["task".into()]),
    });
    assert_eq!(
        serialize_rollout_item(&checkpoint),
        r#"{"kind":"compacted","summary":"summary","replacedRange":[0,1],"replacementHistory":[],"sourceMessageIds":["task"]}"#
    );
    assert_eq!(
        parse_rollout_line(&serialize_rollout_item(&checkpoint)),
        Some(checkpoint)
    );
    let legacy = parse_rollout_line(
        r#"{"kind":"compacted","summary":"summary","replacedRange":[0,1],"replacementHistory":[]}"#,
    )
    .unwrap();
    assert!(project_for_prompt(&[
        RolloutItem::Message {
            message: user("old-answer", "Legacy answer", true)
        },
        legacy
    ])
    .is_empty());
}
