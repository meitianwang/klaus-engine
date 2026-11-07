//! Defect: an answer accepted during canonical context refresh missed the next request.
//! Root cause: the committed transcript preserved fresh answers while the query kept its earlier candidate.
//! Fix: 603e425d; merge concurrently accepted answers into the continuing request.

use async_trait::async_trait;
use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::deps::{CompactionResult, Compactor};
use klaus_agent_core::engine::session::{resolve_config, Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{RuntimeMap, RuntimeString};
use klaus_agent_core::schemas::config::ResolvedConfig;
use klaus_agent_core::schemas::message::{CompactBoundaryMessage, Message, Usage};
use klaus_agent_core::schemas::rollout::RolloutItem;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::projection::project_for_prompt;
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::tools::builtins::ask_user::create_ask_user_async_tool;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::permission::ExternalPermissionMode;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo, SystemPromptPart,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

const MODEL: &str = "refresh-answer-model";
const QUESTION: &str = r#"{"questions":[{"question":"Which layout?","header":"Layout","options":[{"label":"Dense","description":"More rows"},{"label":"Spacious","description":"More space"}],"multiSelect":false}]}"#;

#[derive(Default)]
struct Gates {
    old_requests: AtomicUsize,
    summaries: AtomicUsize,
    summary_completed: AtomicBool,
    canonical_paused: AtomicBool,
    continue_target_once: AtomicBool,
    source_step_started: Notify,
    release_source_step: Notify,
    canonical_started: Notify,
    release_canonical: Notify,
    requests: Mutex<Vec<(bool, ModelRequest)>>,
}

struct Provider {
    info: ProviderInfo,
    target: bool,
    gates: Arc<Gates>,
}

fn stopped(reason: ModelStopReason, input_tokens: u64) -> ModelStreamEvent {
    ModelStreamEvent::MessageStop {
        stop_reason: reason,
        usage: Usage {
            input_tokens,
            output_tokens: 10,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
            total_tokens: None,
        },
    }
}

impl ModelProvider for Provider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: true,
            tools: true,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: if self.target { 40_000 } else { 100_000 },
            max_output_tokens: 1_000,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
            supports_image_detail_original: false,
        }
    }

    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        self.gates
            .requests
            .lock()
            .unwrap()
            .push((self.target, request));
        if self.target {
            return Box::pin(futures::stream::iter([
                ModelStreamEvent::TextDelta {
                    text: "Completed with the accepted layout.".into(),
                },
                stopped(
                    if self
                        .gates
                        .continue_target_once
                        .swap(false, Ordering::SeqCst)
                    {
                        ModelStopReason::Continue
                    } else {
                        ModelStopReason::EndTurn
                    },
                    1_000,
                ),
            ]));
        }
        match self.gates.old_requests.fetch_add(1, Ordering::SeqCst) {
            0 => Box::pin(futures::stream::iter([
                ModelStreamEvent::ToolUseStart {
                    id: "optional-layout-question".into(),
                    name: "ask_user_async".into(),
                    thought_signature: None,
                    tool_kind: None,
                    input_format: None,
                },
                ModelStreamEvent::ToolUseInputDelta {
                    id: "optional-layout-question".into(),
                    partial_json: QUESTION.into(),
                },
                ModelStreamEvent::ToolUseStop {
                    id: "optional-layout-question".into(),
                },
                stopped(ModelStopReason::ToolUse, 1_000),
            ])),
            1 => {
                let gates = self.gates.clone();
                Box::pin(
                    futures::stream::iter([ModelStreamEvent::TextDelta {
                        text: "The optional question is open while I continue.".into(),
                    }])
                    .chain(futures::stream::once(async move {
                        gates.source_step_started.notify_one();
                        gates.release_source_step.notified().await;
                        stopped(ModelStopReason::Continue, 60_000)
                    })),
                )
            }
            _ => panic!("The selected target must own the next normal request"),
        }
    }
}

struct Summary(Arc<Gates>);

#[async_trait]
impl Compactor for Summary {
    fn rebind_model(
        &self,
        _: Arc<dyn ModelProvider>,
        _: &ResolvedConfig,
    ) -> Result<Arc<dyn Compactor>, String> {
        Ok(Arc::new(Self(self.0.clone())))
    }

    async fn maybe_compact(
        &self,
        messages: &[Message],
        _: u64,
        config: &ResolvedConfig,
        instructions: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.reactive_compact(messages, config, instructions).await
    }

    async fn reactive_compact(
        &self,
        _: &[Message],
        _: &ResolvedConfig,
        _: Option<&RuntimeString>,
    ) -> CompactionResult {
        let call = self.0.summaries.fetch_add(1, Ordering::SeqCst) + 1;
        self.0.summary_completed.store(true, Ordering::SeqCst);
        CompactionResult {
            messages: vec![Message::CompactBoundary(CompactBoundaryMessage {
                uuid: format!("canonical-refresh-checkpoint-{call}").into(),
                summary: "Continue the layout task and honor the optional question answer.".into(),
                native_compaction: None,
            })],
            freed_tokens: 50_000,
            compacted: true,
            reason: None,
            diagnostics: Vec::new(),
        }
    }
}

fn provider(target: bool, gates: &Arc<Gates>) -> Arc<dyn ModelProvider> {
    Arc::new(Provider {
        info: ProviderInfo {
            name: if target {
                "refresh-target"
            } else {
                "refresh-source"
            }
            .into(),
            default_model: MODEL.into(),
            base_url: None,
            env_key: None,
        },
        target,
        gates: gates.clone(),
    })
}

fn answer_count(messages: &[Message], id: &RuntimeString) -> usize {
    messages
        .iter()
        .filter(|message| {
            matches!(message,
        Message::User(user) if &user.uuid == id && user.question_answer.is_some())
        })
        .count()
}

#[tokio::test]
async fn an_answer_accepted_during_canonical_refresh_reaches_the_immediate_target_request() {
    exercise_refresh_answer(false).await;
}

#[tokio::test]
async fn a_continued_target_keeps_the_answer_once_without_an_extra_followup() {
    exercise_refresh_answer(true).await;
}

async fn exercise_refresh_answer(continue_target: bool) {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let root = std::env::temp_dir().join(format!("klaus-refresh-answer-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let gates = Arc::new(Gates::default());
    gates
        .continue_target_once
        .store(continue_target, Ordering::SeqCst);
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
    options.provider = provider(false, &gates);
    options.compactor = Some(Arc::new(Summary(gates.clone())));
    options.tools = Some(vec![create_ask_user_async_tool()]);
    options.include_default_agents = Some(false);
    options.cwd = Some(root.to_string_lossy().into_owned());
    options.config = Some(serde_json::json!({
        "model":MODEL, "contextWindow":100_000, "maxOutputTokens":1_000,
        "autoCompaction":true, "prewarm":false, "thinking":false,
        "loopDetection":false, "autoMemory":false, "autoDream":false,
        "memoryRecall":false, "maxRetries":0, "approval_policy":"never",
        "sandbox_mode":"danger-full-access",
        "modelSource":{"source":"refresh-source","modelId":MODEL,"selection":format!("refresh-source:{MODEL}")}
    }));
    options.system_prompt_provider = Some({
        let gates = gates.clone();
        Arc::new(move || {
            let gates = gates.clone();
            Box::pin(async move {
                if gates.summary_completed.load(Ordering::SeqCst)
                    && !gates.canonical_paused.swap(true, Ordering::SeqCst)
                {
                    gates.canonical_started.notify_one();
                    gates.release_canonical.notified().await;
                }
                vec![SystemPromptPart {
                    name: "current-layout-policy".into(),
                    text: "Complete the layout task using the user's current preferences.".into(),
                    cacheable: None,
                    cache_scope: None,
                }]
            })
        })
    });
    let session = Agent::new(options)
        .open_session(
            store.clone(),
            StartSessionOptions {
                cwd: Some(root.to_string_lossy().into_owned().into()),
                user_config_dir: Some(root.to_string_lossy().into_owned().into()),
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
                    Input::Text("Ask about the layout and keep working.".into()),
                    TurnOptions::default(),
                )
                .events
                .collect::<Vec<_>>()
                .await
        })
    };
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        gates.source_step_started.notified(),
    )
    .await
    .expect("The source continuation must pause with a real pending question");
    let pending = session.pending_questions();
    assert_eq!(pending.len(), 1);
    session.reconfigure_inference(
        provider(true, &gates),
        resolve_config(serde_json::json!({
            "model":MODEL,"contextWindow":40_000,"maxOutputTokens":1_000,
            "modelSource":{"source":"refresh-target","modelId":MODEL,"selection":format!("refresh-target:{MODEL}")}
        })).unwrap(),
        None, None, session.model_revision(),
    ).unwrap();
    gates.release_source_step.notify_one();
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        gates.canonical_started.notified(),
    )
    .await
    .expect("Canonical refresh must pause after the summary and before its commit");
    assert_eq!(gates.summaries.load(Ordering::SeqCst), 1);
    assert!(
        !session
            .history()
            .iter()
            .any(|message| matches!(message, Message::CompactBoundary(_))),
        "The answer must arrive after candidate reconstruction and before checkpoint commit"
    );
    let question = &pending[0];
    let mut answers = RuntimeMap::new();
    answers.insert("Which layout?".into(), "Dense".into());
    let receipt = session
        .answer_question(
            question.turn_id.clone(),
            question.request_id.clone(),
            "canonical-refresh-answer".into(),
            answers,
            None,
        )
        .await
        .unwrap();
    assert!(receipt.accepted);
    assert_eq!(
        answer_count(&session.history(), &receipt.user_message_id),
        1
    );
    gates.release_canonical.notify_one();
    let events = tokio::time::timeout(std::time::Duration::from_secs(10), running)
        .await
        .expect("The migrated turn must settle after refresh is released")
        .unwrap();
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
    {
        let requests = gates.requests.lock().unwrap();
        let target = requests
            .iter()
            .filter(|(target, _)| *target)
            .map(|(_, request)| request)
            .collect::<Vec<_>>();
        assert_eq!(
            target.len(),
            if continue_target { 2 } else { 1 },
            "Already included answers must not create another followup"
        );
        assert_eq!(
            answer_count(&target[0].messages, &receipt.user_message_id),
            1,
            "The immediate request must contain the freshly accepted typed answer"
        );
        for request in &target {
            assert_eq!(answer_count(&request.messages, &receipt.user_message_id), 1);
        }
        let answer = target[0]
            .messages
            .iter()
            .find_map(|message| match message {
                Message::User(user) if user.uuid == receipt.user_message_id => {
                    user.question_answer.as_ref()
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(
            answer.answers.get(&RuntimeString::from("Which layout?")),
            Some(&RuntimeString::from("Dense"))
        );
    }
    assert_eq!(
        answer_count(&session.history(), &receipt.user_message_id),
        1
    );
    session.flush_rollout().await.unwrap();
    let durable = store
        .read_thread(&session.id.try_to_utf8().unwrap())
        .await
        .unwrap();
    assert_eq!(
        answer_count(
            &project_for_prompt(&durable.raw_items),
            &receipt.user_message_id
        ),
        1
    );
    let checkpoint = durable
        .raw_items
        .iter()
        .find_map(|item| match item {
            RolloutItem::Compacted(item) => Some(item),
            _ => None,
        })
        .expect("The source migration must commit a checkpoint");
    assert!(!checkpoint
        .source_message_ids
        .as_ref()
        .unwrap()
        .contains(&receipt.user_message_id));
    assert_eq!(
        answer_count(
            checkpoint.replacement_history.as_ref().unwrap(),
            &receipt.user_message_id
        ),
        1
    );
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
