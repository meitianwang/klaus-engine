//! Defect: optional questions blocked the engine and late answers had no durable
//! user message or exact originating-turn admission.
//! Root cause: ask_user used a blocking one-shot; the steering queue drained only
//! after tool results and could spill a text-only-final answer into another turn.
//! Fixed by: 2026-10-04 asynchronous user-question lifecycle work item.

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};
use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, Session, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeMap, RuntimeString};
use klaus_agent_core::schemas::content::{ContentBlock, ImageBlock};
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::tools::builtins::ask_user::create_ask_user_async_tool;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::permission::ExternalPermissionMode;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

const QUESTIONS: &str = r#"{"questions":[{"question":"Which layout?","header":"Layout","options":[{"label":"Dense","description":"More rows"},{"label":"Spacious","description":"More space"}],"multiSelect":false},{"question":"Which language?","header":"Language","options":[{"label":"English","description":"English text"},{"label":"Chinese","description":"Chinese text"}],"multiSelect":false}]}"#;

struct QuestionProvider {
    questions: RuntimeString,
    info: ProviderInfo,
    calls: AtomicUsize,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
    final_started: Arc<Notify>,
    release_final: Arc<Notify>,
}

fn usage() -> Usage {
    Usage {
        input_tokens: 1,
        output_tokens: 1,
        cache_read_input_tokens: 0,
        cache_creation_input_tokens: 0,
        total_tokens: None,
    }
}

impl ModelProvider for QuestionProvider {
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
            context_window: 200_000,
            max_output_tokens: 16_000,
            native_pdf: false,
            vision: true,
            image_limits: None,
            max_media_blocks: None,
        }
    }
    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        self.requests.lock().unwrap().push(request);
        let questions = self.questions.clone();
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let started = self.final_started.clone();
        let release = self.release_final.clone();
        Box::pin(async_stream::stream! {
            yield ModelStreamEvent::MessageStart {model:"question-model".into()};
            if call == 0 {
                yield ModelStreamEvent::ToolUseStart {id:"optional-question-call".into(),name:"ask_user_async".into(),thought_signature:None,tool_kind:None,input_format:None};
                yield ModelStreamEvent::ToolUseInputDelta {id:"optional-question-call".into(),partial_json:questions};
                yield ModelStreamEvent::ToolUseStop {id:"optional-question-call".into()};
                yield ModelStreamEvent::MessageStop {stop_reason:ModelStopReason::ToolUse,usage:usage()};
            } else {
                yield ModelStreamEvent::TextDelta {text:if call == 1 {"Still working"} else {"Received your answer"}.into()};
                if call == 1 {
                    started.notify_one();
                    tokio::select! {_ = release.notified() => {}, _ = signal.cancelled() => {return;}}
                }
                yield ModelStreamEvent::MessageStop {stop_reason:ModelStopReason::EndTurn,usage:usage()};
            }
        })
    }
}

type Harness<T> = (T, Arc<Mutex<Vec<ModelRequest>>>, Arc<Notify>, Arc<Notify>);

fn agent_harness() -> Harness<Agent> {
    agent_harness_with_questions(QUESTIONS)
}

fn agent_harness_with_questions(questions: &str) -> Harness<Agent> {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut options =
        agent_options(RuntimeProcessContext::initialize(process_supervisor()).unwrap());
    options.provider = Arc::new(QuestionProvider {
        questions: questions.into(),
        info: ProviderInfo {
            name: "question-provider".into(),
            base_url: None,
            env_key: None,
            default_model: "question-model".into(),
        },
        calls: AtomicUsize::new(0),
        requests: requests.clone(),
        final_started: started.clone(),
        release_final: release.clone(),
    });
    options.tools = Some(vec![create_ask_user_async_tool()]);
    (Agent::new(options), requests, started, release)
}

pub(super) fn harness_with_questions(questions: &str) -> Harness<Arc<Session>> {
    let (agent, requests, started, release) = agent_harness_with_questions(questions);
    (
        agent.start_session(StartSessionOptions {
            permission_mode: Some(ExternalPermissionMode::BypassPermissions),
            config: Some(serde_json::json!({"prewarm":false,"goal":false})),
            ..Default::default()
        }),
        requests,
        started,
        release,
    )
}

fn harness() -> Harness<Arc<Session>> {
    let (agent, requests, started, release) = agent_harness();
    let session = agent.start_session(StartSessionOptions {
        permission_mode: Some(ExternalPermissionMode::BypassPermissions),
        config: Some(serde_json::json!({"prewarm":false,"goal":false})),
        ..Default::default()
    });
    (session, requests, started, release)
}

fn answers(question: &str, value: &str) -> RuntimeMap<RuntimeString> {
    let mut answers = RuntimeMap::new();
    answers.insert(question.into(), value.into());
    answers
}

#[tokio::test]
async fn async_questions_continue_then_consume_partial_late_answers_before_text_only_final() {
    let (session, requests, started, release) = harness();
    let running = {
        let session = session.clone();
        tokio::spawn(async move {
            session
                .run(Input::Text("work".into()), TurnOptions::default())
                .await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(10), started.notified())
        .await
        .expect("the model must continue without answering");
    let pending = session.pending_questions();
    assert_eq!(pending.len(), 1);
    let question = pending[0].clone();
    assert_eq!(question.tool_call_id, "optional-question-call");
    assert_eq!(question.request.questions.len(), 2);
    assert!(session
        .answer_question(
            "different-turn".into(),
            question.request_id.clone(),
            "wrong".into(),
            answers("Which layout?", "Dense"),
            None
        )
        .await
        .is_err());
    let first = session
        .answer_question(
            question.turn_id.clone(),
            question.request_id.clone(),
            "first".into(),
            answers("Which layout?", "Dense"),
            None,
        )
        .await
        .unwrap();
    assert_eq!(first.state, "pending");
    assert_eq!(session.pending_questions()[0].answers.len(), 1);
    let duplicate = session
        .answer_question(
            question.turn_id.clone(),
            question.request_id.clone(),
            "first".into(),
            answers("Which layout?", "Dense"),
            None,
        )
        .await
        .unwrap();
    assert_eq!(duplicate.user_message_id, first.user_message_id);
    assert!(session
        .answer_question(
            question.turn_id.clone(),
            question.request_id.clone(),
            "first".into(),
            answers("Which layout?", "Spacious"),
            None
        )
        .await
        .is_err());
    let image = ContentBlock::Image(ImageBlock {
        mime_type: "image/png".into(),
        base64: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScLbtAAAAABJRU5ErkJggg==".into(),
        detail: None,
        source_path: None,
    });
    let mut images = RuntimeMap::new();
    images.insert("Which language?".into(), vec![image.clone()]);
    let second = session
        .answer_question(
            question.turn_id.clone(),
            question.request_id.clone(),
            "second".into(),
            answers("Which language?", "Chinese"),
            Some(Box::new(images)),
        )
        .await
        .unwrap();
    assert_eq!(second.state, "answered");
    assert!(session.pending_questions().is_empty());
    release.notify_one();
    let result = running.await.unwrap();
    assert!(result.error.is_none(), "turn failed: {:?}", result.error);
    let requests = requests.lock().unwrap().clone();
    assert!(
        requests.len() >= 3,
        "the text-only final must continue after accepted late input"
    );
    let final_answers = requests[2]
        .messages
        .iter()
        .filter_map(|message| match message {
            Message::User(user) if user.question_answer.is_some() => Some(user),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(final_answers.len(), 2);
    assert!(final_answers[1].content.contains(&image));
    drop(requests);
    let history = session.history();
    let replies = history
        .iter()
        .filter_map(|message| match message {
            Message::User(user) if user.question_answer.is_some() => Some(user),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        replies.len(),
        2,
        "duplicate answers must not duplicate user history"
    );
    let encoded = runtime_codec::encode_value(&history).unwrap();
    let restored: Vec<Message> = runtime_codec::decode_value(&encoded).unwrap();
    let restored_replies = restored
        .iter()
        .filter_map(|message| match message {
            Message::User(user) if user.question_answer.is_some() => Some(user),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        restored_replies, replies,
        "typed quotes and images must survive history serialization"
    );
    let index = session.turn_index_snapshot();
    assert_eq!(
        index.message_turn.get(&first.user_message_id),
        Some(&question.turn_id)
    );
    assert!(session
        .answer_question(
            question.turn_id.clone(),
            question.request_id.clone(),
            "late".into(),
            answers("Which layout?", "Spacious"),
            None
        )
        .await
        .is_err());
    assert_eq!(
        session
            .answer_question(
                question.turn_id,
                question.request_id,
                "first".into(),
                answers("Which layout?", "Dense"),
                None
            )
            .await
            .unwrap()
            .user_message_id,
        first.user_message_id
    );
    assert!(
        session.queued_messages().is_empty(),
        "answers must never spill into the generic next-turn queue"
    );
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn interrupted_optional_questions_expire_with_original_context_preserved() {
    let (session, _, started, _) = harness();
    let events = Arc::new(Mutex::new(Vec::new()));
    let running = {
        let session = session.clone();
        let events = events.clone();
        tokio::spawn(async move {
            let mut stream = session
                .run_streamed(Input::Text("work".into()), TurnOptions::default())
                .events;
            while let Some(event) = stream.next().await {
                events.lock().unwrap().push(event);
            }
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(10), started.notified())
        .await
        .unwrap();
    let question = session.pending_questions()[0].clone();
    session.interrupt();
    running.await.unwrap();
    assert!(session.pending_questions().is_empty());
    assert!(session
        .answer_question(
            question.turn_id,
            question.request_id,
            "after-interrupt".into(),
            answers("Which layout?", "Dense"),
            None
        )
        .await
        .is_err());
    {
        let events = events.lock().unwrap();
        assert!(events
            .iter()
            .any(|event| matches!(event, EngineEvent::QuestionRequested { .. })));
        assert!(events.iter().any(|event| matches!(event,EngineEvent::QuestionResolved {question_resolved} if question_resolved.state=="cancelled")));
    }
    assert!(session.history().iter().any(|message| matches!(message,Message::ToolResult(result) if result.tool_use_id=="optional-question-call")));
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn persisted_question_answers_reopen_as_real_user_messages_without_stale_entries() {
    use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
    struct TemporaryDirectory(std::path::PathBuf);
    impl Drop for TemporaryDirectory {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).expect("remove the isolated question history fixture");
        }
    }
    let directory = TemporaryDirectory(std::env::temp_dir().join(format!(
        "klaus-async-question-history-{}",
        uuid::Uuid::new_v4()
    )));
    std::fs::create_dir_all(&directory.0).unwrap();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(directory.0.to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let (agent, _, started, release) = agent_harness();
    let options = || StartSessionOptions {
        permission_mode: Some(ExternalPermissionMode::BypassPermissions),
        config: Some(serde_json::json!({"prewarm":false,"goal":false})),
        ..Default::default()
    };
    let session = agent.open_session(store.clone(), options()).await.unwrap();
    let thread_id = session.id.clone();
    let running = {
        let session = session.clone();
        tokio::spawn(async move {
            session
                .run(Input::Text("work".into()), TurnOptions::default())
                .await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(10), started.notified())
        .await
        .unwrap();
    let question = session.pending_questions()[0].clone();
    let receipt = session
        .answer_question(
            question.turn_id.clone(),
            question.request_id.clone(),
            "durable".into(),
            answers("Which layout?", "Spacious"),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        receipt.state, "pending",
        "other questions remain unanswered"
    );
    release.notify_one();
    assert!(running.await.unwrap().error.is_none());
    session.shutdown_and_wait().await.unwrap();
    let resumed = agent
        .resume_session(store, &thread_id.try_to_utf8().unwrap(), options())
        .await
        .unwrap();
    assert!(
        resumed.pending_questions().is_empty(),
        "an ended turn must never restore an interactive chip"
    );
    let history = resumed.history();
    let reply = history
        .iter()
        .find_map(|message| match message {
            Message::User(user) if user.uuid == receipt.user_message_id => Some(user),
            _ => None,
        })
        .expect("the answer must survive a real rollout and resume");
    assert_eq!(
        reply
            .question_answer
            .as_ref()
            .unwrap()
            .answers
            .get(&RuntimeString::from("Which layout?")),
        Some(&RuntimeString::from("Spacious"))
    );
    assert_eq!(
        resumed
            .turn_index_snapshot()
            .message_turn
            .get(&receipt.user_message_id),
        Some(&question.turn_id)
    );
    assert!(history.iter().any(|message| matches!(message,Message::Assistant(assistant) if assistant.content.iter().any(|block| matches!(block,ContentBlock::ToolUse(tool) if tool.id=="optional-question-call")))));
    assert!(resumed
        .answer_question(
            question.turn_id,
            question.request_id,
            "after-resume".into(),
            answers("Which language?", "Chinese"),
            None
        )
        .await
        .is_err());
    resumed.shutdown_and_wait().await.unwrap();
}
