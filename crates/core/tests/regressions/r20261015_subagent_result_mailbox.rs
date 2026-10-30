//! Defect: detached descendants could keep ancestors alive or lose late results.
//! Root cause: completion delivery did not distinguish passive mail from new work.
//! Work item: 2026-10-15 nested child completion and passive result delivery.

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};
use futures::{FutureExt, StreamExt};
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::message_queue::{QueueSnapshot, QueuedMessage, QueuedValue};
use klaus_agent_core::engine::session::{Input, Session, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeJson, RuntimeString};
use klaus_agent_core::schemas::content::{ContentBlock, OutputBlock};
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::schemas::subagent_lifecycle::SubagentStatus;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::tools::builtins::task::{create_task_tool, TaskToolOptions};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{ProviderError, ProviderErrorKind};
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::SubagentMessageRequest;
use serde_json::json;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, Semaphore};

const TIMEOUT: Duration = Duration::from_secs(10);
const SETUP_TIMEOUT: Duration = Duration::from_secs(25);
const MODEL: &str = "result-mailbox-test";
const CHILD_SEED: &str = "mailbox-child-seed";
const GRANDCHILD_SEED: &str = "mailbox-grandchild-seed";
const CHILD_RESULT: &str = "mailbox-child-final-result";
const GRANDCHILD_RESULT: &str = "mailbox-grandchild-final-result";
const FOLLOWUP_RESULT: &str = "mailbox-child-followup-result";

fn info() -> ProviderInfo {
    ProviderInfo {
        name: MODEL.into(),
        default_model: MODEL.into(),
        base_url: None,
        env_key: None,
    }
}

fn capabilities() -> ProviderCapabilities {
    ProviderCapabilities {
        supports_image_detail_original: false,
        streaming: true,
        tools: true,
        thinking: false,
        structured_output: false,
        web_search: false,
        prompt_cache_control: false,
        context_window: 200_000,
        max_output_tokens: 8_192,
        native_pdf: false,
        vision: false,
        image_limits: None,
        max_media_blocks: None,
    }
}

fn has_directive(request: &ModelRequest, directive: &str) -> bool {
    request.messages.iter().any(|message| match message {
        Message::User(message) => message
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::Text(text) if text.text == directive)),
        _ => false,
    })
}

fn user_text(request: &ModelRequest) -> String {
    request
        .messages
        .iter()
        .filter_map(|message| match message {
            Message::User(user) => Some(
                user.content
                    .iter()
                    .filter_map(|block| match block {
                        ContentBlock::Text(text) => Some(text.text.try_to_utf8().unwrap()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("\n"),
            ),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn stop(reason: ModelStopReason) -> ModelStreamEvent {
    ModelStreamEvent::MessageStop {
        stop_reason: reason,
        usage: Usage {
            input_tokens: 10,
            output_tokens: 4,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
            total_tokens: None,
        },
    }
}

fn final_events(text: &str) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::MessageStart {
            model: MODEL.into(),
        },
        ModelStreamEvent::TextDelta { text: text.into() },
        stop(ModelStopReason::EndTurn),
    ]
}

fn task_events(
    id: &str,
    description: &str,
    prompt: &str,
    background: bool,
) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::MessageStart {
            model: MODEL.into(),
        },
        ModelStreamEvent::ToolUseStart {
            id: id.into(),
            name: "task".into(),
            thought_signature: None,
            tool_kind: None,
            input_format: None,
        },
        ModelStreamEvent::ToolUseInputDelta {
            id: id.into(),
            partial_json: json!({"model": "low",
                "description": description,
                "prompt": prompt,
                "fork_turns":"none",
                "run_in_background": background,
            })
            .to_string()
            .into(),
        },
        ModelStreamEvent::ToolUseStop { id: id.into() },
        stop(ModelStopReason::ToolUse),
    ]
}

fn gated_final(
    text: &'static str,
    waiting: Arc<Semaphore>,
    release: Arc<Semaphore>,
    signal: AbortSignal,
) -> ModelStream {
    Box::pin(async_stream::stream! {
        waiting.add_permits(1);
        tokio::select! {
            permit = release.acquire() => permit.unwrap().forget(),
            _ = signal.cancelled() => return,
        }
        for event in final_events(text) { yield event; }
    })
}

struct MailboxProvider {
    info: ProviderInfo,
    nested: bool,
    root_calls: AtomicUsize,
    child_calls: AtomicUsize,
    grandchild_calls: AtomicUsize,
    root_requests: Mutex<Vec<ModelRequest>>,
    child_requests: Mutex<Vec<ModelRequest>>,
    child_waiting: Arc<Semaphore>,
    grandchild_waiting: Arc<Semaphore>,
    finish_child: Arc<Semaphore>,
    finish_grandchild: Arc<Semaphore>,
}

impl ModelProvider for MailboxProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }
    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        capabilities()
    }
    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        if has_directive(&request, GRANDCHILD_SEED) {
            self.grandchild_calls.fetch_add(1, Ordering::SeqCst);
            return gated_final(
                GRANDCHILD_RESULT,
                self.grandchild_waiting.clone(),
                self.finish_grandchild.clone(),
                signal,
            );
        }
        if has_directive(&request, CHILD_SEED) {
            let fail_resumed = has_directive(&request, "fail-resumed-child");
            self.child_requests.lock().unwrap().push(request);
            let call = self.child_calls.fetch_add(1, Ordering::SeqCst);
            if fail_resumed {
                return Box::pin(futures::stream::iter([ModelStreamEvent::Error {
                    error: ProviderError {
                        kind: ProviderErrorKind::InvalidRequest,
                        message: "resumed child failed".into(),
                        status: Some(400),
                        code: None,
                        error_type: None,
                        media_origins: None,
                        media_repair_complete: false,
                        retry_after_ms: None,
                        request_id: None,
                    },
                }]));
            }
            if self.nested && call == 0 {
                return Box::pin(futures::stream::iter(task_events(
                    "spawn-grandchild",
                    "mailbox grandchild",
                    GRANDCHILD_SEED,
                    true,
                )));
            }
            if call == usize::from(self.nested) {
                return gated_final(
                    CHILD_RESULT,
                    self.child_waiting.clone(),
                    self.finish_child.clone(),
                    signal,
                );
            }
            return Box::pin(futures::stream::iter(final_events(FOLLOWUP_RESULT)));
        }
        self.root_requests.lock().unwrap().push(request);
        let call = self.root_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(futures::stream::iter(if call == 0 {
            task_events("spawn-child", "mailbox child", CHILD_SEED, true)
        } else {
            final_events("mailbox root final")
        }))
    }
}

struct Fixture {
    process: RuntimeProcessContext,
    session: Arc<Session>,
    provider: Arc<MailboxProvider>,
    store: Arc<JsonlSessionStore>,
    directory: PathBuf,
    session_id: String,
}

impl Fixture {
    async fn new(nested: bool) -> Self {
        let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
        let provider = Arc::new(MailboxProvider {
            info: info(),
            nested,
            root_calls: AtomicUsize::new(0),
            child_calls: AtomicUsize::new(0),
            grandchild_calls: AtomicUsize::new(0),
            root_requests: Mutex::new(Vec::new()),
            child_requests: Mutex::new(Vec::new()),
            child_waiting: Arc::new(Semaphore::new(0)),
            grandchild_waiting: Arc::new(Semaphore::new(0)),
            finish_child: Arc::new(Semaphore::new(0)),
            finish_grandchild: Arc::new(Semaphore::new(0)),
        });
        let mut options = agent_options(process.clone());
        options.provider = provider.clone();
        options.tools = Some(vec![create_task_tool(TaskToolOptions::default())]);
        let config = options.config.as_mut().unwrap();
        config["model"] = json!(MODEL);
        config["agentMemory"] = json!(false);
        config["autoMemory"] = json!(false);
        config["swarm"] = json!(false);
        let directory =
            std::env::temp_dir().join(format!("klaus-mailbox-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let directory_host = directory.to_string_lossy().into_owned();
        let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(directory_host.clone()),
            ..Default::default()
        }));
        let session_id = format!("mailbox-root-{}", uuid::Uuid::new_v4());
        let session = Agent::new(options)
            .open_session(
                store.clone(),
                StartSessionOptions {
                    id: Some(session_id.clone().into()),
                    cwd: Some(directory_host.clone().into()),
                    user_config_dir: Some(directory_host.into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        Self {
            process,
            session,
            provider,
            store,
            directory,
            session_id,
        }
    }

    async fn close(self) {
        self.provider.finish_child.add_permits(10);
        self.provider.finish_grandchild.add_permits(10);
        let ids = self
            .session
            .background_agent_summaries()
            .into_iter()
            .map(|entry| entry.id)
            .collect::<Vec<_>>();
        self.session.shutdown_and_wait().await.unwrap();
        self.process.shutdown().await.unwrap();
        for id in ids {
            let output = std::env::temp_dir().join(format!("klaus-agent-bg-{id}.txt"));
            if output.exists() {
                std::fs::remove_file(output).unwrap();
            }
        }
        std::fs::remove_dir_all(self.directory).unwrap();
    }
}

async fn run_root(session: &Arc<Session>, text: &str) {
    tokio::time::timeout(SETUP_TIMEOUT, async {
        let mut events = session
            .run_streamed(Input::Text(text.into()), TurnOptions::default())
            .events;
        let mut completed = false;
        while let Some(event) = events.next().await {
            match event {
                EngineEvent::TurnFailed { error, .. } => panic!("root turn failed: {error:?}"),
                EngineEvent::TurnCompleted { .. } => completed = true,
                _ => {}
            }
        }
        assert!(completed, "the root publishes its completed terminal event");
    })
    .await
    .expect("the root finishes independently of detached descendants");
}

async fn wait_permit(semaphore: &Semaphore, fixture: &Fixture, label: &str) {
    tokio::time::timeout(SETUP_TIMEOUT, semaphore.acquire())
        .await
        .unwrap_or_else(|error| panic!("{label}: {error:?}; root={}, child={}, grandchild={}; lifecycle={:?}; summaries={:?}; child_requests={:?}",
            fixture.provider.root_calls.load(Ordering::SeqCst), fixture.provider.child_calls.load(Ordering::SeqCst),
            fixture.provider.grandchild_calls.load(Ordering::SeqCst), fixture.session.subagent_lifecycle_snapshot(),
            fixture.session.background_agent_summaries(), fixture.provider.child_requests.lock().unwrap()))
        .unwrap()
        .forget();
}

fn subscribe_queue(
    session: &Session,
) -> (
    Box<dyn Fn() + Send + Sync>,
    mpsc::UnboundedReceiver<QueueSnapshot>,
) {
    let (send, receive) = mpsc::unbounded_channel();
    let unsubscribe = session.subscribe_queue_changes(Arc::new(move |snapshot| {
        let _ = send.send(snapshot);
    }));
    (unsubscribe, receive)
}

fn matching_notice(
    snapshot: QueueSnapshot,
    target: Option<&RuntimeString>,
    marker: &str,
) -> Option<QueuedMessage> {
    snapshot
        .items
        .into_iter()
        .map(|item| item.message)
        .find(|message| {
            message.mode == "task-notification"
                && message.agent_id.as_ref() == target
                && matches!(&message.value, QueuedValue::Text(text) if text.contains(marker))
        })
}

async fn wait_notice(
    session: &Session,
    updates: &mut mpsc::UnboundedReceiver<QueueSnapshot>,
    target: Option<&RuntimeString>,
    marker: &str,
) -> QueuedMessage {
    tokio::time::timeout(TIMEOUT, async {
        if let Some(message) = matching_notice(session.queue_snapshot(), target, marker) {
            return message;
        }
        loop {
            let snapshot = updates.recv().await.expect("queue observer remains alive");
            if let Some(message) = matching_notice(snapshot, target, marker) {
                return message;
            }
        }
    })
    .await
    .expect("the completed result is retained in its direct parent's mailbox")
}

fn assert_passive(message: &QueuedMessage) {
    let encoded = runtime_codec::encode_value(message).unwrap();
    assert_eq!(encoded.get("triggerTurn"), Some(&RuntimeJson::Bool(false)));
}

fn send_request(
    target: &RuntimeString,
    message: &str,
    trigger_turn: bool,
) -> SubagentMessageRequest {
    SubagentMessageRequest {
        target: target.clone(),
        message: message.into(),
        summary: message.into(),
        sender_agent_id: None,
        user_initiated: false,
        trigger_turn,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn passive_child_result_waits_for_explicit_root_input_and_is_consumed_once() {
    let fixture = Fixture::new(false).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let (unsubscribe, mut updates) = subscribe_queue(&fixture.session);
        run_root(&fixture.session, "start passive root fixture").await;
        wait_permit(&fixture.provider.child_waiting, &fixture, "child provider").await;
        assert_eq!(fixture.provider.root_calls.load(Ordering::SeqCst), 2);
        fixture.provider.finish_child.add_permits(1);
        let notice = wait_notice(&fixture.session, &mut updates, None, CHILD_RESULT).await;
        assert_passive(&notice);
        assert!(fixture
            .session
            .resume_pending_streamed(TurnOptions::default())
            .await
            .unwrap()
            .is_none());
        assert_eq!(fixture.provider.root_calls.load(Ordering::SeqCst), 2);

        run_root(&fixture.session, "read the finished child").await;
        {
            let requests = fixture.provider.root_requests.lock().unwrap();
            assert_eq!(requests.len(), 3);
            assert_eq!(
                user_text(&requests[2]).matches(CHILD_RESULT).count(),
                1,
                "the first explicit provider request includes the result exactly once"
            );
        }
        assert!(matching_notice(fixture.session.queue_snapshot(), None, CHILD_RESULT).is_none());
        run_root(&fixture.session, "continue after reading the result").await;
        let requests = fixture.provider.root_requests.lock().unwrap();
        assert_eq!(
            user_text(&requests[3]).matches(CHILD_RESULT).count(),
            1,
            "consumed mail stays in history without being appended again"
        );
        unsubscribe();
    })
    .catch_unwind()
    .await;
    fixture.close().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_resumed_child_delivers_the_current_error_without_its_old_success() {
    let fixture = Fixture::new(false).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let (unsubscribe, mut updates) = subscribe_queue(&fixture.session);
        run_root(&fixture.session, "start failed followup fixture").await;
        wait_permit(&fixture.provider.child_waiting, &fixture, "child provider").await;
        fixture.provider.finish_child.add_permits(1);
        wait_notice(&fixture.session, &mut updates, None, CHILD_RESULT).await;
        run_root(&fixture.session, "consume the first completed result").await;
        let child_id = fixture
            .session
            .subagent_lifecycle_snapshot()
            .agents
            .into_iter()
            .find(|record| record.context.depth == 1)
            .unwrap()
            .agent_id;
        let response = fixture
            .session
            .send_subagent_message(send_request(&child_id, "fail-resumed-child", true))
            .await
            .unwrap()
            .unwrap();
        assert!(response.resumed);
        let notice =
            wait_notice(&fixture.session, &mut updates, None, "resumed child failed").await;
        assert_passive(&notice);
        let QueuedValue::Text(body) = notice.value else {
            panic!("a terminal notice contains text");
        };
        assert!(body.contains("failed"));
        assert!(!body.contains(CHILD_RESULT));
        let record = fixture
            .session
            .subagent_lifecycle_snapshot()
            .agents
            .into_iter()
            .find(|record| record.agent_id == child_id)
            .unwrap();
        assert_eq!(record.status, SubagentStatus::Failed);
        assert!(record.last_error.unwrap().contains("resumed child failed"));
        assert!(fixture
            .session
            .resume_pending_streamed(TurnOptions::default())
            .await
            .unwrap()
            .is_none());
        assert_eq!(fixture.provider.root_calls.load(Ordering::SeqCst), 3);
        unsubscribe();
    })
    .catch_unwind()
    .await;
    fixture.close().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn completed_parent_retains_grandchild_result_until_explicit_followup() {
    let fixture = Fixture::new(true).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let (unsubscribe, mut updates) = subscribe_queue(&fixture.session);
        run_root(&fixture.session, "start nested passive fixture").await;
        wait_permit(&fixture.provider.child_waiting, &fixture, "child provider").await;
        wait_permit(
            &fixture.provider.grandchild_waiting,
            &fixture,
            "grandchild provider",
        )
        .await;
        fixture.provider.finish_child.add_permits(1);
        assert_passive(&wait_notice(&fixture.session, &mut updates, None, CHILD_RESULT).await);
        let records = fixture.session.subagent_lifecycle_snapshot().agents;
        let child = records
            .iter()
            .find(|record| record.context.depth == 1)
            .unwrap();
        let grandchild = records
            .iter()
            .find(|record| record.context.depth == 2)
            .unwrap();
        let child_id = child.agent_id.clone();
        let grandchild_id = grandchild.agent_id.clone();
        let child_spawn_turn = grandchild.context.spawn_turn_id.clone().unwrap();
        assert_eq!(child.status, SubagentStatus::Completed);
        assert_eq!(grandchild.status, SubagentStatus::Running);
        assert_eq!(grandchild.context.parent_agent_id.as_ref(), Some(&child_id));

        fixture.provider.finish_grandchild.add_permits(1);
        let notice = wait_notice(
            &fixture.session,
            &mut updates,
            Some(&child_id),
            GRANDCHILD_RESULT,
        )
        .await;
        assert_passive(&notice);
        assert!(
            matching_notice(fixture.session.queue_snapshot(), None, GRANDCHILD_RESULT).is_none()
        );
        assert!(fixture
            .session
            .resume_pending_streamed(TurnOptions::default())
            .await
            .unwrap()
            .is_none());
        assert_eq!(fixture.provider.root_calls.load(Ordering::SeqCst), 2);
        assert_eq!(fixture.provider.child_calls.load(Ordering::SeqCst), 2);
        assert_eq!(fixture.provider.grandchild_calls.load(Ordering::SeqCst), 1);

        let passive = fixture
            .session
            .send_subagent_message(send_request(&child_id, "queued passive direction", false))
            .await
            .unwrap()
            .unwrap();
        assert!(!passive.resumed);
        assert_eq!(fixture.provider.child_calls.load(Ordering::SeqCst), 2);
        let followup = fixture
            .session
            .send_subagent_message(send_request(&child_id, "explicit child followup", true))
            .await
            .unwrap()
            .unwrap();
        assert!(followup.resumed);
        assert_passive(&wait_notice(&fixture.session, &mut updates, None, FOLLOWUP_RESULT).await);
        {
            let requests = fixture.provider.child_requests.lock().unwrap();
            assert_eq!(requests.len(), 3);
            let first_resumed = user_text(&requests[2]);
            assert_eq!(first_resumed.matches(GRANDCHILD_RESULT).count(), 1);
            assert!(first_resumed.contains("queued passive direction"));
            assert!(first_resumed.contains("explicit child followup"));
        }
        assert!(matching_notice(
            fixture.session.queue_snapshot(),
            Some(&child_id),
            GRANDCHILD_RESULT
        )
        .is_none());
        assert_eq!(fixture.provider.root_calls.load(Ordering::SeqCst), 2);

        let transcript = fixture
            .store
            .load_sub_agent(&fixture.session_id, &child_id.try_to_utf8().unwrap())
            .await
            .unwrap();
        let index = transcript
            .turn_index
            .expect("the direct parent has persisted turn metadata");
        let receipts = index
            .agent_notifications
            .iter()
            .filter(|receipt| receipt.agent_id.as_ref() == Some(&grandchild_id))
            .collect::<Vec<_>>();
        assert_eq!(
            receipts.len(),
            1,
            "a late descendant receipt belongs to one direct-parent transcript"
        );
        assert_eq!(receipts[0].turn_id.as_ref(), Some(&child_spawn_turn));
        unsubscribe();
    })
    .catch_unwind()
    .await;
    fixture.close().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

struct DepthProvider {
    info: ProviderInfo,
    calls: Vec<AtomicUsize>,
    receipts: Mutex<Vec<(usize, bool, String)>>,
    identities: Mutex<Vec<(usize, RuntimeString)>>,
}

fn task_receipt(request: &ModelRequest, id: &str) -> Option<(bool, String)> {
    let text = |content: &[OutputBlock]| {
        content
            .iter()
            .filter_map(|block| match block {
                OutputBlock::Text(text) => Some(text.text.try_to_utf8().unwrap()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    request.messages.iter().find_map(|message| match message {
        Message::ToolResult(result) if result.tool_use_id == id => {
            Some((result.is_error, text(&result.content)))
        }
        Message::User(user) => user.content.iter().find_map(|block| match block {
            ContentBlock::ToolResult(result) if result.tool_use_id == id => {
                Some((result.is_error == Some(true), text(&result.content)))
            }
            _ => None,
        }),
        _ => None,
    })
}

impl ModelProvider for DepthProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }
    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        capabilities()
    }
    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        let depth = (0..self.calls.len())
            .rev()
            .find(|depth| has_directive(&request, &format!("mailbox-depth-{depth}")))
            .unwrap();
        assert_eq!(request.model, MODEL);
        assert!(
            request.tools.iter().any(|tool| tool.name == "task"),
            "legacy depth must not remove an admitted worker's task tool"
        );
        assert!(
            !request
                .tools
                .iter()
                .any(|tool| tool.name == "write" || tool.name == "edit"),
            "nesting must not add file tools outside the parent's tool pool"
        );
        let call = self.calls[depth].fetch_add(1, Ordering::SeqCst);
        if depth > 0 {
            let identity = request.cache_key.clone().expect("child cache identity");
            let mut identities = self.identities.lock().unwrap();
            if let Some((_, previous)) = identities.iter().find(|(level, _)| *level == depth) {
                assert_eq!(&identity, previous, "one worker owns both requests");
            } else {
                assert_eq!(call, 0);
                assert!(identities.iter().all(|(_, previous)| previous != &identity));
                identities.push((depth, identity));
            }
        }
        let id = format!("spawn-depth-{}", depth + 1);
        Box::pin(futures::stream::iter(
            if call == 0 && depth + 1 < self.calls.len() {
                task_events(
                    &id,
                    "bounded nested child",
                    &format!("mailbox-depth-{}", depth + 1),
                    false,
                )
            } else {
                if let Some((is_error, text)) = task_receipt(&request, &id) {
                    self.receipts.lock().unwrap().push((depth, is_error, text));
                }
                final_events("bounded depth final")
            },
        ))
    }
}

async fn depth_fixture(configured: Option<usize>, accepted: usize) {
    let legacy_limit = configured.unwrap_or_else(|| {
        klaus_agent_core::schemas::config::ResolvedConfig::default().max_sub_agent_depth as usize
    });
    assert!(
        accepted > legacy_limit,
        "the fixture must execute beyond the legacy depth value"
    );
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let provider = Arc::new(DepthProvider {
        info: info(),
        calls: (0..accepted + 2).map(|_| AtomicUsize::new(0)).collect(),
        receipts: Mutex::new(Vec::new()),
        identities: Mutex::new(Vec::new()),
    });
    let mut options = agent_options(process.clone());
    options.provider = provider.clone();
    options.tools = Some(vec![create_task_tool(TaskToolOptions::default())]);
    let config = options.config.as_mut().unwrap();
    config["model"] = json!(MODEL);
    config["agentMemory"] = json!(false);
    config["autoMemory"] = json!(false);
    config["maxConcurrentSubAgents"] = json!(accepted);
    config["maxConcurrentThreadsPerSession"] = json!(accepted + 1);
    if let Some(limit) = configured {
        config["maxSubAgentDepth"] = json!(limit);
    }
    let directory =
        std::env::temp_dir().join(format!("klaus-mailbox-depth-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let directory_host = directory.to_string_lossy().into_owned();
    let session = Agent::new(options).start_session(StartSessionOptions {
        cwd: Some(directory_host.clone().into()),
        user_config_dir: Some(directory_host.into()),
        ..Default::default()
    });
    let outcome = std::panic::AssertUnwindSafe(async {
        run_root(&session, "mailbox-depth-0").await;
        for depth in 0..=accepted {
            assert_eq!(
                provider.calls[depth].load(Ordering::SeqCst),
                2,
                "depth {depth} must run and handle its actual task receipt"
            );
        }
        assert_eq!(
            provider.calls[accepted + 1].load(Ordering::SeqCst),
            0,
            "the shared execution pool rejects the next child before inference"
        );
        let receipts = provider.receipts.lock().unwrap();
        assert_eq!(receipts.len(), accepted + 1);
        assert_eq!(
            receipts
                .iter()
                .map(|(depth, _, _)| *depth)
                .collect::<std::collections::HashSet<_>>(),
            (0..=accepted).collect::<std::collections::HashSet<_>>()
        );
        for (depth, is_error, text) in receipts.iter() {
            assert_eq!(
                *is_error,
                *depth == accepted,
                "depth {depth} result: {text}"
            );
            if *depth == accepted {
                assert!(
                    text.contains(&format!(
                        "Sub-agent execution capacity reached (maximum {accepted})."
                    )),
                    "{text}"
                );
            } else {
                assert!(text.contains("bounded depth final"), "{text}");
            }
        }
        drop(receipts);
        let identities = provider.identities.lock().unwrap();
        assert_eq!(identities.len(), accepted);
        let ids = (1..=accepted)
            .map(|depth| {
                identities
                    .iter()
                    .find(|(level, _)| *level == depth)
                    .expect("every admitted depth has an actual provider identity")
                    .1
                    .clone()
            })
            .collect::<Vec<_>>();
        drop(identities);
        let records = session.subagent_lifecycle_snapshot().agents;
        assert_eq!(
            records.len(),
            accepted,
            "rejected admission creates no worker"
        );
        for (index, id) in ids.iter().enumerate() {
            let record = records
                .iter()
                .find(|record| &record.agent_id == id)
                .unwrap();
            assert_eq!(record.context.depth as usize, index + 1);
            assert_eq!(record.status, SubagentStatus::Completed);
            assert_eq!(
                record.context.parent_agent_id.as_ref(),
                index.checked_sub(1).map(|parent| &ids[parent])
            );
        }
    })
    .catch_unwind()
    .await;
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_legacy_depth_allows_four_levels_until_execution_capacity_rejects_the_fifth() {
    depth_fixture(None, 4).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn configured_legacy_depth_allows_five_levels_until_execution_capacity_rejects_the_sixth() {
    depth_fixture(Some(4), 5).await;
}
