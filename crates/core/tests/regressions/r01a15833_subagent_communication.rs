//! Defect: received directions disappeared live, sibling stops crossed recipients,
//! and child mail could not reach the primary conversation.
//! Root cause: prompt publication and routing excluded applied communication.
//! Work item: 01a15833.

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};
use async_trait::async_trait;
use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, Session, SubagentStreamEvent, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimeString};
use klaus_agent_core::schemas::content::{ContentBlock, HarnessEnvelope};
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::tasks::swarm_store;
use klaus_agent_core::tools::build_tool::text_result;
use klaus_agent_core::tools::builtins::swarm_tools::{
    create_send_message_tool, create_wait_for_teammates_tool,
};
use klaus_agent_core::tools::builtins::task::{create_task_tool, TaskToolOptions};
use klaus_agent_core::tools::builtins::task_control::create_task_output_tool;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::ToolError;
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::json::{Json, JsonSchema};
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::SwarmIdentity;
use klaus_agent_core::types::tool::{
    BackgroundTaskRegistry, SubagentMessageRequest, Tool, ToolContext, ToolProgress, ToolResult,
};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, Semaphore};

const TIMEOUT: Duration = Duration::from_secs(25);
const MODEL: &str = "communication-regression";

fn text(message: &Message) -> String {
    match message {
        Message::User(user) => user
            .content
            .iter()
            .filter_map(|block| match block {
                ContentBlock::Text(text) => Some(text.text.try_to_utf8().unwrap()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}
fn has(request: &ModelRequest, needle: &str) -> bool {
    request
        .messages
        .iter()
        .any(|message| text(message).lines().any(|line| line == needle))
}
fn result(request: &ModelRequest, id: &str) -> bool {
    request.messages.iter().any(|message| match message {
        Message::ToolResult(result) => result.tool_use_id == id,
        Message::User(user) => user.content.iter().any(
            |block| matches!(block, ContentBlock::ToolResult(result) if result.tool_use_id == id),
        ),
        _ => false,
    })
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
fn answer(answer: &str) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::MessageStart {
            model: MODEL.into(),
        },
        ModelStreamEvent::TextDelta {
            text: answer.into(),
        },
        stop(ModelStopReason::EndTurn),
    ]
}
fn call(id: &str, name: &str, input: Value) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::MessageStart {
            model: MODEL.into(),
        },
        ModelStreamEvent::ToolUseStart {
            id: id.into(),
            name: name.into(),
            thought_signature: None,
            tool_kind: None,
            input_format: None,
        },
        ModelStreamEvent::ToolUseInputDelta {
            id: id.into(),
            partial_json: input.to_string().into(),
        },
        ModelStreamEvent::ToolUseStop { id: id.into() },
        stop(ModelStopReason::ToolUse),
    ]
}

struct Entered {
    name: String,
    registry: Arc<dyn BackgroundTaskRegistry>,
}
struct Gate {
    schema: JsonSchema,
    entered: mpsc::UnboundedSender<Entered>,
    release: Arc<Semaphore>,
}
#[async_trait]
impl Tool for Gate {
    fn static_name(&self) -> &str {
        "communication_gate"
    }
    fn static_description(&self) -> &str {
        "Wait for the fixture's next tool boundary."
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_read_only(&self, _: &Json) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _: &Json) -> bool {
        true
    }
    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        assert!(self
            .entered
            .send(Entered {
                name: input
                    .get("name")
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .try_to_utf8()
                    .unwrap(),
                registry: ctx.background_tasks.clone().unwrap(),
            })
            .is_ok());
        tokio::select! {
            permit = self.release.acquire() => permit.unwrap().forget(),
            _ = ctx.signal.cancelled() => {},
        }
        Ok(text_result(RuntimeJson::Null, "Boundary released", false))
    }
}
fn spawn_input(name: &str, persistent: bool) -> Value {
    let mut input = json!({"model": "low", "description":"fixture worker", "prompt":name, "name":name, "fork_turns":"none","run_in_background":true});
    if persistent {
        input["peer"] = json!(name);
        input["persistent"] = json!(true);
    }
    input
}

struct Provider {
    info: ProviderInfo,
    nested: bool,
    persistent: bool,
    requests: Mutex<Vec<ModelRequest>>,
    wait_target: Mutex<Option<String>>,
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
    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        self.requests.lock().unwrap().push(request.clone());
        let events = if has(&request, "worker-a")
            || has(&request, "worker-b")
            || has(&request, "grandchild")
        {
            let name = if has(&request, "grandchild") {
                "grandchild"
            } else if has(&request, "worker-b") {
                "worker-b"
            } else {
                "worker-a"
            };
            if self.nested && name == "worker-a" && !result(&request, "spawn-grandchild") {
                call(
                    "spawn-grandchild",
                    "task",
                    json!({"model": "low", "description":"nested worker", "prompt":"grandchild", "name":"grandchild", "fork_turns":"none","run_in_background":true}),
                )
            } else if !result(&request, "gate") {
                call("gate", "communication_gate", json!({"name":name}))
            } else {
                answer(&format!("{name} finished"))
            }
        } else if has(&request, "root-hold") && !result(&request, "root-gate") {
            call("root-gate", "communication_gate", json!({"name":"root"}))
        } else if has(&request, "root-wait") && !result(&request, "root-wait-tool") {
            call(
                "root-wait-tool",
                "task_output",
                json!({"task_id":self.wait_target.lock().unwrap().as_ref().unwrap(),"block":true,"timeout":600000}),
            )
        } else if has(&request, "root-teammate-wait") && !result(&request, "team-wait-tool") {
            call("team-wait-tool", "WaitForTeammates", json!({}))
        } else if has(&request, "root-seed") && !result(&request, "spawn-a") {
            call("spawn-a", "task", spawn_input("worker-a", self.persistent))
        } else if has(&request, "root-seed") && !result(&request, "spawn-b") && !self.nested {
            call("spawn-b", "task", spawn_input("worker-b", self.persistent))
        } else {
            answer("root finished")
        };
        Box::pin(futures::stream::iter(events))
    }
}
struct Fixture {
    session: Arc<Session>,
    process: RuntimeProcessContext,
    provider: Arc<Provider>,
    store: Arc<JsonlSessionStore>,
    _directory: std::path::PathBuf,
    entered: mpsc::UnboundedReceiver<Entered>,
    release: Arc<Semaphore>,
    prompts: Arc<Mutex<Vec<(String, Message)>>>,
    terminals: mpsc::UnboundedReceiver<String>,
    mail_environment: Option<Option<std::ffi::OsString>>,
}
impl Fixture {
    async fn new(nested: bool) -> Self {
        Self::with_mail(nested, false).await
    }
    async fn with_mail(nested: bool, persistent: bool) -> Self {
        let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
        let directory =
            std::env::temp_dir().join(format!("klaus-communication-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let mail_environment = persistent.then(|| {
            let previous = std::env::var_os("KLAUS_CONFIG_DIR");
            klaus_agent_core::runtime_process::set_process_environment_var(
                "KLAUS_CONFIG_DIR",
                &directory,
            );
            previous
        });
        let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(directory.to_string_lossy().into_owned()),
            ..Default::default()
        }));
        let provider = Arc::new(Provider {
            info: ProviderInfo {
                name: MODEL.into(),
                default_model: MODEL.into(),
                base_url: None,
                env_key: None,
            },
            nested,
            persistent,
            requests: Mutex::new(Vec::new()),
            wait_target: Mutex::new(None),
        });
        let (entered_send, entered) = mpsc::unbounded_channel();
        let release = Arc::new(Semaphore::new(0));
        let mut options = agent_options(process.clone());
        options.provider = provider.clone();
        options.tools = Some(vec![create_task_tool(TaskToolOptions::default()), create_send_message_tool(), create_task_output_tool(), create_wait_for_teammates_tool(), Arc::new(Gate {
            schema: json!({"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}).as_object().unwrap().clone(),
            entered: entered_send, release: release.clone(),
        })]);
        let config = options.config.as_mut().unwrap();
        config["model"] = json!(MODEL);
        config["agentMemory"] = json!(false);
        config["autoMemory"] = json!(false);
        config["fork"] = json!(false);
        config["swarm"] = json!(persistent);
        config["maxSubAgentDepth"] = json!(3);
        let session = Agent::new(options)
            .open_session(
                store.clone(),
                StartSessionOptions {
                    id: Some(format!("communication-{}", uuid::Uuid::new_v4()).into()),
                    swarm: persistent.then(|| SwarmIdentity {
                        team: "communication-team".into(),
                        peer: "team-lead".into(),
                    }),
                    cwd: Some(directory.to_string_lossy().into_owned().into()),
                    user_config_dir: Some(directory.to_string_lossy().into_owned().into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let prompts = Arc::new(Mutex::new(Vec::new()));
        let observed = prompts.clone();
        let (terminal_send, terminals) = mpsc::unbounded_channel();
        session.set_subagent_event_sink(Some(Arc::new(move |id, event| match event {
            SubagentStreamEvent::Prompt(message) => observed
                .lock()
                .unwrap()
                .push((id.to_string(), message.clone())),
            SubagentStreamEvent::Event(EngineEvent::TurnCompleted { .. }) => {
                let _ = terminal_send.send(id.to_string());
            }
            _ => {}
        })));
        Self {
            session,
            process,
            provider,
            store,
            _directory: directory,
            entered,
            release,
            prompts,
            terminals,
            mail_environment,
        }
    }
    async fn run(&self, input: &str) {
        let mut events = self
            .session
            .run_streamed(Input::Text(input.into()), TurnOptions::default())
            .events;
        tokio::time::timeout(TIMEOUT, async {
            while let Some(event) = events.next().await {
                assert!(
                    !matches!(event, EngineEvent::TurnFailed { .. }),
                    "{event:?}"
                );
            }
        })
        .await
        .unwrap();
    }
    async fn workers(&mut self) -> Vec<Entered> {
        self.run("root-seed").await;
        let mut entered = Vec::new();
        for _ in 0..2 {
            entered.push(
                tokio::time::timeout(TIMEOUT, self.entered.recv())
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        entered.sort_by(|a, b| a.name.cmp(&b.name));
        entered
    }
    async fn terminal(&mut self, id: &RuntimeString) {
        tokio::time::timeout(TIMEOUT, async {
            loop {
                if self.terminals.recv().await.unwrap() == id.try_to_utf8().unwrap() {
                    break;
                }
            }
            while self
                .session
                .background_agent_summaries()
                .iter()
                .any(|worker| {
                    worker.id == id.try_to_utf8().unwrap()
                        && worker.status
                            == klaus_agent_core::types::tool::BackgroundTaskStatus::Running
                })
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    async fn close(self) {
        self.release.add_permits(10);
        let workers = self.session.background_agent_summaries();
        self.session.shutdown_and_wait().await.unwrap();
        self.process.shutdown().await.unwrap();
        for worker in workers {
            let _ = std::fs::remove_file(worker.output_file);
        }
        std::fs::remove_dir_all(self._directory).unwrap();
        if let Some(previous) = self.mail_environment {
            match previous {
                Some(value) => klaus_agent_core::runtime_process::set_process_environment_var(
                    "KLAUS_CONFIG_DIR",
                    value,
                ),
                None => klaus_agent_core::runtime_process::remove_process_environment_var(
                    "KLAUS_CONFIG_DIR",
                ),
            }
        }
    }
}
fn message(target: &RuntimeString, body: &str, trigger_turn: bool) -> SubagentMessageRequest {
    SubagentMessageRequest {
        target: target.clone(),
        message: body.into(),
        summary: body.into(),
        trigger_turn,
        sender_agent_id: None,
        user_initiated: false,
    }
}
fn assert_communication(message: &Message, body: &str) -> RuntimeString {
    let Message::User(user) = message else {
        panic!("only actual user messages are published")
    };
    assert_eq!(user.is_meta, Some(true));
    assert!(user.content.iter().all(|block| matches!(block, ContentBlock::Text(text) if text.harness_envelope == Some(HarnessEnvelope::SubagentMessage))));
    assert!(text(message).ends_with(body));
    user.uuid.clone()
}

#[tokio::test]
async fn applied_and_resumed_input_streams_once_with_durable_identity() {
    let mut fixture = Fixture::new(false).await;
    let workers = fixture.workers().await;
    let id = workers[0].registry.caller_agent_id().unwrap();
    fixture
        .session
        .send_subagent_message(message(&id, "during-tools", false))
        .await
        .unwrap()
        .unwrap();
    fixture.release.add_permits(2);
    fixture.terminal(&id).await;
    let before = fixture.provider.requests.lock().unwrap().len();
    let passive = fixture
        .session
        .send_subagent_message(message(&id, "while-idle", false))
        .await
        .unwrap()
        .unwrap();
    assert!(!passive.resumed);
    assert_eq!(fixture.provider.requests.lock().unwrap().len(), before);
    fixture
        .session
        .send_subagent_message(message(&id, "after-completion", true))
        .await
        .unwrap()
        .unwrap();
    fixture.terminal(&id).await;
    fixture
        .session
        .send_subagent_message(message(&id, "second-continuation", true))
        .await
        .unwrap()
        .unwrap();
    fixture.terminal(&id).await;
    let prompts = fixture.prompts.lock().unwrap().clone();
    let own = prompts
        .iter()
        .filter(|(agent, _)| agent == &id.try_to_utf8().unwrap())
        .map(|(_, message)| message)
        .collect::<Vec<_>>();
    assert_eq!(
        own.len(),
        5,
        "one initial directive and four received directions"
    );
    assert_eq!(text(own[0]), "worker-a");
    let loaded = fixture
        .store
        .load_sub_agent(
            &fixture.session.id.try_to_utf8().unwrap(),
            &id.try_to_utf8().unwrap(),
        )
        .await
        .unwrap();
    for (message, body) in own[1..].iter().zip([
        "during-tools",
        "while-idle",
        "after-completion",
        "second-continuation",
    ]) {
        let uuid = assert_communication(message, body);
        assert_eq!(
            loaded
                .messages
                .iter()
                .filter(|message| matches!(message, Message::User(user) if user.uuid == uuid))
                .count(),
            1
        );
    }
    fixture.close().await;
}

#[tokio::test]
async fn individual_stop_preserves_sibling_mail_and_root_interrupt_keeps_workers_independent() {
    use klaus_agent_core::types::tool::BackgroundTaskStatus;

    let mut fixture = Fixture::new(false).await;
    let workers = fixture.workers().await;
    let first = workers[0].registry.caller_agent_id().unwrap();
    let second = workers[1].registry.caller_agent_id().unwrap();
    {
        let from_child =
            workers[0]
                .registry
                .send_message(message(&second, "sender-superseded", false));
        tokio::pin!(from_child);
        assert!(futures::poll!(from_child.as_mut()).is_pending());
        let send =
            fixture
                .session
                .send_subagent_message(message(&second, "sibling-independent", false));
        tokio::pin!(send);
        assert!(futures::poll!(send.as_mut()).is_pending());
        let target_send =
            fixture
                .session
                .send_subagent_message(message(&first, "target-superseded", false));
        tokio::pin!(target_send);
        assert!(futures::poll!(target_send.as_mut()).is_pending());
        assert!(workers[0].registry.stop_by_user(&first));
        assert!(from_child
            .await
            .unwrap()
            .unwrap_err()
            .contains("sender cancellation"));
        assert!(target_send
            .await
            .unwrap()
            .unwrap_err()
            .contains("newer user cancellation"));
        let admitted = send.await.unwrap().unwrap();
        assert_eq!(admitted.agent_id, second);
        assert!(!admitted.resumed);
    }
    assert!(workers[0]
        .registry
        .send_message(message(&second, "already-cancelled-sender", false))
        .await
        .unwrap()
        .is_err());
    assert!(fixture
        .session
        .queued_messages()
        .iter()
        .any(|queued| queued.agent_id.as_ref() == Some(&second)));
    fixture.terminal(&first).await;
    assert_eq!(
        workers[0].registry.get(&first).unwrap().status,
        BackgroundTaskStatus::Interrupted
    );
    assert!(fixture
        .session
        .send_subagent_message(message(&first, "automatic-after-target-stop", true))
        .await
        .unwrap()
        .unwrap_err()
        .contains("cancelled by the user"));
    assert_eq!(
        workers[1].registry.get(&second).unwrap().status,
        BackgroundTaskStatus::Running
    );
    let root = tokio::spawn({
        let session = fixture.session.clone();
        async move {
            session
                .run(Input::Text("root-hold".into()), TurnOptions::default())
                .await
        }
    });
    let root_entered = tokio::time::timeout(TIMEOUT, fixture.entered.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(root_entered.name, "root");
    assert!(root_entered.registry.caller_agent_id().is_none());
    {
        let send =
            fixture
                .session
                .send_subagent_message(message(&second, "root-independent", false));
        tokio::pin!(send);
        assert!(futures::poll!(send.as_mut()).is_pending());
        fixture.session.interrupt();
        let admitted = send.await.unwrap().unwrap();
        assert_eq!(admitted.agent_id, second);
        assert!(!admitted.resumed);
    }
    let outcome = tokio::time::timeout(TIMEOUT, root).await.unwrap().unwrap();
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert_eq!(
        outcome.stop_reason,
        klaus_agent_core::types::errors::TerminalReason::Aborted
    );
    let siblings = fixture.session.background_agent_summaries();
    assert_eq!(siblings.len(), 2);
    assert!(siblings
        .iter()
        .any(|worker| worker.id == first.try_to_utf8().unwrap()
            && worker.status == BackgroundTaskStatus::Interrupted));
    assert!(siblings
        .iter()
        .any(|worker| worker.id == second.try_to_utf8().unwrap()
            && worker.status == BackgroundTaskStatus::Running));
    fixture.release.add_permits(1);
    fixture.terminal(&second).await;
    assert_eq!(
        workers[1].registry.get(&second).unwrap().status,
        BackgroundTaskStatus::Completed
    );
    let loaded = fixture
        .store
        .load_sub_agent(
            &fixture.session.id.try_to_utf8().unwrap(),
            &second.try_to_utf8().unwrap(),
        )
        .await
        .unwrap();
    assert!(loaded.messages.iter().any(|message| matches!(
        message,
        Message::Assistant(assistant) if assistant.content.iter().any(|block| matches!(
            block, ContentBlock::Text(text) if text.text == "worker-b finished"
        ))
    )));
    let prompts = fixture.prompts.lock().unwrap().clone();
    let requests = fixture.provider.requests.lock().unwrap().clone();
    for body in ["sibling-independent", "root-independent"] {
        let received = prompts
            .iter()
            .filter(|(_, message)| text(message).ends_with(body))
            .collect::<Vec<_>>();
        assert_eq!(received.len(), 1, "{body}");
        assert_eq!(received[0].0, second.try_to_utf8().unwrap());
        let uuid = assert_communication(&received[0].1, body);
        assert_eq!(
            loaded
                .messages
                .iter()
                .filter(|message| matches!(
                    message, Message::User(user) if user.uuid == uuid
                ))
                .count(),
            1
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.messages.iter().any(|message| matches!(
                    message, Message::User(user) if user.uuid == uuid
                )))
                .count(),
            1
        );
    }
    for rejected in [
        "sender-superseded",
        "target-superseded",
        "already-cancelled-sender",
        "automatic-after-target-stop",
    ] {
        assert!(!prompts
            .iter()
            .any(|(_, message)| text(message).ends_with(rejected)));
        assert!(!loaded
            .messages
            .iter()
            .any(|message| text(message).ends_with(rejected)));
    }
    fixture.close().await;
}

#[tokio::test]
async fn child_to_root_mail_is_passive_authenticated_and_published_when_applied() {
    let mut fixture = Fixture::new(false).await;
    let workers = fixture.workers().await;
    let child = workers[0].registry.caller_agent_id().unwrap();
    let root = fixture.session.id.clone();
    let before = fixture.provider.requests.lock().unwrap().len();
    workers[0]
        .registry
        .send_message(message(&"parent".into(), "child-root-mail", false))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fixture.provider.requests.lock().unwrap().len(), before);
    assert!(fixture
        .session
        .resume_pending_streamed(TurnOptions::default())
        .await
        .unwrap()
        .is_none());
    let queued = fixture
        .session
        .queued_messages()
        .into_iter()
        .find(|item| item.mode == "subagent-message" && item.agent_id.is_none())
        .unwrap();
    let uuid = queued.uuid.clone().unwrap();
    assert!(!queued.triggers_turn());
    let origin = runtime_json::parse(queued.origin.as_ref().unwrap()).unwrap();
    assert_eq!(
        origin.get("sender"),
        Some(&RuntimeJson::String(child.clone()))
    );
    assert!(workers[0]
        .registry
        .send_message(message(&root, "cannot-start-root", true))
        .await
        .unwrap()
        .is_err());
    assert!(fixture
        .session
        .send_subagent_message(message(&root, "missing-sender", false))
        .await
        .unwrap()
        .is_err());
    fixture.run("read-child-mail").await;
    fixture.run("read-once-more").await;
    let prompts = fixture.prompts.lock().unwrap().clone();
    let received = prompts
        .iter()
        .filter(|(agent, message)| {
            agent == &root.try_to_utf8().unwrap() && text(message).ends_with("child-root-mail")
        })
        .collect::<Vec<_>>();
    assert_eq!(received.len(), 1);
    assert_eq!(
        assert_communication(&received[0].1, "child-root-mail"),
        uuid
    );
    let history = fixture.session.history();
    assert_eq!(
        history
            .iter()
            .filter(|message| matches!(message, Message::User(user) if user.uuid == uuid))
            .count(),
        1
    );
    {
        let stale = workers[0]
            .registry
            .send_message(message(&root, "cancelled-child-mail", false));
        tokio::pin!(stale);
        assert!(futures::poll!(stale.as_mut()).is_pending());
        assert!(workers[0].registry.stop_by_user(&child));
        assert!(stale.await.unwrap().is_err());
    }
    fixture.close().await;
}

#[tokio::test]
async fn nested_child_has_its_own_live_prompt_and_terminal_identity() {
    let mut fixture = Fixture::new(true).await;
    let workers = fixture.workers().await;
    let nested = workers
        .iter()
        .find(|entry| entry.name == "grandchild")
        .unwrap()
        .registry
        .caller_agent_id()
        .unwrap();
    let parent = workers
        .iter()
        .find(|entry| entry.name == "worker-a")
        .unwrap()
        .registry
        .caller_agent_id()
        .unwrap();
    let snapshot = fixture.session.subagent_lifecycle_snapshot();
    let record = snapshot
        .agents
        .iter()
        .find(|record| record.agent_id == nested)
        .unwrap();
    assert_eq!(record.context.parent_agent_id.as_ref(), Some(&parent));
    assert_eq!(record.context.depth, 2);
    let prompts = fixture.prompts.lock().unwrap().clone();
    assert_eq!(
        prompts
            .iter()
            .filter(|(id, message)| id == &nested.try_to_utf8().unwrap()
                && text(message) == "grandchild")
            .count(),
        1
    );
    assert!(
        prompts
            .iter()
            .all(|(id, message)| id != &parent.try_to_utf8().unwrap()
                || text(message) != "grandchild")
    );
    let routed = workers
        .iter()
        .find(|entry| entry.name == "grandchild")
        .unwrap()
        .registry
        .send_message(message(&"parent".into(), "nested-parent-mail", false))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(routed.agent_id, parent);
    let queued = fixture.session.queued_messages().into_iter().find(|item| matches!(&item.value, klaus_agent_core::engine::message_queue::QueuedValue::Text(body) if body == "nested-parent-mail")).unwrap();
    assert_eq!(queued.agent_id.as_ref(), Some(&parent));
    let origin = runtime_json::parse(queued.origin.as_ref().unwrap()).unwrap();
    assert_eq!(
        origin.get("sender"),
        Some(&RuntimeJson::String(nested.clone()))
    );
    fixture.release.add_permits(2);
    fixture.terminal(&nested).await;
    fixture.close().await;
}

#[tokio::test]
async fn child_mail_applies_at_a_running_root_tool_boundary() {
    let mut fixture = Fixture::new(false).await;
    let workers = fixture.workers().await;
    let root = fixture.session.id.clone();
    let running = tokio::spawn({
        let session = fixture.session.clone();
        async move {
            let mut events = session
                .run_streamed(Input::Text("root-hold".into()), TurnOptions::default())
                .events;
            while let Some(event) = events.next().await {
                assert!(
                    !matches!(event, EngineEvent::TurnFailed { .. }),
                    "{event:?}"
                );
            }
        }
    });
    let entered = tokio::time::timeout(TIMEOUT, fixture.entered.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(entered.name, "root");
    assert!(entered.registry.caller_agent_id().is_none());
    workers[0]
        .registry
        .send_message(message(&root, "active-root-mail", false))
        .await
        .unwrap()
        .unwrap();
    fixture.release.add_permits(3);
    tokio::time::timeout(TIMEOUT, running)
        .await
        .unwrap()
        .unwrap();
    let prompts = fixture.prompts.lock().unwrap().clone();
    let received = prompts
        .iter()
        .filter(|(id, message)| {
            id == &root.try_to_utf8().unwrap() && text(message).ends_with("active-root-mail")
        })
        .collect::<Vec<_>>();
    assert_eq!(received.len(), 1);
    let uuid = assert_communication(&received[0].1, "active-root-mail");
    assert!(fixture
        .provider
        .requests
        .lock()
        .unwrap()
        .iter()
        .any(|request| request
            .messages
            .iter()
            .any(|message| matches!(message, Message::User(user) if user.uuid == uuid))));
    assert!(fixture
        .session
        .subagent_lifecycle_snapshot()
        .agents
        .iter()
        .all(|record| record.agent_id != root));
    fixture.close().await;
}

#[tokio::test]
async fn root_wait_yields_for_child_mail_before_the_watched_child_finishes() {
    let mut fixture = Fixture::new(false).await;
    let workers = fixture.workers().await;
    let child = workers[0].registry.caller_agent_id().unwrap();
    let root = fixture.session.id.clone();
    *fixture.provider.wait_target.lock().unwrap() = Some(child.try_to_utf8().unwrap());
    let (waiting_send, waiting_receive) = tokio::sync::oneshot::channel();
    let running = tokio::spawn({
        let session = fixture.session.clone();
        async move {
            let mut started = Some(waiting_send);
            let mut events = session
                .run_streamed(Input::Text("root-wait".into()), TurnOptions::default())
                .events;
            while let Some(event) = events.next().await {
                assert!(
                    !matches!(event, EngineEvent::TurnFailed { .. }),
                    "{event:?}"
                );
                if matches!(&event, EngineEvent::ItemStarted { item: klaus_agent_core::types::events::ThreadItem::ToolCall { id, .. }, .. } if id == "root-wait-tool")
                {
                    if let Some(started) = started.take() {
                        let _ = started.send(());
                    }
                }
            }
        }
    });
    tokio::time::timeout(TIMEOUT, waiting_receive)
        .await
        .unwrap()
        .unwrap();
    workers[0]
        .registry
        .send_message(message(&root, "child-needs-parent-response", false))
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(TIMEOUT, running)
        .await
        .expect("mail wakes the wait before its ten-minute deadline")
        .unwrap();
    assert_eq!(
        workers[0].registry.get(&child).unwrap().status,
        klaus_agent_core::types::tool::BackgroundTaskStatus::Running
    );
    let history = fixture.session.history();
    assert!(history.iter().any(|message| match message {
        Message::ToolResult(result) if result.tool_use_id == "root-wait-tool" =>
            result
                .output
                .as_ref()
                .and_then(|output| output.get("retrieval_status"))
                == Some(&RuntimeJson::from("input_pending")),
        _ => false,
    }));
    assert!(fixture
        .prompts
        .lock()
        .unwrap()
        .iter()
        .any(|(id, message)| id == &root.try_to_utf8().unwrap()
            && text(message).ends_with("child-needs-parent-response")));
    fixture.close().await;
}

async fn mail(peer: &str, body: &str, ts: i64) {
    swarm_store::send_message(
        &"communication-team".into(),
        &peer.into(),
        &"sender-peer".into(),
        &body.into(),
        ts,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn persistent_peer_mail_is_visible_active_and_idle_with_stable_replay_identity() {
    let mut fixture = Fixture::with_mail(false, true).await;
    let workers = fixture.workers().await;
    let id = workers[0].registry.caller_agent_id().unwrap();
    mail("worker-a", "active-peer-body", 1).await;
    fixture.release.add_permits(2);
    tokio::time::timeout(TIMEOUT, async {
        loop {
            if fixture
                .session
                .background_agent_summaries()
                .iter()
                .find(|worker| worker.id == id.try_to_utf8().unwrap())
                .is_some_and(|worker| worker.idle)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    mail("worker-a", "idle-peer-first", 2).await;
    mail("worker-a", "idle-peer-second", 3).await;
    tokio::time::timeout(TIMEOUT, async {
        loop {
            if fixture
                .prompts
                .lock()
                .unwrap()
                .iter()
                .any(|(agent, message)| {
                    agent == &id.try_to_utf8().unwrap()
                        && text(message).ends_with("idle-peer-second")
                })
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let prompts = fixture.prompts.lock().unwrap().clone();
    let mut ids = Vec::new();
    for body in ["active-peer-body", "idle-peer-first", "idle-peer-second"] {
        let received = prompts
            .iter()
            .filter(|(agent, message)| {
                agent == &id.try_to_utf8().unwrap() && text(message).ends_with(body)
            })
            .collect::<Vec<_>>();
        assert_eq!(received.len(), 1, "{body}");
        let uuid = assert_communication(&received[0].1, body);
        assert!(uuid.try_to_utf8().unwrap().starts_with("team-mailbox:"));
        assert!(text(&received[0].1).contains("\"sender\":\"sender-peer\""));
        ids.push(uuid);
    }
    fixture.session.shutdown_and_wait().await.unwrap();
    let loaded = fixture
        .store
        .load_sub_agent(
            &fixture.session.id.try_to_utf8().unwrap(),
            &id.try_to_utf8().unwrap(),
        )
        .await
        .unwrap();
    for uuid in ids {
        assert_eq!(
            loaded
                .messages
                .iter()
                .filter(|message| matches!(message, Message::User(user) if user.uuid == uuid))
                .count(),
            1
        );
    }
    fixture.close().await;
}

#[tokio::test]
async fn persistent_mail_wakes_both_wait_tools_without_consuming_the_receipt_early() {
    let mut fixture = Fixture::with_mail(false, true).await;
    let workers = fixture.workers().await;
    let id = workers[0].registry.caller_agent_id().unwrap();
    *fixture.provider.wait_target.lock().unwrap() = Some(id.try_to_utf8().unwrap());
    for (input, tool_id, body, ts) in [
        ("root-wait", "root-wait-tool", "task-output-peer-input", 1),
        (
            "root-teammate-wait",
            "team-wait-tool",
            "team-wait-peer-input",
            2,
        ),
    ] {
        let session = fixture.session.clone();
        let mut events = session
            .run_streamed(Input::Text(input.into()), TurnOptions::default())
            .events;
        let running = tokio::spawn(async move { while events.next().await.is_some() {} });
        tokio::time::timeout(TIMEOUT, async {
            loop {
                if fixture
                    .provider
                    .requests
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|request| has(request, input) && !result(request, tool_id))
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        mail("team-lead", body, ts).await;
        tokio::time::timeout(TIMEOUT, running)
            .await
            .unwrap()
            .unwrap();
        let requests = fixture.provider.requests.lock().unwrap();
        let applied = requests
            .iter()
            .find(|request| result(request, tool_id) && has(request, body))
            .unwrap();
        let encoded = runtime_json::stringify(
            &klaus_agent_core::runtime_values::runtime_codec::encode_value(&applied.messages)
                .unwrap(),
        )
        .try_to_utf8()
        .unwrap();
        assert!(encoded.contains("input_pending"));
        drop(requests);
        assert_eq!(
            fixture
                .prompts
                .lock()
                .unwrap()
                .iter()
                .filter(
                    |(agent, message)| agent == &fixture.session.id.try_to_utf8().unwrap()
                        && text(message).ends_with(body)
                )
                .count(),
            1
        );
        assert!(fixture
            .session
            .background_agent_summaries()
            .iter()
            .any(|worker| worker.id == id.try_to_utf8().unwrap() && !worker.idle));
    }
    fixture.close().await;
}
