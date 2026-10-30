//! Goal work safety streaks survive read-only planning turns.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::deps::NoopCompactor;
use klaus_agent_core::engine::session::{Input, Session, TurnOptions};
use klaus_agent_core::goal::ThreadGoalStatus;
use klaus_agent_core::planning::CollaborationMode;
use klaus_agent_core::runtime_process::{
    remove_process_environment_var, set_process_environment_var, RuntimeProcessContext,
};
use klaus_agent_core::runtime_values::{RuntimeJson, RuntimeString};
use klaus_agent_core::schemas::message::Usage;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::tools::build_tool::text_result;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{
    EngineErrorCode, ProviderError, ProviderErrorKind, TerminalReason, ToolError,
};
use klaus_agent_core::types::events::{EngineEvent, ItemStatus, ThreadItem};
use klaus_agent_core::types::json::{Json, JsonSchema};
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};
use serde_json::{json, Value};
use tokio::sync::Notify;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

const WAIT: Duration = Duration::from_secs(15);
const PROVIDER_FAILURE: &str = "The planning provider rejected this fixture request.";

struct Home {
    path: PathBuf,
    previous: Option<std::ffi::OsString>,
}
impl Home {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("klaus-plan-goal-safety-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        let previous = std::env::var_os("KLAUS_CONFIG_DIR");
        set_process_environment_var("KLAUS_CONFIG_DIR", &path);
        Self { path, previous }
    }
}
impl Drop for Home {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(previous) => set_process_environment_var("KLAUS_CONFIG_DIR", previous),
            None => remove_process_environment_var("KLAUS_CONFIG_DIR"),
        }
        if std::thread::panicking() {
            eprintln!("Retained planning safety fixture: {}", self.path.display());
        } else {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

enum Reply {
    Text(&'static str),
    Tool(&'static str, Value),
    Failure,
    Hold(Arc<Notify>),
}
struct Provider {
    info: ProviderInfo,
    replies: Mutex<VecDeque<Reply>>,
    requests: Mutex<Vec<ModelRequest>>,
}
impl ModelProvider for Provider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }
    fn capabilities(&self, _model: &RuntimeString) -> ProviderCapabilities {
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
    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        let index = {
            let mut requests = self.requests.lock().unwrap();
            assert!(requests.len() < 32, "provider traces remain bounded");
            let index = requests.len();
            requests.push(request);
            index
        };
        let reply = self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("each actual request needs an explicit fixture reply");
        Box::pin(async_stream::stream! {
            yield ModelStreamEvent::MessageStart { model: "plan-goal-safety".into() };
            let stop_reason = match reply {
                Reply::Text(text) => {
                    yield ModelStreamEvent::TextDelta { text: text.into() };
                    ModelStopReason::EndTurn
                }
                Reply::Tool(name, input) => {
                    let id = RuntimeString::from(format!("plan-safety-call-{index}"));
                    yield ModelStreamEvent::ToolUseStart {
                        id: id.clone(), name: name.into(), thought_signature: None,
                        tool_kind: None, input_format: None,
                    };
                    yield ModelStreamEvent::ToolUseInputDelta {
                        id: id.clone(), partial_json: input.to_string().into(),
                    };
                    yield ModelStreamEvent::ToolUseStop { id };
                    ModelStopReason::ToolUse
                }
                Reply::Failure => {
                    yield ModelStreamEvent::Error {
                        error: ProviderError {
                            kind: ProviderErrorKind::InvalidRequest,
                            message: PROVIDER_FAILURE.into(),
                            status: Some(400),
                            code: None,
                            error_type: None,
                            media_origins: None,
                            media_repair_complete: false,
                            retry_after_ms: None,
                            request_id: None,
                        },
                    };
                    return;
                }
                Reply::Hold(entered) => {
                    entered.notify_one();
                    signal.cancelled().await;
                    return;
                }
            };
            yield ModelStreamEvent::MessageStop {
                stop_reason,
                usage: Usage {
                    input_tokens: 3, output_tokens: 2,
                    cache_read_input_tokens: 0, cache_creation_input_tokens: 0,
                    total_tokens: None,
                },
            };
        })
    }
}

struct Operation {
    name: &'static str,
    schema: JsonSchema,
    calls: Arc<Mutex<Vec<String>>>,
}
#[async_trait::async_trait]
impl Tool for Operation {
    fn static_name(&self) -> &str {
        self.name
    }
    fn static_description(&self) -> &str {
        "Runs a controlled read-only operation for safety accounting."
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        false
    }
    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }
    async fn call(
        &self,
        input: Json,
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let outcome = input
            .get("outcome")
            .unwrap()
            .as_str()
            .unwrap()
            .try_to_utf8()
            .unwrap();
        let mut calls = self.calls.lock().unwrap();
        assert!(calls.len() < 16, "actual handler traces remain bounded");
        calls.push(format!("{}:{outcome}", self.name));
        drop(calls);
        match outcome.as_str() {
            "failure" => Err(ToolError::new("The actual execution handler failed.")),
            "success" => Ok(text_result(
                RuntimeJson::Null,
                "The read-only operation succeeded.",
                false,
            )),
            _ => panic!("the fixture only executes explicit success or failure"),
        }
    }
}

struct Fixture {
    session: Arc<Session>,
    provider: Arc<Provider>,
    calls: Arc<Mutex<Vec<String>>>,
    process: RuntimeProcessContext,
    agent: Agent,
    home: Home,
}
impl Fixture {
    async fn new() -> Self {
        let home = Home::new();
        let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
        let provider = Arc::new(Provider {
            info: ProviderInfo {
                name: "plan-goal-safety".into(),
                base_url: None,
                env_key: None,
                default_model: "plan-goal-safety".into(),
            },
            replies: Mutex::new(VecDeque::new()),
            requests: Mutex::new(Vec::new()),
        });
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut options = agent_options(process.clone());
        options.config.as_mut().unwrap()["permissionMode"] = json!("default");
        options.provider = provider.clone();
        options.tools = Some(
            ["exec", "observe"]
                .into_iter()
                .map(|name| {
                    Arc::new(Operation {
                        name,
                        schema: json!({
                            "type":"object","properties":{"outcome":{"type":"string"}},
                            "required":["outcome"],"additionalProperties":false
                        })
                        .as_object()
                        .unwrap()
                        .clone(),
                        calls: calls.clone(),
                    }) as Arc<dyn Tool>
                })
                .collect(),
        );
        options.include_default_agents = Some(false);
        options.compactor = Some(Arc::new(NoopCompactor));
        options.cwd = Some(home.path.to_string_lossy().into_owned());
        let agent = Agent::new(options);
        let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(home.path.join("sessions").to_string_lossy().into_owned()),
            archive_dir: None,
        }));
        let session = agent
            .open_session(
                store,
                StartSessionOptions {
                    cwd: Some(home.path.to_string_lossy().into_owned().into()),
                    user_config_dir: Some(home.path.to_string_lossy().into_owned().into()),
                    config: Some(json!({
                        "model":"plan-goal-safety","goal":true,"permissionMode":"default",
                        "prewarm":false,"autoMemory":false,"autoDream":false,
                        "agentMemory":false,"memoryRecall":false,"memory":{"enabled":false},
                        "idleMicrocompact":false,"earlyMicrocompact":false,
                        "speculativeCompaction":false,"loopDetection":false,"maxRetries":0
                    })),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        session
            .set_thread_goal(
                Some("Complete the authorized safety fixture.".into()),
                None,
                None,
            )
            .await
            .unwrap();
        Self {
            session,
            provider,
            calls,
            process,
            agent,
            home,
        }
    }
    fn script(&self, replies: impl IntoIterator<Item = Reply>) {
        let mut queued = self.provider.replies.lock().unwrap();
        assert!(
            queued.is_empty(),
            "the previous actual turn consumed its complete script"
        );
        queued.extend(replies);
    }
    fn requests(&self) -> usize {
        self.provider.requests.lock().unwrap().len()
    }
    async fn status(&self, expected: ThreadGoalStatus) {
        assert_eq!(
            self.session.thread_goal().await.unwrap().unwrap().status,
            expected
        );
    }
    async fn mode(&self, mode: CollaborationMode) {
        self.session
            .planning_runtime()
            .set_mode(mode, None)
            .unwrap();
        self.session.synchronize_goal_mode().await.unwrap();
        assert_eq!(self.session.collaboration_mode(), mode);
    }
    async fn automatic(&self, replies: impl IntoIterator<Item = Reply>) -> Vec<EngineEvent> {
        assert_eq!(
            self.session.collaboration_mode(),
            CollaborationMode::Default
        );
        self.script(replies);
        let turn = self
            .session
            .run_goal_idle_streamed(TurnOptions::default())
            .await
            .unwrap()
            .expect("the existing active goal admits actual automatic work");
        let events = tokio::time::timeout(WAIT, turn.events.collect::<Vec<_>>())
            .await
            .expect("the actual automatic turn settles");
        assert_completed(&events);
        events
    }
    async fn ordinary(&self, replies: impl IntoIterator<Item = Reply>) -> Vec<EngineEvent> {
        assert_eq!(self.session.collaboration_mode(), CollaborationMode::Plan);
        self.script(replies);
        tokio::time::timeout(
            WAIT,
            self.session
                .run_streamed(
                    Input::Text("Inspect the current evidence in Plan mode.".into()),
                    TurnOptions::default(),
                )
                .events
                .collect::<Vec<_>>(),
        )
        .await
        .expect("the actual planning turn settles")
    }
    async fn qualifying_failure(&self) {
        let before = self.calls.lock().unwrap().len();
        let events = self
            .automatic([
                Reply::Tool("exec", json!({"outcome":"failure"})),
                Reply::Text("The attempted operation failed."),
            ])
            .await;
        assert_failed_tool(&events, "exec");
        assert_eq!(self.calls.lock().unwrap()[before], "exec:failure");
    }
    async fn planning_activity(&self) {
        self.mode(CollaborationMode::Plan).await;
        let before = self.requests();
        let calls_before = self.calls.lock().unwrap().len();
        let success = self
            .ordinary([
                Reply::Tool("observe", json!({"outcome":"success"})),
                Reply::Text("The inspection produced a concrete result."),
            ])
            .await;
        assert_completed(&success);
        assert!(
            success.iter().any(|event| matches!(event,
                EngineEvent::ItemCompleted {item: ThreadItem::ToolCall {
                    tool_name, status: ItemStatus::Completed, ..}, ..}
                if tool_name == "observe"
            )),
            "{success:?}"
        );
        self.status(ThreadGoalStatus::Active).await;
        let failed = self
            .ordinary([
                Reply::Tool("exec", json!({"outcome":"failure"})),
                Reply::Text("The planning inspection reported the failed operation."),
            ])
            .await;
        assert_completed(&failed);
        assert_failed_tool(&failed, "exec");
        self.status(ThreadGoalStatus::Active).await;
        assert_eq!(
            &self.calls.lock().unwrap()[calls_before..],
            ["observe:success", "exec:failure"]
        );
        let text = self
            .ordinary([Reply::Text("The next verification step is identified.")])
            .await;
        assert_completed(&text);
        assert!(
            text.iter().any(|event| matches!(event,
                EngineEvent::ItemCompleted { item: ThreadItem::AgentMessage {text,..},..}
                if !text.try_to_utf8().unwrap().trim().is_empty()
            )),
            "{text:?}"
        );
        self.status(ThreadGoalStatus::Active).await;
        // Ordinary planning turns reject a response with no visible content.
        // This is a no-content failure, not a successful empty automatic turn.
        let no_content = self.ordinary([Reply::Text(" \n\t ")]).await;
        let failures: Vec<_> = no_content
            .iter()
            .filter_map(|event| match event {
                EngineEvent::TurnFailed { error, .. } => Some(error),
                _ => None,
            })
            .collect();
        assert_eq!(failures.len(), 1, "{no_content:?}");
        assert_eq!(failures[0].code, EngineErrorCode::ProviderError);
        assert_eq!(
            failures[0].message,
            "the model returned no content for this turn"
        );
        assert_eq!(failures[0].provider_kind, None);
        assert!(!no_content
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnCompleted { .. })));
        assert!(
            no_content.iter().any(|event| matches!(event,
                EngineEvent::ItemCompleted { item: ThreadItem::AgentMessage { text, .. }, .. }
                if text.try_to_utf8().unwrap().trim().is_empty()
            )),
            "the actual completed whitespace item must be present: {no_content:?}"
        );
        self.status(ThreadGoalStatus::Active).await;
        let failed = self.ordinary([Reply::Failure]).await;
        assert!(
            failed.iter().any(|event| matches!(event,
                EngineEvent::TurnFailed {error,..}
                if error.code == EngineErrorCode::ProviderError
                && error.provider_kind == Some(ProviderErrorKind::InvalidRequest)
                && error.message.try_to_utf8().unwrap().contains(PROVIDER_FAILURE)
            )),
            "{failed:?}"
        );
        self.status(ThreadGoalStatus::Active).await;

        let entered = Arc::new(Notify::new());
        self.script([Reply::Hold(entered.clone())]);
        let turn = self.session.run_streamed(
            Input::Text("Continue inspecting the evidence without implementation.".into()),
            TurnOptions::default(),
        );
        let task = tokio::spawn(async move { turn.events.collect::<Vec<_>>().await });
        tokio::time::timeout(WAIT, entered.notified())
            .await
            .unwrap();
        self.session.interrupt();
        let aborted = tokio::time::timeout(WAIT, task).await.unwrap().unwrap();
        assert!(
            aborted.iter().any(|event| matches!(
                event,
                EngineEvent::TurnCompleted {
                    stop_reason: TerminalReason::Aborted,
                    ..
                }
            )),
            "{aborted:?}"
        );
        assert!(
            !aborted
                .iter()
                .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
            "actual cancellation must not masquerade as provider failure: {aborted:?}"
        );
        self.status(ThreadGoalStatus::Active).await;
        assert_eq!(self.requests() - before, 8);
        assert!(self.provider.replies.lock().unwrap().is_empty());
        assert!(
            self.session
                .run_goal_idle_streamed(TurnOptions::default())
                .await
                .unwrap()
                .is_none(),
            "the goal remains Active while Plan still refuses automatic work"
        );
        self.mode(CollaborationMode::Default).await;
    }
    async fn finish(self) {
        self.session.shutdown_and_wait().await.unwrap();
        self.process.shutdown().await.unwrap();
        drop(self.agent);
        drop(self.home);
    }
}

fn assert_completed(events: &[EngineEvent]) {
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
    assert!(
        events.iter().any(|event| matches!(
            event,
            EngineEvent::TurnCompleted {
                stop_reason: TerminalReason::Completed,
                ..
            }
        )),
        "{events:?}"
    );
}
fn assert_failed_tool(events: &[EngineEvent], name: &str) {
    assert!(
        events.iter().any(|event| matches!(event,
            EngineEvent::ItemUpdated {item:ThreadItem::ToolCall {
                tool_name,status:ItemStatus::Failed,error:Some(_),..},..}
            if tool_name == name
        )),
        "the actual executed handler must fail visibly: {events:?}"
    );
}

#[tokio::test]
async fn planning_activity_preserves_two_executed_failure_turns_until_the_next_default_failure() {
    let fixture = Fixture::new().await;
    for _ in 0..2 {
        fixture.qualifying_failure().await;
        fixture.status(ThreadGoalStatus::Active).await;
    }
    assert_eq!(fixture.requests(), 4);
    fixture.planning_activity().await;
    fixture.qualifying_failure().await;
    fixture.status(ThreadGoalStatus::Blocked).await;
    assert_eq!(fixture.requests(), 14);
    assert!(fixture
        .session
        .run_goal_idle_streamed(TurnOptions::default())
        .await
        .unwrap()
        .is_none());
    fixture.finish().await;
}

#[tokio::test]
async fn planning_activity_preserves_two_empty_automatic_turns_until_the_next_default_empty() {
    let fixture = Fixture::new().await;
    for _ in 0..2 {
        fixture.automatic([Reply::Text(" \n\t ")]).await;
        fixture.status(ThreadGoalStatus::Active).await;
    }
    assert_eq!(fixture.requests(), 2);
    fixture.planning_activity().await;
    fixture.automatic([Reply::Text("\n\t ")]).await;
    fixture.status(ThreadGoalStatus::Blocked).await;
    assert_eq!(fixture.requests(), 11);
    assert!(fixture
        .session
        .run_goal_idle_streamed(TurnOptions::default())
        .await
        .unwrap()
        .is_none());
    fixture.finish().await;
}
