//! Defect: goal fault protection classified results without observing actual execution.
//! Root cause: failure and empty-turn audits lost handler eligibility and idle identity.
//! Work item: 20261017 persistent goal outcome and feature transitions.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::deps::NoopCompactor;
use klaus_agent_core::engine::session::{Input, ReviewRequest, ReviewTarget, Session, TurnOptions};
use klaus_agent_core::goal::types::ThreadGoalStatus;
use klaus_agent_core::runtime_process::{
    remove_process_environment_var, set_process_environment_var, RuntimeProcessContext,
};
use klaus_agent_core::runtime_values::{
    runtime_codec, runtime_json, RuntimeJson, RuntimeMap, RuntimeString,
};
use klaus_agent_core::schemas::content::OutputBlock;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::tools::build_tool::text_result;
use klaus_agent_core::tools::builtins::bash::create_bash_tool;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{TerminalReason, ToolError};
use klaus_agent_core::types::events::{EngineEvent, ThreadItem};
use klaus_agent_core::types::json::{Json, JsonSchema};
use klaus_agent_core::types::permission::{PermissionReason, PermissionResult};
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::{
    ElicitHandler, ElicitRequest, ElicitResponse, Tool, ToolContext, ToolProgress, ToolResult,
    ValidationResult,
};
use serde_json::{json, Value};
use tokio::sync::Notify;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

const DEADLINE: Duration = Duration::from_secs(10);

struct Home {
    path: PathBuf,
    previous: Option<std::ffi::OsString>,
}

impl Home {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("klaus-goal-outcomes-{}", uuid::Uuid::new_v4()));
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
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

#[derive(Clone)]
struct ReviewRootState {
    identity: klaus_agent_core::goal::runtime::GoalRequestIdentity,
    generation: Option<RuntimeString>,
    goal_context_turn: bool,
}

impl ReviewRootState {
    fn capture(session: &Session) -> Self {
        let runtime = session.goal_runtime().unwrap();
        assert!(
            !runtime.turn_in_flight(),
            "review cannot admit a root goal turn"
        );
        Self {
            identity: runtime.request_identity(),
            generation: runtime.active_generation(),
            goal_context_turn: runtime.is_goal_context_turn(),
        }
    }

    fn assert_preserved(&self, session: &Session) {
        let actual = Self::capture(session);
        assert_eq!(actual.generation, self.generation);
        assert_eq!(actual.goal_context_turn, self.goal_context_turn);
        assert_eq!(actual.identity.generation, self.identity.generation);
        assert_eq!(actual.identity.intent, self.identity.intent);
        assert_eq!(actual.identity.durable_intent, self.identity.durable_intent);
        assert_eq!(actual.identity.pursuit_epoch, self.identity.pursuit_epoch);
        assert_eq!(
            actual.identity.accounting_enabled,
            self.identity.accounting_enabled
        );
        assert_eq!(actual.identity.goal_work, self.identity.goal_work);
        assert_eq!(
            actual.identity.provider_admitted,
            self.identity.provider_admitted
        );
    }
}

enum Reply {
    Text(&'static str),
    ReasoningSummary,
    ReviewProbe {
        session: std::sync::Weak<Session>,
        root_before: ReviewRootState,
    },
    Tool(&'static str, Value),
    HeldTool {
        entered: Arc<Notify>,
        release: Arc<tokio::sync::Semaphore>,
        name: &'static str,
        input: Value,
    },
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
            thinking: true,
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

    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        let index = {
            let mut requests = self.requests.lock().unwrap();
            assert!(
                requests.len() < 64,
                "the bounded fixture cannot spend forever"
            );
            let index = requests.len();
            requests.push(request);
            index
        };
        let reply = self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("every real model request needs an explicit reply");
        Box::pin(async_stream::stream! {
            yield ModelStreamEvent::MessageStart { model: "goal-outcomes".into() };
            let reply = if let Reply::HeldTool { entered, release, name, input } = reply {
                entered.notify_one();
                release.acquire().await.unwrap().forget();
                Reply::Tool(name, input)
            } else { reply };
            let stop_reason = match reply {
                Reply::Text(text) => {
                    yield ModelStreamEvent::TextDelta { text: text.into() };
                    ModelStopReason::EndTurn
                }
                Reply::ReviewProbe { session, root_before } => {
                    let session = session.upgrade().expect("the review owner remains live");
                    root_before.assert_preserved(&session);
                    yield ModelStreamEvent::TextDelta {
                        text: "The requested review found no actionable defect.".into(),
                    };
                    ModelStopReason::EndTurn
                }
                Reply::ReasoningSummary => {
                    yield ModelStreamEvent::ThinkingBlock {
                        text: "The current evidence shows measurable progress.".into(),
                        signature: None,
                        encrypted_content: None,
                        is_summary: true,
                    };
                    ModelStopReason::EndTurn
                }
                Reply::Tool(name, input) => {
                    let id = RuntimeString::from(format!("goal-outcome-call-{index}"));
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
                Reply::Hold(entered) => {
                    entered.notify_one();
                    signal.cancelled().await;
                    return;
                }
                Reply::HeldTool { .. } => unreachable!("held requests are released before dispatch"),
            };
            yield ModelStreamEvent::MessageStop {
                stop_reason,
                usage: Usage {
                    input_tokens: 8, output_tokens: 2, cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0, total_tokens: None,
                },
            };
        })
    }
}

struct OutcomeTool {
    name: &'static str,
    schema: JsonSchema,
    calls: Arc<Mutex<Vec<String>>>,
}

fn outcome(input: &Json) -> String {
    input
        .get("outcome")
        .unwrap()
        .as_str()
        .unwrap()
        .try_to_utf8()
        .unwrap()
}

#[async_trait::async_trait]
impl Tool for OutcomeTool {
    fn static_name(&self) -> &str {
        self.name
    }
    fn static_description(&self) -> &str {
        "Produces controlled handler outcomes for the runtime regression fixture."
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
    async fn validate_input(&self, input: &Json, _ctx: &ToolContext) -> Option<ValidationResult> {
        (outcome(input) == "pre_handler").then(|| ValidationResult::Err {
            message: "The fixture rejected input before execution.".into(),
            error_code: None,
        })
    }
    async fn check_permissions(
        &self,
        input: &Json,
        _ctx: &ToolContext,
    ) -> Option<PermissionResult> {
        (outcome(input) == "denied").then(|| PermissionResult::Deny {
            message: "The fixture denies this call before execution.".into(),
            reason: PermissionReason::ToolSelfCheck,
        })
    }
    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let outcome = outcome(&input);
        self.calls
            .lock()
            .unwrap()
            .push(format!("{}:{outcome}", self.name));
        match outcome.as_str() {
            "handler_error" => Err(ToolError::new("The execution handler failed.")),
            "aborted" => {
                ctx.signal.cancel();
                Err(ToolError::new("The execution handler was cancelled."))
            }
            "success" => Ok(text_result(
                RuntimeJson::Null,
                "The operation succeeded.",
                false,
            )),
            other => panic!("unexpected executed outcome: {other}"),
        }
    }
}

struct Answers;
#[async_trait::async_trait]
impl ElicitHandler for Answers {
    async fn elicit(&self, _request: ElicitRequest, _signal: AbortSignal) -> ElicitResponse {
        let mut answers = RuntimeMap::new();
        answers.insert("next_step".into(), vec!["Inspect (Recommended)".into()]);
        ElicitResponse::UserInput {
            answers,
            status: "answered".into(),
        }
    }
}

struct Harness {
    home: Home,
    process: RuntimeProcessContext,
    session: Arc<Session>,
    provider: Arc<Provider>,
    calls: Arc<Mutex<Vec<String>>>,
}

impl Harness {
    async fn new() -> Self {
        Self::with_enabled(true).await
    }

    async fn with_enabled(enabled: bool) -> Self {
        Self::with_outcome_names(enabled, &[]).await
    }

    async fn with_outcome_names(enabled: bool, extra: &[&'static str]) -> Self {
        Self::with_inventory(enabled, extra, false).await
    }

    async fn with_inventory(enabled: bool, extra: &[&'static str], builtin_tools: bool) -> Self {
        let home = Home::new();
        let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
        let provider = Arc::new(Provider {
            info: ProviderInfo {
                name: "goal-outcomes".into(),
                base_url: None,
                env_key: None,
                default_model: "goal-outcomes".into(),
            },
            replies: Mutex::new(VecDeque::new()),
            requests: Mutex::new(Vec::new()),
        });
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut tools: Vec<Arc<dyn Tool>> = if extra.contains(&"bash") {
            Vec::new()
        } else {
            vec![create_bash_tool()]
        };
        for name in ["exec", "observe"].into_iter().chain(extra.iter().copied()) {
            tools.push(Arc::new(OutcomeTool {
                name,
                schema: json!({"type":"object","properties":{"outcome":{"type":"string"}},"required":["outcome"],"additionalProperties":false}).as_object().unwrap().clone(),
                calls: calls.clone(),
            }));
        }
        tools.extend(klaus_agent_core::goal::create_goal_tools());
        let mut options = agent_options(process.clone());
        options.provider = provider.clone();
        options.tools = if builtin_tools { None } else { Some(tools) };
        options.include_default_agents = Some(false);
        options.compactor = Some(Arc::new(NoopCompactor));
        options.elicit_handler = Some(Arc::new(Answers));
        let store: Arc<dyn klaus_agent_core::store::session_store::SessionStore> = Arc::new(
            klaus_agent_core::store::jsonl_store::JsonlSessionStore::new(
                klaus_agent_core::store::jsonl_store::JsonlSessionStoreOptions {
                    dir: Some(home.path.join("sessions").to_string_lossy().into_owned()),
                    archive_dir: None,
                },
            ),
        );
        let session = Agent::new(options)
            .open_session(
                store,
                StartSessionOptions {
                    cwd: Some(home.path.to_string_lossy().into_owned().into()),
                    user_config_dir: Some(home.path.to_string_lossy().into_owned().into()),
                    config: Some(json!({
                    "model":"goal-outcomes", "goal":enabled, "prewarm":false,
                            "autoMemory":false, "agentMemory":false, "loopDetection":false,
                            "requestUserInputDefault":true,
                        })),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        if enabled {
            session
                .set_thread_goal(
                    Some("Complete the authorized objective.".into()),
                    None,
                    None,
                )
                .await
                .unwrap();
        }
        Self {
            home,
            process,
            session,
            provider,
            calls,
        }
    }

    fn script(&self, replies: impl IntoIterator<Item = Reply>) {
        let mut pending = self.provider.replies.lock().unwrap();
        assert!(
            pending.is_empty(),
            "previous turn must consume its complete script"
        );
        pending.extend(replies);
    }

    async fn idle(&self, replies: impl IntoIterator<Item = Reply>) -> Vec<EngineEvent> {
        self.script(replies);
        let turn = self
            .session
            .run_goal_idle_streamed(TurnOptions::default())
            .await
            .unwrap()
            .expect("an active idle goal must admit this turn");
        let events = tokio::time::timeout(DEADLINE, turn.events.collect::<Vec<_>>())
            .await
            .expect("goal outcome must settle within the regression deadline");
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
            "{events:?}"
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, EngineEvent::TurnCompleted { .. })),
            "{events:?}"
        );
        events
    }

    async fn tool(&self, name: &'static str, input: Value) -> Vec<EngineEvent> {
        self.idle([
            Reply::Tool(name, input),
            Reply::Text("Progress was measured."),
        ])
        .await
    }

    async fn failure(&self) {
        self.tool("exec", json!({"outcome":"handler_error"})).await;
    }

    async fn status(&self, expected: ThreadGoalStatus) {
        assert_eq!(
            self.session.thread_goal().await.unwrap().unwrap().status,
            expected
        );
    }

    fn count(&self) -> usize {
        self.provider.requests.lock().unwrap().len()
    }

    async fn shutdown(self, label: &str) {
        let directory =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/goal-outcomes-logs");
        std::fs::create_dir_all(&directory).unwrap();
        let requests =
            runtime_codec::encode_value(&*self.provider.requests.lock().unwrap()).unwrap();
        std::fs::write(
            directory.join(format!("{label}-requests.json")),
            runtime_json::stringify_pretty(&requests).to_node_utf8(),
        )
        .unwrap();
        std::fs::write(
            directory.join(format!("{label}-handlers.json")),
            serde_json::to_vec_pretty(&*self.calls.lock().unwrap()).unwrap(),
        )
        .unwrap();
        self.session.shutdown_and_wait().await.unwrap();
        self.process.shutdown().await.unwrap();
        drop(self.home);
    }
}

#[tokio::test]
async fn qualifying_handler_failures_survive_each_neutral_outcome_until_the_third_turn() {
    for neutral in [
        "exit_one",
        "denied",
        "pre_handler",
        "non_execution_error",
        "aborted",
    ] {
        let harness = Harness::new().await;
        harness.failure().await;
        harness.status(ThreadGoalStatus::Active).await;
        let before = harness.calls.lock().unwrap().len();
        match neutral {
            "exit_one" => {
                let events = harness
                    .tool("bash", json!({"command":"exit 1","timeout":1000}))
                    .await;
                assert!(
                    events.iter().any(|event| matches!(
                        event,
                        EngineEvent::ItemUpdated {
                            item: ThreadItem::ToolCall {
                                tool_name,
                                result: Some(result),
                                ..
                            },
                            ..
                        } if tool_name == "bash" && result.get("exitCode").and_then(RuntimeJson::as_i64) == Some(1)
                    )),
                    "the real command must exit unsuccessfully: {events:?}"
                );
            }
            "denied" | "pre_handler" => {
                let events = harness.tool("exec", json!({"outcome":neutral})).await;
                assert!(
                    events.iter().any(|event| matches!(
                        event,
                        EngineEvent::ItemUpdated {
                            item: ThreadItem::ToolCall { error: Some(_), .. },
                            ..
                        }
                    )),
                    "the rejected call must remain visible as an error: {events:?}"
                );
                assert_eq!(
                    harness.calls.lock().unwrap().len(),
                    before,
                    "{neutral} must never enter the handler"
                );
            }
            "non_execution_error" => {
                harness
                    .tool("observe", json!({"outcome":"handler_error"}))
                    .await;
                assert_eq!(
                    harness.calls.lock().unwrap()[before],
                    "observe:handler_error"
                );
            }
            "aborted" => {
                let events = harness
                    .idle([Reply::Tool("exec", json!({"outcome":"aborted"}))])
                    .await;
                assert!(
                    events.iter().any(|event| matches!(
                        event,
                        EngineEvent::TurnCompleted {
                            stop_reason: TerminalReason::Aborted,
                            ..
                        }
                    )),
                    "{events:?}"
                );
                assert_eq!(harness.calls.lock().unwrap()[before], "exec:aborted");
            }
            _ => unreachable!(),
        }
        harness.status(ThreadGoalStatus::Active).await;
        harness.failure().await;
        harness.status(ThreadGoalStatus::Active).await;
        harness.failure().await;
        harness.status(ThreadGoalStatus::Blocked).await;
        assert!(harness
            .session
            .run_goal_idle_streamed(TurnOptions::default())
            .await
            .unwrap()
            .is_none());
        harness.shutdown(neutral).await;
    }
}

#[tokio::test]
async fn successful_non_execution_tool_resets_failure_streak_and_multiple_failures_count_once() {
    let harness = Harness::new().await;
    harness
        .idle([
            Reply::Tool("exec", json!({"outcome":"handler_error"})),
            Reply::Tool("exec", json!({"outcome":"handler_error"})),
            Reply::Tool("exec", json!({"outcome":"handler_error"})),
            Reply::Text("All three attempts belonged to one turn."),
        ])
        .await;
    harness.status(ThreadGoalStatus::Active).await;
    harness.failure().await;
    harness.status(ThreadGoalStatus::Active).await;
    harness.tool("observe", json!({"outcome":"success"})).await;
    for _ in 0..2 {
        harness.failure().await;
        harness.status(ThreadGoalStatus::Active).await;
    }
    harness.failure().await;
    harness.status(ThreadGoalStatus::Blocked).await;
    assert_eq!(
        harness
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|call| *call == "exec:handler_error")
            .count(),
        7
    );
    harness.shutdown("successful-reset").await;
}

#[tokio::test]
async fn successful_tool_in_the_same_turn_prevents_a_qualifying_failure_turn() {
    let harness = Harness::new().await;
    for _ in 0..3 {
        harness
            .idle([
                Reply::Tool("exec", json!({"outcome":"handler_error"})),
                Reply::Tool("observe", json!({"outcome":"success"})),
                Reply::Text("The successful operation recovered this turn."),
            ])
            .await;
        harness.status(ThreadGoalStatus::Active).await;
    }
    for _ in 0..2 {
        harness.failure().await;
        harness.status(ThreadGoalStatus::Active).await;
    }
    harness.failure().await;
    harness.status(ThreadGoalStatus::Blocked).await;
    harness.shutdown("same-turn-recovery").await;
}

#[tokio::test]
async fn nonblank_text_reasoning_question_and_completed_tool_reset_empty_turn_audit() {
    for activity in ["text", "reasoning_summary", "question", "completed_tool"] {
        let harness = Harness::new().await;
        for _ in 0..2 {
            harness.idle([Reply::Text(" \n\t ")]).await;
            harness.status(ThreadGoalStatus::Active).await;
        }
        let events = match activity {
            "text" => harness.idle([Reply::Text("A concrete result is ready.")]).await,
            "reasoning_summary" => harness.idle([Reply::ReasoningSummary]).await,
            "question" => harness.idle([
                Reply::Tool("request_user_input", json!({"questions":[{
                    "id":"next_step","header":"Next step","question":"Which evidence should be inspected?",
                    "options":[{"label":"Inspect (Recommended)","description":"Inspect the current result."},
                        {"label":"Reproduce","description":"Reproduce the reported behavior."}]
                }]})),
                Reply::Text(" \n "),
            ]).await,
            "completed_tool" => harness.idle([
                Reply::Tool("observe", json!({"outcome":"success"})), Reply::Text(" \n "),
            ]).await,
            _ => unreachable!(),
        };
        if activity == "reasoning_summary" {
            assert!(events.iter().any(|event| matches!(event, EngineEvent::ItemCompleted { item: ThreadItem::Reasoning { text, .. }, .. } if !text.is_empty())), "{events:?}");
        }
        if activity == "question" {
            assert!(events.iter().any(|event| matches!(event, EngineEvent::ItemCompleted { item: ThreadItem::ToolCall { tool_name, .. }, .. } if tool_name == "request_user_input")), "{events:?}");
        }
        for _ in 0..2 {
            harness.idle([Reply::Text("\n\t ")]).await;
            harness.status(ThreadGoalStatus::Active).await;
        }
        harness.idle([Reply::Text(" \n ")]).await;
        harness.status(ThreadGoalStatus::Blocked).await;
        harness.shutdown(activity).await;
    }
}

#[tokio::test]
async fn interrupted_empty_automatic_turn_settles_with_concurrent_idle_observation() {
    let harness = Harness::new().await;
    for _ in 0..2 {
        harness.idle([Reply::Text("\n ")]).await;
    }
    let entered = Arc::new(Notify::new());
    harness.script([Reply::Hold(entered.clone())]);
    let turn = harness
        .session
        .run_goal_idle_streamed(TurnOptions::default())
        .await
        .unwrap()
        .unwrap();
    let task = tokio::spawn(async move { turn.events.collect::<Vec<_>>().await });
    tokio::time::timeout(DEADLINE, entered.notified())
        .await
        .expect("automatic request reached its held provider");
    let runtime = harness.session.goal_runtime().unwrap();
    assert!(runtime.is_automatic_turn());
    let observe = tokio::spawn(async move { runtime.idle_event().await });
    harness.session.interrupt();
    let events = tokio::time::timeout(DEADLINE, task)
        .await
        .expect("interrupted turn must release its lifecycle locks")
        .unwrap();
    tokio::time::timeout(DEADLINE, observe)
        .await
        .expect("idle observation must not invert lifecycle locks")
        .unwrap();
    assert!(
        events.iter().any(|event| matches!(
            event,
            EngineEvent::TurnCompleted {
                stop_reason: TerminalReason::Aborted,
                ..
            }
        )),
        "{events:?}"
    );
    harness.status(ThreadGoalStatus::Blocked).await;
    assert_eq!(harness.count(), 3);
    assert!(harness
        .session
        .run_goal_idle_streamed(TurnOptions::default())
        .await
        .unwrap()
        .is_none());
    harness.shutdown("interrupted-empty").await;
}

#[tokio::test]
async fn queued_input_after_idle_selection_prevents_provider_admission() {
    let harness = Harness::new().await;
    let pending = Arc::new(AtomicBool::new(false));
    let queued = pending.clone();
    let idle = harness
        .session
        .run_goal_idle_streamed(TurnOptions {
            goal_pending_input: Some(Arc::new(move || queued.load(Ordering::SeqCst))),
            ..Default::default()
        })
        .await
        .unwrap()
        .expect("idle selection happens before input arrives");
    pending.store(true, Ordering::SeqCst);
    let events = tokio::time::timeout(DEADLINE, idle.events.collect::<Vec<_>>())
        .await
        .unwrap();
    assert!(
        events.is_empty(),
        "revoked idle admission must not start a turn: {events:?}"
    );
    assert_eq!(harness.count(), 0);
    assert!(!harness.session.goal_runtime().unwrap().turn_in_flight());
    harness.status(ThreadGoalStatus::Active).await;
    harness.script([Reply::Text("The queued human request was handled first.")]);
    let events = harness
        .session
        .run_streamed(
            Input::Text("Inspect this new user request first.".into()),
            TurnOptions::default(),
        )
        .events
        .collect::<Vec<_>>()
        .await;
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
    assert_eq!(harness.count(), 1);
    harness.shutdown("queued-admission").await;
}

#[tokio::test]
async fn live_goal_feature_changes_tools_eligibility_and_idle_admission_together() {
    let harness = Harness::new().await;
    let original = harness.session.thread_goal().await.unwrap().unwrap();
    harness
        .idle([Reply::Text("The enabled goal has made progress.")])
        .await;
    harness.session.set_goal_enabled(false).await.unwrap();
    assert!(harness.session.goal_runtime().is_none());
    assert!(harness
        .session
        .run_goal_idle_streamed(TurnOptions::default())
        .await
        .unwrap()
        .is_none());
    let disabled = harness.session.thread_goal().await.unwrap().unwrap();
    let edited = harness
        .session
        .set_thread_goal(
            Some("The stored objective was refined while disabled.".into()),
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(edited.goal.goal_id, original.goal_id);
    assert!(
        edited.steer.is_none(),
        "a stored edit while disabled cannot steer inference"
    );
    assert!(!edited.turn_in_flight);
    assert_eq!(
        harness.count(),
        1,
        "disabled state edits do not invoke a provider"
    );
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    harness.session.flush_goal_progress().await.unwrap();
    let after_wait = harness.session.thread_goal().await.unwrap().unwrap();
    assert_eq!(
        after_wait.time_used_seconds, disabled.time_used_seconds,
        "disabled editing cannot restart the pursuit clock"
    );
    assert_eq!(after_wait.tokens_used, disabled.tokens_used);
    harness.script([Reply::Text("Ordinary user work remains available.")]);
    harness
        .session
        .run_streamed(
            Input::Text("Inspect this independent request.".into()),
            TurnOptions::default(),
        )
        .events
        .collect::<Vec<_>>()
        .await;
    let disabled_request = harness
        .provider
        .requests
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .clone();
    let disabled_tools = disabled_request
        .tools
        .iter()
        .map(|tool| tool.name.try_to_utf8().unwrap())
        .collect::<Vec<_>>();
    for goal_tool in ["get_goal", "create_goal", "update_goal"] {
        assert!(
            !disabled_tools.iter().any(|name| name == goal_tool),
            "{disabled_tools:?}"
        );
    }
    let thread_id = String::from_utf8(harness.session.id.to_node_utf8()).unwrap();
    let stored = klaus_agent_core::goal::store::get_goal(&thread_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.goal_id, original.goal_id);
    assert_eq!(stored.status, ThreadGoalStatus::Active);
    assert_eq!(
        stored.objective,
        "The stored objective was refined while disabled."
    );
    assert_eq!(stored.time_used_seconds, disabled.time_used_seconds);
    assert_eq!(
        stored.tokens_used, 10,
        "disabled user work does not belong to goal accounting"
    );
    harness.session.set_goal_enabled(true).await.unwrap();
    assert!(harness.session.goal_runtime().is_some());
    assert_eq!(
        harness
            .session
            .thread_goal()
            .await
            .unwrap()
            .unwrap()
            .goal_id,
        original.goal_id
    );
    harness
        .idle([Reply::Text(
            "The same authorized goal continues after re-enabling.",
        )])
        .await;
    let enabled_request = harness
        .provider
        .requests
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .clone();
    let enabled_tools = enabled_request
        .tools
        .iter()
        .map(|tool| tool.name.try_to_utf8().unwrap())
        .collect::<Vec<_>>();
    for goal_tool in ["get_goal", "create_goal", "update_goal"] {
        assert!(
            enabled_tools.iter().any(|name| name == goal_tool),
            "{enabled_tools:?}"
        );
    }
    assert_eq!(
        harness
            .session
            .thread_goal()
            .await
            .unwrap()
            .unwrap()
            .tokens_used,
        20
    );
    assert_eq!(harness.count(), 3);
    harness.shutdown("feature-toggle").await;
}

#[tokio::test]
async fn persistent_session_created_with_goal_feature_off_can_enable_it_without_recreation() {
    let harness = Harness::with_enabled(false).await;
    assert!(harness.session.goal_runtime().is_none());
    assert!(harness.session.thread_goal().await.unwrap().is_none());
    let created = harness
        .session
        .set_thread_goal(
            Some("Complete the newly authorized objective.".into()),
            None,
            None,
        )
        .await
        .unwrap();
    assert!(
        created.steer.is_none(),
        "disabled creation is a stored state mutation"
    );
    assert!(!created.turn_in_flight);
    assert_eq!(harness.count(), 0);
    assert!(harness
        .session
        .run_goal_idle_streamed(TurnOptions::default())
        .await
        .unwrap()
        .is_none());
    let session_id = harness.session.id.clone();
    harness.session.set_goal_enabled(true).await.unwrap();
    assert_eq!(
        harness
            .session
            .thread_goal()
            .await
            .unwrap()
            .unwrap()
            .goal_id,
        created.goal.goal_id
    );
    harness
        .idle([Reply::Text(
            "The newly enabled goal is eligible for ordinary idle work.",
        )])
        .await;
    assert_eq!(harness.session.id, session_id);
    assert_eq!(harness.count(), 1);
    harness.status(ThreadGoalStatus::Active).await;
    let request = harness
        .provider
        .requests
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .clone();
    for goal_tool in ["get_goal", "create_goal", "update_goal"] {
        assert!(
            request.tools.iter().any(|tool| tool.name == goal_tool),
            "{request:?}"
        );
    }
    harness.shutdown("initially-disabled-feature").await;
}

#[tokio::test]
async fn disabled_runtime_rejects_a_goal_tool_from_an_already_admitted_model_request() {
    for (name, input) in [
        ("get_goal", json!({})),
        ("create_goal", json!({"objective":"A different objective"})),
        ("update_goal", json!({"status":"complete"})),
    ] {
        let harness = Harness::new().await;
        let original = harness.session.thread_goal().await.unwrap().unwrap();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        harness.script([Reply::HeldTool {
            entered: entered.clone(),
            release: release.clone(),
            name,
            input,
        }]);
        let turn = harness.session.run_streamed(
            Input::Text("Inspect the current objective.".into()),
            TurnOptions::default(),
        );
        let task = tokio::spawn(async move { turn.events.collect::<Vec<_>>().await });
        tokio::time::timeout(DEADLINE, entered.notified())
            .await
            .expect("the old provider request was actually admitted");
        assert_eq!(harness.count(), 1);
        harness.session.set_goal_enabled(false).await.unwrap();
        release.add_permits(1);
        let events = tokio::time::timeout(DEADLINE, task)
            .await
            .expect("the disabled goal call must settle")
            .unwrap();
        assert!(events.iter().any(|event| matches!(event,
        EngineEvent::ItemUpdated { item: ThreadItem::ToolCall { tool_name, error: Some(_), .. }, .. }
            if tool_name == name
    )), "the actual old goal tool must be refused: {events:?}");
        let stored = harness.session.thread_goal().await.unwrap().unwrap();
        assert_eq!(stored.goal_id, original.goal_id);
        assert_eq!(stored.objective, original.objective);
        assert_eq!(stored.status, ThreadGoalStatus::Active);
        assert_eq!(
            harness.count(),
            1,
            "disabled goal work cannot admit another provider request"
        );
        assert!(harness
            .session
            .run_goal_idle_streamed(TurnOptions::default())
            .await
            .unwrap()
            .is_none());
        harness.shutdown("stale-disabled-model-tool").await;
    }
}

#[cfg(feature = "testing")]
#[tokio::test]
async fn disabling_goal_feature_while_automatic_provider_admission_waits_prevents_the_request() {
    let harness = Harness::new().await;
    let entered = Arc::new(Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let runtime = harness.session.goal_runtime().unwrap();
    runtime.testing_before_automatic_provider_admission(Some(Arc::new({
        let entered = entered.clone();
        let release = release.clone();
        move || {
            let entered = entered.clone();
            let release = release.clone();
            Box::pin(async move {
                entered.notify_one();
                release.acquire().await.unwrap().forget();
            })
        }
    })));
    let idle = harness
        .session
        .run_goal_idle_streamed(TurnOptions::default())
        .await
        .unwrap()
        .unwrap();
    let task = tokio::spawn(async move { idle.events.collect::<Vec<_>>().await });
    tokio::time::timeout(DEADLINE, entered.notified())
        .await
        .expect("automatic work reached the final provider admission barrier");
    tokio::time::timeout(DEADLINE, harness.session.set_goal_enabled(false))
        .await
        .expect("feature mutation must not wait behind an unadmitted request")
        .unwrap();
    release.add_permits(1);
    let _events = tokio::time::timeout(DEADLINE, task)
        .await
        .expect("revoked automatic work must release admission")
        .unwrap();
    assert_eq!(
        harness.count(),
        0,
        "disabling the feature revokes the pending request before inference"
    );
    assert!(!runtime.turn_in_flight());
    harness.status(ThreadGoalStatus::Active).await;
    assert!(harness
        .session
        .run_goal_idle_streamed(TurnOptions::default())
        .await
        .unwrap()
        .is_none());
    harness.shutdown("feature-admission-race").await;
}

#[tokio::test]
async fn only_the_default_exec_handler_error_qualifies_and_alias_failures_preserve_the_streak() {
    for name in [
        "bash",
        "exec_command",
        "shell",
        "computer",
        "computer_use",
        "mcp__foreign__exec",
    ] {
        let harness = Harness::with_outcome_names(true, &[name]).await;
        harness.failure().await;
        for _ in 0..3 {
            let events = harness.tool(name, json!({"outcome":"handler_error"})).await;
            assert!(events.iter().any(|event| matches!(event,
                EngineEvent::ItemUpdated { item: ThreadItem::ToolCall { tool_name, error: Some(_), .. }, .. }
                if tool_name == name)), "the named handler must actually execute and fail: {events:?}");
            harness.status(ThreadGoalStatus::Active).await;
        }
        assert_eq!(
            harness
                .calls
                .lock()
                .unwrap()
                .iter()
                .filter(|call| call.as_str() == format!("{name}:handler_error"))
                .count(),
            3
        );
        harness.failure().await;
        harness.status(ThreadGoalStatus::Active).await;
        harness.failure().await;
        harness.status(ThreadGoalStatus::Blocked).await;
        harness
            .shutdown(&format!("neutral-execution-identity-{name}"))
            .await;
    }
}

#[tokio::test]
async fn model_get_goal_without_a_saved_goal_returns_success_and_explicit_null() {
    let harness = Harness::new().await;
    assert!(harness.session.clear_thread_goal().await.unwrap());
    let before = harness.session.goal_snapshot().await.unwrap();
    assert!(before.goal.is_none());
    harness.script([
        Reply::Tool("get_goal", json!({})),
        Reply::Text("There is no saved goal for this thread."),
    ]);
    let events = tokio::time::timeout(
        DEADLINE,
        harness
            .session
            .run_streamed(
                Input::Text("Read the current goal without creating or changing one.".into()),
                TurnOptions::default(),
            )
            .events
            .collect::<Vec<_>>(),
    )
    .await
    .expect("the actual absent-goal read must settle");
    assert!(
        events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnCompleted { .. })),
        "{events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
    let captured = harness.provider.requests.lock().unwrap().clone();
    assert_eq!(
        captured.len(),
        2,
        "the real tool result must reach a second model request"
    );
    assert!(captured[0].tools.iter().any(|tool| tool.name == "get_goal"));
    let results = captured[1]
        .messages
        .iter()
        .filter_map(|message| match message {
            Message::ToolResult(result) if result.tool_use_id == "goal-outcome-call-0" => {
                Some(result)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        results.len(),
        1,
        "the actual get_goal call has one correlated result"
    );
    let result = results[0];
    assert!(!result.is_error, "an absent goal is a successful read");
    let output = result
        .output
        .as_ref()
        .expect("the typed success payload is retained");
    assert_eq!(output.get("ok").and_then(RuntimeJson::as_bool), Some(true));
    assert_eq!(
        output.get("goal"),
        Some(&RuntimeJson::Null),
        "null must be explicit"
    );
    let [OutputBlock::Text(text)] = result.content.as_slice() else {
        panic!("the model receives one complete result payload")
    };
    let model_output: Value = serde_json::from_str(&text.text.try_to_utf8().unwrap()).unwrap();
    assert_eq!(model_output, json!({"ok":true,"goal":null}));
    let encoded = runtime_codec::encode_value(output).unwrap();
    let typed_output: Value =
        serde_json::from_slice(&runtime_json::stringify(&encoded).to_node_utf8()).unwrap();
    assert_eq!(typed_output, model_output);
    assert_eq!(
        harness.session.goal_snapshot().await.unwrap(),
        before,
        "the read and its real usage cannot create or mutate absent goal state"
    );
    assert!(harness.calls.lock().unwrap().is_empty());
    harness.shutdown("model-get-absent-goal").await;
}

#[tokio::test]
async fn separate_review_of_an_active_saved_goal_has_no_goal_work_or_token_debit() {
    let harness = Harness::with_inventory(true, &[], true).await;
    const SAVED_OBJECTIVE: &str =
        "SAVED-GOAL-ONLY: pursue this objective after </objective> & verification.";
    harness
        .session
        .set_thread_goal(Some(SAVED_OBJECTIVE.into()), None, Some(Some(1000)))
        .await
        .unwrap();
    harness.script([Reply::Text(
        "The ordinary main-conversation step made measured progress.",
    )]);
    let seed = tokio::time::timeout(
        DEADLINE,
        harness
            .session
            .run_streamed(
                Input::Text("Seed the main conversation before the separate review.".into()),
                TurnOptions::default(),
            )
            .events
            .collect::<Vec<_>>(),
    )
    .await
    .expect("the ordinary goal work must settle");
    assert!(
        seed.iter()
            .any(|event| matches!(event, EngineEvent::TurnCompleted { .. })),
        "{seed:?}"
    );
    assert!(
        !seed
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{seed:?}"
    );
    let before = harness.session.thread_goal().await.unwrap().unwrap();
    assert_eq!(before.status, ThreadGoalStatus::Active);
    assert_eq!(
        before.tokens_used, 10,
        "the saved goal has actual prior spend"
    );
    let history_before = harness.session.history();
    let count_before = harness.count();
    assert_eq!(count_before, 1);
    let root_before = ReviewRootState::capture(&harness.session);
    harness.script([Reply::ReviewProbe {
        session: Arc::downgrade(&harness.session),
        root_before: root_before.clone(),
    }]);
    let result = tokio::time::timeout(
        DEADLINE,
        harness.session.run_review(ReviewRequest {
            target: ReviewTarget::Custom {
                instructions: "Review this independent change for actionable defects.".into(),
            },
            user_facing_hint: None,
        }),
    )
    .await
    .expect("the separate review must settle")
    .unwrap();
    assert_eq!(result.stop_reason, TerminalReason::Completed);
    let review_usage = result
        .usage
        .as_ref()
        .expect("the real review reports nonzero usage");
    assert_eq!(review_usage.input_tokens, 8);
    assert_eq!(review_usage.output_tokens, 2);
    assert!(result.output.findings.is_empty());
    assert_eq!(
        result.output.overall_explanation,
        "The requested review found no actionable defect."
    );
    let captured = harness.provider.requests.lock().unwrap().clone();
    assert_eq!(
        captured.len(),
        count_before + 1,
        "review cannot trigger goal continuation"
    );
    let review = &captured[count_before];
    let mut tools = review
        .tools
        .iter()
        .map(|tool| tool.name.try_to_utf8().unwrap())
        .collect::<Vec<_>>();
    tools.sort();
    assert_eq!(
        tools,
        ["bash", "file_search", "glob", "grep", "ls", "read", "think"]
    );
    let [Message::User(user)] = review.messages.as_slice() else {
        panic!("the separate reviewer must receive only its fresh target transcript")
    };
    assert_ne!(user.is_meta, Some(true));
    let [klaus_agent_core::schemas::content::ContentBlock::Text(target)] = user.content.as_slice()
    else {
        panic!("the review target remains one complete user input")
    };
    assert_eq!(
        target.text,
        "Review this independent change for actionable defects."
    );
    let encoded_review = runtime_codec::encode_value(review).unwrap();
    let review_text = runtime_json::stringify(&encoded_review)
        .try_to_utf8()
        .unwrap();
    for forbidden in [
        SAVED_OBJECTIVE,
        "SAVED-GOAL-ONLY:",
        "Seed the main conversation before the separate review.",
        "The ordinary main-conversation step made measured progress.",
        "Current thread goal context for this user turn.",
        "Continue working toward the active thread goal.",
        "The active thread goal has reached its token budget.",
    ] {
        assert!(
            !review_text.contains(forbidden),
            "review inherited goal work or main history: {forbidden}"
        );
    }
    assert!(review.system.iter().any(|part| part.name == "review"));
    assert!(!review
        .system
        .iter()
        .any(|part| part.name == "collaboration-mode" || part.name.starts_with("goal")));
    assert_eq!(harness.session.history(), history_before);
    harness.session.flush_goal_progress().await.unwrap();
    let after = harness.session.thread_goal().await.unwrap().unwrap();
    assert_eq!(after.thread_id, before.thread_id);
    assert_eq!(after.goal_id, before.goal_id);
    assert_eq!(after.objective, before.objective);
    assert_eq!(after.status, ThreadGoalStatus::Active);
    assert_eq!(after.token_budget, before.token_budget);
    assert_eq!(
        after.tokens_used, before.tokens_used,
        "the ten review tokens cannot debit the saved goal"
    );
    assert!(
        after.time_used_seconds >= before.time_used_seconds,
        "the active root clock may advance independently of review work"
    );
    root_before.assert_preserved(&harness.session);
    assert_eq!(harness.count(), count_before + 1);
    assert!(harness.provider.replies.lock().unwrap().is_empty());
    harness.shutdown("active-goal-separate-review").await;
}
