//! Defect: a reconstructed goal could reuse stale time, usage or safety baselines.
//! Root cause: persisted policy and process-local turn observations were conflated.
//! Work item: 20261017 persistent goal restart accounting and fault protection.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::deps::NoopCompactor;
use klaus_agent_core::engine::session::{Session, TurnOptions};
use klaus_agent_core::goal::runtime::GoalRuntime;
use klaus_agent_core::goal::types::{GoalUpdate, ThreadGoal, ThreadGoalStatus};
use klaus_agent_core::runtime_process::{
    remove_process_environment_var, set_process_environment_var, RuntimeProcessContext,
};
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeString};
use klaus_agent_core::schemas::message::Usage;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{TerminalReason, ToolError};
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::json::{Json, JsonSchema};
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};
use serde_json::json;
use tokio::sync::Notify;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

const DEADLINE: Duration = Duration::from_secs(15);
const OFFLINE_MILLIS: i64 = 30 * 24 * 60 * 60 * 1_000;

struct Home {
    path: PathBuf,
    previous: Option<std::ffi::OsString>,
}
impl Home {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("klaus-goal-restart-{}", uuid::Uuid::new_v4()));
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

fn usage(input: u64, output: u64) -> Usage {
    Usage {
        input_tokens: input,
        output_tokens: output,
        cache_read_input_tokens: 17,
        cache_creation_input_tokens: 11,
        total_tokens: Some(999),
    }
}

fn runtime(thread: &str, wall: Arc<AtomicI64>, elapsed: Arc<AtomicI64>) -> Arc<GoalRuntime> {
    Arc::new(GoalRuntime::with_clock(
        thread.into(),
        Arc::new(|| uuid::Uuid::new_v4().to_string()),
        Arc::new(move || wall.load(Ordering::SeqCst)),
        Arc::new(move || elapsed.load(Ordering::SeqCst)),
        None,
        None,
    ))
}

fn metrics(goal: &ThreadGoal) -> (i64, i64) {
    (goal.tokens_used, goal.time_used_seconds)
}

#[tokio::test]
async fn rebuilt_runtime_bills_only_new_active_time_and_usage_with_fractional_flush_carry() {
    let _home = Home::new();
    let wall = Arc::new(AtomicI64::new(1_000));
    let before_clock = Arc::new(AtomicI64::new(50_000));
    let before = runtime("rebuilt-accounting", wall.clone(), before_clock.clone());
    let created = before
        .create_host_goal("Finish the full authorized work.".into(), None, None, None)
        .await
        .unwrap()
        .unwrap()
        .goal;
    before.on_turn_start().await.unwrap();
    before.on_token_usage(&usage(7, 3));
    before_clock.store(50_600, Ordering::SeqCst);
    before.flush_progress().await.unwrap();
    assert_eq!(metrics(&before.goal().await.unwrap().unwrap()), (10, 0));
    before_clock.store(51_000, Ordering::SeqCst);
    before.flush_progress().await.unwrap();
    before.on_turn_end(false).await.unwrap();
    before.pause_for_shutdown().await.unwrap();
    before.flush_stopped_progress().await.unwrap();
    let persisted = before.goal().await.unwrap().unwrap();
    assert_eq!(metrics(&persisted), (10, 1));
    assert_eq!(persisted.status, ThreadGoalStatus::Paused);
    drop(before);

    wall.fetch_add(OFFLINE_MILLIS, Ordering::SeqCst);
    let after_clock = Arc::new(AtomicI64::new(9_000_000));
    let after = runtime("rebuilt-accounting", wall.clone(), after_clock.clone());
    after.recover_interrupted_run().await.unwrap();
    assert_eq!(
        metrics(&after.goal().await.unwrap().unwrap()),
        metrics(&persisted)
    );
    after_clock.store(9_300_000, Ordering::SeqCst);
    after.flush_progress().await.unwrap();
    assert_eq!(
        metrics(&after.goal().await.unwrap().unwrap()),
        (10, 1),
        "paused reopen time is never pursuit time"
    );
    after
        .apply_host_update(GoalUpdate {
            status: Some(ThreadGoalStatus::Active),
            ..Default::default()
        })
        .await
        .unwrap()
        .unwrap();
    after.on_turn_start().await.unwrap();
    after.on_token_usage(&usage(3, 2));
    after_clock.store(9_300_600, Ordering::SeqCst);
    after.flush_progress().await.unwrap();
    assert_eq!(metrics(&after.goal().await.unwrap().unwrap()), (15, 1));
    after_clock.store(9_301_000, Ordering::SeqCst);
    after.flush_progress().await.unwrap();
    assert_eq!(
        metrics(&after.goal().await.unwrap().unwrap()),
        (15, 2),
        "600ms and 400ms flushes retain one second without reusing old usage"
    );
    after.flush_progress().await.unwrap();
    after.on_turn_end(false).await.unwrap();
    let final_goal = after.goal().await.unwrap().unwrap();
    assert_eq!(final_goal.goal_id, created.goal_id);
    assert_eq!(final_goal.created_at_ms, created.created_at_ms);
    assert_eq!(metrics(&final_goal), (15, 2));
    let directory =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/goal-outcomes-logs");
    std::fs::create_dir_all(&directory).unwrap();
    let snapshots = runtime_codec::encode_value(&vec![created, persisted, final_goal]).unwrap();
    std::fs::write(
        directory.join("restart-clock-snapshots.json"),
        runtime_json::stringify_pretty(&snapshots).to_node_utf8(),
    )
    .unwrap();
    after.pause_for_shutdown().await.unwrap();
}

enum Reply {
    Text(&'static str),
    Failure,
    InterruptedEmpty(Arc<Notify>),
}
struct Provider {
    info: ProviderInfo,
    script: Mutex<VecDeque<Reply>>,
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
            assert!(requests.len() < 16, "restart requests remain bounded");
            let index = requests.len();
            requests.push(request);
            index
        };
        let reply = self
            .script
            .lock()
            .unwrap()
            .pop_front()
            .expect("every real request has a scripted reply");
        Box::pin(async_stream::stream! {
            yield ModelStreamEvent::MessageStart { model:"goal-restart".into() };
            let stop_reason = match reply {
                Reply::Text(text) => { yield ModelStreamEvent::TextDelta { text:text.into() }; ModelStopReason::EndTurn }
                Reply::Failure => {
                    let id = RuntimeString::from(format!("restart-exec-{index}"));
                    yield ModelStreamEvent::ToolUseStart { id:id.clone(),name:"exec".into(),thought_signature:None,tool_kind:None,input_format:None };
                    yield ModelStreamEvent::ToolUseInputDelta { id:id.clone(),partial_json:"{}".into() };
                    yield ModelStreamEvent::ToolUseStop { id };
                    ModelStopReason::ToolUse
                }
                Reply::InterruptedEmpty(entered) => {
                    yield ModelStreamEvent::UsageSnapshot { usage:usage(8,2) };
                    entered.notify_one(); signal.cancelled().await; return;
                }
            };
            yield ModelStreamEvent::MessageStop { stop_reason,usage:usage(8,2) };
        })
    }
}

struct ExecutionFailure {
    schema: JsonSchema,
    calls: Arc<AtomicUsize>,
}
#[async_trait::async_trait]
impl Tool for ExecutionFailure {
    fn static_name(&self) -> &str {
        "exec"
    }
    fn static_description(&self) -> &str {
        "Executes a controlled failing handler in the restart fixture."
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
        _input: Json,
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(ToolError::new("The actual execution handler failed."))
    }
}

fn session_options(home: &Home) -> StartSessionOptions {
    StartSessionOptions {
        cwd: Some(home.path.to_string_lossy().into_owned().into()),
        user_config_dir: Some(home.path.to_string_lossy().into_owned().into()),
        config: Some(
            json!({"goal":true,"prewarm":false,"agentMemory":false,"autoMemory":false,"loopDetection":false,"maxRetries":0}),
        ),
        ..Default::default()
    }
}

async fn idle(session: &Arc<Session>, provider: &Provider, failure: bool) {
    {
        let mut script = provider.script.lock().unwrap();
        assert!(script.is_empty());
        if failure {
            script.push_back(Reply::Failure);
        }
        script.push_back(Reply::Text(if failure {
            "The failed execution was observed."
        } else {
            " \n\t "
        }));
    }
    let turn = session
        .run_goal_idle_streamed(TurnOptions::default())
        .await
        .unwrap()
        .expect("an active goal admits this actual idle turn");
    let events = tokio::time::timeout(DEADLINE, turn.events.collect::<Vec<_>>())
        .await
        .unwrap();
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
}

#[tokio::test]
async fn resumed_persistent_sessions_start_new_failure_and_interrupted_empty_streaks() {
    for failure in [true, false] {
        let home = Home::new();
        let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
        let wall = Arc::new(AtomicI64::new(1_000));
        let provider = Arc::new(Provider {
            info: ProviderInfo {
                name: "goal-restart".into(),
                base_url: None,
                env_key: None,
                default_model: "goal-restart".into(),
            },
            script: Mutex::new(VecDeque::new()),
            requests: Mutex::new(Vec::new()),
        });
        let executions = Arc::new(AtomicUsize::new(0));
        let mut options = agent_options(process.clone());
        options.provider = provider.clone();
        options.compactor = Some(Arc::new(NoopCompactor));
        options.include_default_agents = Some(false);
        options.tools = Some(vec![Arc::new(ExecutionFailure {
            schema: json!({"type":"object","properties":{},"additionalProperties":false})
                .as_object()
                .unwrap()
                .clone(),
            calls: executions.clone(),
        })]);
        options.deps.now = Some({
            let wall = wall.clone();
            Arc::new(move || wall.load(Ordering::SeqCst))
        });
        let agent = Agent::new(options);
        let store: Arc<dyn SessionStore> =
            Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
                dir: Some(home.path.join("sessions").to_string_lossy().into_owned()),
                archive_dir: None,
            }));
        let session = agent
            .open_session(store.clone(), session_options(&home))
            .await
            .unwrap();
        let thread = session.id.try_to_utf8().unwrap();
        let original = session
            .set_thread_goal(
                Some("Finish the durable restart objective.".into()),
                None,
                None,
            )
            .await
            .unwrap()
            .goal;
        idle(&session, &provider, failure).await;
        if failure {
            idle(&session, &provider, true).await;
        } else {
            let entered = Arc::new(Notify::new());
            provider
                .script
                .lock()
                .unwrap()
                .push_back(Reply::InterruptedEmpty(entered.clone()));
            let turn = session
                .run_goal_idle_streamed(TurnOptions::default())
                .await
                .unwrap()
                .unwrap();
            let task = tokio::spawn(async move { turn.events.collect::<Vec<_>>().await });
            tokio::time::timeout(DEADLINE, entered.notified())
                .await
                .unwrap();
            session.interrupt();
            let events = tokio::time::timeout(DEADLINE, task).await.unwrap().unwrap();
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
        }
        assert_eq!(
            session.thread_goal().await.unwrap().unwrap().status,
            ThreadGoalStatus::Active
        );
        session.shutdown_and_wait().await.unwrap();
        let saved = session.thread_goal().await.unwrap().unwrap();
        assert_eq!(saved.status, ThreadGoalStatus::Paused);
        assert_eq!(saved.goal_id, original.goal_id);
        assert_eq!(saved.tokens_used, if failure { 40 } else { 20 });
        drop(session);

        wall.fetch_add(OFFLINE_MILLIS, Ordering::SeqCst);
        let resumed = agent
            .resume_session(store.clone(), &thread, session_options(&home))
            .await
            .unwrap();
        let restored = resumed.thread_goal().await.unwrap().unwrap();
        assert_eq!(
            metrics(&restored),
            metrics(&saved),
            "reopening cannot charge offline time or old usage"
        );
        let active_since = Instant::now();
        resumed
            .set_thread_goal(None, Some("active".into()), None)
            .await
            .unwrap();
        for index in 0..3 {
            idle(&resumed, &provider, failure).await;
            let current = resumed.thread_goal().await.unwrap().unwrap();
            assert_eq!(current.goal_id, original.goal_id);
            assert_eq!(
                current.status,
                if index == 2 {
                    ThreadGoalStatus::Blocked
                } else {
                    ThreadGoalStatus::Active
                },
                "the old process's two-turn streak is not restored"
            );
            assert_eq!(
                current.tokens_used,
                saved.tokens_used + (index + 1) * if failure { 20 } else { 10 }
            );
            assert!(
                current.time_used_seconds
                    <= saved.time_used_seconds + active_since.elapsed().as_secs() as i64 + 1,
                "only post-resume elapsed time belongs to this active interval: {current:?}"
            );
        }
        assert!(resumed
            .run_goal_idle_streamed(TurnOptions::default())
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            executions.load(Ordering::SeqCst),
            if failure { 5 } else { 0 }
        );
        assert_eq!(
            provider.requests.lock().unwrap().len(),
            if failure { 10 } else { 5 }
        );
        let directory =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/goal-outcomes-logs");
        std::fs::create_dir_all(&directory).unwrap();
        let snapshots = runtime_codec::encode_value(&vec![
            saved,
            restored,
            resumed.thread_goal().await.unwrap().unwrap(),
        ])
        .unwrap();
        std::fs::write(
            directory.join(format!(
                "restart-{}-snapshots.json",
                if failure { "failure" } else { "empty" }
            )),
            runtime_json::stringify_pretty(&snapshots).to_node_utf8(),
        )
        .unwrap();
        resumed.shutdown_and_wait().await.unwrap();
        process.shutdown().await.unwrap();
    }
}
