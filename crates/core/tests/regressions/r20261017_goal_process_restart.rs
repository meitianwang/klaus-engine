//! Defect: process restart could reuse prior active time or safety streaks.
//! Root cause: durable goal state and process-local evidence lacked OS-process coverage.
//! Work item: 20261017 persistent goal process recovery and exact accounting.

use std::collections::VecDeque;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::deps::NoopCompactor;
use klaus_agent_core::engine::session::{Session, TurnOptions};
use klaus_agent_core::goal::types::{ThreadGoal, ThreadGoalStatus};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
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
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::Notify;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

const IDLE_THREAD: &str = "81b08399-6177-422b-80a2-c362b16b73af";
const INTERRUPTED_THREAD: &str = "f9012d62-0a86-4b60-945a-3d68107fa365";
const WORKER_KIND: &str = "KLAUS_GOAL_PROCESS_RESTART_KIND";
const WORKER_PHASE: &str = "KLAUS_GOAL_PROCESS_RESTART_PHASE";
const PROCESS_DEADLINE: Duration = Duration::from_secs(25);
const MAX_RECEIPT_BYTES: u64 = 8_192;
const STEP_DEADLINE: Duration = Duration::from_secs(15);
const FIRST_WALL_MILLIS: i64 = 1_000;
const OFFLINE_MILLIS: i64 = 60 * 24 * 60 * 60 * 1_000;
const OBJECTIVE: &str = "Finish the durable work with fresh process-local evidence.";

struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("Retained process-restart fixture: {}", self.0.display());
        } else {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

#[derive(Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    goal_id: String,
    objective: String,
    status: String,
    token_budget: Option<i64>,
    tokens_used: i64,
    time_used_seconds: i64,
    created_at_ms: i64,
    updated_at_ms: i64,
}
impl From<&ThreadGoal> for Snapshot {
    fn from(goal: &ThreadGoal) -> Self {
        Self {
            goal_id: goal.goal_id.try_to_utf8().unwrap(),
            objective: goal.objective.try_to_utf8().unwrap(),
            status: goal.status.wire_name().to_owned(),
            token_budget: goal.token_budget,
            tokens_used: goal.tokens_used,
            time_used_seconds: goal.time_used_seconds,
            created_at_ms: goal.created_at_ms,
            updated_at_ms: goal.updated_at_ms,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    pid: u32,
    config_root: PathBuf,
    wall_millis: i64,
    before: Snapshot,
    idle: Snapshot,
    interrupted: Snapshot,
    new_turns: usize,
    idle_requests: usize,
    idle_executions: usize,
    measured_upper_seconds: u64,
}

fn read_receipt(root: &Path, phase: u8) -> Receipt {
    let path = root.join(format!("phase-{phase}.json"));
    let mut bytes = Vec::with_capacity(MAX_RECEIPT_BYTES as usize);
    std::fs::File::open(path)
        .unwrap()
        .take(MAX_RECEIPT_BYTES + 1)
        .read_to_end(&mut bytes)
        .unwrap();
    assert!(
        bytes.len() <= MAX_RECEIPT_BYTES as usize,
        "process receipts remain bounded"
    );
    serde_json::from_slice(&bytes).unwrap()
}

fn log_tail(path: &Path) -> String {
    let mut file = std::fs::File::open(path).unwrap();
    let length = file.metadata().unwrap().len();
    file.seek(SeekFrom::Start(length.saturating_sub(8_192)))
        .unwrap();
    let mut bytes = Vec::with_capacity(8_192);
    file.take(8_192).read_to_end(&mut bytes).unwrap();
    String::from_utf8_lossy(&bytes).into_owned()
}

async fn check_process_restart(kind: &str) {
    let directory = Directory(
        std::env::temp_dir().join(format!("klaus-goal-process-{}", uuid::Uuid::new_v4())),
    );
    std::fs::create_dir_all(&directory.0).unwrap();
    let config_root = std::fs::canonicalize(&directory.0).unwrap();
    let module = module_path!().split_once("::").unwrap().1;
    let worker = format!("{module}::restart_process_worker");
    let mut launched_pids = Vec::with_capacity(2);
    for phase in [1_u8, 2] {
        let log_path = directory.0.join(format!("{kind}-{phase}.log"));
        let log = std::fs::File::create(&log_path).unwrap();
        let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args(["--ignored", "--exact", &worker, "--nocapture"])
            .env(WORKER_KIND, kind)
            .env(WORKER_PHASE, phase.to_string())
            .env("KLAUS_CONFIG_DIR", &config_root)
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .kill_on_drop(true)
            .spawn()
            .expect("the independent test process must start");
        launched_pids.push(child.id().expect("a live child has an OS process ID"));
        let status = match tokio::time::timeout(PROCESS_DEADLINE, child.wait()).await {
            Ok(result) => result.unwrap(),
            Err(_) => {
                let _ = child.start_kill();
                let _ = tokio::time::timeout(Duration::from_secs(2), child.wait()).await;
                panic!(
                    "phase {phase} exceeded its deadline: {}",
                    log_tail(&log_path)
                );
            }
        };
        assert!(
            status.success(),
            "phase {phase} failed with {status}: {}",
            log_tail(&log_path)
        );
    }
    let first = read_receipt(&config_root, 1);
    let second = read_receipt(&config_root, 2);
    assert_eq!(launched_pids, [first.pid, second.pid]);
    assert_ne!(first.pid, second.pid, "recovery runs in another OS process");
    assert_ne!(first.pid, std::process::id());
    assert_ne!(second.pid, std::process::id());
    assert_eq!(first.config_root, config_root);
    assert_eq!(second.config_root, config_root);
    assert_eq!(second.wall_millis - first.wall_millis, OFFLINE_MILLIS);
    assert_eq!(first.idle.status, "active");
    assert_eq!(
        second.before, first.idle,
        "idle recovery preserves the entire snapshot"
    );
    assert_eq!(second.idle.status, "blocked");
    assert_eq!(first.new_turns, 2);
    assert_eq!(
        second.new_turns, 3,
        "the new process receives three fresh eligible turns"
    );
    let responses_per_turn = if kind == "execution" { 2 } else { 1 };
    for receipt in [&first, &second] {
        assert_eq!(receipt.idle.objective, OBJECTIVE);
        assert_eq!(receipt.idle.goal_id, first.idle.goal_id);
        assert_eq!(receipt.idle.token_budget, Some(100_000));
        assert_eq!(
            receipt.idle_requests,
            receipt.new_turns * responses_per_turn
        );
        assert_eq!(
            receipt.idle_executions,
            if kind == "execution" {
                receipt.new_turns
            } else {
                0
            }
        );
        assert_eq!(
            receipt.idle.tokens_used,
            receipt.before.tokens_used + i64::try_from(receipt.idle_requests).unwrap() * 10,
            "each completed provider response contributes its noncached input and output once"
        );
        assert!(receipt.idle.time_used_seconds >= receipt.before.time_used_seconds);
        assert!(
            receipt.idle.time_used_seconds - receipt.before.time_used_seconds
                <= i64::try_from(receipt.measured_upper_seconds).unwrap(),
            "only this process's measured active time may be added: {receipt:?}"
        );
    }
    assert_eq!(first.interrupted.status, "active");
    assert_eq!(second.interrupted.status, "paused");
    assert_eq!(first.interrupted.tokens_used, 7);
    assert_eq!(second.interrupted.tokens_used, 7);
    assert_eq!(second.interrupted.goal_id, first.interrupted.goal_id);
    assert_eq!(second.interrupted.objective, first.interrupted.objective);
    assert_eq!(
        second.interrupted.token_budget,
        first.interrupted.token_budget
    );
    assert_eq!(
        second.interrupted.created_at_ms,
        first.interrupted.created_at_ms
    );
    assert_eq!(
        second.interrupted.time_used_seconds,
        first.interrupted.time_used_seconds
    );
    assert_eq!(second.interrupted.updated_at_ms, second.wall_millis);
    let outcomes =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/goal-outcomes-logs");
    std::fs::create_dir_all(&outcomes).unwrap();
    std::fs::write(
        outcomes.join(format!("process-restart-{kind}-snapshots.json")),
        serde_json::to_vec_pretty(&json!({"first":first,"second":second})).unwrap(),
    )
    .unwrap();
}

#[tokio::test]
async fn idle_active_goal_survives_process_restart_with_a_fresh_empty_turn_streak() {
    check_process_restart("empty").await;
}

#[tokio::test]
async fn execution_failure_streak_resets_across_processes_without_rebilling_saved_usage() {
    check_process_restart("execution").await;
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

enum Reply {
    Empty,
    Failure(Usage),
    ObservedFailure,
    Interrupted(Arc<Notify>),
}
struct Provider {
    info: ProviderInfo,
    script: Mutex<VecDeque<Reply>>,
    requests: AtomicUsize,
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
            max_output_tokens: 8_192,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }
    fn stream(&self, _: ModelRequest, signal: AbortSignal) -> ModelStream {
        let index = self.requests.fetch_add(1, Ordering::SeqCst);
        assert!(index < 8, "a restart worker has a bounded request script");
        let reply = self
            .script
            .lock()
            .unwrap()
            .pop_front()
            .expect("every inference has a scripted reply");
        Box::pin(async_stream::stream! {
            yield ModelStreamEvent::MessageStart { model:"goal-process-restart".into() };
            let (stop_reason, response_usage) = match reply {
                Reply::Empty => {
                    yield ModelStreamEvent::TextDelta { text:" \n\t ".into() };
                    (ModelStopReason::EndTurn, usage(8,2))
                }
                Reply::Failure(response_usage) => {
                    let id=RuntimeString::from(format!("process-restart-exec-{index}"));
                    yield ModelStreamEvent::ToolUseStart { id:id.clone(),name:"exec".into(),thought_signature:None,tool_kind:None,input_format:None };
                    yield ModelStreamEvent::ToolUseInputDelta { id:id.clone(),partial_json:"{}".into() };
                    yield ModelStreamEvent::ToolUseStop { id };
                    (ModelStopReason::ToolUse,response_usage)
                }
                Reply::ObservedFailure => {
                    yield ModelStreamEvent::TextDelta { text:"The execution failure was recorded.".into() };
                    (ModelStopReason::EndTurn, usage(8,2))
                }
                Reply::Interrupted(entered) => {
                    // This next request starts only after the preceding response was billed.
                    entered.notify_one();
                    signal.cancelled().await;
                    return;
                }
            };
            yield ModelStreamEvent::MessageStop { stop_reason,usage:response_usage };
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
        "Execute one controlled failure in the recovery fixture."
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _: &Json) -> bool {
        false
    }
    fn is_read_only(&self, _: &Json) -> bool {
        true
    }
    async fn call(
        &self,
        _: Json,
        _: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(ToolError::new(
            "The recovery fixture's actual execution handler failed.",
        ))
    }
}

fn session_options(root: &Path, thread_id: &str) -> StartSessionOptions {
    StartSessionOptions {
        id: Some(thread_id.into()),
        cwd: Some(root.to_string_lossy().into_owned().into()),
        user_config_dir: Some(root.to_string_lossy().into_owned().into()),
        config: Some(
            json!({"model":"goal-process-restart","goal":true,"prewarm":false,
            "agentMemory":false,"autoMemory":false,"autoDream":false,"memoryRecall":false,
            "memory":{"enabled":false},"idleMicrocompact":false,"earlyMicrocompact":false,
            "speculativeCompaction":false,"loopDetection":false,"maxRetries":0}),
        ),
        ..Default::default()
    }
}

async fn open_or_resume(
    agent: &Agent,
    store: Arc<dyn SessionStore>,
    root: &Path,
    thread: &str,
    phase: u8,
) -> Arc<Session> {
    if phase == 1 {
        agent
            .open_session(store, session_options(root, thread))
            .await
            .unwrap()
    } else {
        agent
            .resume_session(store, thread, session_options(root, thread))
            .await
            .unwrap()
    }
}

async fn idle_turn(session: &Arc<Session>, provider: &Provider, execution: bool) {
    {
        let mut script = provider.script.lock().unwrap();
        assert!(script.is_empty());
        if execution {
            script.push_back(Reply::Failure(usage(8, 2)));
        }
        script.push_back(if execution {
            Reply::ObservedFailure
        } else {
            Reply::Empty
        });
    }
    tokio::time::timeout(STEP_DEADLINE, async {
        let turn = session
            .run_goal_idle_streamed(TurnOptions::default())
            .await
            .unwrap()
            .expect("the active goal admits this automatic turn");
        let mut events = turn.events;
        let mut completed = 0;
        while let Some(event) = events.next().await {
            assert!(
                !matches!(event, EngineEvent::TurnFailed { .. }),
                "{event:?}"
            );
            if let EngineEvent::TurnCompleted { stop_reason, .. } = event {
                assert_eq!(stop_reason, TerminalReason::Completed);
                completed += 1;
            }
        }
        assert_eq!(completed, 1, "the actual automatic turn completes once");
    })
    .await
    .unwrap();
    assert!(provider.script.lock().unwrap().is_empty());
    assert!(!session.goal_runtime().unwrap().turn_in_flight());
}

async fn create_goal(session: &Session, objective: &str) -> ThreadGoal {
    session
        .set_thread_goal(Some(objective.into()), None, Some(Some(100_000)))
        .await
        .unwrap()
        .goal
}

#[tokio::test]
#[ignore = "entry point invoked only by the independent-process recovery tests"]
async fn restart_process_worker() {
    let kind = std::env::var(WORKER_KIND).expect("the parent selects a recovery script");
    assert!(matches!(kind.as_str(), "empty" | "execution"));
    let phase = std::env::var(WORKER_PHASE).unwrap().parse::<u8>().unwrap();
    assert!(matches!(phase, 1 | 2));
    let root = std::fs::canonicalize(std::env::var_os("KLAUS_CONFIG_DIR").unwrap()).unwrap();
    let first = if phase == 2 {
        Some(read_receipt(&root, 1))
    } else {
        None
    };
    let wall_millis = FIRST_WALL_MILLIS + if phase == 1 { 0 } else { OFFLINE_MILLIS };
    let provider = Arc::new(Provider {
        info: ProviderInfo {
            name: "goal-process-restart".into(),
            default_model: "goal-process-restart".into(),
            base_url: None,
            env_key: None,
        },
        script: Mutex::new(VecDeque::new()),
        requests: AtomicUsize::new(0),
    });
    let executions = Arc::new(AtomicUsize::new(0));
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut options = agent_options(process);
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
    options.deps.now = Some(Arc::new(move || wall_millis));
    let agent = Agent::new(options);
    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let measured = Instant::now();
    let idle = open_or_resume(&agent, store.clone(), &root, IDLE_THREAD, phase).await;
    let before = if phase == 1 {
        create_goal(&idle, OBJECTIVE).await
    } else {
        idle.thread_goal().await.unwrap().unwrap()
    };
    assert_eq!(before.status, ThreadGoalStatus::Active);
    if let Some(first) = &first {
        assert_eq!(
            Snapshot::from(&before),
            first.idle,
            "an idle active goal resumes without changing its durable snapshot"
        );
    }
    let new_turns = if phase == 1 { 2 } else { 3 };
    for index in 0..new_turns {
        idle_turn(&idle, &provider, kind == "execution").await;
        let goal = idle.thread_goal().await.unwrap().unwrap();
        assert_eq!(goal.goal_id, before.goal_id);
        assert_eq!(
            goal.status,
            if phase == 2 && index == 2 {
                ThreadGoalStatus::Blocked
            } else {
                ThreadGoalStatus::Active
            }
        );
        assert_eq!(
            goal.tokens_used,
            before.tokens_used
                + i64::try_from(index + 1).unwrap() * if kind == "execution" { 20 } else { 10 }
        );
    }
    let idle_requests = provider.requests.load(Ordering::SeqCst);
    let idle_executions = executions.load(Ordering::SeqCst);
    let tokens = idle.thread_goal().await.unwrap().unwrap().tokens_used;
    for _ in 0..2 {
        idle.flush_goal_progress().await.unwrap();
    }
    idle.flush_rollout().await.unwrap();
    let idle_snapshot = idle.thread_goal().await.unwrap().unwrap();
    assert_eq!(
        idle_snapshot.tokens_used, tokens,
        "repeated flushes cannot rebill old responses"
    );
    let measured_upper_seconds = measured.elapsed().as_secs().saturating_add(1);
    assert!(
        idle_snapshot.time_used_seconds - before.time_used_seconds
            <= i64::try_from(measured_upper_seconds).unwrap()
    );
    if phase == 2 {
        assert!(idle
            .run_goal_idle_streamed(TurnOptions::default())
            .await
            .unwrap()
            .is_none());
        assert_eq!(provider.requests.load(Ordering::SeqCst), idle_requests);
    }

    let interrupted = open_or_resume(&agent, store, &root, INTERRUPTED_THREAD, phase).await;
    let unfinished_turn = if phase == 1 {
        create_goal(
            &interrupted,
            "Retain billed usage from an interrupted active run.",
        )
        .await;
        let entered = Arc::new(Notify::new());
        let entered_wait = entered.notified();
        tokio::pin!(entered_wait);
        entered_wait.as_mut().enable();
        {
            let mut script = provider.script.lock().unwrap();
            script.push_back(Reply::Failure(usage(5, 2)));
            script.push_back(Reply::Interrupted(entered.clone()));
        }
        let turn = interrupted
            .run_goal_idle_streamed(TurnOptions::default())
            .await
            .unwrap()
            .unwrap();
        let task = tokio::spawn(async move {
            let mut events = turn.events;
            while let Some(event) = events.next().await {
                assert!(
                    !matches!(event, EngineEvent::TurnFailed { .. }),
                    "{event:?}"
                );
                assert!(
                    !matches!(event, EngineEvent::TurnCompleted { .. }),
                    "the interrupted provider remains in flight"
                );
            }
        });
        tokio::time::timeout(STEP_DEADLINE, entered_wait)
            .await
            .unwrap();
        assert!(interrupted.goal_runtime().unwrap().turn_in_flight());
        assert_eq!(provider.requests.load(Ordering::SeqCst), idle_requests + 2);
        assert_eq!(executions.load(Ordering::SeqCst), idle_executions + 1);
        assert!(provider.script.lock().unwrap().is_empty());
        interrupted.flush_goal_progress().await.unwrap();
        assert_eq!(
            interrupted
                .thread_goal()
                .await
                .unwrap()
                .unwrap()
                .tokens_used,
            7
        );
        Some(task)
    } else {
        let restored = interrupted.thread_goal().await.unwrap().unwrap();
        let saved = &first.as_ref().unwrap().interrupted;
        assert_eq!(
            restored.status,
            ThreadGoalStatus::Paused,
            "an interrupted run requires explicit resumption"
        );
        assert_eq!(restored.goal_id.try_to_utf8().unwrap(), saved.goal_id);
        assert_eq!(restored.tokens_used, saved.tokens_used);
        assert_eq!(restored.time_used_seconds, saved.time_used_seconds);
        let requests = provider.requests.load(Ordering::SeqCst);
        assert!(interrupted
            .run_goal_idle_streamed(TurnOptions::default())
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            provider.requests.load(Ordering::SeqCst),
            requests,
            "recovered paused work cannot infer automatically"
        );
        for _ in 0..2 {
            interrupted.flush_goal_progress().await.unwrap();
        }
        assert_eq!(
            interrupted
                .thread_goal()
                .await
                .unwrap()
                .unwrap()
                .tokens_used,
            7
        );
        None
    };
    interrupted.flush_rollout().await.unwrap();
    if let Some(task) = &unfinished_turn {
        assert!(
            !task.is_finished(),
            "the crash receipt retains a real active turn"
        );
    }
    let receipt = Receipt {
        pid: std::process::id(),
        config_root: root.clone(),
        wall_millis,
        before: Snapshot::from(&before),
        idle: Snapshot::from(&idle_snapshot),
        interrupted: Snapshot::from(&interrupted.thread_goal().await.unwrap().unwrap()),
        new_turns,
        idle_requests,
        idle_executions,
        measured_upper_seconds,
    };
    let bytes = serde_json::to_vec_pretty(&receipt).unwrap();
    assert!(bytes.len() <= MAX_RECEIPT_BYTES as usize);
    let path = root.join(format!("phase-{phase}.json"));
    let mut receipt_file = std::fs::File::create(path).unwrap();
    receipt_file.write_all(&bytes).unwrap();
    receipt_file.sync_all().unwrap();
    // Exit without shutdown or destructors: the next process must recover the live run lease.
    std::process::exit(0);
}
