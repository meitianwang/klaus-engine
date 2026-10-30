//! Fixing commit: 0a2ef4598881a6dcdbcb8a78254b6e792229f578.
//! Journal append stayed synchronous while the rollout writer became async.
//! Await journal admission before exposing completed results, progress, or replay.

use super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor;
use async_trait::async_trait;
use futures::{poll, StreamExt};
use klaus_agent_core::engine::workflow::runtime::{
    run_workflow, WorkflowRunInput, WorkflowRunResult, WorkflowRuntimeDeps, WorkflowScriptContext,
    WorkflowScriptRunner,
};
use klaus_agent_core::engine::workflow::WorkflowError;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeString};
use klaus_agent_core::schemas::message::Usage;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::TerminalReason;
use klaus_agent_core::types::events::{EngineEvent, ThreadItem, ToolDomainEvent};
use klaus_agent_core::types::json::Json;
use klaus_agent_core::types::tool::{
    BoxFuture, SpawnSubAgent, WorkflowJournal, WorkflowJournalEntry,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;

const RUN_ID: &str = "wf_journal_admission";

struct HeldJournal {
    capacity: Semaphore,
    entered: AtomicBool,
    fail: bool,
    entries: Mutex<Vec<WorkflowJournalEntry>>,
    tasks: Arc<klaus_agent_core::types::tool::RetainedToolTasks>,
}

impl HeldJournal {
    fn new(fail: bool) -> Self {
        Self {
            capacity: Semaphore::new(0),
            entered: AtomicBool::new(false),
            fail,
            entries: Mutex::new(Vec::new()),
            tasks: Arc::new(klaus_agent_core::types::tool::RetainedToolTasks::new()),
        }
    }
}

#[async_trait]
impl WorkflowJournal for HeldJournal {
    fn operation_tasks(&self) -> Option<Arc<klaus_agent_core::types::tool::RetainedToolTasks>> {
        Some(self.tasks.clone())
    }

    async fn append(&self, entry: WorkflowJournalEntry) -> std::io::Result<()> {
        self.entered.store(true, Ordering::SeqCst);
        let _permit = self.capacity.acquire().await.unwrap();
        if self.fail {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "journal writer is closed",
            ));
        }
        self.entries.lock().unwrap().push(entry);
        Ok(())
    }

    fn prior(&self, run_id: &RuntimeString) -> Vec<WorkflowJournalEntry> {
        self.entries
            .lock()
            .unwrap()
            .iter()
            .filter(|entry| &entry.run_id == run_id)
            .cloned()
            .collect()
    }
}

struct AgentThenProgress;

#[async_trait]
impl WorkflowScriptRunner for AgentThenProgress {
    async fn run(&self, ctx: WorkflowScriptContext) -> Result<Json, WorkflowError> {
        let result = ctx.sandbox.agent(Json::from("child"), Json::Null).await?;
        ctx.sandbox.log(&Json::from("after journal"));
        Ok(result)
    }
}

fn start_run(
    process_context: RuntimeProcessContext,
    journal: Arc<HeldJournal>,
    statuses: Arc<Mutex<Vec<RuntimeString>>>,
    spawns: Arc<AtomicUsize>,
) -> BoxFuture<'static, Result<WorkflowRunResult, WorkflowError>> {
    start_run_with_runner(
        process_context,
        journal,
        statuses,
        spawns,
        Arc::new(AgentThenProgress),
    )
}

fn start_run_with_runner(
    process_context: RuntimeProcessContext,
    journal: Arc<HeldJournal>,
    statuses: Arc<Mutex<Vec<RuntimeString>>>,
    spawns: Arc<AtomicUsize>,
    runner: Arc<dyn WorkflowScriptRunner>,
) -> BoxFuture<'static, Result<WorkflowRunResult, WorkflowError>> {
    let spawn: SpawnSubAgent = Arc::new(move |_| {
        spawns.fetch_add(1, Ordering::SeqCst);
        Box::pin(
            futures::stream::iter([
                EngineEvent::ItemCompleted {
                    inference: None,
                    item: ThreadItem::AgentMessage {
                        id: "child-result".into(),
                        text: "completed child".into(),
                        phase: None,
                    },
                },
                EngineEvent::TurnCompleted {
                    turn_id: "child".into(),
                    usage: Some(Usage {
                        input_tokens: 0,
                        output_tokens: 7,
                        cache_read_input_tokens: 0,
                        cache_creation_input_tokens: 0,
                        total_tokens: None,
                    }),
                    stop_reason: TerminalReason::Completed,
                },
            ])
            .chain(futures::stream::pending()),
        )
    });
    Box::pin(run_workflow(
        WorkflowRunInput {
            script: "export const meta = {name:'journal',description:'test'}\nreturn null".into(),
            args: None,
            resume_from_run_id: Some(RUN_ID.into()),
            budget_tokens: None,
        },
        WorkflowRuntimeDeps {
            process_context,
            spawn_workflow_agent: spawn,
            journal: Some(journal),
            emit: Some(Arc::new(move |event| {
                if let ToolDomainEvent::Status { message } = event {
                    statuses.lock().unwrap().push(message);
                }
            })),
            resolve_workflow: None,
            signal: AbortSignal::new(),
            max_concurrency: Some(1),
        },
        runner,
    ))
}

async fn wait_for_journal(journal: &HeldJournal) {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !journal.entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("accepted child reaches checkpoint admission");
}

#[tokio::test]
async fn workflow_waits_for_journal_before_progress_and_result_and_replays_exact_entry() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let journal = Arc::new(HeldJournal::new(false));
    let statuses = Arc::new(Mutex::new(Vec::new()));
    let spawns = Arc::new(AtomicUsize::new(0));
    let mut run = start_run(
        process.clone(),
        journal.clone(),
        statuses.clone(),
        spawns.clone(),
    );
    assert!(poll!(run.as_mut()).is_pending());
    wait_for_journal(&journal).await;
    assert!(journal.prior(&RUN_ID.into()).is_empty());
    assert_eq!(
        *statuses.lock().unwrap(),
        [RuntimeString::from("agent ▸ agent#0")]
    );

    journal.capacity.add_permits(1);
    let result = run.await.unwrap();
    assert_eq!(result.run_id, RUN_ID);
    assert_eq!(result.result, Json::from("completed child"));
    assert_eq!(result.agents_spawned, 1);
    assert_eq!(result.output_tokens_spent, 7);
    let entries = journal.prior(&RUN_ID.into());
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].run_id, RUN_ID);
    assert_eq!(entries[0].idx, 0);
    assert_eq!(entries[0].hash, "30ca14b96ffe91b4");
    assert_eq!(entries[0].result, result.result);
    assert_eq!(entries[0].output_tokens, 7);
    assert_eq!(statuses.lock().unwrap().last().unwrap(), "after journal");

    let resumed = start_run(process.clone(), journal.clone(), statuses, spawns.clone())
        .await
        .unwrap();
    assert_eq!(resumed.result, result.result);
    assert_eq!(resumed.output_tokens_spent, 7);
    assert_eq!(resumed.agents_spawned, 0);
    assert_eq!(spawns.load(Ordering::SeqCst), 1);
    assert_eq!(journal.prior(&RUN_ID.into()), entries);
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropping_a_workflow_waiter_preserves_the_completed_checkpoint_and_replays_once() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let journal = Arc::new(HeldJournal::new(false));
    let statuses = Arc::new(Mutex::new(Vec::new()));
    let spawns = Arc::new(AtomicUsize::new(0));
    let mut run = start_run(
        process.clone(),
        journal.clone(),
        statuses.clone(),
        spawns.clone(),
    );
    assert!(poll!(run.as_mut()).is_pending());
    wait_for_journal(&journal).await;
    drop(run);
    journal.capacity.add_permits(1);
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        journal.tasks.close_and_wait(),
    )
    .await
    .unwrap()
    .unwrap();
    let entries = journal.prior(&RUN_ID.into());
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].result, Json::from("completed child"));
    assert_eq!(entries[0].output_tokens, 7);
    assert_eq!(
        *statuses.lock().unwrap(),
        [RuntimeString::from("agent ▸ agent#0")]
    );
    let resumed = start_run(process.clone(), journal.clone(), statuses, spawns.clone())
        .await
        .unwrap();
    assert_eq!(resumed.result, entries[0].result);
    assert_eq!(spawns.load(Ordering::SeqCst), 1);
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn journal_admission_error_prevents_success_progress_and_replay() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let journal = Arc::new(HeldJournal::new(true));
    let statuses = Arc::new(Mutex::new(Vec::new()));
    journal.capacity.add_permits(1);
    let error = start_run(
        process.clone(),
        journal.clone(),
        statuses.clone(),
        Arc::new(AtomicUsize::new(0)),
    )
    .await
    .unwrap_err();
    assert_checkpoint_failure(&error);
    assert!(journal.prior(&RUN_ID.into()).is_empty());
    assert_eq!(
        *statuses.lock().unwrap(),
        [RuntimeString::from("agent ▸ agent#0")]
    );
    process.shutdown().await.unwrap();
}

fn assert_checkpoint_failure(error: &WorkflowError) {
    assert!(error.message.starts_with(
        "Workflow agent completed, but its journal checkpoint failed: journal writer is closed."
    ));
    assert!(error
        .message
        .contains("do not automatically rerun this call"));
    let message = error.message.try_to_utf8().unwrap();
    let checkpoint = message.split_once("\nCompleted checkpoint: ").unwrap().1;
    let checkpoint = runtime_json::parse(&RuntimeString::from(checkpoint)).unwrap();
    let expected = runtime_json::parse(&RuntimeString::from(
        r#"{"runId":"wf_journal_admission","idx":0,"hash":"30ca14b96ffe91b4","result":"completed child","outputTokens":7}"#,
    ))
    .unwrap();
    assert_eq!(checkpoint, expected);
}

struct ReturningDuringCheckpoint {
    journal: Arc<HeldJournal>,
    throws: bool,
}

#[async_trait]
impl WorkflowScriptRunner for ReturningDuringCheckpoint {
    async fn run(&self, ctx: WorkflowScriptContext) -> Result<Json, WorkflowError> {
        let mut pending = ctx.sandbox.agent(Json::from("child"), Json::Null);
        assert!(poll!(pending.as_mut()).is_pending());
        wait_for_journal(&self.journal).await;
        drop(pending);
        if self.throws {
            Err(WorkflowError::new("script failed after launching child"))
        } else {
            Ok(Json::from("script returned"))
        }
    }
}

#[tokio::test]
async fn early_script_return_and_failure_do_not_discard_an_accepted_child_checkpoint() {
    for throws in [false, true] {
        let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
        let journal = Arc::new(HeldJournal::new(false));
        let spawns = Arc::new(AtomicUsize::new(0));
        let outcome = start_run_with_runner(
            process.clone(),
            journal.clone(),
            Arc::new(Mutex::new(Vec::new())),
            spawns.clone(),
            Arc::new(ReturningDuringCheckpoint {
                journal: journal.clone(),
                throws,
            }),
        )
        .await;
        assert_eq!(outcome.is_err(), throws);
        assert!(journal.prior(&RUN_ID.into()).is_empty());
        journal.capacity.add_permits(1);
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            journal.tasks.close_and_wait(),
        )
        .await
        .unwrap()
        .unwrap();
        let entries = journal.prior(&RUN_ID.into());
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].result, Json::from("completed child"));
        let resumed = start_run(
            process.clone(),
            journal.clone(),
            Arc::new(Mutex::new(Vec::new())),
            spawns.clone(),
        )
        .await
        .unwrap();
        assert_eq!(resumed.result, entries[0].result);
        assert_eq!(spawns.load(Ordering::SeqCst), 1);
        process.shutdown().await.unwrap();
    }
}

struct CatchingCheckpointFailure;

#[async_trait]
impl WorkflowScriptRunner for CatchingCheckpointFailure {
    async fn run(&self, ctx: WorkflowScriptContext) -> Result<Json, WorkflowError> {
        let first = ctx
            .sandbox
            .agent(Json::from("child"), Json::Null)
            .await
            .unwrap_err();
        let second = ctx
            .sandbox
            .agent(Json::from("retry child"), Json::Null)
            .await
            .unwrap_err();
        assert_eq!(first, second);
        Ok(Json::from("caught"))
    }
}

#[tokio::test]
async fn caught_checkpoint_error_still_reports_completed_result_and_prevents_another_spawn() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let journal = Arc::new(HeldJournal::new(true));
    let spawns = Arc::new(AtomicUsize::new(0));
    journal.capacity.add_permits(1);
    let error = start_run_with_runner(
        process.clone(),
        journal.clone(),
        Arc::new(Mutex::new(Vec::new())),
        spawns.clone(),
        Arc::new(CatchingCheckpointFailure),
    )
    .await
    .unwrap_err();
    assert_checkpoint_failure(&error);
    assert!(journal.prior(&RUN_ID.into()).is_empty());
    assert_eq!(spawns.load(Ordering::SeqCst), 1);
    process.shutdown().await.unwrap();
}
