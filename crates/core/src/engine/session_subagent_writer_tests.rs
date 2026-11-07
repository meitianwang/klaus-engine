//! Cancellation persistence is owned independently from its observing caller.

use super::*;
use crate::schemas::rollout::RolloutItem;
use crate::store::session_store::{
    AppendEventOptions, CheckpointRecord, CompactionRecord, LiveRollout, PreparedRolloutBatch,
    RollbackRecord,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

#[derive(Debug)]
struct CancellationAdmissionGate {
    armed: AtomicBool,
    entered: Notify,
    release: Semaphore,
    records: Mutex<Vec<RolloutItem>>,
    fail_flush: AtomicBool,
    shutdown_called: AtomicBool,
}

impl Default for CancellationAdmissionGate {
    fn default() -> Self {
        Self {
            armed: AtomicBool::new(false),
            entered: Notify::new(),
            release: Semaphore::new(0),
            records: Mutex::new(Vec::new()),
            fail_flush: AtomicBool::new(false),
            shutdown_called: AtomicBool::new(false),
        }
    }
}

#[async_trait::async_trait]
impl LiveRollout for CancellationAdmissionGate {
    async fn prepare_batch(
        &self,
        maximum_records: usize,
    ) -> std::io::Result<PreparedRolloutBatch<'_>> {
        if maximum_records == 2 && self.armed.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release
                .acquire()
                .await
                .expect("admission gate remains open")
                .forget();
        }
        crate::testing::immediate_rollout::prepare_immediate_test_rollout_batch(
            self,
            maximum_records,
        )
    }

    async fn append_message(&self, _: Message) {}
    async fn append_turn_context(&self, _: crate::schemas::rollout::TurnContextSnapshot) {}
    async fn append_compaction(&self, _: CompactionRecord) {}
    async fn append_event(&self, event: RuntimeString, options: Option<AppendEventOptions>) {
        let options = options.unwrap_or_default();
        self.records
            .lock()
            .unwrap()
            .push(RolloutItem::Event(crate::schemas::rollout::EventItem {
                event,
                turn_id: options.turn_id,
                payload: options.payload,
            }));
    }
    async fn append_rollback(&self, _: RollbackRecord) {}
    async fn append_checkpoint(&self, _: CheckpointRecord) {}
    async fn append_usage(&self, _: crate::schemas::rollout::UsageLedgerItem) {}
    async fn append_subagent_lifecycle(
        &self,
        record: crate::schemas::subagent_lifecycle::SubagentLifecycleRecord,
    ) {
        self.records
            .lock()
            .unwrap()
            .push(RolloutItem::SubagentLifecycle(
                crate::schemas::rollout::SubagentLifecycleItem { record },
            ));
    }
    async fn flush(&self) -> std::io::Result<()> {
        if self.fail_flush.load(Ordering::SeqCst) {
            return Err(std::io::Error::other("cleanup flush failed"));
        }
        Ok(())
    }
    async fn shutdown(&self) -> std::io::Result<()> {
        self.shutdown_called.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn stop_aborts_immediately_and_cancel_record_survives_a_dropped_durability_waiter() {
    let tool_gate = gate();
    let writer = Arc::new(CancellationAdmissionGate::default());
    let mut config = default_config();
    config.agent_memory = false;
    config.memory_recall = false;
    config.fork = false;
    let session = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        unique_subagent_test_deps(mk_deps(vec![gate_turn()])),
        config,
        vec![tool_gate.clone() as AnyTool],
        None,
        Some(writer.clone()),
        None,
        None,
        None,
    );
    let id = spawn_worker(
        &session,
        SpawnSubAgentOptions {
            prompt: "wait for cancellation".into(),
            ..Default::default()
        },
    )
    .await;
    entered(&tool_gate).await;
    let abort = session
        .subagents
        .background_agents
        .lock()
        .unwrap()
        .get(&id.try_to_utf8().unwrap())
        .unwrap()
        .abort
        .clone();
    writer.armed.store(true, Ordering::SeqCst);
    assert!(session.background_task_registry().stop_by_user(&id));
    assert!(
        abort.is_cancelled(),
        "writer capacity cannot delay physical Stop"
    );
    tokio::time::timeout(Duration::from_secs(5), writer.entered.notified())
        .await
        .unwrap();
    let mut observer = Box::pin(session.await_ordinary_cancellation_persistence());
    assert!(futures::poll!(observer.as_mut()).is_pending());
    drop(observer);
    assert!(!writer
        .records
        .lock()
        .unwrap()
        .iter()
        .any(|item| matches!(item,
        RolloutItem::Event(event) if event.event == "ordinary_subagent_user_cancelled")));
    writer.release.add_permits(1);
    session
        .await_ordinary_cancellation_persistence()
        .await
        .unwrap();
    let records = writer.records.lock().unwrap().clone();
    let cancellation = records
        .iter()
        .find_map(|item| match item {
            RolloutItem::Event(event) if event.event == "ordinary_subagent_user_cancelled" => {
                event.payload.as_ref()
            }
            _ => None,
        })
        .expect("the retained cancellation owner commits after observation is dropped");
    assert_eq!(
        cancellation.get("agentId").and_then(RuntimeJson::as_str),
        Some(&id)
    );
    assert_eq!(
        cancellation
            .get("userCancelled")
            .and_then(RuntimeJson::as_bool),
        Some(true)
    );
    assert_eq!(
        cancellation.get("generation").and_then(RuntimeJson::as_u64),
        Some(1)
    );
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn creation_cleanup_is_fenced_before_a_late_guard_drop_and_reports_errors_after_teardown() {
    let (session, _) = fixture(Vec::new(), Vec::new());
    let writer = Arc::new(CancellationAdmissionGate::default());
    let abort = tokio_util::sync::CancellationToken::new();
    let (id, admission, pending, accepted) = session
        .prepare_external_subagent(
            &SpawnSubAgentOptions {
                prompt: "cancel pending creation".into(),
                ..Default::default()
            },
            1,
            None,
            None,
            abort.clone(),
            None,
            &default_config(),
        )
        .await
        .unwrap();
    session.stage_ordinary_rollout(&id, Some(writer.clone()));
    writer.fail_flush.store(true, Ordering::SeqCst);
    let mut barrier = Box::pin(session.await_pending_ordinary_cleanup());
    assert!(
        futures::poll!(barrier.as_mut()).is_pending(),
        "the guard registers cleanup before Drop"
    );
    drop(pending);
    assert!(abort.is_cancelled());
    let error = tokio::time::timeout(Duration::from_secs(5), barrier)
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error, RuntimeString::from("cleanup flush failed"));
    assert!(
        writer.shutdown_called.load(Ordering::SeqCst),
        "an earlier flush error cannot skip shutdown"
    );
    assert!(!session.has_ordinary_runtime_identity(&id));
    assert!(writer
        .records
        .lock()
        .unwrap()
        .iter()
        .any(|item| matches!(item,
        RolloutItem::Event(event) if event.event == "ordinary_subagent_creation_committed"
            && event.payload == Some(RuntimeJson::Bool(false)))));
    drop(admission);
    drop(accepted);
    let _ = session.shutdown_and_wait().await;
}
