//! Defect: a rejected turn was acknowledged and left its thread permanently active.
//! Root cause: turn admission retained the submission even when its op channel closed.
//! Fixing commit: d958d899b27fa56e0932dce45dfcadfe624849ea.
//! This public facade regression belongs to server; core cannot depend on its own facade.

use super::*;
use crate::app_server::wire;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::message::Message;
use klaus_agent_core::schemas::rollout::{TurnContextSnapshot, UsageLedgerItem};
use klaus_agent_core::schemas::subagent_lifecycle::SubagentLifecycleRecord;
use klaus_agent_core::store::session_store::{
    AppendEventOptions, CheckpointRecord, CompactionRecord, LiveRollout, RollbackRecord,
};

#[derive(Debug)]
struct FailedShutdownRollout;

#[async_trait::async_trait]
impl LiveRollout for FailedShutdownRollout {
    async fn prepare_batch(
        &self,
        maximum_records: usize,
    ) -> std::io::Result<klaus_agent_core::store::session_store::PreparedRolloutBatch<'_>> {
        klaus_agent_core::testing::immediate_rollout::prepare_immediate_test_rollout_batch(
            self,
            maximum_records,
        )
    }

    async fn append_message(&self, _message: Message) {}
    async fn append_turn_context(&self, _context: TurnContextSnapshot) {}
    async fn append_compaction(&self, _record: CompactionRecord) {}
    async fn append_event(&self, _event: RuntimeString, _options: Option<AppendEventOptions>) {}
    async fn append_rollback(&self, _record: RollbackRecord) {}
    async fn append_checkpoint(&self, _record: CheckpointRecord) {}
    async fn append_usage(&self, _record: UsageLedgerItem) {}
    async fn append_subagent_lifecycle(&self, _record: SubagentLifecycleRecord) {}

    async fn flush(&self) -> std::io::Result<()> {
        Ok(())
    }

    async fn shutdown(&self) -> std::io::Result<()> {
        Err(std::io::Error::other("fixture rollout shutdown failed"))
    }
}

async fn request(client: &mut TestClient, id: u64, method: &str, params: Value) -> Value {
    client
        .send(json!({"id":id,"method":method,"params":params}))
        .await;
    client.response_for(id).await.0
}

async fn failed_closed_session() -> (TestClient, tempfile::TempDir) {
    let root = tempfile::tempdir().unwrap();
    let agent = make_agent_for_tests(vec![text_turn("healthy reply")]);
    let create: CreateSession = Arc::new(move |context: SessionContext| {
        let agent = agent.clone();
        Box::pin(async move {
            let failing = context
                .init
                .thread_id
                .as_ref()
                .is_some_and(|id| id == "failed-close");
            Ok::<Arc<Session>, BoxError>(agent.start_session(StartSessionOptions {
                id: context.init.thread_id,
                rollout: failing.then(|| Arc::new(FailedShutdownRollout) as Arc<dyn LiveRollout>),
                config: Some(json!({"prewarm":false,"autoMemory":false})),
                ..Default::default()
            }))
        })
    });
    let mut client = TestClient::start_full(
        create,
        None,
        Some(root.path().join("permissions.json")),
        Some(root.path().join("config.json")),
    );
    client.initialize().await;
    let started = request(
        &mut client,
        2,
        "session/start",
        json!({"init":{"threadId":"failed-close"}}),
    )
    .await;
    assert_eq!(started["result"]["sessionId"], "failed-close");
    // Materialize the lazy session so the public close exercises a real
    // persistent-resource failure, not an artificial channel replacement.
    let history = request(
        &mut client,
        3,
        "session/history",
        json!({"sessionId":"failed-close"}),
    )
    .await;
    assert!(history["result"].is_object(), "{history}");
    let closed = request(
        &mut client,
        4,
        "session/close",
        json!({"sessionId":"failed-close"}),
    )
    .await;
    assert_eq!(closed["error"]["code"], wire::ENGINE_ERROR, "{closed}");
    assert!(closed["error"]["message"]
        .as_str()
        .unwrap()
        .contains("fixture rollout shutdown failed"));
    (client, root)
}

async fn assert_failed_admission(method: &str) {
    let (mut client, _root) = failed_closed_session().await;
    let params = json!({
        "sessionId":"failed-close",
        "submissionId":"retryable-submission",
        "input":"must not be accepted",
        "afterSubmissionId":"previous-turn"
    });
    for id in [5, 6] {
        let reply = request(&mut client, id, method, params.clone()).await;
        assert_eq!(reply["error"]["code"], wire::ENGINE_ERROR, "{reply}");
        assert!(reply.get("result").is_none(), "{reply}");
        assert!(reply["error"]["message"]
            .as_str()
            .unwrap()
            .contains("before the turn was accepted"));
    }
    let snapshot = request(
        &mut client,
        7,
        "engine/readThread",
        json!({"threadId":"failed-close"}),
    )
    .await;
    assert_eq!(
        snapshot["result"]["thread"]["status"]["type"], "notLoaded",
        "{snapshot}"
    );

    let healthy = request(
        &mut client,
        8,
        "session/start",
        json!({"init":{"threadId":"healthy"}}),
    )
    .await;
    assert_eq!(healthy["result"]["sessionId"], "healthy");
    r20260908_live_side_task_fork::run_turn(&mut client, 9, "healthy", "hello").await;
    client.writer.shutdown().await.unwrap();
    drop(client.writer);
    tokio::time::timeout(REPLY_TIMEOUT, client.server)
        .await
        .expect("the failed closed session must not retain the facade connection")
        .unwrap();
}

#[tokio::test]
async fn turn_start_rejects_closed_transport_without_reserving_a_submission() {
    assert_failed_admission("turn/start").await;
}

#[tokio::test]
async fn pending_resume_rejects_closed_transport_without_reserving_a_submission() {
    assert_failed_admission("session/resumePending").await;
}
