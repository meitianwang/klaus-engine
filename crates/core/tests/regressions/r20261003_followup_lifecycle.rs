//! Defect: interrupted follow-ups could not be reconciled to their durable queue identity.
//! Root cause: enqueue always minted a second UUID and accepted duplicate queue identities.
//! Fixed for work item 20261003 follow-up takeover: preserve the Host UUID across restart,
//! reject duplicate pending/drained identities, and hand preserved input to one explicit turn.

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};
use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::message_queue::QueueItemState;
use klaus_agent_core::engine::session::{EnqueueOptions, Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::provider::{ModelStopReason, ModelStreamEvent};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[tokio::test]
async fn followup_identity_survives_stop_restart_and_one_explicit_resume() {
    struct Directory(std::path::PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let root = Directory(
        std::env::temp_dir().join(format!("klaus-followup-identity-{}", uuid::Uuid::new_v4())),
    );
    std::fs::create_dir_all(&root.0).unwrap();
    let store = || {
        Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(root.0.to_string_lossy().into_owned()),
            archive_dir: None,
        }))
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let mut options =
        agent_options(RuntimeProcessContext::initialize(process_supervisor()).unwrap());
    options.deps.call_model = Some({
        let calls = calls.clone();
        Box::new(move |_, _| {
            calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(futures::stream::iter(vec![
                ModelStreamEvent::MessageStart {
                    model: "regression-noop".into(),
                },
                ModelStreamEvent::TextDelta {
                    text: "finished".into(),
                },
                ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::EndTurn,
                    usage: Usage {
                        input_tokens: 1,
                        output_tokens: 1,
                        cache_read_input_tokens: 0,
                        cache_creation_input_tokens: 0,
                        total_tokens: None,
                    },
                },
            ]))
        })
    });
    let agent = Agent::new(options);
    let session_options = || StartSessionOptions {
        cwd: Some(root.0.to_string_lossy().into_owned().into()),
        user_config_dir: Some(root.0.to_string_lossy().into_owned().into()),
        config: Some(
            serde_json::json!({"prewarm":false,"goal":false,"agentMemory":false,"cron":false}),
        ),
        ..Default::default()
    };
    let session = agent
        .open_session(store(), session_options())
        .await
        .unwrap();
    let queue_id = RuntimeString::from("c9a02ab1-092b-4206-8b14-bbb7d37d5f20");
    let enqueue = || EnqueueOptions {
        queue_message_id: Some(queue_id.clone()),
        ..Default::default()
    };
    session
        .enqueue(Input::Text("keep this follow-up".into()), enqueue())
        .unwrap();
    session.await_queue_durability().await.unwrap();
    let before = session.queue_snapshot();
    assert_eq!(before.items[0].message.uuid.as_ref(), Some(&queue_id));
    assert!(session
        .enqueue(Input::Text("duplicate".into()), enqueue())
        .is_err());
    assert_eq!(session.queue_snapshot(), before);
    assert!(session
        .enqueue(
            Input::Text("alternate UUID spelling".into()),
            EnqueueOptions {
                queue_message_id: Some(queue_id.try_to_utf8().unwrap().to_uppercase().into()),
                ..Default::default()
            },
        )
        .is_err());
    assert_eq!(session.queue_snapshot(), before);
    session.interrupt();
    assert_eq!(session.queue_snapshot(), before);
    session.flush_rollout().await.unwrap();
    let thread_id = session.id.try_to_utf8().unwrap();
    session.shutdown_and_wait().await.unwrap();

    let resumed = agent
        .resume_session(store(), &thread_id, session_options())
        .await
        .unwrap();
    let recovered = resumed.queue_snapshot();
    assert_eq!(recovered.items.len(), 1);
    assert_eq!(recovered.items[0].message.uuid.as_ref(), Some(&queue_id));
    assert_eq!(recovered.items[0].state, QueueItemState::Pending);
    assert!(resumed
        .enqueue(Input::Text("duplicate after restart".into()), enqueue())
        .is_err());
    let turn = resumed
        .resume_pending_streamed(TurnOptions::default())
        .await
        .unwrap()
        .unwrap();
    let events: Vec<_> = turn.events.collect().await;
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(resumed.queue_snapshot().items.is_empty());
    assert_eq!(
        resumed
            .history()
            .iter()
            .filter(|message| matches!(message, Message::User(user) if user.uuid == queue_id))
            .count(),
        1
    );
    assert!(resumed
        .resume_pending_streamed(TurnOptions::default())
        .await
        .unwrap()
        .is_none());
    assert!(resumed
        .enqueue(Input::Text("duplicate after delivery".into()), enqueue())
        .is_err());
    resumed.shutdown_and_wait().await.unwrap();

    let delivered = agent
        .resume_session(store(), &thread_id, session_options())
        .await
        .unwrap();
    assert!(delivered.queue_snapshot().items.is_empty());
    assert!(delivered
        .enqueue(
            Input::Text("duplicate terminal identity after restart".into()),
            enqueue()
        )
        .is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    delivered.shutdown_and_wait().await.unwrap();
}
