//! Defect: now/next input left after a text-only turn never reached a new turn.
//! Root cause: takePending destructively exported queue ownership to the host.
//! Fixed for work item 20260913: core hands one original-identity batch to history
//! before consumption, and an unpolled/cancelled continuation restores pending.

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};
use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::message_queue::{QueueItemState, QueuePriority};
use klaus_agent_core::engine::session::{
    EnqueueOptions, Input, Session, TurnOptions, UserContentPart,
};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::provider::{
    ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
};
use std::sync::{Arc, Mutex};

struct CapabilityProvider(Arc<dyn klaus_agent_core::types::provider::ModelProvider>);
impl klaus_agent_core::types::provider::ModelProvider for CapabilityProvider {
    fn info(&self) -> &klaus_agent_core::types::provider::ProviderInfo {
        self.0.info()
    }
    fn capabilities(
        &self,
        model: &RuntimeString,
    ) -> klaus_agent_core::types::provider::ProviderCapabilities {
        let mut capabilities = self.0.capabilities(model);
        if model == "no-pdf" {
            capabilities.native_pdf = false;
        }
        if model == "blind" {
            capabilities.vision = false;
        }
        capabilities
    }
    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        self.0.stream(request, signal)
    }
}

fn agent() -> (Agent, Arc<Mutex<Vec<ModelRequest>>>) {
    let mut options =
        agent_options(RuntimeProcessContext::initialize(process_supervisor()).unwrap());
    options.provider = Arc::new(CapabilityProvider(options.provider.clone()));
    let requests = Arc::new(Mutex::new(Vec::new()));
    options.deps.call_model = Some({
        let requests = requests.clone();
        Box::new(
            move |request: ModelRequest, _signal: AbortSignal| -> ModelStream {
                requests.lock().unwrap().push(request);
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
            },
        )
    });
    (Agent::new(options), requests)
}

fn session() -> (Arc<Session>, Arc<Mutex<Vec<ModelRequest>>>) {
    let (agent, requests) = agent();
    (
        agent.start_session(StartSessionOptions {
            config: Some(serde_json::json!({ "prewarm": false, "goal": false })),
            ..Default::default()
        }),
        requests,
    )
}

#[tokio::test]
async fn pending_resume_hands_one_fifo_batch_to_one_turn_with_original_identity() {
    let (session, requests) = session();
    session
        .enqueue(
            Input::Text("first".into()),
            EnqueueOptions {
                priority: Some(QueuePriority::Next),
                correlation_id: Some("steer-first".into()),
                ..Default::default()
            },
        )
        .unwrap();
    session
        .enqueue(
            Input::Text("second".into()),
            EnqueueOptions {
                priority: Some(QueuePriority::Now),
                is_meta: Some(true),
                origin: Some("fixture".into()),
                correlation_id: Some("steer-second".into()),
                ..Default::default()
            },
        )
        .unwrap();
    session
        .enqueue(
            Input::Text("later".into()),
            EnqueueOptions {
                priority: Some(QueuePriority::Later),
                ..Default::default()
            },
        )
        .unwrap();
    session
        .enqueue(
            Input::Text("child".into()),
            EnqueueOptions {
                agent_id: Some("child-agent".into()),
                ..Default::default()
            },
        )
        .unwrap();
    let before = session.queue_snapshot();
    let turn = session
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
    assert_eq!(
        requests.lock().unwrap().len(),
        1,
        "the batch must incur one inference turn"
    );
    let users: Vec<_> = session
        .history()
        .into_iter()
        .filter_map(|message| match message {
            Message::User(user) => Some(user),
            _ => None,
        })
        .collect();
    assert_eq!(users.len(), 2);
    assert_eq!(users[0].uuid, before.items[0].message.uuid.clone().unwrap());
    assert_eq!(users[1].uuid, before.items[1].message.uuid.clone().unwrap());
    assert_eq!(users[1].is_meta, Some(true));
    let applied: Vec<_> = events
        .into_iter()
        .filter_map(|event| match event {
            EngineEvent::InputApplied { correlation_id } => correlation_id,
            _ => None,
        })
        .collect();
    assert_eq!(
        applied,
        vec![
            RuntimeString::from("steer-first"),
            RuntimeString::from("steer-second")
        ]
    );
    assert_eq!(session.queue_snapshot().items, before.items[2..]);
    assert!(session
        .resume_pending_streamed(TurnOptions::default())
        .await
        .unwrap()
        .is_none());
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn pending_resume_dropped_or_cancelled_before_handoff_keeps_input_mutable() {
    let (session, requests) = session();
    session
        .enqueue(Input::Text("keep me".into()), EnqueueOptions::default())
        .unwrap();
    let before = session.queued_messages();
    let turn = session
        .resume_pending_streamed(TurnOptions::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        session.queue_snapshot().items[0].state,
        QueueItemState::Reserved
    );
    drop(turn);
    let snapshot = session.queue_snapshot();
    assert_eq!(snapshot.items[0].state, QueueItemState::Pending);
    assert_eq!(session.queued_messages(), before);
    assert!(session.history().is_empty());
    let signal = AbortSignal::new();
    let turn = session
        .resume_pending_streamed(TurnOptions {
            signal: Some(signal.clone()),
            ..Default::default()
        })
        .await
        .unwrap()
        .unwrap();
    signal.cancel();
    let _: Vec<_> = turn.events.collect().await;
    assert_eq!(session.queued_messages(), before);
    assert!(session.history().is_empty());
    assert!(requests.lock().unwrap().is_empty());
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn pending_resume_pdf_batch_budget_rejection_leaves_all_original_input_pending() {
    let (session, requests) = session();
    for _ in 0..33 {
        session
            .enqueue(
                Input::Parts(vec![UserContentPart::Document {
                    base64: "JVBERi0xLjcK".into(),
                    mime_type: "application/pdf".into(),
                    filename: Some("fixture.pdf".into()),
                }]),
                EnqueueOptions::default(),
            )
            .unwrap();
    }
    let before = session.queued_messages();
    let result = session
        .resume_pending_streamed(TurnOptions::default())
        .await;
    assert!(result.is_err());
    assert_eq!(session.queued_messages(), before);
    assert!(session
        .queue_snapshot()
        .items
        .iter()
        .all(|item| item.mutable));
    assert!(requests.lock().unwrap().is_empty());
    assert!(session.history().is_empty());
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn pending_resume_real_rollout_recovery_neither_loses_nor_duplicates_input() {
    use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
    struct Directory(std::path::PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let root = Directory(
        std::env::temp_dir().join(format!("klaus-pending-resume-{}", uuid::Uuid::new_v4())),
    );
    let live_dir = root.0.join("live");
    let before_dir = root.0.join("before-handoff");
    let after_dir = root.0.join("after-handoff");
    for directory in [&live_dir, &before_dir, &after_dir] {
        std::fs::create_dir_all(directory).unwrap();
    }
    let store = |directory: &std::path::Path| {
        Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(directory.to_string_lossy().into_owned()),
            archive_dir: None,
        }))
    };
    let (agent, _) = agent();
    let options = || StartSessionOptions {
        config: Some(serde_json::json!({"prewarm":false,"goal":false})),
        ..Default::default()
    };
    let session = agent
        .open_session(store(&live_dir), options())
        .await
        .unwrap();
    session
        .enqueue(
            Input::Text("durable pending input".into()),
            EnqueueOptions::default(),
        )
        .unwrap();
    let turn = session
        .resume_pending_streamed(TurnOptions::default())
        .await
        .unwrap()
        .unwrap();
    let uuid = session.queued_messages()[0].uuid.clone().unwrap();
    session.flush_rollout().await.unwrap();
    let file = std::fs::read_dir(&live_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|ext| ext == "jsonl"))
        .unwrap();
    let name = file.file_name().unwrap();
    std::fs::copy(&file, before_dir.join(name)).unwrap();
    let events: Vec<_> = turn.events.collect().await;
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
    session.flush_rollout().await.unwrap();
    let thread_id = session.id.try_to_utf8().unwrap();
    let text = std::fs::read_to_string(&file).unwrap();
    let mut truncated = String::new();
    let mut saw_handoff = false;
    for line in text.lines() {
        let _: serde_json::Value = serde_json::from_str(line).unwrap();
        if saw_handoff && line.contains("queue_state") {
            break;
        }
        truncated.push_str(line);
        truncated.push('\n');
        if line.contains("queue_resume_input") {
            saw_handoff = true;
        }
    }
    assert!(saw_handoff, "{text}");
    // This is the actual append prefix immediately before consumption's flush.
    std::fs::write(after_dir.join(name), truncated).unwrap();
    session.shutdown_and_wait().await.unwrap();
    for (directory, expected_pending, expected_users) in [(&before_dir, 1, 0), (&after_dir, 0, 1)] {
        let resumed = agent
            .resume_session(store(directory), &thread_id, options())
            .await
            .unwrap();
        assert_eq!(resumed.queue_snapshot().items.len(), expected_pending);
        let count = || {
            resumed
                .history()
                .iter()
                .filter(|message| matches!(message,Message::User(user) if user.uuid == uuid))
                .count()
        };
        assert_eq!(count(), expected_users);
        if let Some(turn) = resumed
            .resume_pending_streamed(TurnOptions::default())
            .await
            .unwrap()
        {
            let _: Vec<_> = turn.events.collect().await;
        }
        assert_eq!(count(), 1);
        resumed.shutdown_and_wait().await.unwrap();
    }
}

#[tokio::test]
async fn pending_resume_revalidates_attachments_against_the_actual_next_model() {
    let (session, requests) = session();
    session
        .enqueue(
            Input::Parts(vec![UserContentPart::Document {
                base64: "JVBERi0xLjcK".into(),
                mime_type: "application/pdf".into(),
                filename: None,
            }]),
            EnqueueOptions::default(),
        )
        .unwrap();
    let before = session.queued_messages();
    assert!(session
        .resume_pending_streamed(TurnOptions {
            model: Some("no-pdf".into()),
            ..Default::default()
        })
        .await
        .is_err());
    assert_eq!(session.queued_messages(), before);
    assert!(requests.lock().unwrap().is_empty());
    session.shutdown_and_wait().await.unwrap();

    let (session, requests) = self::session();
    session
        .enqueue(
            Input::Parts(vec![UserContentPart::Image {
                base64: "QUJD".into(),
                mime_type: "image/png".into(),
                source_path: None,
            }]),
            EnqueueOptions::default(),
        )
        .unwrap();
    let turn = session
        .resume_pending_streamed(TurnOptions {
            model: Some("blind".into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .unwrap();
    let _: Vec<_> = turn.events.collect().await;
    {
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(!requests[0].messages.iter().any(|message| matches!(message,Message::User(user) if user.content.iter().any(|block|matches!(block,klaus_agent_core::schemas::content::ContentBlock::Image(_))))));
    }
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn pending_human_input_runs_once_with_goal_controls_and_an_optional_active_goal() {
    use klaus_agent_core::types::provider::{ModelProvider, ProviderCapabilities, ProviderInfo};

    struct GoalToolsProvider(Arc<dyn ModelProvider>);
    impl ModelProvider for GoalToolsProvider {
        fn info(&self) -> &ProviderInfo {
            self.0.info()
        }
        fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
            let mut capabilities = self.0.capabilities(model);
            capabilities.tools = true;
            capabilities
        }
        fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
            self.0.stream(request, signal)
        }
    }

    struct Directory(std::path::PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    struct ConfigHome(Option<std::ffi::OsString>);
    impl ConfigHome {
        fn new(path: &std::path::Path) -> Self {
            let previous = std::env::var_os("KLAUS_CONFIG_DIR");
            klaus_agent_core::runtime_process::set_process_environment_var(
                "KLAUS_CONFIG_DIR",
                path,
            );
            Self(previous)
        }
    }
    impl Drop for ConfigHome {
        fn drop(&mut self) {
            match self.0.take() {
                Some(previous) => klaus_agent_core::runtime_process::set_process_environment_var(
                    "KLAUS_CONFIG_DIR",
                    previous,
                ),
                None => klaus_agent_core::runtime_process::remove_process_environment_var(
                    "KLAUS_CONFIG_DIR",
                ),
            }
        }
    }
    for active_goal in [false, true] {
        let root = Directory(
            std::env::temp_dir().join(format!("klaus-pending-goal-{}", uuid::Uuid::new_v4())),
        );
        std::fs::create_dir_all(&root.0).unwrap();
        let _config_home = ConfigHome::new(&root.0);
        let (agent, requests) = agent();
        let provider = GoalToolsProvider(
            agent_options(RuntimeProcessContext::initialize(process_supervisor()).unwrap())
                .provider,
        );
        let session = agent
            .open_session(
                Arc::new(
                    klaus_agent_core::store::jsonl_store::JsonlSessionStore::new(
                        klaus_agent_core::store::jsonl_store::JsonlSessionStoreOptions {
                            dir: Some(root.0.to_string_lossy().into_owned()),
                            archive_dir: None,
                        },
                    ),
                ),
                StartSessionOptions {
                    provider: Some(Arc::new(provider)),
                    cwd: Some(root.0.to_string_lossy().into_owned().into()),
                    user_config_dir: Some(root.0.to_string_lossy().into_owned().into()),
                    config: Some(serde_json::json!({
                        "prewarm": false, "goal": true, "autoMemory": false, "autoDream": false
                    })),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let runtime = session.goal_runtime().unwrap();
        if active_goal {
            session
                .set_thread_goal(Some("fixture objective".into()), None, None)
                .await
                .unwrap();
        }
        let before_goal = runtime.goal().await.unwrap();
        assert_eq!(before_goal.is_some(), active_goal);
        session
            .enqueue(
                Input::Text("queued human input".into()),
                EnqueueOptions {
                    correlation_id: Some("queued-human".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        session.await_queue_durability().await.unwrap();
        let before = session.queue_snapshot();
        assert_eq!(before.items.len(), 1);
        let human_uuid = before.items[0].message.uuid.clone().unwrap();
        let turn = session
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
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, EngineEvent::TurnCompleted { .. }))
                .count(),
            1,
            "one pending human batch must produce exactly one completed turn"
        );
        assert_eq!(
            events.iter().filter(|event| matches!(event, EngineEvent::InputApplied { correlation_id: Some(id) } if id == "queued-human")).count(),
            1
        );
        {
            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert!(requests[0].messages.iter().any(|message| matches!(message,
                Message::User(user) if user.uuid == human_uuid && user.is_meta != Some(true))));
            for name in ["get_goal", "create_goal", "update_goal"] {
                assert!(requests[0].tools.iter().any(|tool| tool.name == name));
            }
        }
        let metadata = session.turn_index_snapshot().turn_goal_metadata;
        assert_eq!(metadata.len(), 1);
        let metadata = metadata.values().next().unwrap();
        assert!(!metadata.goal_continuation);
        assert_eq!(metadata.input_origin, "user");
        assert_eq!(
            metadata.goal_id,
            before_goal.as_ref().map(|goal| goal.goal_id.clone())
        );
        assert!(!runtime.is_automatic_turn());
        let after_goal = runtime.goal().await.unwrap();
        match (before_goal, after_goal) {
            (Some(before), Some(after)) => {
                assert_eq!(after.goal_id, before.goal_id);
                assert_eq!(after.objective, before.objective);
                assert_eq!(
                    after.status,
                    klaus_agent_core::goal::types::ThreadGoalStatus::Active
                );
            }
            (None, None) => {}
            other => panic!("pending human input changed goal existence: {other:?}"),
        }
        assert!(session.queue_snapshot().items.is_empty());
        assert!(session
            .resume_pending_streamed(TurnOptions::default())
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            requests.lock().unwrap().len(),
            1,
            "draining pending human input cannot launch an extra automatic turn"
        );
        session.shutdown_and_wait().await.unwrap();
    }
}
