//! Defect: resumed now/next messages reached the provider with unexpanded mentions.
//! Root cause: delivery preprocessing inspected only the synthetic empty turn input.
//! Fixed for the 2026-09-14 handoff work item: expand each original message separately.

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};
use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::agent_definitions::AgentDefinition;
use klaus_agent_core::engine::message_queue::QueuePriority;
use klaus_agent_core::engine::session::{EnqueueOptions, Input, Session, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::tools::builtins::read::create_read_tool;
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::provider::{ModelRequest, ModelStopReason, ModelStreamEvent};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

struct Fixture {
    root: PathBuf,
    session: Arc<Session>,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn fixture() -> Fixture {
    let root =
        std::env::temp_dir().join(format!("klaus-pending-mentions-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("first.txt"), "FIRST_FILE_MARKER\n").unwrap();
    std::fs::write(root.join("second.txt"), "SECOND_FILE_MARKER\n").unwrap();
    let mut options =
        agent_options(RuntimeProcessContext::initialize(process_supervisor()).unwrap());
    options.tools = Some(vec![create_read_tool()]);
    options.agents = Some(vec![AgentDefinition {
        name: "mention-fixture".into(),
        description: "A fixture agent".into(),
        ..Default::default()
    }]);
    let requests = Arc::new(Mutex::new(Vec::new()));
    options.deps.call_model = Some({
        let requests = requests.clone();
        Box::new(move |request, _| {
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
        })
    });
    let session = Agent::new(options).start_session(StartSessionOptions {
        cwd: Some(root.to_string_lossy().into_owned().into()),
        user_config_dir: Some(root.to_string_lossy().into_owned().into()),
        config: Some(serde_json::json!({
            "prewarm": false, "goal": false, "autoMemory": false,
            "autoDream": false, "agentMemory": false, "checkpointing": false
        })),
        ..Default::default()
    });
    Fixture {
        root,
        session,
        requests,
    }
}

fn text_for(messages: &[Message], uuid: &RuntimeString) -> String {
    let user = messages
        .iter()
        .find_map(|message| match message {
            Message::User(user) if &user.uuid == uuid => Some(user),
            _ => None,
        })
        .expect("original user message survives delivery");
    user.content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text(text) => Some(text.text.try_to_utf8().expect("scalar fixture text")),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[tokio::test]
async fn pending_mentions_expand_each_original_message_before_one_provider_call() {
    let fixture = fixture();
    for (input, priority) in [
        (
            "inspect @first.txt @agent-mention-fixture",
            QueuePriority::Next,
        ),
        ("inspect @second.txt", QueuePriority::Now),
    ] {
        fixture
            .session
            .enqueue(
                Input::Text(input.into()),
                EnqueueOptions {
                    priority: Some(priority),
                    ..Default::default()
                },
            )
            .unwrap();
    }
    let before = fixture.session.queue_snapshot();
    let ids: Vec<_> = before
        .items
        .iter()
        .map(|item| item.message.uuid.clone().unwrap())
        .collect();
    let turn = fixture
        .session
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
        1
    );
    assert!(fixture.session.queue_snapshot().items.is_empty());
    {
        let requests = fixture.requests.lock().unwrap();
        assert_eq!(
            requests.len(),
            1,
            "one batch must remain one inference turn"
        );
        for messages in [&requests[0].messages, &fixture.session.history()] {
            let first = text_for(messages, &ids[0]);
            let second = text_for(messages, &ids[1]);
            assert!(first.starts_with("inspect @first.txt @agent-mention-fixture"));
            assert!(first.contains("FIRST_FILE_MARKER"), "{first}");
            assert!(!first.contains("SECOND_FILE_MARKER"), "{first}");
            assert!(
                first.contains("subagent_type \"mention-fixture\""),
                "{first}"
            );
            assert!(second.starts_with("inspect @second.txt"));
            assert!(second.contains("SECOND_FILE_MARKER"), "{second}");
            assert!(!second.contains("FIRST_FILE_MARKER"), "{second}");
            assert!(!second.contains("subagent_type"), "{second}");
        }
    }
    fixture.session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn direct_mentions_still_expand_after_shared_delivery_processing() {
    let fixture = fixture();
    let events: Vec<_> = fixture
        .session
        .run_streamed(
            Input::Text("inspect @first.txt".into()),
            TurnOptions::default(),
        )
        .events
        .collect()
        .await;
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
    {
        let requests = fixture.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let uuid = requests[0]
            .messages
            .iter()
            .find_map(|message| match message {
                Message::User(user) => Some(user.uuid.clone()),
                _ => None,
            })
            .unwrap();
        assert!(text_for(&requests[0].messages, &uuid).contains("FIRST_FILE_MARKER"));
    }
    fixture.session.shutdown_and_wait().await.unwrap();
}
