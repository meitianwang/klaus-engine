//! Defect: reloading a goal conversation rendered internal context as repeated user objectives.
//! Root cause: rollout writes discarded the captured goal-context message's meta identity.
//! Work item: 20261018 goal-context history recovery.

use super::goal_stream_fixture::{idle, session_slot, text_events, usage, Fixture, WAIT};
use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};
use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, Turn, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::{Message, UserMessage};
use klaus_agent_core::schemas::rollout::RolloutItem;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::types::events::EngineEvent;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

async fn drain(mut turn: Turn) -> RuntimeString {
    tokio::time::timeout(WAIT, async {
        let mut started = None;
        let mut completed = 0;
        let mut count = 0;
        while let Some(event) = turn.events.next().await {
            count += 1;
            assert!(count <= 128, "unexpected turn event flood");
            match event {
                EngineEvent::TurnStarted { turn_id } => {
                    assert!(started.replace(turn_id).is_none());
                }
                EngineEvent::TurnCompleted { .. } => completed += 1,
                EngineEvent::TurnFailed { error, .. } => panic!("{error:?}"),
                _ => {}
            }
        }
        assert_eq!(completed, 1);
        started.expect("one actual turn started")
    })
    .await
    .expect("the scripted turn settles")
}

fn last_user(messages: &[Message]) -> UserMessage {
    messages
        .iter()
        .rev()
        .find_map(|message| match message {
            Message::User(user) => Some(user.clone()),
            _ => None,
        })
        .expect("the actual input is present")
}

fn visible_users(messages: &[Message]) -> Vec<UserMessage> {
    messages
        .iter()
        .filter_map(|message| match message {
            Message::User(user) if user.is_meta != Some(true) => Some(user.clone()),
            _ => None,
        })
        .collect()
}

fn text(user: &UserMessage) -> RuntimeString {
    assert_eq!(user.content.len(), 1);
    match &user.content[0] {
        ContentBlock::Text(block) => block.text.clone(),
        other => panic!("expected the exact text input, got {other:?}"),
    }
}

#[tokio::test]
async fn initial_and_automatic_goal_context_stay_meta_after_rollout_and_reload() {
    let slot = session_slot();
    let fixture = Fixture::new(&slot, |_, _, call| {
        assert!(
            call < 3,
            "only two goal turns and one human turn are admitted"
        );
        Box::pin(futures::stream::iter(text_events(
            "Recorded progress.",
            usage(1, 1),
        )))
    })
    .await;
    let objective: RuntimeString = "Keep one visible objective after reconnect".into();
    fixture
        .session
        .set_thread_goal(Some(objective.clone()), None, None)
        .await
        .unwrap();
    let goal_id = fixture
        .session
        .thread_goal()
        .await
        .unwrap()
        .unwrap()
        .goal_id;
    let submission = fixture
        .session
        .record_submitted_goal_objective(
            "goal-context-rollout-meta".into(),
            objective.clone(),
            None,
        )
        .await
        .unwrap();
    fixture.session.mark_goal_submission_pending(&goal_id);

    let initial_turn = drain(idle(&fixture.session).await).await;
    let initial = last_user(&fixture.session.history());
    let automatic_turn = drain(idle(&fixture.session).await).await;
    let automatic = last_user(&fixture.session.history());
    assert_ne!(initial.uuid, automatic.uuid);
    assert_ne!(initial.uuid, submission.message_uuid);
    assert_eq!(initial.is_meta, Some(true));
    assert_eq!(automatic.is_meta, Some(true));
    let index = fixture.session.turn_index_snapshot();
    assert!(
        !index.turn_goal_metadata[&initial_turn].goal_continuation,
        "the initial submission context is internal even though it is not a continuation"
    );
    assert_eq!(index.turn_goal_metadata[&initial_turn].input_origin, "user");
    assert!(index.turn_goal_metadata[&automatic_turn].goal_continuation);
    assert_eq!(
        index.turn_goal_metadata[&automatic_turn].input_origin,
        "automatic"
    );
    assert_eq!(
        index.turn_goal_metadata[&initial_turn].goal_id.as_ref(),
        Some(&goal_id)
    );
    assert_eq!(
        index.turn_goal_metadata[&automatic_turn].goal_id.as_ref(),
        Some(&goal_id)
    );
    let live_visible = visible_users(&fixture.session.history());
    assert_eq!(live_visible.len(), 1);
    assert_eq!(live_visible[0].uuid, submission.message_uuid);
    assert_eq!(text(&live_visible[0]), objective);

    fixture
        .session
        .set_thread_goal(None, Some("paused".into()), None)
        .await
        .unwrap();
    let human_text: RuntimeString = "Continue my ordinary human follow-up visibly".into();
    drain(
        fixture
            .session
            .run_streamed(Input::Text(human_text.clone()), TurnOptions::default()),
    )
    .await;
    let human = last_user(&fixture.session.history());
    assert_eq!(human.is_meta, None);
    assert_eq!(text(&human), human_text);
    let expected_visible = visible_users(&fixture.session.history());
    assert_eq!(expected_visible.len(), 2);
    assert_eq!(expected_visible[0], live_visible[0]);
    assert_eq!(expected_visible[1], human);
    assert_eq!(fixture.requests.lock().unwrap().len(), 3);
    for (request, expected) in fixture
        .requests
        .lock()
        .unwrap()
        .iter()
        .zip([&initial, &automatic, &human])
    {
        let actual = request
            .messages
            .iter()
            .find_map(|message| match message {
                Message::User(user) if user.uuid == expected.uuid => Some(user),
                _ => None,
            })
            .expect("the recorded identity reached the real provider");
        assert_eq!(actual, expected);
    }

    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(
            fixture
                .config_root()
                .join("sessions")
                .to_string_lossy()
                .into_owned(),
        ),
        archive_dir: None,
    }));
    let thread = fixture.session.id.try_to_utf8().unwrap();
    fixture.session.flush_rollout().await.unwrap();
    let saved = store.read_thread(&thread).await.unwrap();
    let saved_messages: Vec<_> = saved
        .raw_items
        .into_iter()
        .filter_map(|item| match item {
            RolloutItem::Message { message } => Some(message),
            _ => None,
        })
        .collect();
    for expected in [&initial, &automatic] {
        let saved_user = saved_messages
            .iter()
            .find_map(|message| match message {
                Message::User(user) if user.uuid == expected.uuid => Some(user),
                _ => None,
            })
            .expect("the exact context UUID was persisted");
        assert_eq!(
            saved_user.is_meta,
            Some(true),
            "captured goal context must remain internal in the durable rollout"
        );
        assert_eq!(saved_user, expected);
    }
    assert_eq!(
        visible_users(&saved_messages),
        expected_visible,
        "saved history has the same genuine objective and ordinary human input as live history"
    );

    fixture.session.shutdown_and_wait().await.unwrap();
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let reload_calls = Arc::new(AtomicUsize::new(0));
    let mut options = agent_options(process.clone());
    options.deps.call_model = Some(Box::new({
        let reload_calls = reload_calls.clone();
        move |_, _| {
            reload_calls.fetch_add(1, Ordering::SeqCst);
            panic!("history reload must not invoke the provider")
        }
    }));
    let resumed = Agent::new(options)
        .resume_session(
            store,
            &thread,
            StartSessionOptions {
                cwd: Some(fixture.config_root().to_string_lossy().into_owned().into()),
                user_config_dir: Some(fixture.config_root().to_string_lossy().into_owned().into()),
                config: Some(
                    json!({"goal":true,"prewarm":false,"autoMemory":false,"autoDream":false,
            "memoryRecall":false,"agentMemory":false,"memory":{"enabled":false}}),
                ),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let reopened = resumed.history();
    assert_eq!(visible_users(&reopened), expected_visible);
    for expected in [&initial, &automatic] {
        assert!(
            reopened.iter().any(|message| matches!(message,
            Message::User(user) if user == expected)),
            "reload preserves the exact internal message, including its meta flag"
        );
    }
    assert_eq!(reload_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.requests.lock().unwrap().len(), 3);
    resumed.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    fixture.finish().await;
}
