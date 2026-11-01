//! A message the host appends without running a turn reaches the live history.
//!
//! What broke: friend messages written with `append_display_message` went to
//! the rollout only. While the session stayed open the model never saw them;
//! after a resume the same text appeared in its context. Two identical
//! conversations therefore gave the model different context depending on
//! whether the session had been reloaded.
//!
//! Root cause: `append_display_message` appended the user message and its
//! display record to the rollout without pushing them into the session's
//! in-memory history.
//!
//! Fixed by: pushing both records into the live history (once per message id)
//! and indexing them under the appended turn. Fixing change: 2026-10-31
//! working change; no separate tracker item.
//!
//! Second defect, same entry point: the composer never offered a suggestion
//! after an appended row. A conversation holding only friend messages had no
//! suggestion source at all, and an older conversation kept suggesting from
//! the turn before the friend message. Root cause: the appended turn had no
//! live `turn_status` and did not move the prediction source checkpoint, which
//! only real turn admission advanced. Fixed by marking the appended turn
//! completed and admitting it as the prediction source (persisted, so a
//! resumed session agrees) while no turn is running. Fixing change:
//! 2026-10-31 working change; no separate tracker item.
use super::context_fixture;
use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::Input;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeString};
use klaus_agent_core::schemas::message::{Message, SystemSubtype};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use std::io::Write;
use std::sync::Arc;

fn store(path: &std::path::Path) -> Arc<JsonlSessionStore> {
    std::fs::create_dir_all(path).unwrap();
    let mut file = std::fs::File::create(path.join("1-display-live.jsonl")).unwrap();
    file.write_all(concat!(r#"{"kind":"session_meta","schemaVersion":3,"meta":{"id":"display-live","createdAt":1,"cwd":"/tmp","model":"fixture"}}"#, "\n").as_bytes()).unwrap();
    Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(path.to_string_lossy().into_owned()),
        archive_dir: None,
    }))
}

fn user_texts(history: &[Message]) -> Vec<String> {
    history
        .iter()
        .filter_map(|message| match message {
            Message::User(user) => Some(
                user.content
                    .iter()
                    .filter_map(|block| match block {
                        klaus_agent_core::schemas::content::ContentBlock::Text(text) => {
                            Some(text.text.try_to_utf8().unwrap())
                        }
                        _ => None,
                    })
                    .collect::<String>(),
            ),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn an_appended_message_is_in_the_open_session_and_after_a_resume() {
    let dir = context_fixture::tempdir().unwrap();
    let sessions = dir.path().join("sessions");
    let store = store(&sessions);
    let open = |store: Arc<JsonlSessionStore>| {
        let dir = dir.path().to_string_lossy().into_owned();
        async move {
            let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
            Agent::new(agent_options(process))
                .resume_session(
                    store,
                    "display-live",
                    StartSessionOptions {
                        cwd: Some(dir.clone().into()),
                        user_config_dir: Some(dir.into()),
                        system_prompt_provider: Some(Arc::new(|| Box::pin(async { Vec::new() }))),
                        ..Default::default()
                    },
                )
                .await
                .unwrap()
        }
    };
    let session = open(store.clone()).await;
    let metadata = runtime_json::parse(&RuntimeString::from(
        r#"{"externalFriendMessage":true,"displayText":"good night"}"#,
    ))
    .unwrap();
    let text = "<friend-message from=\"Nick\">good night</friend-message>";
    session
        .append_display_message(
            "friend-message-1".into(),
            Input::Text(text.into()),
            metadata.clone(),
        )
        .await
        .unwrap();
    // The open session reads it on its next turn, without a reload.
    let live = session.history();
    assert_eq!(user_texts(&live), vec![text.to_string()]);
    assert!(live.iter().any(|message| matches!(message, Message::System(system) if system.subtype == SystemSubtype::UserDisplay)));
    let appended_turn = session
        .turn_index_snapshot()
        .message_turn
        .get(&RuntimeString::from("friend-message-1"))
        .cloned()
        .expect("appended message is indexed under its turn");
    // The composer suggests a reply to the appended row right away.
    let context = session.prediction_context().await.unwrap();
    assert_eq!(context.turn_id, Some(appended_turn.clone()));
    assert!(!context.running);
    // A host retry with the same id is not added twice.
    session
        .append_display_message(
            "friend-message-1".into(),
            Input::Text(text.into()),
            metadata,
        )
        .await
        .unwrap();
    assert_eq!(user_texts(&session.history()), vec![text.to_string()]);
    session.shutdown_and_wait().await.unwrap();
    // A resumed session sees the same message.
    let resumed = open(store.clone()).await;
    assert_eq!(
        user_texts(&resumed.history()).first(),
        Some(&text.to_string())
    );
    let context = resumed.prediction_context().await.unwrap();
    assert_eq!(context.turn_id, Some(appended_turn));
    assert!(!context.running);
    resumed.shutdown_and_wait().await.unwrap();
    let reopened = store
        .resume_runtime_for_write("display-live")
        .await
        .unwrap();
    reopened.rollout.shutdown().await.unwrap();
}
