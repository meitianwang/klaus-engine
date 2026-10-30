//! Broke: long-lived sessions decoded every obsolete review snapshot on resume
//! and child hydration read the same parent rollout again.
//! Root cause: continuation shared the untrimmed audit-read contract rather than
//! reconstructing runtime state from authoritative checkpoints and original sources.
//! Native work item: b7c2c984-235c-4169-a129-beba4ca4ef77.

use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::approval_capture::ReviewParentHistory;
use klaus_agent_core::engine::questions::QuestionAnswer;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeString};
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{AssistantMessage, Message, Usage, UserMessage};
use klaus_agent_core::schemas::rollout::{
    CheckpointItem, CompactedItem, EventItem, RollbackItem, RolloutItem,
};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::rollout::{parse_rollout_line, serialize_rollout_item};
use klaus_agent_core::store::runtime_resume::RuntimeUsage;
use klaus_agent_core::store::session_store::SessionStore;
use std::io::Write;
use std::sync::Arc;

fn user(id: &str, answer: bool) -> Message {
    Message::User(UserMessage {
        uuid: id.into(),
        content: vec![ContentBlock::Text(TextBlock {
            text: format!("Original content of {id}").into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })],
        is_meta: None,
        question_answer: answer.then(|| {
            Box::new(QuestionAnswer {
                request_id: "question".into(),
                turn_id: "old-turn".into(),
                tool_call_id: "question-tool".into(),
                client_response_id: "response".into(),
                questions: Vec::new(),
                answers: Default::default(),
            })
        }),
    })
}
fn checkpoint(
    replacement: Vec<Message>,
    source: Option<Vec<RuntimeString>>,
    complete: bool,
) -> RolloutItem {
    RolloutItem::Compacted(CompactedItem {
        summary: "Resume from the retained task".into(),
        replaced_range: (0, 100),
        replacement_history: Some(replacement),
        source_message_ids: source,
        approval_parent_history: complete.then(|| {
            ReviewParentHistory::default()
                .checkpoint()
                .to_runtime_value()
                .unwrap()
        }),
    })
}
fn event(name: &str, turn: Option<&str>, payload: &str) -> RolloutItem {
    RolloutItem::Event(EventItem {
        event: name.into(),
        turn_id: turn.map(Into::into),
        payload: Some(runtime_json::parse(&payload.into()).unwrap()),
    })
}
fn fixture(path: &std::path::Path, items: &[RolloutItem]) -> Arc<JsonlSessionStore> {
    std::fs::create_dir_all(path).unwrap();
    let mut file = std::fs::File::create(path.join("1-runtime-resume.jsonl")).unwrap();
    file.write_all(concat!(r#"{"kind":"session_meta","schemaVersion":3,"meta":{"id":"runtime-resume","createdAt":1,"cwd":"/tmp","model":"fixture"}}"#, "\n").as_bytes()).unwrap();
    for item in items {
        writeln!(file, "{}", serialize_rollout_item(item)).unwrap();
    }
    Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(path.to_string_lossy().into_owned()),
        archive_dir: None,
    }))
}
fn context() -> RolloutItem {
    parse_rollout_line(r#"{"kind":"turn_context","ctx":{"cwd":"/tmp","model":"fixture","permissionMode":"default","turnId":"old-turn","startedAtMs":42}}"#).unwrap()
}

#[tokio::test]
async fn runtime_window_preserves_typed_answers_full_index_billing_and_original_reads() {
    let dir = super::context_fixture::tempdir().unwrap();
    let original = user("old-user", false);
    let answer = user("late-answer", true);
    let retained = user("retained-user", false);
    let assistant = Message::Assistant(AssistantMessage {
        uuid: "old-assistant".into(),
        model: "fixture".into(),
        content: Vec::new(),
        usage: Some(Usage {
            input_tokens: 17,
            output_tokens: 3,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
            total_tokens: None,
        }),
    });
    let mut items = vec![
        context(),
        RolloutItem::Message {
            message: original.clone(),
        },
        RolloutItem::Message { message: assistant },
        RolloutItem::Message {
            message: answer.clone(),
        },
    ];
    items.push(event(
        "file_change",
        None,
        r#"{"path":"src/retained.rs","kind":"modify","added":7,"removed":2}"#,
    ));
    items.push(event(
        "tool_lifecycle",
        Some("old-turn"),
        r#"{"phase":"cancelled","toolUseId":"cancelled-tool","turnId":"old-turn"}"#,
    ));
    items.push(RolloutItem::Checkpoint(CheckpointItem {
        hash: Some("old-tree-hash".into()),
    }));
    let value = ReviewParentHistory::default()
        .checkpoint()
        .to_runtime_value()
        .unwrap();
    for _ in 0..128 {
        items.push(RolloutItem::Event(EventItem {
            event: "approval_parent_history".into(),
            turn_id: None,
            payload: Some(value.clone()),
        }));
    }
    items.push(event("turn_lifecycle", Some("old-turn"), r#"{"status":"failed","error":"Original failure","finalMessageId":"old-assistant","startedAtMs":42,"finishedAtMs":50}"#));
    // This answer was admitted outside the source watermark and must survive.
    items.push(checkpoint(
        vec![retained.clone()],
        Some(vec!["old-user".into(), "old-assistant".into()]),
        true,
    ));
    items.push(RolloutItem::Message {
        message: user("suffix-user", false),
    });
    let store = fixture(dir.path(), &items);
    let full = store.resume("runtime-resume").await.unwrap();
    let runtime = store
        .resume_runtime_for_write("runtime-resume")
        .await
        .unwrap();
    assert_eq!(runtime.resume.messages, full.messages);
    assert!(runtime.resume.messages.contains(&answer));
    assert_eq!(runtime.resume.turn_index, full.turn_index);
    assert_eq!(runtime.resume.file_changes, full.file_changes);
    assert_eq!(
        runtime.resume.file_changes.turns[0].turn_id,
        Some("old-turn".into())
    );
    assert_eq!(runtime.resume.file_changes.turns[0].files[0].added, Some(7));
    assert_eq!(
        runtime
            .resume
            .turn_index
            .turn_cancelled_tools
            .get(&RuntimeString::from("old-turn")),
        Some(&vec![RuntimeString::from("cancelled-tool")])
    );
    assert_eq!(
        klaus_agent_core::store::projection::project_checkpoints(&runtime.resume.recovery_items),
        vec![Some("old-tree-hash".to_string())]
    );

    assert_eq!(
        runtime
            .resume
            .turn_index
            .message_turn
            .get(&RuntimeString::from("old-user")),
        Some(&RuntimeString::from("old-turn"))
    );
    assert_eq!(
        runtime
            .resume
            .turn_index
            .turn_errors
            .get(&RuntimeString::from("old-turn")),
        Some(&RuntimeString::from("Original failure"))
    );
    assert!(runtime
        .resume
        .persisted_user_uuids
        .contains(&RuntimeString::from("old-user")));
    assert_eq!(
        runtime
            .resume
            .usage
            .iter()
            .filter(|usage| matches!(usage, RuntimeUsage::Legacy { usage: Some(_), .. }))
            .count(),
        1
    );
    assert!(!runtime
        .resume
        .recovery_items
        .iter()
        .any(|item| matches!(item, RolloutItem::Message { message } if message == &original)));
    assert_eq!(runtime.resume.recovery_items.iter().filter(|item| matches!(item, RolloutItem::Event(event) if event.event == "approval_parent_history")).count(), 0);
    runtime.rollout.shutdown().await.unwrap();
    let mut expected_items = full.raw_items;
    expected_items.push(RolloutItem::Event(EventItem {
        event: "resumed".into(),
        turn_id: None,
        payload: None,
    }));
    assert_eq!(
        store.read_thread("runtime-resume").await.unwrap().raw_items,
        expected_items
    );
    let selected = store
        .read_messages_by_ids(
            "runtime-resume",
            &[
                "late-answer".into(),
                "old-user".into(),
                "missing".into(),
                "late-answer".into(),
            ],
        )
        .await
        .unwrap();
    assert_eq!(selected, vec![answer.clone(), original, answer]);
}

#[tokio::test]
async fn rollback_and_legacy_source_aware_replacement_match_complete_replay() {
    for complete in [false, true] {
        let dir = super::context_fixture::tempdir().unwrap();
        let mut items = vec![
            context(),
            RolloutItem::Message {
                message: user("old-user", false),
            },
            RolloutItem::Message {
                message: user("answer", true),
            },
            checkpoint(
                vec![user("retained", false)],
                Some(vec!["old-user".into()]),
                complete,
            ),
        ];
        items.push(RolloutItem::Message {
            message: user("rolled-back", false),
        });
        items.push(RolloutItem::Rollback(RollbackItem { num_turns: 1 }));
        let store = fixture(dir.path(), &items);
        let full = store.resume("runtime-resume").await.unwrap();
        let runtime = store
            .resume_runtime_for_write("runtime-resume")
            .await
            .unwrap();
        assert_eq!(runtime.resume.messages, full.messages);
        assert_eq!(runtime.resume.turn_index, full.turn_index);
        assert!(runtime.resume.recovery_items.iter().any(|item| matches!(item, RolloutItem::Message { message: Message::User(user) } if user.uuid == "old-user")));
        runtime.rollout.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn public_resume_skips_superseded_review_payload_and_hands_off_the_writer() {
    let dir = super::context_fixture::tempdir().unwrap();
    let items = vec![
        context(),
        RolloutItem::Message {
            message: user("old-user", false),
        },
        event(
            "approval_parent_history",
            None,
            r#"{"obsolete":"This superseded payload is not a valid checkpoint"}"#,
        ),
        checkpoint(
            vec![user("retained-user", false)],
            Some(vec!["old-user".into()]),
            true,
        ),
    ];
    let store = fixture(&dir.path().join("sessions"), &items);
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let agent =
        Agent::new(super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process));
    let session = agent
        .resume_session(
            store.clone(),
            "runtime-resume",
            StartSessionOptions {
                cwd: Some(dir.path().to_string_lossy().into_owned().into()),
                user_config_dir: Some(dir.path().to_string_lossy().into_owned().into()),
                system_prompt_provider: Some(Arc::new(|| Box::pin(async { Vec::new() }))),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(session.history(), vec![user("retained-user", false)]);
    assert!(session
        .turn_index_snapshot()
        .message_turn
        .contains_key(&RuntimeString::from("old-user")));
    session.shutdown_and_wait().await.unwrap();
    // A successful public continuation must release its exact storage lease.
    let reopened = store
        .resume_runtime_for_write("runtime-resume")
        .await
        .unwrap();
    reopened.rollout.shutdown().await.unwrap();
}

#[tokio::test]
async fn prefix_replacements_do_not_resurrect_previously_removed_answers() {
    let dir = super::context_fixture::tempdir().unwrap();
    let items = vec![
        context(),
        RolloutItem::Message {
            message: user("original", false),
        },
        RolloutItem::Message {
            message: user("removed-answer", true),
        },
        checkpoint(vec![user("legacy-retained", false)], None, false),
        RolloutItem::Message {
            message: user("surviving-answer", true),
        },
        checkpoint(
            vec![user("new-retained", false)],
            Some(vec!["legacy-retained".into()]),
            true,
        ),
    ];
    let store = fixture(dir.path(), &items);
    let full = store.resume("runtime-resume").await.unwrap();
    let resumed = store
        .resume_runtime_for_write("runtime-resume")
        .await
        .unwrap();
    assert_eq!(resumed.resume.messages, full.messages);
    assert!(!resumed
        .resume
        .messages
        .contains(&user("removed-answer", true)));
    assert!(resumed
        .resume
        .messages
        .contains(&user("surviving-answer", true)));
    resumed.rollout.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_authoritative_checkpoint_releases_its_writer_lease() {
    let dir = super::context_fixture::tempdir().unwrap();
    let mut bad = checkpoint(vec![user("retained", false)], Some(Vec::new()), true);
    if let RolloutItem::Compacted(compacted) = &mut bad {
        compacted.approval_parent_history = Some(runtime_json::parse(&"{}".into()).unwrap());
    }
    let store = fixture(dir.path(), &[context(), bad]);
    let error = store
        .resume_runtime_for_write("runtime-resume")
        .await
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    let reopened = store.reopen("runtime-resume").await.unwrap();
    reopened.shutdown().await.unwrap();
}
