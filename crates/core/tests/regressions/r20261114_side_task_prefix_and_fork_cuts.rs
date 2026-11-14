//! Defect: persisted side tasks exposed inherited parent messages in saved history
//! projections and lost hidden-prefix boundaries on cold resume; fork cuts lacked
//! turn-inclusive and post-message addressing.
//! Root cause: `start_side_task` only kept hidden message UUIDs in memory while
//! writing parent messages into the side rollout without a durable boundary, and
//! `fork_session` only supported `beforeMessageUuid` against post-compaction messages.

use std::sync::{Arc, Mutex};

use klaus_agent_core::engine::agent::{Agent, ForkCut, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::rollout::parse_rollout_line;
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::store::thread_history::project_thread_history;
use klaus_agent_core::types::provider::{ModelRequest, ModelStopReason, ModelStreamEvent};
use serde_json::{json, Value};

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

fn temp_store_root(label: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!(
        "klaus-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    root
}

fn turn_ctx(id: &str, at: i64) -> Value {
    json!({"kind":"turn_context","ctx":{"cwd":"/tmp","model":"m","permissionMode":"default","turnId":id,"startedAtMs":at}})
}

fn user_row(id: &str, text: &str) -> Value {
    json!({"kind":"message","message":{"type":"user","uuid":id,"content":[{"type":"text","text":text}]}})
}

fn assistant_row(id: &str, text: &str) -> Value {
    json!({"kind":"message","message":{"type":"assistant","uuid":id,"content":[{"type":"text","text":text}],"model":"m","usage":{"inputTokens":10,"outputTokens":5}}})
}

fn completed_row(turn: &str, final_message: &str, start: i64, end: i64) -> Value {
    json!({"kind":"event","event":"turn_lifecycle","turnId":turn,"payload":{"status":"completed","finalMessageId":final_message,"startedAtMs":start,"finishedAtMs":end,"durationMs":(end - start)}})
}

fn message_id_of(message: &Message) -> String {
    match message {
        Message::User(m) => m.uuid.try_to_utf8().unwrap(),
        Message::Assistant(m) => m.uuid.try_to_utf8().unwrap(),
        Message::ToolResult(m) => m.uuid.try_to_utf8().unwrap(),
        Message::System(m) => m.uuid.try_to_utf8().unwrap(),
        Message::CompactBoundary(m) => m.uuid.try_to_utf8().unwrap(),
    }
}

fn is_skill_catalog(message: &Message) -> bool {
    matches!(message, Message::User(user) if user.is_meta == Some(true)
        && user.content.iter().any(|block| matches!(block, ContentBlock::Text(t)
            if t.text.try_to_utf8().unwrap().starts_with("<skill-catalog:"))))
}

fn message_ids(messages: &[Message]) -> Vec<String> {
    messages
        .iter()
        .filter(|m| !is_skill_catalog(m))
        .map(message_id_of)
        .collect()
}

fn message_texts(messages: &[Message]) -> Vec<String> {
    messages
        .iter()
        .filter(|m| !is_skill_catalog(m))
        .filter_map(|m| match m {
            Message::User(u) if u.is_meta != Some(true) => u.content.iter().find_map(|b| match b {
                ContentBlock::Text(t) => Some(t.text.try_to_utf8().unwrap()),
                _ => None,
            }),
            Message::Assistant(a) => a.content.iter().find_map(|b| match b {
                ContentBlock::Text(t) => Some(t.text.try_to_utf8().unwrap()),
                _ => None,
            }),
            _ => None,
        })
        .collect()
}

fn test_agent_replying(replies: Vec<&'static str>) -> (Agent, Arc<Mutex<Vec<ModelRequest>>>) {
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let queue = Arc::new(Mutex::new(std::collections::VecDeque::from(replies)));
    let mut options =
        agent_options(RuntimeProcessContext::initialize(process_supervisor()).unwrap());
    options.config = Some(
        json!({"autoMemory":false,"autoDream":false,"memoryRecall":false,"memory":{"enabled":false}}),
    );
    options.deps.call_model = Some(Box::new({
        let requests = requests.clone();
        let queue = queue.clone();
        move |request, _| {
            requests.lock().unwrap().push(request);
            let reply = queue.lock().unwrap().pop_front().unwrap_or("ok");
            Box::pin(futures::stream::iter(vec![
                ModelStreamEvent::MessageStart { model: "m".into() },
                ModelStreamEvent::TextDelta { text: reply.into() },
                ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::EndTurn,
                    usage: Usage {
                        input_tokens: 4,
                        output_tokens: 2,
                        cache_read_input_tokens: 0,
                        cache_creation_input_tokens: 0,
                        total_tokens: None,
                    },
                },
            ]))
        }
    }));
    (Agent::new(options), requests)
}

fn write_rollout(root: &std::path::Path, id: &str, rows: &[Value]) {
    let text = rows
        .iter()
        .map(|row| {
            let line = row.to_string();
            assert!(parse_rollout_line(&line).is_some(), "{line}");
            line
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    std::fs::write(root.join(format!("1700000000000-{id}.jsonl")), text).unwrap();
}

#[tokio::test]
async fn persisted_side_task_hides_inherited_prefix_across_page_projection_cold_resume_and_rollback(
) {
    let root = temp_store_root("side-task-prefix");
    let rows = vec![
        json!({"kind":"session_meta","schemaVersion":3,"meta":{"id":"parent","createdAt":1700000000000_i64,"cwd":root,"model":"m","role":"main"}}),
        turn_ctx("p-turn-1", 1700000000100),
        user_row("p-u1", "parent prompt 1"),
        assistant_row("p-a1", "parent answer 1"),
        completed_row("p-turn-1", "p-a1", 1700000000100, 1700000000200),
        turn_ctx("p-turn-2", 1700000000300),
        user_row("p-u2", "parent prompt 2"),
        assistant_row("p-a2", "parent answer 2"),
        completed_row("p-turn-2", "p-a2", 1700000000300, 1700000000400),
    ];
    write_rollout(&root, "parent", &rows);

    let jsonl = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let store: Arc<dyn SessionStore> = jsonl.clone();
    let (agent, requests) = test_agent_replying(vec!["side reply 1", "side reply 2"]);

    let side = agent
        .create_side_task(store.clone(), "parent", StartSessionOptions::default())
        .await
        .unwrap();
    let side_id = side.id.try_to_utf8().unwrap();
    side.flush_rollout().await.unwrap();

    // Fresh side task before any turn: visible projections on disk and in memory are empty.
    assert!(side.visible_history().is_empty());
    assert_eq!(
        message_ids(&side.history()),
        vec!["p-u1", "p-a1", "p-u2", "p-a2"]
    );
    let initial_page = jsonl
        .paged_thread_history(&side_id, None, Some(25), Some("full"), Some("desc"))
        .await
        .unwrap()
        .unwrap();
    assert!(initial_page.shells.is_empty());
    assert!(initial_page.history.history.is_empty());
    let initial_raw = store.read_thread(&side_id).await.unwrap();
    assert!(project_thread_history(initial_raw.raw_items)
        .history
        .is_empty());

    // Submit two side-task turns.
    side.run(Input::Text("side ask 1".into()), TurnOptions::default())
        .await;
    side.run(Input::Text("side ask 2".into()), TurnOptions::default())
        .await;
    side.dispose().await;

    // Paged SQLite projection and full rollout projection return only the two side turns.
    let paged = jsonl
        .paged_thread_history(&side_id, None, Some(25), Some("full"), Some("desc"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(paged.shells.len(), 2);
    assert_eq!(
        message_texts(&paged.history.history),
        vec!["side ask 1", "side reply 1", "side ask 2", "side reply 2"]
    );
    let raw_after = store.read_thread(&side_id).await.unwrap();
    let projected = project_thread_history(raw_after.raw_items);
    assert_eq!(
        message_texts(&projected.history),
        vec!["side ask 1", "side reply 1", "side ask 2", "side reply 2"]
    );

    // Cold resume keeps parent context for the model, hides it from visible_history,
    // does not double-bill parent assistant turns, and bounds rollback to side turns.
    {
        let probed = store.resume_runtime_for_write(&side_id).await.unwrap();
        let billed_ids: Vec<String> = probed
            .resume
            .usage
            .iter()
            .map(|entry| match entry {
                klaus_agent_core::store::runtime_resume::RuntimeUsage::Typed(item) => {
                    item.assistant_message_id.try_to_utf8().unwrap()
                }
                klaus_agent_core::store::runtime_resume::RuntimeUsage::Legacy {
                    assistant_message_id,
                    ..
                } => assistant_message_id.try_to_utf8().unwrap(),
            })
            .collect();
        assert!(
            !billed_ids.iter().any(|id| id == "p-a1" || id == "p-a2"),
            "inherited parent replies must not be billed to the side task on resume: {billed_ids:?}"
        );
        assert!(!billed_ids.is_empty());
        probed.rollout.shutdown().await.unwrap();
    }
    let resumed_side = agent
        .resume_session(store.clone(), &side_id, StartSessionOptions::default())
        .await
        .unwrap();
    assert_eq!(
        message_texts(&resumed_side.visible_history()),
        vec!["side ask 1", "side reply 1", "side ask 2", "side reply 2"]
    );
    assert_eq!(
        &message_ids(&resumed_side.history())[..4],
        &["p-u1", "p-a1", "p-u2", "p-a2"]
    );

    let r1 = resumed_side.rollback(1).await.unwrap();
    assert_eq!(r1.dropped_turns, 1);
    assert_eq!(r1.remaining_turns, 1);
    assert_eq!(
        message_texts(&resumed_side.visible_history()),
        vec!["side ask 1", "side reply 1"]
    );

    let r2 = resumed_side.rollback(5).await.unwrap();
    assert_eq!(r2.dropped_turns, 1);
    assert_eq!(r2.remaining_turns, 0);
    assert!(resumed_side.visible_history().is_empty());
    assert_eq!(
        message_ids(&resumed_side.history()),
        vec!["p-u1", "p-a1", "p-u2", "p-a2"],
        "rollback must never erase the inherited parent context prefix"
    );
    resumed_side.dispose().await;

    // Paged read after rollback also stays empty and never exposes parent messages.
    let paged_after_rollback = jsonl
        .paged_thread_history(&side_id, None, Some(25), Some("full"), Some("desc"))
        .await
        .unwrap()
        .unwrap();
    assert!(paged_after_rollback.shells.is_empty());
    assert!(paged_after_rollback.history.history.is_empty());
    assert_eq!(requests.lock().unwrap().len(), 2);

    std::fs::remove_dir_all(&root).ok();
}

#[tokio::test]
async fn legacy_side_task_rollout_hides_prefix_and_invalidates_stale_preamble_cache() {
    let root = temp_store_root("legacy-side-task");
    let legacy_rows = vec![
        json!({"kind":"session_meta","schemaVersion":3,"meta":{"id":"legacy-side","createdAt":1700000000000_i64,"cwd":root,"model":"m","role":"side_task","parentThreadId":"parent"}}),
        user_row("p-u1", "inherited parent question"),
        assistant_row("p-a1", "inherited parent answer"),
        json!({"kind":"event","event":"approval_parent_history","payload":{"transcript":[]}}),
        json!({"kind":"checkpoint","hash":null}),
        turn_ctx("side-turn-1", 1700000000500),
        user_row("s-u1", "actual side question"),
        assistant_row("s-a1", "actual side answer"),
        completed_row("side-turn-1", "s-a1", 1700000000500, 1700000000600),
    ];
    write_rollout(&root, "legacy-side", &legacy_rows);

    // Seed a v6 SQLite index containing a stale preamble turn row for legacy-side.
    let db_path = root.join("threads-index.sqlite3");
    {
        let db =
            klaus_agent_core::store::thread_index::TurnIndexDb::open(db_path.to_str().unwrap())
                .unwrap();
        drop(db);
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "INSERT INTO page_threads_v2(id,path,indexed_len,stamp,newline,record_count,turn_count)
             VALUES('legacy-side','/stale',999999,1,1,2,1);
             INSERT INTO page_turns_v2(thread,id,offset) VALUES('legacy-side','legacy-side-preamble',0);
             INSERT INTO history_projection_state(thread,next_rollout_byte_offset,next_rollout_ordinal)
             VALUES('legacy-side',999999,10);
             PRAGMA user_version=6;",
        )
        .unwrap();
    }

    let jsonl = JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.to_string_lossy().into_owned()),
        archive_dir: None,
    });
    let paged = jsonl
        .paged_thread_history("legacy-side", None, Some(25), Some("full"), Some("desc"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(paged.shells.len(), 1);
    assert_eq!(paged.shells[0].id, "side-turn-1");
    assert_eq!(
        message_texts(&paged.history.history),
        vec!["actual side question", "actual side answer"]
    );

    let raw = SessionStore::read_thread(&jsonl, "legacy-side")
        .await
        .unwrap();
    assert_eq!(
        message_texts(&project_thread_history(raw.raw_items).history),
        vec!["actual side question", "actual side answer"]
    );

    std::fs::remove_dir_all(&root).ok();
}

#[tokio::test]
async fn fork_session_with_cut_supports_turn_and_post_message_cuts_including_pre_compaction() {
    let root = temp_store_root("fork-cuts");
    let rows = vec![
        json!({"kind":"session_meta","schemaVersion":3,"meta":{"id":"src","createdAt":1700000000000_i64,"cwd":root,"model":"m","role":"main"}}),
        turn_ctx("turn-1", 1700000000100),
        user_row("u1", "question 1"),
        assistant_row("a1", "answer 1"),
        completed_row("turn-1", "a1", 1700000000100, 1700000000200),
        turn_ctx("turn-2", 1700000000300),
        user_row("u2", "question 2"),
        assistant_row("a2", "answer 2"),
        completed_row("turn-2", "a2", 1700000000300, 1700000000400),
        json!({"kind":"compacted","summary":"summary of turns 1 and 2","replacedRange":[0,4]}),
        turn_ctx("turn-3", 1700000000500),
        user_row("u3", "question 3"),
        assistant_row("a3", "answer 3"),
        completed_row("turn-3", "a3", 1700000000500, 1700000000600),
    ];
    write_rollout(&root, "src", &rows);

    let preamble_rows = vec![
        json!({"kind":"session_meta","schemaVersion":3,"meta":{"id":"imported","createdAt":1700000000000_i64,"cwd":root,"model":"m","role":"main","forkedFromId":"older"}}),
        user_row("iu1", "imported q1"),
        assistant_row("ia1", "imported a1"),
        user_row("iu2", "imported q2"),
        assistant_row("ia2", "imported a2"),
    ];
    write_rollout(&root, "imported", &preamble_rows);

    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let (agent, _) = test_agent_replying(Vec::new());

    // LastTurn("turn-1") before the post-turn-2 compaction restores uncompacted turn-1 only.
    let through_turn_1 = agent
        .fork_session_with_cut(
            store.clone(),
            "src",
            Some(ForkCut::LastTurn("turn-1".into())),
            StartSessionOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(message_ids(&through_turn_1.history()), vec!["u1", "a1"]);
    through_turn_1.dispose().await;

    // LastTurn("turn-2") before the compaction keeps turns 1 and 2 inclusive, omitting turn 3.
    let through_turn_2 = agent
        .fork_session_with_cut(
            store.clone(),
            "src",
            Some(ForkCut::LastTurn("turn-2".into())),
            StartSessionOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        message_ids(&through_turn_2.history()),
        vec!["u1", "a1", "u2", "a2"]
    );
    through_turn_2.dispose().await;

    // BeforeTurn("turn-2") keeps strictly before turn 2.
    let before_turn_2 = agent
        .fork_session_with_cut(
            store.clone(),
            "src",
            Some(ForkCut::BeforeTurn("turn-2".into())),
            StartSessionOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(message_ids(&before_turn_2.history()), vec!["u1", "a1"]);
    before_turn_2.dispose().await;

    // AfterMessage("ia1") cuts deterministically even when inherited rows have no turn_context.
    let after_imported = agent
        .fork_session_with_cut(
            store.clone(),
            "imported",
            Some(ForkCut::AfterMessage("ia1".into())),
            StartSessionOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(message_ids(&after_imported.history()), vec!["iu1", "ia1"]);
    after_imported.dispose().await;

    // Missing cut points fail loudly instead of copying the full transcript.
    assert!(agent
        .fork_session_with_cut(
            store.clone(),
            "src",
            Some(ForkCut::LastTurn("missing-turn".into())),
            StartSessionOptions::default(),
        )
        .await
        .is_err());
    assert!(agent
        .fork_session_with_cut(
            store,
            "imported",
            Some(ForkCut::AfterMessage("missing-msg".into())),
            StartSessionOptions::default(),
        )
        .await
        .is_err());

    std::fs::remove_dir_all(&root).ok();
}
