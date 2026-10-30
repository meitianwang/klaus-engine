//! Work item: saved-thread-load-recovery.
//! Metadata rewrites shifted byte indexes, making saved pages unreadable.
//! Keep metadata authoritative in SQLite and recover keyed display projections
//! from the durable append boundary without poisoning the writer.
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeJson};
use klaus_agent_core::schemas::message::Message;
use klaus_agent_core::schemas::rollout::{RolloutItem, SessionMeta};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::rollout::parse_rollout_line;
use klaus_agent_core::store::session_store::{ListOptions, RollbackRecord, SessionStore};
use serde_json::{json, Value};

fn open_store(dir: &std::path::Path) -> JsonlSessionStore {
    JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.to_string_lossy().into_owned()),
        archive_dir: None,
    })
}
fn meta(id: &str) -> SessionMeta {
    match parse_rollout_line(&json!({"kind":"session_meta","schemaVersion":2,"meta":{"id":id,"createdAt":1700000000000_i64,"cwd":"/tmp","model":"m","title":"Original"}}).to_string()).unwrap() {
        RolloutItem::SessionMeta {meta,..}=>meta,_=>unreachable!(),
    }
}
fn user(id: &str, text: &str) -> Message {
    match parse_rollout_line(&json!({"kind":"message","message":{"type":"user","uuid":id,"content":[{"type":"text","text":text}]}}).to_string()).unwrap() {
        RolloutItem::Message {message}=>message,_=>unreachable!(),
    }
}
fn context(id: &str) -> klaus_agent_core::schemas::rollout::TurnContextSnapshot {
    match parse_rollout_line(
        &json!({"kind":"turn_context","ctx":{"cwd":"/tmp","model":"m","turnId":id}}).to_string(),
    )
    .unwrap()
    {
        RolloutItem::TurnContext { ctx } => ctx,
        _ => unreachable!(),
    }
}
fn value(history: &klaus_agent_core::store::thread_history::ThreadHistory) -> Value {
    serde_json::from_slice(&runtime_codec::encode_bytes(history).unwrap()).unwrap()
}

#[tokio::test]
async fn title_and_host_updates_preserve_source_bytes_cursors_and_cold_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_store(dir.path());
    let live = store.create(meta("history")).await.unwrap();
    live.append_turn_context(context("one")).await;
    live.append_message(user("one-user", "first")).await;
    live.append_turn_context(context("two")).await;
    live.append_message(user("two-user", "second")).await;
    live.flush().await.unwrap();
    let path = dir.path().join("1700000000000-history.jsonl");
    let before = std::fs::read(&path).unwrap();
    let first = store
        .paged_thread_history("history", None, Some(1), Some("summary"), None)
        .await
        .unwrap()
        .unwrap();
    let cursor = first.page.next_cursor.clone().unwrap();
    let long_title = "A substantially longer saved title that must not shift any transcript byte";
    live.update_title(long_title).await.unwrap();
    live.update_host_metadata(Some(RuntimeJson::from("host attachment")))
        .await
        .unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), before);
    live.shutdown().await.unwrap();
    store.update_title("history", "Cold title").await.unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), before);
    let fresh = open_store(dir.path());
    assert_eq!(
        fresh
            .read_thread("history")
            .await
            .unwrap()
            .meta
            .title
            .unwrap(),
        "Cold title"
    );
    assert_eq!(
        fresh.resume("history").await.unwrap().meta.title.unwrap(),
        "Cold title"
    );
    assert_eq!(
        fresh
            .read_session_summary("history")
            .await
            .unwrap()
            .unwrap()
            .title
            .unwrap(),
        "Cold title"
    );
    let listed = fresh.list(ListOptions::default()).await.unwrap();
    assert_eq!(listed.items[0].title.as_ref().unwrap(), "Cold title");
    assert_eq!(
        listed.items[0].host_metadata,
        Some(RuntimeJson::from("host attachment"))
    );
    let older = fresh
        .paged_thread_history("history", Some(&cursor), Some(1), Some("full"), None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value(&older.history)["history"][0]["uuid"], "one-user");
    assert_eq!(first.shells[0].ordinal, 3);
    assert_eq!(older.shells[0].ordinal, 1);
}

#[tokio::test]
async fn projection_failure_does_not_poison_append_and_flush_catches_up() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_store(dir.path());
    let live = store.create(meta("history")).await.unwrap();
    live.append_turn_context(context("one")).await;
    live.append_message(user("old", "committed")).await;
    live.flush().await.unwrap();
    let conn = rusqlite::Connection::open(dir.path().join("threads-index.sqlite3")).unwrap();
    let before: i64 = conn
        .query_row(
            "SELECT next_rollout_byte_offset FROM history_projection_state WHERE thread='history'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    conn.execute_batch("CREATE TRIGGER fail_projection BEFORE INSERT ON history_item_chunks BEGIN SELECT RAISE(ABORT,'injected projection failure'); END;").unwrap();
    live.append_message(user("new", "durable but not yet projected"))
        .await;
    live.flush().await.unwrap();
    assert!(
        std::fs::read_to_string(dir.path().join("1700000000000-history.jsonl"))
            .unwrap()
            .contains("durable but not yet projected")
    );
    let unchanged: i64 = conn
        .query_row(
            "SELECT next_rollout_byte_offset FROM history_projection_state WHERE thread='history'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(unchanged, before);
    let page = store
        .paged_turn_slice("history", "one", None, Some(100), None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(page.history.history.len(), 1);
    conn.execute_batch("DROP TRIGGER fail_projection").unwrap();
    live.flush().await.unwrap();
    let page = store
        .paged_turn_slice("history", "one", None, Some(100), None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value(&page.history)["history"][1]["uuid"], "new");
    let next: i64 = conn
        .query_row(
            "SELECT next_rollout_byte_offset FROM history_projection_state WHERE thread='history'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        next,
        std::fs::metadata(dir.path().join("1700000000000-history.jsonl"))
            .unwrap()
            .len() as i64
    );
    live.shutdown().await.unwrap();
}

#[tokio::test]
async fn rollback_keeps_the_source_prefix_and_stable_surviving_turn_identity() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_store(dir.path());
    let live = store.create(meta("history")).await.unwrap();
    live.append_turn_context(context("one")).await;
    live.append_message(user("one-user", "kept")).await;
    live.append_turn_context(context("two")).await;
    live.append_message(user("two-user", "removed")).await;
    live.flush().await.unwrap();
    let path = dir.path().join("1700000000000-history.jsonl");
    let before = std::fs::read(&path).unwrap();
    live.append_rollback(RollbackRecord { num_turns: 1 }).await;
    live.append_turn_context(context("three")).await;
    live.append_message(user("three-user", "replacement")).await;
    live.flush().await.unwrap();
    assert!(std::fs::read(&path).unwrap().starts_with(&before));
    let page = store
        .paged_thread_history("history", None, Some(100), Some("full"), Some("asc"))
        .await
        .unwrap()
        .unwrap();
    let ids = value(&page.history)["history"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["uuid"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(ids, ["one-user", "three-user"]);
    assert_eq!(
        page.shells
            .iter()
            .find(|shell| shell.id == "one")
            .unwrap()
            .ordinal,
        1
    );
    assert!(
        page.shells
            .iter()
            .find(|shell| shell.id == "three")
            .unwrap()
            .ordinal
            > 3
    );
    live.shutdown().await.unwrap();
}

#[tokio::test]
async fn metadata_and_history_survive_archive_rename_and_identity_reuse() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_store(dir.path());
    let live = store.create(meta("history")).await.unwrap();
    live.append_turn_context(context("one")).await;
    live.append_message(user("old", "kept across moves")).await;
    live.update_title("Saved authority title").await.unwrap();
    live.shutdown().await.unwrap();
    store.archive("history").await.unwrap();
    let archived = store
        .read_session_summary("history")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(archived.archived, Some(true));
    assert_eq!(archived.title.unwrap(), "Saved authority title");
    store.unarchive("history").await;
    store.rename("history", "renamed").await.unwrap();
    let fresh = open_store(dir.path());
    assert!(fresh
        .read_session_summary("history")
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        fresh
            .read_thread("renamed")
            .await
            .unwrap()
            .meta
            .title
            .unwrap(),
        "Saved authority title"
    );
    let page = fresh
        .paged_thread_history("renamed", None, Some(100), Some("full"), None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value(&page.history)["history"][0]["uuid"], "old");
    fresh.delete("renamed").await.unwrap();
    let live = fresh.create(meta("renamed")).await.unwrap();
    live.append_turn_context(context("new-turn")).await;
    live.append_message(user("new", "new identity")).await;
    live.flush().await.unwrap();
    let page = fresh
        .paged_thread_history("renamed", None, Some(100), Some("full"), None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value(&page.history)["history"][0]["uuid"], "new");
    assert_eq!(
        fresh
            .read_thread("renamed")
            .await
            .unwrap()
            .meta
            .title
            .unwrap(),
        "Original"
    );
    live.shutdown().await.unwrap();
}

#[tokio::test]
async fn metadata_failure_ack_does_not_poison_later_durable_appends() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_store(dir.path());
    let live = store.create(meta("history")).await.unwrap();
    live.append_turn_context(context("one")).await;
    live.append_message(user("old", "committed")).await;
    live.flush().await.unwrap();
    let db = rusqlite::Connection::open(dir.path().join("thread-metadata.sqlite3")).unwrap();
    db.execute_batch("CREATE TRIGGER fail_metadata BEFORE UPDATE ON thread_metadata_v1 BEGIN SELECT RAISE(ABORT,'injected metadata failure'); END;").unwrap();
    assert!(live
        .update_title("Rejected")
        .await
        .unwrap_err()
        .to_string()
        .contains("injected metadata failure"));
    assert!(live
        .update_host_metadata(Some(RuntimeJson::from("Rejected")))
        .await
        .is_err());
    live.append_message(user("new", "still durable")).await;
    live.flush().await.unwrap();
    let page = store
        .paged_turn_slice("history", "one", None, Some(100), None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value(&page.history)["history"][1]["uuid"], "new");
    assert_eq!(
        store
            .read_thread("history")
            .await
            .unwrap()
            .meta
            .title
            .unwrap(),
        "Original"
    );
    live.shutdown().await.unwrap();
}
