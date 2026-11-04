//! Regression coverage for materialized history and bounded message windows.
use super::*;
use serde_json::{json, Value};
use std::io::Write;

struct TempDir(std::path::PathBuf);
impl TempDir {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("klaus-message-index-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn path(&self) -> &Path {
        &self.0
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn fixture() -> Vec<String> {
    let source: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/complete_thread_history.json"
    ))
    .unwrap();
    source["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(Value::to_string)
        .collect()
}
fn save(dir: &Path, rows: &[String]) -> (String, u64) {
    for row in rows {
        assert!(
            parse_rollout_line(row).is_some(),
            "invalid source-owned fixture: {row}"
        );
    }
    let path = dir.join("rollout.jsonl");
    std::fs::write(&path, rows.join("\n") + "\n").unwrap();
    (
        path.to_string_lossy().into_owned(),
        std::fs::metadata(path).unwrap().len(),
    )
}
fn encoded(history: &ThreadHistory) -> crate::runtime_values::RuntimeJson {
    crate::runtime_values::runtime_codec::encode_value(history).unwrap()
}
fn context(id: &str) -> String {
    json!({"kind":"turn_context","ctx":{"cwd":"/tmp","model":"m","permissionMode":"default","turnId":id}}).to_string()
}
fn message(id: &str, text: &str) -> String {
    json!({"kind":"message","message":{"type":"user","uuid":id,"content":[{"type":"text","text":text}]}}).to_string()
}

#[test]
fn indexed_projection_preserves_outputs_compactions_and_metadata() {
    let dir = TempDir::new();
    let rows = fixture();
    let (path, len) = save(dir.path(), &rows);
    let mut db = TurnIndexDb::open(dir.path().join("index.sqlite3").to_str().unwrap()).unwrap();
    let page = db
        .window("t", &path, len, 1, None, 100, true, "full")
        .unwrap();
    let full = project_thread_history(
        rows.iter()
            .filter_map(|row| parse_rollout_line(row))
            .collect(),
    );
    assert_eq!(encoded(&page.history), encoded(&full));
    // Incremental batches and rewinds must produce the same metadata as a
    // complete replay after every accepted source record.
    let incremental_dir = TempDir::new();
    let (incremental_path, _) = save(incremental_dir.path(), &rows[..1]);
    let mut incremental = TurnIndexDb::open(
        incremental_dir
            .path()
            .join("index.sqlite3")
            .to_str()
            .unwrap(),
    )
    .unwrap();
    incremental
        .materialize_rollout("t", &incremental_path)
        .unwrap();
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&incremental_path)
        .unwrap();
    for (index, row) in rows.iter().enumerate().skip(1) {
        writeln!(file, "{row}").unwrap();
        file.flush().unwrap();
        incremental
            .materialize_rollout("t", &incremental_path)
            .unwrap();
        let snapshot = incremental
            .window("t", &incremental_path, 0, 0, None, 100, true, "full")
            .unwrap();
        let replay = project_thread_history(
            rows[..=index]
                .iter()
                .filter_map(|row| parse_rollout_line(row))
                .collect(),
        );
        assert_eq!(
            encoded(&snapshot.history),
            encoded(&replay),
            "incremental metadata at source record {index}"
        );
    }
    for shell in &page.shells {
        let mut cursor = None;
        let mut assembled = Vec::new();
        loop {
            let page = db
                .turn_slice(
                    "t",
                    &path,
                    len,
                    1,
                    &shell.id,
                    MessageWindow {
                        cursor: cursor.as_deref(),
                        limit: 1,
                        desc: false,
                    },
                )
                .unwrap()
                .unwrap();
            assembled.extend(page.history.history);
            cursor = page.page.next_cursor;
            if !page.page.has_more {
                break;
            }
        }
        let expected = full
            .history
            .iter()
            .filter(|message| {
                let value = crate::runtime_values::runtime_codec::encode_value(*message).unwrap();
                value
                    .get("uuid")
                    .and_then(|value| value.as_string())
                    .and_then(|uuid| full.turn_index.message_turn.get(uuid))
                    .is_some_and(|id| id == shell.id.as_str())
            })
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(assembled, expected, "message windows for {}", shell.id);
    }
}

#[test]
fn growing_mtime_extends_index_without_reading_giant_old_message() {
    let dir = TempDir::new();
    let path = dir.path().join("rollout.jsonl");
    let mut file = std::fs::File::create(&path).unwrap();
    writeln!(file, "{}", context("turn")).unwrap();
    // The UUID follows the body: extraction must skip it without buffering it.
    file.write_all(b"{\"kind\":\"message\",\"message\":{\"type\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"").unwrap();
    let chunk = vec![b'x'; 64 * 1024];
    for _ in 0..320 {
        file.write_all(&chunk).unwrap();
    }
    file.write_all(b"\"}],\"uuid\":\"giant\"}}\n").unwrap();
    file.flush().unwrap();
    let path = path.to_str().unwrap();
    let len = std::fs::metadata(path).unwrap().len();
    let db_path = dir.path().join("index.sqlite3");
    let mut db = TurnIndexDb::open(db_path.to_str().unwrap()).unwrap();
    db.window("t", path, len, 10, None, 1, true, "notLoaded")
        .unwrap();
    let payload_bytes: i64 = db
        .conn
        .query_row(
            "SELECT SUM(length(payload)) FROM history_item_chunks WHERE item_id='message:giant'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        payload_bytes >= 20 * 1024 * 1024,
        "the complete unrequested item must be materialized without decoding it"
    );
    db.conn.execute_batch("CREATE TRIGGER forbid_rescan BEFORE INSERT ON page_records_v2 WHEN NEW.offset=0 BEGIN SELECT RAISE(ABORT,'old prefix rescanned'); END;").unwrap();
    let mut file = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    writeln!(file, "{}", message("fresh", "new content")).unwrap();
    let len2 = std::fs::metadata(path).unwrap().len();
    db.materialize_rollout("t", path).unwrap();
    let page = db
        .turn_slice(
            "t",
            path,
            len2,
            20,
            "turn",
            MessageWindow {
                cursor: None,
                limit: 1,
                desc: true,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(page.history.history.len(), 1);
    assert_eq!(page.page.next_cursor.as_deref(), Some("fresh"));
    assert_eq!(
        encoded(&page.history)
            .get("history")
            .unwrap()
            .as_array()
            .unwrap()[0]
            .get("uuid")
            .unwrap()
            .as_string()
            .unwrap(),
        "fresh"
    );
    let error = db
        .turn_slice(
            "t",
            path,
            len2,
            20,
            "turn",
            MessageWindow {
                cursor: Some("fresh"),
                limit: 1,
                desc: true,
            },
        )
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("decoded message length too large"));
}

#[test]
fn saved_declaration_pairs_a_legacy_capture_before_the_page_guard() {
    let dir = TempDir::new();
    let image = dir.path().join("capture.png");
    std::fs::write(&image, b"image bytes").unwrap();
    let path = dir.path().join("rollout.jsonl");
    let mut file = std::fs::File::create(&path).unwrap();
    writeln!(file, "{}", context("turn")).unwrap();
    writeln!(
        file,
        "{}",
        json!({
            "kind":"message",
            "message":{
                "type":"tool_result",
                "uuid":"capture",
                "toolUseId":"call",
                "isError":false,
                "content":[
                    {"type":"text","text":format!("Saved: {}", json!(image.to_string_lossy()))},
                    {"type":"image","mimeType":"image/png","base64":"a".repeat(17 * 1024 * 1024)}
                ]
            }
        })
    )
    .unwrap();
    file.flush().unwrap();
    let path = path.to_string_lossy().into_owned();
    let len = std::fs::metadata(&path).unwrap().len();
    let mut db = TurnIndexDb::open(dir.path().join("index.sqlite3").to_str().unwrap()).unwrap();
    let page = db
        .turn_slice(
            "t",
            &path,
            len,
            1,
            "turn",
            MessageWindow {
                cursor: None,
                limit: 1,
                desc: true,
            },
        )
        .unwrap()
        .unwrap();
    let value = encoded(&page.history);
    let image = &value["history"][0]["content"][1];
    assert_eq!(
        image["sourcePath"].as_string().unwrap(),
        dir.path().join("capture.png").to_string_lossy().as_ref()
    );
    assert_eq!(image["base64"].as_string().unwrap(), "");
}

#[test]
fn explicit_projection_rebuild_handles_rewrites_and_shrinks() {
    let dir = TempDir::new();
    let (path, len) = save(dir.path(), &[context("one"), message("old", "body")]);
    let mut db = TurnIndexDb::open(dir.path().join("index.sqlite3").to_str().unwrap()).unwrap();
    db.window("t", &path, len, 1, None, 10, true, "notLoaded")
        .unwrap();
    let (_, len2) = save(dir.path(), &[context("two"), message("new", "body")]);
    assert_eq!(len, len2);
    db.delete_thread("t").unwrap();
    db.materialize_rollout("t", &path).unwrap();
    let page = db
        .window("t", &path, len2, 2, None, 10, true, "full")
        .unwrap();
    assert_eq!(page.shells[0].id, "two");
    assert_eq!(page.history.history.len(), 1);
    std::fs::write(&path, context("end") + "\n").unwrap();
    let len3 = std::fs::metadata(&path).unwrap().len();
    db.delete_thread("t").unwrap();
    db.materialize_rollout("t", &path).unwrap();
    let page = db
        .window("t", &path, len3, 3, None, 10, true, "notLoaded")
        .unwrap();
    assert_eq!(page.shells[0].id, "end");
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    writeln!(file, "\n{}", message("tail", "after newline")).unwrap();
    let len4 = std::fs::metadata(&path).unwrap().len();
    db.materialize_rollout("t", &path).unwrap();
    let page = db
        .turn_slice(
            "t",
            &path,
            len4,
            4,
            "end",
            MessageWindow {
                cursor: None,
                limit: 10,
                desc: true,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(page.history.history.len(), 1);
}

#[test]
fn rollback_removes_only_rewound_records_and_keeps_compaction_cursor_stable() {
    let dir = TempDir::new();
    let rows = vec![
        context("one"),
        message("one-user", "one"),
        json!({"kind":"compacted","summary":"kept","replacedRange":[0,1],"replacementHistory":[]})
            .to_string(),
        context("two"),
        message("two-user", "discard"),
        json!({"kind":"rollback","numTurns":1}).to_string(),
        context("three"),
        message("three-user", "three"),
    ];
    let (path, len) = save(dir.path(), &rows);
    let mut db = TurnIndexDb::open(dir.path().join("index.sqlite3").to_str().unwrap()).unwrap();
    let page = db
        .window("t", &path, len, 1, None, 100, true, "full")
        .unwrap();
    let expected = project_thread_history(
        rows.iter()
            .filter_map(|row| parse_rollout_line(row))
            .collect(),
    );
    assert_eq!(encoded(&page.history), encoded(&expected));
    assert_eq!(
        page.shells
            .iter()
            .map(|shell| shell.id.as_str())
            .collect::<Vec<_>>(),
        vec!["one", "three"]
    );
    let part = db
        .turn_slice(
            "t",
            &path,
            len,
            1,
            "one",
            MessageWindow {
                cursor: None,
                limit: 1,
                desc: true,
            },
        )
        .unwrap()
        .unwrap();
    assert!(
        matches!(&part.history.history[0],Message::CompactBoundary(message) if message.uuid=="history-compaction-2")
    );
}

#[test]
fn invalid_cursor_and_missing_projected_payload_return_errors() {
    let dir = TempDir::new();
    let (path, len) = save(dir.path(), &[context("one"), message("row", "body")]);
    let mut db = TurnIndexDb::open(dir.path().join("index.sqlite3").to_str().unwrap()).unwrap();
    db.window("t", &path, len, 1, None, 10, true, "notLoaded")
        .unwrap();
    assert_eq!(
        db.turn_slice(
            "t",
            &path,
            len,
            1,
            "one",
            MessageWindow {
                cursor: Some("missing"),
                limit: 1,
                desc: true
            }
        )
        .unwrap_err()
        .kind(),
        io::ErrorKind::InvalidInput
    );
    db.conn
        .execute(
            "DELETE FROM history_item_chunks WHERE item_id='message:row'",
            [],
        )
        .unwrap();
    assert!(db
        .window("t", &path, len, 1, None, 10, true, "full")
        .unwrap_err()
        .to_string()
        .contains("projection is incomplete"));
    db.conn.execute_batch("DROP TABLE history_items").unwrap();
    assert!(db
        .turn_slice(
            "t",
            &path,
            len,
            1,
            "one",
            MessageWindow {
                cursor: None,
                limit: 1,
                desc: true
            }
        )
        .is_err());
}

#[test]
fn concurrent_cold_pages_share_one_atomic_scan_and_ignore_legacy_watermarks() {
    let dir = TempDir::new();
    let (path, len) = save(dir.path(), &fixture());
    let db_path = dir
        .path()
        .join("index.sqlite3")
        .to_string_lossy()
        .into_owned();
    let db = TurnIndexDb::open(&db_path).unwrap();
    db.conn
        .execute_batch(
            "CREATE TABLE threads(id TEXT PRIMARY KEY,path TEXT,indexed_len INTEGER,stamp INTEGER);
         INSERT INTO threads VALUES('t','legacy',999999,1);
         CREATE TABLE scan_count(n INTEGER);
         INSERT INTO scan_count VALUES(0);
         CREATE TRIGGER count_scans AFTER INSERT ON page_records_v2 WHEN NEW.offset=0
         BEGIN UPDATE scan_count SET n=n+1; END;",
        )
        .unwrap();
    drop(db);
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let handles = [1, 100].map(|limit| {
        let path = path.clone();
        let db_path = db_path.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            barrier.wait();
            let mut db = TurnIndexDb::open(&db_path).unwrap();
            let page = db
                .window("t", &path, len, 10, None, limit, true, "notLoaded")
                .unwrap();
            assert!(!page.shells.is_empty());
        })
    });
    for handle in handles {
        handle.join().unwrap();
    }
    let db = TurnIndexDb::open(&db_path).unwrap();
    let scans: i64 = db
        .conn
        .query_row("SELECT n FROM scan_count", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        scans, 1,
        "concurrent first pages must publish one complete watermark"
    );
}

#[test]
fn repeated_tool_ids_keep_outputs_that_arrive_after_another_context() {
    let dir = TempDir::new();
    let result = |id: &str| {
        json!({"kind":"message","message":{"type":"tool_result","uuid":id,"toolUseId":"shared","content":[{"type":"text","text":"result"}],"isError":false}}).to_string()
    };
    let output = |value: i32| {
        json!({"kind":"event","event":"tool_result_output","turnId":"one","payload":{"toolUseId":"shared","output":{"value":value}}}).to_string()
    };
    let rows=vec![context("one"),message("user","question"),result("first"),output(1),result("second"),context("two"),output(2),
        json!({"kind":"event","event":"turn_lifecycle","turnId":"one","payload":{"status":"completed"}}).to_string(),message("two-user","next")];
    let (path, len) = save(dir.path(), &rows);
    let mut db = TurnIndexDb::open(dir.path().join("index.sqlite3").to_str().unwrap()).unwrap();
    let page = db
        .turn_slice(
            "t",
            &path,
            len,
            1,
            "one",
            MessageWindow {
                cursor: None,
                limit: 100,
                desc: true,
            },
        )
        .unwrap()
        .unwrap();
    let value = encoded(&page.history);
    let history = value.get("history").unwrap().as_array().unwrap();
    assert_eq!(history.len(), 3);
    assert_eq!(
        history[1].pointer("/output/value").unwrap().as_i64(),
        Some(1)
    );
    assert_eq!(
        history[2].pointer("/output/value").unwrap().as_i64(),
        Some(2)
    );
    assert_eq!(
        page.history
            .turn_index
            .turn_status
            .get(&RuntimeString::from("one"))
            .unwrap(),
        "completed"
    );
}

#[test]
fn superseded_large_tool_output_is_not_materialized_for_a_requested_result() {
    let dir = TempDir::new();
    let path = dir.path().join("rollout.jsonl");
    let mut file = std::fs::File::create(&path).unwrap();
    writeln!(file, "{}", context("one")).unwrap();
    writeln!(file,"{}",json!({"kind":"message","message":{"type":"tool_result","uuid":"result","toolUseId":"shared","content":[{"type":"text","text":"result"}],"isError":false}})).unwrap();
    file.write_all(b"{\"kind\":\"event\",\"event\":\"tool_result_output\",\"turnId\":\"one\",\"payload\":{\"toolUseId\":\"shared\",\"output\":{\"stdout\":\"").unwrap();
    let chunk = vec![b'x'; 64 * 1024];
    for _ in 0..1600 {
        file.write_all(&chunk).unwrap();
    }
    file.write_all(b"\"}}}\n").unwrap();
    writeln!(file,"{}",json!({"kind":"event","event":"tool_result_output","turnId":"one","payload":{"toolUseId":"shared","output":{"kept":"complete"}}})).unwrap();
    file.flush().unwrap();
    let len = std::fs::metadata(&path).unwrap().len();
    let mut db = TurnIndexDb::open(dir.path().join("index.sqlite3").to_str().unwrap()).unwrap();
    let page = db
        .turn_slice(
            "t",
            path.to_str().unwrap(),
            len,
            1,
            "one",
            MessageWindow {
                cursor: None,
                limit: 1,
                desc: true,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        encoded(&page.history)
            .pointer("/history/0/output/kept")
            .unwrap()
            .as_string()
            .unwrap(),
        "complete"
    );
}

#[test]
fn decoded_guard_accepts_large_escaped_json_that_decodes_to_a_small_page() {
    let dir = TempDir::new();
    let path = dir.path().join("rollout.jsonl");
    let mut file = std::fs::File::create(&path).unwrap();
    writeln!(file, "{}", context("one")).unwrap();
    file.write_all(b"{\"kind\":\"message\",\"message\":{\"type\":\"user\",\"uuid\":\"escaped\",\"content\":[{\"type\":\"text\",\"text\":\"").unwrap();
    let chunk = br"\u0061".repeat(8192);
    for _ in 0..342 {
        file.write_all(&chunk).unwrap();
    }
    file.write_all(b"\"}]}}\n").unwrap();
    file.flush().unwrap();
    let len = std::fs::metadata(&path).unwrap().len();
    assert!(len > MAX_ITEMS_PAGE_BYTES);
    let mut db = TurnIndexDb::open(dir.path().join("index.sqlite3").to_str().unwrap()).unwrap();
    let page = db
        .turn_slice(
            "t",
            path.to_str().unwrap(),
            len,
            1,
            "one",
            MessageWindow {
                cursor: None,
                limit: 1,
                desc: true,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        encoded(&page.history)
            .pointer("/history/0/content/0/text")
            .unwrap()
            .as_string()
            .unwrap()
            .as_utf16()
            .len(),
        342 * 8192
    );
}

#[test]
fn page_reads_use_committed_payloads_without_opening_the_rollout() {
    let dir = TempDir::new();
    let (path, len) = save(dir.path(), &[context("one"), message("row", "committed")]);
    let mut db = TurnIndexDb::open(dir.path().join("index.sqlite3").to_str().unwrap()).unwrap();
    let before = db
        .window("t", &path, len, 1, None, 100, true, "full")
        .unwrap();
    std::fs::remove_file(&path).unwrap();
    let after = db
        .window("t", &path, len, 999, None, 100, true, "full")
        .unwrap();
    assert_eq!(encoded(&before.history), encoded(&after.history));
}

#[test]
fn failed_later_batch_keeps_prior_checkpoint_and_cancels_the_producer() {
    let dir = TempDir::new();
    let mut rows = vec![context("one")];
    rows.extend((0..1100).map(|index| message(&format!("row-{index}"), "complete")));
    let (path, _) = save(dir.path(), &rows);
    let db_path = dir.path().join("index.sqlite3");
    let mut db = TurnIndexDb::open(db_path.to_str().unwrap()).unwrap();
    let boundary = rows[..256].iter().map(|row| row.len() + 1).sum::<usize>();
    db.conn.execute_batch(&format!("CREATE TRIGGER fail_later_batch BEFORE INSERT ON page_records_v2 WHEN NEW.offset>={boundary} BEGIN SELECT RAISE(ABORT,'injected batch failure'); END;")).unwrap();
    let error = db.materialize_rollout("t", &path).unwrap_err();
    assert!(error.to_string().contains("injected batch failure"));
    let checkpoint:(i64,i64)=db.conn.query_row("SELECT next_rollout_byte_offset,next_rollout_ordinal FROM history_projection_state WHERE thread='t'",[],|row|Ok((row.get(0)?,row.get(1)?))).unwrap();
    assert_eq!(checkpoint, (boundary as i64, 256));
    let count: i64 = db
        .conn
        .query_row(
            "SELECT COUNT(DISTINCT item_id) FROM history_item_chunks WHERE thread='t'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 255);
    db.conn
        .execute_batch("DROP TRIGGER fail_later_batch")
        .unwrap();
    db.materialize_rollout("t", &path).unwrap();
    let page = db
        .window(
            "t",
            &path,
            std::fs::metadata(&path).unwrap().len(),
            0,
            None,
            100,
            true,
            "full",
        )
        .unwrap();
    assert_eq!(page.history.history.len(), 1100);
    let count: i64 = db
        .conn
        .query_row(
            "SELECT COUNT(DISTINCT item_id) FROM history_item_chunks WHERE thread='t'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1100, "recovery must be idempotent");
}

#[test]
fn incomplete_tail_does_not_advance_the_projection_checkpoint() {
    let dir = TempDir::new();
    let (path, len) = save(dir.path(), &[context("one"), message("old", "committed")]);
    let mut db = TurnIndexDb::open(dir.path().join("index.sqlite3").to_str().unwrap()).unwrap();
    db.materialize_rollout("t", &path).unwrap();
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    write!(
        file,
        "{}",
        message("new", "complete JSON without its newline")
    )
    .unwrap();
    file.flush().unwrap();
    db.materialize_rollout("t", &path).unwrap();
    let checkpoint: i64 = db
        .conn
        .query_row(
            "SELECT next_rollout_byte_offset FROM history_projection_state WHERE thread='t'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(checkpoint, len as i64);
    writeln!(file).unwrap();
    file.flush().unwrap();
    db.materialize_rollout("t", &path).unwrap();
    let page = db
        .window(
            "t",
            &path,
            std::fs::metadata(&path).unwrap().len(),
            0,
            None,
            100,
            true,
            "full",
        )
        .unwrap();
    assert_eq!(page.history.history.len(), 2);
}

#[test]
fn failed_legacy_import_resumes_its_journal_after_reopening() {
    let dir = TempDir::new();
    let mut rows = vec![context("one")];
    rows.extend((0..700).map(|index| message(&format!("row-{index}"), "complete")));
    let (path, len) = save(dir.path(), &rows);
    let db_path = dir.path().join("index.sqlite3");
    let mut db = TurnIndexDb::open(db_path.to_str().unwrap()).unwrap();
    let boundary = rows[..256].iter().map(|row| row.len() + 1).sum::<usize>();
    db.conn.execute_batch(&format!("CREATE TRIGGER fail_import BEFORE INSERT ON page_records_v2 WHEN NEW.offset>={boundary} BEGIN SELECT RAISE(ABORT,'injected import failure'); END;")).unwrap();
    assert!(db
        .window("t", &path, len, 1, None, 100, true, "full")
        .unwrap_err()
        .to_string()
        .contains("injected import failure"));
    assert_eq!(
        db.conn
            .query_row(
                "SELECT COUNT(*) FROM history_legacy_imports WHERE thread='t'",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
    drop(db);
    let mut db = TurnIndexDb::open(db_path.to_str().unwrap()).unwrap();
    db.conn.execute_batch("DROP TRIGGER fail_import").unwrap();
    let page = db
        .window("t", &path, len, 1, None, 100, true, "full")
        .unwrap();
    assert_eq!(page.history.history.len(), 700);
    assert_eq!(
        db.conn
            .query_row(
                "SELECT COUNT(*) FROM history_legacy_imports WHERE thread='t'",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
}

#[test]
fn published_projection_rejects_source_shrink_without_discarding_saved_items() {
    let dir = TempDir::new();
    let (path, len) = save(dir.path(), &[context("one"), message("old", "committed")]);
    let mut db = TurnIndexDb::open(dir.path().join("index.sqlite3").to_str().unwrap()).unwrap();
    let before = db
        .window("t", &path, len, 1, None, 100, true, "full")
        .unwrap();
    std::fs::write(&path, context("short") + "\n").unwrap();
    assert!(db
        .materialize_rollout("t", &path)
        .unwrap_err()
        .to_string()
        .contains("shrank"));
    let after = db
        .window("t", &path, 0, 999, None, 100, true, "full")
        .unwrap();
    assert_eq!(encoded(&before.history), encoded(&after.history));
}

#[test]
fn initial_projection_stops_at_the_captured_source_end() {
    let dir = TempDir::new();
    let (path, len) = save(dir.path(), &[context("one"), message("old", "captured")]);
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    writeln!(file, "{}", message("new", "after snapshot")).unwrap();
    file.flush().unwrap();
    let mut db = TurnIndexDb::open(dir.path().join("index.sqlite3").to_str().unwrap()).unwrap();
    let before = db
        .window("t", &path, len, 1, None, 100, true, "full")
        .unwrap();
    assert_eq!(before.history.history.len(), 1);
    db.materialize_rollout("t", &path).unwrap();
    let after = db
        .window("t", &path, len, 1, None, 100, true, "full")
        .unwrap();
    assert_eq!(after.history.history.len(), 2);
}

#[test]
fn summary_uses_the_explicit_final_reply_before_later_assistant_commentary() {
    let dir = TempDir::new();
    let assistant = |id: &str| {
        json!({"kind":"message","message":{"type":"assistant","uuid":id,"model":"m","content":[{"type":"text","text":id}]}}).to_string()
    };
    let rows=vec![context("one"),message("user","question"),assistant("final"),
        json!({"kind":"event","event":"turn_lifecycle","turnId":"one","payload":{"status":"completed","finalMessageId":"final"}}).to_string(),
        assistant("commentary")];
    let (path, len) = save(dir.path(), &rows);
    let mut db = TurnIndexDb::open(dir.path().join("index.sqlite3").to_str().unwrap()).unwrap();
    let page = db
        .window("t", &path, len, 1, None, 100, true, "summary")
        .unwrap();
    let ids = encoded(&page.history)
        .get("history")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            row.get("uuid")
                .unwrap()
                .as_string()
                .unwrap()
                .try_to_utf8()
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, ["user", "final"]);
    assert_eq!(page.shells[0].final_agent_item_id.as_deref(), Some("final"));
}

#[test]
fn published_pages_do_not_wait_for_the_projection_writer_lease_or_transaction() {
    let dir = TempDir::new();
    let (path, len) = save(dir.path(), &[context("one"), message("old", "committed")]);
    let db_path = dir
        .path()
        .join("index.sqlite3")
        .to_string_lossy()
        .into_owned();
    let mut writer = TurnIndexDb::open(&db_path).unwrap();
    writer.materialize_rollout("t", &path).unwrap();
    let lease = writer.projection_lease().unwrap();
    writer.conn.execute_batch("BEGIN IMMEDIATE; UPDATE history_turn_projection SET byte_len=byte_len+1 WHERE thread='t';").unwrap();
    let (send, receive) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        let mut db = TurnIndexDb::open(&db_path).unwrap();
        let result = db.window("t", &path, len, 1, None, 100, true, "full");
        send.send(result).unwrap();
    });
    let page = receive
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("a WAL reader must not wait for projection admission or an uncommitted batch")
        .unwrap();
    assert_eq!(page.history.history.len(), 1);
    writer.conn.execute_batch("ROLLBACK").unwrap();
    drop(lease);
    reader.join().unwrap();
}

#[test]
fn append_work_is_bounded_by_the_new_batch_and_keeps_requested_message_ownership() {
    use std::sync::atomic::{AtomicU64, Ordering};

    unsafe extern "C" fn count_work(
        event: u32,
        context: *mut std::ffi::c_void,
        statement: *mut std::ffi::c_void,
        _elapsed: *mut std::ffi::c_void,
    ) -> i32 {
        if event == rusqlite::ffi::SQLITE_TRACE_PROFILE {
            // SQLite calls this only while the connection and counter below are
            // alive. VM steps measure actual database work without wall-clock
            // thresholds or assumptions about the text of individual queries.
            let counter = unsafe { &*context.cast::<AtomicU64>() };
            let steps = unsafe {
                rusqlite::ffi::sqlite3_stmt_status(
                    statement.cast(),
                    rusqlite::ffi::SQLITE_STMTSTATUS_VM_STEP,
                    1,
                )
            };
            counter.fetch_add(steps.max(0) as u64, Ordering::Relaxed);
        }
        0
    }

    fn measure(history_count: usize) -> u64 {
        let dir = TempDir::new();
        let mut rows = vec![context("one")];
        rows.extend((0..history_count).map(|index| message(&format!("old-{index}"), "saved")));
        let (path, _) = save(dir.path(), &rows);
        let mut db = TurnIndexDb::open(dir.path().join("index.sqlite3").to_str().unwrap()).unwrap();
        db.materialize_rollout("t", &path).unwrap();
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        for index in 0..64 {
            writeln!(file, "{}", message(&format!("new-{index}"), "appended")).unwrap();
        }
        file.flush().unwrap();
        let counter = AtomicU64::new(0);
        let handle = unsafe { db.conn.handle() };
        assert_eq!(
            unsafe {
                rusqlite::ffi::sqlite3_trace_v2(
                    handle,
                    rusqlite::ffi::SQLITE_TRACE_PROFILE,
                    Some(count_work),
                    (&counter as *const AtomicU64).cast_mut().cast(),
                )
            },
            rusqlite::ffi::SQLITE_OK
        );
        let result = db.materialize_rollout("t", &path);
        unsafe {
            rusqlite::ffi::sqlite3_trace_v2(handle, 0, None, std::ptr::null_mut());
        }
        result.unwrap();
        let work = counter.load(Ordering::Relaxed);
        let page = db
            .turn_slice(
                "t",
                &path,
                std::fs::metadata(&path).unwrap().len(),
                0,
                "one",
                MessageWindow {
                    cursor: None,
                    limit: 64,
                    desc: true,
                },
            )
            .unwrap()
            .unwrap();
        assert_eq!(page.history.history.len(), 64);
        assert_eq!(page.history.turn_index.message_turn.len(), 64);
        assert_eq!(
            encoded(&page.history)
                .pointer("/history/63/uuid")
                .unwrap()
                .as_string()
                .unwrap(),
            "new-63"
        );
        let count: i64 = db
            .conn
            .query_row("SELECT COUNT(*) FROM history_items", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count as usize, history_count + 64);
        work
    }

    let short = measure(1024);
    let long = measure(4096);
    eprintln!("append SQLite VM steps: short={short}, long={long}");
    assert!(
        long < short * 3,
        "quadrupling retained history must not cause quadratic append work: {short} -> {long}"
    );
}

#[test]
fn presentation_v4_backfill_is_once_only_and_normal_pages_use_keyed_payloads() {
    let dir = TempDir::new();
    let mut rows = fixture();
    rows.push(json!({"kind":"message","message":{"type":"system","uuid":"unrelated-system","subtype":"info","text":"x".repeat(2*1024*1024)}}).to_string());
    let (path, len) = save(dir.path(), &rows);
    let db_path = dir.path().join("index.sqlite3");
    let mut db = TurnIndexDb::open(db_path.to_str().unwrap()).unwrap();
    db.window("t", &path, len, 1, None, 100, true, "full")
        .unwrap();
    let checkpoint: i64 = db
        .conn
        .query_row(
            "SELECT next_rollout_byte_offset FROM history_projection_state WHERE thread='t'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    db.conn.execute_batch("DROP TABLE history_user_presentations; DROP TABLE history_presentation_backfill; PRAGMA user_version=4;").unwrap();
    drop(db);
    let mut db = TurnIndexDb::open(db_path.to_str().unwrap()).unwrap();
    let page = db
        .window("t", &path, len, 1, None, 100, true, "summary")
        .unwrap();
    assert_eq!(page.presentation_records.len(), 1);
    assert_eq!(
        page.presentation_records[0]
            .get("uuid")
            .unwrap()
            .as_str()
            .unwrap(),
        "display-one"
    );
    let pending: i64 = db
        .conn
        .query_row(
            "SELECT count(*) FROM history_presentation_backfill",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(pending, 0);
    let after: i64 = db
        .conn
        .query_row(
            "SELECT next_rollout_byte_offset FROM history_projection_state WHERE thread='t'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        checkpoint, after,
        "backfill must not rewrite the source checkpoint"
    );
    // A normal page must not decode another system record after migration.
    db.conn.execute("UPDATE history_item_chunks SET payload=x'7b' WHERE thread='t' AND item_id='message:unrelated-system'",[]).unwrap();
    drop(db);
    let mut db = TurnIndexDb::open(db_path.to_str().unwrap()).unwrap();
    assert_eq!(
        db.window("t", &path, len, 1, None, 100, true, "summary")
            .unwrap()
            .presentation_records
            .len(),
        1
    );
    let plan: String = db.conn.query_row(
        "EXPLAIN QUERY PLAN SELECT display_id FROM history_user_presentations WHERE thread=?1 AND target_id=?2 ORDER BY ordinal DESC LIMIT 1",
        params!["t","user-one"],|row|row.get(3),
    ).unwrap();
    assert!(plan.contains("history_presentation_target"), "{plan}");
}

#[test]
fn presentation_retention_removes_rewound_targets_and_display_records() {
    let dir = TempDir::new();
    let display = |id: &str, target: &str| {
        json!({"kind":"message","message":{
            "type":"system","uuid":id,"subtype":"user_display",
            "text":json!({"messageUuid":target,"localRender":{"displayText":target}}).to_string()
        }})
        .to_string()
    };
    let rows = vec![
        context("one"),
        display("d1", "u1"),
        message("u1", "kept"),
        context("two"),
        message("u2", "removed"),
        display("d2", "u2"),
    ];
    let (path, len) = save(dir.path(), &rows);
    let mut db = TurnIndexDb::open(dir.path().join("index.sqlite3").to_str().unwrap()).unwrap();
    assert_eq!(
        db.window("t", &path, len, 1, None, 100, true, "summary")
            .unwrap()
            .presentation_records
            .len(),
        2
    );
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    writeln!(file, "{}", json!({"kind":"rollback","numTurns":1})).unwrap();
    file.flush().unwrap();
    db.materialize_rollout("t", &path).unwrap();
    let len = std::fs::metadata(&path).unwrap().len();
    let page = db
        .window("t", &path, len, 2, None, 100, true, "summary")
        .unwrap();
    assert_eq!(page.presentation_records.len(), 1);
    assert_eq!(
        page.presentation_records[0]
            .get("uuid")
            .unwrap()
            .as_str()
            .unwrap(),
        "d1"
    );
    let stale: i64 = db.conn.query_row(
        "SELECT count(*) FROM history_user_presentations WHERE target_id='u2' OR display_id='d2'",[],|row|row.get(0),
    ).unwrap();
    assert_eq!(stale, 0);
}

#[test]
fn presentation_failed_compatibility_batch_keeps_its_progress_and_original_payload() {
    let dir = TempDir::new();
    let (path, len) = save(dir.path(), &fixture());
    let db_path = dir.path().join("index.sqlite3");
    let mut db = TurnIndexDb::open(db_path.to_str().unwrap()).unwrap();
    db.window("t", &path, len, 1, None, 100, true, "full")
        .unwrap();
    db.conn.execute_batch("DROP TABLE history_user_presentations; DROP TABLE history_presentation_backfill; PRAGMA user_version=4;").unwrap();
    db.conn
        .execute(
            "UPDATE history_item_chunks SET payload=x'7b' WHERE item_id='message:display-one'",
            [],
        )
        .unwrap();
    drop(db);
    let mut db = TurnIndexDb::open(db_path.to_str().unwrap()).unwrap();
    assert!(db
        .window("t", &path, len, 1, None, 100, true, "summary")
        .is_err());
    let progress: i64 = db
        .conn
        .query_row(
            "SELECT next_ordinal FROM history_presentation_backfill WHERE thread='t'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(progress, -1);
    let original: Vec<u8> = db.conn.query_row("SELECT payload FROM history_item_chunks WHERE item_id='message:display-one' AND part=0",[],|row|row.get(0)).unwrap();
    assert_eq!(original, b"{");
}

#[test]
fn undecodable_message_advances_checkpoint_and_keeps_later_history() {
    let dir = TempDir::new();
    let rows = [
        context("t1"),
        message("u1", "before"),
        json!({"kind":"message","message":{"type":"unknown_kind","uuid":"boom","content":[{"type":"text","text":"??"}]}})
            .to_string(),
        json!({"kind":"message","message":{"type":"assistant","uuid":"final-answer","model":"m","content":[{"type":"text","text":"final answer"}]}})
            .to_string(),
    ];
    let path = dir.path().join("rollout.jsonl");
    std::fs::write(&path, rows.join("\n") + "\n").unwrap();
    let len = std::fs::metadata(&path).unwrap().len();
    let mut db = TurnIndexDb::open(dir.path().join("index.sqlite3").to_str().unwrap()).unwrap();
    db.window("t", path.to_str().unwrap(), len, 1, None, 100, true, "full")
        .expect("one undecodable message must not fail the projection");
    let indexed: i64 = db
        .conn
        .query_row(
            "SELECT indexed_len FROM page_threads_v2 WHERE id='t'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        indexed, len as i64,
        "checkpoint must advance past the bad record"
    );
    let has_final: bool = db
        .conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM history_items WHERE thread='t' AND item_id='final-answer')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(has_final, "a later final answer must still be indexed");
}

#[test]
fn read_path_catches_up_a_stale_projection() {
    let dir = TempDir::new();
    let rows = [
        context("t1"),
        message("u1", "before"),
        json!({"kind":"message","message":{"type":"assistant","uuid":"final-answer","model":"m","content":[{"type":"text","text":"final answer"}]}})
            .to_string(),
    ];
    let full_path = dir.path().join("full.jsonl");
    std::fs::write(&full_path, rows.join("\n") + "\n").unwrap();
    let full_len = std::fs::metadata(&full_path).unwrap().len();
    let bytes = std::fs::read(&full_path).unwrap();
    let prefix_len = rows[0].len() + 1 + rows[1].len() + 1;
    let partial_path = dir.path().join("partial.jsonl");
    std::fs::write(&partial_path, &bytes[..prefix_len]).unwrap();

    let mut db = TurnIndexDb::open(dir.path().join("index.sqlite3").to_str().unwrap()).unwrap();
    db.materialize_rollout("t", partial_path.to_str().unwrap())
        .expect("partial projection");
    let before: i64 = db
        .conn
        .query_row(
            "SELECT COUNT(*) FROM history_items WHERE thread='t'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(before > 0, "partial projection should contain the prefix");

    db.window(
        "t",
        full_path.to_str().unwrap(),
        full_len,
        2,
        None,
        100,
        true,
        "full",
    )
    .expect("a read must catch up a stale projection");
    let has_final: bool = db
        .conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM history_items WHERE thread='t' AND item_id='final-answer')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        has_final,
        "the read must surface the unindexed final answer"
    );
}

#[test]
fn projection_failures_persist_the_frozen_checkpoint() {
    let dir = TempDir::new();
    let db_path = dir.path().join("index.sqlite3");
    let db = TurnIndexDb::open(db_path.to_str().unwrap()).unwrap();
    db.conn
        .execute(
            "INSERT INTO history_projection_state(thread,next_rollout_byte_offset,next_rollout_ordinal) VALUES('t',1234,56)",
            [],
        )
        .unwrap();
    db.record_projection_failure("t", "database is locked")
        .unwrap();
    let (offset, ordinal, message): (i64, i64, String) = db
        .conn
        .query_row(
            "SELECT byte_offset,ordinal,message FROM history_projection_failures WHERE thread='t' ORDER BY id DESC LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!((offset, ordinal), (1234, 56));
    assert_eq!(message, "database is locked");

    for _ in 0..25 {
        db.record_projection_failure("t", "again").unwrap();
    }
    let retained: i64 = db
        .conn
        .query_row(
            "SELECT COUNT(*) FROM history_projection_failures WHERE thread='t'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(retained, 20, "failure history stays bounded per thread");
}

#[test]
fn indexed_turn_windows_preserve_cursor_edges_and_global_shell_positions() {
    let dir = TempDir::new();
    let rows: Vec<String> = (0..35)
        .flat_map(|i| {
            [
                context(&format!("turn-{i:03}")),
                message(&format!("row-{i:03}"), "retained"),
            ]
        })
        .collect();
    let (path, len) = save(dir.path(), &rows);
    let mut db = TurnIndexDb::open(dir.path().join("index.sqlite3").to_str().unwrap()).unwrap();
    db.materialize_rollout("t", &path).unwrap();
    let ids: Vec<String> = (0..35).map(|i| format!("turn-{i:03}")).collect();
    for desc in [true, false] {
        for limit in [1, 5, 100] {
            for cursor in std::iter::once(None).chain(ids.iter().map(|id| Some(id.as_str()))) {
                let (start, end, expected) = page_window(&ids, cursor, limit, desc).unwrap();
                let page = db
                    .window("t", &path, len, 1, cursor, limit, desc, "notLoaded")
                    .unwrap();
                assert_eq!(
                    page.shells
                        .iter()
                        .map(|shell| &shell.id)
                        .collect::<Vec<_>>(),
                    ids[start..end].iter().collect::<Vec<_>>()
                );
                assert_eq!(
                    page.shells.first().map(|shell| shell.ord),
                    (start < end).then_some(start as i64)
                );
                assert_eq!(page.page.next_cursor, expected.next_cursor);
                assert_eq!(page.page.backwards_cursor, expected.backwards_cursor);
                assert_eq!(page.page.has_more, expected.has_more);
            }
        }
    }
    let plan: String = db.conn.query_row("EXPLAIN QUERY PLAN SELECT turn,ordinal FROM history_turn_projection WHERE thread='t' AND ordinal<9223372036854775807 ORDER BY ordinal DESC LIMIT 6",[],|row|row.get(3)).unwrap();
    assert!(plan.contains("history_turn_order"), "{plan}");
}

#[test]
fn change_metadata_reads_do_not_materialize_any_message_chunks() {
    let dir = TempDir::new();
    let mut rows = fixture();
    rows.push(json!({"kind":"event","event":"file_change","turnId":"one","payload":{"path":"/tmp/a","kind":"modify","added":3,"removed":1,"turnDiff":"complete diff"}}).to_string());
    rows.push(json!({"kind":"event","event":"file_change","turnId":"three","payload":{"path":"/tmp/b","kind":"create","added":null,"removed":null,"turnDiff":null}}).to_string());
    let (path, len) = save(dir.path(), &rows);
    let mut db = TurnIndexDb::open(dir.path().join("index.sqlite3").to_str().unwrap()).unwrap();
    let before = db.file_changes("t", &path, len, 1).unwrap();
    assert_eq!(before.turns.len(), 2);
    db.conn
        .execute_batch("DELETE FROM history_item_chunks")
        .unwrap();
    let after = db.file_changes("t", &path, len, 1).unwrap();
    assert_eq!(before, after);
}

#[test]
fn stable_item_anchors_survive_storage_cursor_epochs() {
    let dir = TempDir::new();
    let (path, len) = save(
        dir.path(),
        &[
            context("one"),
            message("a", "first"),
            message("b", "second"),
            message("c", "third"),
        ],
    );
    let mut db = TurnIndexDb::open(dir.path().join("index.sqlite3").to_str().unwrap()).unwrap();
    db.materialize_rollout("t", &path).unwrap();
    db.conn
        .execute(
            "INSERT OR REPLACE INTO page_epochs_v1(thread,epoch) VALUES('t','epoch-current')",
            [],
        )
        .unwrap();
    let before = db
        .turn_slice_before("t", &path, len, 1, "one", "c", 1)
        .unwrap()
        .unwrap();
    assert_eq!(
        encoded(&before.history)
            .get("history")
            .unwrap()
            .as_array()
            .unwrap()[0]
            .get("uuid")
            .unwrap()
            .as_str()
            .unwrap()
            .try_to_utf8()
            .unwrap(),
        "b"
    );
    assert_eq!(
        before.page.next_cursor.as_deref(),
        Some("migration:epoch-current:b")
    );
    let previous = db
        .turn_slice(
            "t",
            &path,
            len,
            1,
            "one",
            MessageWindow {
                cursor: before.page.next_cursor.as_deref(),
                limit: 1,
                desc: true,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        encoded(&previous.history)
            .get("history")
            .unwrap()
            .as_array()
            .unwrap()[0]
            .get("uuid")
            .unwrap()
            .as_str()
            .unwrap()
            .try_to_utf8()
            .unwrap(),
        "a"
    );
}
