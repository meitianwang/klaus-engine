//! Defect: transcripts written before turn clocks existed flagged the whole
//! profile as unreadable, so the profile page showed a read-failure warning
//! with a retry that could never succeed.
//! Root cause: undated usage and missing turn durations set the read-failure
//! flag instead of a separate coverage flag.
//! Fixing commit: 2a6667dcd772a8a0dc16e7a022b04117ab0b715c.

use std::io::Write;
use std::path::PathBuf;

use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use serde_json::{json, Value};

struct History(PathBuf);

impl History {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("activity-partial-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn store(&self) -> JsonlSessionStore {
        JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(self.0.to_string_lossy().into_owned()),
            archive_dir: None,
        })
    }
}

impl Drop for History {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn clock(day: u32) -> i64 {
    chrono::NaiveDate::from_ymd_opt(2026, 10, day)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_millis()
}

fn header() -> Value {
    json!({"kind":"session_meta","schemaVersion":2,"meta":{
        "id":"history","createdAt":clock(18),"cwd":"/fixture","model":"fixture"
    }})
}

fn ledger(id: &str) -> Value {
    json!({"kind":"usage_ledger","requestId":id,"assistantMessageId":id,"model":"fixture",
        "usage":{"inputTokens":101,"outputTokens":0,"cacheReadInputTokens":0,
        "cacheCreationInputTokens":0},"costUsd":0})
}

fn assistant(id: &str) -> Value {
    json!({"kind":"message","message":{
        "type":"assistant","uuid":id,"content":[],"model":"fixture",
        "usage":{"inputTokens":101,"outputTokens":0,"cacheReadInputTokens":0,
        "cacheCreationInputTokens":0}}})
}

fn write(path: &std::path::Path, items: &[Value]) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(
        path,
        items
            .iter()
            .map(|item| format!("{item}\n"))
            .collect::<String>(),
    )
    .unwrap();
}

#[tokio::test]
async fn profile_activity_keeps_legacy_undated_usage_partial_without_read_failure() {
    let history = History::new();
    // A rollout from before turn clocks: the context carries no startedAtMs and
    // no turn_lifecycle event ever arrives, yet the billed usage is intact.
    let items = vec![
        header(),
        json!({"kind":"turn_context","ctx":{"cwd":"/fixture","model":"fixture",
            "permissionMode":"default","turnId":"legacy"}}),
        assistant("legacy-request"),
        ledger("legacy-request"),
    ];
    write(&history.0.join("history.jsonl"), &items);
    let result = history.store().profile_activity(clock(19)).await.unwrap();
    assert!(
        !result.incomplete,
        "readable history is not a read failure: {result:?}"
    );
    // Project through serde so this regression also compiles on engines that
    // predate the partial flag, where it must fail at runtime instead.
    let projected = serde_json::to_value(&result).unwrap();
    assert_eq!(
        projected.get("partial"),
        Some(&json!(true)),
        "undated usage must stay explicit: {projected:?}"
    );
    assert_eq!(result.total_tokens, 101);
    assert_eq!(result.peak_daily_tokens, 0);
    assert!(result.days.is_empty());
    assert_eq!(result.longest_task_ms, 0);
    assert_eq!(result.longest_streak_days, 0);
    assert_eq!(result.current_streak_days, 0);
}

#[tokio::test]
async fn profile_activity_counts_running_turn_usage_without_any_warning_flag() {
    let history = History::new();
    // A turn that is still running: its clock exists, its final duration does
    // not. The calendar can place the usage; nothing failed and nothing is
    // permanently missing.
    let items = vec![
        header(),
        json!({"kind":"turn_context","ctx":{"cwd":"/fixture","model":"fixture",
            "permissionMode":"default","turnId":"running","startedAtMs":clock(18)}}),
        assistant("running-request"),
        ledger("running-request"),
    ];
    write(&history.0.join("history.jsonl"), &items);
    let result = history.store().profile_activity(clock(19)).await.unwrap();
    assert!(!result.incomplete, "{result:?}");
    let projected = serde_json::to_value(&result).unwrap();
    assert_eq!(projected.get("partial"), None, "{projected:?}");
    assert_eq!(result.total_tokens, 101);
    assert_eq!(
        result.days,
        vec![
            klaus_agent_core::store::profile_activity::ProfileActivityDay {
                date: "2026-10-18".into(),
                tokens: 101
            }
        ]
    );
    assert_eq!(result.longest_task_ms, 0);
    assert_eq!(result.current_streak_days, 1);
}

#[tokio::test]
async fn profile_activity_keeps_truncated_records_explicit_as_read_failures() {
    let history = History::new();
    let file = history.0.join("history.jsonl");
    write(
        &file,
        &[
            header(),
            json!({"kind":"turn_context","ctx":{"cwd":"/fixture","model":"fixture",
                "permissionMode":"default","turnId":"legacy"}}),
            assistant("legacy-request"),
            ledger("legacy-request"),
        ],
    );
    std::fs::OpenOptions::new()
        .append(true)
        .open(&file)
        .unwrap()
        .write_all(b"{\"kind\":\"usage_ledger\", truncated\n")
        .unwrap();
    let result = history.store().profile_activity(clock(19)).await.unwrap();
    assert!(
        result.incomplete,
        "a torn record must remain a read failure: {result:?}"
    );
    assert_eq!(result.total_tokens, 101);
}
