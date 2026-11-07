//! Defect: valid unmetered assistant history triggered incomplete activity.
//! Root cause: the metrics reader required a model on legacy unmetered messages.
//! Fixing commit: 7ec59717e02633c271d3cd1a8588a0716b9f9f09.

use std::io::Write;
use std::path::{Path, PathBuf};

use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::profile_activity::ProfileActivityDay;
use klaus_agent_core::store::session_store::SessionStore;
use serde_json::{json, Value};

struct History(PathBuf);

impl History {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("activity-legacy-{}", uuid::Uuid::new_v4()));
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

fn turn(day: u32) -> Vec<Value> {
    vec![
        json!({"kind":"turn_context","ctx":{"cwd":"/fixture","model":"fixture",
            "permissionMode":"default","turnId":"turn","startedAtMs":clock(day)}}),
        json!({"kind":"event","event":"turn_lifecycle","turnId":"turn","payload":{
            "status":"completed","startedAtMs":clock(day),"finishedAtMs":clock(day)+500,
            "durationMs":500}}),
    ]
}

fn ledger(id: &str) -> Value {
    json!({"kind":"usage_ledger","requestId":id,"assistantMessageId":id,"model":"fixture",
        "usage":{"inputTokens":101,"outputTokens":0,"cacheReadInputTokens":0,
            "cacheCreationInputTokens":0},"costUsd":0})
}

fn write(path: &Path, items: &[Value]) {
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
async fn profile_activity_accepts_legacy_unmetered_assistants_without_rewriting_history() {
    let history = History::new();
    let file = history.0.join("history.jsonl");
    let mut items = vec![header()];
    items.extend(turn(18));
    items.push(json!({"kind":"message","message":{
        "type":"assistant","uuid":"unmetered","content":[]}}));
    items.push(json!({"kind":"message","message":{
        "type":"assistant","uuid":"null-usage","content":[],"usage":null}}));
    items.push(ledger("request"));
    write(&file, &items);
    std::fs::OpenOptions::new().append(true).open(&file).unwrap().write_all(
        b"{\"kind\":\"message\",\"message\":{\"type\":\"assistant\",\"uuid\":\"surrogate\",\"content\":[{\"type\":\"text\",\"text\":\"\\ud800\"}]}}\n",
    ).unwrap();
    let before = std::fs::read(&file).unwrap();
    let result = history.store().profile_activity(clock(19)).await.unwrap();
    assert!(!result.incomplete, "{result:?}");
    assert_eq!(result.total_tokens, 101);
    assert_eq!(result.peak_daily_tokens, 101);
    assert_eq!(result.longest_task_ms, 500);
    assert_eq!(result.longest_streak_days, 1);
    assert_eq!(result.current_streak_days, 1);
    assert_eq!(
        result.days,
        vec![ProfileActivityDay {
            date: "2026-10-18".into(),
            tokens: 101
        }]
    );
    assert_eq!(std::fs::read(&file).unwrap(), before);
    assert!(!history.0.join("history.jsonl.lock").exists());
}

#[tokio::test]
async fn profile_activity_legacy_assistant_ids_keep_parent_clock_ownership() {
    let history = History::new();
    let mut parent = vec![header()];
    parent.extend(turn(18));
    parent.push(json!({"kind":"message","message":{
        "type":"assistant","uuid":"request","content":[]}}));
    parent.push(ledger("request"));
    write(&history.0.join("history.jsonl"), &parent);
    let mut child = turn(17);
    child.push(ledger("request"));
    write(
        &history.0.join("history/subagents/agent-child.jsonl"),
        &child,
    );
    let result = history.store().profile_activity(clock(19)).await.unwrap();
    assert!(!result.incomplete, "{result:?}");
    assert_eq!(result.total_tokens, 101);
    assert_eq!(
        result.days,
        vec![ProfileActivityDay {
            date: "2026-10-18".into(),
            tokens: 101
        }]
    );
}

#[tokio::test]
async fn profile_activity_legacy_compatibility_keeps_corrupt_records_explicit() {
    let corrupt = [
        json!({"type":"assistant","uuid":"bad","content":[],"usage":ledger("bad")["usage"]}),
        json!({"type":"assistant","uuid":"bad","content":[],"usage":{}}),
        json!({"type":"assistant","uuid":"bad","content":[],"model":null}),
        json!({"type":"assistant","uuid":"bad","content":[],"model":42}),
        json!({"type":"assistant","uuid":42,"content":[]}),
        json!({"type":"assistant","uuid":"bad","content":42}),
        json!({"type":"assistant","uuid":"bad"}),
    ];
    for message in corrupt {
        let history = History::new();
        let mut items = vec![header()];
        items.extend(turn(18));
        items.push(ledger("known"));
        items.push(json!({"kind":"message","message":message}));
        write(&history.0.join("history.jsonl"), &items);
        let result = history.store().profile_activity(clock(19)).await.unwrap();
        assert!(result.incomplete, "{message}: {result:?}");
        assert_eq!(result.total_tokens, 101);
    }
}
