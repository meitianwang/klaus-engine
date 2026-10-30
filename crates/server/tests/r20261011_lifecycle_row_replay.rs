//! Defect: a sub-agent lifecycle row replayed from rollout landed on whatever
//! turn happened to be LAST in the projection, so a row whose owner turn had
//! long closed stranded itself at the transcript tail as a turn of its own.
//! Root cause: the replay loop appended lifecycle rows to `turns.last_mut()`
//! instead of the turn that spawned them.
//! Fixed by: 2026-10-11 subagent lifecycle attribution work item — the row
//! carries its spawn turn; replay places it there (even in a closed turn) and
//! drops rows whose owner turn is unknown or unstamped.

use super::thread_read_facade::{
    fixture_header, fixture_turn, read, read_only_client, store, write_fixture,
};
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

fn lifecycle_row(spawn_turn: Option<&str>, agent: &str) -> Value {
    let mut context = json!({
        "sessionId": "replay-root",
        "parentAgentId": null,
        "parentThreadId": "replay-root",
        "agentType": null,
        "cwd": "/tmp",
        "description": "late child",
        "depth": 1,
        "resumeFrom": null,
    });
    context["spawnTurnId"] = match spawn_turn {
        Some(turn) => json!(turn),
        None => Value::Null,
    };
    json!({"kind":"subagent_lifecycle","record":{
        "agentId": agent,
        "kind": "background",
        "status": "completed",
        "context": context,
        "startedAtMs": 1700000000300_i64,
        "updatedAtMs": 1700000000400_i64,
        "completedAtMs": 1700000000400_i64,
        "lastResult": "done",
        "lastError": null,
        "cancellationRequested": false,
    }})
}

#[tokio::test]
async fn read_thread_places_a_late_lifecycle_row_in_its_spawn_turn_and_drops_unowned_rows() {
    let dir = std::env::temp_dir().join(format!("klaus-lifecycle-replay-{}", uuid::Uuid::new_v4()));
    let mut rows = vec![fixture_header("replay-root")];
    rows.extend(fixture_turn("owner", "first question"));
    rows.extend(fixture_turn("later", "second question"));
    // The child settled after BOTH turns closed: its rows sit at the end of
    // the file, behind the later turn's boundary.
    rows.push(lifecycle_row(Some("owner"), "late-child"));
    rows.push(lifecycle_row(None, "orphan-child"));
    write_fixture(&dir, "replay-root", &rows);
    let activations = Arc::new(AtomicUsize::new(0));
    let mut client = read_only_client(store(&dir), activations.clone());
    client.initialize().await;
    let result = read(
        &mut client,
        20,
        json!({"threadId":"replay-root","limit":10}),
    )
    .await;
    let turns = result["result"]["turns"].as_array().expect("turns").clone();
    let owner = turns
        .iter()
        .find(|turn| turn["id"] == "owner")
        .expect("owner turn");
    let later = turns
        .iter()
        .find(|turn| turn["id"] == "later")
        .expect("later turn");
    let owner_rows = owner["items"]
        .as_array()
        .expect("owner items")
        .iter()
        .filter(|item| item["type"] == "klausItem" && item["item"]["kind"] == "subagent_lifecycle")
        .count();
    assert_eq!(
        owner_rows, 1,
        "the late row settles inside its spawn turn: {owner}"
    );
    let later_rows = later["items"]
        .as_array()
        .expect("later items")
        .iter()
        .filter(|item| item["type"] == "klausItem" && item["item"]["kind"] == "subagent_lifecycle")
        .count();
    assert_eq!(
        later_rows, 0,
        "the later turn must not inherit the late row: {later}"
    );
    let total = turns
        .iter()
        .flat_map(|turn| turn["items"].as_array().cloned().unwrap_or_default())
        .filter(|item| item["type"] == "klausItem" && item["item"]["kind"] == "subagent_lifecycle")
        .count();
    assert_eq!(
        total, 1,
        "an unstamped row is dropped, never stranded: {turns:?}"
    );
    assert_eq!(activations.load(Ordering::SeqCst), 0);
    client.close().await;
    std::fs::remove_dir_all(dir).unwrap();
}
