//! Defect: saved transcript fallback lost message blocks and tool terminal metadata.
//! Root cause: inspection projections were used as a substitute for full history.
//! Fixed by: 2026-09-13 desktop complete-history parity work item.

use klaus_agent_core::runtime_values::{runtime_codec, RuntimeEncode};
use klaus_agent_core::schemas::rollout::RolloutItem;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::rollout::parse_rollout_line;
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::store::thread_history::project_thread_history;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

fn fixture() -> Value {
    serde_json::from_str(include_str!("../fixtures/complete_thread_history.json")).unwrap()
}

fn items(rows: &[Value]) -> Vec<RolloutItem> {
    rows.iter()
        .map(|row| parse_rollout_line(&row.to_string()).expect("valid source-owned rollout"))
        .collect()
}

fn encode(value: &impl RuntimeEncode) -> Value {
    serde_json::from_slice(&runtime_codec::encode_bytes(value).unwrap()).unwrap()
}

fn write_rows(path: &Path, rows: &[Value]) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    items(rows);
    std::fs::write(
        path,
        rows.iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n")
            + "\n",
    )
    .unwrap();
}

fn files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn visit(root: &Path, dir: &Path, result: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(root, &path, result);
            } else {
                result.insert(
                    path.strip_prefix(root).unwrap().to_path_buf(),
                    std::fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut result = BTreeMap::new();
    visit(root, root, &mut result);
    result
}

#[test]
fn complete_history_preserves_messages_outputs_and_terminal_metadata() {
    let source = fixture();
    let rows = source["items"].as_array().unwrap();
    let result = encode(&project_thread_history(items(rows)));
    let expected = source["expected"].as_object().unwrap();
    let history = result["history"].as_array().unwrap();
    for (key, expected) in expected {
        if key == "messageIds" {
            assert_eq!(
                json!(history
                    .iter()
                    .map(|message| &message["uuid"])
                    .collect::<Vec<_>>()),
                *expected
            );
        } else {
            assert_eq!(result[key], *expected, "history metadata {key}");
        }
    }
    for uuid in [
        "display-one",
        "user-one",
        "assistant-one",
        "user-two",
        "user-three",
        "assistant-three",
    ] {
        let original = &rows
            .iter()
            .find(|row| row["message"]["uuid"] == uuid)
            .unwrap()["message"];
        let actual = history
            .iter()
            .find(|message| message["uuid"] == uuid)
            .unwrap();
        assert_eq!(actual, original, "complete message {uuid}");
    }
    let mut tool = rows
        .iter()
        .find(|row| row["message"]["uuid"] == "result-one")
        .unwrap()["message"]
        .clone();
    tool["output"] = json!({"exit_code":130,"stdout":"Stopped","fixture":"structured"});
    assert_eq!(
        *history
            .iter()
            .find(|message| message["uuid"] == "result-one")
            .unwrap(),
        tool
    );
    assert_eq!(
        history[4],
        json!({"type":"compact_boundary","uuid":"history-compaction-10","summary":"Saved summary"})
    );
    assert!(!result.to_string().contains("Removed"));
    assert!(!result.to_string().contains("discarded"));
}

#[test]
fn rollback_keeps_meta_messages_with_their_real_user_turn() {
    let rows = vec![
        json!({"kind":"message","message":{"type":"user","uuid":"one","content":[{"type":"text","text":"first"}]}}),
        json!({"kind":"message","message":{"type":"user","uuid":"meta","isMeta":true,"content":[{"type":"text","text":"system update"}]}}),
        json!({"kind":"message","message":{"type":"user","uuid":"two","content":[{"type":"text","text":"second"}]}}),
        json!({"kind":"rollback","numTurns":1}),
        json!({"kind":"message","message":{"type":"user","uuid":"replacement","content":[{"type":"text","text":"new branch"}]}}),
    ];
    let result = encode(&project_thread_history(items(&rows)));
    assert_eq!(
        result["history"]
            .as_array()
            .unwrap()
            .iter()
            .map(|message| message["uuid"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["one", "meta", "replacement"]
    );
    assert_eq!(result["historyTurnIds"], json!({}));
    assert_eq!(result["agentNotifications"], json!([]));
}

#[test]
fn repeated_tool_call_ids_keep_each_result_output() {
    let mut rows = Vec::new();
    for (turn, output) in [("one", "first output"), ("two", "second output")] {
        rows.extend([
            json!({"kind":"turn_context","ctx":{"cwd":"/tmp","model":"m","permissionMode":"default","turnId":turn}}),
            json!({"kind":"message","message":{"type":"tool_result","uuid":format!("result-{turn}"),"toolUseId":"call-0","content":[],"isError":false}}),
            json!({"kind":"event","event":"tool_result_output","turnId":turn,"payload":{"toolUseId":"call-0","output":{"text":output}}}),
        ]);
    }
    let result = encode(&project_thread_history(items(&rows)));
    assert_eq!(
        result["history"][0]["output"],
        json!({"text":"first output"})
    );
    assert_eq!(
        result["history"][1]["output"],
        json!({"text":"second output"})
    );
}

#[tokio::test]
async fn saved_child_history_is_read_only_for_active_and_archived_parents() {
    for archived in [false, true] {
        let dir =
            std::env::temp_dir().join(format!("klaus-complete-history-{}", uuid::Uuid::new_v4()));
        let namespace = if archived {
            dir.join("archived")
        } else {
            dir.clone()
        };
        let source = fixture();
        let rows = source["items"].as_array().unwrap();
        let parent = namespace.join("1700000000000-history.jsonl");
        write_rows(&parent, &rows[..1]);
        let child_dir = parent.with_extension("").join("subagents");
        let child = child_dir.join("agent-reviewer.jsonl");
        let mut child_rows = rows[1..].to_vec();
        child_rows.push(json!({"kind":"event","event":"tools_surfaced","payload":{"names":["Read","WebFetch","Read"]}}));
        write_rows(&child, &child_rows);
        let meta = json!({"agentType":"reviewer","model":"fixture-model","permissionMode":"default","effort":"high","createdAt":1700000000000_i64});
        std::fs::write(child_dir.join("agent-reviewer.meta.json"), meta.to_string()).unwrap();

        // A live parent writer and child writer must not block display reads.
        let parent_lock = std::fs::File::create(parent.with_extension("jsonl.lock")).unwrap();
        let child_lock = std::fs::File::create(child.with_extension("jsonl.lock")).unwrap();
        fs2::FileExt::lock_exclusive(&parent_lock).unwrap();
        fs2::FileExt::lock_exclusive(&child_lock).unwrap();
        let before = files(&dir);
        let store = JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(dir.to_string_lossy().into_owned()),
            archive_dir: None,
        });
        let snapshot = store.read_sub_agent("history", "reviewer").await.unwrap();
        assert_eq!(encode(&snapshot.meta), meta);
        assert_eq!(
            encode(&snapshot.surfaced_tools),
            json!(["Read", "WebFetch"])
        );
        let mut expected = encode(&project_thread_history(items(rows)));
        // A child rollout has no parent session_meta header, so its synthetic
        // compaction identity is local to the child's record positions.
        expected["history"][4]["uuid"] = json!("history-compaction-9");
        expected["historyTurnIds"]
            .as_object_mut()
            .unwrap()
            .remove("history-compaction-10");
        expected["historyTurnIds"]["history-compaction-9"] = json!("one");
        assert_eq!(
            encode(&project_thread_history(snapshot.raw_items)),
            expected
        );
        assert!(store
            .read_sub_agent("history", "missing")
            .await
            .unwrap()
            .raw_items
            .is_empty());
        assert_eq!(
            store
                .read_sub_agent("missing", "reviewer")
                .await
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::NotFound
        );
        assert_eq!(
            files(&dir),
            before,
            "display reads must not write any source or sidecar"
        );
        assert!(!dir.join(".lifecycle-locks").exists());
        drop(child_lock);
        drop(parent_lock);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
