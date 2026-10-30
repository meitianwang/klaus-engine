//! Work item: saved-user-presentation-dependencies (2026-10-24).
//! Summary anchors and split item pages discarded user_display records, so
//! directory cards disappeared after reload. Keep raw pagination unchanged and
//! serve keyed presentation dependencies for each returned user.
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeJson};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use serde_json::{json, Value};

fn open(dir: &std::path::Path) -> JsonlSessionStore {
    JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.to_string_lossy().into_owned()),
        archive_dir: None,
    })
}
fn display(id: &str, target: &str, text: &str) -> Value {
    json!({"kind":"message","message":{"type":"system","uuid":id,"subtype":"user_display",
        "text":json!({"messageUuid":target,"localRender":{"displayText":text,
            "attachments":[{"type":"file","name":"images.png","path":"/fixture/images.png/"}]}}).to_string()}})
}
fn seed(dir: &std::path::Path) {
    let rows = vec![
        json!({"kind":"session_meta","schemaVersion":2,"meta":{"id":"presentation","createdAt":1700000000000_i64,"cwd":"/tmp","model":"m"}}),
        json!({"kind":"turn_context","ctx":{"cwd":"/tmp","model":"m","turnId":"one"}}),
        display("before", "user", "before"),
        json!({"kind":"message","message":{"type":"user","uuid":"user","content":[
            {"type":"text","text":"request\n\nAttached:\n- /fixture/images.png/"},
            {"type":"image","mimeType":"image/png","base64":"aGVsbG8="}]}}),
        display("after", "user", "after"),
        json!({"kind":"message","message":{"type":"system","uuid":"invalid","subtype":"user_display","text":"not a JSON envelope"}}),
        json!({"kind":"message","message":{"type":"system","uuid":"unrelated","subtype":"info","text":"unrelated payload"}}),
        json!({"kind":"message","message":{"type":"assistant","uuid":"final","model":"m","content":[{"type":"text","text":"done"}]}}),
        json!({"kind":"event","event":"turn_lifecycle","turnId":"one","payload":{"status":"completed","finalMessageId":"final"}}),
        json!({"kind":"message","message":{"type":"assistant","uuid":"commentary","model":"m","content":[{"type":"text","text":"later commentary"}]}}),
    ];
    std::fs::write(
        dir.join("1700000000000-presentation.jsonl"),
        rows.iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n")
            + "\n",
    )
    .unwrap();
}
fn value(value: &impl klaus_agent_core::runtime_values::RuntimeEncode) -> Value {
    serde_json::from_slice(&runtime_codec::encode_bytes(value).unwrap()).unwrap()
}
fn assert_sidecar(records: &[RuntimeJson]) {
    let rows = value(&records.to_vec());
    assert_eq!(rows.as_array().unwrap().len(), 1);
    assert_eq!(rows[0]["uuid"], "after");
    let envelope: Value = serde_json::from_str(rows[0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(envelope["messageUuid"], "user");
    assert_eq!(envelope["localRender"]["displayText"], "after");
    assert_eq!(
        envelope["localRender"]["attachments"][0]["path"],
        "/fixture/images.png/"
    );
}

#[tokio::test]
async fn summary_keeps_raw_anchors_and_last_valid_selected_user_presentation() {
    let dir = tempfile::tempdir().unwrap();
    seed(dir.path());
    let store = open(dir.path());
    let summary = store
        .paged_thread_history("presentation", None, Some(5), Some("summary"), Some("asc"))
        .await
        .unwrap()
        .unwrap();
    let rows = value(&summary.history)["history"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(
        rows.iter()
            .map(|row| row["uuid"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["user", "final"]
    );
    assert_eq!(rows[0]["content"][1]["type"], "image");
    assert_sidecar(&summary.presentation_records);
    let full = store
        .paged_thread_history("presentation", None, Some(5), Some("full"), None)
        .await
        .unwrap()
        .unwrap();
    assert!(full.presentation_records.is_empty());
}

#[tokio::test]
async fn ascending_and_descending_limit_one_keep_exact_raw_membership_and_dependencies() {
    let dir = tempfile::tempdir().unwrap();
    seed(dir.path());
    let store = open(dir.path());
    let full = store
        .paged_thread_history("presentation", None, Some(5), Some("full"), None)
        .await
        .unwrap()
        .unwrap();
    let expected = value(&full.history)["history"].as_array().unwrap().clone();
    for direction in ["asc", "desc"] {
        let mut cursor = None;
        let mut actual = Vec::new();
        loop {
            let page = store
                .paged_turn_slice(
                    "presentation",
                    "one",
                    cursor.as_deref(),
                    Some(1),
                    Some(direction),
                )
                .await
                .unwrap()
                .unwrap();
            let rows = value(&page.history)["history"].as_array().unwrap().clone();
            assert!(rows.len() <= 1);
            if rows.iter().any(|row| row["type"] == "user") {
                assert_sidecar(&page.presentation_records);
            } else {
                assert!(page.presentation_records.is_empty());
            }
            if direction == "asc" {
                actual.extend(rows);
            } else {
                actual.splice(0..0, rows);
            }
            cursor = page.page.next_cursor;
            if !page.page.has_more {
                break;
            }
        }
        assert_eq!(
            actual, expected,
            "raw item membership is unchanged for {direction}"
        );
    }
}
