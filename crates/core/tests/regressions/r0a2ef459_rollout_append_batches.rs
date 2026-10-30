//! Fixing commit: 0a2ef4598881a6dcdbcb8a78254b6e792229f578.
//! Repeated file and projection setup made burst persistence scale per record.
//! Preserve ordered durable barriers, metadata updates, and cold history replay.

use klaus_agent_core::schemas::message::Message;
use klaus_agent_core::schemas::rollout::{RolloutItem, SessionMeta};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::rollout::parse_rollout_line;
use klaus_agent_core::store::session_store::SessionStore;
use serde_json::json;
use std::time::Instant;

fn store(root: &std::path::Path) -> JsonlSessionStore {
    JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.to_string_lossy().into_owned()),
        archive_dir: None,
    })
}

fn meta() -> SessionMeta {
    match parse_rollout_line(
        &json!({
            "kind": "session_meta", "schemaVersion": 2,
            "meta": {"id": "append-batches", "createdAt": 1700000000000_i64,
                "cwd": "/tmp", "model": "fixture", "title": "Original"}
        })
        .to_string(),
    )
    .unwrap()
    {
        RolloutItem::SessionMeta { meta, .. } => meta,
        _ => unreachable!(),
    }
}

fn message(index: usize, text: &str) -> Message {
    match parse_rollout_line(
        &json!({
            "kind": "message", "message": {"type": "user", "uuid": format!("row-{index}"),
                "content": [{"type": "text", "text": text}]}
        })
        .to_string(),
    )
    .unwrap()
    {
        RolloutItem::Message { message } => message,
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn rollout_append_burst_preserves_durable_order_and_metadata_barriers() {
    let directory = tempfile::tempdir().unwrap();
    let active = store(directory.path());
    let live = active.create(meta()).await.unwrap();
    let payload = "ordered 文本 🧭 ".repeat(128);
    let started = Instant::now();
    for index in 0..256 {
        live.append_message(message(index, &payload)).await;
    }
    live.update_title("After the first batch").await.unwrap();
    live.flush().await.unwrap();
    let first_barrier_ms = started.elapsed().as_secs_f64() * 1000.0;
    let checkpoint = store(directory.path())
        .resume("append-batches")
        .await
        .unwrap();
    assert_eq!(checkpoint.messages.len(), 256);
    assert_eq!(checkpoint.meta.title.unwrap(), "After the first batch");
    for index in 256..512 {
        live.append_message(message(index, &payload)).await;
    }
    live.shutdown().await.unwrap();
    let durable_ms = started.elapsed().as_secs_f64() * 1000.0;
    live.append_message(message(512, "after shutdown must be rejected"))
        .await;
    live.shutdown().await.unwrap();
    let cold = store(directory.path())
        .resume("append-batches")
        .await
        .unwrap();
    assert_eq!(cold.messages.len(), 512);
    for (index, row) in cold.messages.iter().enumerate() {
        match row {
            Message::User(row) => {
                assert_eq!(row.uuid, format!("row-{index}"));
                assert_eq!(row.content.len(), 1);
                let klaus_agent_core::schemas::content::ContentBlock::Text(text) = &row.content[0]
                else {
                    panic!("the original text block must remain intact")
                };
                assert_eq!(text.text, payload);
            }
            _ => panic!("the original user message must remain intact"),
        }
    }
    let path = directory.path().join("1700000000000-append-batches.jsonl");
    let raw = std::fs::read_to_string(&path).unwrap();
    assert!(raw.ends_with('\n'));
    assert!(raw.lines().all(|line| parse_rollout_line(line).is_some()));
    eprintln!(
        "ROLLOUT_APPEND_MEASUREMENT {}",
        json!({
            "messages": 512, "payloadBytes": payload.len(), "fileBytes": raw.len(),
            "firstBarrierMs": first_barrier_ms, "durableMs": durable_ms,
            "coldMessages": cold.messages.len()
        })
    );
}
