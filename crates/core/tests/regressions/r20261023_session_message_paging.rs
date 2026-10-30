//! Saved-session message paging regression (work item: session-load-stall).
//! A turn was decoded in full before its message limit was applied; appends
//! also rebuilt the complete index whenever the modification time changed.
//! The store must select byte ranges before materializing message content.
use klaus_agent_core::schemas::rollout::RolloutItem;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::rollout::parse_rollout_line;
use klaus_agent_core::store::session_store::SessionStore;
use std::io::Write;

struct TempDir(std::path::PathBuf);
impl TempDir {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("klaus-message-paging-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn saved_session_message_paging_does_not_decode_unrequested_oversized_rows() {
    let dir = TempDir::new();
    let path = dir.path().join("1700000000000-paging.jsonl");
    let mut file = std::fs::File::create(&path).unwrap();
    writeln!(file,"{}",serde_json::json!({"kind":"session_meta","schemaVersion":2,"meta":{"id":"paging","createdAt":1700000000000_i64,"cwd":"/tmp","model":"m"}})).unwrap();
    writeln!(
        file,
        "{}",
        serde_json::json!({"kind":"turn_context","ctx":{"cwd":"/tmp","model":"m","turnId":"turn"}})
    )
    .unwrap();
    file.write_all(b"{\"kind\":\"message\",\"message\":{\"type\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"").unwrap();
    let chunk = vec![b'x'; 65536];
    for _ in 0..257 {
        file.write_all(&chunk).unwrap();
    }
    file.write_all(b"\"}],\"uuid\":\"old-big\"}}\n").unwrap();
    file.flush().unwrap();
    let store = JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.path().to_string_lossy().into_owned()),
        archive_dir: None,
    });
    let shells = store
        .paged_thread_history("paging", None, Some(100), Some("notLoaded"), None)
        .await
        .unwrap()
        .unwrap();
    assert!(shells.history.history.is_empty());
    drop(file);
    let live = store.reopen("paging").await.unwrap();
    let item=parse_rollout_line(&serde_json::json!({"kind":"message","message":{"type":"user","uuid":"fresh","content":[{"type":"text","text":"complete fresh message"}]}}).to_string()).unwrap();
    if let RolloutItem::Message { message } = item {
        live.append_message(message).await;
    } else {
        unreachable!();
    }
    live.flush().await.unwrap();
    let page = store
        .paged_turn_slice("paging", "turn", None, Some(1), None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(page.history.history.len(), 1);
    assert!(page.page.has_more);
    assert_eq!(page.page.next_cursor.as_deref(), Some("fresh"));
    let error = store
        .paged_turn_slice("paging", "turn", Some("fresh"), Some(1), None)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(error
        .to_string()
        .contains("decoded message length too large"));
    let error = store
        .paged_turn_slice("paging", "turn", Some("missing"), Some(1), None)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    live.shutdown().await.unwrap();
}
