//! Defect: sidebar pinning failed with `has a concurrent lifecycle operation` or `is locked by another writer` while a session rollout was open or restoring.
//! Root cause: `JsonlSessionStore::update_host_metadata` acquired `.jsonl.lock` with zero-wait lifecycle checks even though host metadata lives solely in `thread-metadata.sqlite3`.
//! Fixed by: 2026-11-23 sidebar pinning concurrency repair.

use klaus_agent_core::runtime_values::runtime_json;
use klaus_agent_core::schemas::rollout::SessionMeta;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::{ListOptions, SessionStore};
use std::io;

#[tokio::test]
async fn cold_store_host_metadata_update_succeeds_while_writer_lease_is_held_and_preserves_destructive_fences(
) {
    let dir = std::env::temp_dir().join(format!("klaus-pin-restore-{}", uuid::Uuid::new_v4()));
    let owner = JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.to_string_lossy().into_owned()),
        archive_dir: None,
    });
    let seed = owner
        .create(SessionMeta {
            id: "thread-pin".into(),
            created_at: 1_700_000_000,
            cwd: "/tmp".into(),
            model: "m".into(),
            title: None,
            originator: None,
            cli_version: None,
            source: None,
            role: None,
            parent_thread_id: None,
            forked_from_id: None,
            git: None,
            host_metadata: None,
        })
        .await
        .unwrap();
    seed.shutdown().await.unwrap();

    let resumed = owner.resume_runtime_for_write("thread-pin").await.unwrap();
    let contender = JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.to_string_lossy().into_owned()),
        archive_dir: None,
    });
    let pinned = runtime_json::parse(&r#"{"sidebarPinnedAt":1700000000123}"#.into()).unwrap();
    contender
        .update_host_metadata("thread-pin", Some(pinned.clone()))
        .await
        .unwrap();
    assert_eq!(
        contender.list(ListOptions::default()).await.unwrap().items[0].host_metadata,
        Some(pinned)
    );

    // Destructive lifecycle mutations must still honor the active `.jsonl.lock` lease.
    assert_eq!(
        contender.delete("thread-pin").await.unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );

    resumed.rollout.shutdown().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}
