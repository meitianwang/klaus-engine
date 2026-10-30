//! Defect: creating a different rollout filename could replace an existing thread ID in the index.
//! Root cause: create published its index before writer acquisition and did not serialize ID checks.
//! Fixed for the 2026-09-08 live side-task fork work item.

use klaus_agent_core::schemas::rollout::{SessionMeta, SessionRole};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::{ListOptions, SessionStore};
use std::path::{Path, PathBuf};

struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("klaus-create-identity-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn store(dir: &Path) -> JsonlSessionStore {
    JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.to_string_lossy().into_owned()),
        archive_dir: None,
    })
}

fn meta(id: &str, created_at: i64) -> SessionMeta {
    SessionMeta {
        id: id.into(),
        created_at,
        cwd: "/tmp".into(),
        model: "fixture".into(),
        title: None,
        originator: None,
        cli_version: None,
        source: None,
        role: Some(SessionRole::Main),
        parent_thread_id: None,
        forked_from_id: None,
        git: None,
        host_metadata: None,
    }
}

#[tokio::test]
async fn thread_create_identity_preserves_existing_active_and_archived_threads() {
    let dir = Scratch::new();
    let original = store(&dir.0);
    let live = original.create(meta("same-id", 1)).await.unwrap();
    live.shutdown().await.unwrap();
    let before = original.read_thread("same-id").await.unwrap();
    let contender = store(&dir.0);
    let error = contender.create(meta("same-id", 2)).await.unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    assert_eq!(original.read_thread("same-id").await.unwrap(), before);
    original.archive("same-id").await.unwrap();
    let archived = original.read_thread("same-id").await.unwrap();
    let error = contender.create(meta("same-id", 3)).await.unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    assert_eq!(original.read_thread("same-id").await.unwrap(), archived);
    assert!(!dir.0.join("2-same-id.jsonl").exists());
    assert!(!dir.0.join("3-same-id.jsonl").exists());
}

#[tokio::test]
async fn thread_create_identity_has_one_winner_across_independent_store_instances() {
    let dir = Scratch::new();
    let first = store(&dir.0);
    let second = store(&dir.0);
    let (one, two) = tokio::join!(
        first.create(meta("race-id", 11)),
        second.create(meta("race-id", 22))
    );
    assert_eq!(usize::from(one.is_ok()) + usize::from(two.is_ok()), 1);
    for outcome in [one, two] {
        match outcome {
            Ok(live) => live.shutdown().await.unwrap(),
            Err(error) => assert!(
                matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::AlreadyExists
                ),
                "{error}"
            ),
        }
    }
    let fresh = store(&dir.0);
    let page = fresh.list(ListOptions::default()).await.unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].id, "race-id");
    assert!(fresh.read_thread("race-id").await.is_ok());
}

#[tokio::test]
async fn thread_create_identity_does_not_publish_an_index_entry_when_writer_acquisition_fails() {
    let dir = Scratch::new();
    let store = store(&dir.0);
    let lock_path = dir.0.join("1-blocked.jsonl.lock");
    std::fs::write(&lock_path, format!("{}\n", std::process::id())).unwrap();
    let error = store.create(meta("blocked", 1)).await.unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    let page = store.list(ListOptions::default()).await.unwrap();
    assert!(
        page.items.is_empty(),
        "failed create leaked an index row: {page:?}"
    );
    std::fs::remove_file(lock_path).unwrap();
    let live = store.create(meta("blocked", 2)).await.unwrap();
    live.shutdown().await.unwrap();
    assert_eq!(
        store.read_thread("blocked").await.unwrap().meta.created_at,
        2
    );
}
