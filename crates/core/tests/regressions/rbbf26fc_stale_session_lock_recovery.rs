//! Defect: reopening a session called an unreclaimable stale lock an active writer.
//! Root cause: stale-lock rename errors were collapsed into a lock conflict.
//! Fixed by bbf26fc.

use klaus_agent_core::schemas::rollout::{SessionMeta, SessionRole};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use std::path::{Path, PathBuf};

struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "klaus-stale-lock-regression-{}",
            uuid::Uuid::new_v4()
        ));
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

fn meta() -> SessionMeta {
    SessionMeta {
        id: "sess-1".into(),
        created_at: 1,
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
async fn failed_stale_lock_reclaim_is_not_reported_as_an_active_writer() {
    let dir = Scratch::new();
    let store = store(&dir.0);
    let live = store.create(meta()).await.unwrap();
    live.shutdown().await.unwrap();

    let lock = dir.0.join("1-sess-1.jsonl.lock");
    std::fs::write(&lock, "2147483647\n").unwrap();
    std::fs::create_dir(format!("{}.stale-{}", lock.display(), std::process::id())).unwrap();

    let error = store.reopen("sess-1").await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("cannot reclaim stale session lock"),
        "{error}"
    );
    assert!(
        lock.exists(),
        "failed reclamation must leave the original lock intact"
    );
}
