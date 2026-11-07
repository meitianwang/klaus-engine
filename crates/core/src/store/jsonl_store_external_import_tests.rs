//! Actual filesystem failure/cancellation boundaries for atomic external import.

use super::*;
use crate::schemas::content::{ContentBlock, TextBlock};
use crate::schemas::message::UserMessage;
use crate::store::external_import::ExternalImportMessage;
use std::sync::Arc;

const ID: &str = "e561cbaa-76cb-5d4c-8dce-11d10f4dfe82";
struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("klaus-import-atomic-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct ReleaseGate(ImportTestGate);
impl ReleaseGate {
    fn new() -> Self {
        Self((
            Arc::new(tokio::sync::Notify::new()),
            Arc::new((StdMutex::new(false), std::sync::Condvar::new())),
        ))
    }
    fn release(&self) {
        *self.0 .1 .0.lock().unwrap() = true;
        self.0 .1 .1.notify_all();
    }
}
impl Drop for ReleaseGate {
    fn drop(&mut self) {
        self.release();
    }
}
fn store(path: &Path) -> Arc<JsonlSessionStore> {
    Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(path.to_string_lossy().into_owned()),
        archive_dir: None,
    }))
}
fn input() -> ExternalSessionImport {
    ExternalSessionImport {
        allow_create: true,
        meta: SessionMeta {
            id: ID.into(),
            created_at: 1,
            cwd: "/fixture".into(),
            model: "external".into(),
            title: None,
            originator: Some("external-import".into()),
            cli_version: None,
            source: Some("external:fixture".into()),
            role: None,
            parent_thread_id: None,
            forked_from_id: None,
            git: None,
            host_metadata: None,
        },
        provenance: runtime_json::parse(
            &"{\"requestSha256\":\"fixture\",\"source\":{\"providerId\":\"fixture\"}}".into(),
        )
        .unwrap(),
        messages: vec![ExternalImportMessage {
            message: Message::User(UserMessage {
                question_answer: None,
                uuid: "external-user".into(),
                content: vec![ContentBlock::Text(TextBlock {
                    text: "complete imported history".into(),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                })],
                is_meta: None,
            }),
            origin: runtime_json::parse(&"{\"messageId\":\"external-user\"}".into()).unwrap(),
        }],
        receipt: runtime_json::parse(&"{\"requestSha256\":\"fixture\",\"messageCount\":1}".into())
            .unwrap(),
    }
}

#[tokio::test]
async fn external_import_failed_atomic_publication_preserves_collision_and_removes_candidate() {
    let scratch = Scratch::new();
    let store = store(&scratch.0);
    let gate = ReleaseGate::new();
    *store.import_before_publish.lock().unwrap() = Some(gate.0.clone());
    let operation = tokio::spawn({
        let store = store.clone();
        async move { store.import_external_session(input()).await }
    });
    gate.0 .0.notified().await;
    assert!(
        store
            .list(Default::default())
            .await
            .unwrap()
            .items
            .is_empty(),
        "private complete candidate must remain invisible"
    );
    let target = scratch.0.join(format!("1-{ID}.jsonl"));
    std::fs::write(&target, b"preexisting collision bytes").unwrap();
    gate.release();
    let error = operation.await.unwrap().unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
    assert_eq!(
        std::fs::read(&target).unwrap(),
        b"preexisting collision bytes"
    );
    let entries: Vec<_> = std::fs::read_dir(&scratch.0)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert!(!entries
        .iter()
        .any(|name| name.to_string_lossy().ends_with(".tmp")));
    assert!(store
        .list(Default::default())
        .await
        .unwrap()
        .items
        .is_empty());
}

#[tokio::test]
async fn external_import_cancelled_waiter_retains_identity_lock_until_complete_publication() {
    let scratch = Scratch::new();
    let first = store(&scratch.0);
    let second = store(&scratch.0);
    let gate = ReleaseGate::new();
    *first.import_before_publish.lock().unwrap() = Some(gate.0.clone());
    let operation = tokio::spawn({
        let store = first.clone();
        async move { store.import_external_session(input()).await }
    });
    gate.0 .0.notified().await;
    operation.abort();
    assert!(operation.await.unwrap_err().is_cancelled());
    assert!(second
        .list(Default::default())
        .await
        .unwrap()
        .items
        .is_empty());
    let error = second.create(input().meta).await.unwrap_err();
    assert_eq!(
        error.kind(),
        io::ErrorKind::WouldBlock,
        "cancelled waiter must not release the blocking publisher's lock"
    );
    gate.release();
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match second.import_external_session(input()).await {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await
                }
                result => break result.unwrap(),
            }
        }
    })
    .await
    .unwrap();
    assert!(result.already_imported);
    assert_eq!(result.message_count, 1);
    let snapshot = second.read_thread(ID).await.unwrap();
    assert_eq!(snapshot.raw_items.len(), 5);
    assert_eq!(
        second.list(Default::default()).await.unwrap().items.len(),
        1
    );
}
