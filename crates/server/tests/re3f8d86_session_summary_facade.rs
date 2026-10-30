//! Defect: refreshing one remote catalog row required loading a transcript or every session.
//! Root cause: the named facade had no metadata-only lookup operation.
//! Fixing commit: e3f8d8612aa38c0217810f219e3b31642f311277.

use super::*;
use crate::app_server::wire;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
async fn session_summary_facade_returns_exact_list_row_without_history_or_provider_activation() {
    let dir = tempfile::tempdir().unwrap();
    let archived = dir.path().join("archived");
    std::fs::create_dir_all(&archived).unwrap();
    let file = archived.join("1-summary.jsonl");
    let header = json!({
        "kind":"session_meta","schemaVersion":2,"meta":{"id":"summary","createdAt":1,
        "cwd":"/fixture","model":"fixture","title":"Archived title","role":"main",
        "forkedFromId":"source","hostMetadata":{"pinned":true,"custom":{"a":1}}}
    });
    std::fs::write(
        &file,
        header.to_string() + "\nmalformed body that may never be loaded\n",
    )
    .unwrap();
    let before = std::fs::read(&file).unwrap();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.path().to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let activations = Arc::new(AtomicUsize::new(0));
    let activation_counter = activations.clone();
    let create: CreateSession = Arc::new(move |_| {
        activation_counter.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err("catalog reads must not create a model session".into()) })
    });
    let lifecycle =
        crate::lifecycle::protocol_lifecycle(make_agent_for_tests(vec![]), Some(store.clone()));
    let mut client = TestClient::start_with(create, Some(lifecycle));
    client.initialize().await;
    client.send(json!({"id":10,"method":"engine/listSessions","params":{"options":{"includeArchived":true}}})).await;
    let listed = client.response_for(10).await.0;
    let row = listed["result"]["sessions"]["items"][0].clone();
    assert_eq!(
        row["id"], "summary",
        "fixture must be a canonical header: {listed}"
    );
    assert!(listed["result"]["sessions"]["membershipFingerprint"]
        .as_str()
        .is_some());
    assert!(listed["result"]["sessions"]["metadataFingerprint"]
        .as_str()
        .is_some());
    let io_before = store.catalog_io_stats().await;
    let pending_journal = dir.path().join(".unarchive-journal/pending/intent.json");
    std::fs::create_dir_all(pending_journal.parent().unwrap()).unwrap();
    std::fs::write(
        &pending_journal,
        "unfinished transaction owned by another process",
    )
    .unwrap();
    client
        .send(
            json!({"id":11,"method":"engine/readSessionSummary","params":{"sessionId":"summary"}}),
        )
        .await;
    let response = client.response_for(11).await.0;
    assert_eq!(response["result"], json!({"session":row}));
    let io_after = store.catalog_io_stats().await;
    assert_eq!(io_after.header_reads, io_before.header_reads);
    assert_eq!(io_after.directory_scans, io_before.directory_scans);
    assert_eq!(io_after.metadata_reads, io_before.metadata_reads + 1);
    client
        .send(json!({"id":12,"method":"engine/readSessionSummary","params":{"sessionId":"absent"}}))
        .await;
    assert_eq!(
        client.response_for(12).await.0["result"],
        json!({"session":null})
    );
    assert_eq!(std::fs::read(&file).unwrap(), before);
    assert!(!dir.path().join("1-summary.jsonl").exists());
    assert!(!archived.join("1-summary.jsonl.lock").exists());
    assert_eq!(
        std::fs::read_to_string(pending_journal).unwrap(),
        "unfinished transaction owned by another process",
        "metadata-only reads must never run lifecycle recovery"
    );
    assert_eq!(activations.load(Ordering::SeqCst), 0);
    client.close().await;
}

#[tokio::test]
async fn session_summary_facade_validates_params_and_has_a_published_named_schema() {
    let mut client = TestClient::start(make_create_session(&[]));
    client.initialize().await;
    for (index, params) in [
        json!({}),
        json!({"sessionId":" "}),
        json!({"sessionId":12}),
        json!({"sessionId":"x","history":true}),
    ]
    .into_iter()
    .enumerate()
    {
        let id = index as u64 + 10;
        client
            .send(json!({"id":id,"method":"engine/readSessionSummary","params":params}))
            .await;
        assert_eq!(
            client.response_for(id).await.0["error"]["code"],
            wire::INVALID_PARAMS
        );
    }
    client
        .send(json!({"id":20,"method":"engine/readSessionSummary","params":{"sessionId":"x"}}))
        .await;
    assert_eq!(
        client.response_for(20).await.0["error"]["code"],
        wire::ENGINE_ERROR,
        "unsupported stores must not pretend the session is absent"
    );
    let schema = schema_bundle();
    let declared = &schema["requests"]["engine/readSessionSummary"];
    assert_eq!(declared["params"]["required"], json!(["sessionId"]));
    assert_eq!(declared["params"]["additionalProperties"], false);
    assert!(
        declared["result"]["properties"]["session"].is_object()
            || declared["result"]["properties"]["session"] == true
    );
    client.close().await;
}
