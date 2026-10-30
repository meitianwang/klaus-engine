//! Profile metrics must read saved records without constructing a provider or writer.

use super::*;
use crate::app_server::wire;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
async fn profile_activity_facade_reads_archived_usage_without_session_activation() {
    let root = tempfile::tempdir().unwrap();
    let archived = root.path().join("archived");
    std::fs::create_dir_all(&archived).unwrap();
    let file = archived.join("1-profile.jsonl");
    let records = [
        json!({"kind":"session_meta","schemaVersion":2,"meta":{"id":"profile",
            "createdAt":1704110400000_i64,"cwd":"/fixture","model":"fixture"}}),
        json!({"kind":"turn_context","ctx":{"cwd":"/fixture","model":"fixture",
            "permissionMode":"default","turnId":"turn","startedAtMs":1704110400000_i64}}),
        json!({"kind":"usage_ledger","requestId":"req","assistantMessageId":"assistant",
            "model":"fixture","usage":{"inputTokens":10,"outputTokens":3,
            "cacheReadInputTokens":4,"cacheCreationInputTokens":5,"totalTokens":22},"costUsd":0}),
        json!({"kind":"event","event":"turn_lifecycle","turnId":"turn","payload":{
            "status":"completed","startedAtMs":1704110400000_i64,"durationMs":500}}),
    ];
    std::fs::write(
        &file,
        records
            .iter()
            .map(|record| format!("{record}\n"))
            .collect::<String>(),
    )
    .unwrap();
    let before = std::fs::read(&file).unwrap();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.path().to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let activations = Arc::new(AtomicUsize::new(0));
    let counter = activations.clone();
    let create: CreateSession = Arc::new(move |_| {
        counter.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err("profile reads must not construct a session".into()) })
    });
    let lifecycle = crate::lifecycle::protocol_lifecycle(make_agent_for_tests(vec![]), Some(store));
    let mut client = TestClient::start_with(create, Some(lifecycle));
    client.initialize().await;
    client
        .send(json!({"id":10,"method":"engine/profileActivity","params":{}}))
        .await;
    let response = client.response_for(10).await.0;
    assert_eq!(
        response["result"],
        json!({
            "days":[{"date":"2024-01-01","tokens":22}],"totalTokens":22,"peakDailyTokens":22,
            "longestTaskMs":500,"longestStreakDays":1,"currentStreakDays":0
        })
    );
    let _: wire::EngineProfileActivityResult =
        serde_json::from_value(response["result"].clone()).unwrap();
    assert_eq!(activations.load(Ordering::SeqCst), 0);
    assert_eq!(std::fs::read(file).unwrap(), before);
    assert!(!root.path().join("1-profile.jsonl").exists());
    assert!(!archived.join("1-profile.jsonl.lock").exists());
    client.close().await;
}

#[tokio::test]
async fn profile_activity_facade_validates_params_and_publishes_its_schema() {
    let mut client = TestClient::start(make_create_session(&[]));
    client.initialize().await;
    client
        .send(json!({"id":10,"method":"engine/profileActivity","params":{"sessionId":"x"}}))
        .await;
    assert_eq!(
        client.response_for(10).await.0["error"]["code"],
        wire::INVALID_PARAMS
    );
    client
        .send(json!({"id":11,"method":"engine/profileActivity","params":{}}))
        .await;
    assert_eq!(
        client.response_for(11).await.0["error"]["code"],
        wire::ENGINE_ERROR
    );
    let schema = schema_bundle();
    let declared = &schema["requests"]["engine/profileActivity"];
    assert_eq!(declared["params"]["additionalProperties"], false);
    assert!(declared["result"]["properties"]["days"].is_object());
    assert!(declared["result"]["properties"]["totalTokens"].is_object());
    client.close().await;
}
