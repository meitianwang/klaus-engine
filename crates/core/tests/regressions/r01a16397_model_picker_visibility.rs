//! Defect: internal models appeared as conversation model choices.
//! Root cause: picker reads forwarded the complete runtime model directory.
//! Work item: 01a16397 conversation model visibility.

use super::{make_create_session, TestClient};
use crate::app_server::{codex_catalog, model_sources};
use serde_json::{json, Value};

fn ids(models: &Value) -> Vec<Value> {
    models
        .as_array()
        .expect("model directory")
        .iter()
        .map(|model| model["id"].clone())
        .collect()
}

#[tokio::test]
async fn conversation_catalog_hides_internal_rows_without_losing_runtime_metadata() {
    let vector: Value =
        serde_json::from_str(include_str!("../fixtures/model-picker-visibility.json")).unwrap();
    let root = tempfile::tempdir().unwrap();
    let account = format!("picker-{}", root.path().display());
    model_sources::write_codex_login(root.path(), json!({"accountId": account})).unwrap();
    std::fs::write(
        root.path().join("codex-model-catalog.json"),
        json!({
            "schemaVersion": 3,
            "accountId": account,
            "clientVersion": model_sources::codex_client_version(),
            "models": vector["models"]
        })
        .to_string(),
    )
    .unwrap();
    let mut client = TestClient::start_full(
        make_create_session(&[]),
        None,
        Some(root.path().join("rules.json")),
        Some(root.path().join("config.json")),
    );
    client.initialize().await;
    for (id, read) in [(91, "warm"), (92, "cached")] {
        client
            .send(json!({"id":id,"method":"codex/models","params":{"read":read}}))
            .await;
        let response = client.response_for(id).await.0;
        assert_eq!(response["result"]["ok"], true, "{response}");
        assert_eq!(
            ids(&response["result"]["models"]),
            vector["pickerIds"].as_array().unwrap().clone(),
            "{read}: {response}"
        );
    }
    let runtime = codex_catalog::all_models(root.path()).await.unwrap();
    assert_eq!(runtime, vector["models"].as_array().unwrap().clone());
    let offline = codex_catalog::all_models_offline(root.path(), &account);
    assert_eq!(offline, runtime);
    for id in vector["internalIds"].as_array().unwrap() {
        assert!(runtime.iter().any(|model| model["id"] == *id));
    }
    let review = codex_catalog::limits(root.path(), "codex-auto-review").await;
    assert_eq!(review.context_window, Some(872000));
    client.close().await;
}

#[tokio::test]
async fn bundled_conversation_catalog_excludes_internal_review_rows() {
    let root = tempfile::tempdir().unwrap();
    model_sources::write_codex_login(
        root.path(),
        json!({"accountId":format!("bundled-picker-{}", root.path().display())}),
    )
    .unwrap();
    let result = codex_catalog::list(root.path(), codex_catalog::CatalogRead::Warm).await;
    assert_eq!(result["ok"], true);
    let models = ids(&result["models"]);
    assert!(!models.is_empty());
    assert!(!models.contains(&json!("codex-auto-review")));
    assert!(codex_catalog::all_models(root.path())
        .await
        .unwrap()
        .iter()
        .any(|model| model["id"] == "codex-auto-review"));
}
