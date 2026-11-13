//! Defect: dedicated media and editor-task models appeared as conversation choices.
//! Root cause: conversation responses exposed normalized inventories without purpose filtering.
//! Work item: 01a16397 source-wide conversation model purpose audit.

use super::{make_create_session, TestClient};
use crate::app_server::catalog_test_support::{store_credential, Gateway};
use crate::app_server::{
    antigravity, ark, bailian, kimi, minimax, qoder, qwenwork, workbuddy, zhipu,
};
use serde_json::{json, Value};
use std::time::Duration;

struct SourceHome(Option<std::ffi::OsString>);

impl Drop for SourceHome {
    fn drop(&mut self) {
        match self.0.take() {
            Some(previous) => klaus_agent_core::runtime_process::set_process_environment_var(
                "QODER_HOME",
                previous,
            ),
            None => klaus_agent_core::runtime_process::remove_process_environment_var("QODER_HOME"),
        }
    }
}

fn source_home(source: &str, root: &std::path::Path, rows: &[Value]) -> Option<SourceHome> {
    if source != "qoder" {
        return None;
    }
    let home = root.join("source-cache");
    std::fs::create_dir_all(home.join(".auth")).unwrap();
    std::fs::write(
        home.join(".auth/models"),
        json!({"assistant":rows}).to_string(),
    )
    .unwrap();
    let previous = std::env::var_os("QODER_HOME");
    klaus_agent_core::runtime_process::set_process_environment_var("QODER_HOME", home.as_os_str());
    Some(SourceHome(previous))
}

fn source_lock(source: &str) -> &'static tokio::sync::Mutex<()> {
    match source {
        "qoder" => qoder::test_env_lock(),
        "qwenwork" => qwenwork::test_env_lock(),
        "workbuddy" => workbuddy::test_env_lock(),
        "kimi" => kimi::test_env_lock(),
        "bailian" => bailian::test_env_lock(),
        "minimax" => minimax::test_env_lock(),
        "zhipu" => zhipu::test_env_lock(),
        _ => ark::test_env_lock(),
    }
}

fn source_root(source: &str, root: &std::path::Path) -> String {
    match source {
        "qoder" => {
            qoder::set_config_root(root);
            qoder::auth::catalog_fingerprint()
        }
        "qwenwork" => {
            qwenwork::set_config_root(root);
            qwenwork::auth::catalog_fingerprint()
        }
        "workbuddy" => {
            workbuddy::set_config_root(root);
            workbuddy::auth::catalog_fingerprint()
        }
        "kimi" => {
            kimi::set_config_root(root);
            kimi::auth::catalog_fingerprint()
        }
        "bailian" => {
            bailian::set_config_root(root);
            bailian::auth::catalog_fingerprint()
        }
        "minimax" => {
            minimax::set_config_root(root);
            minimax::auth::catalog_fingerprint()
        }
        "zhipu" => {
            zhipu::set_config_root(root);
            zhipu::auth::catalog_fingerprint()
        }
        _ => {
            ark::set_config_root(root);
            ark::auth::catalog_fingerprint()
        }
    }
    .unwrap()
}

#[tokio::test]
async fn source_catalog_responses_filter_dedicated_models_from_existing_snapshots() {
    let vector: Value =
        serde_json::from_str(include_str!("../fixtures/conversation-model-purpose.json")).unwrap();
    for case in vector["cases"].as_array().unwrap() {
        let source = case["source"].as_str().unwrap();
        let _guard = source_lock(source).lock().await;
        let root = tempfile::tempdir().unwrap();
        store_credential(source, root.path(), "purpose");
        let fingerprint = source_root(source, root.path());
        let mut rows = vector["conversationRows"].as_array().unwrap().clone();
        rows.extend(vector["dedicatedRows"].as_array().unwrap().clone());
        rows.extend(
            case["dedicated"]
                .as_array()
                .unwrap()
                .iter()
                .map(|key| json!({"key":key})),
        );
        let path = root.path().join(if source == "minimax" {
            "minimax-mplan-catalog.json".into()
        } else {
            format!("{source}-model-catalog.json")
        });
        std::fs::write(
            &path,
            json!({"projectionVersion":1,"fingerprint":fingerprint,"models":rows}).to_string(),
        )
        .unwrap();
        let _source_home = source_home(source, root.path(), &rows);
        let _gateway = Gateway::start(source, Duration::from_secs(20)).await;
        let mut client = TestClient::start_full(
            make_create_session(&[]),
            None,
            Some(root.path().join("rules.json")),
            Some(root.path().join("config.json")),
        );
        client.initialize().await;
        for (id, read) in [(91, "warm"), (92, "cached")] {
            client
                .send(json!({"id":id,"method":format!("{source}/models"),"params":{"read":read}}))
                .await;
            let response = client.response_for(id).await.0;
            let shown = response["result"]["models"]
                .as_array()
                .unwrap_or_else(|| panic!("{source}/{read}: {response}"));
            let expected = vector["conversationRows"].as_array().unwrap();
            assert_eq!(
                shown.iter().map(|model| &model["key"]).collect::<Vec<_>>(),
                expected
                    .iter()
                    .map(|model| &model["key"])
                    .collect::<Vec<_>>(),
                "{source}/{read}: {response}"
            );
            if source != "qoder" {
                assert_eq!(
                    json!(shown),
                    json!(expected),
                    "{source}/{read} preserves cached metadata"
                );
            }
        }
        let snapshot: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(
            snapshot["models"],
            json!(rows),
            "{source} runtime inventory remains complete"
        );
        client.close().await;
    }
}

#[tokio::test]
async fn live_source_projections_preserve_purpose_before_normalizing_metadata() {
    let vector: Value =
        serde_json::from_str(include_str!("../fixtures/conversation-model-purpose.json")).unwrap();
    for case in vector["cases"].as_array().unwrap() {
        let source = case["source"].as_str().unwrap();
        let _guard = source_lock(source).lock().await;
        let root = tempfile::tempdir().unwrap();
        store_credential(source, root.path(), "live-purpose");
        source_root(source, root.path());
        let _source_home = source_home(source, root.path(), &[]);
        let mut rows = vector["conversationRows"].as_array().unwrap().clone();
        rows.extend(vector["dedicatedRows"].as_array().unwrap().clone());
        rows.extend(
            case["dedicated"]
                .as_array()
                .unwrap()
                .iter()
                .map(|key| json!({"key":key})),
        );
        for row in &mut rows {
            row["id"] = row["key"].clone();
            row["name"] = row["key"].clone();
        }
        let body = match source {
            "qoder" => json!({"assistant":rows}),
            "qwenwork" => json!({"qwork":rows}),
            "workbuddy" => json!({"models":rows}),
            _ => json!({"data":rows,"has_more":false}),
        };
        let _gateway = Gateway::start_with_body(source, Duration::ZERO, body).await;
        let mut client = TestClient::start_full(
            make_create_session(&[]),
            None,
            Some(root.path().join("rules.json")),
            Some(root.path().join("config.json")),
        );
        client.initialize().await;
        client
            .send(json!({"id":91,"method":format!("{source}/models"),"params":{"read":"warm"}}))
            .await;
        let response = client.response_for(91).await.0;
        let models = response["result"]["models"]
            .as_array()
            .unwrap_or_else(|| panic!("{source}: {response}"));
        for row in vector["conversationRows"].as_array().unwrap() {
            assert!(
                models.iter().any(|model| model["key"] == row["key"]),
                "{source} retains {}: {response}",
                row["key"]
            );
        }
        for row in vector["dedicatedRows"].as_array().unwrap() {
            assert!(
                !models.iter().any(|model| model["key"] == row["key"]),
                "{source} excludes {}: {response}",
                row["key"]
            );
        }
        for id in case["dedicated"].as_array().unwrap() {
            assert!(
                !models.iter().any(|model| model["key"] == *id),
                "{source} excludes {id}: {response}"
            );
        }
        client.close().await;
    }
}

#[tokio::test]
async fn native_capability_inventory_stays_separate_from_conversation_choices() {
    let root = tempfile::tempdir().unwrap();
    antigravity::set_config_root(root.path());
    antigravity::auth::adopt(antigravity::auth::Credential {
        access_token: "fixture-access".into(),
        refresh_token: None,
        expires_at: None,
        account: Some("purpose@example.test".into()),
        auth_method: "consumer".into(),
        project: Some("fixture-project".into()),
        client_id: None,
        client_secret: None,
    })
    .await
    .unwrap();
    let generation = antigravity::auth::generation().unwrap();
    let payload = json!({
        "defaultAgentModelId":"vision-chat", "tabModelIds":["tab-model"],
        "imageGenerationModelIds":["native-image"], "webSearchModelIds":["native-search"],
        "audioTranscriptionModelIds":["native-transcription"],
        "models": {
            "vision-chat":{"supportsImages":true,"supportedMimeTypes":{"image/png":true,"video/mp4":true}},
            "tab-model":{}, "native-image":{}, "native-search":{}, "native-transcription":{},
            "private-model":{"isInternal":true}, "internal-only-model":{"isInternalOnly":true}
        }
    });
    std::fs::write(root.path().join("antigravity-models.json"),json!({"generation":generation,"fetchedAt":chrono::Utc::now().timestamp_millis(),"payload":payload}).to_string()).unwrap();
    let mut client = TestClient::start_full(
        make_create_session(&[]),
        None,
        Some(root.path().join("rules.json")),
        Some(root.path().join("config.json")),
    );
    client.initialize().await;
    client
        .send(json!({"id":91,"method":"antigravity/models","params":{"read":"cached"}}))
        .await;
    let response = client.response_for(91).await.0;
    let models = response["result"]["models"].as_array().unwrap();
    assert!(models.iter().any(|model| model["key"] == "vision-chat"));
    for id in [
        "tab-model",
        "native-image",
        "native-search",
        "native-transcription",
        "private-model",
        "internal-only-model",
    ] {
        assert!(
            !models.iter().any(|model| model["key"] == id),
            "{id}: {response}"
        );
    }
    let native = antigravity::capabilities().await.unwrap();
    assert_eq!(native["image"]["models"][0]["id"], "native-image");
    assert_eq!(native["search"]["models"][0]["id"], "native-search");
    assert_eq!(
        native["transcription"]["models"][0]["id"],
        "native-transcription"
    );
    client.close().await;
}
