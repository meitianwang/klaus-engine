//! Defect: model directories hid upstream rows based on local eligibility rules.
//! Root cause: projection and persisted snapshots discarded disabled or gated models.
//! Work item: 20261021 account-independent model catalog selection.

use super::{make_create_session, TestClient};
use crate::app_server::catalog_test_support::{store_credential, Gateway};
use crate::app_server::{ark, bailian, qoder, qwenwork, workbuddy, zhipu};
use serde_json::{json, Value};
use std::time::Duration;

#[tokio::test]
async fn named_model_catalogs_keep_upstream_rows_and_replace_filtered_snapshots() {
    for source in ["qoder", "qwenwork", "workbuddy", "bailian", "zhipu", "ark"] {
        let lock = match source {
            "qoder" => qoder::test_env_lock(),
            "qwenwork" => qwenwork::test_env_lock(),
            "workbuddy" => workbuddy::test_env_lock(),
            "zhipu" => zhipu::test_env_lock(),
            "bailian" => bailian::test_env_lock(),
            _ => ark::test_env_lock(),
        };
        let _guard = lock.lock().await;
        let root = tempfile::tempdir().unwrap();
        store_credential(source, root.path(), "catalog");
        let fingerprint = match source {
            "qoder" => {
                qoder::set_config_root(root.path());
                qoder::auth::catalog_fingerprint()
            }
            "qwenwork" => {
                qwenwork::set_config_root(root.path());
                qwenwork::auth::catalog_fingerprint()
            }
            "workbuddy" => {
                workbuddy::set_config_root(root.path());
                workbuddy::auth::catalog_fingerprint()
            }
            "bailian" => {
                bailian::set_config_root(root.path());
                bailian::auth::catalog_fingerprint()
            }
            "zhipu" => {
                zhipu::set_config_root(root.path());
                zhipu::auth::catalog_fingerprint()
            }
            _ => {
                ark::set_config_root(root.path());
                ark::auth::catalog_fingerprint()
            }
        }
        .expect("fixture credential fingerprint");
        std::fs::write(
            root.path().join(format!("{source}-model-catalog.json")),
            json!({
                "fingerprint": fingerprint,
                "models": [{"key":"filtered-snapshot","display_name":"Old projection"}]
            })
            .to_string(),
        )
        .unwrap();
        let restricted = match source {
            "zhipu" => "glm-5.3-flashx",
            _ => "restricted-model",
        };
        let rows = json!([
            {"key":restricted,"id":restricted,"display_name":"Restricted","enable":false,"disabled":true,"status":"Shutdown","supportsToolCall":false},
            {"key":"ready-model","id":"ready-model","display_name":"Ready","enable":true,"supportsToolCall":true}
        ]);
        let body = match source {
            "qoder" => json!({"assistant":rows}),
            "qwenwork" => json!({"qwork":rows}),
            "workbuddy" => {
                json!({"models":rows,"agents":[{"name":"cli","models":["ready-model"]}]})
            }
            _ => json!({"data":rows,"has_more":false}),
        };
        let gateway = Gateway::start_with_body(source, Duration::ZERO, body).await;
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
        assert!(
            models.iter().any(|row| row["key"] == restricted),
            "{source}: {models:?}"
        );
        assert!(
            models.iter().any(|row| row["key"] == "ready-model"),
            "{source}: {models:?}"
        );
        assert!(!models.iter().any(|row| row["key"] == "filtered-snapshot"));
        assert_eq!(
            gateway.request_count(),
            1,
            "{source} only fetches the catalog"
        );
        let snapshot: Value = serde_json::from_slice(
            &std::fs::read(root.path().join(format!("{source}-model-catalog.json"))).unwrap(),
        )
        .unwrap();
        assert_eq!(snapshot["projectionVersion"], 1);
        client.close().await;
    }
}
