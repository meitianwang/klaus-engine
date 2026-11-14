//! Defect: Antigravity models configured for orchestration, advisor, tiers, or feature targets failed with HOST_TARGET_REQUIRED.
//! Root cause: `resolve_tier_target` and `text_completion` omitted `"antigravity"` from source target resolution and credential pinning.
//! Work item: 20261113 Antigravity auxiliary and orchestration model targets.
//! Mounted by the app-server aggregate to exercise the public session facade.

use super::*;

async fn antigravity_fixture() -> Fixture {
    let store: Arc<dyn klaus_agent_core::credential_store::CredentialStore> =
        Arc::new(klaus_agent_core::mcp::credential_store::InMemoryCredentialStore::new());
    let fixture = Fixture::with_credentials(false, None, Some(store)).await;
    std::fs::write(
        fixture.root.path().join("config.toml"),
        "# Isolated fixture\n",
    )
    .unwrap();
    crate::app_server::antigravity::set_config_root(fixture.root.path());
    crate::app_server::antigravity::auth::adopt(crate::app_server::antigravity::auth::Credential {
        access_token: "fixture-antigravity-access".into(),
        refresh_token: Some("fixture-antigravity-refresh".into()),
        expires_at: Some(32_503_680_000_000i64),
        account: Some("antigravity@example.invalid".into()),
        auth_method: "consumer".into(),
        project: Some("fixture-project".into()),
        client_id: None,
        client_secret: None,
    })
    .await
    .unwrap();
    let generation = crate::app_server::antigravity::auth::generation().unwrap();
    let payload = json!({
        "defaultAgentModelId": "gemini-4-hydrogen-high",
        "imageGenerationModelIds": ["native-image-only"],
        "models": {
            "gemini-4-hydrogen-high": {
                "displayName": "Gemini 4 Hydrogen High",
                "supportsImages": true,
                "supportsThinking": true,
                "maxTokens": 1048576,
                "maxOutputTokens": 65536
            },
            "native-image-only": {}
        }
    });
    std::fs::write(
        fixture.root.path().join("antigravity-models.json"),
        json!({
            "generation": generation,
            "clientIdentity": crate::app_server::antigravity::cloud_test_identity(),
            "fetchedAt": chrono::Utc::now().timestamp_millis(),
            "payload": payload
        })
        .to_string(),
    )
    .unwrap();
    fixture
}

fn antigravity_target(model_id: &str) -> Value {
    json!({"type": "source", "source": "antigravity", "modelId": model_id})
}

fn assert_ready_antigravity_target(target: &Value, expected_model: &str) {
    assert_eq!(target["type"], "ready", "{target}");
    assert_eq!(target["source"], "antigravity", "{target}");
    assert_eq!(target["modelId"], expected_model, "{target}");
    assert_eq!(target["model"], expected_model, "{target}");
    for key in ["provider", "auth", "pricing"] {
        assert!(target.get(key).is_none(), "{target}");
    }
}

#[tokio::test]
async fn antigravity_orchestrator_advisor_tier_and_feature_targets_are_ready_on_start_and_reconfigure(
) {
    let main = LoopbackProvider::start().await;
    let mut fixture = antigravity_fixture().await;
    let mut init = main.init("fixture-main");
    init["orchestrationMode"] = json!(true);
    init["orchestratorTarget"] = antigravity_target("gemini-4-hydrogen-high");
    init["advisorTarget"] = antigravity_target("gemini-4-hydrogen-high");
    init["modelTierTargets"] = json!({"medium": antigravity_target("gemini-4-hydrogen-high")});
    init["featureTargets"] = json!({
        "title": antigravity_target("gemini-4-hydrogen-high"),
        "prediction": null,
        "memory": null
    });
    start_with_init(&mut fixture, 200, "antigravity-targets-start", init.clone()).await;
    let runtime = named_runtime(&mut fixture.client, 201, "antigravity-targets-start").await;
    assert_eq!(runtime["orchestrationMode"], true, "{runtime}");
    assert_ready_antigravity_target(&runtime["orchestratorTarget"], "gemini-4-hydrogen-high");
    assert_ready_antigravity_target(&runtime["advisorTarget"], "gemini-4-hydrogen-high");
    assert_ready_antigravity_target(
        &runtime["modelTierTargets"]["medium"],
        "gemini-4-hydrogen-high",
    );
    assert_ready_antigravity_target(
        &runtime["featureTargets"]["title"],
        "gemini-4-hydrogen-high",
    );
    assert!(!runtime.to_string().contains("fixture-antigravity-"));

    let reply = reconfigure(&mut fixture, 202, init, Some(0)).await;
    assert_eq!(reply["result"]["revision"], 1, "{reply}");
    let reconfigured = super::runtime(&mut fixture, 203).await;
    assert_ready_antigravity_target(
        &reconfigured["orchestratorTarget"],
        "gemini-4-hydrogen-high",
    );
    assert_ready_antigravity_target(&reconfigured["advisorTarget"], "gemini-4-hydrogen-high");
    assert_ready_antigravity_target(
        &reconfigured["modelTierTargets"]["medium"],
        "gemini-4-hydrogen-high",
    );

    let mut invalid = main.init("fixture-main");
    invalid["orchestratorTarget"] = antigravity_target("native-image-only");
    let reply = reconfigure(&mut fixture, 204, invalid, Some(1)).await;
    assert_eq!(reply["result"]["revision"], 2, "{reply}");
    let rejected = super::runtime(&mut fixture, 205).await;
    assert_eq!(
        rejected["orchestratorTarget"]["type"], "unavailable",
        "{rejected}"
    );
    assert_eq!(
        rejected["orchestratorTarget"]["code"], "MODEL_TARGET_UNAVAILABLE",
        "{rejected}"
    );
    fixture.close().await;
}
