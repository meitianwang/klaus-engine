//! Defect: Auxiliary/orchestration target resolution maintained separate hardcoded source lists and dead `HOST_TARGET_REQUIRED` branches.
//! Root cause: `resolve_tier_target`, `validate_model_source`, CLI `--model` parsing, and `text_completion` drifted instead of sharing `is_supported_model_source`.
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

#[tokio::test]
async fn antigravity_cold_account_reconfigure_resolves_unbundled_and_placeholder_models() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let server_task = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let mut received = Vec::new();
                let mut buffer = [0u8; 4096];
                loop {
                    let Ok(count) = socket.read(&mut buffer).await else {
                        return;
                    };
                    if count == 0 || received.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                        break;
                    }
                    received.extend_from_slice(&buffer[..count]);
                    if received.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                        break;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(40)).await;
                let body = json!({
                    "defaultAgentModelId": "fixture-live-only-high",
                    "models": {
                        "fixture-live-only-high": {
                            "displayName": "Fixture Live Only (High)",
                            "supportsImages": true,
                            "supportsThinking": true,
                            "thinkingLevel": 3,
                            "maxTokens": 1048576,
                            "maxOutputTokens": 65536
                        },
                        "claude-sonnet-4-6": {"displayName": "Claude Sonnet 4.6"},
                        "claude-opus-5-5-disabled": {"displayName": "Unavailable placeholder"}
                    }
                })
                .to_string();
                let _ = socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await;
                let _ = socket.shutdown().await;
            });
        }
    });

    let previous_api = std::env::var_os("KLAUS_ANTIGRAVITY_API_URL");
    let previous_direct = std::env::var_os("KLAUS_ANTIGRAVITY_ALLOW_DIRECT");
    unsafe {
        std::env::set_var("KLAUS_ANTIGRAVITY_API_URL", &origin);
        std::env::set_var("KLAUS_ANTIGRAVITY_ALLOW_DIRECT", "1");
    }

    let main = LoopbackProvider::start().await;
    let store: Arc<dyn klaus_agent_core::credential_store::CredentialStore> =
        Arc::new(klaus_agent_core::mcp::credential_store::InMemoryCredentialStore::new());
    let mut fixture = Fixture::with_credentials(false, None, Some(store)).await;
    let _ = main;
    std::fs::write(
        fixture.root.path().join("config.toml"),
        "# Isolated fixture\n",
    )
    .unwrap();
    crate::app_server::antigravity::set_config_root(fixture.root.path());
    crate::app_server::antigravity::auth::adopt(crate::app_server::antigravity::auth::Credential {
        access_token: "cold-account-access".into(),
        refresh_token: Some("cold-account-refresh".into()),
        expires_at: Some(32_503_680_000_000i64),
        account: Some("cold@example.invalid".into()),
        auth_method: "consumer".into(),
        project: Some("cold-project".into()),
        client_id: None,
        client_secret: None,
    })
    .await
    .unwrap();
    assert!(
        !fixture.root.path().join("antigravity-models.json").exists(),
        "test requires a cold account with no disk catalog snapshot"
    );

    let fenced = request(
        &mut fixture.client,
        210,
        "session/beginModelUpdate",
        json!({
            "sessionId": "side",
            "changeId": "chg-cold-hydrogen",
            "expectedRevision": 0
        }),
    )
    .await;
    assert_eq!(fenced["result"]["revision"], 0, "{fenced}");

    let reply = request(
        &mut fixture.client,
        211,
        "session/reconfigureModel",
        json!({
            "sessionId": "side",
            "changeId": "chg-cold-hydrogen",
            "expectedRevision": 0,
            "init": {
                "modelSelection": "antigravity:claude-opus-5-5-disabled",
                "orchestratorTarget": antigravity_target("claude-sonnet-4-6-disabled")
            }
        }),
    )
    .await;
    assert_eq!(reply["result"]["revision"], 1, "{reply}");
    let runtime = super::runtime(&mut fixture, 212).await;
    assert_eq!(runtime["model"], "fixture-live-only-high", "{runtime}");
    assert!(runtime["pendingChangeId"].is_null(), "{runtime}");
    assert!(runtime["modelUpdateError"].is_null(), "{runtime}");
    assert_ready_antigravity_target(&runtime["orchestratorTarget"], "claude-sonnet-4-6");

    for (id, selection, expected) in [
        (213, "fixture-live-only-high", "fixture-live-only-high"),
        (214, "gemini-4-neon-disabled", "fixture-live-only-high"),
        (215, "gemini-4-argon-disabled", "fixture-live-only-high"),
        (216, "claude-sonnet-4-6-disabled", "claude-sonnet-4-6"),
        (217, "high", "fixture-live-only-high"),
    ] {
        let reply = request(
            &mut fixture.client,
            id,
            "model/selection/resolve",
            json!({
                "source": "antigravity",
                "selection": selection,
                "modelTierSelections": {"high": "antigravity:claude-opus-5-5-disabled"}
            }),
        )
        .await;
        assert_eq!(
            reply["result"]["selection"],
            format!("antigravity:{expected}"),
            "{reply}"
        );
    }

    fixture.close().await;
    crate::app_server::antigravity::auth::logout()
        .await
        .unwrap();
    server_task.abort();
    unsafe {
        match previous_api {
            Some(value) => std::env::set_var("KLAUS_ANTIGRAVITY_API_URL", value),
            None => std::env::remove_var("KLAUS_ANTIGRAVITY_API_URL"),
        }
        match previous_direct {
            Some(value) => std::env::set_var("KLAUS_ANTIGRAVITY_ALLOW_DIRECT", value),
            None => std::env::remove_var("KLAUS_ANTIGRAVITY_ALLOW_DIRECT"),
        }
    }
}
