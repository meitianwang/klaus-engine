//! Defect: session publication prepared every ready auxiliary inference target
//! (model tiers, advisor, feature slots) and propagated a preparation failure,
//! so a revoked subscription login on an auxiliary source blocked conversation
//! startup (app-server error -32000 "OAuth token refresh failed") even when the
//! session's own model was healthy and needed no OAuth at all.
//! Fixed by degrading a slot whose preparation fails to Unavailable: tiers and
//! the advisor report at use time, feature slots follow the main model, and
//! only the conversation's own model gates startup.

use super::*;
use klaus_agent_core::credential_store::{model_credential_key, CredentialStore};
use klaus_agent_core::mcp::credential_store::InMemoryCredentialStore;

/// A stored Codex login whose access token is expired and whose record carries
/// no refresh token: every refresh attempt fails deterministically without
/// touching the network.
fn expired_codex_credentials() -> Value {
    json!({
        "defaultAccountId": "dead-account",
        "accounts": {
            "dead-account": {
                "accountId": "dead-account",
                "email": "dead@example.com",
                "accessToken": "expired-fixture-secret",
                "accessTokenExpiresAt": 1_700_000_000_000i64
            }
        }
    })
}

fn model_config() -> &'static str {
    r#"
[modelProviders.preferred]
provider = "openai"
model = "preferred-model"
default = true
"#
}

#[tokio::test]
async fn revoked_auxiliary_login_degrades_the_tier_and_session_prepare_still_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    std::fs::write(&config, model_config()).unwrap();
    std::fs::write(
        dir.path().join("openai-subscription-oauth.json"),
        expired_codex_credentials().to_string(),
    )
    .unwrap();
    let store: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
    store
        .set(&model_credential_key("preferred"), "fixture-key")
        .await
        .unwrap();
    let mut client = TestClient::start_full_with_credentials(
        make_create_session(&[]),
        Some(config),
        Some(store),
    );
    client.initialize().await;
    client
        .send(
            json!({"id": 2, "method": "session/start", "params": {"init": {
                "modelSelection": "custom:preferred",
                "modelTierTargets": {
                    "high": {"type": "source", "source": "codex", "modelId": "gpt-fixture"}
                }
            }}}),
        )
        .await;
    let (started, _) = client.response_for(2).await;
    assert!(
        started.get("error").is_none(),
        "session start must succeed: {started}"
    );
    let session_id = started["result"]["sessionId"]
        .as_str()
        .expect("session id")
        .to_string();
    client
        .send(json!({"id": 3, "method": "session/prepare", "params": {"sessionId": session_id}}))
        .await;
    let (prepared, _) = client.response_for(3).await;
    assert!(
        prepared.get("error").is_none(),
        "auxiliary preparation failure must degrade, not block startup: {prepared}"
    );
    assert_eq!(
        prepared["result"]["ready"],
        json!(true),
        "prepare must report ready on the conversation's own model: {prepared}"
    );
}
