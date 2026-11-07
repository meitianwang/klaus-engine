//! Defect: a declared subscription alias was unavailable before any advisor request.
//! Root cause: target validation compared the alias with the resolved provider ID.
//! Work item: 20261018 subscription advisor alias identity.
//! Mounted by the app-server aggregate to exercise the public session facade.

use super::*;

async fn subscription_fixture() -> Fixture {
    let store: Arc<dyn klaus_agent_core::credential_store::CredentialStore> =
        Arc::new(klaus_agent_core::mcp::credential_store::InMemoryCredentialStore::new());
    let fixture = Fixture::with_credentials(false, None, Some(store)).await;
    std::fs::write(
        fixture.root.path().join("config.toml"),
        "# Isolated fixture\n",
    )
    .unwrap();
    std::fs::write(
        fixture.root.path().join(".credentials.json"),
        json!({
            "claudeAiOauth": {
                "accessToken": "fixture-subscription-access",
                "refreshToken": "fixture-subscription-refresh",
                "expiresAt": 32_503_680_000_000i64
            }
        })
        .to_string(),
    )
    .unwrap();
    fixture
}

fn subscription_target(model_id: &str) -> Value {
    json!({"type":"source","source":"subscription","modelId":model_id})
}

fn assert_opus_target(target: &Value) {
    assert_eq!(target["type"], "ready", "{target}");
    assert_eq!(target["source"], "subscription", "{target}");
    assert_eq!(target["modelId"], "opus", "{target}");
    assert_eq!(target["model"], "claude-opus-5-5", "{target}");
    for key in ["provider", "auth", "pricing"] {
        assert!(target.get(key).is_none(), "{target}");
    }
}

#[test]
fn only_declared_source_aliases_may_change_target_model_identity() {
    use crate::app_server::model_sources::{claude_lineup, model_target_matches_provider};
    let lineup = claude_lineup();
    for model in lineup["models"].as_array().unwrap() {
        let alias = model["alias"].as_str().unwrap();
        let provider_model = model["model"].as_str().unwrap();
        assert!(model_target_matches_provider(
            "subscription",
            alias,
            provider_model
        ));
        assert!(model_target_matches_provider(
            "subscription",
            provider_model,
            provider_model
        ));
        assert!(!model_target_matches_provider(
            "subscription",
            alias,
            "different-provider-model"
        ));
        for source in ["codex", "qoder", "cursor", "kimi"] {
            assert!(!model_target_matches_provider(
                source,
                alias,
                provider_model
            ));
            assert!(model_target_matches_provider(
                source,
                provider_model,
                provider_model
            ));
        }
    }
    for invalid in [
        "auto", "high", "medium", "low", "inherit", "opux", "opus[1m]",
    ] {
        assert!(!model_target_matches_provider(
            "subscription",
            invalid,
            "claude-opus-5-5"
        ));
    }
}

#[tokio::test]
async fn subscription_advisor_alias_is_ready_on_session_start() {
    let main = LoopbackProvider::start().await;
    let mut fixture = subscription_fixture().await;
    let mut init = main.init("fixture-main");
    init["advisorTarget"] = subscription_target("opus");
    init["modelTierTargets"] = json!({"high":subscription_target("opus")});
    start_with_init(&mut fixture, 100, "subscription-alias-start", init).await;
    let runtime = named_runtime(&mut fixture.client, 102, "subscription-alias-start").await;
    assert_opus_target(&runtime["advisorTarget"]);
    assert_opus_target(&runtime["modelTierTargets"]["high"]);
    assert_eq!(runtime["model"], "fixture-main", "{runtime}");
    assert!(!runtime.to_string().contains("fixture-subscription-"));
    assert!(main.requests.lock().unwrap().is_empty());
    fixture.close().await;
}

#[tokio::test]
async fn subscription_advisor_alias_is_ready_after_live_reconfigure() {
    let main = LoopbackProvider::start().await;
    let mut fixture = subscription_fixture().await;
    let mut init = main.init("fixture-main");
    init["advisorTarget"] = subscription_target("opus");
    init["modelTierTargets"] = json!({"high":subscription_target("opus")});
    let reply = reconfigure(&mut fixture, 100, init, Some(0)).await;
    assert_eq!(reply["result"]["revision"], 1, "{reply}");
    let runtime = runtime(&mut fixture, 101).await;
    assert_opus_target(&runtime["advisorTarget"]);
    assert_opus_target(&runtime["modelTierTargets"]["high"]);
    assert_eq!(runtime["model"], "fixture-main", "{runtime}");
    assert!(!runtime.to_string().contains("fixture-subscription-"));
    assert!(main.requests.lock().unwrap().is_empty());
    fixture.close().await;
}

#[tokio::test]
async fn subscription_advisor_default_and_invalid_sources_stay_unavailable() {
    let main = LoopbackProvider::start().await;
    let mut fixture = subscription_fixture().await;
    for (index, target) in [
        subscription_target("auto"),
        subscription_target("high"),
        subscription_target(""),
        json!({"type":"source","source":"local","modelId":"opus"}),
    ]
    .into_iter()
    .enumerate()
    {
        let mut init = main.init("fixture-main");
        init["advisorTarget"] = target.clone();
        let id = 100 + index as u64 * 2;
        let reply = reconfigure(&mut fixture, id, init, Some(index as u64)).await;
        assert_eq!(reply["result"]["revision"], index as u64 + 1, "{reply}");
        let runtime = runtime(&mut fixture, id + 1).await;
        assert_eq!(runtime["advisorTarget"]["type"], "unavailable", "{runtime}");
        assert_eq!(runtime["advisorTarget"]["source"], target["source"]);
        assert_eq!(runtime["advisorTarget"]["modelId"], target["modelId"]);
    }
    assert!(main.requests.lock().unwrap().is_empty());
    fixture.close().await;
}
