//! The advisor is an independent optional target in the existing session facade.
//! Startup and complete model publications must preserve unavailable advisor state
//! without preventing the main provider from serving a turn.

use super::*;

#[path = "../../core/tests/regressions/r20261018_claude_advisor_alias.rs"]
mod claude_advisor_alias;

#[path = "../../core/tests/regressions/r20261113_antigravity_model_targets.rs"]
mod antigravity_model_targets;

fn resolved_advisor(provider: &LoopbackProvider, model_id: &str) -> Value {
    json!({
        "type":"resolved","source":"codex","modelId":model_id,
        "provider":{"type":"openai","api":"chat","model":"advisor-model","baseUrl":provider.url},
        "auth":{"type":"api_key","key":"fixture-advisor-secret"},
        "capabilities":{"tools":false,"contextWindow":12000,"maxOutputTokens":512},
        "pricing":[]
    })
}

fn unavailable_advisors() -> Vec<Value> {
    let invalid_provider = json!({
        "type":"resolved","source":"custom","modelId":"invalid-provider",
        "provider":{"type":"unsupported-provider","model":"advisor-model"},
        "auth":{"type":"api_key","key":"fixture-advisor-secret"},
        "capabilities":{"contextWindow":12000,"maxOutputTokens":512}
    });
    let invalid_auth = {
        let mut value = invalid_provider.clone();
        value["provider"] = json!({"type":"openai","api":"chat","model":"advisor-model"});
        value["auth"] = json!({"type":"api_key_stored","credentialId":"missing-advisor-key"});
        value
    };
    vec![
        json!({"type":"unavailable","source":"custom","modelId":"removed-advisor",
            "code":"MODEL_REMOVED","message":"The advisor model was removed."}),
        json!({"type":"source","source":"custom","modelId":"missing-advisor"}),
        json!({"type":"source","source":"local","modelId":"retired-local-advisor"}),
        json!({"type":"source","source":"custom","modelId":""}),
        json!({"type":"source","source":"custom","modelId":"high"}),
        invalid_provider,
        invalid_auth,
    ]
}

async fn named_runtime(client: &mut TestClient, id: u64, session_id: &str) -> Value {
    let reply = request(
        client,
        id,
        "session/modelRuntime",
        json!({"sessionId":session_id}),
    )
    .await;
    assert!(reply.get("error").is_none(), "{reply}");
    reply["result"].clone()
}

async fn start_with_init(fixture: &mut Fixture, id: u64, session_id: &str, mut init: Value) {
    // Leave room for the prompt and tool catalog so startup checks exercise
    // main inference without triggering compaction.
    init["config"]["contextWindow"] = json!(131000);
    init["threadId"] = json!(session_id);
    init["cwd"] = json!(fixture.root.path());
    let started = request(
        &mut fixture.client,
        id,
        "session/start",
        json!({"init":init}),
    )
    .await;
    assert_eq!(started["result"]["sessionId"], session_id, "{started}");
    let bound = request(
        &mut fixture.client,
        id + 1,
        "session/runtimeCapabilities",
        json!({"sessionId":session_id}),
    )
    .await;
    assert!(bound.get("error").is_none(), "{bound}");
}

fn assert_non_secret(runtime: &Value) {
    let text = runtime.to_string();
    for secret in [
        "fixture-advisor-secret",
        "fixture-only-key",
        "http://127.0.0.1",
    ] {
        assert!(!text.contains(secret), "{runtime}");
    }
    let advisor = &runtime["advisorTarget"];
    for key in ["provider", "auth", "pricing"] {
        assert!(advisor.get(key).is_none(), "{runtime}");
    }
}

#[tokio::test]
async fn start_off_and_unavailable_advisors_do_not_block_main_provider() {
    let main = LoopbackProvider::start().await;
    let mut fixture = Fixture::with_provider_transport().await;
    let targets = std::iter::once(None)
        .chain(std::iter::once(Some(Value::Null)))
        .chain(unavailable_advisors().into_iter().map(Some));
    for (index, target) in targets.enumerate() {
        let session_id = format!("advisor-start-{index}");
        let id = 100 + index as u64 * 10;
        let mut init = main.init("main-model");
        // Keep routing assertions independent of prompt catalog compaction.
        init["config"]["contextWindow"] = json!(131000);
        init["modelTierTargets"] = json!({});
        if let Some(target) = target {
            init["advisorTarget"] = target;
        }
        start_with_init(&mut fixture, id, &session_id, init).await;
        let runtime = named_runtime(&mut fixture.client, id + 2, &session_id).await;
        assert_eq!(runtime["model"], "main-model", "{runtime}");
        if index < 2 {
            assert!(runtime.get("advisorTarget").unwrap().is_null(), "{runtime}");
        } else {
            assert_eq!(runtime["advisorTarget"]["type"], "unavailable", "{runtime}");
        }
        assert_non_secret(&runtime);
        let before = main.requests.lock().unwrap().len();
        run_turn(&mut fixture.client, id + 3, &session_id, "Answer directly.").await;
        assert!(summary_requests(&main).is_empty());
        assert_eq!(main.requests.lock().unwrap().len(), before + 1);
    }
    fixture.close().await;
}

#[tokio::test]
async fn start_resolved_cross_source_advisor_is_independent_of_tier_targets() {
    let main = LoopbackProvider::start().await;
    let advisor = LoopbackProvider::start().await;
    let tier = LoopbackProvider::start().await;
    let mut fixture = Fixture::with_provider_transport().await;
    let mut init = main.init("main-model");
    // Keep routing assertions independent of prompt catalog compaction.
    init["config"]["contextWindow"] = json!(131000);
    init["modelSource"] = json!({"source":"custom","modelId":"main-entry"});
    init["advisorTarget"] = resolved_advisor(&advisor, "codex-advisor");
    init["modelTierTargets"] = json!({
        "high":tier_target(&tier,"custom","high-entry","high-key",32000,1024,1.0)
    });
    start_with_init(&mut fixture, 100, "advisor-cross-source", init).await;
    let runtime = named_runtime(&mut fixture.client, 102, "advisor-cross-source").await;
    assert_eq!(runtime["model"], "main-model");
    assert_eq!(runtime["advisorTarget"]["type"], "ready", "{runtime}");
    assert_eq!(runtime["advisorTarget"]["source"], "codex");
    assert_eq!(runtime["advisorTarget"]["modelId"], "codex-advisor");
    assert_eq!(runtime["advisorTarget"]["model"], "advisor-model");
    assert_eq!(runtime["modelTierTargets"]["high"]["modelId"], "high-entry");
    assert_non_secret(&runtime);
    run_turn(
        &mut fixture.client,
        103,
        "advisor-cross-source",
        "Answer directly.",
    )
    .await;
    assert!(summary_requests(&main).is_empty());
    assert_eq!(main.requests.lock().unwrap().len(), 1);
    assert!(advisor.requests.lock().unwrap().is_empty());
    assert!(tier.requests.lock().unwrap().is_empty());
    fixture.close().await;
}

#[tokio::test]
async fn full_reconfigure_omission_or_null_disables_advisor_but_tier_only_preserves_it() {
    let main = LoopbackProvider::start().await;
    let advisor = LoopbackProvider::start().await;
    let mut fixture = Fixture::new(false, None).await;
    let mut enabled = main.init("main-model");
    enabled["advisorTarget"] = resolved_advisor(&advisor, "local-advisor");
    assert_eq!(
        reconfigure(&mut fixture, 20, enabled.clone(), Some(0)).await["result"]["revision"],
        1
    );
    let before = runtime(&mut fixture, 21).await;
    assert_eq!(before["advisorTarget"]["type"], "ready", "{before}");
    assert_eq!(
        reconfigure(
            &mut fixture,
            22,
            json!({"modelTierTargets":{"high":null}}),
            Some(1)
        )
        .await["result"]["revision"],
        2
    );
    assert_eq!(
        runtime(&mut fixture, 23).await["advisorTarget"],
        before["advisorTarget"]
    );
    let mut off = main.init("next-main-model");
    off["advisorTarget"] = Value::Null;
    assert_eq!(
        reconfigure(&mut fixture, 24, off, Some(2)).await["result"]["revision"],
        3
    );
    assert!(runtime(&mut fixture, 25).await["advisorTarget"].is_null());
    assert_eq!(
        reconfigure(&mut fixture, 26, enabled, Some(3)).await["result"]["revision"],
        4
    );
    assert_eq!(
        runtime(&mut fixture, 27).await["advisorTarget"]["type"],
        "ready"
    );
    assert_eq!(
        reconfigure(&mut fixture, 28, main.init("last-main-model"), Some(4)).await["result"]
            ["revision"],
        5
    );
    let runtime = runtime(&mut fixture, 29).await;
    assert_eq!(runtime["model"], "last-main-model");
    assert!(runtime.get("advisorTarget").unwrap().is_null(), "{runtime}");
    assert_non_secret(&runtime);
    run_turn(&mut fixture.client, 30, "side", "Answer directly.").await;
    assert_eq!(main.requests.lock().unwrap()[0]["model"], "last-main-model");
    assert!(advisor.requests.lock().unwrap().is_empty());
    fixture.close().await;
}

#[tokio::test]
async fn reconfigure_unavailable_advisors_preserves_main_provider_availability() {
    let main = LoopbackProvider::start().await;
    let mut fixture = Fixture::new(false, None).await;
    for (index, target) in unavailable_advisors().into_iter().enumerate() {
        let id = 100 + index as u64 * 10;
        let mut init = main.init("main-model");
        let source = target["source"].clone();
        let model_id = target["modelId"].clone();
        init["advisorTarget"] = target;
        let updated = reconfigure(&mut fixture, id, init, Some(index as u64)).await;
        assert_eq!(updated["result"]["revision"], index as u64 + 1, "{updated}");
        let runtime = runtime(&mut fixture, id + 1).await;
        assert_eq!(runtime["model"], "main-model");
        assert_eq!(runtime["advisorTarget"]["type"], "unavailable", "{runtime}");
        assert_eq!(runtime["advisorTarget"]["source"], source);
        assert_eq!(runtime["advisorTarget"]["modelId"], model_id);
        assert_non_secret(&runtime);
        run_turn(&mut fixture.client, id + 2, "side", "Answer directly.").await;
        assert_eq!(main.requests.lock().unwrap().len(), index + 1);
    }
    fixture.close().await;
}

#[tokio::test]
async fn advisor_target_participates_in_named_publication_identity() {
    let main = LoopbackProvider::start().await;
    let advisor = LoopbackProvider::start().await;
    let mut fixture = Fixture::new(false, None).await;
    let mut init = main.init("main-model");
    init["advisorTarget"] = resolved_advisor(&advisor, "advisor-a");
    assert_eq!(
        begin_named(&mut fixture.client, 20, "side", "advisor-change", 0).await["result"]
            ["revision"],
        0
    );
    assert_eq!(
        publish_named(
            &mut fixture.client,
            21,
            "side",
            "advisor-change",
            0,
            init.clone()
        )
        .await["result"]["revision"],
        1
    );
    let duplicate = publish_named(
        &mut fixture.client,
        22,
        "side",
        "advisor-change",
        0,
        init.clone(),
    )
    .await;
    assert_eq!(duplicate["result"]["revision"], 1, "{duplicate}");
    init["advisorTarget"] = resolved_advisor(&advisor, "advisor-b");
    let conflict = publish_named(&mut fixture.client, 23, "side", "advisor-change", 0, init).await;
    assert!(conflict.get("error").is_some(), "{conflict}");
    let runtime = runtime(&mut fixture, 24).await;
    assert_eq!(runtime["revision"], 1);
    assert_eq!(runtime["advisorTarget"]["modelId"], "advisor-a");
    assert_non_secret(&runtime);
    fixture.close().await;
}
