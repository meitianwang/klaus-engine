//! Defect: an explicit child source was sent as a model name to the parent provider.
//! Root cause: child selection resolved only tier tokens into independent providers.
//! Work item: 20261019 source-qualified sub-agent provider binding.
//! Mounted by the app-server aggregate to exercise named methods and the real task tool.

use super::*;
use klaus_agent_core::credential_store::model_credential_key;
use klaus_agent_core::engine::subagent_lifecycle::SubagentStatus;

async fn delegate(fixture: &mut Fixture, main: &LoopbackProvider, selection: &str) {
    delegate_with_options(fixture, main, selection, json!({})).await;
}

async fn delegate_with_options(
    fixture: &mut Fixture,
    main: &LoopbackProvider,
    selection: &str,
    options: Value,
) {
    delegate_on(fixture, main, selection, options, "side").await;
}

async fn delegate_on(
    fixture: &mut Fixture,
    main: &LoopbackProvider,
    selection: &str,
    options: Value,
    session_id: &str,
) {
    *main.delegation.lock().unwrap() = Some(json!({
        "description":"Source child", "prompt":"Complete the child request",
        "agentType":"Explore", "model":selection, "run_in_background":false
    }));
    fixture
        .client
        .send(json!({"id":21,"method":"turn/start","params":{
            "sessionId":session_id,"input":"delegate via task","options":options
        }}))
        .await;
    let (reply, seen) = fixture.client.response_for(21).await;
    assert!(reply["result"]["turnId"].is_string(), "{reply}");
    let mut messages = std::collections::VecDeque::from(seen);
    loop {
        let message = match messages.pop_front() {
            Some(message) => message,
            None => fixture.client.next_message().await,
        };
        if message["method"] == "approval/request" {
            assert_eq!(
                message["params"]["request"]["toolName"], "task",
                "{message}"
            );
            assert_eq!(
                message["params"]["request"]["input"]["model"], selection,
                "{message}"
            );
            fixture
                .client
                .send(json!({"id":message["id"],"result":{"response":{"decision":"allow"}}}))
                .await;
        }
        if message["method"] == "turn/completed" && message["params"]["sessionId"] == session_id {
            assert!(message["params"].get("error").is_none(), "{message}");
            break;
        }
    }
}

#[tokio::test]
async fn a_cold_child_resume_restores_its_source_and_stored_credential() {
    cold_child_resume(None).await;
}

#[tokio::test]
async fn a_cold_raw_model_resume_keeps_the_original_provider_binding() {
    for model in ["fixture-low", "declared-child-model"] {
        cold_child_resume(Some(model)).await;
    }
}

async fn cold_child_resume(raw_model: Option<&str>) {
    let main = LoopbackProvider::start().await;
    let child = LoopbackProvider::start().await;
    let same_provider = raw_model.is_some();
    let child_provider = if same_provider { &main } else { &child };
    let child_model = raw_model.unwrap_or("same-name");
    let child_selection = raw_model.unwrap_or("custom:child");
    let child_key = if same_provider {
        "fixture-only-key"
    } else {
        "cold-child-key"
    };
    let child_requests = || {
        child_provider
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request["model"] == child_model)
            .cloned()
            .collect::<Vec<_>>()
    };
    let store: Arc<dyn CredentialStore> =
        Arc::new(klaus_agent_core::mcp::credential_store::InMemoryCredentialStore::new());
    store
        .set(&model_credential_key("child"), "cold-child-key")
        .await
        .unwrap();
    let mut fixture = Fixture::with_provider_transport_and_credentials(Some(store)).await;
    std::fs::write(
        fixture.root.path().join("config.toml"),
        format!(
            r#"
[modelProviders.child]
provider = "anthropic"
model = "same-name"
baseUrl = "{}"
maxContextTokens = 64000
[modelProviders.child.capabilities]
contextWindow = 64000
maxOutputTokens = 733
"#,
            child.url
        ),
    )
    .unwrap();
    let mut init = main.init("same-name");
    init["threadId"] = json!("provider-parent");
    init["cwd"] = json!(fixture.root.path());
    init["modelSource"] = json!({"source":"codex","modelId":"same-name"});
    // Keep this provider-recovery case independent of opaque tool authority.
    init["agents"] = json!([{"name":"Explore","description":"Source-bound reasoning","tools":[]}]);
    if raw_model == Some("declared-child-model") {
        init["agents"][0]["model"] = json!("declared-child-model");
    }
    let started = request(
        &mut fixture.client,
        30,
        "session/start",
        json!({"init":init}),
    )
    .await;
    assert_eq!(
        started["result"]["sessionId"], "provider-parent",
        "{started}"
    );
    request(
        &mut fixture.client,
        31,
        "session/runtimeCapabilities",
        json!({"sessionId":"provider-parent"}),
    )
    .await;
    let mut inference = init.clone();
    inference.as_object_mut().unwrap().remove("threadId");
    inference.as_object_mut().unwrap().remove("cwd");
    inference.as_object_mut().unwrap().remove("agents");
    let updated = request(
        &mut fixture.client,
        32,
        "session/reconfigureModel",
        json!({"sessionId":"provider-parent","init":inference}),
    )
    .await;
    assert!(updated.get("error").is_none(), "{updated}");
    delegate_on(
        &mut fixture,
        &main,
        child_selection,
        json!({}),
        "provider-parent",
    )
    .await;
    let before = request(
        &mut fixture.client,
        33,
        "session/subagentLifecycle",
        json!({"sessionId":"provider-parent"}),
    )
    .await;
    let agent_id = before["result"]["subagentLifecycle"]["agents"][0]["agentId"]
        .as_str()
        .unwrap_or_else(|| panic!("child lifecycle identity: {before}"))
        .to_string();
    assert_eq!(
        before["result"]["subagentLifecycle"]["agents"][0]["status"], "completed",
        "{before}"
    );
    assert_eq!(child_requests().len(), 1);
    let closed = request(
        &mut fixture.client,
        34,
        "session/close",
        json!({"sessionId":"provider-parent"}),
    )
    .await;
    assert!(closed.get("error").is_none(), "{closed}");
    let restarted = request(
        &mut fixture.client,
        35,
        "session/start",
        json!({"init":init,"resume":"provider-parent"}),
    )
    .await;
    assert_eq!(
        restarted["result"]["sessionId"], "provider-parent",
        "{restarted}"
    );
    request(
        &mut fixture.client,
        36,
        "session/runtimeCapabilities",
        json!({"sessionId":"provider-parent"}),
    )
    .await;
    let resumed=request(&mut fixture.client,37,"session/sendSubagentMessage",json!({"sessionId":"provider-parent","agentId":agent_id,"message":"Continue the source-bound child request","userInitiated":true})).await;
    assert!(resumed.get("error").is_none(), "{resumed}");
    assert_eq!(
        resumed["result"]["subagentMessage"]["ok"], true,
        "{resumed}"
    );
    tokio::time::timeout(REPLY_TIMEOUT, async {
        loop {
            let entered = child_provider.entered.notified();
            if child_requests().len() >= 2 {
                break;
            }
            entered.await;
        }
    })
    .await
    .expect("the restored child reaches its own provider");
    let requests = child_requests();
    assert_eq!(requests.len(), 2, "{requests:?}");
    assert!(requests
        .iter()
        .all(|request| request["model"] == child_model && request["_wireAuth"] == child_key));
    let closed = request(
        &mut fixture.client,
        38,
        "session/close",
        json!({"sessionId":"provider-parent"}),
    )
    .await;
    assert!(closed.get("error").is_none(), "{closed}");
    fixture.close().await;
}

async fn configured_main(fixture: &mut Fixture, main: &LoopbackProvider) {
    let mut init = main.init("same-name");
    init["modelSource"] = json!({"source":"codex","modelId":"same-name"});
    assert_eq!(
        reconfigure(fixture, 20, init, Some(0)).await["result"]["revision"],
        1
    );
}

#[tokio::test]
async fn subscription_child_alias_and_canonical_id_reuse_the_captured_source() {
    for selection in ["subscription:opus", "subscription:claude-opus-5-5"] {
        let main = LoopbackProvider::start().await;
        let child = LoopbackProvider::start().await;
        let mut fixture = Fixture::with_provider_transport().await;
        let mut init = main.init("claude-opus-5-5");
        init["modelSource"] = json!({"source":"codex","modelId":"claude-opus-5-5"});
        let mut advisor = tier_target(&child, "subscription", "opus", "child-key", 64000, 700, 2.0);
        advisor["provider"]["model"] = json!("claude-opus-5-5");
        advisor["pricing"][0]["model"] = json!("claude-opus-5-5");
        init["advisorTarget"] = advisor;
        assert_eq!(
            reconfigure(&mut fixture, 20, init, Some(0)).await["result"]["revision"],
            1
        );
        delegate(&mut fixture, &main, selection).await;
        let requests = child.requests.lock().unwrap().clone();
        assert_eq!(requests.len(), 1, "{requests:?}");
        assert_eq!(requests[0]["model"], "claude-opus-5-5");
        assert_eq!(requests[0]["_wireAuth"], "child-key");
        assert_eq!(requests[0]["max_tokens"], 700);
        let snapshot = fixture.source.subagent_lifecycle_snapshot();
        assert_eq!(snapshot.agents.len(), 1, "{snapshot:?}");
        assert_eq!(snapshot.agents[0].status, SubagentStatus::Completed);
        assert_eq!(
            snapshot.agents[0].model_source.as_ref(),
            Some(&"subscription".into())
        );
        let main_context = fixture.source.context_snapshot();
        assert_eq!(main_context.context_window, 29000);
        assert_eq!(main_context.output_reserve_tokens, 1450);
        assert_eq!(main_context.compact_threshold_tokens, 26100);
        assert!(main
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|request| request["_wireAuth"] == "fixture-only-key"));
        fixture.close().await;
    }
}

#[tokio::test]
async fn an_explicit_configured_child_gets_its_own_provider_and_stored_credential() {
    let main = LoopbackProvider::start().await;
    let child = LoopbackProvider::start().await;
    let store: Arc<dyn CredentialStore> =
        Arc::new(klaus_agent_core::mcp::credential_store::InMemoryCredentialStore::new());
    store
        .set(&model_credential_key("child"), "child-stored-key")
        .await
        .unwrap();
    let mut fixture = Fixture::with_credentials(false, None, Some(store)).await;
    std::fs::write(
        fixture.root.path().join("config.toml"),
        format!(
            r#"
[modelProviders.child]
provider = "anthropic"
model = "same-name"
baseUrl = "{}"
maxContextTokens = 64000
[modelProviders.child.capabilities]
contextWindow = 64000
maxOutputTokens = 733
"#,
            child.url
        ),
    )
    .unwrap();
    configured_main(&mut fixture, &main).await;
    delegate(&mut fixture, &main, "custom:child").await;
    let requests = child.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 1, "{requests:?}");
    assert_eq!(requests[0]["model"], "same-name");
    assert_eq!(requests[0]["_wireAuth"], "child-stored-key");
    assert_eq!(requests[0]["max_tokens"], 733);
    let snapshot = fixture.source.subagent_lifecycle_snapshot();
    assert_eq!(
        snapshot.agents[0].status,
        SubagentStatus::Completed,
        "{snapshot:?}"
    );
    assert_eq!(
        snapshot.agents[0].model_source.as_ref(),
        Some(&"custom".into())
    );
    let main_context = fixture.source.context_snapshot();
    assert_eq!(main_context.context_window, 29000);
    assert_eq!(main_context.output_reserve_tokens, 1450);
    assert_eq!(main_context.compact_threshold_tokens, 26100);
    fixture.close().await;
}

#[tokio::test]
async fn an_explicit_unavailable_source_never_falls_back_to_the_parent_provider() {
    for selection in [
        "subscription:opus",
        "unknown:opus",
        "subscription:auto",
        "codex:foreign-model",
    ] {
        let main = LoopbackProvider::start().await;
        let mut fixture = Fixture::with_provider_transport().await;
        std::fs::write(
            fixture.root.path().join("config.toml"),
            "# Isolated fixture\n",
        )
        .unwrap();
        if selection == "codex:foreign-model" {
            let mut init = main.init("same-name");
            init["modelSource"] = json!({"source":"codex","modelId":"same-name"});
            let mut other = tier_target(
                &main,
                "subscription",
                "other-entry",
                "other-key",
                64000,
                700,
                1.0,
            );
            other["provider"]["model"] = json!("foreign-model");
            init["modelTierTargets"] = json!({"high":other});
            assert_eq!(
                reconfigure(&mut fixture, 20, init, Some(0)).await["result"]["revision"],
                1
            );
        } else {
            configured_main(&mut fixture, &main).await;
        }
        delegate(&mut fixture, &main, selection).await;
        let snapshot = fixture.source.subagent_lifecycle_snapshot();
        assert!(
            snapshot
                .agents
                .iter()
                .all(|agent| agent.status != SubagentStatus::Completed),
            "{snapshot:?}"
        );
        assert!(main
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|request| request["model"] == "same-name"));
        let text = format!("{:?}", fixture.source.history());
        if selection == "unknown:opus" {
            assert!(
                text.contains("not bound to the captured inference provider"),
                "{text}"
            );
        } else {
            assert!(text.contains("Sub-agent model unavailable"), "{text}");
            assert!(
                !text.contains("not bound to the captured inference provider"),
                "{text}"
            );
        }
        fixture.close().await;
    }
}

#[tokio::test]
async fn custom_catalog_ids_do_not_collide_with_another_entrys_transport_model() {
    let main = LoopbackProvider::start().await;
    let child = LoopbackProvider::start().await;
    let store: Arc<dyn CredentialStore> =
        Arc::new(klaus_agent_core::mcp::credential_store::InMemoryCredentialStore::new());
    store
        .set(&model_credential_key("child"), "distinct-child-key")
        .await
        .unwrap();
    let mut fixture = Fixture::with_credentials(false, None, Some(store)).await;
    std::fs::write(
        fixture.root.path().join("config.toml"),
        format!(
            r#"
[modelProviders.child]
provider = "anthropic"
model = "same-name"
baseUrl = "{}"
[modelProviders.child.capabilities]
contextWindow = 64000
maxOutputTokens = 733
"#,
            child.url
        ),
    )
    .unwrap();
    let mut init = main.init("child");
    init["modelSource"] = json!({"source":"custom","modelId":"primary"});
    let mut other = tier_target(
        &main,
        "custom",
        "another-entry",
        "wrong-key",
        64000,
        700,
        1.0,
    );
    other["provider"]["model"] = json!("child");
    init["advisorTarget"] = other;
    assert_eq!(
        reconfigure(&mut fixture, 20, init, Some(0)).await["result"]["revision"],
        1
    );
    delegate(&mut fixture, &main, "custom:child").await;
    let requests = child.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 1, "{requests:?}");
    assert_eq!(requests[0]["_wireAuth"], "distinct-child-key");
    assert!(main
        .requests
        .lock()
        .unwrap()
        .iter()
        .all(|request| request["_wireAuth"] == "fixture-only-key"));
    fixture.close().await;
}

#[tokio::test]
async fn a_captured_raw_model_with_a_version_colon_keeps_its_provider() {
    let model = "anthropic.claude-3-5-sonnet-20241022-v2:0";
    let main = LoopbackProvider::start().await;
    let mut fixture = Fixture::with_provider_transport().await;
    assert_eq!(
        reconfigure(&mut fixture, 20, main.init(model), Some(0)).await["result"]["revision"],
        1
    );
    delegate(&mut fixture, &main, model).await;
    let snapshot = fixture.source.subagent_lifecycle_snapshot();
    assert_eq!(
        snapshot.agents[0].status,
        SubagentStatus::Completed,
        "{snapshot:?}"
    );
    assert_eq!(snapshot.agents[0].model.as_ref(), Some(&model.into()));
    let requests = main.requests.lock().unwrap().clone();
    assert!(
        requests.len() >= 3,
        "parent and child inference must reach the captured provider"
    );
    assert!(requests.iter().all(|request| request["model"] == model));
    fixture.close().await;
}

#[tokio::test]
async fn a_parent_model_override_does_not_change_an_explicit_child_model() {
    let main = LoopbackProvider::start().await;
    let mut fixture = Fixture::with_provider_transport().await;
    let mut init = main.init("same-name");
    init["modelSource"] = json!({"source":"codex","modelId":"same-name"});
    // Keep source selection independent of the fixed instruction capacity.
    init["capabilitiesByModel"][0]["capabilities"]["contextWindow"] = json!(29000);
    assert_eq!(
        reconfigure(&mut fixture, 20, init, Some(0)).await["result"]["revision"],
        1
    );
    delegate_with_options(
        &mut fixture,
        &main,
        "codex:same-name",
        json!({"model":"fixture-low"}),
    )
    .await;
    let requests = main.requests.lock().unwrap().clone();
    let models: Vec<_> = requests
        .iter()
        .map(|request| request["model"].as_str().unwrap())
        .collect();
    assert_eq!(
        models.iter().filter(|model| **model == "same-name").count(),
        1,
        "{models:?}"
    );
    assert_eq!(models.first(), Some(&"fixture-low"));
    assert_eq!(models.last(), Some(&"fixture-low"));
    assert!(models
        .iter()
        .all(|model| matches!(*model, "fixture-low" | "same-name")));
    let snapshot = fixture.source.subagent_lifecycle_snapshot();
    assert_eq!(
        snapshot.agents[0].status,
        SubagentStatus::Completed,
        "{snapshot:?}"
    );
    assert_eq!(snapshot.agents[0].model.as_ref(), Some(&"same-name".into()));
    fixture.close().await;
}

struct DroppedPreparation(Arc<AtomicBool>);

impl Drop for DroppedPreparation {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn cancelling_the_parent_drops_a_pending_child_source_preparation() {
    let main = LoopbackProvider::start().await;
    let mut fixture = Fixture::with_provider_transport().await;
    configured_main(&mut fixture, &main).await;
    let entered = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    fixture.source.install_subagent_target_resolver(Arc::new({
        let entered = entered.clone();
        let dropped = dropped.clone();
        move |source, model_id| {
            assert_eq!(source, "custom");
            assert_eq!(model_id, "pending");
            let entered = entered.clone();
            let dropped = dropped.clone();
            Box::pin(async move {
                let _guard = DroppedPreparation(dropped);
                entered.notify_one();
                futures::future::pending().await
            })
        }
    }));
    let source = fixture.source.clone();
    let interrupt = tokio::spawn(async move {
        entered.notified().await;
        source.interrupt();
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        delegate(&mut fixture, &main, "custom:pending"),
    )
    .await
    .unwrap();
    interrupt.await.unwrap();
    assert!(
        dropped.load(Ordering::SeqCst),
        "cancellation must drop pending source admission"
    );
    assert!(fixture
        .source
        .subagent_lifecycle_snapshot()
        .agents
        .iter()
        .all(|agent| agent.status != SubagentStatus::Completed));
    fixture.close().await;
}
