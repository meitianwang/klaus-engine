//! Defect: a source switch persisted bare auto and lost the subscription source.
//! Root cause: picker default selection and runtime provider selection disagreed.
//! Fixed for the 2026-09-07 model-source isolation work item.
//! This public facade regression belongs to server: importing it from core
//! would invert the engine's dependencies.

use super::*;

fn model_config() -> &'static str {
    r#"
[modelProviders.alphabetical]
provider = "openai"
model = "alphabetical-model"
[modelProviders.preferred]
provider = "openai"
model = "preferred-model"
default = true
[modelProviders.low]
provider = "openai"
model = "literal-low-model"
[modelProviders.auto]
provider = "openai"
model = "literal-auto-model"
"#
}

#[tokio::test]
async fn model_selection_resolve_pins_the_source_without_requiring_credentials() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    std::fs::write(&path, model_config()).unwrap();
    // The qoder rows below assert the service-route fallback, which only holds
    // while no catalog is reachable. Without an empty `QODER_HOME` the test
    // would read whatever catalog the machine happens to cache and resolve the
    // bare default to its head instead. The catalog tests repoint the same
    // variable, so this shares their lock.
    let _env_lock = super::super::qoder::test_env_lock().lock().await;
    let qoder_home = dir.path().join("qoder-home");
    std::fs::create_dir_all(&qoder_home).unwrap();
    klaus_agent_core::runtime_process::set_process_environment_var(
        "QODER_HOME",
        qoder_home.as_os_str(),
    );
    struct QoderHomeGuard;
    impl Drop for QoderHomeGuard {
        fn drop(&mut self) {
            klaus_agent_core::runtime_process::remove_process_environment_var("QODER_HOME");
        }
    }
    let _guard = QoderHomeGuard;
    let mut client = TestClient::start_full(
        make_create_session(&[]),
        None,
        Some(dir.path().join("rules.json")),
        Some(path),
    );
    client.initialize().await;
    for (index, (params, selection)) in [
        (json!({"source":"custom", "selection":"auto"}), "custom:preferred"),
        // A bare tier name with no host pick falls back to the default entry;
        // the host's tier map is what routes a tier to a specific entry.
        (json!({"source":"custom", "selection":"high"}), "custom:preferred"),
        (json!({"source":"custom", "selection":"high", "modelTierSelections":{"high":"alphabetical"}}), "custom:alphabetical"),
        (json!({"source":"custom", "selection":"custom:alphabetical"}), "custom:alphabetical"),
        (json!({"source":"custom", "selection":"custom:low"}), "custom:low"),
        (json!({"source":"custom", "selection":"custom:auto"}), "custom:auto"),
        (json!({"source":"codex", "selection":"codex:gpt-6-astra"}), "codex:gpt-6-astra"),
        (json!({"source":"codex", "selection":"auto"}), "codex:gpt-5.6-terra"),
        // A bare default names the lineup's top tier model; the `high` tier word
        // maps to the same model, and `medium` stays on the mid one.
        (json!({"source":"subscription", "selection":"auto"}), "subscription:opus"),
        (json!({"source":"subscription", "selection":"high"}), "subscription:opus"),
        (json!({"source":"subscription", "selection":"medium"}), "subscription:sonnet"),
        (json!({"source":"qoder", "selection":"auto"}), "qoder:auto"),
        (json!({"source":"qoder", "selection":"high", "modelTierSelections":{"high":"ultimate"}}), "qoder:ultimate"),
        (json!({"source":"qwenwork", "selection":"auto"}), "qwenwork:qwork-auto"),
        (json!({"source":"qwenwork", "selection":"high", "modelTierSelections":{"high":"flash"}}), "qwenwork:flash"),
        // Preserve the automatic identity without consulting credentials or a
        // runtime catalog. Provider initialization resolves its service model.
        (json!({"source":"workbuddy", "selection":"auto"}), "workbuddy:auto"),
        (json!({"source":"workbuddy", "selection":"high"}), "workbuddy:auto"),
        (json!({"source":"workbuddy", "selection":"high", "modelTierSelections":{"high":"gpt-5.5"}}), "workbuddy:gpt-5.5"),
        (json!({"source":"workbuddy", "selection":"workbuddy:kimi-k3"}), "workbuddy:kimi-k3"),
        // With no catalog reachable the Kimi default names the plan's 1M-context
        // coding model rather than a synthetic automatic route: Kimi publishes no
        // `auto` model, and which models a plan serves is only knowable from the
        // service.
        (json!({"source":"kimi", "selection":"auto"}), "kimi:k3"),
        (json!({"source":"kimi", "selection":"high", "modelTierSelections":{"high":"k3"}}), "kimi:k3"),
        (json!({"source":"kimi", "selection":"kimi:k3-256k"}), "kimi:k3-256k"),
        // Bailian's `auto` is a real model id but is excluded from the catalog
        // because it refuses the `tool_choice` parameter structured requests need,
        // so a stale `auto` selection routes to the catalog head rather than
        // reaching the wire; with no catalog reachable that head is the default.
        (json!({"source":"bailian", "selection":"auto"}), "bailian:qwen3.8-max"),
        (json!({"source":"bailian", "selection":"high", "modelTierSelections":{"high":"qwen3.8-max"}}), "bailian:qwen3.8-max"),
        (json!({"source":"bailian", "selection":"bailian:glm-5.3"}), "bailian:glm-5.3"),
        // M Plan publishes no `auto` routing model, so a bare selection names the
        // plan's flagship. That model is deliberately absent from both catalog
        // routes even though every tier serves it, which is why the catalog injects
        // it rather than resolving it away to whatever the live list happens to
        // head with; with no catalog reachable the same name is the answer.
        (json!({"source":"minimax", "selection":"auto"}), "minimax:MiniMax-M3.1-Flash-Preview"),
        (json!({"source":"minimax", "selection":"high", "modelTierSelections":{"high":"MiniMax-M3"}}), "minimax:MiniMax-M3"),
        (json!({"source":"minimax", "selection":"minimax:MiniMax-M2.7"}), "minimax:MiniMax-M2.7"),
        // Zhipu has no routing model by the name `auto` — asking the gateway for it
        // answers `400 [1211][模型不存在]` — but `auto` is still what the host stores
        // as its default pick, so it must resolve to the catalog head here rather
        // than reaching the wire; with no catalog reachable that head is the default.
        (json!({"source":"zhipu", "selection":"auto"}), "zhipu:glm-5.3"),
        (json!({"source":"zhipu", "selection":"high", "modelTierSelections":{"high":"glm-5.3"}}), "zhipu:glm-5.3"),
        (json!({"source":"zhipu", "selection":"zhipu:glm-4.6"}), "zhipu:glm-4.6"),
        // Ark has no routing model by the name `auto` in its catalog — the alias is
        // injected at projection because the platform list omits it — so a bare
        // selection resolves to the plan's routing alias, and a tier word resolves
        // through the host's map.
        (json!({"source":"ark", "selection":"auto"}), "ark:ark-code-latest"),
        (json!({"source":"ark", "selection":"high", "modelTierSelections":{"high":"doubao-seed-2.1-pro"}}), "ark:doubao-seed-2.1-pro"),
        (json!({"source":"ark", "selection":"ark:kimi-k3"}), "ark:kimi-k3"),
        (json!({"source":"local", "selection":"auto", "catalog":[{"value":"local:on-device.gguf"}]}), "local:on-device.gguf"),
        (json!({"source":"local", "selection":"high", "modelTierSelections":{"high":"auto"}, "catalog":[{"value":"local:on-device.gguf"}]}), "local:on-device.gguf"),
    ].into_iter().enumerate() {
        let id = index as u64 + 10;
        let source = params["source"].clone();
        client.send(json!({"id":id, "method":"model/selection/resolve", "params":params})).await;
        let (response, _) = client.response_for(id).await;
        assert_eq!(response["result"], json!({"source":source, "selection":selection}), "{response}");
    }
    for (index, params) in [
        json!({"source":"custom", "selection":"codex:gpt-6-astra"}),
        json!({"source":"invalid", "selection":"auto"}),
        json!({"source":"klaus", "selection":"auto", "catalog":[{"value":"klaus:retired"}]}),
        json!({"source":"custom", "selection":"custom:"}),
        json!({"source":"custom", "selection":"auto", "catalog":[{"value":"custom:forged"}]}),
        json!({"source":"local", "selection":"auto"}),
    ]
    .into_iter()
    .enumerate()
    {
        let id = index as u64 + 30;
        client
            .send(json!({"id":id, "method":"model/selection/resolve", "params":params}))
            .await;
        let (response, _) = client.response_for(id).await;
        assert_eq!(
            response["error"]["code"],
            json!(super::super::wire::INVALID_PARAMS),
            "{response}"
        );
    }
    client.close().await;
}

#[tokio::test]
async fn persisted_custom_default_and_runtime_auto_build_the_same_provider() {
    use klaus_agent_core::credential_store::{model_credential_key, CredentialStore};
    use klaus_agent_core::mcp::credential_store::InMemoryCredentialStore;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    std::fs::write(&path, model_config()).unwrap();
    let store: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
    store
        .set(&model_credential_key("preferred"), "fixture-key")
        .await
        .unwrap();
    let captured = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let agent = make_agent_for_tests(vec![]);
    let create: CreateSession = {
        let captured = captured.clone();
        Arc::new(move |ctx: SessionContext| {
            captured.lock().unwrap().push(
                ctx.provider
                    .as_ref()
                    .unwrap()
                    .info()
                    .default_model
                    .try_to_utf8()
                    .unwrap(),
            );
            let agent = agent.clone();
            Box::pin(async move {
                Ok::<Arc<Session>, BoxError>(agent.start_session(StartSessionOptions {
                    id: ctx.init.thread_id.clone(),
                    permission_mode: Some(ExternalPermissionMode::BypassPermissions),
                    ..Default::default()
                }))
            })
        })
    };
    let mut client = TestClient::start_full_with_credentials(create, Some(path), Some(store));
    client.initialize().await;
    client.send(json!({"id":2, "method":"model/selection/resolve", "params":{"source":"custom", "selection":"auto"}})).await;
    let (resolved, _) = client.response_for(2).await;
    assert_eq!(resolved["result"]["selection"], "custom:preferred");
    for (id, selection) in [
        (3, json!("auto")),
        (4, resolved["result"]["selection"].clone()),
    ] {
        client.send(json!({"id":id, "method":"session/start", "params":{"init":{"modelSelection":selection}}})).await;
        let (response, _) = client.response_for(id).await;
        assert!(response["result"]["sessionId"].is_string(), "{response}");
        // Session startup stores the init; this read-only public request forces
        // lazy provider construction without sending a turn to any model.
        let activation_id = id + 10;
        client.send(json!({"id":activation_id, "method":"session/runtimeCapabilities", "params":{"sessionId":response["result"]["sessionId"]}})).await;
        let (activation, _) = client.response_for(activation_id).await;
        assert!(activation.get("error").is_none(), "{activation}");
    }
    assert_eq!(
        *captured.lock().unwrap(),
        vec!["preferred-model", "preferred-model"]
    );
    // Selecting a keyless tier must fail on that exact model, even though the
    // account holds a key for the default. No inference request is made.
    client
        .send(
            json!({"id":5, "method":"session/start", "params":{"init":{"modelSelection":"high", "modelTierSelections":{"high":"alphabetical"}}}}),
        )
        .await;
    let (response, _) = client.response_for(5).await;
    assert!(
        response["error"]["message"]
            .as_str()
            .unwrap()
            .contains("alphabetical"),
        "{response}"
    );
    assert_eq!(captured.lock().unwrap().len(), 2);
    client.close().await;
}

#[test]
fn model_selection_resolve_is_in_the_public_schema() {
    let schema = schema_bundle();
    let request = &schema["requests"]["model/selection/resolve"];
    assert!(
        request["params"]["properties"]["source"].is_object(),
        "{request}"
    );
    assert!(
        request["result"]["properties"]["selection"].is_object(),
        "{request}"
    );
}
