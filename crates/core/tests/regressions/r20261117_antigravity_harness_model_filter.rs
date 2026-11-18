//! Defect: Antigravity catalog entries reserved for external multi-step harness
//! routing appeared in Klaus's conversation model picker, and saved selections
//! or tier targets referencing those aliases failed purpose validation once
//! filtered out of conversation choices.
//! Root cause: `conversation_catalog` lacked an Antigravity dedicated-identity
//! rule for `gemini-pro-orchestrator*` and `gemini-flash-worker-agy`, and model
//! selection / target resolution passed those aliases straight through instead
//! of remapping them to their concrete underlying chat models.
//! Work item: 20261117 Antigravity harness alias exclusion and fallback mapping.
//! Mounted by the app-server aggregate to exercise the public session facade.

use super::{make_create_session, CreateSession, SessionContext, TestClient};
use crate::app_server::antigravity;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};

fn sample_catalog() -> Value {
    json!({
        "defaultAgentModelId": "gemini-pro-orchestrator",
        "agentModelSorts": [{
            "displayName": "Recommended",
            "groups": [{
                "modelIds": [
                    "gemini-pro-orchestrator",
                    "gemini-pro-orchestrator-argon",
                    "gemini-4-carbon-high",
                    "gemini-4-argon-high",
                    "gemini-4-hydrogen-xhigh"
                ]
            }]
        }],
        "models": {
            "gemini-pro-orchestrator": {
                "displayName": "Orchestrator (Carbon)",
                "recommended": true,
                "maxTokens": 1_048_576,
                "maxOutputTokens": 262_144,
                "supportsThinking": true,
                "thinkingLevel": 3,
                "modelProvider": "MODEL_PROVIDER_GOOGLE"
            },
            "gemini-pro-orchestrator-argon": {
                "displayName": "Orchestrator (Argon)",
                "recommended": true,
                "maxTokens": 1_048_576,
                "maxOutputTokens": 65_536,
                "supportsThinking": true,
                "thinkingLevel": 3,
                "modelProvider": "MODEL_PROVIDER_GOOGLE"
            },
            "gemini-flash-worker-agy": {
                "displayName": "Gemini 4 Hydrogen (Orchestrator)",
                "maxTokens": 1_048_576,
                "maxOutputTokens": 262_144,
                "supportsThinking": true,
                "thinkingLevel": 100,
                "modelProvider": "MODEL_PROVIDER_GOOGLE"
            },
            "gemini-4-carbon-high": {
                "displayName": "Gemini 4 Carbon (High)",
                "recommended": true,
                "maxTokens": 1_048_576,
                "maxOutputTokens": 262_144,
                "supportsThinking": true,
                "thinkingLevel": 3,
                "modelProvider": "MODEL_PROVIDER_GOOGLE"
            },
            "gemini-4-argon-high": {
                "displayName": "Gemini 4 Argon (High)",
                "recommended": true,
                "maxTokens": 1_048_576,
                "maxOutputTokens": 65_536,
                "supportsThinking": true,
                "thinkingLevel": 3,
                "modelProvider": "MODEL_PROVIDER_GOOGLE"
            },
            "gemini-4-hydrogen-xhigh": {
                "displayName": "Gemini 4 Hydrogen (XHigh)",
                "recommended": true,
                "maxTokens": 1_048_576,
                "maxOutputTokens": 262_144,
                "supportsThinking": true,
                "thinkingLevel": 100,
                "modelProvider": "MODEL_PROVIDER_GOOGLE"
            }
        }
    })
}

async fn run_harness_alias_filter_and_remap() {
    let root = tempfile::tempdir().unwrap();
    antigravity::set_config_root(root.path());
    antigravity::auth::adopt(antigravity::auth::Credential {
        access_token: "fixture-access".into(),
        refresh_token: None,
        expires_at: None,
        account: Some("harness-filter@example.invalid".into()),
        auth_method: "consumer".into(),
        project: Some("fixture-project".into()),
        client_id: None,
        client_secret: None,
    })
    .await
    .unwrap();
    let generation = antigravity::auth::generation().unwrap();
    std::fs::write(
        root.path().join("antigravity-models.json"),
        json!({
            "generation": generation,
            "fetchedAt": chrono::Utc::now().timestamp_millis(),
            "clientIdentity": antigravity::cloud_test_identity(),
            "payload": sample_catalog(),
        })
        .to_string(),
    )
    .unwrap();

    let captured = Arc::new(Mutex::new(Vec::<Value>::new()));
    let observed = captured.clone();
    let base = make_create_session(&["OK"]);
    let create: CreateSession = Arc::new(move |ctx: SessionContext| {
        let provider = ctx.provider.as_ref().expect("a source-resolved provider");
        let model = provider.info().default_model.try_to_utf8().unwrap();
        let str_field =
            |obj: &klaus_agent_core::runtime_values::RuntimeJson, key: &str| -> String {
                obj.get(key)
                    .and_then(|v| v.as_str())
                    .map(|s| s.try_to_utf8().unwrap())
                    .unwrap_or_default()
            };
        let project_target = |raw: &klaus_agent_core::runtime_values::RuntimeJson| -> Value {
            let provider_model = raw
                .get("provider")
                .and_then(|p| p.get("model"))
                .and_then(|v| v.as_str())
                .map(|s| s.try_to_utf8().unwrap())
                .unwrap_or_default();
            json!({
                "type": str_field(raw, "type"),
                "modelId": str_field(raw, "modelId"),
                "provider": {"model": provider_model},
            })
        };
        let low = ctx
            .init
            .model_tier_targets
            .as_ref()
            .and_then(|m| m.get("low"))
            .map(project_target)
            .unwrap_or(Value::Null);
        let title = ctx
            .init
            .feature_targets
            .as_ref()
            .and_then(|m| m.get("title"))
            .map(project_target)
            .unwrap_or(Value::Null);
        observed.lock().unwrap().push(json!({
            "model": model,
            "init": {
                "orchestratorTarget": ctx.init.orchestrator_target.as_ref().map(project_target).unwrap_or(Value::Null),
                "advisorTarget": ctx.init.advisor_target.as_ref().map(project_target).unwrap_or(Value::Null),
                "modelTierTargets": {"low": low},
                "featureTargets": {"title": title},
            }
        }));
        base(ctx)
    });

    let mut client = TestClient::start_full(
        create,
        None,
        Some(root.path().join("rules.json")),
        Some(root.path().join("config.toml")),
    );
    client.initialize().await;

    client
        .send(json!({"id": 201, "method": "antigravity/models", "params": {"read": "cached"}}))
        .await;
    let models_reply = client.response_for(201).await.0;
    assert_eq!(models_reply["result"]["catalogBaseline"], json!(false));
    let models = models_reply["result"]["models"]
        .as_array()
        .unwrap_or_else(|| panic!("expected models array: {models_reply}"));
    for excluded in [
        "gemini-pro-orchestrator",
        "gemini-pro-orchestrator-argon",
        "gemini-flash-worker-agy",
    ] {
        assert!(
            !models.iter().any(|row| row["key"] == excluded),
            "harness alias {excluded} leaked into conversation catalog: {models_reply}"
        );
    }
    for expected in [
        "gemini-4-carbon-high",
        "gemini-4-argon-high",
        "gemini-4-hydrogen-xhigh",
    ] {
        assert!(
            models.iter().any(|row| row["key"] == expected),
            "concrete chat model {expected} missing: {models_reply}"
        );
    }
    let carbon_row = models
        .iter()
        .find(|row| row["key"] == "gemini-4-carbon-high")
        .unwrap();
    assert_eq!(carbon_row["isDefault"], json!(true));

    for (req_id, saved, expected) in [
        (
            202,
            "antigravity:gemini-pro-orchestrator",
            "antigravity:gemini-4-carbon-high",
        ),
        (
            203,
            "antigravity:gemini-pro-orchestrator-argon",
            "antigravity:gemini-4-argon-high",
        ),
        (
            204,
            "antigravity:gemini-flash-worker-agy",
            "antigravity:gemini-4-hydrogen-xhigh",
        ),
        (205, "high", "antigravity:gemini-4-carbon-high"),
    ] {
        client
            .send(json!({
                "id": req_id,
                "method": "model/selection/resolve",
                "params": {
                    "source": "antigravity",
                    "selection": saved,
                    "modelTierSelections": {
                        "high": "gemini-pro-orchestrator",
                        "medium": "gemini-pro-orchestrator-argon",
                        "low": "gemini-flash-worker-agy"
                    }
                }
            }))
            .await;
        let reply = client.response_for(req_id).await.0;
        assert_eq!(reply["result"]["selection"], json!(expected), "{reply}");
    }

    client
        .send(json!({
            "id": 206,
            "method": "session/start",
            "params": {
                "init": {
                    "cwd": root.path(),
                    "modelSelection": "antigravity:gemini-pro-orchestrator",
                    "orchestrationMode": true,
                    "orchestratorTarget": {"type": "source", "source": "antigravity", "modelId": "gemini-pro-orchestrator"},
                    "advisorTarget": {"type": "source", "source": "antigravity", "modelId": "gemini-pro-orchestrator-argon"},
                    "modelTierTargets": {
                        "low": {"type": "source", "source": "antigravity", "modelId": "gemini-flash-worker-agy"}
                    },
                    "featureTargets": {
                        "title": {"type": "source", "source": "antigravity", "modelId": "gemini-flash-worker-agy"},
                        "prediction": null,
                        "memory": null
                    },
                    "config": {
                        "prewarm": false,
                        "autoMemory": false,
                        "checkpointing": false,
                        "includeGitContext": false,
                        "shellEnvCapture": false,
                        "maxTurns": 1
                    }
                }
            }
        }))
        .await;
    let started = client.response_for(206).await.0;
    assert!(
        started.get("error").is_none(),
        "session/start failed: {started}"
    );
    let session_id = started["result"]["sessionId"]
        .as_str()
        .unwrap_or_else(|| panic!("missing sessionId: {started}"));

    client
        .send(json!({
            "id": 207,
            "method": "turn/start",
            "params": {"sessionId": session_id, "input": "Reply OK without using tools."}
        }))
        .await;
    let turn_reply = client.response_for(207).await.0;
    assert!(
        turn_reply.get("error").is_none(),
        "turn/start failed: {turn_reply}"
    );
    let recorded = captured
        .lock()
        .unwrap()
        .pop()
        .expect("captured session init");
    assert_eq!(
        recorded["model"],
        json!("gemini-4-carbon-high"),
        "{recorded}"
    );
    for (pointer, expected_model) in [
        ("/init/orchestratorTarget", "gemini-4-carbon-high"),
        ("/init/advisorTarget", "gemini-4-argon-high"),
        ("/init/modelTierTargets/low", "gemini-4-hydrogen-xhigh"),
        ("/init/featureTargets/title", "gemini-4-hydrogen-xhigh"),
    ] {
        let target = recorded
            .pointer(pointer)
            .unwrap_or_else(|| panic!("missing {pointer}: {recorded}"));
        assert_eq!(target["type"], json!("resolved"), "{pointer}: {target}");
        assert_eq!(
            target["modelId"],
            json!(expected_model),
            "{pointer}: {target}"
        );
        assert_eq!(
            target["provider"]["model"],
            json!(expected_model),
            "{pointer}: {target}"
        );
    }

    client.close().await;
    antigravity::auth::logout().await.unwrap();
}

#[test]
fn antigravity_harness_aliases_stay_out_of_conversation_models_and_remap_saved_selections() {
    std::thread::Builder::new()
        .name("antigravity-harness-filter".into())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(Box::pin(run_harness_alias_filter_and_remap()));
        })
        .unwrap()
        .join()
        .unwrap();
}
