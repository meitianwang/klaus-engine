//! Canonical sandbox config, live policy updates, and retired diagnostics share one facade contract.

use super::*;
use crate::app_server::wire;

#[test]
fn canonical_runtime_policy_schema_has_optional_network_and_exact_workspace_table() {
    let schema = schema_bundle();
    let params = &schema["requests"]["session/setRuntimePolicy"]["params"];
    let required = params["required"].as_array().unwrap();
    assert!(!required.contains(&json!("networkAllowed")));
    assert!(!required.contains(&json!("sandboxWorkspaceWrite")));
    for retired in ["sandbox", "requireRealSandbox", "sandboxRetryOnDenial"] {
        assert!(params["properties"].get(retired).is_none(), "{retired}");
    }
    let table = &params["definitions"]["SandboxWorkspaceWriteSchema"];
    assert_eq!(table["additionalProperties"], false);
    for key in [
        "writable_roots",
        "network_access",
        "exclude_tmpdir_env_var",
        "exclude_slash_tmp",
    ] {
        assert!(table["properties"][key].is_object(), "{key}: {table}");
    }
    assert!(schema["requests"]["session/start"]["params"]["definitions"]
        ["SessionRoutingInitSchema"]["properties"]["networkAllowed"]
        .is_object());
    assert!(
        schema["requests"]["config/read"]["result"]["properties"]["migrationWarnings"].is_object()
    );
    let minimal = json!({"sessionId":"one"});
    let decoded: wire::SessionSetRuntimePolicyParams =
        serde_json::from_value(minimal.clone()).unwrap();
    assert!(decoded.network_allowed.is_none());
    assert!(decoded.sandbox_workspace_write.is_none());
    assert_eq!(serde_json::to_value(decoded).unwrap(), minimal);
    for (key, value) in [
        ("sandbox", json!("auto")),
        ("requireRealSandbox", json!(true)),
        ("sandboxRetryOnDenial", json!(false)),
    ] {
        let mut retired = minimal.clone();
        retired[key] = value;
        assert!(serde_json::from_value::<wire::SessionSetRuntimePolicyParams>(retired).is_err());
    }
}

#[tokio::test]
async fn retired_sandbox_diagnostic_methods_are_absent_from_schema_and_dispatch() {
    let schema = schema_bundle();
    let mut client = TestClient::start(make_create_session(&[]));
    client.initialize().await;
    for (index, method) in [
        "session/sandboxDiagnostics",
        "engine/windowsSandboxSetupStart",
        "engine/windowsSandboxSetupCancel",
    ]
    .into_iter()
    .enumerate()
    {
        assert!(schema["requests"].get(method).is_none(), "{method}");
        let id = index as u64 + 2;
        client
            .send(json!({"id":id,"method":method,"params":{}}))
            .await;
        let (reply, _) = client.response_for(id).await;
        assert_eq!(reply["error"]["code"], wire::METHOD_NOT_FOUND, "{reply}");
    }
    client.close().await;
}

#[tokio::test]
async fn config_read_returns_legacy_sandbox_migration_warning() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("config.toml");
    std::fs::write(
        &path,
        "sandbox = \"auto\"\nsandboxNetworkAllowed = true\nrequireRealSandbox = false\n",
    )
    .unwrap();
    let mut client = TestClient::start_full(make_create_session(&[]), None, None, Some(path));
    client.initialize().await;
    client
        .send(json!({"id":2,"method":"config/read","params":{}}))
        .await;
    let (reply, _) = client.response_for(2).await;
    assert!(reply.get("error").is_none(), "{reply}");
    let result = &reply["result"];
    let warnings = result["migrationWarnings"].as_array().unwrap();
    let mut keys = Vec::new();
    for warning in warnings {
        assert_eq!(warning["code"], "legacySandboxConfigMigrated");
        assert!(!warning["message"].as_str().unwrap().is_empty());
        keys.extend(warning["keys"].as_array().unwrap().iter().cloned());
    }
    for key in ["sandbox", "sandboxNetworkAllowed", "requireRealSandbox"] {
        assert!(keys.contains(&json!(key)), "{warnings:?}");
        assert!(result["values"].get(key).is_none(), "{result}");
    }
    assert!(result["values"].get("sandbox_mode").is_some());
    assert!(
        result["values"]["sandbox_workspace_write"]["network_access"]
            .as_bool()
            .unwrap()
    );
    client.close().await;
}

#[tokio::test]
async fn session_start_workspace_roots_use_only_the_explicit_init_cwd() {
    use klaus_agent_core::runtime_values::runtime_codec;
    use std::sync::Mutex;

    let root = tempfile::tempdir().unwrap();
    let base = root.path().join("uncreated-base");
    let absolute = root.path().join("absolute-root");
    for (label, cwd, writable_root, expected) in [
        (
            "explicit-base",
            Some(base.clone()),
            "nested/../allowed".to_string(),
            Some(base.join("allowed")),
        ),
        ("missing-base", None, "relative-root".to_string(), None),
        (
            "relative-base",
            Some(std::path::PathBuf::from("relative-base")),
            "relative-root".to_string(),
            None,
        ),
        (
            "absolute-root",
            None,
            absolute.to_string_lossy().into_owned(),
            Some(absolute.clone()),
        ),
    ] {
        let captured = Arc::new(Mutex::new(None));
        let capture = captured.clone();
        let original = make_create_session(&[]);
        let factory: CreateSession = Arc::new(move |context| {
            let original = original.clone();
            let capture = capture.clone();
            Box::pin(async move {
                *capture.lock().unwrap() = context.init.config.clone();
                original(context).await
            })
        });
        let mut client = TestClient::start(factory);
        client.initialize().await;
        let mut init = json!({
            "threadId":format!("workspace-root-{label}"),
            "config":{"prewarm":false,"sandbox_mode":"workspace-write",
                "sandbox_workspace_write":{"writable_roots":[writable_root]}}
        });
        if let Some(cwd) = cwd {
            init["cwd"] = json!(cwd);
        }
        client
            .send(json!({"id":2,"method":"session/start","params":{"init":init}}))
            .await;
        let (started, _) = client.response_for(2).await;
        match expected {
            Some(expected) => {
                assert!(started.get("error").is_none(), "{label}: {started}");
                client
                    .send(json!({"id":3,"method":"session/history","params":{
                        "sessionId":started["result"]["sessionId"]
                    }}))
                    .await;
                let (history, _) = client.response_for(3).await;
                assert!(history.get("error").is_none(), "{history}");
                let config = captured
                    .lock()
                    .unwrap()
                    .clone()
                    .expect("materialized config");
                let config = runtime_codec::encode_text(&config)
                    .unwrap()
                    .try_to_utf8()
                    .unwrap();
                let config: Value = serde_json::from_str(&config).unwrap();
                assert_eq!(
                    config["sandbox_workspace_write"]["writable_roots"],
                    json!([expected])
                );
            }
            None => {
                assert!(started.get("error").is_some(), "{label}: {started}");
                assert!(
                    captured.lock().unwrap().is_none(),
                    "invalid roots never materialize"
                );
            }
        }
        client.close().await;
    }
    assert!(!base.exists(), "normalization does not create config roots");
    assert!(!absolute.exists(), "absolute roots need not exist");
}
