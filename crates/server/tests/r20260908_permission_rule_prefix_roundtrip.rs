//! Defect: the named rule-write facade acknowledged prefixes that vanished during evaluation.
//! Root cause: an unrestricted fallback bypassed the config grammar and the serializer had a different grammar.
//! Fixed for the 2026-09-08 Electron permission-rule migration work item.

use super::*;
use klaus_agent_core::permission::rules::{parse_rule_string, rule_matches};
use klaus_agent_core::runtime_values::RuntimeString;

#[tokio::test]
async fn permission_rule_prefix_named_facade_roundtrips_literal_commands() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("permission-rules.json");
    let prefix = r"echo (fixture)\\server\share\";
    let raw = format!("Bash({prefix})");
    let mut client = TestClient::start_full(
        make_create_session(&[]),
        None,
        Some(path.clone()),
        Some(root.path().join("config.toml")),
    );
    client.initialize().await;
    client
        .send(
            json!({ "id": 2, "method": "engine/addPermissionRules", "params": {
        "behavior": "ask", "rules": [{ "toolName": "Bash", "argumentPrefix": prefix }]
    } }),
        )
        .await;
    let (added, _) = client.response_for(2).await;
    assert_eq!(added["result"]["ask"], json!([raw.clone()]));
    client.close().await;

    let mut client = TestClient::start_full(
        make_create_session(&[]),
        None,
        Some(path),
        Some(root.path().join("config.toml")),
    );
    client.initialize().await;
    client
        .send(json!({ "id": 2, "method": "engine/permissionRules", "params": {} }))
        .await;
    let (loaded, _) = client.response_for(2).await;
    let stored = loaded["result"]["ask"][0].as_str().unwrap();
    assert_eq!(stored, raw);
    let parsed = parse_rule_string(stored).expect("a successful facade grant must actually parse");
    assert_eq!(parsed.argument_prefix, Some(RuntimeString::from(prefix)));
    assert!(rule_matches(
        &parsed,
        &RuntimeString::from("Bash"),
        Some(prefix)
    ));
    assert!(!rule_matches(
        &parsed,
        &RuntimeString::from("Bash"),
        Some("another command")
    ));
    client.send(json!({ "id": 3, "method": "engine/removePermissionRule", "params": { "behavior": "ask", "rule": raw } })).await;
    let (removed, _) = client.response_for(3).await;
    assert_eq!(removed["result"]["ask"], json!([]));
    client.close().await;
}

#[tokio::test]
async fn permission_rule_prefix_named_facade_rejects_invalid_batches_without_writes() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("permission-rules.json");
    let mut client = TestClient::start_full(
        make_create_session(&[]),
        None,
        Some(path.clone()),
        Some(root.path().join("config.toml")),
    );
    client.initialize().await;
    for (index, invalid) in [
        json!({ "toolName": "invalid tool" }),
        json!({ "toolName": "Bash", "argumentPrefix": 7 }),
        json!({ "toolName": "Bash(command-in-tool)" }),
        json!({ "toolName": "mcp__server__*", "argumentPrefix": "ignored-narrowing" }),
    ]
    .into_iter()
    .enumerate()
    {
        let id = 2 + index as u64;
        client
            .send(
                json!({ "id": id, "method": "engine/addPermissionRules", "params": {
            "rules": [{ "toolName": "Read" }, invalid]
        } }),
            )
            .await;
        let (response, _) = client.response_for(id).await;
        assert_eq!(
            response["error"]["code"],
            json!(super::super::wire::INVALID_PARAMS)
        );
        assert!(
            !path.exists(),
            "the valid prefix of a refused batch must not be saved"
        );
    }
    client.close().await;
}

#[tokio::test]
async fn permission_rule_prefix_named_admin_facade_does_not_overwrite_corrupt_state() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("permission-rules.json");
    let original = b"{incomplete permissions";
    std::fs::write(&path, original).unwrap();
    let mut client = TestClient::start_full(
        make_create_session(&[]),
        None,
        Some(path.clone()),
        Some(root.path().join("config.toml")),
    );
    client.initialize().await;
    for (index, (method, params)) in [
        ("engine/permissionRules", json!({})),
        (
            "engine/addPermissionRules",
            json!({ "rules": [{ "toolName": "Read" }] }),
        ),
        ("engine/removePermissionRule", json!({ "rule": "Read" })),
    ]
    .into_iter()
    .enumerate()
    {
        let id = index as u64 + 2;
        client
            .send(json!({ "id": id, "method": method, "params": params }))
            .await;
        let (response, _) = client.response_for(id).await;
        assert_eq!(
            response["error"]["code"],
            json!(super::super::wire::ENGINE_ERROR)
        );
        assert_eq!(std::fs::read(&path).unwrap(), original);
    }
    client.close().await;
}
