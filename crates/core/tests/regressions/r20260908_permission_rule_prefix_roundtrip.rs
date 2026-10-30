//! Defect: saved argument prefixes containing ')' became inert or undeletable rules.
//! Root cause: the serializer accepted literal prefixes but the parser stopped at the first ')'.
//! Fixed for the 2026-09-08 Electron permission-rule migration work item.

use klaus_agent_core::permission::rule_store;
use klaus_agent_core::permission::rules::{format_rule_string, parse_rule_string, rule_matches};
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::types::permission::{PermissionBehavior, PermissionRule};
use std::path::PathBuf;

struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("klaus-rule-prefix-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn rule(prefix: &str) -> PermissionRule {
    PermissionRule {
        tool_name: RuntimeString::from("Bash"),
        argument_prefix: Some(RuntimeString::from(prefix)),
        tool_name_prefix: None,
    }
}

#[tokio::test]
async fn permission_rule_prefix_roundtrip_survives_persistence_evaluation_and_delete() {
    let root = Scratch::new();
    let file = root.0.join("permission-rules.json");
    for prefix in [
        "echo (fixture)",
        "echo ) && (",
        r"C:\Users\fixture\",
        r"\\server\share\",
        r"echo \(literal\)\\tail",
        "printf '你好🙂' (",
    ] {
        let expected = rule(prefix);
        let raw = format_rule_string("Bash", Some(prefix));
        assert_eq!(rule_store::rule_to_string(&expected), raw);
        assert_eq!(parse_rule_string(&raw), Some(expected.clone()));
        rule_store::add(
            &file,
            PermissionBehavior::Ask,
            std::slice::from_ref(&expected),
        )
        .await
        .unwrap();
        let loaded = rule_store::load(&file).await;
        assert_eq!(loaded.ask, vec![raw.clone()]);
        let parsed = parse_rule_string(&loaded.ask[0]).expect("saved rule must be evaluable");
        assert!(rule_matches(
            &parsed,
            &RuntimeString::from("Bash"),
            Some(&format!("{prefix} --fixture"))
        ));
        assert!(!rule_matches(
            &parsed,
            &RuntimeString::from("Bash"),
            Some("unrelated-command")
        ));
        let removed = rule_store::remove(&file, PermissionBehavior::Ask, &raw)
            .await
            .unwrap();
        assert!(removed.ask.is_empty());
    }
}

#[tokio::test]
async fn permission_rule_prefix_existing_literal_files_keep_their_exact_meaning() {
    let root = Scratch::new();
    let file = root.0.join("permission-rules.json");
    let raw = r"Bash(\\server\share\)";
    std::fs::write(
        &file,
        serde_json::json!({ "allow": [raw], "deny": [], "ask": [] }).to_string(),
    )
    .unwrap();
    let loaded = rule_store::load(&file).await;
    assert_eq!(loaded.allow, vec![raw.to_string()]);
    let parsed = parse_rule_string(&loaded.allow[0]).unwrap();
    assert_eq!(
        parsed.argument_prefix,
        Some(RuntimeString::from(r"\\server\share\"))
    );
    assert!(rule_matches(
        &parsed,
        &RuntimeString::from("Bash"),
        Some(r"\\server\share\file")
    ));
    assert!(!rule_matches(
        &parsed,
        &RuntimeString::from("Bash"),
        Some(r"\server\share\file")
    ));
    assert!(rule_store::remove(&file, PermissionBehavior::Allow, raw)
        .await
        .unwrap()
        .allow
        .is_empty());
}

#[tokio::test]
async fn permission_rule_prefix_invalid_batch_cannot_persist_a_partial_grant() {
    let root = Scratch::new();
    let file = root.0.join("permission-rules.json");
    rule_store::add(&file, PermissionBehavior::Deny, &[rule("safe-before")])
        .await
        .unwrap();
    let original = std::fs::read(&file).unwrap();
    let invalid = PermissionRule {
        tool_name: RuntimeString::from("invalid tool"),
        argument_prefix: None,
        tool_name_prefix: None,
    };
    assert!(rule_store::add(
        &file,
        PermissionBehavior::Allow,
        &[rule("must-not-save"), invalid]
    )
    .await
    .is_err());
    assert_eq!(std::fs::read(&file).unwrap(), original);
}

#[test]
fn permission_rule_prefix_keeps_malformed_outer_forms_invalid() {
    for raw in [
        "Bash(prefix)tail",
        "Bash(unclosed",
        "(prefix)",
        "bad tool(prefix)",
        "mcp__server__*(ignored-prefix)",
    ] {
        assert!(parse_rule_string(raw).is_none(), "{raw}");
    }
    // MCP server names have their own prefix semantics and were not limited
    // to the built-in tool identifier grammar before this fix.
    let mcp = parse_rule_string("mcp__legacy.server__*").unwrap();
    assert!(rule_matches(
        &mcp,
        &RuntimeString::from("mcp__legacy.server__write"),
        None
    ));
}

#[tokio::test]
async fn permission_rule_prefix_admin_errors_preserve_corrupt_bytes_and_legacy_startup_fallback() {
    let root = Scratch::new();
    let file = root.0.join("permission-rules.json");
    assert_eq!(
        rule_store::load_strict(&file).await.unwrap(),
        rule_store::DurableRules::default()
    );
    for raw in [b"{corrupt".as_slice(), br#"{"deny": [false]}"#, br#"[]"#] {
        std::fs::write(&file, raw).unwrap();
        assert!(rule_store::load_strict(&file).await.is_err());
        assert!(
            rule_store::add(&file, PermissionBehavior::Allow, &[rule("new")])
                .await
                .is_err()
        );
        assert!(rule_store::remove(&file, PermissionBehavior::Deny, "Read")
            .await
            .is_err());
        assert_eq!(std::fs::read(&file).unwrap(), raw);
    }
    std::fs::write(&file, b"{corrupt").unwrap();
    assert_eq!(
        rule_store::load(&file).await,
        rule_store::DurableRules::default()
    );
    std::fs::remove_file(&file).unwrap();
    std::fs::create_dir(&file).unwrap();
    assert!(rule_store::load_strict(&file).await.is_err());
    assert!(
        rule_store::add(&file, PermissionBehavior::Ask, &[rule("new")])
            .await
            .is_err()
    );
    assert!(file.is_dir());
}
