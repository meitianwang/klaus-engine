//! Canonical configuration, migration and automatic sandbox selection contracts.

use klaus_agent_core::testing::config_sandbox_trace::config_sandbox_trace;

#[test]
fn config_and_sandbox_defaults_validation_match_the_golden_baseline() {
    let expected: serde_json::Value = serde_json::from_str(include_str!(
        "../src/schemas/fixtures/config_sandbox_trace.json"
    ))
    .unwrap();

    let mut actual = config_sandbox_trace();
    assert!(actual
        .pointer("/config/defaults")
        .is_some_and(serde_json::Value::is_object));
    assert!(actual
        .pointer("/config/validation")
        .is_some_and(serde_json::Value::is_object));

    assert_eq!(
        actual.pointer("/sandbox/fullAccess/ok"),
        Some(&serde_json::json!(true))
    );
    assert_eq!(
        actual.pointer("/sandbox/fullAccess/kind"),
        Some(&serde_json::json!("process"))
    );
    for mode in ["readOnly", "workspaceWrite"] {
        let result = actual.pointer(&format!("/sandbox/{mode}")).unwrap();
        if result["ok"] == true {
            let kind = result["kind"]
                .as_str()
                .expect("a selected backend identifies itself");
            assert_ne!(
                kind, "process",
                "a confined mode cannot degrade to a process"
            );
        } else {
            assert_eq!(result["ok"], false);
            assert!(result["message"]
                .as_str()
                .is_some_and(|message| !message.is_empty()));
        }
    }
    // Executor availability belongs to the current platform and executable.
    // Assert fail-closed selection above and compare only deterministic config state.
    actual.as_object_mut().unwrap().remove("sandbox");

    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/schemas/fixtures/config_sandbox_trace.json");
        std::fs::write(path, serde_json::to_string_pretty(&actual).unwrap() + "\n").unwrap();
        return;
    }
    assert_eq!(actual, expected);
}
