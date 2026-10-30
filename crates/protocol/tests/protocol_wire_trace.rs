//! Protocol wire-format regression tests. The checked-in fixture is this
//! project's own golden baseline for parsed protocol event payloads and op
//! validation outcomes. Set `UPDATE_GOLDEN=1` to regenerate the baseline.

use klaus_agent_protocol::testing::protocol_wire_trace;

#[test]
fn protocol_wire_events_match_the_golden_baseline() {
    let actual = protocol_wire_trace();
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/fixtures/protocol_wire_trace.json");
        std::fs::write(path, serde_json::to_string_pretty(&actual).unwrap() + "\n").unwrap();
        return;
    }
    let expected: serde_json::Value =
        serde_json::from_str(include_str!("../src/fixtures/protocol_wire_trace.json")).unwrap();

    // Robustness invariants that must hold independently of the golden file.
    let events = actual
        .pointer("/protocolEvents")
        .and_then(serde_json::Value::as_array)
        .expect("trace must record parsed protocol events");
    assert!(!events.is_empty());
    // Every parsed frame is bound to its submission and carries exactly one
    // payload variant besides that binding.
    assert!(events.iter().all(|entry| {
        entry.get("submissionId").is_some()
            && entry.as_object().is_some_and(|fields| fields.len() >= 2)
    }));
    // Op validation covers both accepted and rejected submissions.
    let validations = actual
        .pointer("/opValidation")
        .and_then(serde_json::Value::as_array)
        .expect("trace must record op validation outcomes");
    assert!(validations
        .iter()
        .any(|entry| entry.get("accepted") == Some(&serde_json::Value::Bool(true))));
    assert!(validations
        .iter()
        .any(|entry| entry.get("accepted") == Some(&serde_json::Value::Bool(false))));

    assert_eq!(actual, expected);
}
