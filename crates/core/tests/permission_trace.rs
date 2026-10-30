//! Permission decision and approval-flow regression tests. The checked-in
//! fixture is this project's own golden baseline covering rule precedence,
//! path gates, safety gates, and same-turn approval updates.

use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimeString};
use klaus_agent_core::testing::permission_trace::permission_trace;
use klaus_agent_core::testing::shared_test_process_supervisor;

#[tokio::test]
async fn permission_decisions_and_approval_shapes_match_the_golden_baseline() {
    let expected = runtime_json::parse(&RuntimeString::from_utf8(include_str!(
        "../src/permission/fixtures/permission_trace.json"
    )))
    .expect("permission trace baseline must be exact JSON");

    let process = RuntimeProcessContext::initialize(shared_test_process_supervisor()).unwrap();
    let actual = permission_trace(&process).await;
    process.shutdown().await.unwrap();

    // Robustness invariants that must hold independently of the golden file.
    let decisions = actual
        .get("decisions")
        .and_then(RuntimeJson::as_array)
        .expect("trace must record static permission decisions");
    assert!(!decisions.is_empty());
    let outcomes: Vec<&RuntimeString> = decisions
        .iter()
        .filter_map(|entry| entry.pointer("/decision/decision")?.as_str())
        .collect();
    assert_eq!(outcomes.len(), decisions.len());
    // The decision matrix must exercise all three behaviors, including the
    // fail-closed deny path.
    for behavior in ["allow", "ask", "deny"] {
        let behavior = RuntimeString::from(behavior);
        assert!(
            outcomes.contains(&&behavior),
            "decision matrix must include a {behavior:?} outcome"
        );
    }
    // Approving a mutating tool call mid-turn applies the new rule within the
    // same turn instead of asking again.
    assert!(actual
        .pointer("/approvalFlow/approvalRequests")
        .and_then(RuntimeJson::as_array)
        .is_some_and(|requests| !requests.is_empty()));
    assert!(actual
        .pointer("/approvalFlow/finalSessionRules")
        .is_some_and(|rules| !rules.is_null()));

    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/permission/fixtures/permission_trace.json");
        let mut bytes = runtime_json::stringify_pretty(&actual).to_node_utf8();
        bytes.push(b'\n');
        std::fs::write(path, bytes).unwrap();
        return;
    }
    assert_eq!(actual, expected);
}
