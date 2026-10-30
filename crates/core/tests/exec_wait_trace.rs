//! Exec/wait code-mode regression tests. The checked-in fixture is this
//! project's own golden baseline covering model-facing metadata, JS
//! execution, nested tools, pending cells, truncation, and termination.

use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimeString};
use klaus_agent_core::testing::exec_wait_trace::exec_wait_trace;
use klaus_agent_core::testing::shared_test_process_supervisor;

#[tokio::test]
async fn exec_wait_code_mode_matches_the_golden_baseline() {
    let expected = runtime_json::parse(&RuntimeString::from_utf8(include_str!(
        "../src/tools/code_mode/fixtures/exec_wait_trace.json"
    )))
    .expect("exec/wait trace baseline must be exact JSON");

    let process = RuntimeProcessContext::initialize(shared_test_process_supervisor()).unwrap();
    let actual = exec_wait_trace(&process).await;
    process.shutdown().await.unwrap();

    let actual = actual.unwrap();

    // Robustness invariants that must hold independently of the golden file.
    // The model-facing code-mode protocol is exactly `exec` + `wait`.
    let tool_names: Vec<&RuntimeString> = actual
        .pointer("/metadata/toolNames")
        .and_then(RuntimeJson::as_array)
        .expect("trace must record the model-facing tool names")
        .iter()
        .filter_map(RuntimeJson::as_str)
        .collect();
    assert_eq!(
        tool_names,
        vec![&RuntimeString::from("exec"), &RuntimeString::from("wait")]
    );
    // The scenario matrix must keep exercising the lifecycle edge cases:
    // pending cells, truncation continuations, termination, and errors.
    for call in [
        "immediate",
        "nested",
        "pending",
        "truncated",
        "terminate",
        "errors",
    ] {
        assert!(
            actual.pointer(&format!("/calls/{call}")).is_some(),
            "trace must cover the {call} call scenario"
        );
    }

    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/tools/code_mode/fixtures/exec_wait_trace.json");
        let mut bytes = runtime_json::stringify_pretty(&actual).to_node_utf8();
        bytes.push(b'\n');
        std::fs::write(path, bytes).unwrap();
        return;
    }
    assert_eq!(actual, expected);
}
