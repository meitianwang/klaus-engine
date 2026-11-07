//! Workflow runtime regression tests: built-in `deep-research-strict`,
//! inline JS workflows, registered/nested workflows, and disk
//! discovery/save. The checked-in fixtures are this project's own golden
//! baselines.

use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimeString};
use klaus_agent_core::testing::node_interpreter_present;
use klaus_agent_core::testing::shared_test_process_supervisor;
use klaus_agent_core::testing::workflow_trace::{
    deep_research_strict_trace, inline_workflow_trace, registered_workflow_trace,
    workflow_disk_trace,
};

fn exact_fixture(source: &str) -> RuntimeJson {
    runtime_json::parse(&RuntimeString::from_utf8(source))
        .expect("workflow trace baseline must be exact JSON")
}

// Robustness invariant shared by the runtime scenarios: every trace records
// at least one workflow run, independently of the golden file.
fn assert_records_runs(trace: &RuntimeJson) {
    assert!(trace
        .get("runs")
        .and_then(RuntimeJson::as_array)
        .is_some_and(|runs| !runs.is_empty()));
}

// The deep-research, inline and registered scenarios all run raw-JavaScript
// workflow scripts, which the runner hands to a real Node subprocess. On a host
// without Node there is nothing to run them in, and the spawn failure would read
// as a defect in the workflow runner rather than as a missing interpreter, so
// they skip with the reason printed.
fn skip_without_node() -> bool {
    if node_interpreter_present() {
        return true;
    }
    eprintln!(
        "skipping the raw-JavaScript workflow trace: no `node` on PATH (set KLAUS_NODE, \
         or install Node.js)"
    );
    false
}

#[tokio::test]
async fn deep_research_strict_matches_the_golden_baseline() {
    if !skip_without_node() {
        return;
    }
    let expected = exact_fixture(include_str!(
        "../src/engine/workflow/fixtures/deep_research_strict_trace.json"
    ));

    let process = RuntimeProcessContext::initialize(shared_test_process_supervisor()).unwrap();
    let actual = deep_research_strict_trace(&process).await;
    process.shutdown().await.unwrap();

    let actual = actual.unwrap();
    assert_records_runs(&actual);
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/engine/workflow/fixtures/deep_research_strict_trace.json");
        let mut bytes = runtime_json::stringify_pretty(&actual).to_node_utf8();
        bytes.push(b'\n');
        std::fs::write(path, bytes).unwrap();
        return;
    }
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn inline_workflow_matches_the_golden_baseline() {
    if !skip_without_node() {
        return;
    }
    let expected = exact_fixture(include_str!(
        "../src/engine/workflow/fixtures/inline_workflow_trace.json"
    ));

    let process = RuntimeProcessContext::initialize(shared_test_process_supervisor()).unwrap();
    let actual = inline_workflow_trace(&process).await;
    process.shutdown().await.unwrap();

    let actual = actual.unwrap();
    assert_records_runs(&actual);
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/engine/workflow/fixtures/inline_workflow_trace.json");
        let mut bytes = runtime_json::stringify_pretty(&actual).to_node_utf8();
        bytes.push(b'\n');
        std::fs::write(path, bytes).unwrap();
        return;
    }
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn registered_workflow_matches_the_golden_baseline() {
    if !skip_without_node() {
        return;
    }
    let expected = exact_fixture(include_str!(
        "../src/engine/workflow/fixtures/registered_workflow_trace.json"
    ));

    let process = RuntimeProcessContext::initialize(shared_test_process_supervisor()).unwrap();
    let actual = registered_workflow_trace(&process).await;
    process.shutdown().await.unwrap();

    let actual = actual.unwrap();
    assert_records_runs(&actual);
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/engine/workflow/fixtures/registered_workflow_trace.json");
        let mut bytes = runtime_json::stringify_pretty(&actual).to_node_utf8();
        bytes.push(b'\n');
        std::fs::write(path, bytes).unwrap();
        return;
    }
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn workflow_disk_discovery_save_matches_the_golden_baseline() {
    let expected = exact_fixture(include_str!(
        "../src/engine/workflow/fixtures/workflow_disk_trace.json"
    ));

    let actual = workflow_disk_trace().await.unwrap();

    // Robustness invariants that must hold independently of the golden file:
    // disk discovery finds workflows and saving reports a concrete path.
    assert!(actual
        .get("discovered")
        .is_some_and(|discovered| !discovered.is_null()));
    assert!(actual.get("save").is_some_and(|save| !save.is_null()));

    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/engine/workflow/fixtures/workflow_disk_trace.json");
        let mut bytes = runtime_json::stringify_pretty(&actual).to_node_utf8();
        bytes.push(b'\n');
        std::fs::write(path, bytes).unwrap();
        return;
    }
    assert_eq!(actual, expected);
}
