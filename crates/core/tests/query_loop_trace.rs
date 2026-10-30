//! Query-loop regression tests under a scripted provider. The checked-in
//! fixture is this project's own golden baseline of event order, tool-result
//! shaping, and persisted messages.

use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimeString};
use klaus_agent_core::testing::query_trace::query_loop_trace;
use klaus_agent_core::testing::shared_test_process_supervisor;

#[tokio::test]
async fn query_loop_matches_the_golden_baseline() {
    let expected = runtime_json::parse(&RuntimeString::from_utf8(include_str!(
        "../src/engine/fixtures/query_loop_trace.json"
    )))
    .expect("query trace baseline must be exact JSON");

    let process = RuntimeProcessContext::initialize(shared_test_process_supervisor()).unwrap();
    let actual = query_loop_trace(&process).await;
    process.shutdown().await.unwrap();

    let actual = actual.unwrap();

    // Robustness invariants that must hold independently of the golden file.
    let event_types = actual
        .get("eventTypes")
        .and_then(RuntimeJson::as_array)
        .expect("trace must record the event-type order");
    // Every turn opens with thread lifecycle events and closes the turn.
    assert_eq!(
        event_types.first().and_then(RuntimeJson::as_str),
        Some(&RuntimeString::from("thread.started"))
    );
    assert_eq!(
        event_types.last().and_then(RuntimeJson::as_str),
        Some(&RuntimeString::from("turn.completed"))
    );
    // The event-type projection stays in lockstep with the full event list.
    let events = actual
        .get("events")
        .and_then(RuntimeJson::as_array)
        .expect("trace must record the full events");
    assert_eq!(events.len(), event_types.len());
    // Completed turns persist their conversation messages.
    assert!(actual
        .get("persistedMessages")
        .and_then(RuntimeJson::as_array)
        .is_some_and(|messages| !messages.is_empty()));

    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/engine/fixtures/query_loop_trace.json");
        let mut bytes = runtime_json::stringify_pretty(&actual).to_node_utf8();
        bytes.push(b'\n');
        std::fs::write(path, bytes).unwrap();
        return;
    }
    assert_eq!(actual, expected);
}
