//! Zero-config session-memory regression tests. The checked-in fixture is
//! this project's own golden baseline for main-loop memory prompt injection
//! and the `agentMemory: false` opt-out.

use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimeString};
use klaus_agent_core::testing::session_memory_trace::session_memory_trace;
use klaus_agent_core::testing::shared_test_process_supervisor;

#[tokio::test]
async fn zero_config_session_memory_behavior_matches_the_golden_baseline() {
    let expected = runtime_json::parse(&RuntimeString::from_utf8(include_str!(
        "../src/engine/fixtures/session_memory_trace.json"
    )))
    .expect("session-memory trace baseline must be exact JSON");

    let process = RuntimeProcessContext::initialize(shared_test_process_supervisor()).unwrap();
    let actual = session_memory_trace(&process).await;
    process.shutdown().await.unwrap();

    let actual = actual.unwrap();

    // Robustness invariants that must hold independently of the golden file.
    // A zero-config session injects the main-loop memory system part and
    // creates the main memory dir.
    assert_eq!(
        actual
            .pointer("/default/systemMemoryPartName")
            .and_then(RuntimeJson::as_str),
        Some(&RuntimeString::from("memory"))
    );
    assert_eq!(
        actual
            .pointer("/default/memoryDirExistsAfterTurn")
            .and_then(RuntimeJson::as_bool),
        Some(true)
    );
    // Disabling agent memory omits the system part and never touches disk.
    assert!(actual
        .pointer("/agentMemoryOff/systemMemoryPartName")
        .is_some_and(RuntimeJson::is_null));
    assert_eq!(
        actual
            .pointer("/agentMemoryOff/memoryDirExistsAfterTurn")
            .and_then(RuntimeJson::as_bool),
        Some(false)
    );

    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/engine/fixtures/session_memory_trace.json");
        let mut bytes = runtime_json::stringify_pretty(&actual).to_node_utf8();
        bytes.push(b'\n');
        std::fs::write(path, bytes).unwrap();
        return;
    }
    assert_eq!(actual, expected);
}
