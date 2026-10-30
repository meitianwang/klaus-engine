//! Session persistence/resume/fork/rewind regression tests. The checked-in
//! fixture is this project's own golden baseline for the JSONL projection,
//! checkpoint ledger, sub-agent sidecars, fork seeding, and scoped rewind.

use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeString};
use klaus_agent_core::testing::session_persistence_trace::session_persistence_trace;
use klaus_agent_core::testing::shared_test_process_supervisor;

fn assert_exact_json(
    path: &RuntimeString,
    actual: &klaus_agent_core::runtime_values::RuntimeJson,
    expected: &klaus_agent_core::runtime_values::RuntimeJson,
) {
    use klaus_agent_core::runtime_values::RuntimeJson;

    match (actual, expected) {
        (RuntimeJson::Array(actual), RuntimeJson::Array(expected)) => {
            assert_eq!(actual.len(), expected.len(), "array length at {path:?}");
            for (index, (actual, expected)) in actual.iter().zip(expected).enumerate() {
                assert_exact_json(
                    &path.concat(&RuntimeString::from(format!("/{index}"))),
                    actual,
                    expected,
                );
            }
        }
        (RuntimeJson::Object(actual), RuntimeJson::Object(expected)) => {
            let actual_keys = actual.keys().collect::<Vec<_>>();
            let expected_keys = expected.keys().collect::<Vec<_>>();
            assert_eq!(actual_keys, expected_keys, "object keys at {path:?}");
            for (key, actual) in actual {
                assert_exact_json(
                    &path.concat(&RuntimeString::from("/")).concat(&key),
                    actual,
                    expected
                        .get(key.clone())
                        .expect("matching exact object key"),
                );
            }
        }
        _ => assert_eq!(actual, expected, "value at {path:?}"),
    }
}

#[tokio::test]
async fn session_persistence_resume_fork_rewind_match_the_golden_baseline() {
    let expected = runtime_json::parse(&RuntimeString::from(include_str!(
        "../src/store/fixtures/session_persistence_trace.json"
    )))
    .unwrap();

    let process = RuntimeProcessContext::initialize(shared_test_process_supervisor()).unwrap();
    let actual = session_persistence_trace(&process).await;
    process.shutdown().await.unwrap();

    let actual = actual.unwrap();

    // Robustness invariants that must hold independently of the golden file.
    use klaus_agent_core::runtime_values::RuntimeJson;
    // A reopened store must replay the persisted conversation.
    assert!(actual
        .pointer("/store/messages")
        .and_then(RuntimeJson::as_array)
        .is_some_and(|messages| !messages.is_empty()));
    assert!(actual
        .pointer("/store/checkpoints")
        .is_some_and(|checkpoints| !checkpoints.is_null()));
    // Fork seeding must produce an inherited transcript with placeholders for
    // unresolved tool uses.
    assert!(actual
        .pointer("/fork/transcript")
        .and_then(RuntimeJson::as_array)
        .is_some_and(|transcript| !transcript.is_empty()));
    // Scoped rewind must cover the `both` restore target and reject invalid
    // turn counts instead of corrupting state.
    assert!(actual.pointer("/rewind/both").is_some());
    assert!(actual.pointer("/rewind/invalid").is_some());

    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/store/fixtures/session_persistence_trace.json");
        let mut bytes = runtime_json::stringify_pretty(&actual).to_node_utf8();
        bytes.push(b'\n');
        std::fs::write(path, bytes).unwrap();
        return;
    }
    assert_exact_json(&RuntimeString::default(), &actual, &expected);
}
