//! Tool-result shaping and truncation regression tests. The checked-in
//! fixture is this project's own golden baseline for spill previews, disk
//! spill paths, failed-spill fallback, and per-message budget trimming.

use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimeString};
use klaus_agent_core::testing::tool_result_trace::tool_result_trace;

#[tokio::test]
async fn tool_result_shaping_and_truncation_match_the_golden_baseline() {
    let expected = runtime_json::parse(&RuntimeString::from(include_str!(
        "../src/tools/fixtures/tool_result_trace.json"
    )))
    .unwrap();

    let actual = tool_result_trace().await.unwrap();

    // Robustness invariants that must hold independently of the golden file.
    // Oversized results spill to disk with a bounded preview and a real path.
    assert!(actual
        .pointer("/oversizedSpill/spilledPath")
        .and_then(RuntimeJson::as_str)
        .is_some_and(|path| !path.is_empty()));
    assert!(actual
        .pointer("/oversizedSpill/diskTextUnits")
        .and_then(RuntimeJson::as_u64)
        .is_some_and(|units| units > 0));
    // A failed spill must fall back to the full in-memory output instead of
    // silently dropping content.
    assert!(actual
        .pointer("/failedSpill/content")
        .and_then(RuntimeJson::as_array)
        .is_some_and(|content| !content.is_empty()));
    // Per-message aggregate trimming stays covered.
    assert!(actual.get("perMessageBudget").is_some_and(|v| !v.is_null()));

    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/tools/fixtures/tool_result_trace.json");
        let mut bytes = runtime_json::stringify_pretty(&actual).to_node_utf8();
        bytes.push(b'\n');
        std::fs::write(path, bytes).unwrap();
        return;
    }
    assert_eq!(actual, expected);
}
