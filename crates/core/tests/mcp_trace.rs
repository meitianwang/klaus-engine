//! MCP resource/prompt/search/wrapper regression tests. The checked-in
//! fixture is this project's own golden baseline covering model-visible
//! outputs, progress events, UTF-16 truncation, and persisted blob paths.

use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimeString};
use klaus_agent_core::testing::mcp_trace::mcp_trace;
use klaus_agent_core::testing::shared_test_process_supervisor;

#[tokio::test]
async fn mcp_resources_prompts_search_and_wrapper_match_the_golden_baseline() {
    let expected = runtime_json::parse(&RuntimeString::from_utf8(include_str!(
        "../src/mcp/fixtures/mcp_trace.json"
    )))
    .expect("MCP trace baseline must be exact JSON");

    let process = RuntimeProcessContext::initialize(shared_test_process_supervisor()).unwrap();
    let actual = mcp_trace(&process).await;
    process.shutdown().await.unwrap();

    let actual = actual.unwrap();

    // Robustness invariants that must hold independently of the golden file.
    for section in ["resources", "prompts", "search", "wrappedTool"] {
        assert!(
            actual.get(section).is_some_and(|v| !v.is_null()),
            "trace must cover the {section} surface"
        );
    }
    // MCP server progress must reach the outward tool event stream.
    assert!(actual
        .pointer("/wrappedTool/progressEvents")
        .and_then(RuntimeJson::as_array)
        .is_some_and(|events| !events.is_empty()));
    // Wrapped tool results carry the MCP metadata through to the caller.
    assert!(actual
        .pointer("/wrappedTool/result/mcpMeta")
        .is_some_and(|meta| !meta.is_null()));

    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/mcp/fixtures/mcp_trace.json");
        let mut bytes = runtime_json::stringify_pretty(&actual).to_node_utf8();
        bytes.push(b'\n');
        std::fs::write(path, bytes).unwrap();
        return;
    }
    assert_eq!(actual, expected);
}
