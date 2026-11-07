//! Default tool list and model-visible schema regression tests. The
//! checked-in fixture is this project's own golden baseline for the
//! zero-config built-in pool, tool metadata, and provider tool specs.

use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimeString};
use klaus_agent_core::testing::tool_schema_trace::tool_schema_trace;

#[test]
fn default_tool_list_and_model_visible_specs_match_the_golden_baseline() {
    let expected = runtime_json::parse(
        &include_str!("../src/tools/builtins/fixtures/tool_schema_trace.json").into(),
    )
    .unwrap();

    let actual = tool_schema_trace();

    // Robustness invariants that must hold independently of the golden file.
    let tool_names: Vec<&RuntimeString> = actual
        .get("toolNames")
        .and_then(RuntimeJson::as_array)
        .expect("trace must record the zero-config tool names")
        .iter()
        .filter_map(RuntimeJson::as_str)
        .collect();
    assert!(!tool_names.is_empty());
    // Core built-ins must stay present in the zero-config pool.
    for name in ["bash", "edit", "read", "write"] {
        let name = RuntimeString::from(name);
        assert!(
            tool_names.contains(&&name),
            "zero-config pool must expose the {name:?} tool"
        );
    }
    // Every pooled tool has exactly one metadata entry and one provider spec.
    let metadata = actual
        .get("toolMetadata")
        .and_then(RuntimeJson::as_array)
        .expect("trace must record tool metadata");
    let specs = actual
        .get("toolSpecs")
        .and_then(RuntimeJson::as_array)
        .expect("trace must record provider tool specs");
    assert_eq!(metadata.len(), tool_names.len());
    assert_eq!(specs.len(), tool_names.len());
    assert!(tool_names.iter().any(|name| *name == "TaskCreate"));
    assert!(tool_names.iter().any(|name| *name == "TaskUpdate"));
    assert!(!tool_names
        .iter()
        .any(|name| *name == "todo_write" || *name == "update_plan"));

    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/tools/builtins/fixtures/tool_schema_trace.json");
        let mut bytes = runtime_json::stringify_pretty(&actual).to_node_utf8();
        bytes.push(b'\n');
        std::fs::write(path, bytes).unwrap();
        return;
    }
    assert_eq!(actual, expected);
}
