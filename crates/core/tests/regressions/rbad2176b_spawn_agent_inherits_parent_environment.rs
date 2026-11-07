//! Defect: spawn_agent rejected `cwd:""` plus `isolation:"worktree"` before spawning.
//! Root cause: model-controlled environment fields were exposed and empty cwd counted as explicit.
//! Fixed by: defect session bad2176b; task children now inherit the parent environment.

use klaus_agent_core::runtime_values::{runtime_json, RuntimeString};
use klaus_agent_core::tools::builtins::task::{create_task_tool, TaskToolOptions};

#[test]
fn spawn_agent_schema_inherits_parent_environment() {
    let tool = create_task_tool(TaskToolOptions::default());
    let schema = tool.input_schema();
    let properties = schema
        .get("properties")
        .and_then(|value| value.as_object())
        .expect("task schema properties");

    assert!(!properties.contains_key("cwd"));
    assert!(!properties.contains_key("isolation"));

    let input = runtime_json::parse(&RuntimeString::from(
        r#"{"model":"test-model","description":"inspect","prompt":"inspect the project","cwd":"","isolation":"worktree"}"#,
    ))
    .expect("valid task input");
    let parsed = tool
        .parse_input(&input)
        .expect("legacy payload must retain only supported task fields");
    assert!(parsed.get("cwd").is_none());
    assert!(parsed.get("isolation").is_none());
}
