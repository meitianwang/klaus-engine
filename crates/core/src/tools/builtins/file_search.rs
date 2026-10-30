//! The `file_search` [`Tool`]: a fuzzy filename/path finder over the engine's
//! [`FileIndex`] (subsequence ranking, `@`-mention typeahead semantics). Good
//! when the model remembers part of a name but not the exact path; for an exact
//! glob use `glob`, for file contents use `grep`. The index is ignore-aware and
//! rebuilt per call. Descriptions are original.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::fs::file_index::{EntryKind, FileIndex, FileIndexOptions};
use crate::fs::node_path::{project_runtime_path, resolve_runtime};
use crate::fs::ripgrep::to_relative_path;
use crate::runtime_values::{
    RuntimeJson, RuntimeNumber, RuntimeObject, RuntimePath, RuntimeString,
};
use crate::tools::build_tool::text_result;
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{
    PathTargets, PermissionPathKind, Tool, ToolContext, ToolProgress, ToolResult, ValidationResult,
};

const FILE_SEARCH_DESCRIPTION: &str = "Find files by a FUZZY path/name match (subsequence ranking, like @-mention typeahead) — best when you remember part of a name but not its exact path. Respects .gitignore (in a git repo) and .ignore/.rgignore. For exact glob patterns use glob; for file contents use grep.";
const INPUT_SCHEMA_PROPERTY_ORDER: &[&str] = &["query", "path", "limit"];

/// The `file_search` fuzzy finder tool.
pub struct FileSearchTool {
    input_schema: JsonSchema,
    max_results: usize,
}

impl FileSearchTool {
    fn new(max_results: usize) -> Self {
        FileSearchTool {
            input_schema: build_input_schema(max_results),
            max_results,
        }
    }
}

/// Construct the `file_search` tool as an erased registry-side [`Tool`] (default
/// cap 50).
pub fn create_file_search_tool() -> Arc<dyn Tool> {
    Arc::new(FileSearchTool::new(50))
}

/// Construct the `file_search` tool with a custom result cap.
pub fn create_file_search_tool_with_max_results(max_results: usize) -> Arc<dyn Tool> {
    Arc::new(FileSearchTool::new(max_results))
}

/// Build the input JSON Schema the model sees; the `limit` description carries the
/// tool's configured default.
fn build_input_schema(max_results: usize) -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Fuzzy filename/path pattern, e.g. \"fileidx\" to find \"fs/fileIndex.ts\"."
            },
            "path": {
                "description": "Directory to search under. Omit to use the current working directory.",
                "type": "string"
            },
            "limit": {
                "description": format!("Max results to return (default {max_results})."),
                "type": "integer",
                "exclusiveMinimum": 0,
                "maximum": 9_007_199_254_740_991_i64
            }
        },
        "required": ["query"],
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .expect("file_search schema has an object root")
}

fn runtime_object(fields: impl IntoIterator<Item = (&'static str, RuntimeJson)>) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    for (key, value) in fields {
        object.insert(key, value);
    }
    RuntimeJson::Object(object)
}

fn runtime_number(value: usize) -> RuntimeJson {
    RuntimeJson::Number(RuntimeNumber::from_f64(value as f64))
}

fn join_runtime(values: &[RuntimeString], separator: &str) -> RuntimeString {
    let mut output = RuntimeString::default();
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            output = output.concat(&RuntimeString::from(separator));
        }
        output = output.concat(value);
    }
    output
}

fn ui_string(value: &RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8()).expect("Node UTF-8 UI projection is valid UTF-8")
}

#[async_trait]
impl Tool for FileSearchTool {
    fn static_name(&self) -> &str {
        "file_search"
    }

    fn static_description(&self) -> &str {
        FILE_SEARCH_DESCRIPTION
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn input_schema_property_order(
        &self,
        object_path: &[String],
    ) -> Option<&'static [&'static str]> {
        object_path
            .is_empty()
            .then_some(INPUT_SCHEMA_PROPERTY_ORDER)
    }

    fn max_result_size_chars(&self) -> Option<u64> {
        Some(100_000)
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        true
    }

    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        Some(false)
    }

    fn permission_path_kind(&self) -> Option<PermissionPathKind> {
        Some(PermissionPathKind::Read)
    }

    fn get_path(&self, input: &Json) -> Option<PathTargets> {
        input
            .get("path")
            .and_then(Json::as_str)
            .map(|path| PathTargets::One(RuntimePath::from(path.clone())))
    }

    fn get_raw_path(&self, input: &Json) -> Option<Json> {
        input.get("path").cloned()
    }

    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        let query = input
            .get("query")
            .and_then(Json::as_str)
            .cloned()
            .unwrap_or_default();
        Some(RuntimeString::from("Searching files for ").concat(&query))
    }

    async fn validate_input(&self, input: &Json, ctx: &ToolContext) -> Option<ValidationResult> {
        let path = match input.get("path").and_then(Json::as_str) {
            Some(p) => p,
            None => return Some(ValidationResult::Ok),
        };
        if path.contains("\0") {
            return Some(ValidationResult::Err {
                message: "Path contains null bytes".to_string(),
                error_code: Some(3),
            });
        }
        let path = RuntimePath::from(path.clone());
        let abs = resolve_runtime(&ctx.cwd, &[ctx.cwd.clone(), path.clone()])
            .expect("an explicit file-search path resolves exactly");
        if abs.as_runtime_string().starts_with("\\\\") || abs.as_runtime_string().starts_with("//")
        {
            return Some(ValidationResult::Ok);
        }
        let projected = match project_runtime_path(&abs, "path") {
            Ok(path) => path,
            Err(error) => {
                return Some(ValidationResult::Fatal {
                    error: ToolError::fatal(ui_string(error.message())),
                })
            }
        };
        match tokio::fs::metadata(projected).await {
            Ok(m) => {
                if m.is_dir() {
                    Some(ValidationResult::Ok)
                } else {
                    Some(ValidationResult::Err {
                        message: format!(
                            "Path is not a directory: {}",
                            ui_string(path.as_runtime_string())
                        ),
                        error_code: Some(2),
                    })
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Some(ValidationResult::Err {
                message: format!(
                    "Directory does not exist: {}. Paths are resolved against the current working directory: {}.",
                    ui_string(path.as_runtime_string()),
                    ui_string(ctx.cwd.as_runtime_string())
                ),
                error_code: Some(1),
            }),
            Err(_) => Some(ValidationResult::Ok),
        }
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let query = input
            .get("query")
            .and_then(Json::as_str)
            .cloned()
            .unwrap_or_default();
        let root = match input.get("path").and_then(Json::as_str) {
            Some(path) => resolve_runtime(
                &ctx.cwd,
                &[ctx.cwd.clone(), RuntimePath::from(path.clone())],
            )
            .expect("an explicit file-search root resolves exactly"),
            None => ctx.cwd.clone(),
        };
        let limit = input
            .get("limit")
            .and_then(Json::as_u64)
            .map(|n| n as usize)
            .unwrap_or(self.max_results);

        // Build the index fresh per call (no watcher); git ls-files is fast on large repos.
        let index = FileIndex::create(
            ctx.process_context.clone(),
            FileIndexOptions {
                root: root.clone(),
                watch: false,
                ..Default::default()
            },
        )
        .await;

        // Over-fetch by one to detect truncation.
        let hits = index.search(&query, limit + 1);
        let truncated = hits.len() > limit;
        let mut results_json: Vec<Json> = Vec::new();
        let mut lines: Vec<RuntimeString> = Vec::new();
        for h in hits.iter().take(limit) {
            // FileIndex paths are POSIX-relative to `root`; rebase to absolute, then
            // re-relativize to cwd for the model.
            let absolute =
                resolve_runtime(&root, &[root.clone(), RuntimePath::from(h.path.clone())])
                    .expect("a file-index result resolves exactly under its root");
            let rel = to_relative_path(&absolute, &ctx.cwd);
            let type_str = match h.kind {
                EntryKind::File => "file",
                EntryKind::Dir => "dir",
            };
            results_json.push(runtime_object([
                ("path", RuntimeJson::String(rel.as_runtime_string().clone())),
                ("type", RuntimeJson::String(RuntimeString::from(type_str))),
            ]));
            // A directory hit is shown with a trailing slash so it reads as one.
            lines.push(if type_str == "dir" {
                rel.as_runtime_string().concat(&RuntimeString::from("/"))
            } else {
                rel.into_runtime_string()
            });
        }
        let num_results = results_json.len();
        let scanned = index.scanned_file_count();
        index.close();

        let output = runtime_object([
            ("numResults", runtime_number(num_results)),
            ("results", RuntimeJson::Array(results_json)),
            ("truncated", RuntimeJson::Bool(truncated)),
            ("scanned", RuntimeJson::Number(RuntimeNumber::from(scanned))),
        ]);
        if num_results == 0 {
            return Ok(text_result(
                output,
                RuntimeString::from("No files matched \"")
                    .concat(&query)
                    .concat(&RuntimeString::from("\".")),
                false,
            ));
        }
        let mut text_lines = lines;
        if truncated {
            text_lines.push(RuntimeString::from(
                "(more results — raise limit or refine the query.)",
            ));
        }
        let text = join_runtime(&text_lines, "\n");
        Ok(text_result(output, text, false))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::runtime_json;
    use crate::tools::builtins::test_ctx::make_ctx;
    use serde_json::Value;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn runtime_from_serde(value: Value) -> RuntimeJson {
        match value {
            Value::Null => RuntimeJson::Null,
            Value::Bool(value) => RuntimeJson::Bool(value),
            Value::Number(value) => {
                RuntimeJson::Number(RuntimeNumber::from_f64(value.as_f64().unwrap()))
            }
            Value::String(value) => RuntimeJson::String(RuntimeString::from(value)),
            Value::Array(values) => {
                RuntimeJson::Array(values.into_iter().map(runtime_from_serde).collect())
            }
            Value::Object(values) => {
                let mut object = RuntimeObject::new();
                for (key, value) in values {
                    object.insert(key, runtime_from_serde(value));
                }
                RuntimeJson::Object(object)
            }
        }
    }

    macro_rules! rjson {
        ($($json:tt)+) => {
            runtime_from_serde(serde_json::json!($($json)+))
        };
    }

    fn rs(value: impl Into<RuntimeString>) -> RuntimeString {
        value.into()
    }

    fn temp_dir() -> PathBuf {
        let mut dir = std::env::temp_dir();
        let unique = format!(
            "ae-fsearch-tool-{}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        dir.push(unique);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn text_of(res: &ToolResult) -> String {
        match &res.serializable.model_content[0] {
            crate::schemas::content::OutputBlock::Text(t) => ui_string(&t.text),
            _ => panic!("expected a text block"),
        }
    }

    fn paths(res: &ToolResult) -> Vec<String> {
        res.serializable
            .output
            .get("results")
            .and_then(Json::as_array)
            .unwrap()
            .iter()
            .map(|value| ui_string(value.get("path").unwrap().as_str().unwrap()))
            .collect()
    }

    #[test]
    fn exposes_expected_metadata() {
        let tool = create_file_search_tool();
        assert_eq!(tool.name(), "file_search");
        assert_eq!(tool.description(), FILE_SEARCH_DESCRIPTION);
        assert_eq!(tool.permission_path_kind(), Some(PermissionPathKind::Read));
        assert_eq!(tool.max_result_size_chars(), Some(100_000));
        let input = rjson!({ "query": "x" });
        assert!(tool.is_read_only(&input));
        assert!(tool.is_concurrency_safe(&input));
        assert_eq!(tool.is_destructive(&input), Some(false));
        assert_eq!(tool.summarize(&input), Some(rs("Searching files for x")));
    }

    #[test]
    fn summary_preserves_lone_surrogate_query() {
        let input = runtime_json::parse(&RuntimeString::from(r#"{"query":"\ud800"}"#)).unwrap();
        let summary = create_file_search_tool().summarize(&input).unwrap();
        assert_eq!(
            summary,
            RuntimeString::from("Searching files for ")
                .concat(&RuntimeString::from_utf16(vec![0xd800]))
        );
    }

    #[test]
    fn limit_description_carries_the_default() {
        let tool = create_file_search_tool();
        let d = tool
            .static_input_schema()
            .get("properties")
            .and_then(|p| p.get("limit"))
            .and_then(|l| l.get("description"))
            .and_then(Value::as_str)
            .unwrap();
        assert_eq!(d, "Max results to return (default 50).");
        let tool2 = create_file_search_tool_with_max_results(7);
        let d2 = tool2
            .static_input_schema()
            .get("properties")
            .and_then(|p| p.get("limit"))
            .and_then(|l| l.get("description"))
            .and_then(Value::as_str)
            .unwrap();
        assert_eq!(d2, "Max results to return (default 7).");
    }

    #[tokio::test]
    async fn fuzzy_matches_a_subsequence_query() {
        let dir = temp_dir();
        std::fs::create_dir_all(dir.join("src").join("fs")).unwrap();
        std::fs::write(dir.join("src").join("fs").join("fileindex.ts"), "x").unwrap();
        std::fs::write(dir.join("src").join("other.ts"), "x").unwrap();
        let cwd = dir.to_string_lossy().into_owned();
        let r = create_file_search_tool()
            .call(rjson!({ "query": "fidx" }), &make_ctx(&cwd), None)
            .await
            .unwrap();
        assert!(paths(&r).contains(&"src/fs/fileindex.ts".to_string()));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn returns_directories_with_a_trailing_slash() {
        let dir = temp_dir();
        std::fs::create_dir_all(dir.join("widgets")).unwrap();
        std::fs::write(dir.join("widgets").join("a.ts"), "x").unwrap();
        let cwd = dir.to_string_lossy().into_owned();
        let r = create_file_search_tool()
            .call(rjson!({ "query": "widgets" }), &make_ctx(&cwd), None)
            .await
            .unwrap();
        assert!(text_of(&r).contains("widgets/"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn respects_limit_and_reports_truncation() {
        let dir = temp_dir();
        for i in 0..10 {
            std::fs::write(dir.join(format!("match{i}.ts")), "x").unwrap();
        }
        let cwd = dir.to_string_lossy().into_owned();
        let r = create_file_search_tool()
            .call(
                rjson!({ "query": "match", "limit": 3 }),
                &make_ctx(&cwd),
                None,
            )
            .await
            .unwrap();
        assert_eq!(r.serializable.output.get("numResults").unwrap(), &rjson!(3));
        assert_eq!(
            r.serializable.output.get("truncated").unwrap(),
            &rjson!(true)
        );
        assert!(text_of(&r).contains("more results"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn reports_no_matches() {
        let dir = temp_dir();
        std::fs::write(dir.join("a.ts"), "x").unwrap();
        let cwd = dir.to_string_lossy().into_owned();
        let r = create_file_search_tool()
            .call(rjson!({ "query": "zzzznomatch" }), &make_ctx(&cwd), None)
            .await
            .unwrap();
        assert_eq!(r.serializable.output.get("numResults").unwrap(), &rjson!(0));
        assert!(text_of(&r).contains("No files matched"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn searches_under_a_subdirectory_and_re_relativizes() {
        let dir = temp_dir();
        std::fs::create_dir_all(dir.join("sub")).unwrap();
        std::fs::write(dir.join("sub").join("target.ts"), "x").unwrap();
        std::fs::write(dir.join("toplevel.ts"), "x").unwrap();
        let cwd = dir.to_string_lossy().into_owned();
        let r = create_file_search_tool()
            .call(
                rjson!({ "query": "target", "path": "sub" }),
                &make_ctx(&cwd),
                None,
            )
            .await
            .unwrap();
        assert!(paths(&r).contains(&"sub/target.ts".to_string()));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn parsed_lone_surrogate_root_projects_for_file_index_io() {
        let dir = temp_dir();
        let actual = dir.join("\u{fffd}");
        std::fs::create_dir_all(&actual).unwrap();
        std::fs::write(actual.join("target.ts"), "x").unwrap();
        let cwd = dir.to_string_lossy().into_owned();
        let input = runtime_json::parse(&RuntimeString::from(
            r#"{"query":"target","path":"\ud800"}"#,
        ))
        .unwrap();

        let result = create_file_search_tool()
            .call(input, &make_ctx(&cwd), None)
            .await
            .unwrap();

        assert_eq!(result.serializable.is_error, Some(false));
        assert_eq!(
            runtime_json::stringify(result.serializable.output.get("results").unwrap()),
            r#"[{"path":"\ud800/target.ts","type":"file"}]"#
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn permission_paths_keep_surrogate_and_replacement_identities_distinct() {
        let tool = create_file_search_tool();
        let input = |path: RuntimeString| {
            runtime_object([
                ("query", Json::String(RuntimeString::from("x"))),
                ("path", Json::String(path)),
            ])
        };
        let high = tool.get_path(&input(RuntimeString::from_utf16(vec![0xd800])));
        let low = tool.get_path(&input(RuntimeString::from_utf16(vec![0xdc00])));
        let replacement = tool.get_path(&input(RuntimeString::from("\u{fffd}")));
        assert_ne!(high, low);
        assert_ne!(high, replacement);
        assert_ne!(low, replacement);
    }

    #[tokio::test]
    async fn validate_input_rejects_non_existent_path() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let missing = dir.join("nope").to_string_lossy().into_owned();
        let v = create_file_search_tool()
            .validate_input(&rjson!({ "query": "x", "path": missing }), &make_ctx(&cwd))
            .await;
        assert!(matches!(v, Some(ValidationResult::Err { .. })));
        std::fs::remove_dir_all(&dir).ok();
    }
}
