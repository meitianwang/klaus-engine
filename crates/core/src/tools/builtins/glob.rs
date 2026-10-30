//! The `glob` [`Tool`]: fast file search by glob pattern, powered by ripgrep
//! (`rg --files --glob … --sort=modified`). Hidden files INCLUDED by default
//! (`KLAUS_GLOB_HIDDEN=false` to exclude), `.gitignore` NOT respected by default
//! (`KLAUS_GLOB_NO_IGNORE=false` to respect), results sorted by mtime
//! OLDEST-first, capped at 100. Descriptions/messages are original; the rg flags,
//! sort order, defaults, and output shape are reproduced exactly.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::fs::node_path::{
    dirname_runtime, is_absolute_runtime, project_runtime_path, resolve_runtime,
};
use crate::fs::ripgrep::{
    default_search_roots, expand_path, is_env_truthy, node_stat_error_message, run_task_ripgrep,
    to_relative_path, RipgrepError,
};
use crate::runtime_values::{
    RuntimeJson, RuntimeNumber, RuntimeObject, RuntimePath, RuntimeString,
};
use crate::tools::build_tool::text_result;
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{
    PathTargets, PermissionPathKind, Tool, ToolContext, ToolProgress, ToolResult, ValidationResult,
};

/// Platform path separator (mirrors node:path `sep`).
#[cfg(windows)]
const SEP: u16 = b'\\' as u16;
#[cfg(not(windows))]
const SEP: u16 = b'/' as u16;

const GLOB_DESCRIPTION: &str = "Fast file search by glob pattern, powered by ripgrep. Returns paths sorted by modification time (oldest first). Supports \"**\" for any depth, \"*\"/\"?\" wildcards, character classes, and \"{a,b}\" alternation. Hidden files are included and .gitignore is not applied by default. When the current collaboration mode permits delegation, a bounded exploration subtask can keep a broad search focused and reduce raw file output in the parent context. Keep a short or dependent search in the current agent.";

/// POSIX `path.basename` for the no-metachar branch of glob base extraction,
/// operating directly on exact JavaScript UTF-16 code units.
fn basename(pattern: &RuntimeString) -> RuntimeString {
    let units = pattern.as_utf16();
    let end = units
        .iter()
        .rposition(|unit| *unit != u16::from(b'/'))
        .map_or(0, |index| index + 1);
    let start = units[..end]
        .iter()
        .rposition(|unit| *unit == u16::from(b'/'))
        .map_or(0, |index| index + 1);
    pattern.slice_utf16(start..end)
}

/// Split a glob into its static base directory and the relative pattern under it:
/// everything before the first glob metachar (`* ? [ {`) up to the last path
/// separator is the base; the rest is the pattern.
fn extract_glob_base_directory(pattern: &RuntimeString) -> (RuntimePath, RuntimeString) {
    let units = pattern.as_utf16();
    let meta = units.iter().position(|unit| {
        matches!(*unit, value if value == u16::from(b'*')
            || value == u16::from(b'?')
            || value == u16::from(b'[')
            || value == u16::from(b'{'))
    });
    let idx = match meta {
        None => {
            return (
                dirname_runtime(&RuntimePath::from(pattern.clone())),
                basename(pattern),
            )
        }
        Some(i) => i,
    };
    let static_prefix = &units[..idx];
    let last_slash = static_prefix
        .iter()
        .rposition(|unit| *unit == u16::from(b'/'));
    let last_sep = static_prefix.iter().rposition(|unit| *unit == SEP);
    let last = match (last_slash, last_sep) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    };
    let ls = match last {
        None => return (RuntimePath::default(), pattern.clone()),
        Some(i) => i,
    };
    let mut base_dir = pattern.slice_utf16(..ls);
    let relative_pattern = pattern.slice_utf16(ls + 1..);
    // Root pattern (e.g. `/*.txt`): the separator was at index 0.
    if base_dir.is_empty() && ls == 0 {
        base_dir = RuntimeString::from("/");
    }
    // Windows `C:` means "current dir on C", not the drive root — append a separator.
    #[cfg(windows)]
    {
        let b = base_dir.as_utf16();
        if b.len() == 2 && (b[0] as u8).is_ascii_alphabetic() && b[1] == u16::from(b':') {
            base_dir = base_dir.concat(&RuntimeString::from_utf16(vec![SEP]));
        }
    }
    (RuntimePath::from(base_dir), relative_pattern)
}

/// The empty-result structured output, sharing the same shape as a real run.
fn glob_empty_output(start: &std::time::Instant) -> Json {
    runtime_object([
        (
            "durationMs",
            runtime_number(start.elapsed().as_millis() as f64),
        ),
        ("numFiles", runtime_number(0.0)),
        ("filenames", RuntimeJson::Array(Vec::new())),
        ("truncated", RuntimeJson::Bool(false)),
    ])
}

fn runtime_object(fields: impl IntoIterator<Item = (&'static str, RuntimeJson)>) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    for (key, value) in fields {
        object.insert(key, value);
    }
    RuntimeJson::Object(object)
}

fn runtime_number(value: f64) -> RuntimeJson {
    RuntimeJson::Number(RuntimeNumber::from_f64(value))
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

/// The `glob` file-search tool.
pub struct GlobTool {
    input_schema: JsonSchema,
    max_results: usize,
}

impl GlobTool {
    fn new(max_results: usize) -> Self {
        GlobTool {
            input_schema: build_input_schema(),
            max_results,
        }
    }
}

/// Construct the `glob` tool as an erased registry-side [`Tool`] (default cap 100).
pub fn create_glob_tool() -> Arc<dyn Tool> {
    Arc::new(GlobTool::new(100))
}

/// Construct the `glob` tool with a custom result cap.
pub fn create_glob_tool_with_max_results(max_results: usize) -> Arc<dyn Tool> {
    Arc::new(GlobTool::new(max_results))
}

/// Build the input JSON Schema the model sees.
fn build_input_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "pattern": {
                "type": "string",
                "description": "Glob pattern to match files against, e.g. \"**/*.ts\" or \"src/**/*.{js,jsx}\"."
            },
            "path": {
                "description": "Directory to search in. Omit to use the current working directory — do NOT pass the string \"undefined\" or \"null\", just leave it out. Must be an existing directory if given.",
                "type": "string"
            }
        },
        "required": ["pattern"],
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .expect("glob schema has an object root")
}

#[async_trait]
impl Tool for GlobTool {
    fn static_name(&self) -> &str {
        "glob"
    }

    fn static_description(&self) -> &str {
        GLOB_DESCRIPTION
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
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
        if let Some(path) = input.get("path").and_then(Json::as_str) {
            return Some(PathTargets::One(RuntimePath::from(path.clone())));
        }
        let pattern = input
            .get("pattern")
            .and_then(Json::as_str)
            .cloned()
            .unwrap_or_default();
        let (base_dir, _) = extract_glob_base_directory(&pattern);
        if base_dir.as_runtime_string().is_empty() {
            None
        } else {
            Some(PathTargets::One(base_dir))
        }
    }

    fn get_raw_path(&self, input: &Json) -> Option<Json> {
        if let Some(path) = input.get("path").filter(|path| !path.is_null()) {
            return Some(path.clone());
        }
        let pattern = input
            .get("pattern")
            .and_then(Json::as_str)
            .cloned()
            .unwrap_or_default();
        let (base_dir, _) = extract_glob_base_directory(&pattern);
        (!base_dir.as_runtime_string().is_empty())
            .then_some(Json::String(base_dir.into_runtime_string()))
    }

    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        let pattern = input
            .get("pattern")
            .and_then(Json::as_str)
            .cloned()
            .unwrap_or_default();
        Some(RuntimeString::from("Finding ").concat(&pattern))
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
        let abs = match expand_path(path, &ctx.cwd) {
            Ok(a) => a,
            Err(_) => {
                return Some(ValidationResult::Err {
                    message: "Path contains null bytes".to_string(),
                    error_code: Some(3),
                })
            }
        };
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
        let pinned_snapshot = match &ctx.permission.pinned_memory_root {
            Some(root) => match root.snapshot_search(&projected) {
                Ok(snapshot) => Some(snapshot),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Some(ValidationResult::Err {
                        message: format!(
                            "Directory does not exist: {}. Paths are resolved against the current working directory: {}.",
                            ui_string(path),
                            ui_string(ctx.cwd.as_runtime_string())
                        ),
                        error_code: Some(1),
                    })
                }
                Err(error) => {
                    return Some(ValidationResult::Fatal {
                        error: ToolError::fatal(format!(
                            "pinned memory glob validation failed: {error}"
                        )),
                    })
                }
            },
            None => None,
        };
        let projected = pinned_snapshot
            .as_ref()
            .map(|snapshot| snapshot.path())
            .unwrap_or(projected.as_path());
        match tokio::fs::metadata(projected).await {
            Ok(m) => {
                if m.is_dir() {
                    Some(ValidationResult::Ok)
                } else {
                    Some(ValidationResult::Err {
                        message: format!("Path is not a directory: {}", ui_string(path)),
                        error_code: Some(2),
                    })
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Some(ValidationResult::Err {
                message: format!(
                    "Directory does not exist: {}. Paths are resolved against the current working directory: {}.",
                    ui_string(path),
                    ui_string(ctx.cwd.as_runtime_string())
                ),
                error_code: Some(1),
            }),
            Err(error) => Some(ValidationResult::Fatal {
                error: ToolError::fatal(ui_string(&node_stat_error_message(&error, &abs))),
            }),
        }
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let start = std::time::Instant::now();
        let pattern = input
            .get("pattern")
            .and_then(Json::as_str)
            .cloned()
            .unwrap_or_default();
        let mut search_dir = match input.get("path").and_then(Json::as_str) {
            Some(path) => expand_path(path, &ctx.cwd)?,
            None => ctx.cwd.clone(),
        };
        // The search root is explicit when the caller passed `path` or an
        // absolute pattern (which carries its own base dir); otherwise the
        // DEFAULT covers the whole workspace (cwd + extra roots) below.
        let mut search_dir_explicit = input.get("path").and_then(Json::as_str).is_some();
        let mut search_pattern = pattern.clone();
        // An absolute pattern carries its own base directory; rg's --glob needs a relative one.
        if is_absolute_runtime(&RuntimePath::from(pattern.clone())) {
            let (base_dir, relative_pattern) = extract_glob_base_directory(&pattern);
            if !base_dir.as_runtime_string().is_empty() {
                search_dir = base_dir;
                search_pattern = relative_pattern;
                search_dir_explicit = true;
            }
        }

        let no_ignore = {
            let v = std::env::var("KLAUS_GLOB_NO_IGNORE").unwrap_or_default();
            let v = if v.is_empty() { "true".to_string() } else { v };
            is_env_truthy(Some(&v))
        };
        let hidden = {
            let v = std::env::var("KLAUS_GLOB_HIDDEN").unwrap_or_default();
            let v = if v.is_empty() { "true".to_string() } else { v };
            is_env_truthy(Some(&v))
        };
        let mut args: Vec<RuntimeString> = vec![
            RuntimeString::from("--files"),
            RuntimeString::from("--glob"),
            search_pattern,
            RuntimeString::from("--sort=modified"),
        ];
        if no_ignore {
            args.push(RuntimeString::from("--no-ignore"));
        }
        if hidden {
            args.push(RuntimeString::from("--hidden"));
        }

        let projected_search_dir = project_runtime_path(&search_dir, "path")
            .map_err(|error| ToolError::new(ui_string(error.message())))?;
        let pinned_search =
            match &ctx.permission.pinned_memory_root {
                Some(root) => Some(root.snapshot_search(&projected_search_dir).map_err(
                    |error| ToolError::new(format!("pinned memory glob failed: {error}")),
                )?),
                None => None,
            };
        let rg_search_dir = pinned_search
            .as_ref()
            .map(|snapshot| snapshot.runtime_path())
            .unwrap_or_else(|| search_dir.clone());
        // Same rule as grep: pinned → snapshot root; explicit path/pattern base →
        // that dir; otherwise DEFAULT across the workspace's roots in one pass.
        let rg_targets: Vec<RuntimePath> = match &pinned_search {
            Some(_) => vec![rg_search_dir.clone()],
            None if search_dir_explicit => vec![rg_search_dir.clone()],
            None => default_search_roots(&ctx.cwd, &ctx.permission.additional_writable_roots),
        };
        let all_paths = match run_task_ripgrep(ctx, &args, &rg_targets).await {
            Ok(r) => r,
            Err(RipgrepError::Unavailable(msg)) => {
                return Ok(text_result(glob_empty_output(&start), msg, true))
            }
            Err(RipgrepError::Timeout { message, .. }) => {
                return Ok(text_result(glob_empty_output(&start), message, true))
            }
            Err(RipgrepError::Spawn(e)) => return Err(ToolError::new(e.to_string())),
        };
        let all_paths = match &pinned_search {
            Some(snapshot) => all_paths
                .into_iter()
                .map(|path| snapshot.restore_search_output(&path))
                .collect(),
            None => all_paths,
        };

        let absolute_paths: Vec<RuntimePath> = all_paths
            .into_iter()
            .map(|p| {
                let path = RuntimePath::from(p);
                if is_absolute_runtime(&path) {
                    path
                } else {
                    resolve_runtime(&search_dir, &[search_dir.clone(), path])
                        .expect("a ripgrep result resolves exactly under its search root")
                }
            })
            .collect();
        let truncated = absolute_paths.len() > self.max_results;
        let filenames: Vec<RuntimeString> = absolute_paths
            .iter()
            .take(self.max_results)
            .map(|path| to_relative_path(path, &ctx.cwd).into_runtime_string())
            .collect();
        let output = runtime_object([
            (
                "durationMs",
                runtime_number(start.elapsed().as_millis() as f64),
            ),
            ("numFiles", runtime_number(filenames.len() as f64)),
            (
                "filenames",
                RuntimeJson::Array(filenames.iter().cloned().map(RuntimeJson::String).collect()),
            ),
            ("truncated", RuntimeJson::Bool(truncated)),
        ]);
        if filenames.is_empty() {
            return Ok(text_result(
                output,
                RuntimeString::from("No files matched."),
                false,
            ));
        }
        let mut text_lines = filenames;
        if truncated {
            text_lines.push(RuntimeString::from(
                "(Results truncated — narrow the path or pattern for the rest.)",
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
            "ae-glob-tool-{}-{}-{}",
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

    fn rg_available() -> bool {
        std::process::Command::new("rg")
            .arg("--version")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    fn text_of(res: &ToolResult) -> String {
        match &res.serializable.model_content[0] {
            crate::schemas::content::OutputBlock::Text(t) => ui_string(&t.text),
            _ => panic!("expected a text block"),
        }
    }

    fn filenames(res: &ToolResult) -> Vec<String> {
        res.serializable
            .output
            .get("filenames")
            .and_then(Json::as_array)
            .unwrap()
            .iter()
            .map(|value| ui_string(value.as_str().unwrap()))
            .collect()
    }

    #[test]
    fn exposes_expected_metadata() {
        let tool = create_glob_tool();
        assert_eq!(tool.name(), "glob");
        assert_eq!(tool.description(), GLOB_DESCRIPTION);
        assert_eq!(tool.permission_path_kind(), Some(PermissionPathKind::Read));
        assert_eq!(tool.max_result_size_chars(), Some(100_000));
        let input = rjson!({ "pattern": "**/*.ts" });
        assert!(tool.is_read_only(&input));
        assert!(tool.is_concurrency_safe(&input));
        assert_eq!(tool.is_destructive(&input), Some(false));
        assert_eq!(tool.summarize(&input), Some(rs("Finding **/*.ts")));
    }

    #[test]
    fn summary_preserves_lone_surrogate_pattern() {
        let input = runtime_json::parse(&RuntimeString::from(r#"{"pattern":"\ud800"}"#)).unwrap();
        let summary = create_glob_tool().summarize(&input).unwrap();
        assert_eq!(
            summary,
            RuntimeString::from("Finding ").concat(&RuntimeString::from_utf16(vec![0xd800]))
        );
    }

    #[test]
    fn input_schema_shape() {
        let tool = create_glob_tool();
        let schema = tool.static_input_schema();
        assert_eq!(schema.get("type").and_then(Value::as_str), Some("object"));
        assert_eq!(schema.get("required").unwrap(), &json!(["pattern"]));
        assert_eq!(schema.get("additionalProperties").unwrap(), &json!(false));
    }

    #[test]
    fn extract_glob_base_directory_cases() {
        assert_eq!(
            extract_glob_base_directory(&RuntimeString::from("/etc/**/*.conf")),
            (RuntimePath::from("/etc"), RuntimeString::from("**/*.conf"))
        );
        assert_eq!(
            extract_glob_base_directory(&RuntimeString::from("*.ts")),
            (RuntimePath::default(), RuntimeString::from("*.ts"))
        );
        assert_eq!(
            extract_glob_base_directory(&RuntimeString::from("/*.txt")),
            (RuntimePath::from("/"), RuntimeString::from("*.txt"))
        );
        assert_eq!(
            extract_glob_base_directory(&RuntimeString::from("src/pkg/*.rs")),
            (RuntimePath::from("src/pkg"), RuntimeString::from("*.rs"))
        );
    }

    #[test]
    fn get_path_falls_back_to_the_glob_base_directory() {
        let tool = create_glob_tool();
        assert_eq!(
            tool.get_path(&rjson!({ "pattern": "x", "path": "src" })),
            Some(PathTargets::One(RuntimePath::from("src")))
        );
        assert_eq!(
            tool.get_path(&rjson!({ "pattern": "/etc/**/*.conf" })),
            Some(PathTargets::One(RuntimePath::from("/etc")))
        );
        assert_eq!(tool.get_path(&rjson!({ "pattern": "*.ts" })), None);
    }

    #[tokio::test]
    async fn matches_files_by_pattern() {
        let _external = crate::test_support::external_process_lock().await;
        if !rg_available() {
            return;
        }
        let dir = temp_dir();
        std::fs::write(dir.join("one.ts"), "").unwrap();
        std::fs::write(dir.join("two.ts"), "").unwrap();
        std::fs::write(dir.join("note.md"), "").unwrap();
        let cwd = dir.to_string_lossy().into_owned();
        let r = create_glob_tool()
            .call(rjson!({ "pattern": "**/*.ts" }), &make_ctx(&cwd), None)
            .await
            .unwrap();
        let names = filenames(&r);
        assert!(names.contains(&"one.ts".to_string()));
        assert!(names.contains(&"two.ts".to_string()));
        assert!(!names.contains(&"note.md".to_string()));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn parsed_lone_surrogate_path_and_pattern_project_for_ripgrep() {
        let _external = crate::test_support::external_process_lock().await;
        let dir = temp_dir();
        let actual = dir.join("\u{fffd}");
        std::fs::create_dir_all(&actual).unwrap();
        std::fs::write(actual.join("\u{fffd}.txt"), "x").unwrap();
        let cwd = dir.to_string_lossy().into_owned();
        let input = runtime_json::parse(&RuntimeString::from(
            r#"{"pattern":"\ud801.txt","path":"\ud800"}"#,
        ))
        .unwrap();

        let result = create_glob_tool()
            .call(input, &make_ctx(&cwd), None)
            .await
            .unwrap();

        assert_eq!(result.serializable.is_error, Some(false));
        assert_eq!(
            runtime_json::stringify(result.serializable.output.get("filenames").unwrap()),
            r#"["�/�.txt"]"#
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn logical_glob_paths_do_not_alias_surrogates_with_replacement() {
        let tool = create_glob_tool();
        let input = |path: RuntimeString| {
            runtime_object([
                ("pattern", Json::String(RuntimeString::from("*"))),
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
    async fn truncates_at_the_result_cap() {
        let _external = crate::test_support::external_process_lock().await;
        if !rg_available() {
            return;
        }
        let dir = temp_dir();
        for i in 0..5 {
            std::fs::write(dir.join(format!("f{i}.ts")), "").unwrap();
        }
        let cwd = dir.to_string_lossy().into_owned();
        let r = create_glob_tool_with_max_results(3)
            .call(rjson!({ "pattern": "*.ts" }), &make_ctx(&cwd), None)
            .await
            .unwrap();
        assert_eq!(
            r.serializable.output.get("truncated").unwrap(),
            &rjson!(true)
        );
        assert_eq!(r.serializable.output.get("numFiles").unwrap(), &rjson!(3));
        assert!(text_of(&r).contains("Results truncated"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn no_match_message() {
        let _external = crate::test_support::external_process_lock().await;
        if !rg_available() {
            return;
        }
        let dir = temp_dir();
        std::fs::write(dir.join("only.md"), "").unwrap();
        let cwd = dir.to_string_lossy().into_owned();
        let r = create_glob_tool()
            .call(rjson!({ "pattern": "*.ts" }), &make_ctx(&cwd), None)
            .await
            .unwrap();
        assert_eq!(r.serializable.output.get("numFiles").unwrap(), &rjson!(0));
        assert_eq!(text_of(&r), "No files matched.");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn validate_input_directory_checks() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let tool = create_glob_tool();
        // missing dir
        let missing = dir.join("nope").to_string_lossy().into_owned();
        match tool
            .validate_input(
                &rjson!({ "pattern": "*", "path": missing }),
                &make_ctx(&cwd),
            )
            .await
        {
            Some(ValidationResult::Err { message, .. }) => {
                assert!(message.contains("Directory does not exist"))
            }
            other => panic!("expected err, got {other:?}"),
        }
        // a file, not a directory
        std::fs::write(dir.join("afile.txt"), "").unwrap();
        let afile = dir.join("afile.txt").to_string_lossy().into_owned();
        match tool
            .validate_input(&rjson!({ "pattern": "*", "path": afile }), &make_ctx(&cwd))
            .await
        {
            Some(ValidationResult::Err { message, .. }) => {
                assert!(message.contains("not a directory"))
            }
            other => panic!("expected err, got {other:?}"),
        }
        // existing dir
        assert_eq!(
            tool.validate_input(
                &rjson!({ "pattern": "*", "path": cwd.clone() }),
                &make_ctx(&cwd)
            )
            .await,
            Some(ValidationResult::Ok)
        );
        // null bytes
        let nul = format!("{cwd}\0evil");
        match tool
            .validate_input(&rjson!({ "pattern": "*", "path": nul }), &make_ctx(&cwd))
            .await
        {
            Some(ValidationResult::Err { message, .. }) => assert!(message.contains("null bytes")),
            other => panic!("expected err, got {other:?}"),
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn validate_input_propagates_symlink_loop_as_node_stat_error() {
        use std::os::unix::fs::symlink;

        let dir = temp_dir();
        let loop_path = dir.join("loop");
        symlink("loop", &loop_path).unwrap();
        let cwd = dir.to_string_lossy().into_owned();
        let path = loop_path.to_string_lossy().into_owned();
        let tool = create_glob_tool();
        let Some(ValidationResult::Fatal { error }) = tool
            .validate_input(&rjson!({ "pattern": "*", "path": path }), &make_ctx(&cwd))
            .await
        else {
            panic!("ELOOP must return a typed fatal glob validation result");
        };
        assert_eq!(error.kind, crate::types::errors::ToolErrorKind::Fatal);
        assert_eq!(
            error.message,
            format!(
                "ELOOP: too many symbolic links encountered, stat '{}'",
                loop_path.display()
            )
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
