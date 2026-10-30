//! The `ls` [`Tool`]: list a directory as an indented tree, depth-limited and
//! entry-capped. Skips `node_modules`/`.git`/`dist` and dotfiles unless `all` is
//! set; directories are marked with a trailing slash.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::fs::node_path::{project_runtime_path, resolve_runtime, runtime_string_from_native};
use crate::runtime_values::{
    RuntimeJson, RuntimeNumber, RuntimeObject, RuntimePath, RuntimeString,
};
use crate::tools::build_tool::text_result;
use crate::types::abort::AbortSignal;
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{
    PathTargets, PermissionPathKind, Tool, ToolContext, ToolProgress, ToolResult,
};

const LS_DESCRIPTION: &str = "List a directory as an indented tree. Skips node_modules/.git/dist and dotfiles unless `all` is set; directories are marked with a trailing slash.";
const INPUT_SCHEMA_PROPERTY_ORDER: &[&str] = &["path", "depth", "all"];

const SKIP_DIRS: [&str; 3] = ["node_modules", ".git", "dist"];

/// Walk state for a single `ls` render.
struct LsWalker<'s> {
    pinned_root: Option<Arc<crate::memory::pinned_root::PinnedMemoryRoot>>,
    max_depth: u64,
    all: bool,
    max_entries: usize,
    signal: &'s AbortSignal,
    lines: Vec<RuntimeString>,
    count: usize,
    truncated: bool,
}

impl<'s> LsWalker<'s> {
    fn walk<'a>(
        &'a mut self,
        dir: RuntimePath,
        depth: u64,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            if depth > self.max_depth || self.truncated || self.signal.is_cancelled() {
                return;
            }
            let projected = match project_runtime_path(&dir, "path") {
                Ok(path) => path,
                Err(_) => return,
            };
            let mut entries: Vec<(RuntimeString, bool)> = if let Some(root) = &self.pinned_root {
                match root.directory_entries(projected) {
                    Ok(entries) => entries
                        .into_iter()
                        .map(|(name, directory)| (runtime_string_from_native(&name), directory))
                        .collect(),
                    Err(_) => return,
                }
            } else {
                let mut rd = match tokio::fs::read_dir(projected).await {
                    Ok(r) => r,
                    Err(_) => return,
                };
                let mut entries: Vec<(RuntimeString, bool)> = Vec::new();
                while let Ok(Some(e)) = rd.next_entry().await {
                    let name = runtime_string_from_native(&e.file_name());
                    let is_dir = e.file_type().await.map(|t| t.is_dir()).unwrap_or(false);
                    entries.push((name, is_dir));
                }
                entries
            };
            // Directories first, then files; each group sorted by name.
            entries.sort_by(|a, b| {
                let ad = if a.1 { 0 } else { 1 };
                let bd = if b.1 { 0 } else { 1 };
                ad.cmp(&bd).then_with(|| a.0.cmp(&b.0))
            });

            for (name, is_dir) in entries {
                if self.truncated || self.signal.is_cancelled() {
                    return;
                }
                if !self.all && name.starts_with(".") {
                    continue;
                }
                if is_dir && !self.all && SKIP_DIRS.iter().any(|skip| &name == skip) {
                    continue;
                }
                if self.count >= self.max_entries {
                    self.truncated = true;
                    return;
                }
                self.count += 1;
                let indent = RuntimeString::from("  ".repeat((depth - 1) as usize));
                let line = indent.concat(&name).concat(&RuntimeString::from(if is_dir {
                    "/"
                } else {
                    ""
                }));
                self.lines.push(line);
                if is_dir {
                    let child = resolve_runtime(&dir, &[dir.clone(), RuntimePath::from(name)])
                        .expect("an explicit directory resolves its child exactly");
                    self.walk(child, depth + 1).await;
                }
            }
        })
    }
}

/// The `ls` directory-listing tool.
pub struct LsTool {
    input_schema: JsonSchema,
    max_entries: usize,
}

impl LsTool {
    fn new(max_entries: usize) -> Self {
        LsTool {
            input_schema: build_input_schema(),
            max_entries,
        }
    }
}

/// Construct the `ls` tool as an erased registry-side [`Tool`] (default cap 1000).
pub fn create_ls_tool() -> Arc<dyn Tool> {
    Arc::new(LsTool::new(1000))
}

/// Construct the `ls` tool with a custom entry cap.
pub fn create_ls_tool_with_max_entries(max_entries: usize) -> Arc<dyn Tool> {
    Arc::new(LsTool::new(max_entries))
}

/// Build the input JSON Schema the model sees.
fn build_input_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "path": {
                "description": "Directory to list (default: cwd)",
                "type": "string"
            },
            "depth": {
                "description": "Recursion depth (default: 2)",
                "type": "integer",
                "exclusiveMinimum": 0,
                "maximum": 9_007_199_254_740_991_i64
            },
            "all": {
                "description": "Include dotfiles and node_modules/.git/dist (default: false)",
                "type": "boolean"
            }
        },
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .expect("ls schema has an object root")
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

fn ui_path(value: &RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8()).expect("Node UTF-8 UI projection is valid UTF-8")
}

#[async_trait]
impl Tool for LsTool {
    fn static_name(&self) -> &str {
        "ls"
    }

    fn static_description(&self) -> &str {
        LS_DESCRIPTION
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
            .and_then(Json::as_string)
            .cloned()
            .map(RuntimePath::from)
            .map(PathTargets::One)
    }

    fn get_raw_path(&self, input: &Json) -> Option<Json> {
        input.get("path").cloned()
    }

    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        let path = input
            .get("path")
            .and_then(Json::as_string)
            .cloned()
            .unwrap_or_else(|| RuntimeString::from("."));
        Some(RuntimeString::from("Listing ").concat(&path))
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let path_input = input.get("path").and_then(Json::as_string);
        let logical = RuntimePath::from(
            path_input
                .cloned()
                .unwrap_or_else(|| RuntimeString::from(".")),
        );
        let base = resolve_runtime(&ctx.cwd, &[ctx.cwd.clone(), logical])
            .map_err(|error| ToolError::new(ui_path(error.message())))?;
        let max_depth = input.get("depth").and_then(Json::as_u64).unwrap_or(2);
        let all = input.get("all").and_then(Json::as_bool).unwrap_or(false);

        let projected_base = project_runtime_path(&base, "path")
            .map_err(|error| ToolError::new(ui_path(error.message())))?;
        let is_dir = if let Some(root) = &ctx.permission.pinned_memory_root {
            root.directory_entries(projected_base).is_ok()
        } else {
            matches!(tokio::fs::metadata(projected_base).await, Ok(m) if m.is_dir())
        };
        if !is_dir {
            let output = runtime_object([
                (
                    "path",
                    RuntimeJson::String(base.as_runtime_string().clone()),
                ),
                ("found", RuntimeJson::Bool(false)),
                ("entries", runtime_number(0)),
                ("truncated", RuntimeJson::Bool(false)),
            ]);
            let shown = path_input
                .cloned()
                .unwrap_or_else(|| RuntimeString::from("."));
            return Ok(text_result(
                output,
                RuntimeString::from("Not a directory: ").concat(&shown),
                true,
            ));
        }

        let mut walker = LsWalker {
            pinned_root: ctx.permission.pinned_memory_root.clone(),
            max_depth,
            all,
            max_entries: self.max_entries,
            signal: &ctx.signal,
            lines: Vec::new(),
            count: 0,
            truncated: false,
        };
        walker.walk(base.clone(), 1).await;

        let body = if !walker.lines.is_empty() {
            join_runtime(&walker.lines, "\n")
        } else {
            RuntimeString::from("(empty)")
        };
        let note = if walker.truncated {
            RuntimeString::from(format!("\n… [truncated at {} entries] …", self.max_entries))
        } else {
            RuntimeString::default()
        };
        let output = runtime_object([
            (
                "path",
                RuntimeJson::String(base.as_runtime_string().clone()),
            ),
            ("found", RuntimeJson::Bool(true)),
            ("entries", runtime_number(walker.count)),
            ("truncated", RuntimeJson::Bool(walker.truncated)),
        ]);
        let text = base
            .as_runtime_string()
            .concat(&RuntimeString::from("\n"))
            .concat(&body)
            .concat(&note);
        Ok(text_result(output, text, false))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::{runtime_codec, ScalarJson};
    use crate::tools::builtins::test_ctx::make_ctx;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn exact_json(value: serde_json::Value) -> RuntimeJson {
        runtime_codec::encode_value(
            &ScalarJson::validate_static_schema(value).expect("test fixture is scalar JSON"),
        )
        .expect("scalar test fixture has an exact runtime encoding")
    }

    macro_rules! json {
        ($($json:tt)+) => {
            exact_json(serde_json::json!($($json)+))
        };
    }

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_dir() -> PathBuf {
        let mut dir = std::env::temp_dir();
        let unique = format!(
            "ae-ls-tool-{}-{}-{}",
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
            crate::schemas::content::OutputBlock::Text(t) => {
                String::from_utf8(t.text.to_node_utf8()).unwrap()
            }
            _ => panic!("expected a text block"),
        }
    }

    #[test]
    fn exposes_expected_metadata() {
        let tool = create_ls_tool();
        assert_eq!(tool.name(), "ls");
        assert_eq!(tool.description(), LS_DESCRIPTION);
        assert_eq!(tool.permission_path_kind(), Some(PermissionPathKind::Read));
        assert_eq!(tool.max_result_size_chars(), None);
        let input = json!({});
        assert!(tool.is_read_only(&input));
        assert!(tool.is_concurrency_safe(&input));
        assert_eq!(tool.is_destructive(&input), Some(false));
        assert_eq!(
            tool.summarize(&input).as_ref(),
            Some(&RuntimeString::from("Listing ."))
        );
        assert_eq!(
            tool.summarize(&json!({ "path": "src" })).as_ref(),
            Some(&RuntimeString::from("Listing src"))
        );
    }

    #[test]
    fn input_schema_has_no_required_key() {
        let tool = create_ls_tool();
        let schema = tool.static_input_schema();
        assert_eq!(
            schema.get("type").and_then(serde_json::Value::as_str),
            Some("object")
        );
        assert!(schema.get("required").is_none());
        assert_eq!(
            schema.get("additionalProperties").unwrap(),
            &serde_json::json!(false)
        );
    }

    #[tokio::test]
    async fn lists_a_tree_marking_directories_and_skipping_dotfiles() {
        let dir = temp_dir();
        std::fs::create_dir(dir.join("src")).unwrap();
        std::fs::write(dir.join("src").join("index.ts"), "").unwrap();
        std::fs::write(dir.join("README.md"), "").unwrap();
        std::fs::create_dir(dir.join("node_modules")).unwrap();
        std::fs::write(dir.join("node_modules").join("ignored.js"), "").unwrap();
        std::fs::write(dir.join(".hidden"), "").unwrap();
        let cwd = dir.to_string_lossy().into_owned();

        let r = create_ls_tool()
            .call(json!({}), &make_ctx(&cwd), None)
            .await
            .unwrap();
        let text = text_of(&r);
        assert_eq!(r.serializable.is_error, Some(false));
        assert!(text.contains("src/"));
        assert!(text.contains("index.ts"));
        assert!(text.contains("README.md"));
        assert!(!text.contains("node_modules"));
        assert!(!text.contains(".hidden"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn includes_dotfiles_and_skipped_dirs_with_all_true() {
        let dir = temp_dir();
        std::fs::write(dir.join(".hidden"), "").unwrap();
        std::fs::create_dir(dir.join("node_modules")).unwrap();
        let cwd = dir.to_string_lossy().into_owned();

        let r = create_ls_tool()
            .call(json!({ "all": true }), &make_ctx(&cwd), None)
            .await
            .unwrap();
        let text = text_of(&r);
        assert!(text.contains(".hidden"));
        assert!(text.contains("node_modules/"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn reports_a_non_directory_as_an_error() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let r = create_ls_tool()
            .call(json!({ "path": "nope" }), &make_ctx(&cwd), None)
            .await
            .unwrap();
        assert_eq!(r.serializable.is_error, Some(true));
        assert!(text_of(&r).contains("Not a directory: nope"));
        assert_eq!(r.serializable.output.get("found").unwrap(), &json!(false));
        std::fs::remove_dir_all(&dir).ok();
    }
}
