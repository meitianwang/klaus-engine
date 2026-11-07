//! The `notebook_edit` [`Tool`]: edit a Jupyter notebook (`.ipynb`) by cell.
//!
//! Supports three edit modes over the notebook's `cells` array: `replace` (swap
//! a cell's source, optionally its type), `insert` (add a new cell after a given
//! cell, or at the top), and `delete` (remove a cell). The notebook is parsed as
//! JSON, mutated in place, and written back pretty-printed. Edits are gated by
//! the edit-class path-permission rules and the read-before-write / stale-write
//! guard, mirroring the `edit`/`write` tools.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::fs::node_path::{posix_rendered_runtime, project_runtime_path, resolve_runtime};
use crate::fs::path_safety::{assert_writable, writes_are_unconfined, PathContext};
use crate::fs::text_file::{read_text_file, write_text_file};
use crate::runtime_values::{runtime_json, RuntimeJson, RuntimeObject, RuntimePath, RuntimeString};
use crate::tools::build_tool::text_result;
use crate::tools::freshness::check_read_before_write;
use crate::types::errors::ToolError;
use crate::types::events::{FileChange, FileChangeKind, ToolDomainEvent};
use crate::types::file_state::RecordOptions;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{
    PathTargets, PermissionPathKind, Tool, ToolContext, ToolProgress, ToolResult,
};

fn build_input_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "notebook_path": {
                "type": "string",
                "description": "The absolute path (or cwd-relative) to the Jupyter notebook (.ipynb) file to edit"
            },
            "cell_id": {
                "type": "string",
                "description": "The ID of the cell to edit. For insert, the new cell is placed after this cell, or at the beginning if omitted."
            },
            "new_source": {
                "type": "string",
                "description": "The new source for the cell"
            },
            "cell_type": {
                "type": "string",
                "enum": ["code", "markdown"],
                "description": "The type of the cell. Defaults to the current cell type on replace; required when edit_mode is insert."
            },
            "edit_mode": {
                "type": "string",
                "enum": ["replace", "insert", "delete"],
                "description": "The type of edit to make (defaults to replace)."
            }
        },
        "required": ["notebook_path", "new_source"],
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .expect("notebook_edit schema is an object")
}

/// Edit a Jupyter notebook by cell (replace / insert / delete).
pub struct NotebookEditTool {
    input_schema: JsonSchema,
}

impl NotebookEditTool {
    fn new() -> Self {
        NotebookEditTool {
            input_schema: build_input_schema(),
        }
    }
}

/// Construct the `notebook_edit` tool as an erased registry-side [`Tool`].
pub fn create_notebook_edit_tool() -> Arc<dyn Tool> {
    Arc::new(NotebookEditTool::new())
}

fn runtime_to_summary(value: &RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8()).expect("Node UTF-8 tool summary is valid UTF-8")
}

fn string_field(input: &Json, key: &str) -> Option<RuntimeString> {
    input.get(key).and_then(RuntimeJson::as_string).cloned()
}

/// Split a source string into Jupyter's line-array representation, keeping the
/// trailing newline on every line except the last (the canonical nbformat form).
fn source_to_lines(source: &RuntimeString) -> RuntimeJson {
    let units = source.as_utf16();
    let mut lines: Vec<RuntimeJson> = Vec::new();
    let mut start = 0usize;
    for (index, unit) in units.iter().enumerate() {
        if *unit == u16::from(b'\n') {
            lines.push(RuntimeJson::String(RuntimeString::from_utf16(
                units[start..=index].to_vec(),
            )));
            start = index + 1;
        }
    }
    if start < units.len() {
        lines.push(RuntimeJson::String(RuntimeString::from_utf16(
            units[start..].to_vec(),
        )));
    }
    RuntimeJson::Array(lines)
}

/// A fresh cell id (nbformat 4.5 requires a unique per-cell id).
fn new_cell_id() -> RuntimeString {
    let id = uuid::Uuid::new_v4().simple().to_string();
    RuntimeString::from(&id[..8])
}

/// Build a new notebook cell object of the given type.
fn build_cell(cell_type: &str, source: &RuntimeString) -> RuntimeJson {
    let mut cell = RuntimeObject::new();
    cell.insert("cell_type", RuntimeJson::from(cell_type));
    cell.insert("id", RuntimeJson::String(new_cell_id()));
    cell.insert("metadata", RuntimeJson::Object(RuntimeObject::new()));
    cell.insert("source", source_to_lines(source));
    if cell_type == "code" {
        cell.insert("outputs", RuntimeJson::Array(Vec::new()));
        cell.insert("execution_count", RuntimeJson::Null);
    }
    RuntimeJson::Object(cell)
}

fn cell_matches(cell: &RuntimeJson, cell_id: &RuntimeString) -> bool {
    cell.get("id")
        .and_then(RuntimeJson::as_string)
        .is_some_and(|id| id == cell_id)
}

/// Apply the requested edit to the `cells` array. Returns a short action summary
/// on success, or a user-facing error message on failure.
fn apply_notebook_edit(
    cells: &mut Vec<RuntimeJson>,
    edit_mode: &str,
    cell_id: Option<&RuntimeString>,
    cell_type: Option<&RuntimeString>,
    new_source: &RuntimeString,
) -> Result<String, String> {
    let cell_type_str = cell_type.map(runtime_to_summary);
    if let Some(ct) = cell_type_str.as_deref() {
        if ct != "code" && ct != "markdown" {
            return Err(format!(
                "cell_type must be \"code\" or \"markdown\", got {ct:?}"
            ));
        }
    }
    match edit_mode {
        "replace" => {
            let cell_id = cell_id.ok_or("cell_id is required for replace")?;
            let index = cells
                .iter()
                .position(|cell| cell_matches(cell, cell_id))
                .ok_or_else(|| format!("no cell with id {}", runtime_to_summary(cell_id)))?;
            let cell = cells[index]
                .as_object_mut()
                .ok_or("target cell is not an object")?;
            if let Some(ct) = cell_type_str.as_deref() {
                cell.insert("cell_type", RuntimeJson::from(ct));
                if ct == "markdown" {
                    cell.remove("outputs");
                    cell.remove("execution_count");
                } else {
                    if cell.get("outputs").is_none() {
                        cell.insert("outputs", RuntimeJson::Array(Vec::new()));
                    }
                    if cell.get("execution_count").is_none() {
                        cell.insert("execution_count", RuntimeJson::Null);
                    }
                }
            }
            cell.insert("source", source_to_lines(new_source));
            Ok(format!("replaced cell {}", runtime_to_summary(cell_id)))
        }
        "insert" => {
            let ct = cell_type_str
                .as_deref()
                .ok_or("cell_type is required for insert")?;
            let cell = build_cell(ct, new_source);
            let position = match cell_id {
                Some(id) => {
                    let index = cells
                        .iter()
                        .position(|cell| cell_matches(cell, id))
                        .ok_or_else(|| format!("no cell with id {}", runtime_to_summary(id)))?;
                    index + 1
                }
                None => 0,
            };
            cells.insert(position, cell);
            Ok(format!("inserted {ct} cell at position {position}"))
        }
        "delete" => {
            let cell_id = cell_id.ok_or("cell_id is required for delete")?;
            let index = cells
                .iter()
                .position(|cell| cell_matches(cell, cell_id))
                .ok_or_else(|| format!("no cell with id {}", runtime_to_summary(cell_id)))?;
            cells.remove(index);
            Ok(format!("deleted cell {}", runtime_to_summary(cell_id)))
        }
        other => Err(format!(
            "edit_mode must be replace, insert, or delete, got {other:?}"
        )),
    }
}

fn mtime_ms(md: &std::fs::Metadata) -> f64 {
    md.modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as f64)
        .unwrap_or(0.0)
}

fn edit_output(path: &RuntimePath, edit_mode: &str) -> RuntimeJson {
    let mut output = RuntimeObject::new();
    output.insert(
        "notebook_path",
        RuntimeJson::String(posix_rendered_runtime(path).as_runtime_string().clone()),
    );
    output.insert("edit_mode", RuntimeJson::from(edit_mode));
    RuntimeJson::Object(output)
}

#[async_trait]
impl Tool for NotebookEditTool {
    fn static_name(&self) -> &str {
        "notebook_edit"
    }

    fn static_description(&self) -> &str {
        "Edit a Jupyter notebook (.ipynb) by cell: replace a cell's source (and optionally its type), insert a new cell, or delete a cell. Read the notebook before editing it."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        false
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        false
    }

    fn is_destructive(&self, input: &Json) -> Option<bool> {
        // Deleting a cell is destructive; other modes rewrite in place.
        let destructive = string_field(input, "edit_mode").is_some_and(|mode| mode == "delete");
        Some(destructive)
    }

    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        let path = string_field(input, "notebook_path").unwrap_or_default();
        Some(RuntimeString::from("Editing notebook ").concat(&path))
    }

    fn permission_path_kind(&self) -> Option<PermissionPathKind> {
        Some(PermissionPathKind::Edit)
    }

    fn get_path(&self, input: &Json) -> Option<PathTargets> {
        string_field(input, "notebook_path")
            .map(RuntimePath::from)
            .map(PathTargets::One)
    }

    fn get_raw_path(&self, input: &Json) -> Option<Json> {
        input.get("notebook_path").cloned()
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let path_str = string_field(&input, "notebook_path").unwrap_or_default();
        let new_source = string_field(&input, "new_source").unwrap_or_default();
        let cell_id = string_field(&input, "cell_id");
        let cell_type = string_field(&input, "cell_type");
        let edit_mode =
            string_field(&input, "edit_mode").unwrap_or_else(|| RuntimeString::from("replace"));
        let edit_mode_str = runtime_to_summary(&edit_mode);

        let abs = resolve_runtime(&ctx.cwd, &[RuntimePath::from(path_str.clone())])
            .map_err(|error| ToolError::new(runtime_to_summary(error.message())))?;
        let os_abs = project_runtime_path(&abs, "path")
            .map_err(|error| ToolError::new(runtime_to_summary(error.message())))?;

        let pctx = PathContext {
            cwd: &ctx.cwd,
            additional_writable_roots: &ctx.permission.additional_writable_roots,
            unconfined_writes: writes_are_unconfined(ctx.sandbox_mode),
        };
        if let Err(message) = assert_writable(&abs, &pctx).await {
            return Ok(text_result(
                edit_output(&abs, &edit_mode_str),
                message,
                true,
            ));
        }

        let metadata = match tokio::fs::metadata(&os_abs).await {
            Ok(md) => md,
            Err(_) => {
                return Ok(text_result(
                    edit_output(&abs, &edit_mode_str),
                    RuntimeString::from("notebook does not exist; create it before editing"),
                    true,
                ));
            }
        };
        // Read-before-write + stale-write guard, identical to write: a cell is
        // replaced by id, with no old content to match, so an unread notebook
        // could lose a cell silently.
        if let Some(blocked) =
            check_read_before_write(&abs, mtime_ms(&metadata), ctx.file_state.as_ref(), None)
        {
            return Ok(text_result(
                edit_output(&abs, &edit_mode_str),
                RuntimeString::from(blocked),
                true,
            ));
        }

        let notebook_file = read_text_file(&os_abs)
            .await
            .map_err(|error| ToolError::new(error.to_string()))?;
        let mut notebook = runtime_json::parse(&notebook_file.content)
            .map_err(|_| ToolError::new("notebook is not valid JSON".to_string()))?;
        let notebook_object = notebook
            .as_object_mut()
            .ok_or_else(|| ToolError::new("notebook root is not a JSON object".to_string()))?;
        let cells = match notebook_object
            .get_mut("cells")
            .and_then(RuntimeJson::as_array_mut)
        {
            Some(cells) => cells,
            None => {
                return Ok(text_result(
                    edit_output(&abs, &edit_mode_str),
                    RuntimeString::from("notebook has no \"cells\" array"),
                    true,
                ))
            }
        };

        let action = match apply_notebook_edit(
            cells,
            &edit_mode_str,
            cell_id.as_ref(),
            cell_type.as_ref(),
            &new_source,
        ) {
            Ok(action) => action,
            Err(message) => {
                return Ok(text_result(
                    edit_output(&abs, &edit_mode_str),
                    RuntimeString::from(message),
                    true,
                ))
            }
        };

        let serialized = runtime_json::stringify_pretty(&notebook);
        let written = write_text_file(
            &os_abs,
            &serialized,
            notebook_file.encoding,
            notebook_file.line_endings,
        )
        .await
        .map_err(|error| ToolError::new(error.to_string()))?;
        let st_after = tokio::fs::metadata(&os_abs)
            .await
            .map_err(|error| ToolError::new(error.to_string()))?;
        ctx.file_state.record(
            &abs,
            mtime_ms(&st_after),
            RecordOptions {
                size: Some(st_after.len()),
                content: Some(written),
                encoding: None,
            },
        );
        // Against what this TURN first found in the notebook, so editing several
        // cells across several calls counts each line once.
        let baseline = ctx
            .file_state
            .turn_baseline(&abs, Some(&notebook_file.content));
        let existed_before = ctx.file_state.turn_existed(&abs, true);
        let counts = baseline.as_ref().and_then(|baseline| {
            crate::context::edited_files_diff::line_change_counts(baseline, &serialized)
        });
        (ctx.emit)(ToolDomainEvent::FileChange {
            change: FileChange {
                turn_diff: baseline.as_ref().map(|before| {
                    crate::context::edited_files_diff::full_turn_file_diff(
                        before,
                        &serialized,
                        existed_before,
                        true,
                    )
                }),
                path: posix_rendered_runtime(&abs).as_runtime_string().clone(),
                kind: FileChangeKind::Modify,
                diff: Some(crate::context::edited_files_diff::full_file_diff(
                    &notebook_file.content,
                    &serialized,
                )),
                added: counts.map(|(added, _)| added),
                removed: counts.map(|(_, removed)| removed),
            },
        })
        .await;

        Ok(text_result(
            edit_output(&abs, &edit_mode_str),
            RuntimeString::from(format!(
                "Edited {}: {action}",
                runtime_to_summary(&path_str)
            )),
            false,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::builtins::test_ctx::make_ctx;
    use crate::types::file_state::{FileStateCache, FileStateEntry};
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_dir() -> PathBuf {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "ae-notebook-edit-{}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[derive(Default)]
    struct RecordingState {
        map: Mutex<HashMap<RuntimePath, FileStateEntry>>,
    }

    impl FileStateCache for RecordingState {
        fn get(&self, path: &RuntimePath) -> Option<FileStateEntry> {
            self.map.lock().unwrap().get(path).cloned()
        }
        fn record(&self, path: &RuntimePath, mtime_ms: f64, opts: RecordOptions) {
            self.map.lock().unwrap().insert(
                path.clone(),
                FileStateEntry {
                    mtime_ms,
                    size: opts.size,
                    content: opts.content,
                    encoding: opts.encoding,
                },
            );
        }
        fn has(&self, path: &RuntimePath) -> bool {
            self.map.lock().unwrap().contains_key(path)
        }
        fn entries(&self) -> Vec<(RuntimePath, FileStateEntry)> {
            self.map
                .lock()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        }
        fn delete(&self, path: &RuntimePath) {
            self.map.lock().unwrap().remove(path);
        }
        fn clear(&self) {
            self.map.lock().unwrap().clear();
        }
        fn clone_box(&self) -> Box<dyn FileStateCache> {
            Box::new(RecordingState {
                map: Mutex::new(self.map.lock().unwrap().clone()),
            })
        }
    }

    const NOTEBOOK: &str = r##"{"cells":[{"cell_type":"code","id":"aaa","metadata":{},"source":["print(1)"],"outputs":[],"execution_count":null},{"cell_type":"markdown","id":"bbb","metadata":{},"source":["# title"]}],"metadata":{},"nbformat":4,"nbformat_minor":5}"##;

    /// Build a context whose file_state has already recorded the notebook as read
    /// at its current mtime, so the read-before-write guard passes.
    fn ctx_with_read_notebook(
        dir: &std::path::Path,
        cwd: &str,
    ) -> (ToolContext, Arc<RecordingState>) {
        let path = dir.join("nb.ipynb");
        std::fs::write(&path, NOTEBOOK).unwrap();
        let abs =
            resolve_runtime(&RuntimePath::from(cwd), &[RuntimePath::from("nb.ipynb")]).unwrap();
        let mtime = mtime_ms(&std::fs::metadata(&path).unwrap());
        let state = Arc::new(RecordingState::default());
        state.record(
            &abs,
            mtime,
            RecordOptions {
                size: None,
                content: Some(RuntimeString::from(NOTEBOOK)),
                encoding: None,
            },
        );
        let mut ctx = make_ctx(cwd);
        ctx.file_state = state.clone();
        (ctx, state)
    }

    fn read_cells(path: &std::path::Path) -> Vec<RuntimeJson> {
        let text = std::fs::read_to_string(path).unwrap();
        let value = runtime_json::parse(&RuntimeString::from(text.as_str())).unwrap();
        value
            .get("cells")
            .and_then(RuntimeJson::as_array)
            .cloned()
            .unwrap()
    }

    fn cell_id_at(cells: &[RuntimeJson], index: usize) -> String {
        runtime_to_summary(
            cells[index]
                .get("id")
                .and_then(RuntimeJson::as_string)
                .unwrap(),
        )
    }

    #[test]
    fn metadata_and_schema_shape() {
        let tool = create_notebook_edit_tool();
        assert_eq!(tool.name(), "notebook_edit");
        assert_eq!(tool.permission_path_kind(), Some(PermissionPathKind::Edit));
        let replace = runtime_json::parse(&RuntimeString::from(
            r#"{"notebook_path":"n.ipynb","new_source":"x","edit_mode":"replace"}"#,
        ))
        .unwrap();
        assert_eq!(tool.is_destructive(&replace), Some(false));
        let delete = runtime_json::parse(&RuntimeString::from(
            r#"{"notebook_path":"n.ipynb","new_source":"","edit_mode":"delete","cell_id":"a"}"#,
        ))
        .unwrap();
        assert_eq!(tool.is_destructive(&delete), Some(true));
        assert_eq!(
            tool.get_path(&replace),
            Some(PathTargets::One("n.ipynb".into()))
        );
        let schema = tool.static_input_schema();
        assert_eq!(
            schema.get("required").unwrap(),
            &serde_json::json!(["notebook_path", "new_source"])
        );
    }

    #[test]
    fn source_to_lines_keeps_trailing_newlines() {
        let RuntimeJson::Array(ref lines) = source_to_lines(&RuntimeString::from("a\nb\n")) else {
            panic!("expected array");
        };
        assert_eq!(lines.len(), 2);
        let RuntimeJson::Array(ref single) = source_to_lines(&RuntimeString::from("only")) else {
            panic!("expected array");
        };
        assert_eq!(single.len(), 1);
    }

    #[tokio::test]
    async fn replace_updates_cell_source_and_type() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let (ctx, _state) = ctx_with_read_notebook(&dir, &cwd);
        let tool = create_notebook_edit_tool();

        let res = tool
            .call(
                runtime_json::parse(&RuntimeString::from(
                    r#"{"notebook_path":"nb.ipynb","cell_id":"aaa","new_source":"print(42)","cell_type":"markdown","edit_mode":"replace"}"#,
                ))
                .unwrap(),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        let cells = read_cells(&dir.join("nb.ipynb"));
        assert_eq!(cell_id_at(&cells, 0), "aaa");
        let cell = cells[0].as_object().unwrap();
        assert!(cell.get("outputs").is_none());
        assert!(cell.get("execution_count").is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn insert_places_new_cell_after_target() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let (ctx, _state) = ctx_with_read_notebook(&dir, &cwd);
        let tool = create_notebook_edit_tool();

        tool.call(
            runtime_json::parse(&RuntimeString::from(
                r#"{"notebook_path":"nb.ipynb","cell_id":"aaa","new_source":"x=1","cell_type":"code","edit_mode":"insert"}"#,
            ))
            .unwrap(),
            &ctx,
            None,
        )
        .await
        .unwrap();
        let cells = read_cells(&dir.join("nb.ipynb"));
        assert_eq!(cells.len(), 3);
        assert_eq!(cell_id_at(&cells, 0), "aaa");
        assert_eq!(cell_id_at(&cells, 2), "bbb");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn delete_removes_the_cell() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let (ctx, _state) = ctx_with_read_notebook(&dir, &cwd);
        let tool = create_notebook_edit_tool();

        tool.call(
            runtime_json::parse(&RuntimeString::from(
                r#"{"notebook_path":"nb.ipynb","cell_id":"bbb","new_source":"","edit_mode":"delete"}"#,
            ))
            .unwrap(),
            &ctx,
            None,
        )
        .await
        .unwrap();
        let cells = read_cells(&dir.join("nb.ipynb"));
        assert_eq!(cells.len(), 1);
        assert_eq!(cell_id_at(&cells, 0), "aaa");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn insert_requires_cell_type() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let (ctx, _state) = ctx_with_read_notebook(&dir, &cwd);
        let res = create_notebook_edit_tool()
            .call(
                runtime_json::parse(&RuntimeString::from(
                    r#"{"notebook_path":"nb.ipynb","cell_id":"aaa","new_source":"x","edit_mode":"insert"}"#,
                ))
                .unwrap(),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn blocks_edit_of_unread_notebook() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        std::fs::write(dir.join("nb.ipynb"), NOTEBOOK).unwrap();
        let ctx = make_ctx(&cwd);
        let res = create_notebook_edit_tool()
            .call(
                runtime_json::parse(&RuntimeString::from(
                    r#"{"notebook_path":"nb.ipynb","cell_id":"aaa","new_source":"y","edit_mode":"replace"}"#,
                ))
                .unwrap(),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        std::fs::remove_dir_all(&dir).ok();
    }
}
