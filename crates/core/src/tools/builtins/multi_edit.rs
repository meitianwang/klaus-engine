//! The `multi_edit` [`Tool`]: apply several exact-string replacements to one file
//! atomically.
//!
//! Every edit is validated against an in-memory buffer first (each oldString
//! found, and unique unless replaceAll), applied in sequence so a later edit can
//! target text an earlier one introduced, so a failure anywhere leaves the file
//! untouched. Matching mirrors the single-`edit` tool: exact, glyph-tolerant, then
//! (non-Markdown) whitespace-flexible; trailing whitespace is stripped from
//! `newString` (except Markdown) and reserved tags restored on a miss.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::fs::edit_input_rewrite::{
    desanitize_reserved_tags, is_markdown_path, strip_trailing_whitespace, DesanitizeResult,
};
use crate::fs::flexible_edit_match::flexible_line_replace;
use crate::fs::node_path::{posix_rendered_runtime, project_runtime_path, resolve_runtime};
use crate::fs::path_safety::{assert_writable, writes_are_unconfined, PathContext};
use crate::fs::quote_match::{
    escape_mismatch_hint, find_actual_string, preserve_quote_style, preserve_unicode_escape,
};
use crate::fs::text_file::{read_text_file, write_text_file, TextFile};
use crate::runtime_values::{RuntimeJson, RuntimeObject, RuntimePath, RuntimeString};
use crate::tools::build_tool::text_result;
use crate::tools::builtins::edit::{
    contains_exact, count_occurrences, mtime_ms, replace_first, split_join, to_state_enc,
};
use crate::tools::freshness::check_stale_write;
use crate::types::errors::ToolError;
use crate::types::events::{FileChange, FileChangeKind, ToolDomainEvent};
use crate::types::file_state::RecordOptions;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{
    PathTargets, PermissionPathKind, Tool, ToolContext, ToolProgress, ToolResult,
};

/// Build the tool's input JSON Schema: a `path` plus a non-empty `edits` array.
fn build_input_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "path": {
                "type": "string",
                "description": "Absolute or cwd-relative file path"
            },
            "edits": {
                "minItems": 1,
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "oldString": {
                            "type": "string",
                            "description": "The exact string to replace"
                        },
                        "newString": {
                            "type": "string",
                            "description": "The replacement string"
                        },
                        "replaceAll": {
                            "description": "Replace every occurrence instead of requiring a unique match",
                            "type": "boolean"
                        }
                    },
                    "required": ["oldString", "newString"],
                    "additionalProperties": false
                },
                "description": "Edits applied in order; all must succeed or none are written"
            }
        },
        "required": ["path", "edits"],
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .expect("multi_edit schema is an object")
}

/// Apply several string replacements to one file atomically.
pub struct MultiEditTool {
    input_schema: JsonSchema,
}

impl MultiEditTool {
    fn new() -> Self {
        MultiEditTool {
            input_schema: build_input_schema(),
        }
    }
}

/// Construct the `multi_edit` tool as an erased registry-side [`Tool`].
pub fn create_multi_edit_tool() -> Arc<dyn Tool> {
    Arc::new(MultiEditTool::new())
}

/// A model-error result carrying the empty multi-edit output shape.
fn output(path: &RuntimePath, edits: usize, replacements: usize) -> RuntimeJson {
    let mut output = RuntimeObject::new();
    output.insert(
        "path",
        RuntimeJson::String(posix_rendered_runtime(path).as_runtime_string().clone()),
    );
    output.insert("edits", RuntimeJson::from(edits as u64));
    output.insert("replacements", RuntimeJson::from(replacements as u64));
    RuntimeJson::Object(output)
}

fn fail(path: &RuntimePath, message: RuntimeString) -> ToolResult {
    text_result(output(path, 0, 0), message, true)
}

fn runtime_to_error(value: &RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8()).expect("Node UTF-8 error projection is valid UTF-8")
}

#[async_trait]
impl Tool for MultiEditTool {
    fn static_name(&self) -> &str {
        "multi_edit"
    }

    fn static_description(&self) -> &str {
        "Apply multiple exact-string replacements to a single file atomically. If any edit cannot apply, the file is left unchanged."
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

    fn is_destructive(&self, _input: &Json) -> Option<bool> {
        Some(false)
    }

    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        let path = input
            .get("path")
            .and_then(RuntimeJson::as_string)
            .cloned()
            .unwrap_or_default();
        let count = input
            .get("edits")
            .and_then(RuntimeJson::as_array)
            .map(|a| a.len())
            .unwrap_or(0);
        Some(
            RuntimeString::from("Editing ")
                .concat(&path)
                .concat(&RuntimeString::from(" ("))
                .concat(&RuntimeString::from(count.to_string()))
                .concat(&RuntimeString::from(" edits)")),
        )
    }

    fn permission_path_kind(&self) -> Option<PermissionPathKind> {
        Some(PermissionPathKind::Edit)
    }

    fn get_path(&self, input: &Json) -> Option<PathTargets> {
        input
            .get("path")
            .and_then(RuntimeJson::as_string)
            .cloned()
            .map(RuntimePath::from)
            .map(PathTargets::One)
    }

    fn get_raw_path(&self, input: &Json) -> Option<Json> {
        input.get("path").cloned()
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let path_str = input
            .get("path")
            .and_then(RuntimeJson::as_string)
            .cloned()
            .unwrap_or_default();
        let empty: Vec<Json> = Vec::new();
        let edits = input
            .get("edits")
            .and_then(RuntimeJson::as_array)
            .unwrap_or(&empty);
        let abs = resolve_runtime(&ctx.cwd, &[RuntimePath::from(path_str.clone())])
            .map_err(|error| ToolError::new(runtime_to_error(error.message())))?;
        let os_abs = project_runtime_path(&abs, "path")
            .map_err(|error| ToolError::new(runtime_to_error(error.message())))?;
        // Touching this path may activate a conditional skill whose `paths` glob
        // matches.
        if let Some(activate) = &ctx.activate_skill_paths {
            activate(vec![abs.clone()]);
        }
        let st = tokio::fs::metadata(&os_abs).await.ok();
        if !st.as_ref().map(|m| m.is_file()).unwrap_or(false) {
            return Ok(fail(
                &abs,
                RuntimeString::from("File not found: ").concat(&path_str),
            ));
        }
        let st = st.unwrap();

        // Read with encoding detection + CRLF→LF normalization so LF `old_string`s
        // match a CRLF file and a utf16le file decodes correctly.
        let TextFile {
            content: original,
            encoding,
            line_endings,
        } = read_text_file(&os_abs)
            .await
            .map_err(|e| ToolError::new(e.to_string()))?;
        // Stale-write guard. As in the single-`edit` tool, a file with no baseline
        // is not blocked (every oldString is resolved against `original`, the text
        // just read off disk); a baseline that really changed since is.
        if let Some(blocked) =
            check_stale_write(&abs, mtime_ms(&st), ctx.file_state.as_ref(), &original)
        {
            return Ok(fail(&abs, RuntimeString::from(blocked)));
        }

        // Apply to an in-memory buffer, validating each edit as we go. Because
        // edits apply in sequence, validation runs against the evolving buffer so
        // an edit can target text a prior edit introduced.
        let markdown = is_markdown_path(&abs);
        // The pre-edit content, kept for the change event's line counts: the
        // buffer below becomes the post-edit content, so the baseline has to be
        // captured before the first edit lands.
        let baseline = original.clone();
        let mut buffer = original;
        let mut replacements: usize = 0;
        for (i, edit) in edits.iter().enumerate() {
            let old_string = edit
                .get("oldString")
                .and_then(RuntimeJson::as_string)
                .cloned()
                .unwrap_or_default();
            let new_string = edit
                .get("newString")
                .and_then(RuntimeJson::as_string)
                .cloned()
                .unwrap_or_default();
            let replace_all = edit
                .get("replaceAll")
                .and_then(RuntimeJson::as_bool)
                .unwrap_or(false);
            // Pre-match input rewrites: strip trailing whitespace from newString
            // (except Markdown), and — when oldString misses the evolving buffer —
            // restore transport-shortened reserved tags, mirroring onto newString.
            let mut effective_old = old_string;
            let mut effective_new = if markdown {
                new_string
            } else {
                strip_trailing_whitespace(&new_string)
            };
            if !contains_exact(&buffer, &effective_old) {
                let DesanitizeResult {
                    result,
                    applied_replacements,
                } = desanitize_reserved_tags(&effective_old);
                if !applied_replacements.is_empty() && contains_exact(&buffer, &result) {
                    effective_old = result;
                    for ar in &applied_replacements {
                        effective_new = split_join(&effective_new, &ar.from, &ar.to);
                    }
                }
            }
            // No-op guard on the NORMALIZED old/new: reject a true no-op, but allow
            // an edit whose only change is stripped trailing whitespace.
            if effective_old == effective_new {
                return Ok(fail(
                    &abs,
                    RuntimeString::from("Edit ")
                        .concat(&RuntimeString::from((i + 1).to_string()))
                        .concat(&RuntimeString::from(
                            ": oldString and newString are identical",
                        )),
                ));
            }
            // Resolve against the evolving buffer with the same quote / escape
            // tolerance as the Edit tool; count and replace the real buffer bytes.
            match find_actual_string(&buffer, &effective_old) {
                Some(actual_old) => {
                    let count = count_occurrences(&buffer, &actual_old);
                    if count > 1 && !replace_all {
                        return Ok(fail(
                            &abs,
                            RuntimeString::from("Edit ")
                                .concat(&RuntimeString::from((i + 1).to_string()))
                                .concat(&RuntimeString::from(": oldString is not unique ("))
                                .concat(&RuntimeString::from(count.to_string()))
                                .concat(&RuntimeString::from(
                                    " matches); pass replaceAll or add more context",
                                )),
                        ));
                    }
                    let new_str = preserve_unicode_escape(
                        &effective_old,
                        &actual_old,
                        &preserve_quote_style(&effective_old, &actual_old, &effective_new),
                    );
                    buffer = if replace_all {
                        split_join(&buffer, &actual_old, &new_str)
                    } else {
                        replace_first(&buffer, &actual_old, &new_str)
                    };
                    replacements += if replace_all { count } else { 1 };
                }
                None => {
                    // Representation ladder missed — whitespace-flexible recovery
                    // against the evolving buffer, skipped for Markdown.
                    let flex = if markdown {
                        None
                    } else {
                        flexible_line_replace(&buffer, &effective_old, &effective_new)
                    };
                    match flex {
                        None => {
                            return Ok(fail(
                                &abs,
                                RuntimeString::from("Edit ")
                                    .concat(&RuntimeString::from((i + 1).to_string()))
                                    .concat(&RuntimeString::from(": oldString not found"))
                                    .concat(&escape_mismatch_hint(&effective_old)),
                            ));
                        }
                        Some(flex) => {
                            if flex.occurrences > 1 && !replace_all {
                                return Ok(fail(
                                    &abs,
                                    RuntimeString::from("Edit ")
                                        .concat(&RuntimeString::from((i + 1).to_string()))
                                        .concat(&RuntimeString::from(": oldString is not unique ("))
                                        .concat(&RuntimeString::from(flex.occurrences.to_string()))
                                        .concat(&RuntimeString::from(
                                            " matches); pass replaceAll or add more context",
                                        )),
                                ));
                            }
                            // A flexible match that reproduces the buffer
                            // byte-for-byte is a genuine no-op.
                            if flex.new_content == buffer {
                                return Ok(fail(
                                    &abs,
                                    RuntimeString::from("Edit ")
                                        .concat(&RuntimeString::from((i + 1).to_string()))
                                        .concat(&RuntimeString::from(
                                            ": oldString and newString are identical",
                                        )),
                                ));
                            }
                            buffer = flex.new_content;
                            replacements += flex.occurrences;
                        }
                    }
                }
            }
        }

        // All edits validated and applied in-memory — write once.
        let pctx = PathContext {
            cwd: &ctx.cwd,
            additional_writable_roots: &ctx.permission.additional_writable_roots,
            unconfined_writes: writes_are_unconfined(ctx.sandbox_mode),
        };
        if let Err(message) = assert_writable(&abs, &pctx).await {
            return Ok(fail(&abs, message));
        }
        // Snapshot diagnostics, then exclude same-document queries through the
        // filesystem write and its matching didChange/didOpen synchronization.
        let lsp_mutation = match &ctx.lsp {
            Some(lsp) => {
                lsp.before_edited(&abs).await;
                Some(
                    lsp.begin_document_mutation(std::slice::from_ref(&abs))
                        .await,
                )
            }
            None => None,
        };
        // Restore the file's original encoding + line endings; record the
        // CANONICAL written form (what a re-read decodes).
        let written = write_text_file(&os_abs, &buffer, encoding, line_endings)
            .await
            .map_err(|e| ToolError::new(e.to_string()))?;
        let after = tokio::fs::metadata(&os_abs)
            .await
            .map_err(|e| ToolError::new(e.to_string()))?;
        ctx.file_state.record(
            &abs,
            mtime_ms(&after),
            RecordOptions {
                size: Some(after.len()),
                content: Some(written),
                encoding: Some(to_state_enc(encoding)),
            },
        );
        // Against what this TURN first found in the file, so a file this turn has
        // already edited is not charged for those lines again.
        let turn_baseline = ctx.file_state.turn_baseline(&abs, Some(&baseline));
        let existed_before = ctx.file_state.turn_existed(&abs, true);
        let counts = turn_baseline.as_ref().and_then(|baseline| {
            crate::context::edited_files_diff::line_change_counts(baseline, &buffer)
        });
        (ctx.emit)(ToolDomainEvent::FileChange {
            change: FileChange {
                turn_diff: turn_baseline.as_ref().map(|before| {
                    crate::context::edited_files_diff::full_turn_file_diff(
                        before,
                        &buffer,
                        existed_before,
                        true,
                    )
                }),
                path: posix_rendered_runtime(&abs).as_runtime_string().clone(),
                kind: FileChangeKind::Modify,
                diff: Some(crate::context::edited_files_diff::full_file_diff(
                    &baseline, &buffer,
                )),
                added: counts.map(|(added, _)| added),
                removed: counts.map(|(_, removed)| removed),
            },
        })
        .await;
        // Sync the language server (if any) so its diagnostics reflect the edits.
        if let Some(lsp) = &ctx.lsp {
            lsp.notify_edited(&abs, Some(&buffer)).await;
        }
        drop(lsp_mutation);

        let edits_len = edits.len();
        let edit_plural = if edits_len == 1 { "" } else { "s" };
        let repl_plural = if replacements == 1 { "" } else { "s" };
        Ok(text_result(
            output(&abs, edits_len, replacements),
            RuntimeString::from("Edited ")
                .concat(&path_str)
                .concat(&RuntimeString::from(" ("))
                .concat(&RuntimeString::from(edits_len.to_string()))
                .concat(&RuntimeString::from(" edit"))
                .concat(&RuntimeString::from(edit_plural))
                .concat(&RuntimeString::from(", "))
                .concat(&RuntimeString::from(replacements.to_string()))
                .concat(&RuntimeString::from(" replacement"))
                .concat(&RuntimeString::from(repl_plural))
                .concat(&RuntimeString::from(")")),
            false,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::{runtime_codec, runtime_json, ScalarJson};
    use crate::schemas::content::OutputBlock;
    use crate::types::abort::AbortSignal;
    use crate::types::file_state::{Encoding as StateEncoding, FileStateCache, FileStateEntry};
    use crate::types::permission::PermissionContext;
    use crate::types::sandbox::{
        ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec,
    };
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;

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
            "ae-multiedit-tool-{}-{}-{}",
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

    struct NoopSandbox;
    impl SandboxBackend for NoopSandbox {
        fn kind(&self) -> SandboxKind {
            SandboxKind::Process
        }
        fn is_available(&self) -> bool {
            false
        }
        fn transform(&self, req: &ExecRequest, _policy: &SandboxPolicy) -> SpawnSpec {
            SpawnSpec {
                file: req.argv.first().cloned().unwrap_or_default(),
                args: req.argv.iter().skip(1).cloned().collect(),
                cwd: req.cwd.clone(),
                env: crate::runtime_process::RuntimeEnv::new(),
            }
        }
    }

    fn make_ctx(
        cwd: &str,
        file_state: Arc<dyn FileStateCache>,
        events: Arc<Mutex<Vec<ToolDomainEvent>>>,
    ) -> ToolContext {
        let sink = events;
        ToolContext {
            async_question: None,
            background_processes: Arc::new(
                crate::command_runtime::background::BackgroundCommandRegistry::new(),
            ),
            process_context: crate::runtime_process::test_runtime_process_context(),
            cwd: RuntimePath::from(cwd),
            signal: AbortSignal::new(),
            file_state,
            permission: PermissionContext {
                allow_rules: Vec::new(),
                deny_rules: Vec::new(),
                ask_rules: Vec::new(),
                additional_writable_roots: Vec::new(),
                internal_writable_roots: Vec::new(),
                pinned_memory_root: None,
                should_avoid_prompts: false,
            },
            sandbox: Arc::new(NoopSandbox),
            planning_read_only: false,
            approval_policy: None,
            sandbox_mode: None,
            approvals_reviewer: None,
            web_search_access: None,
            network_allowed: None,
            emit: std::sync::Arc::new(move |e: ToolDomainEvent| {
                sink.lock().unwrap().push(e);
                Box::pin(async {})
            }),
            native_pdf: None,
            vision: None,
            shell_env: None,
            depth: None,
            forward_subagent_text: None,
            spawn_sub_agent: None,
            spawn_background_agent: None,
            spawn_workflow_agent: None,
            surface_tools: None,
            activate_skill_paths: None,
            tool_catalog: None,
            elicit: None,
            plan_file: None,
            apply_skill_context: None,
            run_bash: None,
            mcp_clients: None,
            background_tasks: None,
            task_list_id: None,
            task_plan: None,
            worktree: None,
            workflow_journal: None,
            swarm: None,
            cron: None,
            goal: None,
            goal_generation: None,
            goal_owned_resources: None,
            lsp: None,
            browser: None,
            browser_tool_mapping: None,
            run_hook: None,
            invoke_tool: None,
            advisor: None,
            sandbox_retry_on_denial: None,
            request_approval: None,
            request_network_access: None,
            enqueue_background_notice: None,
            record_rollout_event: None,
        }
    }

    fn text_of(res: &ToolResult) -> String {
        match &res.serializable.model_content[0] {
            OutputBlock::Text(t) => String::from_utf8(t.text.to_node_utf8()).unwrap(),
            _ => panic!("expected a text block"),
        }
    }

    fn seed(state: &dyn FileStateCache, path: &Path, contents: &str) {
        std::fs::write(path, contents).unwrap();
        let md = std::fs::metadata(path).unwrap();
        state.record(
            &crate::fs::node_path::runtime_path_from_native(path),
            mtime_ms(&md),
            RecordOptions {
                size: Some(md.len()),
                content: Some(RuntimeString::from(contents)),
                encoding: Some(StateEncoding::Utf8),
            },
        );
    }

    #[test]
    fn exposes_expected_metadata() {
        let tool = create_multi_edit_tool();
        assert_eq!(tool.name(), "multi_edit");
        assert_eq!(tool.permission_path_kind(), Some(PermissionPathKind::Edit));
        let input = json!({ "path": "a.txt", "edits": [
            { "oldString": "a", "newString": "b" },
            { "oldString": "c", "newString": "d" }
        ] });
        assert_eq!(tool.is_destructive(&input), Some(false));
        assert_eq!(
            tool.summarize(&input).as_ref(),
            Some(&RuntimeString::from("Editing a.txt (2 edits)"))
        );
    }

    #[test]
    fn input_schema_shape() {
        let tool = create_multi_edit_tool();
        let schema = tool.static_input_schema();
        assert_eq!(
            schema.get("required").unwrap(),
            &serde_json::json!(["path", "edits"])
        );
        let edits = schema
            .get("properties")
            .and_then(|p| p.get("edits"))
            .unwrap();
        assert_eq!(edits.get("minItems"), Some(&serde_json::json!(1)));
        assert_eq!(
            edits.get("type").and_then(serde_json::Value::as_str),
            Some("array")
        );
        let item_required = edits.get("items").and_then(|i| i.get("required")).unwrap();
        assert_eq!(
            item_required,
            &serde_json::json!(["oldString", "newString"])
        );
    }

    #[tokio::test]
    async fn applies_two_edits_in_sequence() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let state = Arc::new(RecordingState::default());
        let events = Arc::new(Mutex::new(Vec::new()));
        seed(state.as_ref(), &dir.join("f.txt"), "one two three\n");
        let ctx = make_ctx(&cwd, state.clone(), events.clone());
        let tool = create_multi_edit_tool();

        let p = dir.join("f.txt").to_string_lossy().into_owned();
        let res = tool
            .call(
                json!({ "path": p, "edits": [
                    { "oldString": "one", "newString": "1" },
                    { "oldString": "three", "newString": "3" }
                ] }),
                &ctx,
                None,
            )
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            std::fs::read_to_string(dir.join("f.txt")).unwrap(),
            "1 two 3\n"
        );
        assert_eq!(
            text_of(&res),
            format!("Edited {p} (2 edits, 2 replacements)")
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn parsed_lone_surrogates_project_at_multi_edit_fs_boundaries() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let actual = dir.join("\u{fffd}.txt");
        std::fs::write(&actual, "one two").unwrap();
        let state = Arc::new(RecordingState::default());
        let logical = runtime_json::parse(&RuntimeString::from(r#""\ud800.txt""#)).unwrap();
        let cwd_path = crate::fs::node_path::runtime_path_from_native(&dir);
        let logical = resolve_runtime(
            &cwd_path,
            &[RuntimePath::from(logical.as_str().unwrap().clone())],
        )
        .unwrap();
        let md = std::fs::metadata(&actual).unwrap();
        state.record(
            &logical,
            mtime_ms(&md),
            RecordOptions {
                size: Some(md.len()),
                content: Some(RuntimeString::from("one two")),
                encoding: Some(StateEncoding::Utf8),
            },
        );
        let ctx = make_ctx(&cwd, state, Arc::new(Mutex::new(Vec::new())));
        let input = runtime_json::parse(&RuntimeString::from(
            r#"{"path":"\ud800.txt","edits":[{"oldString":"one","newString":"\ud801"},{"oldString":"two","newString":"done"}]}"#,
        ))
        .unwrap();

        let result = create_multi_edit_tool()
            .call(input, &ctx, None)
            .await
            .unwrap();

        assert_eq!(result.serializable.is_error, Some(false));
        assert_eq!(std::fs::read(actual).unwrap(), "\u{fffd} done".as_bytes());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn empty_replace_all_preserves_utf16le_astral_and_lone_units() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let path = dir.join("utf16.txt");
        let units = [0xfeff_u16, 0xd83d, 0xde00, 0xd800];
        let bytes = units
            .iter()
            .flat_map(|unit| unit.to_le_bytes())
            .collect::<Vec<_>>();
        std::fs::write(&path, bytes).unwrap();
        let state = Arc::new(RecordingState::default());
        let decoded = crate::fs::text_file::decode_text_buffer(&std::fs::read(&path).unwrap());
        let md = std::fs::metadata(&path).unwrap();
        state.record(
            &crate::fs::node_path::runtime_path_from_native(&path),
            mtime_ms(&md),
            RecordOptions {
                size: Some(md.len()),
                content: Some(decoded.content),
                encoding: Some(StateEncoding::Utf16Le),
            },
        );
        let ctx = make_ctx(&cwd, state, Arc::new(Mutex::new(Vec::new())));

        let result = create_multi_edit_tool()
            .call(
                json!({
                    "path": path.to_string_lossy(),
                    "edits": [{
                        "oldString": "",
                        "newString": "|",
                        "replaceAll": true
                    }]
                }),
                &ctx,
                None,
            )
            .await
            .unwrap();

        assert_eq!(result.serializable.is_error, Some(false));
        let expected = [0xfeff_u16, 0x007c, 0xd83d, 0x007c, 0xde00, 0x007c, 0xd800]
            .iter()
            .flat_map(|unit| unit.to_le_bytes())
            .collect::<Vec<_>>();
        assert_eq!(std::fs::read(&path).unwrap(), expected);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn later_edit_targets_earlier_output() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let state = Arc::new(RecordingState::default());
        let events = Arc::new(Mutex::new(Vec::new()));
        seed(state.as_ref(), &dir.join("f.txt"), "a\n");
        let ctx = make_ctx(&cwd, state.clone(), events.clone());
        let tool = create_multi_edit_tool();

        let p = dir.join("f.txt").to_string_lossy().into_owned();
        let res = tool
            .call(
                json!({ "path": p, "edits": [
                    { "oldString": "a", "newString": "b" },
                    { "oldString": "b", "newString": "c" }
                ] }),
                &ctx,
                None,
            )
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(std::fs::read_to_string(dir.join("f.txt")).unwrap(), "c\n");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_failing_edit_leaves_the_file_untouched() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let state = Arc::new(RecordingState::default());
        let events = Arc::new(Mutex::new(Vec::new()));
        seed(state.as_ref(), &dir.join("f.txt"), "keep me\n");
        let ctx = make_ctx(&cwd, state.clone(), events.clone());
        let tool = create_multi_edit_tool();

        let p = dir.join("f.txt").to_string_lossy().into_owned();
        let res = tool
            .call(
                json!({ "path": p, "edits": [
                    { "oldString": "keep", "newString": "kept" },
                    { "oldString": "absent", "newString": "x" }
                ] }),
                &ctx,
                None,
            )
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(true));
        assert!(text_of(&res).starts_with("Edit 2: oldString not found"));
        assert_eq!(
            std::fs::read_to_string(dir.join("f.txt")).unwrap(),
            "keep me\n"
        );
        assert!(events.lock().unwrap().is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }
}
