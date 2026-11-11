//! The `edit` [`Tool`]: replace an exact string in a file, with a stale-write
//! guard and a uniqueness check.
//!
//! Matching walks a tolerance ladder: an exact substring match; a glyph-tolerant
//! match (curly/straight quotes, `\uXXXX` escapes); then, for non-Markdown files,
//! a whitespace-flexible block match that re-indents the replacement. Trailing
//! whitespace is stripped from `newString` (except Markdown), transport-shortened
//! reserved tags are restored when `oldString` misses, and a true no-op is
//! rejected. The file's original encoding + line endings are restored on write.

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
use crate::fs::text_file::{
    read_text_file, read_text_file_from_file, write_text_file, write_text_file_to_file,
    Encoding as FileEncoding, TextFile,
};
use crate::runtime_values::{RuntimeJson, RuntimeObject, RuntimePath, RuntimeString};
use crate::tools::build_tool::text_result;
use crate::tools::freshness::check_stale_write;
use crate::types::errors::ToolError;
use crate::types::events::{FileChange, FileChangeKind, ToolDomainEvent};
use crate::types::file_state::{Encoding as StateEncoding, RecordOptions};
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{
    PathTargets, PermissionPathKind, Tool, ToolContext, ToolProgress, ToolResult,
};

/// Build the tool's input JSON Schema.
fn build_input_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "path": {
                "type": "string",
                "description": "Absolute or cwd-relative file path"
            },
            "oldString": {
                "type": "string",
                "description": "The exact string to replace — match the file byte-for-byte. When copying from read output, use the indentation exactly as it appears AFTER the line-number prefix, and do not include that prefix."
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
        "required": ["path", "oldString", "newString"],
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .expect("edit schema is an object")
}

/// Replace an exact string in a file, with a stale-write guard and uniqueness
/// check.
pub struct EditTool {
    input_schema: JsonSchema,
}

impl EditTool {
    fn new() -> Self {
        EditTool {
            input_schema: build_input_schema(),
        }
    }
}

/// Construct the `edit` tool as an erased registry-side [`Tool`].
pub fn create_edit_tool() -> Arc<dyn Tool> {
    Arc::new(EditTool::new())
}

/// Map a text-I/O encoding to the tracked-file encoding recorded in file state.
pub(crate) fn to_state_enc(e: FileEncoding) -> StateEncoding {
    match e {
        FileEncoding::Utf8 => StateEncoding::Utf8,
        FileEncoding::Utf16Le => StateEncoding::Utf16Le,
    }
}

/// The mtime (ms) of a stat result, floored to 0 on any clock error.
pub(crate) fn mtime_ms(md: &std::fs::Metadata) -> f64 {
    md.modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as f64)
        .unwrap_or(0.0)
}

/// Count non-overlapping occurrences of `needle` in `haystack` (empty needle → 0).
pub(crate) fn count_occurrences(haystack: &RuntimeString, needle: &RuntimeString) -> usize {
    if needle.is_empty() {
        return 0;
    }
    let mut count = 0;
    let mut idx = 0;
    while let Some(pos) = find_units(&haystack.as_utf16()[idx..], needle.as_utf16()) {
        count += 1;
        idx += pos + needle.len_utf16();
    }
    count
}

/// Replace the FIRST occurrence of `needle` with `replacement`.
pub(crate) fn replace_first(
    haystack: &RuntimeString,
    needle: &RuntimeString,
    replacement: &RuntimeString,
) -> RuntimeString {
    match find_units(haystack.as_utf16(), needle.as_utf16()) {
        None => haystack.clone(),
        Some(idx) => haystack
            .slice_utf16(..idx)
            .concat(replacement)
            .concat(&haystack.slice_utf16(idx + needle.len_utf16()..)),
    }
}

/// Replace EVERY occurrence of `sep` with `rep`.
pub(crate) fn split_join(
    haystack: &RuntimeString,
    sep: &RuntimeString,
    rep: &RuntimeString,
) -> RuntimeString {
    if sep.is_empty() {
        // Splits between each UTF-16 code unit, joined with `rep`.
        let pieces: Vec<RuntimeString> = haystack
            .as_utf16()
            .iter()
            .map(|unit| RuntimeString::from_utf16(vec![*unit]))
            .collect();
        return join_exact(&pieces, rep);
    }
    let mut output = RuntimeString::default();
    let mut index = 0;
    while let Some(position) = find_units(&haystack.as_utf16()[index..], sep.as_utf16()) {
        output = output
            .concat(&haystack.slice_utf16(index..index + position))
            .concat(rep);
        index += position + sep.len_utf16();
    }
    output.concat(&haystack.slice_utf16(index..))
}

fn find_units(haystack: &[u16], needle: &[u16]) -> Option<usize> {
    if needle.is_empty() {
        Some(0)
    } else {
        haystack
            .windows(needle.len())
            .position(|window| window == needle)
    }
}

pub(crate) fn contains_exact(haystack: &RuntimeString, needle: &RuntimeString) -> bool {
    find_units(haystack.as_utf16(), needle.as_utf16()).is_some()
}

fn join_exact(values: &[RuntimeString], separator: &RuntimeString) -> RuntimeString {
    let mut output = RuntimeString::default();
    for (index, value) in values.iter().enumerate() {
        if index != 0 {
            output = output.concat(separator);
        }
        output = output.concat(value);
    }
    output
}

fn edit_output(path: &RuntimePath, replacements: usize) -> RuntimeJson {
    let mut output = RuntimeObject::new();
    output.insert(
        "path",
        RuntimeJson::String(posix_rendered_runtime(path).as_runtime_string().clone()),
    );
    output.insert("replacements", RuntimeJson::from(replacements as u64));
    RuntimeJson::Object(output)
}

fn runtime_to_error(value: &RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8()).expect("Node UTF-8 error projection is valid UTF-8")
}

#[async_trait]
impl Tool for EditTool {
    fn static_name(&self) -> &str {
        "edit"
    }

    fn static_description(&self) -> &str {
        "Replace an exact string in a file. Keep oldString minimal — usually 1-3 lines, just enough to be unique. Fails if oldString is missing or (without replaceAll) not unique. The call is also rejected if the file changed on disk since you last read it; read it again first."
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
        Some(RuntimeString::from("Editing ").concat(&path))
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
        let old_string = input
            .get("oldString")
            .and_then(RuntimeJson::as_string)
            .cloned()
            .unwrap_or_default();
        let new_string = input
            .get("newString")
            .and_then(RuntimeJson::as_string)
            .cloned()
            .unwrap_or_default();
        let replace_all = input
            .get("replaceAll")
            .and_then(RuntimeJson::as_bool)
            .unwrap_or(false);
        let abs = resolve_runtime(&ctx.cwd, &[RuntimePath::from(path_str.clone())])
            .map_err(|error| ToolError::new(runtime_to_error(error.message())))?;
        let os_abs = project_runtime_path(&abs, "path")
            .map_err(|error| ToolError::new(runtime_to_error(error.message())))?;
        // Touching this path may activate a conditional skill whose `paths` glob
        // matches.
        if let Some(activate) = &ctx.activate_skill_paths {
            activate(vec![abs.clone()]);
        }
        let mut pinned_file = match &ctx.permission.pinned_memory_root {
            Some(root) => match root.open_edit(&os_abs) {
                Ok(file) => Some(file),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(text_result(
                        edit_output(&abs, 0),
                        RuntimeString::from("File not found: ").concat(&path_str),
                        true,
                    ));
                }
                Err(error) => {
                    return Err(ToolError::new(format!(
                        "pinned memory edit failed: {error}"
                    )))
                }
            },
            None => None,
        };
        let effective_os_abs = os_abs.clone();
        let st = match &pinned_file {
            Some(file) => file.metadata().ok(),
            None => tokio::fs::metadata(&effective_os_abs).await.ok(),
        };
        if !st.as_ref().map(|m| m.is_file()).unwrap_or(false) {
            return Ok(text_result(
                edit_output(&abs, 0),
                RuntimeString::from("File not found: ").concat(&path_str),
                true,
            ));
        }
        let st = st.unwrap();

        // Read with encoding detection + CRLF→LF normalization: the model's LF
        // `old_string` matches a CRLF file, and a utf16le file decodes correctly.
        let TextFile {
            content,
            encoding,
            line_endings,
        } = match pinned_file.take() {
            Some(file) => {
                let (file, text) =
                    file.run_blocking(read_text_file_from_file)
                        .await
                        .map_err(|error| {
                            ToolError::new(format!("pinned memory edit read failed: {error}"))
                        })?;
                file.validate_namespace().map_err(|error| {
                    ToolError::new(format!(
                        "pinned memory edit lost its namespace identity while reading: {error}"
                    ))
                })?;
                pinned_file = Some(file);
                text
            }
            None => read_text_file(&effective_os_abs)
                .await
                .map_err(|e| ToolError::new(e.to_string()))?,
        };
        // Stale-write guard. A file with no baseline is NOT blocked: `old_string`
        // is resolved against `content`, the text just read off disk, so nothing
        // the agent has not seen can be clobbered. What is refused is a file the
        // agent read and that really changed since — the model is reasoning about
        // a version that no longer exists.
        if let Some(blocked) =
            check_stale_write(&abs, mtime_ms(&st), ctx.file_state.as_ref(), &content)
        {
            return Ok(text_result(
                edit_output(&abs, 0),
                RuntimeString::from(blocked),
                true,
            ));
        }

        // Pre-match input rewrites: strip trailing whitespace from newString
        // (except Markdown), and — when oldString fails to match exactly — restore
        // transport-shortened reserved tags, mirroring onto newString.
        let markdown = is_markdown_path(&abs);
        let mut effective_old = old_string;
        let mut effective_new = if markdown {
            new_string
        } else {
            strip_trailing_whitespace(&new_string)
        };
        if !contains_exact(&content, &effective_old) {
            let DesanitizeResult {
                result,
                applied_replacements,
            } = desanitize_reserved_tags(&effective_old);
            if !applied_replacements.is_empty() && contains_exact(&content, &result) {
                effective_old = result;
                for ar in &applied_replacements {
                    effective_new = split_join(&effective_new, &ar.from, &ar.to);
                }
            }
        }

        // No-op guard on the NORMALIZED old/new: a true no-op is rejected, but an
        // edit whose only change is stripped trailing whitespace still applies.
        if effective_old == effective_new {
            return Ok(text_result(
                edit_output(&abs, 0),
                RuntimeString::from("oldString and newString are identical"),
                true,
            ));
        }

        // Resolve oldString to the actual file substring, tolerating quote /
        // escape mismatches; count + replace operate on the REAL bytes.
        let replaced: RuntimeString;
        let replacements: usize;
        match find_actual_string(&content, &effective_old) {
            Some(actual_old) => {
                let count = count_occurrences(&content, &actual_old);
                if count > 1 && !replace_all {
                    return Ok(text_result(
                        edit_output(&abs, 0),
                        RuntimeString::from("oldString is not unique (")
                            .concat(&RuntimeString::from(count.to_string()))
                            .concat(&RuntimeString::from(
                                " matches); pass replaceAll or add more context",
                            )),
                        true,
                    ));
                }
                // Re-apply the quote and `\uXXXX` transforms to newString so the
                // file keeps its representation; a no-op for an exact match.
                let new_str = preserve_unicode_escape(
                    &effective_old,
                    &actual_old,
                    &preserve_quote_style(&effective_old, &actual_old, &effective_new),
                );
                replaced = if replace_all {
                    split_join(&content, &actual_old, &new_str)
                } else {
                    replace_first(&content, &actual_old, &new_str)
                };
                replacements = if replace_all { count } else { 1 };
            }
            None => {
                // Representation ladder missed — fall through to the
                // whitespace-flexible recovery, skipped for Markdown (a trailing
                // two-space hard break is load-bearing there).
                let flex = if markdown {
                    None
                } else {
                    flexible_line_replace(&content, &effective_old, &effective_new)
                };
                match flex {
                    None => {
                        return Ok(text_result(
                            edit_output(&abs, 0),
                            RuntimeString::from("oldString not found in ")
                                .concat(&path_str)
                                .concat(&escape_mismatch_hint(&effective_old)),
                            true,
                        ));
                    }
                    Some(flex) => {
                        if flex.occurrences > 1 && !replace_all {
                            return Ok(text_result(
                                edit_output(&abs, 0),
                                RuntimeString::from("oldString is not unique (")
                                    .concat(&RuntimeString::from(flex.occurrences.to_string()))
                                    .concat(&RuntimeString::from(
                                        " matches); pass replaceAll or add more context",
                                    )),
                                true,
                            ));
                        }
                        // A flexible match that reproduces the file byte-for-byte
                        // is a genuine no-op — reject it rather than report a
                        // phantom replacement.
                        if flex.new_content == content {
                            return Ok(text_result(
                                edit_output(&abs, 0),
                                RuntimeString::from("oldString and newString are identical"),
                                true,
                            ));
                        }
                        replaced = flex.new_content;
                        replacements = flex.occurrences;
                    }
                }
            }
        }

        let pctx = PathContext {
            cwd: &ctx.cwd,
            additional_writable_roots: &ctx.permission.additional_writable_roots,
            unconfined_writes: writes_are_unconfined(ctx.sandbox_mode),
        };
        if let Err(message) = assert_writable(&abs, &pctx).await {
            return Ok(text_result(edit_output(&abs, 0), message, true));
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
        // Against what this TURN first found in the file, not against the content
        // this one edit replaced: consecutive edits to one file otherwise charge
        // the same lines once per edit.
        let baseline = ctx.file_state.turn_baseline(&abs, Some(&content));
        let existed_before = ctx.file_state.turn_existed(&abs, true);
        let counts = baseline.as_ref().and_then(|baseline| {
            crate::context::edited_files_diff::line_change_counts(baseline, &replaced)
        });
        // Restore the file's original encoding + line endings. Record the
        // CANONICAL written form (what a re-read decodes) so the agent's OWN edit
        // never trips the stale-write guard; `size` is the ON-DISK byte count.
        let (written, after) = match pinned_file.take() {
            Some(file) => {
                let owned_replaced = replaced.clone();
                let (file, written) = file
                    .replace_blocking(move |handle| {
                        write_text_file_to_file(handle, &owned_replaced, encoding, line_endings)
                    })
                    .await
                    .map_err(|error| {
                        ToolError::new(format!("pinned memory edit write failed: {error}"))
                    })?;
                file.validate_namespace().map_err(|error| {
                    ToolError::new(format!(
                        "pinned memory edit lost its namespace identity while writing: {error}"
                    ))
                })?;
                let metadata = file.metadata().map_err(|error| {
                    ToolError::new(format!("pinned memory edit stat failed: {error}"))
                })?;
                pinned_file = Some(file);
                (written, metadata)
            }
            None => {
                let written = write_text_file(&effective_os_abs, &replaced, encoding, line_endings)
                    .await
                    .map_err(|e| ToolError::new(e.to_string()))?;
                let metadata = tokio::fs::metadata(&effective_os_abs)
                    .await
                    .map_err(|e| ToolError::new(e.to_string()))?;
                (written, metadata)
            }
        };
        ctx.file_state.record(
            &abs,
            mtime_ms(&after),
            RecordOptions {
                size: Some(after.len()),
                content: Some(written),
                encoding: Some(to_state_enc(encoding)),
                partial_view: false,
            },
        );
        (ctx.emit)(ToolDomainEvent::FileChange {
            change: FileChange {
                turn_diff: baseline.as_ref().map(|before| {
                    crate::context::edited_files_diff::full_turn_file_diff(
                        before,
                        &replaced,
                        existed_before,
                        true,
                    )
                }),
                path: posix_rendered_runtime(&abs).as_runtime_string().clone(),
                kind: FileChangeKind::Modify,
                diff: Some(crate::context::edited_files_diff::full_file_diff(
                    &content, &replaced,
                )),
                added: counts.map(|(added, _)| added),
                removed: counts.map(|(_, removed)| removed),
            },
        })
        .await;
        // Sync the language server (if any) so its diagnostics reflect the edit.
        if let Some(lsp) = &ctx.lsp {
            lsp.notify_edited(&abs, Some(&replaced)).await;
        }
        drop(lsp_mutation);
        drop(pinned_file);

        let plural = if replacements == 1 { "" } else { "s" };
        Ok(text_result(
            edit_output(&abs, replacements),
            RuntimeString::from("Edited ")
                .concat(&path_str)
                .concat(&RuntimeString::from(" ("))
                .concat(&RuntimeString::from(replacements.to_string()))
                .concat(&RuntimeString::from(" replacement"))
                .concat(&RuntimeString::from(plural))
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
    use crate::types::file_state::{FileStateCache, FileStateEntry};
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
            "ae-edit-tool-{}-{}-{}",
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
                    partial_view: opts.partial_view,
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

    /// Write `contents` to disk and record it as read in `state`.
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
                partial_view: false,
            },
        );
    }

    #[test]
    fn exposes_expected_metadata() {
        let tool = create_edit_tool();
        assert_eq!(tool.name(), "edit");
        assert_eq!(tool.permission_path_kind(), Some(PermissionPathKind::Edit));
        let input = json!({ "path": "a.txt", "oldString": "x", "newString": "y" });
        assert!(!tool.is_read_only(&input));
        assert_eq!(tool.is_destructive(&input), Some(false));
        assert_eq!(
            tool.summarize(&input).as_ref(),
            Some(&RuntimeString::from("Editing a.txt"))
        );
    }

    #[test]
    fn input_schema_shape() {
        let tool = create_edit_tool();
        let schema = tool.static_input_schema();
        assert_eq!(
            schema.get("required").unwrap(),
            &serde_json::json!(["path", "oldString", "newString"])
        );
        let old = schema
            .get("properties")
            .and_then(|p| p.get("oldString"))
            .unwrap();
        assert!(old
            .get("description")
            .and_then(serde_json::Value::as_str)
            .unwrap()
            .contains("byte-for-byte"));
    }

    #[test]
    fn empty_separator_split_join_preserves_javascript_utf16_units() {
        assert_eq!(
            runtime_json::stringify(&RuntimeJson::String(split_join(
                &RuntimeString::from("😀"),
                &RuntimeString::default(),
                &RuntimeString::from("|"),
            ))),
            r#""\ud83d|\ude00""#
        );
        let parsed = runtime_json::parse(&RuntimeString::from(r#""\ud800x""#)).unwrap();
        assert_eq!(
            runtime_json::stringify(&RuntimeJson::String(split_join(
                parsed.as_str().unwrap(),
                &RuntimeString::default(),
                &RuntimeString::from("|"),
            ))),
            r#""\ud800|x""#
        );
    }

    #[tokio::test]
    async fn replaces_a_unique_string() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let state = Arc::new(RecordingState::default());
        let events = Arc::new(Mutex::new(Vec::new()));
        seed(state.as_ref(), &dir.join("f.txt"), "alpha\nbeta\ngamma\n");
        let ctx = make_ctx(&cwd, state.clone(), events.clone());
        let tool = create_edit_tool();

        let p = dir.join("f.txt").to_string_lossy().into_owned();
        let res = tool
            .call(
                json!({ "path": p, "oldString": "beta", "newString": "BETA" }),
                &ctx,
                None,
            )
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            res.serializable.output.get("replacements").unwrap(),
            &json!(1)
        );
        assert_eq!(
            std::fs::read_to_string(dir.join("f.txt")).unwrap(),
            "alpha\nBETA\ngamma\n"
        );
        assert!(text_of(&res).ends_with("(1 replacement)"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn edits_a_file_that_was_never_recorded_as_read() {
        // A ranged read records no content, and a grep/shell view records nothing
        // at all. Neither can hide a clobber here, because oldString has to match
        // the text read off disk in this call, so the edit runs.
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let state = Arc::new(RecordingState::default());
        let events = Arc::new(Mutex::new(Vec::new()));
        std::fs::write(dir.join("f.txt"), "alpha\nbeta\ngamma\n").unwrap();
        let ctx = make_ctx(&cwd, state.clone(), events.clone());
        let tool = create_edit_tool();

        let p = dir.join("f.txt").to_string_lossy().into_owned();
        let res = tool
            .call(
                json!({ "path": p, "oldString": "beta", "newString": "BETA" }),
                &ctx,
                None,
            )
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            std::fs::read_to_string(dir.join("f.txt")).unwrap(),
            "alpha\nBETA\ngamma\n"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn refuses_a_file_that_changed_under_the_recorded_baseline() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let state = Arc::new(RecordingState::default());
        let events = Arc::new(Mutex::new(Vec::new()));
        let file = dir.join("f.txt");
        // Baseline: what the agent read, stamped old. Disk then moved on to
        // different content with a current (newer) mtime.
        state.record(
            &crate::fs::node_path::runtime_path_from_native(&file),
            1000.0,
            RecordOptions {
                size: None,
                content: Some(RuntimeString::from("alpha\nbeta\ngamma\n")),
                encoding: Some(StateEncoding::Utf8),
                partial_view: false,
            },
        );
        std::fs::write(&file, "alpha\nbeta\nDELTA\n").unwrap();
        let ctx = make_ctx(&cwd, state.clone(), events.clone());
        let tool = create_edit_tool();

        let p = file.to_string_lossy().into_owned();
        let res = tool
            .call(
                json!({ "path": p, "oldString": "beta", "newString": "BETA" }),
                &ctx,
                None,
            )
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(text_of(&res), crate::tools::freshness::STALE_MESSAGE);
        // Left untouched.
        assert_eq!(
            std::fs::read_to_string(&file).unwrap(),
            "alpha\nbeta\nDELTA\n"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn parsed_lone_surrogates_project_at_edit_fs_boundaries() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let actual = dir.join("\u{fffd}.txt");
        std::fs::write(&actual, "before").unwrap();
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
                content: Some(RuntimeString::from("before")),
                encoding: Some(StateEncoding::Utf8),
                partial_view: false,
            },
        );
        let ctx = make_ctx(&cwd, state, Arc::new(Mutex::new(Vec::new())));
        let input = runtime_json::parse(&RuntimeString::from(
            r#"{"path":"\ud800.txt","oldString":"before","newString":"\ud801"}"#,
        ))
        .unwrap();

        let result = create_edit_tool().call(input, &ctx, None).await.unwrap();

        assert_eq!(result.serializable.is_error, Some(false));
        assert_eq!(std::fs::read(actual).unwrap(), "\u{fffd}".as_bytes());
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
                partial_view: false,
            },
        );
        let ctx = make_ctx(&cwd, state, Arc::new(Mutex::new(Vec::new())));

        let result = create_edit_tool()
            .call(
                json!({
                    "path": path.to_string_lossy(),
                    "oldString": "",
                    "newString": "|",
                    "replaceAll": true
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
    async fn not_unique_without_replace_all_errors() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let state = Arc::new(RecordingState::default());
        let events = Arc::new(Mutex::new(Vec::new()));
        seed(state.as_ref(), &dir.join("f.txt"), "x\nx\n");
        let ctx = make_ctx(&cwd, state.clone(), events.clone());
        let tool = create_edit_tool();

        let p = dir.join("f.txt").to_string_lossy().into_owned();
        let res = tool
            .call(
                json!({ "path": p, "oldString": "x", "newString": "y" }),
                &ctx,
                None,
            )
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res),
            "oldString is not unique (2 matches); pass replaceAll or add more context"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn replace_all_replaces_every_occurrence() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let state = Arc::new(RecordingState::default());
        let events = Arc::new(Mutex::new(Vec::new()));
        seed(state.as_ref(), &dir.join("f.txt"), "x\nx\n");
        let ctx = make_ctx(&cwd, state.clone(), events.clone());
        let tool = create_edit_tool();

        let p = dir.join("f.txt").to_string_lossy().into_owned();
        let res = tool
            .call(
                json!({ "path": p, "oldString": "x", "newString": "y", "replaceAll": true }),
                &ctx,
                None,
            )
            .await
            .unwrap();

        assert_eq!(
            res.serializable.output.get("replacements").unwrap(),
            &json!(2)
        );
        assert_eq!(
            std::fs::read_to_string(dir.join("f.txt")).unwrap(),
            "y\ny\n"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn identical_old_new_is_rejected() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let state = Arc::new(RecordingState::default());
        let events = Arc::new(Mutex::new(Vec::new()));
        seed(state.as_ref(), &dir.join("f.txt"), "keep\n");
        let ctx = make_ctx(&cwd, state.clone(), events.clone());
        let tool = create_edit_tool();

        let p = dir.join("f.txt").to_string_lossy().into_owned();
        let res = tool
            .call(
                json!({ "path": p, "oldString": "keep", "newString": "keep" }),
                &ctx,
                None,
            )
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(text_of(&res), "oldString and newString are identical");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn missing_file_reports_not_found() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let state = Arc::new(RecordingState::default());
        let events = Arc::new(Mutex::new(Vec::new()));
        let ctx = make_ctx(&cwd, state.clone(), events.clone());
        let tool = create_edit_tool();

        let res = tool
            .call(
                json!({ "path": "nope.txt", "oldString": "a", "newString": "b" }),
                &ctx,
                None,
            )
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(text_of(&res), "File not found: nope.txt");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn count_and_replace_helpers() {
        assert_eq!(
            count_occurrences(&RuntimeString::from("aaa"), &RuntimeString::from("a")),
            3
        );
        assert_eq!(
            count_occurrences(&RuntimeString::from("abcabc"), &RuntimeString::from("bc")),
            2
        );
        assert_eq!(
            count_occurrences(&RuntimeString::from("x"), &RuntimeString::default()),
            0
        );
        assert_eq!(
            replace_first(
                &RuntimeString::from("abcabc"),
                &RuntimeString::from("bc"),
                &RuntimeString::from("Z"),
            ),
            RuntimeString::from("aZabc")
        );
        assert_eq!(
            split_join(
                &RuntimeString::from("abcabc"),
                &RuntimeString::from("bc"),
                &RuntimeString::from("Z"),
            ),
            RuntimeString::from("aZaZ")
        );
    }
}
