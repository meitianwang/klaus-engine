//! The `write` [`Tool`]: create a new file, or fully overwrite an existing one,
//! with the model-supplied contents.
//!
//! The write is gated by the edit-class path-permission rules and a
//! read-before-write / stale-write guard: an EXISTING file must have been read
//! (and not changed on disk) before it can be overwritten. Trailing whitespace is
//! stripped from every line (except Markdown), the existing file's encoding is
//! preserved (utf8 / utf16le), and the content is written as LF. A sub-agent that
//! writes a REPORT/SUMMARY/FINDINGS/ANALYSIS markdown file is softly nudged to
//! return its findings as text instead.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::fs::edit_input_rewrite::{is_markdown_path, strip_trailing_whitespace};
use crate::fs::node_path::{
    dirname_runtime, posix_rendered_runtime, project_runtime_path, resolve_runtime,
};
use crate::fs::path_safety::{assert_writable, writes_are_unconfined, PathContext};
use crate::fs::text_file::{
    read_text_file, read_text_file_from_file, write_text_file, write_text_file_to_file,
    Encoding as FileEncoding, LineEndings,
};
use crate::runtime_values::{RuntimeJson, RuntimeObject, RuntimePath, RuntimeString};
use crate::schemas::content::HarnessEnvelope;
use crate::tools::build_tool::{text_result, text_result_with_envelope};
use crate::tools::freshness::check_read_before_write;
use crate::types::errors::ToolError;
use crate::types::events::{FileChange, FileChangeKind, ToolDomainEvent};
use crate::types::file_state::{Encoding as StateEncoding, RecordOptions};
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{
    PathTargets, PermissionPathKind, Tool, ToolContext, ToolProgress, ToolResult,
};

/// Report-file basenames a sub-agent is NUDGED away from writing — it should
/// usually return findings as text. This is a soft system-prompt-style
/// instruction, not a hard tool block: the write still succeeds, and a non-error
/// reminder is appended so the sub-agent prefers inlining its findings.
pub(crate) const WRITE_REPORT_NUDGE: &str = "Sub-agents should usually return their findings as text in the final response rather than writing them to a report file. The file was written, but prefer including this content directly in your response.";

pub(crate) fn write_success_result(
    output: Json,
    summary: RuntimeString,
    include_report_nudge: bool,
) -> ToolResult {
    let mut result = text_result(output, summary, false);
    if include_report_nudge {
        let hinted = text_result_with_envelope(
            Json::Null,
            RuntimeString::from(WRITE_REPORT_NUDGE),
            false,
            HarnessEnvelope::SystemReminder,
        );
        result
            .serializable
            .model_content
            .extend(hinted.serializable.model_content);
    }
    result
}

/// Build the tool's input JSON Schema: required `path` + `content` strings, plus
/// the truncation-continuation protocol (`append` + `continuation_context`).
fn build_input_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "path": {
                "type": "string",
                "description": "Absolute or cwd-relative file path"
            },
            "content": {
                "type": "string",
                "description": "Full file contents to write. When `append` is true, only the remaining content, starting right after the `continuation_context`."
            },
            "append": {
                "type": "boolean",
                "description": "Set to true to append `content` to an existing file, continuing a previous Write that was cut off by the output limit (or deliberately generating a file in parts). Default: false."
            },
            "continuation_context": {
                "type": "string",
                "description": "The last 3 lines of the file as written so far. Required when `append` is true: it is checked against the file's actual tail so the continuation lands at the right position. Read the last 3 lines of the file and pass them here."
            }
        },
        "required": ["path", "content"],
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .expect("write schema is an object")
}

/// Write a file (creating parent dirs). Path-safety checked; refuses
/// symlinks/protected paths.
pub struct WriteTool {
    input_schema: JsonSchema,
}

impl WriteTool {
    fn new() -> Self {
        WriteTool {
            input_schema: build_input_schema(),
        }
    }
}

/// Construct the `write` tool as an erased registry-side [`Tool`].
pub fn create_write_tool() -> Arc<dyn Tool> {
    Arc::new(WriteTool::new())
}

/// Map a tracked-file encoding to the text-I/O encoding used for writing.
fn to_file_enc(e: StateEncoding) -> FileEncoding {
    match e {
        StateEncoding::Utf8 => FileEncoding::Utf8,
        StateEncoding::Utf16Le => FileEncoding::Utf16Le,
    }
}

/// The mtime (ms) of a stat result, floored to 0 on any clock error.
fn mtime_ms(md: &std::fs::Metadata) -> f64 {
    md.modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as f64)
        .unwrap_or(0.0)
}

/// The last path segment of a (normalized) path — enough for the report-file
/// nudge check.
fn ascii_lower(unit: u16) -> u16 {
    if (u16::from(b'A')..=u16::from(b'Z')).contains(&unit) {
        unit + u16::from(b'a' - b'A')
    } else {
        unit
    }
}

/// Whether a path's basename receives the sub-agent report-write nudge.
pub(crate) fn is_report_file_path(path: &RuntimePath) -> bool {
    let basename = crate::fs::node_path::basename_runtime(path).into_runtime_string();
    let folded = basename
        .as_utf16()
        .iter()
        .copied()
        .map(ascii_lower)
        .collect::<Vec<_>>();
    let starts_with_report_name = ["report", "summary", "findings", "analysis"]
        .iter()
        .any(|prefix| folded.starts_with(&prefix.encode_utf16().collect::<Vec<_>>()));
    starts_with_report_name && folded.ends_with(&".md".encode_utf16().collect::<Vec<_>>())
}

fn runtime_to_summary(value: &RuntimeString) -> String {
    String::from_utf8(value.to_node_utf8()).expect("Node UTF-8 tool summary is valid UTF-8")
}

fn write_output(path: &RuntimePath, bytes: usize, kind: &str) -> RuntimeJson {
    let mut output = RuntimeObject::new();
    output.insert(
        "path",
        RuntimeJson::String(posix_rendered_runtime(path).as_runtime_string().clone()),
    );
    output.insert("bytes", RuntimeJson::from(bytes as u64));
    output.insert("kind", RuntimeJson::from(kind));
    RuntimeJson::Object(output)
}

/// Lines the `continuation_context` protocol compares: the model is told to pass
/// "the last 3 lines of the file".
const CONTINUATION_CONTEXT_LINES: usize = 3;

/// Reported when an `append` cannot be positioned. The sentence is addressed to
/// the model and its next action depends on reading it.
const CONTINUATION_MISMATCH_MESSAGE: &str = "continuation_context mismatch, the file content has changed. Please read the file to get the latest content before retrying.";
const CONTINUATION_MISSING_MESSAGE: &str = "append is true but continuation_context is missing. Read the last 3 lines of the file and pass them as continuation_context.";
const APPEND_TARGET_MISSING_MESSAGE: &str = "append is true but the file does not exist. Write creates it; append continues an existing file.";

/// Split LF-normalized content into lines, dropping the empty line a trailing
/// newline would otherwise produce, and keep the last `n`.
///
/// Both sides of the `continuation_context` comparison go through this, so a
/// file that ends with a newline and a model that omitted it agree.
fn tail_lines(content: &RuntimeString, n: usize) -> Vec<RuntimeString> {
    let units = content.as_utf16();
    let mut lines: Vec<RuntimeString> = Vec::new();
    let mut start = 0usize;
    for (index, unit) in units.iter().enumerate() {
        if *unit == u16::from(b'\n') {
            lines.push(RuntimeString::from_utf16(units[start..index].to_vec()));
            start = index + 1;
        }
    }
    if start < units.len() {
        lines.push(RuntimeString::from_utf16(units[start..].to_vec()));
    }
    while lines.last().is_some_and(|line| line.len_utf16() == 0) {
        lines.pop();
    }
    let skip = lines.len().saturating_sub(n);
    lines.into_iter().skip(skip).collect()
}

/// Whether a model-supplied `continuation_context` names the file's actual tail.
fn continuation_context_matches(existing: &RuntimeString, supplied: &RuntimeString) -> bool {
    let want = tail_lines(existing, CONTINUATION_CONTEXT_LINES);
    let got = tail_lines(supplied, CONTINUATION_CONTEXT_LINES);
    if want.len() != got.len() {
        return false;
    }
    want.iter()
        .zip(got.iter())
        .all(|(a, b)| a.as_utf16() == b.as_utf16())
}

/// The file's last [`CONTINUATION_CONTEXT_LINES`] lines, joined with LF — the
/// exact value the append protocol expects back as `continuation_context`.
///
/// Returned to the model rather than left for it to guess: it was told to "read
/// the last 3 lines of the file", and handing it the answer removes a round trip
/// and the chance it reads them differently than the check does.
pub(crate) fn continuation_context_for(content: &RuntimeString) -> RuntimeString {
    let lines = tail_lines(content, CONTINUATION_CONTEXT_LINES);
    let mut joined = RuntimeString::default();
    for (index, line) in lines.iter().enumerate() {
        if index != 0 {
            joined = joined.concat(&RuntimeString::from("\n"));
        }
        joined = joined.concat(line);
    }
    joined
}

/// The instruction returned with a `write` that landed PARTIAL content because the
/// model's arguments were cut off by the output ceiling.
///
/// Parameter names are this tool's own (`path`, `content`): the sentence is read
/// as an instruction to call THIS tool, and a name its schema rejects would fail
/// the follow-up call.
pub(crate) fn truncated_write_continuation(
    path: &RuntimeString,
    continuation_context: &RuntimeString,
) -> RuntimeString {
    RuntimeString::from(format!(
        "File creation was truncated. To continue writing, call Write with:\n- path: {path}\n- append: true\n- continuation_context: \"{continuation}\"\n- content: <remaining content, starting right after the continuation_context>",
        path = String::from_utf8_lossy(&path.to_node_utf8()),
        continuation = String::from_utf8_lossy(&continuation_context.to_node_utf8()),
    ))
}

#[async_trait]
impl Tool for WriteTool {
    fn static_name(&self) -> &str {
        "write"
    }

    fn static_description(&self) -> &str {
        "Create a new file, or fully rewrite an existing one, with the given contents. To change part of an existing file prefer the edit tool (it sends only the diff); read a file before overwriting it."
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
        Some(true)
    }

    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        let path = input
            .get("path")
            .and_then(RuntimeJson::as_string)
            .cloned()
            .unwrap_or_default();
        Some(RuntimeString::from("Writing ").concat(&path))
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
        let input_content = input
            .get("content")
            .and_then(RuntimeJson::as_string)
            .cloned()
            .unwrap_or_default();
        let abs = resolve_runtime(&ctx.cwd, &[RuntimePath::from(path_str.clone())])
            .map_err(|error| ToolError::new(runtime_to_summary(error.message())))?;
        let os_abs = project_runtime_path(&abs, "path")
            .map_err(|error| ToolError::new(runtime_to_summary(error.message())))?;
        // Truncation-continuation protocol: `append` extends an existing file
        // with the remaining content, positioned by `continuation_context` (the
        // file's last 3 lines) so a continuation that drifted lands as a refusal
        // rather than a duplicated or misplaced chunk.
        //
        // The concatenation adds NO separator: the caller is told to supply
        // "remaining content, starting right after the continuation_context", so
        // the file's own tail decides whether a newline is needed.
        let append = input
            .get("append")
            .and_then(RuntimeJson::as_bool)
            .unwrap_or(false);
        // `Some((content, encoding))` read from disk for an append. The encoding
        // comes from the file itself rather than from fileState: an append target
        // may predate this session, and re-encoding a utf16le file as utf8 would
        // corrupt everything already in it.
        let mut append_existing: Option<(RuntimeString, FileEncoding)> = None;
        if append {
            let existing = if let Some(root) = &ctx.permission.pinned_memory_root {
                let file = root.open_read(&os_abs).map_err(|error| {
                    ToolError::new(format!("pinned resource append read failed: {error}"))
                })?;
                let (file, text) =
                    file.run_blocking(read_text_file_from_file)
                        .await
                        .map_err(|error| {
                            ToolError::new(format!("pinned resource append read failed: {error}"))
                        })?;
                file.validate_namespace().map_err(|error| {
                    ToolError::new(format!("pinned resource append target changed: {error}"))
                })?;
                text
            } else {
                match tokio::fs::metadata(&os_abs).await {
                    Ok(metadata) if metadata.is_file() => {
                        read_text_file(&os_abs).await.map_err(|error| {
                            ToolError::new(format!("failed to read the file to append to: {error}"))
                        })?
                    }
                    _ => {
                        return Ok(text_result(
                            write_output(&abs, 0, "append"),
                            APPEND_TARGET_MISSING_MESSAGE,
                            true,
                        ));
                    }
                }
            };
            let Some(supplied) = input
                .get("continuation_context")
                .and_then(RuntimeJson::as_string)
                .filter(|value| value.len_utf16() > 0)
            else {
                return Ok(text_result(
                    write_output(&abs, 0, "append"),
                    CONTINUATION_MISSING_MESSAGE,
                    true,
                ));
            };
            if !continuation_context_matches(&existing.content, supplied) {
                return Ok(text_result(
                    write_output(&abs, 0, "append"),
                    CONTINUATION_MISMATCH_MESSAGE,
                    true,
                ));
            }
            append_existing = Some((existing.content.clone(), existing.encoding));
        }
        // Strip trailing whitespace from every line before writing, except for
        // Markdown where two trailing spaces are a hard line break. The stripped
        // form is authoritative for the write, the reported byte count, and the
        // LSP sync.
        //
        // An append normalizes ONLY the new content: the bytes already on disk
        // were not produced by this call, so re-stripping them would silently
        // rewrite lines the model never asked to touch.
        let normalized = if is_markdown_path(&abs) {
            input_content
        } else {
            strip_trailing_whitespace(&input_content)
        };
        // The complete file content this call writes. For an append it is the
        // on-disk prefix concatenated with the new content; `content` is what every
        // step below (the write, the byte count, the diff, the LSP sync) reads.
        let content = match append_existing.as_ref() {
            Some((existing, _)) => existing.concat(&normalized),
            None => normalized,
        };
        // Touching this path may activate a conditional skill whose `paths` glob
        // matches.
        if let Some(activate) = &ctx.activate_skill_paths {
            activate(vec![abs.clone()]);
        }
        let pctx = PathContext {
            cwd: &ctx.cwd,
            additional_writable_roots: &ctx.permission.additional_writable_roots,
            unconfined_writes: writes_are_unconfined(ctx.sandbox_mode),
        };
        if let Err(message) = assert_writable(&abs, &pctx).await {
            return Ok(text_result(write_output(&abs, 0, "modify"), message, true));
        }
        let pinned_root = ctx.permission.pinned_memory_root.clone();
        // Admission for an existing target pins only that file. A missing leaf
        // (and its missing parents) stays absent through stale checks and the
        // LSP pre-edit gate; it is created exclusively at the mutation boundary.
        let mut pinned_file = match &pinned_root {
            Some(root) => match root.open_write_existing(&os_abs) {
                Ok(file) => Some(file),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => {
                    return Err(ToolError::new(format!(
                        "pinned memory write admission failed: {error}"
                    )));
                }
            },
            None => None,
        };
        let effective_os_abs = os_abs.clone();
        let st = match (&pinned_root, &pinned_file) {
            (_, Some(file)) => Some(file.metadata().map_err(|error| {
                ToolError::new(format!("pinned memory write stat failed: {error}"))
            })?),
            (Some(_), None) => None,
            (None, None) => tokio::fs::metadata(&effective_os_abs).await.ok(),
        };
        let existed = if pinned_root.is_some() {
            pinned_file.is_some()
        } else {
            st.as_ref().map(|m| m.is_file()).unwrap_or(false)
        };
        // Read-before-write + stale-write guard. Only for an EXISTING file —
        // creating a new file needs no prior read. Write blocks on mtime alone (no
        // content fallback), so an unread or externally-modified file is refused
        // until re-read.
        //
        // An append is exempt: it was just read from disk above, and its
        // `continuation_context` check is a CONTENT comparison against that read
        // — strictly stronger than the mtime-only guard, and the reason a drifted
        // file is refused here rather than there.
        if existed && !append {
            if let Some(st) = st.as_ref() {
                if let Some(blocked) =
                    check_read_before_write(&abs, mtime_ms(st), ctx.file_state.as_ref(), None)
                {
                    return Ok(text_result(
                        write_output(&abs, 0, "modify"),
                        RuntimeString::from(blocked),
                        true,
                    ));
                }
            }
        }
        // Preserve the existing file's encoding on overwrite: a utf16le file stays
        // utf16le; a new file is utf8. The read-before-write guard above means an
        // existing file was read, so its encoding is in fileState.
        //
        // An append takes its encoding from the disk read it just did instead: the
        // file may predate this session, so fileState may hold nothing (or an
        // unrelated utf8 default), and writing a utf16le file back as utf8 would
        // corrupt every line already in it.
        //
        // The same entry carries the content this write replaces, which is what
        // the change event's line counts are measured against. It has to be read
        // BEFORE the record below overwrites it with the new content.
        let previous = ctx.file_state.get(&abs);
        let enc = match append_existing.as_ref().map(|(_, encoding)| encoding) {
            Some(FileEncoding::Utf16Le) => StateEncoding::Utf16Le,
            Some(FileEncoding::Utf8) => StateEncoding::Utf8,
            None => previous
                .as_ref()
                .and_then(|e| e.encoding)
                .unwrap_or(StateEncoding::Utf8),
        };
        let parent = dirname_runtime(&abs);
        let os_parent = project_runtime_path(&parent, "path")
            .map_err(|error| ToolError::new(runtime_to_summary(error.message())))?;
        // Snapshot pre-edit LSP diagnostics BEFORE writing so the reminder diffs
        // against pre-edit state even for a pull server reading the file off
        // disk, then exclude same-document queries through the write + sync.
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
        if let Some(root) = &pinned_root {
            if pinned_file.is_none() {
                let file = root.create_write_new(&os_abs).map_err(|error| {
                    let detail = if error.kind() == std::io::ErrorKind::AlreadyExists {
                        "target appeared after stale-write admission; re-read before overwriting"
                            .to_string()
                    } else {
                        error.to_string()
                    };
                    ToolError::new(format!("pinned memory write creation failed: {detail}"))
                })?;
                pinned_file = Some(file);
            }
        } else {
            tokio::fs::create_dir_all(os_parent)
                .await
                .map_err(|e| ToolError::new(e.to_string()))?;
        }
        // A full Write writes the model's content with the file's encoding but as
        // LF — deliberately NOT re-applying the old file's line endings (the
        // model's content is authoritative). Record the CANONICAL written form
        // (BOM-inclusive for utf16le) so the baseline equals what a re-read
        // decodes.
        let (written, st_after) = match pinned_file.take() {
            Some(file) => {
                let owned_content = content.clone();
                let encoding = to_file_enc(enc);
                let (file, written) = file
                    .replace_blocking(move |handle| {
                        write_text_file_to_file(handle, &owned_content, encoding, LineEndings::Lf)
                    })
                    .await
                    .map_err(|error| {
                        ToolError::new(format!("pinned memory write failed: {error}"))
                    })?;
                file.validate_namespace().map_err(|error| {
                    ToolError::new(format!(
                        "pinned memory write lost its namespace identity: {error}"
                    ))
                })?;
                let metadata = file.metadata().map_err(|error| {
                    ToolError::new(format!("pinned memory write stat failed: {error}"))
                })?;
                pinned_file = Some(file);
                (written, metadata)
            }
            None => {
                let written = write_text_file(
                    &effective_os_abs,
                    &content,
                    to_file_enc(enc),
                    LineEndings::Lf,
                )
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
            mtime_ms(&st_after),
            RecordOptions {
                size: Some(st_after.len()),
                content: Some(written),
                encoding: Some(enc),
                partial_view: false,
            },
        );
        let kind = if existed { "modify" } else { "create" };
        // Measure against what THIS TURN first found here, not what the previous
        // write left: the same file written twice in one turn is one volume, not
        // two. A creation's turn baseline is empty content, so the whole new file
        // counts as added. A file with no readable baseline at all reports unknown
        // counts rather than claiming every line is new.
        let candidate = if existed {
            previous.and_then(|entry| entry.content)
        } else {
            Some(RuntimeString::default())
        };
        let baseline = ctx.file_state.turn_baseline(&abs, candidate.as_ref());
        let diff = if existed {
            candidate
                .as_ref()
                .map(|before| crate::context::edited_files_diff::full_file_diff(before, &content))
        } else {
            Some(content.clone())
        };
        let existed_before = ctx.file_state.turn_existed(&abs, existed);
        let counts = baseline.as_ref().and_then(|baseline| {
            crate::context::edited_files_diff::line_change_counts(baseline, &content)
        });
        (ctx.emit)(ToolDomainEvent::FileChange {
            change: FileChange {
                turn_diff: baseline.as_ref().map(|before| {
                    crate::context::edited_files_diff::full_turn_file_diff(
                        before,
                        &content,
                        existed_before,
                        true,
                    )
                }),
                path: posix_rendered_runtime(&abs).as_runtime_string().clone(),
                kind: if existed {
                    FileChangeKind::Modify
                } else {
                    FileChangeKind::Create
                },
                diff,
                added: counts.map(|(added, _)| added),
                removed: counts.map(|(_, removed)| removed),
            },
        })
        .await;
        // Sync the language server (if any) so its diagnostics reflect the new
        // content.
        if let Some(lsp) = &ctx.lsp {
            lsp.notify_edited(&abs, Some(&content)).await;
        }
        drop(lsp_mutation);
        // On Windows this retains the no-delete namespace lease through the
        // public success/event boundary; on Unix the post-write identity check
        // above is the operation's namespace linearization point.
        drop(pinned_file);
        // Soft report-file nudge: a sub-agent (depth > 0) that wrote a
        // REPORT/SUMMARY/FINDINGS/ANALYSIS markdown file is reminded to return
        // findings as text — non-blocking, the write already succeeded. A
        // top-level agent (depth 0) is never nudged.
        let include_report_nudge = ctx.depth.unwrap_or(0) > 0 && is_report_file_path(&abs);
        let bytes = content.to_node_utf8().len();
        let verb = if kind == "create" {
            "Created"
        } else {
            "Updated"
        };
        Ok(write_success_result(
            write_output(&abs, bytes, kind),
            RuntimeString::from(verb)
                .concat(&RuntimeString::from(" "))
                .concat(&path_str)
                .concat(&RuntimeString::from(format!(" ({bytes} bytes)"))),
            include_report_nudge,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::{runtime_codec, runtime_json, ScalarJson};
    use crate::schemas::content::OutputBlock;
    use crate::tools::freshness::NOT_READ_MESSAGE;
    use crate::types::abort::AbortSignal;
    use crate::types::file_state::{FileStateCache, FileStateEntry};
    use crate::types::permission::PermissionContext;
    use crate::types::sandbox::{
        ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec,
    };
    use std::collections::HashMap;
    use std::path::PathBuf;
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
            "ae-write-tool-{}-{}-{}",
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
        depth: Option<u32>,
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
            depth,
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

    #[test]
    fn exposes_expected_metadata() {
        let tool = create_write_tool();
        assert_eq!(tool.name(), "write");
        assert_eq!(tool.permission_path_kind(), Some(PermissionPathKind::Edit));
        let input = json!({ "path": "a.txt", "content": "x" });
        assert!(!tool.is_read_only(&input));
        assert!(!tool.is_concurrency_safe(&input));
        assert!(tool.is_enabled());
        assert_eq!(tool.is_destructive(&input), Some(true));
        assert_eq!(
            tool.summarize(&input).as_ref(),
            Some(&RuntimeString::from("Writing a.txt"))
        );
        assert_eq!(
            tool.get_path(&input),
            Some(PathTargets::One("a.txt".into()))
        );
    }

    #[test]
    fn input_schema_shape() {
        let tool = create_write_tool();
        let schema = tool.static_input_schema();
        assert_eq!(
            schema.get("type").and_then(serde_json::Value::as_str),
            Some("object")
        );
        assert_eq!(
            schema.get("required").unwrap(),
            &serde_json::json!(["path", "content"])
        );
        assert_eq!(
            schema.get("additionalProperties"),
            Some(&serde_json::json!(false))
        );
        let properties = schema.get("properties").unwrap();
        let content = properties.get("content").unwrap();
        assert_eq!(
            content
                .get("description")
                .and_then(serde_json::Value::as_str),
            Some(
                "Full file contents to write. When `append` is true, only the remaining content, starting right after the `continuation_context`."
            )
        );
        // The truncation-continuation protocol: both optional, and `required`
        // stays exactly path + content so a plain write is unchanged.
        let append = properties.get("append").unwrap();
        assert_eq!(
            append.get("type").and_then(serde_json::Value::as_str),
            Some("boolean")
        );
        let continuation = properties.get("continuation_context").unwrap();
        assert_eq!(
            continuation.get("type").and_then(serde_json::Value::as_str),
            Some("string")
        );
    }

    #[tokio::test]
    async fn creates_a_new_file_and_records_state() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let state = Arc::new(RecordingState::default());
        let events = Arc::new(Mutex::new(Vec::new()));
        let ctx = make_ctx(&cwd, state.clone(), events.clone(), None);
        let tool = create_write_tool();

        let res = tool
            .call(
                json!({ "path": "new.txt", "content": "hello\n" }),
                &ctx,
                None,
            )
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            std::fs::read_to_string(dir.join("new.txt")).unwrap(),
            "hello\n"
        );
        assert_eq!(text_of(&res), "Created new.txt (6 bytes)");
        let abs = resolve_runtime(&ctx.cwd, &[RuntimePath::from("new.txt")]).unwrap();
        assert!(state.has(&abs));
        assert_eq!(events.lock().unwrap().len(), 1);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The continuation protocol: an append extends the file, positioned by a
    /// `continuation_context` that matches the tail it was given. No separator is
    /// inserted — the caller supplies "remaining content, starting right after the
    /// continuation_context", so the file's own tail decides the newline.
    #[tokio::test]
    async fn append_continues_a_file_positioned_by_continuation_context() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let state = Arc::new(RecordingState::default());
        let events = Arc::new(Mutex::new(Vec::new()));
        let ctx = make_ctx(&cwd, state, events, None);
        let tool = create_write_tool();

        // Three lines on disk; the tail the model is told to echo back.
        tool.call(
            json!({ "path": "part.txt", "content": "one\ntwo\nthree\n" }),
            &ctx,
            None,
        )
        .await
        .unwrap();
        let res = tool
            .call(
                json!({
                    "path": "part.txt",
                    "append": true,
                    "continuation_context": "one\ntwo\nthree",
                    "content": "four\n",
                }),
                &ctx,
                None,
            )
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            std::fs::read_to_string(dir.join("part.txt")).unwrap(),
            "one\ntwo\nthree\nfour\n"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A drifted tail is refused, not written: that is the whole point of checking
    /// `continuation_context` — a continuation landing in the wrong place is worse
    /// than one that waits for a re-read.
    #[tokio::test]
    async fn append_refuses_a_continuation_context_that_does_not_match() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let state = Arc::new(RecordingState::default());
        let events = Arc::new(Mutex::new(Vec::new()));
        let ctx = make_ctx(&cwd, state, events, None);
        let tool = create_write_tool();

        tool.call(
            json!({ "path": "part.txt", "content": "one\ntwo\nthree\n" }),
            &ctx,
            None,
        )
        .await
        .unwrap();
        let res = tool
            .call(
                json!({
                    "path": "part.txt",
                    "append": true,
                    "continuation_context": "stale\nlines\nhere",
                    "content": "four\n",
                }),
                &ctx,
                None,
            )
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(true));
        assert!(
            text_of(&res).starts_with("continuation_context mismatch"),
            "the refusal names the mismatch: {}",
            text_of(&res)
        );
        assert_eq!(
            std::fs::read_to_string(dir.join("part.txt")).unwrap(),
            "one\ntwo\nthree\n",
            "a refused append leaves the file untouched"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// `append` without a `continuation_context` cannot be positioned, so it is
    /// refused rather than appended blindly.
    #[tokio::test]
    async fn append_requires_a_continuation_context() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let state = Arc::new(RecordingState::default());
        let events = Arc::new(Mutex::new(Vec::new()));
        let ctx = make_ctx(&cwd, state, events, None);
        let tool = create_write_tool();

        tool.call(
            json!({ "path": "part.txt", "content": "one\n" }),
            &ctx,
            None,
        )
        .await
        .unwrap();
        let res = tool
            .call(
                json!({ "path": "part.txt", "append": true, "content": "two\n" }),
                &ctx,
                None,
            )
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(true));
        assert!(
            text_of(&res).contains("continuation_context is missing"),
            "the refusal asks for the missing parameter: {}",
            text_of(&res)
        );
        assert_eq!(
            std::fs::read_to_string(dir.join("part.txt")).unwrap(),
            "one\n"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// An append to a missing file is refused: Write creates, append continues.
    #[tokio::test]
    async fn append_refuses_a_missing_target() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let state = Arc::new(RecordingState::default());
        let events = Arc::new(Mutex::new(Vec::new()));
        let ctx = make_ctx(&cwd, state, events, None);
        let tool = create_write_tool();

        let res = tool
            .call(
                json!({
                    "path": "absent.txt",
                    "append": true,
                    "continuation_context": "anything",
                    "content": "text\n",
                }),
                &ctx,
                None,
            )
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(true));
        assert!(
            text_of(&res).contains("does not exist"),
            "the refusal explains append needs an existing file: {}",
            text_of(&res)
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// `tail_lines` keeps the last N lines and tolerates a trailing newline, so a
    /// file that ends with one and a model that echoed it without one agree.
    #[test]
    fn continuation_context_matching_tolerates_a_trailing_newline() {
        let on_disk = RuntimeString::from("a\nb\nc\n");
        assert!(continuation_context_matches(
            &on_disk,
            &RuntimeString::from("a\nb\nc")
        ));
        assert!(continuation_context_matches(
            &on_disk,
            &RuntimeString::from("a\nb\nc\n")
        ));
        // Only the last three lines are compared, so older content may differ.
        assert!(continuation_context_matches(
            &RuntimeString::from("old\na\nb\nc\n"),
            &RuntimeString::from("a\nb\nc"),
        ));
        assert!(!continuation_context_matches(
            &on_disk,
            &RuntimeString::from("a\nb\nd")
        ));
        // A short file compares what it has, not a padded tail.
        assert!(continuation_context_matches(
            &RuntimeString::from("a\n"),
            &RuntimeString::from("a"),
        ));
        assert!(!continuation_context_matches(
            &RuntimeString::from("a\n"),
            &RuntimeString::from("a\nb"),
        ));
    }

    #[tokio::test]
    async fn parsed_lone_surrogates_project_at_path_and_utf8_write_boundaries() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let state = Arc::new(RecordingState::default());
        let events = Arc::new(Mutex::new(Vec::new()));
        let ctx = make_ctx(&cwd, state, events, None);
        let input = runtime_json::parse(&RuntimeString::from(
            r#"{"path":"\ud800.txt","content":"\ud801"}"#,
        ))
        .unwrap();

        let res = create_write_tool().call(input, &ctx, None).await.unwrap();

        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            std::fs::read(dir.join("\u{fffd}.txt")).unwrap(),
            "\u{fffd}".as_bytes()
        );
        // The tool echoes the resolved path POSIX-rendered, which is the shape the engine
        // promises the model on every host, so the expectation carries `/` even where the
        // resolved cwd itself arrived with the host separator.
        let expected_cwd = cwd.replace('\\', "/");
        assert_eq!(
            runtime_json::stringify(&res.serializable.output),
            format!(r#"{{"path":"{expected_cwd}/\ud800.txt","bytes":3,"kind":"create"}}"#)
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn strips_trailing_whitespace_on_write() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let state = Arc::new(RecordingState::default());
        let events = Arc::new(Mutex::new(Vec::new()));
        let ctx = make_ctx(&cwd, state.clone(), events.clone(), None);
        let tool = create_write_tool();

        tool.call(
            json!({ "path": "s.txt", "content": "a  \nb\t\n" }),
            &ctx,
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.join("s.txt")).unwrap(),
            "a\nb\n"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn blocks_overwrite_of_unread_existing_file() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        std::fs::write(dir.join("exists.txt"), "old\n").unwrap();
        let state = Arc::new(RecordingState::default());
        let events = Arc::new(Mutex::new(Vec::new()));
        let ctx = make_ctx(&cwd, state.clone(), events.clone(), None);
        let tool = create_write_tool();

        let res = tool
            .call(
                json!({ "path": "exists.txt", "content": "new\n" }),
                &ctx,
                None,
            )
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(text_of(&res), NOT_READ_MESSAGE);
        assert_eq!(
            std::fs::read_to_string(dir.join("exists.txt")).unwrap(),
            "old\n"
        );
        assert!(events.lock().unwrap().is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn sub_agent_report_write_gets_a_nudge() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let state = Arc::new(RecordingState::default());
        let events = Arc::new(Mutex::new(Vec::new()));
        let ctx = make_ctx(&cwd, state.clone(), events.clone(), Some(1));
        let tool = create_write_tool();

        let res = tool
            .call(
                json!({ "path": "REPORT.md", "content": "# hi\n" }),
                &ctx,
                None,
            )
            .await
            .unwrap();

        let text = text_of(&res);
        assert!(
            text.starts_with("Created REPORT.md (5 bytes)"),
            "text: {text}"
        );
        assert_eq!(res.serializable.model_content.len(), 2);
        let OutputBlock::Text(nudge) = &res.serializable.model_content[1] else {
            panic!("expected report nudge text block")
        };
        assert_eq!(nudge.text, WRITE_REPORT_NUDGE);
        assert_eq!(
            nudge.harness_envelope,
            Some(HarnessEnvelope::SystemReminder)
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
