//! The `apply_patch` [`Tool`]: applies a *** Begin Patch multi-file patch
//! (add / update / delete, with optional rename) in a single tool call, gated by
//! the edit-class path-permission rules.
//!
//! This wires the pure [`super::parser`] + [`super::apply`] logic into the
//! engine's [`Tool`] abstraction: it parses the patch, activates any
//! conditional skills on the target paths, snapshots pre-edit language-server
//! diagnostics, applies the ops through a path-safe [`FileIO`], emits
//! `file_change` progress, and reports a per-file summary.

use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::FutureExt;
use serde_json::{json, Map, Value};

use crate::fs::node_path::{
    dirname_runtime, posix_rendered_runtime, project_runtime_path, resolve_runtime,
};
use crate::fs::path_safety::{assert_writable, writes_are_unconfined, PathContext};
use crate::fs::text_file::{read_text_file, write_text_file, Encoding, LineEndings};
use crate::lsp::types::LspController;
use crate::runtime_values::{RuntimeJson, RuntimeObject, RuntimePath, RuntimeString};
use crate::types::errors::ToolError;
use crate::types::events::{FileChange, FileChangeKind, ToolDomainEvent};
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{
    PathTargets, PermissionPathKind, Tool, ToolContext, ToolProgress, ToolResult,
};

use super::apply::{
    apply_patch_ops_tracked, ApplyError, ApplyKind, ApplyOpsFailure, ApplyResult, FileIO,
};
use super::parser::{parse_patch, PatchOp};

/// Build the tool's input JSON Schema: a single required `patch` string.
fn build_input_schema() -> JsonSchema {
    let mut patch_prop = Map::new();
    patch_prop.insert("type".to_string(), json!("string"));
    patch_prop.insert(
        "description".to_string(),
        json!("A patch in the *** Begin Patch / *** End Patch format"),
    );
    let mut properties = Map::new();
    properties.insert("patch".to_string(), Value::Object(patch_prop));
    let mut schema = Map::new();
    schema.insert("type".to_string(), json!("object"));
    schema.insert("properties".to_string(), Value::Object(properties));
    schema.insert("required".to_string(), json!(["patch"]));
    schema.insert("additionalProperties".to_string(), json!(false));
    schema
}

/// Apply a *** Begin Patch multi-file patch (add / update / delete) in one tool call.
pub struct ApplyPatchTool {
    input_schema: JsonSchema,
}

impl ApplyPatchTool {
    fn new() -> Self {
        ApplyPatchTool {
            input_schema: build_input_schema(),
        }
    }
}

/// Construct the `apply_patch` tool as an erased registry-side [`Tool`].
pub fn create_apply_patch_tool() -> Arc<dyn Tool> {
    Arc::new(ApplyPatchTool::new())
}

#[async_trait]
impl Tool for ApplyPatchTool {
    fn static_name(&self) -> &str {
        "apply_patch"
    }

    fn static_description(&self) -> &str {
        "Apply a multi-file patch (add/update/delete) in the codex *** Begin Patch format."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn approval_metadata(&self) -> Option<crate::types::permission::ApprovalActionMetadata> {
        use crate::types::permission::{
            ApprovalActionCallSource, ApprovalActionInputKind, ApprovalActionMetadata,
            ApprovalActionSource,
        };
        Some(ApprovalActionMetadata {
            registered_tool_name: self.name(),
            source: ApprovalActionSource::Builtin,
            call_source: ApprovalActionCallSource::Direct,
            originating_item_id: None,
            input_kind: ApprovalActionInputKind::Function,
            paths: Vec::new(),
            path_kind: None,
            cwd: None,
            terminal_launch: None,
            network_trigger: None,
        })
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

    fn summarize(&self, _input: &Json) -> Option<RuntimeString> {
        Some(RuntimeString::from("Applying patch"))
    }

    /// Edit-class: every add / update / delete target (and a rename's
    /// destination) must clear the permission engine's path-scoped rules and the
    /// sensitive-file gate, so a patch can't quietly write a shell rc file, an
    /// engine config file, or a git hook under accept-edits. Only
    /// bypassPermissions skips the gate, because the user explicitly asked for no
    /// prompts at all.
    fn permission_path_kind(&self) -> Option<PermissionPathKind> {
        Some(PermissionPathKind::Edit)
    }

    fn get_path(&self, input: &Json) -> Option<PathTargets> {
        let patch = input
            .get("patch")
            .and_then(RuntimeJson::as_string)
            .cloned()
            .unwrap_or_default();
        // A malformed patch yields no paths — `call` will surface the parse error.
        let ops = match parse_patch(&patch) {
            Ok(ops) => ops,
            Err(_) => return None,
        };
        let mut paths: Vec<RuntimePath> = Vec::new();
        for op in &ops {
            paths.push(RuntimePath::from(op_path(op).clone()));
            if let PatchOp::Update {
                move_path: Some(mp),
                ..
            } = op
            {
                if !mp.is_empty() {
                    paths.push(RuntimePath::from(mp.clone()));
                }
            }
        }
        Some(PathTargets::Many(paths))
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let patch = input
            .get("patch")
            .and_then(RuntimeJson::as_string)
            .cloned()
            .unwrap_or_default();
        let results = match run_patch(&patch, ctx).await {
            Ok(applied) => applied,
            Err(message) => {
                return Ok(text_result(
                    empty_files_output(),
                    RuntimeString::from("apply_patch failed: ").concat(&message),
                    true,
                ));
            }
        };

        for r in &results {
            let source = resolve_tool_path(&ctx.cwd, &r.path).expect("an applied path resolves");
            let moved = r.move_path.as_ref().is_some_and(|path| path != &r.path);
            let source_after = if moved {
                None
            } else {
                r.after_content.as_ref()
            };
            let (source_turn_diff, source_counts) = turn_change_metrics(
                ctx,
                &source,
                r.turn_before_content.as_ref(),
                source_after,
                r.turn_before_exists,
            );
            if r.kind == ApplyKind::Update && moved {
                let mp = r.move_path.as_ref().expect("moved path exists");
                (ctx.emit)(ToolDomainEvent::FileChange {
                    change: FileChange {
                        path: posix_rendered_runtime(&source).into_runtime_string(),
                        kind: FileChangeKind::Delete,
                        diff: r.before_content.clone(),
                        turn_diff: source_turn_diff,
                        added: source_counts.map(|(added, _)| added),
                        removed: source_counts.map(|(_, removed)| removed),
                    },
                })
                .await;
                let destination =
                    resolve_tool_path(&ctx.cwd, mp).expect("an applied move path resolves");
                let (turn_diff, counts) = turn_change_metrics(
                    ctx,
                    &destination,
                    r.move_before_content.as_ref(),
                    r.after_content.as_ref(),
                    r.move_before_exists,
                );
                (ctx.emit)(ToolDomainEvent::FileChange {
                    change: FileChange {
                        path: posix_rendered_runtime(&destination).into_runtime_string(),
                        kind: FileChangeKind::Create,
                        diff: r.after_content.clone(),
                        turn_diff,
                        added: counts.map(|(added, _)| added),
                        removed: counts.map(|(_, removed)| removed),
                    },
                })
                .await;
                continue;
            }
            (ctx.emit)(ToolDomainEvent::FileChange {
                change: FileChange {
                    path: posix_rendered_runtime(&source).into_runtime_string(),
                    kind: kind_to_file_change(&r.kind),
                    diff: match (&r.before_content, &r.after_content) {
                        (Some(before), Some(after)) => Some(
                            crate::context::edited_files_diff::full_file_diff(before, after),
                        ),
                        (Some(before), None) => Some(before.clone()),
                        (None, Some(after)) => Some(after.clone()),
                        (None, None) => None,
                    },
                    turn_diff: source_turn_diff,
                    added: source_counts.map(|(added, _)| added),
                    removed: source_counts.map(|(_, removed)| removed),
                },
            })
            .await;
        }

        let lines: Vec<RuntimeString> = results.iter().map(result_summary_line).collect();
        let summary = join_runtime(&lines, "\n");

        let fuzzy_count = results.iter().filter(|r| !r.exact).count();
        let note = if fuzzy_count > 0 {
            RuntimeString::from("\nNote: ")
                .concat(&RuntimeString::from(fuzzy_count.to_string()))
                .concat(&RuntimeString::from(if fuzzy_count == 1 {
                    " file"
                } else {
                    " files"
                }))
                .concat(&RuntimeString::from(
                    " applied with fuzzy (non-exact) context matching.",
                ))
        } else {
            RuntimeString::default()
        };

        let files_json: Vec<Json> = results.iter().map(result_to_json).collect();
        let mut output_map = RuntimeObject::new();
        output_map.insert("files", RuntimeJson::Array(files_json));
        let output = RuntimeJson::Object(output_map);

        let text = RuntimeString::from("Applied patch to ")
            .concat(&RuntimeString::from(results.len().to_string()))
            .concat(&RuntimeString::from(if results.len() == 1 {
                " file:\n"
            } else {
                " files:\n"
            }))
            .concat(&summary)
            .concat(&note);
        Ok(text_result(output, text, false))
    }
}

/// Measure the latest content against the shared turn baseline, including
/// forwarded sub-agent edits. Missing input stays unknown; it is not an empty file.
fn turn_change_metrics(
    ctx: &ToolContext,
    path: &RuntimePath,
    before: Option<&RuntimeString>,
    after: Option<&RuntimeString>,
    observed_before: bool,
) -> (Option<RuntimeString>, Option<(u64, u64)>) {
    let baseline = ctx.file_state.turn_baseline(path, before);
    let existed_before = ctx.file_state.turn_existed(path, observed_before);
    let exists_after = after.is_some();
    let after = after.cloned().unwrap_or_default();
    let diff = baseline.as_ref().map(|before| {
        crate::context::edited_files_diff::full_turn_file_diff(
            before,
            &after,
            existed_before,
            exists_after,
        )
    });
    let counts = baseline
        .as_ref()
        .and_then(|before| crate::context::edited_files_diff::line_change_counts(before, &after));
    (diff, counts)
}

/// Parse + apply the patch, returning the applied results or an error message.
///
/// Conditional skills are activated on the patch's target paths up front
/// (consistent with read / write / edit / multi-edit, which fire on the input
/// path before the op). Each non-delete target's PRE-edit language-server
/// diagnostics are snapshotted BEFORE the patch is applied (a pull server reads
/// an unopened file off disk). A rename's baseline lands on the source path; the
/// post-edit reminder keys on the destination, so a renamed file just reports its
/// full set (a safe over-report, never an under-report).
async fn run_patch(
    patch: &RuntimeString,
    ctx: &ToolContext,
) -> Result<Vec<ApplyResult>, RuntimeString> {
    let ops = parse_patch(patch).map_err(|e| e.message)?;

    let touched: Vec<RuntimePath> = ops
        .iter()
        .flat_map(|op| match op {
            PatchOp::Update {
                path,
                move_path: Some(mp),
                ..
            } if !mp.is_empty() => {
                vec![
                    resolve_tool_path(&ctx.cwd, path),
                    resolve_tool_path(&ctx.cwd, mp),
                ]
            }
            _ => vec![resolve_tool_path(&ctx.cwd, op_path(op))],
        })
        .collect::<Result<Vec<_>, _>>()?;
    if !touched.is_empty() {
        if let Some(activate) = &ctx.activate_skill_paths {
            activate(touched.clone());
        }
    }

    if let Some(lsp) = &ctx.lsp {
        for op in &ops {
            if matches!(op, PatchOp::Delete { .. }) {
                continue;
            }
            let path = resolve_tool_path(&ctx.cwd, op_path(op))?;
            lsp.before_edited(&path).await;
        }
    }

    let cwd = ctx.cwd.clone();
    let additional_writable_roots = ctx.permission.additional_writable_roots.clone();
    let unconfined_writes = writes_are_unconfined(ctx.sandbox_mode);
    let lsp = ctx.lsp.clone();
    let owner = tokio::spawn(async move {
        run_retained_patch_owner(
            ops,
            touched,
            cwd,
            additional_writable_roots,
            unconfined_writes,
            lsp,
        )
        .await
    });
    match owner.await {
        Ok(result) => result,
        Err(error) => Err(RuntimeString::from(format!(
            "apply_patch retained mutation owner ended unexpectedly: {error}"
        ))),
    }
}

/// Once this owner starts, caller cancellation only detaches its JoinHandle;
/// the task retains the mutation guard, filesystem operation, and final LSP
/// reconciliation through one terminal result.
async fn run_retained_patch_owner(
    ops: Vec<PatchOp>,
    touched: Vec<RuntimePath>,
    cwd: RuntimePath,
    additional_writable_roots: Vec<RuntimePath>,
    unconfined_writes: bool,
    lsp: Option<Arc<dyn LspController>>,
) -> Result<Vec<ApplyResult>, RuntimeString> {
    let lsp_mutation = match &lsp {
        Some(lsp) => Some(lsp.begin_document_mutation(&touched).await),
        None => None,
    };
    let io = PatchFileIo::new(cwd.clone(), additional_writable_roots, unconfined_writes);
    let operation = async {
        match apply_patch_ops_tracked(&ops, &io).await {
            Ok(results) => {
                synchronize_successful_patch_lsp(lsp.as_ref(), &cwd, &results).await;
                Ok(results)
            }
            Err(failure) => {
                reconcile_failed_patch_lsp(lsp.as_ref(), &cwd, &failure).await;
                Err(failure.error.message)
            }
        }
    };
    let result = match std::panic::AssertUnwindSafe(operation).catch_unwind().await {
        Ok(result) => result,
        Err(_) => {
            // A panic can occur after an arbitrary OS mutation boundary. Keep
            // the outer guard and re-read every admitted target before ending.
            reconcile_lsp_paths(lsp.as_ref(), &touched).await;
            Err(RuntimeString::from(
                "apply_patch retained mutation owner panicked",
            ))
        }
    };
    drop(lsp_mutation);
    result
}

async fn synchronize_successful_patch_lsp(
    lsp: Option<&Arc<dyn LspController>>,
    cwd: &RuntimePath,
    results: &[ApplyResult],
) {
    let Some(lsp) = lsp else {
        return;
    };
    for result in results {
        if result.kind == ApplyKind::Delete {
            if let Ok(source) = resolve_tool_path(cwd, &result.path) {
                lsp.notify_deleted(&source).await;
            }
            continue;
        }
        let destination = if result.kind == ApplyKind::Update {
            match &result.move_path {
                Some(move_path) if move_path != &result.path => {
                    if let Ok(source) = resolve_tool_path(cwd, &result.path) {
                        lsp.notify_deleted(&source).await;
                    }
                    move_path
                }
                _ => &result.path,
            }
        } else {
            &result.path
        };
        if let Ok(destination) = resolve_tool_path(cwd, destination) {
            lsp.notify_edited(&destination, None).await;
        }
    }
}

/// Repair LSP state for the exact paths that crossed, or may have crossed, an
/// OS mutation boundary before a multi-operation patch failed. A completed
/// rename is reconciled source-before-destination, preserving the ordinary
/// didClose-before-didOpen lifecycle. Indeterminate I/O is resolved from the
/// real final disk state rather than inferred from an error string.
async fn reconcile_failed_patch_lsp(
    lsp: Option<&Arc<dyn LspController>>,
    cwd: &RuntimePath,
    failure: &ApplyOpsFailure,
) {
    let mut paths = Vec::<RuntimePath>::new();
    let mut push = |relative: &RuntimeString| {
        let Ok(path) = resolve_tool_path(cwd, relative) else {
            return;
        };
        if !paths.iter().any(|candidate| candidate == &path) {
            paths.push(path);
        }
    };
    for result in &failure.completed {
        push(&result.path);
        if let Some(move_path) = &result.move_path {
            if move_path != &result.path {
                push(move_path);
            }
        }
    }
    for path in &failure.reconcile_paths {
        push(path);
    }
    reconcile_lsp_paths(lsp, &paths).await;
}

async fn reconcile_lsp_paths(lsp: Option<&Arc<dyn LspController>>, paths: &[RuntimePath]) {
    let Some(lsp) = lsp else {
        return;
    };
    let mut unique_paths = Vec::<RuntimePath>::new();
    for path in paths {
        if unique_paths.iter().any(|candidate| candidate == path) {
            continue;
        }
        unique_paths.push(path.clone());
        let state = match project_runtime_path(path, "failed apply_patch LSP reconciliation") {
            Ok(native) => match tokio::fs::metadata(native).await {
                Ok(metadata) if metadata.is_file() => Some(true),
                Ok(_) => Some(false),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Some(false),
                Err(_) => None,
            },
            Err(_) => None,
        };
        match state {
            Some(false) => lsp.notify_deleted(path).await,
            // `notify_edited(..., None)` performs its own authoritative,
            // handle-validated disk read and closes the URI if an unknown I/O
            // state resolves to missing while the notification is processed.
            Some(true) | None => lsp.notify_edited(path, None).await,
        }
    }
}

/// The primary target path of a patch op (the file it reads/writes/deletes).
fn op_path(op: &PatchOp) -> &RuntimeString {
    match op {
        PatchOp::Add { path, .. } | PatchOp::Delete { path } | PatchOp::Update { path, .. } => path,
    }
}

fn resolve_tool_path(
    cwd: &RuntimePath,
    path: &RuntimeString,
) -> Result<RuntimePath, RuntimeString> {
    resolve_runtime(cwd, &[RuntimePath::from(path.clone())])
        .map_err(|error| error.message().clone())
}

/// The lowercase directive name reported in the summary (`add`/`update`/`delete`).
fn kind_str(kind: &ApplyKind) -> &'static str {
    match kind {
        ApplyKind::Add => "add",
        ApplyKind::Update => "update",
        ApplyKind::Delete => "delete",
    }
}

/// Map an applied op kind to the `file_change` event kind.
fn kind_to_file_change(kind: &ApplyKind) -> FileChangeKind {
    match kind {
        ApplyKind::Add => FileChangeKind::Create,
        ApplyKind::Delete => FileChangeKind::Delete,
        ApplyKind::Update => FileChangeKind::Modify,
    }
}

/// Serialize one [`ApplyResult`] into the tool's structured output shape.
fn result_to_json(r: &ApplyResult) -> Json {
    let mut m = RuntimeObject::new();
    m.insert("path", RuntimeJson::String(r.path.clone()));
    m.insert("kind", RuntimeJson::from(kind_str(&r.kind)));
    m.insert("exact", RuntimeJson::from(r.exact));
    m.insert("added", RuntimeJson::from(r.added as u64));
    m.insert("removed", RuntimeJson::from(r.removed as u64));
    if let Some(mp) = &r.move_path {
        m.insert("movePath", RuntimeJson::String(mp.clone()));
    }
    RuntimeJson::Object(m)
}

/// A plain text tool result carrying `output` as its structured payload.
fn text_result(output: Json, text: RuntimeString, is_error: bool) -> ToolResult {
    crate::tools::build_tool::text_result(output, text, is_error)
}

fn empty_files_output() -> RuntimeJson {
    let mut output = RuntimeObject::new();
    output.insert("files", RuntimeJson::Array(Vec::new()));
    RuntimeJson::Object(output)
}

fn result_summary_line(result: &ApplyResult) -> RuntimeString {
    let mut line = RuntimeString::from(kind_str(&result.kind))
        .concat(&RuntimeString::from(" "))
        .concat(&result.path);
    if result.kind == ApplyKind::Update {
        if let Some(move_path) = &result.move_path {
            if move_path != &result.path {
                line = line.concat(&RuntimeString::from(" -> ")).concat(move_path);
            }
        }
    }
    line = match result.kind {
        ApplyKind::Add => line
            .concat(&RuntimeString::from(" (+"))
            .concat(&RuntimeString::from(result.added.to_string()))
            .concat(&RuntimeString::from(")")),
        ApplyKind::Delete => line
            .concat(&RuntimeString::from(" (-"))
            .concat(&RuntimeString::from(result.removed.to_string()))
            .concat(&RuntimeString::from(")")),
        ApplyKind::Update => line
            .concat(&RuntimeString::from(" (+"))
            .concat(&RuntimeString::from(result.added.to_string()))
            .concat(&RuntimeString::from(" -"))
            .concat(&RuntimeString::from(result.removed.to_string()))
            .concat(&RuntimeString::from(")")),
    };
    if !result.exact {
        line = line.concat(&RuntimeString::from(" (fuzzy)"));
    }
    line
}

fn join_runtime(values: &[RuntimeString], separator: &str) -> RuntimeString {
    if values.is_empty() {
        return RuntimeString::default();
    }
    let separator = separator.encode_utf16().collect::<Vec<_>>();
    let mut output = Vec::new();
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            output.extend_from_slice(&separator);
        }
        output.extend_from_slice(value.as_utf16());
    }
    RuntimeString::from_utf16(output)
}

/// A real [`FileIO`] bound to the session cwd, applying path-safety on every
/// write / remove.
///
/// Each file's encoding + line endings are remembered between the read (which the
/// patch context matches against, LF-normalized) and the write (which restores
/// them).
struct PatchFileIo {
    cwd: RuntimePath,
    additional_writable_roots: Vec<RuntimePath>,
    /// Mirrors [`PathContext::unconfined_writes`] for the guards this IO applies.
    unconfined_writes: bool,
    /// Per-path encoding + line endings captured on read, replayed on write.
    meta: Mutex<HashMap<RuntimePath, (Encoding, LineEndings)>>,
    indeterminate_writes: Mutex<Vec<RuntimeString>>,
    indeterminate_removes: Mutex<Vec<RuntimeString>>,
}

impl PatchFileIo {
    fn new(
        cwd: RuntimePath,
        additional_writable_roots: Vec<RuntimePath>,
        unconfined_writes: bool,
    ) -> Self {
        PatchFileIo {
            cwd,
            additional_writable_roots,
            unconfined_writes,
            meta: Mutex::new(HashMap::new()),
            indeterminate_writes: Mutex::new(Vec::new()),
            indeterminate_removes: Mutex::new(Vec::new()),
        }
    }

    fn path_ctx(&self) -> PathContext<'_> {
        PathContext {
            cwd: &self.cwd,
            additional_writable_roots: &self.additional_writable_roots,
            unconfined_writes: self.unconfined_writes,
        }
    }
}

#[async_trait]
impl FileIO for PatchFileIo {
    async fn snapshot(&self, path: &RuntimeString) -> Result<Option<RuntimeString>, ApplyError> {
        let abs = resolve_tool_path(&self.cwd, path).map_err(ApplyError::new)?;
        let os_abs = project_runtime_path(&abs, "path")
            .map_err(|error| ApplyError::new(error.message().clone()))?;
        match read_text_file(&os_abs).await {
            Ok(tf) => Ok(Some(tf.content)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(ApplyError::new(e.to_string())),
        }
    }

    async fn read(&self, path: &RuntimeString) -> Result<Option<RuntimeString>, ApplyError> {
        let abs = resolve_tool_path(&self.cwd, path).map_err(ApplyError::new)?;
        let os_abs = project_runtime_path(&abs, "path")
            .map_err(|error| ApplyError::new(error.message().clone()))?;
        match read_text_file(&os_abs).await {
            Ok(tf) => {
                self.meta
                    .lock()
                    .unwrap()
                    .insert(abs, (tf.encoding, tf.line_endings));
                Ok(Some(tf.content))
            }
            // A missing file reads as `None`; any other error propagates.
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(ApplyError::new(e.to_string())),
        }
    }

    async fn write(
        &self,
        path: &RuntimeString,
        content: &RuntimeString,
        from: Option<&RuntimeString>,
    ) -> Result<(), ApplyError> {
        let abs = resolve_tool_path(&self.cwd, path).map_err(ApplyError::new)?;
        assert_writable(&abs, &self.path_ctx())
            .await
            .map_err(ApplyError::new)?;
        let parent = dirname_runtime(&abs);
        let os_parent = project_runtime_path(&parent, "path")
            .map_err(|error| ApplyError::new(error.message().clone()))?;
        tokio::fs::create_dir_all(os_parent)
            .await
            .map_err(|e| ApplyError::new(e.to_string()))?;
        // A rename's content IS the source file, so the SOURCE's encoding + line
        // endings are authoritative — and must WIN over any stale meta still keyed
        // on the destination path (e.g. a preceding delete/update of that same
        // path left an entry behind; `remove` doesn't evict it). An in-place
        // update (`from` is `None`) uses its own read meta; a freshly-added file
        // (no read, no `from`) falls through to utf8 / LF.
        let m = {
            let meta = self.meta.lock().unwrap();
            from.and_then(|f| resolve_tool_path(&self.cwd, f).ok())
                .and_then(|source| meta.get(&source).copied())
                .or_else(|| meta.get(&abs).copied())
        };
        let (encoding, line_endings) = m.unwrap_or((Encoding::Utf8, LineEndings::Lf));
        let os_abs = project_runtime_path(&abs, "path")
            .map_err(|error| ApplyError::new(error.message().clone()))?;
        write_text_file(&os_abs, content, encoding, line_endings)
            .await
            .map_err(|e| {
                self.indeterminate_writes.lock().unwrap().push(path.clone());
                ApplyError::new(e.to_string())
            })?;
        Ok(())
    }

    async fn remove(&self, path: &RuntimeString) -> Result<(), ApplyError> {
        let abs = resolve_tool_path(&self.cwd, path).map_err(ApplyError::new)?;
        assert_writable(&abs, &self.path_ctx())
            .await
            .map_err(ApplyError::new)?;
        let os_abs = project_runtime_path(&abs, "path")
            .map_err(|error| ApplyError::new(error.message().clone()))?;
        tokio::fs::remove_file(os_abs).await.map_err(|e| {
            self.indeterminate_removes
                .lock()
                .unwrap()
                .push(path.clone());
            ApplyError::new(e.to_string())
        })?;
        Ok(())
    }

    fn write_error_may_have_mutated(&self, path: &RuntimeString) -> bool {
        take_runtime_path_marker(&self.indeterminate_writes, path)
    }

    fn remove_error_may_have_mutated(&self, path: &RuntimeString) -> bool {
        take_runtime_path_marker(&self.indeterminate_removes, path)
    }
}

fn take_runtime_path_marker(markers: &Mutex<Vec<RuntimeString>>, path: &RuntimeString) -> bool {
    let mut markers = markers.lock().unwrap();
    let Some(index) = markers.iter().position(|candidate| candidate == path) else {
        return false;
    };
    markers.remove(index);
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::node_path::runtime_path_from_native;
    use crate::runtime_values::runtime_json;
    use crate::schemas::content::OutputBlock;
    use crate::types::abort::AbortSignal;
    use crate::types::file_state::{FileStateCache, FileStateEntry, RecordOptions};
    use crate::types::permission::PermissionContext;
    use crate::types::sandbox::{
        ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec,
    };
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    /// A unique temp dir (atomic counter + pid + nanos) so parallel tests never
    /// collide.
    fn temp_dir() -> PathBuf {
        let mut dir = std::env::temp_dir();
        let unique = format!(
            "ae-applypatch-tool-{}-{}-{}",
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

    struct NoopFileState;

    impl FileStateCache for NoopFileState {
        fn get(&self, _path: &RuntimePath) -> Option<FileStateEntry> {
            None
        }
        fn record(&self, _path: &RuntimePath, _mtime_ms: f64, _opts: RecordOptions) {}
        fn has(&self, _path: &RuntimePath) -> bool {
            false
        }
        fn entries(&self) -> Vec<(RuntimePath, FileStateEntry)> {
            Vec::new()
        }
        fn delete(&self, _path: &RuntimePath) {}
        fn clear(&self) {}
        fn clone_box(&self) -> Box<dyn FileStateCache> {
            Box::new(NoopFileState)
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

    #[derive(Default)]
    struct RecordingLsp {
        lifecycle: Arc<Mutex<Vec<(RuntimeString, RuntimePath)>>>,
        mutation_gate: Arc<tokio::sync::RwLock<()>>,
        notify_barrier: Mutex<
            Option<(
                tokio::sync::oneshot::Sender<()>,
                tokio::sync::oneshot::Receiver<()>,
            )>,
        >,
    }

    impl RecordingLsp {
        async fn pause_notification(&self) {
            let barrier = self.notify_barrier.lock().unwrap().take();
            if let Some((reached, resume)) = barrier {
                let _ = reached.send(());
                let _ = resume.await;
            }
        }
    }

    #[async_trait]
    impl crate::lsp::types::LspController for RecordingLsp {
        fn available(&self) -> bool {
            true
        }
        async fn diagnostics(&self, _path: &RuntimePath) -> Vec<crate::lsp::types::LspDiagnostic> {
            Vec::new()
        }
        async fn hover(
            &self,
            _path: &RuntimePath,
            _line: u32,
            _character: u32,
        ) -> Option<crate::lsp::types::LspHoverResult> {
            None
        }
        async fn definition(
            &self,
            _path: &RuntimePath,
            _line: u32,
            _character: u32,
        ) -> Vec<crate::lsp::types::LspLocation> {
            Vec::new()
        }
        async fn references(
            &self,
            _path: &RuntimePath,
            _line: u32,
            _character: u32,
        ) -> Vec<crate::lsp::types::LspLocation> {
            Vec::new()
        }
        async fn implementation(
            &self,
            _path: &RuntimePath,
            _line: u32,
            _character: u32,
        ) -> Vec<crate::lsp::types::LspLocation> {
            Vec::new()
        }
        async fn document_symbols(&self, _path: &RuntimePath) -> Vec<crate::lsp::types::LspSymbol> {
            Vec::new()
        }
        async fn workspace_symbol(
            &self,
            _query: &RuntimeString,
        ) -> Vec<crate::lsp::types::LspWorkspaceSymbol> {
            Vec::new()
        }
        async fn prepare_call_hierarchy(
            &self,
            _path: &RuntimePath,
            _line: u32,
            _character: u32,
        ) -> Vec<crate::lsp::types::LspCallHierarchyItem> {
            Vec::new()
        }
        async fn incoming_calls(
            &self,
            _path: &RuntimePath,
            _line: u32,
            _character: u32,
        ) -> Vec<crate::lsp::types::LspCall> {
            Vec::new()
        }
        async fn outgoing_calls(
            &self,
            _path: &RuntimePath,
            _line: u32,
            _character: u32,
        ) -> Vec<crate::lsp::types::LspCall> {
            Vec::new()
        }
        async fn begin_document_mutation(
            &self,
            paths: &[RuntimePath],
        ) -> crate::lsp::types::LspDocumentMutationGuard {
            let guard = self.mutation_gate.clone().write_owned().await;
            self.lifecycle.lock().unwrap().extend(
                paths
                    .iter()
                    .cloned()
                    .map(|path| (RuntimeString::from("begin"), path)),
            );
            crate::lsp::types::LspDocumentMutationGuard::new(vec![guard])
        }
        async fn before_edited(&self, _path: &RuntimePath) {}
        async fn notify_edited(&self, path: &RuntimePath, _text: Option<&RuntimeString>) {
            assert!(
                self.mutation_gate.try_read().is_err(),
                "post-write LSP synchronization must remain inside the mutation boundary"
            );
            let native = project_runtime_path(path, "recording LSP edited path").unwrap();
            assert!(
                native.is_file(),
                "the filesystem write must precede post-write LSP synchronization"
            );
            self.pause_notification().await;
            self.lifecycle
                .lock()
                .unwrap()
                .push((RuntimeString::from("edit"), path.clone()));
        }
        async fn notify_deleted(&self, path: &RuntimePath) {
            assert!(
                self.mutation_gate.try_read().is_err(),
                "post-delete LSP close must remain inside the mutation boundary"
            );
            let native = project_runtime_path(path, "recording LSP deleted path").unwrap();
            assert!(
                !native.exists(),
                "the filesystem delete must precede post-delete LSP synchronization"
            );
            self.pause_notification().await;
            self.lifecycle
                .lock()
                .unwrap()
                .push((RuntimeString::from("delete"), path.clone()));
        }
    }

    /// A `ToolContext` rooted at `cwd`, capturing emitted events into `events`.
    fn make_ctx(cwd: &std::path::Path, events: Arc<Mutex<Vec<ToolDomainEvent>>>) -> ToolContext {
        let sink = events;
        ToolContext {
            async_question: None,
            background_processes: Arc::new(
                crate::command_runtime::background::BackgroundCommandRegistry::new(),
            ),
            process_context: crate::runtime_process::test_runtime_process_context(),
            cwd: runtime_path_from_native(cwd),
            signal: AbortSignal::new(),
            file_state: Arc::new(NoopFileState),
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
            sandbox_retry_on_denial: None,
            request_approval: None,
            request_network_access: None,
            emit: std::sync::Arc::new(move |e: ToolDomainEvent| {
                sink.lock().unwrap().push(e);
                Box::pin(async {})
            }),
            enqueue_background_notice: None,
            record_rollout_event: None,
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
        }
    }

    fn make_ctx_with_lsp(
        cwd: &std::path::Path,
        events: Arc<Mutex<Vec<ToolDomainEvent>>>,
        lsp: Arc<dyn crate::lsp::types::LspController>,
    ) -> ToolContext {
        let mut context = make_ctx(cwd, events);
        context.lsp = Some(lsp);
        context
    }

    fn text_of(res: &ToolResult) -> String {
        match &res.serializable.model_content[0] {
            OutputBlock::Text(t) => String::from_utf8(t.text.to_node_utf8())
                .expect("Node UTF-8 test text is valid UTF-8"),
            _ => panic!("expected a text block"),
        }
    }

    fn patch_input(patch: impl Into<RuntimeString>) -> Json {
        let mut object = RuntimeObject::new();
        object.insert("patch", RuntimeJson::String(patch.into()));
        RuntimeJson::Object(object)
    }

    fn parse_runtime_json(text: &str) -> RuntimeJson {
        runtime_json::parse(&RuntimeString::from(text)).expect("valid exact test JSON")
    }

    #[test]
    fn exposes_expected_metadata() {
        let tool = create_apply_patch_tool();
        assert_eq!(tool.name(), "apply_patch");
        assert_eq!(
            tool.description(),
            "Apply a multi-file patch (add/update/delete) in the codex *** Begin Patch format."
        );
        assert_eq!(tool.permission_path_kind(), Some(PermissionPathKind::Edit));
        let input = patch_input("");
        assert!(!tool.is_read_only(&input));
        assert!(!tool.is_concurrency_safe(&input));
        assert!(tool.is_enabled());
        assert_eq!(tool.is_destructive(&input), Some(false));
        assert_eq!(
            tool.summarize(&input).as_ref(),
            Some(&RuntimeString::from("Applying patch"))
        );
    }

    #[test]
    fn input_schema_describes_the_patch_field() {
        let tool = create_apply_patch_tool();
        let schema = tool.static_input_schema();
        assert_eq!(schema.get("type").and_then(Value::as_str), Some("object"));
        let patch = schema
            .get("properties")
            .and_then(|p| p.get("patch"))
            .expect("patch property");
        assert_eq!(patch.get("type").and_then(Value::as_str), Some("string"));
        assert_eq!(
            patch.get("description").and_then(Value::as_str),
            Some("A patch in the *** Begin Patch / *** End Patch format")
        );
        assert_eq!(schema.get("required").unwrap(), &json!(["patch"]));
    }

    #[test]
    fn get_path_collects_targets_including_move_destination() {
        let tool = create_apply_patch_tool();
        let patch = [
            "*** Begin Patch",
            "*** Add File: a.txt",
            "+x",
            "*** Update File: b.txt",
            "*** Move to: c.txt",
            "@@",
            "-y",
            "+Y",
            "*** Delete File: d.txt",
            "*** End Patch",
        ]
        .join("\n");
        let got = tool.get_path(&patch_input(patch));
        assert_eq!(
            got,
            Some(PathTargets::Many(vec![
                RuntimePath::from("a.txt"),
                RuntimePath::from("b.txt"),
                RuntimePath::from("c.txt"),
                RuntimePath::from("d.txt"),
            ]))
        );
    }

    #[test]
    fn get_path_returns_none_for_a_malformed_patch() {
        let tool = create_apply_patch_tool();
        assert_eq!(tool.get_path(&patch_input("not a patch")), None);
    }

    #[tokio::test]
    async fn call_applies_an_add_and_emits_a_file_change() {
        let dir = temp_dir();
        let events = Arc::new(Mutex::new(Vec::new()));
        let ctx = make_ctx(&dir, events.clone());
        let tool = create_apply_patch_tool();
        let patch = [
            "*** Begin Patch",
            "*** Add File: greeting.txt",
            "+hello",
            "+world",
            "*** End Patch",
        ]
        .join("\n");

        let res = tool.call(patch_input(patch), &ctx, None).await.unwrap();

        assert_eq!(
            std::fs::read_to_string(dir.join("greeting.txt")).unwrap(),
            "hello\nworld\n"
        );
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(
            res.serializable.output,
            parse_runtime_json(
                r#"{"files":[{"path":"greeting.txt","kind":"add","exact":true,"added":2,"removed":0}]}"#,
            )
        );
        assert_eq!(
            text_of(&res),
            "Applied patch to 1 file:\nadd greeting.txt (+2)"
        );

        let evs = events.lock().unwrap();
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            ToolDomainEvent::FileChange { change } => {
                assert_eq!(change.kind, FileChangeKind::Create);
                assert!(change.path.ends_with("greeting.txt"));
            }
            _ => panic!("expected a file_change event"),
        }
        drop(evs);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn call_reports_a_parse_failure_as_a_model_error() {
        let dir = temp_dir();
        let events = Arc::new(Mutex::new(Vec::new()));
        let ctx = make_ctx(&dir, events.clone());
        let tool = create_apply_patch_tool();

        let res = tool.call(patch_input("garbage"), &ctx, None).await.unwrap();

        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            res.serializable.output,
            parse_runtime_json(r#"{"files":[]}"#)
        );
        assert!(text_of(&res).starts_with("apply_patch failed:"));
        assert!(events.lock().unwrap().is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn call_notes_a_fuzzy_match() {
        let dir = temp_dir();
        // Trailing whitespace on the context lines forces a non-exact (rstrip) match.
        std::fs::write(
            dir.join("code.txt"),
            "const a = 1   \nconst b = 2\nconst c = 3  \n",
        )
        .unwrap();
        let events = Arc::new(Mutex::new(Vec::new()));
        let ctx = make_ctx(&dir, events.clone());
        let tool = create_apply_patch_tool();
        let patch = [
            "*** Begin Patch",
            "*** Update File: code.txt",
            " const a = 1",
            "-const b = 2",
            "+const b = 22",
            " const c = 3",
            "*** End Patch",
        ]
        .join("\n");

        let res = tool.call(patch_input(patch), &ctx, None).await.unwrap();

        let text = text_of(&res);
        assert!(text.contains("(fuzzy)"), "text: {text}");
        assert!(
            text.contains("\nNote: 1 file applied with fuzzy (non-exact) context matching."),
            "text: {text}"
        );
        assert_eq!(res.serializable.is_error, Some(false));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn call_refuses_to_write_a_protected_path() {
        let dir = temp_dir();
        let events = Arc::new(Mutex::new(Vec::new()));
        let lsp = Arc::new(RecordingLsp::default());
        let lifecycle = lsp.lifecycle.clone();
        let ctx = make_ctx_with_lsp(&dir, events.clone(), lsp);
        let tool = create_apply_patch_tool();
        let patch = [
            "*** Begin Patch",
            "*** Add File: .env",
            "+SECRET=1",
            "*** End Patch",
        ]
        .join("\n");

        let res = tool.call(patch_input(patch), &ctx, None).await.unwrap();

        assert_eq!(res.serializable.is_error, Some(true));
        let text = text_of(&res);
        assert!(text.starts_with("apply_patch failed:"), "text: {text}");
        assert!(text.contains("protected path"), "text: {text}");
        assert!(!dir.join(".env").exists());
        let lifecycle = lifecycle.lock().unwrap();
        assert_eq!(lifecycle.len(), 1);
        assert_eq!(lifecycle[0].0, "begin");
        drop(lifecycle);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn call_renames_a_file_and_emits_delete_plus_create() {
        let dir = temp_dir();
        std::fs::write(dir.join("old.txt"), "hi\nthere\n").unwrap();
        let events = Arc::new(Mutex::new(Vec::new()));
        let lsp = Arc::new(RecordingLsp::default());
        let lifecycle = lsp.lifecycle.clone();
        let ctx = make_ctx_with_lsp(&dir, events.clone(), lsp);
        let tool = create_apply_patch_tool();
        let patch = [
            "*** Begin Patch",
            "*** Update File: old.txt",
            "*** Move to: new.txt",
            "@@",
            "-hi",
            "+HI",
            " there",
            "*** End Patch",
        ]
        .join("\n");

        let res = tool.call(patch_input(patch), &ctx, None).await.unwrap();

        assert!(!dir.join("old.txt").exists());
        assert_eq!(
            std::fs::read_to_string(dir.join("new.txt")).unwrap(),
            "HI\nthere\n"
        );
        assert_eq!(
            text_of(&res),
            "Applied patch to 1 file:\nupdate old.txt -> new.txt (+2 -2)"
        );
        assert_eq!(
            res.serializable.output,
            parse_runtime_json(
                r#"{"files":[{"path":"old.txt","kind":"update","exact":true,"added":2,"removed":2,"movePath":"new.txt"}]}"#,
            )
        );

        let evs = events.lock().unwrap();
        assert_eq!(evs.len(), 2);
        match &evs[0] {
            ToolDomainEvent::FileChange { change } => {
                assert_eq!(change.kind, FileChangeKind::Delete);
                assert!(change.path.ends_with("old.txt"));
            }
            _ => panic!("expected a delete file_change"),
        }
        match &evs[1] {
            ToolDomainEvent::FileChange { change } => {
                assert_eq!(change.kind, FileChangeKind::Create);
                assert!(change.path.ends_with("new.txt"));
            }
            _ => panic!("expected a create file_change"),
        }
        drop(evs);

        let lifecycle = lifecycle.lock().unwrap();
        assert_eq!(lifecycle.len(), 4);
        assert_eq!(lifecycle[0].0, "begin");
        assert!(lifecycle[0].1.as_runtime_string().ends_with("old.txt"));
        assert_eq!(lifecycle[1].0, "begin");
        assert!(lifecycle[1].1.as_runtime_string().ends_with("new.txt"));
        assert_eq!(lifecycle[2].0, "delete");
        assert!(lifecycle[2].1.as_runtime_string().ends_with("old.txt"));
        assert_eq!(lifecycle[3].0, "edit");
        assert!(lifecycle[3].1.as_runtime_string().ends_with("new.txt"));
        drop(lifecycle);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn call_deletes_a_file_and_closes_its_lsp_document() {
        let dir = temp_dir();
        std::fs::write(dir.join("gone.txt"), "gone\n").unwrap();
        let events = Arc::new(Mutex::new(Vec::new()));
        let lsp = Arc::new(RecordingLsp::default());
        let lifecycle = lsp.lifecycle.clone();
        let ctx = make_ctx_with_lsp(&dir, events, lsp);
        let tool = create_apply_patch_tool();
        let patch = [
            "*** Begin Patch",
            "*** Delete File: gone.txt",
            "*** End Patch",
        ]
        .join("\n");

        let result = tool.call(patch_input(patch), &ctx, None).await.unwrap();

        assert_eq!(result.serializable.is_error, Some(false));
        assert!(!dir.join("gone.txt").exists());
        let lifecycle = lifecycle.lock().unwrap();
        assert_eq!(lifecycle.len(), 2);
        assert_eq!(lifecycle[0].0, "begin");
        assert!(lifecycle[0].1.as_runtime_string().ends_with("gone.txt"));
        assert_eq!(lifecycle[1].0, "delete");
        assert!(lifecycle[1].1.as_runtime_string().ends_with("gone.txt"));
        drop(lifecycle);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn later_failure_reconciles_every_completed_lsp_mutation_before_releasing_queries() {
        let dir = temp_dir();
        std::fs::write(dir.join("gone.txt"), "gone\n").unwrap();
        std::fs::write(dir.join("old.txt"), "old\n").unwrap();
        std::fs::write(dir.join("edit.txt"), "before\n").unwrap();
        let events = Arc::new(Mutex::new(Vec::new()));
        let lsp = Arc::new(RecordingLsp::default());
        let lifecycle = lsp.lifecycle.clone();
        let mutation_gate = lsp.mutation_gate.clone();
        let ctx = make_ctx_with_lsp(&dir, events, lsp);
        let tool = create_apply_patch_tool();
        let patch = [
            "*** Begin Patch",
            "*** Delete File: gone.txt",
            "*** Update File: old.txt",
            "*** Move to: new.txt",
            "@@",
            "-old",
            "+renamed",
            "*** Update File: edit.txt",
            "@@",
            "-before",
            "+after",
            "*** Delete File: missing.txt",
            "*** End Patch",
        ]
        .join("\n");

        let result = tool.call(patch_input(patch), &ctx, None).await.unwrap();

        assert_eq!(result.serializable.is_error, Some(true));
        assert!(text_of(&result).contains("cannot delete missing file"));
        assert!(!dir.join("gone.txt").exists());
        assert!(!dir.join("old.txt").exists());
        assert_eq!(
            std::fs::read_to_string(dir.join("new.txt")).unwrap(),
            "renamed\n"
        );
        assert_eq!(
            std::fs::read_to_string(dir.join("edit.txt")).unwrap(),
            "after\n"
        );
        assert!(
            mutation_gate.try_read().is_ok(),
            "the LSP mutation lease is released only after partial-state repair"
        );

        let lifecycle = lifecycle.lock().unwrap();
        let repaired = lifecycle
            .iter()
            .filter(|(kind, _)| kind != "begin")
            .map(|(kind, path)| {
                let path = path
                    .as_runtime_string()
                    .try_to_utf8()
                    .expect("temporary test paths are scalar UTF-8");
                (
                    kind.clone(),
                    RuntimeString::from(
                        crate::permission::bash::node_path::basename(&path).as_str(),
                    ),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            repaired,
            vec![
                (
                    RuntimeString::from("delete"),
                    RuntimeString::from("gone.txt")
                ),
                (
                    RuntimeString::from("delete"),
                    RuntimeString::from("old.txt")
                ),
                (RuntimeString::from("edit"), RuntimeString::from("new.txt")),
                (RuntimeString::from("edit"), RuntimeString::from("edit.txt")),
            ]
        );
        drop(lifecycle);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn caller_cancellation_cannot_abandon_post_write_lsp_synchronization() {
        let dir = temp_dir();
        std::fs::write(dir.join("edit.txt"), "before\n").unwrap();
        let events = Arc::new(Mutex::new(Vec::new()));
        let lsp = Arc::new(RecordingLsp::default());
        let lifecycle = lsp.lifecycle.clone();
        let mutation_gate = lsp.mutation_gate.clone();
        let (reached_send, reached) = tokio::sync::oneshot::channel();
        let (resume, resume_receive) = tokio::sync::oneshot::channel();
        *lsp.notify_barrier.lock().unwrap() = Some((reached_send, resume_receive));
        let ctx = make_ctx_with_lsp(&dir, events, lsp);
        let tool = create_apply_patch_tool();
        let patch = [
            "*** Begin Patch",
            "*** Update File: edit.txt",
            "@@",
            "-before",
            "+after",
            "*** End Patch",
        ]
        .join("\n");
        let caller = tokio::spawn(async move { tool.call(patch_input(patch), &ctx, None).await });

        reached
            .await
            .expect("retained owner reached LSP notification");
        assert_eq!(
            std::fs::read_to_string(dir.join("edit.txt")).unwrap(),
            "after\n"
        );
        caller.abort();
        let _ = caller.await;
        assert!(
            mutation_gate.try_read().is_err(),
            "caller cancellation must not release the retained mutation guard"
        );
        let _ = resume.send(());
        for _ in 0..64 {
            if mutation_gate.try_read().is_ok() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            mutation_gate.try_read().is_ok(),
            "retained owner must release the guard after LSP synchronization"
        );
        let lifecycle = lifecycle.lock().unwrap();
        assert!(lifecycle.iter().any(|(kind, path)| {
            kind == "edit" && path.as_runtime_string().ends_with("edit.txt")
        }));
        drop(lifecycle);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn failed_delete_releases_lsp_mutation_boundary_without_false_close() {
        let dir = temp_dir();
        let events = Arc::new(Mutex::new(Vec::new()));
        let lsp = Arc::new(RecordingLsp::default());
        let lifecycle = lsp.lifecycle.clone();
        let mutation_gate = lsp.mutation_gate.clone();
        let ctx = make_ctx_with_lsp(&dir, events, lsp);
        let tool = create_apply_patch_tool();
        let patch = [
            "*** Begin Patch",
            "*** Delete File: missing.txt",
            "*** End Patch",
        ]
        .join("\n");

        let result = tool.call(patch_input(patch), &ctx, None).await.unwrap();

        assert_eq!(result.serializable.is_error, Some(true));
        assert!(text_of(&result).contains("cannot delete missing file"));
        assert!(
            mutation_gate.try_read().is_ok(),
            "an apply failure must release the exclusive LSP mutation lease"
        );
        let lifecycle = lifecycle.lock().unwrap();
        assert_eq!(lifecycle.len(), 1);
        assert_eq!(lifecycle[0].0, "begin");
        assert!(lifecycle[0].1.as_runtime_string().ends_with("missing.txt"));
        drop(lifecycle);
        std::fs::remove_dir_all(&dir).ok();
    }
}
