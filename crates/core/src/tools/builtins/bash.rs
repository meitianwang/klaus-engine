//! The `bash` [`Tool`]: run a shell command under the sandbox with bounded,
//! streamed output — or, on request, launch it in the background and return a
//! shellId the model polls with `bash_output` and stops with `kill_shell`.
//!
//! A genuine (non-timeout/abort) failure is checked against the sandbox-denial
//! heuristic so the model is told "the sandbox blocked this" instead of chasing
//! a phantom bug. When the approval policy permits a denial retry and an
//! approval path exists, a real denial can be re-run WITHOUT the sandbox after
//! the user approves.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::fs::node_path::resolve_runtime;
use crate::fs::path_safety::{writable_roots, writes_are_unconfined, PathContext};
use crate::permission::PLAN_MODE_READONLY_ENFORCED_KEY;
use crate::runtime_process::{path_join, NodePathDialect, RuntimeEnv, RuntimeProcessContext};
use crate::runtime_values::{RuntimeJson, RuntimeObject, RuntimePath, RuntimeString};
use crate::sandbox::denial::is_likely_sandbox_denied;
use crate::sandbox::plan_readonly::PlanReadOnlySandbox;
use crate::sandbox::process::ProcessSandbox;
use crate::schemas::content::{OutputBlock, TextBlock};
use crate::tools::background_shells::{BackgroundShellStatus, CompleteResult, StartShellOptions};
use crate::tools::build_tool::text_result;
use crate::tools::builtins::shell_common::{
    code_str, first_chars, request_shell_escalation, DEFAULT_MAX_OUTPUT_BYTES,
};
use crate::tools::exec::{run_shell, OutputStream, RunShellOptions, RunShellResult};
use crate::tools::output_truncation::{
    truncate_tail_text, TruncationPolicy, DEFAULT_OUTPUT_TRUNCATION_POLICY,
};
use crate::tools::result_storage::{maybe_spill_result, SpillOptions};
use crate::types::abort::AbortSignal;
use crate::types::errors::ToolError;
use crate::types::events::ToolDomainEvent;
use crate::types::json::{Json, JsonSchema};
use crate::types::sandbox::{SandboxBackend, SandboxKind};
use crate::types::tool::{
    EmitFn, Tool, ToolContext, ToolProgress, ToolResult, ToolResultSerializable,
};

/// Construction options for the `bash` tool.
#[derive(Debug, Clone, Default)]
pub struct BashToolOptions {
    pub default_timeout_ms: Option<u64>,
    pub max_output_bytes: Option<usize>,
}

pub(crate) const DEFAULT_TIMEOUT_MS: u64 = 120_000;
const INPUT_SCHEMA_PROPERTY_ORDER: &[&str] = &[
    "command",
    "timeout",
    "cwd",
    "yieldMs",
    "runInBackground",
    "sandboxPermissions",
    "justification",
];

/// The `type` discriminator string a [`SandboxKind`] presents to the model — the
/// same lowercase/kebab tokens the config uses (`process`, `seatbelt`,
/// `bubblewrap`, `native-helper`).
fn sandbox_kind_str(kind: SandboxKind) -> &'static str {
    match kind {
        SandboxKind::Process => "process",
        SandboxKind::Seatbelt => "seatbelt",
        SandboxKind::Bubblewrap => "bubblewrap",
        SandboxKind::NativeHelper => "native-helper",
        SandboxKind::WindowsRestrictedToken => "windows-restricted-token",
        SandboxKind::WindowsMxc => "windows-mxc",
    }
}

/// True when the permission engine stamped this input as a plan-mode run whose
/// read-only-ness could not be proven statically. The marker is engine-added
/// only (the tool schema does not declare it), so a model-supplied copy never
/// reaches here.
fn plan_readonly_enforced(input: &Json) -> bool {
    input
        .get(PLAN_MODE_READONLY_ENFORCED_KEY)
        .and_then(RuntimeJson::as_bool)
        .unwrap_or(false)
}

/// Parse the leading base-10 integer of `s`, mirroring JS `Number.parseInt(s, 10)`:
/// skip leading whitespace, read an optional sign, consume digits, and stop at the
/// first non-digit. Returns `None` (JS `NaN`) when no digits are present.
fn parse_int_base10(s: &str) -> Option<i64> {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    let mut sign: i64 = 1;
    if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
        if bytes[i] == b'-' {
            sign = -1;
        }
        i += 1;
    }
    let start = i;
    let mut val: i64 = 0;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        val = val
            .saturating_mul(10)
            .saturating_add((bytes[i] - b'0') as i64);
        i += 1;
    }
    if i == start {
        return None;
    }
    Some(sign * val)
}

/// Clamp a model-supplied timeout to an upper bound (`BASH_MAX_TIMEOUT_MS`). The
/// cap is the env value when it parses to a positive number (but never below the
/// tool's default), otherwise 10 minutes (again floored at the default). No lower
/// bound — a model may ask for a very short timeout.
pub fn resolve_bash_timeout(
    input_timeout_ms: Option<u64>,
    default_timeout_ms: u64,
    env_max: Option<&str>,
) -> u64 {
    let parsed = parse_int_base10(env_max.unwrap_or(""));
    let max_timeout_ms = match parsed {
        Some(p) if p > 0 => (p as u64).max(default_timeout_ms),
        _ => 600_000u64.max(default_timeout_ms),
    };
    input_timeout_ms
        .unwrap_or(default_timeout_ms)
        .min(max_timeout_ms)
}

/// Render the model-visible outcome of one foreground command as structured
/// sections: exit code, wall time, the original line count when truncation
/// changed it, and the output body under the shared truncation policy. An
/// oversized body keeps its newest tail: a command's final state and error
/// diagnostics land at the end of its output. Failure context (timeout,
/// abort) is re-prepended after truncation so it survives an output of any
/// size. Returns the text, whether the outcome is an error, and whether the
/// body was truncated.
///
/// `wall_time` must be the millisecond-quantized duration that is also recorded
/// in the result payload, so any consumer can re-render the exact text from the
/// recorded fields alone.
pub(crate) fn render_bash_outcome_text(
    output: &RuntimeString,
    exit_code: Option<i32>,
    timed_out: bool,
    aborted: bool,
    wall_time: std::time::Duration,
    policy: TruncationPolicy,
) -> (RuntimeString, bool, bool) {
    let is_error = exit_code != Some(0) || timed_out || aborted;
    let base = if output.is_empty() {
        RuntimeString::from("(no output)")
    } else {
        output.clone()
    };
    let failure_prefix = if timed_out {
        format!(
            "command timed out after {} milliseconds\n",
            wall_time.as_millis()
        )
    } else if aborted {
        "[command aborted]\n".to_string()
    } else {
        String::new()
    };
    let content = if failure_prefix.is_empty() {
        base
    } else {
        RuntimeString::from_utf8(&failure_prefix).concat(&base)
    };
    // Tail truncation works on UTF-8 bytes; non-scalar runtime text (lone
    // surrogates from lossy sources) has no UTF-8 length and passes through
    // unchanged instead of being corrupted by a lossy rewrite.
    let mut total_lines_section = None;
    let mut truncated = false;
    let body = match content.try_to_utf8() {
        Ok(utf8) if utf8.len() > policy.byte_budget() => {
            let total_lines = utf8.lines().count();
            // Truncate the command output alone and re-prepend the failure
            // context: tail truncation elides the head, so a prefix inside
            // the truncated body would be the first thing lost.
            let formatted = format!(
                "{failure_prefix}{}",
                truncate_tail_text(&utf8[failure_prefix.len()..], policy)
            );
            if total_lines != formatted.lines().count() {
                total_lines_section = Some(format!("Total output lines: {total_lines}"));
            }
            truncated = true;
            RuntimeString::from_utf8(&formatted)
        }
        _ => content,
    };
    // Round to one decimal place for a stable, compact metadata line.
    let duration_seconds = ((wall_time.as_secs_f32()) * 10.0).round() / 10.0;
    let mut text = RuntimeString::from("Exit code: ")
        .concat(&code_str(exit_code))
        .concat(&RuntimeString::from_utf8(&format!(
            "\nWall time: {duration_seconds} seconds"
        )));
    if let Some(section) = total_lines_section {
        text = text.concat(&RuntimeString::from_utf8(&format!("\n{section}")));
    }
    (
        text.concat(&RuntimeString::from("\nOutput:\n"))
            .concat(&body),
        is_error,
        truncated,
    )
}

/// Persist the FULL output of a truncated command beside the session's
/// other tool-result artifacts and return the reference note to append
/// (leading newlines included). The artifact name is the content hash, so
/// identical output re-spills onto the same file deterministically and an
/// independent reconstructor can recompute the exact path. A failed persist
/// degrades to the truncated text alone — never to a tool error.
async fn spill_truncated_bash_output(
    output: &RuntimeString,
    cwd: &RuntimePath,
) -> Option<RuntimeString> {
    let digest = Sha256::digest(output.to_node_utf8());
    let mut id = String::from("bash-");
    for byte in &digest[..8] {
        id.push_str(&format!("{byte:02x}"));
    }
    let dir = path_join(
        bash_spill_path_dialect(),
        &[
            cwd.clone(),
            RuntimePath::from(".klaus"),
            RuntimePath::from("tool-results"),
        ],
    );
    let spilled = maybe_spill_result(
        vec![OutputBlock::Text(TextBlock {
            text: output.clone(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })],
        SpillOptions {
            // The caller already established that this output overflows the
            // model budget; the plan exists to compute the exact artifact
            // payload and path.
            max_chars: 0.0,
            dir,
            id: RuntimeString::from_utf8(&id),
        },
        |_preview, full_chars, path| {
            RuntimeString::from_utf8(&format!(
                "\n\nTool output was truncated. The full {full_chars} chars were saved to "
            ))
            .concat(path.as_runtime_string())
            .concat(&RuntimeString::from(
                "; read it only if the omitted content is needed.",
            ))
        },
    )
    .await;
    match (&spilled.spilled_path, spilled.content.first()) {
        (Some(_), Some(OutputBlock::Text(note))) => Some(note.text.clone()),
        _ => None,
    }
}

fn bash_spill_path_dialect() -> NodePathDialect {
    if cfg!(windows) {
        NodePathDialect::Win32
    } else {
        NodePathDialect::Posix
    }
}

/// Build the `bash` input JSON Schema (identical to the provider-normalized form).
fn build_input_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "command": {
                "type": "string",
                "description": "The shell command to run. Runs in bash on macOS/Linux and in real bash (git-bash/WSL) on Windows when available; write POSIX shell syntax."
            },
            "timeout": {
                "description": "Timeout in milliseconds. Clamped to a maximum (default 10 minutes; raise it with the BASH_MAX_TIMEOUT_MS env var). No minimum.",
                "type": "integer",
                "exclusiveMinimum": 0,
                "maximum": 9007199254740991_i64
            },
            "cwd": {
                "description": "Working directory (relative to session cwd)",
                "type": "string"
            },
            "yieldMs": {
                "description": "Initial observation window for a resumable command. Defaults to 10000 ms; effective range is 250-30000 ms, with a 10000 ms minimum on Windows.",
                "type": "integer",
                "minimum": 0,
                "maximum": 9007199254740991_i64
            },
            "runInBackground": {
                "description": "Run a resumable command, returning its output and exit code if it finishes within the initial observation window, or a shellId if it remains running. Observe that shellId with bash_output and stop it with kill_shell.",
                "type": "boolean"
            },
            "sandboxPermissions": {
                "description": "Use require_escalated to request approval before running a command that needs network or filesystem access outside the sandbox. Use use_default for ordinary sandboxed execution.",
                "type":"string", "enum":["use_default","require_escalated"]
            },
            "justification": {
                "description":"Explain why this exact command needs access outside the sandbox. Required for require_escalated.",
                "type":"string", "minLength":1, "maxLength":2048
            }
        },
        "required": ["command"],
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap()
}

/// Run a shell command under the sandbox with a bounded, streamed output.
enum InitialObservationNotice {
    Observing(Option<RuntimeString>),
    Live,
    Finished,
}

pub struct BashTool {
    default_timeout_ms: u64,
    max_output_bytes: usize,
    input_schema: JsonSchema,
}

impl BashTool {
    fn new(opts: BashToolOptions) -> Self {
        BashTool {
            default_timeout_ms: opts.default_timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS),
            max_output_bytes: opts.max_output_bytes.unwrap_or(DEFAULT_MAX_OUTPUT_BYTES),
            input_schema: build_input_schema(),
        }
    }
}

/// Construct the `bash` tool with default options.
pub fn create_bash_tool() -> Arc<dyn Tool> {
    Arc::new(BashTool::new(BashToolOptions::default()))
}

/// Construct the `bash` tool with explicit options.
pub fn create_bash_tool_with(opts: BashToolOptions) -> Arc<dyn Tool> {
    Arc::new(BashTool::new(opts))
}

/// One shell run under a given sandbox — reused verbatim for the initial
/// sandboxed attempt and (on approval) the unsandboxed re-run, so their args can
/// never drift apart. Streams each stdout/stderr chunk through exactly one
/// channel: `on_progress` when Query installed it, otherwise `emit`.
#[allow(clippy::too_many_arguments)]
async fn run_one(
    process_context: &RuntimeProcessContext,
    sandbox: &dyn SandboxBackend,
    command: &RuntimeString,
    cwd: &RuntimePath,
    network_allowed: Option<bool>,
    signal: AbortSignal,
    timeout_ms: u64,
    max_output_bytes: usize,
    writable: Vec<RuntimeString>,
    unconfined_writes: bool,
    env: Option<RuntimeEnv>,
    emit: &EmitFn,
    on_progress: &Option<ToolProgress>,
) -> RunShellResult {
    let opts = RunShellOptions {
        process_context: process_context.clone(),
        command: command.clone(),
        cwd: cwd.clone(),
        sandbox,
        signal,
        timeout_ms,
        max_output_bytes,
        env,
        network_allowed,
        managed_network: None,
        writable_roots: Some(writable),
        unconfined_writes,
        on_chunk: Some(Box::new(
            move |stream: OutputStream, chunk: &RuntimeString| {
                let build = |s: OutputStream| -> ToolDomainEvent {
                    match s {
                        OutputStream::Stdout => ToolDomainEvent::Stdout {
                            chunk: chunk.clone(),
                        },
                        OutputStream::Stderr => ToolDomainEvent::Stderr {
                            chunk: chunk.clone(),
                        },
                    }
                };
                if let Some(cb) = on_progress {
                    cb(build(stream))
                } else {
                    emit(build(stream))
                }
            },
        )),
    };
    run_shell(opts).await
}

#[async_trait]
impl Tool for BashTool {
    fn static_name(&self) -> &str {
        "bash"
    }

    fn static_description(&self) -> &str {
        "Execute a shell command and return its combined output and exit code. Prefer the dedicated file tools when they fit; shell reads and searches are also appropriate when composition or unsupported options are useful. For shell searches, prefer rg or rg --files when available. Quote paths containing spaces and take care with command substitution. The working directory persists between calls: use absolute paths or an explicit command directory, and track any intentional directory change. Confirm uncertain destinations before creating files or directories; do not repeat existence checks when the current context already establishes the destination."
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
        false
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        false
    }

    fn planning_capability(&self, _input: &Json) -> crate::types::tool::PlanningToolCapability {
        crate::types::tool::PlanningToolCapability::Shell
    }

    fn to_command_string(&self, input: &Json) -> Option<RuntimeString> {
        input.get("command").and_then(RuntimeJson::as_str).cloned()
    }

    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        let command = input
            .get("command")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap_or_default();
        Some(RuntimeString::from("Running ").concat(&first_chars(&command, 60)))
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let command = input
            .get("command")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap_or_default();
        let cwd = match input.get("cwd").and_then(RuntimeJson::as_str) {
            Some(path) if !path.is_empty() => {
                resolve_runtime(&ctx.cwd, &[RuntimePath::from_runtime_string(path.clone())])
                    .map_err(|error| ToolError::new(format!("{error:?}")))?
            }
            _ => ctx.cwd.clone(),
        };
        // Captured interactive login-shell env (shell snapshot), if opted in — gives
        // the command the user's rc-sourced PATH/exports a bare `bash -c` would miss.
        let shell_env = match &ctx.shell_env {
            Some(f) => f().await,
            None => None,
        };
        // Apply the captured filesystem policy to execution as well as admission.
        let explicitly_escalated = request_shell_escalation(&input, ctx).await?;
        let unconfined_writes = explicitly_escalated || writes_are_unconfined(ctx.sandbox_mode);

        let writable = writable_roots(&PathContext {
            cwd: &cwd,
            additional_writable_roots: &ctx.permission.additional_writable_roots,
            unconfined_writes,
        })
        .into_iter()
        .map(RuntimePath::into_runtime_string)
        .collect::<Vec<_>>();

        // Plan mode auto-allows a command it cannot prove read-only by enforcing
        // a read-only filesystem (scratch temp only) and no network at the OS
        // layer instead of prompting per command. Provably read-only commands
        // never carry the marker, so their sandbox is unchanged.
        let plan_enforced = plan_readonly_enforced(&input);
        let sandbox_for_run: Arc<dyn SandboxBackend> = if explicitly_escalated {
            Arc::new(ProcessSandbox::new())
        } else if plan_enforced && ctx.sandbox.kind() != SandboxKind::Process {
            Arc::new(PlanReadOnlySandbox::new(ctx.sandbox.clone()))
        } else {
            ctx.sandbox.clone()
        };
        let network_for_run = if explicitly_escalated {
            Some(true)
        } else if plan_enforced {
            Some(false)
        } else {
            ctx.network_allowed
        };

        if input
            .get("runInBackground")
            .and_then(RuntimeJson::as_bool)
            .unwrap_or(false)
        {
            // When the shell exits on its own, tell the model (task-notification) so it
            // stops polling bash_output. A model-initiated kill_shell is skipped — the
            // model already knows.
            let enqueue = ctx.enqueue_background_notice.clone();
            let initial_notice = Arc::new(std::sync::Mutex::new(
                InitialObservationNotice::Observing(None),
            ));
            let notice_state = initial_notice.clone();
            let notice_processes = Arc::downgrade(&ctx.background_processes);
            let shell_id = ctx
                .background_processes
                .start(
                    ctx.process_context.clone(),
                    StartShellOptions {
                        command: command.clone(),
                        cwd,
                        sandbox: sandbox_for_run.clone(),
                        env: shell_env,
                        max_output_bytes: self.max_output_bytes,
                        writable_roots: Some(writable),
                        unconfined_writes,
                        network_allowed: network_for_run,
                        managed_network: None,
                        on_complete: Some(Box::new(move |r: CompleteResult| {
                            if r.status == BackgroundShellStatus::Killed
                                || notice_processes.upgrade().is_some_and(|processes| {
                                    processes.model_output_collected(&r.id)
                                })
                            {
                                return;
                            }
                            let code = match r.exit_code {
                                None => String::new(),
                                Some(c) => format!(" (exit code {c})"),
                            };
                            if let Some(enqueue) = &enqueue {
                                let code = RuntimeString::from_utf8(&code);
                                let message = RuntimeString::from("Background shell ")
                                    .concat(&r.id)
                                    .concat(&RuntimeString::from(" has finished"))
                                    .concat(&code)
                                    .concat(&RuntimeString::from(
                                        ". Read its final output with bash_output (shellId: \"",
                                    ))
                                    .concat(&r.id)
                                    .concat(&RuntimeString::from("\")."));
                                let mut state =
                                    notice_state.lock().unwrap_or_else(|e| e.into_inner());
                                match &mut *state {
                                    InitialObservationNotice::Observing(pending) => {
                                        *pending = Some(message)
                                    }
                                    InitialObservationNotice::Live => {
                                        drop(state);
                                        enqueue(message);
                                    }
                                    InitialObservationNotice::Finished => {}
                                }
                            }
                        })),
                    },
                )
                .await
                .map_err(ToolError::new)?;
            let requested = input
                .get("yieldMs")
                .and_then(RuntimeJson::as_u64)
                .unwrap_or(10_000);
            let minimum = if cfg!(windows) { 10_000 } else { 250 };
            let observed = ctx
                .background_processes
                .read_new_wait(
                    &shell_id,
                    requested.clamp(minimum, 30_000),
                    &ctx.signal,
                    None,
                )
                .await;
            let pending_notice = {
                let mut state = initial_notice.lock().unwrap_or_else(|e| e.into_inner());
                let next = if observed
                    .as_ref()
                    .is_ok_and(|result| result.status != BackgroundShellStatus::Running)
                {
                    InitialObservationNotice::Finished
                } else {
                    InitialObservationNotice::Live
                };
                match std::mem::replace(&mut *state, next) {
                    InitialObservationNotice::Observing(pending) => pending,
                    _ => None,
                }
            };
            if observed.as_ref().map_or(true, |result| {
                result.status == BackgroundShellStatus::Running
            }) {
                if let (Some(enqueue), Some(message)) =
                    (&ctx.enqueue_background_notice, pending_notice)
                {
                    enqueue(message);
                }
            }
            let observed = observed.map_err(ToolError::new)?;
            let running = observed.status == BackgroundShellStatus::Running;
            let text = RuntimeString::from("Started background shell ")
                .concat(&shell_id)
                .concat(&RuntimeString::from(
                    ".\nPoll its output with the bash_output tool (shellId: \"",
                ))
                .concat(&shell_id)
                .concat(&RuntimeString::from("\") and stop it with kill_shell."));
            let text = if running {
                observed
                    .output
                    .concat(&RuntimeString::from("\n"))
                    .concat(&text)
            } else {
                observed.output.concat(&RuntimeString::from_utf8(&format!(
                    "\n[status: exited, exit code {}]",
                    observed
                        .exit_code
                        .map_or_else(|| "unknown".into(), |code| code.to_string())
                )))
            };
            let mut output = RuntimeObject::new();
            output.insert(
                "exitCode",
                observed
                    .exit_code
                    .map_or(RuntimeJson::Null, RuntimeJson::from),
            );
            output.insert("timedOut", RuntimeJson::Bool(false));
            output.insert("aborted", RuntimeJson::Bool(false));
            output.insert(
                "status",
                RuntimeJson::from(if running { "running" } else { "exited" }),
            );
            output.insert(
                "shellId",
                if running {
                    RuntimeJson::String(shell_id)
                } else {
                    RuntimeJson::Null
                },
            );
            return Ok(text_result(RuntimeJson::Object(output), text, false));
        }

        let timeout_ms = resolve_bash_timeout(
            input.get("timeout").and_then(RuntimeJson::as_u64),
            self.default_timeout_ms,
            std::env::var("BASH_MAX_TIMEOUT_MS").ok().as_deref(),
        );
        // Baseline for the post-command stale-read sweep: only files whose
        // mtime moves past this point (and past their recorded baseline) were
        // touched by this command.
        let command_started_at_ms = crate::tools::freshness::now_ms();

        let shell_started_at = std::time::Instant::now();
        let res = run_one(
            &ctx.process_context,
            sandbox_for_run.as_ref(),
            &command,
            &cwd,
            network_for_run,
            ctx.signal.clone(),
            timeout_ms,
            self.max_output_bytes,
            writable.clone(),
            unconfined_writes,
            shell_env.clone(),
            &ctx.emit,
            &on_progress,
        )
        .await;
        let wall_ms = u64::try_from(shell_started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
        let wall_time = std::time::Duration::from_millis(wall_ms);

        let (_, is_error, _) = render_bash_outcome_text(
            &res.output,
            res.exit_code,
            res.timed_out,
            res.aborted,
            wall_time,
            DEFAULT_OUTPUT_TRUNCATION_POLICY,
        );
        // A command that genuinely failed (not a timeout/abort) MAY have been blocked
        // by the active sandbox rather than failing on its own — surface that so the
        // model doesn't waste turns "fixing" a command that is fine. Not evaluated for
        // timeout/abort, which are their own failure modes.
        let sandbox_denied = is_error
            && !res.timed_out
            && !res.aborted
            && is_likely_sandbox_denied(
                ctx.sandbox.kind(),
                res.exit_code,
                res.signal.as_ref(),
                &res.output,
            );

        // A policy-admitted denial retry still requires an approval decision.
        // Only a real sandbox denial enters this path. Approved ⇒ re-run
        // WITHOUT the sandbox (ProcessSandbox = env-scrub only, no fs/syscall
        // isolation). Denied / no approver ⇒ fall through to the surfaced denial.
        if let Some(request_approval) = ctx.request_approval.as_ref().filter(|_| {
            sandbox_denied
                && !ctx.planning_read_only
                && ctx
                    .approval_policy
                    .unwrap_or_default()
                    .allows_prompt(crate::types::permission::ApprovalPromptKind::Sandbox)
                && ctx.sandbox_retry_on_denial == Some(true)
        }) {
            let approved = request_approval(
                RuntimeString::from_utf8(&format!(
                    "The {} sandbox blocked this command (likely a file write outside the workspace or a forbidden syscall):\n  ",
                    sandbox_kind_str(ctx.sandbox.kind()),
                ))
                .concat(&command)
                .concat(&RuntimeString::from("\nRe-run it WITHOUT the sandbox?")),
            )
            .await;
            if approved {
                let process_sandbox = ProcessSandbox::new();
                let rerun_started_at = std::time::Instant::now();
                let rerun = run_one(
                    &ctx.process_context,
                    &process_sandbox,
                    &command,
                    &cwd,
                    Some(true),
                    ctx.signal.clone(),
                    timeout_ms,
                    self.max_output_bytes,
                    writable.clone(),
                    true,
                    shell_env.clone(),
                    &ctx.emit,
                    &on_progress,
                )
                .await;
                let rerun_wall_ms =
                    u64::try_from(rerun_started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
                let (mut rtext, rerun_error, rerun_truncated) = render_bash_outcome_text(
                    &rerun.output,
                    rerun.exit_code,
                    rerun.timed_out,
                    rerun.aborted,
                    std::time::Duration::from_millis(rerun_wall_ms),
                    DEFAULT_OUTPUT_TRUNCATION_POLICY,
                );
                if rerun_truncated {
                    if let Some(note) = spill_truncated_bash_output(&rerun.output, &cwd).await {
                        rtext = rtext.concat(&note);
                    }
                }
                rtext = rtext.concat(&RuntimeString::from(
                    "\n[Re-ran WITHOUT the sandbox after approval.]",
                ));
                if !rerun.aborted && !rerun.timed_out {
                    let stale = crate::tools::freshness::tracked_files_modified_since(
                        &command,
                        command_started_at_ms,
                        ctx.file_state.as_ref(),
                    )
                    .await;
                    if let Some(hint) = crate::tools::freshness::stale_read_hint(&stale, &cwd) {
                        rtext = rtext.concat(&RuntimeString::from("\n\n")).concat(&hint);
                    }
                }
                let mut output = RuntimeObject::new();
                output.insert(
                    "exitCode",
                    rerun.exit_code.map_or(RuntimeJson::Null, RuntimeJson::from),
                );
                output.insert("timedOut", RuntimeJson::Bool(rerun.timed_out));
                output.insert("aborted", RuntimeJson::Bool(rerun.aborted));
                output.insert("wallTimeMs", RuntimeJson::from(rerun_wall_ms));
                output.insert("sandboxRetried", RuntimeJson::Bool(true));
                return Ok(ToolResult {
                    serializable: ToolResultSerializable {
                        output: RuntimeJson::Object(output),
                        model_content: vec![crate::schemas::content::OutputBlock::Text(
                            crate::schemas::content::TextBlock {
                                text: rtext,
                                citations: None,
                                harness_envelope: None,
                                message_id: None,
                                phase: None,
                            },
                        )],
                        is_error: Some(rerun_error),
                        mcp_meta: None,
                        extra_user_blocks: None,
                    },
                    side_effects: None,
                });
            }
        }

        let (mut text, reconstructed_is_error, truncated) = render_bash_outcome_text(
            &res.output,
            res.exit_code,
            res.timed_out,
            res.aborted,
            wall_time,
            DEFAULT_OUTPUT_TRUNCATION_POLICY,
        );
        debug_assert_eq!(is_error, reconstructed_is_error);
        if truncated {
            if let Some(note) = spill_truncated_bash_output(&res.output, &cwd).await {
                text = text.concat(&note);
            }
        }
        if sandbox_denied {
            text = text.concat(&RuntimeString::from_utf8(&format!(
                "\n[This failure looks like the active sandbox ({kind}) denied the command — e.g. a blocked file write outside the workspace or a forbidden syscall — not a fault in the command itself. The engine will not silently re-run it unsandboxed; if it genuinely needs broader access, relax the sandbox (config sandbox: 'process') or widen the writable roots, then retry.]",
                kind = sandbox_kind_str(ctx.sandbox.kind()),
            )));
        }

        // A foreground command that rewrote files the model has already read
        // leaves stale baselines behind; surface that here instead of letting
        // the next edit bounce off the write guard.
        if !res.aborted && !res.timed_out {
            let stale = crate::tools::freshness::tracked_files_modified_since(
                &command,
                command_started_at_ms,
                ctx.file_state.as_ref(),
            )
            .await;
            if let Some(hint) = crate::tools::freshness::stale_read_hint(&stale, &cwd) {
                text = text.concat(&RuntimeString::from("\n\n")).concat(&hint);
            }
        }

        let mut output = RuntimeObject::new();
        output.insert(
            "exitCode",
            res.exit_code.map_or(RuntimeJson::Null, RuntimeJson::from),
        );
        output.insert("timedOut", RuntimeJson::Bool(res.timed_out));
        output.insert("aborted", RuntimeJson::Bool(res.aborted));
        output.insert("wallTimeMs", RuntimeJson::from(wall_ms));
        if sandbox_denied {
            output.insert("sandboxDenied", RuntimeJson::Bool(true));
        }

        Ok(text_result(RuntimeJson::Object(output), text, is_error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use crate::runtime_process::ParentEnvSnapshot;
    use crate::runtime_values::RuntimeNumber;
    #[cfg(unix)]
    use crate::tools::builtins::test_ctx::make_ctx;
    #[cfg(unix)]
    use crate::types::sandbox::{ExecRequest, SandboxPolicy, SpawnSpec};
    #[cfg(unix)]
    use crate::types::tool::RequestApprovalFn;
    use serde_json::Value;
    #[cfg(unix)]
    use std::path::PathBuf;
    #[cfg(unix)]
    use std::sync::atomic::{AtomicU64, Ordering};
    #[cfg(unix)]
    use std::sync::Mutex;

    #[cfg(unix)]
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

    #[cfg(unix)]
    fn temp_dir() -> PathBuf {
        let mut dir = std::env::temp_dir();
        let unique = format!(
            "ae-bash-tool-{}-{}-{}",
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

    /// Passes argv straight through to the real shell but REPORTS a chosen kind,
    /// so the denial heuristic engages on the command's output. The child inherits
    /// this process's env (plus any request env) so `bash` resolves on PATH.
    #[cfg(unix)]
    struct PassthroughSandbox {
        kind: SandboxKind,
    }
    #[cfg(unix)]
    impl SandboxBackend for PassthroughSandbox {
        fn kind(&self) -> SandboxKind {
            self.kind
        }
        fn is_available(&self) -> bool {
            true
        }
        fn transform(&self, req: &ExecRequest, _policy: &SandboxPolicy) -> SpawnSpec {
            let file = req.argv.first().cloned().unwrap_or_default();
            let args = req.argv.iter().skip(1).cloned().collect();
            let env = req.env.clone().unwrap_or_else(ParentEnvSnapshot::capture);
            SpawnSpec {
                file,
                args,
                cwd: req.cwd.clone(),
                env,
            }
        }
    }

    /// Blocks every command (emits a denial keyword + fails), engaging the
    /// escalation heuristic. The unsandboxed re-run uses a genuine ProcessSandbox.
    #[cfg(unix)]
    struct BlockingSandbox {
        kind: SandboxKind,
    }
    #[cfg(unix)]
    impl SandboxBackend for BlockingSandbox {
        fn kind(&self) -> SandboxKind {
            self.kind
        }
        fn is_available(&self) -> bool {
            true
        }
        fn transform(&self, req: &ExecRequest, _policy: &SandboxPolicy) -> SpawnSpec {
            let env = req.env.clone().unwrap_or_else(ParentEnvSnapshot::capture);
            SpawnSpec {
                file: RuntimeString::from("sh"),
                args: vec![
                    RuntimeString::from("-c"),
                    RuntimeString::from("printf \"operation not permitted\\n\" >&2; exit 1"),
                ],
                cwd: req.cwd.clone(),
                env,
            }
        }
    }

    /// Records each policy it is handed, then transforms to a harmless `sh`.
    #[cfg(unix)]
    struct CapturingSandbox {
        captured: Arc<Mutex<Vec<SandboxPolicy>>>,
    }
    #[cfg(unix)]
    impl SandboxBackend for CapturingSandbox {
        fn kind(&self) -> SandboxKind {
            SandboxKind::Process
        }
        fn is_available(&self) -> bool {
            true
        }
        fn transform(&self, req: &ExecRequest, policy: &SandboxPolicy) -> SpawnSpec {
            self.captured.lock().unwrap().push(policy.clone());
            let file = req
                .argv
                .first()
                .cloned()
                .unwrap_or_else(|| RuntimeString::from("echo"));
            let args = req.argv.iter().skip(1).cloned().collect();
            SpawnSpec {
                file,
                args,
                cwd: req.cwd.clone(),
                env: ParentEnvSnapshot::capture(),
            }
        }
    }

    #[cfg(unix)]
    fn base_ctx(cwd: &str, sandbox: Arc<dyn SandboxBackend>) -> ToolContext {
        let mut ctx = make_ctx(cwd);
        ctx.sandbox = sandbox;
        ctx
    }

    #[cfg(unix)]
    fn ui_string(value: &RuntimeString) -> String {
        String::from_utf8(value.to_node_utf8()).expect("Node UTF-8 UI projection is valid UTF-8")
    }

    #[cfg(unix)]
    fn text_of(res: &ToolResult) -> String {
        res.serializable
            .model_content
            .iter()
            .map(|b| match b {
                OutputBlock::Text(t) => ui_string(&t.text),
                _ => String::new(),
            })
            .collect()
    }

    // ---- resolve_bash_timeout ----

    const DEF: u64 = 120_000;

    #[test]
    fn timeout_uses_the_default_when_none_given() {
        assert_eq!(resolve_bash_timeout(None, DEF, None), DEF);
    }

    #[test]
    fn timeout_passes_an_in_range_value_through() {
        assert_eq!(resolve_bash_timeout(Some(5_000), DEF, None), 5_000);
    }

    #[test]
    fn timeout_clamps_above_the_default_cap() {
        assert_eq!(resolve_bash_timeout(Some(99_999_999), DEF, None), 600_000);
    }

    #[test]
    fn timeout_uses_env_max_when_positive() {
        assert_eq!(
            resolve_bash_timeout(Some(99_999_999), DEF, Some("300000")),
            300_000
        );
        assert_eq!(
            resolve_bash_timeout(Some(100_000), DEF, Some("300000")),
            100_000
        );
    }

    #[test]
    fn timeout_never_lets_the_cap_fall_below_the_default() {
        assert_eq!(
            resolve_bash_timeout(Some(99_999_999), DEF, Some("1000")),
            DEF
        );
    }

    #[test]
    fn timeout_falls_back_to_ten_minutes_on_an_invalid_env_value() {
        assert_eq!(
            resolve_bash_timeout(Some(99_999_999), DEF, Some("abc")),
            600_000
        );
        assert_eq!(
            resolve_bash_timeout(Some(99_999_999), DEF, Some("0")),
            600_000
        );
        assert_eq!(
            resolve_bash_timeout(Some(99_999_999), DEF, Some("-5")),
            600_000
        );
    }

    #[test]
    fn timeout_imposes_no_lower_bound() {
        assert_eq!(resolve_bash_timeout(Some(1), DEF, None), 1);
    }

    #[test]
    fn foreground_outcome_formatter_renders_metadata_sections() {
        for (exit, timed_out, aborted, wall_ms, output, expected, is_error) in [
            (
                Some(0),
                false,
                false,
                0u64,
                "OK",
                "Exit code: 0\nWall time: 0 seconds\nOutput:\nOK",
                false,
            ),
            (
                Some(1),
                false,
                false,
                0,
                "OK",
                "Exit code: 1\nWall time: 0 seconds\nOutput:\nOK",
                true,
            ),
            (
                None,
                false,
                false,
                0,
                "",
                "Exit code: null\nWall time: 0 seconds\nOutput:\n(no output)",
                true,
            ),
            (
                Some(1),
                true,
                true,
                123,
                "OK",
                "Exit code: 1\nWall time: 0.1 seconds\nOutput:\ncommand timed out after 123 milliseconds\nOK",
                true,
            ),
            (
                Some(1),
                false,
                true,
                0,
                "OK",
                "Exit code: 1\nWall time: 0 seconds\nOutput:\n[command aborted]\nOK",
                true,
            ),
            (
                Some(0),
                false,
                false,
                2500,
                "OK",
                "Exit code: 0\nWall time: 2.5 seconds\nOutput:\nOK",
                false,
            ),
        ] {
            assert_eq!(
                render_bash_outcome_text(
                    &rs(output),
                    exit,
                    timed_out,
                    aborted,
                    std::time::Duration::from_millis(wall_ms),
                    DEFAULT_OUTPUT_TRUNCATION_POLICY,
                ),
                (rs(expected), is_error, false)
            );
        }
    }

    #[test]
    fn outcome_tail_truncates_large_output_and_reports_the_original_line_count() {
        let output = rs((0..2_000)
            .map(|i| format!("line {i}\n"))
            .collect::<String>());
        let (text, is_error, truncated) = render_bash_outcome_text(
            &output,
            Some(0),
            false,
            false,
            std::time::Duration::ZERO,
            DEFAULT_OUTPUT_TRUNCATION_POLICY,
        );
        assert!(truncated);
        assert!(!is_error);
        let text = text.try_to_utf8().unwrap();
        // The body totals 18,890 bytes; the retained newest 10,000 start
        // exactly at "line 1000", and the elided head is marked with its
        // size.
        assert!(
            text.starts_with(
                "Exit code: 0\nWall time: 0 seconds\nTotal output lines: 2000\nOutput:\n…8890 chars truncated…line 1000\n"
            ),
            "{text}"
        );
        assert!(text.ends_with("line 1999\n"), "{text}");
        assert!(!text.contains("line 0\n"), "{text}");
    }

    #[test]
    fn outcome_keeps_failure_context_ahead_of_a_tail_truncated_body() {
        let output = rs("x".repeat(20_000));
        let (text, is_error, truncated) = render_bash_outcome_text(
            &output,
            Some(0),
            true,
            false,
            std::time::Duration::from_millis(123),
            DEFAULT_OUTPUT_TRUNCATION_POLICY,
        );
        assert!(truncated);
        assert!(is_error);
        let text = text.try_to_utf8().unwrap();
        // The timeout notice is re-prepended after truncation, so it survives
        // even though the head of the body is the elided side.
        assert!(
            text.contains(
                "Output:\ncommand timed out after 123 milliseconds\n…10000 chars truncated…"
            ),
            "{text}"
        );
        assert!(text.ends_with("xxxx"), "{text}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn truncated_output_spills_the_full_text_into_the_workspace_artifacts() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let ctx = base_ctx(&cwd, Arc::new(ProcessSandbox::new()));
        let result = create_bash_tool()
            .call(
                rjson!({
                    "command": "awk 'BEGIN { printf \"HEAD\\n\"; for (i = 0; i < 20000; i++) printf \"x\"; printf \"\\nTAIL\\n\" }'"
                }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(result.serializable.is_error, Some(false));
        assert!(result
            .serializable
            .output
            .get("wallTimeMs")
            .and_then(RuntimeJson::as_u64)
            .is_some());
        let text = text_of(&result);
        assert!(text.contains("chars truncated…"), "{text}");
        assert!(
            text.contains("Tool output was truncated. The full 20011 chars were saved to"),
            "{text}"
        );
        assert!(text.contains(".klaus/tool-results/bash-"), "{text}");
        let spill_dir = dir.join(".klaus").join("tool-results");
        let entries: Vec<_> = std::fs::read_dir(&spill_dir)
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        assert_eq!(entries.len(), 1);
        assert!(entries[0]
            .file_name()
            .to_string_lossy()
            .starts_with("bash-"));
        let saved = std::fs::read_to_string(entries[0].path()).unwrap();
        assert_eq!(saved.len(), 20_011);
        assert!(saved.starts_with("HEAD\n"));
        assert!(saved.ends_with("\nTAIL\n"));
        std::fs::remove_dir_all(&dir).ok();
    }

    // ---- metadata + schema ----

    #[test]
    fn exposes_expected_metadata() {
        let tool = create_bash_tool();
        assert_eq!(tool.name(), "bash");
        let input = rjson!({ "command": "echo hi" });
        assert!(!tool.is_read_only(&input));
        assert!(!tool.is_concurrency_safe(&input));
        assert!(tool.is_enabled());
        assert_eq!(tool.to_command_string(&input), Some(rs("echo hi")));
        assert_eq!(tool.summarize(&input), Some(rs("Running echo hi")));
    }

    #[test]
    fn summary_prefix_uses_javascript_utf16_slice() {
        let command = format!("{}😀x", "a".repeat(59));
        assert_eq!(
            crate::runtime_values::runtime_json::stringify(&RuntimeJson::String(first_chars(
                &rs(command.as_str()),
                60,
            ))),
            format!(r#""{}\ud83d""#, "a".repeat(59))
        );
        let parsed =
            crate::runtime_values::runtime_json::parse(&RuntimeString::from(r#""\ud800x""#))
                .unwrap();
        assert_eq!(
            crate::runtime_values::runtime_json::stringify(&RuntimeJson::String(first_chars(
                parsed.as_str().unwrap(),
                1,
            ))),
            r#""\ud800""#
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn parsed_lone_surrogate_command_projects_at_spawn() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let ctx = base_ctx(
            &cwd,
            Arc::new(PassthroughSandbox {
                kind: SandboxKind::Process,
            }),
        );
        let input = crate::runtime_values::runtime_json::parse(&RuntimeString::from(
            r#"{"command":"printf node-boundary > \ud800.txt"}"#,
        ))
        .unwrap();

        let result = create_bash_tool().call(input, &ctx, None).await.unwrap();

        assert_eq!(result.serializable.is_error, Some(false));
        assert_eq!(
            std::fs::read_to_string(dir.join("\u{fffd}.txt")).unwrap(),
            "node-boundary"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn input_schema_matches_the_expected_shape() {
        let tool = create_bash_tool();
        let schema = Value::Object(tool.static_input_schema().clone());
        assert_eq!(
            schema,
            json!({
                "type": "object",
                "properties": {
                    "command": {
                        "type": "string",
                        "description": "The shell command to run. Runs in bash on macOS/Linux and in real bash (git-bash/WSL) on Windows when available; write POSIX shell syntax."
                    },
                    "timeout": {
                        "description": "Timeout in milliseconds. Clamped to a maximum (default 10 minutes; raise it with the BASH_MAX_TIMEOUT_MS env var). No minimum.",
                        "type": "integer",
                        "exclusiveMinimum": 0,
                        "maximum": 9007199254740991_i64
                    },
                    "cwd": {
                        "description": "Working directory (relative to session cwd)",
                        "type": "string"
                    },
                    "yieldMs": {
                "description": "Initial observation window for a resumable command. Defaults to 10000 ms; effective range is 250-30000 ms, with a 10000 ms minimum on Windows.",
                "type": "integer",
                "minimum": 0,
                "maximum": 9007199254740991_i64
            },
            "runInBackground": {
                        "description": "Run a resumable command, returning its output and exit code if it finishes within the initial observation window, or a shellId if it remains running. Observe that shellId with bash_output and stop it with kill_shell.",
                        "type": "boolean"
                    },
                    "sandboxPermissions": {
                        "description": "Use require_escalated to request approval before running a command that needs network or filesystem access outside the sandbox. Use use_default for ordinary sandboxed execution.",
                        "type": "string", "enum": ["use_default", "require_escalated"]
                    },
                    "justification": {
                        "description": "Explain why this exact command needs access outside the sandbox. Required for require_escalated.",
                        "type": "string", "minLength": 1, "maxLength": 2048
                    }
                },
                "required": ["command"],
                "additionalProperties": false
            })
        );
    }

    // ---- sandbox-denial annotation ----

    #[cfg(unix)]
    #[tokio::test]
    async fn annotates_a_failure_that_looks_like_a_sandbox_denial() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let ctx = base_ctx(
            &cwd,
            Arc::new(PassthroughSandbox {
                kind: SandboxKind::Seatbelt,
            }),
        );
        let res = create_bash_tool()
            .call(
                rjson!({ "command": "printf 'open: Operation not permitted\\n' >&2; exit 1" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            res.serializable.output.get("sandboxDenied"),
            Some(&rjson!(true))
        );
        assert!(text_of(&res).contains("active sandbox (seatbelt) denied"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn does_not_annotate_an_ordinary_command_failure() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let ctx = base_ctx(
            &cwd,
            Arc::new(PassthroughSandbox {
                kind: SandboxKind::Seatbelt,
            }),
        );
        let res = create_bash_tool()
            .call(
                rjson!({ "command": "printf 'nope\\n' >&2; exit 1" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.output.get("sandboxDenied"), None);
        assert!(!text_of(&res).contains("denied the command"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn never_annotates_under_the_process_backend() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let ctx = base_ctx(
            &cwd,
            Arc::new(PassthroughSandbox {
                kind: SandboxKind::Process,
            }),
        );
        let res = create_bash_tool()
            .call(
                rjson!({ "command": "printf 'permission denied\\n' >&2; exit 1" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.output.get("sandboxDenied"), None);
        std::fs::remove_dir_all(&dir).ok();
    }

    // ---- network egress threading ----

    #[cfg(unix)]
    #[tokio::test]
    async fn threads_network_allowed_into_the_sandbox_policy() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();

        let denied = Arc::new(Mutex::new(Vec::new()));
        let ctx = base_ctx(
            &cwd,
            Arc::new(CapturingSandbox {
                captured: denied.clone(),
            }),
        );
        create_bash_tool()
            .call(rjson!({ "command": "echo hi" }), &ctx, None)
            .await
            .unwrap();
        assert!(!denied.lock().unwrap()[0].network_allowed);

        let allowed = Arc::new(Mutex::new(Vec::new()));
        let mut ctx = base_ctx(
            &cwd,
            Arc::new(CapturingSandbox {
                captured: allowed.clone(),
            }),
        );
        ctx.network_allowed = Some(true);
        create_bash_tool()
            .call(rjson!({ "command": "echo hi" }), &ctx, None)
            .await
            .unwrap();
        assert!(allowed.lock().unwrap()[0].network_allowed);
        std::fs::remove_dir_all(&dir).ok();
    }

    // ---- plan-mode read-only enforcement ----

    /// Records each policy it is handed, presenting itself as a REAL isolation
    /// backend so the plan-mode wrapper engages (the process backend is skipped).
    #[cfg(unix)]
    struct SeatbeltCapturingSandbox {
        captured: Arc<Mutex<Vec<SandboxPolicy>>>,
    }
    #[cfg(unix)]
    impl SandboxBackend for SeatbeltCapturingSandbox {
        fn kind(&self) -> SandboxKind {
            SandboxKind::Seatbelt
        }
        fn is_available(&self) -> bool {
            true
        }
        fn transform(&self, req: &ExecRequest, policy: &SandboxPolicy) -> SpawnSpec {
            self.captured.lock().unwrap().push(policy.clone());
            SpawnSpec {
                file: req
                    .argv
                    .first()
                    .cloned()
                    .unwrap_or_else(|| RuntimeString::from("echo")),
                args: req.argv.iter().skip(1).cloned().collect(),
                cwd: req.cwd.clone(),
                env: ParentEnvSnapshot::capture(),
            }
        }
    }

    /// The permission engine's plan-mode marker turns the run's sandbox policy
    /// read-only: no writable roots, no scratch-temp grants, no network. A
    /// command WITHOUT the marker keeps the session policy untouched.
    #[cfg(unix)]
    #[tokio::test]
    async fn plan_mode_marker_enforces_a_read_only_sandbox_policy() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();

        let captured = Arc::new(Mutex::new(Vec::new()));
        let mut ctx = base_ctx(
            &cwd,
            Arc::new(SeatbeltCapturingSandbox {
                captured: captured.clone(),
            }),
        );
        ctx.network_allowed = Some(true);
        let mut input = rjson!({ "command": "python3 -c 'print(1)'" });
        if let RuntimeJson::Object(object) = &mut input {
            object.insert(
                crate::permission::PLAN_MODE_READONLY_ENFORCED_KEY,
                RuntimeJson::Bool(true),
            );
        }
        create_bash_tool().call(input, &ctx, None).await.unwrap();
        let policy = captured.lock().unwrap().last().cloned().unwrap();
        assert!(
            policy.writable_roots.is_empty(),
            "the enforced run must not grant any writable root"
        );
        assert_eq!(policy.exclude_tmpdir_env_var, Some(true));
        assert_eq!(policy.exclude_slash_tmp, Some(true));
        assert!(!policy.network_allowed, "the enforced run must be offline");

        // Without the marker the session policy is untouched.
        let plain = Arc::new(Mutex::new(Vec::new()));
        let mut ctx = base_ctx(
            &cwd,
            Arc::new(SeatbeltCapturingSandbox {
                captured: plain.clone(),
            }),
        );
        ctx.network_allowed = Some(true);
        create_bash_tool()
            .call(rjson!({ "command": "echo hi" }), &ctx, None)
            .await
            .unwrap();
        let policy = plain.lock().unwrap().last().cloned().unwrap();
        assert!(
            !policy.writable_roots.is_empty(),
            "an unmarked run keeps the session writable roots"
        );
        assert!(policy.network_allowed);
        std::fs::remove_dir_all(&dir).ok();
    }

    // ---- shell-env injection ----

    #[cfg(unix)]
    #[tokio::test]
    async fn injects_the_captured_login_shell_env() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let mut ctx = base_ctx(&cwd, Arc::new(ProcessSandbox::new()));
        ctx.shell_env = Some(Arc::new(|| {
            Box::pin(async {
                let mut m = RuntimeEnv::new();
                m.insert(rs("AE_SNAPSHOT_VAR"), rs("from-snapshot"));
                Some(m)
            })
        }));
        let res = create_bash_tool()
            .call(
                rjson!({ "command": "echo \"[$AE_SNAPSHOT_VAR]\"" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert!(text_of(&res).contains("[from-snapshot]"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn runs_normally_when_no_shell_env_is_provided() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let ctx = base_ctx(&cwd, Arc::new(ProcessSandbox::new()));
        let res = create_bash_tool()
            .call(rjson!({ "command": "echo hi" }), &ctx, None)
            .await
            .unwrap();
        assert!(text_of(&res).contains("hi"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[cfg(unix)]
    use crate::types::file_state::FileStateCache as _;

    #[cfg(unix)]
    #[derive(Default)]
    struct RecordingFileState(
        std::sync::Mutex<
            std::collections::HashMap<RuntimePath, crate::types::file_state::FileStateEntry>,
        >,
    );

    #[cfg(unix)]
    impl crate::types::file_state::FileStateCache for RecordingFileState {
        fn get(&self, path: &RuntimePath) -> Option<crate::types::file_state::FileStateEntry> {
            self.0.lock().unwrap().get(path).cloned()
        }
        fn record(
            &self,
            path: &RuntimePath,
            mtime_ms: f64,
            opts: crate::types::file_state::RecordOptions,
        ) {
            self.0.lock().unwrap().insert(
                path.clone(),
                crate::types::file_state::FileStateEntry {
                    mtime_ms,
                    size: opts.size,
                    content: opts.content,
                    encoding: opts.encoding,
                },
            );
        }
        fn has(&self, path: &RuntimePath) -> bool {
            self.0.lock().unwrap().contains_key(path)
        }
        fn entries(&self) -> Vec<(RuntimePath, crate::types::file_state::FileStateEntry)> {
            self.0
                .lock()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        }
        fn delete(&self, path: &RuntimePath) {
            self.0.lock().unwrap().remove(path);
        }
        fn clear(&self) {
            self.0.lock().unwrap().clear();
        }
        fn clone_box(&self) -> Box<dyn crate::types::file_state::FileStateCache> {
            Box::new(Self(std::sync::Mutex::new(self.0.lock().unwrap().clone())))
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_modifying_command_flags_tracked_reads_as_stale() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let tracked = dir.join("tracked.txt");
        std::fs::write(&tracked, "original").unwrap();
        let mut ctx = base_ctx(&cwd, Arc::new(ProcessSandbox::new()));
        let cache = Arc::new(RecordingFileState::default());
        cache.record(
            &RuntimePath::from(tracked.to_string_lossy().into_owned()),
            1.0,
            crate::types::file_state::RecordOptions {
                size: None,
                content: Some(RuntimeString::from("original")),
                encoding: None,
            },
        );
        ctx.file_state = cache;
        let res = create_bash_tool()
            .call(
                rjson!({ "command": "echo changed > tracked.txt && true --write" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        let text = text_of(&res);
        assert!(
            text.contains(
                "[This command modified 1 file you've previously read: tracked.txt. Call Read before editing.]"
            ),
            "{text}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_plain_command_leaves_tracked_reads_unflagged() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let tracked = dir.join("tracked.txt");
        std::fs::write(&tracked, "original").unwrap();
        let mut ctx = base_ctx(&cwd, Arc::new(ProcessSandbox::new()));
        let cache = Arc::new(RecordingFileState::default());
        cache.record(
            &RuntimePath::from(tracked.to_string_lossy().into_owned()),
            1.0,
            crate::types::file_state::RecordOptions {
                size: None,
                content: Some(RuntimeString::from("original")),
                encoding: None,
            },
        );
        ctx.file_state = cache;
        let res = create_bash_tool()
            .call(
                rjson!({ "command": "echo changed > tracked.txt" }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        let text = text_of(&res);
        assert!(!text.contains("This command modified"), "{text}");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_chunk_uses_exactly_one_progress_channel_when_both_are_available() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let emitted = Arc::new(Mutex::new(Vec::<ToolDomainEvent>::new()));
        let progressed = Arc::new(Mutex::new(Vec::<ToolDomainEvent>::new()));
        let mut ctx = base_ctx(&cwd, Arc::new(ProcessSandbox::new()));
        let emitted_clone = Arc::clone(&emitted);
        ctx.emit = std::sync::Arc::new(move |event| {
            emitted_clone.lock().unwrap().push(event);
            Box::pin(async {})
        });
        let progressed_clone = Arc::clone(&progressed);
        let progress: ToolProgress = Box::new(move |event| {
            progressed_clone.lock().unwrap().push(event);
            Box::pin(async {})
        });

        let result = create_bash_tool()
            .call(
                rjson!({ "command": "printf SINGLE_CHANNEL" }),
                &ctx,
                Some(progress),
            )
            .await
            .unwrap();

        let text = text_of(&result);
        assert!(
            text.starts_with("Exit code: 0\nWall time: ")
                && text.ends_with(" seconds\nOutput:\nSINGLE_CHANNEL"),
            "{text}"
        );
        assert!(emitted.lock().unwrap().is_empty());
        let progressed = progressed.lock().unwrap();
        assert_eq!(progressed.len(), 1, "{progressed:?}");
        assert!(matches!(
            &progressed[0],
            ToolDomainEvent::Stdout { chunk } if chunk == "SINGLE_CHANNEL"
        ));
        std::fs::remove_dir_all(&dir).ok();
    }

    // ---- sandbox-denial escalation ----

    #[cfg(unix)]
    #[tokio::test]
    async fn escalation_off_by_default_denial_surfaced_and_approval_never_requested() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let asked = Arc::new(Mutex::new(false));
        let asked_cb = asked.clone();
        let mut ctx = base_ctx(
            &cwd,
            Arc::new(BlockingSandbox {
                kind: SandboxKind::Seatbelt,
            }),
        );
        // sandbox_retry_on_denial left off.
        let approver: RequestApprovalFn = Arc::new(move |_m| {
            let asked_cb = asked_cb.clone();
            Box::pin(async move {
                *asked_cb.lock().unwrap() = true;
                true
            })
        });
        ctx.request_approval = Some(approver);
        let res = create_bash_tool()
            .call(rjson!({ "command": "echo RERUN_OK" }), &ctx, None)
            .await
            .unwrap();
        assert!(!*asked.lock().unwrap());
        assert_eq!(
            res.serializable.output.get("sandboxDenied"),
            Some(&rjson!(true))
        );
        assert_eq!(res.serializable.output.get("sandboxRetried"), None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn escalation_on_and_approved_reruns_without_the_sandbox() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let asked_message = Arc::new(Mutex::new(String::new()));
        let msg_cb = asked_message.clone();
        let mut ctx = base_ctx(
            &cwd,
            Arc::new(BlockingSandbox {
                kind: SandboxKind::Seatbelt,
            }),
        );
        ctx.sandbox_retry_on_denial = Some(true);
        let approver: RequestApprovalFn = Arc::new(move |m: RuntimeString| {
            let msg_cb = msg_cb.clone();
            Box::pin(async move {
                *msg_cb.lock().unwrap() = ui_string(&m);
                true
            })
        });
        ctx.request_approval = Some(approver);
        let res = create_bash_tool()
            .call(rjson!({ "command": "printf RERUN_OK" }), &ctx, None)
            .await
            .unwrap();
        assert!(asked_message.lock().unwrap().contains("sandbox blocked"));
        assert_eq!(
            res.serializable.output.get("sandboxRetried"),
            Some(&rjson!(true))
        );
        assert_eq!(res.serializable.output.get("exitCode"), Some(&rjson!(0)));
        let text = text_of(&res);
        assert!(text.contains("RERUN_OK"), "text: {text}");
        assert!(text.contains("WITHOUT the sandbox"), "text: {text}");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn escalation_on_and_denied_falls_through_to_the_surfaced_denial() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let mut ctx = base_ctx(
            &cwd,
            Arc::new(BlockingSandbox {
                kind: SandboxKind::Seatbelt,
            }),
        );
        ctx.sandbox_retry_on_denial = Some(true);
        let approver: RequestApprovalFn = Arc::new(|_m: RuntimeString| Box::pin(async { false }));
        ctx.request_approval = Some(approver);
        let res = create_bash_tool()
            .call(rjson!({ "command": "echo X" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(
            res.serializable.output.get("sandboxDenied"),
            Some(&rjson!(true))
        );
        assert_eq!(res.serializable.output.get("sandboxRetried"), None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn escalation_with_no_approver_degrades_to_the_surfaced_denial() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let mut ctx = base_ctx(
            &cwd,
            Arc::new(BlockingSandbox {
                kind: SandboxKind::Seatbelt,
            }),
        );
        ctx.sandbox_retry_on_denial = Some(true);
        // request_approval absent.
        let res = create_bash_tool()
            .call(rjson!({ "command": "echo X" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(
            res.serializable.output.get("sandboxDenied"),
            Some(&rjson!(true))
        );
        assert_eq!(res.serializable.output.get("sandboxRetried"), None);
        std::fs::remove_dir_all(&dir).ok();
    }

    // ---- background ----

    #[cfg(unix)]
    #[tokio::test]
    async fn initial_completed_output_does_not_enqueue_a_redundant_completion_notice() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let notices = Arc::new(Mutex::new(Vec::<String>::new()));
        let sink = notices.clone();
        let mut ctx = base_ctx(&cwd, Arc::new(ProcessSandbox::new()));
        ctx.enqueue_background_notice = Some(Arc::new(move |text| {
            sink.lock().unwrap().push(ui_string(&text))
        }));
        let result = create_bash_tool()
            .call(
                rjson!({ "command": "printf final-output; exit 7", "runInBackground": true }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        ctx.background_processes
            .close_and_wait(tokio::time::Instant::now() + std::time::Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(
            result.serializable.output.get("shellId"),
            Some(&RuntimeJson::Null)
        );
        assert_eq!(result.serializable.output.get("exitCode"), Some(&rjson!(7)));
        assert!(text_of(&result).contains("final-output"));
        assert!(notices.lock().unwrap().is_empty());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn final_output_collected_by_a_later_observation_does_not_enqueue_another_read() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let notices = Arc::new(Mutex::new(Vec::<String>::new()));
        let sink = notices.clone();
        let mut ctx = base_ctx(&cwd, Arc::new(ProcessSandbox::new()));
        ctx.enqueue_background_notice = Some(Arc::new(move |text| {
            sink.lock().unwrap().push(ui_string(&text))
        }));
        let result = create_bash_tool().call(rjson!({ "command": "sleep 0.5; printf final-output", "runInBackground": true, "yieldMs": 250 }), &ctx, None).await.unwrap();
        let id = result
            .serializable
            .output
            .get("shellId")
            .and_then(RuntimeJson::as_str)
            .unwrap()
            .clone();
        let output = ctx
            .background_processes
            .read_new_wait(&id, 5_000, &ctx.signal, None)
            .await
            .unwrap();
        ctx.background_processes
            .close_and_wait(tokio::time::Instant::now() + std::time::Duration::from_secs(5))
            .await
            .unwrap();
        assert!(output.output.contains("final-output"));
        assert!(notices.lock().unwrap().is_empty());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_in_background_returns_a_shell_id_and_notifies_on_completion() {
        let dir = temp_dir();
        let cwd = dir.to_string_lossy().into_owned();
        let notices = Arc::new(Mutex::new(Vec::<String>::new()));
        let sink = notices.clone();
        let mut ctx = base_ctx(&cwd, Arc::new(ProcessSandbox::new()));
        ctx.enqueue_background_notice = Some(Arc::new(move |t: RuntimeString| {
            sink.lock().unwrap().push(ui_string(&t))
        }));
        let res = create_bash_tool()
            .call(
                rjson!({ "command": "sleep 0.5; echo bg", "runInBackground": true, "yieldMs": 250 }),
                &ctx,
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        let shell_id = res
            .serializable
            .output
            .get("shellId")
            .and_then(RuntimeJson::as_str)
            .map(ui_string)
            .unwrap();
        assert!(text_of(&res).contains(&format!("Started background shell {shell_id}")));

        // The shell exits on its own; wait for the completion notice.
        for _ in 0..100 {
            if !notices.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let got = notices.lock().unwrap().clone();
        assert!(
            got.iter()
                .any(|n| n.contains(&shell_id) && n.contains("has finished")),
            "notices: {got:?}"
        );
        let notice = got
            .iter()
            .find(|n| n.contains(&shell_id))
            .expect("completion notice");
        assert_eq!(
            notice,
            &format!(
                "Background shell {shell_id} has finished (exit code 0). Read its final output with bash_output (shellId: \"{shell_id}\")."
            )
        );
        assert!(!notice.contains("<system-reminder>"));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(notices.lock().unwrap().len(), 1);
        ctx.background_processes.remove(&rs(shell_id.as_str()));
        std::fs::remove_dir_all(&dir).ok();
    }
}

#[cfg(test)]
mod exact_tests {
    use super::*;

    #[test]
    fn outcome_text_preserves_exact_output() {
        let output = RuntimeString::from_utf16(vec![b'o' as u16, 0xd800, b'k' as u16]);
        let (rendered, is_error, truncated) = render_bash_outcome_text(
            &output,
            Some(0),
            false,
            false,
            std::time::Duration::ZERO,
            DEFAULT_OUTPUT_TRUNCATION_POLICY,
        );
        let expected =
            RuntimeString::from("Exit code: 0\nWall time: 0 seconds\nOutput:\n").concat(&output);
        assert_eq!(rendered, expected);
        assert!(!is_error);
        assert!(!truncated);
    }
}
