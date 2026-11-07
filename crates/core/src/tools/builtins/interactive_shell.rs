//! The interactive-shell [`Tool`]s: `exec_command` runs a command in a
//! persistent shell session (finishes → output + exit code; stays alive → a
//! sessionId the model drives), and `write_stdin` feeds input to (or polls output
//! from) such a session.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::command_runtime::background::AdoptBackgroundCommandOptions;
use crate::fs::node_path::resolve_runtime;
use crate::fs::path_safety::{writable_roots, writes_are_unconfined, PathContext};
use crate::runtime_values::{RuntimeJson, RuntimeObject, RuntimePath, RuntimeString};
use crate::tools::build_tool::text_result;
use crate::tools::shell_session::{
    ExecCommandParams, TerminalOptions, WriteStdinErrorClass, WriteStdinFailure, WriteStdinParams,
};
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{Tool, ToolContext, ToolExposure, ToolProgress, ToolResult};

use super::shell_common::{
    code_str, first_chars, request_shell_escalation, DEFAULT_MAX_OUTPUT_BYTES,
};

const MAX_TERMINAL_DIMENSION: u16 = 10_000;
const EXEC_INPUT_SCHEMA_PROPERTY_ORDER: &[&str] = &[
    "command",
    "yieldMs",
    "cwd",
    "tty",
    "cols",
    "rows",
    "sandboxPermissions",
    "justification",
];

/// Construction options for `exec_command`.
#[derive(Debug, Clone, Default)]
pub struct InteractiveShellOptions {
    pub max_output_bytes: Option<usize>,
}

fn terminal_u16(input: &Json, key: &str, default: u16) -> u16 {
    input
        .get(key)
        .and_then(RuntimeJson::as_u64)
        .and_then(|v| u16::try_from(v).ok())
        .filter(|v| *v > 0)
        .map(|v| v.min(MAX_TERMINAL_DIMENSION))
        .unwrap_or(default)
}

fn terminal_size_json(size: Option<TerminalOptions>) -> RuntimeJson {
    let Some(size) = size else {
        return RuntimeJson::Null;
    };
    let mut object = RuntimeObject::new();
    object.insert("cols", RuntimeJson::from(size.cols));
    object.insert("rows", RuntimeJson::from(size.rows));
    RuntimeJson::Object(object)
}

// ===================== exec_command =====================

fn exec_input_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "command": {
                "type": "string",
                "description": "The command to run. Prefer bash with runInBackground plus bash_output and kill_shell for long-running non-interactive commands. Use exec_command only when the same live process must accept later input or preserve REPL/TTY state; if it stays alive, a sessionId is returned for write_stdin. write_stdin terminal failures allow zero retries; transient failures allow one recovery call before the engine blocks that sessionId."
            },
            "yieldMs": {
                "description": "How long to wait for it to finish before handing back a session (default 10000).",
                "type": "integer",
                "exclusiveMinimum": 0,
                "maximum": 9007199254740991_i64
            },
            "cwd": {
                "description": "Working directory (relative to session cwd).",
                "type": "string"
            },
            "tty": {
                "description": "Run the command attached to a real pseudo-terminal instead of pipes. Use only for full-screen TUI programs or commands that require a TTY.",
                "type": "boolean"
            },
            "cols": {
                "description": "Initial terminal width in columns when tty is true (default 80).",
                "type": "integer",
                "minimum": 1,
                "maximum": MAX_TERMINAL_DIMENSION
            },
            "rows": {
                "description": "Initial terminal height in rows when tty is true (default 24).",
                "type": "integer",
                "minimum": 1,
                "maximum": MAX_TERMINAL_DIMENSION
            },
            "sandboxPermissions": {
                "description":"Use require_escalated to request approval before running a command that needs network or filesystem access outside the sandbox. Use use_default for ordinary sandboxed execution.",
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

/// Run a command in a persistent, interactive shell session.
pub struct ExecCommandTool {
    max_output_bytes: usize,
    input_schema: JsonSchema,
}

impl ExecCommandTool {
    fn new(opts: InteractiveShellOptions) -> Self {
        ExecCommandTool {
            max_output_bytes: opts.max_output_bytes.unwrap_or(DEFAULT_MAX_OUTPUT_BYTES),
            input_schema: exec_input_schema(),
        }
    }
}

/// Construct the `exec_command` tool with default options.
pub fn create_exec_command_tool() -> Arc<dyn Tool> {
    Arc::new(ExecCommandTool::new(InteractiveShellOptions::default()))
}

/// Construct the `exec_command` tool with explicit options.
pub fn create_exec_command_tool_with(opts: InteractiveShellOptions) -> Arc<dyn Tool> {
    Arc::new(ExecCommandTool::new(opts))
}

#[async_trait]
impl Tool for ExecCommandTool {
    fn static_name(&self) -> &str {
        "exec_command"
    }

    fn static_description(&self) -> &str {
        "Run a command in a persistent interactive shell. Prefer bash in background mode with bash_output and kill_shell for long-running non-interactive work. Use exec_command only when the same live process must receive later input or preserve REPL/TTY state. If it stays alive, it returns a sessionId for write_stdin; stop it with kill_shell. write_stdin terminal failures allow zero retries; transient failures allow one recovery call before the engine blocks that sessionId."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn exposure(&self) -> ToolExposure {
        ToolExposure::Deferred
    }

    fn input_schema_property_order(
        &self,
        object_path: &[String],
    ) -> Option<&'static [&'static str]> {
        object_path
            .is_empty()
            .then_some(EXEC_INPUT_SCHEMA_PROPERTY_ORDER)
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

    fn to_command_string(&self, input: &Json) -> Option<RuntimeString> {
        input.get("command").and_then(RuntimeJson::as_str).cloned()
    }

    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        let command = input
            .get("command")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap_or_default();
        Some(RuntimeString::from("exec ").concat(&first_chars(&command, 60)))
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
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
        let terminal = input
            .get("tty")
            .and_then(RuntimeJson::as_bool)
            .filter(|tty| *tty)
            .map(|_| TerminalOptions {
                cols: terminal_u16(&input, "cols", 80),
                rows: terminal_u16(&input, "rows", 24),
            });

        let shell_sessions = ctx.background_processes.shell_sessions();
        let command_for_process = command.clone();
        let cwd_for_process = cwd.clone();
        let mut r = shell_sessions
            .exec(
                ctx.process_context.clone(),
                ExecCommandParams {
                    command: command.clone(),
                    cwd: cwd.clone(),
                    sandbox: if explicitly_escalated {
                        Arc::new(crate::sandbox::process::ProcessSandbox::new())
                    } else {
                        ctx.sandbox.clone()
                    },
                    env: None,
                    network_allowed: if explicitly_escalated {
                        Some(true)
                    } else {
                        ctx.network_allowed
                    },
                    managed_network: None,
                    writable_roots: Some(writable.clone()),
                    unconfined_writes,
                    yield_ms: input.get("yieldMs").and_then(RuntimeJson::as_i64),
                    max_output_bytes: Some(self.max_output_bytes),
                    signal: Some(ctx.signal.clone()),
                    terminal,
                },
            )
            .await;

        let sandbox_denied = r.exited
            && r.exit_code != Some(0)
            && crate::sandbox::denial::is_likely_sandbox_denied(
                ctx.sandbox.kind(),
                r.exit_code,
                None,
                &r.output,
            );
        if let Some(approve) = ctx.request_approval.as_ref().filter(|_| {
            sandbox_denied
                && !ctx.planning_read_only
                && ctx
                    .approval_policy
                    .unwrap_or_default()
                    .allows_prompt(crate::types::permission::ApprovalPromptKind::Sandbox)
                && ctx.sandbox_retry_on_denial == Some(true)
        }) {
            let message = RuntimeString::from(
                "The sandbox blocked this command. Allow it to run outside the sandbox?\n\n",
            )
            .concat(&command);
            if approve(message).await {
                r = shell_sessions
                    .exec(
                        ctx.process_context.clone(),
                        ExecCommandParams {
                            command,
                            cwd,
                            sandbox: Arc::new(crate::sandbox::process::ProcessSandbox::new()),
                            env: None,
                            network_allowed: Some(true),
                            managed_network: None,
                            writable_roots: Some(writable),
                            unconfined_writes: true,
                            yield_ms: input.get("yieldMs").and_then(RuntimeJson::as_i64),
                            max_output_bytes: Some(self.max_output_bytes),
                            signal: Some(ctx.signal.clone()),
                            terminal,
                        },
                    )
                    .await;
                r.output = r.output.concat(&RuntimeString::from(
                    "\n[Re-ran outside the sandbox after approval.]",
                ));
            }
        }

        if let Some(session_id) = r.session_id.clone() {
            let events = shell_sessions
                .subscribe_events(&session_id)
                .map_err(ToolError::new)?;
            let stop_sessions = shell_sessions.clone();
            let enqueue = ctx.enqueue_background_notice.clone();
            ctx.background_processes
                .adopt(AdoptBackgroundCommandOptions {
                    id: session_id.clone(),
                    command: command_for_process,
                    cwd: cwd_for_process,
                    max_output_bytes: self.max_output_bytes,
                    events,
                    stop: Arc::new(move |id, signal| stop_sessions.kill_with_signal(id, signal)),
                    on_complete: Some(Box::new(move |result| {
                        if result.status
                            == crate::tools::background_shells::BackgroundShellStatus::Killed
                        {
                            return;
                        }
                        if let Some(enqueue) = &enqueue {
                            let code = result
                                .exit_code
                                .map(|value| format!(" (exit code {value})"))
                                .unwrap_or_default();
                            enqueue(
                                RuntimeString::from("Background shell ")
                                    .concat(&result.id)
                                    .concat(&RuntimeString::from(" has finished"))
                                    .concat(&RuntimeString::from_utf8(&code))
                                    .concat(&RuntimeString::from(
                                        ". Read its final output with write_stdin (sessionId: \"",
                                    ))
                                    .concat(&result.id)
                                    .concat(&RuntimeString::from("\").")),
                            );
                        }
                    })),
                })
                .await
                .map_err(ToolError::new)?;
        }

        let body = if r.output.is_empty() {
            RuntimeString::from("(no output)")
        } else {
            r.output.clone()
        };
        let status = if r.exited {
            RuntimeString::from("[exited, code ")
                .concat(&code_str(r.exit_code))
                .concat(&RuntimeString::from("]"))
        } else {
            let session_id = r
                .session_id
                .clone()
                .unwrap_or_else(|| RuntimeString::from("null"));
            RuntimeString::from("[session ")
                .concat(&session_id)
                .concat(&RuntimeString::from(
                    " still running — send input with write_stdin, stop with kill_shell]",
                ))
        };
        let mut output = RuntimeObject::new();
        output.insert("exited", RuntimeJson::Bool(r.exited));
        output.insert(
            "exitCode",
            r.exit_code.map_or(RuntimeJson::Null, RuntimeJson::from),
        );
        output.insert(
            "sessionId",
            r.session_id
                .clone()
                .map_or(RuntimeJson::Null, RuntimeJson::String),
        );
        Ok(text_result(
            RuntimeJson::Object(output),
            body.concat(&RuntimeString::from("\n")).concat(&status),
            r.exited && r.exit_code != Some(0),
        ))
    }
}

// ===================== write_stdin =====================

fn write_stdin_input_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "sessionId": {
                "type": "string",
                "description": "The sessionId returned by exec_command"
            },
            "input": {
                "description": "Bytes to send (include your own newline; \"\\u0003\" = Ctrl-C, \"\\u0004\" = EOF). Omit to just poll for more output.",
                "type": "string"
            },
            "yieldMs": {
                "description": "How long to wait for output (default 5000 polling, 10000 when sending input).",
                "type": "integer",
                "exclusiveMinimum": 0,
                "maximum": 9007199254740991_i64
            },
            "cols": {
                "description": "Resize a PTY-backed session to this terminal width in columns before polling or writing input.",
                "type": "integer",
                "minimum": 1,
                "maximum": MAX_TERMINAL_DIMENSION
            },
            "rows": {
                "description": "Resize a PTY-backed session to this terminal height in rows before polling or writing input.",
                "type": "integer",
                "minimum": 1,
                "maximum": MAX_TERMINAL_DIMENSION
            },
            "tailBytes": {
                "description": "Also return a non-consuming snapshot of the latest retained output bytes, useful when re-attaching to a session.",
                "type": "integer",
                "minimum": 1,
                "maximum": 9007199254740991_i64
            }
        },
        "required": ["sessionId"],
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap()
}

/// Send input to (or poll output from) a session started by `exec_command`.
pub struct WriteStdinTool {
    input_schema: JsonSchema,
}

impl WriteStdinTool {
    fn new() -> Self {
        WriteStdinTool {
            input_schema: write_stdin_input_schema(),
        }
    }
}

/// Construct the `write_stdin` tool.
pub fn create_write_stdin_tool() -> Arc<dyn Tool> {
    Arc::new(WriteStdinTool::new())
}

#[async_trait]
impl Tool for WriteStdinTool {
    fn static_name(&self) -> &str {
        "write_stdin"
    }

    fn static_description(&self) -> &str {
        "Send input to, or poll output from, a persistent shell session started by exec_command. Prefer bash in background mode with bash_output and kill_shell for long-running non-interactive commands; do not use write_stdin merely to wait for a build or server. Use write_stdin only when the same live process must receive interactive input or preserve REPL/TTY state. On failure inspect errorClass, retryAllowed, retriesRemaining, sessionBlocked, and attemptPrevented: terminal failures allow zero retries; transient failures allow exactly one recovery call before the engine blocks that sessionId. Never repeat identical failed calls. Send \"\\u0003\" to interrupt or \"\\u0004\" / \"exit\\n\" to end a shell."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn exposure(&self) -> ToolExposure {
        ToolExposure::Deferred
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

    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        let session_id = input
            .get("sessionId")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap_or_default();
        Some(RuntimeString::from("write_stdin ").concat(&first_chars(&session_id, 8)))
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let session_id = input
            .get("sessionId")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap_or_default();
        let resize = if input.get("cols").is_some() || input.get("rows").is_some() {
            Some(TerminalOptions {
                cols: terminal_u16(&input, "cols", 80),
                rows: terminal_u16(&input, "rows", 24),
            })
        } else {
            None
        };
        let r = ctx
            .background_processes
            .shell_sessions()
            .write_stdin_classified(WriteStdinParams {
                session_id: session_id.clone(),
                input: input.get("input").and_then(RuntimeJson::as_str).cloned(),
                yield_ms: input.get("yieldMs").and_then(RuntimeJson::as_i64),
                signal: Some(ctx.signal.clone()),
                resize,
                tail_bytes: input
                    .get("tailBytes")
                    .and_then(RuntimeJson::as_u64)
                    .and_then(|v| usize::try_from(v).ok())
                    .filter(|v| *v > 0),
            })
            .await;
        let r = match r {
            Ok(result) => result,
            Err(failure) => return Ok(write_stdin_error_result(&session_id, failure)),
        };
        let body = if r.output.is_empty() {
            match r.tail.as_ref().filter(|tail| !tail.is_empty()) {
                Some(tail) => RuntimeString::from("(no new output)\n[tail]\n").concat(tail),
                None => RuntimeString::from("(no new output)"),
            }
        } else {
            r.output.clone()
        };
        let mut status = if r.exited {
            RuntimeString::from("[exited, code ")
                .concat(&code_str(r.exit_code))
                .concat(&RuntimeString::from("]"))
        } else {
            let session_id = r
                .session_id
                .clone()
                .unwrap_or_else(|| RuntimeString::from("null"));
            RuntimeString::from("[session ")
                .concat(&session_id)
                .concat(&RuntimeString::from(" still running]"))
        };
        if r.resized {
            if let Some(size) = r.terminal_size {
                status = status.concat(&RuntimeString::from_utf8(&format!(
                    "\n[terminal resized to {}x{}]",
                    size.cols, size.rows
                )));
            }
        }
        let mut output = RuntimeObject::new();
        output.insert("found", RuntimeJson::Bool(true));
        output.insert("exited", RuntimeJson::Bool(r.exited));
        output.insert(
            "exitCode",
            r.exit_code.map_or(RuntimeJson::Null, RuntimeJson::from),
        );
        output.insert(
            "sessionId",
            r.session_id
                .clone()
                .map_or(RuntimeJson::Null, RuntimeJson::String),
        );
        output.insert("terminal", RuntimeJson::Bool(r.terminal));
        output.insert("terminalSize", terminal_size_json(r.terminal_size));
        output.insert("resized", RuntimeJson::Bool(r.resized));
        output.insert(
            "tail",
            r.tail
                .clone()
                .map_or(RuntimeJson::Null, RuntimeJson::String),
        );
        Ok(text_result(
            RuntimeJson::Object(output),
            body.concat(&RuntimeString::from("\n")).concat(&status),
            false,
        ))
    }
}

fn write_stdin_error_result(
    requested_session_id: &RuntimeString,
    failure: WriteStdinFailure,
) -> ToolResult {
    let code = RuntimeString::from(failure.code.as_str());
    let class = RuntimeString::from(failure.code.class().as_str());
    let guidance = match (
        failure.attempt_prevented,
        failure.code.class(),
        failure.retry_allowed,
    ) {
        (true, WriteStdinErrorClass::Terminal, _) => {
            "Blocked by the engine before process I/O because terminal failures have zero retry budget. Do not retry this sessionId."
        }
        (true, WriteStdinErrorClass::Transient, _) => {
            "Blocked by the engine before process I/O because this sessionId exhausted its single transient recovery call. Do not retry it."
        }
        (false, WriteStdinErrorClass::Terminal, _) => {
            "Terminal failure: this session is blocked and the engine will reject further write_stdin calls for this sessionId. Do not retry it. Start a new command; prefer background bash unless interactive input is required."
        }
        (false, WriteStdinErrorClass::Transient, true) => {
            "Transient failure: exactly one recovery call remains for this sessionId. Change the call based on errorCode; do not repeat identical arguments. If recovery fails, stop this session and start a new command."
        }
        (false, WriteStdinErrorClass::Transient, false) => {
            "Transient recovery was exhausted: this session is now blocked and the engine will reject further write_stdin calls for this sessionId. Stop it and start a new command."
        }
    };
    let message = RuntimeString::from("write_stdin failed [")
        .concat(&code)
        .concat(&RuntimeString::from("] for session "))
        .concat(requested_session_id)
        .concat(&RuntimeString::from(": "))
        .concat(&failure.detail)
        .concat(&RuntimeString::from(". "))
        .concat(&RuntimeString::from(guidance));
    let mut output = RuntimeObject::new();
    output.insert("found", RuntimeJson::Bool(failure.found));
    output.insert("exited", RuntimeJson::Bool(failure.exited));
    output.insert(
        "exitCode",
        failure
            .exit_code
            .map_or(RuntimeJson::Null, RuntimeJson::from),
    );
    output.insert(
        "sessionId",
        failure
            .session_id
            .map_or(RuntimeJson::Null, RuntimeJson::String),
    );
    output.insert("terminal", RuntimeJson::Bool(failure.terminal));
    output.insert("terminalSize", terminal_size_json(failure.terminal_size));
    output.insert("resized", RuntimeJson::Bool(failure.resized));
    output.insert(
        "tail",
        failure.tail.map_or(RuntimeJson::Null, RuntimeJson::String),
    );
    output.insert("output", RuntimeJson::String(failure.output.clone()));
    output.insert("errorCode", RuntimeJson::String(code));
    output.insert("errorClass", RuntimeJson::String(class));
    output.insert("retryAllowed", RuntimeJson::Bool(failure.retry_allowed));
    output.insert(
        "retriesRemaining",
        RuntimeJson::from(failure.retries_remaining),
    );
    output.insert("sessionBlocked", RuntimeJson::Bool(failure.session_blocked));
    output.insert(
        "attemptPrevented",
        RuntimeJson::Bool(failure.attempt_prevented),
    );
    output.insert("errorMessage", RuntimeJson::String(message.clone()));
    let model_message = if failure.output.is_empty() {
        message
    } else {
        message
            .concat(&RuntimeString::from("\n[retained output]\n"))
            .concat(&failure.output)
    };
    text_result(RuntimeJson::Object(output), model_message, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_process::{ParentEnvSnapshot, RuntimeEnv};
    use crate::runtime_values::RuntimeNumber;
    use crate::schemas::content::OutputBlock;
    use crate::tools::builtins::test_ctx::make_ctx;
    use crate::types::sandbox::{
        ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec,
    };
    use serde_json::Value;

    /// Whether the pty cases below can run here, printing the reason when they
    /// cannot. A host whose system-wide ptmx ceiling is already taken fails these
    /// on a missing exit code, which says nothing about the tool.
    #[cfg(unix)]
    fn skip_without_a_pty() -> bool {
        if crate::runtime_process::pty_allocatable() {
            return true;
        }
        eprintln!("skipping pty test: this environment cannot allocate a pty");
        false
    }

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

    /// Env-inheriting identity sandbox so `bash`/`cat` resolve on PATH.
    struct TestSandbox;
    impl SandboxBackend for TestSandbox {
        fn kind(&self) -> SandboxKind {
            SandboxKind::Process
        }
        fn is_available(&self) -> bool {
            true
        }
        fn transform(&self, req: &ExecRequest, _policy: &SandboxPolicy) -> SpawnSpec {
            let file = req.argv.first().cloned().unwrap_or_default();
            let args = req.argv.iter().skip(1).cloned().collect();
            let mut env: RuntimeEnv = ParentEnvSnapshot::capture();
            if let Some(extra) = &req.env {
                env.overlay(extra);
            }
            SpawnSpec {
                file,
                args,
                cwd: req.cwd.clone(),
                env,
            }
        }
    }

    fn ctx() -> ToolContext {
        let mut ctx = make_ctx(&std::env::temp_dir().to_string_lossy());
        ctx.sandbox = Arc::new(TestSandbox);
        ctx
    }

    fn ui_string(value: &RuntimeString) -> String {
        String::from_utf8(value.to_node_utf8()).expect("Node UTF-8 UI projection is valid UTF-8")
    }

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

    #[test]
    fn exec_command_metadata_and_schema() {
        let tool = create_exec_command_tool();
        assert_eq!(tool.name(), "exec_command");
        assert_eq!(tool.exposure(), ToolExposure::Deferred);
        assert!(tool
            .static_description()
            .contains("Prefer bash in background mode with bash_output and kill_shell"));
        assert!(tool
            .static_description()
            .contains("only when the same live process must receive later input"));
        let input = rjson!({ "command": "python" });
        assert!(!tool.is_read_only(&input));
        assert!(!tool.is_concurrency_safe(&input));
        assert_eq!(tool.to_command_string(&input), Some(rs("python")));
        assert_eq!(tool.summarize(&input), Some(rs("exec python")));
        let schema = Value::Object(tool.static_input_schema().clone());
        assert_eq!(
            schema,
            json!({
                "type": "object",
                "properties": {
                    "command": {
                        "type": "string",
                        "description": "The command to run. Prefer bash with runInBackground plus bash_output and kill_shell for long-running non-interactive commands. Use exec_command only when the same live process must accept later input or preserve REPL/TTY state; if it stays alive, a sessionId is returned for write_stdin. write_stdin terminal failures allow zero retries; transient failures allow one recovery call before the engine blocks that sessionId."
                    },
                    "yieldMs": {
                        "description": "How long to wait for it to finish before handing back a session (default 10000).",
                        "type": "integer",
                        "exclusiveMinimum": 0,
                        "maximum": 9007199254740991_i64
                    },
                    "cwd": {
                        "description": "Working directory (relative to session cwd).",
                        "type": "string"
                    },
                    "tty": {
                        "description": "Run the command attached to a real pseudo-terminal instead of pipes. Use only for full-screen TUI programs or commands that require a TTY.",
                        "type": "boolean"
                    },
                    "cols": {
                        "description": "Initial terminal width in columns when tty is true (default 80).",
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 10000
                    },
                    "rows": {
                        "description": "Initial terminal height in rows when tty is true (default 24).",
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 10000
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
    async fn exec_and_write_stdin_project_lone_surrogates_at_os_boundaries() {
        let dir = std::env::temp_dir().join(format!(
            "ae-interactive-node-boundary-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut context = ctx();
        context.cwd = RuntimePath::from(dir.to_string_lossy().as_ref());
        let lone = crate::runtime_values::runtime_json::parse(&RuntimeString::from(r#""\ud800""#))
            .unwrap();
        let lone = lone.as_str().unwrap();

        let command = rs("printf node-boundary > ")
            .concat(lone)
            .concat(&rs(".txt"));
        let mut exec_input = RuntimeObject::new();
        exec_input.insert("command", RuntimeJson::String(command));
        exec_input.insert("yieldMs", RuntimeJson::from(1000));
        let exec = create_exec_command_tool()
            .call(RuntimeJson::Object(exec_input), &context, None)
            .await
            .unwrap();
        assert_eq!(exec.serializable.is_error, Some(false));
        assert_eq!(
            std::fs::read_to_string(dir.join("\u{fffd}.txt")).unwrap(),
            "node-boundary"
        );

        let cat = create_exec_command_tool()
            .call(rjson!({ "command": "cat", "yieldMs": 300 }), &context, None)
            .await
            .unwrap();
        let session_id = cat
            .serializable
            .output
            .get("sessionId")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap();
        let mut stdin_input = RuntimeObject::new();
        stdin_input.insert("sessionId", RuntimeJson::String(session_id.clone()));
        stdin_input.insert("input", RuntimeJson::String(lone.concat(&rs("\n"))));
        stdin_input.insert("yieldMs", RuntimeJson::from(1000));
        let sent = create_write_stdin_tool()
            .call(RuntimeJson::Object(stdin_input), &context, None)
            .await
            .unwrap();
        assert!(text_of(&sent).contains('\u{fffd}'));
        assert!(!text_of(&sent).contains("ud800"));
        context.background_processes.kill(&session_id);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn write_stdin_metadata_and_schema() {
        let tool = create_write_stdin_tool();
        assert_eq!(tool.name(), "write_stdin");
        assert_eq!(tool.exposure(), ToolExposure::Deferred);
        assert!(tool
            .static_description()
            .contains("Prefer bash in background mode with bash_output and kill_shell"));
        assert!(tool
            .static_description()
            .contains("only when the same live process must receive interactive input"));
        let input = rjson!({ "sessionId": "abcdefghijkl" });
        assert!(!tool.is_read_only(&input));
        assert!(!tool.is_concurrency_safe(&input));
        assert_eq!(tool.summarize(&input), Some(rs("write_stdin abcdefgh")));
        let schema = Value::Object(tool.static_input_schema().clone());
        assert_eq!(
            schema,
            json!({
                "type": "object",
                "properties": {
                    "sessionId": {
                        "type": "string",
                        "description": "The sessionId returned by exec_command"
                    },
                    "input": {
                        "description": "Bytes to send (include your own newline; \"\\u0003\" = Ctrl-C, \"\\u0004\" = EOF). Omit to just poll for more output.",
                        "type": "string"
                    },
                    "yieldMs": {
                        "description": "How long to wait for output (default 5000 polling, 10000 when sending input).",
                        "type": "integer",
                        "exclusiveMinimum": 0,
                        "maximum": 9007199254740991_i64
                    },
                    "cols": {
                        "description": "Resize a PTY-backed session to this terminal width in columns before polling or writing input.",
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 10000
                    },
                    "rows": {
                        "description": "Resize a PTY-backed session to this terminal height in rows before polling or writing input.",
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 10000
                    },
                    "tailBytes": {
                        "description": "Also return a non-consuming snapshot of the latest retained output bytes, useful when re-attaching to a session.",
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 9007199254740991_i64
                    }
                },
                "required": ["sessionId"],
                "additionalProperties": false
            })
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn exec_command_runs_a_one_shot_command() {
        let res = create_exec_command_tool()
            .call(
                rjson!({ "command": "echo hi", "yieldMs": 2000 }),
                &ctx(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(res.serializable.output.get("exited"), Some(&rjson!(true)));
        assert_eq!(res.serializable.output.get("exitCode"), Some(&rjson!(0)));
        assert_eq!(
            res.serializable.output.get("sessionId"),
            Some(&rjson!(null))
        );
        let text = text_of(&res);
        assert!(text.contains("hi"), "text: {text}");
        assert!(text.contains("[exited, code 0]"), "text: {text}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn exec_command_propagates_a_non_zero_exit_code() {
        let res = create_exec_command_tool()
            .call(
                rjson!({ "command": "exit 3", "yieldMs": 2000 }),
                &ctx(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(res.serializable.output.get("exitCode"), Some(&rjson!(3)));
        assert!(text_of(&res).contains("[exited, code 3]"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn exec_command_can_request_a_pty_transport() {
        if !skip_without_a_pty() {
            return;
        }
        let res = create_exec_command_tool()
            .call(
                rjson!({ "command": "test -t 1 && echo tool-tty", "yieldMs": 2000, "tty": true }),
                &ctx(),
                None,
            )
            .await
            .unwrap();

        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(res.serializable.output.get("exited"), Some(&rjson!(true)));
        assert_eq!(res.serializable.output.get("exitCode"), Some(&rjson!(0)));
        assert!(
            text_of(&res).contains("tool-tty"),
            "text: {}",
            text_of(&res)
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn exec_command_keeps_a_live_session_and_write_stdin_drives_it() {
        let context = ctx();
        let exec = create_exec_command_tool()
            .call(rjson!({ "command": "cat", "yieldMs": 300 }), &context, None)
            .await
            .unwrap();
        assert_eq!(exec.serializable.output.get("exited"), Some(&rjson!(false)));
        let session_id = exec
            .serializable
            .output
            .get("sessionId")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap();
        assert!(text_of(&exec).contains("still running"));
        let running = context.background_processes.snapshot();
        assert_eq!(running.processes.len(), 1);
        assert_eq!(running.processes[0].shell_id, session_id);
        assert_eq!(
            running.processes[0].status,
            crate::schemas::background_process::BackgroundProcessStatus::Running
        );

        let echoed = create_write_stdin_tool()
            .call(
                rjson!({ "sessionId": ui_string(&session_id), "input": "ping\n", "yieldMs": 4000 }),
                &context,
                None,
            )
            .await
            .unwrap();
        assert_eq!(echoed.serializable.output.get("found"), Some(&rjson!(true)));
        assert!(
            text_of(&echoed).contains("ping"),
            "text: {}",
            text_of(&echoed)
        );
        let host_output = context
            .background_processes
            .read_output(&session_id, Some(0), None);
        assert!(host_output.output.contains("ping"));
        // Stop the session through the same registry the Overview controls.
        context.background_processes.kill(&session_id);
        let stopped = context.background_processes.snapshot();
        assert_eq!(
            stopped.processes[0].status,
            crate::schemas::background_process::BackgroundProcessStatus::Stopped
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn yielded_exec_command_completes_in_background_process_lifecycle() {
        let context = ctx();
        let exec = create_exec_command_tool()
            .call(
                rjson!({ "command": "sleep 0.05; printf exec-finished", "yieldMs": 1 }),
                &context,
                None,
            )
            .await
            .unwrap();
        let session_id = exec
            .serializable
            .output
            .get("sessionId")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .expect("yielded exec returns a session id");

        let record = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let record = context
                    .background_processes
                    .snapshot()
                    .processes
                    .into_iter()
                    .find(|record| record.shell_id == session_id)
                    .expect("yielded exec is registered");
                if record.status.is_terminal() {
                    break record;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("yielded exec reaches a terminal lifecycle state");
        assert_eq!(
            record.status,
            crate::schemas::background_process::BackgroundProcessStatus::Completed
        );
        assert_eq!(record.exit_code, Some(0));
        let output = context
            .background_processes
            .read_output(&session_id, Some(0), None);
        assert!(output.output.contains("exec-finished"));
        assert!(output.eof);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn write_stdin_can_resize_a_pty_session_and_return_tail() {
        if !skip_without_a_pty() {
            return;
        }
        let context = ctx();
        let exec = create_exec_command_tool()
            .call(
                rjson!({ "command": "cat", "yieldMs": 300, "tty": true }),
                &context,
                None,
            )
            .await
            .unwrap();
        let session_id = exec
            .serializable
            .output
            .get("sessionId")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap();

        let updated = create_write_stdin_tool()
            .call(
                rjson!({
                    "sessionId": ui_string(&session_id),
                    "input": "tool-tail\n",
                    "yieldMs": 4000,
                    "cols": 100,
                    "rows": 40,
                    "tailBytes": 1024
                }),
                &context,
                None,
            )
            .await
            .unwrap();

        assert_eq!(
            updated.serializable.output.get("found"),
            Some(&rjson!(true))
        );
        assert_eq!(
            updated.serializable.output.get("terminal"),
            Some(&rjson!(true))
        );
        assert_eq!(
            updated.serializable.output.get("resized"),
            Some(&rjson!(true))
        );
        assert_eq!(
            updated.serializable.output.get("terminalSize"),
            Some(&rjson!({ "cols": 100, "rows": 40 }))
        );
        assert!(updated
            .serializable
            .output
            .get("tail")
            .and_then(RuntimeJson::as_str)
            .map(ui_string)
            .unwrap_or_default()
            .contains("tool-tail"));
        assert!(
            text_of(&updated).contains("tool-tail"),
            "text: {}",
            text_of(&updated)
        );
        context.background_processes.kill(&session_id);
    }

    #[tokio::test]
    async fn write_stdin_reports_a_missing_session() {
        let res = create_write_stdin_tool()
            .call(
                rjson!({ "sessionId": "nope", "input": "x\n" }),
                &ctx(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            res.serializable.output,
            rjson!({
                "found": false,
                "exited": true,
                "exitCode": null,
                "sessionId": null,
                "terminal": false,
                "terminalSize": null,
                "resized": false,
                "tail": null,
                "output": "",
                "errorCode": "session_not_found",
                "errorClass": "terminal",
                "retryAllowed": false,
                "retriesRemaining": 0,
                "sessionBlocked": true,
                "attemptPrevented": true,
                "errorMessage": "write_stdin failed [session_not_found] for session nope: the session does not exist or has been evicted. Blocked by the engine before process I/O because terminal failures have zero retry budget. Do not retry this sessionId."
            })
        );
        assert_eq!(
            text_of(&res),
            "write_stdin failed [session_not_found] for session nope: the session does not exist or has been evicted. Blocked by the engine before process I/O because terminal failures have zero retry budget. Do not retry this sessionId."
        );

        let prevented = create_write_stdin_tool()
            .call(
                rjson!({ "sessionId": "nope", "input": "x\n" }),
                &ctx(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            prevented.serializable.output.get("attemptPrevented"),
            Some(&rjson!(true))
        );
        assert_eq!(
            prevented.serializable.output.get("sessionBlocked"),
            Some(&rjson!(true))
        );
    }
}

#[cfg(test)]
mod exact_tests {
    use super::*;

    #[test]
    fn summaries_slice_exact_utf16() {
        let mut input = RuntimeObject::new();
        input.insert(
            "command",
            RuntimeJson::String(RuntimeString::from_utf16(vec![0xd83d, 0xde00, b'x' as u16])),
        );
        let tool = ExecCommandTool::new(InteractiveShellOptions::default());
        assert!(tool
            .summarize(&RuntimeJson::Object(input))
            .expect("summary")
            .starts_with("exec "));
    }
}
