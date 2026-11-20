//! `TaskOutput` / `TaskStop` — the unified background-task control surface.
//!
//! One reader and one stopper cover every kind of background work a session
//! owns: sub-agents started with `run_in_background` (via `ctx.background_tasks`)
//! and background shell commands started by `bash` (via
//! `ctx.background_processes`). Targets resolve by exact id, by bound agent
//! name, or by unambiguous id/name prefix; the legacy per-kind tool names
//! (`task_output`, `bash_output`, `task_stop`, `kill_shell`, `kill_bash`)
//! remain accepted as aliases so recorded history and habitual calls keep
//! working.
//!
//! Neither tool waits. Waiting for background work is the `Yield` tool's job;
//! completion results also reach the model by themselves through the
//! background-notice rail. `TaskOutput` answers a bounded number of repeat
//! reads of the same task within one assistant reply so a lost track costs one
//! check, not a polling loop.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use crate::runtime_values::{NodeBufferUtf8, RuntimeJson, RuntimeObject, RuntimeString};
use crate::tools::build_tool::text_result;
use crate::tools::shell::Signal;
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{BackgroundTaskStatus, Tool, ToolContext, ToolProgress, ToolResult};

use super::shell_common::first_chars;

/// How many times one assistant reply may re-read the same still-running task
/// before the reader refuses to answer the repetition again.
const READ_ANSWER_BUDGET: u32 = 2;

/// The wire form of a background sub-agent status.
fn status_str(status: BackgroundTaskStatus) -> &'static str {
    match status {
        BackgroundTaskStatus::Running => "running",
        BackgroundTaskStatus::Completed => "completed",
        BackgroundTaskStatus::Failed => "failed",
        BackgroundTaskStatus::Interrupted => "interrupted",
    }
}

/// The wire form of a background shell status.
fn shell_status_str(
    status: crate::tools::background_shells::BackgroundShellStatus,
) -> &'static str {
    use crate::tools::background_shells::BackgroundShellStatus;
    match status {
        BackgroundShellStatus::Running => "running",
        BackgroundShellStatus::Exited => "exited",
        BackgroundShellStatus::Killed => "killed",
    }
}

fn runtime_object(fields: impl IntoIterator<Item = (&'static str, RuntimeJson)>) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    for (key, value) in fields {
        object.insert(key, value);
    }
    RuntimeJson::Object(object)
}

/// The first accepted identifier field: `task_id`, then the legacy
/// `shell_id` / `shellId` / `taskId` spellings.
fn target_id(input: &Json) -> Option<RuntimeString> {
    ["task_id", "shell_id", "shellId", "taskId"]
        .iter()
        .find_map(|key| {
            input
                .get(*key)
                .and_then(RuntimeJson::as_str)
                .filter(|value| !value.is_empty())
                .cloned()
        })
}

fn parse_signal(signal: Option<&RuntimeString>) -> Result<Signal, RuntimeString> {
    let signal = signal
        .cloned()
        .unwrap_or_else(|| RuntimeString::from("SIGTERM"));
    if signal == "SIGTERM" {
        Ok(Signal::Term)
    } else if signal == "SIGKILL" {
        Ok(Signal::Kill)
    } else {
        Err(RuntimeString::from("Unsupported signal ")
            .concat(&signal)
            .concat(&RuntimeString::from(". Use SIGTERM or SIGKILL.")))
    }
}

fn signal_label(signal: Signal) -> &'static str {
    match signal {
        Signal::Term => "SIGTERM",
        Signal::Kill => "SIGKILL",
        Signal::Int => "SIGINT",
        Signal::Hup => "SIGHUP",
        Signal::Quit => "SIGQUIT",
        Signal::Usr1 => "SIGUSR1",
        Signal::Usr2 => "SIGUSR2",
    }
}

// ---- shell output line filtering --------------------------------------------

fn split_lines(value: &RuntimeString) -> Vec<RuntimeString> {
    let mut lines = Vec::new();
    let mut start = 0;
    for (index, unit) in value.as_utf16().iter().copied().enumerate() {
        if unit == u16::from(b'\n') {
            lines.push(RuntimeString::from_utf16(
                value.as_utf16()[start..index].to_vec(),
            ));
            start = index + 1;
        }
    }
    lines.push(RuntimeString::from_utf16(
        value.as_utf16()[start..].to_vec(),
    ));
    lines
}

fn join_lines(lines: &[RuntimeString]) -> RuntimeString {
    let mut joined = RuntimeString::default();
    for (index, line) in lines.iter().enumerate() {
        if index > 0 {
            joined = joined.concat(&RuntimeString::from("\n"));
        }
        joined = joined.concat(line);
    }
    joined
}

fn v8_string(scope: &v8::PinScope<'_, '_>, value: v8::Local<'_, v8::String>) -> RuntimeString {
    let mut units = vec![0_u16; value.length()];
    value.write_v2(scope, 0, &mut units, v8::WriteFlags::default());
    RuntimeString::from_utf16(units)
}

fn filter_lines_exact(
    output: &RuntimeString,
    pattern: &RuntimeString,
) -> Result<RuntimeString, RuntimeString> {
    crate::v8_platform::initialize_v8().map_err(RuntimeString::from)?;
    // The filter pattern is model-authored: cap the heap so a pathological
    // regex or subject cannot pressure the shared engine process.
    let (mut heap_isolate, heap_watch) =
        crate::v8_platform::heap_limited_isolate(crate::v8_platform::AUX_SCRIPT_HEAP_LIMIT_BYTES);
    let isolate = heap_isolate.as_mut();
    v8::scope!(let scope, isolate);
    let context = v8::Context::new(scope, Default::default());
    let scope = &mut v8::ContextScope::new(scope, context);
    let tc = std::pin::pin!(v8::TryCatch::new(scope));
    let tc = tc.init();
    let pattern = v8::String::new_from_two_byte(&tc, pattern.as_utf16(), v8::NewStringType::Normal)
        .ok_or_else(|| RuntimeString::from("failed to allocate filter regex"))?;
    let regex =
        v8::RegExp::new(&tc, pattern, v8::RegExpCreationFlags::empty()).ok_or_else(|| {
            tc.exception()
                .and_then(|value| value.to_string(&tc))
                .map(|value| v8_string(&tc, value))
                .unwrap_or_else(|| RuntimeString::from("invalid filter regex"))
        })?;
    let mut retained = Vec::new();
    for line in split_lines(output) {
        let subject =
            v8::String::new_from_two_byte(&tc, line.as_utf16(), v8::NewStringType::Normal)
                .ok_or_else(|| RuntimeString::from("failed to allocate filter subject"))?;
        // `RegExp::exec` surfaces the JavaScript result object: on "no match"
        // it yields the JS `null` VALUE (not an empty handle), so the presence
        // of a handle alone must not be read as a match.
        let matched = regex.exec(&tc, subject);
        if matched.is_none() {
            if let Some(message) = heap_watch.error_text() {
                return Err(message);
            }
            if tc.has_terminated() {
                return Err(RuntimeString::from("filter execution was terminated"));
            }
        }
        if matched.is_some_and(|result| !result.is_null_or_undefined()) {
            retained.push(line);
        }
    }
    Ok(join_lines(&retained))
}

// ---- TaskOutput ---------------------------------------------------------------

fn task_output_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "task_id": {
                "type": "string",
                "description": "The id of the background task to read: the task id of a sub-agent started in the background, or the shell id returned by a background command. A named sub-agent can also be read by its name."
            },
            "shell_id": {
                "type": "string",
                "description": "Deprecated alias for task_id"
            },
            "shellId": {
                "type": "string",
                "description": "Deprecated alias for task_id"
            },
            "filter": {
                "type": "string",
                "description": "Optional JS regex; only shell output lines matching it are returned"
            },
            "block": {
                "type": "boolean",
                "description": "Deprecated and ignored: this tool never waits. Call Yield to wait for background work."
            },
            "timeout": {
                "type": "number",
                "description": "Deprecated and ignored: this tool never waits. Call Yield to wait for background work."
            },
            "yieldMs": {
                "type": "integer",
                "description": "Deprecated and ignored: this tool never waits. Call Yield to wait for background work."
            }
        },
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap_or_default()
}

const TASK_OUTPUT_DESCRIPTION: &str = "Returns the current state of a background task — a shell command that kept running in the background, or a sub-agent started with run_in_background — whether it is still running or has since finished.

- status: \"running\" (still working; the output so far is returned), \"completed\", \"failed\", \"interrupted\", or for shell commands \"exited\" / \"killed\"
- Results also reach you by themselves when a background task finishes, unless the message that started it said otherwise, so you don't need to check on it. Use this tool to read a finished task's result again, or to check once on a task you have lost track of — never to wait: while a task runs, calling this tool only returns its state so far, and when it finishes its result reaches you without a call. To wait for background work, call Yield.";

/// Read the state and output of any background task (sub-agent or shell).
pub struct TaskOutputTool {
    input_schema: JsonSchema,
}

/// Construct the `TaskOutput` tool as an erased registry-side [`Tool`].
pub fn create_task_output_tool() -> Arc<dyn Tool> {
    Arc::new(TaskOutputTool {
        input_schema: task_output_schema(),
    })
}

#[async_trait]
impl Tool for TaskOutputTool {
    fn static_name(&self) -> &str {
        "TaskOutput"
    }

    fn aliases(&self) -> Vec<RuntimeString> {
        vec![
            RuntimeString::from("task_output"),
            RuntimeString::from("bash_output"),
        ]
    }

    fn static_description(&self) -> &str {
        TASK_OUTPUT_DESCRIPTION
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn static_search_hint(&self) -> Option<&str> {
        Some("retrieve a background task result")
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

    fn input_schema_property_order(
        &self,
        object_path: &[String],
    ) -> Option<&'static [&'static str]> {
        object_path.is_empty().then_some(&[
            "task_id", "shell_id", "shellId", "filter", "block", "timeout", "yieldMs",
        ])
    }

    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        let task_id = target_id(input).unwrap_or_default();
        Some(RuntimeString::from("Reading background task ").concat(&first_chars(&task_id, 8)))
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let Some(task_id) = target_id(&input) else {
            return Ok(text_result(
                runtime_object([
                    ("task_id", RuntimeJson::String(RuntimeString::default())),
                    ("status", RuntimeJson::from("not_found")),
                    ("output", RuntimeJson::String(RuntimeString::default())),
                    ("retrieval_status", RuntimeJson::from("not_ready")),
                ]),
                "A task_id is required.",
                true,
            ));
        };

        let reg = ctx.background_tasks.as_ref();
        let mut handle = reg.and_then(|r| r.get(&task_id));
        // A bound name or an unambiguous prefix addresses the same sub-agent.
        if handle.is_none() {
            if let Some(registry) = reg {
                match registry.resolve_target(&task_id) {
                    Some(Ok(agent_id)) => handle = registry.get(&agent_id),
                    Some(Err(message)) => {
                        return Ok(text_result(
                            runtime_object([
                                ("task_id", RuntimeJson::String(task_id)),
                                ("status", RuntimeJson::from("not_found")),
                                ("output", RuntimeJson::String(RuntimeString::default())),
                                ("retrieval_status", RuntimeJson::from("not_ready")),
                            ]),
                            message,
                            true,
                        ));
                    }
                    None => {}
                }
            }
        }

        if let Some(handle) = handle {
            return self.read_agent(handle, ctx, &task_id).await;
        }

        // Shell commands live in the process registry, not the agent registry.
        // A zero-window read consumes the new output and marks a finished
        // shell's output as collected, exactly like the legacy shell reader.
        let shell = ctx
            .background_processes
            .read_new_wait(&task_id, 0, &ctx.signal, None)
            .await
            .map_err(ToolError::new)?;
        if shell.found {
            return self.read_shell(shell, &input, &task_id);
        }
        // A finished shell whose final output was already collected still has
        // an honest terminal state to report.
        if let Some(entry) = ctx
            .background_processes
            .list()
            .into_iter()
            .find(|entry| entry.id == task_id)
        {
            let status = shell_status_str(entry.status);
            let text = RuntimeString::from("Task ")
                .concat(&task_id)
                .concat(&RuntimeString::from(" finished ("))
                .concat(&RuntimeString::from(status.to_string()))
                .concat(&match entry.exit_code {
                    Some(code) => RuntimeString::from_utf8(&format!(", exit code {code}")),
                    None => RuntimeString::default(),
                })
                .concat(&RuntimeString::from(
                    "); its final output was already collected by an earlier read.",
                ));
            return Ok(text_result(
                runtime_object([
                    ("task_id", RuntimeJson::String(task_id)),
                    ("task_type", RuntimeJson::from("shell")),
                    ("status", RuntimeJson::from(status.to_string())),
                    (
                        "exitCode",
                        entry.exit_code.map_or(RuntimeJson::Null, RuntimeJson::from),
                    ),
                    ("output", RuntimeJson::String(RuntimeString::default())),
                    ("retrieval_status", RuntimeJson::from("already_collected")),
                ]),
                text,
                false,
            ));
        }

        let text = RuntimeString::from("No background task with id ")
            .concat(&task_id)
            .concat(&RuntimeString::from(
                ". Ids come from the result that started the background work; a named sub-agent can also be read by its name.",
            ));
        Ok(text_result(
            runtime_object([
                ("task_id", RuntimeJson::String(task_id)),
                ("status", RuntimeJson::from("not_found")),
                ("output", RuntimeJson::String(RuntimeString::default())),
                ("retrieval_status", RuntimeJson::from("not_ready")),
            ]),
            text,
            true,
        ))
    }
}

impl TaskOutputTool {
    /// Bounded re-answers: reading the same still-running task more than the
    /// budget within one reply is refused so the reader never degenerates into
    /// a polling loop.
    fn over_read_budget(&self, ctx: &ToolContext, task_id: &RuntimeString) -> bool {
        ctx.reply_gate.as_ref().is_some_and(|gate| {
            gate.count_task_read(&task_id.try_to_utf8().unwrap_or_default()) > READ_ANSWER_BUDGET
        })
    }

    fn budget_message(&self, task_id: &RuntimeString, status: &str) -> ToolResult {
        let message = RuntimeString::from("TaskOutput was already answered ")
            .concat(&RuntimeString::from_utf8(&READ_ANSWER_BUDGET.to_string()))
            .concat(&RuntimeString::from(" times for task "))
            .concat(task_id)
            .concat(&RuntimeString::from(" in this response (status: "))
            .concat(&RuntimeString::from(status.to_string()))
            .concat(&RuntimeString::from(
                "); the same call repeated within one response is not answered again. To wait for background work, call Yield.",
            ));
        text_result(
            runtime_object([
                ("task_id", RuntimeJson::String(task_id.clone())),
                ("status", RuntimeJson::from(status.to_string())),
                ("output", RuntimeJson::String(RuntimeString::default())),
                ("retrieval_status", RuntimeJson::from("repeat_call")),
            ]),
            message,
            true,
        )
    }

    async fn read_agent(
        &self,
        handle: crate::types::tool::BackgroundTaskHandle,
        ctx: &ToolContext,
        requested_id: &RuntimeString,
    ) -> Result<ToolResult, ToolError> {
        // Still running: the transcript accumulated so far shows progress
        // instead of empty, keeping the honest status. Waiting is Yield's job.
        if handle.status == BackgroundTaskStatus::Running {
            if self.over_read_budget(ctx, requested_id) {
                return Ok(self.budget_message(requested_id, "running"));
            }
            let partial = handle.partial.clone().unwrap_or_default();
            let text = if !partial.is_empty() {
                RuntimeString::from("Task ")
                    .concat(&handle.agent_id)
                    .concat(&RuntimeString::from(
                        " still running; partial output above. To wait for background work, call Yield; you are also notified when it completes.",
                    ))
            } else {
                RuntimeString::from("Task ")
                    .concat(&handle.agent_id)
                    .concat(&RuntimeString::from(
                        " has not finished yet. To wait for background work, call Yield; you are also notified when it completes.",
                    ))
            };
            return Ok(text_result(
                runtime_object([
                    ("task_id", RuntimeJson::String(handle.agent_id.clone())),
                    ("task_type", RuntimeJson::from("agent")),
                    ("status", RuntimeJson::from(status_str(handle.status))),
                    ("output", RuntimeJson::String(partial)),
                    ("retrieval_status", RuntimeJson::from("not_ready")),
                ]),
                text,
                false,
            ));
        }

        // Cancellation and recovery both leave incomplete work, never a final answer.
        if handle.status == BackgroundTaskStatus::Interrupted {
            let partial = handle.partial.clone().unwrap_or_default();
            let text = if !partial.is_empty() {
                RuntimeString::from("Task ")
                    .concat(&handle.agent_id)
                    .concat(&RuntimeString::from(
                        " was interrupted and never produced a final answer. The available partial activity is not a completed result. Resume or re-spawn the task if the work is still needed.",
                    ))
            } else {
                RuntimeString::from("Task ")
                    .concat(&handle.agent_id)
                    .concat(&RuntimeString::from(
                        " was interrupted and left no output. Resume or re-spawn the task if the work is still needed.",
                    ))
            };
            return Ok(text_result(
                runtime_object([
                    ("task_id", RuntimeJson::String(handle.agent_id.clone())),
                    ("task_type", RuntimeJson::from("agent")),
                    ("status", RuntimeJson::from(status_str(handle.status))),
                    ("output", RuntimeJson::String(partial)),
                    ("retrieval_status", RuntimeJson::from("interrupted")),
                ]),
                text,
                true,
            ));
        }

        // The clean final answer (output file) is written only on completion.
        let from_file = match crate::fs::node_path::project_runtime_path(
            &handle.output_file,
            "background task output file",
        ) {
            Ok(path) => tokio::fs::read(path)
                .await
                .map(|bytes| NodeBufferUtf8::decode(&bytes))
                .unwrap_or_default(),
            Err(_) => RuntimeString::default(),
        };
        // That file lives in the OS temp directory, which is not durable: it can
        // be cleared between the restart and this read. The agent's transcript
        // lives under the session directory and is, so fall back to the work
        // projected from it rather than reporting a successful read of nothing.
        // Naming the fallback matters: a transcript is the activity log, not the
        // clean final answer, and claiming `success` for it would be a lie.
        let partial = handle.partial.clone().unwrap_or_default();
        let failed = handle.status == BackgroundTaskStatus::Failed;
        let (output, retrieval) = if failed {
            (
                if from_file.is_empty() {
                    partial
                } else {
                    from_file
                },
                "failed",
            )
        } else if !from_file.is_empty() {
            (from_file, "success")
        } else if !partial.is_empty() {
            (partial, "transcript_only")
        } else {
            (RuntimeString::default(), "unavailable")
        };
        let text = match retrieval {
            "failed" => RuntimeString::from("Task ")
                .concat(&handle.agent_id)
                .concat(&RuntimeString::from(" failed; any available output is incomplete activity, not a final answer.\n"))
                .concat(&output),
            "success" => output.clone(),
            "transcript_only" => RuntimeString::from("Task ")
                .concat(&handle.agent_id)
                .concat(&RuntimeString::from(" "))
                .concat(&RuntimeString::from(status_str(handle.status)))
                .concat(&RuntimeString::from(
                    ", but its result file is gone, so the transcript it left behind is returned instead. That is its activity log, not its clean final answer.",
                )),
            _ => RuntimeString::from("Task ")
                .concat(&handle.agent_id)
                .concat(&RuntimeString::from(" "))
                .concat(&RuntimeString::from(status_str(handle.status)))
                .concat(&RuntimeString::from(" with no output.")),
        };
        Ok(text_result(
            runtime_object([
                ("task_id", RuntimeJson::String(handle.agent_id)),
                ("task_type", RuntimeJson::from("agent")),
                ("status", RuntimeJson::from(status_str(handle.status))),
                ("output", RuntimeJson::String(output)),
                ("retrieval_status", RuntimeJson::from(retrieval)),
            ]),
            text,
            failed,
        ))
    }

    fn read_shell(
        &self,
        res: crate::tools::background_shells::ReadNewResult,
        input: &Json,
        requested_id: &RuntimeString,
    ) -> Result<ToolResult, ToolError> {
        let mut output = res.output.clone();
        let mut filter_error: Option<RuntimeString> = None;
        let filter = input.get("filter").and_then(RuntimeJson::as_str);
        if let Some(filter) = filter {
            if !output.is_empty() {
                match filter_lines_exact(&output, filter) {
                    Ok(filtered) => output = filtered,
                    Err(error) => {
                        filter_error = Some(
                            RuntimeString::from("\n[invalid filter regex: ")
                                .concat(&error)
                                .concat(&RuntimeString::from("]")),
                        );
                    }
                }
            }
        }

        let status_line = RuntimeString::from("[status: ")
            .concat(&RuntimeString::from(shell_status_str(res.status)))
            .concat(&match res.exit_code {
                Some(code) => RuntimeString::from_utf8(&format!(", exit code {code}")),
                None => RuntimeString::default(),
            })
            .concat(&RuntimeString::from("]"));
        let body = if output.is_empty() {
            RuntimeString::from("(no new output)")
        } else {
            output
        };
        let text = body
            .concat(&RuntimeString::from("\n"))
            .concat(&status_line)
            .concat(&filter_error.unwrap_or_default());

        let mut result = RuntimeObject::new();
        result.insert("found", RuntimeJson::Bool(true));
        result.insert("task_id", RuntimeJson::String(requested_id.clone()));
        result.insert("task_type", RuntimeJson::from("shell"));
        result.insert("status", RuntimeJson::from(shell_status_str(res.status)));
        result.insert(
            "shellId",
            if res.status == crate::tools::background_shells::BackgroundShellStatus::Running {
                RuntimeJson::String(requested_id.clone())
            } else {
                RuntimeJson::Null
            },
        );
        result.insert(
            "exitCode",
            res.exit_code.map_or(RuntimeJson::Null, RuntimeJson::from),
        );
        result.insert("hasNewOutput", RuntimeJson::Bool(!res.output.is_empty()));

        Ok(text_result(RuntimeJson::Object(result), text, false))
    }
}

// ---- TaskStop -------------------------------------------------------------------

fn task_stop_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "task_id": {
                "type": "string",
                "description": "The id of the background task to stop: the task id of a running sub-agent, or the shell id of a background command. To stop an agent-team teammate, pass its agent id or bare teammate name; to stop a background agent spawned with a name, pass that name."
            },
            "shell_id": {
                "type": "string",
                "description": "Deprecated alias for task_id"
            },
            "shellId": {
                "type": "string",
                "description": "Deprecated alias for task_id"
            },
            "signal": {
                "type": "string",
                "description": "Signal used to stop a background command: SIGTERM (default) or SIGKILL"
            }
        },
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap_or_default()
}

const TASK_STOP_DESCRIPTION: &str = "- Stops a running background task by its ID: a background shell command or a background sub-agent
- Takes a task_id parameter identifying the task to stop
- To stop an agent-team teammate, pass its agent ID or bare teammate name as task_id
- To stop a background agent spawned with a name, pass that name as task_id
- Returns a success or failure status
- Use this tool when you need to terminate a long-running task";

/// Stop any running background task (sub-agent or shell) by id or name.
pub struct TaskStopTool {
    input_schema: JsonSchema,
}

/// Construct the `TaskStop` tool as an erased registry-side [`Tool`].
pub fn create_task_stop_tool() -> Arc<dyn Tool> {
    Arc::new(TaskStopTool {
        input_schema: task_stop_schema(),
    })
}

#[async_trait]
impl Tool for TaskStopTool {
    fn static_name(&self) -> &str {
        "TaskStop"
    }

    fn aliases(&self) -> Vec<RuntimeString> {
        vec![
            RuntimeString::from("task_stop"),
            RuntimeString::from("kill_shell"),
            RuntimeString::from("kill_bash"),
            RuntimeString::from("KillShell"),
            RuntimeString::from("KillBash"),
        ]
    }

    fn static_description(&self) -> &str {
        TASK_STOP_DESCRIPTION
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn static_search_hint(&self) -> Option<&str> {
        Some("terminate a running background task")
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        false
    }

    fn input_schema_property_order(
        &self,
        object_path: &[String],
    ) -> Option<&'static [&'static str]> {
        object_path
            .is_empty()
            .then_some(&["task_id", "shell_id", "shellId", "signal"])
    }

    fn summarize(&self, input: &Json) -> Option<RuntimeString> {
        let task_id = target_id(input).unwrap_or_default();
        Some(RuntimeString::from("Stopping background task ").concat(&first_chars(&task_id, 8)))
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let Some(id) = target_id(&input) else {
            return Ok(text_result(
                runtime_object([
                    ("message", RuntimeJson::from("A task_id is required.")),
                    ("task_id", RuntimeJson::String(RuntimeString::default())),
                    ("task_type", RuntimeJson::String(RuntimeString::default())),
                ]),
                "A task_id is required.",
                true,
            ));
        };

        // 1) An exact sub-agent id.
        let reg = ctx.background_tasks.as_ref();
        if let Some(handle) = reg.and_then(|r| r.get(&id)) {
            return Ok(self.stop_agent(reg, handle, &id));
        }

        // 2) A background shell command id.
        let known_shell = ctx
            .background_processes
            .list()
            .into_iter()
            .find(|entry| entry.id == id);
        if let Some(entry) = known_shell {
            return self.stop_shell(ctx, &input, &id, entry.command).await;
        }

        // 3) A bound agent/teammate name or an unambiguous prefix.
        if let Some(registry) = reg {
            match registry.resolve_target(&id) {
                Some(Ok(agent_id)) => {
                    if let Some(handle) = registry.get(&agent_id) {
                        return Ok(self.stop_agent(reg, handle, &agent_id));
                    }
                }
                Some(Err(message)) => {
                    return Ok(text_result(
                        runtime_object([
                            ("message", RuntimeJson::String(message.clone())),
                            ("task_id", RuntimeJson::String(id)),
                            ("task_type", RuntimeJson::String(RuntimeString::default())),
                        ]),
                        message,
                        true,
                    ));
                }
                None => {}
            }
        }

        let message = RuntimeString::from("No background task with id ")
            .concat(&id)
            .concat(&RuntimeString::from("."));
        Ok(text_result(
            runtime_object([
                ("message", RuntimeJson::String(message.clone())),
                ("task_id", RuntimeJson::String(id)),
                ("task_type", RuntimeJson::String(RuntimeString::default())),
            ]),
            message,
            true,
        ))
    }
}

impl TaskStopTool {
    fn stop_agent(
        &self,
        reg: Option<&Arc<dyn crate::types::tool::BackgroundTaskRegistry>>,
        handle: crate::types::tool::BackgroundTaskHandle,
        id: &RuntimeString,
    ) -> ToolResult {
        // A persistent mailbox can still be live after its latest run failed.
        // The registry owns lifetime liveness, so its stop result is authoritative.
        if !reg.is_some_and(|registry| registry.stop(id)) {
            let msg = RuntimeString::from("Task ")
                .concat(id)
                .concat(&RuntimeString::from(" is not running (status: "))
                .concat(&RuntimeString::from(status_str(handle.status)))
                .concat(&RuntimeString::from(")."));
            return text_result(
                runtime_object([
                    ("message", RuntimeJson::String(msg.clone())),
                    ("task_id", RuntimeJson::String(id.clone())),
                    ("task_type", RuntimeJson::from("agent")),
                ]),
                msg,
                true,
            );
        }
        let message = RuntimeString::from("Successfully stopped task: ").concat(id);
        let mut output = runtime_object([
            ("message", RuntimeJson::String(message.clone())),
            ("task_id", RuntimeJson::String(id.clone())),
            ("task_type", RuntimeJson::from("agent")),
        ]);
        if let Some(desc) = handle.description.as_ref() {
            if !desc.is_empty() {
                output
                    .as_object_mut()
                    .unwrap()
                    .insert("command", RuntimeJson::String(desc.clone()));
            }
        }
        text_result(output, message, false)
    }

    async fn stop_shell(
        &self,
        ctx: &ToolContext,
        input: &Json,
        id: &RuntimeString,
        command: RuntimeString,
    ) -> Result<ToolResult, ToolError> {
        let signal = match parse_signal(input.get("signal").and_then(RuntimeJson::as_str)) {
            Ok(signal) => signal,
            Err(message) => {
                return Ok(text_result(
                    runtime_object([
                        ("message", RuntimeJson::String(message.clone())),
                        ("task_id", RuntimeJson::String(id.clone())),
                        ("task_type", RuntimeJson::from("shell")),
                    ]),
                    message,
                    true,
                ));
            }
        };
        let killed = ctx
            .background_processes
            .stop_with_signal_and_wait(id, signal)
            .await
            .map_err(ToolError::new)?
            .accepted;
        if !killed {
            let message = RuntimeString::from("Task ")
                .concat(id)
                .concat(&RuntimeString::from(" is not running."));
            return Ok(text_result(
                runtime_object([
                    ("message", RuntimeJson::String(message.clone())),
                    ("task_id", RuntimeJson::String(id.clone())),
                    ("task_type", RuntimeJson::from("shell")),
                ]),
                message,
                true,
            ));
        }
        let message = RuntimeString::from("Successfully stopped task: ")
            .concat(id)
            .concat(&RuntimeString::from(" ("))
            .concat(&command)
            .concat(&RuntimeString::from(") with "))
            .concat(&RuntimeString::from(signal_label(signal)));
        Ok(text_result(
            runtime_object([
                ("message", RuntimeJson::String(message.clone())),
                ("task_id", RuntimeJson::String(id.clone())),
                ("task_type", RuntimeJson::from("shell")),
                ("command", RuntimeJson::String(command)),
            ]),
            message,
            false,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::{runtime_json, RuntimePath};
    use crate::tools::builtins::test_ctx::make_ctx;
    use crate::types::tool::{BackgroundTaskHandle, ReplyScopedGate};
    use std::sync::Mutex;

    macro_rules! rjson {
        ($($json:tt)+) => {{
            runtime_json::parse(&RuntimeString::from(stringify!($($json)+)))
                .expect("test JSON literal must parse as exact runtime JSON")
        }};
    }

    fn text_of(res: &ToolResult) -> RuntimeString {
        match &res.serializable.model_content[0] {
            crate::schemas::content::OutputBlock::Text(t) => t.text.clone(),
            _ => panic!("expected a text block"),
        }
    }

    fn is_err(res: &ToolResult) -> bool {
        res.serializable.is_error == Some(true)
    }

    fn utf8(value: &RuntimeString) -> String {
        value.try_to_utf8().unwrap_or_default()
    }

    fn handle(id: &str, status: BackgroundTaskStatus, partial: &str) -> BackgroundTaskHandle {
        BackgroundTaskHandle {
            agent_id: RuntimeString::from(id),
            output_file: RuntimePath::from(format!("/tmp/klaus-test-{id}.out")),
            status,
            description: Some(RuntimeString::from("stub task")),
            partial: Some(RuntimeString::from(partial)),
        }
    }

    /// A registry over a fixed set of handles with name resolution and stop recording.
    struct StubRegistry {
        handles: Vec<BackgroundTaskHandle>,
        stopped: Mutex<Vec<RuntimeString>>,
        names: Vec<(String, String)>,
        ambiguous: Vec<String>,
    }

    impl StubRegistry {
        fn new(handles: Vec<BackgroundTaskHandle>) -> Self {
            StubRegistry {
                handles,
                stopped: Mutex::new(Vec::new()),
                names: Vec::new(),
                ambiguous: Vec::new(),
            }
        }
    }

    #[async_trait]
    impl crate::types::tool::BackgroundTaskRegistry for StubRegistry {
        fn get(&self, agent_id: &RuntimeString) -> Option<BackgroundTaskHandle> {
            self.handles
                .iter()
                .find(|h| &h.agent_id == agent_id)
                .cloned()
        }
        fn list(&self) -> Vec<BackgroundTaskHandle> {
            self.handles.clone()
        }
        fn resolve_target(
            &self,
            target: &RuntimeString,
        ) -> Option<Result<RuntimeString, RuntimeString>> {
            let key = target.try_to_utf8().unwrap_or_default();
            if self.ambiguous.contains(&key) {
                return Some(Err(RuntimeString::from(
                    "Ambiguous sub-agent target; use an unambiguous agent ID.",
                )));
            }
            self.names
                .iter()
                .find(|(name, _)| *name == key)
                .map(|(_, id)| Ok(RuntimeString::from(id.as_str())))
        }
        fn stop(&self, agent_id: &RuntimeString) -> bool {
            if !self
                .get(agent_id)
                .is_some_and(|handle| handle.status == BackgroundTaskStatus::Running)
            {
                return false;
            }
            self.stopped.lock().unwrap().push(agent_id.clone());
            true
        }
        async fn wait_for_idle(&self) {}
    }

    fn ctx_with(registry: StubRegistry) -> ToolContext {
        let mut ctx = make_ctx("/tmp");
        ctx.background_tasks = Some(Arc::new(registry));
        ctx.reply_gate = Some(Arc::new(ReplyScopedGate::default()));
        if let Some(gate) = &ctx.reply_gate {
            gate.begin_reply("reply-1".to_string());
        }
        ctx
    }

    #[test]
    fn unified_names_and_legacy_aliases() {
        let output = create_task_output_tool();
        assert_eq!(output.name(), "TaskOutput");
        let aliases = output.aliases();
        assert!(aliases.contains(&RuntimeString::from("task_output")));
        assert!(aliases.contains(&RuntimeString::from("bash_output")));
        let stop = create_task_stop_tool();
        assert_eq!(stop.name(), "TaskStop");
        let aliases = stop.aliases();
        for legacy in [
            "task_stop",
            "kill_shell",
            "kill_bash",
            "KillShell",
            "KillBash",
        ] {
            assert!(
                aliases.contains(&RuntimeString::from(legacy)),
                "missing alias {legacy}"
            );
        }
    }

    #[tokio::test]
    async fn output_reads_running_agent_snapshot_without_waiting() {
        let ctx = ctx_with(StubRegistry::new(vec![handle(
            "agent-1",
            BackgroundTaskStatus::Running,
            "partial work",
        )]));
        let res = create_task_output_tool()
            .call(
                rjson!({"task_id":"agent-1","block":true,"timeout":60000}),
                &ctx,
                None,
            )
            .await
            .expect("call succeeds");
        let text = text_of(&res);
        assert!(text.contains("still running"), "{}", utf8(&text));
        assert!(text.contains("call Yield"), "{}", utf8(&text));
        assert!(
            res.serializable.output.to_string().contains("partial work"),
            "structured output carries the partial transcript"
        );
        assert!(!is_err(&res));
    }

    #[tokio::test]
    async fn output_repeats_are_bounded_per_reply() {
        let ctx = ctx_with(StubRegistry::new(vec![handle(
            "agent-1",
            BackgroundTaskStatus::Running,
            "work",
        )]));
        let tool = create_task_output_tool();
        for _ in 0..READ_ANSWER_BUDGET {
            let res = tool
                .call(rjson!({"task_id":"agent-1"}), &ctx, None)
                .await
                .expect("call succeeds");
            assert!(!is_err(&res));
        }
        let blocked = tool
            .call(rjson!({"task_id":"agent-1"}), &ctx, None)
            .await
            .expect("call succeeds");
        let text = text_of(&blocked);
        assert!(is_err(&blocked));
        assert!(
            text.contains("not answered again"),
            "third read must be refused: {}",
            utf8(&text)
        );
        // A new reply resets the budget.
        ctx.reply_gate
            .as_ref()
            .unwrap()
            .begin_reply("reply-2".to_string());
        let fresh = tool
            .call(rjson!({"task_id":"agent-1"}), &ctx, None)
            .await
            .expect("call succeeds");
        assert!(!is_err(&fresh));
    }

    #[tokio::test]
    async fn output_resolves_bound_names_and_reports_ambiguity() {
        let mut registry = StubRegistry::new(vec![handle(
            "agent-1",
            BackgroundTaskStatus::Running,
            "named work",
        )]);
        registry.names.push(("writer".into(), "agent-1".into()));
        registry.ambiguous.push("dup".into());
        let ctx = ctx_with(registry);
        let tool = create_task_output_tool();
        let by_name = tool
            .call(rjson!({"task_id":"writer"}), &ctx, None)
            .await
            .expect("call succeeds");
        assert!(
            by_name
                .serializable
                .output
                .to_string()
                .contains("named work"),
            "name resolution reads the bound agent"
        );
        let ambiguous = tool
            .call(rjson!({"task_id":"dup"}), &ctx, None)
            .await
            .expect("call succeeds");
        assert!(is_err(&ambiguous));
        assert!(text_of(&ambiguous).contains("Ambiguous sub-agent target"));
    }

    #[tokio::test]
    async fn stop_terminates_running_agent_and_reports_finished_tasks() {
        let registry = StubRegistry::new(vec![
            handle("agent-1", BackgroundTaskStatus::Running, ""),
            handle("agent-2", BackgroundTaskStatus::Completed, ""),
        ]);
        let stopped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let recorder = Arc::clone(&stopped);
        let ctx = ctx_with(registry);
        let tool = create_task_stop_tool();
        let res = tool
            .call(rjson!({"task_id":"agent-1"}), &ctx, None)
            .await
            .expect("call succeeds");
        assert!(!is_err(&res));
        assert!(text_of(&res).contains("Successfully stopped task: agent-1"));
        let finished = tool
            .call(rjson!({"task_id":"agent-2"}), &ctx, None)
            .await
            .expect("call succeeds");
        assert!(is_err(&finished));
        assert!(text_of(&finished).contains("is not running (status: completed)"));
        let missing = tool
            .call(rjson!({"task_id":"nope"}), &ctx, None)
            .await
            .expect("call succeeds");
        assert!(is_err(&missing));
        assert!(text_of(&missing).contains("No background task with id nope."));
        recorder.store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(stopped.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn stop_describes_teammate_and_name_targets() {
        let tool = create_task_stop_tool();
        let description = tool.static_description();
        assert!(description.contains("bare teammate name"));
        assert!(description.contains("pass that name as task_id"));
    }

    #[cfg(unix)]
    struct IdSandbox;

    #[cfg(unix)]
    impl crate::types::sandbox::SandboxBackend for IdSandbox {
        fn kind(&self) -> crate::types::sandbox::SandboxKind {
            crate::types::sandbox::SandboxKind::Process
        }
        fn is_available(&self) -> bool {
            true
        }
        fn transform(
            &self,
            req: &crate::types::sandbox::ExecRequest,
            _policy: &crate::types::sandbox::SandboxPolicy,
        ) -> crate::types::sandbox::SpawnSpec {
            crate::types::sandbox::SpawnSpec {
                file: req.argv.first().cloned().unwrap_or_default(),
                args: req
                    .argv
                    .get(1..)
                    .map(<[RuntimeString]>::to_vec)
                    .unwrap_or_default(),
                env: req
                    .env
                    .clone()
                    .unwrap_or_else(crate::runtime_process::ParentEnvSnapshot::capture),
                cwd: req.cwd.clone(),
            }
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unified_tools_cover_background_shells() {
        use crate::tools::background_shells::StartShellOptions;
        let ctx = ctx_with(StubRegistry::new(vec![]));
        let shell_id = ctx
            .background_processes
            .start(
                ctx.process_context.clone(),
                StartShellOptions {
                    command: RuntimeString::from("sleep 30"),
                    cwd: RuntimePath::from("/tmp"),
                    sandbox: Arc::new(IdSandbox),
                    env: None,
                    max_output_bytes: 64 * 1024,
                    writable_roots: None,
                    unconfined_writes: false,
                    network_allowed: None,
                    managed_network: None,
                    on_complete: None,
                },
            )
            .await
            .expect("shell starts");
        // The reader accepts the legacy shellId spelling and reports the shell.
        let read = create_task_output_tool()
            .call(rjson!({"shellId":"PLACEHOLDER"}), &ctx, None)
            .await
            .expect("call succeeds");
        let _ = read;
        let by_shell_id = create_task_output_tool()
            .call(
                runtime_json::parse(
                    &RuntimeString::from("{\"shellId\":\"")
                        .concat(&shell_id)
                        .concat(&RuntimeString::from("\"}")),
                )
                .expect("dynamic JSON"),
                &ctx,
                None,
            )
            .await
            .expect("call succeeds");
        let text = text_of(&by_shell_id);
        assert!(text.contains("[status: "), "{}", utf8(&text));
        // The unified stopper terminates the shell through the legacy alias too.
        let stopped = create_task_stop_tool()
            .call(
                runtime_json::parse(
                    &RuntimeString::from("{\"task_id\":\"")
                        .concat(&shell_id)
                        .concat(&RuntimeString::from("\",\"signal\":\"SIGKILL\"}")),
                )
                .expect("dynamic JSON"),
                &ctx,
                None,
            )
            .await
            .expect("call succeeds");
        let text = text_of(&stopped);
        assert!(!is_err(&stopped), "{}", utf8(&text));
        assert!(
            text.contains("Successfully stopped task: "),
            "{}",
            utf8(&text)
        );
        assert!(text.contains("sleep 30"), "{}", utf8(&text));
        let after = create_task_output_tool()
            .call(
                runtime_json::parse(
                    &RuntimeString::from("{\"task_id\":\"")
                        .concat(&shell_id)
                        .concat(&RuntimeString::from("\"}")),
                )
                .expect("dynamic JSON"),
                &ctx,
                None,
            )
            .await
            .expect("call succeeds");
        let after_text = text_of(&after);
        assert!(
            after_text.contains("[status: killed"),
            "{}",
            utf8(&after_text)
        );
        // A repeat read after collection still reports the honest terminal state.
        let collected = create_task_output_tool()
            .call(
                runtime_json::parse(
                    &RuntimeString::from("{\"task_id\":\"")
                        .concat(&shell_id)
                        .concat(&RuntimeString::from("\"}")),
                )
                .expect("dynamic JSON"),
                &ctx,
                None,
            )
            .await
            .expect("call succeeds");
        let collected_text = text_of(&collected);
        assert!(
            collected_text.contains("already collected"),
            "{}",
            utf8(&collected_text)
        );
    }
}
