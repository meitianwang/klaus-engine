//! `task_output` / `task_stop` — read and terminate the session's background
//! sub-agents (the ones started with `run_in_background`), exposed via
//! `ctx.background_tasks`. The `shell_id` field on `task_stop` is accepted as a
//! deprecated alias for `task_id`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::json;

use crate::runtime_values::{NodeBufferUtf8, RuntimeJson, RuntimeObject, RuntimeString};
use crate::tools::build_tool::text_result;
use crate::types::errors::ToolError;
use crate::types::json::{Json, JsonSchema};
use crate::types::tool::{BackgroundTaskStatus, Tool, ToolContext, ToolProgress, ToolResult};

const POLL_INTERVAL_MS: u64 = 100;

/// The wire form of a background task status.
fn status_str(status: BackgroundTaskStatus) -> &'static str {
    match status {
        BackgroundTaskStatus::Running => "running",
        BackgroundTaskStatus::Completed => "completed",
        BackgroundTaskStatus::Failed => "failed",
        BackgroundTaskStatus::Interrupted => "interrupted",
    }
}

fn runtime_object(fields: impl IntoIterator<Item = (&'static str, RuntimeJson)>) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    for (key, value) in fields {
        object.insert(key, value);
    }
    RuntimeJson::Object(object)
}

// ---- task_output -----------------------------------------------------------

fn task_output_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "task_id": {
                "type": "string",
                "description": "Identifier of the background task to read"
            },
            "block": {
                "type": "boolean",
                "default": true,
                "description": "Block until the task finishes"
            },
            "timeout": {
                "type": "number",
                "minimum": 0,
                "maximum": 600000,
                "default": 30000,
                "description": "Maximum milliseconds to wait"
            }
        },
        "required": ["task_id", "block", "timeout"],
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap_or_default()
}

/// Get the output (and status) of a background task by its id.
pub struct TaskOutputTool {
    input_schema: JsonSchema,
}

/// Construct the `task_output` tool as an erased registry-side [`Tool`].
pub fn create_task_output_tool() -> Arc<dyn Tool> {
    Arc::new(TaskOutputTool {
        input_schema: task_output_schema(),
    })
}

#[async_trait]
impl Tool for TaskOutputTool {
    fn static_name(&self) -> &str {
        "task_output"
    }

    fn static_description(&self) -> &str {
        "Get the output (and status) of a background task by its id."
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

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let task_id = input
            .get("task_id")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .unwrap_or_default();
        let block = input
            .get("block")
            .and_then(RuntimeJson::as_bool)
            .unwrap_or(true);
        let timeout = input
            .get("timeout")
            .and_then(RuntimeJson::as_f64)
            .unwrap_or(30_000.0);

        let reg = ctx.background_tasks.as_ref();
        let Some(mut handle) = reg.and_then(|r| r.get(&task_id)) else {
            let text = RuntimeString::from("No background task with id ")
                .concat(&task_id)
                .concat(&RuntimeString::from("."));
            return Ok(text_result(
                runtime_object([
                    ("task_id", RuntimeJson::String(task_id)),
                    ("status", RuntimeJson::from("not_found")),
                    ("output", RuntimeJson::String(RuntimeString::default())),
                    ("retrieval_status", RuntimeJson::from("not_ready")),
                ]),
                text,
                true,
            ));
        };

        // New input must reach the caller even when the watched child is waiting
        // for that caller's response.
        let mut input_pending = false;
        if block && handle.status == BackgroundTaskStatus::Running {
            let deadline = Instant::now() + Duration::from_millis(timeout.max(0.0) as u64);
            while handle.status == BackgroundTaskStatus::Running && Instant::now() < deadline {
                if ctx.signal.is_cancelled() {
                    break;
                }
                let mailbox_pending = match &ctx.swarm {
                    Some(swarm) => swarm.has_pending_input().await.map_err(ToolError::new)?,
                    None => false,
                };
                if mailbox_pending || reg.is_some_and(|registry| registry.has_pending_input()) {
                    input_pending = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
                if let Some(next) = reg.and_then(|r| r.get(&task_id)) {
                    handle = next;
                }
            }
        }

        // While still running, return the transcript accumulated so far so a poll
        // shows progress instead of empty — keeping the honest status.
        if handle.status == BackgroundTaskStatus::Running {
            let retrieval = if input_pending {
                "input_pending"
            } else if block {
                "timeout"
            } else {
                "not_ready"
            };
            let partial = handle.partial.clone().unwrap_or_default();
            let text = if !partial.is_empty() {
                RuntimeString::from("Task ")
                    .concat(&handle.agent_id)
                    .concat(&RuntimeString::from(" still running ("))
                    .concat(&RuntimeString::from(retrieval))
                    .concat(&RuntimeString::from("); partial output above."))
            } else {
                RuntimeString::from("Task ")
                    .concat(&handle.agent_id)
                    .concat(&RuntimeString::from(" has not finished yet ("))
                    .concat(&RuntimeString::from(retrieval))
                    .concat(&RuntimeString::from(")."))
            };
            return Ok(text_result(
                runtime_object([
                    ("task_id", RuntimeJson::String(handle.agent_id.clone())),
                    ("status", RuntimeJson::from(status_str(handle.status))),
                    ("output", RuntimeJson::String(partial)),
                    ("retrieval_status", RuntimeJson::from(retrieval)),
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
                ("status", RuntimeJson::from(status_str(handle.status))),
                ("output", RuntimeJson::String(output)),
                ("retrieval_status", RuntimeJson::from(retrieval)),
            ]),
            text,
            failed,
        ))
    }
}

// ---- task_stop -------------------------------------------------------------

fn task_stop_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "task_id": {
                "type": "string",
                "description": "Identifier of the background task to terminate"
            },
            "shell_id": {
                "type": "string",
                "description": "Deprecated alias for task_id"
            }
        },
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .unwrap_or_default()
}

/// Stop a running background task by its id.
pub struct TaskStopTool {
    input_schema: JsonSchema,
}

/// Construct the `task_stop` tool as an erased registry-side [`Tool`].
pub fn create_task_stop_tool() -> Arc<dyn Tool> {
    Arc::new(TaskStopTool {
        input_schema: task_stop_schema(),
    })
}

#[async_trait]
impl Tool for TaskStopTool {
    fn static_name(&self) -> &str {
        "task_stop"
    }

    fn static_description(&self) -> &str {
        "Stop a running background task by its id."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.input_schema
    }

    fn input_schema_property_order(
        &self,
        object_path: &[String],
    ) -> Option<&'static [&'static str]> {
        object_path.is_empty().then_some(&["task_id", "shell_id"])
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

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let id = input
            .get("task_id")
            .and_then(RuntimeJson::as_str)
            .or_else(|| input.get("shell_id").and_then(RuntimeJson::as_str));
        let Some(id) = id else {
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
        let reg = ctx.background_tasks.as_ref();
        let Some(handle) = reg.and_then(|r| r.get(id)) else {
            let message = RuntimeString::from("No background task with id ")
                .concat(id)
                .concat(&RuntimeString::from("."));
            return Ok(text_result(
                runtime_object([
                    ("message", RuntimeJson::String(message.clone())),
                    ("task_id", RuntimeJson::String(id.clone())),
                    ("task_type", RuntimeJson::String(RuntimeString::default())),
                ]),
                message,
                true,
            ));
        };
        // A persistent mailbox can still be live after its latest run failed.
        // The registry owns lifetime liveness, so its stop result is authoritative.
        if !reg.is_some_and(|registry| registry.stop(id)) {
            let msg = RuntimeString::from("Background task ")
                .concat(id)
                .concat(&RuntimeString::from(" already finished (status: "))
                .concat(&RuntimeString::from(status_str(handle.status)))
                .concat(&RuntimeString::from(")."));
            return Ok(text_result(
                runtime_object([
                    ("message", RuntimeJson::String(msg.clone())),
                    ("task_id", RuntimeJson::String(id.clone())),
                    ("task_type", RuntimeJson::from("agent")),
                ]),
                msg,
                true,
            ));
        }
        let message = RuntimeString::from("Stopped background task ")
            .concat(id)
            .concat(&RuntimeString::from("."));
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
        Ok(text_result(output, message, false))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::{runtime_json, RuntimePath};
    use crate::tools::builtins::test_ctx::make_ctx;
    use crate::types::tool::BackgroundTaskHandle;
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

    /// A registry over a fixed set of handles, recording any stop() calls.
    struct StubRegistry {
        handles: Vec<BackgroundTaskHandle>,
        stopped: Mutex<Vec<RuntimeString>>,
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

    #[test]
    fn output_metadata_and_schema() {
        let tool = create_task_output_tool();
        assert_eq!(tool.name(), "task_output");
        assert_eq!(
            tool.search_hint(),
            Some(RuntimeString::from("retrieve a background task result"))
        );
        assert_eq!(tool.max_result_size_chars(), Some(100_000));
        assert!(tool.is_read_only(&rjson!({})));
        assert!(tool.is_concurrency_safe(&rjson!({})));
        let schema = tool.input_schema();
        assert_eq!(
            schema.get("required").unwrap(),
            &rjson!(["task_id", "block", "timeout"])
        );
        let t = schema
            .get("properties")
            .and_then(|p| p.get("timeout"))
            .unwrap();
        assert_eq!(t.get("minimum").unwrap(), &rjson!(0));
        assert_eq!(t.get("maximum").unwrap(), &rjson!(600000));
        assert_eq!(t.get("default").unwrap(), &rjson!(30000));
    }

    #[tokio::test]
    async fn output_reports_a_missing_task() {
        let tool = create_task_output_tool();
        let mut ctx = make_ctx("/tmp");
        ctx.background_tasks = Some(Arc::new(StubRegistry {
            handles: Vec::new(),
            stopped: Mutex::new(Vec::new()),
        }));
        let res = tool
            .call(rjson!({ "task_id": "abc", "block": false }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(text_of(&res), "No background task with id abc.");
        assert_eq!(
            res.serializable
                .output
                .get("status")
                .and_then(RuntimeJson::as_str),
            Some(&RuntimeString::from("not_found"))
        );
    }

    #[tokio::test]
    async fn output_reads_a_completed_tasks_result_file() {
        let dir = std::env::temp_dir().join(format!("ae-taskctl-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("result.txt");
        std::fs::write(&file, "the answer").unwrap();
        let tool = create_task_output_tool();
        let mut ctx = make_ctx("/tmp");
        ctx.background_tasks = Some(Arc::new(StubRegistry {
            handles: vec![BackgroundTaskHandle {
                agent_id: "job1".into(),
                output_file: crate::fs::node_path::runtime_path_from_native(&file),
                status: BackgroundTaskStatus::Completed,
                description: None,
                partial: None,
            }],
            stopped: Mutex::new(Vec::new()),
        }));
        let res = tool
            .call(rjson!({ "task_id": "job1", "block": false }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(text_of(&res), "the answer");
        assert_eq!(
            res.serializable
                .output
                .get("retrieval_status")
                .and_then(RuntimeJson::as_str),
            Some(&RuntimeString::from("success"))
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    fn retrieval_status(res: &ToolResult) -> RuntimeString {
        res.serializable
            .output
            .get("retrieval_status")
            .and_then(RuntimeJson::as_str)
            .cloned()
            .expect("task_output always reports a retrieval status")
    }

    /// The result file lives in the OS temp directory and is not durable, while
    /// the agent's transcript under the session directory is. A completed task
    /// whose file was cleared must return the transcript it left behind and say
    /// which one it is — reporting `success` with an empty output would claim a
    /// result that was never read.
    #[tokio::test]
    async fn output_falls_back_to_the_transcript_when_the_result_file_is_gone() {
        let tool = create_task_output_tool();
        let mut ctx = make_ctx("/tmp");
        ctx.background_tasks = Some(Arc::new(StubRegistry {
            handles: vec![BackgroundTaskHandle {
                agent_id: "job1".into(),
                output_file: RuntimePath::from("/definitely/not/here.txt"),
                status: BackgroundTaskStatus::Completed,
                description: None,
                partial: Some("ran the tests\nall passed".into()),
            }],
            stopped: Mutex::new(Vec::new()),
        }));
        let res = tool
            .call(rjson!({ "task_id": "job1", "block": false }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(retrieval_status(&res), "transcript_only");
        assert_eq!(
            res.serializable
                .output
                .get("output")
                .and_then(RuntimeJson::as_str),
            Some(&RuntimeString::from("ran the tests\nall passed")),
            "the transcript is what the caller gets"
        );
        let text = text_of(&res).try_to_utf8().unwrap();
        assert!(
            text.contains("result file is gone"),
            "the reply must not pass the transcript off as the final answer: {text}"
        );
    }

    /// Neither source survived: say so plainly instead of reporting success.
    #[tokio::test]
    async fn output_reports_unavailable_when_both_sources_are_gone() {
        let tool = create_task_output_tool();
        let mut ctx = make_ctx("/tmp");
        ctx.background_tasks = Some(Arc::new(StubRegistry {
            handles: vec![BackgroundTaskHandle {
                agent_id: "job1".into(),
                output_file: RuntimePath::from("/definitely/not/here.txt"),
                status: BackgroundTaskStatus::Completed,
                description: None,
                partial: None,
            }],
            stopped: Mutex::new(Vec::new()),
        }));
        let res = tool
            .call(rjson!({ "task_id": "job1", "block": false }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(retrieval_status(&res), "unavailable");
        assert_eq!(text_of(&res), "Task job1 completed with no output.");
    }

    /// An interrupted task reports its partial work and never claims completion,
    /// whether or not anything survived.
    #[tokio::test]
    async fn output_reports_an_interrupted_task_as_interrupted_not_completed() {
        let tool = create_task_output_tool();
        for partial in [Some("half the work".into()), None] {
            let mut ctx = make_ctx("/tmp");
            ctx.background_tasks = Some(Arc::new(StubRegistry {
                handles: vec![BackgroundTaskHandle {
                    agent_id: "job1".into(),
                    output_file: RuntimePath::from("/definitely/not/here.txt"),
                    status: BackgroundTaskStatus::Interrupted,
                    description: None,
                    partial: partial.clone(),
                }],
                stopped: Mutex::new(Vec::new()),
            }));
            let res = tool
                .call(rjson!({ "task_id": "job1", "block": false }), &ctx, None)
                .await
                .unwrap();
            assert_eq!(retrieval_status(&res), "interrupted");
            assert_eq!(res.serializable.is_error, Some(true));
            assert_eq!(
                res.serializable
                    .output
                    .get("status")
                    .and_then(RuntimeJson::as_str),
                Some(&RuntimeString::from("interrupted"))
            );
            let text = text_of(&res).try_to_utf8().unwrap();
            assert!(
                text.contains("was interrupted"),
                "the reply must state why there is no final answer: {text}"
            );
            // The wording may mention "completed" only to deny it, so the
            // no-completion claim above is asserted on the reported status field
            // rather than on a substring of prose.
            if let Some(partial) = &partial {
                assert_eq!(
                    res.serializable
                        .output
                        .get("output")
                        .and_then(RuntimeJson::as_str)
                        .map(|value| value.try_to_utf8().unwrap()),
                    Some(partial.try_to_utf8().unwrap()),
                    "the partial work is returned so the caller can judge it"
                );
            }
        }
    }

    #[tokio::test]
    async fn stop_requires_a_task_id() {
        let tool = create_task_stop_tool();
        let ctx = make_ctx("/tmp");
        let res = tool.call(rjson!({}), &ctx, None).await.unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(text_of(&res), "A task_id is required.");
    }

    #[tokio::test]
    async fn stop_terminates_a_running_task_and_accepts_the_shell_id_alias() {
        let tool = create_task_stop_tool();
        let mut ctx = make_ctx("/tmp");
        let reg = Arc::new(StubRegistry {
            handles: vec![BackgroundTaskHandle {
                agent_id: "job2".into(),
                output_file: RuntimePath::from("/x"),
                status: BackgroundTaskStatus::Running,
                description: Some("do the thing".into()),
                partial: None,
            }],
            stopped: Mutex::new(Vec::new()),
        });
        ctx.background_tasks = Some(reg.clone());
        let res = tool
            .call(rjson!({ "shell_id": "job2" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(false));
        assert_eq!(text_of(&res), "Stopped background task job2.");
        assert_eq!(
            res.serializable
                .output
                .get("command")
                .and_then(RuntimeJson::as_str),
            Some(&RuntimeString::from("do the thing"))
        );
        assert_eq!(
            reg.stopped.lock().unwrap().as_slice(),
            [RuntimeString::from("job2")]
        );
    }

    #[tokio::test]
    async fn stop_reports_an_already_finished_task() {
        let tool = create_task_stop_tool();
        let mut ctx = make_ctx("/tmp");
        ctx.background_tasks = Some(Arc::new(StubRegistry {
            handles: vec![BackgroundTaskHandle {
                agent_id: "job3".into(),
                output_file: RuntimePath::from("/x"),
                status: BackgroundTaskStatus::Completed,
                description: None,
                partial: None,
            }],
            stopped: Mutex::new(Vec::new()),
        }));
        let res = tool
            .call(rjson!({ "task_id": "job3" }), &ctx, None)
            .await
            .unwrap();
        assert_eq!(res.serializable.is_error, Some(true));
        assert_eq!(
            text_of(&res),
            "Background task job3 already finished (status: completed)."
        );
    }
}
