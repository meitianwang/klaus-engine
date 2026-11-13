//! Defect: a tool loop never receives missing-plan guidance, and a task
//! reminder captured at turn entry remains stale after the plan changes.
//! Root cause: task reminders were evaluated only before the model loop and
//! treated an absent plan like a completed one.
//! Fixed by: 2026-09-07 plan-creation follow-up work item.

use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::tools::builtins::task_tools::{
    create_task_list_tool, create_task_write_tool,
};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

enum Reply {
    Done,
    Json,
    Tool(&'static str, Value),
}

struct RecordingProvider {
    info: ProviderInfo,
    requests: Mutex<Vec<ModelRequest>>,
    replies: Mutex<VecDeque<Reply>>,
}

impl ModelProvider for RecordingProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: true,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: 200_000,
            max_output_tokens: 8192,
            native_pdf: true,
            vision: true,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        for message in &request.messages {
            if let Message::ToolResult(result) = message {
                assert!(!result.is_error, "unexpected tool failure: {result:?}");
            }
        }
        let mut requests = self.requests.lock().unwrap();
        let id = RuntimeString::from(format!("plan-probe-{}", requests.len()));
        requests.push(request);
        let reply = self.replies.lock().unwrap().pop_front().unwrap();
        let (mut events, reason) = match reply {
            Reply::Done => (
                vec![ModelStreamEvent::TextDelta {
                    text: "Done.".into(),
                }],
                ModelStopReason::EndTurn,
            ),
            Reply::Json => (
                vec![ModelStreamEvent::TextDelta { text: "{}".into() }],
                ModelStopReason::EndTurn,
            ),
            Reply::Tool(name, input) => (
                vec![
                    ModelStreamEvent::ToolUseStart {
                        id: id.clone(),
                        name: name.into(),
                        thought_signature: None,
                        tool_kind: None,
                        input_format: None,
                    },
                    ModelStreamEvent::ToolUseInputDelta {
                        id: id.clone(),
                        partial_json: input.to_string().into(),
                    },
                    ModelStreamEvent::ToolUseStop { id },
                ],
                ModelStopReason::ToolUse,
            ),
        };
        events.push(ModelStreamEvent::MessageStop {
            stop_reason: reason,
            usage: Usage {
                input_tokens: 100,
                output_tokens: 10,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
                total_tokens: None,
            },
        });
        Box::pin(futures::stream::iter(events))
    }
}

struct Directory(std::path::PathBuf);

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn record(replies: Vec<Reply>, inputs: &[&str], has_write: bool) -> Vec<ModelRequest> {
    record_with_schema(replies, inputs, has_write, None).await
}

async fn record_with_schema(
    replies: Vec<Reply>,
    inputs: &[&str],
    has_write: bool,
    output_schema: Option<klaus_agent_core::types::json::JsonSchema>,
) -> Vec<ModelRequest> {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let root = Directory(std::env::temp_dir().join(format!(
        "klaus-plan-guidance-regression-{}",
        uuid::Uuid::new_v4()
    )));
    std::fs::create_dir_all(&root.0).unwrap();
    let provider = Arc::new(RecordingProvider {
        info: ProviderInfo {
            name: "plan-guidance-regression".into(),
            base_url: None,
            env_key: None,
            default_model: "test-model".into(),
        },
        requests: Mutex::new(Vec::new()),
        replies: Mutex::new(replies.into()),
    });
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
    options.provider = provider.clone();
    options.cwd = Some(root.0.to_string_lossy().into_owned());
    let config = options.config.as_mut().unwrap();
    config["model"] = json!("test-model");
    config["agentMemory"] = json!(false);
    config["autoMemory"] = json!(false);
    config["loopDetection"] = json!(false);
    let mut tools = vec![create_task_list_tool()];
    if has_write {
        tools.push(create_task_write_tool());
    }
    options.tools = Some(tools);
    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions {
        cwd: Some(root.0.to_string_lossy().into_owned().into()),
        user_config_dir: Some(root.0.to_string_lossy().into_owned().into()),
        ..Default::default()
    });
    let list_id = String::from_utf8(session.task_list_id().to_node_utf8()).unwrap();
    let _tasks = Directory(klaus_agent_core::fs::paths::tasks_dir(&list_id).into());
    for input in inputs {
        let result = session
            .run(
                Input::Text((*input).into()),
                TurnOptions {
                    output_schema: output_schema.clone(),
                    ..Default::default()
                },
            )
            .await;
        assert!(result.error.is_none(), "{:?}", result.error);
    }
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    assert!(provider.replies.lock().unwrap().is_empty());
    let requests = provider.requests.lock().unwrap().clone();
    requests
}

fn task_reminder(request: &ModelRequest) -> String {
    request
        .system
        .iter()
        .filter(|part| part.name == "task-plan-reminder")
        .map(|part| String::from_utf8(part.text.to_node_utf8()).unwrap())
        .collect::<Vec<_>>()
        .join("\n")
}

fn reads(count: usize) -> Vec<Reply> {
    (0..count)
        .map(|_| Reply::Tool("TaskList", json!({})))
        .collect()
}

fn plan(completed: bool) -> Reply {
    Reply::Tool(
        "TaskWrite",
        json!({"tasks": [
            {"id":"inspect", "subject":"Inspect the defect", "status":if completed {"completed"} else {"in_progress"}},
            {"id":"fix", "subject":"Implement the fix", "status":if completed {"completed"} else {"pending"}},
            {"id":"verify", "subject":"Verify the result", "status":if completed {"completed"} else {"pending"}}
        ]}),
    )
}

#[tokio::test]
async fn missing_plan_reminder_arrives_inside_one_tool_loop_and_clears_after_completion() {
    let mut replies = reads(1);
    replies.push(plan(false));
    replies.extend(reads(8));
    replies.push(Reply::Tool(
        "TaskWrite",
        json!({"merge":true,"tasks":[
            {"id":"inspect","status":"completed"},
            {"id":"fix","status":"completed"},
            {"id":"verify","status":"completed"}
        ]}),
    ));
    replies.push(Reply::Done);
    let requests = record(
        replies,
        &["Investigate, fix, and verify this defect."],
        true,
    )
    .await;
    assert_eq!(requests.len(), 12);
    assert!(task_reminder(&requests[0]).is_empty());
    assert!(
        task_reminder(&requests[1]).contains("TaskWrite"),
        "a missing plan must be noticed immediately after initial scoping"
    );
    assert!(requests[2..10]
        .iter()
        .all(|request| task_reminder(request).is_empty()));
    let active = task_reminder(&requests[10]);
    assert!(active.contains("Inspect the defect"), "{active}");
    assert!(!active.contains("No current plan"), "{active}");
    assert!(task_reminder(&requests[11]).is_empty());
}

#[tokio::test]
async fn completed_plan_is_not_treated_as_a_missing_plan_on_the_next_request() {
    let mut replies = vec![plan(true), Reply::Done];
    replies.extend(reads(11));
    replies.push(Reply::Done);
    let requests = record(
        replies,
        &["Finish the task.", "Review the completed work."],
        true,
    )
    .await;
    assert!(requests
        .iter()
        .all(|request| task_reminder(request).is_empty()));
}

#[tokio::test]
async fn short_answers_and_tool_filtered_sessions_receive_no_plan_nag() {
    let replies = (0..12).map(|_| Reply::Done).collect();
    let requests = record(replies, &["Hello."; 12], true).await;
    assert!(requests
        .iter()
        .all(|request| task_reminder(request).is_empty()));
    let mut replies = reads(11);
    replies.push(Reply::Done);
    let requests = record(replies, &["Read the available task state."], false).await;
    assert!(requests
        .iter()
        .all(|request| task_reminder(request).is_empty()));
}

#[tokio::test]
async fn text_only_resamples_do_not_count_as_tool_work() {
    let requests = record_with_schema(
        vec![Reply::Done, Reply::Done, Reply::Json],
        &["Return an empty JSON object."],
        true,
        Some(serde_json::from_value(json!({"type":"object"})).unwrap()),
    )
    .await;
    assert_eq!(requests.len(), 3);
    assert!(requests
        .iter()
        .all(|request| task_reminder(request).is_empty()));
}

#[tokio::test]
async fn first_request_advertises_optional_checklists_and_persistent_work() {
    let requests = record(
        vec![Reply::Done],
        &["Implement a feature and verify it."],
        true,
    )
    .await;
    let request = &requests[0];
    let write = request
        .tools
        .iter()
        .find(|tool| tool.name == "TaskWrite")
        .unwrap();
    assert!(write
        .description
        .contains("when it helps carry out multi-step work"));
    assert!(write
        .description
        .contains("does not enter or exit plan mode"));
    assert!(!write.description.contains("three or more"));
    assert!(request
        .system
        .iter()
        .any(|part| { part.name == "base" && part.text.contains("# Task execution") }));
}
