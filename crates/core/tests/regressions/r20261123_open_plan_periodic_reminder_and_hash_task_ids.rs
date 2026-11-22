//! Defect: on prefix-cached compatible sources, an open task checklist reminder
//! was emitted once at round 10 and never re-appended across 100+ subsequent
//! tool rounds, while follow-up turns temporarily cleared `[task-plan-reminder]`
//! on round 0; additionally, `TaskGet` / `TaskUpdate` rejected `#<id>` prefixes
//! echoed from `#<id>` display lines (`Task ##... does not exist.`).
//! Root cause: `runtime_reminders` suppressed all unchanged snapshots regardless
//! of elapsed tool rounds on an open checklist, `TaskReminderProvider` ignored
//! prior-turn tool rounds for active plans, and task tools did not strip a
//! leading `#` from `taskId` / dependency arguments.
//! Fixed by: 2026-11-23 periodic append-only open-plan reminder & `#` task-id normalization.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::content::{ContentBlock, OutputBlock};
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::tasks::task_store::{list_plan_tasks, TaskStatus};
use klaus_agent_core::tools::builtins::read::create_read_tool;
use klaus_agent_core::tools::builtins::task_tools::{
    create_task_create_tool, create_task_get_tool, create_task_list_tool, create_task_update_tool,
    create_task_write_tool,
};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    ModelCapabilities, ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use serde_json::{json, Value};

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

const MODEL: &str = "qwen-periodic-reminder-fixture";
const PHASE: &str = "antigravity-runtime-reminders";

fn is_step(message: &Message) -> bool {
    matches!(
        message,
        Message::User(user)
            if user.is_meta == Some(true)
                && user.content.iter().any(|block| {
                    matches!(
                        block,
                        ContentBlock::Text(text)
                            if text.phase.as_ref().is_some_and(|phase| phase == PHASE)
                    )
                })
    )
}

fn step_texts(messages: &[Message]) -> Vec<String> {
    messages
        .iter()
        .filter(|message| is_step(message))
        .map(|message| {
            let Message::User(user) = message else {
                unreachable!()
            };
            let ContentBlock::Text(text) = &user.content[0] else {
                unreachable!()
            };
            String::from_utf8(text.text.to_node_utf8()).unwrap()
        })
        .collect()
}

enum ScriptedStep {
    Tool(&'static str, Value),
    CompleteAllFromReminderWithHashPrefix,
    Done,
}

struct RecordingCompatProvider {
    info: ProviderInfo,
    requests: Mutex<Vec<ModelRequest>>,
    steps: Mutex<VecDeque<ScriptedStep>>,
}

impl RecordingCompatProvider {
    fn new(steps: Vec<ScriptedStep>) -> Arc<Self> {
        Arc::new(Self {
            info: ProviderInfo {
                name: "bailian".into(),
                default_model: MODEL.into(),
                base_url: Some("https://dashscope.aliyuncs.com/compatible-mode/v1".into()),
                env_key: None,
            },
            requests: Mutex::new(Vec::new()),
            steps: Mutex::new(steps.into()),
        })
    }
}

impl ModelProvider for RecordingCompatProvider {
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
            max_output_tokens: 8_192,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn model_metadata(&self, _: &RuntimeString) -> ModelCapabilities {
        ModelCapabilities {
            context_window: Some(200_000),
            max_output_tokens: Some(8_192),
            tools: Some(true),
            ..Default::default()
        }
    }

    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        for message in &request.messages {
            if let Message::ToolResult(result) = message {
                assert!(!result.is_error, "unexpected tool error: {result:?}");
                for block in &result.content {
                    if let OutputBlock::Text(text) = block {
                        assert!(
                            !text.text.contains("does not exist"),
                            "unexpected missing-task tool output: {}",
                            String::from_utf8_lossy(&text.text.to_node_utf8())
                        );
                    }
                }
            }
        }
        let call_idx = {
            let mut guard = self.requests.lock().unwrap();
            let idx = guard.len();
            guard.push(request.clone());
            idx
        };
        let step = self
            .steps
            .lock()
            .unwrap()
            .pop_front()
            .expect("scripted step");
        let mut events = vec![ModelStreamEvent::MessageStart {
            model: MODEL.into(),
        }];
        let stop_reason = match step {
            ScriptedStep::Tool(name, input) => {
                let id: RuntimeString = format!("call-{call_idx}").into();
                events.extend([
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
                ]);
                ModelStopReason::ToolUse
            }
            ScriptedStep::CompleteAllFromReminderWithHashPrefix => {
                let latest_reminder = step_texts(&request.messages)
                    .pop()
                    .expect("expected active runtime reminder step");
                let hash_ids: Vec<String> = latest_reminder
                    .lines()
                    .filter(|line| line.starts_with("#plan-step-"))
                    .map(|line| line.split_whitespace().next().unwrap().to_string())
                    .collect();
                assert_eq!(
                    hash_ids.len(),
                    2,
                    "expected two open #plan-step-* entries in {latest_reminder}"
                );
                for (sub_idx, hash_id) in hash_ids.into_iter().enumerate() {
                    assert!(hash_id.starts_with('#'));
                    let id: RuntimeString = format!("call-{call_idx}-{sub_idx}").into();
                    let args = json!({
                        "taskId": hash_id,
                        "status": "completed"
                    });
                    events.extend([
                        ModelStreamEvent::ToolUseStart {
                            id: id.clone(),
                            name: "TaskUpdate".into(),
                            thought_signature: None,
                            tool_kind: None,
                            input_format: None,
                        },
                        ModelStreamEvent::ToolUseInputDelta {
                            id: id.clone(),
                            partial_json: args.to_string().into(),
                        },
                        ModelStreamEvent::ToolUseStop { id },
                    ]);
                }
                ModelStopReason::ToolUse
            }
            ScriptedStep::Done => {
                events.push(ModelStreamEvent::TextDelta {
                    text: "done".into(),
                });
                ModelStopReason::EndTurn
            }
        };
        events.push(ModelStreamEvent::MessageStop {
            stop_reason,
            usage: Usage {
                input_tokens: 4_096,
                output_tokens: 32,
                cache_read_input_tokens: 3_584,
                cache_creation_input_tokens: 0,
                total_tokens: None,
            },
        });
        Box::pin(futures::stream::iter(events))
    }
}

struct CleanupDir(std::path::PathBuf);

impl Drop for CleanupDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn open_plan_re_appends_reminder_on_cadence_without_rewriting_prefix_and_accepts_hash_ids() {
    let root = tempfile::tempdir().unwrap();
    let fixture_file = root.path().join("build.log");
    std::fs::write(&fixture_file, "ok\n").unwrap();
    let read_path = fixture_file.to_string_lossy().into_owned();

    let mut scripted = Vec::new();
    // Turn 1: create 2 tasks at step 0, then run 21 Read calls without touching the plan.
    scripted.push(ScriptedStep::Tool(
        "TaskWrite",
        json!({
            "tasks": [
                { "id": "stage-1", "subject": "Build desktop bundle", "status": "in_progress" },
                { "id": "stage-2", "subject": "Publish release", "status": "pending" }
            ]
        }),
    ));
    for _ in 0..21 {
        scripted.push(ScriptedStep::Tool("read", json!({ "path": read_path })));
    }
    scripted.push(ScriptedStep::Done);
    // Turn 2: continue with `#plan-step-*` IDs copied directly from the reminder.
    scripted.push(ScriptedStep::CompleteAllFromReminderWithHashPrefix);
    scripted.push(ScriptedStep::Done);

    let provider = RecordingCompatProvider::new(scripted);
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut options = agent_options(process.clone());
    options.provider = provider.clone();
    options.cwd = Some(root.path().to_string_lossy().into_owned());
    options.config = Some(json!({
        "model": MODEL,
        "modelSource": { "source": "bailian", "selection": format!("bailian:{MODEL}") },
        "prewarm": false,
        "autoMemory": false,
        "autoDream": false,
        "memoryRecall": false,
        "memory": { "enabled": false },
        "checkpointing": false,
        "speculativeCompaction": false,
        "earlyMicrocompact": false,
        "idleMicrocompact": false,
        "compactionSelfCheck": false,
        "loopDetection": false,
        "autoCompaction": false,
        "totalTokensReminder": "off",
        "approval_policy": "never",
        "sandbox_mode": "danger-full-access"
    }));
    options.tools = Some(vec![
        create_read_tool(),
        create_task_write_tool(),
        create_task_update_tool(),
        create_task_list_tool(),
    ]);
    options.include_default_agents = Some(false);

    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions {
        cwd: Some(root.path().to_string_lossy().into_owned().into()),
        user_config_dir: Some(root.path().to_string_lossy().into_owned().into()),
        ..Default::default()
    });
    let list_id_rt = session.task_list_id();
    let list_id = String::from_utf8(list_id_rt.to_node_utf8()).unwrap();
    let _tasks_cleanup = CleanupDir(klaus_agent_core::fs::paths::tasks_dir(&list_id).into());

    let turn1 = session
        .run(
            Input::Text("Run the full build and release pipeline.".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(turn1.error.is_none(), "{:?}", turn1.error);

    let turn1_requests = provider.requests.lock().unwrap().clone();
    // 1 (TaskWrite) + 21 (Read) + 1 (Done) = 23 requests.
    assert_eq!(turn1_requests.len(), 23);

    // Every request keeps `request.system` byte-identical and prefix-append-only.
    for pair in turn1_requests.windows(2) {
        assert_eq!(
            pair[0].system, pair[1].system,
            "request.system must remain byte-stable across periodic reminder injections"
        );
        assert!(
            !pair[1]
                .system
                .iter()
                .any(|part| part.name == "task-plan-reminder"),
            "task-plan-reminder must be demoted out of request.system"
        );
        assert!(
            pair[1].messages.starts_with(&pair[0].messages),
            "earlier message prefix (including prior reminder steps) must never be rewritten"
        );
    }

    // Request 0..9: only the initial runtime snapshot exists (1 step, no open-plan reminder).
    for req in &turn1_requests[..10] {
        let texts = step_texts(&req.messages);
        assert_eq!(texts.len(), 1);
        assert!(!texts[0].contains("task(s) still open in your task list:"));
    }

    // Request 10 (10 completed tool rounds): first open-plan reminder step is appended (2 steps total).
    let steps_at_10 = step_texts(&turn1_requests[10].messages);
    assert_eq!(steps_at_10.len(), 2);
    assert!(steps_at_10[1].contains("2 task(s) still open in your task list:"));

    // Requests 11..19 (1..9 rounds since reminder at request 10): suppressed, still 2 steps.
    for (offset, req) in turn1_requests[11..20].iter().enumerate() {
        assert_eq!(
            step_texts(&req.messages).len(),
            2,
            "request {} should not re-append before cadence threshold",
            11 + offset
        );
    }

    // Request 20 (10 tool rounds since request 10 without plan updates): re-appended at tail (3 steps)!
    let steps_at_20 = step_texts(&turn1_requests[20].messages);
    assert_eq!(steps_at_20.len(), 3);
    assert!(steps_at_20[2].contains("2 task(s) still open in your task list:"));
    assert!(
        is_step(turn1_requests[20].messages.last().unwrap()),
        "periodic reminder must be appended at the very tail of messages"
    );

    // Requests 21..22: quiet again after the round-20 refresh (still 3 steps).
    assert_eq!(step_texts(&turn1_requests[22].messages).len(), 3);

    // Turn 2: follow-up continuation must NOT wipe `[task-plan-reminder]` on round 0,
    // and `#plan-step-*` IDs must update durable task state to Completed.
    let turn2 = session
        .run(
            Input::Text("Continue and finish updating the checklist.".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(turn2.error.is_none(), "{:?}", turn2.error);

    let all_requests = provider.requests.lock().unwrap().clone();
    assert_eq!(all_requests.len(), 25);
    // Request 23 is Turn 2 Round 0: still 3 steps (no bogus snapshot wiping `[task-plan-reminder]`).
    assert_eq!(
        step_texts(&all_requests[23].messages).len(),
        3,
        "follow-up turn round 0 must preserve the active open-plan reminder without churn"
    );
    // Request 24 is after completing both tasks via `#plan-step-*`: snapshot clears `[task-plan-reminder]`.
    let final_steps = step_texts(&all_requests[24].messages);
    assert_eq!(final_steps.len(), 4);
    assert!(!final_steps[3].contains("task(s) still open in your task list:"));

    let tasks = list_plan_tasks(&list_id_rt, None).await.unwrap();
    assert_eq!(tasks.len(), 2);
    assert!(tasks
        .iter()
        .all(|task| task.status == TaskStatus::Completed));

    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn task_get_and_update_strip_leading_hash_through_public_session() {
    let root = tempfile::tempdir().unwrap();
    let scripted = vec![
        ScriptedStep::Tool(
            "TaskCreate",
            json!({ "subject": "First", "description": "step 1" }),
        ),
        ScriptedStep::Tool(
            "TaskCreate",
            json!({ "subject": "Second", "description": "step 2" }),
        ),
        ScriptedStep::Tool("TaskGet", json!({ "taskId": "#1" })),
        ScriptedStep::Tool(
            "TaskUpdate",
            json!({
                "taskId": "#2",
                "status": "in_progress",
                "addBlockedBy": ["#1"]
            }),
        ),
        ScriptedStep::Tool("TaskGet", json!({ "taskId": "#2" })),
        ScriptedStep::Done,
    ];

    let provider = RecordingCompatProvider::new(scripted);
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut options = agent_options(process.clone());
    options.provider = provider.clone();
    options.cwd = Some(root.path().to_string_lossy().into_owned());
    options.config = Some(json!({
        "model": MODEL,
        "modelSource": { "source": "bailian", "selection": format!("bailian:{MODEL}") },
        "prewarm": false,
        "autoMemory": false,
        "autoDream": false,
        "memoryRecall": false,
        "memory": { "enabled": false },
        "checkpointing": false,
        "speculativeCompaction": false,
        "earlyMicrocompact": false,
        "idleMicrocompact": false,
        "compactionSelfCheck": false,
        "loopDetection": false,
        "autoCompaction": false,
        "totalTokensReminder": "off",
        "approval_policy": "never",
        "sandbox_mode": "danger-full-access"
    }));
    options.tools = Some(vec![
        create_task_create_tool(),
        create_task_get_tool(),
        create_task_update_tool(),
        create_task_list_tool(),
    ]);
    options.include_default_agents = Some(false);

    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions {
        cwd: Some(root.path().to_string_lossy().into_owned().into()),
        user_config_dir: Some(root.path().to_string_lossy().into_owned().into()),
        ..Default::default()
    });
    let list_id_rt = session.task_list_id();
    let list_id = String::from_utf8(list_id_rt.to_node_utf8()).unwrap();
    let _tasks_cleanup = CleanupDir(klaus_agent_core::fs::paths::tasks_dir(&list_id).into());

    let turn = session
        .run(
            Input::Text("Create two tasks and link them using #id arguments.".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(turn.error.is_none(), "{:?}", turn.error);

    let history = session.history();
    let tool_outputs: Vec<String> = history
        .iter()
        .filter_map(|msg| match msg {
            Message::ToolResult(tr) => tr.content.first().and_then(|block| match block {
                OutputBlock::Text(t) => Some(String::from_utf8(t.text.to_node_utf8()).unwrap()),
                _ => None,
            }),
            _ => None,
        })
        .collect();
    assert_eq!(tool_outputs.len(), 5);
    assert!(tool_outputs[2].starts_with("Task #1: First\nStatus: pending"));
    assert_eq!(tool_outputs[3], "Updated #2 (status, blockedBy)");
    assert!(tool_outputs[4].contains("Blocked by: #1"));

    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}
