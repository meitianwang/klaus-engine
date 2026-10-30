//! Defect: cancelled children exposed progress as a successful result, and workflow
//! joins erased failed terminal states. Root cause: terminal collection accepted
//! progress without completion and result retrieval ignored interrupted status.
//! Work item: 2026-10-14 explicit child cancellation and incomplete results.

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};
use async_trait::async_trait;
use futures::FutureExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::engine::workflow::runtime::{
    run_workflow, Thunk, WorkflowRunInput, WorkflowRuntimeDeps, WorkflowScriptContext,
    WorkflowScriptRunner,
};
use klaus_agent_core::engine::workflow::WorkflowError;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{RuntimeObject, RuntimeString};
use klaus_agent_core::schemas::content::{ContentBlock, OutputBlock};
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::schemas::subagent_lifecycle::SubagentStatus;
use klaus_agent_core::tools::build_tool::text_result;
use klaus_agent_core::tools::builtins::task::{create_task_tool, TaskToolOptions};
use klaus_agent_core::tools::builtins::task_control::create_task_output_tool;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{TerminalReason, ToolError};
use klaus_agent_core::types::events::{EngineEvent, ThreadItem};
use klaus_agent_core::types::json::{Json, JsonSchema};
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::{
    BackgroundTaskStatus, SpawnSubAgent, SpawnSubAgentOptions, Tool, ToolContext, ToolProgress,
    ToolResult, WorkflowJournal, WorkflowJournalEntry,
};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

const PROGRESS: &str = "The investigation has started; the result is not ready.";

fn stopped(reason: ModelStopReason) -> ModelStreamEvent {
    ModelStreamEvent::MessageStop {
        stop_reason: reason,
        usage: Usage {
            input_tokens: 10,
            output_tokens: 4,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
            total_tokens: None,
        },
    }
}

fn tool_events(name: &str, input: Value, progress: bool) -> Vec<ModelStreamEvent> {
    let mut events = vec![ModelStreamEvent::MessageStart {
        model: "cancellation-test".into(),
    }];
    if progress {
        events.push(ModelStreamEvent::TextDelta {
            text: PROGRESS.into(),
        });
    }
    events.extend([
        ModelStreamEvent::ToolUseStart {
            id: name.into(),
            name: name.into(),
            thought_signature: None,
            tool_kind: None,
            input_format: None,
        },
        ModelStreamEvent::ToolUseInputDelta {
            id: name.into(),
            partial_json: input.to_string().into(),
        },
        ModelStreamEvent::ToolUseStop { id: name.into() },
        stopped(ModelStopReason::ToolUse),
    ]);
    events
}

struct CancellationProvider {
    info: ProviderInfo,
    calls: AtomicUsize,
    background: bool,
}

impl ModelProvider for CancellationProvider {
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
    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        let child = request.messages.iter().any(|message| {
            match message {
            Message::User(message) => message.content.iter().any(|block| {
                matches!(block, ContentBlock::Text(text) if text.text == "cancel-after-progress")
            }),
            _ => false,
        }
        });
        let events = if child {
            tool_events("fixture_interrupt", json!({}), true)
        } else {
            match self.calls.fetch_add(1, Ordering::SeqCst) {
                0 => tool_events(
                    "task",
                    json!({"model": "low",
                        "description":"cancel after progress", "prompt":"cancel-after-progress",
                        "fork_turns":"none", "run_in_background":self.background,
                    }),
                    false,
                ),
                1 if self.background => tool_events("fixture_collect", json!({}), false),
                _ => vec![
                    ModelStreamEvent::MessageStart {
                        model: "cancellation-test".into(),
                    },
                    ModelStreamEvent::TextDelta {
                        text: "Cancellation handled.".into(),
                    },
                    stopped(ModelStopReason::EndTurn),
                ],
            }
        };
        Box::pin(futures::stream::iter(events))
    }
}

#[derive(Debug)]
struct Observation {
    source: &'static str,
    is_error: Option<bool>,
    output: Json,
    text: RuntimeString,
}

fn observe(source: &'static str, result: &ToolResult) -> Observation {
    let mut text = RuntimeString::default();
    for block in &result.serializable.model_content {
        if let OutputBlock::Text(block) = block {
            text = text.concat(&block.text);
        }
    }
    Observation {
        source,
        is_error: result.serializable.is_error,
        output: result.serializable.output.clone(),
        text,
    }
}

struct RecordingTask {
    inner: Arc<dyn Tool>,
    observations: mpsc::UnboundedSender<Observation>,
}

#[async_trait]
impl Tool for RecordingTask {
    fn static_name(&self) -> &str {
        "task"
    }
    fn description(&self) -> RuntimeString {
        self.inner.description()
    }
    fn input_schema(&self) -> Json {
        self.inner.input_schema()
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, input: &Json) -> bool {
        self.inner.is_concurrency_safe(input)
    }
    fn is_read_only(&self, input: &Json) -> bool {
        self.inner.is_read_only(input)
    }
    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let result = self.inner.call(input, ctx, progress).await?;
        let _ = self.observations.send(observe("task", &result));
        Ok(result)
    }
}

struct ControlTool {
    interrupt: bool,
    schema: JsonSchema,
    observations: mpsc::UnboundedSender<Observation>,
}

#[async_trait]
impl Tool for ControlTool {
    fn static_name(&self) -> &str {
        if self.interrupt {
            "fixture_interrupt"
        } else {
            "fixture_collect"
        }
    }
    fn static_description(&self) -> &str {
        "Control the cancellation fixture."
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _: &Json) -> bool {
        true
    }
    fn is_read_only(&self, _: &Json) -> bool {
        true
    }
    async fn call(
        &self,
        _: Json,
        ctx: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        if self.interrupt {
            ctx.signal.cancel();
            return Ok(text_result(Json::Bool(true), "Fixture interrupted.", false));
        }
        let handles = ctx.background_tasks.as_ref().unwrap().list();
        assert_eq!(handles.len(), 1);
        let mut input = RuntimeObject::new();
        input.insert("task_id", Json::String(handles[0].agent_id.clone()));
        input.insert("block", Json::Bool(true));
        input.insert("timeout", Json::from(5000));
        let result = create_task_output_tool()
            .call(Json::Object(input), ctx, None)
            .await?;
        let _ = self.observations.send(observe("task_output", &result));
        Ok(result)
    }
}

async fn cancellation_fixture(background: bool) {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let (tx, mut observations) = mpsc::unbounded_channel();
    let mut options = agent_options(process.clone());
    options.provider = Arc::new(CancellationProvider {
        info: ProviderInfo {
            name: "cancellation-test".into(),
            default_model: "cancellation-test".into(),
            base_url: None,
            env_key: None,
        },
        calls: AtomicUsize::new(0),
        background,
    });
    options.tools = Some(vec![
        Arc::new(RecordingTask {
            inner: create_task_tool(TaskToolOptions::default()),
            observations: tx.clone(),
        }),
        Arc::new(ControlTool {
            interrupt: true,
            schema: JsonSchema::new(),
            observations: tx.clone(),
        }),
        Arc::new(ControlTool {
            interrupt: false,
            schema: JsonSchema::new(),
            observations: tx,
        }),
    ]);
    options.config.as_mut().unwrap()["model"] = json!("cancellation-test");
    options.config.as_mut().unwrap()["agentMemory"] = json!(false);
    options.config.as_mut().unwrap()["autoMemory"] = json!(false);
    let directory =
        std::env::temp_dir().join(format!("klaus-cancellation-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let directory_host = directory.to_string_lossy().into_owned();
    let session = Agent::new(options).start_session(StartSessionOptions {
        cwd: Some(directory_host.clone().into()),
        user_config_dir: Some(directory_host.into()),
        ..Default::default()
    });
    let notices = Arc::new(Mutex::new(Vec::new()));
    let notices_sink = notices.clone();
    session.set_agent_notification_sink(Some(Arc::new(move |notice| {
        notices_sink.lock().unwrap().push(notice.clone())
    })));
    let outcome = std::panic::AssertUnwindSafe(async {
        tokio::time::timeout(
            Duration::from_secs(20),
            session.run(
                Input::Text("Run cancellation fixture".into()),
                TurnOptions::default(),
            ),
        )
        .await
        .unwrap();
        let source = if background { "task_output" } else { "task" };
        let observed = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let result = observations.recv().await.unwrap();
                if result.source == source {
                    break result;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(observed.is_error, Some(true), "{observed:?}");
        assert!(observed.text.contains("interrupted"), "{observed:?}");
        let records = session.subagent_lifecycle_snapshot().agents;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, SubagentStatus::Cancelled);
        assert!(records[0].last_result.is_none());
        assert!(notices
            .lock()
            .unwrap()
            .iter()
            .all(|notice| notice.status != "completed"));
        if background {
            assert_eq!(
                observed.output.get("status").and_then(Json::as_str),
                Some(&RuntimeString::from("interrupted"))
            );
            assert_eq!(
                observed
                    .output
                    .get("retrieval_status")
                    .and_then(Json::as_str),
                Some(&RuntimeString::from("interrupted"))
            );
            let summaries = session.background_agent_summaries();
            assert_eq!(summaries[0].status, BackgroundTaskStatus::Interrupted);
            assert!(summaries[0].transcript.contains(PROGRESS));
            assert!(summaries[0].transcript.contains("not a completed result"));
            assert!(
                notices.lock().unwrap().is_empty(),
                "cancellation must not deliver a completion notice"
            );
        }
    })
    .catch_unwind()
    .await;
    let files: Vec<_> = session
        .background_agent_summaries()
        .into_iter()
        .map(|entry| entry.output_file)
        .collect();
    session.interrupt();
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    for path in files {
        if std::path::Path::new(&path).exists() {
            std::fs::remove_file(path).unwrap();
        }
    }
    std::fs::remove_dir_all(directory).unwrap();
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn foreground_cancellation_after_progress_is_an_error_without_completion_notice() {
    cancellation_fixture(false).await;
}

#[tokio::test]
async fn background_cancellation_preserves_partial_activity_and_error_retrieval() {
    cancellation_fixture(true).await;
}

#[derive(Default)]
struct Journal(Mutex<Vec<WorkflowJournalEntry>>);
#[async_trait]
impl WorkflowJournal for Journal {
    async fn append(&self, entry: WorkflowJournalEntry) -> std::io::Result<()> {
        self.0.lock().unwrap().push(entry);
        Ok(())
    }
    fn prior(&self, _: &RuntimeString) -> Vec<WorkflowJournalEntry> {
        self.0.lock().unwrap().clone()
    }
}

struct JoiningRunner;
#[async_trait]
impl WorkflowScriptRunner for JoiningRunner {
    async fn run(&self, ctx: WorkflowScriptContext) -> Result<Json, WorkflowError> {
        let sandbox = ctx.sandbox;
        let child = sandbox.clone();
        let results = sandbox
            .parallel(Some(vec![Thunk::Fn(Box::new(move || {
                child.agent(Json::from("child"), Json::Null)
            }))]))
            .await?;
        Ok(Json::Array(results))
    }
}

#[tokio::test]
async fn workflow_join_rejects_aborted_and_missing_terminal_progress_without_journaling() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    for terminal in [Some(TerminalReason::Aborted), None] {
        let spawn: SpawnSubAgent = Arc::new(move |_: SpawnSubAgentOptions| {
            let mut events = vec![EngineEvent::ItemCompleted {
                inference: None,
                item: ThreadItem::AgentMessage {
                    id: "progress".into(),
                    text: PROGRESS.into(),
                    phase: Some("commentary".into()),
                },
            }];
            if let Some(stop_reason) = terminal {
                events.push(EngineEvent::TurnCompleted {
                    turn_id: "child".into(),
                    usage: None,
                    stop_reason,
                });
            }
            Box::pin(futures::stream::iter(events))
        });
        let journal = Arc::new(Journal::default());
        let result = run_workflow(WorkflowRunInput {
            script:"export const meta = {name:'cancellation',description:'test',phases:[]}\nreturn null".into(),
            args:None, resume_from_run_id:None, budget_tokens:None,
        }, WorkflowRuntimeDeps {
            process_context:process.clone(), spawn_workflow_agent:spawn, journal:Some(journal.clone()),
            emit:None, resolve_workflow:None, signal:AbortSignal::new(), max_concurrency:Some(1),
        }, Arc::new(JoiningRunner)).await.unwrap();
        let branch = &result.result.as_array().unwrap()[0];
        assert_eq!(branch.get("ok").and_then(Json::as_bool), Some(false));
        let error = branch.get("error").and_then(Json::as_str).unwrap();
        assert!(error.contains(if terminal.is_some() {
            "interrupted"
        } else {
            "without a terminal event"
        }));
        assert!(error.contains("Partial activity; not a completed result"));
        assert!(error.contains(PROGRESS));
        assert!(journal.0.lock().unwrap().is_empty());
    }
    process.shutdown().await.unwrap();
}
