//! Defect: a queued workflow child mixed a newly published model with the
//! initiating step's inherited effort and delegation mode.
//! Root cause: workflow admission recaptured session inference after queueing
//! instead of retaining the same frozen parent guard as ordinary child spawns.
//! Work item: 20261024 workflow parent-step inference capture.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{
    resolve_config, InferencePublication, InferenceTarget, InferenceTargetState, InferenceTargets,
    Input, TurnOptions,
};
use klaus_agent_core::engine::workflow::runtime::{
    self, WorkflowScriptContext, WorkflowScriptRunner,
};
use klaus_agent_core::engine::workflow::WorkflowError;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeString};
use klaus_agent_core::schemas::config::ResolvedConfig;
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::effort::{EffortLevel, EffortValue};
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::tools::build_tool::text_result;
use klaus_agent_core::tools::builtins::task::{create_task_tool, TaskToolOptions};
use klaus_agent_core::tools::builtins::workflow::{
    create_workflow_tool, WorkflowMeta, WorkflowRunInput, WorkflowRunResult, WorkflowRunner,
    WorkflowRuntimeDeps, WorkflowToolOptions,
};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{TerminalReason, ToolError};
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::json::Json;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::{Tool, ToolContext, ToolProgress, ToolResult};
use serde_json::json;
use tokio::sync::Notify;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

const WAIT: Duration = Duration::from_secs(10);
const FIRST: &str = "workflow-capture-first";
const QUEUED: &str = "workflow-capture-queued";
const EXPLICIT: &str = "workflow-capture-explicit";
type Requests = Arc<Mutex<Vec<(String, ModelRequest)>>>;

#[derive(Default)]
struct Gate {
    queued: Notify,
    entered: Notify,
    release: Notify,
}

struct Provider {
    info: ProviderInfo,
    requests: Requests,
    gate: Arc<Gate>,
    root_started: AtomicBool,
    first_started: AtomicBool,
}

impl Provider {
    fn new(model: &str, requests: &Requests, gate: &Arc<Gate>) -> Arc<Self> {
        Arc::new(Self {
            info: ProviderInfo {
                name: model.into(),
                default_model: model.into(),
                base_url: None,
                env_key: None,
            },
            requests: requests.clone(),
            gate: gate.clone(),
            root_started: AtomicBool::new(false),
            first_started: AtomicBool::new(false),
        })
    }
}

fn carries(request: &ModelRequest, marker: &str) -> bool {
    request.messages.iter().any(|message| {
        matches!(message, Message::User(user) if user.content.iter().any(|block| {
            matches!(block, ContentBlock::Text(text) if text.text.contains(marker))
        }))
    })
}

fn stop(reason: ModelStopReason) -> ModelStreamEvent {
    ModelStreamEvent::MessageStop {
        stop_reason: reason,
        usage: Usage {
            input_tokens: 10,
            output_tokens: 1,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
            total_tokens: None,
        },
    }
}

fn tool_reply(name: &str, input: &str) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::ToolUseStart {
            id: format!("call-{name}").into(),
            name: name.into(),
            thought_signature: None,
            tool_kind: None,
            input_format: None,
        },
        ModelStreamEvent::ToolUseInputDelta {
            id: format!("call-{name}").into(),
            partial_json: input.into(),
        },
        ModelStreamEvent::ToolUseStop {
            id: format!("call-{name}").into(),
        },
        stop(ModelStopReason::ToolUse),
    ]
}

impl ModelProvider for Provider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: true,
            thinking: true,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            native_pdf: true,
            vision: true,
            image_limits: None,
            max_media_blocks: None,
            context_window: 200_000,
            max_output_tokens: 8_192,
        }
    }

    fn model_metadata(
        &self,
        _: &RuntimeString,
    ) -> klaus_agent_core::types::provider::ModelCapabilities {
        klaus_agent_core::types::provider::ModelCapabilities {
            supported_reasoning_efforts: ["low", "medium", "high"]
                .into_iter()
                .map(Into::into)
                .collect(),
            default_reasoning_effort: Some("medium".into()),
            ..Default::default()
        }
    }

    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        let first = carries(&request, FIRST);
        let child = first || carries(&request, QUEUED) || carries(&request, EXPLICIT);
        let launch = self.info.default_model == "capture-a"
            && !child
            && !self.root_started.swap(true, Ordering::SeqCst);
        let pause = first && !self.first_started.swap(true, Ordering::SeqCst);
        self.requests
            .lock()
            .unwrap()
            .push((self.info.name.try_to_utf8().unwrap(), request));
        let gate = self.gate.clone();
        Box::pin(async_stream::stream! {
            if pause {
                gate.entered.notify_one();
                tokio::select! {
                    _ = gate.release.notified() => {},
                    _ = signal.cancelled() => return,
                }
            }
            let events = if launch {
                tool_reply("workflow", r#"{"script":"export const meta = { name: 'capture-queue', description: 'Check queued child inference' }; return null;"}"#)
            } else if pause {
                tool_reply("capture_noop", "{}")
            } else {
                vec![
                    ModelStreamEvent::TextDelta { text: "Captured work completed.".into() },
                    stop(ModelStopReason::EndTurn),
                ]
            };
            for event in events {
                yield event;
            }
        })
    }
}

struct NoopTool;

#[async_trait]
impl Tool for NoopTool {
    fn static_name(&self) -> &str {
        "capture_noop"
    }
    fn static_description(&self) -> &str {
        "Continue the captured child with another inference step"
    }
    fn input_schema(&self) -> Json {
        runtime_json::parse(&RuntimeString::from(r#"{"type":"object"}"#)).unwrap()
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_read_only(&self, _: &Json) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _: &Json) -> bool {
        true
    }
    async fn call(
        &self,
        _: Json,
        _: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        Ok(text_result(Json::Null, "Continue the same child", false))
    }
}

struct QueuedScript {
    gate: Arc<Gate>,
}

#[async_trait]
impl WorkflowScriptRunner for QueuedScript {
    async fn run(&self, context: WorkflowScriptContext) -> Result<Json, WorkflowError> {
        let prompt = |text: &str| Json::String(text.into());
        let calls = async {
            let (first, queued, explicit) = futures::join!(
                context.sandbox.agent(prompt(FIRST), Json::Null),
                context.sandbox.agent(prompt(QUEUED), Json::Null),
                context.sandbox.agent(
                    prompt(EXPLICIT),
                    runtime_json::parse(&RuntimeString::from(r#"{"model":"low"}"#)).unwrap(),
                ),
            );
            Ok(Json::Array(vec![first?, queued?, explicit?]))
        };
        tokio::pin!(calls);
        // Poll all three calls before publication: one owns the runtime permit,
        // and two await it. The script executor is fixed; admission is real.
        assert!(futures::poll!(calls.as_mut()).is_pending());
        self.gate.queued.notify_one();
        calls.await
    }
}

struct QueuedRunner {
    gate: Arc<Gate>,
}

#[async_trait]
impl WorkflowRunner for QueuedRunner {
    async fn run<'a>(
        &self,
        input: WorkflowRunInput,
        deps: WorkflowRuntimeDeps<'a>,
    ) -> Result<WorkflowRunResult, RuntimeString> {
        let result = runtime::run_workflow(
            runtime::WorkflowRunInput {
                script: input.script,
                args: Some(input.args),
                resume_from_run_id: input.resume_from_run_id,
                budget_tokens: input.budget_tokens,
            },
            runtime::WorkflowRuntimeDeps {
                process_context: deps.process_context,
                spawn_workflow_agent: deps.spawn_workflow_agent,
                journal: deps.journal,
                emit: None,
                resolve_workflow: Some(deps.resolve_workflow),
                signal: deps.signal,
                max_concurrency: Some(1),
            },
            Arc::new(QueuedScript {
                gate: self.gate.clone(),
            }),
        )
        .await
        .map_err(|error| error.message)?;
        Ok(WorkflowRunResult {
            run_id: result.run_id,
            result: result.result,
            meta: WorkflowMeta {
                name: result.meta.name,
            },
            agents_spawned: result.agents_spawned,
            output_tokens_spent: result.output_tokens_spent,
        })
    }
}

fn selected(model: &str, switched: bool) -> ResolvedConfig {
    resolve_config(json!({
        "model": model, "prewarm": false, "fork": false, "maxTurns": 8,
        "thinking": true, "effort": if switched { "high" } else { "low" },
        "ultraMode": switched, "workflow": true,
        "approval_policy": "never", "sandbox_mode": "danger-full-access",
        "memory": {"enabled": false}, "autoMemory": false, "autoDream": false,
        "memoryRecall": false, "speculativeCompaction": false,
        "earlyMicrocompact": false, "idleMicrocompact": false,
        "compactionSelfCheck": false, "loopDetection": false,
    }))
    .unwrap()
}

fn publication(revision: u64, target: Arc<Provider>) -> InferencePublication {
    InferencePublication {
        expected_revision: revision,
        change_id: None,
        targets: Some(InferenceTargets {
            low: Some(InferenceTargetState::Ready(InferenceTarget {
                source: "local".into(),
                model_id: target.info.default_model.try_to_utf8().unwrap(),
                model: target.info.default_model.clone(),
                provider: target,
                pricing: Vec::new(),
                review_requirements: None,
            })),
            ..Default::default()
        }),
        advisor_target: None,
        orchestrator_target: None,
        orchestration_mode: false,
        feature_targets: Default::default(),
        source: None,
        rates: None,
        publication_key: None,
        review_requirements: None,
    }
}

#[tokio::test]
async fn queued_workflow_children_keep_the_initiating_step_and_explicit_target_after_parent_update()
{
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let gate = Arc::new(Gate::default());
    let a = Provider::new("capture-a", &requests, &gate);
    let b = Provider::new("capture-b", &requests, &gate);
    let c = Provider::new("capture-c", &requests, &gate);
    let d = Provider::new("capture-d", &requests, &gate);
    let mut options = agent_options(process.clone());
    options.provider = a.clone();
    options.config = Some(serde_json::to_value(selected("capture-a", false)).unwrap());
    options.include_default_agents = Some(false);
    options.tools = Some(vec![
        create_task_tool(TaskToolOptions {
            model_catalog: false,
            model_options: Vec::new(),
            agents: Vec::new(),
        }),
        Arc::new(NoopTool),
        create_workflow_tool(WorkflowToolOptions {
            workflows: None,
            allow_inline_script: Some(true),
            runner: Arc::new(QueuedRunner { gate: gate.clone() }),
        }),
    ]);
    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions {
        cwd: Some(dir.path().to_string_lossy().into_owned().into()),
        user_config_dir: Some(dir.path().to_string_lossy().into_owned().into()),
        ..Default::default()
    });
    assert_eq!(
        session
            .reconfigure_inference_with_targets(
                a,
                selected("capture-a", false),
                None,
                None,
                publication(0, c),
            )
            .unwrap(),
        1
    );
    let turn = session.run_streamed(
        Input::Text("Run the bounded workflow".into()),
        TurnOptions::default(),
    );
    let task = tokio::spawn(async move { turn.events.collect::<Vec<_>>().await });
    tokio::time::timeout(WAIT, gate.queued.notified())
        .await
        .unwrap();
    tokio::time::timeout(WAIT, gate.entered.notified())
        .await
        .unwrap();
    {
        let sent = requests.lock().unwrap();
        assert_eq!(
            sent.len(),
            2,
            "only the root and admitted first child may request inference: {sent:?}"
        );
        assert!(carries(&sent[1].1, FIRST));
    }
    assert_eq!(
        session
            .reconfigure_inference_with_targets(
                b,
                selected("capture-b", true),
                None,
                None,
                publication(1, d),
            )
            .unwrap(),
        2
    );
    gate.release.notify_one();
    let events = tokio::time::timeout(WAIT, task).await.unwrap().unwrap();
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
    assert!(
        events.iter().any(|event| matches!(
            event,
            EngineEvent::TurnCompleted {
                stop_reason: TerminalReason::Completed,
                ..
            }
        )),
        "{events:?}"
    );
    let sent = requests.lock().unwrap().clone();
    for (marker, model, count, effort) in [
        (FIRST, "capture-a", 2, EffortLevel::Low),
        (QUEUED, "capture-a", 1, EffortLevel::Low),
        (EXPLICIT, "capture-c", 1, EffortLevel::Medium),
    ] {
        let children: Vec<_> = sent
            .iter()
            .filter(|(_, request)| carries(request, marker))
            .collect();
        assert_eq!(children.len(), count, "{marker}: {sent:?}");
        for (provider, request) in children {
            assert_eq!(provider, model, "{marker}: {request:?}");
            assert_eq!(request.model, model);
            assert_eq!(request.effort, Some(EffortValue::Level(effort)));
            assert!(
                request.system.iter().any(|part| part
                    .text
                    .contains("Any earlier instruction enabling proactive multi-agent delegation no longer applies")),
                "{marker}: {:?}",
                request.system
            );
        }
    }
    let (provider, root_next) = sent.last().unwrap();
    assert_eq!(provider, "capture-b");
    assert_eq!(root_next.model, "capture-b");
    assert!(
        root_next.system.iter().any(|part| part
            .text
            .contains("Proactive multi-agent delegation is active")),
        "{:?}",
        root_next.system
    );
    assert_eq!(session.model_runtime().1.model, "capture-b");
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}
