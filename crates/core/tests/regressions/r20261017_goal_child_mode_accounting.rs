//! Defect: permission restrictions and collaboration mode shared goal billing rules.
//! Root cause: a delegated request could retain stale work or lose valid root usage.
//! Work item: 20261017 independent mode admission and actual descendant accounting.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::agent_definitions::AgentDefinition;
use klaus_agent_core::engine::deps::NoopCompactor;
use klaus_agent_core::engine::message_queue::QueuePriority;
use klaus_agent_core::engine::session::{EnqueueOptions, Input, Session, TurnOptions};
use klaus_agent_core::goal::ThreadGoalStatus;
use klaus_agent_core::planning::{types::PlanningSnapshot, CollaborationMode};
use klaus_agent_core::runtime_process::{
    remove_process_environment_var, set_process_environment_var, RuntimeProcessContext,
};
use klaus_agent_core::runtime_values::{runtime_json, RuntimeString};
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::tools::build_tool::text_result;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{TerminalReason, ToolError};
use klaus_agent_core::types::events::{EngineEvent, ToolDomainEvent};
use klaus_agent_core::types::json::Json;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::{
    SpawnSubAgentOptions, Tool, ToolContext, ToolProgress, ToolResult,
};
use serde_json::json;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

const WAIT: Duration = Duration::from_secs(15);
const MODEL: &str = "goal-child-mode";
const WARM: &str = "GOAL_CHILD_WARM_REQUEST";
const ROOT: &str = "GOAL_CHILD_ROOT_REQUEST";
const CHILD: &str = "GOAL_CHILD_ACTUAL_REQUEST";
const DELEGATE: &str = "fixture_goal_child_delegate";
const ROLE: &str = "goal-child-role";

#[derive(Clone, Copy)]
enum Case {
    CollaborationPlan,
    ReadOnlyPolicy,
}

impl Case {
    fn next_mode(self) -> CollaborationMode {
        match self {
            Self::CollaborationPlan => CollaborationMode::Plan,
            Self::ReadOnlyPolicy => CollaborationMode::Default,
        }
    }

    fn final_tokens(self) -> i64 {
        match self {
            Self::CollaborationPlan => 13,
            Self::ReadOnlyPolicy => 13 + 110 + 880 + 220,
        }
    }
}

struct Home {
    path: PathBuf,
    previous: Option<std::ffi::OsString>,
}

impl Home {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("klaus-goal-child-mode-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        let path = path.canonicalize().unwrap();
        let previous = std::env::var_os("KLAUS_CONFIG_DIR");
        set_process_environment_var("KLAUS_CONFIG_DIR", &path);
        Self { path, previous }
    }
}

impl Drop for Home {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(value) => set_process_environment_var("KLAUS_CONFIG_DIR", value),
            None => remove_process_environment_var("KLAUS_CONFIG_DIR"),
        }
        if std::thread::panicking() {
            eprintln!(
                "Retained actual goal-child fixture: {}",
                self.path.display()
            );
        } else {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

fn usage(input: u64, output: u64) -> Usage {
    Usage {
        input_tokens: input,
        output_tokens: output,
        cache_read_input_tokens: 0,
        cache_creation_input_tokens: 0,
        total_tokens: None,
    }
}

fn answer(text: &str, input: u64, output: u64) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::MessageStart {
            model: MODEL.into(),
        },
        ModelStreamEvent::TextDelta { text: text.into() },
        ModelStreamEvent::MessageStop {
            stop_reason: ModelStopReason::EndTurn,
            usage: usage(input, output),
        },
    ]
}

fn delegate() -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::MessageStart {
            model: MODEL.into(),
        },
        ModelStreamEvent::ToolUseStart {
            id: "actual-child-delegation".into(),
            name: DELEGATE.into(),
            thought_signature: None,
            tool_kind: None,
            input_format: None,
        },
        ModelStreamEvent::ToolUseInputDelta {
            id: "actual-child-delegation".into(),
            partial_json: json!({"prompt":CHILD}).to_string().into(),
        },
        ModelStreamEvent::ToolUseStop {
            id: "actual-child-delegation".into(),
        },
        ModelStreamEvent::MessageStop {
            stop_reason: ModelStopReason::ToolUse,
            usage: usage(100, 10),
        },
    ]
}

fn directive(request: &ModelRequest) -> String {
    request
        .messages
        .iter()
        .rev()
        .find_map(|message| {
            let Message::User(message) = message else {
                return None;
            };
            if message.is_meta == Some(true) {
                return None;
            }
            message.content.iter().rev().find_map(|block| {
                let ContentBlock::Text(text) = block else {
                    return None;
                };
                if text.harness_envelope.is_some() {
                    return None;
                }
                let text = text.text.try_to_utf8().unwrap();
                [CHILD, ROOT, WARM]
                    .into_iter()
                    .find(|needle| text.contains(*needle))
                    .map(str::to_owned)
            })
        })
        .expect("the actual request retains its user directive")
}

fn assert_request_mode(request: &ModelRequest, expected: CollaborationMode) {
    let parts = request
        .system
        .iter()
        .filter(|part| part.name == "collaboration-mode")
        .collect::<Vec<_>>();
    assert_eq!(parts.len(), 1, "one effective collaboration instruction");
    let header = match expected {
        CollaborationMode::Default => "<collaboration_mode>\n# Collaboration Mode: Default\n",
        CollaborationMode::Plan => "<collaboration_mode>\n# Collaboration Mode: Plan\n",
    };
    assert!(
        parts[0].text.try_to_utf8().unwrap().starts_with(header),
        "the actual provider request carries {expected:?}"
    );
}

struct Provider {
    info: ProviderInfo,
    case: Case,
    session: Mutex<Weak<Session>>,
    acknowledged: Mutex<Option<PlanningSnapshot>>,
    warm_calls: AtomicUsize,
    root_calls: AtomicUsize,
    child_calls: AtomicUsize,
    requests: Mutex<Vec<ModelRequest>>,
}

impl ModelProvider for Provider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
        assert!(!model.is_empty(), "capability lookup has a resolved model");
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
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        assert!(!signal.is_cancelled(), "the real request is admitted");
        let requested = directive(&request);
        let mode = if requested == WARM {
            CollaborationMode::Default
        } else {
            self.case.next_mode()
        };
        assert_request_mode(&request, mode);
        let mut requests = self.requests.lock().unwrap();
        assert!(
            requests.len() < 4,
            "no extra automatic or descendant requests"
        );
        requests.push(request);
        drop(requests);
        let session = self
            .session
            .lock()
            .unwrap()
            .upgrade()
            .expect("the root session remains alive");
        assert_eq!(
            session.planning_runtime().mode(),
            mode,
            "the captured active mode matches the real request"
        );
        assert_eq!(
            session.runtime_policy_state().sandbox_mode,
            Some(klaus_agent_core::types::permission::SandboxMode::ReadOnly),
            "collaboration and turn overrides preserve the saved ordinary policy"
        );
        let events = match requested.as_str() {
            WARM => {
                assert_eq!(self.warm_calls.fetch_add(1, Ordering::SeqCst), 0);
                match self.case {
                    Case::CollaborationPlan => {
                        let planning = session.planning_runtime();
                        let before = planning.snapshot().unwrap();
                        planning
                            .set_mode(CollaborationMode::Plan, Some(before.revision))
                            .unwrap();
                        let acknowledged = planning.snapshot().unwrap();
                        assert_eq!(acknowledged.mode, CollaborationMode::Plan);
                        assert!(acknowledged.revision > before.revision);
                        assert_eq!(
                            planning.mode(),
                            CollaborationMode::Default,
                            "already-admitted work keeps its execution mode"
                        );
                        *self.acknowledged.lock().unwrap() = Some(acknowledged);
                    }
                    Case::ReadOnlyPolicy => {
                        session
                            .enqueue(
                                Input::Text("Keep this accepted human work for later.".into()),
                                EnqueueOptions {
                                    priority: Some(QueuePriority::Later),
                                    ..Default::default()
                                },
                            )
                            .unwrap();
                    }
                }
                answer("The admitted execution checkpoint completed.", 11, 2)
            }
            CHILD => {
                assert_eq!(
                    self.child_calls.fetch_add(1, Ordering::SeqCst),
                    0,
                    "one actual child provider request"
                );
                answer("The actual delegated investigation completed.", 800, 80)
            }
            ROOT => match self.root_calls.fetch_add(1, Ordering::SeqCst) {
                0 => delegate(),
                1 => answer("The explicit user request is complete.", 200, 20),
                call => panic!("unexpected root provider request {call}"),
            },
            other => panic!("unexpected actual user directive {other}"),
        };
        Box::pin(futures::stream::iter(events))
    }
}

struct ChildRun {
    id: RuntimeString,
    parent_planning_read_only: bool,
}

struct DelegateTool {
    children: Arc<Mutex<Vec<ChildRun>>>,
}

#[async_trait::async_trait]
impl Tool for DelegateTool {
    fn static_name(&self) -> &str {
        DELEGATE
    }
    fn static_description(&self) -> &str {
        "Run one actual read-only investigation child."
    }
    fn input_schema(&self) -> Json {
        runtime_json::parse(&RuntimeString::from(r#"{"type":"object","properties":{"prompt":{"type":"string"}},"required":["prompt"],"additionalProperties":false}"#)).unwrap()
    }
    fn is_enabled(&self) -> bool {
        true
    }
    fn is_read_only(&self, _: &Json) -> bool {
        true
    }
    fn is_concurrency_safe(&self, _: &Json) -> bool {
        false
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let spawn = ctx
            .spawn_sub_agent
            .as_ref()
            .ok_or_else(|| ToolError::fatal("actual child dispatch is unavailable"))?;
        let prompt = input
            .as_object()
            .and_then(|object| object.get("prompt"))
            .and_then(Json::as_str)
            .ok_or_else(|| ToolError::fatal("child prompt is missing"))?
            .clone();
        let run = spawn(SpawnSubAgentOptions {
            prompt,
            agent_type: Some(ROLE.into()),
            ..Default::default()
        })
        .await
        .map_err(ToolError::new)?;
        let id = run
            .agent_id
            .as_str()
            .expect("the real foreground child exposes its identity")
            .clone();
        {
            let mut children = self.children.lock().unwrap();
            assert!(children.is_empty(), "one actual spawn");
            children.push(ChildRun {
                id,
                parent_planning_read_only: ctx.planning_read_only,
            });
        }
        let mut events = run.events;
        let mut completed = false;
        let mut event_count = 0;
        while let Some(event) = events.next().await {
            event_count += 1;
            assert!(event_count <= 256, "bounded actual child event stream");
            match event {
                EngineEvent::TurnFailed { error, .. } => {
                    return Err(ToolError::fatal(error.message))
                }
                EngineEvent::TurnCompleted {
                    stop_reason: TerminalReason::Completed,
                    ..
                } => completed = true,
                EngineEvent::TurnCompleted { stop_reason, .. } => {
                    return Err(ToolError::fatal(format!(
                        "actual child ended with {stop_reason:?}"
                    )))
                }
                _ => {}
            }
        }
        if !completed {
            return Err(ToolError::fatal("the actual child did not complete"));
        }
        if let Some(progress) = on_progress {
            progress(ToolDomainEvent::Status {
                message: "The actual child completed.".into(),
            })
            .await;
        }
        Ok(text_result(input, "The actual child completed.", false))
    }
}

async fn run(session: &Arc<Session>, prompt: &str, options: TurnOptions) {
    let mut stream = session
        .run_streamed(Input::Text(prompt.into()), options)
        .events;
    let mut events = Vec::new();
    tokio::time::timeout(WAIT, async {
        while let Some(event) = stream.next().await {
            assert!(events.len() < 256, "bounded explicit root event stream");
            events.push(event);
        }
    })
    .await
    .expect("the actual explicit root request completes");
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{events:#?}"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                EngineEvent::TurnCompleted {
                    stop_reason: TerminalReason::Completed,
                    ..
                }
            ))
            .count(),
        1,
        "one successful actual root turn: {events:#?}"
    );
}

async fn check(case: Case) {
    let home = Home::new();
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let provider = Arc::new(Provider {
        info: ProviderInfo {
            name: MODEL.into(),
            default_model: MODEL.into(),
            base_url: None,
            env_key: None,
        },
        case,
        session: Mutex::new(Weak::new()),
        acknowledged: Mutex::new(None),
        warm_calls: AtomicUsize::new(0),
        root_calls: AtomicUsize::new(0),
        child_calls: AtomicUsize::new(0),
        requests: Mutex::new(Vec::new()),
    });
    let children = Arc::new(Mutex::new(Vec::new()));
    let mut options = agent_options(process.clone());
    options.config.as_mut().unwrap()["sandbox_mode"] = json!("read-only");
    options.config.as_mut().unwrap()["model"] = json!(MODEL);
    options.provider = provider.clone();
    options.tools = Some(vec![Arc::new(DelegateTool {
        children: children.clone(),
    })]);
    options.agents = Some(vec![AgentDefinition {
        name: ROLE.into(),
        description: "An actual child with a deliberately looser declared I/O policy.".into(),
        runtime_policy: Some(klaus_agent_core::engine::session::RuntimePolicyUpdate {
            approval_policy: Some(klaus_agent_core::types::permission::ApprovalPolicy::Never),
            sandbox_mode: Some(klaus_agent_core::types::permission::SandboxMode::DangerFullAccess),
            approvals_reviewer: None,
            sandbox_workspace_write: None,
            network_allowed: Some(true),
        }),
        ..Default::default()
    }]);
    options.include_default_agents = Some(false);
    options.compactor = Some(Arc::new(NoopCompactor));
    options.cwd = Some(home.path.to_string_lossy().into_owned());
    let agent = Agent::new(options);
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(home.path.join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let session = agent.open_session(store.clone(), StartSessionOptions {
        cwd: Some(home.path.to_string_lossy().into_owned().into()),
        user_config_dir: Some(home.path.to_string_lossy().into_owned().into()),
        config: Some(json!({"model":MODEL,"goal":true,"sandbox_mode":"read-only","approval_policy":"never","prewarm":false,"autoMemory":false,"autoDream":false,"agentMemory":false,"memoryRecall":false,"memory":{"enabled":false},"idleMicrocompact":false,"earlyMicrocompact":false,"speculativeCompaction":false,"loopDetection":false,"maxRetries":0,"forwardSubagentText":false})),
        ..Default::default()
    }).await.unwrap();
    *provider.session.lock().unwrap() = Arc::downgrade(&session);
    assert_eq!(session.collaboration_mode(), CollaborationMode::Default);
    session
        .set_thread_goal(
            Some("Finish the authorized delegated objective.".into()),
            None,
            None,
        )
        .await
        .unwrap();

    run(&session, WARM, TurnOptions::default()).await;
    let before = session.thread_goal().await.unwrap().unwrap();
    assert_eq!(before.status, ThreadGoalStatus::Active);
    assert_eq!(
        before.tokens_used, 13,
        "already-admitted Default work is really billed"
    );
    assert_eq!(session.collaboration_mode(), case.next_mode());
    assert_eq!(
        session.runtime_policy_state().sandbox_mode,
        Some(klaus_agent_core::types::permission::SandboxMode::ReadOnly)
    );
    assert!(
        session
            .run_goal_idle_streamed(TurnOptions::default())
            .await
            .unwrap()
            .is_none(),
        "Plan or accepted pending human work prevents an automatic request"
    );
    assert_eq!(provider.requests.lock().unwrap().len(), 1);
    if let Case::CollaborationPlan = case {
        assert_eq!(
            session.planning_runtime().snapshot().unwrap(),
            provider.acknowledged.lock().unwrap().clone().unwrap(),
            "settlement preserves the acknowledged next Plan mode"
        );
    }

    run(
        &session,
        ROOT,
        TurnOptions {
            ..Default::default()
        },
    )
    .await;
    let after = session.thread_goal().await.unwrap().unwrap();
    assert_eq!(after.goal_id, before.goal_id);
    assert_eq!(after.status, ThreadGoalStatus::Active);
    assert_eq!(after.tokens_used, case.final_tokens(), "actual descendant billing follows admitted collaboration work, independently of I/O restrictions");
    assert_eq!(
        after.tokens_used - before.tokens_used,
        match case {
            Case::CollaborationPlan => 0,
            Case::ReadOnlyPolicy => 1210,
        }
    );
    assert_eq!(session.collaboration_mode(), case.next_mode());
    assert_eq!(
        session.runtime_policy_state().sandbox_mode,
        Some(klaus_agent_core::types::permission::SandboxMode::ReadOnly)
    );
    assert_eq!(provider.warm_calls.load(Ordering::SeqCst), 1);
    assert_eq!(provider.root_calls.load(Ordering::SeqCst), 2);
    assert_eq!(provider.child_calls.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests.lock().unwrap().len(), 4);
    if let Case::CollaborationPlan = case {
        assert!(session
            .run_goal_idle_streamed(TurnOptions::default())
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            provider.requests.lock().unwrap().len(),
            4,
            "an Active Plan goal remains idle"
        );
    }

    let (child_id, parent_planning_read_only) = {
        let children = children.lock().unwrap();
        assert_eq!(children.len(), 1);
        (
            children[0].id.try_to_utf8().unwrap(),
            children[0].parent_planning_read_only,
        )
    };
    assert_eq!(
        parent_planning_read_only,
        match case {
            Case::CollaborationPlan => true,
            Case::ReadOnlyPolicy => false,
        }
    );
    tokio::time::timeout(WAIT, session.shutdown_and_wait())
        .await
        .expect("bounded fixture session shutdown")
        .unwrap();
    let child = store
        .load_sub_agent(&session.id.try_to_utf8().unwrap(), &child_id)
        .await
        .unwrap();
    let metadata = child
        .meta
        .expect("the actual child persists its resolved provenance");
    assert_eq!(metadata.agent_type, Some(ROLE.into()));
    assert_eq!(
        metadata.permission_mode, None,
        "new child metadata does not persist a retired permission preset"
    );
    let persisted = klaus_agent_core::goal::store::get_goal(&session.id.try_to_utf8().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(persisted.goal_id, before.goal_id);
    assert_eq!(
        persisted.tokens_used,
        case.final_tokens(),
        "actual child cleanup cannot debit usage twice"
    );
    tokio::time::timeout(WAIT, process.shutdown())
        .await
        .expect("bounded fixture process shutdown")
        .unwrap();
}

#[tokio::test]
async fn actual_plan_descendants_do_not_reuse_a_previous_execution_goal_scope() {
    check(Case::CollaborationPlan).await;
}

#[tokio::test]
async fn read_only_policy_descendants_retain_the_admitted_execution_goal_scope() {
    check(Case::ReadOnlyPolicy).await;
}
