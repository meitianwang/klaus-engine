//! Defect: automatic memory extraction could not approve writes to a custom sensitive root.
//! Root cause: the restricted fork dropped the session approval callback when building its query.
//! Fixed by: recovery of the memory approval regression originally recorded in 8399e9e.

use async_trait::async_trait;
use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, Session, TurnOptions};
use klaus_agent_core::memory::agent_memory::{
    AgentMemoryHost, AgentMemoryLayout, AgentMemoryScope,
};
use klaus_agent_core::memory::maintenance::{
    AutoMemoryRun, AutoMemoryRunReason, MemoryMaintenanceStatus,
};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::tools::build_tool::text_result;
use klaus_agent_core::tools::builtins::write::create_write_tool;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::ToolError;
use klaus_agent_core::types::json::{Json, JsonSchema};
use klaus_agent_core::types::permission::{
    ApprovalHandler, ApprovalRequest, ApprovalResponse, ExternalPermissionMode,
};
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::tool::{
    ForkContext, SpawnSubAgentOptions, Tool, ToolContext, ToolProgress, ToolResult,
};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let root =
            std::env::temp_dir().join(format!("klaus-memory-approval-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        Self(root)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct CustomMemoryHost {
    root: PathBuf,
}

impl AgentMemoryLayout for CustomMemoryHost {
    fn dir_for(&self, agent_type: &str, scope: AgentMemoryScope) -> String {
        self.root
            .join(agent_type)
            .join(scope.as_str())
            .to_string_lossy()
            .into_owned()
    }

    fn snapshot_dir_for(&self, agent_type: &str) -> String {
        self.root
            .join("snapshots")
            .join(agent_type)
            .to_string_lossy()
            .into_owned()
    }
}

#[async_trait]
impl AgentMemoryHost for CustomMemoryHost {
    fn layout(&self) -> &dyn AgentMemoryLayout {
        self
    }
}

#[derive(Default)]
struct RecordingApprovalHandler {
    requests: Mutex<Vec<ApprovalRequest>>,
}

#[async_trait]
impl ApprovalHandler for RecordingApprovalHandler {
    async fn approve(&self, request: ApprovalRequest, _: AbortSignal) -> ApprovalResponse {
        self.requests.lock().unwrap().push(request);
        ApprovalResponse::Allow {
            updated_input: None,
            updated_permissions: None,
        }
    }
}

enum Reply {
    Text,
    Tool(&'static str, serde_json::Value),
}

struct ScriptedProvider {
    info: ProviderInfo,
    replies: Mutex<VecDeque<Reply>>,
    tool_errors: Mutex<Vec<bool>>,
}

impl ScriptedProvider {
    fn new(replies: Vec<Reply>) -> Self {
        Self {
            info: ProviderInfo {
                name: "memory-approval-regression".into(),
                base_url: None,
                env_key: None,
                default_model: "memory-approval-model".into(),
            },
            replies: Mutex::new(replies.into()),
            tool_errors: Mutex::new(Vec::new()),
        }
    }
}

fn stop(reason: ModelStopReason) -> ModelStreamEvent {
    ModelStreamEvent::MessageStop {
        stop_reason: reason,
        usage: Usage {
            input_tokens: 10,
            output_tokens: 10,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
            total_tokens: None,
        },
    }
}

impl ModelProvider for ScriptedProvider {
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
        self.tool_errors
            .lock()
            .unwrap()
            .extend(request.messages.iter().filter_map(|message| match message {
                Message::ToolResult(result) => Some(result.is_error),
                _ => None,
            }));
        let reply = self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected provider request beyond the scripted parent and child turns");
        let mut events = vec![ModelStreamEvent::MessageStart {
            model: "memory-approval-model".into(),
        }];
        match reply {
            Reply::Text => {
                events.push(ModelStreamEvent::TextDelta {
                    text: "Complete.".into(),
                });
                events.push(stop(ModelStopReason::EndTurn));
            }
            Reply::Tool(name, input) => {
                assert!(
                    request.tools.iter().any(|tool| tool.name == name),
                    "the child must actually expose {name}"
                );
                events.extend([
                    ModelStreamEvent::ToolUseStart {
                        id: format!("regression-{name}").into(),
                        name: name.into(),
                        thought_signature: None,
                        tool_kind: None,
                        input_format: None,
                    },
                    ModelStreamEvent::ToolUseInputDelta {
                        id: format!("regression-{name}").into(),
                        partial_json: input.to_string().into(),
                    },
                    ModelStreamEvent::ToolUseStop {
                        id: format!("regression-{name}").into(),
                    },
                    stop(ModelStopReason::ToolUse),
                ]);
            }
        }
        Box::pin(futures::stream::iter(events))
    }
}

fn session_config(auto_memory: bool) -> serde_json::Value {
    serde_json::json!({
        "model": "memory-approval-model",
        "permissionMode": "default",
        "approval_policy": "on-request",
        "sandbox_mode": "workspace-write",
        "sandbox": "process",
        "requireRealSandbox": false,
        "agentMemory": auto_memory,
        "autoMemory": auto_memory,
        "autoDream": false,
        "speculativeCompaction": false,
        "maxRetries": 0,
        "fork": false
    })
}

async fn wait_for_memory(session: &Arc<Session>) -> AutoMemoryRun {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let snapshot = session.memory_maintenance_snapshot().unwrap();
            if let Some(run) = snapshot.auto_memory_runs.into_iter().find(|run| {
                matches!(
                    run.status,
                    MemoryMaintenanceStatus::Completed | MemoryMaintenanceStatus::Failed
                )
            }) {
                return run;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the public maintenance snapshot must publish a terminal extraction outcome")
}

async fn memory_case(with_handler: bool) {
    let directory = TestDirectory::new();
    // A custom root below .klaus needs approval even though memory tools are allowed.
    let host = Arc::new(CustomMemoryHost {
        root: directory.0.join(".klaus/host-selected-memory"),
    });
    let memory_dir = PathBuf::from(host.dir_for("main", AgentMemoryScope::Project));
    std::fs::create_dir_all(&memory_dir).unwrap();
    let provider = Arc::new(ScriptedProvider::new(vec![
        Reply::Text,
        Reply::Tool(
            "write",
            serde_json::json!({"path": "MEMORY.md", "content": "approved\n"}),
        ),
        Reply::Text,
    ]));
    let handler = Arc::new(RecordingApprovalHandler::default());
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
    options.provider = provider.clone();
    options.config = Some(session_config(true));
    options.cwd = Some(directory.0.to_string_lossy().into_owned());
    options.tools = Some(vec![create_write_tool()]);
    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions {
        cwd: Some(directory.0.to_string_lossy().into_owned().into()),
        user_config_dir: Some(
            directory
                .0
                .join("config")
                .to_string_lossy()
                .into_owned()
                .into(),
        ),
        config: Some(session_config(true)),
        permission_mode: Some(ExternalPermissionMode::Default),
        agent_memory: Some(host),
        approval_handler: with_handler.then(|| handler.clone() as Arc<dyn ApprovalHandler>),
        ..Default::default()
    });

    assert_eq!(
        session.runtime_policy_state().approval_policy,
        Some(klaus_agent_core::types::permission::ApprovalPolicy::OnRequest)
    );
    session
        .run(
            Input::Text("Remember the approved project preference.".into()),
            TurnOptions::default(),
        )
        .await;
    let run = wait_for_memory(&session).await;
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();

    assert!(provider.replies.lock().unwrap().is_empty());
    let requests = handler.requests.lock().unwrap();
    if with_handler {
        assert_eq!(run.status, MemoryMaintenanceStatus::Completed, "{run:?}");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].tool_name, RuntimeString::from("write"));
        assert_eq!(
            std::fs::read_to_string(memory_dir.join("MEMORY.md")).unwrap(),
            "approved\n"
        );
    } else {
        assert_eq!(run.status, MemoryMaintenanceStatus::Failed, "{run:?}");
        assert_eq!(run.reason, Some(AutoMemoryRunReason::ToolFailure));
        assert!(requests.is_empty());
        assert!(!memory_dir.join("MEMORY.md").exists());
        assert!(provider.tool_errors.lock().unwrap().contains(&true));
    }
}

pub(super) async fn assert_internal_memory_maintenance_is_not_a_user_subagent() {
    let directory = TestDirectory::new();
    let host = Arc::new(CustomMemoryHost {
        root: directory.0.join(".klaus/host-selected-memory"),
    });
    let memory_dir = PathBuf::from(host.dir_for("main", AgentMemoryScope::Project));
    std::fs::create_dir_all(&memory_dir).unwrap();
    let provider = Arc::new(ScriptedProvider::new(vec![Reply::Text, Reply::Text]));
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
    options.provider = provider;
    options.config = Some(session_config(true));
    options.cwd = Some(directory.0.to_string_lossy().into_owned());
    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions {
        cwd: Some(directory.0.to_string_lossy().into_owned().into()),
        user_config_dir: Some(
            directory
                .0
                .join("config")
                .to_string_lossy()
                .into_owned()
                .into(),
        ),
        config: Some(session_config(true)),
        agent_memory: Some(host),
        ..Default::default()
    });

    session
        .run(
            Input::Text("Remember this project preference.".into()),
            TurnOptions::default(),
        )
        .await;
    let run = wait_for_memory(&session).await;
    assert_eq!(run.status, MemoryMaintenanceStatus::Completed, "{run:?}");
    assert!(
        session.subagent_lifecycle_snapshot().agents.is_empty(),
        "internal memory maintenance must stay out of the user-visible sub-agent lifecycle"
    );
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn custom_sensitive_memory_root_uses_session_approval_handler() {
    memory_case(true).await;
}

#[tokio::test]
async fn custom_sensitive_memory_root_fails_closed_without_approval_handler() {
    memory_case(false).await;
}

struct SpawnProbe {
    schema: JsonSchema,
}

#[async_trait]
impl Tool for SpawnProbe {
    fn static_name(&self) -> &str {
        "spawn_probe"
    }

    fn static_description(&self) -> &str {
        "Exercise an ordinary child through the public tool context."
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _: &Json) -> bool {
        false
    }

    fn is_read_only(&self, _: &Json) -> bool {
        true
    }

    async fn call(
        &self,
        input: Json,
        ctx: &ToolContext,
        _: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        let spawn = ctx
            .spawn_sub_agent
            .as_ref()
            .expect("public session spawner");
        let mut child = spawn(SpawnSubAgentOptions {
            prompt: "Attempt the requested protected write.".into(),
            tools: Some(vec![create_write_tool()]),
            fork_context: Some(ForkContext::None),
            memory_dir: None,
            ..Default::default()
        })
        .await
        .map_err(ToolError::new)?;
        while child.events.next().await.is_some() {}
        Ok(text_result(input, "Child finished.", false))
    }
}

#[tokio::test]
async fn ordinary_child_does_not_inherit_session_approval_handler() {
    let directory = TestDirectory::new();
    let protected = directory.0.join(".klaus/protected/MEMORY.md");
    std::fs::create_dir_all(protected.parent().unwrap()).unwrap();
    let provider = Arc::new(ScriptedProvider::new(vec![
        Reply::Tool("spawn_probe", serde_json::json!({})),
        Reply::Tool(
            "write",
            serde_json::json!({"path": protected, "content": "unexpected"}),
        ),
        Reply::Text,
        Reply::Text,
    ]));
    let handler = Arc::new(RecordingApprovalHandler::default());
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
    options.provider = provider.clone();
    options.config = Some(session_config(false));
    options.cwd = Some(directory.0.to_string_lossy().into_owned());
    options.tools = Some(vec![Arc::new(SpawnProbe {
        schema:
            serde_json::json!({"type": "object", "properties": {}, "additionalProperties": false})
                .as_object()
                .unwrap()
                .clone(),
    })]);
    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions {
        cwd: Some(directory.0.to_string_lossy().into_owned().into()),
        user_config_dir: Some(
            directory
                .0
                .join("config")
                .to_string_lossy()
                .into_owned()
                .into(),
        ),
        config: Some(session_config(false)),
        permission_mode: Some(ExternalPermissionMode::Default),
        approval_handler: Some(handler.clone()),
        ..Default::default()
    });
    assert_eq!(
        session.runtime_policy_state().sandbox_mode,
        Some(klaus_agent_core::types::permission::SandboxMode::WorkspaceWrite)
    );
    tokio::time::timeout(
        Duration::from_secs(20),
        session.run(
            Input::Text("Run the child probe.".into()),
            TurnOptions::default(),
        ),
    )
    .await
    .expect("the ordinary child must fail its protected write without prompting the parent");
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();

    assert!(provider.replies.lock().unwrap().is_empty());
    assert!(handler.requests.lock().unwrap().is_empty());
    assert!(provider.tool_errors.lock().unwrap().contains(&true));
    assert!(!protected.exists());
}

pub(super) async fn assert_readonly_parent_keeps_private_memory_writes_scoped() {
    let directory = TestDirectory::new();
    let host = Arc::new(CustomMemoryHost {
        root: directory.0.join("private-memory"),
    });
    let memory_dir = PathBuf::from(host.dir_for("main", AgentMemoryScope::Project));
    std::fs::create_dir_all(&memory_dir).unwrap();
    let parent_target = directory.0.join("parent-must-not-write.txt");
    let maintenance_target = directory.0.join("maintenance-must-not-write.txt");
    let provider = Arc::new(ScriptedProvider::new(vec![
        Reply::Tool(
            "write",
            serde_json::json!({"path":parent_target,"content":"forbidden"}),
        ),
        Reply::Text,
        Reply::Tool(
            "write",
            serde_json::json!({"path":"MEMORY.md","content":"private note\n"}),
        ),
        Reply::Tool(
            "write",
            serde_json::json!({"path":maintenance_target,"content":"forbidden"}),
        ),
        Reply::Text,
    ]));
    let handler = Arc::new(RecordingApprovalHandler::default());
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let mut config = session_config(true);
    let object = config.as_object_mut().unwrap();
    object.remove("sandbox");
    object.remove("requireRealSandbox");
    object.insert("sandbox_mode".into(), serde_json::json!("read-only"));
    object.insert("approval_policy".into(), serde_json::json!("never"));
    object.insert("approvals_reviewer".into(), serde_json::json!("user"));
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
    options.provider = provider.clone();
    options.config = Some(config.clone());
    options.cwd = Some(directory.0.to_string_lossy().into_owned());
    options.tools = Some(vec![create_write_tool()]);
    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions {
        cwd: Some(directory.0.to_string_lossy().into_owned().into()),
        user_config_dir: Some(
            directory
                .0
                .join("config")
                .to_string_lossy()
                .into_owned()
                .into(),
        ),
        config: Some(config),
        agent_memory: Some(host),
        approval_handler: Some(handler.clone()),
        ..Default::default()
    });
    let original = session.runtime_policy_state();
    assert_eq!(
        original.sandbox_mode,
        Some(klaus_agent_core::types::permission::SandboxMode::ReadOnly)
    );
    session
        .run(
            Input::Text("Retain this preference within the private memory scope.".into()),
            TurnOptions::default(),
        )
        .await;
    let run = wait_for_memory(&session).await;
    let final_policy = session.runtime_policy_state();
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    assert_eq!(
        run.status,
        MemoryMaintenanceStatus::Failed,
        "the out-of-scope mutation must fail: {run:?}"
    );
    assert_eq!(run.reason, Some(AutoMemoryRunReason::ToolFailure));
    assert_eq!(
        std::fs::read_to_string(memory_dir.join("MEMORY.md")).unwrap(),
        "private note\n"
    );
    assert!(!parent_target.exists());
    assert!(!maintenance_target.exists());
    assert!(handler.requests.lock().unwrap().is_empty());
    assert!(provider.replies.lock().unwrap().is_empty());
    assert!(provider.tool_errors.lock().unwrap().contains(&false));
    assert!(provider.tool_errors.lock().unwrap().contains(&true));
    assert_eq!(final_policy, original);
}
