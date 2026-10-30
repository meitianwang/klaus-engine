//! Defect: filesystem policy and approval routing were coupled to a permission preset.
//! Root cause: legacy modes also selected sandbox effects and prompt eligibility.
//! Work item: 20261018 independent runtime settings.

use super::r20261015_browser_diagnostics_and_targets::make_ctx;
use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, RuntimePolicyUpdate, Session, TurnOptions};
use klaus_agent_core::permission::engine::{
    PermissionDecision, PermissionEngine, StaticPermissionEngine,
};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimeString};
use klaus_agent_core::schemas::config::default_config;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::tools::builtins::{
    bash::create_bash_tool, interactive_shell::create_exec_command_tool, write::create_write_tool,
};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::permission::{
    ApprovalHandler, ApprovalPolicy, ApprovalRequest, ApprovalResponse, ApprovalsReviewer,
    PermissionRule, SandboxMode,
};
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use klaus_agent_core::types::sandbox::{
    ExecRequest, SandboxBackend, SandboxKind, SandboxPolicy, SpawnSpec,
};
use klaus_agent_core::types::tool::Tool;
use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

struct DenyingSandbox;
impl SandboxBackend for DenyingSandbox {
    fn kind(&self) -> SandboxKind {
        SandboxKind::Seatbelt
    }
    fn is_available(&self) -> bool {
        true
    }
    fn transform(&self, request: &ExecRequest, _: &SandboxPolicy) -> SpawnSpec {
        SpawnSpec {
            file: "/bin/sh".into(),
            args: vec![
                "-c".into(),
                "printf 'Operation not permitted\\n'; exit 126".into(),
            ],
            cwd: request.cwd.clone(),
            env: request.env.clone().unwrap_or_default(),
        }
    }
}
fn input(value: serde_json::Value) -> RuntimeJson {
    runtime_json::parse(&RuntimeString::from(value.to_string())).unwrap()
}

#[tokio::test]
async fn independent_policy_preserves_rules_and_executes_workspace_commands_without_approvals() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut ctx = make_ctx("/tmp/klaus-policy-workspace", process);
    ctx.sandbox_mode = Some(SandboxMode::WorkspaceWrite);
    ctx.approval_policy = Some(ApprovalPolicy::OnRequest);
    ctx.approvals_reviewer = Some(ApprovalsReviewer::Automatic);
    ctx.sandbox = Arc::new(DenyingSandbox);
    let evaluator = StaticPermissionEngine::new(&default_config());
    for tool in [create_bash_tool(), create_exec_command_tool()] {
        assert!(matches!(
            evaluator
                .evaluate(
                    tool.as_ref(),
                    &input(serde_json::json!({"command":"python3 -c 'print(1)'"})),
                    &ctx
                )
                .await,
            PermissionDecision::Allow { .. }
        ));
        ctx.permission.deny_rules = vec![PermissionRule {
            tool_name: tool.name(),
            argument_prefix: Some("blocked".into()),
            tool_name_prefix: None,
        }];
        assert!(matches!(
            evaluator
                .evaluate(
                    tool.as_ref(),
                    &input(serde_json::json!({"command":"blocked action"})),
                    &ctx
                )
                .await,
            PermissionDecision::Deny { .. }
        ));
        ctx.permission.deny_rules.clear();
    }
    let write = create_write_tool();
    let ordinary = input(serde_json::json!({"path":"note.txt","content":"work"}));
    assert!(matches!(
        evaluator.evaluate(write.as_ref(), &ordinary, &ctx).await,
        PermissionDecision::Allow { .. }
    ));
    ctx.sandbox_mode = Some(SandboxMode::ReadOnly);
    assert!(matches!(
        evaluator.evaluate(write.as_ref(), &ordinary, &ctx).await,
        PermissionDecision::Ask { .. }
    ));
    ctx.sandbox_mode = Some(SandboxMode::WorkspaceWrite);
    assert!(matches!(
        evaluator
            .evaluate(
                write.as_ref(),
                &input(serde_json::json!({"path":"/outside/note.txt","content":"work"})),
                &ctx
            )
            .await,
        PermissionDecision::Ask { .. }
    ));
}

#[tokio::test]
async fn public_runtime_policy_updates_preserve_independent_fields_for_legacy_callers() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let agent = Agent::new(agent_options(process));
    let session = agent.start_session(StartSessionOptions::default());
    let mut update = RuntimePolicyUpdate {
        approval_policy: Some(ApprovalPolicy::Never),
        sandbox_mode: Some(SandboxMode::DangerFullAccess),
        approvals_reviewer: Some(ApprovalsReviewer::Automatic),

        network_allowed: Some(false),

        sandbox_workspace_write: None,
    };
    session.set_runtime_policy(update.clone()).unwrap();
    let initial = session.runtime_policy_state();
    assert_eq!(initial.sandbox_mode, Some(SandboxMode::DangerFullAccess));
    assert!(initial.network_allowed);
    update.approval_policy = None;
    update.sandbox_mode = None;
    update.approvals_reviewer = None;
    session.set_runtime_policy(update).unwrap();
    let final_state = session.runtime_policy_state();
    assert_eq!(final_state.approval_policy, Some(ApprovalPolicy::Never));
    assert_eq!(
        final_state.sandbox_mode,
        Some(SandboxMode::DangerFullAccess)
    );
    assert_eq!(
        final_state.approvals_reviewer,
        Some(ApprovalsReviewer::Automatic)
    );
    assert_eq!(final_state.generation, initial.generation + 1);
    session.dispose().await;
}

#[cfg(unix)]
#[tokio::test]
async fn interactive_shell_escalates_only_when_approval_policy_allows_it() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let directory =
        std::env::temp_dir().join(format!("klaus-runtime-policy-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let mut ctx = make_ctx(directory.to_str().unwrap(), process);
    ctx.sandbox = Arc::new(DenyingSandbox);
    ctx.sandbox_mode = Some(SandboxMode::ReadOnly);
    ctx.sandbox_retry_on_denial = Some(true);
    let asked = Arc::new(AtomicUsize::new(0));
    let observed = asked.clone();
    ctx.request_approval = Some(Arc::new(move |_| {
        observed.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { true })
    }));
    let tool = create_exec_command_tool();
    let command =
        input(serde_json::json!({"command":"printf approved > result.txt","yieldMs":1000}));
    ctx.approval_policy = Some(ApprovalPolicy::Never);
    let denied = tool.call(command.clone(), &ctx, None).await.unwrap();
    assert_eq!(denied.serializable.is_error, Some(true));
    assert_eq!(asked.load(Ordering::SeqCst), 0);
    assert!(!directory.join("result.txt").exists());
    ctx.approval_policy = Some(ApprovalPolicy::OnRequest);
    let approved = tool.call(command, &ctx, None).await.unwrap();
    assert_ne!(approved.serializable.is_error, Some(true));
    assert_eq!(asked.load(Ordering::SeqCst), 1);
    assert_eq!(
        std::fs::read_to_string(directory.join("result.txt")).unwrap(),
        "approved"
    );
    std::fs::remove_dir_all(directory).unwrap();
}

pub(super) struct Directory(pub(super) PathBuf);
impl Directory {
    pub(super) fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("klaus-policy-effects-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path.canonicalize().unwrap())
    }
    #[cfg(target_os = "macos")]
    fn native() -> Self {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target")
            .join(format!("runtime-policy-effects-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path.canonicalize().unwrap())
    }
    pub(super) fn child(&self, name: &str) -> PathBuf {
        let path = self.0.join(name);
        std::fs::create_dir_all(&path).unwrap();
        path
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub(super) struct Step {
    tool: &'static str,
    input: serde_json::Value,
}
struct ScriptedProvider {
    info: ProviderInfo,
    steps: Mutex<VecDeque<Step>>,
    requests: Mutex<Vec<ModelRequest>>,
    assessments: Mutex<Vec<ModelRequest>>,
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
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }
    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        if request
            .system
            .iter()
            .any(|part| part.name == "approval-review-policy")
        {
            let mut assessments = self.assessments.lock().unwrap();
            assert!(assessments.is_empty(), "bounded automatic shell assessment");
            assert!(request.tools.iter().any(|tool| tool.name == "exec_command"));
            assert!(request.tools.iter().all(|tool| [
                "exec_command",
                "write_stdin",
                "view_image",
                "exec",
                "wait"
            ]
            .iter()
            .any(|name| tool.name == *name)));
            assert!(request.web_search.is_none());
            let model = request.model.clone();
            assessments.push(request);
            return Box::pin(futures::stream::iter([
                ModelStreamEvent::MessageStart { model },
                ModelStreamEvent::TextDelta {
                    text: r#"{"outcome":"allow"}"#.into(),
                },
                ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::EndTurn,
                    usage: Usage {
                        input_tokens: 10,
                        output_tokens: 4,
                        cache_read_input_tokens: 0,
                        cache_creation_input_tokens: 0,
                        total_tokens: Some(14),
                    },
                },
            ]));
        }
        let index = {
            let mut requests = self.requests.lock().unwrap();
            assert!(
                requests.len() < 5,
                "the public fixture has a bounded tool script"
            );
            let index = requests.len();
            requests.push(request);
            index
        };
        let step = self.steps.lock().unwrap().pop_front();
        let mut events = vec![ModelStreamEvent::MessageStart {
            model: "runtime-policy-effects".into(),
        }];
        let stop_reason = match step {
            Some(step) => {
                let id: RuntimeString = format!("policy-effect-call-{index}").into();
                events.extend([
                    ModelStreamEvent::ToolUseStart {
                        id: id.clone(),
                        name: step.tool.into(),
                        thought_signature: None,
                        tool_kind: None,
                        input_format: None,
                    },
                    ModelStreamEvent::ToolUseInputDelta {
                        id: id.clone(),
                        partial_json: step.input.to_string().into(),
                    },
                    ModelStreamEvent::ToolUseStop { id },
                ]);
                ModelStopReason::ToolUse
            }
            None => {
                events.push(ModelStreamEvent::TextDelta {
                    text: "The authorized policy fixture completed.".into(),
                });
                ModelStopReason::EndTurn
            }
        };
        events.push(ModelStreamEvent::MessageStop {
            stop_reason,
            usage: Usage {
                input_tokens: 10,
                output_tokens: 4,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
                total_tokens: Some(14),
            },
        });
        Box::pin(futures::stream::iter(events))
    }
}

struct ScriptedApprovals {
    decisions: Mutex<VecDeque<bool>>,
    requests: Mutex<Vec<ApprovalRequest>>,
    updated_input: Mutex<Option<RuntimeJson>>,
}
#[async_trait::async_trait]
impl ApprovalHandler for ScriptedApprovals {
    async fn approve(&self, request: ApprovalRequest, signal: AbortSignal) -> ApprovalResponse {
        assert!(!signal.is_cancelled());
        self.requests.lock().unwrap().push(request);
        match self
            .decisions
            .lock()
            .unwrap()
            .pop_front()
            .expect("every actual prompt has an explicit fixture decision")
        {
            true => ApprovalResponse::Allow {
                updated_input: self.updated_input.lock().unwrap().take(),
                updated_permissions: None,
            },
            false => ApprovalResponse::Deny {
                message: Some("Only the first explicitly approved operation is authorized.".into()),
            },
        }
    }
}

pub(super) struct PublicRig {
    process: RuntimeProcessContext,
    pub(super) session: Arc<Session>,
    provider: Arc<ScriptedProvider>,
    approvals: Arc<ScriptedApprovals>,
}
impl PublicRig {
    pub(super) fn new(
        workspace: &Path,
        config_root: &Path,
        sandbox: &str,
        approval: &str,
        tools: Vec<Arc<dyn Tool>>,
        steps: Vec<Step>,
        decisions: Vec<bool>,
    ) -> Self {
        Self::build(
            workspace,
            config_root,
            (sandbox, approval),
            tools,
            steps,
            decisions,
            false,
        )
    }

    #[cfg(target_os = "macos")]
    fn with_managed_network(
        workspace: &Path,
        config_root: &Path,
        sandbox: &str,
        approval: &str,
        tools: Vec<Arc<dyn Tool>>,
        steps: Vec<Step>,
        decisions: Vec<bool>,
    ) -> Self {
        Self::build(
            workspace,
            config_root,
            (sandbox, approval),
            tools,
            steps,
            decisions,
            true,
        )
    }

    fn build(
        workspace: &Path,
        config_root: &Path,
        policy: (&str, &str),
        tools: Vec<Arc<dyn Tool>>,
        steps: Vec<Step>,
        decisions: Vec<bool>,
        managed_network: bool,
    ) -> Self {
        let (sandbox, approval) = policy;
        let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
        let provider = Arc::new(ScriptedProvider {
            info: ProviderInfo {
                name: "runtime-policy-effects".into(),
                base_url: None,
                env_key: None,
                default_model: "runtime-policy-effects".into(),
            },
            steps: Mutex::new(steps.into()),
            requests: Mutex::new(Vec::new()),
            assessments: Mutex::new(Vec::new()),
        });
        let approvals = Arc::new(ScriptedApprovals {
            decisions: Mutex::new(decisions.into()),
            requests: Mutex::new(Vec::new()),
            updated_input: Mutex::new(None),
        });
        let mut options = agent_options(process.clone());
        options.provider = provider.clone();
        let surfaced = tools.iter().map(|tool| tool.name()).collect();
        options.tools = Some(tools);
        options.approval_handler = Some(approvals.clone());
        options.cwd = Some(workspace.to_string_lossy().into_owned());
        options.include_default_agents = Some(false);
        options.config = Some(serde_json::json!({
            "model":"runtime-policy-effects", "permissionMode":"default", "approval_policy":approval,
            "sandbox_mode":sandbox, "approvals_reviewer":"user",
            "networkProxy":{"enabled":managed_network},
            "sandbox_workspace_write":{"network_access":false,"exclude_tmpdir_env_var":true,"exclude_slash_tmp":true}, "useStoredPermissionRules":false,
            "goal":false, "prewarm":false, "autoMemory":false, "agentMemory":false,
            "autoDream":false, "memoryRecall":false, "memory":{"enabled":false}, "loopDetection":false
        }));
        let session = Agent::new(options).start_session(StartSessionOptions {
            initial_surfaced_tools: Some(surfaced),
            user_config_dir: Some(config_root.to_string_lossy().into_owned().into()),
            ..Default::default()
        });
        Self {
            process,
            session,
            provider,
            approvals,
        }
    }
    pub(super) async fn run(&self) -> BTreeMap<RuntimeString, bool> {
        let result = tokio::time::timeout(
            Duration::from_secs(15),
            self.session.run(
                Input::Text("Perform only the requested local policy fixture operations.".into()),
                TurnOptions::default(),
            ),
        )
        .await
        .expect("the bounded public policy turn completes");
        assert!(result.error.is_none(), "{:?}", result.error);
        assert!(self.provider.steps.lock().unwrap().is_empty());
        let requests = self.provider.requests.lock().unwrap();
        requests
            .last()
            .unwrap()
            .messages
            .iter()
            .filter_map(|message| match message {
                Message::ToolResult(result) => Some((result.tool_use_id.clone(), result.is_error)),
                _ => None,
            })
            .collect()
    }
    pub(super) async fn finish(self) {
        self.session.dispose().await;
        self.process.shutdown().await.unwrap();
    }
}

pub(super) fn write_step(path: &Path, content: &str) -> Step {
    Step {
        tool: "write",
        input: serde_json::json!({"path":path.to_str().unwrap(),"content":content}),
    }
}

pub(super) fn edit_step(path: &Path, old: &str, new: &str) -> Step {
    Step {
        tool: "edit",
        input: serde_json::json!({"path":path.to_str().unwrap(),"oldString":old,"newString":new}),
    }
}

#[tokio::test]
async fn public_workspace_write_approval_creates_an_outside_file_and_never_policy_rejects_it() {
    for (approval, decisions, expected_created) in [
        ("on-request", vec![true], true),
        ("never", Vec::new(), false),
    ] {
        let directory = Directory::new();
        let workspace = directory.child("workspace");
        let outside = directory.child("outside");
        let target = outside.join("approved.txt");
        let rig = PublicRig::new(
            &workspace,
            &directory.0.join("config"),
            "workspace-write",
            approval,
            vec![create_write_tool()],
            vec![write_step(&target, "approved outside content")],
            decisions,
        );
        let results = rig.run().await;
        assert_eq!(
            results.get(&RuntimeString::from("policy-effect-call-0")),
            Some(&!expected_created),
            "{approval}"
        );
        assert_eq!(
            rig.approvals.requests.lock().unwrap().len(),
            usize::from(expected_created)
        );
        if expected_created {
            assert_eq!(
                std::fs::read_to_string(&target).unwrap(),
                "approved outside content"
            );
        } else {
            assert!(!target.exists(), "never cannot create an outside file");
        }
        rig.finish().await;
    }
}

#[tokio::test]
async fn public_read_only_approval_grants_one_file_operation_without_a_durable_write_grant() {
    let directory = Directory::new();
    let workspace = directory.child("workspace");
    let target = workspace.join("single-approved.txt");
    let sibling = workspace.join("unapproved-sibling.txt");
    let rig = PublicRig::new(
        &workspace,
        &directory.0.join("config"),
        "read-only",
        "on-request",
        vec![create_write_tool()],
        vec![
            write_step(&target, "approved content"),
            write_step(&target, "unapproved overwrite"),
            write_step(&sibling, "unapproved sibling"),
        ],
        vec![true, false, false],
    );
    let before = rig.session.runtime_policy_state();
    let results = rig.run().await;
    assert_eq!(
        results.get(&RuntimeString::from("policy-effect-call-0")),
        Some(&false)
    );
    assert_eq!(
        results.get(&RuntimeString::from("policy-effect-call-1")),
        Some(&true)
    );
    assert_eq!(
        results.get(&RuntimeString::from("policy-effect-call-2")),
        Some(&true)
    );
    assert_eq!(
        rig.approvals.requests.lock().unwrap().len(),
        3,
        "the same file and its sibling still require separate approvals"
    );
    assert_eq!(
        std::fs::read_to_string(&target).unwrap(),
        "approved content"
    );
    assert!(!sibling.exists());
    assert_eq!(
        rig.session.runtime_policy_state(),
        before,
        "an operation approval cannot change the session policy"
    );
    rig.finish().await;
}

#[tokio::test]
async fn public_full_access_writes_outside_without_a_separate_permission_mode() {
    let directory = Directory::new();
    let workspace = directory.child("workspace");
    let target = directory.child("outside").join("full-access.txt");
    let rig = PublicRig::new(
        &workspace,
        &directory.0.join("config"),
        "danger-full-access",
        "never",
        vec![create_write_tool()],
        vec![write_step(&target, "full access content")],
        Vec::new(),
    );
    let results = rig.run().await;
    assert_eq!(
        rig.session.runtime_policy_state().sandbox_mode,
        Some(SandboxMode::DangerFullAccess)
    );
    assert_eq!(
        results.get(&RuntimeString::from("policy-effect-call-0")),
        Some(&false)
    );
    assert!(rig.approvals.requests.lock().unwrap().is_empty());
    assert_eq!(
        std::fs::read_to_string(&target).unwrap(),
        "full access content"
    );
    rig.finish().await;
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn native_bash_and_interactive_shell_enforce_workspace_and_read_only_file_boundaries() {
    for tool in [create_bash_tool(), create_exec_command_tool()] {
        for mode in ["workspace-write", "read-only"] {
            let directory = Directory::native();
            let workspace = directory.child("workspace");
            let outside = directory.child("outside");
            let target = if mode == "read-only" {
                workspace.join("blocked.txt")
            } else {
                outside.join("blocked.txt")
            };
            std::fs::write(&target, "host control").unwrap();
            std::fs::remove_file(&target).unwrap();
            let command = format!("printf blocked > '{}'", target.display());
            let tool_name = if tool.name() == "bash" {
                "bash"
            } else {
                "exec_command"
            };
            let rig = PublicRig::new(
                &workspace,
                &directory.0.join("config"),
                mode,
                "never",
                vec![tool.clone()],
                vec![
                    Step {
                        tool: tool_name,
                        input: serde_json::json!({"command":"printf native-ready"}),
                    },
                    Step {
                        tool: tool_name,
                        input: serde_json::json!({"command":command}),
                    },
                ],
                Vec::new(),
            );
            let results = rig.run().await;
            assert_eq!(
                results.get(&RuntimeString::from("policy-effect-call-0")),
                Some(&false),
                "a harmless command must run through the native sandbox"
            );
            assert_eq!(
                results.get(&RuntimeString::from("policy-effect-call-1")),
                Some(&true),
                "{mode} {tool_name}"
            );
            assert!(
                !target.exists(),
                "the real OS sandbox must prevent {mode} {tool_name} from creating {}",
                target.display()
            );
            assert!(rig.approvals.requests.lock().unwrap().is_empty());
            rig.finish().await;
        }
    }
}

#[cfg(target_os = "macos")]
struct LocalHttp {
    address: std::net::SocketAddr,
    hits: Arc<AtomicUsize>,
    shutdown: AbortSignal,
    server: tokio::task::JoinHandle<()>,
}

#[cfg(target_os = "macos")]
impl LocalHttp {
    async fn start() -> Self {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let observed = hits.clone();
        let shutdown = AbortSignal::new();
        let stop = shutdown.clone();
        let server = tokio::spawn(async move {
            loop {
                let connection = tokio::select! { _ = stop.cancelled() => break, connection = listener.accept() => connection.unwrap() };
                let (mut socket, _) = connection;
                let mut buffer = [0; 1024];
                let count = tokio::time::timeout(Duration::from_secs(2), socket.read(&mut buffer))
                    .await
                    .unwrap()
                    .unwrap();
                if count > 0 {
                    observed.fetch_add(1, Ordering::SeqCst);
                    socket
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK",
                        )
                        .await
                        .unwrap();
                }
            }
        });
        let mut control = tokio::net::TcpStream::connect(address).await.unwrap();
        control
            .write_all(b"GET /control HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        let mut bounded_control = control.take(1024);
        tokio::time::timeout(
            Duration::from_secs(2),
            bounded_control.read_to_end(&mut response),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(response.starts_with(b"HTTP/1.1 200 OK"));
        assert_eq!(
            hits.swap(0, Ordering::SeqCst),
            1,
            "the local fixture is reachable outside the sandbox"
        );
        Self {
            address,
            hits,
            shutdown,
            server,
        }
    }
    pub(super) async fn finish(self) {
        self.shutdown.cancel();
        self.server.await.unwrap();
    }
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn native_bash_and_interactive_shell_cannot_reach_a_local_server_when_network_is_disabled() {
    let server = LocalHttp::start().await;
    let address = server.address;
    for tool in [create_bash_tool(), create_exec_command_tool()] {
        let directory = Directory::new();
        let workspace = directory.child("workspace");
        let tool_name = if tool.name() == "bash" {
            "bash"
        } else {
            "exec_command"
        };
        let command = format!("/usr/bin/curl --noproxy '*' --fail --silent --show-error --max-time 2 http://{address}/blocked");
        let rig = PublicRig::new(
            &workspace,
            &directory.0.join("config"),
            "workspace-write",
            "never",
            vec![tool],
            vec![
                Step {
                    tool: tool_name,
                    input: serde_json::json!({"command":"printf native-ready"}),
                },
                Step {
                    tool: tool_name,
                    input: serde_json::json!({"command":command}),
                },
            ],
            Vec::new(),
        );
        let results = rig.run().await;
        assert_eq!(
            results.get(&RuntimeString::from("policy-effect-call-0")),
            Some(&false),
            "a harmless command must run through the native sandbox"
        );
        assert_eq!(
            results.get(&RuntimeString::from("policy-effect-call-1")),
            Some(&true),
            "{tool_name}"
        );
        assert_eq!(
            server.hits.load(Ordering::SeqCst),
            0,
            "the native {tool_name} policy must block the real HTTP request"
        );
        assert!(rig.approvals.requests.lock().unwrap().is_empty());
        rig.finish().await;
    }
    server.finish().await;
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn native_bash_and_interactive_shell_approve_one_explicit_outside_write() {
    for tool in [create_bash_tool(), create_exec_command_tool()] {
        let directory = Directory::native();
        let workspace = directory.child("workspace");
        let target = directory.child("outside").join("approved-escalation.txt");
        std::fs::write(&target, "host control").unwrap();
        std::fs::remove_file(&target).unwrap();
        let command = format!("printf 'approved escalation' > '{}'", target.display());
        let tool_name = if tool.name() == "bash" {
            "bash"
        } else {
            "exec_command"
        };
        let rig = PublicRig::new(
            &workspace,
            &directory.0.join("config"),
            "workspace-write",
            "on-request",
            vec![tool],
            vec![Step {
                tool: tool_name,
                input: serde_json::json!({"command":command,"sandboxPermissions":"require_escalated","justification":"Write the explicitly requested fixture outside the workspace."}),
            }],
            vec![true],
        );
        let results = rig.run().await;
        assert_eq!(
            results.get(&RuntimeString::from("policy-effect-call-0")),
            Some(&false),
            "{tool_name}"
        );
        assert_eq!(
            rig.approvals.requests.lock().unwrap().len(),
            1,
            "the real sandbox denial must request one operation approval: {tool_name}"
        );
        assert_eq!(
            std::fs::read_to_string(&target).unwrap(),
            "approved escalation",
            "the approved native {tool_name} retry must actually write outside the workspace"
        );
        assert_eq!(
            rig.session.runtime_policy_state().sandbox_mode,
            Some(SandboxMode::WorkspaceWrite)
        );
        rig.finish().await;
    }
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn native_bash_and_interactive_shell_approve_network_elevation_with_legacy_retry_disabled() {
    let server = LocalHttp::start().await;
    let address = server.address;
    for tool in [create_bash_tool(), create_exec_command_tool()] {
        let directory = Directory::new();
        let workspace = directory.child("workspace");
        let tool_name = if tool.name() == "bash" {
            "bash"
        } else {
            "exec_command"
        };
        let command = format!("/usr/bin/curl --noproxy '*' --fail --silent --show-error --max-time 2 http://{address}/approved");
        let before = server.hits.load(Ordering::SeqCst);
        let rig = PublicRig::new(
            &workspace,
            &directory.0.join("config"),
            "workspace-write",
            "on-request",
            vec![tool],
            vec![Step {
                tool: tool_name,
                input: serde_json::json!({"command":command,"sandboxPermissions":"require_escalated","justification":"Reach the explicitly requested local HTTP fixture."}),
            }],
            vec![true],
        );
        let results = rig.run().await;
        let tool_outputs: Vec<_> = rig
            .provider
            .requests
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .messages
            .iter()
            .filter_map(|message| match message {
                Message::ToolResult(result) => Some(result.content.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            results.get(&RuntimeString::from("policy-effect-call-0")),
            Some(&false),
            "{tool_name}: {tool_outputs:?}"
        );
        assert_eq!(
            rig.approvals.requests.lock().unwrap().len(),
            1,
            "the explicit native network request must ask exactly once: {tool_name}"
        );
        assert_eq!(
            server.hits.load(Ordering::SeqCst),
            before + 1,
            "the approved native {tool_name} retry must reach the actual HTTP server"
        );
        assert!(
            !rig.session.runtime_policy_state().network_allowed,
            "one operation cannot grant durable network access"
        );
        rig.finish().await;
    }
    server.finish().await;
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn native_explicit_shell_network_elevation_uses_the_automatic_verdict_without_a_human_prompt()
{
    let server = LocalHttp::start().await;
    for tool in [create_bash_tool(), create_exec_command_tool()] {
        let directory = Directory::new();
        let workspace = directory.child("workspace");
        let name = if tool.name() == "bash" {
            "bash"
        } else {
            "exec_command"
        };
        let before = server.hits.load(Ordering::SeqCst);
        let command = format!("/usr/bin/curl --noproxy '*' --fail --silent --show-error --max-time 2 http://{}/automatic", server.address);
        let rig = PublicRig::new(
            &workspace,
            &directory.0.join("config"),
            "workspace-write",
            "on-request",
            vec![tool],
            vec![Step {
                tool: name,
                input: serde_json::json!({"command":command,"sandboxPermissions":"require_escalated","justification":"Reach the requested local fixture once."}),
            }],
            Vec::new(),
        );
        rig.session
            .set_runtime_policy(RuntimePolicyUpdate {
                approval_policy: Some(ApprovalPolicy::OnRequest),
                sandbox_mode: Some(SandboxMode::WorkspaceWrite),
                approvals_reviewer: Some(ApprovalsReviewer::Automatic),

                network_allowed: Some(false),

                sandbox_workspace_write: None,
            })
            .unwrap();
        let results = rig.run().await;
        assert_eq!(
            results.get(&RuntimeString::from("policy-effect-call-0")),
            Some(&false),
            "{name}"
        );
        assert_eq!(rig.provider.assessments.lock().unwrap().len(), 1, "{name}");
        assert!(rig.approvals.requests.lock().unwrap().is_empty(), "{name}");
        assert_eq!(server.hits.load(Ordering::SeqCst), before + 1, "{name}");
        assert!(!rig.session.runtime_policy_state().network_allowed);
        rig.finish().await;
    }
    server.finish().await;
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn native_explicit_shell_elevation_never_runs_after_a_denial_or_under_never_policy() {
    let server = LocalHttp::start().await;
    for approval in ["on-request", "never"] {
        for tool in [create_bash_tool(), create_exec_command_tool()] {
            let directory = Directory::new();
            let workspace = directory.child("workspace");
            let name = if tool.name() == "bash" {
                "bash"
            } else {
                "exec_command"
            };
            let before = server.hits.load(Ordering::SeqCst);
            let command = format!("/usr/bin/curl --noproxy '*' --fail --silent --show-error --max-time 2 http://{}/denied",server.address);
            let rig = PublicRig::new(
                &workspace,
                &directory.0.join("config"),
                "workspace-write",
                approval,
                vec![tool],
                vec![Step {
                    tool: name,
                    input: serde_json::json!({"command":command,"sandboxPermissions":"require_escalated","justification":"Request access to the local fixture."}),
                }],
                if approval == "on-request" {
                    vec![false]
                } else {
                    Vec::new()
                },
            );
            let results = rig.run().await;
            assert_eq!(
                results.get(&RuntimeString::from("policy-effect-call-0")),
                Some(&true),
                "{name}, {approval}"
            );
            assert_eq!(
                rig.approvals.requests.lock().unwrap().len(),
                usize::from(approval == "on-request")
            );
            assert_eq!(
                server.hits.load(Ordering::SeqCst),
                before,
                "{name}, {approval}"
            );
            assert!(rig.provider.assessments.lock().unwrap().is_empty());
            assert!(!rig.session.runtime_policy_state().network_allowed);
            rig.finish().await;
        }
    }
    server.finish().await;
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn an_edited_mid_command_approval_requires_a_new_permission_checked_invocation() {
    for tool in [create_bash_tool(), create_exec_command_tool()] {
        let directory = Directory::new();
        let workspace = directory.child("workspace");
        let outside = directory.child("outside");
        let original = outside.join("original.txt");
        let revised = outside.join("revised.txt");
        let name = if tool.name() == "bash" {
            "bash"
        } else {
            "exec_command"
        };
        let command = format!("printf original > '{}'", original.display());
        let rig = PublicRig::new(
            &workspace,
            &directory.0.join("config"),
            "workspace-write",
            "on-request",
            vec![tool],
            vec![Step {
                tool: name,
                input: serde_json::json!({"command":command,"sandboxPermissions":"require_escalated","justification":"Write the local fixture outside the workspace."}),
            }],
            vec![true],
        );
        *rig.approvals.updated_input.lock().unwrap() = Some(input(
            serde_json::json!({"command":format!("printf revised > '{}'",revised.display()),"sandboxPermissions":"require_escalated","justification":"Use the revised local destination."}),
        ));
        let result = rig.run().await;
        assert_eq!(
            result.get(&RuntimeString::from("policy-effect-call-0")),
            Some(&true),
            "{name}"
        );
        assert!(
            !original.exists(),
            "the original command was not approved: {name}"
        );
        assert!(
            !revised.exists(),
            "the revised input must receive a fresh validation: {name}"
        );
        assert_eq!(rig.approvals.requests.lock().unwrap().len(), 1);
        rig.finish().await;
    }
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn mandatory_network_routing_survives_full_access_startup_and_live_filesystem_changes() {
    let server = LocalHttp::start().await;
    for (initial, next) in [
        ("danger-full-access", SandboxMode::DangerFullAccess),
        ("workspace-write", SandboxMode::ReadOnly),
        ("workspace-write", SandboxMode::DangerFullAccess),
    ] {
        let directory = Directory::native();
        let workspace = directory.child("workspace");
        let outside = directory.child("outside").join("effect");
        let write = if next == SandboxMode::DangerFullAccess {
            format!("printf allowed > '{}' || exit 12", outside.display())
        } else {
            format!(
                "if printf blocked > '{}' 2>/dev/null; then exit 13; fi",
                outside.display()
            )
        };
        let command = format!(
            "test -n \"$HTTPS_PROXY\" || exit 10; if /usr/bin/curl --noproxy '*' --fail --silent --max-time 1 http://{}/direct >/dev/null 2>&1; then exit 11; fi; {write}",
            server.address,
        );
        let rig = PublicRig::with_managed_network(
            &workspace,
            &directory.0.join("config"),
            initial,
            "never",
            vec![create_bash_tool()],
            vec![Step {
                tool: "bash",
                input: serde_json::json!({"command":command}),
            }],
            Vec::new(),
        );
        rig.session
            .set_runtime_policy(RuntimePolicyUpdate {
                approval_policy: None,
                sandbox_mode: Some(next),
                approvals_reviewer: None,

                sandbox_workspace_write: None,
                network_allowed: Some(true),
            })
            .unwrap();
        let results = rig.run().await;
        assert_eq!(
            results.get(&RuntimeString::from("policy-effect-call-0")),
            Some(&false),
            "{initial} -> {next:?}: {:?}",
            rig.provider
                .requests
                .lock()
                .unwrap()
                .last()
                .unwrap()
                .messages
                .iter()
                .filter(|message| matches!(message, Message::ToolResult(_)))
                .collect::<Vec<_>>()
        );
        assert_eq!(outside.exists(), next == SandboxMode::DangerFullAccess);
        assert_eq!(
            server.hits.load(Ordering::SeqCst),
            0,
            "direct traffic must retain the captured network route ceiling"
        );
        rig.finish().await;
    }
    server.finish().await;
}
