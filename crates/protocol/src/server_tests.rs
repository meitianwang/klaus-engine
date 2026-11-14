use super::*;

#[path = "server_turn_submission_tests.rs"]
mod turn_submission_tests;

#[path = "server_subagent_stream_contract_tests.rs"]
mod subagent_stream_contract_tests;

#[path = "server_control_dispatch_tests.rs"]
mod control_dispatch_tests;
use crate::client::{ClientError, ProtocolClient, ProtocolClientOptions};
use crate::types::{
    OpenAiRequestIdentityInit, ProtocolBrowserCapabilityInit, ProtocolCommandHook,
    ProtocolDynamicToolCallOutputContentItem, ProtocolDynamicToolFunctionSpec,
    ProtocolDynamicToolNamespaceSpec, ProtocolDynamicToolNamespaceTool,
    ProtocolDynamicToolResponse, ProtocolDynamicToolSpec, ProtocolHostCapabilitiesInit,
    ProtocolMcpServerInit, ProtocolModelPricing, ProtocolRuntimeCapabilityRootInit,
    ProtocolSelectedCapabilityRootInit, ProviderAuthInit, ProviderInit,
};
use klaus_agent_core::browser::BrowserHostToolMapping;
use klaus_agent_core::capabilities::{
    RuntimeCapabilityRoot, RuntimeCapabilitySourceKind, SelectedRuntimeCapabilityRoot,
};
use klaus_agent_core::credential_store::{CredentialStore, NamespacedCredentialStore};
use klaus_agent_core::engine::agent::{Agent, AgentOptions, StartSessionOptions};
use klaus_agent_core::engine::agent_definitions::AgentDefinition;
use klaus_agent_core::engine::deps::{noop_compactor, EngineDepsOverrides};
use klaus_agent_core::engine::session::EnqueueOptions;
use klaus_agent_core::engine::session::RolloutSink;
use klaus_agent_core::engine::subagent_lifecycle::{
    SubagentContext, SubagentKind, SubagentLifecycleRecord, SubagentStatus,
};
use klaus_agent_core::extensions::output_styles::OutputStyle;
use klaus_agent_core::extensions::plugins::loader::LoadedPlugin;
use klaus_agent_core::extensions::runtime_snapshot::{
    ExtensionKind, ExtensionSourceKind, HookHandlerKind,
};
use klaus_agent_core::extensions::skills::loader::Skill;
use klaus_agent_core::hooks::types::{HookEvent, HookFn, HookInput, HookOutcome};
use klaus_agent_core::lsp::types::LspServerConfig;
use klaus_agent_core::mcp::credential_store::InMemoryCredentialStore;
use klaus_agent_core::mcp::transports::{Transport, TransportEvent};
use klaus_agent_core::mcp::types::{McpInProcessServer, McpServerConfig, McpServerOptions};
use klaus_agent_core::memory::agent_memory::{
    AgentMemoryHost, AgentMemoryLayout, AgentMemoryMergeChoice, AgentMemoryScope,
};
use klaus_agent_core::memory::maintenance::{
    AutoMemoryRun, AutoMemoryRunReason, MemoryMaintenanceEvent, MemoryMaintenanceSnapshot,
    MemoryMaintenanceStatus, MemoryMaintenanceTrigger, MEMORY_MAINTENANCE_ROLLOUT_EVENT,
};
use klaus_agent_core::memory::snapshot_decision::{
    prepare_agent_memory_snapshot_request, AgentMemorySnapshotDecisionAudit,
    AgentMemorySnapshotDecisionPolicy, AgentMemorySnapshotDecisionRequest,
    AgentMemorySnapshotDecisionSource, AgentMemorySnapshotDecisionStatus, AgentMemorySnapshotDiff,
    AgentMemorySnapshotPolicyAction, AGENT_MEMORY_SNAPSHOT_DECISION_ROLLOUT_EVENT,
};
use klaus_agent_core::observability::cost_tracker::ModelPricing;
use klaus_agent_core::runtime_process::{
    RuntimeEnv, RuntimeProcessContext, RuntimeProcessSupervisor,
};
use klaus_agent_core::schemas::content::{ContentBlock, HarnessEnvelope, TextBlock};
use klaus_agent_core::schemas::message::{Message, Usage, UserMessage};
use klaus_agent_core::schemas::rollout::{SessionMeta, UsageLedgerItem};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::{
    AppendEventOptions, CheckpointRecord, CompactionRecord, ListOptions, LiveRollout, ResumeResult,
    RollbackRecord, SessionStore, SubAgentEffort, SubAgentMeta, SubAgentTranscript, ThreadPage,
};
use klaus_agent_core::testing::{ev, fake_provider, scripted_call_model, ScriptedTurns};
use klaus_agent_core::types::permission::{ApprovalKind, ExternalPermissionMode};
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, Waker};
use tokio::io::AsyncReadExt;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{timeout, Duration};

/// Safety net for `timeout(..)` waits whose assertion is LIVENESS ("the peer
/// task/thread does eventually enter the gate / reply / finish"), never
/// latency. A host saturated by a parallel build can starve a freshly spawned
/// task for many seconds, so these waits must be generous: a genuine hang still
/// fails the test, just later. Negative assertions ("must NOT have replied
/// yet", e.g. `from_millis(100)`) keep their own short, deliberate windows.
const CROSS_TASK_LIVENESS_TIMEOUT: Duration = Duration::from_secs(120);

/// Serializes every test that points `KLAUS_CONFIG_DIR` at a temp directory.
/// That variable is PROCESS-global and the task store reads it on every call, so
/// two such tests running in parallel silently swap each other's store: one
/// creates a task under its own dir while the other's query resolves to a
/// different dir and sees a smaller list. `klaus_agent_core`'s equivalent lock is
/// `pub(crate)` and unreachable from here, and this crate's cases only race one
/// another, so a crate-local lock is sufficient.
static CONFIG_DIR_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Points `KLAUS_CONFIG_DIR` at `dir` for as long as the guard lives, holding
/// [`CONFIG_DIR_ENV_LOCK`] throughout and restoring the previous value on drop.
///
/// Taking the lock INSIDE the guard is deliberate: the lock and the env write can
/// then never be separated, which is exactly the mistake that made the TodoV2
/// cases flaky when each of them open-coded its own unlocked guard.
struct ConfigDirEnvGuard {
    previous: Option<std::ffi::OsString>,
    _lock: std::sync::MutexGuard<'static, ()>,
}

impl ConfigDirEnvGuard {
    fn set(dir: &std::path::Path) -> Self {
        use klaus_agent_core::runtime_process::set_process_environment_var;
        // A poisoned lock still serializes correctly here: the guarded data is
        // `()`, and every guard restores the variable on drop.
        let lock = CONFIG_DIR_ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous = std::env::var_os("KLAUS_CONFIG_DIR");
        set_process_environment_var("KLAUS_CONFIG_DIR", dir);
        Self {
            previous,
            _lock: lock,
        }
    }
}

impl Drop for ConfigDirEnvGuard {
    fn drop(&mut self) {
        use klaus_agent_core::runtime_process::{
            remove_process_environment_var, set_process_environment_var,
        };
        match self.previous.take() {
            Some(value) => set_process_environment_var("KLAUS_CONFIG_DIR", value),
            None => remove_process_environment_var("KLAUS_CONFIG_DIR"),
        }
    }
}

fn runtime_value(source: &str) -> RuntimeJson {
    runtime_json::parse(&RuntimeString::from_utf8(source)).expect("valid runtime JSON fixture")
}

fn oauth_tokens_json(tokens: &OAuthTokens) -> String {
    runtime_codec::encode_text(tokens)
        .expect("OAuth token fixture encodes")
        .try_to_utf8()
        .expect("OAuth token fixture JSON is scalar UTF-8")
}

fn decode_oauth_tokens(source: &str) -> OAuthTokens {
    runtime_codec::decode_text(&RuntimeString::from_utf8(source))
        .expect("OAuth token fixture decodes")
}

fn test_process_context() -> RuntimeProcessContext {
    static TEST_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    static TEST_CONTEXT: OnceLock<RuntimeProcessContext> = OnceLock::new();
    TEST_CONTEXT
        .get_or_init(|| {
            let runtime = TEST_RUNTIME.get_or_init(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .expect("protocol test process Tokio runtime")
            });
            let supervisor = RuntimeProcessSupervisor::initialize(runtime.handle().clone())
                .expect("protocol test process supervisor");
            RuntimeProcessContext::initialize(supervisor).expect("protocol test process context")
        })
        .clone()
}

#[test]
fn qwen_reasoning_dialect_uses_the_existing_capability_field() {
    let capabilities: ModelCapabilitiesInit = runtime_codec::decode_text(&RuntimeString::from(
        r#"{"reasoningDialect":"qwen","thinking":{"adaptive":false,"interleaved":false}}"#,
    ))
    .expect("declared thinking and its request dialect decode together");
    assert_eq!(
        openai_reasoning_dialect_from_init(Some(&capabilities)),
        OpenAiReasoningDialect::Qwen
    );
    assert!(capabilities.thinking.is_some());
}

#[test]
fn qoder_reasoning_dialect_uses_the_existing_capability_field() {
    let capabilities: ModelCapabilitiesInit =
        runtime_codec::decode_text(&RuntimeString::from(r#"{"reasoningDialect":"qoder"}"#))
            .expect("the existing optional capability field accepts the Qoder dialect");
    assert_eq!(
        openai_reasoning_dialect_from_init(Some(&capabilities)),
        OpenAiReasoningDialect::Qoder,
    );
    assert_eq!(
        openai_reasoning_dialect_from_init(None),
        OpenAiReasoningDialect::OpenAi,
    );
}

#[test]
fn mcp_stdio_init_preserves_exact_environment_carrier() {
    let surrogate = RuntimeString::from_utf16([0xd800]);
    let mut environment = RuntimeEnv::new();
    environment.insert(RuntimeString::from("B"), RuntimeString::from("first"));
    environment.insert(RuntimeString::from("A"), surrogate.clone());
    environment.insert(RuntimeString::from("B"), RuntimeString::from("last"));

    let init = ProtocolMcpServerInit::Stdio {
        name: RuntimeString::from("ordered"),
        command: RuntimeString::from("node"),
        args: None,
        env: Some(environment.clone()),
        always_load: None,
        disabled_tools: None,
    };
    let config = mcp_server_config_from_init(&init).expect("valid stdio MCP init");
    let McpServerConfig::Stdio { env: Some(env), .. } = config else {
        panic!("expected exact stdio environment")
    };

    assert_eq!(env, environment);
    assert_eq!(
        env.entries()
            .map(|(key, value)| (key, value.clone()))
            .collect::<Vec<_>>(),
        vec![
            (RuntimeString::from("B"), RuntimeString::from("last")),
            (RuntimeString::from("A"), surrogate),
        ]
    );
}

/// The per-tool deny list has to survive the wire, or a host can only offer
/// "whole server on/off" while the engine already knows how to drop single tools
/// after `tools/list`. It rides on the server, so it also survives a reload.
#[test]
fn mcp_init_carries_the_per_tool_deny_list_on_every_transport() {
    let deny = Some(vec![
        RuntimeString::from("dangerous"),
        RuntimeString::from("noisy"),
    ]);
    let expected = Some(vec!["dangerous".to_string(), "noisy".to_string()]);

    let stdio = mcp_server_config_from_init(&ProtocolMcpServerInit::Stdio {
        name: RuntimeString::from("srv"),
        command: RuntimeString::from("node"),
        args: None,
        env: None,
        always_load: None,
        disabled_tools: deny.clone(),
    })
    .expect("valid stdio MCP init");
    assert_eq!(stdio.options().disabled_tools, expected);

    let http = mcp_server_config_from_init(&ProtocolMcpServerInit::Http {
        name: RuntimeString::from("srv"),
        url: RuntimeString::from("https://example.invalid/mcp"),
        headers: None,
        oauth: None,
        always_load: None,
        disabled_tools: deny.clone(),
    })
    .expect("valid http MCP init");
    assert_eq!(http.options().disabled_tools, expected);

    let sse = mcp_server_config_from_init(&ProtocolMcpServerInit::Sse {
        name: RuntimeString::from("srv"),
        url: RuntimeString::from("https://example.invalid/sse"),
        headers: None,
        oauth: None,
        always_load: None,
        disabled_tools: deny,
    })
    .expect("valid sse MCP init");
    assert_eq!(sse.options().disabled_tools, expected);

    // No list at all stays absent: an empty deny list must not appear as one the
    // engine has to compare against on every tool.
    let none = mcp_server_config_from_init(&ProtocolMcpServerInit::Stdio {
        name: RuntimeString::from("srv"),
        command: RuntimeString::from("node"),
        args: None,
        env: None,
        always_load: None,
        disabled_tools: None,
    })
    .expect("valid stdio MCP init");
    assert_eq!(none.options().disabled_tools, None);
}

fn text_turn(t: &str) -> Vec<ModelStreamEvent> {
    vec![
        ev::message_start(None),
        ev::text(t),
        ev::stop(Some(ModelStopReason::EndTurn), None),
    ]
}

#[test]
fn dynamic_tool_namespace_description_limit_uses_javascript_utf16_length() {
    let namespace = |description: String| {
        vec![ProtocolDynamicToolSpec::Namespace(
            ProtocolDynamicToolNamespaceSpec {
                name: "desktop".into(),
                description: description.into(),
                tools: vec![ProtocolDynamicToolNamespaceTool::Function(
                    ProtocolDynamicToolFunctionSpec {
                        name: "lookup".into(),
                        description: "lookup".into(),
                        input_schema: runtime_json_from_scalar_value(&json!({ "type": "object" }))
                            .unwrap(),
                        defer_loading: false,
                        read_only: false,
                    },
                )],
            },
        )]
    };

    assert!(validate_dynamic_tools(&namespace("😀".repeat(512))).is_ok());
    assert_eq!(
        validate_dynamic_tools(&namespace("😀".repeat(513))).unwrap_err(),
        "dynamic tool namespace description must be at most 1024 characters"
    );
}

/// The host's `readOnly` reaches the permission engine through the tool the
/// adapter presents, so it has to be answered from the spec and not hardcoded.
/// A host read tool that reports itself as mutating asks the user before every
/// question it answers and is denied outright in plan mode; the default stays
/// mutating, because a host that declares nothing has not earned the pass.
#[test]
fn a_dynamic_tool_reports_the_hosts_read_only_declaration() {
    let spec = |name: &str, read_only: bool| ProtocolDynamicToolFunctionSpec {
        name: name.into(),
        description: "probe".into(),
        input_schema: runtime_json_from_scalar_value(&json!({ "type": "object" })).unwrap(),
        defer_loading: false,
        read_only,
    };
    let adapter = |tool: ProtocolDynamicToolFunctionSpec| {
        dynamic_tool_adapter(None, None, &tool, Weak::new()).expect("adapter builds")
    };
    let input = runtime_json_from_scalar_value(&json!({})).unwrap();

    assert!(adapter(spec("klaus_query", true)).is_read_only(&input));
    assert!(!adapter(spec("klaus_action", false)).is_read_only(&input));
    assert!(adapter(spec("web_search", true)).task_scope_host_capability());
    for name in [
        "reply_to_friend",
        "ask_friend_for_information",
        "request_directory_access",
    ] {
        assert!(
            adapter(spec(name, false)).task_scope_host_capability(),
            "{name} answers the task's own requester or owner"
        );
    }
    // A task may start a collaboration with another friend; the task's
    // permission mode decides whether the owner approves it first.
    assert!(adapter(spec("send_message_to_friend", false)).task_scope_host_capability());
    assert!(!adapter(spec("arbitrary_host_command", true)).task_scope_host_capability());
}

struct ProtocolElicitationServer {
    transport: Mutex<Option<Arc<dyn Transport>>>,
    pending: Arc<Mutex<HashMap<i64, oneshot::Sender<RuntimeJson>>>>,
}

impl ProtocolElicitationServer {
    fn new() -> Self {
        Self {
            transport: Mutex::new(None),
            pending: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn request_elicitation(&self, id: i64, params: Value) -> RuntimeJson {
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, tx);
        let transport = self.transport.lock().unwrap().clone().unwrap();
        let mut request = RuntimeObject::new();
        request.insert("jsonrpc", RuntimeJson::from("2.0"));
        request.insert("id", RuntimeJson::from(id));
        request.insert("method", RuntimeJson::from("elicitation/create"));
        request.insert(
            "params",
            runtime_json_from_scalar_value(&params).expect("scalar MCP request fixture"),
        );
        transport.send(RuntimeJson::Object(request)).await.unwrap();
        rx.await.unwrap_or(RuntimeJson::Null)
    }
}

#[async_trait]
impl McpInProcessServer for ProtocolElicitationServer {
    async fn connect(&self, transport: Box<dyn Transport>) {
        let transport: Arc<dyn Transport> = Arc::from(transport);
        *self.transport.lock().unwrap() = Some(transport.clone());
        let (tx, mut rx) = mpsc::unbounded_channel();
        transport.start(tx).await.unwrap();
        let pending = self.pending.clone();
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                match event {
                    TransportEvent::Message(message) => {
                        if message.get("method").is_none()
                            && (message.get("result").is_some() || message.get("error").is_some())
                        {
                            if let Some(id) = message.get("id").and_then(RuntimeJson::as_i64) {
                                if let Some(sender) = pending.lock().unwrap().remove(&id) {
                                    let _ = sender.send(
                                        message.get("result").cloned().unwrap_or(RuntimeJson::Null),
                                    );
                                }
                            }
                            continue;
                        }
                        let Some(method) = message.get("method").and_then(RuntimeJson::as_str)
                        else {
                            continue;
                        };
                        let Some(id) = message.get("id").cloned() else {
                            continue;
                        };
                        let result = if method == "initialize" {
                            runtime_value(
                                r#"{
                                    "protocolVersion": "2025-06-18",
                                    "capabilities": {},
                                    "serverInfo": { "name": "protocol-elicitation", "version": "1.0.0" }
                                }"#,
                            )
                        } else {
                            runtime_value("{}")
                        };
                        let mut response = RuntimeObject::new();
                        response.insert("jsonrpc", RuntimeJson::from("2.0"));
                        response.insert("id", id);
                        response.insert("result", result);
                        transport.send(RuntimeJson::Object(response)).await.unwrap();
                    }
                    TransportEvent::Close => break,
                    TransportEvent::Error(_) => {}
                }
            }
        });
    }
}

fn tool_use_turn(id: &str, name: &str, input: Value) -> Vec<ModelStreamEvent> {
    let mut t = vec![ev::message_start(None)];
    let input = runtime_json_from_scalar_value(&input).expect("scalar tool input fixture");
    t.extend(ev::tool_use(id, name, &input));
    t.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    t
}

fn make_agent(cwd: &str, turns: ScriptedTurns, config: Option<Value>) -> Arc<Agent> {
    make_agent_with_memory(cwd, turns, config, None)
}

fn make_agent_with_memory(
    cwd: &str,
    turns: ScriptedTurns,
    config: Option<Value>,
    agent_memory: Option<Arc<dyn AgentMemoryHost>>,
) -> Arc<Agent> {
    make_agent_with_memory_and_hooks(cwd, turns, config, agent_memory, Vec::new())
}

fn make_agent_with_memory_and_hooks(
    cwd: &str,
    turns: ScriptedTurns,
    config: Option<Value>,
    agent_memory: Option<Arc<dyn AgentMemoryHost>>,
    hooks: Vec<Hook>,
) -> Arc<Agent> {
    Arc::new(Agent::new(AgentOptions {
        process_context: test_process_context(),
        provider: Arc::new(fake_provider()) as Arc<dyn ModelProvider>,
        config,
        tools: None,
        web_fetch_page: None,
        browser_runtime: None,
        capability_roots: Vec::new(),
        cwd: Some(cwd.to_string()),
        deps: EngineDepsOverrides {
            call_model: Some(scripted_call_model(turns)),
            estimate_tokens: None,
            compactor: Some(noop_compactor()),
            uuid: None,
            now: None,
        },
        compactor: None,
        approval_handler: None,
        approval_reviewer: None,
        elicit_handler: None,
        credential_store: None,
        hooks,
        observer: None,
        logger: None,
        pricing: None,
        agents: None,
        include_default_agents: None,
        skills: None,
        output_styles: None,
        workflows: None,
        mcp_catalog: None,
        plugin_catalog: None,
        lsp_servers: None,
        agent_memory,
        mcp_clients: None,
        mcp_server_configs: Vec::new(),
        mcp_tools: None,
        refresh_tools: None,
        system_prompt_provider: None,
        plugins: Vec::new(),
        plugin_load_report: None,
    }))
}

struct ProtocolTestMemoryLayout {
    dir: String,
}

impl AgentMemoryLayout for ProtocolTestMemoryLayout {
    fn dir_for(&self, _agent_type: &str, _scope: AgentMemoryScope) -> String {
        self.dir.clone()
    }

    fn snapshot_dir_for(&self, _agent_type: &str) -> String {
        std::path::Path::new(&self.dir)
            .join("snapshots")
            .to_string_lossy()
            .into_owned()
    }
}

struct ProtocolTestMemoryHost {
    layout: ProtocolTestMemoryLayout,
}

#[async_trait]
impl AgentMemoryHost for ProtocolTestMemoryHost {
    fn layout(&self) -> &dyn AgentMemoryLayout {
        &self.layout
    }
}

fn make_recording_agent(cwd: &str, requests: Arc<Mutex<Vec<ModelRequest>>>) -> Arc<Agent> {
    let call_model = {
        let requests = requests.clone();
        Box::new(
            move |req: ModelRequest, signal: AbortSignal| -> ModelStream {
                requests.lock().unwrap().push(req);
                Box::pin(
                    futures::stream::iter(text_turn("ok"))
                        .take_while(move |_| futures::future::ready(!signal.is_cancelled())),
                )
            },
        )
    };
    Arc::new(Agent::new(AgentOptions {
        process_context: test_process_context(),
        provider: Arc::new(fake_provider()) as Arc<dyn ModelProvider>,
        config: None,
        tools: None,
        web_fetch_page: None,
        browser_runtime: None,
        capability_roots: Vec::new(),
        cwd: Some(cwd.to_string()),
        deps: EngineDepsOverrides {
            call_model: Some(call_model),
            estimate_tokens: None,
            compactor: Some(noop_compactor()),
            uuid: None,
            now: None,
        },
        compactor: None,
        approval_handler: None,
        approval_reviewer: None,
        elicit_handler: None,
        credential_store: None,
        hooks: vec![],
        observer: None,
        logger: None,
        pricing: None,
        agents: None,
        include_default_agents: None,
        skills: None,
        output_styles: None,
        workflows: None,
        mcp_catalog: None,
        plugin_catalog: None,
        lsp_servers: None,
        agent_memory: None,
        mcp_clients: None,
        mcp_server_configs: Vec::new(),
        mcp_tools: None,
        refresh_tools: None,
        system_prompt_provider: None,
        plugins: Vec::new(),
        plugin_load_report: None,
    }))
}

fn make_blocking_agent(cwd: &str, started: Arc<Notify>, cancelled: Arc<Notify>) -> Arc<Agent> {
    let call_model = Box::new(
        move |_req: ModelRequest, signal: AbortSignal| -> ModelStream {
            started.notify_one();
            let cancelled = cancelled.clone();
            Box::pin(futures::stream::once(async move {
                signal.cancelled().await;
                cancelled.notify_one();
                ev::stop(Some(ModelStopReason::EndTurn), None)
            }))
        },
    );
    Arc::new(Agent::new(AgentOptions {
        process_context: test_process_context(),
        provider: Arc::new(fake_provider()) as Arc<dyn ModelProvider>,
        config: None,
        tools: None,
        web_fetch_page: None,
        browser_runtime: None,
        capability_roots: Vec::new(),
        cwd: Some(cwd.to_string()),
        deps: EngineDepsOverrides {
            call_model: Some(call_model),
            estimate_tokens: None,
            compactor: Some(noop_compactor()),
            uuid: None,
            now: None,
        },
        compactor: None,
        approval_handler: None,
        approval_reviewer: None,
        elicit_handler: None,
        credential_store: None,
        hooks: vec![],
        observer: None,
        logger: None,
        pricing: None,
        agents: None,
        include_default_agents: None,
        skills: None,
        output_styles: None,
        workflows: None,
        mcp_catalog: None,
        plugin_catalog: None,
        lsp_servers: None,
        agent_memory: None,
        mcp_clients: None,
        mcp_server_configs: Vec::new(),
        mcp_tools: None,
        refresh_tools: None,
        system_prompt_provider: None,
        plugins: Vec::new(),
        plugin_load_report: None,
    }))
}

struct GatedSessionEndHook {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

#[async_trait]
impl HookFn for GatedSessionEndHook {
    async fn run(&self, _input: HookInput) -> HookOutcome {
        self.entered.notify_one();
        self.release.notified().await;
        HookOutcome::default()
    }
}

fn make_gated_teardown_agent(
    cwd: &str,
    started: Arc<Notify>,
    cancelled: Arc<Notify>,
    release_turn: Arc<Notify>,
    hook_entered: Arc<Notify>,
    release_hook: Arc<Notify>,
) -> Arc<Agent> {
    let call_model = Box::new(
        move |_req: ModelRequest, signal: AbortSignal| -> ModelStream {
            started.notify_one();
            let cancelled = cancelled.clone();
            let release_turn = release_turn.clone();
            Box::pin(futures::stream::once(async move {
                signal.cancelled().await;
                cancelled.notify_one();
                release_turn.notified().await;
                ev::stop(Some(ModelStopReason::EndTurn), None)
            }))
        },
    );
    Arc::new(Agent::new(AgentOptions {
        process_context: test_process_context(),
        provider: Arc::new(fake_provider()) as Arc<dyn ModelProvider>,
        config: None,
        tools: None,
        web_fetch_page: None,
        browser_runtime: None,
        capability_roots: Vec::new(),
        cwd: Some(cwd.to_string()),
        deps: EngineDepsOverrides {
            call_model: Some(call_model),
            estimate_tokens: None,
            compactor: Some(noop_compactor()),
            uuid: None,
            now: None,
        },
        compactor: None,
        approval_handler: None,
        approval_reviewer: None,
        elicit_handler: None,
        credential_store: None,
        hooks: vec![Hook {
            event: HookEvent::SessionEnd,
            matcher: None,
            handler: HookHandler::Fn(Arc::new(GatedSessionEndHook {
                entered: hook_entered,
                release: release_hook,
            })),
            is_async: false,
        }],
        observer: None,
        logger: None,
        pricing: None,
        agents: None,
        include_default_agents: None,
        skills: None,
        output_styles: None,
        workflows: None,
        mcp_catalog: None,
        plugin_catalog: None,
        lsp_servers: None,
        agent_memory: None,
        mcp_clients: None,
        mcp_server_configs: Vec::new(),
        mcp_tools: None,
        refresh_tools: None,
        system_prompt_provider: None,
        plugins: Vec::new(),
        plugin_load_report: None,
    }))
}

fn make_seed_then_blocking_agent(
    cwd: &str,
    started: Arc<Notify>,
    cancelled: Arc<Notify>,
) -> Arc<Agent> {
    let calls = Arc::new(AtomicUsize::new(0));
    let call_model = Box::new(
        move |_req: ModelRequest, signal: AbortSignal| -> ModelStream {
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                return Box::pin(futures::stream::iter(text_turn("seed reply")));
            }
            started.notify_one();
            let cancelled = cancelled.clone();
            Box::pin(futures::stream::once(async move {
                signal.cancelled().await;
                cancelled.notify_one();
                ev::stop(Some(ModelStopReason::EndTurn), None)
            }))
        },
    );
    Arc::new(Agent::new(AgentOptions {
        process_context: test_process_context(),
        provider: Arc::new(fake_provider()) as Arc<dyn ModelProvider>,
        config: None,
        tools: None,
        web_fetch_page: None,
        browser_runtime: None,
        capability_roots: Vec::new(),
        cwd: Some(cwd.to_string()),
        deps: EngineDepsOverrides {
            call_model: Some(call_model),
            estimate_tokens: None,
            compactor: Some(noop_compactor()),
            uuid: None,
            now: None,
        },
        compactor: None,
        approval_handler: None,
        approval_reviewer: None,
        elicit_handler: None,
        credential_store: None,
        hooks: vec![],
        observer: None,
        logger: None,
        pricing: None,
        agents: None,
        include_default_agents: None,
        skills: None,
        output_styles: None,
        workflows: None,
        mcp_catalog: None,
        plugin_catalog: None,
        lsp_servers: None,
        agent_memory: None,
        mcp_clients: None,
        mcp_server_configs: Vec::new(),
        mcp_tools: None,
        refresh_tools: None,
        system_prompt_provider: None,
        plugins: Vec::new(),
        plugin_load_report: None,
    }))
}

type Boxed<T> = Box<T>;
#[allow(clippy::type_complexity)]
fn duplex_pair() -> (
    (
        Boxed<dyn AsyncRead + Unpin + Send>,
        Boxed<dyn AsyncWrite + Unpin + Send>,
    ),
    (
        Boxed<dyn AsyncRead + Unpin + Send>,
        Boxed<dyn AsyncWrite + Unpin + Send>,
    ),
) {
    let (client_side, server_side) = tokio::io::duplex(1 << 20);
    let (client_r, client_w) = tokio::io::split(client_side);
    let (server_r, server_w) = tokio::io::split(server_side);
    (
        (Box::new(server_r), Box::new(server_w)),
        (Box::new(client_r), Box::new(client_w)),
    )
}

#[derive(Debug, Default)]
struct QueueFlushGate {
    armed: AtomicBool,
    fail_armed: AtomicBool,
    blocked: AtomicBool,
    released: AtomicBool,
    entered: Notify,
    release: Notify,
    completed: AtomicUsize,
    completed_notify: Notify,
}

impl QueueFlushGate {
    fn arm_next_queue_flush(&self) {
        self.blocked.store(false, Ordering::SeqCst);
        self.released.store(false, Ordering::SeqCst);
        assert!(
            self.armed
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok(),
            "only one queue flush may be armed at a time"
        );
    }

    fn fail_next_queue_flush(&self) {
        assert!(
            self.fail_armed
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok(),
            "only one queue flush failure may be armed at a time"
        );
    }

    fn take_queue_flush_failure(&self) -> bool {
        self.fail_armed.swap(false, Ordering::SeqCst)
    }

    async fn block_if_armed(&self) {
        if !self.armed.swap(false, Ordering::SeqCst) {
            return;
        }
        self.blocked.store(true, Ordering::SeqCst);
        self.entered.notify_waiters();
        loop {
            let notified = self.release.notified();
            if self.released.load(Ordering::SeqCst) {
                break;
            }
            notified.await;
        }
    }

    async fn wait_until_blocked(&self) {
        loop {
            let notified = self.entered.notified();
            if self.blocked.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }

    fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.release.notify_waiters();
    }

    fn record_completed_queue_flush(&self) {
        self.completed.fetch_add(1, Ordering::SeqCst);
        self.completed_notify.notify_waiters();
    }

    async fn wait_for_completed_queue_flushes(&self, target: usize) {
        loop {
            let notified = self.completed_notify.notified();
            if self.completed.load(Ordering::SeqCst) >= target {
                return;
            }
            notified.await;
        }
    }
}

#[derive(Debug)]
struct GatedQueueLiveRollout {
    inner: Box<dyn LiveRollout>,
    gate: Arc<QueueFlushGate>,
    pending_queue_appends: AtomicUsize,
    pending_queue_failures: AtomicUsize,
}

#[derive(Debug, Default)]
struct FailingSnapshotRecoveryRollout {
    snapshot_append_pending: AtomicBool,
    flush_entered: AtomicBool,
    flush_entered_notify: Notify,
    flush_released: AtomicBool,
    flush_release_notify: Notify,
}

/// Deterministically panic at the exact maintenance append boundary. Core
/// owns and catches that append, then must make every later maintenance
/// query fail with the retained typed persistence error instead of exposing
/// a stale snapshot as authoritative.
#[derive(Debug, Default)]
struct PanickingMaintenanceRollout {
    append_panicked: AtomicBool,
    append_panicked_notify: Notify,
}

impl PanickingMaintenanceRollout {
    async fn wait_until_append_panicked(&self) {
        loop {
            let notified = self.append_panicked_notify.notified();
            if self.append_panicked.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }
}

#[async_trait]
impl LiveRollout for PanickingMaintenanceRollout {
    async fn prepare_batch(
        &self,
        maximum_records: usize,
    ) -> std::io::Result<klaus_agent_core::store::session_store::PreparedRolloutBatch<'_>> {
        klaus_agent_core::testing::immediate_rollout::prepare_immediate_test_rollout_batch(
            self,
            maximum_records,
        )
    }

    async fn append_message(&self, _message: Message) {}

    async fn append_turn_context(
        &self,
        _ctx: klaus_agent_core::schemas::rollout::TurnContextSnapshot,
    ) {
    }

    async fn append_compaction(&self, _record: CompactionRecord) {}

    async fn append_event(&self, event: RuntimeString, _opts: Option<AppendEventOptions>) {
        if event == MEMORY_MAINTENANCE_ROLLOUT_EVENT {
            self.append_panicked.store(true, Ordering::SeqCst);
            self.append_panicked_notify.notify_waiters();
            panic!("injected memory maintenance rollout append panic");
        }
    }

    async fn append_rollback(&self, _record: RollbackRecord) {}

    async fn append_checkpoint(&self, _record: CheckpointRecord) {}

    async fn append_usage(&self, _record: UsageLedgerItem) {}

    async fn append_subagent_lifecycle(&self, _record: SubagentLifecycleRecord) {}

    async fn flush(&self) -> io::Result<()> {
        Ok(())
    }

    async fn shutdown(&self) -> io::Result<()> {
        Ok(())
    }
}

impl FailingSnapshotRecoveryRollout {
    async fn wait_until_flush_entered(&self) {
        loop {
            let notified = self.flush_entered_notify.notified();
            if self.flush_entered.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }

    fn release_flush_failure(&self) {
        self.flush_released.store(true, Ordering::SeqCst);
        self.flush_release_notify.notify_waiters();
    }
}

#[async_trait]
impl LiveRollout for FailingSnapshotRecoveryRollout {
    async fn prepare_batch(
        &self,
        maximum_records: usize,
    ) -> std::io::Result<klaus_agent_core::store::session_store::PreparedRolloutBatch<'_>> {
        klaus_agent_core::testing::immediate_rollout::prepare_immediate_test_rollout_batch(
            self,
            maximum_records,
        )
    }

    async fn append_message(&self, _message: Message) {}

    async fn append_turn_context(
        &self,
        _ctx: klaus_agent_core::schemas::rollout::TurnContextSnapshot,
    ) {
    }

    async fn append_compaction(&self, _record: CompactionRecord) {}

    async fn append_event(&self, event: RuntimeString, _opts: Option<AppendEventOptions>) {
        if event == AGENT_MEMORY_SNAPSHOT_DECISION_ROLLOUT_EVENT {
            self.snapshot_append_pending.store(true, Ordering::SeqCst);
        }
    }

    async fn append_rollback(&self, _record: RollbackRecord) {}

    async fn append_checkpoint(&self, _record: CheckpointRecord) {}

    async fn append_usage(&self, _record: UsageLedgerItem) {}

    async fn append_subagent_lifecycle(&self, _record: SubagentLifecycleRecord) {}

    async fn flush(&self) -> io::Result<()> {
        if self.snapshot_append_pending.swap(false, Ordering::SeqCst) {
            self.flush_entered.store(true, Ordering::SeqCst);
            self.flush_entered_notify.notify_waiters();
            loop {
                let notified = self.flush_release_notify.notified();
                if self.flush_released.load(Ordering::SeqCst) {
                    break;
                }
                notified.await;
            }
            Err(io::Error::other(
                "injected snapshot recovery rollout flush failure",
            ))
        } else {
            Ok(())
        }
    }

    async fn shutdown(&self) -> io::Result<()> {
        Ok(())
    }
}

impl GatedQueueLiveRollout {
    fn should_record_event(&self, event: &RuntimeString) -> bool {
        if event == "queue_state" {
            self.pending_queue_appends.fetch_add(1, Ordering::SeqCst);
            if self.gate.take_queue_flush_failure() {
                // Rejected checkpoints must not become durable during cleanup.
                self.pending_queue_failures.fetch_add(1, Ordering::SeqCst);
                return false;
            }
        }
        true
    }

    fn new(inner: Box<dyn LiveRollout>, gate: Arc<QueueFlushGate>) -> Self {
        Self {
            inner,
            gate,
            pending_queue_appends: AtomicUsize::new(0),
            pending_queue_failures: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl LiveRollout for GatedQueueLiveRollout {
    async fn prepare_batch(
        &self,
        maximum_records: usize,
    ) -> io::Result<klaus_agent_core::store::session_store::PreparedRolloutBatch<'_>> {
        let admission = self.inner.prepare_batch(maximum_records).await?;
        Ok(
            klaus_agent_core::store::session_store::PreparedRolloutBatch::new(move |items| {
                assert!(items.len() <= maximum_records);
                admission.commit(
                    items
                        .into_iter()
                        .filter(|item| match item {
                            klaus_agent_core::schemas::rollout::RolloutItem::Event(event) => {
                                self.should_record_event(&event.event)
                            }
                            _ => true,
                        })
                        .collect(),
                );
            }),
        )
    }

    async fn append_message(&self, message: Message) {
        self.inner.append_message(message).await;
    }

    async fn append_turn_context(
        &self,
        ctx: klaus_agent_core::schemas::rollout::TurnContextSnapshot,
    ) {
        self.inner.append_turn_context(ctx).await;
    }

    async fn append_compaction(&self, record: CompactionRecord) {
        self.inner.append_compaction(record).await;
    }

    async fn append_event(&self, event: RuntimeString, opts: Option<AppendEventOptions>) {
        if !self.should_record_event(&event) {
            return;
        }
        self.inner.append_event(event, opts).await;
    }

    async fn append_rollback(&self, record: RollbackRecord) {
        self.inner.append_rollback(record).await;
    }

    async fn append_checkpoint(&self, record: CheckpointRecord) {
        self.inner.append_checkpoint(record).await;
    }

    async fn append_usage(&self, record: UsageLedgerItem) {
        self.inner.append_usage(record).await;
    }

    async fn append_subagent_lifecycle(&self, record: SubagentLifecycleRecord) {
        self.inner.append_subagent_lifecycle(record).await;
    }

    async fn append_background_process_lifecycle(
        &self,
        record: klaus_agent_core::schemas::background_process::BackgroundProcessRecord,
    ) {
        self.inner.append_background_process_lifecycle(record).await;
    }

    async fn flush(&self) -> io::Result<()> {
        let is_queue_flush = self.pending_queue_appends.swap(0, Ordering::SeqCst) > 0;
        if self.pending_queue_failures.swap(0, Ordering::SeqCst) > 0 {
            return Err(io::Error::other("forced queue checkpoint flush failure"));
        }
        if is_queue_flush {
            self.gate.block_if_armed().await;
        }
        let result = self.inner.flush().await;
        if is_queue_flush && result.is_ok() {
            self.gate.record_completed_queue_flush();
        }
        result
    }

    async fn shutdown(&self) -> io::Result<()> {
        self.inner.shutdown().await
    }
}

struct GatedQueueStore {
    inner: Arc<JsonlSessionStore>,
    gate: Arc<QueueFlushGate>,
}

impl GatedQueueStore {
    fn wrap(&self, rollout: Box<dyn LiveRollout>) -> Box<dyn LiveRollout> {
        Box::new(GatedQueueLiveRollout::new(rollout, self.gate.clone()))
    }
}

#[async_trait]
impl SessionStore for GatedQueueStore {
    async fn create(&self, meta: SessionMeta) -> io::Result<Box<dyn LiveRollout>> {
        Ok(self.wrap(self.inner.create(meta).await?))
    }

    async fn resume(&self, thread_id: &str) -> io::Result<ResumeResult> {
        self.inner.resume(thread_id).await
    }

    async fn resume_for_write(&self, thread_id: &str) -> io::Result<ResumedLiveRollout> {
        let resumed = self.inner.resume_for_write(thread_id).await?;
        Ok(ResumedLiveRollout {
            resume: resumed.resume,
            rollout: self.wrap(resumed.rollout),
        })
    }

    async fn reopen(&self, thread_id: &str) -> io::Result<Box<dyn LiveRollout>> {
        Ok(self.wrap(self.inner.reopen(thread_id).await?))
    }

    async fn list(&self, options: ListOptions) -> io::Result<ThreadPage> {
        self.inner.list(options).await
    }

    async fn archive(&self, thread_id: &str) -> io::Result<String> {
        self.inner.archive(thread_id).await
    }

    async fn unarchive(&self, thread_id: &str) -> UnarchiveSessionResult {
        self.inner.unarchive(thread_id).await
    }

    async fn delete(&self, thread_id: &str) -> io::Result<()> {
        self.inner.delete(thread_id).await
    }

    async fn update_title(&self, thread_id: &str, title: &str) -> io::Result<()> {
        self.inner.update_title(thread_id, title).await
    }

    async fn open_sub_agent(
        &self,
        thread_id: &str,
        agent_id: &str,
        meta: SubAgentMeta,
    ) -> io::Result<Box<dyn LiveRollout>> {
        self.inner.open_sub_agent(thread_id, agent_id, meta).await
    }

    async fn load_sub_agent(
        &self,
        thread_id: &str,
        agent_id: &str,
    ) -> io::Result<SubAgentTranscript> {
        self.inner.load_sub_agent(thread_id, agent_id).await
    }

    async fn list_sub_agents(&self, thread_id: &str) -> io::Result<Vec<String>> {
        self.inner.list_sub_agents(thread_id).await
    }

    async fn count_sessions_touched_since(
        &self,
        since_ms: f64,
        exclude_id: Option<&str>,
    ) -> io::Result<usize> {
        self.inner
            .count_sessions_touched_since(since_ms, exclude_id)
            .await
    }
}

struct RecordingWriter {
    inner: Box<dyn AsyncWrite + Unpin + Send>,
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl AsyncWrite for RecordingWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut *this.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(written)) => {
                this.bytes
                    .lock()
                    .unwrap()
                    .extend_from_slice(&buf[..written]);
                Poll::Ready(Ok(written))
            }
            other => other,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.get_mut().inner).poll_shutdown(cx)
    }
}

struct PartialThenPendingWriter {
    bytes: Arc<Mutex<Vec<u8>>>,
    wrote_first_byte: bool,
    entered: Arc<Notify>,
}

impl AsyncWrite for PartialThenPendingWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if !self.wrote_first_byte && !buf.is_empty() {
            self.bytes.lock().unwrap().push(buf[0]);
            self.wrote_first_byte = true;
            self.entered.notify_one();
            return Poll::Ready(Ok(1));
        }
        Poll::Pending
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Pending
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

struct ZeroThenReadyWriter {
    bytes: Arc<Mutex<Vec<u8>>>,
    first_poll_pending: bool,
    entered: Arc<Notify>,
}

#[derive(Default)]
struct FlushGate {
    entered: Notify,
    released: AtomicBool,
    fail: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl FlushGate {
    async fn wait_until_entered(&self) {
        loop {
            let entered = self.entered.notified();
            if self.waker.lock().unwrap().is_some() {
                return;
            }
            entered.await;
        }
    }

    fn release(&self, fail: bool) {
        self.fail.store(fail, Ordering::SeqCst);
        self.released.store(true, Ordering::SeqCst);
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }
}

struct FullThenGatedFlushWriter {
    bytes: Arc<Mutex<Vec<u8>>>,
    flush: Arc<FlushGate>,
}

struct PanicOnceWriter {
    bytes: Arc<Mutex<Vec<u8>>>,
    panic_next_write: Arc<AtomicBool>,
}

impl AsyncWrite for PanicOnceWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.panic_next_write.swap(false, Ordering::SeqCst) {
            panic!("intentional unsolicited writer panic");
        }
        self.bytes.lock().unwrap().extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for FullThenGatedFlushWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.bytes.lock().unwrap().extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.flush.released.load(Ordering::SeqCst) {
            *self.flush.waker.lock().unwrap() = Some(cx.waker().clone());
            self.flush.entered.notify_waiters();
            return Poll::Pending;
        }
        if self.flush.fail.load(Ordering::SeqCst) {
            Poll::Ready(Err(io::Error::other("forced BoundSession flush failure")))
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for ZeroThenReadyWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.first_poll_pending {
            self.first_poll_pending = false;
            self.entered.notify_one();
            return Poll::Pending;
        }
        self.bytes.lock().unwrap().extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn publication_test_memory_event(run_id: &str) -> MemoryMaintenanceEvent {
    MemoryMaintenanceEvent::AutoMemory {
        run: AutoMemoryRun {
            run_id: run_id.into(),
            session_id: "publication-session".into(),
            trigger: MemoryMaintenanceTrigger::Auto,
            scope: AgentMemoryScope::Project,
            target_dir: "/tmp/publication-memory".into(),
            status: MemoryMaintenanceStatus::Started,
            started_at: 1,
            ended_at: None,
            reason: None,
            error: None,
            message_count: 1,
            cursor_message_uuid: None,
            files_touched: Vec::new(),
            turn_count: None,
        },
    }
}

fn publication_test_snapshot_audit(request_id: &str) -> AgentMemorySnapshotDecisionAudit {
    AgentMemorySnapshotDecisionAudit {
        request: AgentMemorySnapshotDecisionRequest {
            request_id: request_id.into(),
            agent_type: "publication-agent".into(),
            scope: AgentMemoryScope::Project,
            snapshot_timestamp: "1".into(),
            snapshot_id: "snapshot-1".into(),
            local_state_id: "local-1".into(),
            diff: AgentMemorySnapshotDiff {
                added_count: 0,
                modified_count: 0,
                removed_count: 0,
                entries: Vec::new(),
                truncated: false,
            },
            first_use: false,
            requested_at: 1,
            deadline_at: 2,
        },
        status: AgentMemorySnapshotDecisionStatus::Requested,
        decision: None,
        source: None,
        decided_at: None,
        ended_at: None,
        error: None,
    }
}

async fn restorable_publication_test_snapshot_audit(
) -> (std::path::PathBuf, AgentMemorySnapshotDecisionAudit) {
    // Generate the fixture through the same production capture path used
    // by a real snapshot request. Registry restore deliberately rejects a
    // non-User scope or a request id that does not match the captured
    // snapshot/local identity.
    let root = unique_temp_dir("snapshot-recovery-audit");
    let memory_dir = root.join("memory");
    let snapshot_dir = memory_dir.join("snapshots");
    std::fs::create_dir_all(&snapshot_dir).unwrap();
    std::fs::write(
        snapshot_dir.join("snapshot.json"),
        r#"{"updatedAt":"2026-07-18T00:00:00Z"}"#,
    )
    .unwrap();
    std::fs::write(snapshot_dir.join("remote.md"), "remote\n").unwrap();
    std::fs::write(memory_dir.join("local.md"), "local\n").unwrap();
    let layout = ProtocolTestMemoryLayout {
        dir: memory_dir.to_string_lossy().into_owned(),
    };
    let request = prepare_agent_memory_snapshot_request(&layout, "publication-agent", 1, 2)
        .await
        .expect("capture restorable snapshot request")
        .expect("snapshot fixture differs from local memory");
    (
        root,
        AgentMemorySnapshotDecisionAudit {
            request,
            status: AgentMemorySnapshotDecisionStatus::Requested,
            decision: None,
            source: None,
            decided_at: None,
            ended_at: None,
            error: None,
        },
    )
}

fn publication_test_queue_snapshot(
    revision: u64,
) -> klaus_agent_core::engine::message_queue::QueueSnapshot {
    klaus_agent_core::engine::message_queue::QueueSnapshot {
        revision,
        items: Vec::new(),
        status: klaus_agent_core::engine::message_queue::QueueSnapshotStatus::Authoritative,
        error: None,
    }
}

fn publication_test_task_change(
    revision: u64,
    publication_sequence: u64,
) -> klaus_agent_core::tasks::task_store::TaskListChangeEnvelope {
    klaus_agent_core::tasks::task_store::TaskListChangeEnvelope {
        change: klaus_agent_core::tasks::task_store::TaskListChange {
            plan_id: None,
            plan_kind: "tasks".into(),
            session_id: "publication-session".into(),
            task_list_id: "publication-list".into(),
            generation: "publication-generation".into(),
            revision,
            fingerprint: format!("fingerprint-{revision}").into(),
            tasks: Vec::new(),
        },
        publication_sequence,
    }
}

fn bind_to_session(session: Arc<Session>) -> BindSession {
    Arc::new(
        move |_args, _thread_id, _new_thread_id, _before_message_uuid| {
            let session = session.clone();
            Box::pin(async move { Ok::<Arc<Session>, BoxError>(session) })
        },
    )
}

async fn wait_for_recorded_event_count(bytes: &Arc<Mutex<Vec<u8>>>, expected: usize) {
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        loop {
            let count = bytes
                .lock()
                .unwrap()
                .iter()
                .filter(|byte| **byte == b'\n')
                .count();
            if count >= expected {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("protocol events were not fully written");
}

fn recorded_events(bytes: &Arc<Mutex<Vec<u8>>>) -> Vec<ProtocolEvent> {
    let raw = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
    raw.lines()
        .map(|line| {
            runtime_codec::decode_text(&RuntimeString::from_utf8(line))
                .expect("recorded protocol event decodes")
        })
        .collect()
}

fn fast_shutdown_timings() -> ProtocolShutdownTimings {
    ProtocolShutdownTimings {
        grace: Duration::from_millis(50),
        force: Duration::from_millis(100),
        transport: Duration::from_millis(150),
        close: Duration::from_millis(200),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_stop_waits_for_the_complete_admission_close_boundary() {
    let agent = make_agent(".", vec![], None);
    let (server_output, _client_input) = tokio::io::duplex(4096);
    let mut raw_server = ProtocolServer::new(ProtocolServerOptions {
        create_session: make_create_session(agent, None),
        lifecycle: None,
        plugin_admin: None,
        credential_store: None,
        input: Box::new(tokio::io::empty()),
        output: Box::new(server_output),
    });
    raw_server.shutdown_timings = fast_shutdown_timings();
    let server = Arc::new(raw_server);
    let gate = Arc::new(StopAdmissionTestGate::new());
    let _release = StopAdmissionReleaseOnDrop(gate.clone());
    let mut entered = gate.subscribe();
    *server.inner.stop_admission_test_gate.lock().unwrap() = Some(gate.clone());

    let first_stop = tokio::task::spawn_blocking({
        let inner = server.inner.clone();
        move || inner.stop()
    });
    while !*entered.borrow() {
        entered.changed().await.unwrap();
    }
    assert!(server.inner.stopped.load(Ordering::SeqCst));
    assert!(
        !server.inner.dispatches.admission_is_closed(),
        "test gate pauses before admission close"
    );

    let shutdown = tokio::spawn({
        let server = server.clone();
        async move { server.shutdown_and_wait().await }
    });
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert!(
        !shutdown.is_finished(),
        "a concurrent shutdown caller cannot pass the partial stop boundary"
    );

    gate.release();
    first_stop.await.expect("first stop owner joins");
    shutdown
        .await
        .expect("shutdown owner joins")
        .expect("empty endpoint shuts down cleanly");
    assert!(server.inner.dispatches.admission_is_closed());
    assert!(server.inner.readonly_tasks.admission_is_closed());
    assert!(server.inner.completion_tasks.admission_is_closed());
    assert!(server.inner.durable_lifecycle_tasks.admission_is_closed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canonical_full_access_keeps_an_existing_approval_bound_to_its_owner() {
    use klaus_agent_core::types::permission::{ApprovalPolicy, ApprovalsReviewer, SandboxMode};

    let agent = make_agent(
        ".",
        vec![],
        Some(serde_json::json!({
            "sandbox_mode":"read-only", "approval_policy":"on-request",
            "approvals_reviewer":"user", "prewarm":false,
            "autoMemory":false, "autoDream":false, "memoryRecall":false,
            "memory":{"enabled":false}
        })),
    );
    let (server_output, _client_input) = tokio::io::duplex(1 << 16);
    let server = Arc::new(ProtocolServer::new(ProtocolServerOptions {
        create_session: make_create_session(agent, None),
        lifecycle: None,
        plugin_admin: None,
        credential_store: None,
        input: Box::new(tokio::io::empty()),
        output: Box::new(server_output),
    }));
    let session = timeout(CROSS_TASK_LIVENESS_TIMEOUT, server.inner.ensure_session())
        .await
        .expect("session materializes")
        .expect("session initializes");
    let approval = tokio::spawn({
        let adapter = ApprovalAdapter {
            inner: server.inner.clone(),
        };
        async move {
            adapter
                .approve(
                    ApprovalRequest {
                        id: "explicit-rule-approval".into(),
                        kind: ApprovalKind::Tool,
                        tool_name: "mcp__example__read".into(),
                        input: RuntimeJson::Null,
                        message: "An explicit tool rule requires owner approval.".into(),
                        suggestions: None,
                    },
                    AbortSignal::new(),
                )
                .await
        }
    });
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        loop {
            if !server
                .inner
                .pending_prompts
                .lock()
                .unwrap()
                .approvals
                .is_empty()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("approval parks");

    server
        .inner
        .clone()
        .dispatch(Op {
            id: "canonical-policy-change".into(),
            op: OpKind::SetRuntimePolicy {
                permission_mode: None,
                pre_plan_permission_mode: None,
                approval_policy: Some(ApprovalPolicy::Never),
                sandbox_mode: Some(SandboxMode::DangerFullAccess),
                approvals_reviewer: Some(ApprovalsReviewer::User),
                sandbox_workspace_write: None,
                network_allowed: None,
            },
        })
        .await;
    let policy = session.runtime_policy_state();
    assert_eq!(policy.approval_policy, Some(ApprovalPolicy::Never));
    assert_eq!(policy.sandbox_mode, Some(SandboxMode::DangerFullAccess));
    assert!(
        server
            .inner
            .pending_prompts
            .lock()
            .unwrap()
            .approvals
            .contains_key(&RuntimeString::from("explicit-rule-approval")),
        "changing canonical policy must not approve an existing explicit request"
    );
    assert!(
        !approval.is_finished(),
        "the approval still belongs to its original owner"
    );

    server
        .inner
        .clone()
        .dispatch(Op {
            id: "owner-denies-explicit-request".into(),
            op: OpKind::Approve {
                request_id: "explicit-rule-approval".into(),
                response: ApprovalResponse::Deny {
                    message: Some("owner denied".into()),
                },
            },
        })
        .await;

    let response = timeout(CROSS_TASK_LIVENESS_TIMEOUT, approval)
        .await
        .expect("the owner response releases the approval")
        .expect("approval task joins");
    assert!(matches!(response, ApprovalResponse::Deny { message }
        if message == Some("owner denied".into())));
    assert!(server
        .inner
        .pending_prompts
        .lock()
        .unwrap()
        .approvals
        .is_empty());
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, server.shutdown_and_wait())
        .await
        .expect("server shutdown completes")
        .expect("server shuts down cleanly");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_before_approval_registration_rejects_late_insert_without_hanging() {
    let agent = make_agent(".", vec![], None);
    let (server_output, _client_input) = tokio::io::duplex(4096);
    let server = Arc::new(ProtocolServer::new(ProtocolServerOptions {
        create_session: make_create_session(agent, None),
        lifecycle: None,
        plugin_admin: None,
        credential_store: None,
        input: Box::new(tokio::io::empty()),
        output: Box::new(server_output),
    }));
    let gate = Arc::new(PendingPromptRegistrationTestGate::new());
    let mut entered = gate.subscribe();
    *server
        .inner
        .pending_prompt_registration_test_gate
        .lock()
        .unwrap() = Some(gate.clone());
    let approval = tokio::spawn({
        let adapter = ApprovalAdapter {
            inner: server.inner.clone(),
        };
        async move {
            adapter
                .approve(
                    ApprovalRequest {
                        id: "late-approval".into(),
                        kind: ApprovalKind::Tool,
                        tool_name: "tool".into(),
                        input: RuntimeJson::Null,
                        message: "approve?".into(),
                        suggestions: None,
                    },
                    AbortSignal::new(),
                )
                .await
        }
    });
    while !*entered.borrow() {
        entered.changed().await.unwrap();
    }

    server.stop();
    gate.release();
    let response = timeout(CROSS_TASK_LIVENESS_TIMEOUT, approval)
        .await
        .expect("late approval returns")
        .expect("approval task joins");
    assert!(matches!(response, ApprovalResponse::Deny { .. }));
    let pending = server.inner.pending_prompts.lock().unwrap();
    assert!(pending.closed);
    assert!(pending.approvals.is_empty());
    assert!(pending.elicitations.is_empty());
    assert!(pending.mcp_authorizations.is_empty());
    assert!(pending.dynamic_calls.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_before_elicitation_registration_rejects_late_insert_without_hanging() {
    let agent = make_agent(".", vec![], None);
    let (server_output, _client_input) = tokio::io::duplex(4096);
    let server = Arc::new(ProtocolServer::new(ProtocolServerOptions {
        create_session: make_create_session(agent, None),
        lifecycle: None,
        plugin_admin: None,
        credential_store: None,
        input: Box::new(tokio::io::empty()),
        output: Box::new(server_output),
    }));
    let gate = Arc::new(PendingPromptRegistrationTestGate::new());
    let mut entered = gate.subscribe();
    *server
        .inner
        .pending_prompt_registration_test_gate
        .lock()
        .unwrap() = Some(gate.clone());
    let elicitation = tokio::spawn({
        let adapter = ElicitAdapter {
            inner: server.inner.clone(),
        };
        async move {
            adapter
                .elicit(
                    ElicitRequest {
                        questions: None,
                        attachments_supported: None,
                        metadata: None,
                        prompt: "answer?".into(),
                        options: None,
                        multi_select: None,
                    },
                    AbortSignal::new(),
                )
                .await
        }
    });
    while !*entered.borrow() {
        entered.changed().await.unwrap();
    }

    server.stop();
    gate.release();
    let response = timeout(CROSS_TASK_LIVENESS_TIMEOUT, elicitation)
        .await
        .expect("late elicitation returns")
        .expect("elicitation task joins");
    assert!(matches!(response, ElicitResponse::Decline { .. }));
    let pending = server.inner.pending_prompts.lock().unwrap();
    assert!(pending.closed);
    assert!(pending.approvals.is_empty());
    assert!(pending.elicitations.is_empty());
    assert!(pending.mcp_authorizations.is_empty());
    assert!(pending.dynamic_calls.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_before_mcp_authorization_registration_rejects_late_insert_without_hanging() {
    let agent = make_agent(".", vec![], None);
    let (server_output, _client_input) = tokio::io::duplex(4096);
    let server = Arc::new(ProtocolServer::new(ProtocolServerOptions {
        create_session: make_create_session(agent, None),
        lifecycle: None,
        plugin_admin: None,
        credential_store: None,
        input: Box::new(tokio::io::empty()),
        output: Box::new(server_output),
    }));
    let gate = Arc::new(PendingPromptRegistrationTestGate::new());
    let mut entered = gate.subscribe();
    *server
        .inner
        .pending_prompt_registration_test_gate
        .lock()
        .unwrap() = Some(gate.clone());
    let authorization = tokio::spawn({
        let adapter = McpAuthorizeAdapter {
            inner: server.inner.clone(),
        };
        async move {
            adapter
                .authorize(
                    McpAuthorizeRequest {
                        server_name: "late".to_string(),
                        authorization_url: "https://auth.example/late".to_string(),
                        redirect_uri: "http://127.0.0.1/late".to_string(),
                    },
                    AbortSignal::new(),
                )
                .await
        }
    });
    while !*entered.borrow() {
        entered.changed().await.unwrap();
    }

    server.stop();
    gate.release();
    let response = timeout(CROSS_TASK_LIVENESS_TIMEOUT, authorization)
        .await
        .expect("late MCP authorization returns")
        .expect("authorization task joins");
    assert!(matches!(response, McpAuthorizeResponse::Decline { .. }));
    let pending = server.inner.pending_prompts.lock().unwrap();
    assert!(pending.closed);
    assert!(pending.mcp_authorizations.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_before_dynamic_call_registration_rejects_late_insert_without_hanging() {
    let agent = make_agent(".", vec![], None);
    let (server_output, _client_input) = tokio::io::duplex(4096);
    let server = Arc::new(ProtocolServer::new(ProtocolServerOptions {
        create_session: make_create_session(agent, None),
        lifecycle: None,
        plugin_admin: None,
        credential_store: None,
        input: Box::new(tokio::io::empty()),
        output: Box::new(server_output),
    }));
    let gate = Arc::new(PendingPromptRegistrationTestGate::new());
    let mut entered = gate.subscribe();
    *server
        .inner
        .pending_prompt_registration_test_gate
        .lock()
        .unwrap() = Some(gate.clone());
    let registration = tokio::spawn({
        let inner = server.inner.clone();
        async move {
            inner.pause_pending_prompt_registration_for_test().await;
            let (tx, rx) = oneshot::channel();
            match inner.register_pending_dynamic_call("late-dynamic".into(), tx) {
                Err(error) => Err(error),
                Ok(_token) => match rx.await {
                    Ok(Err(error)) => Err(error),
                    Ok(Ok(_)) => Ok(()),
                    Err(_) => Err("dynamic response channel closed".into()),
                },
            }
        }
    });
    while !*entered.borrow() {
        entered.changed().await.unwrap();
    }

    server.stop();
    gate.release();
    let error = timeout(CROSS_TASK_LIVENESS_TIMEOUT, registration)
        .await
        .expect("late dynamic registration returns")
        .expect("dynamic registration task joins")
        .expect_err("closed Host admission rejects the late call");
    assert!(error.contains("stopping"), "{error:?}");
    let pending = server.inner.pending_prompts.lock().unwrap();
    assert!(pending.closed);
    assert!(pending.dynamic_calls.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsolicited_writer_panic_is_retained_as_shutdown_failure() {
    let agent = make_agent(".", vec![], None);
    let ((server_r, _server_w), (_client_r, _client_w)) = duplex_pair();
    let raw_server = ProtocolServer::new(ProtocolServerOptions {
        create_session: make_create_session(agent, None),
        lifecycle: None,
        plugin_admin: None,
        credential_store: None,
        input: server_r,
        output: Box::new(PanicOnceWriter {
            bytes: Arc::new(Mutex::new(Vec::new())),
            panic_next_write: Arc::new(AtomicBool::new(true)),
        }),
    });
    let server = Arc::new(raw_server);
    let done = tokio::spawn(server.clone().start());
    assert!(server
        .inner
        .enqueue_queue_ordered_write(UnsolicitedWrite::Event(ProtocolEvent {
            submission_id: "panic-writer".into(),
            initialized: Some(crate::types::InitSessionResult { accepted: true }),
            ..Default::default()
        })));
    timeout(
        CROSS_TASK_LIVENESS_TIMEOUT,
        server.inner.wait_for_unsolicited_finish(),
    )
    .await
    .expect("panicking writer finishes after transaction Drop");

    let error = timeout(CROSS_TASK_LIVENESS_TIMEOUT, server.shutdown_and_wait())
        .await
        .expect("shutdown remains bounded")
        .expect_err("writer panic cannot report successful shutdown");
    assert!(
        error.contains("protocol unsolicited writer panicked"),
        "{error}"
    );
    assert!(
        !error.contains("timed out"),
        "panic is a typed transport failure, not a synthesized timeout: {error}"
    );
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, done)
        .await
        .expect("server start owner exits")
        .ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pre_set_publication_panic_registers_cleanup_before_parent_completion() {
    let agent = make_agent(".", vec![], None);
    let session = agent.start_session(StartSessionOptions {
        permission_mode: Some(ExternalPermissionMode::BypassPermissions),
        ..Default::default()
    });
    let ((server_r, server_w), (_client_r, _client_w)) = duplex_pair();
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        server_r,
        Box::new(RecordingWriter {
            inner: server_w,
            bytes,
        }),
    );
    let commit_gate = Arc::new(SessionPublicationCommitTestGate::new());
    let mut commit_entered = commit_gate.subscribe();
    *server
        .inner
        .session_publication_commit_test_gate
        .lock()
        .unwrap() = Some(commit_gate.clone());
    server
        .inner
        .session_publication_commit_panic_for_test
        .store(true, Ordering::SeqCst);
    let bind_result = server
        .inner
        .start_session_publication(SessionPublicationRequest::Bind {
            submission_id: "pre-set-panic".into(),
            thread_id: "persisted-thread".into(),
            new_thread_id: None,
            fork_cut: None,
            bind: bind_to_session(session.clone()),
        });
    while !*commit_entered.borrow() {
        commit_entered.changed().await.unwrap();
    }

    let snapshot_gate = Arc::new(SessionPublicationCallbackTestGate::new());
    let snapshot_holder = tokio::spawn({
        let session = session.clone();
        let snapshot_gate = snapshot_gate.clone();
        async move {
            session
                .with_agent_memory_snapshot_order(move || snapshot_gate.pause())
                .await;
        }
    });
    timeout(
        CROSS_TASK_LIVENESS_TIMEOUT,
        snapshot_gate.entered.notified(),
    )
    .await
    .expect("snapshot cleanup boundary is held");
    commit_gate.release();

    let bind_error = match bind_result.await.expect("publication waiter reports panic") {
        Err(error) => error,
        Ok(_) => panic!("pre-set panic cannot publish Session"),
    };
    assert!(
        bind_error.contains("BoundSession writer ended before its flush receipt"),
        "{bind_error}"
    );
    assert!(server.inner.session.get().is_none());
    assert!(server
        .inner
        .session_publication_inflight
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .is_none());
    assert!(
        server.inner.completion_tasks.has_active(),
        "cleanup child is active after the publication parent has ended"
    );

    snapshot_gate.release();
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, snapshot_holder)
        .await
        .expect("snapshot holder exits")
        .expect("snapshot holder joins");
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, session.shutdown_and_wait())
        .await
        .expect("retained Session cleanup completes")
        .expect("Session shutdown result");
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, done)
        .await
        .expect("server start owner exits")
        .ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bound_flush_is_the_publication_boundary_and_all_four_sinks_survive_commit() {
    let agent = make_agent(".", vec![], None);
    let session = agent.start_session(StartSessionOptions {
        permission_mode: Some(ExternalPermissionMode::BypassPermissions),
        ..Default::default()
    });
    let ((server_r, server_w), (_client_r, _client_w)) = duplex_pair();
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let writer: Box<dyn AsyncWrite + Unpin + Send> = Box::new(RecordingWriter {
        inner: server_w,
        bytes: bytes.clone(),
    });
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        server_r,
        writer,
    );
    let commit_gate = Arc::new(SessionPublicationCommitTestGate::new());
    let mut commit_entered = commit_gate.subscribe();
    *server
        .inner
        .session_publication_commit_test_gate
        .lock()
        .unwrap() = Some(commit_gate.clone());

    let bind_result = server
        .inner
        .start_session_publication(SessionPublicationRequest::Bind {
            submission_id: "bind-publication".into(),
            thread_id: "persisted-thread".into(),
            new_thread_id: None,
            fork_cut: None,
            bind: bind_to_session(session.clone()),
        });
    while !*commit_entered.borrow() {
        commit_entered.changed().await.unwrap();
    }
    assert!(
        server.inner.session.get().is_none(),
        "a flushed BoundSession is not published until the no-await commit"
    );

    let (memory_sink, snapshot_sink, queue_sink, task_sink) = {
        let sinks = server
            .inner
            .session_publication_sinks_for_test
            .lock()
            .unwrap();
        (
            sinks.memory.clone().expect("memory sink installed"),
            sinks.snapshot.clone().expect("snapshot sink installed"),
            sinks.queue.clone().expect("queue sink installed"),
            sinks.task.clone().expect("task sink installed"),
        )
    };
    memory_sink(publication_test_memory_event("pending-memory"));
    snapshot_sink(publication_test_snapshot_audit("pending-snapshot"));
    queue_sink(publication_test_queue_snapshot(1));
    task_sink(publication_test_task_change(1, 1));
    wait_for_recorded_event_count(&bytes, 1).await;
    assert_eq!(
        recorded_events(&bytes).len(),
        1,
        "initial sink events remain buffered behind BoundSession"
    );

    let ensure = tokio::spawn({
        let inner = server.inner.clone();
        async move {
            inner
                .ensure_session()
                .await
                .map_err(|error| error.to_string())
        }
    });
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert!(
        !ensure.is_finished(),
        "ensure cannot observe OnceCell before barrier release"
    );

    commit_gate.release();
    let bound = bind_result
        .await
        .expect("publication owner result")
        .expect("bind publication succeeds");
    let ensured = ensure
        .await
        .expect("ensure task joins")
        .expect("ensure succeeds");
    assert!(Arc::ptr_eq(&bound, &session));
    assert!(Arc::ptr_eq(&ensured, &session));
    assert!(Arc::ptr_eq(
        server.inner.session.get().expect("published Session"),
        &session
    ));

    // The committed closures are the strong Published-state owners. These
    // post-commit callbacks prove they remain admitted after the temporary
    // UnpublishedSessionGuard has been dropped.
    memory_sink(publication_test_memory_event("published-memory"));
    snapshot_sink(publication_test_snapshot_audit("published-snapshot"));
    queue_sink(publication_test_queue_snapshot(2));
    task_sink(publication_test_task_change(2, 2));
    wait_for_recorded_event_count(&bytes, 9).await;
    let events = recorded_events(&bytes);
    assert_eq!(events[0].submission_id, "bind-publication");
    assert_eq!(
        events[0].session.as_ref().map(|bound| &bound.thread_id),
        Some(&session.id)
    );
    assert!(events[1].memory_maintenance_event.is_some());
    assert!(events[2].agent_memory_snapshot_decision.is_some());
    assert_eq!(events[3].queue_snapshot.as_ref().unwrap().revision, 1);
    assert_eq!(events[4].task_list_change.as_ref().unwrap().revision, 1);
    assert!(events[5].memory_maintenance_event.is_some());
    assert!(events[6].agent_memory_snapshot_decision.is_some());
    assert_eq!(events[7].queue_snapshot.as_ref().unwrap().revision, 2);
    assert_eq!(events[8].task_list_change.as_ref().unwrap().revision, 2);

    server.stop();
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, done)
        .await
        .expect("protocol server stops")
        .ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closed_unsolicited_receiver_after_bound_flush_stops_published_endpoint() {
    let agent = make_agent(".", vec![], None);
    let session = agent.start_session(StartSessionOptions {
        permission_mode: Some(ExternalPermissionMode::BypassPermissions),
        ..Default::default()
    });
    let ((server_r, server_w), (_client_r, _client_w)) = duplex_pair();
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        server_r,
        Box::new(RecordingWriter {
            inner: server_w,
            bytes: bytes.clone(),
        }),
    );
    let commit_gate = Arc::new(SessionPublicationCommitTestGate::new());
    let mut commit_entered = commit_gate.subscribe();
    *server
        .inner
        .session_publication_commit_test_gate
        .lock()
        .unwrap() = Some(commit_gate.clone());
    let bind_result = server
        .inner
        .start_session_publication(SessionPublicationRequest::Bind {
            submission_id: "closed-publication-fifo".into(),
            thread_id: "persisted-thread".into(),
            new_thread_id: None,
            fork_cut: None,
            bind: bind_to_session(session.clone()),
        });
    while !*commit_entered.borrow() {
        commit_entered.changed().await.unwrap();
    }
    wait_for_recorded_event_count(&bytes, 1).await;
    let memory_sink = server
        .inner
        .session_publication_sinks_for_test
        .lock()
        .unwrap()
        .memory
        .clone()
        .expect("memory sink installed");
    memory_sink(publication_test_memory_event("closed-fifo-buffered"));
    assert_eq!(
        server
            .inner
            .queue_publication_barrier
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .buffered
            .len(),
        1,
        "the callback is retained behind the flushed BoundSession"
    );

    // The BoundSession writer already owns the old receiver and has fully
    // flushed its frame. Replace the publication sender with a receiver-
    // closed FIFO before commit to exercise the post-flush drain race.
    let (closed_sender, closed_receiver) = mpsc::unbounded_channel();
    drop(closed_receiver);
    let old_sender = server
        .inner
        .unsolicited_tx
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .replace(closed_sender);
    drop(old_sender);
    commit_gate.release();

    let bound = timeout(CROSS_TASK_LIVENESS_TIMEOUT, bind_result)
        .await
        .expect("publication result remains bounded")
        .expect("publication owner reports its result")
        .expect("a flushed BoundSession remains committed");
    assert!(Arc::ptr_eq(&bound, &session));
    assert!(Arc::ptr_eq(
        server
            .inner
            .session
            .get()
            .expect("Session remains published"),
        &session
    ));
    assert!(server.inner.stopped.load(Ordering::SeqCst));
    assert!(server.inner.session_closing.load(Ordering::SeqCst));
    assert_eq!(
        server
            .inner
            .output_write_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_deref(),
        Some("protocol unsolicited FIFO closed during session publication")
    );

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, done)
        .await
        .expect("receiver-close failure tears down the endpoint")
        .ok();
    assert!(session.is_disposed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preparation_waiter_cancellation_keeps_the_publication_owner() {
    let agent = make_agent(".", vec![], None);
    let session = agent.start_session(StartSessionOptions {
        permission_mode: Some(ExternalPermissionMode::BypassPermissions),
        ..Default::default()
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let create_session: CreateSession = {
        let session = session.clone();
        let calls = calls.clone();
        Arc::new(move |_args| {
            let session = session.clone();
            calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { Ok::<Arc<Session>, BoxError>(session) })
        })
    };
    let server = Arc::new(ProtocolServer::new(ProtocolServerOptions {
        create_session,
        lifecycle: None,
        plugin_admin: None,
        credential_store: None,
        input: Box::new(tokio::io::empty()),
        output: Box::new(tokio::io::sink()),
    }));
    let commit_gate = Arc::new(SessionPublicationCommitTestGate::new());
    let mut entered = commit_gate.subscribe();
    *server
        .inner
        .session_publication_ensure_commit_test_gate
        .lock()
        .unwrap() = Some(commit_gate.clone());
    let first = tokio::spawn({
        let server = server.clone();
        async move { server.prepare_session().await }
    });
    while !*entered.borrow() {
        timeout(CROSS_TASK_LIVENESS_TIMEOUT, entered.changed())
            .await
            .unwrap()
            .unwrap();
    }
    first.abort();
    match first.await {
        Err(error) => assert!(error.is_cancelled()),
        Ok(_) => panic!("the first preparation waiter must be cancelled"),
    }
    assert!(server.existing_session().is_none());
    assert!(!session.is_disposed());
    let second = tokio::spawn({
        let server = server.clone();
        async move { server.prepare_session().await }
    });
    commit_gate.release();
    let prepared = timeout(CROSS_TASK_LIVENESS_TIMEOUT, second)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(Arc::ptr_eq(&prepared, &session));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(Arc::ptr_eq(&server.existing_session().unwrap(), &session));
    server.shutdown_and_wait().await.unwrap();
    assert!(session.is_disposed());
    assert!(server.prepare_session().await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_wins_lazy_ensure_precommit_and_discards_unpublished_state() {
    let agent = make_agent(".", vec![], None);
    let session = agent.start_session(StartSessionOptions {
        permission_mode: Some(ExternalPermissionMode::BypassPermissions),
        ..Default::default()
    });
    let create_session: CreateSession = {
        let session = session.clone();
        Arc::new(move |_args| {
            let session = session.clone();
            Box::pin(async move { Ok::<Arc<Session>, BoxError>(session) })
        })
    };
    let server = Arc::new(ProtocolServer::new(ProtocolServerOptions {
        create_session,
        lifecycle: None,
        plugin_admin: None,
        credential_store: None,
        input: Box::new(tokio::io::empty()),
        output: Box::new(tokio::io::sink()),
    }));
    let commit_gate = Arc::new(SessionPublicationCommitTestGate::new());
    let mut commit_entered = commit_gate.subscribe();
    *server
        .inner
        .session_publication_ensure_commit_test_gate
        .lock()
        .unwrap() = Some(commit_gate.clone());

    let ensure_result = server
        .inner
        .start_session_publication(SessionPublicationRequest::Ensure);
    while !*commit_entered.borrow() {
        timeout(CROSS_TASK_LIVENESS_TIMEOUT, commit_entered.changed())
            .await
            .expect("lazy Ensure reaches its precommit boundary")
            .expect("Ensure commit gate remains observable");
    }
    let (memory_sink, snapshot_sink, queue_sink, task_sink) = {
        let sinks = server
            .inner
            .session_publication_sinks_for_test
            .lock()
            .unwrap();
        (
            sinks.memory.clone().expect("memory sink installed"),
            sinks.snapshot.clone().expect("snapshot sink installed"),
            sinks.queue.clone().expect("queue sink installed"),
            sinks.task.clone().expect("task sink installed"),
        )
    };
    memory_sink(publication_test_memory_event("ensure-stop-memory"));
    snapshot_sink(publication_test_snapshot_audit("ensure-stop-snapshot"));
    queue_sink(publication_test_queue_snapshot(11));
    task_sink(publication_test_task_change(11, 11));
    assert_eq!(
        server
            .inner
            .queue_publication_barrier
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .buffered
            .len(),
        4,
        "unpublished callbacks remain behind the lazy Ensure barrier"
    );
    assert_eq!(
        server
            .inner
            .session_publication_inflight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .map(|publication| publication.awaiting_bound_write),
        Some(false)
    );

    // Stop takes the same inflight mutex used by commit and closes lazy
    // publication after the outer precheck has already passed.
    server.stop();
    assert!(server.inner.session_closing.load(Ordering::SeqCst));
    commit_gate.release();

    let result = timeout(CROSS_TASK_LIVENESS_TIMEOUT, ensure_result)
        .await
        .expect("stopped lazy Ensure remains bounded")
        .expect("publication owner reports its result");
    match result {
        Err(error) => assert!(error.contains("protocol session has shut down"), "{error}"),
        Ok(_) => panic!("stop must win a lazy Ensure that has not published"),
    }
    assert!(server.inner.session.get().is_none());
    assert!(server
        .inner
        .session_publication_inflight
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .is_none());
    {
        let barrier = server
            .inner
            .queue_publication_barrier
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(!barrier.active);
        assert!(
            barrier.buffered.is_empty(),
            "rejected Ensure discards every unpublished callback"
        );
    }
    assert!(
        session.is_disposed(),
        "rejected Ensure transfers its Session to retained cleanup"
    );
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, server.shutdown_and_wait())
        .await
        .expect("never-started endpoint shutdown remains bounded")
        .expect("lazy Ensure retained cleanup succeeds");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acked_bind_commits_after_stop_and_shutdown_cleans_the_published_session() {
    let agent = make_agent(".", vec![], None);
    let session = agent.start_session(StartSessionOptions {
        permission_mode: Some(ExternalPermissionMode::BypassPermissions),
        ..Default::default()
    });
    let ((server_r, server_w), (_client_r, _client_w)) = duplex_pair();
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let writer: Box<dyn AsyncWrite + Unpin + Send> = Box::new(RecordingWriter {
        inner: server_w,
        bytes: bytes.clone(),
    });
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        server_r,
        writer,
    );
    let commit_gate = Arc::new(SessionPublicationCommitTestGate::new());
    let mut commit_entered = commit_gate.subscribe();
    *server
        .inner
        .session_publication_commit_test_gate
        .lock()
        .unwrap() = Some(commit_gate.clone());
    let bind_result = server
        .inner
        .start_session_publication(SessionPublicationRequest::Bind {
            submission_id: "acked-before-stop".into(),
            thread_id: "persisted-thread".into(),
            new_thread_id: None,
            fork_cut: None,
            bind: bind_to_session(session.clone()),
        });
    while !*commit_entered.borrow() {
        commit_entered.changed().await.unwrap();
    }
    let (memory_sink, snapshot_sink, queue_sink, task_sink) = {
        let sinks = server
            .inner
            .session_publication_sinks_for_test
            .lock()
            .unwrap();
        (
            sinks.memory.clone().unwrap(),
            sinks.snapshot.clone().unwrap(),
            sinks.queue.clone().unwrap(),
            sinks.task.clone().unwrap(),
        )
    };
    memory_sink(publication_test_memory_event("acked-memory"));
    snapshot_sink(publication_test_snapshot_audit("acked-snapshot"));
    queue_sink(publication_test_queue_snapshot(7));
    task_sink(publication_test_task_change(7, 7));
    assert_eq!(
        server
            .inner
            .session_publication_inflight
            .lock()
            .unwrap()
            .as_ref()
            .map(|publication| publication.awaiting_bound_write),
        Some(true)
    );

    server.stop();
    assert_eq!(
        server
            .inner
            .session_publication_inflight
            .lock()
            .unwrap()
            .as_ref()
            .map(|publication| publication.awaiting_bound_write),
        Some(true),
        "stop cannot roll back a flushed BoundSession"
    );
    commit_gate.release();
    let bound = bind_result
        .await
        .expect("publication owner result")
        .expect("Acked publication commits despite closing");
    assert!(Arc::ptr_eq(&bound, &session));
    assert!(Arc::ptr_eq(
        server.inner.session.get().expect("Acked Session published"),
        &session
    ));
    wait_for_recorded_event_count(&bytes, 5).await;
    let events = recorded_events(&bytes);
    assert!(events[0].session.is_some());
    assert!(events[1].memory_maintenance_event.is_some());
    assert!(events[2].agent_memory_snapshot_decision.is_some());
    assert_eq!(events[3].queue_snapshot.as_ref().unwrap().revision, 7);
    assert_eq!(events[4].task_list_change.as_ref().unwrap().revision, 7);

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, done)
        .await
        .expect("shutdown waits for Acked publication and exits")
        .ok();
    assert!(session.is_disposed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flushed_bind_commits_after_output_poison_and_is_then_torn_down() {
    let agent = make_agent(".", vec![], None);
    let session = agent.start_session(StartSessionOptions {
        permission_mode: Some(ExternalPermissionMode::BypassPermissions),
        ..Default::default()
    });
    let ((server_r, server_w), (_client_r, _client_w)) = duplex_pair();
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        server_r,
        Box::new(RecordingWriter {
            inner: server_w,
            bytes: bytes.clone(),
        }),
    );
    let commit_gate = Arc::new(SessionPublicationCommitTestGate::new());
    let mut commit_entered = commit_gate.subscribe();
    *server
        .inner
        .session_publication_commit_test_gate
        .lock()
        .unwrap() = Some(commit_gate.clone());
    let bind_result = server
        .inner
        .start_session_publication(SessionPublicationRequest::Bind {
            submission_id: "flushed-before-poison".into(),
            thread_id: "persisted-thread".into(),
            new_thread_id: None,
            fork_cut: None,
            bind: bind_to_session(session.clone()),
        });
    while !*commit_entered.borrow() {
        timeout(CROSS_TASK_LIVENESS_TIMEOUT, commit_entered.changed())
            .await
            .expect("BoundSession reaches its post-flush commit boundary")
            .expect("BoundSession commit gate remains observable");
    }
    wait_for_recorded_event_count(&bytes, 1).await;

    // The Host has already observed the BoundSession. Poison synchronously
    // stops the endpoint before commit, but may not roll that frame back.
    server
        .inner
        .record_output_write_error("injected output poison after BoundSession flush".to_string());
    assert!(server.inner.stopped.load(Ordering::SeqCst));
    assert!(server.inner.session_closing.load(Ordering::SeqCst));
    assert_eq!(
        server
            .inner
            .session_publication_inflight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .map(|publication| publication.awaiting_bound_write),
        Some(true)
    );
    commit_gate.release();

    let bound = timeout(CROSS_TASK_LIVENESS_TIMEOUT, bind_result)
        .await
        .expect("post-poison BoundSession publication remains bounded")
        .expect("publication owner reports its result")
        .expect("a flushed BoundSession remains committed after poison");
    assert!(Arc::ptr_eq(&bound, &session));
    assert!(Arc::ptr_eq(
        server
            .inner
            .session
            .get()
            .expect("Host-visible BoundSession is retained"),
        &session
    ));
    let events = recorded_events(&bytes);
    assert_eq!(events.len(), 1, "poison rejects all later output frames");
    assert_eq!(events[0].submission_id, "flushed-before-poison");
    assert!(events[0].session.is_some());

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, done)
        .await
        .expect("poisoned endpoint waits for the committed Session teardown")
        .ok();
    assert!(session.is_disposed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_bound_flush_closes_old_memory_callback_before_barrier_discard() {
    let agent = make_agent(".", vec![], None);
    let session = agent.start_session(StartSessionOptions {
        permission_mode: Some(ExternalPermissionMode::BypassPermissions),
        ..Default::default()
    });
    let ((server_r, _server_w), (_client_r, _client_w)) = duplex_pair();
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let flush = Arc::new(FlushGate::default());
    let writer: Box<dyn AsyncWrite + Unpin + Send> = Box::new(FullThenGatedFlushWriter {
        bytes: bytes.clone(),
        flush: flush.clone(),
    });
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        server_r,
        writer,
    );
    let callback_gate = Arc::new(SessionPublicationCallbackTestGate::new());
    *server
        .inner
        .session_publication_callback_test_gate
        .lock()
        .unwrap() = Some(callback_gate.clone());

    let bind_result = server
        .inner
        .start_session_publication(SessionPublicationRequest::Bind {
            submission_id: "bind-flush-failure".into(),
            thread_id: "persisted-thread".into(),
            new_thread_id: None,
            fork_cut: None,
            bind: bind_to_session(session),
        });
    flush.wait_until_entered().await;
    let memory_sink = server
        .inner
        .session_publication_sinks_for_test
        .lock()
        .unwrap()
        .memory
        .clone()
        .expect("memory sink installed before BoundSession flush");
    let callback = std::thread::spawn(move || {
        memory_sink(publication_test_memory_event("late-old-callback"));
    });
    timeout(
        CROSS_TASK_LIVENESS_TIMEOUT,
        callback_gate.entered.notified(),
    )
    .await
    .expect("old callback reached the pre-admission gate");

    flush.release(true);
    let error = match bind_result
        .await
        .expect("publication owner reports its failure")
    {
        Err(error) => error,
        Ok(_) => panic!("failed flush cannot publish Session"),
    };
    assert!(
        error.contains("forced BoundSession flush failure"),
        "{error}"
    );
    assert!(server.inner.session.get().is_none());
    let admission = server
        .inner
        .session_publication_admission_for_test
        .lock()
        .unwrap()
        .as_ref()
        .and_then(Weak::upgrade)
        .expect("publication admission remains owned by the cloned callback");
    assert_eq!(admission.state(), SessionPublicationAdmissionState::Closed);
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        loop {
            {
                let state = server.inner.queue_publication_barrier.lock().unwrap();
                if !state.active && state.buffered.is_empty() {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("failed publication barrier is discarded");

    let before_release = bytes.lock().unwrap().len();
    callback_gate.release();
    callback.join().expect("old callback returns");
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert_eq!(bytes.lock().unwrap().len(), before_release);
    let events = recorded_events(&bytes);
    assert_eq!(events.len(), 1, "no failed-publication event escapes");
    assert!(
        events[0].session.is_some(),
        "only the attempted BoundSession was written"
    );

    let publication_lease = timeout(
        CROSS_TASK_LIVENESS_TIMEOUT,
        server.inner.session_publication_gate.lock(),
    )
    .await
    .expect("failed publication releases its lease");
    drop(publication_lease);
    assert!(
        server
            .inner
            .session_publication_inflight
            .lock()
            .unwrap()
            .is_none(),
        "failed publication cleanup releases the inflight slot"
    );

    server.stop();
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, done)
        .await
        .expect("protocol server stops")
        .ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disposed_unpublished_session_cannot_be_rebound_with_same_arc() {
    let agent = make_agent(".", vec![], None);
    let session = agent.start_session(StartSessionOptions {
        permission_mode: Some(ExternalPermissionMode::BypassPermissions),
        ..Default::default()
    });
    let ((server_r, _server_w), (_client_r, _client_w)) = duplex_pair();
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let flush = Arc::new(FlushGate::default());
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        server_r,
        Box::new(FullThenGatedFlushWriter {
            bytes: bytes.clone(),
            flush: flush.clone(),
        }),
    );
    let first_bind = server
        .inner
        .start_session_publication(SessionPublicationRequest::Bind {
            submission_id: "first-failed-bind".into(),
            thread_id: "persisted-thread".into(),
            new_thread_id: None,
            fork_cut: None,
            bind: bind_to_session(session.clone()),
        });
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, flush.wait_until_entered())
        .await
        .expect("first BoundSession reaches flush");
    flush.release(true);
    let first_result = timeout(CROSS_TASK_LIVENESS_TIMEOUT, first_bind)
        .await
        .expect("failed publication remains bounded")
        .expect("first publication owner reports its result");
    match first_result {
        Err(error) => assert!(
            error.contains("forced BoundSession flush failure"),
            "{error}"
        ),
        Ok(_) => panic!("failed BoundSession flush cannot publish Session"),
    }
    assert!(
        session.is_disposed(),
        "unpublished rejection synchronously closes Session admission"
    );
    let first_bytes = bytes.lock().unwrap().len();
    let first_events = recorded_events(&bytes);
    assert_eq!(first_events.len(), 1);
    assert_eq!(first_events[0].submission_id, "first-failed-bind");

    // Make the writer healthy again. Without the disposed checks, this
    // immediate retry would write and publish a second BoundSession for the
    // exact same dying Arc.
    flush.release(false);
    let second_bind = server
        .inner
        .start_session_publication(SessionPublicationRequest::Bind {
            submission_id: "second-rejected-bind".into(),
            thread_id: "persisted-thread".into(),
            new_thread_id: None,
            fork_cut: None,
            bind: bind_to_session(session.clone()),
        });
    let second_result = timeout(CROSS_TASK_LIVENESS_TIMEOUT, second_bind)
        .await
        .expect("same-Arc retry remains bounded")
        .expect("second publication owner reports its result");
    match second_result {
        Err(error) => assert!(error.contains("protocol session has shut down"), "{error}"),
        Ok(_) => panic!("a disposed Session cannot be rebound"),
    }
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        bytes.lock().unwrap().len(),
        first_bytes,
        "the rejected retry cannot write another BoundSession"
    );
    assert!(server.inner.session.get().is_none());
    assert!(server
        .inner
        .session_publication_inflight
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .is_none());
    assert!(server.inner.queue_unsubscribe.lock().unwrap().is_none());
    assert!(server.inner.task_list_unsubscribe.lock().unwrap().is_none());

    server.stop();
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, done)
        .await
        .expect("protocol server stops")
        .ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disposed_session_between_bound_flush_and_commit_is_not_published() {
    let agent = make_agent(".", vec![], None);
    let session = agent.start_session(StartSessionOptions {
        permission_mode: Some(ExternalPermissionMode::BypassPermissions),
        ..Default::default()
    });
    let ((server_r, server_w), (_client_r, _client_w)) = duplex_pair();
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        server_r,
        Box::new(RecordingWriter {
            inner: server_w,
            bytes: bytes.clone(),
        }),
    );
    let commit_gate = Arc::new(SessionPublicationCommitTestGate::new());
    let mut commit_entered = commit_gate.subscribe();
    *server
        .inner
        .session_publication_commit_test_gate
        .lock()
        .unwrap() = Some(commit_gate.clone());
    let bind_result = server
        .inner
        .start_session_publication(SessionPublicationRequest::Bind {
            submission_id: "disposed-before-commit".into(),
            thread_id: "persisted-thread".into(),
            new_thread_id: None,
            fork_cut: None,
            bind: bind_to_session(session.clone()),
        });
    while !*commit_entered.borrow() {
        timeout(CROSS_TASK_LIVENESS_TIMEOUT, commit_entered.changed())
            .await
            .expect("BoundSession reaches its commit boundary")
            .expect("commit gate remains observable");
    }
    wait_for_recorded_event_count(&bytes, 1).await;
    session.initiate_shutdown();
    assert!(session.is_disposed());
    commit_gate.release();

    let result = timeout(CROSS_TASK_LIVENESS_TIMEOUT, bind_result)
        .await
        .expect("disposed pre-set publication remains bounded")
        .expect("publication owner reports its result");
    match result {
        Err(error) => assert!(error.contains("protocol session has shut down"), "{error}"),
        Ok(_) => panic!("a Session disposed before OnceCell::set cannot publish"),
    }
    assert!(server.inner.session.get().is_none());
    assert!(server
        .inner
        .session_publication_inflight
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .is_none());
    assert_eq!(recorded_events(&bytes).len(), 1);

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, done)
        .await
        .expect("post-flush publication failure stops the server")
        .ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn force_aborted_unpublished_adapter_cleanup_releases_protocol_owners_but_not_core_owner() {
    let agent = make_agent(".", vec![], None);
    let session = agent.start_session(StartSessionOptions {
        permission_mode: Some(ExternalPermissionMode::BypassPermissions),
        ..Default::default()
    });
    let ((server_r, _server_w), (_client_r, _client_w)) = duplex_pair();
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let flush = Arc::new(FlushGate::default());
    let mut raw_server = ProtocolServer::new(ProtocolServerOptions {
        create_session: make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        lifecycle: None,
        plugin_admin: None,
        credential_store: None,
        input: server_r,
        output: Box::new(FullThenGatedFlushWriter {
            bytes,
            flush: flush.clone(),
        }),
    });
    raw_server.shutdown_timings = fast_shutdown_timings();
    let server = Arc::new(raw_server);
    let done = tokio::spawn(server.clone().start());
    let bind_result = server
        .inner
        .start_session_publication(SessionPublicationRequest::Bind {
            submission_id: "blocked-adapter-cleanup".into(),
            thread_id: "persisted-thread".into(),
            new_thread_id: None,
            fork_cut: None,
            bind: bind_to_session(session.clone()),
        });
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, flush.wait_until_entered())
        .await
        .expect("BoundSession writer reaches its flush boundary");

    let snapshot_gate = Arc::new(SessionPublicationCallbackTestGate::new());
    let _snapshot_release = SessionPublicationCallbackReleaseOnDrop(snapshot_gate.clone());
    let snapshot_holder = tokio::spawn({
        let session = session.clone();
        let snapshot_gate = snapshot_gate.clone();
        async move {
            session
                .with_agent_memory_snapshot_order(move || snapshot_gate.pause())
                .await;
        }
    });
    timeout(
        CROSS_TASK_LIVENESS_TIMEOUT,
        snapshot_gate.entered.notified(),
    )
    .await
    .expect("snapshot event gate is held");

    flush.release(true);
    let publication_result = timeout(CROSS_TASK_LIVENESS_TIMEOUT, bind_result)
        .await
        .expect("failed BoundSession publication remains bounded")
        .expect("publication owner result");
    match publication_result {
        Err(_) => {}
        Ok(_) => panic!("flush failure cannot publish Session"),
    }
    assert!(
        server.inner.completion_tasks.has_active(),
        "retained cleanup child is registered before the publication parent ends"
    );
    let shutdown_error = timeout(CROSS_TASK_LIVENESS_TIMEOUT, server.shutdown_and_wait())
        .await
        .expect("protocol force-close remains bounded")
        .expect_err("blocked adapter cleanup is typed as shutdown failure");
    assert!(
        shutdown_error.contains("protocol completion work")
            || shutdown_error.contains("core session cleanup"),
        "{shutdown_error}"
    );
    assert_eq!(
        server
            .inner
            .session_publication_admission_for_test
            .lock()
            .unwrap()
            .as_ref()
            .and_then(Weak::upgrade)
            .map(|admission| admission.state()),
        Some(SessionPublicationAdmissionState::Closed),
        "force-abort leaves any core-held old closure permanently closed"
    );
    assert!(
        !server
            .inner
            .queue_publication_barrier
            .lock()
            .unwrap()
            .active
    );
    let publication_lease = timeout(
        CROSS_TASK_LIVENESS_TIMEOUT,
        server.inner.session_publication_gate.lock(),
    )
    .await
    .expect("force-abort releases the protocol publication lease");
    drop(publication_lease);
    assert!(server
        .inner
        .session_publication_inflight
        .lock()
        .unwrap()
        .is_none());

    // Protocol ownership is gone, but the Session-owned shutdown task that
    // was started synchronously in guard Drop is still alive. Releasing the
    // deliberately blocked core gate lets that same owner finish.
    snapshot_gate.release();
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, snapshot_holder)
        .await
        .expect("snapshot gate holder exits")
        .expect("snapshot holder task joins");
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, session.shutdown_and_wait())
        .await
        .expect("Session-owned shutdown completes after gate release")
        .expect("Session shutdown result");
    assert!(session.is_disposed());
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, done)
        .await
        .expect("server start owner exits")
        .ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_during_fully_written_pending_bound_flush_never_late_publishes() {
    let agent = make_agent(".", vec![], None);
    let session = agent.start_session(StartSessionOptions {
        permission_mode: Some(ExternalPermissionMode::BypassPermissions),
        ..Default::default()
    });
    let ((server_r, _server_w), (_client_r, _client_w)) = duplex_pair();
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let flush = Arc::new(FlushGate::default());
    let mut raw_server = ProtocolServer::new(ProtocolServerOptions {
        create_session: make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        lifecycle: None,
        plugin_admin: None,
        credential_store: None,
        input: server_r,
        output: Box::new(FullThenGatedFlushWriter {
            bytes: bytes.clone(),
            flush: flush.clone(),
        }),
    });
    raw_server.shutdown_timings = fast_shutdown_timings();
    let server = Arc::new(raw_server);
    let done = tokio::spawn(server.clone().start());
    let bind_result = server
        .inner
        .start_session_publication(SessionPublicationRequest::Bind {
            submission_id: "bind-pending-flush".into(),
            thread_id: "persisted-thread".into(),
            new_thread_id: None,
            fork_cut: None,
            bind: bind_to_session(session.clone()),
        });
    flush.wait_until_entered().await;
    assert_eq!(recorded_events(&bytes).len(), 1);
    assert!(server.inner.session.get().is_none());
    assert!(server
        .inner
        .session_publication_inflight
        .lock()
        .unwrap()
        .as_ref()
        .is_some_and(|pending| Arc::ptr_eq(&pending.session, &session)));

    let shutdown = timeout(CROSS_TASK_LIVENESS_TIMEOUT, server.shutdown_and_wait())
        .await
        .expect("shutdown remains bounded");
    let shutdown_error = shutdown.expect_err("interrupted flush is reported");
    assert!(
        shutdown_error.contains("BoundSession write was interrupted before flush"),
        "{shutdown_error}"
    );
    let bind_error = match bind_result
        .await
        .expect("detached publication waiter reports writer cancellation")
    {
        Err(error) => error,
        Ok(_) => panic!("force-aborted writer cannot publish Session"),
    };
    assert!(
        bind_error.contains("BoundSession writer ended before its flush receipt"),
        "{bind_error}"
    );
    assert!(server.inner.session.get().is_none());
    assert!(server
        .inner
        .session_publication_inflight
        .lock()
        .unwrap()
        .is_none());
    assert!(
        session.is_disposed(),
        "the single retained Session is cleaned"
    );
    let admission = server
        .inner
        .session_publication_admission_for_test
        .lock()
        .unwrap()
        .as_ref()
        .and_then(Weak::upgrade);
    assert!(
        admission
            .as_ref()
            .is_none_or(|admission| admission.state() == SessionPublicationAdmissionState::Closed),
        "shutdown closes callback admission"
    );
    flush.release(false);
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert!(
        server.inner.session.get().is_none(),
        "no late Session publication"
    );
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, done)
        .await
        .expect("server start owner exits")
        .ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publication_wins_init_gate_and_late_init_cannot_overwrite_state() {
    let agent = make_agent(".", vec![], None);
    let session = agent.start_session(StartSessionOptions {
        permission_mode: Some(ExternalPermissionMode::BypassPermissions),
        ..Default::default()
    });
    let factory_entered = Arc::new(Notify::new());
    let factory_release = Arc::new(Notify::new());
    let create_session: CreateSession = {
        let factory_entered = factory_entered.clone();
        let factory_release = factory_release.clone();
        let session = session.clone();
        Arc::new(move |_args| {
            let factory_entered = factory_entered.clone();
            let factory_release = factory_release.clone();
            let session = session.clone();
            Box::pin(async move {
                factory_entered.notify_one();
                factory_release.notified().await;
                Ok::<Arc<Session>, BoxError>(session)
            })
        })
    };
    let ((server_r, server_w), (_client_r, _client_w)) = duplex_pair();
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let (server, done) = start_server(
        create_session,
        server_r,
        Box::new(RecordingWriter {
            inner: server_w,
            bytes: bytes.clone(),
        }),
    );
    let publication = server
        .inner
        .start_session_publication(SessionPublicationRequest::Ensure);
    factory_entered.notified().await;
    let late_init = tokio::spawn({
        let inner = server.inner.clone();
        async move {
            inner
                .dispatch_init_session(
                    "late-init".into(),
                    ProtocolSessionInit {
                        cwd: Some("/must-not-win".into()),
                        ..Default::default()
                    },
                )
                .await;
        }
    });
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert!(
        !late_init.is_finished(),
        "late init waits for publication gate"
    );
    assert!(server.inner.session_init.lock().unwrap().raw.cwd.is_none());

    factory_release.notify_one();
    publication
        .await
        .expect("publication owner result")
        .expect("Session publishes");
    late_init.await.expect("late init task joins");
    assert!(server.inner.session_init.lock().unwrap().raw.cwd.is_none());
    wait_for_recorded_event_count(&bytes, 1).await;
    let events = recorded_events(&bytes);
    assert!(events.iter().any(|event| {
        event.submission_id == "late-init"
            && event
                .error
                .as_ref()
                .is_some_and(|error| error.contains("before the protocol session is bound"))
    }));

    server.stop();
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, done)
        .await
        .expect("protocol server stops")
        .ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn init_ack_wins_publication_gate_and_bind_uses_committed_state() {
    let agent = make_agent(".", vec![], None);
    let session = agent.start_session(StartSessionOptions {
        permission_mode: Some(ExternalPermissionMode::BypassPermissions),
        ..Default::default()
    });
    let observed_cwd = Arc::new(Mutex::new(None::<RuntimeString>));
    let bind: BindSession = {
        let observed_cwd = observed_cwd.clone();
        let session = session.clone();
        Arc::new(
            move |args, _thread_id, _new_thread_id, _before_message_uuid| {
                *observed_cwd.lock().unwrap() = args.init.cwd.clone();
                let session = session.clone();
                Box::pin(async move { Ok::<Arc<Session>, BoxError>(session) })
            },
        )
    };
    let ((server_r, _server_w), (_client_r, _client_w)) = duplex_pair();
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let flush = Arc::new(FlushGate::default());
    let server = Arc::new(ProtocolServer::new(ProtocolServerOptions {
        create_session: make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        lifecycle: None,
        plugin_admin: None,
        credential_store: None,
        input: server_r,
        output: Box::new(FullThenGatedFlushWriter {
            bytes: bytes.clone(),
            flush: flush.clone(),
        }),
    }));
    let done = tokio::spawn(server.clone().start());
    let init = tokio::spawn({
        let inner = server.inner.clone();
        async move {
            inner
                .dispatch_init_session(
                    "init-first".into(),
                    ProtocolSessionInit {
                        cwd: Some("/init-winner".into()),
                        ..Default::default()
                    },
                )
                .await;
        }
    });
    flush.wait_until_entered().await;
    let bind_result = server
        .inner
        .start_session_publication(SessionPublicationRequest::Bind {
            submission_id: "bind-after-init".into(),
            thread_id: "persisted-thread".into(),
            new_thread_id: None,
            fork_cut: None,
            bind,
        });
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert!(
        observed_cwd.lock().unwrap().is_none(),
        "bind factory waits for initialized ACK flush"
    );

    flush.release(false);
    init.await.expect("init task joins");
    bind_result
        .await
        .expect("publication owner result")
        .expect("bind publishes");
    assert_eq!(
        observed_cwd.lock().unwrap().as_ref(),
        Some(&RuntimeString::from("/init-winner"))
    );
    wait_for_recorded_event_count(&bytes, 2).await;
    let events = recorded_events(&bytes);
    assert!(events[0].initialized.is_some());
    assert_eq!(events[0].submission_id, "init-first");
    assert!(events[1].session.is_some());
    assert_eq!(events[1].submission_id, "bind-after-init");

    server.stop();
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, done)
        .await
        .expect("protocol server stops")
        .ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_init_ack_preserves_prior_state_for_later_bind() {
    let agent = make_agent(".", vec![], None);
    let session = agent.start_session(StartSessionOptions {
        permission_mode: Some(ExternalPermissionMode::BypassPermissions),
        ..Default::default()
    });
    let observed_cwd = Arc::new(Mutex::new(None::<RuntimeString>));
    let bind: BindSession = {
        let observed_cwd = observed_cwd.clone();
        let session = session.clone();
        Arc::new(
            move |args, _thread_id, _new_thread_id, _before_message_uuid| {
                *observed_cwd.lock().unwrap() = args.init.cwd.clone();
                let session = session.clone();
                Box::pin(async move { Ok::<Arc<Session>, BoxError>(session) })
            },
        )
    };
    let ((server_r, _server_w), (_client_r, _client_w)) = duplex_pair();
    let failed_bytes = Arc::new(Mutex::new(Vec::new()));
    let flush = Arc::new(FlushGate::default());
    let server = Arc::new(ProtocolServer::new(ProtocolServerOptions {
        create_session: make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        lifecycle: None,
        plugin_admin: None,
        credential_store: None,
        input: server_r,
        output: Box::new(FullThenGatedFlushWriter {
            bytes: failed_bytes.clone(),
            flush: flush.clone(),
        }),
    }));
    server.inner.session_init.lock().unwrap().raw.cwd = Some("/prior-init".into());
    let done = tokio::spawn(server.clone().start());
    let init = tokio::spawn({
        let inner = server.inner.clone();
        async move {
            inner
                .dispatch_init_session(
                    "failed-init".into(),
                    ProtocolSessionInit {
                        cwd: Some("/unacknowledged-init".into()),
                        ..Default::default()
                    },
                )
                .await;
        }
    });
    flush.wait_until_entered().await;
    flush.release(true);
    init.await.expect("failed init task joins");
    assert_eq!(
        server.inner.session_init.lock().unwrap().raw.cwd.as_ref(),
        Some(&RuntimeString::from("/prior-init"))
    );
    assert!(server.inner.stopped.load(Ordering::SeqCst));
    let bytes_at_poison = failed_bytes.lock().unwrap().len();
    let bind_result = server
        .inner
        .start_session_publication(SessionPublicationRequest::Bind {
            submission_id: "bind-after-failed-init".into(),
            thread_id: "persisted-thread".into(),
            new_thread_id: None,
            fork_cut: None,
            bind,
        })
        .await;
    let bind_error = match bind_result {
        Ok(Err(error)) => error,
        Err(_) => "session publication owner ended after stop".to_string(),
        Ok(Ok(_)) => panic!("poisoned Init ACK cannot be followed by BoundSession"),
    };
    assert!(
        bind_error.contains("shut down") || bind_error.contains("after stop"),
        "{bind_error}"
    );
    assert!(observed_cwd.lock().unwrap().is_none());
    assert!(server.inner.session.get().is_none());
    assert_eq!(
        failed_bytes.lock().unwrap().len(),
        bytes_at_poison,
        "no bytes may be appended after the failed Init ACK flush"
    );

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, done)
        .await
        .expect("protocol server stops")
        .ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_init_write_preserves_prior_state_for_later_bind() {
    let agent = make_agent(".", vec![], None);
    let session = agent.start_session(StartSessionOptions {
        permission_mode: Some(ExternalPermissionMode::BypassPermissions),
        ..Default::default()
    });
    let observed_cwd = Arc::new(Mutex::new(None::<RuntimeString>));
    let bind: BindSession = {
        let observed_cwd = observed_cwd.clone();
        let session = session.clone();
        Arc::new(
            move |args, _thread_id, _new_thread_id, _before_message_uuid| {
                *observed_cwd.lock().unwrap() = args.init.cwd.clone();
                let session = session.clone();
                Box::pin(async move { Ok::<Arc<Session>, BoxError>(session) })
            },
        )
    };
    let ((server_r, _server_w), (_client_r, _client_w)) = duplex_pair();
    let flush = Arc::new(FlushGate::default());
    let cancelled_bytes = Arc::new(Mutex::new(Vec::new()));
    let server = Arc::new(ProtocolServer::new(ProtocolServerOptions {
        create_session: make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        lifecycle: None,
        plugin_admin: None,
        credential_store: None,
        input: server_r,
        output: Box::new(FullThenGatedFlushWriter {
            bytes: cancelled_bytes.clone(),
            flush: flush.clone(),
        }),
    }));
    server.inner.session_init.lock().unwrap().raw.cwd = Some("/prior-init".into());
    let done = tokio::spawn(server.clone().start());
    let init = tokio::spawn({
        let inner = server.inner.clone();
        async move {
            inner
                .dispatch_init_session(
                    "cancelled-init".into(),
                    ProtocolSessionInit {
                        cwd: Some("/cancelled-init".into()),
                        ..Default::default()
                    },
                )
                .await;
        }
    });
    flush.wait_until_entered().await;
    init.abort();
    assert!(init.await.unwrap_err().is_cancelled());
    assert_eq!(
        server.inner.session_init.lock().unwrap().raw.cwd.as_ref(),
        Some(&RuntimeString::from("/prior-init"))
    );
    assert_eq!(
        server.inner.output_write_error.lock().unwrap().as_deref(),
        Some("protocol InitSession ACK write was interrupted before flush")
    );
    assert!(server.inner.stopped.load(Ordering::SeqCst));

    let bytes_at_poison = cancelled_bytes.lock().unwrap().len();
    let bind_result = server
        .inner
        .start_session_publication(SessionPublicationRequest::Bind {
            submission_id: "bind-after-cancelled-init".into(),
            thread_id: "persisted-thread".into(),
            new_thread_id: None,
            fork_cut: None,
            bind,
        })
        .await;
    let bind_error = match bind_result {
        Ok(Err(error)) => error,
        Err(_) => "session publication owner ended after stop".to_string(),
        Ok(Ok(_)) => panic!("interrupted Init ACK cannot be followed by BoundSession"),
    };
    assert!(
        bind_error.contains("shut down") || bind_error.contains("after stop"),
        "{bind_error}"
    );
    assert!(observed_cwd.lock().unwrap().is_none());
    assert!(server.inner.session.get().is_none());
    assert_eq!(
        cancelled_bytes.lock().unwrap().len(),
        bytes_at_poison,
        "no bytes may be appended after the interrupted Init ACK"
    );

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, done)
        .await
        .expect("protocol server stops")
        .ok();
}

#[test]
fn shutdown_timeout_typing_never_depends_on_human_error_text() {
    let mut failures = ProtocolShutdownFailures::default();
    failures.push_error("provider diagnostic contains timeout but is not a deadline");
    failures.extend_core(SessionShutdownOutcome {
        errors: vec!["core adapter also mentioned timed out in prose".to_string()],
        timed_out: false,
    });
    assert!(
        !failures.timed_out,
        "ordinary error text must never synthesize timedOut=true"
    );

    failures.push_timeout("a real protocol deadline elapsed");
    assert!(failures.timed_out);

    let mut core_timeout = ProtocolShutdownFailures::default();
    core_timeout.extend_core(SessionShutdownOutcome {
        errors: vec!["opaque structured core failure".to_string()],
        timed_out: true,
    });
    assert!(
        core_timeout.timed_out,
        "the typed core outcome must propagate without parsing its message"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_force_abort_does_not_cancel_owned_unarchive_transaction() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let completed = Arc::new(AtomicBool::new(false));
    let lifecycle = ProtocolLifecycle {
        unarchive_session: Some(Arc::new({
            let entered = entered.clone();
            let release = release.clone();
            let completed = completed.clone();
            move |thread_id: String| {
                let entered = entered.clone();
                let release = release.clone();
                let completed = completed.clone();
                Box::pin(async move {
                    entered.notify_one();
                    release.notified().await;
                    completed.store(true, Ordering::SeqCst);
                    UnarchiveSessionResult {
                        thread_id: thread_id.into(),
                        status: UnarchiveSessionStatus::Restored,
                        thread: None,
                        error: None,
                    }
                }) as UnarchiveSessionFuture
            }
        })),
        ..Default::default()
    };
    let create_session: CreateSession = Arc::new(|_args| {
        Box::pin(async {
            Err::<Arc<Session>, BoxError>("session creation is unused in this test".into())
        })
    });
    let ((server_r, server_w), (client_r, client_w)) = duplex_pair();
    let mut raw_server = ProtocolServer::new(ProtocolServerOptions {
        create_session,
        lifecycle: Some(lifecycle),
        plugin_admin: None,
        credential_store: None,
        input: server_r,
        output: server_w,
    });
    raw_server.shutdown_timings = ProtocolShutdownTimings {
        grace: Duration::from_millis(30),
        force: Duration::from_millis(500),
        transport: Duration::from_millis(700),
        close: Duration::from_millis(800),
    };
    let server = Arc::new(raw_server);
    let done = tokio::spawn(server.clone().start());
    let client = Arc::new(ProtocolClient::new(ProtocolClientOptions {
        input: client_r,
        output: client_w,
    }));

    let request = tokio::spawn({
        let client = client.clone();
        async move { client.unarchive_session("thread-1".into()).await }
    });
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, entered.notified())
        .await
        .expect("owned unarchive task started");

    let mut shutdown = tokio::spawn({
        let server = server.clone();
        async move { server.shutdown_and_wait().await }
    });
    timeout(
        Duration::from_millis(250),
        server.inner.wait_for_chain_finish(),
    )
    .await
    .expect("shutdown force-aborted only the chain waiter");
    assert!(!completed.load(Ordering::SeqCst));
    assert!(
        timeout(Duration::from_millis(40), &mut shutdown)
            .await
            .is_err(),
        "shutdown must wait for retained unarchive ownership"
    );

    release.notify_one();
    let error = timeout(CROSS_TASK_LIVENESS_TIMEOUT, shutdown)
        .await
        .expect("shutdown remained bounded")
        .expect("shutdown owner joined")
        .expect_err("the force-aborted request chain remains reported");
    assert!(
        error.contains("protocol operation chain timed out"),
        "{error}"
    );
    assert!(completed.load(Ordering::SeqCst));
    timeout(
        CROSS_TASK_LIVENESS_TIMEOUT,
        server.inner.durable_lifecycle_tasks.wait_for_idle(),
    )
    .await
    .expect("retained unarchive tracker settled");

    let _ = request.await;
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, done)
        .await
        .expect("server start settled")
        .expect("server task joined");
}

#[tokio::test]
async fn non_io_dispatch_timeout_returns_correlated_shutdown_completion() {
    let agent = make_agent(".", vec![], None);
    let (server_output, mut client_input) = tokio::io::duplex(4096);
    let mut raw_server = ProtocolServer::new(ProtocolServerOptions {
        create_session: make_create_session(agent, None),
        lifecycle: None,
        plugin_admin: None,
        credential_store: None,
        input: Box::new(tokio::io::empty()),
        output: Box::new(server_output),
    });
    raw_server.shutdown_timings = fast_shutdown_timings();
    let server = Arc::new(raw_server);
    *server.inner.shutdown_submission_id.lock().unwrap() =
        Some(RuntimeString::from("shutdown-timeout"));

    let guard = server
        .inner
        .dispatches
        .begin()
        .expect("dispatch admitted before shutdown");
    let id = guard.id();
    let task = tokio::spawn(async move {
        let _guard = guard;
        futures::future::pending::<()>().await;
    });
    server.inner.dispatches.attach(id, task.abort_handle());

    let closing = tokio::spawn({
        let server = server.clone();
        async move { server.shutdown_and_wait().await }
    });
    let error = timeout(CROSS_TASK_LIVENESS_TIMEOUT, closing)
        .await
        .expect("shutdown remains bounded")
        .expect("shutdown owner joins")
        .expect_err("timed-out dispatch is reported");
    assert!(error.contains("protocol dispatch timed out"), "{error}");
    assert!(
        server.inner.output.lock().await.is_none(),
        "writer was dropped"
    );

    let mut output = String::new();
    tokio::time::timeout(
        CROSS_TASK_LIVENESS_TIMEOUT,
        client_input.read_to_string(&mut output),
    )
    .await
    .expect("hard output close produces EOF")
    .unwrap();
    let events = output
        .lines()
        .map(|line| {
            runtime_codec::decode_text::<ProtocolEvent>(&RuntimeString::from_utf8(line))
                .expect("complete protocol frame")
        })
        .collect::<Vec<_>>();
    let completion = events
        .iter()
        .find_map(|event| event.shutdown_completed.as_deref())
        .expect("correlated shutdown completion remains parseable");
    assert!(!completion.ok);
    assert!(completion.flush_completed);
    assert!(!completion.cleanup_completed);
    assert!(completion.timed_out);
    assert!(completion
        .error
        .as_ref()
        .is_some_and(|error| error.contains("protocol dispatch timed out")));
    assert_eq!(
        events
            .iter()
            .find(|event| event.shutdown_completed.is_some())
            .unwrap()
            .submission_id,
        RuntimeString::from("shutdown-timeout")
    );
}

#[tokio::test]
async fn partial_frame_poison_stops_writes_and_rejects_late_bind_publication() {
    let agent = make_agent(".", vec![], None);
    let session = agent.start_session(StartSessionOptions {
        permission_mode: Some(ExternalPermissionMode::BypassPermissions),
        ..Default::default()
    });
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let entered = Arc::new(Notify::new());
    let mut raw_server = ProtocolServer::new(ProtocolServerOptions {
        create_session: make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        lifecycle: None,
        plugin_admin: None,
        credential_store: None,
        input: Box::new(tokio::io::empty()),
        output: Box::new(PartialThenPendingWriter {
            bytes: bytes.clone(),
            wrote_first_byte: false,
            entered: entered.clone(),
        }),
    });
    raw_server.shutdown_timings = fast_shutdown_timings();
    let server = Arc::new(raw_server);
    let writer = tokio::spawn({
        let inner = server.inner.clone();
        async move {
            inner
                .write_during_shutdown(ProtocolEvent {
                    submission_id: "partial".into(),
                    initialized: Some(crate::types::InitSessionResult { accepted: true }),
                    ..Default::default()
                })
                .await
        }
    });
    loop {
        let notified = entered.notified();
        if !bytes.lock().unwrap().is_empty() {
            break;
        }
        notified.await;
    }
    writer.abort();
    assert!(writer.await.unwrap_err().is_cancelled());
    assert!(server.inner.stopped.load(Ordering::SeqCst));
    assert!(server.inner.output_write_error.lock().unwrap().is_some());
    assert_eq!(bytes.lock().unwrap().len(), 1);

    let later_write = server
        .inner
        .write_during_shutdown(ProtocolEvent {
            submission_id: "must-not-write".into(),
            initialized: Some(crate::types::InitSessionResult { accepted: true }),
            ..Default::default()
        })
        .await
        .expect_err("poisoned output rejects every later frame");
    assert!(later_write.contains("poisoned"), "{later_write}");
    assert_eq!(bytes.lock().unwrap().len(), 1, "no later bytes appended");

    let bind_result = server
        .inner
        .start_session_publication(SessionPublicationRequest::Bind {
            submission_id: "late-bind".into(),
            thread_id: "persisted-thread".into(),
            new_thread_id: None,
            fork_cut: None,
            bind: bind_to_session(session),
        })
        .await;
    let bind_error = match bind_result {
        Ok(Err(error)) => error,
        Err(_) => "session publication owner ended after stop".to_string(),
        Ok(Ok(_)) => panic!("poisoned output cannot publish a Session"),
    };
    assert!(
        bind_error.contains("shut down") || bind_error.contains("after stop"),
        "{bind_error}"
    );
    assert!(server.inner.session.get().is_none());
    assert_eq!(bytes.lock().unwrap().len(), 1, "Bind writes no bytes");

    let shutdown_error = server
        .shutdown_and_wait()
        .await
        .expect_err("poison remains visible to shutdown");
    assert!(shutdown_error.contains("interrupted"), "{shutdown_error}");
}

#[tokio::test]
async fn interrupted_partial_frame_skips_shutdown_ack_and_hard_closes_output() {
    let agent = make_agent(".", vec![], None);
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let entered = Arc::new(Notify::new());
    let mut raw_server = ProtocolServer::new(ProtocolServerOptions {
        create_session: make_create_session(agent, None),
        lifecycle: None,
        plugin_admin: None,
        credential_store: None,
        input: Box::new(tokio::io::empty()),
        output: Box::new(PartialThenPendingWriter {
            bytes: bytes.clone(),
            wrote_first_byte: false,
            entered: entered.clone(),
        }),
    });
    raw_server.shutdown_timings = fast_shutdown_timings();
    let server = Arc::new(raw_server);
    *server.inner.shutdown_submission_id.lock().unwrap() =
        Some(RuntimeString::from("shutdown-partial"));

    let guard = server
        .inner
        .dispatches
        .begin()
        .expect("dispatch admitted before shutdown");
    let id = guard.id();
    let writer_inner = server.inner.clone();
    let write_task = tokio::spawn(async move {
        let _guard = guard;
        let _ = writer_inner
            .write_during_shutdown(ProtocolEvent {
                submission_id: RuntimeString::from("ordinary"),
                event: Some(EngineEvent::Notification {
                    level: NotificationLevel::Info,
                    message: RuntimeString::from("ordinary frame"),
                }),
                ..Default::default()
            })
            .await;
    });
    server
        .inner
        .dispatches
        .attach(id, write_task.abort_handle());
    entered.notified().await;

    let closing = tokio::spawn({
        let server = server.clone();
        async move { server.shutdown_and_wait().await }
    });
    let error = timeout(CROSS_TASK_LIVENESS_TIMEOUT, closing)
        .await
        .expect("shutdown remains bounded")
        .expect("shutdown owner joins")
        .expect_err("partial frame makes shutdown fail");
    assert!(
        error.contains("shutdown ACK skipped")
            && error.contains("output frame write was interrupted"),
        "{error}"
    );
    assert_eq!(bytes.lock().unwrap().len(), 1, "no ACK bytes appended");
    assert!(
        server.inner.output.lock().await.is_none(),
        "writer was dropped"
    );
}

#[tokio::test]
async fn interrupted_zero_byte_write_preserves_correlated_shutdown_ack() {
    let agent = make_agent(".", vec![], None);
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let entered = Arc::new(Notify::new());
    let mut raw_server = ProtocolServer::new(ProtocolServerOptions {
        create_session: make_create_session(agent, None),
        lifecycle: None,
        plugin_admin: None,
        credential_store: None,
        input: Box::new(tokio::io::empty()),
        output: Box::new(ZeroThenReadyWriter {
            bytes: bytes.clone(),
            first_poll_pending: true,
            entered: entered.clone(),
        }),
    });
    raw_server.shutdown_timings = fast_shutdown_timings();
    let server = Arc::new(raw_server);
    *server.inner.shutdown_submission_id.lock().unwrap() =
        Some(RuntimeString::from("shutdown-zero-byte"));

    let guard = server
        .inner
        .dispatches
        .begin()
        .expect("dispatch admitted before shutdown");
    let id = guard.id();
    let writer_inner = server.inner.clone();
    let write_task = tokio::spawn(async move {
        let _guard = guard;
        let _ = writer_inner
            .write_during_shutdown(ProtocolEvent {
                submission_id: RuntimeString::from("ordinary"),
                event: Some(EngineEvent::Notification {
                    level: NotificationLevel::Info,
                    message: RuntimeString::from("ordinary frame"),
                }),
                ..Default::default()
            })
            .await;
    });
    server
        .inner
        .dispatches
        .attach(id, write_task.abort_handle());
    entered.notified().await;

    let error = timeout(CROSS_TASK_LIVENESS_TIMEOUT, server.shutdown_and_wait())
        .await
        .expect("shutdown remains bounded")
        .expect_err("timed-out dispatch remains reported");
    assert!(error.contains("protocol dispatch timed out"), "{error}");

    let output = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
    let events = output
        .lines()
        .map(|line| {
            runtime_codec::decode_text::<ProtocolEvent>(&RuntimeString::from_utf8(line))
                .expect("complete shutdown frame")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        events.len(),
        1,
        "the zero-byte ordinary frame emits nothing"
    );
    let event = &events[0];
    assert_eq!(event.submission_id, "shutdown-zero-byte");
    let completion = event
        .shutdown_completed
        .as_deref()
        .expect("correlated shutdown completion");
    assert!(!completion.ok);
    assert!(completion.flush_completed);
    assert!(!completion.cleanup_completed);
    assert!(completion.timed_out);
}

#[tokio::test]
async fn readonly_side_question_larger_envelope_does_not_raise_ordinary_op_limit() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let agent = make_recording_agent("/tmp/klaus-proto", requests.clone());
    let ((sr, sw), (cr, mut cw)) = duplex_pair();
    let (server, done) = start_server(make_create_session(agent, None), sr, sw);
    let value = json!({"id":"too-large-run","op":{"type":"run","input":"x".repeat(MAX_PROTOCOL_LINE_BYTES)}});
    let line = format!("{value}\n");
    assert!(line.len() < MAX_SIDE_QUESTION_LINE_BYTES);
    cw.write_all(line.as_bytes()).await.unwrap();
    let mut lines = BufReader::new(cr).lines();
    let response = timeout(Duration::from_secs(10), lines.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let event: Value = serde_json::from_str(&response).unwrap();
    assert!(
        event["error"]
            .as_str()
            .unwrap()
            .contains(&MAX_PROTOCOL_LINE_BYTES.to_string()),
        "{event}"
    );
    assert_eq!(event["errorDetail"]["limitBytes"], MAX_PROTOCOL_LINE_BYTES);
    assert!(requests.lock().unwrap().is_empty());
    server.stop();
    done.await.ok();
}

#[tokio::test]
async fn protocol_line_reader_rejects_oversized_frame_and_resynchronizes() {
    let bytes = std::io::Cursor::new(b"123456789\nok\n".to_vec());
    let mut reader = BufReader::new(bytes);

    assert_eq!(
        read_protocol_line_with_limit(&mut reader, 8).await.unwrap(),
        ProtocolLine::TooLarge
    );
    assert_eq!(
        read_protocol_line_with_limit(&mut reader, 8).await.unwrap(),
        ProtocolLine::Line("ok".to_string())
    );
    assert_eq!(
        read_protocol_line_with_limit(&mut reader, 8).await.unwrap(),
        ProtocolLine::Eof
    );
}

#[test]
fn pending_dispatches_bound_bytes_and_preserve_fifo_after_rejection() {
    let op = |id: &str| AdmittedOp {
        op: Op {
            id: id.into(),
            op: OpKind::GetHistory,
        },
        run_signal: None,
        foreground_guard: None,
        busy_guard: None,
        interrupt: None,
    };
    let mut pending = PendingDispatches::default();
    assert!(pending
        .push(op("first"), MAX_PENDING_PROTOCOL_BYTES - 1)
        .is_none());
    assert_eq!(pending.push(op("overflow"), 2).unwrap().op.id, "overflow");
    assert!(pending.push(op("second"), 1).is_none());
    assert_eq!(pending.pop().unwrap().op.id, "first");
    assert!(pending
        .push(op("third"), MAX_PENDING_PROTOCOL_BYTES - 1)
        .is_none());
    assert_eq!(pending.pop().unwrap().op.id, "second");
    assert_eq!(pending.pop().unwrap().op.id, "third");
    assert_eq!(pending.bytes, 0);
    assert!(pending.pop().is_none());
}

#[tokio::test]
async fn completing_dispatch_keeps_a_partially_consumed_input_frame() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let factory = make_create_session(make_agent(".", vec![], None), None);
    let create: CreateSession = {
        let entered = entered.clone();
        let release = release.clone();
        Arc::new(move |args| {
            let entered = entered.clone();
            let release = release.clone();
            let factory = factory.clone();
            Box::pin(async move {
                entered.notify_one();
                release.notified().await;
                factory(args).await
            })
        })
    };
    // One byte of capacity forces the reader to consume a partial second frame
    // before the first dispatch is released; a restarted read would lose it.
    let (server_input, mut client_output) = tokio::io::duplex(1);
    let (server_output, client_input) = tokio::io::duplex(4096);
    let (server, done) = start_server(create, Box::new(server_input), Box::new(server_output));
    let mut lines = BufReader::new(client_input).lines();
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        client_output
            .write_all(b"{\"id\":\"first\",\"op\":{\"type\":\"get_cost\"}}\n")
            .await
            .unwrap();
        entered.notified().await;
        client_output
            .write_all(b"{\"id\":\"fragmented\",\"op\":{\"type\":\"get_cost\"")
            .await
            .unwrap();
        release.notify_one();
        loop {
            let line = lines.next_line().await.unwrap().unwrap();
            let value: ProtocolEvent =
                runtime_codec::decode_text(&RuntimeString::from_utf8(&line)).unwrap();
            if value.submission_id == "first" {
                assert!(value.cost.is_some());
                break;
            }
        }
        client_output.write_all(b"}}\n").await.unwrap();
        loop {
            let line = lines.next_line().await.unwrap().unwrap();
            let value: ProtocolEvent =
                runtime_codec::decode_text(&RuntimeString::from_utf8(&line)).unwrap();
            if value.submission_id == "fragmented" {
                assert!(value.cost.is_some());
                break;
            }
        }
    })
    .await
    .expect("a dispatch completion cannot truncate the next frame");
    server.stop();
    done.await.unwrap();
}

#[tokio::test]
async fn dispatch_line_accepts_a_lone_surrogate_escape() {
    let agent = make_agent(".", vec![], None);
    let (server_output, client_input) = tokio::io::duplex(4096);
    let server = ProtocolServer::new(ProtocolServerOptions {
        create_session: make_create_session(agent, None),
        lifecycle: None,
        plugin_admin: None,
        credential_store: None,
        input: Box::new(tokio::io::empty()),
        output: Box::new(server_output),
    });

    let op = server
            .inner
            .parse_line(
                r#"{"id":"surrogate-dispatch","op":{"type":"init_session","init":{"baseInstructions":"\ud800"}}}"#
            )
            .await
            .expect("valid protocol operation");
    server.inner.clone().dispatch(op).await;

    let line = BufReader::new(client_input)
        .lines()
        .next_line()
        .await
        .unwrap()
        .unwrap();
    let event: ProtocolEvent =
        runtime_codec::decode_text(&RuntimeString::from_utf8(&line)).unwrap();
    assert!(event.initialized.is_some(), "{line}");
}

#[tokio::test]
async fn write_emits_lone_surrogate_as_a_json_escape() {
    let agent = make_agent(".", vec![], None);
    let (server_output, client_input) = tokio::io::duplex(4096);
    let server = ProtocolServer::new(ProtocolServerOptions {
        create_session: make_create_session(agent, None),
        lifecycle: None,
        plugin_admin: None,
        credential_store: None,
        input: Box::new(tokio::io::empty()),
        output: Box::new(server_output),
    });

    server
        .inner
        .write(ProtocolEvent {
            submission_id: "surrogate-write".into(),
            event: Some(EngineEvent::Notification {
                level: NotificationLevel::Info,
                message: runtime_value(r#""\ud800""#).into_string().unwrap(),
            }),
            ..Default::default()
        })
        .await;

    let line = BufReader::new(client_input)
        .lines()
        .next_line()
        .await
        .unwrap()
        .unwrap();
    assert!(line.contains(r#""message":"\ud800""#), "{line}");
    assert!(!line.contains('\u{fdd0}'), "{line}");
}

/// The four MCP request ops reach a dispatcher that handles them.
///
/// `dispatch` picks between two dispatchers using a hand-maintained list of op
/// variants, and neither dispatcher is exhaustive on its own — each one ends in a
/// catch-all for "not mine". So a variant the list sends to the wrong dispatcher
/// is not a compile error; it is an op that arrives somewhere with no arm for it.
/// These four were listed as secondary while their handlers stayed in
/// `dispatch_primary`, and the catch-all was `unreachable!()`, so the op panicked
/// its task partway through. Nothing was ever written back, which made the
/// symptom a client hung forever on a correlated reply that could no longer
/// arrive — a very long way from the four-line cause.
///
/// Every other op in the list was reachable and stayed reachable, which is why
/// none of the rest of this suite noticed. What is asserted here is only that a
/// reply comes back at all: no server is configured, so the ENGINE's answer is
/// expected to be an error or an empty result, and either one proves the routing.
#[tokio::test]
async fn every_mcp_request_op_reaches_a_dispatcher_that_handles_it() {
    for (label, op) in [
        ("list_mcp_prompts", OpKind::ListMcpPrompts),
        (
            "get_mcp_prompt",
            OpKind::GetMcpPrompt {
                server_name: "nosuch".into(),
                name: "nosuch".into(),
                arguments: None,
            },
        ),
        (
            "call_mcp_tool",
            OpKind::CallMcpTool {
                server_name: "nosuch".into(),
                tool: "nosuch".into(),
                arguments: None,
            },
        ),
        (
            "read_mcp_resource",
            OpKind::ReadMcpResource {
                server_name: "nosuch".into(),
                uri: "file:///nothing".into(),
            },
        ),
    ] {
        let agent = make_agent(".", vec![], None);
        // Roomy on purpose: nothing reads this pipe until the dispatch below has
        // returned, so a buffer the exchange could fill would deadlock the test
        // rather than fail it.
        let (server_output, client_input) = tokio::io::duplex(1 << 16);
        let server = ProtocolServer::new(ProtocolServerOptions {
            create_session: make_create_session(agent, None),
            lifecycle: None,
            plugin_admin: None,
            credential_store: None,
            input: Box::new(tokio::io::empty()),
            output: Box::new(server_output),
        });

        let submission_id = format!("route-{label}");
        // Dispatched as a typed `Op` rather than a JSON line, because the routing
        // decision is made on the variant: a wire shape in between could only add
        // a way for this test to be wrong about what it submitted.
        server
            .inner
            .clone()
            .dispatch(Op {
                id: RuntimeString::from(submission_id.as_str()),
                op,
            })
            .await;

        // Read with a timeout rather than to EOF: the server still holds the
        // write half, so the line after the last one never arrives and never ends.
        let mut lines = BufReader::new(client_input).lines();
        let mut replied = false;
        while let Ok(Ok(Some(line))) =
            tokio::time::timeout(Duration::from_secs(10), lines.next_line()).await
        {
            assert!(
                !line.contains("wrong dispatcher"),
                "{label} was routed to a dispatcher with no arm for it: {line}"
            );
            let event: ProtocolEvent =
                runtime_codec::decode_text(&RuntimeString::from_utf8(&line)).unwrap();
            if event.submission_id == submission_id.as_str() {
                replied = true;
                break;
            }
        }
        assert!(
            replied,
            "{label} produced no reply correlated to its submission, so a client \
             that submitted it would wait forever"
        );
    }
}

fn make_create_session(
    agent: Arc<Agent>,
    permission_mode: Option<ExternalPermissionMode>,
) -> CreateSession {
    make_create_session_with_recovery(agent, permission_mode, None, None, None)
}

fn make_create_session_with_usage_ledger(
    agent: Arc<Agent>,
    permission_mode: Option<ExternalPermissionMode>,
    initial_usage_ledger: Option<Vec<UsageLedgerItem>>,
) -> CreateSession {
    make_create_session_with_recovery(agent, permission_mode, initial_usage_ledger, None, None)
}

fn make_create_session_with_recovery(
    agent: Arc<Agent>,
    permission_mode: Option<ExternalPermissionMode>,
    initial_usage_ledger: Option<Vec<UsageLedgerItem>>,
    initial_subagent_lifecycle: Option<Vec<SubagentLifecycleRecord>>,
    rollout: Option<RolloutSink>,
) -> CreateSession {
    Arc::new(move |args: CreateSessionArgs| {
        let agent = agent.clone();
        let pm = permission_mode;
        let initial_usage_ledger = initial_usage_ledger.clone();
        let initial_subagent_lifecycle = initial_subagent_lifecycle.clone();
        let rollout = rollout.clone();
        Box::pin(async move {
            let lsp_servers = args.init.lsp_servers.clone();
            let skills = args.skills.clone();
            let output_styles = args.output_styles.clone();
            let session = agent.start_session(StartSessionOptions {
                approval_handler: Some(args.approval_handler),
                elicit_handler: Some(args.elicit_handler),
                mcp_elicitation_presenter: Some(args.mcp_elicitation_presenter),
                mcp_authorize_handler: Some(args.mcp_authorize_handler),
                mcp_oauth_redirect_uri: args
                    .init
                    .mcp_oauth_redirect_uri
                    .clone()
                    .map(|value| scalar_runtime_string(value, "MCP OAuth redirect URI"))
                    .transpose()?,
                cwd: args.init.cwd.clone().map(Into::into),
                config: args
                    .init
                    .config
                    .as_ref()
                    .map(|value| scalar_json_value(value, "session config"))
                    .transpose()?,
                permission_mode: args.init.permission_mode.or(pm),
                provider: args.provider.clone(),
                mcp_server_configs: Some(args.mcp_server_configs.clone()),
                capability_roots: Some(args.capability_roots.clone()),
                selected_capability_roots: args.selected_capability_roots.clone(),
                base_instructions: args.init.base_instructions.clone(),
                developer_instructions: args.init.developer_instructions.clone(),
                agent_memory_snapshot_policy: args.init.agent_memory_snapshot_policy.clone(),
                mcp_tools: (!args.dynamic_tools.is_empty()).then_some(args.dynamic_tools),
                lsp_servers: (!lsp_servers.is_empty())
                    .then_some(Arc::new(move || lsp_servers.clone())),
                agents: (!args.agents.is_empty()).then_some(args.agents),
                skills: (!skills.is_empty()).then_some(Arc::new(move || skills.clone())),
                output_styles: (!output_styles.is_empty())
                    .then_some(Arc::new(move || output_styles.clone())),
                selected_output_style: args.init.selected_output_style.clone(),
                hooks: (!args.hooks.is_empty()).then_some(args.hooks),
                pricing: args.pricing,
                shell_env: args.init.shell_env.clone(),
                initial_usage_ledger,
                initial_subagent_lifecycle,
                rollout,
                ..Default::default()
            });
            Ok::<Arc<Session>, BoxError>(session)
        }) as CreateSessionFuture
    })
}

/// Points this test process at a private config directory instead of the
/// developer's real one.
///
/// The engine resolves its config directory on every call, so a test that never
/// sets `KLAUS_CONFIG_DIR` reads whatever the machine has: with `autoDream`
/// enabled in `~/.klaus/config.toml`, `dream_now` came back ok and the
/// gate-closed assertion failed on that machine and nowhere else. Every server
/// start funnels through here, so isolating once per process removes the
/// dependency instead of leaving each case to remember it. Cases that need a
/// specific directory still take [`ConfigDirEnvGuard`], which restores this one.
fn isolate_config_dir() {
    static ISOLATED: std::sync::LazyLock<()> = std::sync::LazyLock::new(|| {
        if std::env::var_os("KLAUS_CONFIG_DIR").is_some() {
            return;
        }
        let dir = unique_temp_dir("config");
        std::fs::create_dir_all(&dir).expect("create isolated protocol config dir");
        klaus_agent_core::runtime_process::set_process_environment_var("KLAUS_CONFIG_DIR", &dir);
    });
    std::sync::LazyLock::force(&ISOLATED);
}

fn start_server(
    create_session: CreateSession,
    server_r: Box<dyn AsyncRead + Unpin + Send>,
    server_w: Box<dyn AsyncWrite + Unpin + Send>,
) -> (Arc<ProtocolServer>, tokio::task::JoinHandle<()>) {
    start_server_with_lifecycle(create_session, None, server_r, server_w)
}

fn start_server_with_lifecycle(
    create_session: CreateSession,
    lifecycle: Option<ProtocolLifecycle>,
    server_r: Box<dyn AsyncRead + Unpin + Send>,
    server_w: Box<dyn AsyncWrite + Unpin + Send>,
) -> (Arc<ProtocolServer>, tokio::task::JoinHandle<()>) {
    isolate_config_dir();
    let server = Arc::new(ProtocolServer::new(ProtocolServerOptions {
        create_session,
        lifecycle,
        plugin_admin: None,
        credential_store: None,
        input: server_r,
        output: server_w,
    }));
    let handle = tokio::spawn(server.clone().start());
    (server, handle)
}

fn start_server_with_credential_store(
    create_session: CreateSession,
    credential_store: Arc<dyn CredentialStore>,
    server_r: Box<dyn AsyncRead + Unpin + Send>,
    server_w: Box<dyn AsyncWrite + Unpin + Send>,
) -> (Arc<ProtocolServer>, tokio::task::JoinHandle<()>) {
    isolate_config_dir();
    let server = Arc::new(ProtocolServer::new(ProtocolServerOptions {
        create_session,
        lifecycle: None,
        plugin_admin: None,
        credential_store: Some(credential_store),
        input: server_r,
        output: server_w,
    }));
    let handle = tokio::spawn(server.clone().start());
    (server, handle)
}

fn meta_with_id(id: &str, created_at: i64, cwd: &str) -> SessionMeta {
    SessionMeta {
        id: id.into(),
        created_at,
        cwd: cwd.into(),
        model: "fake-model".into(),
        title: None,
        originator: None,
        cli_version: None,
        source: None,
        role: None,
        parent_thread_id: None,
        forked_from_id: None,
        git: None,
        host_metadata: None,
    }
}

fn user_msg(uuid: &str, text: &str) -> Message {
    Message::User(UserMessage {
        question_answer: None,
        uuid: uuid.into(),
        content: vec![ContentBlock::Text(TextBlock {
            text: text.into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })],
        is_meta: None,
    })
}

fn sub_meta(created_at: i64) -> SubAgentMeta {
    SubAgentMeta {
        agent_type: Some("Explore".into()),
        model: "fake-model".into(),
        permission_mode: Some("default".into()),
        effort: Some(SubAgentEffort::Text("medium".into())),
        created_at,
    }
}

async fn write_persisted_session(store: &JsonlSessionStore, id: &str, created_at: i64) {
    let live = store
        .create(meta_with_id(id, created_at, "/tmp/klaus-proto"))
        .await
        .unwrap();
    live.append_message(user_msg("u1", "hello")).await;
    live.shutdown().await.unwrap();
}

fn protocol_lifecycle(agent: Arc<Agent>, store: Arc<dyn SessionStore>) -> ProtocolLifecycle {
    let list_store = store.clone();
    let resume_agent = agent.clone();
    let resume_store = store.clone();
    let fork_agent = agent.clone();
    let fork_store = store.clone();
    let archive_store = store.clone();
    let unarchive_store = store.clone();
    let delete_store = store.clone();
    let title_store = store.clone();
    let rename_store = store.clone();
    let transcript_store = store;
    ProtocolLifecycle {
        rollout_maintenance: None,
        classify_goal_pursuit: None,
        saved_goal_request: None,
        set_skills: None,
        read_thread: None,
        read_thread_page: None,
        read_thread_file_changes: None,
        read_session_summary: None,
        read_profile_activity: None,
        read_subagent_history: None,
        import_external_session: None,
        list_sessions: Some(Arc::new(move |opts: ListOptions| {
            let store = list_store.clone();
            Box::pin(async move { store.list(opts).await.map_err(|e| Box::new(e) as BoxError) })
        })),
        create_side_task: None,
        resume_session: Some(Arc::new(
            move |args: CreateSessionArgs,
                  thread_id: String,
                  _new_thread_id: Option<String>,
                  _fork_cut: Option<klaus_agent_core::engine::agent::ForkCut>| {
                let agent = resume_agent.clone();
                let store = resume_store.clone();
                Box::pin(async move {
                    let lsp_servers = args.init.lsp_servers.clone();
                    agent
                        .resume_session(
                            store,
                            &thread_id,
                            StartSessionOptions {
                                approval_handler: Some(args.approval_handler),
                                elicit_handler: Some(args.elicit_handler),
                                mcp_elicitation_presenter: Some(args.mcp_elicitation_presenter),
                                mcp_authorize_handler: Some(args.mcp_authorize_handler),
                                mcp_oauth_redirect_uri: args
                                    .init
                                    .mcp_oauth_redirect_uri
                                    .clone()
                                    .map(|value| {
                                        scalar_runtime_string(value, "MCP OAuth redirect URI")
                                    })
                                    .transpose()?,
                                cwd: args.init.cwd.clone().map(Into::into),
                                config: args
                                    .init
                                    .config
                                    .as_ref()
                                    .map(|value| scalar_json_value(value, "session config"))
                                    .transpose()?,
                                permission_mode: args.init.permission_mode,
                                provider: args.provider.clone(),
                                mcp_server_configs: Some(args.mcp_server_configs.clone()),
                                capability_roots: Some(args.capability_roots.clone()),
                                selected_capability_roots: args.selected_capability_roots.clone(),
                                base_instructions: args.init.base_instructions.clone(),
                                developer_instructions: args.init.developer_instructions.clone(),
                                mcp_tools: (!args.dynamic_tools.is_empty())
                                    .then_some(args.dynamic_tools),
                                lsp_servers: (!lsp_servers.is_empty())
                                    .then_some(Arc::new(move || lsp_servers.clone())),
                                shell_env: args.init.shell_env.clone(),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(|e| Box::new(e) as BoxError)
                }) as CreateSessionFuture
            },
        )),
        fork_session: Some(Arc::new(
            move |args: CreateSessionArgs,
                  thread_id: String,
                  new_thread_id: Option<String>,
                  fork_cut: Option<klaus_agent_core::engine::agent::ForkCut>| {
                let agent = fork_agent.clone();
                let store = fork_store.clone();
                Box::pin(async move {
                    let lsp_servers = args.init.lsp_servers.clone();
                    agent
                        .fork_session_with_cut(
                            store,
                            &thread_id,
                            fork_cut,
                            StartSessionOptions {
                                id: new_thread_id.map(Into::into),
                                approval_handler: Some(args.approval_handler),
                                elicit_handler: Some(args.elicit_handler),
                                mcp_elicitation_presenter: Some(args.mcp_elicitation_presenter),
                                mcp_authorize_handler: Some(args.mcp_authorize_handler),
                                mcp_oauth_redirect_uri: args
                                    .init
                                    .mcp_oauth_redirect_uri
                                    .clone()
                                    .map(|value| {
                                        scalar_runtime_string(value, "MCP OAuth redirect URI")
                                    })
                                    .transpose()?,
                                cwd: args.init.cwd.clone().map(Into::into),
                                config: args
                                    .init
                                    .config
                                    .as_ref()
                                    .map(|value| scalar_json_value(value, "session config"))
                                    .transpose()?,
                                permission_mode: args.init.permission_mode,
                                provider: args.provider.clone(),
                                mcp_server_configs: Some(args.mcp_server_configs.clone()),
                                capability_roots: Some(args.capability_roots.clone()),
                                selected_capability_roots: args.selected_capability_roots.clone(),
                                base_instructions: args.init.base_instructions.clone(),
                                developer_instructions: args.init.developer_instructions.clone(),
                                mcp_tools: (!args.dynamic_tools.is_empty())
                                    .then_some(args.dynamic_tools),
                                lsp_servers: (!lsp_servers.is_empty())
                                    .then_some(Arc::new(move || lsp_servers.clone())),
                                shell_env: args.init.shell_env.clone(),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(|e| Box::new(e) as BoxError)
                }) as CreateSessionFuture
            },
        )),
        archive_session: Some(Arc::new(move |thread_id: String| {
            let store = archive_store.clone();
            Box::pin(async move {
                store
                    .archive(&thread_id)
                    .await
                    .map_err(|e| Box::new(e) as BoxError)?;
                Ok(ArchiveSessionResult {
                    thread_id: thread_id.into(),
                    archived: true,
                })
            })
        })),
        unarchive_session: Some(Arc::new(move |thread_id: String| {
            let store = unarchive_store.clone();
            Box::pin(async move { store.unarchive(&thread_id).await })
        })),
        delete_session: Some(Arc::new(move |thread_id: String| {
            let store = delete_store.clone();
            Box::pin(async move {
                store
                    .delete(&thread_id)
                    .await
                    .map_err(|e| Box::new(e) as BoxError)?;
                Ok(DeleteSessionResult {
                    thread_id: thread_id.into(),
                    deleted: true,
                })
            })
        })),
        update_title: Some(Arc::new(move |thread_id: String, title: String| {
            let store = title_store.clone();
            Box::pin(async move {
                store
                    .update_title(&thread_id, &title)
                    .await
                    .map_err(|e| Box::new(e) as BoxError)?;
                Ok(UpdateTitleResult {
                    thread_id: thread_id.into(),
                    title: title.into(),
                })
            })
        })),
        rename_thread: Some(Arc::new(move |thread_id: String, new_thread_id: String| {
            let store = rename_store.clone();
            Box::pin(async move {
                store
                    .rename(&thread_id, &new_thread_id)
                    .await
                    .map_err(|e| Box::new(e) as BoxError)?;
                Ok(RenameThreadResult {
                    thread_id: new_thread_id.into(),
                    renamed: true,
                })
            })
        })),
        update_thread_host_metadata: None,
        load_subagent_transcript: Some(Arc::new(move |thread_id: String, agent_id: String| {
            let store = transcript_store.clone();
            Box::pin(async move {
                store
                    .load_sub_agent(&thread_id, &agent_id)
                    .await
                    .map_err(|e| Box::new(e) as BoxError)
            })
        })),
    }
}

fn unique_temp_dir(tag: &str) -> std::path::PathBuf {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "klaus-proto-{tag}-{}-{}-{}",
        std::process::id(),
        n,
        nanos
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn is_completed(e: &ProtocolEvent) -> bool {
    matches!(e.event, Some(EngineEvent::TurnCompleted { .. }))
}
fn is_terminal(e: &ProtocolEvent) -> bool {
    matches!(
        e.event,
        Some(EngineEvent::TurnCompleted { .. }) | Some(EngineEvent::TurnFailed { .. })
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn relays_a_scripted_text_turn_tagged_with_submission_id() {
    let agent = make_agent(".", vec![text_turn("hello from the engine")], None);
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(make_create_session(agent, None), sr, sw);
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    let (received, submission_id) = timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        let mut events = client.events();
        let submission_id = client.run(Input::Text("say hello".into()), None).await;
        let mut received: Vec<Arc<ProtocolEvent>> = Vec::new();
        while let Some(evt) = events.next().await {
            let stop = is_completed(&evt);
            received.push(evt);
            if stop {
                break;
            }
        }
        (received, submission_id)
    })
    .await
    .expect("timed out");

    server.stop();
    done.await.ok();

    assert!(!received.is_empty());
    assert!(received
        .iter()
        .filter(|e| e.event.is_some())
        .all(|e| e.submission_id == submission_id));
    assert!(received
        .iter()
        .any(|e| matches!(e.event, Some(EngineEvent::TurnStarted { .. }))));
    assert!(is_completed(received.last().unwrap()));
    let text = received
        .iter()
        .filter_map(|e| match &e.event {
            Some(EngineEvent::TextDelta { text, .. }) => Some(text.clone()),
            _ => None,
        })
        .fold(RuntimeString::default(), |output, text| {
            output.concat(&text)
        });
    assert!(text.contains("hello from the engine"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupt_releases_a_turn_parked_on_an_approval() {
    let agent = make_agent(
        ".",
        vec![
            tool_use_turn("t1", "write", json!({ "path": "x.txt", "content": "y" })),
            text_turn("after"),
        ],
        None,
    );
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(make_create_session(agent, None), sr, sw);
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    let received = timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        let mut events = client.events();
        client.run(Input::Text("write a file".into()), None).await;
        let mut received: Vec<Arc<ProtocolEvent>> = Vec::new();
        let mut sent_interrupt = false;
        while let Some(evt) = events.next().await {
            let is_approval = matches!(evt.event, Some(EngineEvent::ApprovalRequested { .. }));
            let terminal = is_terminal(&evt);
            received.push(evt);
            if is_approval && !sent_interrupt {
                sent_interrupt = true;
                client.interrupt().await;
            }
            if terminal {
                break;
            }
        }
        received
    })
    .await
    .expect("timed out");

    server.stop();
    done.await.ok();

    assert!(received
        .iter()
        .any(|e| matches!(e.event, Some(EngineEvent::ApprovalRequested { .. }))));
    assert!(received.iter().any(|e| is_terminal(e)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn surfaces_ask_user_as_elicitation_and_resumes() {
    let agent = make_agent(
        ".",
        vec![
            tool_use_turn(
                "t1",
                "ask_user",
                json!({ "question": "pick one", "options": ["a", "b"] }),
            ),
            text_turn("chose a"),
        ],
        None,
    );
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    let prompt = timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        let mut events = client.events();
        client
            .run(Input::Text("ask me something".into()), None)
            .await;
        let mut prompt: Option<RuntimeString> = None;
        while let Some(evt) = events.next().await {
            if let Some(el) = &evt.elicitation {
                prompt = Some(el.request.prompt.clone());
                client
                    .resolve_elicitation(
                        el.request_id
                            .try_to_utf8()
                            .expect("scalar elicitation request id"),
                        crate::types::ElicitResponseWire::Accept {
                            values: vec!["a".into()],
                        },
                    )
                    .await;
            }
            if is_terminal(&evt) {
                break;
            }
        }
        prompt
    })
    .await
    .expect("timed out");

    server.stop();
    done.await.ok();
    assert_eq!(prompt.as_ref(), Some(&RuntimeString::from("pick one")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_oauth_authorization_round_trip_releases_the_core_handler_without_echoing_code() {
    let agent = make_agent(".", vec![text_turn("authorized")], None);
    let responses = Arc::new(StdMutex::new(Vec::new()));
    let responses_for_session = responses.clone();
    let create_session: CreateSession = Arc::new(move |args: CreateSessionArgs| {
        let agent = agent.clone();
        let responses = responses_for_session.clone();
        Box::pin(async move {
            let response = args
                .mcp_authorize_handler
                .authorize(
                    klaus_agent_core::mcp::client::McpAuthorizeRequest {
                        server_name: "docs".to_string(),
                        authorization_url: "https://auth.example/authorize?state=csrf-state"
                            .to_string(),
                        redirect_uri: "http://127.0.0.1:33418/callback".to_string(),
                    },
                    klaus_agent_core::types::abort::AbortSignal::new(),
                )
                .await;
            responses.lock().unwrap().push(response);
            Ok::<Arc<Session>, BoxError>(agent.start_session(StartSessionOptions {
                approval_handler: Some(args.approval_handler),
                elicit_handler: Some(args.elicit_handler),
                mcp_elicitation_presenter: Some(args.mcp_elicitation_presenter),
                mcp_authorize_handler: Some(args.mcp_authorize_handler),
                ..Default::default()
            }))
        }) as CreateSessionFuture
    });
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(create_session, sr, sw);
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    let serialized_events = timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        let mut events = client.events();
        let run_id = client.run(Input::Text("connect MCP".into()), None).await;
        let mut serialized = Vec::new();
        let mut resolved = false;
        while let Some(event) = events.next().await {
            serialized.push(
                runtime_codec::encode_text(&*event)
                    .unwrap()
                    .try_to_utf8()
                    .unwrap(),
            );
            if let Some(request) = &event.mcp_authorization {
                assert_eq!(event.submission_id, run_id);
                assert_eq!(request.server_name, "docs");
                let ack = client
                    .resolve_mcp_authorization(
                        request
                            .request_id
                            .try_to_utf8()
                            .expect("scalar MCP authorization request id"),
                        crate::types::McpAuthorizationResponseWire::Code {
                            code: "authorization-code-secret".into(),
                            state: Some("csrf-state".into()),
                        },
                    )
                    .await
                    .expect("authorization resolved");
                assert!(ack.accepted);
            }
            if let Some(ack) = &event.mcp_authorization_resolved {
                assert!(!event.submission_id.is_empty());
                assert!(ack.accepted);
                resolved = true;
            }
            if is_completed(&event) {
                assert!(resolved);
                break;
            }
        }
        serialized
    })
    .await
    .expect("timed out");

    {
        let responses = responses.lock().unwrap();
        assert_eq!(responses.len(), 1);
        assert!(matches!(
            &responses[0],
            klaus_agent_core::mcp::client::McpAuthorizeResponse::Code { code, state }
                if code == "authorization-code-secret" && state.as_deref() == Some("csrf-state")
        ));
    }
    assert!(serialized_events
        .iter()
        .all(|event| !event.contains("authorization-code-secret")));

    server.stop();
    done.await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_oauth_status_and_revoke_use_the_endpoint_credential_store() {
    let store = Arc::new(InMemoryCredentialStore::new());
    let mcp_url = "https://mcp.example.test/mcp";
    let server_key = klaus_agent_core::mcp::oauth_provider::mcp_oauth_server_key(
        &klaus_agent_core::mcp::oauth_provider::McpOAuthConfig {
            transport: "http".to_string(),
            name: "docs".to_string(),
            url: mcp_url.to_string(),
            headers: None,
        },
    );
    store
        .set(
            &format!("mcp:oauth:{server_key}:tokens"),
            &json!({
                "tokens": {
                    "access_token": "protocol-access-secret",
                    "refresh_token": "protocol-refresh-secret",
                    "token_type": "Bearer",
                    "expires_in": 3600
                },
                "expiresAt": 4_102_444_800_000_i64
            })
            .to_string(),
        )
        .await
        .unwrap();
    store
        .set(
            &format!("mcp:oauth:{server_key}:client"),
            "{\"client_id\":\"persisted-client\"}",
        )
        .await
        .unwrap();
    store
        .set(
            &format!("mcp:oauth:{server_key}:discovery"),
            "{\"authorization_server_url\":\"https://auth.example.test\"}",
        )
        .await
        .unwrap();

    let agent = make_agent(".", vec![], None);
    let session_store = store.clone();
    let create_session: CreateSession = Arc::new(move |args: CreateSessionArgs| {
        let agent = agent.clone();
        let store = session_store.clone();
        Box::pin(async move {
            Ok::<Arc<Session>, BoxError>(agent.start_session(StartSessionOptions {
                approval_handler: Some(args.approval_handler),
                elicit_handler: Some(args.elicit_handler),
                mcp_elicitation_presenter: Some(args.mcp_elicitation_presenter),
                mcp_authorize_handler: Some(args.mcp_authorize_handler),
                credential_store: Some(store),
                mcp_server_configs: Some(args.mcp_server_configs),
                ..Default::default()
            }))
        }) as CreateSessionFuture
    });
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server_with_credential_store(create_session, store.clone(), sr, sw);
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        client
            .init_session(ProtocolSessionInit {
                host_capabilities: Some(ProtocolHostCapabilitiesInit {
                    mcp_servers: vec![ProtocolMcpServerInit::Http {
                        name: "docs".into(),
                        url: mcp_url.into(),
                        headers: None,
                        oauth: Some(ProtocolMcpOAuthOption::Advertised(true)),
                        always_load: None,
                        disabled_tools: None,
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .expect("session init");

        let status = client
            .get_mcp_authorization_status("docs".to_string())
            .await
            .expect("authorization status");
        assert_eq!(status.auth_status, crate::types::McpAuthStatus::OAuth);

        let revoked = client
            .revoke_mcp_authorization("docs".to_string())
            .await
            .expect("authorization revoked");
        assert!(revoked.revoked);
        assert_eq!(
            revoked.auth_status,
            crate::types::McpAuthStatus::NotLoggedIn
        );
        let status = client
            .get_mcp_authorization_status("docs".to_string())
            .await
            .expect("post-revoke status");
        assert_eq!(status.auth_status, crate::types::McpAuthStatus::NotLoggedIn);
    })
    .await
    .expect("timed out");

    for part in ["tokens", "client", "discovery"] {
        assert!(store
            .get(&format!("mcp:oauth:{server_key}:{part}"))
            .await
            .unwrap()
            .is_none());
    }
    server.stop();
    done.await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_runtime_ops_return_authoritative_snapshot() {
    let turn_started = Arc::new(Notify::new());
    let turn_cancelled = Arc::new(Notify::new());
    let agent = make_blocking_agent(".", turn_started.clone(), turn_cancelled.clone());
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(make_create_session(agent, None), sr, sw);
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });
    let missing_server = |name: &str| ProtocolMcpServerInit::Stdio {
        name: name.into(),
        command: "/definitely/missing/klaus-mcp-server".into(),
        args: None,
        env: None,
        always_load: Some(true),
        disabled_tools: None,
    };

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        client
            .init_session(ProtocolSessionInit {
                host_capabilities: Some(ProtocolHostCapabilitiesInit {
                    mcp_servers: vec![missing_server("configured")],
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .expect("session init");

        let runtime = client.get_mcp_runtime().await.expect("runtime snapshot");
        assert_eq!(
            runtime.activation_scope,
            crate::types::McpRuntimeScope::ActiveSession
        );
        assert_eq!(runtime.runtime.servers.len(), 1);
        assert_eq!(runtime.runtime.servers[0].name, "configured");
        assert_eq!(
            runtime.runtime.servers[0].state,
            klaus_agent_core::mcp::types::McpConnectionState::Failed
        );
        assert!(runtime.runtime.servers[0].tools.is_empty());

        let upserted = client
            .upsert_mcp_server(missing_server("added"))
            .await
            .expect("upsert runtime");
        assert_eq!(upserted.runtime.servers.len(), 2);
        assert!(
            upserted
                .runtime
                .servers
                .iter()
                .all(|server| server.state
                    == klaus_agent_core::mcp::types::McpConnectionState::Failed)
        );

        let removed = client
            .remove_mcp_server("configured".to_string())
            .await
            .expect("remove runtime");
        assert_eq!(
            removed
                .runtime
                .servers
                .iter()
                .map(|server| server.name.clone())
                .collect::<Vec<_>>(),
            vec![RuntimeString::from("added")]
        );

        let reloaded = client
            .reload_mcp_servers(vec![missing_server("reloaded")])
            .await
            .expect("reload runtime");
        assert_eq!(reloaded.runtime.servers[0].name, "reloaded");
        let reconnected = client
            .reconnect_mcp_server("reloaded".to_string())
            .await
            .expect("reconnect runtime");
        assert_eq!(
            reconnected.runtime.servers[0].state,
            klaus_agent_core::mcp::types::McpConnectionState::Failed
        );

        client.run(Input::Text("hold turn".into()), None).await;
        turn_started.notified().await;
        let busy = client
            .remove_mcp_server("reloaded".to_string())
            .await
            .expect_err("active-turn mutation must fail");
        assert!(matches!(busy, ClientError::Op(message) if message.starts_with("session_busy:")));
        client.interrupt().await;
        turn_cancelled.notified().await;
    })
    .await
    .expect("timed out");

    server.stop();
    done.await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_elicitation_round_trips_structured_protocol_response() {
    let mcp_server = Arc::new(ProtocolElicitationServer::new());
    let mcp_config = McpServerConfig::InProcess {
        name: "interactive".to_string(),
        server: mcp_server.clone(),
        options: McpServerOptions::default(),
    };
    let turn_started = Arc::new(Notify::new());
    let turn_cancelled = Arc::new(Notify::new());
    let agent = make_blocking_agent(".", turn_started.clone(), turn_cancelled.clone());
    let create_session: CreateSession = Arc::new(move |args: CreateSessionArgs| {
        let agent = agent.clone();
        let mcp_config = mcp_config.clone();
        Box::pin(async move {
            Ok(agent.start_session(StartSessionOptions {
                approval_handler: Some(args.approval_handler),
                elicit_handler: Some(args.elicit_handler),
                mcp_elicitation_presenter: Some(args.mcp_elicitation_presenter),
                mcp_authorize_handler: Some(args.mcp_authorize_handler),
                mcp_server_configs: Some(vec![mcp_config]),
                ..Default::default()
            }))
        })
    });
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (protocol_server, done) = start_server(create_session, sr, sw);
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });
    let mut events = client.events();

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
            client.get_mcp_runtime().await.expect("connect MCP runtime");
            let request = {
                let mcp_server = mcp_server.clone();
                tokio::spawn(async move {
                    mcp_server
                        .request_elicitation(
                            7,
                            json!({
                                "_meta": { "persist": true },
                                "message": "Allow calendar access?",
                                "requestedSchema": {
                                    "type": "object",
                                    "properties": {
                                        "account": { "type": "string", "format": "email" },
                                        "remember": { "type": "boolean" }
                                    },
                                    "required": ["account"]
                                }
                            }),
                        )
                        .await
                })
            };
            let elicitation = loop {
                let event = events.next().await.expect("MCP elicitation event");
                if let Some(elicitation) = &event.mcp_elicitation {
                    break elicitation.clone();
                }
            };
            assert_eq!(elicitation.server_name, "interactive");
            assert_eq!(
                elicitation.request.meta(),
                Some(
                    &runtime_json_from_scalar_value(&json!({ "persist": true }))
                        .expect("scalar MCP metadata fixture")
                )
            );
            let resolved = client
                .resolve_mcp_elicitation(
                    elicitation
                        .request_id
                        .try_to_utf8()
                        .expect("scalar MCP elicitation request id"),
                    crate::types::McpElicitationResponse {
                        action: crate::types::McpElicitationAction::Accept,
                        content: Some(
                            runtime_json_from_scalar_value(&json!({
                                "account": "person@example.com",
                                "remember": true
                            }))
                            .expect("scalar MCP content fixture"),
                        ),
                        meta: Some(
                            runtime_json_from_scalar_value(&json!({ "persist": true }))
                                .expect("scalar MCP metadata fixture"),
                        ),
                    },
                )
                .await
                .expect("resolve MCP elicitation");
            assert_eq!(resolved.request_id, elicitation.request_id);
            assert_eq!(resolved.action, crate::types::McpElicitationAction::Accept);
            let duplicate = client
                .resolve_mcp_elicitation(
                    elicitation
                        .request_id
                        .try_to_utf8()
                        .expect("scalar MCP elicitation request id"),
                    crate::types::McpElicitationResponse {
                        action: crate::types::McpElicitationAction::Decline,
                        content: None,
                        meta: None,
                    },
                )
                .await
                .expect_err("duplicate resolution must fail");
            assert!(
                matches!(duplicate, ClientError::Op(message) if message.starts_with("request_not_found:"))
            );
            let response = request.await.unwrap();
            assert_eq!(response["action"], RuntimeJson::from("accept"));
            assert_eq!(
                response["content"]["account"],
                RuntimeJson::from("person@example.com")
            );
            assert_eq!(
                response["_meta"],
                runtime_json_from_scalar_value(&json!({ "persist": true }))
                    .expect("scalar MCP metadata fixture")
            );

            client
                .run(Input::Text("hold turn".into()), None)
                .await;
            turn_started.notified().await;
            let interrupted_request = {
                let mcp_server = mcp_server.clone();
                tokio::spawn(async move {
                    mcp_server
                        .request_elicitation(
                            8,
                            json!({
                                "message": "Pending during turn",
                                "requestedSchema": { "type": "object", "properties": {} }
                            }),
                        )
                        .await
                })
            };
            loop {
                let event = events
                    .next()
                    .await
                    .expect("interruptible MCP elicitation event");
                if event.mcp_elicitation.is_some() {
                    break;
                }
            }
            client.interrupt().await;
            turn_cancelled.notified().await;
            assert_eq!(
                interrupted_request.await.unwrap()["action"],
                RuntimeJson::from("cancel")
            );
            loop {
                let event = events.next().await.expect("turn terminal event");
                if is_terminal(&event) {
                    break;
                }
            }

            let shutdown_request = {
                let mcp_server = mcp_server.clone();
                tokio::spawn(async move {
                    mcp_server
                        .request_elicitation(
                            9,
                            json!({
                                "message": "Pending at shutdown",
                                "requestedSchema": { "type": "object", "properties": {} }
                            }),
                        )
                        .await
                })
            };
            let shutdown_event = loop {
                let event = events
                    .next()
                    .await
                    .expect("shutdown MCP elicitation event");
                if event.mcp_elicitation.is_some() {
                    break event;
                }
            };
            assert!(shutdown_event.submission_id.is_empty());
            protocol_server.stop();
            assert_eq!(
                shutdown_request.await.unwrap()["action"],
                RuntimeJson::from("cancel")
            );
        })
        .await
        .expect("timed out");

    done.await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_capability_snapshot_is_effective_state() {
    let roots = vec![
        RuntimeCapabilityRoot::host("host.browser")
            .with_browser_tool_mapping(BrowserHostToolMapping::for_mcp_server("host-browser")),
        RuntimeCapabilityRoot::plugin("calendar", Some("2.0.0"), "/plugins/calendar")
            .with_browser_tool_mapping(BrowserHostToolMapping::for_mcp_server("plugin-browser")),
        RuntimeCapabilityRoot::environment("desktop", "/runtime/browser")
            .with_browser_tool_mapping(BrowserHostToolMapping::for_mcp_server(
                "environment-browser",
            )),
    ];
    let selected = vec![
        SelectedRuntimeCapabilityRoot::host("host.browser"),
        SelectedRuntimeCapabilityRoot::plugin("calendar"),
        SelectedRuntimeCapabilityRoot::environment("desktop", "/runtime/browser"),
        SelectedRuntimeCapabilityRoot::plugin("missing"),
    ];
    let agent = make_agent(".", vec![text_turn("unused")], None);
    let create_session: CreateSession = Arc::new(move |args: CreateSessionArgs| {
        let agent = agent.clone();
        let roots = roots.clone();
        let selected = selected.clone();
        Box::pin(async move {
            Ok(agent.start_session(StartSessionOptions {
                approval_handler: Some(args.approval_handler),
                elicit_handler: Some(args.elicit_handler),
                mcp_elicitation_presenter: Some(args.mcp_elicitation_presenter),
                mcp_authorize_handler: Some(args.mcp_authorize_handler),
                capability_roots: Some(roots),
                selected_capability_roots: Some(selected),
                ..Default::default()
            }))
        })
    });
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(create_session, sr, sw);
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    let snapshot = client
        .get_runtime_capabilities()
        .await
        .expect("runtime capability snapshot");
    assert_eq!(snapshot.selected_roots.len(), 4);
    assert_eq!(
        snapshot
            .selected_roots
            .iter()
            .map(|root| root.id.clone())
            .collect::<Vec<_>>(),
        vec![
            RuntimeString::from("host.browser"),
            RuntimeString::from("calendar"),
            RuntimeString::from("desktop:/runtime/browser"),
            RuntimeString::from("missing"),
        ]
    );
    assert_eq!(snapshot.effective_roots.len(), 3);
    assert_eq!(snapshot.descriptors.len(), 3);
    assert_eq!(
        snapshot.descriptors[0].source.kind,
        RuntimeCapabilitySourceKind::Host
    );
    assert_eq!(
        snapshot.descriptors[1].source.version.as_ref(),
        Some(&RuntimeString::from("2.0.0"))
    );
    assert_eq!(
        snapshot.descriptors[2].source.root.as_ref(),
        Some(&RuntimeString::from("/runtime/browser"))
    );
    assert_eq!(snapshot.missing_roots.len(), 1);
    assert_eq!(snapshot.missing_roots[0].id, "missing");

    server.stop();
    done.await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stdio_extensions_are_effective_and_diagnostic() {
    let dir = unique_temp_dir("stdio-extensions");
    let hook_marker = dir.join("pre-tool-use-ran");
    let hook_command = format!(
        "printf hit > '{}'; printf '%s' '{}'",
        hook_marker.display(),
        r#"{"decision":"block","reason":"blocked by protocol hook"}"#
    );
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let calls = Arc::new(AtomicUsize::new(0));
    let call_model = {
        let requests = requests.clone();
        let calls = calls.clone();
        Box::new(
            move |request: ModelRequest, signal: AbortSignal| -> ModelStream {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                requests.lock().unwrap().push(request);
                let events = if call == 0 {
                    let mut events =
                        tool_use_turn("blocked-read", "read", json!({ "path": "Cargo.toml" }));
                    *events.last_mut().unwrap() = ev::stop(
                        Some(ModelStopReason::ToolUse),
                        Some(Usage {
                            input_tokens: 0,
                            output_tokens: 0,
                            cache_read_input_tokens: 0,
                            cache_creation_input_tokens: 0,
                            total_tokens: None,
                        }),
                    );
                    events
                } else {
                    vec![
                        ev::message_start(None),
                        ev::text("hook blocked the read"),
                        ev::stop(
                            Some(ModelStopReason::EndTurn),
                            Some(Usage {
                                input_tokens: 1_000_000,
                                output_tokens: 0,
                                cache_read_input_tokens: 0,
                                cache_creation_input_tokens: 0,
                                total_tokens: None,
                            }),
                        ),
                    ]
                };
                Box::pin(
                    futures::stream::iter(events)
                        .take_while(move |_| futures::future::ready(!signal.is_cancelled())),
                )
            },
        )
    };
    let plugin = LoadedPlugin {
        name: "plugin-docs".into(),
        version: Some("1.0.0".into()),
        display_name: None,
        description: None,
        plugin_namespace: "plugin-docs".into(),
        keywords: Vec::new(),
        dependencies: Vec::new(),
        user_config: json!({ "apiKey": "plugin-secret" })
            .as_object()
            .unwrap()
            .clone(),
        settings: json!({ "internalPrompt": "plugin-settings-secret" })
            .as_object()
            .unwrap()
            .clone(),
        dir: "/plugins/plugin-docs".into(),
        skills: vec![Skill {
            name: "desktop-skill".into(),
            description: "plugin definition that must be suppressed".into(),
            body: "plugin skill secret body".into(),
            path: "/plugins/plugin-docs/skills/desktop-skill/SKILL.md".into(),
            ..Default::default()
        }],
        commands: Vec::new(),
        agents: Vec::new(),
        hooks: Vec::new(),
        output_styles: Vec::new(),
        mcp_servers: Vec::new(),
        lsp_servers: Default::default(),
        runtime_capabilities: Vec::new(),
        interface_capabilities: Vec::new(),
        app_connectors: Vec::new(),
    };
    let agent = Arc::new(Agent::new(AgentOptions {
        process_context: test_process_context(),
        provider: Arc::new(fake_provider()) as Arc<dyn ModelProvider>,
        config: None,
        tools: None,
        web_fetch_page: None,
        browser_runtime: None,
        capability_roots: Vec::new(),
        cwd: Some(dir.to_string_lossy().into_owned()),
        deps: EngineDepsOverrides {
            call_model: Some(call_model),
            estimate_tokens: None,
            compactor: Some(noop_compactor()),
            uuid: None,
            now: None,
        },
        compactor: None,
        approval_handler: None,
        approval_reviewer: None,
        elicit_handler: None,
        credential_store: None,
        hooks: Vec::new(),
        observer: Some(None),
        logger: None,
        pricing: Some(HashMap::from([(
            "fake-model".to_string(),
            ModelPricing {
                input_per_m_tok: 99.0,
                output_per_m_tok: 0.0,
                cache_read_per_m_tok: 0.0,
                cache_write_per_m_tok: 0.0,
            },
        )])),
        agents: Some(vec![AgentDefinition {
            name: "desktop-agent".into(),
            description: "endpoint definition that must be suppressed".into(),
            prompt: Some("endpoint agent secret prompt".into()),
            ..Default::default()
        }]),
        include_default_agents: None,
        skills: Some(Arc::new(|| {
            vec![Skill {
                name: "desktop-skill".into(),
                description: "endpoint definition that must be suppressed".into(),
                body: "endpoint skill secret body".into(),
                path: "/endpoint/skills/desktop-skill/SKILL.md".into(),
                ..Default::default()
            }]
        })),
        output_styles: Some(Arc::new(|| {
            vec![OutputStyle {
                name: "formal".into(),
                description: Some("Endpoint formal prose".into()),
                body: "ENDPOINT STYLE SECRET BODY".into(),
            }]
        })),
        workflows: None,
        mcp_catalog: None,
        plugin_catalog: None,
        lsp_servers: None,
        agent_memory: None,
        mcp_clients: None,
        mcp_server_configs: Vec::new(),
        mcp_tools: None,
        refresh_tools: None,
        system_prompt_provider: None,
        plugins: vec![plugin],
        plugin_load_report: None,
    }));
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        client
            .init_session(ProtocolSessionInit {
                agents: vec![AgentDefinition {
                    name: "desktop-agent".into(),
                    description: "host-provided desktop agent".into(),
                    prompt: Some("desktop agent secret prompt".into()),
                    ..Default::default()
                }],
                skills: vec![Skill {
                    name: "desktop-skill".into(),
                    description: "host-provided desktop skill".into(),
                    body: "desktop skill secret body".into(),
                    path: "/desktop/skills/desktop-skill/SKILL.md".into(),
                    ..Default::default()
                }],
                output_styles: vec![OutputStyle {
                    name: "formal".into(),
                    description: Some("Formal prose".into()),
                    body: "FORMAL STYLE BODY SENTINEL".into(),
                }],
                selected_output_style: Some("formal".into()),
                command_hooks: vec![ProtocolCommandHook {
                    event: HookEvent::PreToolUse,
                    matcher: Some("read".into()),
                    command: hook_command.clone().into(),
                    timeout_ms: Some(5_000),
                    is_async: false,
                }],
                pricing: Some(vec![ProtocolModelPricing {
                    model: "fake-model".into(),
                    pricing: ModelPricing {
                        input_per_m_tok: 7.0,
                        output_per_m_tok: 0.0,
                        cache_read_per_m_tok: 0.0,
                        cache_write_per_m_tok: 0.0,
                    },
                }]),
                permission_mode: Some(ExternalPermissionMode::BypassPermissions),
                ..Default::default()
            })
            .await
            .expect("stdio extensions init");
        client
            .run_to_completion(Input::Text("exercise extensions".into()), None)
            .await
            .expect("run");

        assert_eq!(std::fs::read_to_string(&hook_marker).unwrap(), "hit");
        let requests = requests.lock().unwrap().clone();
        assert_eq!(requests.len(), 2);
        let tool_surface = runtime_codec::encode_text(&requests[0].tools)
            .unwrap()
            .try_to_utf8()
            .unwrap();
        assert!(tool_surface.contains("desktop-agent"));
        assert!(runtime_codec::encode_text(&requests[0])
            .unwrap()
            .try_to_utf8()
            .unwrap()
            .contains("desktop-skill"));
        assert!(requests[0]
            .system
            .iter()
            .any(|part| part.text.contains("FORMAL STYLE BODY SENTINEL")));
        assert!(runtime_codec::encode_text(&requests[1].messages)
            .unwrap()
            .try_to_utf8()
            .unwrap()
            .contains("blocked by protocol hook"));

        let cost = client.get_cost().await.expect("cost");
        assert_eq!(cost.total_cost_usd, 7.0);
        let snapshot = client
            .get_extension_runtime()
            .await
            .expect("effective extension runtime");
        assert!(snapshot.agents.iter().any(|entry| {
            entry.name == "desktop-agent" && entry.source.kind == ExtensionSourceKind::Session
        }));
        assert!(snapshot.skills.iter().any(|entry| {
            entry.name == "desktop-skill" && entry.source.kind == ExtensionSourceKind::Session
        }));
        assert!(snapshot.output_styles.iter().any(|entry| {
            entry.name == "formal" && entry.source.kind == ExtensionSourceKind::Session
        }));
        assert_eq!(
            snapshot.selected_output_style.as_ref(),
            Some(&RuntimeString::from("formal"))
        );
        assert!(snapshot.hooks.iter().any(|entry| {
            entry.event == HookEvent::PreToolUse
                && entry.handler_kind == HookHandlerKind::Command
                && entry.source.kind == ExtensionSourceKind::Session
        }));
        assert_eq!(snapshot.pricing.model_ids, vec!["fake-model"]);
        assert!(snapshot.suppressed.iter().any(|entry| {
            entry.kind == ExtensionKind::Skill
                && entry.name == "desktop-skill"
                && entry.source.kind == ExtensionSourceKind::Plugin
                && entry.replaced_by.kind == ExtensionSourceKind::Session
        }));
        assert!(snapshot.suppressed.iter().any(|entry| {
            entry.kind == ExtensionKind::Agent
                && entry.name == "desktop-agent"
                && entry.source.kind == ExtensionSourceKind::Host
                && entry.replaced_by.kind == ExtensionSourceKind::Session
        }));
        assert!(snapshot.suppressed.iter().any(|entry| {
            entry.kind == ExtensionKind::Skill
                && entry.name == "desktop-skill"
                && entry.source.kind == ExtensionSourceKind::Host
                && entry.replaced_by.kind == ExtensionSourceKind::Session
        }));
        assert!(snapshot.suppressed.iter().any(|entry| {
            entry.kind == ExtensionKind::OutputStyle
                && entry.name == "formal"
                && entry.source.kind == ExtensionSourceKind::Host
                && entry.replaced_by.kind == ExtensionSourceKind::Session
        }));
        let diagnostic_json = runtime_codec::encode_text(&snapshot)
            .unwrap()
            .try_to_utf8()
            .unwrap();
        for secret in [
            &hook_command,
            "plugin-secret",
            "plugin-settings-secret",
            "plugin skill secret body",
            "endpoint skill secret body",
            "endpoint agent secret prompt",
            "ENDPOINT STYLE SECRET BODY",
            "desktop skill secret body",
            "desktop agent secret prompt",
            "FORMAL STYLE BODY SENTINEL",
        ] {
            assert!(
                !diagnostic_json.contains(secret),
                "diagnostic leaked {secret}"
            );
        }
    })
    .await
    .expect("timed out");

    server.stop();
    done.await.ok();
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn stdio_extensions_reject_ambiguous_or_callback_configuration() {
    let duplicate = ProtocolSessionInit {
        agents: vec![
            AgentDefinition {
                name: "duplicate".into(),
                description: "first".into(),
                ..Default::default()
            },
            AgentDefinition {
                name: "duplicate".into(),
                description: "second".into(),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    assert_eq!(
        validate_serializable_session_init(&duplicate).unwrap_err(),
        "duplicate agent 'duplicate'"
    );

    let exact_names = ProtocolSessionInit {
        agents: vec![AgentDefinition {
            name: RuntimeString::from_utf16(vec![0xd800]),
            description: "exact agent".into(),
            ..Default::default()
        }],
        skills: vec![Skill {
            name: RuntimeString::from_utf16(vec![0xdc00]),
            description: "exact skill".into(),
            body: "body".into(),
            path: "/skills/exact/SKILL.md".into(),
            ..Default::default()
        }],
        output_styles: vec![OutputStyle {
            name: RuntimeString::from_utf16(vec![0xd800, 0xdc00]),
            description: None,
            body: "body".into(),
        }],
        selected_output_style: Some(RuntimeString::from_utf16(vec![0xd800, 0xdc00])),
        ..Default::default()
    };
    validate_serializable_session_init(&exact_names)
        .expect("extension names retain exact UTF-16 identity during validation");

    let zero_timeout = ProtocolSessionInit {
        command_hooks: vec![ProtocolCommandHook {
            event: HookEvent::PreToolUse,
            matcher: None,
            command: "check".into(),
            timeout_ms: Some(0),
            is_async: false,
        }],
        ..Default::default()
    };
    assert!(validate_serializable_session_init(&zero_timeout)
        .unwrap_err()
        .contains("greater than zero"));

    let invalid_rate = ProtocolSessionInit {
        pricing: Some(vec![ProtocolModelPricing {
            model: "fake-model".into(),
            pricing: ModelPricing {
                input_per_m_tok: -1.0,
                output_per_m_tok: 0.0,
                cache_read_per_m_tok: 0.0,
                cache_write_per_m_tok: 0.0,
            },
        }]),
        ..Default::default()
    };
    assert!(validate_serializable_session_init(&invalid_rate)
        .unwrap_err()
        .contains("invalid inputPerMTok"));

    let callback_shaped = json!({
        "commandHooks": [{
            "event": "PreToolUse",
            "command": "check",
            "handler": { "type": "callback", "id": "host-callback" }
        }]
    });
    assert!(runtime_codec::decode_value::<ProtocolSessionInit>(
        &runtime_json_from_scalar_value(&callback_shaped)
            .expect("scalar invalid session init fixture")
    )
    .is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn endpoint_shutdown_declines_a_pending_mcp_authorization() {
    let agent = make_agent(".", vec![text_turn("unused")], None);
    let (response_tx, response_rx) = oneshot::channel();
    let response_tx = Arc::new(StdMutex::new(Some(response_tx)));
    let create_session: CreateSession = Arc::new(move |args: CreateSessionArgs| {
        let agent = agent.clone();
        let response_tx = response_tx.clone();
        Box::pin(async move {
            let response = args
                .mcp_authorize_handler
                .authorize(
                    McpAuthorizeRequest {
                        server_name: "docs".to_string(),
                        authorization_url: "https://auth.example/authorize?state=pending"
                            .to_string(),
                        redirect_uri: "http://127.0.0.1:33418/callback".to_string(),
                    },
                    AbortSignal::new(),
                )
                .await;
            if let Some(tx) = response_tx.lock().unwrap().take() {
                let _ = tx.send(response);
            }
            Ok::<Arc<Session>, BoxError>(agent.start_session(StartSessionOptions::default()))
        }) as CreateSessionFuture
    });
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(create_session, sr, sw);
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });
    let mut events = client.events();
    client.run(Input::Text("connect".into()), None).await;
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        while let Some(event) = events.next().await {
            if event.mcp_authorization.is_some() {
                break;
            }
        }
    })
    .await
    .expect("authorization event");

    server.stop();
    let response = timeout(CROSS_TASK_LIVENESS_TIMEOUT, response_rx)
        .await
        .expect("handler released")
        .expect("handler response");
    assert_eq!(
        response,
        McpAuthorizeResponse::Decline {
            reason: Some("server stopped".to_string())
        }
    );
    done.await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_op_stops_the_server() {
    let agent = make_agent(".", vec![text_turn("hi")], None);
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (_server, done) = start_server(make_create_session(agent, None), sr, sw);
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        let mut events = client.events();
        client.run(Input::Text("hi".into()), None).await;
        while let Some(evt) = events.next().await {
            if is_completed(&evt) {
                break;
            }
        }
        let completion = client.shutdown().await.expect("shutdown completion");
        assert_eq!(
            completion,
            ShutdownCompletion {
                ok: true,
                flush_completed: true,
                cleanup_completed: true,
                timed_out: false,
                error: None,
            }
        );
    })
    .await
    .expect("timed out");

    // The shutdown op should stop the server (its start() future resolves).
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, done)
        .await
        .expect("server did not stop")
        .ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_drains_maintenance_terminal_before_correlated_ack_and_eof() {
    let started = Arc::new(Notify::new());
    let cancelled = Arc::new(Notify::new());
    let release_turn = Arc::new(Notify::new());
    let hook_entered = Arc::new(Notify::new());
    let release_hook = Arc::new(Notify::new());
    let agent = make_gated_teardown_agent(
        ".",
        started.clone(),
        cancelled.clone(),
        release_turn.clone(),
        hook_entered.clone(),
        release_hook.clone(),
    );
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(make_create_session(agent, None), sr, sw);
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });
    let mut events = client.events();

    client.run(Input::Text("block".into()), None).await;
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, started.notified())
        .await
        .expect("turn started");
    let shutdown_client = client.clone();
    let mut shutdown = tokio::spawn(async move { shutdown_client.shutdown().await });
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, cancelled.notified())
        .await
        .expect("turn observed shutdown cancellation");
    release_turn.notify_one();
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, hook_entered.notified())
        .await
        .expect("core cleanup entered SessionEnd hook");
    assert!(
        timeout(Duration::from_millis(50), &mut shutdown)
            .await
            .is_err(),
        "shutdown ACK was emitted before core cleanup completed"
    );

    let terminal = MemoryMaintenanceEvent::AutoMemory {
        run: AutoMemoryRun {
            run_id: "memory-run-shutdown".into(),
            session_id: "session-shutdown".into(),
            trigger: MemoryMaintenanceTrigger::Auto,
            scope: AgentMemoryScope::Project,
            target_dir: "/workspace/.klaus/memory".into(),
            status: MemoryMaintenanceStatus::Failed,
            started_at: 10,
            ended_at: Some(20),
            reason: Some(AutoMemoryRunReason::Shutdown),
            error: Some("memory extraction stopped during shutdown".into()),
            message_count: 1,
            cursor_message_uuid: None,
            files_touched: Vec::new(),
            turn_count: Some(0),
        },
    };
    server
        .inner
        .unsolicited_tx
        .lock()
        .unwrap()
        .as_ref()
        .expect("maintenance sink remains open during core shutdown")
        .send(UnsolicitedWrite::Event(ProtocolEvent {
            submission_id: RuntimeString::default(),
            memory_maintenance_event: Some(terminal.clone()),
            ..Default::default()
        }))
        .expect("terminal maintenance event accepted");
    release_hook.notify_one();

    let completion = timeout(CROSS_TASK_LIVENESS_TIMEOUT, shutdown)
        .await
        .expect("shutdown completion timed out")
        .expect("shutdown task panicked")
        .expect("shutdown query failed");
    assert!(completion.ok);
    assert!(completion.flush_completed);
    assert!(completion.cleanup_completed);
    assert!(!completion.timed_out);

    let received = timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        let mut received = Vec::new();
        while let Some(event) = events.next().await {
            received.push(event);
        }
        received
    })
    .await
    .expect("protocol EOF did not follow shutdown ACK");
    let terminal_index = received
        .iter()
        .position(|event| event.memory_maintenance_event.as_ref() == Some(&terminal))
        .expect("terminal maintenance lifecycle event");
    let ack_index = received
        .iter()
        .position(|event| event.shutdown_completed.as_deref() == Some(&completion))
        .expect("correlated shutdown completion");
    assert!(
        terminal_index < ack_index,
        "maintenance terminal must precede shutdown completion"
    );
    assert!(!received[ack_index].submission_id.is_empty());
    assert_eq!(ack_index + 1, received.len(), "shutdown ACK must be final");
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, done)
        .await
        .expect("server did not close after shutdown ACK")
        .expect("server task panicked");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_and_wait_drains_the_op_chain_and_session_before_all_callers_return() {
    let started = Arc::new(Notify::new());
    let cancelled = Arc::new(Notify::new());
    let release_turn = Arc::new(Notify::new());
    let hook_entered = Arc::new(Notify::new());
    let release_hook = Arc::new(Notify::new());
    let agent = make_gated_teardown_agent(
        ".",
        started.clone(),
        cancelled.clone(),
        release_turn.clone(),
        hook_entered.clone(),
        release_hook.clone(),
    );
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, mut done) = start_server(make_create_session(agent, None), sr, sw);
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    client.run(Input::Text("block".into()), None).await;
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, started.notified())
        .await
        .expect("turn started");

    let first_server = server.clone();
    let mut first = tokio::spawn(async move { first_server.shutdown_and_wait().await });
    let second_server = server.clone();
    let mut second = tokio::spawn(async move { second_server.shutdown_and_wait().await });

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, cancelled.notified())
        .await
        .expect("active turn observed cancellation");
    assert!(
        timeout(Duration::from_millis(50), &mut first)
            .await
            .is_err(),
        "shutdown returned before the cancelled op-chain job settled"
    );
    assert!(
        timeout(Duration::from_millis(50), &mut done).await.is_err(),
        "ProtocolServer::start returned before the op-chain settled"
    );

    release_turn.notify_one();
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, hook_entered.notified())
        .await
        .expect("Session::shutdown_and_wait reached SessionEnd teardown");
    assert!(
        timeout(Duration::from_millis(50), &mut first)
            .await
            .is_err(),
        "first shutdown caller did not await Session teardown"
    );
    assert!(
        timeout(Duration::from_millis(50), &mut second)
            .await
            .is_err(),
        "concurrent shutdown caller returned before shared teardown completed"
    );

    release_hook.notify_one();
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, first)
        .await
        .expect("first shutdown timed out")
        .expect("first shutdown task panicked")
        .expect("first shutdown failed");
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, second)
        .await
        .expect("second shutdown timed out")
        .expect("second shutdown task panicked")
        .expect("second shutdown failed");
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, done)
        .await
        .expect("ProtocolServer::start did not await teardown")
        .expect("ProtocolServer::start task panicked");
    assert!(server.inner.session.get().unwrap().is_disposed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_first_server_shutdown_caller_does_not_cancel_endpoint_teardown() {
    let started = Arc::new(Notify::new());
    let cancelled = Arc::new(Notify::new());
    let release_turn = Arc::new(Notify::new());
    let hook_entered = Arc::new(Notify::new());
    let release_hook = Arc::new(Notify::new());
    let agent = make_gated_teardown_agent(
        ".",
        started.clone(),
        cancelled.clone(),
        release_turn.clone(),
        hook_entered.clone(),
        release_hook.clone(),
    );
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(make_create_session(agent, None), sr, sw);
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    client.run(Input::Text("block".into()), None).await;
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, started.notified())
        .await
        .expect("turn started");
    let first_server = server.clone();
    let first = tokio::spawn(async move { first_server.shutdown_and_wait().await });
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, cancelled.notified())
        .await
        .expect("active turn observed cancellation");
    release_turn.notify_one();
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, hook_entered.notified())
        .await
        .expect("endpoint teardown entered SessionEnd hook");

    first.abort();
    let _ = first.await;
    let second_server = server.clone();
    let mut second = tokio::spawn(async move { second_server.shutdown_and_wait().await });
    assert!(
        timeout(Duration::from_millis(50), &mut second)
            .await
            .is_err(),
        "retry returned before the endpoint-owned teardown finished"
    );

    release_hook.notify_one();
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, second)
        .await
        .expect("retry shutdown timed out")
        .expect("retry shutdown task panicked")
        .expect("retry shutdown failed");
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, done)
        .await
        .expect("ProtocolServer::start did not await retained teardown")
        .expect("ProtocolServer::start task panicked");
    assert!(server.inner.session.get().unwrap().is_disposed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_waits_for_immediate_session_creation_and_rejects_the_late_session() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let created: Arc<Mutex<Option<Arc<Session>>>> = Arc::new(Mutex::new(None));
    let base = make_create_session(make_agent(".", vec![text_turn("unused")], None), None);
    let create_session: CreateSession = {
        let entered = entered.clone();
        let release = release.clone();
        let created = created.clone();
        Arc::new(move |args| {
            let entered = entered.clone();
            let release = release.clone();
            let created = created.clone();
            let base = base.clone();
            Box::pin(async move {
                entered.notify_one();
                release.notified().await;
                let session = base(args).await?;
                *created.lock().unwrap() = Some(session.clone());
                Ok(session)
            }) as CreateSessionFuture
        })
    };
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(create_session, sr, sw);
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });
    let request = tokio::spawn(async move { client.get_history().await });
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, entered.notified())
        .await
        .expect("immediate create_session was entered");

    let closing_server = server.clone();
    let mut close = tokio::spawn(async move { closing_server.shutdown_and_wait().await });
    assert!(
        timeout(Duration::from_millis(50), &mut close)
            .await
            .is_err(),
        "shutdown missed the active immediate dispatch"
    );

    release.notify_one();
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, close)
        .await
        .expect("shutdown timed out")
        .expect("shutdown task panicked")
        .expect("shutdown failed");
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, done)
        .await
        .expect("start did not join immediate dispatch")
        .expect("start task panicked");
    assert!(
        server.inner.session.get().is_none(),
        "a session created after admission closed was published"
    );
    assert!(
        created
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|session| session.is_disposed()),
        "the rejected late session was not fully disposed"
    );
    request.abort();
    let _ = request.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_start_waits_for_the_owner_and_owner_drop_finishes_shared_teardown() {
    let agent = make_agent(".", vec![text_turn("unused")], None);
    let ((sr, sw), (_cr, _cw)) = duplex_pair();
    let server = Arc::new(ProtocolServer::new(ProtocolServerOptions {
        create_session: make_create_session(agent, None),
        lifecycle: None,
        plugin_admin: None,
        credential_store: None,
        input: sr,
        output: sw,
    }));

    let first_server = server.clone();
    let first = tokio::spawn(async move { first_server.start().await });
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        while *server.start_state.lock().unwrap() != StartState::Running {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first start did not claim ownership");

    let second_server = server.clone();
    let mut second = tokio::spawn(async move { second_server.start().await });
    assert!(
        timeout(Duration::from_millis(50), &mut second)
            .await
            .is_err(),
        "a concurrent start returned while the owner was still serving"
    );

    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, second)
        .await
        .expect("owner-drop teardown timed out")
        .expect("concurrent start task panicked");
    assert_eq!(*server.start_state.lock().unwrap(), StartState::Finished);
    assert!(server.shutdown_result.get().is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persists_the_session_across_runs_and_rewinds_the_conversation() {
    let agent = make_agent(".", vec![text_turn("one"), text_turn("two")], None);
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(make_create_session(agent, None), sr, sw);
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    let (completed, rewound) = timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        let mut events = client.events();
        client.run(Input::Text("first".into()), None).await;
        let mut completed = 0;
        let mut rewound = None;
        while let Some(evt) = events.next().await {
            if let Some(r) = &evt.rewound {
                rewound = Some(r.clone());
                break;
            }
            if is_completed(&evt) {
                completed += 1;
                if completed == 1 {
                    client.run(Input::Text("second".into()), None).await;
                } else if completed == 2 {
                    client.rewind(1, Some(RewindScope::Conversation)).await;
                }
            }
        }
        (completed, rewound)
    })
    .await
    .expect("timed out");

    server.stop();
    done.await.ok();

    assert_eq!(completed, 2);
    let r = rewound.expect("a rewound outcome");
    assert_eq!(r.scope, RewindScope::Conversation);
    assert_eq!(r.dropped_turns, 1);
    assert_eq!(r.remaining_turns, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rewind_before_any_run_reports_no_session_default_scope_both() {
    let agent = make_agent(".", vec![], None);
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(make_create_session(agent, None), sr, sw);
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    let rewound = timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        let mut events = client.events();
        client.rewind(1, None).await;
        let mut rewound = None;
        while let Some(evt) = events.next().await {
            if let Some(r) = &evt.rewound {
                rewound = Some(r.clone());
                break;
            }
        }
        rewound
    })
    .await
    .expect("timed out");

    server.stop();
    done.await.ok();
    let r = rewound.expect("a rewound outcome");
    assert_eq!(r.scope, RewindScope::Both);
    assert!(r.error.unwrap().contains("no session"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rewinds_files_to_the_pre_turn_checkpoint() {
    let dir = unique_temp_dir("rewind");
    let agent = make_agent(
        dir.to_str().unwrap(),
        vec![
            tool_use_turn("w1", "write", json!({ "path": "f.txt", "content": "hi" })),
            text_turn("wrote it"),
        ],
        Some(json!({ "checkpointing": true })),
    );
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    let rewound = timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        let mut events = client.events();
        client.run(Input::Text("write a file".into()), None).await;
        let mut rewound = None;
        while let Some(evt) = events.next().await {
            if let Some(r) = &evt.rewound {
                rewound = Some(r.clone());
                break;
            }
            if is_completed(&evt) {
                client.rewind(1, Some(RewindScope::Files)).await;
            }
        }
        rewound
    })
    .await
    .expect("timed out");

    server.stop();
    done.await.ok();

    let r = rewound.expect("a rewound outcome");
    assert_eq!(r.scope, RewindScope::Files);
    assert!(r.files_restored);
    // The file written during the turn is gone after restoring the pre-turn
    // checkpoint.
    assert!(std::fs::read_to_string(dir.join("f.txt")).is_err());
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn previews_the_file_rewind_without_touching_the_tree() {
    let dir = unique_temp_dir("rewind-preview");
    let agent = make_agent(
        dir.to_str().unwrap(),
        vec![
            tool_use_turn("w1", "write", json!({ "path": "f.txt", "content": "hi" })),
            text_turn("wrote it"),
        ],
        Some(json!({ "checkpointing": true })),
    );
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    let preview = timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        let mut events = client.events();
        client.run(Input::Text("write a file".into()), None).await;
        let mut preview = None;
        while let Some(evt) = events.next().await {
            if let Some(p) = &evt.rewind_preview {
                preview = Some(p.clone());
                break;
            }
            if is_completed(&evt) {
                client.rewind_preview(1).await;
            }
        }
        preview
    })
    .await
    .expect("timed out");

    server.stop();
    done.await.ok();

    let p = preview.expect("a rewind preview");
    assert!(p.can_rewind);
    assert_eq!(p.dropped_turns, 1);
    // It names the file the turn wrote, because that is what the restore would
    // take back out.
    assert!(
        p.files.contains(&RuntimeString::from("f.txt")),
        "expected f.txt in {:?}",
        p.files
    );
    assert_eq!(p.error, None);
    // And the whole point: the file is STILL THERE. A preview that restored
    // anything would be a rewind with a different name.
    assert_eq!(
        std::fs::read_to_string(dir.join("f.txt")).ok().as_deref(),
        Some("hi")
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serializes_overlapping_run_ops() {
    let agent = make_agent(".", vec![text_turn("A"), text_turn("B")], None);
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(make_create_session(agent, None), sr, sw);
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    let order = timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        let mut events = client.events();
        // Fire both runs back-to-back — they must NOT overlap on the session.
        client.run(Input::Text("A".into()), None).await;
        client.run(Input::Text("B".into()), None).await;
        let mut order: Vec<&'static str> = Vec::new();
        let mut completed = 0;
        while let Some(evt) = events.next().await {
            match &evt.event {
                Some(EngineEvent::TurnStarted { .. }) => order.push("start"),
                Some(EngineEvent::TurnCompleted { .. }) => {
                    order.push("done");
                    completed += 1;
                    if completed == 2 {
                        break;
                    }
                }
                _ => {}
            }
        }
        order
    })
    .await
    .expect("timed out");

    server.stop();
    done.await.ok();
    assert_eq!(order, vec!["start", "done", "start", "done"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_to_completion_returns_final_text_and_parses_structured_output() {
    let agent = make_agent(".", vec![text_turn("{\"answer\":42}")], None);
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    let result = timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        client
            .run_to_completion(
                Input::Text("give json".into()),
                Some(RunOpOptions {
                    output_schema: Some(runtime_value(r#"{"type":"object"}"#)),
                    ..Default::default()
                }),
            )
            .await
    })
    .await
    .expect("timed out")
    .expect("a result");

    assert_eq!(result.final_text, "{\"answer\":42}");
    assert_eq!(
        result.structured_output,
        Some(runtime_value(r#"{"answer":42}"#))
    );
    assert_eq!(result.stop_reason, TerminalReason::Completed);

    server.stop();
    done.await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_to_completion_parses_lone_surrogate_structured_output() {
    let final_text = r#"{"answer":"\ud800"}"#;
    let agent = make_agent(".", vec![text_turn(final_text)], None);
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    let result = timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        client
            .run_to_completion(
                Input::Text("give json".into()),
                Some(RunOpOptions {
                    output_schema: Some(runtime_value(r#"{"type":"object"}"#)),
                    ..Default::default()
                }),
            )
            .await
    })
    .await
    .expect("timed out")
    .expect("a result");

    assert_eq!(result.final_text, final_text);
    assert_eq!(
        result.structured_output,
        Some(runtime_value(r#"{"answer":"\ud800"}"#))
    );

    server.stop();
    done.await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn steer_enqueues_and_drains() {
    let agent = make_agent(".", vec![text_turn("ok")], None);
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        client
            .steer(
                Input::Text("steer me".into()),
                Some(crate::types::SteerPriority::Now),
            )
            .await;
        let queued = client.get_queued().await.expect("queued");
        assert_eq!(queued.len(), 1);
        assert_eq!(
            queued[0].value,
            klaus_agent_core::engine::message_queue::QueuedValue::Text("steer me".into())
        );
        assert_eq!(
            queued[0].priority,
            Some(klaus_agent_core::engine::message_queue::QueuePriority::Now)
        );

        let pending = client.take_pending().await.expect("pending");
        assert_eq!(pending.len(), 1);
        assert_eq!(client.get_queued().await.expect("queued").len(), 0);
    })
    .await
    .expect("timed out");

    server.stop();
    done.await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_mutations_are_ordered_cas_idempotent_and_reconnect_queryable() {
    use klaus_agent_core::engine::message_queue::{QueueEdit, QueueMutationStatus, QueuedValue};

    let agent = make_agent(".", vec![text_turn("ok")], None);
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });
    let mut events = client.events();

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        client
            .steer(
                Input::Text("first".into()),
                Some(crate::types::SteerPriority::Next),
            )
            .await;
        client
            .steer(
                Input::Text("second".into()),
                Some(crate::types::SteerPriority::Next),
            )
            .await;

        let initial = client.get_queue_snapshot().await.expect("initial snapshot");
        assert_eq!(initial.revision, 2);
        assert_eq!(initial.items.len(), 2);
        let context = client.context_snapshot().await.expect("context");
        assert!(initial.items.iter().all(|item| {
            item.message.uuid.is_some()
                && item.message.session_id.as_ref() == Some(&context.thread_id)
                && item.message.agent_id.is_none()
        }));
        let first = initial.items[0].message.uuid.clone().unwrap();
        let second = initial.items[1].message.uuid.clone().unwrap();

        let edited = client
            .edit_queued(
                "queue-edit-1",
                first.clone(),
                initial.revision,
                QueueEdit {
                    value: Some(QueuedValue::Text("edited".into())),
                    ..Default::default()
                },
            )
            .await
            .expect("edit");
        assert_eq!(edited.status, QueueMutationStatus::Applied);
        assert_eq!(edited.snapshot.revision, 3);

        let duplicate = client
            .edit_queued(
                "queue-edit-1",
                first.clone(),
                initial.revision,
                QueueEdit {
                    value: Some(QueuedValue::Text("edited".into())),
                    ..Default::default()
                },
            )
            .await
            .expect("idempotent edit replay");
        assert_eq!(duplicate, edited);

        let stale = client
            .remove_queued("queue-remove-stale", second.clone(), initial.revision)
            .await
            .expect("stale remove result");
        assert_eq!(stale.status, QueueMutationStatus::StaleRevision);
        assert_eq!(stale.snapshot.revision, 3);

        let reordered = client
            .reorder_queued(
                "queue-reorder-1",
                second.clone(),
                Some(first.clone()),
                edited.snapshot.revision,
            )
            .await
            .expect("reorder");
        assert_eq!(reordered.status, QueueMutationStatus::Applied);
        assert_eq!(reordered.snapshot.revision, 4);

        let removed = client
            .remove_queued("queue-remove-1", first, reordered.snapshot.revision)
            .await
            .expect("remove");
        assert_eq!(removed.status, QueueMutationStatus::Applied);
        assert_eq!(removed.snapshot.revision, 5);

        // A Host that missed every live event can recover the same
        // authoritative state without replaying local UI mutations.
        let recovered = client
            .get_queue_snapshot()
            .await
            .expect("recovered snapshot");
        assert_eq!(recovered, removed.snapshot);
        assert_eq!(recovered.items[0].message.uuid.as_ref(), Some(&second));

        let taken = client.take_pending().await.expect("take pending");
        assert_eq!(taken.len(), 1);
        let after_take = client.get_queue_snapshot().await.expect("after take");
        assert_eq!(after_take.revision, 6);
        assert!(after_take.items.is_empty());
        let taken_tombstone = client
            .remove_queued("queue-remove-after-take", second, after_take.revision)
            .await
            .expect("taken tombstone");
        assert_eq!(taken_tombstone.status, QueueMutationStatus::Missing);
        assert_eq!(
            taken_tombstone.terminal_state,
            Some(klaus_agent_core::engine::message_queue::QueueTerminalState::Taken)
        );

        let mut revisions = Vec::new();
        while revisions.len() < 6 {
            let event = events
                .next()
                .await
                .expect("queue event stream remains open");
            if event.submission_id.is_empty() {
                if let Some(snapshot) = &event.queue_snapshot {
                    revisions.push(snapshot.revision);
                }
            }
        }
        assert_eq!(revisions, vec![1, 2, 3, 4, 5, 6]);
    })
    .await
    .expect("queue protocol test timed out");

    server.stop();
    done.await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_mutation_result_precedes_buffered_revisions_and_concurrent_snapshot_query() {
    use klaus_agent_core::engine::message_queue::{QueueEdit, QueueMutationStatus, QueuedValue};

    #[derive(Debug)]
    enum QueueWireObservation {
        Mutation {
            mutation_id: RuntimeString,
            revision: u64,
        },
        Snapshot {
            revision: u64,
            unsolicited: bool,
        },
    }

    let dir = unique_temp_dir("queue-publication-barrier");
    let gate = Arc::new(QueueFlushGate::default());
    let store = Arc::new(GatedQueueStore {
        inner: Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(dir.to_string_lossy().into_owned()),
            archive_dir: None,
        })),
        gate: gate.clone(),
    });
    let session_slot = Arc::new(Mutex::new(None::<Arc<Session>>));
    let agent = make_agent(".", vec![text_turn("unused")], None);
    let create_session: CreateSession = {
        let store = store.clone();
        let session_slot = session_slot.clone();
        Arc::new(move |_args: CreateSessionArgs| {
            let agent = agent.clone();
            let store = store.clone();
            let session_slot = session_slot.clone();
            Box::pin(async move {
                let session = agent
                    .open_session(
                        store,
                        StartSessionOptions {
                            permission_mode: Some(ExternalPermissionMode::BypassPermissions),
                            ..Default::default()
                        },
                    )
                    .await?;
                *session_slot.lock().unwrap() = Some(session.clone());
                Ok::<Arc<Session>, BoxError>(session)
            }) as CreateSessionFuture
        })
    };

    let ((server_r, server_w), (client_r, client_w)) = duplex_pair();
    let wire_bytes = Arc::new(Mutex::new(Vec::new()));
    let recording_writer: Box<dyn AsyncWrite + Unpin + Send> = Box::new(RecordingWriter {
        inner: server_w,
        bytes: wire_bytes.clone(),
    });
    let (server, done) = start_server(create_session, server_r, recording_writer);
    let client = Arc::new(ProtocolClient::new(ProtocolClientOptions {
        input: client_r,
        output: client_w,
    }));
    let mut events = client.events();

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
            client
                .steer(
                    Input::Text("first".into()),
                    Some(crate::types::SteerPriority::Next),
                )
                .await;
            let initial = client
                .get_queue_snapshot()
                .await
                .expect("initial queue snapshot");
            assert_eq!(initial.revision, 1);
            let first_uuid = initial.items[0].message.uuid.clone().unwrap();
            let session = session_slot
                .lock()
                .unwrap()
                .clone()
                .expect("protocol session was captured");
            session
                .flush_rollout()
                .await
                .expect("initial queue checkpoint flush");
            gate.wait_for_completed_queue_flushes(1).await;

            // Consume the initial unsolicited revision before taking the raw
            // wire marker, so every later queue frame belongs to the blocked
            // mutation epoch under test.
            loop {
                let event = events.next().await.expect("queue event stream remains open");
                if event.submission_id.is_empty()
                    && event
                        .queue_snapshot
                        .as_ref()
                        .is_some_and(|snapshot| snapshot.revision == 1)
                {
                    break;
                }
            }
            let wire_marker = wire_bytes.lock().unwrap().len();

            gate.arm_next_queue_flush();
            let edit_client = client.clone();
            let mut edit_task = tokio::spawn(async move {
                edit_client
                    .edit_queued(
                        "blocked-edit",
                        first_uuid,
                        1,
                        QueueEdit {
                            value: Some(QueuedValue::Text("edited".into())),
                            ..Default::default()
                        },
                    )
                    .await
            });
            gate.wait_until_blocked().await;

            session
                .enqueue(Input::Text("higher revision".into()), EnqueueOptions::default())
                .expect("concurrent core enqueue");
            assert_eq!(
                session.queue_snapshot().revision,
                1,
                "a revision awaiting durability must remain invisible to public readers"
            );

            let snapshot_client = client.clone();
            let mut snapshot_task = tokio::spawn(async move {
                snapshot_client.get_queue_snapshot().await
            });
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
            assert!(
                !edit_task.is_finished(),
                "mutation result must wait for its exact durable receipt"
            );
            assert!(
                !snapshot_task.is_finished(),
                "the concurrent snapshot query must remain behind the mutation publication epoch"
            );
            assert_eq!(
                wire_bytes.lock().unwrap().len(),
                wire_marker,
                "no queue frame may escape while the mutation durability receipt is blocked"
            );

            gate.release();
            let edited = (&mut edit_task)
                .await
                .expect("edit task join")
                .expect("edit result");
            assert_eq!(edited.status, QueueMutationStatus::Applied);
            assert_eq!(edited.snapshot.revision, 2);
            let queried = (&mut snapshot_task)
                .await
                .expect("snapshot task join")
                .expect("snapshot result");
            assert_eq!(queried.revision, 3);

            let mut event_revisions = Vec::new();
            while event_revisions.len() < 2 {
                let event = events.next().await.expect("queue event stream remains open");
                if event.submission_id.is_empty() {
                    if let Some(snapshot) = &event.queue_snapshot {
                        if snapshot.revision >= 2 {
                            event_revisions.push(snapshot.revision);
                        }
                    }
                }
            }
            assert_eq!(event_revisions, vec![2, 3]);
            session
                .flush_rollout()
                .await
                .expect("higher queue revision flush");
            gate.wait_for_completed_queue_flushes(3).await;

            let raw = {
                let bytes = wire_bytes.lock().unwrap();
                String::from_utf8(bytes[wire_marker..].to_vec()).expect("protocol output is UTF-8")
            };
            let mut observations = Vec::new();
            for line in raw.lines().filter(|line| !line.is_empty()) {
                let event: ProtocolEvent =
                    runtime_codec::decode_text(&RuntimeString::from_utf8(line))
                        .expect("recorded protocol event decodes");
                if let Some(result) = event.queue_mutation {
                    observations.push(QueueWireObservation::Mutation {
                        mutation_id: result.mutation_id,
                        revision: result.snapshot.revision,
                    });
                }
                if let Some(snapshot) = event.queue_snapshot {
                    observations.push(QueueWireObservation::Snapshot {
                        revision: snapshot.revision,
                        unsolicited: event.submission_id.is_empty(),
                    });
                }
            }

            assert!(
                matches!(
                    observations.first(),
                    Some(QueueWireObservation::Mutation { mutation_id, revision: 2 })
                        if mutation_id == &RuntimeString::from("blocked-edit")
                ),
                "the correlated revision-2 mutation result must be the first queue frame: {observations:?}"
            );
            let event_two = observations
                .iter()
                .position(|observation| matches!(observation, QueueWireObservation::Snapshot { revision: 2, unsolicited: true }))
                .expect("buffered revision-2 event");
            let event_three = observations
                .iter()
                .position(|observation| matches!(observation, QueueWireObservation::Snapshot { revision: 3, unsolicited: true }))
                .expect("buffered revision-3 event");
            let query_three = observations
                .iter()
                .position(|observation| matches!(observation, QueueWireObservation::Snapshot { revision: 3, unsolicited: false }))
                .expect("correlated revision-3 snapshot query result");
            assert!(
                event_two < event_three && event_three < query_three,
                "wire FIFO must be result N, event N, event N+1, then query N+1: {observations:?}"
            );

            let visible_revisions = observations
                .iter()
                .map(|observation| match observation {
                    QueueWireObservation::Mutation { revision, .. }
                    | QueueWireObservation::Snapshot { revision, .. } => *revision,
                })
                .collect::<Vec<_>>();
            assert!(
                visible_revisions
                    .windows(2)
                    .all(|pair| pair[0] <= pair[1]),
                "queue revisions must never move backward on the wire: {observations:?}"
            );
        })
        .await
        .expect("queue publication barrier test timed out");

    let completion = client.shutdown().await.expect("shutdown");
    assert!(completion.ok, "{completion:?}");
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, done)
        .await
        .expect("protocol server did not stop")
        .ok();
    server.stop();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_flush_failure_is_terminal_explicit_and_recovers_last_durable_truth() {
    use klaus_agent_core::engine::message_queue::{
        QueueEdit, QueueMutationStatus, QueueSnapshotStatus, QueuedValue,
    };

    let dir = unique_temp_dir("queue-flush-failure");
    let inner_store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let gate = Arc::new(QueueFlushGate::default());
    let gated_store = Arc::new(GatedQueueStore {
        inner: inner_store.clone(),
        gate: gate.clone(),
    });
    let agent = make_agent(".", vec![text_turn("unused")], None);
    let create_session: CreateSession = {
        let agent = agent.clone();
        let store = gated_store.clone();
        Arc::new(move |_args: CreateSessionArgs| {
            let agent = agent.clone();
            let store = store.clone();
            Box::pin(async move {
                agent
                    .open_session(
                        store,
                        StartSessionOptions {
                            permission_mode: Some(ExternalPermissionMode::BypassPermissions),
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(Into::into)
            }) as CreateSessionFuture
        })
    };

    let ((server_r, server_w), (client_r, client_w)) = duplex_pair();
    let wire_bytes = Arc::new(Mutex::new(Vec::new()));
    let recording_writer: Box<dyn AsyncWrite + Unpin + Send> = Box::new(RecordingWriter {
        inner: server_w,
        bytes: wire_bytes.clone(),
    });
    let (server, done) = start_server(create_session, server_r, recording_writer);
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: client_r,
        output: client_w,
    });

    let (thread_id, durable_snapshot, failed_snapshot) =
        timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
            client
                .steer(
                    Input::Text("durable original".into()),
                    Some(crate::types::SteerPriority::Next),
                )
                .await;
            let durable = client
                .get_queue_snapshot()
                .await
                .expect("initial checkpoint becomes durable");
            assert_eq!(durable.revision, 1);
            assert_eq!(durable.status, QueueSnapshotStatus::Authoritative);
            assert_eq!(durable.error, None);
            let uuid = durable.items[0].message.uuid.clone().unwrap();
            let thread_id = client
                .context_snapshot()
                .await
                .expect("session context")
                .thread_id;

            gate.fail_next_queue_flush();
            let failed = client
                .edit_queued(
                    "failed-edit",
                    uuid.clone(),
                    durable.revision,
                    QueueEdit {
                        value: Some(QueuedValue::Text("speculative edit".into())),
                        ..Default::default()
                    },
                )
                .await
                .expect("failed durability still has a terminal mutation result");
            assert_eq!(failed.status, QueueMutationStatus::OutcomeUnknown);
            assert_eq!(failed.snapshot.revision, durable.revision);
            assert_eq!(
                failed.snapshot.status,
                QueueSnapshotStatus::PersistenceFailed
            );
            assert_eq!(
                failed.snapshot.items[0].message.value,
                QueuedValue::Text("durable original".into())
            );
            assert!(failed
                .error
                .as_ref()
                .is_some_and(|error| error == "forced queue checkpoint flush failure"));

            let later = client
                .remove_queued("after-poison", uuid, durable.revision)
                .await
                .expect("poisoned mutation has an explicit terminal result");
            assert_eq!(later.status, QueueMutationStatus::PersistenceFailed);
            assert_eq!(later.snapshot, failed.snapshot);

            let queued_error = client
                .get_queued()
                .await
                .expect_err("get_queued must reject a poisoned queue");
            assert!(matches!(
                queued_error,
                ClientError::Op(message) if message == "forced queue checkpoint flush failure"
            ));
            let take_error = client
                .take_pending()
                .await
                .expect_err("take_pending must reject a poisoned queue");
            assert!(matches!(
                take_error,
                ClientError::Op(message) if message == "forced queue checkpoint flush failure"
            ));
            let diagnostic = client
                .get_queue_snapshot()
                .await
                .expect("degraded snapshot remains queryable");
            assert_eq!(diagnostic, failed.snapshot);

            (thread_id, durable, diagnostic)
        })
        .await
        .expect("queue flush failure protocol test timed out");

    server.stop();
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, done)
        .await
        .expect("failed queue endpoint did not stop")
        .ok();

    let first_wire = {
        let bytes = wire_bytes.lock().unwrap();
        String::from_utf8(bytes.clone()).expect("protocol output is UTF-8")
    };
    let mut terminal_snapshots = Vec::new();
    let mut visible_revisions = Vec::new();
    for line in first_wire.lines().filter(|line| !line.is_empty()) {
        let event: ProtocolEvent = runtime_codec::decode_text(&RuntimeString::from_utf8(line))
            .expect("protocol event decodes");
        if let Some(snapshot) = event.queue_snapshot {
            visible_revisions.push(snapshot.revision);
            if event.submission_id.is_empty()
                && snapshot.status == QueueSnapshotStatus::PersistenceFailed
            {
                terminal_snapshots.push(snapshot);
            }
        }
        if let Some(result) = event.queue_mutation {
            visible_revisions.push(result.snapshot.revision);
        }
    }
    assert_eq!(
        terminal_snapshots,
        vec![failed_snapshot.clone()],
        "the first failed receipt publishes exactly one terminal queue event"
    );
    assert!(
        visible_revisions
            .iter()
            .all(|revision| *revision <= durable_snapshot.revision),
        "the speculative failed revision must never reach the wire: {visible_revisions:?}"
    );

    let thread_id_host = thread_id.try_to_utf8().expect("scalar thread id");
    let create_resume: CreateSession = {
        let agent = agent.clone();
        let store = inner_store.clone();
        Arc::new(move |_args: CreateSessionArgs| {
            let agent = agent.clone();
            let store = store.clone();
            let thread_id = thread_id_host.clone();
            Box::pin(async move {
                agent
                    .resume_session(store, &thread_id, StartSessionOptions::default())
                    .await
                    .map_err(Into::into)
            }) as CreateSessionFuture
        })
    };
    let ((resume_sr, resume_sw), (resume_cr, resume_cw)) = duplex_pair();
    let (resume_server, resume_done) = start_server(create_resume, resume_sr, resume_sw);
    let resume_client = ProtocolClient::new(ProtocolClientOptions {
        input: resume_cr,
        output: resume_cw,
    });
    let recovered = timeout(
        CROSS_TASK_LIVENESS_TIMEOUT,
        resume_client.get_queue_snapshot(),
    )
    .await
    .expect("cold resume query timed out")
    .expect("cold resume queue snapshot");
    assert_eq!(recovered, durable_snapshot);
    assert_eq!(recovered.status, QueueSnapshotStatus::Authoritative);
    assert_eq!(
        recovered.items[0].message.value,
        QueuedValue::Text("durable original".into())
    );

    resume_server.stop();
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, resume_done)
        .await
        .expect("resumed endpoint did not stop")
        .ok();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_state_survives_a_new_protocol_endpoint_and_real_session_resume() {
    use klaus_agent_core::engine::message_queue::{
        QueueEdit, QueueMutationStatus, QueueTerminalState, QueuedValue,
    };

    let dir = unique_temp_dir("queue-endpoint-resume");
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let agent = make_agent(".", vec![text_turn("unused")], None);

    let create_open: CreateSession = {
        let agent = agent.clone();
        let store = store.clone();
        Arc::new(move |_args: CreateSessionArgs| {
            let agent = agent.clone();
            let store = store.clone();
            Box::pin(async move {
                let session = agent
                    .open_session(
                        store,
                        StartSessionOptions {
                            permission_mode: Some(ExternalPermissionMode::BypassPermissions),
                            ..Default::default()
                        },
                    )
                    .await?;
                Ok::<Arc<Session>, BoxError>(session)
            }) as CreateSessionFuture
        })
    };
    let ((first_sr, first_sw), (first_cr, first_cw)) = duplex_pair();
    let (_first_server, first_done) = start_server(create_open, first_sr, first_sw);
    let first_client = ProtocolClient::new(ProtocolClientOptions {
        input: first_cr,
        output: first_cw,
    });

    let (thread_id, first_uuid, second_uuid, edited, removed, stale, final_snapshot) =
        timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
            first_client
                .steer(
                    Input::Text("first".into()),
                    Some(crate::types::SteerPriority::Next),
                )
                .await;
            first_client
                .steer(
                    Input::Text("second".into()),
                    Some(crate::types::SteerPriority::Next),
                )
                .await;
            let initial = first_client
                .get_queue_snapshot()
                .await
                .expect("first endpoint queue");
            let first_uuid = initial.items[0].message.uuid.clone().unwrap();
            let second_uuid = initial.items[1].message.uuid.clone().unwrap();
            let edited = first_client
                .edit_queued(
                    "endpoint-edit",
                    first_uuid.clone(),
                    initial.revision,
                    QueueEdit {
                        value: Some(QueuedValue::Text("edited".into())),
                        ..Default::default()
                    },
                )
                .await
                .expect("edit on first endpoint");
            let removed = first_client
                .remove_queued(
                    "endpoint-remove",
                    second_uuid.clone(),
                    edited.snapshot.revision,
                )
                .await
                .expect("remove on first endpoint");
            let stale = first_client
                .remove_queued("endpoint-stale", first_uuid.clone(), initial.revision)
                .await
                .expect("ledger-only mutation on first endpoint");
            assert_eq!(stale.status, QueueMutationStatus::StaleRevision);
            let final_snapshot = first_client
                .get_queue_snapshot()
                .await
                .expect("authoritative first endpoint snapshot");
            let thread_id = first_client
                .context_snapshot()
                .await
                .expect("first endpoint context")
                .thread_id;
            let completion = first_client.shutdown().await.expect("first shutdown");
            assert!(completion.ok, "{completion:?}");
            (
                thread_id,
                first_uuid,
                second_uuid,
                edited,
                removed,
                stale,
                final_snapshot,
            )
        })
        .await
        .expect("first endpoint timed out");
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, first_done)
        .await
        .expect("first endpoint did not stop")
        .ok();

    let create_resume: CreateSession = {
        let agent = agent.clone();
        let store = store.clone();
        let thread_id = thread_id.try_to_utf8().unwrap();
        Arc::new(move |_args: CreateSessionArgs| {
            let agent = agent.clone();
            let store = store.clone();
            let thread_id = thread_id.clone();
            Box::pin(async move {
                let session = agent
                    .resume_session(store, &thread_id, StartSessionOptions::default())
                    .await?;
                Ok::<Arc<Session>, BoxError>(session)
            }) as CreateSessionFuture
        })
    };
    let ((second_sr, second_sw), (second_cr, second_cw)) = duplex_pair();
    let (_second_server, second_done) = start_server(create_resume, second_sr, second_sw);
    let second_client = ProtocolClient::new(ProtocolClientOptions {
        input: second_cr,
        output: second_cw,
    });

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        let recovered = second_client
            .get_queue_snapshot()
            .await
            .expect("new endpoint recovered queue");
        assert_eq!(recovered, final_snapshot);
        assert_eq!(recovered.items.len(), 1);
        assert_eq!(
            recovered.items[0].message.value,
            QueuedValue::Text("edited".into())
        );
        assert_eq!(
            second_client
                .edit_queued(
                    "endpoint-edit",
                    first_uuid.clone(),
                    edited.snapshot.revision - 1,
                    QueueEdit {
                        value: Some(QueuedValue::Text("edited".into())),
                        ..Default::default()
                    },
                )
                .await
                .expect("edit idempotency after reconnect"),
            edited
        );
        assert_eq!(
            second_client
                .remove_queued(
                    "endpoint-remove",
                    second_uuid.clone(),
                    edited.snapshot.revision,
                )
                .await
                .expect("remove idempotency after reconnect"),
            removed
        );
        assert_eq!(
            second_client
                .remove_queued(
                    "endpoint-stale",
                    first_uuid.clone(),
                    edited.snapshot.revision - 1,
                )
                .await
                .expect("ledger-only idempotency after reconnect"),
            stale
        );
        let missing = second_client
            .remove_queued("endpoint-tombstone", second_uuid, recovered.revision)
            .await
            .expect("tombstone after reconnect");
        assert_eq!(missing.status, QueueMutationStatus::Missing);
        assert_eq!(missing.terminal_state, Some(QueueTerminalState::Removed));
        let completion = second_client.shutdown().await.expect("second shutdown");
        assert!(completion.ok, "{completion:?}");
    })
    .await
    .expect("second endpoint timed out");
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, second_done)
        .await
        .expect("second endpoint did not stop")
        .ok();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_reserved_queue_recovers_as_interrupted_without_ghost_running_state() {
    use klaus_agent_core::engine::message_queue::{QueueMutationStatus, QueueTerminalState};

    let dir = unique_temp_dir("queue-reserved-recovery");
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let agent = make_agent(".", vec![text_turn("unused")], None);
    let create_open: CreateSession = {
        let agent = agent.clone();
        let store = store.clone();
        Arc::new(move |_args: CreateSessionArgs| {
            let agent = agent.clone();
            let store = store.clone();
            Box::pin(async move {
                Ok::<Arc<Session>, BoxError>(
                    agent
                        .open_session(store, StartSessionOptions::default())
                        .await?,
                )
            }) as CreateSessionFuture
        })
    };
    let ((first_sr, first_sw), (first_cr, first_cw)) = duplex_pair();
    let (_first_server, first_done) = start_server(create_open, first_sr, first_sw);
    let first_client = ProtocolClient::new(ProtocolClientOptions {
        input: first_cr,
        output: first_cw,
    });
    let (thread_id, queued_uuid, pending_revision) = timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        first_client
            .steer(
                Input::Text("reserved across crash".into()),
                Some(crate::types::SteerPriority::Next),
            )
            .await;
        let snapshot = first_client
            .get_queue_snapshot()
            .await
            .expect("pending before simulated crash");
        let thread_id = first_client
            .context_snapshot()
            .await
            .expect("session identity")
            .thread_id;
        let queued_uuid = snapshot.items[0].message.uuid.clone().unwrap();
        assert!(first_client.shutdown().await.expect("first shutdown").ok);
        (thread_id, queued_uuid, snapshot.revision)
    })
    .await
    .expect("first endpoint timed out");
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, first_done)
        .await
        .expect("first endpoint did not stop")
        .ok();

    // Recreate the exact last checkpoint a killed process leaves after
    // reserving the item but before persisting/removing its continuation.
    // This is rollout state, never Host initialization input.
    let thread_id_host = thread_id.try_to_utf8().unwrap();
    let queued_uuid_host = queued_uuid.try_to_utf8().unwrap();
    let reserved_revision = pending_revision + 1;
    let message = json!({
        "value": "reserved across crash",
        "mode": "prompt",
        "priority": "next",
        "uuid": queued_uuid_host,
        "sessionId": thread_id_host,
    });
    let reserved_item = json!({
        "message": message.clone(),
        "state": "reserved",
        "mutable": false,
    });
    let reserved_snapshot = json!({
        "revision": reserved_revision,
        "items": [reserved_item],
        "status": "authoritative",
    });
    let checkpoint = json!({
        "schemaVersion": 1,
        "sessionId": thread_id_host,
        "revision": reserved_revision,
        "items": [{
            "message": message,
            "pdfBytes": 0,
            "pdfDocuments": 0,
        }],
        "reserved": [queued_uuid_host],
        "tombstones": [],
        "mutations": [{
            "mutationId": "busy-before-crash",
            "request": {
                "type": "remove",
                "uuid": queued_uuid_host,
                "expectedRevision": reserved_revision,
            },
            "result": {
                "mutationId": "busy-before-crash",
                "status": "busy",
                "snapshot": reserved_snapshot,
                "error": "queued item is reserved by the active continuation",
            },
        }],
    });
    let live = store.reopen(&thread_id_host).await.unwrap();
    live.append_event(
        "queue_state".into(),
        Some(AppendEventOptions {
            turn_id: None,
            payload: Some(runtime_json_from_scalar_value(&checkpoint).unwrap()),
        }),
    )
    .await;
    live.flush().await.unwrap();
    live.shutdown().await.unwrap();

    let make_resume = |agent: Arc<Agent>, store: Arc<JsonlSessionStore>, thread_id: String| {
        Arc::new(move |_args: CreateSessionArgs| {
            let agent = agent.clone();
            let store = store.clone();
            let thread_id = thread_id.clone();
            Box::pin(async move {
                Ok::<Arc<Session>, BoxError>(
                    agent
                        .resume_session(store, &thread_id, StartSessionOptions::default())
                        .await?,
                )
            }) as CreateSessionFuture
        }) as CreateSession
    };

    let ((second_sr, second_sw), (second_cr, second_cw)) = duplex_pair();
    let (_second_server, second_done) = start_server(
        make_resume(agent.clone(), store.clone(), thread_id_host.clone()),
        second_sr,
        second_sw,
    );
    let second_client = ProtocolClient::new(ProtocolClientOptions {
        input: second_cr,
        output: second_cw,
    });
    let (recovered_snapshot, reconciled_duplicate) = timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        let recovered = second_client
            .get_queue_snapshot()
            .await
            .expect("recovered queue");
        assert_eq!(recovered.revision, reserved_revision + 1);
        assert!(recovered.items.is_empty(), "no ghost reserved item");
        let duplicate = second_client
            .remove_queued("busy-before-crash", queued_uuid.clone(), reserved_revision)
            .await
            .expect("reconciled mutation replay");
        assert_eq!(duplicate.status, QueueMutationStatus::Missing);
        assert_eq!(
            duplicate.terminal_state,
            Some(QueueTerminalState::Interrupted)
        );
        let fresh = second_client
            .remove_queued("fresh-after-crash", queued_uuid.clone(), recovered.revision)
            .await
            .expect("fresh interrupted tombstone query");
        assert_eq!(fresh.status, QueueMutationStatus::Missing);
        assert_eq!(fresh.terminal_state, Some(QueueTerminalState::Interrupted));
        assert!(second_client.shutdown().await.expect("second shutdown").ok);
        (recovered, duplicate)
    })
    .await
    .expect("second endpoint timed out");
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, second_done)
        .await
        .expect("second endpoint did not stop")
        .ok();

    // The normalization itself was checkpointed. A third endpoint must not
    // increment the revision again or resurrect the reservation.
    let ((third_sr, third_sw), (third_cr, third_cw)) = duplex_pair();
    let (_third_server, third_done) = start_server(
        make_resume(agent, store, thread_id_host),
        third_sr,
        third_sw,
    );
    let third_client = ProtocolClient::new(ProtocolClientOptions {
        input: third_cr,
        output: third_cw,
    });
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        assert_eq!(
            third_client
                .get_queue_snapshot()
                .await
                .expect("third endpoint queue"),
            recovered_snapshot
        );
        assert_eq!(
            third_client
                .remove_queued("busy-before-crash", queued_uuid, reserved_revision)
                .await
                .expect("third endpoint idempotency"),
            reconciled_duplicate
        );
        assert!(third_client.shutdown().await.expect("third shutdown").ok);
    })
    .await
    .expect("third endpoint timed out");
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, third_done)
        .await
        .expect("third endpoint did not stop")
        .ok();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupt_before_continuation_leaves_queued_input_pending_and_mutable() {
    use klaus_agent_core::engine::message_queue::QueueItemState;

    let started = Arc::new(Notify::new());
    let cancelled = Arc::new(Notify::new());
    let agent = make_blocking_agent(".", started.clone(), cancelled.clone());
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = Arc::new(ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    }));

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        let run_client = client.clone();
        let run = tokio::spawn(async move {
            run_client
                .run_to_completion(Input::Text("block".into()), None)
                .await
        });
        started.notified().await;
        client
            .steer(
                Input::Text("do not lose me".into()),
                Some(crate::types::SteerPriority::Next),
            )
            .await;
        let before = client
            .get_queue_snapshot()
            .await
            .expect("queued before interrupt");
        assert_eq!(before.items.len(), 1);

        client.interrupt().await;
        cancelled.notified().await;
        run.await
            .expect("run join")
            .expect("interrupted run has a terminal result");

        let after = client
            .get_queue_snapshot()
            .await
            .expect("queued after interrupt");
        assert_eq!(after, before);
        assert_eq!(after.items[0].state, QueueItemState::Pending);
        assert!(after.items[0].mutable);
    })
    .await
    .expect("interrupt queue test timed out");

    server.stop();
    done.await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn setup_protocol_is_correlated_idempotent_and_feeds_only_the_first_prompt() {
    use crate::types::{SetupRunRequest, SetupRunStatus, SetupTrigger};

    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let agent = make_recording_agent(".", requests.clone());
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });
    let command = r#"printf '%s' '{"hookSpecificOutput":{"hookEventName":"Setup","additionalContext":"PROTO SETUP CONTEXT"}}'"#;

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        client
            .init_session(ProtocolSessionInit {
                command_hooks: vec![ProtocolCommandHook {
                    event: HookEvent::Setup,
                    matcher: None,
                    command: command.into(),
                    timeout_ms: Some(5_000),
                    // Setup forces even an async-declared command to finish
                    // synchronously before it returns.
                    is_async: true,
                }],
                ..Default::default()
            })
            .await
            .expect("init with Setup hook");

        let request = SetupRunRequest {
            request_id: "setup-init-1".into(),
            trigger: SetupTrigger::Init,
        };
        let initialized = client.run_setup(request.clone()).await.expect("run setup");
        assert_eq!(initialized.status, SetupRunStatus::Completed);
        assert_eq!(initialized.hook_count, 1);
        assert_eq!(
            initialized.additional_context,
            vec![RuntimeString::from("PROTO SETUP CONTEXT")]
        );
        assert_eq!(
            client.run_setup(request).await.expect("idempotent replay"),
            initialized
        );
        let cached = client
            .run_setup(SetupRunRequest {
                request_id: "setup-init-2".into(),
                trigger: SetupTrigger::Init,
            })
            .await
            .expect("cached init");
        assert_eq!(cached.status, SetupRunStatus::AlreadyCompleted);
        assert_eq!(
            cached.source_request_id.as_ref(),
            Some(&RuntimeString::from("setup-init-1"))
        );

        client
            .run_to_completion(Input::Text("first prompt".into()), None)
            .await
            .expect("first run");
        let late = client
            .run_setup(SetupRunRequest {
                request_id: "setup-init-late".into(),
                trigger: SetupTrigger::Init,
            })
            .await
            .expect("late init result");
        assert_eq!(late.status, SetupRunStatus::TooLate);

        let maintenance = client
            .run_setup(SetupRunRequest {
                request_id: "setup-maintenance-1".into(),
                trigger: SetupTrigger::Maintenance,
            })
            .await
            .expect("maintenance setup");
        assert_eq!(maintenance.status, SetupRunStatus::Completed);
        client
            .run_to_completion(Input::Text("second prompt".into()), None)
            .await
            .expect("second run");

        let requests = requests.lock().unwrap().clone();
        assert_eq!(requests.len(), 2);
        let first = runtime_codec::encode_text(&requests[0].messages)
            .unwrap()
            .try_to_utf8()
            .unwrap();
        let second = runtime_codec::encode_text(&requests[1].messages)
            .unwrap()
            .try_to_utf8()
            .unwrap();
        assert!(first.contains("PROTO SETUP CONTEXT"), "{first}");
        assert!(!second.contains("PROTO SETUP CONTEXT"), "{second}");

        let lifecycle = client.get_setup_lifecycle().await.expect("setup lifecycle");
        assert_eq!(lifecycle.runs.len(), 2);
        assert_eq!(
            lifecycle.context_consumed_request_id.as_ref(),
            Some(&RuntimeString::from("setup-init-1"))
        );
    })
    .await
    .expect("setup protocol test timed out");

    server.stop();
    done.await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn todo_v2_protocol_pages_authoritative_state_and_orders_invalidations() {
    use klaus_agent_core::tasks::task_store::{create_task, CreateTaskInput, TaskListCursorStatus};

    let dir = unique_temp_dir("todo-v2-protocol");
    // Holds the crate-wide config-dir lock for the whole test: the task store
    // resolves KLAUS_CONFIG_DIR per call, so a parallel case swapping it would
    // make this one query a different store than it wrote to.
    let _config_guard = ConfigDirEnvGuard::set(&dir);
    let agent = make_agent(".", vec![text_turn("ok")], None);
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });
    let mut events = client.events();

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        let empty = client.get_task_list(None).await.expect("empty task list");
        assert!(empty.tasks.is_empty());
        assert_eq!(empty.cursor_status, TaskListCursorStatus::Current);
        assert_eq!(empty.task_list_id, empty.session_id);

        create_task(
            &empty.task_list_id,
            CreateTaskInput {
                subject: "first".into(),
                description: "first task".into(),
                ..Default::default()
            },
        )
        .await
        .expect("create first");
        create_task(
            &empty.task_list_id,
            CreateTaskInput {
                subject: "second".into(),
                description: "second task".into(),
                ..Default::default()
            },
        )
        .await
        .expect("create second");

        let first_page = client
            .get_task_list(Some(crate::types::TaskListQueryOptions {
                cursor: None,
                limit: Some(1),
            }))
            .await
            .expect("first page");
        assert_eq!(first_page.total, 2);
        assert_eq!(first_page.tasks[0].id, "1");
        let cursor = first_page.next_cursor.clone().expect("next cursor");
        let second_page = client
            .get_task_list(Some(crate::types::TaskListQueryOptions {
                cursor: Some(cursor.clone()),
                limit: Some(1),
            }))
            .await
            .expect("second page");
        assert_eq!(second_page.tasks[0].id, "2");

        create_task(
            &empty.task_list_id,
            CreateTaskInput {
                subject: "third".into(),
                description: "third task".into(),
                ..Default::default()
            },
        )
        .await
        .expect("create third");
        let stale = client
            .get_task_list(Some(crate::types::TaskListQueryOptions {
                cursor: Some(cursor),
                limit: Some(1),
            }))
            .await
            .expect("stale cursor is typed");
        assert_eq!(stale.cursor_status, TaskListCursorStatus::Stale);
        assert_eq!(stale.tasks[0].id, "1");
        assert_eq!(stale.total, 3);

        create_task(
            &empty.task_list_id,
            CreateTaskInput {
                subject: "fourth".into(),
                description: "newer than the correlated snapshot".into(),
                ..Default::default()
            },
        )
        .await
        .expect("create fourth");
        loop {
            let event = events.next().await.expect("task event stream remains open");
            if event.submission_id.is_empty() {
                if let Some(change) = &event.task_list_change {
                    assert_eq!(change.session_id, empty.session_id);
                    assert_eq!(change.task_list_id, empty.task_list_id);
                    assert_eq!(change.generation, stale.generation);
                    if change.revision > stale.revision {
                        break;
                    }
                }
            }
        }
    })
    .await
    .expect("TodoV2 protocol test timed out");

    server.stop();
    done.await.ok();
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn todo_query_snapshot_supersedes_buffered_and_late_publications_through_cutoff() {
    use klaus_agent_core::tasks::task_store::{create_task, CreateTaskInput};

    let dir = unique_temp_dir("todo-v2-query-cutoff");
    // Holds the crate-wide config-dir lock for the whole test: the task store
    // resolves KLAUS_CONFIG_DIR per call, so a parallel case swapping it would
    // make this one query a different store than it wrote to.
    let _config_guard = ConfigDirEnvGuard::set(&dir);
    let agent = make_agent(".", vec![text_turn("ok")], None);
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = Arc::new(ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    }));
    let mut events = client.events();

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        let empty = client.get_task_list(None).await.expect("initial query");
        let gate = Arc::new(TodoQueryTestGate::new());
        let mut entered = gate.subscribe();
        *server.inner.todo_query_test_gate.lock().unwrap() = Some(gate.clone());
        let query_client = client.clone();
        let query = tokio::spawn(async move { query_client.get_task_list(None).await });
        while !*entered.borrow() {
            entered.changed().await.unwrap();
        }
        assert!(
            server
                .inner
                .queue_publication_barrier
                .lock()
                .unwrap()
                .active
        );

        create_task(
            &empty.task_list_id,
            CreateTaskInput {
                subject: "buffered revision one".into(),
                description: "d".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
            loop {
                let buffered = server
                    .inner
                    .queue_publication_barrier
                    .lock()
                    .unwrap()
                    .buffered
                    .iter()
                    .any(|write| matches!(write, UnsolicitedWrite::TaskListChange { .. }));
                if buffered {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("revision-one callback was not buffered behind the active barrier");

        gate.release();
        let snapshot = query.await.unwrap().expect("correlated snapshot");
        assert_eq!(snapshot.revision, 1);
        let cutoff = {
            let state = server.inner.queue_publication_barrier.lock().unwrap();
            *state
                .task_publication_floors
                .get(&snapshot.session_id)
                .expect("query installed its persistent publication floor")
        };

        // A callback for the Session's previous team/list identity that
        // was admitted by core before the query cutoff but reaches
        // protocol only after barrier release is rejected too. The floor
        // is Session-wide rather than keyed by list, so repeated identity
        // changes neither leak map entries nor resurrect list A after a
        // correlated snapshot for list B.
        assert!(server
            .inner
            .enqueue_queue_ordered_write(UnsolicitedWrite::TaskListChange {
                change: klaus_agent_core::tasks::task_store::TaskListChange {
                    plan_id: None,
                    plan_kind: "tasks".into(),
                    session_id: snapshot.session_id.clone(),
                    task_list_id: RuntimeString::from("old-team-list"),
                    generation: snapshot.generation.clone(),
                    revision: snapshot.revision,
                    fingerprint: snapshot.fingerprint.clone(),
                    tasks: snapshot.tasks.clone(),
                },
                publication_sequence: cutoff,
            },));

        create_task(
            &empty.task_list_id,
            CreateTaskInput {
                subject: "revision two".into(),
                description: "d".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        loop {
            let event = events.next().await.expect("task event stream remains open");
            if let Some(change) = &event.task_list_change {
                assert_eq!(change.generation, snapshot.generation);
                assert_eq!(
                    change.revision, 2,
                    "response N must never be followed by a buffered/late <=N invalidation"
                );
                break;
            }
        }
    })
    .await
    .expect("TodoV2 cutoff protocol test timed out");

    server.stop();
    done.await.ok();
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn todo_query_snapshot_supersedes_a_real_callback_that_arrives_after_release() {
    use klaus_agent_core::tasks::task_store::{create_task, CreateTaskInput};

    let dir = unique_temp_dir("todo-v2-real-late-callback");
    // Holds the crate-wide config-dir lock for the whole test: the task store
    // resolves KLAUS_CONFIG_DIR per call, so a parallel case swapping it would
    // make this one query a different store than it wrote to.
    let _config_guard = ConfigDirEnvGuard::set(&dir);
    let agent = make_agent(".", vec![text_turn("ok")], None);
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });
    let mut events = client.events();

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        let empty = client.get_task_list(None).await.expect("initial query");
        let callback_gate = Arc::new(TodoCallbackTestGate::new());
        *server.inner.todo_callback_test_gate.lock().unwrap() = Some(callback_gate.clone());
        let entered = callback_gate.entered.notified();
        create_task(
            &empty.task_list_id,
            CreateTaskInput {
                subject: "late revision one".into(),
                description: "d".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        entered.await;

        // The real core callback has selected revision one, but is held
        // immediately before protocol admission. The query advances its
        // cutoff and releases its barrier before that callback resumes.
        let snapshot = client.get_task_list(None).await.expect("snapshot");
        assert_eq!(snapshot.revision, 1);
        callback_gate.release();

        create_task(
            &empty.task_list_id,
            CreateTaskInput {
                subject: "revision two".into(),
                description: "d".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        loop {
            let event = events.next().await.expect("task event stream remains open");
            let Some(change) = &event.task_list_change else {
                continue;
            };
            if change.session_id != snapshot.session_id || change.generation != snapshot.generation
            {
                continue;
            }
            assert!(
                change.revision > snapshot.revision,
                "a real callback admitted after query release must obey the persistent cutoff"
            );
            assert_eq!(change.revision, 2);
            break;
        }
    })
    .await
    .expect("TodoV2 real late-callback test timed out");

    server.stop();
    done.await.ok();
    std::fs::remove_dir_all(dir).ok();
}

fn protocol_pdf_input(base64: &str, mime_type: &str, filename: &str) -> Input {
    Input::Parts(vec![
        klaus_agent_core::engine::session::UserContentPart::Document {
            base64: base64.into(),
            mime_type: mime_type.into(),
            filename: Some(filename.into()),
        },
    ])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_pdf_run_and_steer_use_protocol_document_input() {
    let agent = make_agent(".", vec![text_turn("pdf received")], None);
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    let result = client
        .run_to_completion(
            protocol_pdf_input("JVBERi0xLjcK", "APPLICATION/PDF", r"folder\report.pdf"),
            None,
        )
        .await
        .expect("valid PDF run");
    assert_eq!(result.final_text, "pdf received");
    let history = client.get_history().await.expect("history");
    let Message::User(user) = &history[0] else {
        panic!("expected user history");
    };
    assert_eq!(
        user.content,
        vec![klaus_agent_core::schemas::content::ContentBlock::Document(
            klaus_agent_core::schemas::content::DocumentBlock {
                mime_type: "application/pdf".into(),
                base64: "JVBERi0xLjcK".into(),
                filename: Some("report.pdf".into()),
            }
        )]
    );

    client
        .steer(
            protocol_pdf_input("JVBERi0xLjcK", "application/pdf", "steer.pdf"),
            Some(crate::types::SteerPriority::Now),
        )
        .await;
    let queued = client.get_queued().await.expect("queued");
    assert_eq!(queued.len(), 1);
    assert_eq!(
        queued[0].value,
        klaus_agent_core::engine::message_queue::QueuedValue::Blocks(vec![
            klaus_agent_core::schemas::content::ContentBlock::Document(
                klaus_agent_core::schemas::content::DocumentBlock {
                    mime_type: "application/pdf".into(),
                    base64: "JVBERi0xLjcK".into(),
                    filename: Some("steer.pdf".into()),
                }
            )
        ])
    );

    server.stop();
    done.await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_pdf_run_and_steer_return_typed_errors_without_mutation() {
    let agent = make_agent(".", vec![text_turn("must not run")], None);
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    let error = client
        .run_to_completion(
            protocol_pdf_input("%%%not-base64%%%", "application/pdf", "bad.pdf"),
            None,
        )
        .await
        .expect_err("invalid PDF run should fail");
    match error {
        crate::client::ClientError::InvalidInput { message, detail } => {
            assert!(message.contains("valid standard base64"));
            assert_eq!(
                detail.code,
                crate::types::InputValidationErrorCode::InvalidDocumentBase64
            );
        }
        other => panic!("expected typed invalid-input error, got {other:?}"),
    }
    assert!(client.get_history().await.expect("history").is_empty());

    let steer_error = client
        .steer_to_completion(
            protocol_pdf_input("JVBERi0xLjcK", "text/plain", "bad.pdf"),
            None,
        )
        .await
        .expect_err("invalid PDF steer should fail");
    match steer_error {
        crate::client::ClientError::InvalidInput { detail, .. } => assert_eq!(
            detail.code,
            crate::types::InputValidationErrorCode::InvalidDocumentMime
        ),
        other => panic!("expected typed invalid-input error, got {other:?}"),
    }
    assert!(client.get_queued().await.expect("queued").is_empty());
    assert!(client.get_history().await.expect("history").is_empty());

    server.stop();
    done.await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checked_pdf_steer_resolves_only_after_real_tool_continuation_delivery() {
    let agent = make_agent(
        ".",
        vec![
            tool_use_turn(
                "t1",
                "ask_user",
                json!({ "question": "continue?", "options": ["yes"] }),
            ),
            text_turn("used steered PDF"),
        ],
        None,
    );
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    let steer_result = timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
            let mut events = client.events();
            client.run(Input::Text("start".into()), None).await;
            let mut steer = None;
            while let Some(event) = events.next().await {
                if let Some(elicitation) = &event.elicitation {
                    let request_id = elicitation
                        .request_id
                        .try_to_utf8()
                        .expect("scalar elicitation request id");
                    let steer_client = client.clone();
                    let queue_client = client.clone();
                    let (queued_tx, queued_rx) = oneshot::channel();
                    steer = Some(tokio::spawn(async move {
                        steer_client
                            .steer_to_completion_after_send(
                                protocol_pdf_input("JVBERi0xLjcK", "application/pdf", "steer.pdf"),
                                Some(crate::types::SteerPriority::Now),
                                async move {
                                    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
                                        loop {
                                            let queued =
                                                queue_client.get_queued().await.expect("queued");
                                            if queued.iter().any(|message| {
                                                matches!(
                                                    &message.value,
                                                    klaus_agent_core::engine::message_queue::QueuedValue::Blocks(blocks)
                                                        if blocks.iter().any(|block| matches!(block, ContentBlock::Document(_)))
                                                )
                                            }) {
                                                break;
                                            }
                                            tokio::task::yield_now().await;
                                        }
                                    })
                                    .await
                                    .expect(
                                        "steered PDF was not queued before elicitation resolution",
                                    );
                                    let _ = queued_tx.send(());
                                },
                            )
                            .await
                    }));
                    queued_rx
                        .await
                        .expect("steered PDF was not queued before elicitation resolution");
                    client
                        .resolve_elicitation(
                            request_id,
                            crate::types::ElicitResponseWire::Accept {
                                values: vec!["yes".into()],
                            },
                        )
                        .await;
                }
                if is_completed(&event) {
                    break;
                }
            }
            steer.expect("steer task").await.unwrap()
        })
        .await
        .expect("timed out")
        .expect("steer applied");
    assert!(steer_result.applied);

    let history = client.get_history().await.expect("history");
    let steered = history.iter().find_map(|message| match message {
        Message::ToolResult(result) => result.extra_user_blocks.as_ref(),
        _ => None,
    });
    assert_eq!(
        steered,
        Some(&vec![
            klaus_agent_core::schemas::content::ContentBlock::Document(
                klaus_agent_core::schemas::content::DocumentBlock {
                    mime_type: "application/pdf".into(),
                    base64: "JVBERi0xLjcK".into(),
                    filename: Some("steer.pdf".into()),
                }
            )
        ])
    );
    assert!(client.get_queued().await.expect("queued").is_empty());

    server.stop();
    done.await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_history_cost_subagents() {
    let agent = make_agent(".", vec![text_turn("hi there")], None);
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        let _ = client
            .run_to_completion(Input::Text("hello".into()), None)
            .await;
        let history = client.get_history().await.expect("history");
        let is_retained_skill_catalog = |message: &Message| {
            matches!(message, Message::User(user)
                if user.is_meta == Some(true)
                    && matches!(user.content.as_slice(), [ContentBlock::Text(text)]
                        if text.harness_envelope == Some(HarnessEnvelope::SystemReminder)
                            && text.text.starts_with("<skill-catalog:")))
        };
        assert_eq!(
            history
                .iter()
                .filter(|message| is_retained_skill_catalog(message))
                .count(),
            1,
            "history must retain the trusted skill catalog"
        );
        let users = history
            .iter()
            .filter(|message| !is_retained_skill_catalog(message))
            .filter_map(|message| match message {
                Message::User(user) => Some(user),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].is_meta, None);
        assert!(!users[0].uuid.is_empty());
        assert!(
            matches!(users[0].content.as_slice(), [ContentBlock::Text(text)]
            if text.text == "hello" && text.harness_envelope.is_none())
        );
        assert!(history.iter().any(|message| matches!(message,
            Message::Assistant(assistant)
                if matches!(assistant.content.as_slice(), [ContentBlock::Text(text)]
                    if text.text == "hi there"))));
        let reminders = history.iter().filter_map(|message| match message {
            Message::User(user) if user.is_meta == Some(true) => Some(user),
            _ => None,
        }).collect::<Vec<_>>();
        assert!(reminders.len() <= 1, "a fresh history cannot duplicate its initial catalog page");
        for reminder in reminders {
            use klaus_agent_core::extensions::skills::builtin_skills::builtin_skills;
            use klaus_agent_core::extensions::skills::loader::model_invocable;
            use klaus_agent_core::extensions::skills::skill_listing_reminder::create_skill_listing_reminder_provider;
            let [ContentBlock::Text(text)] = &reminder.content[..] else {
                panic!("only a retained skill text reminder may accompany the user input");
            };
            assert_eq!(text.harness_envelope, Some(klaus_agent_core::schemas::content::HarnessEnvelope::SystemReminder));
            let text = text.text.try_to_utf8().unwrap();
            let (marker, listing) = text.split_once('\n').expect("retained catalog marker");
            let parts = marker.strip_prefix("<skill-catalog:").and_then(|value| value.strip_suffix('>'))
                .expect("only a retained skill catalog may accompany the user input").split(':').collect::<Vec<_>>();
            assert_eq!(parts.len(), 3);
            assert_eq!(parts[0].len(), 64);
            assert!(parts[0].bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
            assert_eq!(parts[1], "0");
            assert_eq!(parts[2].parse::<usize>().unwrap(), model_invocable(builtin_skills()).len());
            let expected = create_skill_listing_reminder_provider(builtin_skills, None).tick().unwrap();
            assert_eq!(listing, expected.try_to_utf8().unwrap(), "unexpected retained context");
        }

        let _cost = client.get_cost().await.expect("cost");
        assert_eq!(client.get_subagents().await.expect("subagents").len(), 0);
    })
    .await
    .expect("timed out");

    server.stop();
    done.await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_ordered_channel_loss_stops_without_direct_error_write() {
    let agent = make_agent(".", vec![], None);
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let server = Arc::new(ProtocolServer::new(ProtocolServerOptions {
        create_session: make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        lifecycle: None,
        plugin_admin: None,
        credential_store: None,
        input: Box::new(tokio::io::empty()),
        output: Box::new(RecordingWriter {
            inner: Box::new(tokio::io::sink()),
            bytes: bytes.clone(),
        }),
    }));
    let _ = server.inner.unsolicited_tx.lock().unwrap().take();

    server
        .inner
        .report_agent_memory_snapshot_op_error_ordered(
            None,
            "snapshot-channel-closed",
            "forced snapshot failure".into(),
        )
        .await;

    assert!(server.inner.stopped.load(Ordering::SeqCst));
    assert!(
        bytes.lock().unwrap().is_empty(),
        "a closed snapshot FIFO must not fall back to direct output"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closing_session_snapshot_error_waits_for_snapshot_event_gate() {
    let agent = make_agent(".", vec![], None);
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });
    assert!(client
        .get_agent_memory_snapshot_decisions()
        .await
        .expect("bind snapshot session")
        .is_empty());
    let mut events = client.events();
    let session = server.inner.session.get().cloned().expect("bound session");
    let snapshot_sink = server
        .inner
        .session_publication_sinks_for_test
        .lock()
        .unwrap()
        .snapshot
        .clone()
        .expect("published snapshot sink");

    let snapshot_gate = Arc::new(SessionPublicationCallbackTestGate::new());
    let _snapshot_release = SessionPublicationCallbackReleaseOnDrop(snapshot_gate.clone());
    let snapshot_holder = tokio::spawn({
        let session = session.clone();
        let snapshot_gate = snapshot_gate.clone();
        async move {
            session
                .with_agent_memory_snapshot_order(move || {
                    snapshot_gate.pause();
                    snapshot_sink(publication_test_snapshot_audit("closing-gate"));
                })
                .await;
        }
    });
    timeout(
        CROSS_TASK_LIVENESS_TIMEOUT,
        snapshot_gate.entered.notified(),
    )
    .await
    .expect("snapshot event gate is held");

    // `ensure_session` now rejects the op, but the existing Session still
    // owns lifecycle publications already admitted under this gate. The
    // correlated shutdown error must queue behind them instead of using the
    // direct writer.
    server.inner.session_closing.store(true, Ordering::SeqCst);
    let query_client = client.clone();
    let mut query =
        tokio::spawn(async move { query_client.get_agent_memory_snapshot_decisions().await });
    let resolve_client = client.clone();
    let mut resolve = tokio::spawn(async move {
        resolve_client
            .resolve_agent_memory_snapshot_decision(
                "unused-closing-request",
                AgentMemoryMergeChoice::Keep,
            )
            .await
    });
    assert!(
        timeout(Duration::from_millis(100), &mut query)
            .await
            .is_err(),
        "snapshot shutdown error bypassed the in-flight event gate"
    );
    assert!(
        timeout(Duration::from_millis(100), &mut resolve)
            .await
            .is_err(),
        "snapshot resolve shutdown error bypassed the in-flight event gate"
    );

    snapshot_gate.release();
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, snapshot_holder)
        .await
        .expect("snapshot gate holder exits")
        .expect("snapshot gate holder joins");
    let error = timeout(CROSS_TASK_LIVENESS_TIMEOUT, query)
        .await
        .expect("ordered snapshot shutdown error completes")
        .expect("snapshot query task joins")
        .expect_err("closing Session rejects the snapshot query");
    assert!(
        matches!(error, ClientError::Op(ref message) if message.contains("protocol session has shut down")),
        "unexpected snapshot shutdown error: {error:?}"
    );
    let resolve_error = timeout(CROSS_TASK_LIVENESS_TIMEOUT, resolve)
        .await
        .expect("ordered snapshot resolve shutdown error completes")
        .expect("snapshot resolve task joins")
        .expect_err("closing Session rejects snapshot resolution");
    assert!(
        matches!(resolve_error, ClientError::Op(ref message) if message.contains("protocol session has shut down")),
        "unexpected snapshot resolve shutdown error: {resolve_error:?}"
    );

    let mut ordered = Vec::new();
    while ordered.len() < 3 {
        let event = timeout(CROSS_TASK_LIVENESS_TIMEOUT, events.next())
            .await
            .expect("ordered closing event arrives")
            .expect("protocol event stream remains open");
        if event
            .agent_memory_snapshot_decision
            .as_ref()
            .is_some_and(|audit| audit.request.request_id == "closing-gate")
        {
            assert!(event.submission_id.is_empty());
            ordered.push("lifecycle");
        } else if event.error.as_ref().is_some_and(|message| {
            message
                .try_to_utf8()
                .is_ok_and(|message| message.contains("protocol session has shut down"))
        }) {
            assert!(!event.submission_id.is_empty());
            assert!(event.agent_memory_snapshot_decision.is_none());
            assert!(event.agent_memory_snapshot_decision_query.is_none());
            assert!(event.agent_memory_snapshot_decisions.is_none());
            assert!(event.error_detail.is_none());
            ordered.push("error");
        }
    }
    assert_eq!(ordered, vec!["lifecycle", "error", "error"]);

    server.stop();
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, done)
        .await
        .expect("server shutdown completes")
        .ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_query_recovery_failures_are_correlated_without_success_payloads() {
    for query_all in [false, true] {
        let (snapshot_root, restored_audit) = restorable_publication_test_snapshot_audit().await;
        let request_id = restored_audit.request.request_id.clone();
        let failing_rollout = Arc::new(FailingSnapshotRecoveryRollout::default());
        let agent = make_agent(".", vec![], None);
        let session = agent.start_session(StartSessionOptions {
            permission_mode: Some(ExternalPermissionMode::BypassPermissions),
            rollout: Some(failing_rollout.clone()),
            initial_agent_memory_snapshot_decisions: Some(vec![restored_audit]),
            ..Default::default()
        });
        let create_session: CreateSession = Arc::new(move |_args| {
            let session = session.clone();
            Box::pin(async move { Ok::<Arc<Session>, BoxError>(session) }) as CreateSessionFuture
        });
        let ((sr, sw), (cr, cw)) = duplex_pair();
        let (server, done) = start_server(create_session, sr, sw);
        let client = ProtocolClient::new(ProtocolClientOptions {
            input: cr,
            output: cw,
        });
        let mut events = client.events();

        let query_client = client.clone();
        let mut query = if query_all {
            tokio::spawn(async move {
                query_client
                    .get_agent_memory_snapshot_decisions()
                    .await
                    .map(|_| ())
            })
        } else {
            tokio::spawn(async move {
                query_client
                    .get_agent_memory_snapshot_decision(request_id)
                    .await
                    .map(|_| ())
            })
        };
        timeout(
            CROSS_TASK_LIVENESS_TIMEOUT,
            failing_rollout.wait_until_flush_entered(),
        )
        .await
        .expect("snapshot recovery flush was entered");
        assert!(
            timeout(Duration::from_millis(100), &mut query)
                .await
                .is_err(),
            "snapshot query replied before recovery durability resolved"
        );

        failing_rollout.release_flush_failure();
        let error = timeout(CROSS_TASK_LIVENESS_TIMEOUT, query)
            .await
            .expect("snapshot query completed after flush failure")
            .expect("snapshot query task joined")
            .expect_err("snapshot recovery failure must reject the query");
        assert!(
            matches!(
                error,
                ClientError::Op(ref message)
                    if message.starts_with(
                        "snapshot decision persistence is inconsistent:"
                    ) && message.contains(
                        "injected snapshot recovery rollout flush failure"
                    )
            ),
            "unexpected snapshot query failure: {error:?}"
        );

        let failure_event = timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
            loop {
                let event = events.next().await.expect("protocol event");
                let Some(error) = &event.error else {
                    continue;
                };
                if error.try_to_utf8().is_ok_and(|message| {
                    message.starts_with("snapshot decision persistence is inconsistent:")
                }) {
                    break event;
                }
            }
        })
        .await
        .expect("correlated snapshot query failure event");
        assert!(!failure_event.submission_id.is_empty());
        assert!(failure_event.agent_memory_snapshot_decision.is_none());
        assert!(failure_event.agent_memory_snapshot_decision_query.is_none());
        assert!(failure_event.agent_memory_snapshot_decisions.is_none());
        assert!(failure_event.error_detail.is_none());

        server.stop();
        timeout(CROSS_TASK_LIVENESS_TIMEOUT, done)
            .await
            .expect("protocol server stopped")
            .ok();
        std::fs::remove_dir_all(snapshot_root).ok();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_memory_snapshot_request_is_queryable_and_resolves_exactly_once_over_protocol() {
    let root = unique_temp_dir("snapshot-protocol-resolve");
    let memory_dir = root.join("memory");
    let snapshot_dir = memory_dir.join("snapshots");
    std::fs::create_dir_all(&snapshot_dir).unwrap();
    std::fs::write(
        snapshot_dir.join("snapshot.json"),
        r#"{"updatedAt":"2026-07-18T00:00:00Z"}"#,
    )
    .unwrap();
    std::fs::write(snapshot_dir.join("remote.md"), "remote\n").unwrap();
    std::fs::write(memory_dir.join("local.md"), "local\n").unwrap();

    let agent = make_agent_with_memory(
        root.to_str().unwrap(),
        vec![
            tool_use_turn(
                "snapshot-task",
                "task",
                json!({"model": "low",
                    "description": "exercise snapshot protocol",
                    "prompt": "read memory and return",
                    "subagent_type": "snapshot-agent",
                    "run_in_background": false
                }),
            ),
            text_turn("child done"),
            text_turn("parent done"),
        ],
        Some(json!({
            "agentMemory": true,
            "agentMemorySnapshot": true,
            "maxRetries": 0
        })),
        Some(Arc::new(ProtocolTestMemoryHost {
            layout: ProtocolTestMemoryLayout {
                dir: memory_dir.to_string_lossy().into_owned(),
            },
        })),
    );
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
            let mut events = client.events();
            client
                .init_session(ProtocolSessionInit {
                    agents: vec![AgentDefinition {
                        name: "snapshot-agent".into(),
                        description: "snapshot protocol fixture".into(),
                        memory: Some(AgentMemoryScope::User),
                        ..Default::default()
                    }],
                    ..Default::default()
                })
                .await
                .expect("init session");

            let run_client = client.clone();
            let run = tokio::spawn(async move {
                run_client
                    .run_to_completion(Input::Text("delegate snapshot work".into()), None)
                    .await
            });

            let requested = loop {
                let event = events.next().await.expect("protocol event");
                if let Some(audit) = event.agent_memory_snapshot_decision.clone() {
                    if audit.status == AgentMemorySnapshotDecisionStatus::Requested {
                        break audit;
                    }
                }
            };
            assert_eq!(requested.request.agent_type, "snapshot-agent");
            assert_eq!(requested.request.scope, AgentMemoryScope::User);
            assert!(!requested.request.first_use);
            assert_eq!(requested.decision, None);
            assert_eq!(requested.source, None);

            let request_id = requested.request.request_id.clone();
            let one = client
                .get_agent_memory_snapshot_decision(request_id.clone())
                .await
                .expect("query one requested audit");
            assert_eq!(one.request_id, request_id);
            assert_eq!(one.audit, Some(requested.clone()));
            assert_eq!(
                client
                    .get_agent_memory_snapshot_decisions()
                    .await
                    .expect("query all requested audits"),
                vec![requested]
            );

            let applied = client
                .resolve_agent_memory_snapshot_decision(
                    request_id.clone(),
                    AgentMemoryMergeChoice::Keep,
                )
                .await
                .expect("resolve snapshot decision");
            assert_eq!(applied.status, AgentMemorySnapshotDecisionStatus::Applied);
            assert_eq!(applied.decision, Some(AgentMemoryMergeChoice::Keep));
            assert_eq!(
                applied.source,
                Some(AgentMemorySnapshotDecisionSource::Host)
            );
            assert!(applied.decided_at.is_some());
            assert!(applied.ended_at.is_some());

            let idempotent = client
                .resolve_agent_memory_snapshot_decision(
                    request_id.clone(),
                    AgentMemoryMergeChoice::Keep,
                )
                .await
                .expect("repeat the same snapshot decision");
            assert_eq!(idempotent, applied);
            assert_eq!(
                client
                    .get_agent_memory_snapshot_decision(request_id.clone())
                    .await
                    .expect("query applied audit")
                    .audit,
                Some(applied.clone())
            );

            let conflict = client
                .resolve_agent_memory_snapshot_decision(
                    request_id,
                    AgentMemoryMergeChoice::Merge,
                )
                .await
                .expect_err("a different decision must conflict");
            assert!(
                matches!(conflict, ClientError::Op(ref message) if message.contains("snapshot decision conflict")),
                "unexpected conflict: {conflict:?}"
            );

            let result = run.await.unwrap().expect("run completes after resolution");
            assert_eq!(result.final_text, "parent done");
        })
        .await
        .expect("snapshot protocol resolution timed out");

    server.stop();
    done.await.ok();
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merge_snapshot_resolution_replies_decision_recorded_before_provider_admission() {
    let root = unique_temp_dir("snapshot-protocol-merge-admission");
    let memory_dir = root.join("memory");
    let snapshot_dir = memory_dir.join("snapshots");
    std::fs::create_dir_all(&snapshot_dir).unwrap();
    std::fs::write(
        snapshot_dir.join("snapshot.json"),
        r#"{"updatedAt":"2026-07-18T00:00:00Z"}"#,
    )
    .unwrap();
    std::fs::write(snapshot_dir.join("remote.md"), "remote\n").unwrap();
    std::fs::write(memory_dir.join("local.md"), "local\n").unwrap();

    let agent = make_agent_with_memory(
        root.to_str().unwrap(),
        vec![
            tool_use_turn(
                "snapshot-merge-task",
                "task",
                json!({"model": "low",
                    "description": "exercise merge admission protocol",
                    "prompt": "read merged memory and return",
                    "subagent_type": "snapshot-agent",
                    "run_in_background": false
                }),
            ),
            text_turn("child done"),
            text_turn("parent done"),
        ],
        Some(json!({
            "agentMemory": true,
            "agentMemorySnapshot": true,
            "maxRetries": 0
        })),
        Some(Arc::new(ProtocolTestMemoryHost {
            layout: ProtocolTestMemoryLayout {
                dir: memory_dir.to_string_lossy().into_owned(),
            },
        })),
    );
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        let mut events = client.events();
        client
            .init_session(ProtocolSessionInit {
                agents: vec![AgentDefinition {
                    name: "snapshot-agent".into(),
                    description: "snapshot merge admission fixture".into(),
                    memory: Some(AgentMemoryScope::User),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("init session");
        let run_client = client.clone();
        let run = tokio::spawn(async move {
            run_client
                .run_to_completion(Input::Text("delegate merge snapshot work".into()), None)
                .await
        });
        let requested = loop {
            let event = events.next().await.expect("snapshot requested event");
            if let Some(audit) = event.agent_memory_snapshot_decision.clone() {
                if audit.status == AgentMemorySnapshotDecisionStatus::Requested {
                    break audit;
                }
            }
        };
        let request_id = requested.request.request_id.clone();
        let recorded = client
            .resolve_agent_memory_snapshot_decision(
                request_id.clone(),
                AgentMemoryMergeChoice::Merge,
            )
            .await
            .expect("record merge decision");
        assert_eq!(
            recorded.status,
            AgentMemorySnapshotDecisionStatus::DecisionRecorded
        );
        assert_eq!(recorded.ended_at, None);
        assert_eq!(recorded.decision, Some(AgentMemoryMergeChoice::Merge));
        assert_eq!(
            recorded.source,
            Some(AgentMemorySnapshotDecisionSource::Host)
        );

        let mut lifecycle = Vec::new();
        while lifecycle.last().copied() != Some(AgentMemorySnapshotDecisionStatus::Applied) {
            let event = events.next().await.expect("snapshot lifecycle event");
            if let Some(audit) = event.agent_memory_snapshot_decision.clone() {
                if audit.request.request_id == request_id {
                    lifecycle.push(audit.status);
                }
            }
        }
        assert_eq!(
            lifecycle,
            vec![
                AgentMemorySnapshotDecisionStatus::DecisionRecorded,
                AgentMemorySnapshotDecisionStatus::Applied,
            ]
        );
        let authoritative = client
            .get_agent_memory_snapshot_decision(request_id)
            .await
            .expect("query applied merge audit")
            .audit
            .expect("merge audit remains queryable");
        assert_eq!(
            authoritative.status,
            AgentMemorySnapshotDecisionStatus::Applied
        );
        assert!(authoritative.ended_at.is_some());
        assert_eq!(
            run.await
                .unwrap()
                .expect("run completes after merge admission")
                .final_text,
            "parent done"
        );
    })
    .await
    .expect("merge snapshot protocol admission timed out");

    server.stop();
    done.await.ok();
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_policy_unavailable_reply_precedes_provider_applied() {
    let root = unique_temp_dir("snapshot-protocol-unavailable-merge");
    let memory_dir = root.join("memory");
    let snapshot_dir = memory_dir.join("snapshots");
    std::fs::create_dir_all(&snapshot_dir).unwrap();
    std::fs::write(
        snapshot_dir.join("snapshot.json"),
        r#"{"updatedAt":"2026-07-18T00:00:00Z"}"#,
    )
    .unwrap();
    std::fs::write(snapshot_dir.join("remote.md"), "remote\n").unwrap();
    std::fs::write(memory_dir.join("local.md"), "local\n").unwrap();

    let agent = make_agent_with_memory(
        root.to_str().unwrap(),
        vec![
            tool_use_turn(
                "snapshot-unavailable-merge-task",
                "task",
                json!({"model": "low",
                    "description": "exercise unavailable Merge policy ordering",
                    "prompt": "read merged memory and return",
                    "subagent_type": "snapshot-agent",
                    "run_in_background": false
                }),
            ),
            text_turn("child done"),
            text_turn("parent done"),
        ],
        Some(json!({
            "agentMemory": true,
            "agentMemorySnapshot": true,
            "maxRetries": 0
        })),
        Some(Arc::new(ProtocolTestMemoryHost {
            layout: ProtocolTestMemoryLayout {
                dir: memory_dir.to_string_lossy().into_owned(),
            },
        })),
    );
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        let mut events = client.events();
        client
            .init_session(ProtocolSessionInit {
                agents: vec![AgentDefinition {
                    name: "snapshot-agent".into(),
                    description: "snapshot unavailable Merge fixture".into(),
                    memory: Some(AgentMemoryScope::User),
                    ..Default::default()
                }],
                agent_memory_snapshot_policy: Some(AgentMemorySnapshotDecisionPolicy {
                    no_handler: AgentMemorySnapshotPolicyAction::Merge,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .expect("init session");
        let run_client = client.clone();
        let run = tokio::spawn(async move {
            run_client
                .run_to_completion(
                    Input::Text("delegate unavailable merge snapshot work".into()),
                    None,
                )
                .await
        });
        let requested = loop {
            let event = events.next().await.expect("snapshot requested event");
            if let Some(audit) = event.agent_memory_snapshot_decision.clone() {
                if audit.status == AgentMemorySnapshotDecisionStatus::Requested {
                    break audit;
                }
            }
        };
        let request_id = requested.request.request_id.clone();
        let recorded = client
            .settle_agent_memory_snapshot_unavailable(
                request_id.clone(),
                AgentMemorySnapshotUnavailableReason::NoHandler,
                Some("desktop handler unavailable".into()),
            )
            .await
            .expect("settle unavailable through Merge policy");
        assert_eq!(
            recorded.status,
            AgentMemorySnapshotDecisionStatus::DecisionRecorded
        );
        assert_eq!(recorded.decision, Some(AgentMemoryMergeChoice::Merge));
        assert_eq!(
            recorded.source,
            Some(AgentMemorySnapshotDecisionSource::NoHandlerPolicy)
        );

        let mut lifecycle = Vec::new();
        let mut saw_correlated_recorded = false;
        let mut saw_live_applied = false;
        while !(saw_correlated_recorded && saw_live_applied) {
            let event = events.next().await.expect("snapshot lifecycle event");
            if let Some(audit) = &event.agent_memory_snapshot_decision {
                if audit.request.request_id == request_id {
                    lifecycle.push(audit.status);
                    saw_correlated_recorded |= !event.submission_id.is_empty()
                        && audit.status == AgentMemorySnapshotDecisionStatus::DecisionRecorded;
                    saw_live_applied |= event.submission_id.is_empty()
                        && audit.status == AgentMemorySnapshotDecisionStatus::Applied;
                }
            }
        }
        assert_eq!(
            lifecycle,
            vec![
                AgentMemorySnapshotDecisionStatus::DecisionRecorded,
                AgentMemorySnapshotDecisionStatus::Applied,
            ]
        );
        assert_eq!(
            run.await
                .unwrap()
                .expect("run completes after unavailable Merge policy")
                .final_text,
            "parent done"
        );
    })
    .await
    .expect("unavailable Merge policy ordering timed out");

    server.stop();
    done.await.ok();
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delayed_snapshot_writer_cannot_be_overtaken_by_applied_reply_or_query() {
    let root = unique_temp_dir("snapshot-protocol-ordered-writer");
    let memory_dir = root.join("memory");
    let snapshot_dir = memory_dir.join("snapshots");
    std::fs::create_dir_all(&snapshot_dir).unwrap();
    std::fs::write(
        snapshot_dir.join("snapshot.json"),
        r#"{"updatedAt":"2026-07-18T00:00:00Z"}"#,
    )
    .unwrap();
    std::fs::write(snapshot_dir.join("remote.md"), "remote\n").unwrap();
    std::fs::write(memory_dir.join("local.md"), "local\n").unwrap();

    let agent = make_agent_with_memory(
        root.to_str().unwrap(),
        vec![
            tool_use_turn(
                "snapshot-task",
                "task",
                json!({"model": "low",
                    "description": "exercise ordered snapshot protocol",
                    "prompt": "read memory and return",
                    "subagent_type": "snapshot-agent",
                    "run_in_background": false
                }),
            ),
            text_turn("child done"),
            text_turn("parent done"),
        ],
        Some(json!({
            "agentMemory": true,
            "agentMemorySnapshot": true,
            "maxRetries": 0
        })),
        Some(Arc::new(ProtocolTestMemoryHost {
            layout: ProtocolTestMemoryLayout {
                dir: memory_dir.to_string_lossy().into_owned(),
            },
        })),
    );
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let server = Arc::new(ProtocolServer::new(ProtocolServerOptions {
        create_session: make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        lifecycle: None,
        plugin_admin: None,
        credential_store: None,
        input: sr,
        output: sw,
    }));
    let gate = Arc::new(SnapshotWriteTestGate::new());
    let mut entered = gate.subscribe();
    *server.inner.snapshot_write_test_gate.lock().unwrap() = Some(gate.clone());
    let done = tokio::spawn(server.clone().start());
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });
    let mut events = client.events();

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
            client
                .init_session(ProtocolSessionInit {
                    agents: vec![AgentDefinition {
                        name: "snapshot-agent".into(),
                        description: "ordered snapshot protocol fixture".into(),
                        memory: Some(AgentMemoryScope::User),
                        ..Default::default()
                    }],
                    ..Default::default()
                })
                .await
                .expect("init session");

            assert!(client
                .get_agent_memory_snapshot_decisions()
                .await
                .expect("bind session before publication barrier")
                .is_empty());
            let session = server.inner.session.get().cloned().expect("bound session");
            // First hold the queue publication epoch itself. Snapshot live
            // events and correlated success/error/query frames must all join
            // this buffer; a raw unsolicited sender would bypass it.
            let publication_barrier = server.inner.begin_queue_publication_barrier();
            let run_client = client.clone();
            let run = tokio::spawn(async move {
                run_client
                    .run_to_completion(Input::Text("delegate snapshot work".into()), None)
                    .await
            });
            let requested = loop {
                if let Some(audit) = session
                    .agent_memory_snapshot_decisions()
                    .await
                    .expect("snapshot query durability")
                    .into_iter()
                    .find(|audit| {
                        audit.status == AgentMemorySnapshotDecisionStatus::Requested
                    })
                {
                    break audit;
                }
                tokio::task::yield_now().await;
            };
            let request_id = requested.request.request_id.clone();

            let resolve_client = client.clone();
            let resolve_request_id = request_id.clone();
            let mut resolve = tokio::spawn(async move {
                resolve_client
                    .resolve_agent_memory_snapshot_decision(
                        resolve_request_id,
                        AgentMemoryMergeChoice::Keep,
                    )
                    .await
            });
            let authoritative = loop {
                let audit = session
                    .agent_memory_snapshot_decision(&request_id)
                    .await
                    .expect("snapshot query durability")
                    .expect("authoritative snapshot audit");
                if audit.status == AgentMemorySnapshotDecisionStatus::Applied {
                    break audit;
                }
                tokio::task::yield_now().await;
            };
            assert_eq!(
                authoritative.status,
                AgentMemorySnapshotDecisionStatus::Applied
            );
            assert!(
                timeout(Duration::from_millis(100), &mut resolve)
                    .await
                    .is_err(),
                "correlated Applied reply bypassed the active publication barrier"
            );

            let conflict_client = client.clone();
            let conflict_request_id = request_id.clone();
            let mut conflict = tokio::spawn(async move {
                conflict_client
                    .resolve_agent_memory_snapshot_decision(
                        conflict_request_id,
                        AgentMemoryMergeChoice::Merge,
                    )
                    .await
            });
            assert!(
                timeout(Duration::from_millis(100), &mut conflict)
                    .await
                    .is_err(),
                "correlated snapshot conflict error bypassed the active publication barrier"
            );

            let query_client = client.clone();
            let query_request_id = request_id.clone();
            let mut query = tokio::spawn(async move {
                query_client
                    .get_agent_memory_snapshot_decision(query_request_id)
                    .await
            });
            assert!(
                timeout(Duration::from_millis(100), &mut query)
                    .await
                    .is_err(),
                "authoritative Applied query bypassed the active publication barrier"
            );
            let query_all_client = client.clone();
            let mut query_all = tokio::spawn(async move {
                query_all_client.get_agent_memory_snapshot_decisions().await
            });
            assert!(
                timeout(Duration::from_millis(100), &mut query_all)
                    .await
                    .is_err(),
                "authoritative Applied query-all bypassed the active publication barrier"
            );

            publication_barrier.release();
            let entered_requested = loop {
                if let Some(audit) = { entered.borrow().clone() } {
                    break audit;
                }
                entered.changed().await.expect("snapshot writer gate");
            };
            assert_eq!(entered_requested, requested);
            assert!(
                timeout(Duration::from_millis(100), &mut resolve)
                    .await
                    .is_err(),
                "correlated Applied reply overtook delayed Requested"
            );
            assert!(
                timeout(Duration::from_millis(100), &mut conflict)
                    .await
                    .is_err(),
                "correlated snapshot conflict error overtook delayed Requested"
            );
            assert!(
                timeout(Duration::from_millis(100), &mut query)
                    .await
                    .is_err(),
                "authoritative Applied query overtook delayed Requested"
            );
            assert!(
                timeout(Duration::from_millis(100), &mut query_all)
                    .await
                    .is_err(),
                "authoritative Applied query-all overtook delayed Requested"
            );

            gate.release();
            let resolved = resolve.await.unwrap().expect("ordered resolve reply");
            assert_eq!(resolved.status, AgentMemorySnapshotDecisionStatus::Applied);
            let conflict_error = conflict
                .await
                .unwrap()
                .expect_err("different snapshot decision conflicts");
            assert!(
                matches!(conflict_error, ClientError::Op(ref message) if message.contains("snapshot decision conflict")),
                "unexpected ordered snapshot conflict: {conflict_error:?}"
            );
            let queried = query.await.unwrap().expect("ordered query reply");
            assert_eq!(
                queried.audit.as_ref().map(|audit| audit.status),
                Some(AgentMemorySnapshotDecisionStatus::Applied)
            );
            let queried_all = query_all.await.unwrap().expect("ordered query-all reply");
            assert_eq!(
                queried_all
                    .iter()
                    .find(|audit| audit.request.request_id == request_id)
                    .map(|audit| audit.status),
                Some(AgentMemorySnapshotDecisionStatus::Applied)
            );

            let mut statuses = Vec::new();
            let mut saw_live_requested = false;
            let mut saw_live_recorded = false;
            let mut saw_live_applied = false;
            let mut saw_resolve_reply = false;
            let mut saw_query_reply = false;
            let mut saw_query_all_reply = false;
            let mut saw_conflict_error = false;
            while !(saw_live_requested
                && saw_live_recorded
                && saw_live_applied
                && saw_resolve_reply
                && saw_query_reply
                && saw_query_all_reply
                && saw_conflict_error)
            {
                let event = events.next().await.expect("ordered protocol event");
                if event.error.as_ref().is_some_and(|error| {
                    error.try_to_utf8().is_ok_and(|message| {
                        message.contains("snapshot decision conflict")
                    })
                }) {
                    assert!(!event.submission_id.is_empty());
                    assert!(event.agent_memory_snapshot_decision.is_none());
                    assert!(event.agent_memory_snapshot_decision_query.is_none());
                    assert!(event.agent_memory_snapshot_decisions.is_none());
                    assert!(event.error_detail.is_none());
                    saw_conflict_error = true;
                }
                if let Some(audit) = &event.agent_memory_snapshot_decision {
                    if audit.request.request_id == request_id {
                        statuses.push(audit.status);
                        if event.submission_id.is_empty() {
                            saw_live_requested |=
                                audit.status == AgentMemorySnapshotDecisionStatus::Requested;
                            saw_live_recorded |=
                                audit.status == AgentMemorySnapshotDecisionStatus::DecisionRecorded;
                            saw_live_applied |=
                                audit.status == AgentMemorySnapshotDecisionStatus::Applied;
                        } else {
                            saw_resolve_reply |=
                                audit.status == AgentMemorySnapshotDecisionStatus::Applied;
                        }
                    }
                }
                if let Some(result) = &event.agent_memory_snapshot_decision_query {
                    if result.request_id == request_id {
                        let status = result.audit.as_ref().expect("query audit").status;
                        statuses.push(status);
                        saw_query_reply |= status == AgentMemorySnapshotDecisionStatus::Applied;
                    }
                }
                if let Some(audits) = &event.agent_memory_snapshot_decisions {
                    if let Some(audit) = audits
                        .iter()
                        .find(|audit| audit.request.request_id == request_id)
                    {
                        statuses.push(audit.status);
                        saw_query_all_reply |=
                            audit.status == AgentMemorySnapshotDecisionStatus::Applied;
                    }
                }
            }
            let rank = |status| match status {
                AgentMemorySnapshotDecisionStatus::Requested => 0,
                AgentMemorySnapshotDecisionStatus::DecisionRecorded => 1,
                _ => 2,
            };
            assert!(
                statuses
                    .windows(2)
                    .all(|pair| rank(pair[0]) <= rank(pair[1])),
                "snapshot state regressed on the wire: {statuses:?}"
            );
            assert_eq!(
                run.await
                    .unwrap()
                    .expect("run after ordered resolution")
                    .final_text,
                "parent done"
            );
        })
        .await
        .expect("ordered snapshot protocol timed out");

    server.stop();
    done.await.ok();
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn shutdown_invokes_retained_queue_and_task_list_unsubscribers_once() {
    let agent = make_agent(".", vec![], None);
    let (server_output, _client_input) = tokio::io::duplex(4096);
    let server = Arc::new(ProtocolServer::new(ProtocolServerOptions {
        create_session: make_create_session(agent, None),
        lifecycle: None,
        plugin_admin: None,
        credential_store: None,
        input: Box::new(tokio::io::empty()),
        output: Box::new(server_output),
    }));
    let calls = Arc::new(AtomicUsize::new(0));
    let queue_calls = calls.clone();
    *server.inner.queue_unsubscribe.lock().unwrap() = Some(Box::new(move || {
        queue_calls.fetch_add(1, Ordering::SeqCst);
    }));
    let task_calls = calls.clone();
    *server.inner.task_list_unsubscribe.lock().unwrap() = Some(Box::new(move || {
        task_calls.fetch_add(1, Ordering::SeqCst);
    }));

    server.shutdown_and_wait().await.unwrap();
    server.shutdown_and_wait().await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_memory_snapshot_unavailable_settlement_is_correlated_and_queryable() {
    let root = unique_temp_dir("snapshot-protocol-unavailable");
    let memory_dir = root.join("memory");
    let snapshot_dir = memory_dir.join("snapshots");
    std::fs::create_dir_all(&snapshot_dir).unwrap();
    std::fs::write(
        snapshot_dir.join("snapshot.json"),
        r#"{"updatedAt":"2026-07-18T00:00:00Z"}"#,
    )
    .unwrap();
    std::fs::write(snapshot_dir.join("remote.md"), "remote\n").unwrap();
    std::fs::write(memory_dir.join("local.md"), "local\n").unwrap();

    let agent = make_agent_with_memory(
        root.to_str().unwrap(),
        vec![
            tool_use_turn(
                "snapshot-task",
                "task",
                json!({"model": "low",
                    "description": "exercise unavailable settlement",
                    "prompt": "read memory and return",
                    "subagent_type": "snapshot-agent",
                    "run_in_background": false
                }),
            ),
            text_turn("child done"),
            text_turn("parent done"),
        ],
        Some(json!({
            "agentMemory": true,
            "agentMemorySnapshot": true,
            "maxRetries": 0
        })),
        Some(Arc::new(ProtocolTestMemoryHost {
            layout: ProtocolTestMemoryLayout {
                dir: memory_dir.to_string_lossy().into_owned(),
            },
        })),
    );
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        let mut events = client.events();
        client
            .init_session(ProtocolSessionInit {
                agents: vec![AgentDefinition {
                    name: "snapshot-agent".into(),
                    description: "snapshot protocol fixture".into(),
                    memory: Some(AgentMemoryScope::User),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("init session");

        let run_client = client.clone();
        let run = tokio::spawn(async move {
            run_client
                .run_to_completion(Input::Text("delegate snapshot work".into()), None)
                .await
        });
        let requested = loop {
            let event = events.next().await.expect("protocol event");
            if let Some(audit) = event.agent_memory_snapshot_decision.clone() {
                if audit.status == AgentMemorySnapshotDecisionStatus::Requested {
                    break audit;
                }
            }
        };
        let request_id = requested.request.request_id.clone();
        let error = RuntimeString::from("desktop handler unavailable");
        let settled = client
            .settle_agent_memory_snapshot_unavailable(
                request_id.clone(),
                AgentMemorySnapshotUnavailableReason::NoHandler,
                Some(error.clone()),
            )
            .await
            .expect("settle unavailable request");
        assert_eq!(settled.status, AgentMemorySnapshotDecisionStatus::NoHandler);
        assert_eq!(settled.decision, None);
        assert_eq!(settled.source, None);
        assert_eq!(settled.error, Some(error));
        assert!(settled.ended_at.is_some());
        assert_eq!(
            client
                .get_agent_memory_snapshot_decision(request_id)
                .await
                .expect("query unavailable audit")
                .audit,
            Some(settled)
        );
        assert!(!memory_dir.join(".snapshot-synced.json").exists());

        let result = run.await.unwrap().expect("run completes after settlement");
        assert_eq!(result.final_text, "parent done");
    })
    .await
    .expect("snapshot unavailable settlement timed out");

    server.stop();
    done.await.ok();
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_maintenance_events_are_ordered_and_snapshot_is_queryable() {
    let dir = unique_temp_dir("memory-maintenance-events");
    let memory_dir = dir.join("memory");
    std::fs::create_dir_all(&memory_dir).unwrap();
    let agent = make_agent_with_memory(
        dir.to_str().unwrap(),
        vec![text_turn("done"), text_turn("memory done")],
        Some(json!({"agentMemory": true, "autoMemory": true})),
        Some(Arc::new(ProtocolTestMemoryHost {
            layout: ProtocolTestMemoryLayout {
                dir: memory_dir.to_string_lossy().into_owned(),
            },
        })),
    );
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        let mut events = client.events();
        client
            .run_to_completion(Input::Text("trigger maintenance gates".into()), None)
            .await
            .expect("run");
        let mut lifecycle = Vec::new();
        while lifecycle.len() < 2 {
            let event = events.next().await.expect("protocol event");
            if let Some(maintenance) = event.memory_maintenance_event.clone() {
                assert!(event.submission_id.is_empty());
                lifecycle.push(maintenance);
            }
        }
        assert!(matches!(
            &lifecycle[0],
            MemoryMaintenanceEvent::AutoMemory { run }
                if run.status == MemoryMaintenanceStatus::Started
                    && run.reason.is_none()
        ));
        assert!(matches!(
            &lifecycle[1],
            MemoryMaintenanceEvent::AutoMemory { run }
                if run.status == MemoryMaintenanceStatus::Completed
                    && run.reason.is_none()
        ));
        let started_id = match &lifecycle[0] {
            MemoryMaintenanceEvent::AutoMemory { run } => run.run_id.clone(),
            _ => unreachable!(),
        };
        let completed_id = match &lifecycle[1] {
            MemoryMaintenanceEvent::AutoMemory { run } => run.run_id.clone(),
            _ => unreachable!(),
        };
        assert_eq!(started_id, completed_id);

        let snapshot = client
            .get_memory_maintenance()
            .await
            .expect("maintenance snapshot");
        assert_eq!(snapshot.auto_memory_runs.len(), 1);
        assert!(snapshot.auto_dream_runs.is_empty());
        assert_eq!(snapshot.auto_memory_runs[0].run_id, completed_id);
        assert_eq!(
            snapshot.auto_memory_runs[0].status,
            MemoryMaintenanceStatus::Completed
        );
    })
    .await
    .expect("timed out");

    server.stop();
    done.await.ok();
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_persistence_panic_is_a_sticky_typed_protocol_error() {
    let dir = unique_temp_dir("memory-maintenance-query-fatal");
    let memory_dir = dir.join("memory");
    std::fs::create_dir_all(&memory_dir).unwrap();
    let agent = make_agent_with_memory(
        dir.to_str().unwrap(),
        vec![text_turn("done"), text_turn("must not run")],
        Some(json!({"agentMemory": true, "autoMemory": true})),
        Some(Arc::new(ProtocolTestMemoryHost {
            layout: ProtocolTestMemoryLayout {
                dir: memory_dir.to_string_lossy().into_owned(),
            },
        })),
    );
    let rollout = Arc::new(PanickingMaintenanceRollout::default());
    let rollout_sink: RolloutSink = rollout.clone();
    let create_session = make_create_session_with_recovery(
        agent,
        Some(ExternalPermissionMode::BypassPermissions),
        None,
        None,
        Some(rollout_sink),
    );
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(create_session, sr, sw);
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        client
            .run_to_completion(Input::Text("trigger maintenance".into()), None)
            .await
            .expect("main turn completes before background maintenance");
        rollout.wait_until_append_panicked().await;

        let first = loop {
            match client.get_memory_maintenance().await {
                Err(crate::client::ClientError::MemoryMaintenance { message, detail }) => {
                    break (message, detail);
                }
                Ok(_) => tokio::task::yield_now().await,
                Err(other) => panic!("expected typed maintenance error, got {other:?}"),
            }
        };
        assert!(first.0.contains("memory maintenance"), "{}", first.0);
        assert_eq!(
            first.1.code,
            crate::types::MemoryMaintenanceQueryErrorCode::PersistenceInconsistent
        );
        assert_eq!(
            first.1.run_kind,
            Some(crate::types::MemoryMaintenanceRunKind::AutoMemory)
        );
        assert!(first.1.run_id.is_some());

        let mut query_events = client.events();
        let second = client
            .get_memory_maintenance()
            .await
            .expect_err("the first persistence failure remains sticky");
        match second {
            crate::client::ClientError::MemoryMaintenance { message, detail } => {
                assert_eq!(message, first.0);
                assert_eq!(detail, first.1);
            }
            other => panic!("expected the same typed maintenance error, got {other:?}"),
        }
        let error_frame = loop {
            let event = query_events.next().await.expect("query error frame");
            if event.error.is_some() {
                break event;
            }
        };
        assert!(error_frame.memory_maintenance.is_none());
        assert!(matches!(
            error_frame.error_detail.as_ref(),
            Some(crate::types::ProtocolErrorDetail::MemoryMaintenance(detail))
                if detail == &first.1
        ));
    })
    .await
    .expect("typed maintenance query timed out");

    server.stop();
    done.await.ok();
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn memory_maintenance_snapshot_is_frozen_before_ordered_writer_backpressure() {
    let state = Arc::new(Mutex::new(MemoryMaintenanceSnapshot::default()));
    let snapshot = state.lock().unwrap().clone();
    let old_query = UnsolicitedWrite::MemoryMaintenanceSnapshot {
        submission_id: "maintenance-query".into(),
        snapshot,
    };
    let completed = AutoMemoryRun {
        run_id: "run-after-enqueue".into(),
        session_id: "session-1".into(),
        trigger: MemoryMaintenanceTrigger::Auto,
        scope: AgentMemoryScope::Project,
        target_dir: "/memory".into(),
        status: MemoryMaintenanceStatus::Completed,
        started_at: 1,
        ended_at: Some(2),
        reason: None,
        error: None,
        message_count: 1,
        cursor_message_uuid: Some("cursor-1".into()),
        files_touched: Vec::new(),
        turn_count: Some(1),
    };
    let mut started = completed.clone();
    started.status = MemoryMaintenanceStatus::Started;
    started.ended_at = None;
    started.cursor_message_uuid = None;
    started.turn_count = None;
    state
        .lock()
        .unwrap()
        .auto_memory_runs
        .push(completed.clone());

    // The writer is notionally backpressured until all three admissions are
    // queued. It must still emit the old query before Started/Completed;
    // the query payload cannot be revised to terminal at write time.
    let writes = vec![
        old_query,
        UnsolicitedWrite::Event(ProtocolEvent {
            memory_maintenance_event: Some(MemoryMaintenanceEvent::AutoMemory { run: started }),
            ..Default::default()
        }),
        UnsolicitedWrite::Event(ProtocolEvent {
            memory_maintenance_event: Some(MemoryMaintenanceEvent::AutoMemory {
                run: completed.clone(),
            }),
            ..Default::default()
        }),
    ];
    let events = writes
        .into_iter()
        .map(UnsolicitedWrite::into_event)
        .collect::<Vec<_>>();
    assert_eq!(events[0].submission_id, "maintenance-query");
    assert!(events[0]
        .memory_maintenance
        .clone()
        .expect("snapshot reply")
        .auto_memory_runs
        .is_empty());
    assert!(matches!(
        events[1].memory_maintenance_event.as_ref(),
        Some(MemoryMaintenanceEvent::AutoMemory { run })
            if run.status == MemoryMaintenanceStatus::Started
    ));
    assert!(matches!(
        events[2].memory_maintenance_event.as_ref(),
        Some(MemoryMaintenanceEvent::AutoMemory { run })
            if run.status == MemoryMaintenanceStatus::Completed
    ));

    let new_query = UnsolicitedWrite::MemoryMaintenanceSnapshot {
        submission_id: "maintenance-query-new".into(),
        snapshot: state.lock().unwrap().clone(),
    }
    .into_event();
    assert_eq!(
        new_query
            .memory_maintenance
            .expect("new snapshot reply")
            .auto_memory_runs,
        vec![completed]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_cost_exposes_restored_usage_ledger() {
    let agent = make_agent(".", vec![text_turn("resumed")], None);
    let usage = Usage {
        input_tokens: 1_000,
        output_tokens: 250,
        cache_read_input_tokens: 500,
        cache_creation_input_tokens: 125,
        total_tokens: None,
    };
    let create_session = make_create_session_with_usage_ledger(
        agent,
        Some(ExternalPermissionMode::BypassPermissions),
        Some(vec![UsageLedgerItem {
            inference: None,
            request_id: "request-before-restart".into(),
            assistant_message_id: "assistant-before-restart".into(),
            model: "historical-model".into(),
            usage: usage.clone(),
            cost_usd: 1.25,
        }]),
    );
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(create_session, sr, sw);
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        client
            .run_to_completion(Input::Text("continue".into()), None)
            .await
            .expect("run");
        let cost = client.get_cost().await.expect("cost");
        assert_eq!(cost.total_cost_usd, 1.25);
        let expected_cache_hit_rate = 500.0 / 1_625.0;
        let restored_snapshot = cost
            .models
            .iter()
            .find(|entry| entry.totals.model == "historical-model")
            .expect("restored model");
        let restored = &restored_snapshot.totals;
        assert_eq!(restored.model, "historical-model");
        assert_eq!(restored.requests, 1);
        assert_eq!(restored.input_tokens, usage.input_tokens);
        assert_eq!(restored.output_tokens, usage.output_tokens);
        assert_eq!(
            restored.cache_read_input_tokens,
            usage.cache_read_input_tokens
        );
        assert_eq!(
            restored.cache_creation_input_tokens,
            usage.cache_creation_input_tokens
        );
        assert_eq!(restored.cost_usd, 1.25);
        assert!((restored_snapshot.cache_hit_rate - expected_cache_hit_rate).abs() < 1e-10);
        assert!(cost
            .models
            .iter()
            .filter(|entry| entry.totals.model != "historical-model")
            .all(|entry| entry.totals.cost_usd == 0.0));
    })
    .await
    .expect("timed out");

    server.stop();
    done.await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_subagent_lifecycle_returns_all_kinds_and_recovered_status() {
    let agent = make_agent(".", vec![text_turn("resumed")], None);
    let context = |description: &str| SubagentContext {
        session_id: "session-1".into(),
        parent_agent_id: Some("parent-agent".into()),
        parent_thread_id: Some("parent-thread".into()),
        agent_type: Some("general-purpose".into()),
        cwd: "/workspace".into(),
        description: description.into(),
        depth: 2,
        resume_from: None,
        spawn_turn_id: None,
    };
    let completed = SubagentLifecycleRecord {
        agent_id: "foreground-completed".into(),
        kind: SubagentKind::Foreground,
        status: SubagentStatus::Completed,
        context: context("completed foreground"),
        started_at_ms: 10,
        updated_at_ms: 20,
        completed_at_ms: Some(20),
        last_result: Some("done".into()),
        last_error: None,
        cancellation_requested: false,
        model: None,
        model_source: None,
    };
    let running = SubagentLifecycleRecord {
        agent_id: "background-running".into(),
        kind: SubagentKind::Background,
        status: SubagentStatus::Running,
        context: context("running background"),
        started_at_ms: 30,
        updated_at_ms: 40,
        completed_at_ms: None,
        last_result: None,
        last_error: None,
        cancellation_requested: false,
        model: None,
        model_source: None,
    };
    let failed = SubagentLifecycleRecord {
        agent_id: "workflow-failed".into(),
        kind: SubagentKind::Workflow,
        status: SubagentStatus::Failed,
        context: context("failed workflow"),
        started_at_ms: 50,
        updated_at_ms: 60,
        completed_at_ms: Some(60),
        last_result: None,
        last_error: Some("failed".into()),
        cancellation_requested: false,
        model: None,
        model_source: None,
    };
    let idle = SubagentLifecycleRecord {
        agent_id: "persistent-idle".into(),
        kind: SubagentKind::Persistent,
        status: SubagentStatus::Idle,
        context: context("idle persistent"),
        started_at_ms: 70,
        updated_at_ms: 80,
        completed_at_ms: None,
        last_result: Some("last turn".into()),
        last_error: None,
        cancellation_requested: false,
        model: None,
        model_source: None,
    };
    let create_session = make_create_session_with_recovery(
        agent,
        Some(ExternalPermissionMode::BypassPermissions),
        None,
        Some(vec![completed, running, failed, idle]),
        None,
    );
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(create_session, sr, sw);
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        client
            .run_to_completion(Input::Text("continue".into()), None)
            .await
            .expect("run");
        let snapshot = client
            .get_subagent_lifecycle()
            .await
            .expect("subagent lifecycle");
        assert_eq!(snapshot.agents.len(), 4);
        assert_eq!(snapshot.agents[0].agent_id, "background-running");
        assert_eq!(snapshot.agents[0].kind, SubagentKind::Background);
        assert_eq!(snapshot.agents[0].status, SubagentStatus::Interrupted);
        assert_eq!(snapshot.agents[1].agent_id, "foreground-completed");
        assert_eq!(snapshot.agents[1].status, SubagentStatus::Completed);
        assert_eq!(
            snapshot.agents[1].last_result.as_ref(),
            Some(&RuntimeString::from("done"))
        );
        assert_eq!(snapshot.agents[2].agent_id, "persistent-idle");
        assert_eq!(snapshot.agents[2].kind, SubagentKind::Persistent);
        assert_eq!(snapshot.agents[2].status, SubagentStatus::Interrupted);
        assert_eq!(
            snapshot.agents[2].last_result.as_ref(),
            Some(&RuntimeString::from("last turn"))
        );
        assert_eq!(snapshot.agents[3].agent_id, "workflow-failed");
        assert_eq!(snapshot.agents[3].kind, SubagentKind::Workflow);
        assert_eq!(snapshot.agents[3].status, SubagentStatus::Failed);
        assert_eq!(
            snapshot.agents[3].last_error.as_ref(),
            Some(&RuntimeString::from("failed"))
        );
        assert!(client
            .get_subagents()
            .await
            .expect("legacy subagents")
            .is_empty());
    })
    .await
    .expect("timed out");

    server.stop();
    done.await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_ops_round_trip_through_protocol_handlers() {
    let dir = unique_temp_dir("lifecycle");
    let session_cwd = dir.to_string_lossy().into_owned();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    write_persisted_session(&store, "thread-1", 1_700_000_000).await;
    let lifecycle_rollout = store.reopen("thread-1").await.unwrap();
    lifecycle_rollout
        .append_subagent_lifecycle(SubagentLifecycleRecord {
            agent_id: "persisted-foreground".into(),
            kind: SubagentKind::Foreground,
            status: SubagentStatus::Completed,
            context: SubagentContext {
                session_id: "thread-1".into(),
                parent_agent_id: None,
                parent_thread_id: Some("thread-1".into()),
                agent_type: Some("research".into()),
                cwd: "/tmp/klaus-proto".into(),
                description: "persisted protocol child".into(),
                depth: 1,
                resume_from: None,
                spawn_turn_id: None,
            },
            started_at_ms: 1_700_000_001,
            updated_at_ms: 1_700_000_002,
            completed_at_ms: Some(1_700_000_002),
            last_result: Some("persisted result".into()),
            last_error: None,
            cancellation_requested: false,
            model: None,
            model_source: None,
        })
        .await;
    lifecycle_rollout.shutdown().await.unwrap();
    let agent_rollout = store
        .open_sub_agent("thread-1", "agent-A", sub_meta(1_700_000_001))
        .await
        .unwrap();
    agent_rollout
        .append_message(user_msg("agent-u", "child transcript"))
        .await;
    agent_rollout.shutdown().await.unwrap();

    let agent = make_agent(
        "/tmp/klaus-proto",
        vec![
            text_turn("resumed reply"),
            text_turn("side answer"),
            text_turn(r#"{"title":"Protocol lifecycle"}"#),
            text_turn("dream done"),
        ],
        None,
    );
    let lifecycle = protocol_lifecycle(agent.clone(), store.clone());
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server_with_lifecycle(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        Some(lifecycle),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
            let init = client
                .init_session(ProtocolSessionInit {
                    cwd: Some(session_cwd.clone().into()),
                    config: Some(
                        runtime_json_from_scalar_value(&json!({ "contextWindow": 65_536 })).unwrap(),
                    ),
                    permission_mode: Some(ExternalPermissionMode::Plan),
                    ..Default::default()
                })
                .await
                .expect("init");
            assert!(init.accepted);

            let page = client.list_sessions(None).await.expect("list");
            assert_eq!(
                page.items.iter().map(|t| t.id.clone()).collect::<Vec<_>>(),
                vec![RuntimeString::from("thread-1")]
            );

            let transcript = client
                .load_subagent_transcript("thread-1".into(), "agent-A".into())
                .await
                .expect("transcript");
            assert_eq!(transcript.messages.len(), 1);

            let title = client
                .update_title("thread-1".into(), "Renamed".into())
                .await
                .expect("title");
            assert_eq!(title.title, "Renamed");
            let renamed = client.list_sessions(None).await.expect("list renamed");
            assert_eq!(
                renamed.items[0].title.as_ref(),
                Some(&RuntimeString::from("Renamed"))
            );

            let resumed = client
                .resume_session("thread-1".into())
                .await
                .expect("resume");
            assert_eq!(resumed.thread_id, "thread-1");
            let context = client.context_snapshot().await.expect("context");
            assert_eq!(context.thread_id, "thread-1");
            assert_eq!(context.cwd, session_cwd);
            assert_eq!(context.context_window, 65_536);
            let lifecycle = client
                .get_subagent_lifecycle()
                .await
                .expect("persisted lifecycle");
            assert_eq!(lifecycle.agents.len(), 1);
            assert_eq!(lifecycle.agents[0].agent_id, "persisted-foreground");
            assert_eq!(lifecycle.agents[0].status, SubagentStatus::Completed);
            assert_eq!(
                lifecycle.agents[0].last_result.as_ref(),
                Some(&RuntimeString::from("persisted result"))
            );
            assert!(client
                .get_subagents()
                .await
                .expect("legacy background snapshot")
                .is_empty());
            let mut compact_events = client.events();
            let compact_client = client.clone();
            let compact_task = tokio::spawn(async move {
                compact_client
                    .compact_session(Some(RuntimeString::from_utf16(vec![
                        0x0070, 0xd800, 0x0072, 0xdc00,
                    ])))
                    .await
            });
            let mut progress = Vec::new();
            while progress.len() < 2 {
                let event = compact_events.next().await.expect("compact event stream");
                if let Some(item) = &event.compact_progress {
                    progress.push(item.clone());
                }
            }
            let compact = compact_task.await.unwrap().expect("compact");
            assert!(!compact.compacted);
            assert_eq!(
                compact.reason,
                Some(klaus_agent_core::context::compact::CompactionNoopReason::CompactorDisabled)
            );
            assert!(compact.diagnostics.is_empty());
            assert_eq!(compact.before_tokens, compact.after_tokens);
            assert_eq!(compact.before_messages, compact.after_messages);
            assert!(compact.boundary.is_none());
            assert_eq!(progress[0].phase, CompactProgressPhase::Started);
            assert_eq!(progress[1].phase, CompactProgressPhase::Completed);
            assert!(!progress[0].id.is_empty());
            assert_eq!(progress[0].id, progress[1].id);
            assert_eq!(progress[0].trigger, CompactTrigger::Manual);
            assert_eq!(progress[0].mode, CompactionMode::Full);
            assert_eq!(progress[1].result.as_ref(), Some(&compact));
            assert!(progress[0].result.is_none());
            let result = client
                .run_to_completion(Input::Text("continue".into()), None)
                .await
                .expect("run");
            assert_eq!(result.final_text, "resumed reply");
            let history_len = client.get_history().await.expect("history after run").len();
            let side = client
                .side_question("what is the state?".into())
                .await
                .expect("side question");
            assert_eq!(side.response, "side answer");
            assert!(!side.synthetic);
            let generated = client
                .generate_title(None, None, Some(true))
                .await
                .expect("generate title");
            assert_eq!(generated.thread_id, "thread-1");
            assert_eq!(
                generated.title.title.as_ref(),
                Some(&RuntimeString::from("Protocol lifecycle"))
            );
            assert!(generated.title.persisted);
            let after_helpers = client.get_history().await.expect("history after helpers");
            assert_eq!(after_helpers.len(), history_len);
            let generated_title = client.list_sessions(None).await.expect("list generated title");
            assert_eq!(
                generated_title.items[0].title.as_ref(),
                Some(&RuntimeString::from("Protocol lifecycle"))
            );
            let dream = client.dream_now().await.expect("dream");
            assert!(!dream.ok);
            assert_eq!(
                dream.reason,
                Some(klaus_agent_core::engine::session::DreamNowReason::GateClosed)
            );

            let second_resume = client.resume_session("thread-1".into()).await;
            assert!(matches!(second_resume, Err(ClientError::Op(message)) if message.contains("already bound")));
        })
        .await
        .expect("timed out");

    server.stop();
    done.await.ok();
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn context_snapshot_round_trips_with_exhausted_or_small_input_capacity() {
    for (window, auto_window, threshold, reserve, fraction) in [
        (1_u64, None, 0_u64, 1_u64, 0.0),
        (2_048, None, 1_843, 103, 1_843.0 / 1_945.0),
        (8_192, None, 7_372, 410, 7_372.0 / 7_782.0),
        (8_193, None, 7_373, 410, 7_373.0 / 7_783.0),
        (200_000, Some(2_048), 2_048, 10_000, 2_048.0 / 190_000.0),
        (65_536, None, 58_982, 3_277, 58_982.0 / 62_259.0),
    ] {
        let mut config = json!({
            "contextWindow": window,
            "maxOutputTokens": 1,
            "outputReserveTokens": 20_000,
            "compactBufferTokens": 13_000,
            "compactThreshold": null,
            "autoCompaction": true,
            "prewarm": false,
        });
        if let Some(auto_window) = auto_window {
            config["autoCompactWindow"] = json!(auto_window);
        }
        let agent = make_agent("/tmp/klaus-proto", Vec::new(), Some(config));
        let ((sr, sw), (cr, cw)) = duplex_pair();
        let (server, done) = start_server(
            make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
            sr,
            sw,
        );
        let client = ProtocolClient::new(ProtocolClientOptions {
            input: cr,
            output: cw,
        });
        timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
            let session = server.inner.ensure_session().await.expect("session");
            let snapshot = session.context_snapshot();
            let encoded = runtime_codec::encode_text(&snapshot).expect("snapshot encodes");
            assert!(
                snapshot.compact_threshold.is_finite(),
                "non-finite context gauge: ratio={:?}, window={}, reserve={}, threshold={}, wire={}",
                snapshot.compact_threshold,
                snapshot.context_window,
                snapshot.output_reserve_tokens,
                snapshot.compact_threshold_tokens,
                encoded.try_to_utf8().expect("snapshot JSON is scalar text")
            );
            assert_eq!(snapshot.compact_threshold, fraction);
            assert_eq!(snapshot.compact_threshold_tokens, threshold);
            assert_eq!(snapshot.context_window, window);
            assert_eq!(snapshot.output_reserve_tokens, reserve);
            assert_eq!(snapshot.total_tokens, 0);
            let decoded: klaus_agent_core::engine::session::SessionContextSnapshot =
                runtime_codec::decode_text(&encoded).expect("snapshot text decodes");
            assert_eq!(decoded.compact_threshold, fraction);
            let wire = client
                .context_snapshot()
                .await
                .expect("protocol context reply");
            assert_eq!(wire.context_window, window);
            assert_eq!(wire.output_reserve_tokens, reserve);
            assert_eq!(wire.compact_threshold_tokens, threshold);
            assert_eq!(wire.compact_threshold, fraction);
            assert!(wire.compact_threshold.is_finite());
            assert!(client
                .get_history()
                .await
                .expect("reader remains live")
                .is_empty());

            let mut malformed = snapshot;
            malformed.compact_threshold = f64::NAN;
            let malformed = runtime_codec::encode_text(&malformed).expect("native number encodes");
            assert!(
                runtime_codec::decode_text::<
                    klaus_agent_core::engine::session::SessionContextSnapshot,
                >(&malformed)
                .is_err(),
                "wire decoding must continue to reject a non-number gauge"
            );
        })
        .await
        .expect("snapshot round trip timed out");
        server.stop();
        done.await.ok();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compact_failure_emits_failed_progress_before_the_correlated_error() {
    let turn_started = Arc::new(Notify::new());
    let turn_cancelled = Arc::new(Notify::new());
    let agent = make_blocking_agent(
        "/tmp/klaus-proto",
        turn_started.clone(),
        turn_cancelled.clone(),
    );
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });
    let session = timeout(CROSS_TASK_LIVENESS_TIMEOUT, server.inner.ensure_session())
        .await
        .expect("session creation timed out")
        .unwrap();
    let running_session = session.clone();
    let running_turn = tokio::spawn(async move {
        running_session
            .run(Input::Text("hold turn open".into()), TurnOptions::default())
            .await
    });
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, turn_started.notified())
        .await
        .expect("turn did not start");
    let mut events = client.events();

    let error = timeout(CROSS_TASK_LIVENESS_TIMEOUT, client.compact_session(None))
        .await
        .expect("compact failure response timed out")
        .unwrap_err();
    let mut progress = Vec::new();
    while progress.len() < 2 {
        let event = timeout(CROSS_TASK_LIVENESS_TIMEOUT, events.next())
            .await
            .expect("compact progress event timed out")
            .expect("compact event stream");
        if let Some(item) = event.compact_progress.clone() {
            progress.push(item);
        }
    }

    assert!(matches!(error, ClientError::Op(message) if message.contains("turn is in progress")));
    assert_eq!(progress[0].phase, CompactProgressPhase::Started);
    assert_eq!(progress[1].phase, CompactProgressPhase::Failed);
    assert_eq!(progress[0].id, progress[1].id);
    assert!(progress[1]
        .error
        .as_ref()
        .expect("compact failure text")
        .contains("turn is in progress"));
    assert!(progress[1].result.is_none());

    session.interrupt();
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, turn_cancelled.notified())
        .await
        .expect("turn was not cancelled");
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, running_turn)
        .await
        .expect("turn did not stop")
        .unwrap();
    server.stop();
    done.await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compact_session_creation_failure_still_emits_failed_progress() {
    let create_session: CreateSession = Arc::new(|_args: CreateSessionArgs| {
        Box::pin(async { Err::<Arc<Session>, BoxError>("session creation failed".into()) })
            as CreateSessionFuture
    });
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(create_session, sr, sw);
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });
    let mut events = client.events();

    let error = timeout(CROSS_TASK_LIVENESS_TIMEOUT, client.compact_session(None))
        .await
        .expect("compact failure response timed out")
        .unwrap_err();
    let first = timeout(CROSS_TASK_LIVENESS_TIMEOUT, events.next())
        .await
        .expect("compact start event timed out")
        .expect("compact event stream");
    let second = timeout(CROSS_TASK_LIVENESS_TIMEOUT, events.next())
        .await
        .expect("compact failed event timed out")
        .expect("compact event stream");
    let progress = [
        first
            .compact_progress
            .clone()
            .expect("compact started progress"),
        second
            .compact_progress
            .clone()
            .expect("compact failed progress"),
    ];

    assert!(
        matches!(error, ClientError::Op(message) if message.contains("session creation failed"))
    );
    assert_eq!(progress[0].phase, CompactProgressPhase::Started);
    assert_eq!(progress[1].phase, CompactProgressPhase::Failed);
    assert_eq!(progress[0].id, progress[1].id);
    assert!(progress[1]
        .error
        .as_ref()
        .expect("compact failure text")
        .contains("session creation failed"));
    assert!(progress[1].result.is_none());

    server.stop();
    done.await.ok();
}

// The review host entry: `review_session` round-trips through the protocol
// handler, parses the reviewer's structured verdict, and leaves the main
// conversation history untouched.
//
// Uses an explicit runtime with a widened worker stack: full-session
// protocol round-trips exceed the default 2 MiB tokio worker stack in
// unoptimized builds (the pre-existing lifecycle round-trip test hits the
// same limit on such hosts).
#[test]
fn review_session_op_round_trips_and_keeps_main_history_isolated() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_stack_size(16 * 1024 * 1024)
        .enable_all()
        .build()
        .expect("build runtime");
    runtime.block_on(review_session_round_trip_body());
}

async fn review_session_round_trip_body() {
    const REVIEW_JSON: &str = r#"{
          "findings": [
            {
              "title": "[P1] Off-by-one in range check",
              "body": "The loop bound skips the final element.",
              "confidence_score": 0.9,
              "priority": 1,
              "code_location": {
                "absolute_file_path": "/repo/src/lib.rs",
                "line_range": {"start": 10, "end": 12}
              }
            }
          ],
          "overall_correctness": "patch is incorrect",
          "overall_explanation": "One qualifying bug was found.",
          "overall_confidence_score": 0.8
        }"#;
    let agent = make_agent("/tmp/klaus-proto", vec![text_turn(REVIEW_JSON)], None);
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        let review = client
            .review_session(
                klaus_agent_core::engine::session::ReviewTarget::UncommittedChanges,
                None,
            )
            .await
            .expect("review");
        assert_eq!(review.output.findings.len(), 1);
        assert_eq!(
            review.output.findings[0].title,
            "[P1] Off-by-one in range check"
        );
        assert_eq!(review.output.findings[0].priority, Some(1));
        assert_eq!(review.output.overall_correctness, "patch is incorrect");
        assert_eq!(review.user_facing_hint, "current changes");

        // The review turn never wrote into the main conversation history.
        let history_after = client.get_history().await.expect("history after").len();
        assert_eq!(history_after, 0);
    })
    .await
    .expect("timed out");

    server.stop();
    done.await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readonly_side_question_protocol_delivers_blocks_and_rejects_invalid_payload() {
    use klaus_agent_core::engine::session::UserContentPart;
    let requests = Arc::new(Mutex::new(Vec::new()));
    let agent = make_recording_agent("/tmp/klaus-proto", requests.clone());
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });
    client
        .run_to_completion(Input::Text("parent".into()), None)
        .await
        .unwrap();
    let before = client.get_history().await.unwrap();
    let parts = vec![
        UserContentPart::Text {
            text: "parsed text".into(),
        },
        UserContentPart::Image {
            base64: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScLbtAAAAABJRU5ErkJggg==".into(),
            mime_type: "image/png".into(),
            source_path: None,
        },
        UserContentPart::Document {
            base64: "JVBERi0xLjQ=".into(),
            mime_type: "application/pdf".into(),
            filename: Some("report.pdf".into()),
        },
    ];
    assert_eq!(
        client
            .side_question_input(Input::Parts(parts))
            .await
            .unwrap()
            .response,
        "ok"
    );
    assert!(client
        .side_question_input(Input::Parts(vec![UserContentPart::Image {
            base64: "not base64".into(),
            mime_type: "image/png".into(),
            source_path: None,
        },]))
        .await
        .is_err());
    assert_eq!(client.get_history().await.unwrap(), before);
    {
        let calls = requests.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert!(calls[1].tools.is_empty());
        assert_eq!(calls[1].skip_cache_write, Some(true));
        let Message::User(user) = calls[1].messages.last().unwrap() else {
            panic!("user");
        };
        assert!(
            matches!(&user.content[2], ContentBlock::Image(image) if image.base64 == "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScLbtAAAAABJRU5ErkJggg==")
        );
        assert!(
            matches!(&user.content[3], ContentBlock::Document(document) if document.base64 == "JVBERi0xLjQ=")
        );
    }
    server.stop();
    done.await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readonly_helpers_do_not_block_protocol_and_are_independently_cancellable() {
    let helper_started = Arc::new(Notify::new());
    let helper_cancelled = Arc::new(Notify::new());
    let agent = make_seed_then_blocking_agent(
        "/tmp/klaus-proto",
        helper_started.clone(),
        helper_cancelled.clone(),
    );
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });
    client
        .run_to_completion(Input::Text("seed".into()), None)
        .await
        .expect("seed turn");
    let history_len = client.get_history().await.expect("seed history").len();

    let main_client = client.clone();
    let mut main_task = tokio::spawn(async move {
        main_client
            .run_to_completion(Input::Text("keep the main turn running".into()), None)
            .await
    });
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, helper_started.notified())
        .await
        .expect("main turn did not start");

    let side_signal = AbortSignal::new();
    let side_client = client.clone();
    let side_task = {
        let signal = side_signal.clone();
        tokio::spawn(async move {
            side_client
                .side_question_input_with_signal(
                    Input::Parts(vec![
                        klaus_agent_core::engine::session::UserContentPart::Text {
                            text: "what is happening?".into(),
                        },
                        klaus_agent_core::engine::session::UserContentPart::Image {
                            base64: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScLbtAAAAABJRU5ErkJggg==".into(),
                            mime_type: "image/png".into(),
                            source_path: None,
                        },
                    ]),
                    signal,
                )
                .await
        })
    };
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, helper_started.notified())
        .await
        .expect("side question did not start");
    let history = timeout(Duration::from_millis(500), client.get_history())
        .await
        .expect("side question blocked the protocol input loop")
        .expect("history while side question runs");
    assert!(history.len() >= history_len);
    let running_history_len = history.len();
    side_signal.cancel();
    let side_error = timeout(CROSS_TASK_LIVENESS_TIMEOUT, side_task)
        .await
        .expect("side question did not cancel")
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(side_error, ClientError::Op(message) if message.contains("side question cancelled"))
    );
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, helper_cancelled.notified())
        .await
        .expect("side question provider did not receive cancellation");
    assert_eq!(
        client
            .get_history()
            .await
            .expect("history after side cancellation")
            .len(),
        running_history_len
    );
    assert!(
        timeout(Duration::from_millis(200), &mut main_task)
            .await
            .is_err(),
        "cancelling the side question interrupted the main turn"
    );
    client.interrupt().await;
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, helper_cancelled.notified())
        .await
        .expect("main turn provider did not receive interrupt");
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, main_task)
        .await
        .expect("main turn did not stop")
        .unwrap()
        .expect("main turn result");
    let history_len = client
        .get_history()
        .await
        .expect("history after interrupted main turn")
        .len();

    let title_signal = AbortSignal::new();
    let title_client = client.clone();
    let title_task = {
        let signal = title_signal.clone();
        tokio::spawn(async move {
            title_client
                .generate_title_with_signal(None, None, Some(true), signal)
                .await
        })
    };
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, helper_started.notified())
        .await
        .expect("title generation did not start");
    let context = timeout(Duration::from_millis(500), client.context_snapshot())
        .await
        .expect("title generation blocked the protocol input loop")
        .expect("context while title generation runs");
    assert_eq!(context.history_messages, history_len);
    title_signal.cancel();
    let title_error = timeout(CROSS_TASK_LIVENESS_TIMEOUT, title_task)
        .await
        .expect("title generation did not cancel")
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(title_error, ClientError::Op(message) if message.contains("title generation cancelled"))
    );
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, helper_cancelled.notified())
        .await
        .expect("title provider did not receive cancellation");
    assert_eq!(
        client.get_history().await.expect("final history").len(),
        history_len
    );

    server.stop();
    done.await.ok();
}

#[tokio::test]
async fn cancelled_readonly_request_cleanup_preserves_a_reused_submission_id() {
    let agent = make_agent("/tmp/klaus-proto", vec![], None);
    let ((sr, sw), (_cr, _cw)) = duplex_pair();
    let (server, done) = start_server(make_create_session(agent, None), sr, sw);

    let first = server
        .inner
        .begin_readonly_request("reused")
        .expect("first request");
    server.inner.cancel_readonly_request("reused");
    assert!(server.inner.begin_readonly_request("reused").is_err());
    server.inner.finish_readonly_request("reused", &first);
    let second = server
        .inner
        .begin_readonly_request("reused")
        .expect("reused request");

    server.inner.finish_readonly_request("reused", &first);
    let current = server
        .inner
        .pending_readonly
        .lock()
        .unwrap()
        .get("reused")
        .cloned()
        .expect("new request must remain registered");
    assert!(Arc::ptr_eq(&current, &second));
    assert!(!second.signal.is_cancelled());

    server.inner.finish_readonly_request("reused", &second);
    assert!(server.inner.pending_readonly.lock().unwrap().is_empty());
    server.stop();
    done.await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dream_now_emits_terminal_completion_with_real_fork_outputs() {
    let root = unique_temp_dir("dream-completion");
    let memory_dir = root.join("memory");
    std::fs::create_dir_all(&memory_dir).unwrap();
    let host: Arc<dyn AgentMemoryHost> = Arc::new(ProtocolTestMemoryHost {
        layout: ProtocolTestMemoryLayout {
            dir: memory_dir.to_string_lossy().into_owned(),
        },
    });
    let agent = make_agent_with_memory(
        &root.to_string_lossy(),
        vec![
            tool_use_turn(
                "write-1",
                "write",
                json!({
                    "path": "consolidated.md",
                    "content": "protocol consolidation\n"
                }),
            ),
            text_turn("done"),
        ],
        Some(json!({
            "agentMemory": true,
            "autoDream": true,
            "maxRetries": 0
        })),
        Some(host),
    );
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    let mut dream = timeout(
        CROSS_TASK_LIVENESS_TIMEOUT,
        client.dream_now_with_completion(),
    )
    .await
    .expect("dream start timed out")
    .expect("dream start failed");
    assert!(dream.ok);
    let completion = timeout(CROSS_TASK_LIVENESS_TIMEOUT, dream.done.take().unwrap())
        .await
        .expect("dream completion timed out")
        .expect("completion channel closed")
        .expect("completion event failed");
    assert!(completion.ok, "{:?}", completion.reason);
    assert!(completion.turn_count.unwrap_or_default() > 0);
    // `filesTouched` is folded from the write tool's `file_change` events, and the engine
    // spells the paths it reports POSIX-rendered on every host.
    let touched = memory_dir
        .canonicalize()
        .unwrap()
        .join("consolidated.md")
        .to_string_lossy()
        .into_owned();
    let touched = if cfg!(windows) {
        touched.replace('\\', "/")
    } else {
        touched
    };
    assert_eq!(
        completion.files_touched,
        Some(vec![RuntimeString::from(touched)])
    );
    assert_eq!(
        std::fs::read_to_string(memory_dir.join("consolidated.md")).unwrap(),
        "protocol consolidation\n"
    );

    server.stop();
    done.await.ok();
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn openai_gateway_init_builds_provider_and_rejects_unsafe_configuration() {
    let compatibility = OpenAiCompatibilityInit {
        use_model_default_effort: None,
        instruction_role: None,
        responses_function_tools: None,
        extra_headers: Some(BTreeMap::from([(
            "X-Endpoint".into(),
            "private-endpoint-value".into(),
        )])),
        request_identity: Some(OpenAiRequestIdentityInit {
            session_id: "private-session-value".into(),
        }),
    };
    let debug = format!("{compatibility:?}");
    assert!(debug.contains("X-Endpoint"));
    assert!(!debug.contains("private-endpoint-value"));
    assert!(!debug.contains("private-session-value"));

    let init = ProtocolSessionInit {
        provider: Some(ProviderInit::Openai {
            model: Some("qwen3.7-max".into()),
            api: Some("chat".into()),
            base_url: Some("https://gateway.example/v1".into()),
            compatibility: Some(compatibility),
        }),
        auth: Some(ProviderAuthInit::ApiKey {
            key: "fixture-token".into(),
        }),
        ..Default::default()
    };
    let provider = provider_from_init(&init, None)
        .await
        .expect("valid gateway init")
        .expect("provider is configured");
    assert_eq!(provider.info().default_model, "qwen3.7-max");

    let protected = OpenAiCompatibilityInit {
        use_model_default_effort: None,
        instruction_role: None,
        responses_function_tools: None,
        extra_headers: Some(BTreeMap::from([(
            "Authorization".into(),
            "Bearer private-attacker-value".into(),
        )])),
        request_identity: None,
    };
    let error = openai_compatibility_from_init(Some(&protected), OpenAiApi::Chat)
        .expect_err("authentication headers are provider-owned");
    assert!(error.contains("authorization"));
    assert!(!error.contains("private-attacker-value"));

    let responses_only = OpenAiCompatibilityInit {
        use_model_default_effort: None,
        instruction_role: None,
        responses_function_tools: None,
        extra_headers: None,
        request_identity: Some(OpenAiRequestIdentityInit {
            session_id: "private-session-value".into(),
        }),
    };
    let error = openai_compatibility_from_init(Some(&responses_only), OpenAiApi::Responses)
        .expect_err("request identity is chat-only");
    assert!(error.contains("requires api 'chat'"));
}

#[test]
fn openai_responses_function_tools_round_trip_and_reject_inapplicable_api() {
    let init = OpenAiCompatibilityInit {
        use_model_default_effort: None,
        instruction_role: None,
        extra_headers: None,
        request_identity: None,
        responses_function_tools: Some(true),
    };
    let encoded = runtime_codec::encode_text(&init).expect("encode compatibility");
    assert_eq!(
        encoded,
        RuntimeString::from(r#"{"responsesFunctionTools":true}"#)
    );
    let decoded: OpenAiCompatibilityInit =
        runtime_codec::decode_text(&encoded).expect("decode compatibility");
    assert_eq!(decoded, init);
    let options = openai_compatibility_from_init(Some(&decoded), OpenAiApi::Responses)
        .expect("Responses mode")
        .expect("compatibility");
    assert!(format!("{options:?}").contains("responses_function_tools: true"));
    let error = openai_compatibility_from_init(Some(&decoded), OpenAiApi::Chat)
        .expect_err("mode is Responses-only");
    assert_eq!(
        error,
        "openai compatibility responsesFunctionTools requires api 'responses'"
    );
    assert!(
        runtime_codec::decode_text::<OpenAiCompatibilityInit>(&RuntimeString::from(
            r#"{"responsesFunctionTools":"true"}"#
        ))
        .is_err()
    );
}

#[test]
fn openai_instruction_role_round_trips_and_rejects_unsupported_roles() {
    for (wire, expected) in [("system", "System"), ("developer", "Developer")] {
        let init = OpenAiCompatibilityInit {
            use_model_default_effort: None,
            instruction_role: Some(wire.into()),
            extra_headers: None,
            request_identity: None,
            responses_function_tools: None,
        };
        let encoded = runtime_codec::encode_text(&init).expect("encode declared role");
        assert_eq!(
            encoded,
            RuntimeString::from(format!(r#"{{"instructionRole":"{wire}"}}"#))
        );
        let decoded: OpenAiCompatibilityInit =
            runtime_codec::decode_text(&encoded).expect("decode declared role");
        assert_eq!(decoded, init);
        for api in [OpenAiApi::Chat, OpenAiApi::Responses] {
            let options = openai_compatibility_from_init(Some(&decoded), api)
                .unwrap()
                .unwrap();
            assert!(format!("{options:?}").contains(&format!("instruction_role: Some({expected})")));
        }
    }
    let invalid: OpenAiCompatibilityInit =
        runtime_codec::decode_text(&RuntimeString::from(r#"{"instructionRole":"user"}"#)).unwrap();
    assert_eq!(
        openai_compatibility_from_init(Some(&invalid), OpenAiApi::Chat).unwrap_err(),
        "openai compatibility instructionRole must be system or developer"
    );
    assert!(
        runtime_codec::decode_text::<OpenAiCompatibilityInit>(&RuntimeString::from(
            r#"{"instructionRole":true}"#
        ))
        .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_session_validates_dream_thresholds_through_generic_config() {
    let agent = make_agent("/tmp/klaus-proto", vec![], None);
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    let accepted = timeout(
        CROSS_TASK_LIVENESS_TIMEOUT,
        client.init_session(ProtocolSessionInit {
            config: Some(
                runtime_json_from_scalar_value(&json!({
                    "agentMemory": true,
                    "autoDream": true,
                    "dream": { "minHours": 0.5, "minSessions": 9 }
                }))
                .unwrap(),
            ),
            ..Default::default()
        }),
    )
    .await
    .expect("valid init timed out")
    .expect("valid Desktop thresholds should be accepted");
    assert!(accepted.accepted);
    server.stop();
    done.await.ok();

    let agent = make_agent("/tmp/klaus-proto", vec![], None);
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });
    let error = timeout(
        CROSS_TASK_LIVENESS_TIMEOUT,
        client.init_session(ProtocolSessionInit {
            config: Some(
                runtime_json_from_scalar_value(&json!({
                    "dream": { "minHours": 0.0, "minSessions": 1 }
                }))
                .unwrap(),
            ),
            ..Default::default()
        }),
    )
    .await
    .expect("invalid init timed out")
    .expect_err("server must reject out-of-range Dream settings");
    assert!(matches!(error, ClientError::Op(message) if message.contains("dream.minHours")));
    server.stop();
    done.await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_session_provider_auth_payload_binds_the_protocol_session_provider() {
    let agent = make_agent("/tmp/klaus-proto", vec![], None);
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        let init = client
            .init_session(ProtocolSessionInit {
                provider: Some(ProviderInit::Openai {
                    model: Some("gpt-4.1-mini".into()),
                    api: Some("responses".into()),
                    base_url: None,
                    compatibility: None,
                }),
                auth: Some(ProviderAuthInit::StaticBearer {
                    token: "desktop-token".into(),
                }),
                // Context window is now a client-declared capability (the engine no
                // longer infers it from the model id).
                capabilities: Some(crate::types::ModelCapabilitiesInit {
                    context_window: Some(1_047_576),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .expect("provider/auth init should be accepted");
        assert!(init.accepted);

        client
            .run_to_completion(Input::Text("bind".into()), None)
            .await
            .expect("run binds session");
        let context = client.context_snapshot().await.expect("context");
        assert_eq!(context.model, "gpt-4.1-mini");
        assert_eq!(context.context_window, 1_047_576);
    })
    .await
    .expect("timed out");

    server.stop();
    done.await.ok();
}

#[tokio::test]
async fn stored_provider_oauth_uses_the_injected_credential_store() {
    let backing: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
    let namespaced =
        NamespacedCredentialStore::new(backing.clone(), "provider/openai/desktop-account-a");
    namespaced
        .set(
            "oauth:client-a",
            &oauth_tokens_json(&OAuthTokens {
                access_token: "persisted-access".into(),
                refresh_token: Some("rotated-refresh".into()),
                expires_at: None,
                id_token: None,
            }),
        )
        .await
        .unwrap();

    let resolved = resolve_provider_auth(
        "openai",
        "OPENAI_API_KEY",
        &Some(ProviderAuthInit::OAuthStored {
            credential_id: "desktop-account-a".into(),
            oauth: Some(ProviderOAuthInit {
                client_id: Some("client-a".into()),
                authorize_url: None,
                token_url: None,
                redirect_uri: None,
                scopes: None,
                extra_authorize_params: None,
            }),
            base_url: None,
            extra_headers: None,
        }),
        None,
        None,
        Some(backing),
    )
    .await
    .unwrap();

    let ResolvedProviderAuth::Source(source) = resolved else {
        panic!("stored OAuth must resolve to an auth source");
    };
    // Replace the account after preparation but before the first provider use.
    // A cold store read here would silently authorize the replacement account.
    namespaced
        .set(
            "oauth:client-a",
            &oauth_tokens_json(&OAuthTokens {
                access_token: "replacement-account-access".into(),
                refresh_token: Some("replacement-account-refresh".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    let headers = source.get_headers(&AbortSignal::new()).await.unwrap();
    assert_eq!(
        headers.get("Authorization").map(String::as_str),
        Some("Bearer persisted-access")
    );
    assert_eq!(
        decode_oauth_tokens(&namespaced.get("oauth:client-a").await.unwrap().unwrap()).access_token,
        "replacement-account-access"
    );
}

#[tokio::test]
async fn provider_oauth_token_with_credential_id_is_persisted_immediately() {
    let backing: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
    let auth = Some(ProviderAuthInit::OAuthToken {
        access_token: "submitted-access".into(),
        refresh_token: Some("submitted-refresh".into()),
        expires_at: Some(4_000_000_000_000),
        id_token: None,
        oauth: Some(ProviderOAuthInit {
            client_id: Some("client-a".into()),
            authorize_url: None,
            token_url: None,
            redirect_uri: None,
            scopes: None,
            extra_authorize_params: None,
        }),
        base_url: None,
        extra_headers: None,
        credential_id: Some("desktop-account-a".into()),
        replace_stored: None,
        rotation: None,
    });

    resolve_provider_auth(
        "openai",
        "OPENAI_API_KEY",
        &auth,
        None,
        None,
        Some(backing.clone()),
    )
    .await
    .unwrap();

    let namespaced = NamespacedCredentialStore::new(backing, "provider/openai/desktop-account-a");
    let stored = namespaced
        .get("oauth:client-a")
        .await
        .unwrap()
        .expect("persisted token set");
    let tokens = decode_oauth_tokens(&stored);
    assert_eq!(tokens.access_token, "submitted-access");
    assert_eq!(
        tokens.refresh_token.as_ref(),
        Some(&RuntimeString::from("submitted-refresh"))
    );
}

#[tokio::test]
async fn repeated_provider_oauth_token_does_not_overwrite_rotated_store_value() {
    let backing: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
    let namespaced =
        NamespacedCredentialStore::new(backing.clone(), "provider/openai/desktop-account-a");
    namespaced
        .set(
            "oauth:client-a",
            &oauth_tokens_json(&OAuthTokens {
                access_token: "rotated-access".into(),
                refresh_token: Some("rotated-refresh".into()),
                expires_at: None,
                id_token: None,
            }),
        )
        .await
        .unwrap();

    let resolved = resolve_provider_auth(
        "openai",
        "OPENAI_API_KEY",
        &Some(ProviderAuthInit::OAuthToken {
            access_token: "stale-access".into(),
            refresh_token: Some("stale-refresh".into()),
            expires_at: None,
            id_token: None,
            oauth: Some(ProviderOAuthInit {
                client_id: Some("client-a".into()),
                authorize_url: None,
                token_url: None,
                redirect_uri: None,
                scopes: None,
                extra_authorize_params: None,
            }),
            base_url: None,
            extra_headers: None,
            credential_id: Some("desktop-account-a".into()),
            replace_stored: None,
            rotation: None,
        }),
        None,
        None,
        Some(backing),
    )
    .await
    .unwrap();
    let ResolvedProviderAuth::Source(source) = resolved else {
        panic!("provider OAuth must resolve to an auth source");
    };
    let stored = decode_oauth_tokens(
        &namespaced
            .get("oauth:client-a")
            .await
            .unwrap()
            .expect("stored token set"),
    );
    assert_eq!(
        stored.refresh_token.as_ref(),
        Some(&RuntimeString::from("rotated-refresh"))
    );
    namespaced
        .set(
            "oauth:client-a",
            &oauth_tokens_json(&OAuthTokens {
                access_token: "replacement-account-access".into(),
                refresh_token: Some("replacement-account-refresh".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    let headers = source.get_headers(&AbortSignal::new()).await.unwrap();
    assert_eq!(
        headers.get("Authorization").map(String::as_str),
        Some("Bearer rotated-access")
    );
    assert_eq!(
        decode_oauth_tokens(&namespaced.get("oauth:client-a").await.unwrap().unwrap()).access_token,
        "replacement-account-access"
    );
}

#[tokio::test]
async fn provider_oauth_token_empty_prepare_cannot_overwrite_a_concurrent_login() {
    struct LoginRaceStore {
        inner: InMemoryCredentialStore,
        inject: AtomicBool,
        replacement: String,
    }

    #[async_trait::async_trait]
    impl CredentialStore for LoginRaceStore {
        async fn get(
            &self,
            key: &str,
        ) -> Result<Option<String>, klaus_agent_core::credential_store::CredentialStoreError>
        {
            let observed = self.inner.get(key).await?;
            if observed.is_none() && self.inject.swap(false, std::sync::atomic::Ordering::SeqCst) {
                self.inner.set(key, &self.replacement).await?;
            }
            Ok(observed)
        }

        async fn set(
            &self,
            key: &str,
            value: &str,
        ) -> Result<(), klaus_agent_core::credential_store::CredentialStoreError> {
            self.inner.set(key, value).await
        }

        async fn delete(
            &self,
            key: &str,
        ) -> Result<(), klaus_agent_core::credential_store::CredentialStoreError> {
            self.inner.delete(key).await
        }

        async fn compare_exchange(
            &self,
            key: &str,
            expected: Option<&str>,
            replacement: &str,
        ) -> Result<bool, klaus_agent_core::credential_store::CredentialStoreError> {
            self.inner
                .compare_exchange(key, expected, replacement)
                .await
        }
    }

    let replacement = oauth_tokens_json(&OAuthTokens {
        access_token: "concurrent-login-access".into(),
        refresh_token: Some("concurrent-login-refresh".into()),
        ..Default::default()
    });
    let store = Arc::new(LoginRaceStore {
        inner: InMemoryCredentialStore::new(),
        inject: AtomicBool::new(true),
        replacement: replacement.clone(),
    });
    let prepared = resolve_provider_auth(
        "openai",
        "OPENAI_API_KEY",
        &Some(ProviderAuthInit::OAuthToken {
            access_token: "submitted-older-access".into(),
            refresh_token: Some("submitted-older-refresh".into()),
            expires_at: None,
            id_token: None,
            oauth: Some(ProviderOAuthInit {
                client_id: Some("client-a".into()),
                authorize_url: None,
                token_url: None,
                redirect_uri: None,
                scopes: None,
                extra_authorize_params: None,
            }),
            base_url: None,
            extra_headers: None,
            credential_id: Some("empty-prepare-race".into()),
            replace_stored: None,
            rotation: None,
        }),
        None,
        None,
        Some(store.clone()),
    )
    .await;
    let error = match prepared {
        Ok(_) => panic!("stale preparation overwrote the concurrent login"),
        Err(error) => error,
    };
    assert!(error.contains("OAuth credential changed during preparation"));
    let stored = NamespacedCredentialStore::new(store, "provider/openai/empty-prepare-race")
        .get("oauth:client-a")
        .await
        .unwrap();
    assert_eq!(stored.as_deref(), Some(replacement.as_str()));
}

#[tokio::test]
async fn provider_oauth_replace_stored_explicitly_replaces_prior_login() {
    let backing: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
    let namespaced =
        NamespacedCredentialStore::new(backing.clone(), "provider/openai/desktop-account-a");
    namespaced
        .set(
            "oauth:client-a",
            &oauth_tokens_json(&OAuthTokens {
                access_token: "prior-access".into(),
                refresh_token: Some("prior-refresh".into()),
                expires_at: None,
                id_token: None,
            }),
        )
        .await
        .unwrap();

    resolve_provider_auth(
        "openai",
        "OPENAI_API_KEY",
        &Some(ProviderAuthInit::OAuthToken {
            access_token: "new-login-access".into(),
            refresh_token: Some("new-login-refresh".into()),
            expires_at: None,
            id_token: None,
            oauth: Some(ProviderOAuthInit {
                client_id: Some("client-a".into()),
                authorize_url: None,
                token_url: None,
                redirect_uri: None,
                scopes: None,
                extra_authorize_params: None,
            }),
            base_url: None,
            extra_headers: None,
            credential_id: Some("desktop-account-a".into()),
            replace_stored: Some(true),
            rotation: None,
        }),
        None,
        None,
        Some(backing),
    )
    .await
    .unwrap();

    let stored = decode_oauth_tokens(
        &namespaced
            .get("oauth:client-a")
            .await
            .unwrap()
            .expect("stored token set"),
    );
    assert_eq!(stored.access_token, "new-login-access");
    assert_eq!(
        stored.refresh_token.as_ref(),
        Some(&RuntimeString::from("new-login-refresh"))
    );
}

#[tokio::test]
async fn persistent_provider_oauth_requires_a_store_and_safe_credential_id() {
    let oauth = Some(ProviderOAuthInit {
        client_id: Some("client-a".into()),
        authorize_url: None,
        token_url: None,
        redirect_uri: None,
        scopes: None,
        extra_authorize_params: None,
    });
    let missing_store = match resolve_provider_auth(
        "openai",
        "OPENAI_API_KEY",
        &Some(ProviderAuthInit::OAuthStored {
            credential_id: "desktop-account-a".into(),
            oauth: oauth.clone(),
            base_url: None,
            extra_headers: None,
        }),
        None,
        None,
        None,
    )
    .await
    {
        Ok(_) => panic!("persistent OAuth unexpectedly accepted without a store"),
        Err(err) => err,
    };
    assert!(missing_store.contains("requires an endpoint credential store"));

    let unsafe_id = match resolve_provider_auth(
        "openai",
        "OPENAI_API_KEY",
        &Some(ProviderAuthInit::OAuthStored {
            credential_id: "../account".into(),
            oauth,
            base_url: None,
            extra_headers: None,
        }),
        None,
        None,
        Some(Arc::new(InMemoryCredentialStore::new())),
    )
    .await
    {
        Ok(_) => panic!("unsafe credential id unexpectedly accepted"),
        Err(err) => err,
    };
    assert!(unsafe_id.contains("only ASCII letters"));

    let backing: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
    NamespacedCredentialStore::new(backing.clone(), "provider/openai/corrupt-account")
        .set("oauth:client-a", "{}")
        .await
        .unwrap();
    let corrupt = match resolve_provider_auth(
        "openai",
        "OPENAI_API_KEY",
        &Some(ProviderAuthInit::OAuthStored {
            credential_id: "corrupt-account".into(),
            oauth: Some(ProviderOAuthInit {
                client_id: Some("client-a".into()),
                authorize_url: None,
                token_url: None,
                redirect_uri: None,
                scopes: None,
                extra_authorize_params: None,
            }),
            base_url: None,
            extra_headers: None,
        }),
        None,
        None,
        Some(backing.clone()),
    )
    .await
    {
        Ok(_) => panic!("corrupt stored OAuth credential unexpectedly accepted"),
        Err(err) => err,
    };
    assert_eq!(
        corrupt,
        "failed to capture stored provider oauth credential"
    );
    assert_eq!(
        NamespacedCredentialStore::new(backing, "provider/openai/corrupt-account")
            .get("oauth:client-a")
            .await
            .unwrap()
            .as_deref(),
        Some("{}"),
        "preparation failure must not replace the corrupt stored credential"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_session_accepts_stored_provider_oauth_from_endpoint_store() {
    let backing: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
    let namespaced =
        NamespacedCredentialStore::new(backing.clone(), "provider/openai/desktop-account-a");
    namespaced
        .set(
            "oauth:client-a",
            &oauth_tokens_json(&OAuthTokens {
                access_token: "persisted-access".into(),
                refresh_token: Some("rotated-refresh".into()),
                expires_at: None,
                id_token: None,
            }),
        )
        .await
        .unwrap();

    let agent = make_agent("/tmp/klaus-proto", vec![], None);
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server_with_credential_store(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        backing,
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    let init = client
        .init_session(ProtocolSessionInit {
            provider: Some(ProviderInit::Openai {
                model: Some("gpt-4.1-mini".into()),
                api: Some("responses".into()),
                base_url: None,
                compatibility: None,
            }),
            auth: Some(ProviderAuthInit::OAuthStored {
                credential_id: "desktop-account-a".into(),
                oauth: Some(ProviderOAuthInit {
                    client_id: Some("client-a".into()),
                    authorize_url: None,
                    token_url: None,
                    redirect_uri: None,
                    scopes: None,
                    extra_authorize_params: None,
                }),
                base_url: None,
                extra_headers: None,
            }),
            ..Default::default()
        })
        .await
        .expect("stored provider OAuth init should be accepted");
    assert!(init.accepted);

    server.stop();
    done.await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_session_host_capabilities_adopt_browser_mapping_without_engine_builtins() {
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let agent = make_recording_agent("/tmp/klaus-proto", requests.clone());
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        client
            .init_session(ProtocolSessionInit {
                host_capabilities: Some(ProtocolHostCapabilitiesInit {
                    mcp_servers: vec![ProtocolMcpServerInit::Stdio {
                        name: "browser".into(),
                        command: "browser-mcp".into(),
                        args: None,
                        env: None,
                        always_load: Some(true),
                        disabled_tools: None,
                    }],
                    runtime_capability_roots: vec![ProtocolRuntimeCapabilityRootInit {
                        id: "desktop-browser".into(),
                        enabled: Some(true),
                        browser: Some(ProtocolBrowserCapabilityInit {
                            mcp_server: Some("browser".into()),
                            host_reference: None,
                        }),
                    }],
                    selected_capability_roots: Some(vec![
                        ProtocolSelectedCapabilityRootInit::Host {
                            id: "desktop-browser".into(),
                        },
                    ]),
                }),
                ..Default::default()
            })
            .await
            .expect("init");

        client
            .run_to_completion(Input::Text("use browser".into()), None)
            .await
            .expect("run");
    })
    .await
    .expect("timed out");

    server.stop();
    done.await.ok();

    let requests = requests.lock().unwrap();
    let tool_names = requests[0]
        .tools
        .iter()
        .map(|tool| tool.name())
        .collect::<Vec<_>>();
    // The declared mapping routes internal browser dispatch; the model-visible
    // browser surface belongs to the declared MCP server's own tools, so a
    // mapping-only host gets no engine browser_* builtins.
    assert!(
        !tool_names.iter().any(|name| name.starts_with("browser_")),
        "{tool_names:?}"
    );
    assert!(tool_names
        .iter()
        .any(|name| *name == &RuntimeString::from("web_search")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn init_session_drives_instructions_shell_lsp_and_dynamic_tool_bridge() {
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let calls = Arc::new(AtomicUsize::new(0));
    let call_model = {
        let requests = requests.clone();
        let calls = calls.clone();
        Box::new(
            move |req: ModelRequest, signal: AbortSignal| -> ModelStream {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                requests.lock().unwrap().push(req);
                let events = match call {
                    0 => tool_use_turn(
                        "shell-call",
                        "bash",
                        json!({ "command": "printf '%s' \"$KLAUS_PROTOCOL_ENV\"" }),
                    ),
                    1 => tool_use_turn(
                        "dynamic-call",
                        "desktop__lookup",
                        json!({ "key": "answer" }),
                    ),
                    _ => text_turn("done"),
                };
                Box::pin(
                    futures::stream::iter(events)
                        .take_while(move |_| futures::future::ready(!signal.is_cancelled())),
                )
            },
        )
    };
    let agent = Arc::new(Agent::new(AgentOptions {
        process_context: test_process_context(),
        provider: Arc::new(fake_provider()) as Arc<dyn ModelProvider>,
        config: None,
        tools: None,
        web_fetch_page: None,
        browser_runtime: None,
        capability_roots: Vec::new(),
        cwd: Some("/tmp".to_string()),
        deps: EngineDepsOverrides {
            call_model: Some(call_model),
            estimate_tokens: None,
            compactor: Some(noop_compactor()),
            uuid: None,
            now: None,
        },
        compactor: None,
        approval_handler: None,
        approval_reviewer: None,
        elicit_handler: None,
        credential_store: None,
        hooks: vec![],
        observer: None,
        logger: None,
        pricing: None,
        agents: None,
        include_default_agents: None,
        skills: None,
        output_styles: None,
        workflows: None,
        mcp_catalog: None,
        plugin_catalog: None,
        lsp_servers: None,
        agent_memory: None,
        mcp_clients: None,
        mcp_server_configs: Vec::new(),
        mcp_tools: None,
        refresh_tools: None,
        system_prompt_provider: None,
        plugins: Vec::new(),
        plugin_load_report: None,
    }));
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = Arc::new(ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    }));
    let mut events = client.events();

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        client
            .init_session(ProtocolSessionInit {
                base_instructions: Some("desktop base".into()),
                developer_instructions: Some("desktop developer".into()),
                dynamic_tools: vec![ProtocolDynamicToolSpec::Namespace(
                    ProtocolDynamicToolNamespaceSpec {
                        name: "desktop".into(),
                        description: "Desktop services".into(),
                        tools: vec![ProtocolDynamicToolNamespaceTool::Function(
                            ProtocolDynamicToolFunctionSpec {
                                name: "lookup".into(),
                                description: "Look up a value".into(),
                                input_schema: runtime_json_from_scalar_value(&json!({
                                    "type": "object",
                                    "properties": { "key": { "type": "string" } },
                                    "required": ["key"]
                                }))
                                .unwrap(),
                                defer_loading: false,
                                read_only: false,
                            },
                        )],
                    },
                )],
                lsp_servers: vec![LspServerConfig {
                    language: "rust".into(),
                    extensions: vec!["rs".into()],
                    command: "/bin/false".into(),
                    args: None,
                    root_dir: None,
                }],
                shell_env: Some(
                    runtime_codec::decode_value(
                        &runtime_json_from_scalar_value(&json!({
                            "KLAUS_PROTOCOL_ENV": "stdio-value"
                        }))
                        .unwrap(),
                    )
                    .unwrap(),
                ),
                config: Some(
                    runtime_json_from_scalar_value(&json!({ "lsp": true, "sandbox": "process" }))
                        .unwrap(),
                ),
                ..Default::default()
            })
            .await
            .expect("init");

        let run_client = client.clone();
        let run = tokio::spawn(async move {
            run_client
                .run_to_completion(Input::Text("exercise init".into()), None)
                .await
        });
        let dynamic_call = loop {
            let event = events.next().await.expect("protocol event stream ended");
            if let Some(call) = event.dynamic_tool_call.clone() {
                break call;
            }
        };
        assert_eq!(dynamic_call.call_id, "dynamic-call");
        assert_eq!(
            dynamic_call.namespace.as_ref(),
            Some(&RuntimeString::from("desktop"))
        );
        assert_eq!(dynamic_call.tool, "lookup");
        assert_eq!(
            dynamic_call.arguments,
            runtime_json_from_scalar_value(&json!({ "key": "answer" }))
                .expect("scalar dynamic tool arguments fixture")
        );
        client
            .resolve_dynamic_tool_call(
                dynamic_call
                    .call_id
                    .try_to_utf8()
                    .expect("scalar dynamic tool call id"),
                ProtocolDynamicToolResponse {
                    content_items: vec![ProtocolDynamicToolCallOutputContentItem::InputText {
                        text: "42".into(),
                    }],
                    success: true,
                },
            )
            .await
            .expect("resolve dynamic tool");
        assert_eq!(
            run.await.expect("run task").expect("run").final_text,
            "done"
        );
    })
    .await
    .expect("full init flow timed out");

    server.stop();
    done.await.ok();

    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].system[0].text, "desktop base");
    assert_eq!(requests[0].system[1].text, "desktop developer");
    let tool_names = requests[0]
        .tools
        .iter()
        .map(|tool| tool.name())
        .collect::<Vec<_>>();
    assert!(tool_names
        .iter()
        .any(|name| *name == &RuntimeString::from("desktop__lookup")));
    assert!(tool_names.iter().any(|name| name.starts_with("lsp_")));
    let second_request = runtime_codec::encode_text(&requests[1].messages)
        .unwrap()
        .try_to_utf8()
        .unwrap();
    assert!(
        second_request.contains("stdio-value"),
        "second request did not contain explicit shell env output: {second_request}"
    );
    let third_request = runtime_codec::encode_text(&requests[2].messages)
        .unwrap()
        .try_to_utf8()
        .unwrap();
    assert!(third_request.contains("42"));
}

#[test]
fn dynamic_tool_response_accepts_data_images_and_rejects_remote_images() {
    let data = dynamic_tool_result(ProtocolDynamicToolResponse {
        content_items: vec![ProtocolDynamicToolCallOutputContentItem::InputImage {
            image_url: "data:image/png;base64,QQ==".into(),
        }],
        success: true,
    });
    assert_eq!(
        data.serializable.model_content,
        vec![OutputBlock::Image(ImageBlock {
            mime_type: "image/png".into(),
            base64: "QQ==".into(),
            detail: None,
            source_path: None,
        })]
    );
    assert_eq!(data.serializable.is_error, Some(false));

    let remote = dynamic_tool_result(ProtocolDynamicToolResponse {
        content_items: vec![ProtocolDynamicToolCallOutputContentItem::InputImage {
            image_url: "https://example.com/image.png".into(),
        }],
        success: true,
    });
    assert_eq!(remote.serializable.is_error, Some(true));
    assert!(matches!(
        remote.serializable.model_content.as_slice(),
        [OutputBlock::Text(TextBlock { text, .. })]
            if text.contains("base64 data URLs")
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupt_releases_a_turn_parked_on_a_dynamic_tool() {
    let agent = make_agent(
        "/tmp",
        vec![
            tool_use_turn("dynamic-pending", "lookup", json!({ "key": "answer" })),
            text_turn("after"),
        ],
        None,
    );
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });
    client
        .init_session(ProtocolSessionInit {
            dynamic_tools: vec![ProtocolDynamicToolSpec::Function(
                ProtocolDynamicToolFunctionSpec {
                    name: "lookup".into(),
                    description: "Look up a value".into(),
                    input_schema: runtime_json_from_scalar_value(&json!({ "type": "object" }))
                        .unwrap(),
                    defer_loading: false,
                    read_only: false,
                },
            )],
            ..Default::default()
        })
        .await
        .expect("init");

    let received = timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        let mut events = client.events();
        client.run(Input::Text("call lookup".into()), None).await;
        let mut received = Vec::new();
        let mut interrupted = false;
        while let Some(event) = events.next().await {
            let dynamic = event.dynamic_tool_call.is_some();
            let terminal = is_terminal(&event);
            received.push(event);
            if dynamic && !interrupted {
                interrupted = true;
                client.interrupt().await;
            }
            if terminal {
                break;
            }
        }
        received
    })
    .await
    .expect("dynamic tool interrupt timed out");

    assert!(received.iter().any(|event| {
        event
            .dynamic_tool_call
            .as_ref()
            .is_some_and(|call| call.call_id == "dynamic-pending")
    }));
    assert!(received.iter().any(|event| is_terminal(event)));
    server.stop();
    done.await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_session_rejects_live_host_capability_references_over_stdio() {
    let agent = make_agent("/tmp/klaus-proto", vec![], None);
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
            let err = client
                .init_session(ProtocolSessionInit {
                    host_capabilities: Some(ProtocolHostCapabilitiesInit {
                        runtime_capability_roots: vec![ProtocolRuntimeCapabilityRootInit {
                            id: "desktop-browser".into(),
                            enabled: Some(true),
                            browser: Some(ProtocolBrowserCapabilityInit {
                                mcp_server: None,
                                host_reference: Some("browser-runtime-object".into()),
                            }),
                        }],
                        ..Default::default()
                    }),
                    ..Default::default()
                })
                .await
                .expect_err("live host objects should be rejected");
            assert!(matches!(err, ClientError::Op(message) if message.contains("cannot transfer live host capability")));
        })
        .await
        .expect("timed out");

    server.stop();
    done.await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_session_rejects_blank_selected_host_capability_root() {
    let agent = make_agent("/tmp/klaus-proto", vec![], None);
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
            let err = client
                .init_session(ProtocolSessionInit {
                    host_capabilities: Some(ProtocolHostCapabilitiesInit {
                        selected_capability_roots: Some(vec![
                            ProtocolSelectedCapabilityRootInit::Host { id: " ".into() },
                        ]),
                        ..Default::default()
                    }),
                    ..Default::default()
                })
                .await
                .expect_err("blank selected capability root should be rejected");
            assert!(matches!(err, ClientError::Op(message) if message.contains("selected host capability root id must not be empty")));
        })
        .await
        .expect("timed out");

    server.stop();
    done.await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fork_archive_unarchive_and_delete_ops_preserve_store_identity() {
    let dir = unique_temp_dir("fork-archive-delete");
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    write_persisted_session(&store, "thread-1", 1_700_000_000).await;
    let agent = make_agent("/tmp/klaus-proto", vec![text_turn("forked reply")], None);
    let lifecycle = protocol_lifecycle(agent.clone(), store.clone());
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server_with_lifecycle(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        Some(lifecycle),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        let forked = client
            .fork_session("thread-1".into(), None)
            .await
            .expect("fork");
        assert_ne!(forked.thread_id, "thread-1");
        assert_eq!(
            forked.forked_from_id.as_ref(),
            Some(&RuntimeString::from("thread-1"))
        );
        let run = client
            .run_to_completion(Input::Text("continue fork".into()), None)
            .await
            .expect("run");
        assert_eq!(run.final_text, "forked reply");
        let fork_page = client.list_sessions(None).await.expect("list fork");
        let fork_summary = fork_page
            .items
            .iter()
            .find(|item| item.id == forked.thread_id)
            .unwrap();
        assert_eq!(
            fork_summary.forked_from_id.as_ref(),
            Some(&RuntimeString::from("thread-1"))
        );

        let archived = client
            .archive_session("thread-1".into())
            .await
            .expect("archive");
        assert_eq!(archived.thread_id, "thread-1");
        assert!(archived.archived);
        let page = client
            .list_sessions(Some(ListOptions {
                include_archived: Some(true),
                ..Default::default()
            }))
            .await
            .expect("list archived");
        assert!(page
            .items
            .iter()
            .any(|item| item.id == "thread-1" && item.archived == Some(true)));

        let restored = client
            .unarchive_session("thread-1".into())
            .await
            .expect("unarchive");
        assert_eq!(
            restored.status,
            klaus_agent_core::store::session_store::UnarchiveSessionStatus::Restored
        );
        assert_eq!(restored.thread_id, "thread-1");
        assert_eq!(
            restored.thread.as_ref().and_then(|thread| thread.archived),
            Some(false)
        );
        let repeated = client
            .unarchive_session("thread-1".into())
            .await
            .expect("idempotent unarchive");
        assert_eq!(
            repeated.status,
            klaus_agent_core::store::session_store::UnarchiveSessionStatus::AlreadyActive
        );
        let active = client.list_sessions(None).await.expect("list restored");
        assert_eq!(
            active
                .items
                .iter()
                .filter(|item| item.id == "thread-1")
                .count(),
            1
        );
        assert!(active
            .items
            .iter()
            .any(|item| item.id == "thread-1" && item.archived == Some(false)));

        let deleted = client
            .delete_session("thread-1".into())
            .await
            .expect("delete");
        assert!(deleted.deleted);
        let after = client
            .list_sessions(Some(ListOptions {
                include_archived: Some(true),
                ..Default::default()
            }))
            .await
            .expect("list after delete");
        assert!(!after.items.iter().any(|item| item.id == "thread-1"));
        let missing = client
            .unarchive_session("thread-1".into())
            .await
            .expect("typed missing result");
        assert_eq!(
            missing.status,
            klaus_agent_core::store::session_store::UnarchiveSessionStatus::Missing
        );
    })
    .await
    .expect("timed out");

    server.stop();
    done.await.ok();
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fork_adopts_a_host_supplied_thread_id_and_rename_rekeys_a_stored_thread() {
    let dir = unique_temp_dir("fork-adopt-rename");
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    write_persisted_session(&store, "thread-src", 1_700_000_000).await;
    write_persisted_session(&store, "engine-minted", 1_700_000_001).await;
    let agent = make_agent("/tmp/klaus-proto", vec![text_turn("forked reply")], None);
    let lifecycle = protocol_lifecycle(agent.clone(), store.clone());
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server_with_lifecycle(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        Some(lifecycle),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        // A host that keys sessions by thread id pins the forked identity, so
        // the fork is addressable by the host key with no translation layer.
        let forked = client
            .fork_session("thread-src".into(), Some("host-owned-fork".into()))
            .await
            .expect("fork");
        assert_eq!(forked.thread_id, "host-owned-fork");
        assert_eq!(
            forked.forked_from_id.as_ref(),
            Some(&RuntimeString::from("thread-src"))
        );
        let page = client.list_sessions(None).await.expect("list forks");
        assert!(page.items.iter().any(|item| item.id == "host-owned-fork"));

        // A thread minted by an older build converges onto the host key.
        let renamed = client
            .rename_thread("engine-minted".into(), "host-owned-legacy".into())
            .await
            .expect("rename");
        assert_eq!(renamed.thread_id, "host-owned-legacy");
        assert!(renamed.renamed);
        let page = client.list_sessions(None).await.expect("list renamed");
        assert!(page.items.iter().any(|item| item.id == "host-owned-legacy"));
        assert!(page.items.iter().all(|item| item.id != "engine-minted"));

        // The bound fork thread cannot be re-keyed under this endpoint.
        let live = client
            .rename_thread("host-owned-fork".into(), "host-owned-other".into())
            .await
            .expect_err("a live thread must not be renamed");
        assert!(
            matches!(live, ClientError::Op(ref message) if message.contains("inactive")),
            "unexpected error: {live:?}"
        );
    })
    .await
    .expect("timed out");

    server.stop();
    done.await.ok();
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_current_session_interrupts_then_disposes_before_moving_rollout() {
    let dir = unique_temp_dir("archive-active-session");
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    write_persisted_session(&store, "thread-active", 1_700_000_000).await;
    let started = Arc::new(Notify::new());
    let cancelled = Arc::new(Notify::new());
    let agent = make_blocking_agent("/tmp/klaus-proto", started.clone(), cancelled.clone());
    let lifecycle = protocol_lifecycle(agent.clone(), store.clone());
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server_with_lifecycle(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        Some(lifecycle),
        sr,
        sw,
    );
    let client = Arc::new(ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    }));

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        client
            .resume_session("thread-active".into())
            .await
            .expect("resume");
        let run_client = client.clone();
        let run = tokio::spawn(async move {
            run_client
                .run_to_completion(Input::Text("keep running".into()), None)
                .await
        });
        started.notified().await;

        let archived = client
            .archive_session("thread-active".into())
            .await
            .expect("archive active session");
        cancelled.notified().await;
        run.await
            .expect("run task should settle")
            .expect("run result");

        assert_eq!(archived.thread_id, "thread-active");
        assert!(archived.archived);
        assert!(dir
            .join("archived")
            .join("1700000000-thread-active.jsonl")
            .exists());
        assert!(!dir.join("thread-active.jsonl").exists());
        assert!(!dir.join("thread-active.jsonl.lock").exists());
        let unarchive = client
            .unarchive_session("thread-active".into())
            .await
            .expect("shutdown is a typed unarchive result");
        assert_eq!(
            unarchive.status,
            klaus_agent_core::store::session_store::UnarchiveSessionStatus::Shutdown
        );
        assert!(dir
            .join("archived")
            .join("1700000000-thread-active.jsonl")
            .exists());
        let err = client
            .get_history()
            .await
            .expect_err("disposed archived session must reject further session work");
        assert!(matches!(err, ClientError::Op(message) if message.contains("shut down")));
    })
    .await
    .expect("active archive timed out");

    server.stop();
    done.await.ok();
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_current_session_interrupts_then_disposes_before_removing_rollout() {
    let dir = unique_temp_dir("delete-active-session");
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    write_persisted_session(&store, "thread-delete", 1_700_000_000).await;
    let started = Arc::new(Notify::new());
    let cancelled = Arc::new(Notify::new());
    let agent = make_blocking_agent("/tmp/klaus-proto", started.clone(), cancelled.clone());
    let lifecycle = protocol_lifecycle(agent.clone(), store.clone());
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server_with_lifecycle(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        Some(lifecycle),
        sr,
        sw,
    );
    let client = Arc::new(ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    }));

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        client
            .resume_session("thread-delete".into())
            .await
            .expect("resume");
        let run_client = client.clone();
        let run = tokio::spawn(async move {
            run_client
                .run_to_completion(Input::Text("keep running".into()), None)
                .await
        });
        started.notified().await;

        let deleted = client
            .delete_session("thread-delete".into())
            .await
            .expect("delete active session");
        cancelled.notified().await;
        run.await
            .expect("run task should settle")
            .expect("run result");

        assert!(deleted.deleted);
        assert!(!dir.join("thread-delete.jsonl").exists());
        assert!(!dir.join("thread-delete.jsonl.lock").exists());
    })
    .await
    .expect("active delete timed out");

    server.stop();
    done.await.ok();
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_title_waits_for_active_turn_before_rewriting_rollout() {
    let dir = unique_temp_dir("title-active-session");
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    write_persisted_session(&store, "thread-title", 1_700_000_000).await;
    let started = Arc::new(Notify::new());
    let cancelled = Arc::new(Notify::new());
    let agent = make_blocking_agent("/tmp/klaus-proto", started.clone(), cancelled);
    let mut lifecycle = protocol_lifecycle(agent.clone(), store.clone());
    let title_calls = Arc::new(AtomicUsize::new(0));
    let title_calls_for_update = title_calls.clone();
    let title_store = store.clone();
    lifecycle.update_title = Some(Arc::new(move |thread_id: String, title: String| {
        let title_calls = title_calls_for_update.clone();
        let store = title_store.clone();
        Box::pin(async move {
            title_calls.fetch_add(1, Ordering::SeqCst);
            store
                .update_title(&thread_id, &title)
                .await
                .map_err(|e| Box::new(e) as BoxError)?;
            Ok(UpdateTitleResult {
                thread_id: thread_id.into(),
                title: title.into(),
            })
        })
    }));
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server_with_lifecycle(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        Some(lifecycle),
        sr,
        sw,
    );
    let client = Arc::new(ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    }));

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        client
            .resume_session("thread-title".into())
            .await
            .expect("resume");
        let run_client = client.clone();
        let run = tokio::spawn(async move {
            run_client
                .run_to_completion(Input::Text("keep running".into()), None)
                .await
        });
        started.notified().await;
        let title_client = client.clone();
        let update = tokio::spawn(async move {
            title_client
                .update_title("thread-title".into(), "Updated safely".into())
                .await
        });

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            title_calls.load(Ordering::SeqCst),
            0,
            "title rewrite raced the active turn"
        );
        client.interrupt().await;
        run.await
            .expect("run task should settle")
            .expect("run result");
        update
            .await
            .expect("title task should settle")
            .expect("title update");
        assert_eq!(
            title_calls.load(Ordering::SeqCst),
            0,
            "active sessions must use their owned rollout writer instead of a second store writer"
        );
        assert_eq!(
            store.resume("thread-title").await.unwrap().meta.title,
            Some("Updated safely".into())
        );
    })
    .await
    .expect("active title update timed out");

    server.stop();
    done.await.ok();
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_host_metadata_uses_the_live_writer_instead_of_relocking_the_rollout() {
    let dir = unique_temp_dir("host-metadata-live-session");
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    write_persisted_session(&store, "thread-metadata", 1_700_000_000).await;
    let started = Arc::new(Notify::new());
    let cancelled = Arc::new(Notify::new());
    let agent = make_blocking_agent("/tmp/klaus-proto", started.clone(), cancelled);
    let mut lifecycle = protocol_lifecycle(agent.clone(), store.clone());
    let store_calls = Arc::new(AtomicUsize::new(0));
    let store_calls_for_update = store_calls.clone();
    let metadata_store = store.clone();
    lifecycle.update_thread_host_metadata = Some(Arc::new(move |thread_id, host_metadata| {
        let store_calls = store_calls_for_update.clone();
        let store = metadata_store.clone();
        Box::pin(async move {
            store_calls.fetch_add(1, Ordering::SeqCst);
            store
                .update_host_metadata(&thread_id, host_metadata)
                .await
                .map_err(|e| Box::new(e) as BoxError)?;
            Ok(UpdateThreadHostMetadataResult {
                thread_id: thread_id.into(),
                updated: true,
            })
        })
    }));
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server_with_lifecycle(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        Some(lifecycle),
        sr,
        sw,
    );
    let client = Arc::new(ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    }));

    client
        .resume_session("thread-metadata".into())
        .await
        .expect("resume");
    let run_client = client.clone();
    let run = tokio::spawn(async move {
        run_client
            .run_to_completion(Input::Text("keep running".into()), None)
            .await
    });
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, started.notified())
        .await
        .expect("turn started");
    let attachment = runtime_value(r#"{"sidebarPinnedAt":1700000000000}"#);
    // A blocked turn must not delay sidebar organization past the host's 3s
    // acknowledgement deadline or require interrupting the model.
    let result = timeout(
        Duration::from_secs(2),
        client.update_thread_host_metadata("thread-metadata".into(), Some(attachment.clone())),
    )
    .await
    .expect("pin must finish while the turn is still blocked")
    .expect("update live host metadata");

    assert!(result.updated);
    assert!(!run.is_finished(), "pin must not interrupt the active turn");
    assert_eq!(store_calls.load(Ordering::SeqCst), 0);
    let listed = store.list(ListOptions::default()).await.unwrap();
    assert_eq!(listed.items[0].host_metadata, Some(attachment));

    timeout(
        Duration::from_secs(2),
        client.update_thread_host_metadata("thread-metadata".into(), None),
    )
    .await
    .expect("unpin must finish while the turn is still blocked")
    .expect("remove live host metadata");
    assert!(
        !run.is_finished(),
        "unpin must not interrupt the active turn"
    );
    assert_eq!(store_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        store.list(ListOptions::default()).await.unwrap().items[0].host_metadata,
        None
    );
    client.interrupt().await;
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, run)
        .await
        .expect("run should settle")
        .expect("run task")
        .expect("run result");

    server.stop();
    done.await.ok();
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_session_config_changes_only_mutable_fields_on_the_next_turn() {
    let seen: Arc<StdMutex<Vec<Option<RuntimeString>>>> = Arc::new(StdMutex::new(Vec::new()));
    let base = scripted_call_model(vec![text_turn("one"), text_turn("two")]);
    let seen2 = seen.clone();
    let call_model: klaus_agent_core::types::provider::CallModel = Box::new(move |req, signal| {
        seen2.lock().unwrap().push(Some(req.model.clone()));
        base(req, signal)
    });
    let agent = Arc::new(Agent::new(AgentOptions {
        process_context: test_process_context(),
        provider: Arc::new(fake_provider()) as Arc<dyn ModelProvider>,
        config: None,
        tools: None,
        web_fetch_page: None,
        browser_runtime: None,
        capability_roots: Vec::new(),
        cwd: Some(".".to_string()),
        deps: EngineDepsOverrides {
            call_model: Some(call_model),
            estimate_tokens: None,
            compactor: None,
            uuid: None,
            now: None,
        },
        compactor: None,
        approval_handler: None,
        approval_reviewer: None,
        elicit_handler: None,
        credential_store: None,
        hooks: vec![],
        observer: None,
        logger: None,
        pricing: None,
        agents: None,
        include_default_agents: None,
        skills: None,
        output_styles: None,
        workflows: None,
        mcp_catalog: None,
        plugin_catalog: None,
        lsp_servers: None,
        agent_memory: None,
        mcp_clients: None,
        mcp_server_configs: Vec::new(),
        mcp_tools: None,
        refresh_tools: None,
        system_prompt_provider: None,
        plugins: Vec::new(),
        plugin_load_report: None,
    }));
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        let _ = client
            .run_to_completion(Input::Text("first".into()), None)
            .await;
        let updated = client
            .update_session_config(ProtocolSessionConfigUpdate {
                ultra_mode: Some(true),
                subagent_model_catalog: Some(true),
                subagent_model_options: Some(vec![
                    klaus_agent_core::schemas::config::SubagentModelOption {
                        value: "custom:available".into(),
                        label: "Available model".into(),
                    },
                ]),
                goal: None,
                maximum_goal_token_budget: None,
                model: Some("claude-sonnet-4-6".into()),
                permission_mode: Some(ExternalPermissionMode::Plan),
                ..Default::default()
            })
            .await
            .expect("config update");
        assert_eq!(
            updated.model.as_ref(),
            Some(&RuntimeString::from("claude-sonnet-4-6"))
        );
        assert_eq!(updated.permission_mode, None);
        assert!(runtime_codec::encode_value(&updated)
            .unwrap()
            .get("permissionMode")
            .is_none());
        assert_eq!(updated.ultra_mode, Some(true));
        assert_eq!(updated.subagent_model_catalog, Some(true));
        assert_eq!(
            updated.subagent_model_options.unwrap()[0].value,
            "custom:available"
        );
        let mut events = client.events();
        client
            .send(&Op {
                id: "updated-planning".into(),
                op: OpKind::GetPlanning,
            })
            .await;
        let planning = loop {
            let event = events.next().await.expect("planning snapshot reply");
            if event.submission_id == "updated-planning" {
                assert!(event.error.is_none(), "{event:?}");
                break event.planning.clone().expect("canonical planning snapshot");
            }
        };
        assert_eq!(
            planning.mode,
            klaus_agent_core::planning::CollaborationMode::Plan
        );
        let _ = client
            .run_to_completion(Input::Text("second".into()), None)
            .await;
    })
    .await
    .expect("timed out");

    server.stop();
    done.await.ok();

    let seen = seen.lock().unwrap();
    assert!(seen.len() >= 2);
    // Context migration can summarize the source model before the next main request.
    assert_eq!(
        seen.last().and_then(Option::as_ref),
        Some(&RuntimeString::from("claude-sonnet-4-6")),
        "captured request models: {seen:?}"
    );
    assert_ne!(
        seen[0].as_ref(),
        Some(&RuntimeString::from("claude-sonnet-4-6"))
    );
}

/// A live subagent-model directory update must settle while an earlier turn is
/// still holding its provider stream, and the next captured step must observe
/// it. This update used to ride the serial turn chain, so its applied
/// acknowledgement arrived only after the running turn finished.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_session_config_subagent_model_applies_while_turn_is_running() {
    let requests = Arc::new(StdMutex::new(Vec::<ModelRequest>::new()));
    let provider_started = Arc::new(Notify::new());
    let release_provider = Arc::new(tokio::sync::Semaphore::new(0));
    let calls = Arc::new(AtomicUsize::new(0));

    let captured = requests.clone();
    let started = provider_started.clone();
    let gate = release_provider.clone();
    let seen_calls = calls.clone();
    let call_model: klaus_agent_core::types::provider::CallModel =
        Box::new(move |request, signal| {
            captured.lock().unwrap().push(request);
            if seen_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                started.notify_one();
                let gate = gate.clone();
                Box::pin(
                    futures::stream::once(async move {
                        tokio::select! {
                            _ = signal.cancelled() => Vec::new(),
                            _permit = gate.acquire() => text_turn("first"),
                        }
                    })
                    .flat_map(futures::stream::iter),
                )
            } else {
                Box::pin(futures::stream::iter(text_turn("second")))
            }
        });
    let agent = Arc::new(Agent::new(AgentOptions {
        process_context: test_process_context(),
        provider: Arc::new(fake_provider()) as Arc<dyn ModelProvider>,
        config: None,
        tools: None,
        web_fetch_page: None,
        browser_runtime: None,
        capability_roots: Vec::new(),
        cwd: Some(".".to_string()),
        deps: EngineDepsOverrides {
            call_model: Some(call_model),
            estimate_tokens: None,
            compactor: None,
            uuid: None,
            now: None,
        },
        compactor: None,
        approval_handler: None,
        approval_reviewer: None,
        elicit_handler: None,
        credential_store: None,
        hooks: vec![],
        observer: None,
        logger: None,
        pricing: None,
        agents: None,
        include_default_agents: None,
        skills: None,
        output_styles: None,
        workflows: None,
        mcp_catalog: None,
        plugin_catalog: None,
        lsp_servers: None,
        agent_memory: None,
        mcp_clients: None,
        mcp_server_configs: Vec::new(),
        mcp_tools: None,
        refresh_tools: None,
        system_prompt_provider: None,
        plugins: Vec::new(),
        plugin_load_report: None,
    }));
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(
        make_create_session(agent, Some(ExternalPermissionMode::BypassPermissions)),
        sr,
        sw,
    );
    let client = Arc::new(ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    }));

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        let running = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .run_to_completion(Input::Text("first".into()), None)
                    .await
            })
        };
        timeout(Duration::from_secs(5), provider_started.notified())
            .await
            .expect("the first provider request stays held");

        // Turn the catalog ON while the provider is still held.
        let on_start = std::time::Instant::now();
        let updated_on = timeout(
            Duration::from_secs(2),
            client.update_session_config(ProtocolSessionConfigUpdate {
                subagent_model_catalog: Some(true),
                subagent_model_options: Some(vec![
                    klaus_agent_core::schemas::config::SubagentModelOption {
                        value: "custom:available".into(),
                        label: "Available model".into(),
                    },
                ]),
                ..Default::default()
            }),
        )
        .await
        .expect("the subagent directory update must not wait for the running turn")
        .expect("config update");
        let on_elapsed = on_start.elapsed();
        assert_eq!(updated_on.subagent_model_catalog, Some(true));
        assert_eq!(
            updated_on.subagent_model_options.as_ref().unwrap()[0].value,
            "custom:available"
        );
        assert!(
            !running.is_finished(),
            "the turn is still held by its provider when the ON update settles"
        );

        // Turn the catalog OFF while the same provider call is still held.
        let off_start = std::time::Instant::now();
        let updated_off = timeout(
            Duration::from_secs(2),
            client.update_session_config(ProtocolSessionConfigUpdate {
                subagent_model_catalog: Some(false),
                subagent_model_options: Some(vec![]),
                ..Default::default()
            }),
        )
        .await
        .expect("the subagent directory update must not wait for the running turn")
        .expect("config update");
        let off_elapsed = off_start.elapsed();
        assert_eq!(updated_off.subagent_model_catalog, Some(false));
        assert!(updated_off
            .subagent_model_options
            .as_ref()
            .unwrap()
            .is_empty());
        assert!(
            !running.is_finished(),
            "the turn is still held by its provider when the OFF update settles"
        );
        eprintln!(
            "subagent-model-toggle-timing: on={:?} off={:?} while turn held",
            on_elapsed, off_elapsed
        );

        release_provider.add_permits(1);
        running.await.expect("run task").expect("first turn");
        let _ = client
            .run_to_completion(Input::Text("second".into()), None)
            .await;
    })
    .await
    .expect("timed out");

    server.stop();
    done.await.ok();

    let requests = requests.lock().unwrap();
    assert!(requests.len() >= 2, "captured requests: {}", requests.len());
    let last = requests.last().unwrap();
    let tool = last
        .tools
        .iter()
        .find(|tool| tool.name == "task" || tool.name == "spawn_agent" || tool.name == "Task")
        .expect("the next captured step carries the subagent tool");
    assert_eq!(
        tool.input_schema["properties"]["model"]["enum"],
        klaus_agent_core::runtime_values::runtime_json::parse(&r#"["low","medium","high"]"#.into())
            .unwrap(),
        "the captured OFF directory takes effect on the next step"
    );
}

// This path runs two full protocol session creations. Its worker frames exceed
// Tokio's 2 MiB default in debug builds, so give the test runtime the same
// explicit stack budget used by the other full-session round-trip tests.
#[test]
fn a_failing_create_session_rejects_the_op_and_retries() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_stack_size(16 * 1024 * 1024)
        .enable_all()
        .build()
        .expect("build runtime");
    runtime.block_on(async {
        let attempts = Arc::new(AtomicUsize::new(0));
        let agent = make_agent(".", vec![text_turn("recovered")], None);
        let attempts2 = attempts.clone();
        let create_session: CreateSession = Arc::new(move |args: CreateSessionArgs| {
            let agent = agent.clone();
            let attempts = attempts2.clone();
            Box::pin(async move {
                let n = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                if n == 1 {
                    return Err::<Arc<Session>, BoxError>("resume: no such thread".into());
                }
                Ok(agent.start_session(StartSessionOptions {
                    approval_handler: Some(args.approval_handler),
                    elicit_handler: Some(args.elicit_handler),
                    mcp_elicitation_presenter: Some(args.mcp_elicitation_presenter),
                    mcp_authorize_handler: Some(args.mcp_authorize_handler),
                    mcp_oauth_redirect_uri: args
                        .init
                        .mcp_oauth_redirect_uri
                        .clone()
                        .map(|value| scalar_runtime_string(value, "MCP OAuth redirect URI"))
                        .transpose()?,
                    cwd: args.init.cwd.clone().map(Into::into),
                    config: args
                        .init
                        .config
                        .as_ref()
                        .map(|value| scalar_json_value(value, "session config"))
                        .transpose()?,
                    permission_mode: args
                        .init
                        .permission_mode
                        .or(Some(ExternalPermissionMode::BypassPermissions)),
                    ..Default::default()
                }))
            }) as CreateSessionFuture
        });
        let ((sr, sw), (cr, cw)) = duplex_pair();
        let (server, done) = start_server(create_session, sr, sw);
        let client = ProtocolClient::new(ProtocolClientOptions {
            input: cr,
            output: cw,
        });

        timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
            let first = client
                .run_to_completion(Input::Text("first".into()), None)
                .await;
            match first {
                Err(ClientError::Op(msg)) => assert!(msg.to_lowercase().contains("no such thread")),
                other => panic!("expected an op error, got {other:?}"),
            }
            let ok = client
                .run_to_completion(Input::Text("second".into()), None)
                .await
                .expect("second run");
            assert_eq!(ok.final_text, "recovered");
        })
        .await
        .expect("timed out");

        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        server.stop();
        done.await.ok();
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_resume_stop_fences_frames_parked_behind_session_publication() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let calls = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let factory = make_create_session(make_recording_agent(".", calls.clone()), None);
    let create: CreateSession = {
        let entered = entered.clone();
        let release = release.clone();
        Arc::new(move |args| {
            let entered = entered.clone();
            let release = release.clone();
            let factory = factory.clone();
            Box::pin(async move {
                entered.notify_one();
                release.notified().await;
                factory(args).await
            })
        })
    };
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(create, sr, sw);
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });
    let mut events = client.events();
    client
        .send(&Op {
            id: "blocked-history".into(),
            op: OpKind::GetHistory,
        })
        .await;
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, entered.notified())
        .await
        .unwrap();
    client
        .send(&Op {
            id: "parked-run".into(),
            op: OpKind::Run {
                input: Input::Text("must not start".into()),
                options: None,
            },
        })
        .await;
    client
        .send(&Op {
            id: "parked-resume".into(),
            op: OpKind::ResumePending {
                after_submission_id: "previous".into(),
            },
        })
        .await;
    client
        .send(&Op {
            id: "stop-parked-owner".into(),
            op: OpKind::Interrupt {
                origin: Some(crate::types::ProtocolInterruptOrigin::UserStop),
                expected_turn_id: Some("parked-run".into()),
            },
        })
        .await;
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        loop {
            let event = events.next().await.unwrap();
            if event.submission_id == "stop-parked-owner" {
                assert!(event.error.is_none(), "{:?}", event.error);
                let result = event
                    .turn_interrupt
                    .as_ref()
                    .expect("the real Stop has a correlated result");
                assert!(result.accepted);
                assert_eq!(result.interrupted_turn_id, Some("parked-run".into()));
                break;
            }
        }
    })
    .await
    .expect("the captured Stop settles while the lazy factory is held");
    {
        let admission = server.inner.run_admission.lock().unwrap();
        assert_eq!(admission.foreground.len(), 2);
        assert_eq!(admission.foreground[0].submission_id, "parked-run");
        assert!(admission.foreground[0].signal.is_cancelled());
        assert_eq!(admission.foreground[1].submission_id, "parked-resume");
        assert!(!admission.foreground[1].signal.is_cancelled());
    }
    release.notify_one();
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        let mut rejected_run = false;
        let mut rejected_resume = false;
        while let Some(event) = events.next().await {
            if event.submission_id == "parked-run" {
                rejected_run = event.error.as_ref().is_some_and(|error| {
                    error
                        .to_node_utf8()
                        .windows(b"interrupted before admission".len())
                        .any(|chunk| chunk == b"interrupted before admission")
                });
            }
            if event.submission_id == "parked-resume" {
                rejected_resume = event.pending_resume.as_ref().is_some_and(|result| {
                    !result.started
                        && result
                            .reason
                            .as_ref()
                            .is_some_and(|reason| reason == "not_completed")
                });
            }
            if rejected_run && rejected_resume {
                break;
            }
        }
        assert!(rejected_run && rejected_resume);
    })
    .await
    .unwrap();
    assert!(calls.lock().unwrap().is_empty());
    server.stop();
    done.await.unwrap();
}

/// A `run` op's per-turn `effort` must reach the provider request.
///
/// This covers the seam between the two tests that already exist: the golden wire
/// trace proves `"effort"` DECODES into `RunOpOptions`, and the core regression
/// `r20260918_per_turn_effort_override` proves `TurnOptions::effort` reaches the
/// provider. Neither would notice if `server.rs` simply never copied one into the
/// other — a host delivering work into another conversation would then send an
/// effort override that silently does nothing, with every test still green.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_op_per_turn_effort_reaches_the_provider_request() {
    use klaus_agent_core::schemas::effort::{EffortLevel, EffortValue};

    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let agent = make_recording_agent(".", requests.clone());
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(make_create_session(agent, None), sr, sw);
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });

    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        // An explicit level.
        client
            .run_to_completion(
                Input::Text("think hard".into()),
                Some(RunOpOptions {
                    effort: Some(EffortValue::Level(EffortLevel::Max)),
                    ..Default::default()
                }),
            )
            .await
            .expect("run with an effort level");
        // A numeric effort, the other untagged variant.
        client
            .run_to_completion(
                Input::Text("think a little".into()),
                Some(RunOpOptions {
                    effort: Some(EffortValue::Number(20)),
                    ..Default::default()
                }),
            )
            .await
            .expect("run with a numeric effort");
        // No override at all: the request must carry none rather than inherit the
        // previous turn's value.
        client
            .run_to_completion(Input::Text("plain".into()), None)
            .await
            .expect("run without options");

        let captured = requests.lock().unwrap().clone();
        assert_eq!(captured.len(), 3, "one provider request per turn");
        assert_eq!(
            captured[0].effort,
            Some(EffortValue::Level(EffortLevel::Max)),
            "a level override survives the wire and reaches the provider"
        );
        assert_eq!(
            captured[1].effort,
            Some(EffortValue::Number(20)),
            "a numeric override is forwarded unchanged, not bucketed early"
        );
        assert_eq!(
            captured[2].effort, None,
            "an absent override stays absent instead of leaking the prior turn's"
        );
    })
    .await
    .expect("effort mapping test timed out");

    server.stop();
    done.await.ok();
}

/// Defect 20261003: a rejected queued foreground frame left its steer fence live.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn followup_admission_guard_releases_rejected_turn_and_fences_queued_turns() {
    let agent = make_agent(".", Vec::new(), Some(json!({"prewarm":false})));
    let ((sr, sw), (_cr, _cw)) = duplex_pair();
    let (server, done) = start_server(make_create_session(agent, None), sr, sw);
    let admit = |id: &str| {
        server
            .inner
            .admit_op(Op {
                id: id.into(),
                op: OpKind::Run {
                    input: Input::Text("foreground".into()),
                    options: None,
                },
            })
            .expect("foreground slot available")
    };
    let first = admit("first");
    assert!(server
        .inner
        .run_admission
        .lock()
        .unwrap()
        .steer_rejection(Some("first"), false)
        .is_none());
    let second = admit("second");
    assert!(server
        .inner
        .run_admission
        .lock()
        .unwrap()
        .steer_rejection(Some("second"), false)
        .unwrap()
        .contains("mismatch"));
    // Model a rejected queued frame: dropping it must retain the unrelated live turn.
    drop(second);
    assert!(server
        .inner
        .run_admission
        .lock()
        .unwrap()
        .steer_rejection(Some("first"), false)
        .is_none());
    drop(first);
    assert!(server
        .inner
        .run_admission
        .lock()
        .unwrap()
        .steer_rejection(Some("first"), false)
        .unwrap()
        .contains("no longer active"));
    let retry = admit("retry");
    assert!(server
        .inner
        .run_admission
        .lock()
        .unwrap()
        .steer_rejection(Some("retry"), false)
        .is_none());
    let retry_token = retry.foreground_guard.as_ref().unwrap().reservation_token;
    {
        let mut admission = server.inner.run_admission.lock().unwrap();
        // Goal turns retain their foreground owner through the terminal gap.
        admission.finish_steer(retry_token);
        assert!(admission.steer_rejection(Some("retry"), false).is_some());
        admission.mark_steerable(retry_token, false);
        assert!(admission.steer_rejection(Some("retry"), false).is_none());
        // A buffered next TurnStarted cannot reopen a cancelled owner after
        // interrupt has already installed the next admission generation.
        admission.signal.cancel();
        admission.signal = AbortSignal::new();
        admission.finish_steer(retry_token);
        admission.mark_steerable(retry_token, false);
        assert!(admission.steer_turn_id.is_none());
        assert!(admission.steer_rejection(Some("retry"), false).is_some());
    }
    drop(retry);

    // A resume awaiting its durable nonempty batch owns the serial front even
    // though it is not steerable yet. A later run must not claim that gap.
    let resume = server
        .inner
        .admit_op(Op {
            id: "starting-resume".into(),
            op: OpKind::ResumeQueued,
        })
        .expect("resume slot available");
    let resume_token = resume.foreground_guard.as_ref().unwrap().reservation_token;
    let later_run = admit("queued-behind-resume");
    assert!(server
        .inner
        .run_admission
        .lock()
        .unwrap()
        .steer_rejection(Some("queued-behind-resume"), false)
        .unwrap()
        .contains("no longer active"));
    server
        .inner
        .run_admission
        .lock()
        .unwrap()
        .mark_steerable(resume_token, false);
    assert!(server
        .inner
        .run_admission
        .lock()
        .unwrap()
        .steer_rejection(Some("starting-resume"), false)
        .is_none());
    assert!(server
        .inner
        .run_admission
        .lock()
        .unwrap()
        .steer_rejection(Some("queued-behind-resume"), false)
        .unwrap()
        .contains("mismatch"));
    server.inner.finish_steer_turn(resume_token);
    assert!(server
        .inner
        .run_admission
        .lock()
        .unwrap()
        .steer_rejection(Some("queued-behind-resume"), false)
        .is_some());
    drop(resume);
    assert!(server
        .inner
        .run_admission
        .lock()
        .unwrap()
        .steer_rejection(Some("queued-behind-resume"), false)
        .is_none());
    drop(later_run);
    assert!(server
        .inner
        .run_admission
        .lock()
        .unwrap()
        .foreground
        .is_empty());
    server.stop();
    done.await.unwrap();
}

/// Defect 20261003: a resume waiting for durability exposed a later run's steer identity.
/// Drive real protocol frames and a blocked durable claim to pin the recipient boundary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn followup_checked_steer_cannot_target_a_run_queued_behind_starting_resume() {
    let dir = unique_temp_dir("followup-resume-recipient");
    let gate = Arc::new(QueueFlushGate::default());
    let store = Arc::new(GatedQueueStore {
        inner: Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(dir.to_string_lossy().into_owned()),
            archive_dir: None,
        })),
        gate: gate.clone(),
    });
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let agent = make_recording_agent(".", requests.clone());
    let session_slot = Arc::new(Mutex::new(None::<Arc<Session>>));
    let create: CreateSession = {
        let session_slot = session_slot.clone();
        Arc::new(move |_args| {
            let store = store.clone();
            let agent = agent.clone();
            let session_slot = session_slot.clone();
            Box::pin(async move {
                let session = agent
                    .open_session(
                        store,
                        StartSessionOptions {
                            config: Some(json!({"prewarm":false,"goal":false})),
                            permission_mode: Some(ExternalPermissionMode::BypassPermissions),
                            ..Default::default()
                        },
                    )
                    .await?;
                *session_slot.lock().unwrap() = Some(session.clone());
                Ok(session)
            }) as CreateSessionFuture
        })
    };
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(create, sr, sw);
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });
    let mut events = client.events();
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        client
            .steer(Input::Text("seed pending input".into()), None)
            .await;
        let snapshot = client.get_queue_snapshot().await.unwrap();
        assert_eq!(snapshot.items.len(), 1);
        gate.arm_next_queue_flush();
        client
            .send(&Op {
                id: "resume-a".into(),
                op: OpKind::ResumeQueued,
            })
            .await;
        gate.wait_until_blocked().await;
        client
            .send(&Op {
                id: "run-b".into(),
                op: OpKind::Run {
                    input: Input::Text("next foreground".into()),
                    options: None,
                },
            })
            .await;
        client
            .send(&Op {
                id: "steer-b".into(),
                op: OpKind::Steer {
                    input: Input::Text("must not reach resume-a".into()),
                    priority: None,
                    delivery: None,
                    display_metadata: None,
                    expected_turn_id: Some("run-b".into()),
                    queue_message_id: Some("674b3696-1006-4354-a6c1-582939fbaaa4".into()),
                },
            })
            .await;
        loop {
            let event = events.next().await.unwrap();
            if event.submission_id == "steer-b" {
                assert!(event
                    .error
                    .as_ref()
                    .unwrap()
                    .try_to_utf8()
                    .unwrap()
                    .contains("no longer active"));
                break;
            }
        }
        let session = session_slot.lock().unwrap().clone().unwrap();
        assert_eq!(session.queued_messages().len(), 1);
        gate.release();
        let mut resume_finished = false;
        let mut run_finished = false;
        loop {
            let event = events.next().await.unwrap();
            if event.submission_id == "resume-a" && event.result.is_some() {
                resume_finished = true;
            }
            if event.submission_id == "run-b" && event.result.is_some() {
                run_finished = true;
            }
            if resume_finished && run_finished {
                break;
            }
        }
        assert!(session.queue_snapshot().items.is_empty());
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        for request in requests.iter() {
            let text = runtime_codec::encode_text(&request.messages)
                .unwrap()
                .try_to_utf8()
                .unwrap();
            assert!(!text.contains("must not reach resume-a"), "{text}");
        }
    })
    .await
    .expect("checked steer recipient test timed out");
    server.stop();
    done.await.unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

/// Defect 20261003: admitted foreground requests accumulated behind one slow turn.
/// The independent foreground bound rejects the overflow frame with its own ID.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn followup_foreground_roster_is_bounded_and_refuses_overflow_correlated() {
    let started = Arc::new(Notify::new());
    let cancelled = Arc::new(Notify::new());
    let agent = make_blocking_agent(".", started.clone(), cancelled.clone());
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(make_create_session(agent, None), sr, sw);
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });
    let mut events = client.events();
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        for index in 0..MAX_PENDING_PROTOCOL_LINES {
            client
                .send(&Op {
                    id: format!("bounded-{index}").into(),
                    op: OpKind::Run {
                        input: Input::Text("wait".into()),
                        options: None,
                    },
                })
                .await;
            if index == 0 {
                started.notified().await;
            }
        }
        client
            .send(&Op {
                id: "overflow-foreground".into(),
                op: OpKind::ResumeQueued,
            })
            .await;
        loop {
            let event = events.next().await.unwrap();
            if event.submission_id == "overflow-foreground" {
                assert!(event
                    .error
                    .as_ref()
                    .unwrap()
                    .try_to_utf8()
                    .unwrap()
                    .contains("foreground request queue is full"));
                break;
            }
        }
        assert_eq!(
            server.inner.run_admission.lock().unwrap().foreground.len(),
            MAX_PENDING_PROTOCOL_LINES
        );
        assert!(server
            .inner
            .run_admission
            .lock()
            .unwrap()
            .steer_rejection(Some("bounded-0"), false)
            .is_none());
        client.interrupt().await;
        cancelled.notified().await;
    })
    .await
    .expect("foreground bound test timed out");
    server.stop();
    done.await.unwrap();
    assert!(server
        .inner
        .run_admission
        .lock()
        .unwrap()
        .foreground
        .is_empty());
}

/// A completed goal turn transfers steering to its distinct admitted successor.
/// Stopping that owner preserves queued input and rejects stale steering.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn followup_goal_continuation_reactivates_its_owner_and_preserves_pending_on_interrupt() {
    let dir = unique_temp_dir("followup-goal-continuation");
    std::fs::create_dir_all(&dir).unwrap();
    let _env = ConfigDirEnvGuard::set(&dir);
    let started = Arc::new(Notify::new());
    let cancelled = Arc::new(Notify::new());
    let agent = make_seed_then_blocking_agent(".", started.clone(), cancelled.clone());
    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let factory: CreateSession = {
        let root = dir.to_string_lossy().into_owned();
        Arc::new(move |args: CreateSessionArgs| {
            let agent = agent.clone();
            let store = store.clone();
            let root = root.clone();
            Box::pin(async move {
                let lsp_servers = args.init.lsp_servers.clone();
                let skills = args.skills.clone();
                let output_styles = args.output_styles.clone();
                let session = agent
                    .open_session(
                        store,
                        StartSessionOptions {
                            id: args.init.thread_id.clone(),
                            user_config_dir: Some(root.into()),
                            durable_permissions: Some(args.durable_permissions),
                            approval_handler: Some(args.approval_handler),
                            elicit_handler: Some(args.elicit_handler),
                            mcp_elicitation_presenter: Some(args.mcp_elicitation_presenter),
                            mcp_authorize_handler: Some(args.mcp_authorize_handler),
                            mcp_oauth_redirect_uri: args
                                .init
                                .mcp_oauth_redirect_uri
                                .clone()
                                .map(|value| scalar_runtime_string(value, "MCP OAuth redirect URI"))
                                .transpose()?,
                            cwd: args.init.cwd.clone().map(Into::into),
                            config: args
                                .init
                                .config
                                .as_ref()
                                .map(|value| scalar_json_value(value, "session config"))
                                .transpose()?,
                            permission_mode: args.init.permission_mode,
                            provider: args.provider.clone(),
                            mcp_server_configs: Some(args.mcp_server_configs.clone()),
                            capability_roots: Some(args.capability_roots.clone()),
                            selected_capability_roots: args.selected_capability_roots.clone(),
                            base_instructions: args.init.base_instructions.clone(),
                            developer_instructions: args.init.developer_instructions.clone(),
                            agent_memory_snapshot_policy: args
                                .init
                                .agent_memory_snapshot_policy
                                .clone(),
                            mcp_tools: (!args.dynamic_tools.is_empty())
                                .then_some(args.dynamic_tools),
                            lsp_servers: (!lsp_servers.is_empty())
                                .then_some(Arc::new(move || lsp_servers.clone())),
                            agents: (!args.agents.is_empty()).then_some(args.agents),
                            skills: (!skills.is_empty())
                                .then_some(Arc::new(move || skills.clone())),
                            output_styles: (!output_styles.is_empty())
                                .then_some(Arc::new(move || output_styles.clone())),
                            selected_output_style: args.init.selected_output_style.clone(),
                            hooks: (!args.hooks.is_empty()).then_some(args.hooks),
                            pricing: args.pricing,
                            shell_env: args.init.shell_env.clone(),
                            initial_usage_ledger: None,
                            initial_subagent_lifecycle: None,
                            rollout: None,
                            ..Default::default()
                        },
                    )
                    .await?;
                assert!(session.goal_pursuit_eligible());
                assert!(session.goal_runtime().is_some());
                // Seed before installing the idle observer; the repeated wire setter
                // verifies the saved state without racing a new automatic admission.
                session
                    .set_thread_goal(
                        Some("keep working until the user stops the run".into()),
                        None,
                        None,
                    )
                    .await
                    .expect("the unpublished persistent fixture owns its initial goal");
                Ok::<Arc<Session>, BoxError>(session)
            }) as CreateSessionFuture
        })
    };
    let ((sr, sw), (cr, cw)) = duplex_pair();
    let (server, done) = start_server(factory, sr, sw);
    let client = ProtocolClient::new(ProtocolClientOptions {
        input: cr,
        output: cw,
    });
    let mut events = client.events();
    let queue_id = "d26f45e8-a4ab-48d2-99ca-0cffb8691c2d";
    timeout(CROSS_TASK_LIVENESS_TIMEOUT, async {
        client
            .init_session(ProtocolSessionInit {
                config: Some(
                    runtime_json_from_scalar_value(&json!({
                        "goal": true,
                        "prewarm": false,
                        "agentMemory": false,
                        "autoMemory": false,
                        "autoDream": false
                    }))
                    .unwrap(),
                ),
                ..Default::default()
            })
            .await
            .unwrap();
        client
            .send(&Op {
                id: "goal-set".into(),
                op: OpKind::SetGoal {
                    owned_resources: None,
                    client_request_id: None,
                    submit: None,
                    display_text: None,
                    expected_goal_id: None,
                    expected_goal_revision: None,
                    origin: None,
                    objective: Some("keep working until the user stops the run".into()),
                    status: None,
                    token_budget: None,
                },
            })
            .await;
        loop {
            let event = events.next().await.unwrap();
            if event.submission_id == "goal-set" {
                assert!(event.error.is_none(), "{:?}", event.error);
                assert!(event.thread_goal.is_some());
                break;
            }
        }
        client
            .send(&Op {
                id: "goal-foreground".into(),
                op: OpKind::Run {
                    input: Input::Text("start the goal".into()),
                    options: None,
                },
            })
            .await;
        let mut core_starts = 0;
        let mut first_completed = false;
        let mut successor = None;
        let mut decision_id = None;
        let mut continued = false;
        loop {
            let event = events.next().await.unwrap();
            if let Some(decision) = event.goal_continuation_decision.as_ref() {
                if decision.prior_submission_id == "goal-foreground" && decision.pending {
                    successor = decision.successor_submission_id.clone();
                    decision_id = Some(decision.decision_id.clone());
                } else if Some(&decision.decision_id) == decision_id.as_ref() && !decision.pending {
                    assert_eq!(decision.prior_submission_id, "goal-foreground");
                    assert_eq!(decision.successor_submission_id, successor);
                    assert_eq!(decision.continued, Some(true));
                    continued = true;
                }
            }
            let is_first = event.submission_id == "goal-foreground";
            let is_successor = successor.as_ref() == Some(&event.submission_id);
            if !is_first && !is_successor {
                continue;
            }
            assert!(event.error.is_none(), "{:?}", event.error);
            match event.event {
                Some(EngineEvent::TurnStarted { .. }) => {
                    core_starts += 1;
                    assert_eq!(event.goal_continuation, Some(is_successor));
                    if is_successor {
                        assert!(
                            first_completed,
                            "the second start follows the predecessor completion"
                        );
                        break;
                    }
                }
                Some(EngineEvent::TurnCompleted { .. }) if is_first => first_completed = true,
                _ => {}
            }
        }
        assert_eq!(core_starts, 2, "both real admissions start once");
        let successor = successor.expect("the active goal reserves a distinct successor");
        assert_ne!(successor, "goal-foreground");
        started.notified().await;
        if !continued {
            loop {
                let event = events.next().await.unwrap();
                if let Some(decision) = event.goal_continuation_decision.as_ref() {
                    if Some(&decision.decision_id) == decision_id.as_ref() && !decision.pending {
                        assert_eq!(decision.prior_submission_id, "goal-foreground");
                        assert_eq!(decision.successor_submission_id.as_ref(), Some(&successor));
                        assert_eq!(decision.continued, Some(true));
                        break;
                    }
                }
            }
        }
        client
            .send(&Op {
                id: "goal-followup".into(),
                op: OpKind::Steer {
                    input: Input::Text("preserve this user follow-up".into()),
                    priority: None,
                    delivery: None,
                    display_metadata: None,
                    expected_turn_id: Some(successor.clone()),
                    queue_message_id: Some(queue_id.into()),
                },
            })
            .await;
        loop {
            let event = events.next().await.unwrap();
            if event.submission_id == "goal-followup" {
                assert!(event.error.is_none(), "{:?}", event.error);
            }
            if event.queue_snapshot.as_ref().is_some_and(|snapshot| {
                snapshot
                    .items
                    .iter()
                    .any(|item| item.message.uuid.as_ref().is_some_and(|id| id == queue_id))
            }) {
                break;
            }
        }
        let snapshot = client.get_queue_snapshot().await.unwrap();
        assert_eq!(snapshot.items.len(), 1);
        assert_eq!(
            snapshot.items[0].message.uuid,
            Some(RuntimeString::from(queue_id))
        );
        client
            .send(&Op {
                id: "stop-goal-successor".into(),
                op: OpKind::Interrupt {
                    origin: Some(crate::types::ProtocolInterruptOrigin::UserStop),
                    expected_turn_id: Some(successor.clone()),
                },
            })
            .await;
        cancelled.notified().await;
        let mut stop_accepted = false;
        let mut successor_finished = false;
        while !stop_accepted || !successor_finished {
            let event = events.next().await.unwrap();
            if event.submission_id == "stop-goal-successor" {
                assert!(event.error.is_none(), "{:?}", event.error);
                let result = event.turn_interrupt.as_ref().expect("the real Stop result");
                assert!(result.accepted);
                assert_eq!(result.interrupted_turn_id.as_ref(), Some(&successor));
                stop_accepted = true;
            }
            if event.submission_id == successor && event.result.is_some() {
                successor_finished = true;
            }
        }
        let snapshot = client.get_queue_snapshot().await.unwrap();
        assert_eq!(snapshot.items.len(), 1);
        assert_eq!(
            snapshot.items[0].message.uuid,
            Some(RuntimeString::from(queue_id))
        );
        client
            .send(&Op {
                id: "goal-late-followup".into(),
                op: OpKind::Steer {
                    input: Input::Text("must not revive the cancelled goal owner".into()),
                    priority: None,
                    delivery: None,
                    display_metadata: None,
                    expected_turn_id: Some(successor.clone()),
                    queue_message_id: Some("01dca07a-3a62-48f7-9ed9-a0e6b99a11b3".into()),
                },
            })
            .await;
        loop {
            let event = events.next().await.unwrap();
            if event.submission_id == "goal-late-followup" {
                assert!(event
                    .error
                    .as_ref()
                    .unwrap()
                    .try_to_utf8()
                    .unwrap()
                    .contains("no longer active"));
                break;
            }
        }
        let snapshot = client.get_queue_snapshot().await.unwrap();
        assert_eq!(snapshot.items.len(), 1);
        assert_eq!(
            snapshot.items[0].message.uuid,
            Some(RuntimeString::from(queue_id))
        );
    })
    .await
    .expect("goal continuation steering test timed out");
    server.stop();
    done.await.unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn openai_model_default_effort_is_opt_in_and_round_trips() {
    for enabled in [false, true] {
        let init = OpenAiCompatibilityInit {
            instruction_role: None,
            use_model_default_effort: Some(enabled),
            extra_headers: None,
            request_identity: None,
            responses_function_tools: None,
        };
        let encoded = runtime_codec::encode_text(&init).unwrap();
        assert_eq!(
            encoded,
            RuntimeString::from(format!(r#"{{"useModelDefaultEffort":{enabled}}}"#))
        );
        let decoded: OpenAiCompatibilityInit = runtime_codec::decode_text(&encoded).unwrap();
        assert_eq!(decoded, init);
        for api in [OpenAiApi::Chat, OpenAiApi::Responses] {
            let options = openai_compatibility_from_init(Some(&decoded), api)
                .unwrap()
                .unwrap();
            assert!(
                format!("{options:?}").contains(&format!("use_model_default_effort: {enabled}"))
            );
        }
    }
    assert!(
        runtime_codec::decode_text::<OpenAiCompatibilityInit>(&RuntimeString::from(
            r#"{"useModelDefaultEffort":"true"}"#
        ))
        .is_err()
    );
}
