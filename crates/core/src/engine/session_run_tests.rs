use super::fork_snapshot;
#[path = "session_subagent_model_policy_tests.rs"]
mod subagent_model_policy_tests;
use super::forwarded_resume;
#[path = "session_subagent_message_tests.rs"]
mod ordinary_subagent_message_tests;
#[path = "session_prediction_tests.rs"]
mod prediction_tests;
#[path = "session_side_question_tests.rs"]
mod side_question_tests;
use super::inject_setup_owned_task_panic;
use super::memory_maintenance_event_status;
use super::message_uuid;
use super::normalize_document_filename;
use super::parse_generated_title;
use super::persistent_resume_key;
// Gated with their only users: the team/swarm, task-list-reaper, symlinked-memory and
// re-exec cases below are all unix-only, so on Windows these name nothing.
#[cfg(unix)]
use super::reap_task_list_subscriptions;
use super::resolve_abs;
use super::runtime_forwarded_slot_to_typed_host;
use super::runtime_path_from_host_path;
use super::runtime_path_to_file_io_host;
use super::ActiveTurnLease;
use super::AgentMemorySnapshotMergeDelivery;
use super::AnyTool;
use super::ContextWindowState;
use super::DreamInProgressGuard;
use super::DreamNowCompletion;
use super::DreamNowReason;
use super::EngineEventStream;
use super::EnqueueOptions;
use super::GetConversation;
use super::Input;
use super::JsForwardedRawSlot;
use super::MaintenanceTask;
use super::MaintenanceTaskKind;
use super::ReviewRequest;
use super::ReviewTarget;
use super::RolloutSink;
use super::RunSubAgentOpts;
use super::Session;
use super::SessionInit;
use super::SetupOwnedTaskKind;
use super::SubAgentMapKey;
#[cfg(unix)]
use super::TaskListSubscriptionReaper;
use super::ToolError;
use super::TurnOptions;
use super::UserContentPart;
use super::DREAM_SCAN_INTERVAL_MS;
use super::MAINTENANCE_FORCE_ABORT_ERROR;
use super::MAINTENANCE_SHUTDOWN_ERROR;
use super::MANUAL_DREAM_SHUTDOWN_ERROR;
use super::RAW_PERSISTED_RESUME_ERROR;
use super::SESSION_SHUTDOWN_CLEANUP_MS;
use super::SESSION_SHUTDOWN_FLUSH_MS;
use super::SESSION_SHUTDOWN_GRACE_MS;
use super::SESSION_SHUTDOWN_PERSISTENCE_MS;
use crate::browser::BrowserRuntime;
use crate::browser::{attest_spawn_free, BrowserExecutor};
use crate::capabilities::RuntimeCapabilityRegistry;
#[cfg(unix)]
use crate::command_runtime::background::StartBackgroundCommandOptions;
use crate::engine::agent_definitions::AgentDefinition;
use crate::engine::deps::{noop_compactor, EngineDeps};
use crate::engine::fork::is_in_fork_child;
use crate::engine::message_queue::QueueEdit;
use crate::engine::message_queue::QueuePriority;
use crate::engine::message_queue::QueueTerminalState;
use crate::engine::message_queue::QueuedValue;
use crate::engine::message_queue::QUEUE_STATE_ROLLOUT_EVENT;
use crate::engine::session_config::resolve_config;
use crate::engine::subagent_lifecycle::SubagentLifecycleRecord;
use crate::extensions::commands::{Command, CommandMeta, PromptCommand};
use crate::extensions::runtime_snapshot::ExtensionRuntimeSnapshot;
use crate::fs::node_path::posix_rendered_runtime;
use crate::fs::path_safety::canonical_descendant_scope;
use crate::fs::paths::config_home;
use crate::hooks::bus::HookBus;
use crate::hooks::setup::SetupContextAdmissionRecord;
use crate::hooks::setup::SetupContextAdmissionStatus;
use crate::hooks::setup::SetupLifecycleEvent;
use crate::hooks::setup::SetupLifecycleSnapshot;
use crate::hooks::setup::SetupRunRecord;
use crate::hooks::setup::SetupRunRequest;
use crate::hooks::setup::SetupRunStatus;
use crate::hooks::setup::SetupTrigger;
use crate::hooks::setup::SETUP_LIFECYCLE_ROLLOUT_EVENT;
use crate::hooks::types::HookEvent;
use crate::hooks::types::HookInput;
use crate::hooks::types::HookTrigger;
use crate::hooks::types::{Hook, HookFn, HookOutcome, HookResult};
#[cfg(unix)]
use crate::memory::agent_memory::default_agent_memory_layout;
use crate::memory::agent_memory::AgentMemoryHost;
use crate::memory::agent_memory::AgentMemoryLayout;
use crate::memory::agent_memory::AgentMemoryMergeChoice;
use crate::memory::agent_memory::AgentMemoryScope;
use crate::memory::agent_memory::AgentMemorySyncRequest;
use crate::memory::consolidation_lock::read_last_consolidated_at;
use crate::memory::consolidation_lock::record_consolidation;
use crate::memory::consolidation_lock::try_acquire_consolidation_lock;
use crate::memory::consolidation_lock::try_acquire_consolidation_lock_pinned;
use crate::memory::maintenance::AutoDreamRun;
use crate::memory::maintenance::AutoDreamRunReason;
use crate::memory::maintenance::AutoMemoryRun;
use crate::memory::maintenance::AutoMemoryRunReason;
use crate::memory::maintenance::MemoryMaintenanceEvent;
use crate::memory::maintenance::MemoryMaintenanceRunKind;
use crate::memory::maintenance::MemoryMaintenanceSnapshot;
use crate::memory::maintenance::MemoryMaintenanceStatus;
use crate::memory::maintenance::MemoryMaintenanceTrigger;
use crate::memory::maintenance::MEMORY_MAINTENANCE_ROLLOUT_EVENT;
use crate::memory::snapshot_decision::prepare_agent_memory_snapshot_request;
use crate::memory::snapshot_decision::AgentMemorySnapshotApplyError;
use crate::memory::snapshot_decision::AgentMemorySnapshotDecisionAudit;
use crate::memory::snapshot_decision::AgentMemorySnapshotDecisionPolicy;
use crate::memory::snapshot_decision::AgentMemorySnapshotDecisionRegistry;
use crate::memory::snapshot_decision::AgentMemorySnapshotDecisionRequest;
use crate::memory::snapshot_decision::AgentMemorySnapshotDecisionSource;
use crate::memory::snapshot_decision::AgentMemorySnapshotDecisionStatus;
use crate::memory::snapshot_decision::AgentMemorySnapshotRegistryError;
use crate::memory::snapshot_decision::AGENT_MEMORY_SNAPSHOT_DECISION_ROLLOUT_EVENT;
use crate::runtime_values::runtime_codec;
use crate::runtime_values::runtime_json;
use crate::runtime_values::RuntimeJson;
use crate::runtime_values::RuntimeObject;
use crate::runtime_values::RuntimePath;
use crate::runtime_values::RuntimeString;
#[cfg(unix)]
use crate::sandbox::process::ProcessSandbox;
#[cfg(unix)]
use crate::schemas::background_process::BackgroundProcessStatus;
use crate::schemas::config::default_config;
use crate::schemas::config::ResolvedConfig;
use crate::schemas::content::ContentBlock;
use crate::schemas::content::DocumentBlock;
use crate::schemas::content::TextBlock;
use crate::schemas::message::AssistantMessage;
use crate::schemas::message::Message;
use crate::schemas::message::SystemSubtype;
use crate::schemas::message::Usage;
use crate::schemas::message::UserMessage;
use crate::schemas::rollout::TurnContextSnapshot;
use crate::schemas::rollout::UsageLedgerItem;
use crate::schemas::rollout::{RolloutItem, SessionMeta};
use crate::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use crate::store::session_store::AppendEventOptions;
use crate::store::session_store::CheckpointRecord;
use crate::store::session_store::CompactionRecord;
use crate::store::session_store::LiveRollout;
use crate::store::session_store::RollbackRecord;
use crate::store::session_store::SessionStore;
#[cfg(unix)]
use crate::tasks::swarm_store::register_peer as swarm_register_peer;
#[cfg(unix)]
use crate::tasks::swarm_store::team_confirmed_gone as swarm_team_confirmed_gone;
#[cfg(unix)]
use crate::tasks::swarm_store::write_team_config as swarm_write_team_config;
#[cfg(unix)]
use crate::tasks::swarm_store::TeamConfigInput;
#[cfg(unix)]
use crate::tasks::task_store::subscribe_task_states;
#[cfg(unix)]
use crate::tasks::task_store::team_task_list_id;
#[cfg(unix)]
use crate::tasks::task_store::TaskListBinding;
use crate::testing::{ev, fake_provider, make_usage, scripted_call_model, ScriptedTurns};
use crate::tools::build_tool::text_result;
use crate::tools::builtins::read::create_read_tool;
use crate::tools::builtins::write::create_write_tool;
use crate::types::abort::AbortSignal;
use crate::types::errors::EngineErrorCode;
use crate::types::errors::InputValidationErrorCode;
use crate::types::errors::TerminalReason;
use crate::types::events::EngineEvent;
use crate::types::events::ThreadItem;
use crate::types::events::ToolDomainEvent;
use crate::types::json::Json;
use crate::types::json::JsonSchema;
use crate::types::provider::ModelProvider;
use crate::types::provider::{
    ModelRequest, ModelStopReason, ModelStream, ProviderCapabilities, ProviderInfo,
};
use crate::types::tool::ForegroundSubAgentRun;
use crate::types::tool::ForkContext;
use crate::types::tool::PositiveForkNumber;
use crate::types::tool::SpawnForegroundSubAgent;
use crate::types::tool::SpawnSubAgentOptions;
use crate::types::tool::Tool;
use crate::types::tool::ToolContext;
use crate::types::tool::{ToolProgress, ToolResult};
use async_stream::stream;
use base64::Engine;
#[cfg(unix)]
use futures::future::FutureExt;
use futures::stream::StreamExt;
use std::num::NonZeroUsize;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use tokio_util::sync::CancellationToken;

#[cfg(unix)]
struct ConfigEnvGuard {
    _lock: std::sync::MutexGuard<'static, ()>,
    previous: Option<String>,
}

#[cfg(unix)]
impl ConfigEnvGuard {
    fn set(value: &str) -> Self {
        let lock = crate::test_support::CONFIG_ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous = std::env::var("KLAUS_CONFIG_DIR").ok();
        crate::runtime_process::set_process_environment_var("KLAUS_CONFIG_DIR", value);
        Self {
            _lock: lock,
            previous,
        }
    }
}

#[cfg(unix)]
impl Drop for ConfigEnvGuard {
    fn drop(&mut self) {
        match &self.previous {
            Some(value) => {
                crate::runtime_process::set_process_environment_var("KLAUS_CONFIG_DIR", value)
            }
            None => crate::runtime_process::remove_process_environment_var("KLAUS_CONFIG_DIR"),
        }
    }
}

macro_rules! exact_json {
        ($($json:tt)+) => {{
            crate::runtime_values::runtime_json::parse(&RuntimeString::from(stringify!($($json)+)))
                .expect("test JSON literal must parse as exact runtime JSON")
        }};
    }

fn exact_tool_use_events(
    id: &str,
    name: &str,
    input: &RuntimeJson,
) -> Vec<crate::types::provider::ModelStreamEvent> {
    use crate::types::provider::ModelStreamEvent;

    vec![
        ModelStreamEvent::ToolUseStart {
            id: id.into(),
            name: name.into(),
            thought_signature: None,
            tool_kind: None,
            input_format: None,
        },
        ModelStreamEvent::ToolUseInputDelta {
            id: id.into(),
            partial_json: crate::runtime_values::runtime_json::stringify(input),
        },
        ModelStreamEvent::ToolUseStop { id: id.into() },
    ]
}

fn mk_deps(turns: ScriptedTurns) -> EngineDeps {
    let counter = Arc::new(AtomicUsize::new(0));
    EngineDeps {
        provider_id: "test-provider".into(),
        call_model: scripted_call_model(turns),
        estimate_tokens: Arc::new(|_| 0),
        compactor: noop_compactor(),
        uuid: {
            let c = counter.clone();
            Arc::new(move || format!("uuid-{}", c.fetch_add(1, Ordering::SeqCst)))
        },
        now: Arc::new(|| 0),
    }
}

fn unique_subagent_test_deps(mut deps: EngineDeps) -> EngineDeps {
    static NEXT_PREFIX: AtomicUsize = AtomicUsize::new(0);
    let prefix = format!(
        "{}-{}",
        std::process::id(),
        NEXT_PREFIX.fetch_add(1, Ordering::SeqCst)
    );
    let counter = Arc::new(AtomicUsize::new(0));
    deps.uuid = {
        let counter = counter.clone();
        Arc::new(move || {
            format!(
                "subagent-test-{prefix}-{}",
                counter.fetch_add(1, Ordering::SeqCst)
            )
        })
    };
    deps
}

fn mk_session_with(
    turns: ScriptedTurns,
    config: ResolvedConfig,
    tools: Vec<AnyTool>,
) -> Arc<Session> {
    mk_session_with_deps(mk_deps(turns), config, tools)
}

fn mk_session_with_deps(
    deps: EngineDeps,
    config: ResolvedConfig,
    tools: Vec<AnyTool>,
) -> Arc<Session> {
    mk_session_with_deps_and_memory(deps, config, tools, None)
}

fn mk_session_with_deps_and_memory(
    deps: EngineDeps,
    config: ResolvedConfig,
    tools: Vec<AnyTool>,
    agent_memory: Option<Arc<dyn AgentMemoryHost>>,
) -> Arc<Session> {
    mk_session_with_provider_and_memory(
        Arc::new(fake_provider()),
        deps,
        config,
        tools,
        agent_memory,
    )
}

fn mk_session_with_provider_and_memory(
    provider: Arc<dyn ModelProvider>,
    deps: EngineDeps,
    config: ResolvedConfig,
    tools: Vec<AnyTool>,
    agent_memory: Option<Arc<dyn AgentMemoryHost>>,
) -> Arc<Session> {
    mk_session_with_provider_memory_and_store(
        provider,
        deps,
        config,
        tools,
        agent_memory,
        None,
        None,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn mk_session_with_provider_memory_and_store(
    provider: Arc<dyn ModelProvider>,
    deps: EngineDeps,
    config: ResolvedConfig,
    tools: Vec<AnyTool>,
    agent_memory: Option<Arc<dyn AgentMemoryHost>>,
    rollout: Option<RolloutSink>,
    store: Option<Arc<dyn SessionStore>>,
    initial_history: Option<Vec<Message>>,
) -> Arc<Session> {
    mk_session_with_provider_memory_store_and_agents(
        provider,
        deps,
        config,
        tools,
        agent_memory,
        rollout,
        store,
        initial_history,
        None,
    )
}

#[cfg(unix)]
fn session_background_options(command: &str) -> StartBackgroundCommandOptions {
    StartBackgroundCommandOptions {
        command: command.into(),
        cwd: runtime_path_from_host_path(&std::env::temp_dir()),
        sandbox: Arc::new(ProcessSandbox::new()),
        env: None,
        max_output_bytes: 1024,
        writable_roots: None,
        unconfined_writes: false,
        network_allowed: None,
        managed_network: None,
        on_complete: None,
    }
}

#[cfg(unix)]
#[tokio::test]
async fn background_processes_are_session_isolated() {
    let first = mk_session(Vec::new());
    let second = mk_session(Vec::new());
    let id = first
        .background_processes
        .start(
            first.process_context.clone(),
            session_background_options("sleep 30"),
        )
        .await
        .unwrap();

    assert!(first
        .background_process_snapshot()
        .processes
        .iter()
        .any(|p| p.shell_id == id));
    assert!(second.background_process_snapshot().processes.is_empty());
    assert!(
        !second
            .stop_background_process(&id, crate::tools::shell::Signal::Kill)
            .found
    );

    first.shutdown_and_wait().await.unwrap();
    second.shutdown_and_wait().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn shutdown_stops_and_awaits_session_background_processes() {
    let session = mk_session(Vec::new());
    let id = session
        .background_processes
        .start(
            session.process_context.clone(),
            session_background_options("sleep 30"),
        )
        .await
        .unwrap();

    session.shutdown_and_wait().await.unwrap();
    let record = session
        .background_process_snapshot()
        .processes
        .into_iter()
        .find(|record| record.shell_id == id)
        .expect("background process remains queryable after shutdown");
    assert_eq!(record.status, BackgroundProcessStatus::Stopped);
    assert!(session
        .background_processes
        .start(
            session.process_context.clone(),
            session_background_options("exit 0"),
        )
        .await
        .is_err());
}

#[cfg(unix)]
#[test]
#[ignore = "HTTP preview fixture, invoked only by the publication regression"]
fn snapshot_preview_http_process_fixture() {
    use std::io::{Read, Write};
    let port_file = std::env::var_os("KLAUS_SNAPSHOT_TEST_PREVIEW_PORT_FILE")
        .expect("the owning regression supplies a private port file");
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    std::fs::write(port_file, listener.local_addr().unwrap().to_string()).unwrap();
    for socket in listener.incoming() {
        let mut socket = socket.unwrap();
        socket
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        let mut request = [0; 4096];
        if socket.read(&mut request).is_ok() {
            let body = "snapshot-preview-live";
            write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        }
    }
}

#[cfg(unix)]
struct SnapshotContinuationProvider {
    info: ProviderInfo,
}

#[cfg(unix)]
struct SnapshotPreviewCleanup {
    session: Arc<Session>,
    shell_id: RuntimeString,
}

#[cfg(unix)]
impl Drop for SnapshotPreviewCleanup {
    fn drop(&mut self) {
        let _ = self
            .session
            .stop_background_process(&self.shell_id, crate::tools::shell::Signal::Kill);
    }
}

#[cfg(unix)]
impl ModelProvider for SnapshotContinuationProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }
    fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
        fake_provider().capabilities(model)
    }
    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        use crate::types::provider::ModelStreamEvent;
        Box::pin(futures::stream::iter(vec![
            ModelStreamEvent::MessageStart {
                model: request.model,
            },
            ModelStreamEvent::TextDelta {
                text: "Chat continues while the preview stays alive".into(),
            },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: make_usage(Some(1), Some(1)),
            },
        ]))
    }
}

#[cfg(unix)]
#[tokio::test]
async fn live_model_reconfigure_publishes_without_stopping_background_processes() {
    let root = temp_store_dir();
    let port_file = root.join("preview-address");
    let quoted = |path: &Path| format!("'{}'", path.to_string_lossy().replace('\'', "'\\''"));
    let command = format!(
        "KLAUS_SNAPSHOT_TEST_PREVIEW_PORT_FILE={} {} --exact engine::session::turn_tests::snapshot_preview_http_process_fixture --ignored --nocapture --test-threads=1",
        quoted(&port_file), quoted(&std::env::current_exe().unwrap()),
    );
    let session = mk_session(vec![text_turn("Preview service survives a final reply")]);
    let id = session
        .background_processes
        .start(
            session.process_context.clone(),
            session_background_options(&command),
        )
        .await
        .unwrap();
    let _cleanup = SnapshotPreviewCleanup {
        session: session.clone(),
        shell_id: id.clone(),
    };
    let address = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if let Ok(address) = std::fs::read_to_string(&port_file) {
                break address;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap();
    let url = format!("http://{address}/");
    assert_eq!(
        client.get(&url).send().await.unwrap().text().await.unwrap(),
        "snapshot-preview-live"
    );
    let first = session
        .run(
            Input::Text("Keep the preview service running".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(first.error.is_none());
    assert_eq!(first.stop_reason, TerminalReason::Completed);
    let before = session.history();
    let mut config = default_config();
    config.model = "replacement".into();
    let revision = session
        .reconfigure_inference(
            Arc::new(SnapshotContinuationProvider {
                info: ProviderInfo {
                    name: "snapshot-fixture".into(),
                    default_model: "replacement".into(),
                    base_url: None,
                    env_key: None,
                },
            }),
            config,
            None,
            None,
            0,
        )
        .unwrap();
    assert_eq!(revision, 1);
    let record = session
        .background_process_snapshot()
        .processes
        .into_iter()
        .find(|record| record.shell_id == id)
        .expect("publication must retain the session-owned background service");
    assert_eq!(record.status, BackgroundProcessStatus::Running);
    assert_eq!(session.model_runtime().1.model, "replacement");
    assert_eq!(session.history(), before);
    let second = session
        .run(
            Input::Text("Continue chatting immediately".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(second.error.is_none());
    assert_eq!(second.stop_reason, TerminalReason::Completed);
    assert!(second.final_text.contains("Chat continues"));
    assert_eq!(
        client.get(&url).send().await.unwrap().text().await.unwrap(),
        "snapshot-preview-live"
    );
    assert!(session
        .background_process_snapshot()
        .processes
        .iter()
        .any(|record| record.shell_id == id && record.status == BackgroundProcessStatus::Running));
    session.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn live_model_reconfigure_keeps_an_unpolled_background_agent_on_its_captured_provider() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let session = mk_session_with_provider_and_memory(
        Arc::new(fake_provider()),
        recording_deps(requests.clone(), vec![text_turn("child complete")]),
        default_config(),
        Vec::new(),
        None,
    );
    let original_model = session.model_runtime().1.model;
    let spawn = session.make_spawn_background_agent(
        0,
        None,
        session.permission_context(),
        Arc::new(Vec::new),
        false,
        None,
        None,
    );
    let spawned = spawn(SpawnSubAgentOptions {
        prompt: "Complete the bounded fixture task".into(),
        ..Default::default()
    })
    .await
    .expect("child creation must be accepted");
    let mut config = default_config();
    config.model = "replacement".into();
    let id = spawned.agent_id.try_to_utf8().unwrap();
    let agent_abort = session
        .subagents
        .background_agents
        .lock()
        .unwrap()
        .get(&id)
        .unwrap()
        .abort
        .clone();
    assert!(requests.lock().unwrap().is_empty(), "no first poll yet");
    assert_eq!(
        session
            .reconfigure_inference(Arc::new(fake_provider()), config, None, None, 0)
            .unwrap(),
        1
    );
    assert!(!agent_abort.is_cancelled(), "publication is not Stop");
    let pending = std::mem::take(&mut session.background_tasks.lock().unwrap().handles);
    for handle in pending {
        handle.await.unwrap();
    }
    assert!(!agent_abort.is_cancelled());
    let sent = requests.lock().unwrap().clone();
    assert!(!sent.is_empty(), "the admitted child must finish");
    assert!(sent.iter().all(|request| request.model == original_model));
    std::fs::remove_file(runtime_path_to_file_io_host(&spawned.output_file)).unwrap();
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn live_model_reconfigure_does_not_cancel_a_running_background_agent_or_rebind_its_request() {
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let mut deps = recording_deps(
        requests.clone(),
        vec![text_turn("running child completed on A")],
    );
    let call = deps.call_model;
    deps.call_model = Box::new({
        let entered = entered.clone();
        let release = release.clone();
        move |request, signal| {
            let stream = call(request, signal);
            let entered = entered.clone();
            let release = release.clone();
            Box::pin(
                futures::stream::once(async move {
                    entered.notify_one();
                    release.notified().await;
                    stream
                })
                .flatten(),
            )
        }
    });
    let session = mk_session_with_provider_and_memory(
        Arc::new(fake_provider()),
        deps,
        default_config(),
        Vec::new(),
        None,
    );
    let original_model = session.model_runtime().1.model;
    let spawn = session.make_spawn_background_agent(
        0,
        None,
        session.permission_context(),
        Arc::new(Vec::new),
        false,
        None,
        None,
    );
    let spawned = spawn(SpawnSubAgentOptions {
        prompt: "Complete the already running captured request".into(),
        ..Default::default()
    })
    .await
    .expect("child creation must be accepted");
    tokio::time::timeout(std::time::Duration::from_secs(10), entered.notified())
        .await
        .unwrap();
    let captured = requests.lock().unwrap().clone();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].model, original_model);
    let id = spawned.agent_id.try_to_utf8().unwrap();
    let abort = session
        .subagents
        .background_agents
        .lock()
        .unwrap()
        .get(&id)
        .unwrap()
        .abort
        .clone();
    let mut config = default_config();
    config.model = "replacement".into();
    assert_eq!(
        session
            .reconfigure_inference(Arc::new(fake_provider()), config, None, None, 0)
            .unwrap(),
        1
    );
    assert!(!abort.is_cancelled());
    assert_eq!(*requests.lock().unwrap(), captured);
    release.notify_one();
    let pending = std::mem::take(&mut session.background_tasks.lock().unwrap().handles);
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        for handle in pending {
            handle.await.unwrap();
        }
    })
    .await
    .unwrap();
    assert!(!abort.is_cancelled());
    assert_eq!(*requests.lock().unwrap(), captured);
    std::fs::remove_file(runtime_path_to_file_io_host(&spawned.output_file)).unwrap();
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn publication_and_root_interrupt_preserve_children_until_explicit_target_stop() {
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let mut deps = recording_deps(requests, vec![text_turn("child complete")]);
    let call = deps.call_model;
    deps.call_model = Box::new({
        let entered = entered.clone();
        move |request, signal| {
            let cancelled = signal.clone();
            let stream = call(request, signal);
            let entered = entered.clone();
            let release = release.clone();
            Box::pin(
                futures::stream::once(async move {
                    entered.notify_one();
                    tokio::select! {
                        _ = cancelled.cancelled() => None,
                        _ = release.notified() => Some(stream),
                    }
                })
                .filter_map(futures::future::ready)
                .flatten(),
            )
        }
    });
    let session = mk_session_with_provider_and_memory(
        Arc::new(fake_provider()),
        deps,
        default_config(),
        Vec::new(),
        None,
    );
    let spawn = session.make_spawn_background_agent(
        0,
        None,
        session.permission_context(),
        Arc::new(Vec::new),
        false,
        None,
        None,
    );
    let spawned = spawn(SpawnSubAgentOptions {
        prompt: "Complete the bounded fixture task".into(),
        ..Default::default()
    })
    .await
    .expect("child creation must be accepted");
    let id = spawned.agent_id.try_to_utf8().unwrap();
    let agent_abort = session
        .subagents
        .background_agents
        .lock()
        .unwrap()
        .get(&id)
        .unwrap()
        .abort
        .clone();
    let mut config = default_config();
    config.model = "replacement".into();
    assert_eq!(
        session
            .reconfigure_inference(Arc::new(fake_provider()), config, None, None, 0)
            .unwrap(),
        1
    );
    assert!(!agent_abort.is_cancelled());
    tokio::time::timeout(std::time::Duration::from_secs(10), entered.notified())
        .await
        .unwrap();
    session.interrupt();
    assert!(!agent_abort.is_cancelled());
    assert!(session.background_task_registry().stop(&spawned.agent_id));
    assert!(agent_abort.is_cancelled());
    let pending = std::mem::take(&mut session.background_tasks.lock().unwrap().handles);
    for handle in pending {
        handle.await.unwrap();
    }
    assert_eq!(session.model_revision(), 1);
    std::fs::remove_file(runtime_path_to_file_io_host(&spawned.output_file)).ok();
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn live_model_reconfigure_releases_old_provider_resources_only_after_a_successful_commit() {
    let original: Arc<dyn ModelProvider> = Arc::new(fake_provider());
    let retained = Arc::downgrade(&original);
    let mut initial = default_config();
    initial.prewarm = false;
    let deps = crate::engine::deps::production_deps(original.clone(), Default::default());
    let session = mk_session_with_provider_and_memory(original, deps, initial, Vec::new(), None);
    let candidate: Arc<dyn ModelProvider> = Arc::new(fake_provider());
    let rejected = Arc::downgrade(&candidate);
    let mut selected = default_config();
    selected.model = "replacement".into();
    assert!(session
        .reconfigure_inference(candidate, selected.clone(), None, None, 99)
        .unwrap_err()
        .contains("revision conflict"));
    assert!(retained.upgrade().is_some());
    assert!(
        rejected.upgrade().is_none(),
        "failed preparation must not retain the candidate provider"
    );
    assert_eq!(
        session
            .reconfigure_inference(Arc::new(fake_provider()), selected, None, None, 0)
            .unwrap(),
        1
    );
    assert!(
        retained.upgrade().is_none(),
        "successful replacement must release the old provider and model call dependencies"
    );
    session.shutdown_and_wait().await.unwrap();
}

#[allow(clippy::too_many_arguments)]
fn mk_session_with_provider_memory_store_and_agents(
    provider: Arc<dyn ModelProvider>,
    deps: EngineDeps,
    config: ResolvedConfig,
    tools: Vec<AnyTool>,
    agent_memory: Option<Arc<dyn AgentMemoryHost>>,
    rollout: Option<RolloutSink>,
    store: Option<Arc<dyn SessionStore>>,
    initial_history: Option<Vec<Message>>,
    agents: Option<Vec<AgentDefinition>>,
) -> Arc<Session> {
    mk_session_with_provider_memory_store_agents_and_snapshot_decisions(
        provider,
        deps,
        config,
        tools,
        agent_memory,
        rollout,
        store,
        initial_history,
        agents,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn mk_session_with_provider_memory_store_agents_and_snapshot_decisions(
    provider: Arc<dyn ModelProvider>,
    deps: EngineDeps,
    config: ResolvedConfig,
    tools: Vec<AnyTool>,
    agent_memory: Option<Arc<dyn AgentMemoryHost>>,
    rollout: Option<RolloutSink>,
    store: Option<Arc<dyn SessionStore>>,
    initial_history: Option<Vec<Message>>,
    agents: Option<Vec<AgentDefinition>>,
    initial_agent_memory_snapshot_decisions: Option<Vec<AgentMemorySnapshotDecisionAudit>>,
) -> Arc<Session> {
    Session::new(
        session_init_with_provider_memory_store_agents_and_snapshot_decisions(
            provider,
            deps,
            config,
            tools,
            agent_memory,
            rollout,
            store,
            initial_history,
            agents,
            initial_agent_memory_snapshot_decisions,
        ),
    )
}

#[allow(clippy::too_many_arguments)]
fn session_init_with_provider_memory_store_agents_and_snapshot_decisions(
    provider: Arc<dyn ModelProvider>,
    deps: EngineDeps,
    config: ResolvedConfig,
    tools: Vec<AnyTool>,
    agent_memory: Option<Arc<dyn AgentMemoryHost>>,
    rollout: Option<RolloutSink>,
    store: Option<Arc<dyn SessionStore>>,
    initial_history: Option<Vec<Message>>,
    agents: Option<Vec<AgentDefinition>>,
    initial_agent_memory_snapshot_decisions: Option<Vec<AgentMemorySnapshotDecisionAudit>>,
) -> SessionInit {
    SessionInit {
        durable_permissions: None,
        process_context: crate::runtime_process::test_runtime_process_context(),
        user_config_dir: Some(runtime_path_from_host_path(&temp_store_dir())),
        id: Some("s1".into()),
        session_role: None,
        parent_thread_id: None,
        provider,
        deps,
        config,
        network_allowed: None,
        trusted_windows_sandbox_context: None,
        context_window_override: None,
        pricing: None,
        tools,
        refresh_tools: None,
        cwd: runtime_path_from_host_path(&std::env::temp_dir()),
        approval_handler: None,
        approval_reviewer: None,
        elicit_handler: None,
        mcp_elicitation_presenter: None,
        mcp_authorize_handler: None,
        mcp_oauth_redirect_uri: None,
        credential_store: None,
        hook_bus: None,
        rollout,
        store,
        mcp_clients: Vec::new(),
        plugin_mcp_servers: Vec::new(),
        slash_commands: Vec::new(),
        initial_history,
        initial_approval_context: None,
        initial_approval_reviews: None,
        initial_tool_result_budget_records: None,
        hidden_context_message_ids: None,
        initial_usage_ledger: None,
        initial_subagent_lifecycle: None,
        initial_background_processes: None,
        initial_memory_maintenance: None,
        initial_setup_lifecycle: None,
        initial_agent_memory_snapshot_decisions,
        forked_from_id: None,
        initial_checkpoints: None,
        system_prompt_provider: None,
        base_instructions: None,
        developer_instructions: None,
        root_agent_definition: None,
        observer: Some(None),
        logger: None,
        agents,
        skills: None,
        output_styles: None,
        skill_gate: None,
        current_worktree: None,
        prior_workflow_journal: None,
        initial_surfaced_tools: None,
        swarm: None,
        lsp_servers: None,
        shell_env: None,
        agent_memory,
        agent_memory_snapshot_policy: None,
        browser_runtime: None,
        runtime_capabilities: RuntimeCapabilityRegistry::new(),
        selected_capability_roots: None,
        extension_runtime: ExtensionRuntimeSnapshot::default(),
    }
}

fn mk_setup_session(
    deps: EngineDeps,
    hook_bus: Arc<HookBus>,
    rollout: Option<RolloutSink>,
    initial_setup_lifecycle: Option<SetupLifecycleSnapshot>,
) -> Arc<Session> {
    mk_setup_session_with_maintenance(deps, hook_bus, rollout, initial_setup_lifecycle, None)
}

fn mk_setup_session_with_maintenance(
    deps: EngineDeps,
    hook_bus: Arc<HookBus>,
    rollout: Option<RolloutSink>,
    initial_setup_lifecycle: Option<SetupLifecycleSnapshot>,
    initial_memory_maintenance: Option<MemoryMaintenanceSnapshot>,
) -> Arc<Session> {
    let mut config = default_config();
    config.agent_memory = false;
    Session::new(SessionInit {
        durable_permissions: None,
        process_context: crate::runtime_process::test_runtime_process_context(),
        user_config_dir: None,
        id: Some("setup-session".into()),
        session_role: None,
        parent_thread_id: None,
        provider: Arc::new(fake_provider()),
        deps,
        config,
        network_allowed: None,
        trusted_windows_sandbox_context: None,
        context_window_override: None,
        pricing: None,
        tools: Vec::new(),
        refresh_tools: None,
        cwd: runtime_path_from_host_path(&std::env::temp_dir()),
        approval_handler: None,
        approval_reviewer: None,
        elicit_handler: None,
        mcp_elicitation_presenter: None,
        mcp_authorize_handler: None,
        mcp_oauth_redirect_uri: None,
        credential_store: None,
        hook_bus: Some(hook_bus),
        rollout,
        store: None,
        mcp_clients: Vec::new(),
        plugin_mcp_servers: Vec::new(),
        slash_commands: Vec::new(),
        initial_history: None,
        initial_approval_context: None,
        initial_approval_reviews: None,
        initial_tool_result_budget_records: None,
        hidden_context_message_ids: None,
        initial_usage_ledger: None,
        initial_subagent_lifecycle: None,
        initial_background_processes: None,
        initial_memory_maintenance,
        initial_setup_lifecycle,
        initial_agent_memory_snapshot_decisions: None,
        forked_from_id: None,
        initial_checkpoints: None,
        system_prompt_provider: None,
        base_instructions: None,
        developer_instructions: None,
        root_agent_definition: None,
        observer: Some(None),
        logger: None,
        agents: None,
        skills: None,
        output_styles: None,
        skill_gate: None,
        current_worktree: None,
        prior_workflow_journal: None,
        initial_surfaced_tools: None,
        swarm: None,
        lsp_servers: None,
        shell_env: None,
        agent_memory: None,
        agent_memory_snapshot_policy: None,
        browser_runtime: None,
        runtime_capabilities: RuntimeCapabilityRegistry::new(),
        selected_capability_roots: None,
        extension_runtime: ExtensionRuntimeSnapshot::default(),
    })
}

fn mk_session(turns: ScriptedTurns) -> Arc<Session> {
    mk_session_with(turns, default_config(), Vec::new())
}

#[tokio::test]
async fn closed_sessions_release_their_builtin_reminder_owners() {
    let mut retained_cases = Vec::new();
    for (edited_files, swarm, memory) in [
        (false, false, false),
        (true, false, false),
        (false, true, false),
        (false, false, true),
        (true, true, true),
    ] {
        let mut config = default_config();
        config.prewarm = false;
        config.edited_files = edited_files;
        config.swarm = swarm;
        config.memory.enabled = memory;
        config.memory_recall = false;
        config.auto_memory = false;
        config.auto_dream = false;
        let session = mk_session_with(Vec::new(), config, Vec::new());
        let weak = Arc::downgrade(&session);
        // Query snapshots may outlive the Session without owning its history.
        let providers = session.reminder_providers.get().unwrap().clone();
        let edited_provider = session.edited_files_reminder.get().cloned().flatten();
        session.shutdown_and_wait().await.unwrap();
        drop(session);
        let retained = weak.strong_count();
        let released = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while weak.strong_count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok();
        eprintln!("SESSION_RELEASE_MEASUREMENT edited={edited_files} swarm={swarm} memory={memory} retained_after_shutdown={retained} released={released}");
        if !released {
            retained_cases.push((edited_files, swarm, memory, weak.strong_count()));
        }
        drop(providers);
        drop(edited_provider);
    }
    assert!(
        retained_cases.is_empty(),
        "built-in reminder callbacks retained closed Sessions: {retained_cases:?}"
    );
}

#[tokio::test]
async fn saved_raw_history_restores_exactly_and_review_snapshots_refuse_unavailable_evidence() {
    use crate::types::permission::ApprovalReviewContext;
    let original = Message::User(crate::schemas::message::UserMessage {
        uuid: "saved-raw-user".into(),
        content: vec![ContentBlock::Text(crate::schemas::content::TextBlock {
            text: crate::runtime_values::RuntimeString::from_utf16(vec![0x61, 0xd800, 0x62]),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })],
        is_meta: None,
        question_answer: None,
    });
    let session = mk_session_with_provider_memory_and_store(
        Arc::new(fake_provider()),
        mk_deps(Vec::new()),
        default_config(),
        Vec::new(),
        None,
        None,
        None,
        Some(vec![original.clone()]),
    );
    assert_eq!(session.history(), vec![original.clone()]);
    let capture = |agent_id, history| ApprovalReviewContext {
        accounting: None,
        turn_id: "review-turn".into(),
        agent_id,
        request: crate::runtime_values::runtime_codec::decode_value(&exact_json!({
            "model": "test-model", "system": [], "messages": [], "tools": [], "maxOutputTokens": 32
        }))
        .unwrap(),
        canonical_system: Vec::new(),
        history,
        config: default_config(),
        provider: None,
        emit: Arc::new(|_| {}),
    };
    for agent_id in [None, Some("child".into())] {
        let result = session.review_evidence_snapshot(&capture(agent_id, Vec::new()));
        assert!(
            result.is_err(),
            "root unavailable evidence cannot become an empty review"
        );
    }
    let clean = mk_session(Vec::new());
    let child = capture(Some("child".into()), vec![original]);
    assert!(
        clean.review_evidence_snapshot(&child).is_err(),
        "child unavailable evidence cannot become an empty review"
    );
    session.shutdown_and_wait().await.unwrap();
    clean.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn agent_notification_receipts_deduplicate_before_publish_and_after_restore() {
    use crate::schemas::agent_notification::AgentNotification;
    let session = mk_session(Vec::new());
    let received = Arc::new(Mutex::new(Vec::new()));
    let capture = received.clone();
    session.set_agent_notification_sink(Some(Arc::new(move |receipt| {
        capture.lock().unwrap().push(receipt.clone());
    })));
    session
        .notify_background_complete("not-a-registered-agent")
        .await;
    assert!(
        received.lock().unwrap().is_empty(),
        "background processes are not agent notifications"
    );
    let notice = AgentNotification {
        notification_id: "mailbox-stable".into(),
        source: "team-mailbox".into(),
        status: "updated".into(),
        agent_name: Some("child".into()),
        agent_id: None,
        task_id: None,
        timestamp: 1000,
        turn_id: None,
    };
    let lease = ActiveTurnLease::try_new(
        session.clone(),
        session.config.model.clone(),
        CancellationToken::new(),
    )
    .unwrap();
    session
        .turn
        .active_stack
        .lock()
        .unwrap()
        .last_mut()
        .unwrap()
        .turn_id = Some("receiving-turn".into());
    session
        .publish_agent_notification(notice.clone())
        .await
        .unwrap();
    session
        .publish_agent_notification(notice.clone())
        .await
        .unwrap();
    assert_eq!(received.lock().unwrap().len(), 1);
    assert_eq!(
        received.lock().unwrap()[0].turn_id,
        Some("receiving-turn".into())
    );
    assert!(
        session.conversation.history.lock().unwrap().is_empty(),
        "receipt is not model history"
    );
    let persisted = session.turn_index.lock().unwrap().clone();
    drop(lease);
    session.shutdown_and_wait().await.unwrap();
    let restored = mk_session(Vec::new());
    *restored.turn_index.lock().unwrap() = persisted;
    let capture = received.clone();
    restored.set_agent_notification_sink(Some(Arc::new(move |receipt| {
        capture.lock().unwrap().push(receipt.clone());
    })));
    restored.publish_agent_notification(notice).await.unwrap();
    assert_eq!(
        received.lock().unwrap().len(),
        1,
        "mailbox redelivery cannot republish a restored receipt"
    );
    restored.shutdown_and_wait().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn agent_notification_team_send_appears_on_receiving_session_run_only() {
    let root = temp_store_dir();
    let _config = ConfigEnvGuard::set(root.to_str().unwrap());
    let mut config = default_config();
    config.swarm = true;
    let session = mk_session_with(
        vec![text_turn("first"), text_turn("second")],
        config,
        Vec::new(),
    );
    let controller = session.swarm_controller(session.main_swarm_cell()).unwrap();
    let created = controller.create("receipt-team".into(), None, None).await;
    let child_identity = crate::types::tool::SwarmIdentity {
        team: created.identity.team.clone(),
        peer: "reviewer".into(),
    };
    let child_controller = session
        .swarm_controller((
            Arc::new(move || Some(child_identity.clone())),
            Arc::new(|_| {}),
        ))
        .unwrap();
    let received = Arc::new(Mutex::new(Vec::new()));
    let capture = received.clone();
    session.set_agent_notification_sink(Some(Arc::new(move |receipt| {
        capture.lock().unwrap().push(receipt.clone());
    })));
    controller
        .send(&"reviewer".into(), &"child only".into())
        .await;
    child_controller
        .send(&created.identity.peer, &"intermediate report".into())
        .await;
    assert!(
        received.lock().unwrap().is_empty(),
        "sender admission alone is not receiver delivery"
    );
    session
        .run(Input::Text("continue".into()), TurnOptions::default())
        .await;
    let receipt = received.lock().unwrap()[0].clone();
    assert_eq!(received.lock().unwrap().len(), 1);
    assert_eq!(receipt.agent_name, Some("reviewer".into()));
    assert_eq!(receipt.source, "team-mailbox");
    assert_eq!(receipt.status, "updated");
    let index = session.turn_index_snapshot();
    assert!(index
        .turn_status
        .contains_key(receipt.turn_id.as_ref().unwrap()));
    session
        .run(Input::Text("continue again".into()), TurnOptions::default())
        .await;
    assert_eq!(
        received.lock().unwrap().len(),
        1,
        "next run does not repeat delivered inbox rows"
    );
    controller.remove(&created.identity.team, true).await;
    session.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn agent_notification_idle_teammate_delivery_persists_only_child_and_deduplicates_restart() {
    let root = temp_store_dir();
    let _config = ConfigEnvGuard::set(root.to_str().unwrap());
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.to_str().unwrap().to_owned()),
        archive_dir: None,
    }));
    let rollout: RolloutSink = Arc::from(
        store
            .create(SessionMeta {
                id: "s1".into(),
                created_at: 1,
                cwd: runtime_path_from_host_path(&root).into_runtime_string(),
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
            })
            .await
            .unwrap(),
    );
    let mut config = default_config();
    config.swarm = true;
    config.agent_memory = false;
    let session = mk_session_with_provider_memory_and_store(
        Arc::new(fake_provider()),
        unique_subagent_test_deps(mk_deps(vec![text_turn("initial")])),
        config,
        Vec::new(),
        None,
        Some(rollout),
        Some(store.clone()),
        None,
    );
    let controller = session.swarm_controller(session.main_swarm_cell()).unwrap();
    let created = controller
        .create("child-receipt-team".into(), None, None)
        .await;
    let options = SpawnSubAgentOptions {
        prompt: "child directive".into(),
        swarm: Some(crate::types::tool::SwarmSpawn {
            peer: "reviewer".into(),
        }),
        ..Default::default()
    };
    let mut child = session.run_sub_agent(
        options,
        1,
        CancellationToken::new(),
        session.permission_context(),
        "child".into(),
        RunSubAgentOpts {
            suppress_swarm_reminder: Some(true),
            ..Default::default()
        },
    );
    while child.next().await.is_some() {}
    let parent_receipts = Arc::new(Mutex::new(Vec::new()));
    let capture = parent_receipts.clone();
    session.set_agent_notification_sink(Some(Arc::new(move |receipt| {
        capture.lock().unwrap().push(receipt.clone());
    })));
    let child_receipts = Arc::new(Mutex::new(Vec::new()));
    let capture = child_receipts.clone();
    session.set_subagent_event_sink(Some(Arc::new(move |id, event| {
        if let super::SubagentStreamEvent::Notification(receipt) = event {
            capture
                .lock()
                .unwrap()
                .push((id.to_owned(), receipt.clone()));
        }
    })));
    controller
        .send(
            &"reviewer".into(),
            &"wake with an intermediate report".into(),
        )
        .await;
    let signal = CancellationToken::new();
    let received = session
        .wait_for_teammate_message(
            &created.identity.team,
            &"reviewer".into(),
            &signal,
            "child",
            0,
            true,
        )
        .await
        .unwrap();
    assert!(received.0.contains("wake with an intermediate report"));
    assert!(parent_receipts.lock().unwrap().is_empty());
    assert_eq!(child_receipts.lock().unwrap().len(), 1);
    let (recipient, receipt) = child_receipts.lock().unwrap()[0].clone();
    assert_eq!(recipient, "child");
    assert!(
        receipt.turn_id.is_none(),
        "idle receipt cannot borrow the preceding child turn"
    );
    let loaded = store.load_sub_agent("s1", "child").await.unwrap();
    assert_eq!(
        loaded.turn_index.unwrap().agent_notifications,
        vec![receipt]
    );
    assert!(store
        .resume("s1")
        .await
        .unwrap()
        .turn_index
        .agent_notifications
        .is_empty());
    // A restarted in-memory receipt ledger must seed from this child's rollout.
    session
        .subagents
        .notification_ids_by_child
        .lock()
        .unwrap()
        .clear();
    session
        .wait_for_teammate_message(
            &created.identity.team,
            &"reviewer".into(),
            &signal,
            "child",
            0,
            true,
        )
        .await
        .unwrap();
    assert_eq!(child_receipts.lock().unwrap().len(), 1);
    assert!(parent_receipts.lock().unwrap().is_empty());
    controller.remove(&created.identity.team, true).await;
    session.shutdown_and_wait().await.unwrap();
    assert_eq!(
        store
            .load_sub_agent("s1", "child")
            .await
            .unwrap()
            .turn_index
            .unwrap()
            .agent_notifications
            .len(),
        1
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn deferred_notices_use_the_persistent_base_session_model() {
    let mut config = default_config();
    config.model = "gpt-5".into();
    let session = mk_session_with(Vec::new(), config, Vec::new());
    assert_eq!(
        session.render_deferred_notice(&"background finished".into()),
        "# System Reminder\n\nbackground finished"
    );

    session.set_model("claude-sonnet-4-6").unwrap();
    assert_eq!(
        session.render_deferred_notice(&"background finished".into()),
        "<system-reminder>\nbackground finished\n</system-reminder>"
    );
}

#[tokio::test]
async fn context_snapshot_uses_current_provider_context_not_cumulative_usage() {
    let usage = |input_tokens| Usage {
        input_tokens,
        output_tokens: 0,
        cache_read_input_tokens: 0,
        cache_creation_input_tokens: 0,
        total_tokens: None,
    };
    let turns = vec![
        vec![
            ev::message_start(None),
            ev::text("first"),
            ev::stop(Some(ModelStopReason::EndTurn), Some(usage(900))),
        ],
        vec![
            ev::message_start(None),
            ev::text("second"),
            ev::stop(Some(ModelStopReason::EndTurn), Some(usage(100))),
        ],
    ];
    let mut config = default_config();
    config.context_window = 1_000;
    config.output_reserve_tokens = 0;
    // Keep the reservation proportionate to this toy window: the production 13k
    // absolute reservation would swamp a 1k window and clamp the reported
    // threshold to zero, which would make the window-state assertion vacuous.
    config.compact_buffer_tokens = 150;
    config.compact_threshold = Some(0.85);
    let session = mk_session_with(turns, config, Vec::new());

    session
        .run(Input::Text("one".into()), TurnOptions::default())
        .await;
    session
        .run(Input::Text("two".into()), TurnOptions::default())
        .await;

    let snapshot = session.context_snapshot();
    assert_eq!(snapshot.total_tokens, 100);
    assert_eq!(snapshot.token_totals.input_tokens, 1_000);
    assert_eq!(snapshot.window_state, ContextWindowState::Normal);
}

struct RetainingUsageCompactor;

struct CapturingInstructionsCompactor(Arc<Mutex<Option<RuntimeString>>>);

#[async_trait::async_trait]
impl crate::engine::deps::Compactor for CapturingInstructionsCompactor {
    async fn maybe_compact(
        &self,
        messages: &[Message],
        _estimated_tokens: u64,
        config: &ResolvedConfig,
        custom_instructions: Option<&RuntimeString>,
    ) -> crate::engine::deps::CompactionResult {
        self.reactive_compact(messages, config, custom_instructions)
            .await
    }

    async fn reactive_compact(
        &self,
        messages: &[Message],
        _config: &ResolvedConfig,
        custom_instructions: Option<&RuntimeString>,
    ) -> crate::engine::deps::CompactionResult {
        *self.0.lock().unwrap() = custom_instructions.cloned();
        crate::engine::deps::CompactionResult {
            messages: messages.to_vec(),
            freed_tokens: 0,
            compacted: false,
            reason: Some(crate::context::compact::CompactionNoopReason::NoProgress),
            diagnostics: Vec::new(),
        }
    }
}

#[async_trait::async_trait]
impl crate::engine::deps::Compactor for RetainingUsageCompactor {
    async fn maybe_compact(
        &self,
        messages: &[Message],
        _estimated_tokens: u64,
        _config: &ResolvedConfig,
        _custom_instructions: Option<&RuntimeString>,
    ) -> crate::engine::deps::CompactionResult {
        self.reactive_compact(messages, _config, _custom_instructions)
            .await
    }

    async fn reactive_compact(
        &self,
        messages: &[Message],
        _config: &ResolvedConfig,
        _custom_instructions: Option<&RuntimeString>,
    ) -> crate::engine::deps::CompactionResult {
        let mut compacted = vec![Message::CompactBoundary(
            crate::schemas::message::CompactBoundaryMessage {
                native_compaction: None,
                uuid: "compact-boundary".into(),
                summary: "short summary".into(),
            },
        )];
        if let Some(last) = messages.last() {
            compacted.push(last.clone());
        }
        crate::engine::deps::CompactionResult {
            messages: compacted,
            freed_tokens: 800,
            compacted: true,
            reason: None,
            diagnostics: Vec::new(),
        }
    }
}

#[tokio::test]
async fn manual_compaction_forwards_trimmed_exact_utf16_instructions() {
    let seen = Arc::new(Mutex::new(None));
    let mut deps = mk_deps(Vec::new());
    deps.compactor = Arc::new(CapturingInstructionsCompactor(seen.clone()));
    let session = mk_session_with_deps(deps, default_config(), Vec::new());
    let instructions = RuntimeString::from_utf16(vec![0x0020, 0xd800, 0x0061, 0xdc00, 0x0020]);

    let outcome = session.compact(Some(instructions)).await.unwrap();

    assert!(!outcome.compacted);
    assert_eq!(
        outcome.reason,
        Some(crate::context::compact::CompactionNoopReason::NoProgress)
    );
    assert!(outcome.diagnostics.is_empty());
    let seen = seen.lock().unwrap();
    let seen = seen.as_ref().expect("captured compact instructions");
    assert_eq!(seen.as_utf16(), &[0xd800, 0x0061, 0xdc00]);
    assert!(!seen.as_utf16().contains(&0xfffd));
}

#[tokio::test]
async fn manual_compaction_invalidates_retained_precompact_usage_anchor() {
    let high_usage = Usage {
        input_tokens: 90_000,
        output_tokens: 0,
        cache_read_input_tokens: 0,
        cache_creation_input_tokens: 0,
        total_tokens: None,
    };
    let turns = vec![vec![
        ev::message_start(None),
        ev::text("answer"),
        ev::stop(Some(ModelStopReason::EndTurn), Some(high_usage)),
    ]];
    let mut deps = mk_deps(turns);
    deps.compactor = Arc::new(RetainingUsageCompactor);
    let session = mk_session_with_deps(deps, default_config(), Vec::new());
    session
        .run(Input::Text("one".into()), TurnOptions::default())
        .await;
    assert_eq!(session.context_snapshot().total_tokens, 90_000);

    let compacted = session.compact(None).await.unwrap();
    assert!(compacted.compacted);
    assert_eq!(compacted.before_tokens, 90_000);
    assert!(compacted.after_tokens < 100);
    assert_eq!(compacted.before_messages, 2);
    assert_eq!(compacted.after_messages, session.history().len());
    assert!(session.history().iter().all(
        |message| !matches!(message, Message::Assistant(assistant) if assistant.usage.is_some())
    ));
    assert!(session
        .history()
        .iter()
        .any(|message| matches!(message, Message::User(user) if user.is_meta != Some(true))));
    let boundary = compacted.boundary.as_ref().expect("compact boundary");
    assert_eq!(boundary.uuid, "compact-boundary");
    assert_eq!(boundary.summary, "short summary");
    let snapshot = session.context_snapshot();
    assert!(
        snapshot.total_tokens < 10_000,
        "retained pre-compaction usage still anchored the current window: {}",
        snapshot.total_tokens
    );
    assert_eq!(snapshot.token_totals.input_tokens, 90_000);
}

fn recording_deps(requests: Arc<Mutex<Vec<ModelRequest>>>, turns: ScriptedTurns) -> EngineDeps {
    let counter = Arc::new(AtomicUsize::new(0));
    let turns = Arc::new(turns);
    let turn_counter = Arc::new(AtomicUsize::new(0));
    EngineDeps {
        provider_id: "test-provider".into(),
        call_model: Box::new(
            move |req: ModelRequest, signal: AbortSignal| -> ModelStream {
                requests.lock().unwrap().push(req);
                let turns = Arc::clone(&turns);
                let turn_counter = Arc::clone(&turn_counter);
                let stream = futures::stream::once(async move {
                    let i = turn_counter.fetch_add(1, Ordering::SeqCst);
                    let turn = turns
                        .get(i)
                        .cloned()
                        .unwrap_or_else(|| vec![ev::message_start(None), ev::stop(None, None)]);
                    futures::stream::iter(turn)
                        .take_while(move |_| futures::future::ready(!signal.is_cancelled()))
                })
                .flatten();
                Box::pin(stream)
            },
        ),
        estimate_tokens: Arc::new(|_| 0),
        compactor: noop_compactor(),
        uuid: {
            let c = counter.clone();
            Arc::new(move || format!("uuid-{}", c.fetch_add(1, Ordering::SeqCst)))
        },
        now: Arc::new(|| 0),
    }
}

struct SequencedSetupHook {
    calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl HookFn for SequencedSetupHook {
    async fn run(&self, input: HookInput) -> HookOutcome {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        match input.trigger {
            Some(HookTrigger::Init) if call == 0 => HookOutcome {
                result: Some(HookResult::FailedContinue),
                system_message: Some("init failed visibly".into()),
                ..HookOutcome::default()
            },
            Some(HookTrigger::Init) => HookOutcome {
                result: Some(HookResult::Success),
                additional_context: Some("INIT CONTEXT".into()),
                ..HookOutcome::default()
            },
            Some(HookTrigger::Maintenance) => HookOutcome {
                result: Some(HookResult::Success),
                additional_context: Some(format!("MAINTENANCE {call}").into()),
                ..HookOutcome::default()
            },
            _ => HookOutcome::default(),
        }
    }
}

struct StaticSetupHook {
    context: RuntimeString,
}

#[async_trait::async_trait]
impl HookFn for StaticSetupHook {
    async fn run(&self, _input: HookInput) -> HookOutcome {
        HookOutcome {
            result: Some(HookResult::Success),
            additional_context: Some(self.context.clone()),
            ..HookOutcome::default()
        }
    }
}

struct BlockFirstPromptHook {
    calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl HookFn for BlockFirstPromptHook {
    async fn run(&self, _input: HookInput) -> HookOutcome {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            HookOutcome {
                decision: Some(crate::hooks::types::HookDecision::Block),
                permission_reason: Some("block the first prompt".into()),
                ..HookOutcome::default()
            }
        } else {
            HookOutcome {
                result: Some(HookResult::Success),
                ..HookOutcome::default()
            }
        }
    }
}

struct GatedSetupHook {
    calls: Arc<AtomicUsize>,
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl HookFn for GatedSetupHook {
    async fn run(&self, _input: HookInput) -> HookOutcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        self.release.notified().await;
        HookOutcome {
            result: Some(HookResult::Success),
            additional_context: Some("GATED".into()),
            ..HookOutcome::default()
        }
    }
}

struct CancellationObservedSetupHook {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    dropped: Arc<tokio::sync::Notify>,
    side_effects: Arc<AtomicUsize>,
}

struct HookFutureDropNotification(Arc<tokio::sync::Notify>);

impl Drop for HookFutureDropNotification {
    fn drop(&mut self) {
        self.0.notify_waiters();
    }
}

#[async_trait::async_trait]
impl HookFn for CancellationObservedSetupHook {
    async fn run(&self, _input: HookInput) -> HookOutcome {
        let _drop_notification = HookFutureDropNotification(self.dropped.clone());
        self.started.notify_waiters();
        self.release.notified().await;
        self.side_effects.fetch_add(1, Ordering::SeqCst);
        HookOutcome {
            result: Some(HookResult::Success),
            ..HookOutcome::default()
        }
    }
}

fn setup_bus(hook: Hook) -> Arc<HookBus> {
    Arc::new(HookBus::new(
        crate::runtime_process::test_runtime_process_context(),
        vec![hook],
    ))
}

fn replay_setup_lifecycle(items: &[RolloutItem]) -> SetupLifecycleSnapshot {
    let mut replayed = SetupLifecycleSnapshot::default();
    for item in items {
        let RolloutItem::Event(event) = item else {
            continue;
        };
        if event.event != SETUP_LIFECYCLE_ROLLOUT_EVENT {
            continue;
        }
        let event = runtime_codec::decode_value::<SetupLifecycleEvent>(
            event.payload.as_ref().expect("Setup event payload"),
        )
        .unwrap();
        replayed.apply(&event);
    }
    replayed
}

/// Deterministically blocks one selected durability barrier while retaining
/// the exact Setup event stream. Flush calls are one-based.
#[derive(Debug)]
struct ControlledSetupRollout {
    block_prepare: AtomicBool,
    fail_prepare: AtomicBool,
    prepare_entered: AtomicBool,
    prepare_changed: tokio::sync::Notify,
    prepare_release: tokio::sync::Semaphore,
    block_flush_call: usize,
    panic_flush_call: Option<usize>,
    fail_flush_call: Option<usize>,
    panic_append_call: Option<usize>,
    append_calls: AtomicUsize,
    flush_calls: AtomicUsize,
    flush_changed: tokio::sync::Notify,
    flush_release: tokio::sync::Notify,
    block_terminal_append: bool,
    terminal_append_entered: AtomicBool,
    terminal_append_changed: tokio::sync::Notify,
    terminal_append_released: Mutex<bool>,
    terminal_append_release: Condvar,
    setup_events: Mutex<Vec<SetupLifecycleEvent>>,
    shutdown_calls: AtomicUsize,
}

impl ControlledSetupRollout {
    fn append_event_sync(&self, event: RuntimeString, opts: Option<AppendEventOptions>) {
        if event != SETUP_LIFECYCLE_ROLLOUT_EVENT {
            return;
        }
        let append_call = self.append_calls.fetch_add(1, Ordering::SeqCst) + 1;
        if self.panic_append_call == Some(append_call) {
            panic!("injected Setup rollout append panic at call {append_call}");
        }
        let payload = opts
            .and_then(|opts| opts.payload)
            .expect("Setup lifecycle event payload");
        let event = runtime_codec::decode_value::<SetupLifecycleEvent>(&payload)
            .expect("Setup lifecycle event codec");
        let is_terminal = matches!(
            &event,
            SetupLifecycleEvent::Run { run } if run.status != SetupRunStatus::Started
        ) || matches!(
            &event,
            SetupLifecycleEvent::ContextAdmission { admission }
                if admission.status != SetupContextAdmissionStatus::Started
        );
        self.setup_events.lock().unwrap().push(event);
        if self.block_terminal_append && is_terminal {
            self.terminal_append_entered.store(true, Ordering::SeqCst);
            self.terminal_append_changed.notify_waiters();
            let mut released = self.terminal_append_released.lock().unwrap();
            while !*released {
                released = self.terminal_append_release.wait(released).unwrap();
            }
        }
    }

    fn new(block_flush_call: usize) -> Arc<Self> {
        Self::configured(block_flush_call, None, None, None, false)
    }

    fn with_terminal_append_gate() -> Arc<Self> {
        Self::configured(usize::MAX, None, None, None, true)
    }

    fn panicking_flush(panic_flush_call: usize) -> Arc<Self> {
        Self::configured(usize::MAX, Some(panic_flush_call), None, None, false)
    }

    fn failing_flush(fail_flush_call: usize) -> Arc<Self> {
        Self::configured(usize::MAX, None, Some(fail_flush_call), None, false)
    }

    fn panicking_append(panic_append_call: usize) -> Arc<Self> {
        Self::configured(usize::MAX, None, None, Some(panic_append_call), false)
    }

    fn configured(
        block_flush_call: usize,
        panic_flush_call: Option<usize>,
        fail_flush_call: Option<usize>,
        panic_append_call: Option<usize>,
        block_terminal_append: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            block_prepare: AtomicBool::new(false),
            fail_prepare: AtomicBool::new(false),
            prepare_entered: AtomicBool::new(false),
            prepare_changed: tokio::sync::Notify::new(),
            prepare_release: tokio::sync::Semaphore::new(0),
            block_flush_call,
            panic_flush_call,
            fail_flush_call,
            panic_append_call,
            append_calls: AtomicUsize::new(0),
            flush_calls: AtomicUsize::new(0),
            flush_changed: tokio::sync::Notify::new(),
            flush_release: tokio::sync::Notify::new(),
            block_terminal_append,
            terminal_append_entered: AtomicBool::new(false),
            terminal_append_changed: tokio::sync::Notify::new(),
            terminal_append_released: Mutex::new(false),
            terminal_append_release: Condvar::new(),
            setup_events: Mutex::new(Vec::new()),
            shutdown_calls: AtomicUsize::new(0),
        })
    }

    async fn wait_for_flush_calls(&self, expected: usize) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let changed = self.flush_changed.notified();
                if self.flush_calls.load(Ordering::SeqCst) >= expected {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("selected Setup rollout flush should be reached");
    }

    fn release_blocked_flush(&self) {
        self.flush_release.notify_one();
    }

    async fn wait_for_terminal_append(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let changed = self.terminal_append_changed.notified();
                if self.terminal_append_entered.load(Ordering::SeqCst) {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("Setup terminal append gate should be reached");
    }

    fn release_terminal_append(&self) {
        *self.terminal_append_released.lock().unwrap() = true;
        self.terminal_append_release.notify_all();
    }

    fn setup_events(&self) -> Vec<SetupLifecycleEvent> {
        self.setup_events.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl LiveRollout for ControlledSetupRollout {
    async fn prepare_batch(
        &self,
        maximum_records: usize,
    ) -> std::io::Result<crate::store::session_store::PreparedRolloutBatch<'_>> {
        if self.block_prepare.load(Ordering::SeqCst) {
            self.prepare_entered.store(true, Ordering::SeqCst);
            self.prepare_changed.notify_waiters();
            self.prepare_release
                .acquire()
                .await
                .map_err(|_| std::io::Error::other("Setup test admission gate closed"))?
                .forget();
        }
        if self.fail_prepare.load(Ordering::SeqCst) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Setup test writer is closed",
            ));
        }
        Ok(crate::store::session_store::PreparedRolloutBatch::new(
            move |items| {
                assert!(items.len() <= maximum_records);
                for item in items {
                    if let crate::schemas::rollout::RolloutItem::Event(event) = item {
                        self.append_event_sync(
                            event.event,
                            Some(AppendEventOptions {
                                turn_id: event.turn_id,
                                payload: event.payload,
                            }),
                        );
                    }
                }
            },
        ))
    }

    async fn append_message(&self, _message: Message) {}

    async fn append_turn_context(&self, _ctx: TurnContextSnapshot) {}

    async fn append_compaction(&self, _record: CompactionRecord) {}

    async fn append_event(&self, event: RuntimeString, opts: Option<AppendEventOptions>) {
        self.append_event_sync(event, opts);
    }

    async fn append_rollback(&self, _record: RollbackRecord) {}

    async fn append_checkpoint(&self, _record: CheckpointRecord) {}

    async fn append_usage(&self, _record: UsageLedgerItem) {}

    async fn append_subagent_lifecycle(&self, _record: SubagentLifecycleRecord) {}

    async fn append_background_process_lifecycle(
        &self,
        _record: crate::schemas::background_process::BackgroundProcessRecord,
    ) {
    }

    async fn flush(&self) -> std::io::Result<()> {
        let call = self.flush_calls.fetch_add(1, Ordering::SeqCst) + 1;
        self.flush_changed.notify_waiters();
        if self.panic_flush_call == Some(call) {
            panic!("injected Setup rollout flush panic at call {call}");
        }
        if self.fail_flush_call == Some(call) {
            return Err(std::io::Error::other(format!(
                "injected Setup rollout flush failure at call {call}"
            )));
        }
        if call == self.block_flush_call {
            self.flush_release.notified().await;
        }
        Ok(())
    }

    async fn shutdown(&self) -> std::io::Result<()> {
        self.shutdown_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Queue checkpoint adapter with an independently controllable first
/// persistence barrier. It distinguishes a joined error owner (where final
/// rollout flush/shutdown is still ordered and safe) from a live owner that
/// has crossed the shutdown deadline (where that final boundary must not be
/// established).
#[derive(Debug)]
struct ControlledQueueShutdownRollout {
    block_flush_call: Option<usize>,
    fail_flush_call: Option<usize>,
    queue_append_calls: AtomicUsize,
    flush_calls: AtomicUsize,
    flush_changed: tokio::sync::Notify,
    flush_release: tokio::sync::Notify,
    shutdown_calls: AtomicUsize,
}

impl ControlledQueueShutdownRollout {
    fn blocking(flush_call: usize) -> Arc<Self> {
        Self::configured(Some(flush_call), None)
    }

    fn failing(flush_call: usize) -> Arc<Self> {
        Self::configured(None, Some(flush_call))
    }

    fn configured(block_flush_call: Option<usize>, fail_flush_call: Option<usize>) -> Arc<Self> {
        Arc::new(Self {
            block_flush_call,
            fail_flush_call,
            queue_append_calls: AtomicUsize::new(0),
            flush_calls: AtomicUsize::new(0),
            flush_changed: tokio::sync::Notify::new(),
            flush_release: tokio::sync::Notify::new(),
            shutdown_calls: AtomicUsize::new(0),
        })
    }

    async fn wait_for_flush_calls(&self, expected: usize) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let changed = self.flush_changed.notified();
            if self.flush_calls.load(Ordering::SeqCst) >= expected {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "selected queue rollout flush {expected} was not reached"
            );
            // Use a real-time watchdog instead of Tokio time: the queue
            // deadline tests pause Tokio's clock and must not move the
            // shutdown deadline merely while waiting for their setup gate.
            tokio::select! {
                _ = changed => {}
                _ = tokio::task::yield_now() => {}
            }
        }
    }

    fn release_blocked_flush(&self) {
        self.flush_release.notify_one();
    }
}

#[async_trait::async_trait]
impl LiveRollout for ControlledQueueShutdownRollout {
    async fn prepare_batch(
        &self,
        maximum_records: usize,
    ) -> std::io::Result<crate::store::session_store::PreparedRolloutBatch<'_>> {
        crate::testing::immediate_rollout::prepare_immediate_test_rollout_batch(
            self,
            maximum_records,
        )
    }

    async fn append_message(&self, _message: Message) {}

    async fn append_turn_context(&self, _ctx: TurnContextSnapshot) {}

    async fn append_compaction(&self, _record: CompactionRecord) {}

    async fn append_event(&self, event: RuntimeString, _opts: Option<AppendEventOptions>) {
        if event == QUEUE_STATE_ROLLOUT_EVENT {
            self.queue_append_calls.fetch_add(1, Ordering::SeqCst);
        }
    }

    async fn append_rollback(&self, _record: RollbackRecord) {}

    async fn append_checkpoint(&self, _record: CheckpointRecord) {}

    async fn append_usage(&self, _record: UsageLedgerItem) {}

    async fn append_subagent_lifecycle(&self, _record: SubagentLifecycleRecord) {}

    async fn append_background_process_lifecycle(
        &self,
        _record: crate::schemas::background_process::BackgroundProcessRecord,
    ) {
    }

    async fn flush(&self) -> std::io::Result<()> {
        let call = self.flush_calls.fetch_add(1, Ordering::SeqCst) + 1;
        self.flush_changed.notify_waiters();
        if self.fail_flush_call == Some(call) {
            return Err(std::io::Error::other(format!(
                "injected queue rollout flush failure at call {call}"
            )));
        }
        if self.block_flush_call == Some(call) {
            self.flush_release.notified().await;
        }
        Ok(())
    }

    async fn shutdown(&self) -> std::io::Result<()> {
        self.shutdown_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn queue_shutdown_test_session(rollout: RolloutSink) -> Arc<Session> {
    let mut config = default_config();
    config.agent_memory = false;
    mk_session_with_provider_memory_and_store(
        Arc::new(fake_provider()),
        mk_deps(Vec::new()),
        config,
        Vec::new(),
        None,
        Some(rollout),
        None,
        None,
    )
}

async fn wait_for_shutdown_deadlines_to_start(session: &Session) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !session.maintenance_is_closing() {
        assert!(
            std::time::Instant::now() < deadline,
            "shutdown_once did not close maintenance admission"
        );
        // These callers pause Tokio time. Bound the setup wait with the OS
        // clock so observing the shutdown gate cannot move the persistence
        // deadline that the test is about to exercise.
        tokio::task::yield_now().await;
    }
}

/// Deterministic durability adapter for Auto Memory/Auto Dream lifecycle
/// tests. Maintenance append calls and all flush/shutdown barriers are
/// counted independently so visibility can be checked while a selected
/// persistence barrier is held.
#[derive(Debug)]
struct ControlledMaintenanceRollout {
    fail_prepare: AtomicBool,
    block_flush_call: usize,
    panic_append_call: Option<usize>,
    panic_flush_call: Option<usize>,
    append_calls: AtomicUsize,
    flush_calls: AtomicUsize,
    flush_changed: tokio::sync::Notify,
    flush_release: tokio::sync::Notify,
    events: Mutex<Vec<MemoryMaintenanceEvent>>,
    shutdown_calls: AtomicUsize,
}

impl ControlledMaintenanceRollout {
    fn append_event_sync(&self, event: RuntimeString, opts: Option<AppendEventOptions>) {
        if event != MEMORY_MAINTENANCE_ROLLOUT_EVENT {
            return;
        }
        let call = self.append_calls.fetch_add(1, Ordering::SeqCst) + 1;
        if self.panic_append_call == Some(call) {
            panic!("injected memory maintenance append panic at call {call}");
        }
        let payload = opts
            .and_then(|opts| opts.payload)
            .expect("memory maintenance lifecycle payload");
        let event = runtime_codec::decode_value::<MemoryMaintenanceEvent>(&payload)
            .expect("memory maintenance lifecycle codec");
        self.events.lock().unwrap().push(event);
    }

    fn new(block_flush_call: usize) -> Arc<Self> {
        Self::configured(block_flush_call, None, None)
    }

    fn panicking_append(call: usize) -> Arc<Self> {
        Self::configured(usize::MAX, Some(call), None)
    }

    fn panicking_flush(call: usize) -> Arc<Self> {
        Self::configured(usize::MAX, None, Some(call))
    }

    fn configured(
        block_flush_call: usize,
        panic_append_call: Option<usize>,
        panic_flush_call: Option<usize>,
    ) -> Arc<Self> {
        Arc::new(Self {
            fail_prepare: AtomicBool::new(false),
            block_flush_call,
            panic_append_call,
            panic_flush_call,
            append_calls: AtomicUsize::new(0),
            flush_calls: AtomicUsize::new(0),
            flush_changed: tokio::sync::Notify::new(),
            flush_release: tokio::sync::Notify::new(),
            events: Mutex::new(Vec::new()),
            shutdown_calls: AtomicUsize::new(0),
        })
    }

    async fn wait_for_flush(&self, expected: usize) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let changed = self.flush_changed.notified();
                if self.flush_calls.load(Ordering::SeqCst) >= expected {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("selected memory maintenance flush should be reached");
    }

    fn release_flush(&self) {
        self.flush_release.notify_one();
    }

    fn events(&self) -> Vec<MemoryMaintenanceEvent> {
        self.events.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl LiveRollout for ControlledMaintenanceRollout {
    async fn prepare_batch(
        &self,
        maximum_records: usize,
    ) -> std::io::Result<crate::store::session_store::PreparedRolloutBatch<'_>> {
        if self.fail_prepare.load(Ordering::SeqCst) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "maintenance test writer is closed",
            ));
        }

        Ok(crate::store::session_store::PreparedRolloutBatch::new(
            move |items| {
                assert!(items.len() <= maximum_records);
                for item in items {
                    if let crate::schemas::rollout::RolloutItem::Event(event) = item {
                        self.append_event_sync(
                            event.event,
                            Some(AppendEventOptions {
                                turn_id: event.turn_id,
                                payload: event.payload,
                            }),
                        );
                    }
                }
            },
        ))
    }

    async fn append_message(&self, _message: Message) {}

    async fn append_turn_context(&self, _ctx: TurnContextSnapshot) {}

    async fn append_compaction(&self, _record: CompactionRecord) {}

    async fn append_event(&self, event: RuntimeString, opts: Option<AppendEventOptions>) {
        self.append_event_sync(event, opts);
    }

    async fn append_rollback(&self, _record: RollbackRecord) {}

    async fn append_checkpoint(&self, _record: CheckpointRecord) {}

    async fn append_usage(&self, _record: UsageLedgerItem) {}

    async fn append_subagent_lifecycle(&self, _record: SubagentLifecycleRecord) {}

    async fn append_background_process_lifecycle(
        &self,
        _record: crate::schemas::background_process::BackgroundProcessRecord,
    ) {
    }

    async fn flush(&self) -> std::io::Result<()> {
        let call = self.flush_calls.fetch_add(1, Ordering::SeqCst) + 1;
        self.flush_changed.notify_waiters();
        if self.panic_flush_call == Some(call) {
            panic!("injected memory maintenance flush panic at call {call}");
        }
        if self.block_flush_call == call {
            self.flush_release.notified().await;
        }
        Ok(())
    }

    async fn shutdown(&self) -> std::io::Result<()> {
        self.shutdown_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Deterministic Agent Memory snapshot durability adapter. Snapshot append
/// calls are counted separately while flush calls cover the real shared
/// rollout barrier, matching production's append+flush contract.
#[derive(Debug)]
struct ControlledSnapshotRollout {
    block_flush_call: usize,
    panic_append_call: Option<usize>,
    panic_flush_call: Option<usize>,
    fail_flush_call: Option<usize>,
    append_calls: AtomicUsize,
    flush_calls: AtomicUsize,
    flush_changed: tokio::sync::Notify,
    flush_release: tokio::sync::Notify,
    events: Mutex<Vec<AgentMemorySnapshotDecisionAudit>>,
    shutdown_calls: AtomicUsize,
}

impl ControlledSnapshotRollout {
    fn new(block_flush_call: usize) -> Arc<Self> {
        Self::configured(block_flush_call, None, None, None)
    }

    fn panicking_append(call: usize) -> Arc<Self> {
        Self::configured(usize::MAX, Some(call), None, None)
    }

    fn panicking_flush(call: usize) -> Arc<Self> {
        Self::configured(usize::MAX, None, Some(call), None)
    }

    fn failing_flush(call: usize) -> Arc<Self> {
        Self::configured(usize::MAX, None, None, Some(call))
    }

    fn configured(
        block_flush_call: usize,
        panic_append_call: Option<usize>,
        panic_flush_call: Option<usize>,
        fail_flush_call: Option<usize>,
    ) -> Arc<Self> {
        Arc::new(Self {
            block_flush_call,
            panic_append_call,
            panic_flush_call,
            fail_flush_call,
            append_calls: AtomicUsize::new(0),
            flush_calls: AtomicUsize::new(0),
            flush_changed: tokio::sync::Notify::new(),
            flush_release: tokio::sync::Notify::new(),
            events: Mutex::new(Vec::new()),
            shutdown_calls: AtomicUsize::new(0),
        })
    }

    async fn wait_for_flush(&self, expected: usize) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let changed = self.flush_changed.notified();
                if self.flush_calls.load(Ordering::SeqCst) >= expected {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("selected snapshot rollout flush should be reached");
    }

    fn release_flush(&self) {
        self.flush_release.notify_one();
    }

    fn events(&self) -> Vec<AgentMemorySnapshotDecisionAudit> {
        self.events.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl LiveRollout for ControlledSnapshotRollout {
    async fn prepare_batch(
        &self,
        maximum_records: usize,
    ) -> std::io::Result<crate::store::session_store::PreparedRolloutBatch<'_>> {
        crate::testing::immediate_rollout::prepare_immediate_test_rollout_batch(
            self,
            maximum_records,
        )
    }

    async fn append_message(&self, _message: Message) {}

    async fn append_turn_context(&self, _ctx: TurnContextSnapshot) {}

    async fn append_compaction(&self, _record: CompactionRecord) {}

    async fn append_event(&self, event: RuntimeString, opts: Option<AppendEventOptions>) {
        if event != AGENT_MEMORY_SNAPSHOT_DECISION_ROLLOUT_EVENT {
            return;
        }
        let call = self.append_calls.fetch_add(1, Ordering::SeqCst) + 1;
        if self.panic_append_call == Some(call) {
            panic!("injected snapshot rollout append panic at call {call}");
        }
        let payload = opts
            .and_then(|opts| opts.payload)
            .expect("snapshot decision audit payload");
        let audit = runtime_codec::decode_value::<AgentMemorySnapshotDecisionAudit>(&payload)
            .expect("snapshot decision audit codec");
        self.events.lock().unwrap().push(audit);
    }

    async fn append_rollback(&self, _record: RollbackRecord) {}

    async fn append_checkpoint(&self, _record: CheckpointRecord) {}

    async fn append_usage(&self, _record: UsageLedgerItem) {}

    async fn append_subagent_lifecycle(&self, _record: SubagentLifecycleRecord) {}

    async fn append_background_process_lifecycle(
        &self,
        _record: crate::schemas::background_process::BackgroundProcessRecord,
    ) {
    }

    async fn flush(&self) -> std::io::Result<()> {
        let call = self.flush_calls.fetch_add(1, Ordering::SeqCst) + 1;
        self.flush_changed.notify_waiters();
        if self.panic_flush_call == Some(call) {
            panic!("injected snapshot rollout flush panic at call {call}");
        }
        if self.fail_flush_call == Some(call) {
            return Err(std::io::Error::other(format!(
                "injected snapshot rollout flush failure at call {call}"
            )));
        }
        if self.block_flush_call == call {
            self.flush_release.notified().await;
        }
        Ok(())
    }

    async fn shutdown(&self) -> std::io::Result<()> {
        self.shutdown_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn maintenance_durability_test_session(rollout: RolloutSink) -> Arc<Session> {
    let bus = Arc::new(HookBus::new(
        crate::runtime_process::test_runtime_process_context(),
        Vec::new(),
    ));
    mk_setup_session(mk_deps(Vec::new()), bus, Some(rollout), None)
}

fn maintenance_test_started(session: &Session, run_id: &str) -> AutoMemoryRun {
    AutoMemoryRun {
        run_id: run_id.into(),
        session_id: session.id.clone(),
        trigger: MemoryMaintenanceTrigger::Auto,
        scope: AgentMemoryScope::Project,
        target_dir: "/tmp/maintenance-durability".into(),
        status: MemoryMaintenanceStatus::Started,
        started_at: 1,
        ended_at: None,
        reason: None,
        error: None,
        message_count: 1,
        cursor_message_uuid: None,
        files_touched: Vec::new(),
        turn_count: None,
    }
}

fn maintenance_test_dream_started(session: &Session, run_id: &str) -> AutoDreamRun {
    AutoDreamRun {
        dream_run_id: run_id.into(),
        session_id: session.id.clone(),
        trigger: MemoryMaintenanceTrigger::Auto,
        status: MemoryMaintenanceStatus::Started,
        started_at: 1,
        ended_at: None,
        reason: None,
        error: None,
        last_consolidated_at: None,
        sessions_reviewing: 1,
        files_touched: Vec::new(),
        turns: Vec::new(),
        turn_count: 0,
        summary: None,
    }
}

/// The terminal Setup flush never resolves unless its task is incorrectly
/// detached. Counters make every append/flush after final rollout shutdown
/// observable.
#[derive(Debug)]
struct LateFailRollout {
    block_flush_call: usize,
    flush_calls: AtomicUsize,
    terminal_flush_entered: tokio::sync::Notify,
    flush_release: tokio::sync::Notify,
    shutdown: AtomicBool,
    late_append_calls: AtomicUsize,
    late_flush_calls: AtomicUsize,
    setup_events: Mutex<Vec<SetupLifecycleEvent>>,
}

impl LateFailRollout {
    fn append_event_sync(&self, event: RuntimeString, opts: Option<AppendEventOptions>) {
        if self.shutdown.load(Ordering::SeqCst) {
            self.late_append_calls.fetch_add(1, Ordering::SeqCst);
        }
        if event != SETUP_LIFECYCLE_ROLLOUT_EVENT {
            return;
        }
        let payload = opts
            .and_then(|opts| opts.payload)
            .expect("Setup lifecycle event payload");
        let event = runtime_codec::decode_value::<SetupLifecycleEvent>(&payload)
            .expect("Setup lifecycle event codec");
        self.setup_events.lock().unwrap().push(event);
    }

    fn new() -> Arc<Self> {
        Self::blocking_flush(2)
    }

    fn blocked_started() -> Arc<Self> {
        Self::blocking_flush(1)
    }

    fn blocking_flush(block_flush_call: usize) -> Arc<Self> {
        Arc::new(Self {
            block_flush_call,
            flush_calls: AtomicUsize::new(0),
            terminal_flush_entered: tokio::sync::Notify::new(),
            flush_release: tokio::sync::Notify::new(),
            shutdown: AtomicBool::new(false),
            late_append_calls: AtomicUsize::new(0),
            late_flush_calls: AtomicUsize::new(0),
            setup_events: Mutex::new(Vec::new()),
        })
    }

    async fn wait_for_flush(&self, expected: usize) {
        loop {
            let changed = self.terminal_flush_entered.notified();
            if self.flush_calls.load(Ordering::SeqCst) >= expected {
                return;
            }
            changed.await;
        }
    }
}

#[async_trait::async_trait]
impl LiveRollout for LateFailRollout {
    async fn prepare_batch(
        &self,
        maximum_records: usize,
    ) -> std::io::Result<crate::store::session_store::PreparedRolloutBatch<'_>> {
        Ok(crate::store::session_store::PreparedRolloutBatch::new(
            move |items| {
                assert!(items.len() <= maximum_records);
                for item in items {
                    if let crate::schemas::rollout::RolloutItem::Event(event) = item {
                        self.append_event_sync(
                            event.event,
                            Some(AppendEventOptions {
                                turn_id: event.turn_id,
                                payload: event.payload,
                            }),
                        );
                    }
                }
            },
        ))
    }

    async fn append_message(&self, _message: Message) {}

    async fn append_turn_context(&self, _ctx: TurnContextSnapshot) {}

    async fn append_compaction(&self, _record: CompactionRecord) {}

    async fn append_event(&self, event: RuntimeString, opts: Option<AppendEventOptions>) {
        self.append_event_sync(event, opts);
    }

    async fn append_rollback(&self, _record: RollbackRecord) {}

    async fn append_checkpoint(&self, _record: CheckpointRecord) {}

    async fn append_usage(&self, _record: UsageLedgerItem) {}

    async fn append_subagent_lifecycle(&self, _record: SubagentLifecycleRecord) {}

    async fn append_background_process_lifecycle(
        &self,
        _record: crate::schemas::background_process::BackgroundProcessRecord,
    ) {
    }

    async fn flush(&self) -> std::io::Result<()> {
        if self.shutdown.load(Ordering::SeqCst) {
            self.late_flush_calls.fetch_add(1, Ordering::SeqCst);
        }
        let call = self.flush_calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call == self.block_flush_call {
            self.terminal_flush_entered.notify_waiters();
            self.flush_release.notified().await;
        }
        Ok(())
    }

    async fn shutdown(&self) -> std::io::Result<()> {
        self.shutdown.store(true, Ordering::SeqCst);
        Ok(())
    }
}

async fn wait_for_setup_status(session: &Arc<Session>, request_id: &str, expected: SetupRunStatus) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if session
                .setup
                .lifecycle
                .lock()
                .unwrap()
                .run(&RuntimeString::from(request_id))
                .is_some_and(|run| run.status == expected)
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Setup lifecycle should reach the expected status");
}

async fn wait_for_visible_setup_status(
    session: &Arc<Session>,
    request_id: &str,
    expected: SetupRunStatus,
) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if session
                .setup_lifecycle_snapshot()
                .run(&RuntimeString::from(request_id))
                .is_some_and(|run| run.status == expected)
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("visible Setup lifecycle should cross its durability receipt");
}

#[tokio::test]
async fn setup_init_failure_is_visible_retryable_and_success_is_cached() {
    let calls = Arc::new(AtomicUsize::new(0));
    let bus = setup_bus(Hook {
        event: HookEvent::Setup,
        matcher: None,
        handler: crate::hooks::types::HookHandler::Fn(Arc::new(SequencedSetupHook {
            calls: calls.clone(),
        })),
        // Setup must force this normally-background hook to run inline.
        is_async: true,
    });
    let session = mk_setup_session(mk_deps(Vec::new()), bus, None, None);

    let failed = session
        .run_setup_hooks(SetupRunRequest {
            request_id: "init-1".into(),
            trigger: SetupTrigger::Init,
        })
        .await;
    assert_eq!(failed.status, SetupRunStatus::Failed);
    assert_eq!(failed.error, Some("init failed visibly".into()));
    assert_eq!(failed.hook_count, 1);

    let duplicate = session
        .run_setup_hooks(SetupRunRequest {
            request_id: "init-1".into(),
            trigger: SetupTrigger::Init,
        })
        .await;
    assert_eq!(duplicate, failed, "same request id is exactly idempotent");
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let completed = session
        .run_setup_hooks(SetupRunRequest {
            request_id: "init-2".into(),
            trigger: SetupTrigger::Init,
        })
        .await;
    assert_eq!(completed.status, SetupRunStatus::Completed);
    assert_eq!(completed.additional_context, vec!["INIT CONTEXT"]);

    let cached = session
        .run_setup_hooks(SetupRunRequest {
            request_id: "init-3".into(),
            trigger: SetupTrigger::Init,
        })
        .await;
    assert_eq!(cached.status, SetupRunStatus::AlreadyCompleted);
    assert_eq!(cached.source_request_id, Some("init-2".into()));
    assert_eq!(cached.additional_context, vec!["INIT CONTEXT"]);
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    let maintenance_1 = session
        .run_setup_hooks(SetupRunRequest {
            request_id: "maintenance-1".into(),
            trigger: SetupTrigger::Maintenance,
        })
        .await;
    assert_eq!(maintenance_1.status, SetupRunStatus::Completed);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(
        session
            .run_setup_hooks(SetupRunRequest {
                request_id: "maintenance-1".into(),
                trigger: SetupTrigger::Maintenance,
            })
            .await,
        maintenance_1
    );
    let maintenance_2 = session
        .run_setup_hooks(SetupRunRequest {
            request_id: "maintenance-2".into(),
            trigger: SetupTrigger::Maintenance,
        })
        .await;
    assert_eq!(maintenance_2.status, SetupRunStatus::Completed);
    assert_eq!(calls.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn concurrent_setup_init_calls_serialize_and_execute_only_one_success() {
    let calls = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let bus = setup_bus(Hook {
        event: HookEvent::Setup,
        matcher: Some("init".to_string()),
        handler: crate::hooks::types::HookHandler::Fn(Arc::new(GatedSetupHook {
            calls: calls.clone(),
            started: started.clone(),
            release: release.clone(),
        })),
        is_async: true,
    });
    let session = mk_setup_session(mk_deps(Vec::new()), bus, None, None);

    let started_wait = started.notified();
    let first_session = session.clone();
    let first = tokio::spawn(async move {
        first_session
            .run_setup_hooks(SetupRunRequest {
                request_id: "first".into(),
                trigger: SetupTrigger::Init,
            })
            .await
    });
    started_wait.await;
    let second_session = session.clone();
    let second = tokio::spawn(async move {
        second_session
            .run_setup_hooks(SetupRunRequest {
                request_id: "second".into(),
                trigger: SetupTrigger::Init,
            })
            .await
    });
    tokio::task::yield_now().await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    release.notify_one();

    assert_eq!(first.await.unwrap().status, SetupRunStatus::Completed);
    let second = second.await.unwrap();
    assert_eq!(second.status, SetupRunStatus::AlreadyCompleted);
    assert_eq!(second.source_request_id, Some("first".into()));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn concurrent_duplicate_setup_request_waits_for_one_authoritative_receipt() {
    let calls = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let bus = setup_bus(Hook {
        event: HookEvent::Setup,
        matcher: Some("maintenance".to_string()),
        handler: crate::hooks::types::HookHandler::Fn(Arc::new(GatedSetupHook {
            calls: calls.clone(),
            started: started.clone(),
            release: release.clone(),
        })),
        is_async: true,
    });
    let rollout = ControlledSetupRollout::new(usize::MAX);
    let rollout_sink: RolloutSink = rollout.clone();
    let session = mk_setup_session(mk_deps(Vec::new()), bus, Some(rollout_sink), None);
    let request = SetupRunRequest {
        request_id: "duplicate-maintenance".into(),
        trigger: SetupTrigger::Maintenance,
    };

    let started_wait = started.notified();
    let first_session = session.clone();
    let first_request = request.clone();
    let first = tokio::spawn(async move { first_session.run_setup_hooks(first_request).await });
    started_wait.await;

    let duplicate_session = session.clone();
    let duplicate = tokio::spawn(async move { duplicate_session.run_setup_hooks(request).await });
    tokio::task::yield_now().await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    release.notify_one();
    let first = first.await.unwrap();
    let duplicate = duplicate.await.unwrap();
    assert_eq!(first.status, SetupRunStatus::Completed);
    assert_eq!(duplicate, first);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    {
        let registry = session.setup.durability.lock().unwrap();
        assert_eq!(registry.receipts.len(), 1);
        assert!(registry
            .receipts
            .contains_key(&RuntimeString::from("duplicate-maintenance")));
    }
    assert_eq!(
        rollout
            .setup_events()
            .into_iter()
            .filter_map(|event| match event {
                SetupLifecycleEvent::Run { run } => Some(run.status),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![SetupRunStatus::Started, SetupRunStatus::Completed],
        "a duplicate caller must not append a second Started or terminal event"
    );
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn a_pre_admitted_init_finishes_before_session_start_and_the_first_prompt() {
    let calls = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let bus = setup_bus(Hook {
        event: HookEvent::Setup,
        matcher: Some("init".to_string()),
        handler: crate::hooks::types::HookHandler::Fn(Arc::new(GatedSetupHook {
            calls,
            started: started.clone(),
            release: release.clone(),
        })),
        is_async: true,
    });
    let session = mk_setup_session(
        recording_deps(requests.clone(), vec![text_turn("done")]),
        bus,
        None,
        None,
    );

    let setup_session = session.clone();
    let setup = tokio::spawn(async move {
        setup_session
            .run_setup_hooks(SetupRunRequest {
                request_id: "init-before-run".into(),
                trigger: SetupTrigger::Init,
            })
            .await
    });
    started.notified().await;

    let run_session = session.clone();
    let run = tokio::spawn(async move {
        run_session
            .run(Input::Text("go".into()), TurnOptions::default())
            .await
    });
    tokio::task::yield_now().await;
    assert!(
        requests.lock().unwrap().is_empty(),
        "the provider must not run while pre-admitted Setup is pending"
    );

    release.notify_one();
    assert_eq!(setup.await.unwrap().status, SetupRunStatus::Completed);
    assert_eq!(run.await.unwrap().stop_reason, TerminalReason::Completed);
    let requests = requests.lock().unwrap();
    let setup_context = requests[0]
        .messages
        .iter()
        .find_map(|message| match message {
            Message::User(user) if user.is_meta == Some(true) => {
                user.content.iter().find_map(|block| match block {
                    ContentBlock::Text(text) if text.text == "GATED" => Some(text.text.clone()),
                    _ => None,
                })
            }
            _ => None,
        });
    assert_eq!(setup_context, Some("GATED".into()));
}

#[tokio::test]
async fn init_close_append_or_flush_failure_stops_before_provider_admission() {
    let cases = [
        (
            "append-panic",
            ControlledSetupRollout::panicking_append(1),
            "failed to append Setup InitClosed",
        ),
        (
            "flush-failure",
            ControlledSetupRollout::failing_flush(1),
            "failed to persist Setup InitClosed",
        ),
    ];

    for (name, rollout, expected_error) in cases {
        let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
        let bus = Arc::new(HookBus::new(
            crate::runtime_process::test_runtime_process_context(),
            Vec::new(),
        ));
        let rollout_sink: RolloutSink = rollout.clone();
        let session = mk_setup_session(
            recording_deps(requests.clone(), vec![text_turn("must-not-run")]),
            bus,
            Some(rollout_sink),
            None,
        );

        let result = session
            .run(
                Input::Text(RuntimeString::from(name)),
                TurnOptions::default(),
            )
            .await;
        assert_eq!(result.stop_reason, TerminalReason::Error, "{name}");
        assert!(
            result
                .error
                .as_ref()
                .is_some_and(|error| error.message.contains(expected_error)),
            "{name}: {:?}",
            result.error.as_ref().map(|error| &error.message)
        );
        assert!(
            requests.lock().unwrap().is_empty(),
            "{name}: provider execution must remain behind InitClosed durability"
        );
        assert!(
            session
                .setup
                .lifecycle
                .lock()
                .unwrap()
                .init_closed_at
                .is_some(),
            "{name}: live init admission must fail closed"
        );
        assert_eq!(
            session.setup_lifecycle_snapshot().init_closed_at,
            None,
            "{name}: an unacknowledged close must not be query-visible as durable"
        );
        assert_eq!(
            session
                .run_setup_hooks(SetupRunRequest {
                    request_id: RuntimeString::from(format!("late-{name}")),
                    trigger: SetupTrigger::Init,
                })
                .await
                .status,
            SetupRunStatus::TooLate,
            "{name}: the live process must not retry init after an uncertain close"
        );
        let shutdown = session
            .shutdown_and_wait()
            .await
            .expect_err("InitClosed durability failure must make shutdown partial");
        assert!(
            shutdown.contains("Setup InitClosed durability failed"),
            "{shutdown}"
        );
    }
}

#[tokio::test]
async fn dropped_turn_retains_init_close_owner_and_shutdown_drains_it() {
    let rollout = ControlledSetupRollout::new(1);
    let bus = Arc::new(HookBus::new(
        crate::runtime_process::test_runtime_process_context(),
        Vec::new(),
    ));
    let rollout_sink: RolloutSink = rollout.clone();
    let session = mk_setup_session(mk_deps(Vec::new()), bus, Some(rollout_sink), None);

    let turn = session.run_streamed(
        Input::Text("drop-before-poll".into()),
        TurnOptions::default(),
    );
    rollout.wait_for_flush_calls(1).await;
    assert!(session
        .setup
        .lifecycle
        .lock()
        .unwrap()
        .init_closed_at
        .is_some());
    assert_eq!(
        session.setup_lifecycle_snapshot().init_closed_at,
        None,
        "pending InitClosed must project the last crash-safe query state"
    );
    drop(turn);

    let closing_session = session.clone();
    let shutdown = tokio::spawn(async move { closing_session.shutdown_and_wait().await });
    tokio::task::yield_now().await;
    assert!(
        !shutdown.is_finished(),
        "shutdown must retain and drain the caller-independent InitClosed owner"
    );
    rollout.release_blocked_flush();
    shutdown.await.unwrap().unwrap();

    assert!(session.setup_lifecycle_snapshot().init_closed_at.is_some());
    assert_eq!(
        rollout
            .setup_events()
            .iter()
            .filter(|event| matches!(event, SetupLifecycleEvent::InitClosed { .. }))
            .count(),
        1
    );
    let registry = session.setup.durability.lock().unwrap();
    assert!(matches!(
        registry
            .init_close_receipt
            .as_ref()
            .and_then(|receipt| receipt.receive.borrow().clone()),
        Some(Ok(()))
    ));
    assert!(registry.tasks.is_empty());
    assert!(registry.task_tracker.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_runs_share_one_init_close_marker_and_receipt() {
    let rollout = ControlledSetupRollout::new(1);
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let bus = Arc::new(HookBus::new(
        crate::runtime_process::test_runtime_process_context(),
        Vec::new(),
    ));
    let rollout_sink: RolloutSink = rollout.clone();
    let session = mk_setup_session(
        recording_deps(
            requests.clone(),
            vec![text_turn("first"), text_turn("second")],
        ),
        bus,
        Some(rollout_sink),
        None,
    );

    let first_session = session.clone();
    let first = tokio::spawn(async move {
        first_session
            .run(Input::Text("first".into()), TurnOptions::default())
            .await
    });
    rollout.wait_for_flush_calls(1).await;
    let second_session = session.clone();
    let second = tokio::spawn(async move {
        second_session
            .run(Input::Text("second".into()), TurnOptions::default())
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if session.turn.active_stack.lock().unwrap().len() == 2 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both runs must share the pending receipt");
    assert_eq!(
        rollout
            .setup_events()
            .iter()
            .filter(|event| matches!(event, SetupLifecycleEvent::InitClosed { .. }))
            .count(),
        1
    );

    rollout.release_blocked_flush();
    assert_eq!(first.await.unwrap().stop_reason, TerminalReason::Completed);
    assert_eq!(second.await.unwrap().stop_reason, TerminalReason::Completed);
    assert_eq!(requests.lock().unwrap().len(), 2);
    assert_eq!(
        rollout
            .setup_events()
            .iter()
            .filter(|event| matches!(event, SetupLifecycleEvent::InitClosed { .. }))
            .count(),
        1,
        "overlapping runs must never append duplicate close markers"
    );
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn init_close_owner_panic_fails_turn_and_marks_shutdown_partial() {
    let rollout = ControlledSetupRollout::new(usize::MAX);
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let bus = Arc::new(HookBus::new(
        crate::runtime_process::test_runtime_process_context(),
        Vec::new(),
    ));
    let rollout_sink: RolloutSink = rollout.clone();
    let session = mk_setup_session(
        recording_deps(requests.clone(), vec![text_turn("must-not-run")]),
        bus,
        Some(rollout_sink),
        None,
    );
    inject_setup_owned_task_panic(session.id.clone(), SetupOwnedTaskKind::InitClose);

    let result = session
        .run(Input::Text("owner-panic".into()), TurnOptions::default())
        .await;
    assert_eq!(result.stop_reason, TerminalReason::Error);
    assert!(result.error.as_ref().is_some_and(|error| error
        .message
        .contains("InitClosed durability task panicked")));
    assert!(requests.lock().unwrap().is_empty());
    assert_eq!(session.setup_lifecycle_snapshot().init_closed_at, None);

    let shutdown = session
        .shutdown_and_wait()
        .await
        .expect_err("a retained InitClosed owner panic must be observable at shutdown");
    assert!(shutdown.contains("InitClose task panicked"), "{shutdown}");
    let registry = session.setup.durability.lock().unwrap();
    assert!(registry.tasks.is_empty());
    assert!(registry.task_tracker.is_empty());
}

#[tokio::test]
async fn shutdown_winning_before_init_close_registration_rejects_late_owner() {
    let rollout = ControlledSetupRollout::new(usize::MAX);
    let bus = Arc::new(HookBus::new(
        crate::runtime_process::test_runtime_process_context(),
        Vec::new(),
    ));
    let rollout_sink: RolloutSink = rollout.clone();
    let session = mk_setup_session(mk_deps(Vec::new()), bus, Some(rollout_sink), None);
    let lease = ActiveTurnLease::try_new(
        session.clone(),
        session.config.model.clone(),
        CancellationToken::new(),
    )
    .expect("turn admission must win first");

    session.close_setup_durability_registry();
    let receipt = session.close_setup_init_for_run();
    assert!(receipt.borrow().as_ref().is_some_and(|result| result
        .as_ref()
        .is_err_and(|error| error.contains("shutdown"))));
    {
        let registry = session.setup.durability.lock().unwrap();
        assert!(registry.closing);
        assert!(registry.init_close_receipt.is_none());
        assert!(registry.tasks.is_empty());
    }
    assert!(rollout.setup_events().is_empty());

    drop(lease);
    session.shutdown_and_wait().await.unwrap();
}

#[test]
fn synchronous_in_memory_turn_creation_without_runtime_preserves_noop_durability() {
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let bus = Arc::new(HookBus::new(
        crate::runtime_process::test_runtime_process_context(),
        Vec::new(),
    ));
    let session = mk_setup_session(
        recording_deps(requests.clone(), vec![text_turn("must-not-run")]),
        bus,
        None,
        None,
    );

    // No rollout means there is no persistence work to retain, matching
    // the established `flush_rollout()` no-op success contract.
    let turn = session.run_streamed(
        Input::Text("outside-runtime".into()),
        TurnOptions::default(),
    );
    let registry = session.setup.durability.lock().unwrap();
    assert!(matches!(
        registry
            .init_close_receipt
            .as_ref()
            .and_then(|receipt| receipt.receive.borrow().clone()),
        Some(Ok(()))
    ));
    assert!(registry.tasks.is_empty());
    drop(registry);
    assert!(session.setup_lifecycle_snapshot().init_closed_at.is_some());
    drop(turn);
    assert!(requests.lock().unwrap().is_empty());
}

#[test]
fn synchronous_persisted_turn_creation_without_runtime_is_fail_closed_not_a_spawn_panic() {
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let bus = Arc::new(HookBus::new(
        crate::runtime_process::test_runtime_process_context(),
        Vec::new(),
    ));
    let rollout = ControlledSetupRollout::new(usize::MAX);
    let rollout_sink: RolloutSink = rollout.clone();
    let session = mk_setup_session(
        recording_deps(requests.clone(), vec![text_turn("must-not-run")]),
        bus,
        Some(rollout_sink),
        None,
    );

    // `run_streamed` is synchronous and may be constructed before the Host
    // enters its runtime. A persisted Session cannot retain its flush in
    // that state, so it returns a stable error instead of spawning/panicking.
    let turn = session.run_streamed(
        Input::Text("outside-runtime".into()),
        TurnOptions::default(),
    );
    let registry = session.setup.durability.lock().unwrap();
    assert!(matches!(
        registry
            .init_close_receipt
            .as_ref()
            .and_then(|receipt| receipt.receive.borrow().clone()),
        Some(Err(error)) if error.contains("without an active Tokio runtime")
    ));
    assert!(registry.tasks.is_empty());
    drop(registry);
    assert_eq!(session.setup_lifecycle_snapshot().init_closed_at, None);
    drop(turn);
    assert!(requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cancelled_sdk_waiter_keeps_session_owned_setup_until_shutdown_aborts_and_joins() {
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(tokio::sync::Notify::new());
    let side_effects = Arc::new(AtomicUsize::new(0));
    let bus = setup_bus(Hook {
        event: HookEvent::Setup,
        matcher: Some("init".to_string()),
        handler: crate::hooks::types::HookHandler::Fn(Arc::new(CancellationObservedSetupHook {
            started: started.clone(),
            release: release.clone(),
            dropped: dropped.clone(),
            side_effects: side_effects.clone(),
        })),
        is_async: true,
    });
    let session = mk_setup_session(mk_deps(Vec::new()), bus, None, None);

    let started_wait = started.notified();
    let cancelled_session = session.clone();
    let cancelled = tokio::spawn(async move {
        cancelled_session
            .run_setup_hooks(SetupRunRequest {
                request_id: "cancelled-sdk-waiter".into(),
                trigger: SetupTrigger::Init,
            })
            .await
    });
    started_wait.await;
    cancelled.abort();
    assert!(cancelled.await.unwrap_err().is_cancelled());
    wait_for_visible_setup_status(
        &session,
        "cancelled-sdk-waiter",
        SetupRunStatus::OutcomeUnknown,
    )
    .await;
    assert_eq!(
        session
            .setup_lifecycle_snapshot()
            .run(&RuntimeString::from("cancelled-sdk-waiter"))
            .unwrap()
            .status,
        SetupRunStatus::OutcomeUnknown
    );
    assert!(
        session.setup.serial.try_lock().is_err(),
        "caller cancellation must not release the Session-owned serial permit"
    );
    assert!(session
        .setup
        .durability
        .lock()
        .unwrap()
        .tasks
        .values()
        .any(|task| task.kind == SetupOwnedTaskKind::Attempt));

    let hook_dropped = dropped.notified();
    session.shutdown_and_wait().await.unwrap();
    hook_dropped.await;
    release.notify_waiters();
    tokio::task::yield_now().await;
    assert_eq!(
        side_effects.load(Ordering::SeqCst),
        0,
        "the aborted HookFn must not resume an external side effect after shutdown completed"
    );
    {
        let registry = session.setup.durability.lock().unwrap();
        assert!(registry.tasks.is_empty());
        assert!(registry.task_tracker.is_empty());
    }

    let replay = session
        .run_setup_hooks(SetupRunRequest {
            request_id: "cancelled-sdk-waiter".into(),
            trigger: SetupTrigger::Init,
        })
        .await;
    assert_eq!(replay.status, SetupRunStatus::OutcomeUnknown);
}

#[tokio::test]
async fn missing_terminal_receipt_fails_closed_instead_of_replaying_success() {
    let calls = Arc::new(AtomicUsize::new(0));
    let bus = setup_bus(Hook {
        event: HookEvent::Setup,
        matcher: Some("maintenance".to_string()),
        handler: crate::hooks::types::HookHandler::Fn(Arc::new(SequencedSetupHook {
            calls: calls.clone(),
        })),
        is_async: true,
    });
    let session = mk_setup_session(mk_deps(Vec::new()), bus, None, None);
    let request = SetupRunRequest {
        request_id: "missing-receipt".into(),
        trigger: SetupTrigger::Maintenance,
    };

    let completed = session.run_setup_hooks(request.clone()).await;
    assert_eq!(completed.status, SetupRunStatus::Completed);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(session
        .setup
        .durability
        .lock()
        .unwrap()
        .receipts
        .remove(&request.request_id)
        .is_some());

    let replay = session.run_setup_hooks(request).await;
    assert_eq!(replay.status, SetupRunStatus::PersistenceFailed);
    assert!(replay
        .error
        .as_ref()
        .is_some_and(|error| error.contains("authoritative terminal receipt is missing")));
    assert!(replay.additional_context.is_empty());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminal_append_gate_keeps_owner_registration_atomic_against_shutdown() {
    let calls = Arc::new(AtomicUsize::new(0));
    let hook_started = Arc::new(tokio::sync::Notify::new());
    let hook_release = Arc::new(tokio::sync::Notify::new());
    let rollout = ControlledSetupRollout::with_terminal_append_gate();
    let bus = setup_bus(Hook {
        event: HookEvent::Setup,
        matcher: Some("maintenance".to_string()),
        handler: crate::hooks::types::HookHandler::Fn(Arc::new(GatedSetupHook {
            calls: calls.clone(),
            started: hook_started.clone(),
            release: hook_release.clone(),
        })),
        is_async: true,
    });
    let rollout_sink: RolloutSink = rollout.clone();
    let session = mk_setup_session(mk_deps(Vec::new()), bus, Some(rollout_sink), None);

    let hook_started_wait = hook_started.notified();
    let setup_session = session.clone();
    let setup = tokio::spawn(async move {
        setup_session
            .run_setup_hooks(SetupRunRequest {
                request_id: "terminal-append-gated".into(),
                trigger: SetupTrigger::Maintenance,
            })
            .await
    });
    hook_started_wait.await;
    hook_release.notify_one();
    rollout.wait_for_terminal_append().await;

    // Capture observations while the synchronous append is blocked, but
    // defer assertions until after releasing the Condvar. A failing test
    // must never strand a runtime worker inside the injected gate.
    let lifecycle_status = session
        .setup
        .lifecycle
        .lock()
        .unwrap()
        .run(&RuntimeString::from("terminal-append-gated"))
        .map(|run| run.status);
    let registry_owned_during_append = session.setup.durability.try_lock().is_err();

    let shutdown_session = session.clone();
    let shutdown = tokio::spawn(async move { shutdown_session.shutdown_and_wait().await });
    tokio::task::yield_now().await;
    let shutdown_waited_for_owner = !shutdown.is_finished();

    rollout.release_terminal_append();
    let result = setup.await.unwrap();
    let shutdown_result = shutdown.await.unwrap();
    assert_eq!(lifecycle_status, Some(SetupRunStatus::Completed));
    assert!(
        registry_owned_during_append,
        "terminal append must remain inside the registry owner critical section"
    );
    assert!(
        shutdown_waited_for_owner,
        "shutdown cannot close/snapshot Setup between append and owner registration"
    );
    assert_eq!(result.status, SetupRunStatus::Completed);
    shutdown_result.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let registry = session.setup.durability.lock().unwrap();
    assert!(registry.closing);
    assert!(registry.active.is_empty());
    assert!(registry.tasks.is_empty());
    assert!(registry
        .receipts
        .contains_key(&RuntimeString::from("terminal-append-gated")));
    drop(registry);
    assert_eq!(
        rollout
            .setup_events()
            .into_iter()
            .filter_map(|event| match event {
                SetupLifecycleEvent::Run { run } => Some(run.status),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![SetupRunStatus::Started, SetupRunStatus::Completed]
    );
}

#[tokio::test]
async fn pending_init_close_rejects_init_before_writer_capacity_is_available() {
    let calls = Arc::new(AtomicUsize::new(0));
    let rollout = ControlledSetupRollout::new(usize::MAX);
    let bus = setup_bus(Hook {
        event: HookEvent::Setup,
        matcher: Some("init".to_owned()),
        handler: crate::hooks::types::HookHandler::Fn(Arc::new(SequencedSetupHook {
            calls: calls.clone(),
        })),
        is_async: true,
    });
    let sink: RolloutSink = rollout.clone();
    let session = mk_setup_session(mk_deps(Vec::new()), bus, Some(sink), None);
    rollout.block_prepare.store(true, Ordering::SeqCst);
    let receipt = session.close_setup_init_for_run();
    assert!(receipt.borrow().is_none());
    let result = session
        .run_setup_hooks(SetupRunRequest {
            request_id: "init-after-pending-close".into(),
            trigger: SetupTrigger::Init,
        })
        .await;
    assert_eq!(result.status, SetupRunStatus::TooLate);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let changed = rollout.prepare_changed.notified();
            if rollout.prepare_entered.load(Ordering::SeqCst) {
                break;
            }
            changed.await;
        }
    })
    .await
    .expect("InitClosed actual owner waits for prepared admission");
    assert!(session.setup_lifecycle_snapshot().init_closed_at.is_none());
    assert!(rollout.setup_events().is_empty());
    rollout.block_prepare.store(false, Ordering::SeqCst);
    rollout.prepare_release.add_permits(1);
    Session::await_setup_init_close_durability(receipt)
        .await
        .unwrap();
    assert!(session.setup_lifecycle_snapshot().init_closed_at.is_some());
    assert_eq!(
        rollout
            .setup_events()
            .iter()
            .filter(|event| matches!(event, SetupLifecycleEvent::InitClosed { .. }))
            .count(),
        1
    );
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn closed_setup_writer_never_admits_hooks_and_keeps_an_explicit_failed_receipt() {
    let calls = Arc::new(AtomicUsize::new(0));
    let rollout = ControlledSetupRollout::new(usize::MAX);
    let bus = setup_bus(Hook {
        event: HookEvent::Setup,
        matcher: Some("maintenance".to_owned()),
        handler: crate::hooks::types::HookHandler::Fn(Arc::new(SequencedSetupHook {
            calls: calls.clone(),
        })),
        is_async: true,
    });
    let sink: RolloutSink = rollout.clone();
    let session = mk_setup_session(mk_deps(Vec::new()), bus, Some(sink), None);
    rollout.fail_prepare.store(true, Ordering::SeqCst);
    let request = SetupRunRequest {
        request_id: "closed-setup-writer".into(),
        trigger: SetupTrigger::Maintenance,
    };
    let result = session.run_setup_hooks(request.clone()).await;
    assert_eq!(result.status, SetupRunStatus::PersistenceFailed);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(result
        .error
        .as_ref()
        .is_some_and(|error| error.contains("writer is closed")));
    assert_eq!(session.run_setup_hooks(request.clone()).await, result);
    assert_eq!(
        session
            .setup_lifecycle_snapshot()
            .run(&request.request_id)
            .unwrap()
            .status,
        SetupRunStatus::PersistenceFailed
    );
    assert!(rollout.setup_events().is_empty());
    rollout.fail_prepare.store(false, Ordering::SeqCst);
    session
        .shutdown_and_wait()
        .await
        .expect("the failed run remains explicit while its settled cleanup can close normally");
}

#[tokio::test]
async fn setup_started_append_panic_is_contained_without_poisoning_registry() {
    let calls = Arc::new(AtomicUsize::new(0));
    let rollout = ControlledSetupRollout::panicking_append(1);
    let bus = setup_bus(Hook {
        event: HookEvent::Setup,
        matcher: Some("maintenance".to_string()),
        handler: crate::hooks::types::HookHandler::Fn(Arc::new(SequencedSetupHook {
            calls: calls.clone(),
        })),
        is_async: true,
    });
    let rollout_sink: RolloutSink = rollout.clone();
    let session = mk_setup_session(mk_deps(Vec::new()), bus, Some(rollout_sink), None);

    let result = session
        .run_setup_hooks(SetupRunRequest {
            request_id: "started-append-panic".into(),
            trigger: SetupTrigger::Maintenance,
        })
        .await;
    assert_eq!(result.status, SetupRunStatus::Failed);
    assert!(result
        .error
        .as_ref()
        .is_some_and(|error| error.contains("append Setup started")));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(session.setup.durability.lock().is_ok());
    assert!(session.setup.persistence.event_gate.lock().is_ok());
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn setup_terminal_append_panic_is_contained_in_subprocess() {
    const CHILD_ENV: &str = "KLAUS_SETUP_TERMINAL_APPEND_PANIC_CHILD";
    if std::env::var_os(CHILD_ENV).is_none() {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("setup_terminal_append_panic_is_contained_in_subprocess")
            .arg("--nocapture")
            .env(CHILD_ENV, "1")
            .status()
            .expect("spawn isolated terminal-append panic regression");
        assert!(
            status.success(),
            "a contained foreign append panic must not trigger a Drop-time double panic"
        );
        return;
    }

    let calls = Arc::new(AtomicUsize::new(0));
    let rollout = ControlledSetupRollout::panicking_append(2);
    let bus = setup_bus(Hook {
        event: HookEvent::Setup,
        matcher: Some("maintenance".to_string()),
        handler: crate::hooks::types::HookHandler::Fn(Arc::new(SequencedSetupHook {
            calls: calls.clone(),
        })),
        is_async: true,
    });
    let rollout_sink: RolloutSink = rollout.clone();
    let session = mk_setup_session(mk_deps(Vec::new()), bus, Some(rollout_sink), None);
    let result = session
        .run_setup_hooks(SetupRunRequest {
            request_id: "terminal-append-panic".into(),
            trigger: SetupTrigger::Maintenance,
        })
        .await;
    assert_eq!(result.status, SetupRunStatus::PersistenceFailed);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(session.setup.durability.lock().is_ok());
    assert!(session.setup.persistence.event_gate.lock().is_ok());
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn held_setup_flush_gate_returns_typed_timeout_and_skips_rollout_finalization() {
    let rollout = ControlledSetupRollout::new(usize::MAX);
    let bus = setup_bus(Hook {
        event: HookEvent::Setup,
        matcher: Some("maintenance".to_string()),
        handler: crate::hooks::types::HookHandler::Fn(Arc::new(StaticSetupHook {
            context: "unused".into(),
        })),
        is_async: true,
    });
    let rollout_sink: RolloutSink = rollout.clone();
    let session = mk_setup_session(mk_deps(Vec::new()), bus, Some(rollout_sink), None);

    // Model a force-aborted producer whose future did not cooperatively
    // release the Setup flush permit. The shutdown task must not wait on
    // this permit without a deadline.
    let held_flush_gate = session.setup.persistence.serial.clone().lock_owned().await;
    let closing_session = session.clone();
    let shutdown = tokio::spawn(async move { closing_session.shutdown_and_wait_detailed().await });
    tokio::task::yield_now().await;
    tokio::time::advance(std::time::Duration::from_millis(
        SESSION_SHUTDOWN_PERSISTENCE_MS + 1,
    ))
    .await;
    tokio::task::yield_now().await;

    let outcome = tokio::time::timeout(std::time::Duration::from_millis(1), shutdown)
        .await
        .expect("a held Setup flush gate must not make shutdown unbounded")
        .expect("Session-owned shutdown task");
    assert!(outcome.timed_out);
    assert_eq!(
            outcome.errors,
            vec![
                "Setup persistence fence timed out waiting for an in-flight flush; final rollout flush/shutdown was skipped"
                    .to_string()
            ]
        );
    assert_eq!(
        rollout.flush_calls.load(Ordering::SeqCst),
        0,
        "the rollout final flush is unsafe when the Setup fence times out"
    );
    assert_eq!(
        rollout.shutdown_calls.load(Ordering::SeqCst),
        0,
        "rollout shutdown must not be reported as complete without the Setup fence"
    );

    drop(held_flush_gate);
    assert_eq!(
        session.flush_setup_rollout().await.unwrap_err(),
        "Setup persistence is finalized"
    );
    assert_eq!(rollout.flush_calls.load(Ordering::SeqCst), 0);
    assert_eq!(rollout.shutdown_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn setup_terminal_owner_cannot_write_after_final_rollout_shutdown() {
    let rollout = LateFailRollout::new();
    let bus = setup_bus(Hook {
        event: HookEvent::Setup,
        matcher: Some("maintenance".to_string()),
        handler: crate::hooks::types::HookHandler::Fn(Arc::new(StaticSetupHook {
            context: "never acknowledged".into(),
        })),
        is_async: true,
    });
    let rollout_sink: RolloutSink = rollout.clone();
    let session = mk_setup_session(mk_deps(Vec::new()), bus, Some(rollout_sink), None);
    let setup_session = session.clone();
    let setup = tokio::spawn(async move {
        setup_session
            .run_setup_hooks(SetupRunRequest {
                request_id: "late-terminal-owner".into(),
                trigger: SetupTrigger::Maintenance,
            })
            .await
    });
    rollout.wait_for_flush(2).await;

    let closing_session = session.clone();
    let close = tokio::spawn(async move { closing_session.shutdown_and_wait().await });
    tokio::time::advance(std::time::Duration::from_millis(
        SESSION_SHUTDOWN_CLEANUP_MS + 1,
    ))
    .await;
    let error = close
        .await
        .expect("shutdown owner")
        .expect_err("the terminal durability deadline must be typed as partial cleanup");
    assert!(error.contains("Setup durability exceeded its shutdown deadline"));
    let result = setup.await.unwrap();
    assert_eq!(result.status, SetupRunStatus::PersistenceFailed);
    assert!(result.additional_context.is_empty());

    tokio::time::advance(std::time::Duration::from_millis(
        SESSION_SHUTDOWN_PERSISTENCE_MS,
    ))
    .await;
    tokio::task::yield_now().await;
    assert!(rollout.shutdown.load(Ordering::SeqCst));
    assert_eq!(rollout.late_append_calls.load(Ordering::SeqCst), 0);
    assert_eq!(rollout.late_flush_calls.load(Ordering::SeqCst), 0);
    let registry = session.setup.durability.lock().unwrap();
    assert_eq!(
        registry
            .receipts
            .get(&RuntimeString::from("late-terminal-owner"))
            .and_then(|receipt| receipt.receive.borrow().clone())
            .map(|result| result.status),
        Some(SetupRunStatus::PersistenceFailed)
    );
    assert!(registry.tasks.is_empty());
    assert!(registry.task_tracker.is_empty());
}

#[tokio::test(start_paused = true)]
async fn cancelled_started_flush_is_aborted_and_joined_before_final_rollout_shutdown() {
    let calls = Arc::new(AtomicUsize::new(0));
    let rollout = LateFailRollout::blocked_started();
    let bus = setup_bus(Hook {
        event: HookEvent::Setup,
        matcher: Some("init".to_string()),
        handler: crate::hooks::types::HookHandler::Fn(Arc::new(SequencedSetupHook {
            calls: calls.clone(),
        })),
        is_async: true,
    });
    let rollout_sink: RolloutSink = rollout.clone();
    let session = mk_setup_session(mk_deps(Vec::new()), bus, Some(rollout_sink), None);
    let setup_session = session.clone();
    let waiter = tokio::spawn(async move {
        setup_session
            .run_setup_hooks(SetupRunRequest {
                request_id: "cancelled-started-flush".into(),
                trigger: SetupTrigger::Init,
            })
            .await
    });
    rollout.wait_for_flush(1).await;
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    wait_for_setup_status(
        &session,
        "cancelled-started-flush",
        SetupRunStatus::Interrupted,
    )
    .await;
    assert_eq!(
        rollout.flush_calls.load(Ordering::SeqCst),
        1,
        "the terminal owner must queue behind the Session-owned Started flush gate"
    );

    session.shutdown_and_wait().await.unwrap();
    assert!(rollout.shutdown.load(Ordering::SeqCst));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(rollout.flush_calls.load(Ordering::SeqCst), 3);
    rollout.flush_release.notify_waiters();
    tokio::task::yield_now().await;
    assert_eq!(rollout.late_append_calls.load(Ordering::SeqCst), 0);
    assert_eq!(rollout.late_flush_calls.load(Ordering::SeqCst), 0);
    let registry = session.setup.durability.lock().unwrap();
    assert!(registry.tasks.is_empty());
    assert!(registry.task_tracker.is_empty());
    assert_eq!(
        registry
            .receipts
            .get(&RuntimeString::from("cancelled-started-flush"))
            .and_then(|receipt| receipt.receive.borrow().clone())
            .map(|result| result.status),
        Some(SetupRunStatus::Interrupted)
    );
}

#[tokio::test]
async fn immediate_setup_task_completion_leaves_no_stale_controls() {
    let calls = Arc::new(AtomicUsize::new(0));
    let bus = setup_bus(Hook {
        event: HookEvent::Setup,
        matcher: Some("maintenance".to_string()),
        handler: crate::hooks::types::HookHandler::Fn(Arc::new(SequencedSetupHook {
            calls: calls.clone(),
        })),
        is_async: true,
    });
    let session = mk_setup_session(mk_deps(Vec::new()), bus, None, None);
    for index in 0..32 {
        let result = session
            .run_setup_hooks(SetupRunRequest {
                request_id: RuntimeString::from(format!("immediate-{index}")),
                trigger: SetupTrigger::Maintenance,
            })
            .await;
        assert_eq!(result.status, SetupRunStatus::Completed);
    }
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            {
                let registry = session.setup.durability.lock().unwrap();
                if registry.tasks.is_empty() && registry.task_tracker.is_empty() {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("immediate Setup tasks must retire their controls");
    assert_eq!(calls.load(Ordering::SeqCst), 32);
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn terminal_flush_panic_persists_fail_closed_receipt_before_reply() {
    let calls = Arc::new(AtomicUsize::new(0));
    let rollout = ControlledSetupRollout::panicking_flush(2);
    let bus = setup_bus(Hook {
        event: HookEvent::Setup,
        matcher: Some("maintenance".to_string()),
        handler: crate::hooks::types::HookHandler::Fn(Arc::new(SequencedSetupHook {
            calls: calls.clone(),
        })),
        is_async: true,
    });
    let rollout_sink: RolloutSink = rollout.clone();
    let session = mk_setup_session(mk_deps(Vec::new()), bus, Some(rollout_sink), None);
    let request = SetupRunRequest {
        request_id: "terminal-flush-panic".into(),
        trigger: SetupTrigger::Maintenance,
    };

    let result = session.run_setup_hooks(request.clone()).await;
    assert_eq!(result.status, SetupRunStatus::PersistenceFailed);
    assert!(result.additional_context.is_empty());
    assert!(result
        .error
        .as_ref()
        .is_some_and(|error| error.contains("flush adapter panicked")));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        rollout.flush_calls.load(Ordering::SeqCst),
        3,
        "the fail-closed marker must get its own retained flush before reply"
    );
    assert_eq!(
        session
            .setup_lifecycle_snapshot()
            .run(&request.request_id)
            .unwrap()
            .status,
        SetupRunStatus::PersistenceFailed
    );
    assert_eq!(session.run_setup_hooks(request).await, result);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let events = rollout.setup_events();
    assert_eq!(
        events
            .iter()
            .filter_map(|event| match event {
                SetupLifecycleEvent::Run { run } => Some(run.status),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![
            SetupRunStatus::Started,
            SetupRunStatus::Completed,
            SetupRunStatus::PersistenceFailed,
        ]
    );

    session.shutdown_and_wait().await.unwrap();
    assert_eq!(rollout.shutdown_calls.load(Ordering::SeqCst), 1);

    let mut replayed = SetupLifecycleSnapshot::default();
    for event in &events {
        replayed.apply(event);
    }
    assert_eq!(
        replayed
            .run(&RuntimeString::from("terminal-flush-panic"))
            .unwrap()
            .status,
        SetupRunStatus::PersistenceFailed
    );
    let recovered_calls = Arc::new(AtomicUsize::new(0));
    let recovered_bus = setup_bus(Hook {
        event: HookEvent::Setup,
        matcher: Some("maintenance".to_string()),
        handler: crate::hooks::types::HookHandler::Fn(Arc::new(SequencedSetupHook {
            calls: recovered_calls.clone(),
        })),
        is_async: true,
    });
    let recovered_session =
        mk_setup_session(mk_deps(Vec::new()), recovered_bus, None, Some(replayed));
    let recovered = recovered_session
        .run_setup_hooks(SetupRunRequest {
            request_id: "terminal-flush-panic".into(),
            trigger: SetupTrigger::Maintenance,
        })
        .await;
    assert_eq!(recovered, result);
    assert_eq!(recovered_calls.load(Ordering::SeqCst), 0);
    recovered_session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn durability_wrapper_panic_sends_fail_closed_receipt_and_marks_shutdown_partial() {
    let calls = Arc::new(AtomicUsize::new(0));
    let rollout = ControlledSetupRollout::new(usize::MAX);
    let bus = setup_bus(Hook {
        event: HookEvent::Setup,
        matcher: Some("maintenance".to_string()),
        handler: crate::hooks::types::HookHandler::Fn(Arc::new(SequencedSetupHook {
            calls: calls.clone(),
        })),
        is_async: true,
    });
    let rollout_sink: RolloutSink = rollout.clone();
    let session = mk_setup_session(mk_deps(Vec::new()), bus, Some(rollout_sink), None);
    let request_id = RuntimeString::from("durability-wrapper-panic");
    inject_setup_owned_task_panic(request_id.clone(), SetupOwnedTaskKind::Durability);

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        session.run_setup_hooks(SetupRunRequest {
            request_id: request_id.clone(),
            trigger: SetupTrigger::Maintenance,
        }),
    )
    .await
    .expect("retained sender must publish an explicit fail-closed verdict");
    assert_eq!(result.status, SetupRunStatus::PersistenceFailed);
    assert!(result
        .error
        .as_ref()
        .is_some_and(|error| error.contains("task panicked unexpectedly")));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(rollout.flush_calls.load(Ordering::SeqCst), 2);
    let shutdown = session
        .shutdown_and_wait()
        .await
        .expect_err("a contained owner panic must make cleanup observably partial");
    assert!(shutdown.contains("Durability task panicked"), "{shutdown}");
    let registry = session.setup.durability.lock().unwrap();
    assert!(registry.tasks.is_empty());
    assert!(registry.task_tracker.is_empty());
    assert_eq!(
        registry
            .receipts
            .get(&request_id)
            .and_then(|receipt| receipt.receive.borrow().clone())
            .map(|result| result.status),
        Some(SetupRunStatus::PersistenceFailed)
    );
}

#[tokio::test]
async fn shutdown_during_started_flush_interrupts_pre_effect_and_never_dispatches_hook() {
    let calls = Arc::new(AtomicUsize::new(0));
    let rollout = ControlledSetupRollout::new(1);
    let bus = setup_bus(Hook {
        event: HookEvent::Setup,
        matcher: Some("init".to_string()),
        handler: crate::hooks::types::HookHandler::Fn(Arc::new(GatedSetupHook {
            calls: calls.clone(),
            started: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
        })),
        is_async: true,
    });
    let rollout_sink: RolloutSink = rollout.clone();
    let session = mk_setup_session(mk_deps(Vec::new()), bus, Some(rollout_sink), None);

    let setup_session = session.clone();
    let setup = tokio::spawn(async move {
        setup_session
            .run_setup_hooks(SetupRunRequest {
                request_id: "shutdown-pre-effect".into(),
                trigger: SetupTrigger::Init,
            })
            .await
    });
    rollout.wait_for_flush_calls(1).await;

    let shutdown_session = session.clone();
    let shutdown = tokio::spawn(async move { shutdown_session.shutdown_and_wait().await });
    wait_for_setup_status(&session, "shutdown-pre-effect", SetupRunStatus::Interrupted).await;
    rollout.wait_for_flush_calls(2).await;

    {
        let registry = session.setup.durability.lock().unwrap();
        assert!(registry.closing);
        assert!(registry.active.is_empty());
        assert!(registry
            .receipts
            .contains_key(&RuntimeString::from("shutdown-pre-effect")));
        assert!(
                registry.tasks.is_empty(),
                "aborting the Session-owned Started barrier must retire both Attempt and terminal owner"
            );
        assert!(registry.task_tracker.is_empty());
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    rollout.release_blocked_flush();
    let result = setup.await.unwrap();
    assert_eq!(result.status, SetupRunStatus::Interrupted);
    shutdown.await.unwrap().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(rollout.shutdown_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        rollout
            .setup_events()
            .into_iter()
            .filter_map(|event| match event {
                SetupLifecycleEvent::Run { run } => Some(run.status),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![SetupRunStatus::Started, SetupRunStatus::Interrupted]
    );
}

#[tokio::test]
async fn shutdown_after_effect_admission_owns_unknown_receipt_and_ignores_late_completion() {
    let calls = Arc::new(AtomicUsize::new(0));
    let hook_started = Arc::new(tokio::sync::Notify::new());
    let hook_release = Arc::new(tokio::sync::Notify::new());
    let rollout = ControlledSetupRollout::new(2);
    let bus = setup_bus(Hook {
        event: HookEvent::Setup,
        matcher: Some("init".to_string()),
        handler: crate::hooks::types::HookHandler::Fn(Arc::new(GatedSetupHook {
            calls: calls.clone(),
            started: hook_started.clone(),
            release: hook_release.clone(),
        })),
        is_async: true,
    });
    let rollout_sink: RolloutSink = rollout.clone();
    let session = mk_setup_session(mk_deps(Vec::new()), bus, Some(rollout_sink), None);

    let hook_started_wait = hook_started.notified();
    let setup_session = session.clone();
    let setup = tokio::spawn(async move {
        setup_session
            .run_setup_hooks(SetupRunRequest {
                request_id: "shutdown-effect-admitted".into(),
                trigger: SetupTrigger::Init,
            })
            .await
    });
    hook_started_wait.await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let shutdown_session = session.clone();
    let shutdown = tokio::spawn(async move { shutdown_session.shutdown_and_wait().await });
    wait_for_setup_status(
        &session,
        "shutdown-effect-admitted",
        SetupRunStatus::OutcomeUnknown,
    )
    .await;
    rollout.wait_for_flush_calls(2).await;

    {
        let registry = session.setup.durability.lock().unwrap();
        assert!(registry.closing);
        assert!(registry.active.is_empty());
        assert!(registry
            .receipts
            .contains_key(&RuntimeString::from("shutdown-effect-admitted")));
        assert_eq!(registry.tasks.len(), 1);
    }

    hook_release.notify_one();
    tokio::task::yield_now().await;
    assert!(
        !setup.is_finished(),
        "late hook completion must await shutdown's authoritative receipt"
    );
    assert!(
        !shutdown.is_finished(),
        "shutdown must drain the registered terminal owner"
    );
    assert_eq!(
        rollout
            .setup_events()
            .into_iter()
            .filter_map(|event| match event {
                SetupLifecycleEvent::Run { run } => Some(run.status),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![SetupRunStatus::Started, SetupRunStatus::OutcomeUnknown]
    );

    rollout.release_blocked_flush();
    let result = setup.await.unwrap();
    assert_eq!(result.status, SetupRunStatus::OutcomeUnknown);
    assert!(result.additional_context.is_empty());
    shutdown.await.unwrap().unwrap();

    let events = rollout.setup_events();
    let mut replayed = SetupLifecycleSnapshot::default();
    for event in &events {
        replayed.apply(event);
    }
    assert_eq!(
        replayed
            .run(&RuntimeString::from("shutdown-effect-admitted"))
            .unwrap()
            .status,
        SetupRunStatus::OutcomeUnknown
    );
    assert_eq!(
        events
            .into_iter()
            .filter_map(|event| match event {
                SetupLifecycleEvent::Run { run } => Some(run.status),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![SetupRunStatus::Started, SetupRunStatus::OutcomeUnknown],
        "late Completed must never overwrite or append after shutdown's verdict"
    );
}

fn setup_started_before_recovery(request_id: &str) -> SetupLifecycleSnapshot {
    let mut initial = SetupLifecycleSnapshot::default();
    initial.apply(&SetupLifecycleEvent::Run {
        run: SetupRunRecord {
            request_id: request_id.into(),
            session_id: "setup-session".into(),
            trigger: SetupTrigger::Init,
            status: SetupRunStatus::Started,
            started_at: 1,
            ended_at: None,
            hook_count: 0,
            additional_context: Vec::new(),
            system_messages: Vec::new(),
            error: None,
        },
    });
    initial
}

#[tokio::test]
async fn recovered_terminal_waits_for_this_process_durability_receipt() {
    let rollout = ControlledSetupRollout::new(1);
    let calls = Arc::new(AtomicUsize::new(0));
    let bus = setup_bus(Hook {
        event: HookEvent::Setup,
        matcher: Some("init".to_string()),
        handler: crate::hooks::types::HookHandler::Fn(Arc::new(SequencedSetupHook {
            calls: calls.clone(),
        })),
        is_async: true,
    });
    let rollout_sink: RolloutSink = rollout.clone();
    let session = mk_setup_session(
        mk_deps(Vec::new()),
        bus,
        Some(rollout_sink),
        Some(setup_started_before_recovery("recovery-pending")),
    );
    rollout.wait_for_flush_calls(1).await;
    assert_eq!(
        session
            .setup_lifecycle_snapshot()
            .run(&RuntimeString::from("recovery-pending"))
            .unwrap()
            .status,
        SetupRunStatus::Started,
        "query-visible recovery state must not expose OutcomeUnknown before its receipt is durable"
    );

    let barrier_session = session.clone();
    let recovery_barrier = tokio::spawn(async move {
        barrier_session
            .await_pending_setup_recovery_receipts()
            .await
    });

    let query_session = session.clone();
    let query = tokio::spawn(async move {
        query_session
            .run_setup_hooks(SetupRunRequest {
                request_id: "recovery-pending".into(),
                trigger: SetupTrigger::Init,
            })
            .await
    });
    tokio::task::yield_now().await;
    assert!(
        !query.is_finished(),
        "a recovery-generated terminal is not a replay-ready receipt until this process flushes it"
    );
    assert!(
        !recovery_barrier.is_finished(),
        "the resume barrier must retain the constructor recovery receipt"
    );
    rollout.release_blocked_flush();
    recovery_barrier
        .await
        .unwrap()
        .expect("the process-local Setup recovery receipt should become durable");
    assert_eq!(query.await.unwrap().status, SetupRunStatus::OutcomeUnknown);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn setup_recovery_barrier_all_settles_before_aggregating_failure() {
    let rollout = ControlledSetupRollout::configured(2, None, None, Some(1), false);
    let rollout_sink: RolloutSink = rollout.clone();
    let mut initial = setup_started_before_recovery("recovery-all-settled-first");
    initial.apply(&SetupLifecycleEvent::Run {
        run: SetupRunRecord {
            request_id: "recovery-all-settled-second".into(),
            session_id: "setup-session".into(),
            trigger: SetupTrigger::Maintenance,
            status: SetupRunStatus::Started,
            started_at: 2,
            ended_at: None,
            hook_count: 0,
            additional_context: Vec::new(),
            system_messages: Vec::new(),
            error: None,
        },
    });
    let bus = Arc::new(HookBus::new(
        crate::runtime_process::test_runtime_process_context(),
        Vec::new(),
    ));
    let session = mk_setup_session(mk_deps(Vec::new()), bus, Some(rollout_sink), Some(initial));

    let barrier_session = session.clone();
    let recovery_barrier = tokio::spawn(async move {
        barrier_session
            .await_pending_setup_recovery_receipts()
            .await
    });
    rollout.wait_for_flush_calls(2).await;
    assert!(
        !recovery_barrier.is_finished(),
        "one failed receipt must not make the barrier abandon the still-blocked Setup owner"
    );
    rollout.release_blocked_flush();
    let error = recovery_barrier
        .await
        .unwrap()
        .expect_err("the first recovery append panic must fail the aggregate barrier");
    assert!(error
        .try_to_utf8()
        .unwrap()
        .contains("Setup recovery durability failed"));
    {
        let registry = session.setup.durability.lock().unwrap();
        assert_eq!(registry.recovery_receipt_ids.len(), 2);
        assert!(registry
            .recovery_receipt_ids
            .iter()
            .all(|request_id| registry
                .receipts
                .get(request_id)
                .is_some_and(|receipt| receipt.receive.borrow().is_some())));
    }
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn recovered_terminal_append_panic_fails_closed_without_poisoning_constructor_state() {
    let rollout = ControlledSetupRollout::panicking_append(1);
    let calls = Arc::new(AtomicUsize::new(0));
    let bus = setup_bus(Hook {
        event: HookEvent::Setup,
        matcher: Some("init".to_string()),
        handler: crate::hooks::types::HookHandler::Fn(Arc::new(SequencedSetupHook {
            calls: calls.clone(),
        })),
        is_async: true,
    });
    let rollout_sink: RolloutSink = rollout.clone();
    let session = mk_setup_session(
        mk_deps(Vec::new()),
        bus,
        Some(rollout_sink),
        Some(setup_started_before_recovery("recovery-append-panic")),
    );
    let result = session
        .run_setup_hooks(SetupRunRequest {
            request_id: "recovery-append-panic".into(),
            trigger: SetupTrigger::Init,
        })
        .await;
    assert_eq!(result.status, SetupRunStatus::PersistenceFailed);
    let recovery_error = session
        .await_pending_setup_recovery_receipts()
        .await
        .expect_err("a fail-closed recovery receipt must fail the resume barrier");
    assert!(recovery_error
        .try_to_utf8()
        .unwrap()
        .contains("Setup recovery durability failed"));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(session.setup.durability.lock().is_ok());
    assert!(session.setup.persistence.event_gate.lock().is_ok());
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn recovered_unknown_init_outcome_closes_reexecution_without_rerunning_hook() {
    let calls = Arc::new(AtomicUsize::new(0));
    let initial = setup_started_before_recovery("started-before-recovery");
    let bus = setup_bus(Hook {
        event: HookEvent::Setup,
        matcher: Some("init".to_string()),
        handler: crate::hooks::types::HookHandler::Fn(Arc::new(SequencedSetupHook {
            calls: calls.clone(),
        })),
        is_async: true,
    });

    let session = mk_setup_session(mk_deps(Vec::new()), bus, None, Some(initial));

    let recovered = session
        .run_setup_hooks(SetupRunRequest {
            request_id: "started-before-recovery".into(),
            trigger: SetupTrigger::Init,
        })
        .await;
    assert_eq!(recovered.status, SetupRunStatus::OutcomeUnknown);
    assert_eq!(recovered.source_request_id, None);
    assert_eq!(
        session
            .setup_lifecycle_snapshot()
            .run(&RuntimeString::from("started-before-recovery"))
            .unwrap()
            .status,
        SetupRunStatus::OutcomeUnknown
    );

    let retry = session
        .run_setup_hooks(SetupRunRequest {
            request_id: "init-after-recovery".into(),
            trigger: SetupTrigger::Init,
        })
        .await;
    assert_eq!(retry.status, SetupRunStatus::AlreadyCompleted);
    assert_eq!(
        retry.source_request_id,
        Some("started-before-recovery".into())
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

async fn setup_context_test_session(rollout: Option<RolloutSink>) -> Arc<Session> {
    setup_context_test_session_with_deps(mk_deps(Vec::new()), rollout).await
}

async fn setup_context_test_session_with_deps(
    deps: EngineDeps,
    rollout: Option<RolloutSink>,
) -> Arc<Session> {
    let bus = setup_bus(Hook {
        event: HookEvent::Setup,
        matcher: Some("init".to_string()),
        handler: crate::hooks::types::HookHandler::Fn(Arc::new(StaticSetupHook {
            context: "DURABLE SETUP CONTEXT".into(),
        })),
        is_async: true,
    });
    let session = mk_setup_session(deps, bus, rollout, None);
    let setup = session
        .run_setup_hooks(SetupRunRequest {
            request_id: "context-init".into(),
            trigger: SetupTrigger::Init,
        })
        .await;
    assert_eq!(setup.status, SetupRunStatus::Completed);
    session
}

type SetupTerminalObservation = (
    tokio::sync::watch::Receiver<Option<Result<(), RuntimeString>>>,
    Arc<Mutex<Option<SetupContextAdmissionRecord>>>,
);

fn observe_setup_context_terminal_record(
    session: &Arc<Session>,
    admission_id: &RuntimeString,
) -> SetupTerminalObservation {
    let registry = session.setup.durability.lock().unwrap();
    let receipt = registry
        .context_admission_receipts
        .get(admission_id)
        .expect("Setup context receipt");
    (
        receipt.terminal_receive.clone(),
        receipt.terminal_record_handle.clone(),
    )
}

async fn await_observed_setup_context_terminal_record(
    mut receipt: tokio::sync::watch::Receiver<Option<Result<(), RuntimeString>>>,
    terminal_record: Arc<Mutex<Option<SetupContextAdmissionRecord>>>,
) -> (Result<(), RuntimeString>, SetupContextAdmissionRecord) {
    let result = loop {
        if let Some(result) = receipt.borrow().clone() {
            break result;
        }
        receipt
            .changed()
            .await
            .expect("retained Setup context owner must publish a terminal");
    };
    let record = terminal_record
        .lock()
        .unwrap()
        .clone()
        .expect("settled Setup context receipt has a terminal record");
    (result, record)
}

async fn await_setup_context_terminal_record(
    session: &Arc<Session>,
    admission_id: &RuntimeString,
) -> (Result<(), RuntimeString>, SetupContextAdmissionRecord) {
    let (receipt, terminal_record) = observe_setup_context_terminal_record(session, admission_id);
    await_observed_setup_context_terminal_record(receipt, terminal_record).await
}

#[tokio::test]
async fn concurrent_setup_context_claims_serialize_release_and_exactly_once_consumption() {
    let session = setup_context_test_session(None).await;
    let first = session
        .begin_setup_context_claim()
        .await
        .unwrap()
        .expect("completed init has context");
    let first_admission_id = session
        .setup
        .durability
        .lock()
        .unwrap()
        .active_context_admission
        .as_ref()
        .unwrap()
        .record
        .admission_id
        .clone();
    let (first_terminal_receipt, first_terminal_record) =
        observe_setup_context_terminal_record(&session, &first_admission_id);

    let second_session = session.clone();
    let second = tokio::spawn(async move { second_session.begin_setup_context_claim().await });
    tokio::task::yield_now().await;
    assert!(
        !second.is_finished(),
        "a second turn cannot observe/reuse context while the first owns its serial permit"
    );

    drop(first);
    let (released_result, released) =
        await_observed_setup_context_terminal_record(first_terminal_receipt, first_terminal_record)
            .await;
    assert_eq!(released_result, Ok(()));
    assert_eq!(released.status, SetupContextAdmissionStatus::Released);

    let second = second
        .await
        .unwrap()
        .unwrap()
        .expect("a durably released claim is redeliverable");
    assert_eq!(
        second.additional_context,
        vec![RuntimeString::from("DURABLE SETUP CONTEXT")]
    );
    second.admission.before_call().unwrap();
    second.admission.call_returned().await.unwrap();
    drop(second);

    let snapshot = session.setup_lifecycle_snapshot();
    assert_eq!(
        snapshot
            .context_admission
            .as_ref()
            .map(|admission| admission.status),
        Some(SetupContextAdmissionStatus::Consumed)
    );
    assert_eq!(
        snapshot.context_consumed_request_id,
        Some(RuntimeString::from("context-init"))
    );
    assert!(session.begin_setup_context_claim().await.unwrap().is_none());
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn released_setup_context_receipts_are_pruned_before_each_redelivery() {
    let session = setup_context_test_session(None).await;

    for _ in 0..80 {
        let claim = session
            .begin_setup_context_claim()
            .await
            .unwrap()
            .expect("released Setup context remains redeliverable");
        let admission_id = session
            .setup
            .durability
            .lock()
            .unwrap()
            .active_context_admission
            .as_ref()
            .expect("claim owns the active admission")
            .record
            .admission_id
            .clone();
        assert_eq!(
            session
                .setup
                .durability
                .lock()
                .unwrap()
                .context_admission_receipts
                .len(),
            1,
            "a new redelivery must discard its settled predecessor receipt"
        );
        drop(claim);
        let (result, terminal) = await_setup_context_terminal_record(&session, &admission_id).await;
        assert_eq!(result, Ok(()));
        assert_eq!(terminal.status, SetupContextAdmissionStatus::Released);
    }

    let claim = session
        .begin_setup_context_claim()
        .await
        .unwrap()
        .expect("the final claim consumes the retained context");
    claim.admission.before_call().unwrap();
    claim.admission.call_returned().await.unwrap();
    drop(claim);
    assert!(session.begin_setup_context_claim().await.unwrap().is_none());
    assert!(
        session
            .setup
            .durability
            .lock()
            .unwrap()
            .context_admission_receipts
            .is_empty(),
        "the settled consumed receipt has no process-local owner after the next claim probe"
    );
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn consumed_setup_context_receipt_remains_idempotent_until_provider_guard_drop() {
    let session = setup_context_test_session(None).await;
    let claim = session
        .begin_setup_context_claim()
        .await
        .unwrap()
        .expect("completed init has context");
    claim.admission.before_call().unwrap();
    claim.admission.call_returned().await.unwrap();

    assert!(session.begin_setup_context_claim().await.unwrap().is_none());
    assert_eq!(
        session
            .setup
            .durability
            .lock()
            .unwrap()
            .context_admission_receipts
            .len(),
        1,
        "a live provider guard keeps its settled receipt addressable"
    );
    claim
        .admission
        .before_call()
        .expect("provider admission retries stay idempotent while the guard is live");

    drop(claim);
    assert!(session.begin_setup_context_claim().await.unwrap().is_none());
    assert!(session
        .setup
        .durability
        .lock()
        .unwrap()
        .context_admission_receipts
        .is_empty());
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn provider_receipt_owns_terminal_record_after_guard_drop_and_map_prune() {
    let session = setup_context_test_session(None).await;
    let claim = session
        .begin_setup_context_claim()
        .await
        .unwrap()
        .expect("completed init has context");
    let admission_id = session
        .setup
        .durability
        .lock()
        .unwrap()
        .active_context_admission
        .as_ref()
        .unwrap()
        .record
        .admission_id
        .clone();
    claim.admission.before_call().unwrap();
    let receipt = claim.admission.call_returned();
    drop(claim);

    let (terminal_result, terminal) =
        await_setup_context_terminal_record(&session, &admission_id).await;
    assert_eq!(terminal_result, Ok(()));
    assert_eq!(terminal.status, SetupContextAdmissionStatus::Consumed);
    assert!(session.begin_setup_context_claim().await.unwrap().is_none());
    assert!(session
        .setup
        .durability
        .lock()
        .unwrap()
        .context_admission_receipts
        .is_empty());
    receipt
        .await
        .expect("an already-created receipt retains its terminal record independently");
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn cancelled_context_claim_before_started_flush_is_durably_released() {
    let rollout = ControlledSetupRollout::new(3);
    let rollout_sink: RolloutSink = rollout.clone();
    let session = setup_context_test_session(Some(rollout_sink)).await;

    let claiming_session = session.clone();
    let claim = tokio::spawn(async move { claiming_session.begin_setup_context_claim().await });
    rollout.wait_for_flush_calls(3).await;
    let admission_id = session
        .setup
        .durability
        .lock()
        .unwrap()
        .active_context_admission
        .as_ref()
        .unwrap()
        .record
        .admission_id
        .clone();
    claim.abort();
    assert!(matches!(claim.await, Err(error) if error.is_cancelled()));
    rollout.release_blocked_flush();

    let (result, terminal) = await_setup_context_terminal_record(&session, &admission_id).await;
    assert_eq!(result, Ok(()));
    assert_eq!(terminal.status, SetupContextAdmissionStatus::Released);
    assert!(session
        .setup_lifecycle_snapshot()
        .pending_init_context()
        .is_some());
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn setup_context_consumed_projection_waits_for_terminal_flush() {
    let rollout = ControlledSetupRollout::new(4);
    let rollout_sink: RolloutSink = rollout.clone();
    let session = setup_context_test_session(Some(rollout_sink)).await;
    let claim = session
        .begin_setup_context_claim()
        .await
        .unwrap()
        .expect("completed init has context");

    claim.admission.before_call().unwrap();
    let consumed = claim.admission.call_returned();
    rollout.wait_for_flush_calls(4).await;
    let pending = session.setup_lifecycle_snapshot();
    assert_eq!(
        pending
            .context_admission
            .as_ref()
            .map(|admission| admission.status),
        Some(SetupContextAdmissionStatus::Started),
        "an appended but unflushed terminal is not reconnect-visible"
    );
    assert_eq!(pending.context_consumed_request_id, None);
    assert_eq!(pending.context_consumed_at, None);

    rollout.release_blocked_flush();
    consumed.await.unwrap();
    let durable = session.setup_lifecycle_snapshot();
    assert_eq!(
        durable
            .context_admission
            .as_ref()
            .map(|admission| admission.status),
        Some(SetupContextAdmissionStatus::Consumed)
    );
    assert_eq!(
        durable.context_consumed_request_id,
        Some(RuntimeString::from("context-init"))
    );
    drop(claim);
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn setup_context_provider_panic_is_outcome_unknown_and_never_redelivered() {
    let session = setup_context_test_session(None).await;
    let claim = session
        .begin_setup_context_claim()
        .await
        .unwrap()
        .expect("completed init has context");
    claim.admission.before_call().unwrap();
    let error = claim.admission.call_panicked().await.unwrap_err();
    assert!(error.contains("outcome is unknown"));
    drop(claim);

    let snapshot = session.setup_lifecycle_snapshot();
    assert_eq!(
        snapshot
            .context_admission
            .as_ref()
            .map(|admission| admission.status),
        Some(SetupContextAdmissionStatus::OutcomeUnknown)
    );
    assert!(snapshot.pending_init_context().is_none());
    assert!(session.begin_setup_context_claim().await.unwrap().is_none());
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn setup_context_owner_panic_fails_closed_before_provider_admission() {
    let mut deps = mk_deps(Vec::new());
    deps.uuid = Arc::new(|| "setup-context-owner-panic".to_string());
    let session = setup_context_test_session_with_deps(deps, None).await;
    inject_setup_owned_task_panic(
        RuntimeString::from("setup-context-owner-panic"),
        SetupOwnedTaskKind::ContextAdmission,
    );
    let error = match session.begin_setup_context_claim().await {
        Err(error) => error,
        Ok(_) => panic!("a panicked context owner must not return a provider lease"),
    };
    assert!(error.contains("task panicked unexpectedly"));
    assert_eq!(
        session
            .setup_lifecycle_snapshot()
            .context_admission
            .as_ref()
            .map(|admission| admission.status),
        Some(SetupContextAdmissionStatus::PersistenceFailed)
    );
    let shutdown = session.shutdown_and_wait_detailed().await;
    assert!(shutdown.errors.iter().any(|error| {
        error.contains("ContextAdmission task panicked")
            || error.contains("context admission durability failed")
    }));
}

#[tokio::test]
async fn setup_context_started_append_or_flush_failure_blocks_provider_admission() {
    let cases = [
        ("append-panic", ControlledSetupRollout::panicking_append(3)),
        ("flush-failure", ControlledSetupRollout::failing_flush(3)),
    ];
    for (name, rollout) in cases {
        let rollout_sink: RolloutSink = rollout.clone();
        let session = setup_context_test_session(Some(rollout_sink)).await;
        let error = match session.begin_setup_context_claim().await {
            Err(error) => error,
            Ok(_) => panic!("Started durability failure must not return a provider lease"),
        };
        assert!(
            error.contains("failed to persist Setup context admission"),
            "{name}"
        );
        let snapshot = session.setup_lifecycle_snapshot();
        assert_eq!(
            snapshot
                .context_admission
                .as_ref()
                .map(|admission| admission.status),
            Some(SetupContextAdmissionStatus::PersistenceFailed),
            "{name}"
        );
        assert!(snapshot.pending_init_context().is_none(), "{name}");
        let shutdown = session.shutdown_and_wait_detailed().await;
        assert!(
            !shutdown.ok(),
            "{name}: the sticky durability error is reported"
        );
    }
}

#[tokio::test]
async fn setup_context_terminal_append_or_flush_failure_fails_provider_receipt() {
    let cases = [
        ("append-panic", ControlledSetupRollout::panicking_append(4)),
        ("flush-failure", ControlledSetupRollout::failing_flush(4)),
    ];
    for (name, rollout) in cases {
        let rollout_sink: RolloutSink = rollout.clone();
        let session = setup_context_test_session(Some(rollout_sink)).await;
        let claim = session
            .begin_setup_context_claim()
            .await
            .unwrap()
            .expect("Started was durable");
        claim.admission.before_call().unwrap();
        let error = claim.admission.call_returned().await.unwrap_err();
        assert!(
            error.contains("persistence failed")
                || error.contains("failed to persist Setup context admission"),
            "{name}: {error:?}"
        );
        drop(claim);
        assert_eq!(
            session
                .setup_lifecycle_snapshot()
                .context_admission
                .as_ref()
                .map(|admission| admission.status),
            Some(SetupContextAdmissionStatus::PersistenceFailed),
            "{name}"
        );
        assert!(session
            .setup_lifecycle_snapshot()
            .pending_init_context()
            .is_none());
        assert!(session.begin_setup_context_claim().await.unwrap().is_none());
        assert!(session
            .setup
            .durability
            .lock()
            .unwrap()
            .context_admission_receipts
            .values()
            .any(
                |receipt| receipt.terminal_record.as_ref().is_some_and(|record| {
                    record.status == SetupContextAdmissionStatus::PersistenceFailed
                })
            ));
        let shutdown = session.shutdown_and_wait_detailed().await;
        assert!(!shutdown.ok(), "{name}");
    }
}

#[tokio::test]
async fn recovered_started_context_is_outcome_unknown_after_its_recovery_flush() {
    let mut initial = SetupLifecycleSnapshot::default();
    initial.apply(&SetupLifecycleEvent::Run {
        run: SetupRunRecord {
            request_id: "recovered-context-init".into(),
            session_id: "setup-session".into(),
            trigger: SetupTrigger::Init,
            status: SetupRunStatus::Completed,
            started_at: 1,
            ended_at: Some(2),
            hook_count: 1,
            additional_context: vec!["RECOVERED CONTEXT".into()],
            system_messages: Vec::new(),
            error: None,
        },
    });
    initial.apply(&SetupLifecycleEvent::ContextAdmission {
        admission: SetupContextAdmissionRecord {
            admission_id: "recovered-admission".into(),
            request_id: "recovered-context-init".into(),
            session_id: "setup-session".into(),
            status: SetupContextAdmissionStatus::Started,
            started_at: 3,
            ended_at: None,
            error: None,
        },
    });
    let rollout = ControlledSetupRollout::new(1);
    let rollout_sink: RolloutSink = rollout.clone();
    let session = mk_setup_session(
        mk_deps(Vec::new()),
        Arc::new(HookBus::new(
            crate::runtime_process::test_runtime_process_context(),
            Vec::new(),
        )),
        Some(rollout_sink),
        Some(initial),
    );

    rollout.wait_for_flush_calls(1).await;
    assert_eq!(
        session
            .setup_lifecycle_snapshot()
            .context_admission
            .as_ref()
            .map(|admission| admission.status),
        Some(SetupContextAdmissionStatus::Started)
    );
    rollout.release_blocked_flush();
    session
        .await_pending_setup_recovery_receipts()
        .await
        .unwrap();
    let recovered = session.setup_lifecycle_snapshot();
    assert_eq!(
        recovered
            .context_admission
            .as_ref()
            .map(|admission| admission.status),
        Some(SetupContextAdmissionStatus::OutcomeUnknown)
    );
    assert!(recovered.pending_init_context().is_none());
    assert!(session.begin_setup_context_claim().await.unwrap().is_none());
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn shutdown_terminalizes_prepared_and_calling_setup_context_without_aborting_owner() {
    let prepared = setup_context_test_session(None).await;
    let prepared_claim = prepared
        .begin_setup_context_claim()
        .await
        .unwrap()
        .expect("prepared context claim");
    prepared.shutdown_and_wait().await.unwrap();
    assert_eq!(
        prepared
            .setup_lifecycle_snapshot()
            .context_admission
            .as_ref()
            .map(|admission| admission.status),
        Some(SetupContextAdmissionStatus::Released)
    );
    assert!(prepared_claim.admission.before_call().is_err());
    drop(prepared_claim);

    let calling = setup_context_test_session(None).await;
    let calling_claim = calling
        .begin_setup_context_claim()
        .await
        .unwrap()
        .expect("calling context claim");
    calling_claim.admission.before_call().unwrap();
    calling.shutdown_and_wait().await.unwrap();
    assert_eq!(
        calling
            .setup_lifecycle_snapshot()
            .context_admission
            .as_ref()
            .map(|admission| admission.status),
        Some(SetupContextAdmissionStatus::OutcomeUnknown)
    );
    assert!(calling_claim.admission.call_returned().await.is_err());
    drop(calling_claim);
}

#[tokio::test]
async fn blocked_first_prompt_keeps_setup_context_until_provider_admission() {
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let prompt_hook_calls = Arc::new(AtomicUsize::new(0));
    let bus = Arc::new(HookBus::new(
        crate::runtime_process::test_runtime_process_context(),
        vec![
            Hook {
                event: HookEvent::Setup,
                matcher: Some("init".to_string()),
                handler: crate::hooks::types::HookHandler::Fn(Arc::new(StaticSetupHook {
                    context: "CONTEXT AFTER BLOCK".into(),
                })),
                is_async: true,
            },
            Hook {
                event: HookEvent::UserPromptSubmit,
                matcher: None,
                handler: crate::hooks::types::HookHandler::Fn(Arc::new(BlockFirstPromptHook {
                    calls: prompt_hook_calls.clone(),
                })),
                is_async: false,
            },
        ],
    ));
    let session = mk_setup_session(
        recording_deps(requests.clone(), vec![text_turn("allowed")]),
        bus,
        None,
        None,
    );
    let setup = session
        .run_setup_hooks(SetupRunRequest {
            request_id: "init-before-block".into(),
            trigger: SetupTrigger::Init,
        })
        .await;
    assert_eq!(setup.status, SetupRunStatus::Completed);
    assert_eq!(setup.additional_context, vec!["CONTEXT AFTER BLOCK"]);

    session
        .run(Input::Text("blocked".into()), TurnOptions::default())
        .await;
    assert!(requests.lock().unwrap().is_empty());
    assert_eq!(
        session
            .setup_lifecycle_snapshot()
            .context_consumed_request_id,
        None
    );

    session
        .run(Input::Text("allowed".into()), TurnOptions::default())
        .await;
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].messages.iter().any(|message| matches!(
        message,
        Message::User(user)
            if user.is_meta == Some(true)
                && user.content.iter().any(|block| matches!(
                    block,
                    ContentBlock::Text(text) if text.text == "CONTEXT AFTER BLOCK"
                ))
    )));
    drop(requests);
    assert_eq!(prompt_hook_calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        session
            .setup_lifecycle_snapshot()
            .context_consumed_request_id,
        Some("init-before-block".into())
    );
}

#[tokio::test]
async fn init_context_reaches_only_the_first_model_prompt_and_run_closes_init() {
    let requests = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let bus = setup_bus(Hook {
        event: HookEvent::Setup,
        matcher: Some("init".to_string()),
        handler: crate::hooks::types::HookHandler::Fn(Arc::new(StaticSetupHook {
            context: "FIRST PROMPT ONLY".into(),
        })),
        is_async: true,
    });
    let session = mk_setup_session(
        recording_deps(requests.clone(), vec![text_turn("one"), text_turn("two")]),
        bus,
        None,
        None,
    );
    assert_eq!(
        session
            .run_setup_hooks(SetupRunRequest {
                request_id: "init".into(),
                trigger: SetupTrigger::Init,
            })
            .await
            .status,
        SetupRunStatus::Completed
    );

    session
        .run(Input::Text("first".into()), TurnOptions::default())
        .await;
    session
        .run(Input::Text("second".into()), TurnOptions::default())
        .await;

    {
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        let setup_parts = |request: &ModelRequest| {
            request
                .messages
                .iter()
                .filter_map(|message| match message {
                    Message::User(user) if user.is_meta == Some(true) => Some(&user.content),
                    _ => None,
                })
                .flatten()
                .filter_map(|block| match block {
                    ContentBlock::Text(text) if text.text == "FIRST PROMPT ONLY" => {
                        Some(text.text.clone())
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(setup_parts(&requests[0]), vec!["FIRST PROMPT ONLY"]);
        assert!(setup_parts(&requests[1]).is_empty());
    }

    let snapshot = session.setup_lifecycle_snapshot();
    assert!(snapshot.init_closed_at.is_some());
    assert_eq!(snapshot.context_consumed_request_id, Some("init".into()));
    assert_eq!(
        session
            .run_setup_hooks(SetupRunRequest {
                request_id: "late-init".into(),
                trigger: SetupTrigger::Init,
            })
            .await
            .status,
        SetupRunStatus::TooLate
    );
}

#[tokio::test]
async fn setup_success_and_context_consumption_round_trip_through_rollout() {
    let dir = temp_store_dir();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let rollout: RolloutSink = Arc::from(
        store
            .create(SessionMeta {
                id: "setup-session".into(),
                created_at: 1,
                cwd: runtime_path_from_host_path(&dir).into_runtime_string(),
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
            })
            .await
            .unwrap(),
    );
    let bus = setup_bus(Hook {
        event: HookEvent::Setup,
        matcher: Some("init".to_string()),
        handler: crate::hooks::types::HookHandler::Fn(Arc::new(StaticSetupHook {
            context: "PERSISTED CONTEXT".into(),
        })),
        is_async: true,
    });
    let session = mk_setup_session(mk_deps(vec![text_turn("done")]), bus, Some(rollout), None);
    let setup = session
        .run_setup_hooks(SetupRunRequest {
            request_id: "persisted-init".into(),
            trigger: SetupTrigger::Init,
        })
        .await;
    assert_eq!(setup.status, SetupRunStatus::Completed);
    let persisted_before_reply_boundary = store.resume("setup-session").await.unwrap();
    let replayed_before_run = replay_setup_lifecycle(&persisted_before_reply_boundary.raw_items);
    assert_eq!(
        replayed_before_run
            .completed_init()
            .unwrap()
            .additional_context,
        vec!["PERSISTED CONTEXT"]
    );

    session
        .run(Input::Text("go".into()), TurnOptions::default())
        .await;
    let persisted_after_provider = store.resume("setup-session").await.unwrap();
    let replayed_after_provider = replay_setup_lifecycle(&persisted_after_provider.raw_items);
    assert!(replayed_after_provider.init_closed_at.is_some());
    assert_eq!(
        replayed_after_provider.context_consumed_request_id,
        Some("persisted-init".into())
    );
    session.shutdown_and_wait().await.unwrap();

    let resumed = store.resume("setup-session").await.unwrap();
    let replayed = replay_setup_lifecycle(&resumed.raw_items);
    assert_eq!(
        replayed.completed_init().unwrap().additional_context,
        vec!["PERSISTED CONTEXT"]
    );
    assert!(replayed.init_closed_at.is_some());
    assert_eq!(
        replayed.context_consumed_request_id,
        Some("persisted-init".into())
    );
    assert!(replayed.pending_init_context().is_none());
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn setup_maintenance_idempotency_survives_retention_and_cold_rollout_replay() {
    let dir = temp_store_dir();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let rollout: RolloutSink = Arc::from(
        store
            .create(SessionMeta {
                id: "setup-session".into(),
                created_at: 1,
                cwd: runtime_path_from_host_path(&dir).into_runtime_string(),
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
            })
            .await
            .unwrap(),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let bus = setup_bus(Hook {
        event: HookEvent::Setup,
        matcher: None,
        handler: crate::hooks::types::HookHandler::Fn(Arc::new(SequencedSetupHook {
            calls: calls.clone(),
        })),
        is_async: true,
    });
    let session = mk_setup_session(mk_deps(Vec::new()), bus, Some(rollout), None);

    let mut first_result = None;
    for index in 0..66 {
        let result = session
            .run_setup_hooks(SetupRunRequest {
                request_id: RuntimeString::from(format!("maintenance-{index}")),
                trigger: SetupTrigger::Maintenance,
            })
            .await;
        assert_eq!(result.status, SetupRunStatus::Completed);
        if index == 0 {
            first_result = Some(result);
        }
    }
    let first_result = first_result.expect("first maintenance result");
    assert_eq!(calls.load(Ordering::SeqCst), 66);
    let live_snapshot = session.setup_lifecycle_snapshot();
    assert_eq!(
        live_snapshot
            .runs
            .iter()
            .filter(|run| run.trigger == SetupTrigger::Maintenance)
            .count(),
        64,
        "the full lifecycle audit remains bounded"
    );
    assert!(
        live_snapshot
            .run(&RuntimeString::from("maintenance-0"))
            .is_none(),
        "the oldest full maintenance record must actually be evicted"
    );
    assert_eq!(
        live_snapshot.request_result(&RuntimeString::from("maintenance-0")),
        Some(first_result.clone())
    );
    assert_eq!(
        session
            .run_setup_hooks(SetupRunRequest {
                request_id: "maintenance-0".into(),
                trigger: SetupTrigger::Maintenance,
            })
            .await,
        first_result,
        "a live replay after eviction must return the exact original result"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 66);

    session.shutdown_and_wait().await.unwrap();
    let resumed = store.resume("setup-session").await.unwrap();
    let recovered = replay_setup_lifecycle(&resumed.raw_items);
    assert_eq!(
        recovered
            .runs
            .iter()
            .filter(|run| run.trigger == SetupTrigger::Maintenance)
            .count(),
        64
    );
    assert_eq!(
        recovered.request_result(&RuntimeString::from("maintenance-0")),
        Some(first_result.clone()),
        "cold rollout replay must rebuild the exact result receipt"
    );

    let recovered_calls = Arc::new(AtomicUsize::new(0));
    let recovered_bus = setup_bus(Hook {
        event: HookEvent::Setup,
        matcher: None,
        handler: crate::hooks::types::HookHandler::Fn(Arc::new(SequencedSetupHook {
            calls: recovered_calls.clone(),
        })),
        is_async: true,
    });
    let recovered_session =
        mk_setup_session(mk_deps(Vec::new()), recovered_bus, None, Some(recovered));
    assert_eq!(
        recovered_session
            .run_setup_hooks(SetupRunRequest {
                request_id: "maintenance-0".into(),
                trigger: SetupTrigger::Maintenance,
            })
            .await,
        first_result,
        "the old request must remain an exact replay after process recovery"
    );
    let conflict = recovered_session
        .run_setup_hooks(SetupRunRequest {
            request_id: "maintenance-0".into(),
            trigger: SetupTrigger::Init,
        })
        .await;
    assert_eq!(conflict.status, SetupRunStatus::RequestConflict);
    assert_eq!(conflict.source_request_id, Some("maintenance-0".into()));
    assert_eq!(
        recovered_calls.load(Ordering::SeqCst),
        0,
        "neither an idempotent replay nor a conflicting request may execute the hook"
    );

    recovered_session.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(dir).ok();
}

struct ConfiguredInfoProvider {
    info: ProviderInfo,
}

#[async_trait::async_trait]
impl ModelProvider for ConfiguredInfoProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _model: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: true,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: true,
            context_window: 200_000,
            max_output_tokens: 8_192,
            native_pdf: true,
            vision: true,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn stream(&self, _req: ModelRequest, _signal: AbortSignal) -> ModelStream {
        panic!("ConfiguredInfoProvider.stream should not be called")
    }
}

#[tokio::test]
async fn session_passes_provider_info_to_query() {
    let requests: Arc<Mutex<Vec<ModelRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let provider: Arc<dyn ModelProvider> = Arc::new(ConfiguredInfoProvider {
        info: ProviderInfo {
            name: "anthropic-proxy".into(),
            base_url: None,
            env_key: None,
            default_model: "model-x".into(),
        },
    });
    let mut config = default_config();
    config.model = "model-x".into();
    let session = mk_session_with_provider_and_memory(
        provider,
        recording_deps(requests.clone(), vec![text_turn("done")]),
        config,
        Vec::new(),
        None,
    );

    session
        .run(Input::Text("hello".into()), TurnOptions::default())
        .await;

    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let behavior = requests[0]
        .system
        .iter()
        .find(|part| part.name == "provider-behavior:anthropic-reminder")
        .expect("Session should pass configured provider info into Query projection");
    assert!(behavior
        .text
        .starts_with("<system-reminder>\nHarness guidance:"));
}

#[tokio::test]
async fn run_preserves_lone_surrogates_across_input_and_final_result() {
    let input = RuntimeString::from_utf16([0xdc00]);
    let output = RuntimeString::from_utf16([0xd800]);
    let requests: Arc<Mutex<Vec<ModelRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let session = mk_session_with_deps(
        recording_deps(
            requests.clone(),
            vec![vec![
                ev::message_start(None),
                crate::types::provider::ModelStreamEvent::TextDelta {
                    text: output.clone(),
                },
                ev::stop(Some(ModelStopReason::EndTurn), Some(make_usage(None, None))),
            ]],
        ),
        default_config(),
        Vec::new(),
    );

    let result = session
        .run(Input::Text(input.clone()), TurnOptions::default())
        .await;

    assert_eq!(result.final_text, output);
    assert_ne!(result.final_text, RuntimeString::from("\u{fffd}"));
    let requests = requests.lock().unwrap();
    let Message::User(message) = requests[0].messages.last().expect("user message") else {
        panic!("expected user message");
    };
    assert_eq!(
        message.content,
        vec![ContentBlock::Text(TextBlock {
            text: input,
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })]
    );
}

/// A ceiling no default would land on by accident, so a test can tell "followed the
/// model" apart from "used a fixed default".
const SELECTIVE_PROVIDER_MAX_OUTPUT: u64 = 111_000;

struct SelectivePdfProvider {
    info: ProviderInfo,
}

impl SelectivePdfProvider {
    fn new() -> Self {
        Self {
            info: ProviderInfo {
                name: "selective-pdf".into(),
                base_url: None,
                env_key: None,
                default_model: "pdf-model".into(),
            },
        }
    }
}

#[async_trait::async_trait]
impl ModelProvider for SelectivePdfProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: true,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: true,
            context_window: 200_000,
            max_output_tokens: SELECTIVE_PROVIDER_MAX_OUTPUT,
            native_pdf: model != "text-only-model",
            vision: model != "text-only-model",
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn stream(&self, _req: ModelRequest, _signal: AbortSignal) -> ModelStream {
        panic!("SelectivePdfProvider.stream should not be called")
    }
}

fn pdf_part(bytes: &[u8], mime_type: &str, filename: Option<&str>) -> UserContentPart {
    UserContentPart::Document {
        base64: base64::engine::general_purpose::STANDARD
            .encode(bytes)
            .into(),
        mime_type: mime_type.into(),
        filename: filename.map(Into::into),
    }
}

#[test]
fn document_filename_normalization_preserves_lone_surrogate() {
    let filename = RuntimeString::from_utf16([
        b'f' as u16,
        b'o' as u16,
        b'l' as u16,
        b'd' as u16,
        b'e' as u16,
        b'r' as u16,
        b'/' as u16,
        b'e' as u16,
        0x0301,
        0xd800,
        b'.' as u16,
        b'p' as u16,
        b'd' as u16,
        b'f' as u16,
    ]);

    let normalized = normalize_document_filename(&filename).unwrap();

    assert_eq!(
        normalized,
        RuntimeString::from_utf16([
            0x00e9,
            0xd800,
            b'.' as u16,
            b'p' as u16,
            b'd' as u16,
            b'f' as u16
        ])
    );
}

fn assert_input_error(
    session: &Session,
    input: &Input,
    opts: &TurnOptions,
    code: crate::types::errors::InputValidationErrorCode,
) {
    let error = session
        .validate_input_for_turn(input.clone(), opts)
        .expect_err("input should fail validation");
    assert_eq!(error.detail.code, code);
    assert!(!error.message.contains("JVBER"));
}

#[tokio::test]
async fn direct_pdf_input_becomes_native_document() {
    let requests: Arc<Mutex<Vec<ModelRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let session = mk_session_with_deps(
        recording_deps(requests.clone(), vec![text_turn("ok")]),
        default_config(),
        Vec::new(),
    );
    let input = Input::Parts(vec![
        UserContentPart::Text {
            text: "summarize this".into(),
        },
        pdf_part(
            b"%PDF-1.7\n",
            "APPLICATION/PDF",
            Some("C:\\reports/quarterly/e\u{301}.pdf"),
        ),
    ]);

    let result = session.run(input, TurnOptions::default()).await;

    assert_eq!(result.stop_reason, TerminalReason::Completed);
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let Message::User(user) = requests[0].messages.last().expect("user message") else {
        panic!("expected user message");
    };
    assert_eq!(
        user.content,
        vec![
            ContentBlock::Text(TextBlock {
                text: "summarize this".into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            }),
            ContentBlock::Document(crate::schemas::content::DocumentBlock {
                mime_type: "application/pdf".into(),
                base64: "JVBERi0xLjcK".into(),
                filename: Some("é.pdf".into()),
            }),
        ]
    );
}

#[tokio::test]
async fn direct_pdf_input_rejects_invalid_documents_without_persisting() {
    use crate::types::errors::InputValidationErrorCode;

    let cases = [
        (
            Input::Parts(vec![pdf_part(
                b"%PDF-1.7\n",
                "application/msword",
                Some("report.pdf"),
            )]),
            InputValidationErrorCode::InvalidDocumentMime,
        ),
        (
            Input::Parts(vec![UserContentPart::Document {
                base64: "%%%not-base64%%%".into(),
                mime_type: "application/pdf".into(),
                filename: Some("report.pdf".into()),
            }]),
            InputValidationErrorCode::InvalidDocumentBase64,
        ),
        (
            Input::Parts(vec![pdf_part(
                b"not a pdf",
                "application/pdf",
                Some("report.pdf"),
            )]),
            InputValidationErrorCode::InvalidDocumentSignature,
        ),
        (
            Input::Parts(vec![pdf_part(
                b"%PDF-1.7\n",
                "application/pdf",
                Some(&format!("{}.pdf", "x".repeat(252))),
            )]),
            InputValidationErrorCode::InvalidDocumentFilename,
        ),
        (
            Input::Parts(vec![pdf_part(
                b"%PDF-1.7\n",
                "application/pdf",
                Some("C:report.pdf"),
            )]),
            InputValidationErrorCode::InvalidDocumentFilename,
        ),
        (
            Input::Parts(vec![pdf_part(
                b"%PDF-1.7\n",
                "application/pdf",
                Some("report\u{202e}fdp.exe"),
            )]),
            InputValidationErrorCode::InvalidDocumentFilename,
        ),
        (
            Input::Parts(vec![pdf_part(
                b"%PDF-1.7\n",
                "application/pdf",
                Some("report.pdf. "),
            )]),
            InputValidationErrorCode::InvalidDocumentFilename,
        ),
    ];

    for (input, code) in cases {
        let requests: Arc<Mutex<Vec<ModelRequest>>> = Arc::new(Mutex::new(Vec::new()));
        let session = mk_session_with_deps(
            recording_deps(requests.clone(), vec![text_turn("must not run")]),
            default_config(),
            Vec::new(),
        );
        assert_input_error(&session, &input, &TurnOptions::default(), code);

        let result = session.run(input, TurnOptions::default()).await;

        assert_eq!(result.stop_reason, TerminalReason::Error);
        assert_eq!(
            result.error.as_ref().map(|error| error.code),
            Some(EngineErrorCode::InvalidInput)
        );
        assert!(session.history().is_empty());
        assert!(requests.lock().unwrap().is_empty());
    }
}

#[test]
fn direct_pdf_input_enforces_aggregate_decoded_size_limit() {
    use crate::types::errors::InputValidationErrorCode;

    let mut first = vec![b'a'; 10 * 1024 * 1024 + 1];
    first[..5].copy_from_slice(b"%PDF-");
    let mut second = first.clone();
    second[5] = b'2';
    let input = Input::Parts(vec![
        pdf_part(&first, "application/pdf", Some("one.pdf")),
        pdf_part(&second, "application/pdf", Some("two.pdf")),
    ]);
    let session = mk_session(vec![]);

    assert_input_error(
        &session,
        &input,
        &TurnOptions::default(),
        InputValidationErrorCode::DocumentTooLarge,
    );
}

#[test]
fn direct_pdf_input_caps_document_part_count() {
    use crate::types::errors::InputValidationErrorCode;

    let input = Input::Parts(
        (0..33)
            .map(|index| {
                pdf_part(
                    b"%PDF-1.7\n",
                    "application/pdf",
                    Some(&format!("{index}.pdf")),
                )
            })
            .collect(),
    );
    let session = mk_session(vec![]);

    assert_input_error(
        &session,
        &input,
        &TurnOptions::default(),
        InputValidationErrorCode::TooManyDocuments,
    );
}

/// The first request already asks for everything the model will give. The budget
/// used to start at a fixed 8K and only grow by re-asking after a truncation, which
/// spent extra responses to reach what one response could have carried.
#[tokio::test]
async fn the_request_output_budget_is_the_models_own_ceiling() {
    let requests: Arc<Mutex<Vec<ModelRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let session = mk_session_with_provider_and_memory(
        Arc::new(SelectivePdfProvider::new()),
        recording_deps(requests.clone(), vec![text_turn("ok")]),
        default_config(),
        Vec::new(),
        None,
    );

    session
        .run(Input::Text("hi".into()), TurnOptions::default())
        .await;

    let sent = requests.lock().unwrap();
    assert_eq!(
        sent[0].max_output_tokens, SELECTIVE_PROVIDER_MAX_OUTPUT,
        "the budget follows the model's declared ceiling, not a fixed default"
    );
}

/// A call whose arguments never finished arriving is answered as a failure instead
/// of being run: what arrived is a prefix of the JSON the model meant to send, and
/// an edit or a command assembled from a prefix is worse than a call that did not
/// run. The model is told, so it reissues the call.
#[tokio::test]
async fn a_tool_call_cut_off_mid_arguments_is_dropped_not_executed() {
    /// Records whether it ever ran, which is the whole point of the assertion.
    struct RanFlagTool {
        ran: Arc<AtomicBool>,
        schema: JsonSchema,
    }

    #[async_trait::async_trait]
    impl Tool for RanFlagTool {
        fn static_name(&self) -> &str {
            "edit"
        }
        fn static_description(&self) -> &str {
            "test edits"
        }
        fn static_input_schema(&self) -> &JsonSchema {
            &self.schema
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
            _input: Json,
            _ctx: &ToolContext,
            _on_progress: Option<ToolProgress>,
        ) -> Result<ToolResult, crate::types::errors::ToolError> {
            self.ran.store(true, Ordering::SeqCst);
            Ok(text_result(Json::Null, "edited", false))
        }
    }

    let ran = Arc::new(AtomicBool::new(false));
    // The stream ends after a partial argument delta: no ToolUseStop ever arrives,
    // which is what a response cut off mid-call looks like on the wire.
    let truncated_turn = vec![
        ev::message_start(None),
        crate::types::provider::ModelStreamEvent::ToolUseStart {
            id: "edit-cut".into(),
            name: "edit".into(),
            thought_signature: None,
            tool_kind: None,
            input_format: None,
        },
        crate::types::provider::ModelStreamEvent::ToolUseInputDelta {
            id: "edit-cut".into(),
            partial_json: "{\"path\":\"src/n".into(),
        },
        ev::stop(Some(ModelStopReason::MaxTokens), None),
    ];
    let session = mk_session_with_provider_and_memory(
        Arc::new(fake_provider()),
        mk_deps(vec![truncated_turn, text_turn("reissued")]),
        default_config(),
        vec![Arc::new(RanFlagTool {
            ran: ran.clone(),
            schema: JsonSchema::new(),
        })],
        None,
    );

    session
        .run(Input::Text("edit it".into()), TurnOptions::default())
        .await;

    assert!(
        !ran.load(Ordering::SeqCst),
        "a call whose arguments were cut off must not run"
    );
    let results: Vec<String> = session
        .history()
        .into_iter()
        .filter_map(|message| match message {
            Message::ToolResult(result) => Some(
                result
                    .content
                    .iter()
                    .filter_map(|block| match block {
                        crate::schemas::content::OutputBlock::Text(text) => {
                            text.text.try_to_utf8().ok()
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("\n"),
            ),
            _ => None,
        })
        .collect();
    assert_eq!(results.len(), 1, "the one call answered once: {results:?}");
    assert!(
        results[0].contains("dropped") && results[0].contains("arrived incomplete"),
        "the model is told why the call did not run: {results:?}"
    );
}

#[tokio::test]
async fn direct_pdf_input_rejects_models_without_native_pdf_support() {
    use crate::types::errors::InputValidationErrorCode;

    let requests: Arc<Mutex<Vec<ModelRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let session = mk_session_with_provider_and_memory(
        Arc::new(SelectivePdfProvider::new()),
        recording_deps(requests.clone(), vec![text_turn("must not run")]),
        default_config(),
        Vec::new(),
        None,
    );
    let input = Input::Parts(vec![UserContentPart::Document {
        base64: "not-base64".repeat(1024).into(),
        mime_type: "application/pdf".into(),
        filename: Some("report.pdf".into()),
    }]);
    let opts = TurnOptions {
        model: Some("text-only-model".into()),
        ..Default::default()
    };
    let error = session
        .validate_input_for_turn(input.clone(), &opts)
        .expect_err("text-only model should reject PDF input");
    assert_eq!(
        error.detail.code,
        InputValidationErrorCode::UnsupportedInput
    );
    assert_eq!(error.detail.feature, Some("native_pdf".into()));
    assert_eq!(error.detail.provider, Some("selective-pdf".into()));
    assert_eq!(error.detail.model, Some("text-only-model".into()));

    let result = session.run(input, opts).await;

    assert_eq!(result.stop_reason, TerminalReason::Error);
    assert!(session.history().is_empty());
    assert!(requests.lock().unwrap().is_empty());
}

/// Image capability limits apply to requests, while admission and history keep
/// the original attachment for transcript previews and later model selections.
#[tokio::test]
async fn direct_image_input_is_retained_for_models_without_vision_support() {
    let requests: Arc<Mutex<Vec<ModelRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let session = mk_session_with_provider_and_memory(
        Arc::new(SelectivePdfProvider::new()),
        recording_deps(requests.clone(), vec![text_turn("answered the text")]),
        default_config(),
        Vec::new(),
        None,
    );
    let input = Input::Parts(vec![
        UserContentPart::Text {
            text: "what is this?".into(),
        },
        UserContentPart::Image {
            base64: "QUJD".into(),
            mime_type: "image/png".into(),
            source_path: None,
        },
    ]);
    let opts = TurnOptions {
        model: Some("text-only-model".into()),
        ..Default::default()
    };
    let validated = session
        .validate_input_for_turn(input.clone(), &opts)
        .expect("a nonvisual model still admits the user's original input");
    assert_eq!(validated.input, input);
    assert!(validated.has_images);
    let original_content = super::to_content_blocks(&input);

    let result = session.run(input, opts).await;

    assert_eq!(result.stop_reason, TerminalReason::Completed);
    assert!(session.history().iter().any(|message| {
        matches!(message, Message::User(user) if user.content == original_content)
    }));
    let sent = requests.lock().unwrap();
    assert_eq!(sent.len(), 1, "the turn runs");
    let content = sent[0]
        .messages
        .iter()
        .filter_map(|message| match message {
            Message::User(user) => Some(&user.content),
            _ => None,
        })
        .flatten()
        .collect::<Vec<_>>();
    assert!(!content
        .iter()
        .any(|block| matches!(block, ContentBlock::Image(_))));
    let text = content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text(text) => text.text.try_to_utf8().ok(),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(text.contains("what is this?"));
    assert!(text.contains("cannot understand images") && text.contains("Tell the user"));
}

#[tokio::test]
async fn direct_image_input_passes_models_with_vision_support() {
    let requests: Arc<Mutex<Vec<ModelRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let session = mk_session_with_provider_and_memory(
        Arc::new(SelectivePdfProvider::new()),
        recording_deps(requests.clone(), vec![text_turn("saw it")]),
        default_config(),
        Vec::new(),
        None,
    );
    let input = Input::Parts(vec![
        UserContentPart::Text {
            text: "what is this?".into(),
        },
        UserContentPart::Image {
            base64: "QUJD".into(),
            mime_type: "image/png".into(),
            source_path: None,
        },
    ]);

    let result = session.run(input, TurnOptions::default()).await;

    assert_eq!(result.final_text, RuntimeString::from("saw it"));
    assert_eq!(requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn direct_pdf_steer_uses_active_model_and_never_queues_rejected_input() {
    use crate::engine::message_queue::QueuedValue;
    use crate::types::errors::InputValidationErrorCode;

    let requests: Arc<Mutex<Vec<ModelRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let session = mk_session_with_provider_and_memory(
        Arc::new(SelectivePdfProvider::new()),
        recording_deps(requests, vec![text_turn("unused")]),
        default_config(),
        Vec::new(),
        None,
    );
    let pdf = Input::Parts(vec![pdf_part(
        b"%PDF-1.7\n",
        "application/pdf",
        Some(r"folder\steer.pdf"),
    )]);

    session
        .enqueue(pdf.clone(), EnqueueOptions::default())
        .expect("default model supports PDF input");
    let queued = session.take_pending_main_messages().await.unwrap();
    assert_eq!(queued.len(), 1);
    assert_eq!(
        queued[0].value,
        QueuedValue::Blocks(vec![ContentBlock::Document(
            crate::schemas::content::DocumentBlock {
                mime_type: "application/pdf".into(),
                base64: "JVBERi0xLjcK".into(),
                filename: Some("steer.pdf".into()),
            }
        )])
    );

    let _unconsumed_turn = session.run_streamed(
        Input::Text("hold".into()),
        TurnOptions {
            model: Some("text-only-model".into()),
            ..Default::default()
        },
    );
    let error = session
        .enqueue(pdf, EnqueueOptions::default())
        .expect_err("active text-only model should reject PDF steer");
    assert_eq!(
        error.detail.code,
        InputValidationErrorCode::UnsupportedInput
    );
    assert!(session.queued_messages().is_empty());
}

#[tokio::test]
async fn queue_durability_poison_rejects_new_enqueue_with_stable_code() {
    use crate::engine::message_queue::QueueSnapshotStatus;

    let session = mk_session(vec![]);
    session
        .conversation
        .queue
        .set_persistence_sink(Arc::new(|_| {
            Box::pin(async { Err(RuntimeString::from("forced queue checkpoint failure")) })
        }));

    session
        .enqueue(
            Input::Text("accepted before receipt failure".into()),
            EnqueueOptions::default(),
        )
        .expect("the first enqueue owns an asynchronous durability receipt");
    assert_eq!(
        session.await_queue_durability().await,
        Err(RuntimeString::from("forced queue checkpoint failure"))
    );

    let error = session
        .enqueue(
            Input::Text("must not enter a poisoned queue".into()),
            EnqueueOptions::default(),
        )
        .expect_err("a poisoned queue must fail closed before mutation");
    assert_eq!(
        error.detail.code,
        crate::types::errors::InputValidationErrorCode::QueueUnavailable
    );
    assert!(error.message.contains("forced queue checkpoint failure"));
    let snapshot = session.queue_snapshot();
    assert_eq!(snapshot.status, QueueSnapshotStatus::PersistenceFailed);
    assert_eq!(snapshot.revision, 0);
    assert!(snapshot.items.is_empty());

    let shutdown_error = session.shutdown_and_wait().await.unwrap_err();
    assert!(shutdown_error.contains("forced queue checkpoint failure"));
}

#[tokio::test]
async fn failed_retained_queue_mutation_interrupts_the_active_turn() {
    use crate::engine::message_queue::QueueMutationStatus;

    let session = mk_session(vec![]);
    session
        .enqueue(Input::Text("edit me".into()), EnqueueOptions::default())
        .unwrap();
    let initial = session.queue_snapshot();
    let uuid = initial.items[0].message.uuid.clone().unwrap();
    session
        .conversation
        .queue
        .set_persistence_sink(Arc::new(|_| {
            Box::pin(async { Err(RuntimeString::from("forced mutation receipt failure")) })
        }));
    let signal = CancellationToken::new();
    let lease = ActiveTurnLease::try_new(
        session.clone(),
        "queue-failure-model".into(),
        signal.clone(),
    )
    .expect("test turn is admitted");

    let result = session
        .edit_queued_message(
            "failed-edit".into(),
            uuid,
            initial.revision,
            QueueEdit {
                value: Some(QueuedValue::Text("speculative edit".into())),
                ..Default::default()
            },
        )
        .await;

    assert_eq!(result.status, QueueMutationStatus::OutcomeUnknown);
    assert_eq!(
        result.error,
        Some(RuntimeString::from("forced mutation receipt failure"))
    );
    assert!(signal.is_cancelled());

    drop(lease);
    let shutdown_error = session.shutdown_and_wait().await.unwrap_err();
    assert!(shutdown_error.contains("forced mutation receipt failure"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_waits_for_retained_queue_persistence_owners() {
    let session = mk_session(vec![]);
    let receipt_entered = Arc::new(AtomicBool::new(false));
    let release_receipt = Arc::new(tokio::sync::Notify::new());
    let entered_for_sink = receipt_entered.clone();
    let release_for_sink = release_receipt.clone();
    session
        .conversation
        .queue
        .set_persistence_sink(Arc::new(move |_| {
            let entered = entered_for_sink.clone();
            let release = release_for_sink.clone();
            Box::pin(async move {
                entered.store(true, Ordering::SeqCst);
                release.notified().await;
                Ok(())
            })
        }));
    session
        .enqueue(
            Input::Text("durable before shutdown".into()),
            EnqueueOptions::default(),
        )
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !receipt_entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("queue persistence receipt started");

    let shutdown_session = session.clone();
    let shutdown = tokio::spawn(async move { shutdown_session.shutdown_and_wait().await });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !session.lifecycle.is_disposed() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("shutdown closed public turn admission");
    assert!(!shutdown.is_finished());
    let error = session
        .enqueue(
            Input::Text("must be rejected after shutdown starts".into()),
            EnqueueOptions::default(),
        )
        .expect_err("shutdown must reject a late public steering producer");
    assert_eq!(
        error.detail.code,
        crate::types::errors::InputValidationErrorCode::QueueUnavailable
    );

    release_receipt.notify_one();
    shutdown.await.unwrap().unwrap();
    assert_eq!(session.queue_snapshot().items.len(), 1);
}

#[tokio::test]
async fn joined_queue_persistence_error_still_gets_final_rollout_boundary() {
    let rollout = ControlledQueueShutdownRollout::failing(1);
    let rollout_sink: RolloutSink = rollout.clone();
    let session = queue_shutdown_test_session(rollout_sink);
    session
        .enqueue(
            Input::Text("checkpoint will fail".into()),
            EnqueueOptions::default(),
        )
        .unwrap();
    assert_eq!(
        session.await_queue_durability().await,
        Err(RuntimeString::from(
            "injected queue rollout flush failure at call 1"
        ))
    );

    let outcome = session.shutdown_and_wait_detailed().await;
    assert!(!outcome.ok());
    assert!(!outcome.timed_out);
    assert!(outcome.errors.iter().any(|error| {
        error.contains("queue persistence drain failed")
            && error.contains("injected queue rollout flush failure at call 1")
    }));
    assert_eq!(rollout.queue_append_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        rollout.flush_calls.load(Ordering::SeqCst),
        2,
        "the joined queue owner is followed by the rollout's final flush"
    );
    assert_eq!(rollout.shutdown_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn queue_owner_released_before_deadline_precedes_final_rollout_boundary() {
    let rollout = ControlledQueueShutdownRollout::blocking(1);
    let rollout_sink: RolloutSink = rollout.clone();
    let session = queue_shutdown_test_session(rollout_sink);
    session
        .enqueue(
            Input::Text("release before queue fence".into()),
            EnqueueOptions::default(),
        )
        .unwrap();
    rollout.wait_for_flush_calls(1).await;

    let closing = session.clone();
    let shutdown = tokio::spawn(async move { closing.shutdown_and_wait_detailed().await });
    wait_for_shutdown_deadlines_to_start(&session).await;
    tokio::time::advance(std::time::Duration::from_millis(
        SESSION_SHUTDOWN_FLUSH_MS - 1,
    ))
    .await;
    rollout.release_blocked_flush();
    session.await_queue_durability().await.unwrap();

    let outcome = shutdown.await.unwrap();
    assert!(outcome.ok(), "{outcome:?}");
    assert_eq!(rollout.queue_append_calls.load(Ordering::SeqCst), 1);
    assert_eq!(rollout.flush_calls.load(Ordering::SeqCst), 2);
    assert_eq!(rollout.shutdown_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn queue_owner_past_deadline_skips_final_rollout_flush_and_shutdown() {
    let rollout = ControlledQueueShutdownRollout::blocking(1);
    let rollout_sink: RolloutSink = rollout.clone();
    let session = queue_shutdown_test_session(rollout_sink);
    session
        .enqueue(
            Input::Text("outlives queue fence".into()),
            EnqueueOptions::default(),
        )
        .unwrap();
    rollout.wait_for_flush_calls(1).await;

    let closing = session.clone();
    let shutdown = tokio::spawn(async move { closing.shutdown_and_wait_detailed().await });
    wait_for_shutdown_deadlines_to_start(&session).await;
    tokio::time::advance(std::time::Duration::from_millis(
        SESSION_SHUTDOWN_FLUSH_MS + 1,
    ))
    .await;
    let outcome = shutdown.await.unwrap();
    assert!(outcome.timed_out);
    assert!(outcome.errors.iter().any(|error| error.contains(
            "queue persistence drain timed out during shutdown; final rollout flush/shutdown was skipped"
        )));
    assert_eq!(rollout.flush_calls.load(Ordering::SeqCst), 1);
    assert_eq!(rollout.shutdown_calls.load(Ordering::SeqCst), 0);

    // The retained queue owner is not aborted. Completing it later must not
    // retroactively establish the final boundary that shutdown skipped.
    rollout.release_blocked_flush();
    session.await_queue_durability().await.unwrap();
    tokio::task::yield_now().await;
    assert_eq!(rollout.flush_calls.load(Ordering::SeqCst), 1);
    assert_eq!(rollout.shutdown_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn shutdown_terminalizes_reserved_queue_items_as_interrupted() {
    use crate::engine::message_queue::QueueMutationStatus;

    let session = mk_session(vec![]);
    session
        .enqueue(
            Input::Text("reserved at shutdown".into()),
            EnqueueOptions::default(),
        )
        .unwrap();
    let uuid = session.queue_snapshot().items[0]
        .message
        .uuid
        .clone()
        .unwrap();
    let reserved = session
        .conversation
        .queue
        .reserve_commands_by_max_priority(QueuePriority::Next, &|_| true)
        .unwrap();
    assert_eq!(reserved.len(), 1);
    let reserved_revision = session.queue_snapshot().revision;
    assert_eq!(
        session
            .remove_queued_message(
                "busy-at-session-shutdown".into(),
                uuid.clone(),
                reserved_revision,
            )
            .await
            .status,
        QueueMutationStatus::Busy
    );

    session.shutdown_and_wait().await.unwrap();

    let settled = session.queue_snapshot();
    assert!(settled.items.is_empty());
    assert_eq!(settled.revision, reserved_revision + 1);
    let replay = session
        .remove_queued_message("busy-at-session-shutdown".into(), uuid, reserved_revision)
        .await;
    assert_eq!(replay.status, QueueMutationStatus::Missing);
    assert_eq!(replay.terminal_state, Some(QueueTerminalState::Interrupted));
    let late = session
        .remove_queued_message(
            "not-admitted-after-session-shutdown".into(),
            "new-request-target".into(),
            settled.revision,
        )
        .await;
    assert_eq!(late.status, QueueMutationStatus::NotAdmitted);
    assert_eq!(late.snapshot, settled);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_queue_mutation_waiter_does_not_cancel_retained_durability_owner() {
    use crate::engine::message_queue::QueueMutationStatus;

    let session = mk_session(vec![]);
    session
        .enqueue(
            Input::Text("durable retained mutation".into()),
            EnqueueOptions::default(),
        )
        .unwrap();
    let initial = session.queue_snapshot();
    let uuid = initial.items[0].message.uuid.clone().unwrap();
    let receipt_entered = Arc::new(AtomicBool::new(false));
    let release_receipt = Arc::new(tokio::sync::Notify::new());
    let sink_calls = Arc::new(AtomicUsize::new(0));
    let entered_for_sink = receipt_entered.clone();
    let release_for_sink = release_receipt.clone();
    let calls_for_sink = sink_calls.clone();
    session
        .conversation
        .queue
        .set_persistence_sink(Arc::new(move |_| {
            calls_for_sink.fetch_add(1, Ordering::SeqCst);
            let entered = entered_for_sink.clone();
            let release = release_for_sink.clone();
            Box::pin(async move {
                entered.store(true, Ordering::SeqCst);
                release.notified().await;
                Ok(())
            })
        }));
    let observed = Arc::new(Mutex::new(Vec::new()));
    let observed_for_subscriber = observed.clone();
    let _unsubscribe = session.subscribe_queue_changes(Arc::new(move |snapshot| {
        observed_for_subscriber
            .lock()
            .unwrap()
            .push(snapshot.revision);
    }));

    let first_session = session.clone();
    let first_uuid = uuid.clone();
    let first = tokio::spawn(async move {
        first_session
            .edit_queued_message(
                "retained-edit".into(),
                first_uuid,
                initial.revision,
                QueueEdit {
                    value: Some(QueuedValue::Text("edited durably".into())),
                    ..Default::default()
                },
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !receipt_entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("retained owner reached durability receipt");
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());

    let second_session = session.clone();
    let second = tokio::spawn(async move {
        second_session
            .edit_queued_message(
                "retained-edit".into(),
                uuid,
                initial.revision,
                QueueEdit {
                    value: Some(QueuedValue::Text("edited durably".into())),
                    ..Default::default()
                },
            )
            .await
    });
    tokio::task::yield_now().await;
    assert!(!second.is_finished());
    assert!(observed.lock().unwrap().is_empty());

    release_receipt.notify_waiters();
    let terminal = second.await.unwrap();
    assert_eq!(terminal.status, QueueMutationStatus::Applied);
    assert_eq!(sink_calls.load(Ordering::SeqCst), 1);
    assert_eq!(observed.lock().unwrap().as_slice(), &[initial.revision + 1]);
    assert_eq!(
        session.queue_snapshot().items[0].message.value,
        QueuedValue::Text("edited durably".into())
    );
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn direct_pdf_steer_reaches_the_next_model_request_as_native_media() {
    let requests: Arc<Mutex<Vec<ModelRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let tool: AnyTool = Arc::new(CaptureRetryTool {
        schema: JsonSchema::new(),
        seen: Arc::new(Mutex::new(None)),
    });
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events("t1", "capture", &exact_json!({})));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let session = mk_session_with_deps(
        recording_deps(requests.clone(), vec![first, text_turn("done")]),
        default_config(),
        vec![tool],
    );
    session
        .enqueue(
            Input::Parts(vec![
                UserContentPart::Text {
                    text: "use this".into(),
                },
                pdf_part(b"%PDF-1.7\n", "application/pdf", Some("steer.pdf")),
            ]),
            EnqueueOptions {
                correlation_id: Some("steer-native-pdf".into()),
                ..Default::default()
            },
        )
        .expect("valid PDF steer");

    let result = session
        .run(Input::Text("start".into()), TurnOptions::default())
        .await;

    assert_eq!(result.stop_reason, TerminalReason::Completed);
    assert!(session.queued_messages().is_empty());
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    let steered = requests[1]
        .messages
        .iter()
        .find_map(|message| match message {
            Message::ToolResult(result) => result.extra_user_blocks.as_ref(),
            _ => None,
        });
    assert_eq!(
        steered,
        Some(&vec![ContentBlock::Document(DocumentBlock {
            mime_type: "application/pdf".into(),
            base64: "JVBERi0xLjcK".into(),
            filename: Some("steer.pdf".into()),
        })])
    );
}

#[tokio::test]
async fn direct_pdf_steers_share_one_turn_size_budget() {
    use crate::types::errors::InputValidationErrorCode;

    let requests: Arc<Mutex<Vec<ModelRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let tool: AnyTool = Arc::new(CaptureRetryTool {
        schema: JsonSchema::new(),
        seen: Arc::new(Mutex::new(None)),
    });
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events("t1", "capture", &exact_json!({})));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let session = mk_session_with_deps(
        recording_deps(requests, vec![first, text_turn("done")]),
        default_config(),
        vec![tool],
    );
    let mut bytes = vec![b'x'; 11 * 1024 * 1024];
    bytes[..5].copy_from_slice(b"%PDF-");
    for index in 0..2 {
        session
            .enqueue(
                Input::Parts(vec![pdf_part(
                    &bytes,
                    "application/pdf",
                    Some(&format!("{index}.pdf")),
                )]),
                EnqueueOptions {
                    correlation_id: Some(format!("steer-{index}").into()),
                    ..Default::default()
                },
            )
            .expect("each steer is individually below the limit");
    }

    let events: Vec<EngineEvent> = session
        .run_streamed(Input::Text("start".into()), TurnOptions::default())
        .events
        .collect()
        .await;

    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::InputRejected { correlation_id: Some(id), detail, .. }
            if id == "steer-1"
                && detail.code == InputValidationErrorCode::DocumentTooLarge
    )));
    assert!(session.queued_messages().is_empty());
}

#[tokio::test]
async fn direct_pdf_steers_share_one_turn_document_count_limit() {
    let tool: AnyTool = Arc::new(CaptureRetryTool {
        schema: JsonSchema::new(),
        seen: Arc::new(Mutex::new(None)),
    });
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events("t1", "capture", &exact_json!({})));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let session = mk_session_with_deps(
        recording_deps(
            Arc::new(Mutex::new(Vec::new())),
            vec![first, text_turn("done")],
        ),
        default_config(),
        vec![tool],
    );
    for index in 0..33 {
        session
            .enqueue(
                Input::Parts(vec![pdf_part(
                    b"%PDF-1.7\n",
                    "application/pdf",
                    Some(&format!("{index}.pdf")),
                )]),
                EnqueueOptions {
                    correlation_id: Some(format!("steer-{index}").into()),
                    ..Default::default()
                },
            )
            .expect("each steer is individually within the document count limit");
    }

    let events: Vec<EngineEvent> = session
        .run_streamed(Input::Text("start".into()), TurnOptions::default())
        .events
        .collect()
        .await;

    assert!(events.iter().any(|event| matches!(
        event,
        EngineEvent::InputRejected { correlation_id: Some(id), detail, .. }
            if id == "steer-32"
                && detail.code == InputValidationErrorCode::TooManyDocuments
                && detail.limit_count == Some(32)
    )));
    assert!(session.queued_messages().is_empty());
}

struct SyncTestMemoryLayout {
    base: String,
}

impl AgentMemoryLayout for SyncTestMemoryLayout {
    fn dir_for(&self, agent_type: &str, scope: AgentMemoryScope) -> String {
        let mut path = PathBuf::from(&self.base);
        path.push(format!("{}-{}", agent_type, scope.as_str()));
        path.to_str()
            .expect("test memory path is a scalar host path")
            .to_owned()
    }

    fn snapshot_dir_for(&self, agent_type: &str) -> String {
        let mut path = PathBuf::from(&self.base);
        path.push(format!("{agent_type}-snapshots"));
        path.to_str()
            .expect("test memory path is a scalar host path")
            .to_owned()
    }
}

struct SyncTestMemoryHost {
    layout: SyncTestMemoryLayout,
    plans: std::sync::Mutex<
        Vec<(
            AgentMemorySyncRequest,
            Result<crate::memory::sync::MemorySyncPlan, crate::memory::sync::MemorySyncError>,
        )>,
    >,
    applies: std::sync::Mutex<Vec<(AgentMemorySyncRequest, crate::memory::sync::MemorySyncPlan)>>,
    apply_results: std::sync::Mutex<
        Vec<(
            AgentMemorySyncRequest,
            Result<
                crate::memory::sync::MemorySyncApplyResult,
                crate::memory::sync::MemorySyncError,
            >,
        )>,
    >,
}

#[async_trait::async_trait]
impl AgentMemoryHost for SyncTestMemoryHost {
    fn layout(&self) -> &dyn AgentMemoryLayout {
        &self.layout
    }

    async fn memory_sync_input(
        &self,
        request: &AgentMemorySyncRequest,
    ) -> Option<crate::memory::sync::MemorySyncInput> {
        assert_eq!(request.agent_type, "main");
        Some(crate::memory::sync::MemorySyncInput {
            mode: crate::memory::sync::MemorySyncMode::Synced,
            local: vec![crate::memory::sync::MemorySyncItem {
                key: "alpha".into(),
                version: Some("v1".into()),
                body: Some("A".into()),
                deleted: false,
            }],
            remote: crate::memory::sync::RemoteMemoryState::Ready(vec![
                crate::memory::sync::MemorySyncItem {
                    key: "alpha".into(),
                    version: Some("v1".into()),
                    body: Some("A".into()),
                    deleted: false,
                },
            ]),
        })
    }

    async fn on_memory_sync_plan(
        &self,
        request: &AgentMemorySyncRequest,
        result: Result<crate::memory::sync::MemorySyncPlan, crate::memory::sync::MemorySyncError>,
    ) {
        self.plans.lock().unwrap().push((request.clone(), result));
    }

    async fn apply_memory_sync_plan(
        &self,
        request: &AgentMemorySyncRequest,
        plan: &crate::memory::sync::MemorySyncPlan,
    ) -> Result<crate::memory::sync::MemorySyncApplyResult, crate::memory::sync::MemorySyncError>
    {
        self.applies
            .lock()
            .unwrap()
            .push((request.clone(), plan.clone()));
        Ok(crate::memory::sync::MemorySyncApplyResult {
            applied_operations: plan.operations.len(),
        })
    }

    async fn on_memory_sync_apply_result(
        &self,
        request: &AgentMemorySyncRequest,
        result: Result<
            crate::memory::sync::MemorySyncApplyResult,
            crate::memory::sync::MemorySyncError,
        >,
    ) {
        self.apply_results
            .lock()
            .unwrap()
            .push((request.clone(), result));
    }
}

#[derive(Clone)]
enum SnapshotDecisionTestCallback {
    NoHandler,
    Pending,
    Choice(AgentMemoryMergeChoice),
    GatedChoice {
        started: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        choice: AgentMemoryMergeChoice,
    },
}

struct SnapshotDecisionTestHost {
    layout: SyncTestMemoryLayout,
    policy: AgentMemorySnapshotDecisionPolicy,
    callback: SnapshotDecisionTestCallback,
    sync_requests: std::sync::Mutex<Vec<AgentMemorySyncRequest>>,
    local_memory_write_on_sync: std::sync::Mutex<Option<String>>,
}

#[async_trait::async_trait]
impl AgentMemoryHost for SnapshotDecisionTestHost {
    fn layout(&self) -> &dyn AgentMemoryLayout {
        &self.layout
    }

    fn snapshot_decision_policy(&self) -> AgentMemorySnapshotDecisionPolicy {
        self.policy.clone()
    }

    async fn memory_sync_input(
        &self,
        request: &AgentMemorySyncRequest,
    ) -> Option<crate::memory::sync::MemorySyncInput> {
        self.sync_requests.lock().unwrap().push(request.clone());
        if let Some(contents) = self.local_memory_write_on_sync.lock().unwrap().take() {
            let agent_type = request.agent_type.try_to_utf8().unwrap();
            let local = self.layout.dir_for(&agent_type, request.scope);
            std::fs::write(Path::new(&local).join("local.md"), contents).unwrap();
        }
        None
    }

    async fn on_snapshot_decision_request(
        &self,
        _request: &AgentMemorySnapshotDecisionRequest,
    ) -> Option<AgentMemoryMergeChoice> {
        match self.callback.clone() {
            SnapshotDecisionTestCallback::NoHandler => None,
            SnapshotDecisionTestCallback::Pending => {
                std::future::pending::<Option<AgentMemoryMergeChoice>>().await
            }
            SnapshotDecisionTestCallback::Choice(choice) => Some(choice),
            SnapshotDecisionTestCallback::GatedChoice {
                started,
                release,
                choice,
            } => {
                started.notify_one();
                release.notified().await;
                Some(choice)
            }
        }
    }
}

fn snapshot_decision_policy(timeout_ms: u64) -> AgentMemorySnapshotDecisionPolicy {
    use crate::memory::snapshot_decision::AgentMemorySnapshotPolicyAction;

    AgentMemorySnapshotDecisionPolicy {
        timeout_ms,
        first_use: AgentMemorySnapshotPolicyAction::RequireHost,
        timeout: AgentMemorySnapshotPolicyAction::RequireHost,
        disconnect: AgentMemorySnapshotPolicyAction::RequireHost,
        no_handler: AgentMemorySnapshotPolicyAction::RequireHost,
    }
}

fn snapshot_decision_test_host(
    root: &Path,
    callback: SnapshotDecisionTestCallback,
    policy: AgentMemorySnapshotDecisionPolicy,
) -> Arc<SnapshotDecisionTestHost> {
    Arc::new(SnapshotDecisionTestHost {
        layout: SyncTestMemoryLayout {
            base: root
                .to_str()
                .expect("snapshot test path is scalar")
                .to_owned(),
        },
        policy,
        callback,
        sync_requests: std::sync::Mutex::new(Vec::new()),
        local_memory_write_on_sync: std::sync::Mutex::new(None),
    })
}

fn seed_snapshot_decision_test(host: &SnapshotDecisionTestHost, agent_type: &str) {
    let snapshot = host.layout.snapshot_dir_for(agent_type);
    std::fs::create_dir_all(&snapshot).unwrap();
    std::fs::write(
        Path::new(&snapshot).join("snapshot.json"),
        r#"{"updatedAt":"2026-07-18T00:00:00Z"}"#,
    )
    .unwrap();
    std::fs::write(Path::new(&snapshot).join("remote.md"), "remote\n").unwrap();
    let local = host.layout.dir_for(agent_type, AgentMemoryScope::User);
    std::fs::create_dir_all(&local).unwrap();
    // A local markdown file makes this an update, not the explicit CC
    // first-use seed path.
    std::fs::write(Path::new(&local).join("local.md"), "local\n").unwrap();
}

fn snapshot_decision_test_session(host: Arc<SnapshotDecisionTestHost>) -> Arc<Session> {
    let mut config = default_config();
    config.agent_memory = true;
    config.agent_memory_snapshot = true;
    mk_session_with_deps_and_memory(mk_deps(Vec::new()), config, Vec::new(), Some(host))
}

fn snapshot_decision_test_session_with_rollout(
    host: Arc<SnapshotDecisionTestHost>,
    rollout: RolloutSink,
) -> Arc<Session> {
    let mut config = default_config();
    config.agent_memory = true;
    config.agent_memory_snapshot = true;
    mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        mk_deps(Vec::new()),
        config,
        Vec::new(),
        Some(host),
        Some(rollout),
        None,
        None,
        None,
    )
}

fn restarted_snapshot_decision_test_session(
    host: Arc<SnapshotDecisionTestHost>,
    persisted: Vec<AgentMemorySnapshotDecisionAudit>,
) -> Arc<Session> {
    let mut config = default_config();
    config.agent_memory = true;
    config.agent_memory_snapshot = true;
    mk_session_with_provider_memory_store_agents_and_snapshot_decisions(
        Arc::new(fake_provider()),
        mk_deps(Vec::new()),
        config,
        Vec::new(),
        Some(host),
        None,
        None,
        None,
        None,
        Some(persisted),
    )
}

async fn persisted_requested_snapshot_audit(
    host: &SnapshotDecisionTestHost,
    agent_type: &str,
) -> AgentMemorySnapshotDecisionAudit {
    let request = prepare_agent_memory_snapshot_request(
        &host.layout,
        agent_type,
        0,
        i64::try_from(host.policy.timeout_ms).unwrap(),
    )
    .await
    .unwrap()
    .expect("seeded snapshot differs from local memory");
    AgentMemorySnapshotDecisionRegistry::default()
        .register(request)
        .unwrap()
        .audit
}

async fn wait_for_snapshot_decision_status(
    session: &Session,
    request_id: &RuntimeString,
    expected: AgentMemorySnapshotDecisionStatus,
) -> AgentMemorySnapshotDecisionAudit {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let audit = session
                .agent_memory_snapshot_decision(request_id)
                .await
                .expect("snapshot durability query succeeds")
                .expect("persisted snapshot decision remains registered");
            if audit.status == expected {
                return audit;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("snapshot decision did not reach {expected:?}"))
}

async fn assert_snapshot_marker_absent(host: &SnapshotDecisionTestHost, agent_type: &str) {
    assert!(
        crate::memory::snapshot_decision::read_agent_memory_snapshot_marker(
            &host.layout,
            agent_type,
        )
        .await
        .unwrap()
        .is_none()
    );
}

#[tokio::test]
async fn snapshot_requested_is_invisible_until_flush_and_survives_caller_cancellation() {
    let root = temp_store_dir();
    let host = snapshot_decision_test_host(
        &root,
        SnapshotDecisionTestCallback::Pending,
        snapshot_decision_policy(1_000),
    );
    seed_snapshot_decision_test(&host, "requested-flush-gate");
    let rollout = ControlledSnapshotRollout::new(1);
    let session = snapshot_decision_test_session_with_rollout(host.clone(), rollout.clone());
    let request =
        prepare_agent_memory_snapshot_request(&host.layout, "requested-flush-gate", 1, 1_001)
            .await
            .unwrap()
            .unwrap();
    let request_id = request.request_id.clone();
    let mut events = session.subscribe_agent_memory_snapshot_decisions();
    let owner_session = session.clone();
    let caller = tokio::spawn(async move {
        owner_session
            .register_agent_memory_snapshot_request(request)
            .await
    });

    rollout.wait_for_flush(1).await;
    assert_eq!(session.snapshot.decisions.query(&request_id), None);
    assert!(matches!(
        events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    caller.abort();
    let _ = caller.await;
    rollout.release_flush();

    let requested = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        session.agent_memory_snapshot_decision(&request_id),
    )
    .await
    .expect("Session-owned Requested owner should complete")
    .unwrap()
    .unwrap();
    assert_eq!(
        requested.status,
        AgentMemorySnapshotDecisionStatus::Requested
    );
    assert_eq!(events.recv().await.unwrap(), requested);

    session.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn snapshot_decision_and_ordered_reply_wait_for_their_flush_gate() {
    let root = temp_store_dir();
    let host = snapshot_decision_test_host(
        &root,
        SnapshotDecisionTestCallback::Pending,
        snapshot_decision_policy(1_000),
    );
    seed_snapshot_decision_test(&host, "decision-flush-gate");
    let rollout = ControlledSnapshotRollout::new(2);
    let session = snapshot_decision_test_session_with_rollout(host.clone(), rollout.clone());
    let request =
        prepare_agent_memory_snapshot_request(&host.layout, "decision-flush-gate", 1, 1_001)
            .await
            .unwrap()
            .unwrap();
    let request_id = request.request_id.clone();
    let mut events = session.subscribe_agent_memory_snapshot_decisions();
    session
        .register_agent_memory_snapshot_request(request)
        .await
        .unwrap();
    assert_eq!(
        events.recv().await.unwrap().status,
        AgentMemorySnapshotDecisionStatus::Requested
    );
    let replied = Arc::new(AtomicBool::new(false));
    let resolve_session = session.clone();
    let resolve_request_id = request_id.clone();
    let resolve_replied = replied.clone();
    let resolve = tokio::spawn(async move {
        resolve_session
            .resolve_agent_memory_snapshot_decision_ordered(
                &resolve_request_id,
                AgentMemoryMergeChoice::Merge,
                move |audit| {
                    resolve_replied.store(true, Ordering::SeqCst);
                    audit
                },
            )
            .await
    });

    rollout.wait_for_flush(2).await;
    assert_eq!(
        session
            .snapshot
            .decisions
            .query(&request_id)
            .unwrap()
            .status,
        AgentMemorySnapshotDecisionStatus::Requested
    );
    assert!(!replied.load(Ordering::SeqCst));
    assert!(matches!(
        events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    rollout.release_flush();

    let replied_audit = resolve.await.unwrap().unwrap();
    assert_eq!(
        replied_audit.status,
        AgentMemorySnapshotDecisionStatus::DecisionRecorded
    );
    assert!(replied.load(Ordering::SeqCst));
    assert_eq!(events.recv().await.unwrap(), replied_audit);

    session.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn snapshot_applied_is_invisible_until_its_flush_gate() {
    let root = temp_store_dir();
    let host = snapshot_decision_test_host(
        &root,
        SnapshotDecisionTestCallback::Pending,
        snapshot_decision_policy(1_000),
    );
    seed_snapshot_decision_test(&host, "applied-flush-gate");
    let rollout = ControlledSnapshotRollout::new(3);
    let session = snapshot_decision_test_session_with_rollout(host.clone(), rollout.clone());
    let request =
        prepare_agent_memory_snapshot_request(&host.layout, "applied-flush-gate", 1, 1_001)
            .await
            .unwrap()
            .unwrap();
    let request_id = request.request_id.clone();
    session
        .register_agent_memory_snapshot_request(request)
        .await
        .unwrap();
    let resolve_session = session.clone();
    let resolve_request_id = request_id.clone();
    let resolve = tokio::spawn(async move {
        resolve_session
            .resolve_agent_memory_snapshot_decision(
                &resolve_request_id,
                AgentMemoryMergeChoice::Keep,
            )
            .await
    });

    rollout.wait_for_flush(3).await;
    assert_eq!(
        session
            .snapshot
            .decisions
            .query(&request_id)
            .unwrap()
            .status,
        AgentMemorySnapshotDecisionStatus::DecisionRecorded
    );
    assert_eq!(
        rollout
            .events()
            .iter()
            .map(|audit| audit.status)
            .collect::<Vec<_>>(),
        vec![
            AgentMemorySnapshotDecisionStatus::Requested,
            AgentMemorySnapshotDecisionStatus::DecisionRecorded,
            AgentMemorySnapshotDecisionStatus::Applied,
        ]
    );
    assert!(!resolve.is_finished());
    rollout.release_flush();

    assert_eq!(
        resolve.await.unwrap().unwrap().status,
        AgentMemorySnapshotDecisionStatus::Applied
    );
    assert_eq!(
        session
            .agent_memory_snapshot_decision(&request_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        AgentMemorySnapshotDecisionStatus::Applied
    );

    session.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn direct_host_choice_flush_failure_stops_before_provider_admission() {
    let root = temp_store_dir();
    let agent_type = "host-choice-flush-fatal";
    let host = snapshot_decision_test_host(
        &root,
        SnapshotDecisionTestCallback::Choice(AgentMemoryMergeChoice::Keep),
        snapshot_decision_policy(1_000),
    );
    seed_snapshot_decision_test(&host, agent_type);
    let rollout = ControlledSnapshotRollout::failing_flush(2);
    let requests = Arc::new(Mutex::new(Vec::new()));
    let mut config = default_config();
    config.agent_memory = true;
    config.agent_memory_snapshot = true;
    let session = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        unique_subagent_test_deps(recording_deps(
            requests.clone(),
            vec![text_turn("provider must not start after snapshot fatal")],
        )),
        config,
        Vec::new(),
        Some(host.clone()),
        Some(rollout.clone()),
        None,
        None,
        Some(vec![AgentDefinition {
            name: agent_type.into(),
            memory: Some(AgentMemoryScope::User),
            ..Default::default()
        }]),
    );
    let spawn = session.make_spawn_sub_agent(
        0,
        CancellationToken::new(),
        session.permission_context(),
        Arc::new(Vec::new) as GetConversation,
        false,
        None,
        None,
    );
    let failure = spawn(SpawnSubAgentOptions {
        prompt: "must stop at snapshot durability".into(),
        agent_type: Some(agent_type.into()),
        fork_context: Some(ForkContext::None),
        ..Default::default()
    })
    .await
    .err()
    .expect("snapshot flush fatal must refuse creation")
    .try_to_utf8()
    .unwrap();
    assert!(failure.contains("stage=host-decision"), "{failure}");
    assert!(failure.contains("persistence is inconsistent"), "{failure}");
    assert!(requests.lock().unwrap().is_empty());
    assert_eq!(
        session
            .snapshot
            .decisions
            .all()
            .into_iter()
            .map(|audit| audit.status)
            .collect::<Vec<_>>(),
        vec![AgentMemorySnapshotDecisionStatus::Requested]
    );
    let shutdown = session.shutdown_and_wait_detailed().await;
    assert!(!shutdown.ok());
    assert_eq!(rollout.shutdown_calls.load(Ordering::SeqCst), 0);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn exactly_once_loser_requeries_and_returns_winning_merge_delivery() {
    let root = temp_store_dir();
    let agent_type = "race-winner-merge";
    let callback_started = Arc::new(tokio::sync::Notify::new());
    let callback_release = Arc::new(tokio::sync::Notify::new());
    let host = snapshot_decision_test_host(
        &root,
        SnapshotDecisionTestCallback::GatedChoice {
            started: callback_started.clone(),
            release: callback_release.clone(),
            choice: AgentMemoryMergeChoice::Keep,
        },
        snapshot_decision_policy(1_000),
    );
    seed_snapshot_decision_test(&host, agent_type);
    let rollout = ControlledSnapshotRollout::new(2);
    let session = snapshot_decision_test_session_with_rollout(host.clone(), rollout.clone());
    let reconcile_session = session.clone();
    let reconcile_host = host.clone();
    let reconcile = tokio::spawn(async move {
        reconcile_session
            .reconcile_agent_memory_snapshot(
                reconcile_host.as_ref(),
                agent_type,
                &CancellationToken::new(),
            )
            .await
    });
    callback_started.notified().await;
    let request_id = session
        .agent_memory_snapshot_decisions()
        .await
        .unwrap()
        .pop()
        .unwrap()
        .request
        .request_id;
    let winner_session = session.clone();
    let winner_request_id = request_id.clone();
    let winner = tokio::spawn(async move {
        winner_session
            .resolve_agent_memory_snapshot_decision(
                &winner_request_id,
                AgentMemoryMergeChoice::Merge,
            )
            .await
    });
    rollout.wait_for_flush(2).await;
    let winner_owner_id = session
        .snapshot
        .durability
        .lock()
        .unwrap()
        .provisionals
        .get(&request_id)
        .unwrap()
        .owner_id;
    callback_release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let loser_registered = session
                .snapshot
                .durability
                .lock()
                .unwrap()
                .provisionals
                .get(&request_id)
                .is_some_and(|provisional| provisional.owner_id != winner_owner_id);
            if loser_registered {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("direct Host loser must chain behind the protocol winner");
    rollout.release_flush();

    assert_eq!(
        winner.await.unwrap().unwrap().status,
        AgentMemorySnapshotDecisionStatus::DecisionRecorded
    );
    let delivery = reconcile.await.unwrap().unwrap().unwrap();
    assert_eq!(delivery.request_id, request_id);
    assert_eq!(
        delivery.prefix,
        crate::memory::agent_memory::build_merge_prompt_prefix()
    );

    session.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn snapshot_append_or_flush_uncertainty_fails_closed_and_skips_final_rollout() {
    let cases = [
        (
            "append-panic",
            ControlledSnapshotRollout::panicking_append(1),
        ),
        ("flush-panic", ControlledSnapshotRollout::panicking_flush(1)),
        ("flush-error", ControlledSnapshotRollout::failing_flush(1)),
    ];
    for (agent_type, rollout) in cases {
        let root = temp_store_dir();
        let host = snapshot_decision_test_host(
            &root,
            SnapshotDecisionTestCallback::Pending,
            snapshot_decision_policy(1_000),
        );
        seed_snapshot_decision_test(&host, agent_type);
        let session = snapshot_decision_test_session_with_rollout(host.clone(), rollout.clone());
        let request = prepare_agent_memory_snapshot_request(&host.layout, agent_type, 1, 1_001)
            .await
            .unwrap()
            .unwrap();
        let request_id = request.request_id.clone();

        assert!(matches!(
            session
                .register_agent_memory_snapshot_request(request)
                .await,
            Err(AgentMemorySnapshotApplyError::PersistenceInconsistent(_))
        ));
        assert!(matches!(
            session.agent_memory_snapshot_decision(&request_id).await,
            Err(AgentMemorySnapshotApplyError::PersistenceInconsistent(_))
        ));
        let shutdown = session.shutdown_and_wait_detailed().await;
        assert!(!shutdown.ok());
        assert_eq!(rollout.shutdown_calls.load(Ordering::SeqCst), 0);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[tokio::test]
async fn durable_snapshot_commit_mismatch_is_fatal_even_when_old_state_matches_audit() {
    let root = temp_store_dir();
    let host = snapshot_decision_test_host(
        &root,
        SnapshotDecisionTestCallback::Pending,
        snapshot_decision_policy(1_000),
    );
    seed_snapshot_decision_test(&host, "commit-mismatch");
    let rollout = ControlledSnapshotRollout::new(1);
    let session = snapshot_decision_test_session_with_rollout(host.clone(), rollout.clone());
    let request = prepare_agent_memory_snapshot_request(&host.layout, "commit-mismatch", 1, 1_001)
        .await
        .unwrap()
        .unwrap();
    let request_id = request.request_id.clone();
    let owner_session = session.clone();
    let register = tokio::spawn(async move {
        owner_session
            .register_agent_memory_snapshot_request(request)
            .await
    });

    rollout.wait_for_flush(1).await;
    let durable = rollout.events().pop().unwrap();
    session.snapshot.decisions.restore(durable).unwrap();
    rollout.release_flush();
    assert!(matches!(
        register.await.unwrap(),
        Err(AgentMemorySnapshotApplyError::PersistenceInconsistent(_))
    ));
    assert!(matches!(
        session.agent_memory_snapshot_decision(&request_id).await,
        Err(AgentMemorySnapshotApplyError::PersistenceInconsistent(_))
    ));
    assert!(matches!(
        session
            .with_agent_memory_snapshot_decision(&request_id, |_| ())
            .await,
        Err(AgentMemorySnapshotApplyError::PersistenceInconsistent(_))
    ));
    assert!(matches!(
        session.with_agent_memory_snapshot_decisions(|_| ()).await,
        Err(AgentMemorySnapshotApplyError::PersistenceInconsistent(_))
    ));
    assert!(matches!(
        session
            .resolve_agent_memory_snapshot_decision(&request_id, AgentMemoryMergeChoice::Keep,)
            .await,
        Err(AgentMemorySnapshotApplyError::PersistenceInconsistent(_))
    ));
    let shutdown = session.shutdown_and_wait_detailed().await;
    assert!(!shutdown.ok());
    assert_eq!(rollout.shutdown_calls.load(Ordering::SeqCst), 0);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn durable_filesystem_snapshot_cas_mismatch_is_fatal_when_current_equals_proposed() {
    let root = temp_store_dir();
    let host = snapshot_decision_test_host(
        &root,
        SnapshotDecisionTestCallback::Pending,
        snapshot_decision_policy(1_000),
    );
    seed_snapshot_decision_test(&host, "filesystem-commit-mismatch");
    let rollout = ControlledSnapshotRollout::new(3);
    let session = snapshot_decision_test_session_with_rollout(host.clone(), rollout.clone());
    let request =
        prepare_agent_memory_snapshot_request(&host.layout, "filesystem-commit-mismatch", 1, 1_001)
            .await
            .unwrap()
            .unwrap();
    let request_id = request.request_id.clone();
    session
        .register_agent_memory_snapshot_request(request)
        .await
        .unwrap();
    let resolve_session = session.clone();
    let resolve_request_id = request_id.clone();
    let resolve = tokio::spawn(async move {
        resolve_session
            .resolve_agent_memory_snapshot_decision(
                &resolve_request_id,
                AgentMemoryMergeChoice::Keep,
            )
            .await
    });

    rollout.wait_for_flush(3).await;
    let proposed = rollout.events().pop().unwrap();
    assert_eq!(proposed.status, AgentMemorySnapshotDecisionStatus::Applied);
    session
        .snapshot
        .decisions
        .restore(proposed.clone())
        .unwrap();
    assert_eq!(
        session.snapshot.decisions.query(&request_id),
        Some(proposed)
    );
    rollout.release_flush();

    assert!(matches!(
        resolve.await.unwrap(),
        Err(AgentMemorySnapshotApplyError::PersistenceInconsistent(_))
    ));
    assert!(matches!(
        session.agent_memory_snapshot_decision(&request_id).await,
        Err(AgentMemorySnapshotApplyError::PersistenceInconsistent(_))
    ));
    let shutdown = session.shutdown_and_wait_detailed().await;
    assert!(!shutdown.ok());
    assert_eq!(rollout.shutdown_calls.load(Ordering::SeqCst), 0);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn snapshot_apply_failure_publishes_failed_but_never_ordered_success_reply() {
    let root = temp_store_dir();
    let host = snapshot_decision_test_host(
        &root,
        SnapshotDecisionTestCallback::Pending,
        snapshot_decision_policy(1_000),
    );
    let agent_type = "ordered-apply-failure";
    seed_snapshot_decision_test(&host, agent_type);
    let rollout = ControlledSnapshotRollout::new(usize::MAX);
    let session = snapshot_decision_test_session_with_rollout(host.clone(), rollout.clone());
    let request = prepare_agent_memory_snapshot_request(&host.layout, agent_type, 1, 1_001)
        .await
        .unwrap()
        .unwrap();
    let request_id = request.request_id.clone();
    session
        .register_agent_memory_snapshot_request(request)
        .await
        .unwrap();
    let marker = Path::new(&host.layout.dir_for(agent_type, AgentMemoryScope::User))
        .join(".snapshot-synced.json");
    std::fs::create_dir(&marker).unwrap();
    let replies = Arc::new(AtomicUsize::new(0));
    let callback_replies = replies.clone();

    assert!(matches!(
        session
            .resolve_agent_memory_snapshot_decision_ordered(
                &request_id,
                AgentMemoryMergeChoice::Keep,
                move |_| callback_replies.fetch_add(1, Ordering::SeqCst),
            )
            .await,
        Err(AgentMemorySnapshotApplyError::Io(_))
    ));
    assert_eq!(replies.load(Ordering::SeqCst), 0);
    let failed = session
        .agent_memory_snapshot_decision(&request_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed.status, AgentMemorySnapshotDecisionStatus::Failed);
    assert_eq!(
        rollout
            .events()
            .into_iter()
            .map(|audit| audit.status)
            .collect::<Vec<_>>(),
        vec![
            AgentMemorySnapshotDecisionStatus::Requested,
            AgentMemorySnapshotDecisionStatus::DecisionRecorded,
            AgentMemorySnapshotDecisionStatus::Failed,
        ]
    );

    session.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn snapshot_missing_host_publishes_failed_but_never_ordered_success_reply() {
    let root = temp_store_dir();
    let host = snapshot_decision_test_host(
        &root,
        SnapshotDecisionTestCallback::Pending,
        snapshot_decision_policy(1_000),
    );
    let agent_type = "ordered-missing-host";
    seed_snapshot_decision_test(&host, agent_type);
    let request = prepare_agent_memory_snapshot_request(&host.layout, agent_type, 1, 1_001)
        .await
        .unwrap()
        .unwrap();
    let request_id = request.request_id.clone();
    let rollout = ControlledSnapshotRollout::new(usize::MAX);
    let mut config = default_config();
    config.agent_memory = false;
    config.agent_memory_snapshot = true;
    let session = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        mk_deps(Vec::new()),
        config,
        Vec::new(),
        None,
        Some(rollout.clone()),
        None,
        None,
        None,
    );
    session
        .register_agent_memory_snapshot_request(request)
        .await
        .unwrap();
    let replies = Arc::new(AtomicUsize::new(0));
    let callback_replies = replies.clone();

    assert!(matches!(
        session
            .resolve_agent_memory_snapshot_decision_ordered(
                &request_id,
                AgentMemoryMergeChoice::Keep,
                move |_| callback_replies.fetch_add(1, Ordering::SeqCst),
            )
            .await,
        Err(AgentMemorySnapshotApplyError::Registry(
            AgentMemorySnapshotRegistryError::InvalidRequest("agent memory Host is unavailable")
        ))
    ));
    assert_eq!(replies.load(Ordering::SeqCst), 0);
    assert_eq!(
        session
            .agent_memory_snapshot_decision(&request_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        AgentMemorySnapshotDecisionStatus::Failed
    );
    assert_eq!(
        rollout
            .events()
            .into_iter()
            .map(|audit| audit.status)
            .collect::<Vec<_>>(),
        vec![
            AgentMemorySnapshotDecisionStatus::Requested,
            AgentMemorySnapshotDecisionStatus::DecisionRecorded,
            AgentMemorySnapshotDecisionStatus::Failed,
        ]
    );

    session.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn snapshot_constructor_recovery_is_lazy_and_barrier_waits_for_flush() {
    let root = temp_store_dir();
    let host = snapshot_decision_test_host(
        &root,
        SnapshotDecisionTestCallback::Pending,
        snapshot_decision_policy(1_000),
    );
    seed_snapshot_decision_test(&host, "lazy-recovery");
    let requested = persisted_requested_snapshot_audit(&host, "lazy-recovery").await;
    let request_id = requested.request.request_id.clone();
    let rollout = ControlledSnapshotRollout::new(1);
    let mut config = default_config();
    config.agent_memory = true;
    config.agent_memory_snapshot = true;
    let session = mk_session_with_provider_memory_store_agents_and_snapshot_decisions(
        Arc::new(fake_provider()),
        mk_deps(Vec::new()),
        config,
        Vec::new(),
        Some(host.clone()),
        Some(rollout.clone()),
        None,
        None,
        None,
        Some(vec![requested]),
    );
    assert_eq!(rollout.flush_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        session
            .snapshot
            .decisions
            .query(&request_id)
            .unwrap()
            .status,
        AgentMemorySnapshotDecisionStatus::Requested
    );
    let barrier_session = session.clone();
    let barrier = tokio::spawn(async move {
        barrier_session
            .await_pending_snapshot_recovery_owners()
            .await
    });
    rollout.wait_for_flush(1).await;
    assert_eq!(
        session
            .snapshot
            .decisions
            .query(&request_id)
            .unwrap()
            .status,
        AgentMemorySnapshotDecisionStatus::Requested
    );
    rollout.release_flush();
    barrier.await.unwrap().unwrap();
    assert_eq!(
        session
            .agent_memory_snapshot_decision(&request_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        AgentMemorySnapshotDecisionStatus::Disconnected
    );

    session.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn snapshot_prepare_io_failures_refuse_creation_after_sync_before_provider_start() {
    for (agent_type, marker_failure) in [("capture-io-failure", false), ("marker-io-failure", true)]
    {
        let root = temp_store_dir();
        let host = snapshot_decision_test_host(
            &root,
            SnapshotDecisionTestCallback::NoHandler,
            snapshot_decision_policy(100),
        );
        seed_snapshot_decision_test(&host, agent_type);
        let local = host.layout.dir_for(agent_type, AgentMemoryScope::User);
        if marker_failure {
            std::fs::create_dir(Path::new(&local).join(".snapshot-synced.json")).unwrap();
        } else {
            std::fs::remove_dir_all(&local).unwrap();
            std::fs::write(&local, "not a directory").unwrap();
        }

        let requests = Arc::new(Mutex::new(Vec::new()));
        let mut config = default_config();
        config.agent_memory = true;
        config.agent_memory_snapshot = true;
        let session = mk_session_with_provider_memory_store_and_agents(
            Arc::new(fake_provider()),
            unique_subagent_test_deps(recording_deps(
                requests.clone(),
                vec![text_turn("provider must not start")],
            )),
            config,
            Vec::new(),
            Some(host.clone()),
            None,
            None,
            None,
            Some(vec![AgentDefinition {
                name: agent_type.into(),
                memory: Some(AgentMemoryScope::User),
                ..Default::default()
            }]),
        );
        let spawn = session.make_spawn_sub_agent(
            0,
            CancellationToken::new(),
            session.permission_context(),
            Arc::new(Vec::new) as GetConversation,
            false,
            None,
            None,
        );
        let failure = spawn(SpawnSubAgentOptions {
            prompt: "must fail during snapshot preparation".into(),
            agent_type: Some(agent_type.into()),
            fork_context: Some(ForkContext::None),
            ..Default::default()
        })
        .await
        .err()
        .expect("snapshot preparation failure must refuse creation");
        let failure = failure
            .try_to_utf8()
            .expect("snapshot failure is a scalar diagnostic");
        let stable_prefix = format!(
                "agent memory snapshot reconciliation failed: stage=prepare-request; agentType=\"{agent_type}\"; error="
            );
        assert!(failure.starts_with(&stable_prefix), "{failure}");
        assert!(failure.len() > stable_prefix.len(), "{failure}");
        assert_eq!(
            host.sync_requests.lock().unwrap().len(),
            1,
            "Host sync must finish before snapshot identity is prepared"
        );
        assert!(requests.lock().unwrap().is_empty());
        assert!(session
            .agent_memory_snapshot_decisions()
            .await
            .unwrap()
            .is_empty());

        session.shutdown_and_wait().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[tokio::test]
async fn snapshot_registry_registration_failure_is_explicit() {
    let root = temp_store_dir();
    let agent_type = "registry-failure";
    let host = snapshot_decision_test_host(
        &root,
        SnapshotDecisionTestCallback::NoHandler,
        snapshot_decision_policy(100),
    );
    seed_snapshot_decision_test(&host, agent_type);
    let session = snapshot_decision_test_session(host.clone());
    let mut request = prepare_agent_memory_snapshot_request(
        &host.layout,
        agent_type,
        0,
        i64::try_from(host.policy.timeout_ms).unwrap(),
    )
    .await
    .unwrap()
    .unwrap();
    request.request_id = "forced-registration-error".into();

    assert_eq!(
            session
                .reconcile_prepared_agent_memory_snapshot(
                    host.as_ref(),
                    agent_type,
                    &CancellationToken::new(),
                    host.policy.clone(),
                    request,
                )
                .await,
            Err(RuntimeString::from(
                "agent memory snapshot reconciliation failed: stage=register-request; agentType=\"registry-failure\"; error=invalid snapshot request: snapshot request id does not match its identity"
            ))
        );
    assert!(session
        .agent_memory_snapshot_decisions()
        .await
        .unwrap()
        .is_empty());

    session.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn snapshot_direct_host_choice_uses_the_same_record_and_apply_cas() {
    let root = temp_store_dir();
    let host = snapshot_decision_test_host(
        &root,
        SnapshotDecisionTestCallback::Choice(AgentMemoryMergeChoice::Keep),
        snapshot_decision_policy(100),
    );
    seed_snapshot_decision_test(&host, "direct-host");
    let session = snapshot_decision_test_session(host.clone());

    assert_eq!(
        session
            .reconcile_agent_memory_snapshot(
                host.as_ref(),
                "direct-host",
                &CancellationToken::new(),
            )
            .await,
        Ok(None)
    );
    let audit = session
        .agent_memory_snapshot_decisions()
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(audit.status, AgentMemorySnapshotDecisionStatus::Applied);
    assert_eq!(audit.decision, Some(AgentMemoryMergeChoice::Keep));
    assert_eq!(audit.source, Some(AgentMemorySnapshotDecisionSource::Host));
    assert!(
        crate::memory::snapshot_decision::read_agent_memory_snapshot_marker(
            &host.layout,
            "direct-host",
        )
        .await
        .unwrap()
        .is_some()
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn snapshot_no_handler_is_explicit_and_does_not_acknowledge() {
    let root = temp_store_dir();
    let host = snapshot_decision_test_host(
        &root,
        SnapshotDecisionTestCallback::NoHandler,
        snapshot_decision_policy(100),
    );
    seed_snapshot_decision_test(&host, "writer");
    let session = snapshot_decision_test_session(host.clone());

    assert_eq!(
        session
            .reconcile_agent_memory_snapshot(host.as_ref(), "writer", &CancellationToken::new(),)
            .await,
        Ok(None)
    );

    let audits = session.agent_memory_snapshot_decisions().await.unwrap();
    assert_eq!(audits.len(), 1);
    assert_eq!(
        audits[0].status,
        AgentMemorySnapshotDecisionStatus::NoHandler
    );
    assert_eq!(audits[0].decision, None);
    assert_eq!(audits[0].source, None);
    assert_snapshot_marker_absent(&host, "writer").await;
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn snapshot_protocol_resolution_observes_requested_then_applies_once() {
    let root = temp_store_dir();
    let host = snapshot_decision_test_host(
        &root,
        SnapshotDecisionTestCallback::NoHandler,
        snapshot_decision_policy(1_000),
    );
    seed_snapshot_decision_test(&host, "writer");
    let session = snapshot_decision_test_session(host.clone());
    let mut events = session.subscribe_agent_memory_snapshot_decisions();
    let task_session = session.clone();
    let task_host = host.clone();
    let reconcile = tokio::spawn(async move {
        task_session
            .reconcile_agent_memory_snapshot(
                task_host.as_ref(),
                "writer",
                &CancellationToken::new(),
            )
            .await
    });

    let requested = events.recv().await.unwrap();
    assert_eq!(
        requested.status,
        AgentMemorySnapshotDecisionStatus::Requested
    );
    let resolved = session
        .resolve_agent_memory_snapshot_decision(
            &requested.request.request_id,
            AgentMemoryMergeChoice::Merge,
        )
        .await
        .unwrap();
    assert_eq!(
        resolved.status,
        AgentMemorySnapshotDecisionStatus::DecisionRecorded
    );
    assert_eq!(
        resolved.source,
        Some(AgentMemorySnapshotDecisionSource::Host)
    );
    assert_eq!(
        reconcile.await.unwrap(),
        Ok(Some(AgentMemorySnapshotMergeDelivery {
            prefix: crate::memory::agent_memory::build_merge_prompt_prefix(),
            request_id: requested.request.request_id.clone(),
        }))
    );

    let recorded = events.recv().await.unwrap();
    assert_eq!(
        recorded.status,
        AgentMemorySnapshotDecisionStatus::DecisionRecorded
    );
    assert_snapshot_marker_absent(&host, "writer").await;
    let applied = session
        .apply_agent_memory_snapshot_merge_after_provider_admission(&requested.request.request_id)
        .await
        .unwrap();
    let applied_event = events.recv().await.unwrap();
    assert_eq!(applied_event, applied);
    assert_eq!(applied.status, AgentMemorySnapshotDecisionStatus::Applied);
    let queried = session
        .agent_memory_snapshot_decision(&requested.request.request_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(queried, applied_event);
    assert!(matches!(
        crate::memory::snapshot_decision::read_agent_memory_snapshot_marker(
            &host.layout,
            "writer",
        )
        .await
        .unwrap(),
        Some(crate::memory::snapshot_decision::AgentMemorySnapshotMarkerRead::Current(
            marker,
        )) if marker.request_id == requested.request.request_id
            && marker.decision == AgentMemoryMergeChoice::Merge
            && marker.source == AgentMemorySnapshotDecisionSource::Host
    ));
    assert!(matches!(
        session
            .resolve_agent_memory_snapshot_decision(
                &requested.request.request_id,
                AgentMemoryMergeChoice::Replace,
            )
            .await,
        Err(AgentMemorySnapshotApplyError::Registry(
            AgentMemorySnapshotRegistryError::DecisionConflict
        ))
    ));
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn snapshot_merge_is_applied_after_provider_accepts_request_with_prefix() {
    let root = temp_store_dir();
    let agent_type = "provider-merge";
    let host = snapshot_decision_test_host(
        &root,
        SnapshotDecisionTestCallback::Choice(AgentMemoryMergeChoice::Merge),
        snapshot_decision_policy(1_000),
    );
    seed_snapshot_decision_test(&host, agent_type);
    let pre_sync_request =
        prepare_agent_memory_snapshot_request(&host.layout, agent_type, 0, 1_000)
            .await
            .unwrap()
            .unwrap();
    *host.local_memory_write_on_sync.lock().unwrap() =
        Some("local changed by Host sync\n".to_string());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let mut config = default_config();
    config.agent_memory = true;
    config.agent_memory_snapshot = true;
    let session = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        unique_subagent_test_deps(recording_deps(
            requests.clone(),
            vec![text_turn("provider accepted merge")],
        )),
        config,
        Vec::new(),
        Some(host.clone()),
        None,
        None,
        None,
        Some(vec![AgentDefinition {
            name: agent_type.into(),
            memory: Some(AgentMemoryScope::User),
            ..Default::default()
        }]),
    );
    let mut audits = session.subscribe_agent_memory_snapshot_decisions();
    let spawn = session.make_spawn_sub_agent(
        0,
        CancellationToken::new(),
        session.permission_context(),
        Arc::new(Vec::new) as GetConversation,
        false,
        None,
        None,
    );
    let run = spawn(SpawnSubAgentOptions {
        prompt: "use the reconciled memory".into(),
        agent_type: Some(agent_type.into()),
        ..Default::default()
    })
    .await
    .expect("child creation must be accepted");

    assert_eq!(foreground_failure(run.events).await, None);
    {
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let prefix = crate::memory::agent_memory::build_merge_prompt_prefix();
        assert!(requests[0].messages.iter().any(|message| matches!(
            message,
            Message::User(user) if user.content.iter().any(|block| matches!(
                block,
                ContentBlock::Text(text)
                    if text.text.try_to_utf8().is_ok_and(|value| value.contains(&prefix))
            ))
        )));
    }

    let requested = audits.recv().await.unwrap();
    let recorded = audits.recv().await.unwrap();
    let applied = audits.recv().await.unwrap();
    assert_eq!(
        [requested.status, recorded.status, applied.status],
        [
            AgentMemorySnapshotDecisionStatus::Requested,
            AgentMemorySnapshotDecisionStatus::DecisionRecorded,
            AgentMemorySnapshotDecisionStatus::Applied,
        ]
    );
    assert_eq!(recorded.request.request_id, applied.request.request_id);
    assert_ne!(
        applied.request.local_state_id, pre_sync_request.local_state_id,
        "snapshot request identity must be captured after Host sync mutates local memory"
    );
    assert_eq!(
        std::fs::read_to_string(
            Path::new(&host.layout.dir_for(agent_type, AgentMemoryScope::User)).join("local.md")
        )
        .unwrap(),
        "local changed by Host sync\n"
    );
    assert!(matches!(
        crate::memory::snapshot_decision::read_agent_memory_snapshot_marker(
            &host.layout,
            agent_type,
        )
        .await
        .unwrap(),
        Some(crate::memory::snapshot_decision::AgentMemorySnapshotMarkerRead::Current(
            marker,
        )) if marker.request_id == applied.request.request_id
            && marker.decision == AgentMemoryMergeChoice::Merge
    ));

    session.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn snapshot_provider_admission_apply_failure_turn_fails_before_stream_poll() {
    let root = temp_store_dir();
    let agent_type = "provider-merge-apply-failure";
    let host = snapshot_decision_test_host(
        &root,
        SnapshotDecisionTestCallback::Choice(AgentMemoryMergeChoice::Merge),
        snapshot_decision_policy(1_000),
    );
    seed_snapshot_decision_test(&host, agent_type);
    let requests = Arc::new(Mutex::new(Vec::new()));
    let stream_polled = Arc::new(AtomicBool::new(false));
    let marker_path = Path::new(&host.layout.dir_for(agent_type, AgentMemoryScope::User))
        .join(".snapshot-synced.json");
    let mut deps = mk_deps(Vec::new());
    deps.call_model = {
        let requests = requests.clone();
        let stream_polled = stream_polled.clone();
        let marker_path = marker_path.clone();
        Box::new(move |request, signal| {
            requests.lock().unwrap().push(request);
            std::fs::create_dir(&marker_path).unwrap();
            let stream_polled = stream_polled.clone();
            Box::pin(stream! {
                stream_polled.store(true, Ordering::SeqCst);
                for event in text_turn("must not be polled") {
                    if signal.is_cancelled() {
                        break;
                    }
                    yield event;
                }
            }) as ModelStream
        })
    };
    let mut config = default_config();
    config.agent_memory = true;
    config.agent_memory_snapshot = true;
    let session = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        unique_subagent_test_deps(deps),
        config,
        Vec::new(),
        Some(host.clone()),
        None,
        None,
        None,
        Some(vec![AgentDefinition {
            name: agent_type.into(),
            memory: Some(AgentMemoryScope::User),
            ..Default::default()
        }]),
    );
    let spawn = session.make_spawn_sub_agent(
        0,
        CancellationToken::new(),
        session.permission_context(),
        Arc::new(Vec::new) as GetConversation,
        false,
        None,
        None,
    );
    let run = spawn(SpawnSubAgentOptions {
        prompt: "fail at the provider admission boundary".into(),
        agent_type: Some(agent_type.into()),
        ..Default::default()
    })
    .await
    .expect("child creation must be accepted");

    let failure = foreground_failure(run.events)
        .await
        .expect("provider-admission apply failure must surface as TurnFailed")
        .try_to_utf8()
        .unwrap();
    assert!(
        failure.contains("stage=provider-admission-apply"),
        "{failure}"
    );
    assert_eq!(requests.lock().unwrap().len(), 1);
    assert!(!stream_polled.load(Ordering::SeqCst));
    let audit = session
        .agent_memory_snapshot_decisions()
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(audit.status, AgentMemorySnapshotDecisionStatus::Failed);
    assert!(
        Path::new(&host.layout.dir_for(agent_type, AgentMemoryScope::User))
            .join(".snapshot-synced.json")
            .is_dir()
    );

    session.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn snapshot_timeout_and_disconnect_are_distinct_without_markers() {
    for (label, cancel, expected) in [
        (
            "timeout",
            false,
            AgentMemorySnapshotDecisionStatus::TimedOut,
        ),
        (
            "disconnect",
            true,
            AgentMemorySnapshotDecisionStatus::Disconnected,
        ),
    ] {
        let root = temp_store_dir();
        let host = snapshot_decision_test_host(
            &root,
            SnapshotDecisionTestCallback::Pending,
            snapshot_decision_policy(10),
        );
        seed_snapshot_decision_test(&host, label);
        let session = snapshot_decision_test_session(host.clone());
        let signal = CancellationToken::new();
        let task_signal = signal.clone();
        let task_session = session.clone();
        let task_host = host.clone();
        let reconcile = tokio::spawn(async move {
            task_session
                .reconcile_agent_memory_snapshot(task_host.as_ref(), label, &task_signal)
                .await
        });
        if cancel {
            tokio::task::yield_now().await;
            signal.cancel();
        }
        assert_eq!(reconcile.await.unwrap(), Ok(None));
        let audits = session.agent_memory_snapshot_decisions().await.unwrap();
        assert_eq!(audits.len(), 1);
        assert_eq!(audits[0].status, expected);
        assert_snapshot_marker_absent(&host, label).await;
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[tokio::test]
async fn configured_timeout_decision_is_audited_and_acknowledged() {
    use crate::memory::snapshot_decision::AgentMemorySnapshotPolicyAction;

    let root = temp_store_dir();
    let mut policy = snapshot_decision_policy(5);
    policy.timeout = AgentMemorySnapshotPolicyAction::Keep;
    let host = snapshot_decision_test_host(&root, SnapshotDecisionTestCallback::Pending, policy);
    seed_snapshot_decision_test(&host, "configured-timeout");
    let session = snapshot_decision_test_session(host.clone());

    assert_eq!(
        session
            .reconcile_agent_memory_snapshot(
                host.as_ref(),
                "configured-timeout",
                &CancellationToken::new(),
            )
            .await,
        Ok(None)
    );
    let audit = session
        .agent_memory_snapshot_decisions()
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(audit.status, AgentMemorySnapshotDecisionStatus::Applied);
    assert_eq!(audit.decision, Some(AgentMemoryMergeChoice::Keep));
    assert_eq!(
        audit.source,
        Some(AgentMemorySnapshotDecisionSource::TimeoutPolicy)
    );
    assert!(
        crate::memory::snapshot_decision::read_agent_memory_snapshot_marker(
            &host.layout,
            "configured-timeout",
        )
        .await
        .unwrap()
        .is_some()
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn shutdown_settles_a_persisted_requested_snapshot_as_disconnected() {
    let root = temp_store_dir();
    let host = snapshot_decision_test_host(
        &root,
        SnapshotDecisionTestCallback::Pending,
        snapshot_decision_policy(1_000),
    );
    seed_snapshot_decision_test(&host, "shutdown");
    let session = snapshot_decision_test_session(host.clone());
    let request = prepare_agent_memory_snapshot_request(&host.layout, "shutdown", 1, 1_001)
        .await
        .unwrap()
        .unwrap();
    let request_id = request.request_id.clone();
    session
        .register_agent_memory_snapshot_request(request)
        .await
        .unwrap();

    session.shutdown_and_wait().await.unwrap();

    let audit = session
        .agent_memory_snapshot_decision(&request_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        audit.status,
        AgentMemorySnapshotDecisionStatus::Disconnected
    );
    assert_snapshot_marker_absent(&host, "shutdown").await;
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn restart_applies_persisted_requested_keep_disconnect_policy_and_writes_marker() {
    use crate::memory::snapshot_decision::{
        AgentMemorySnapshotMarkerRead, AgentMemorySnapshotPolicyAction,
    };

    let root = temp_store_dir();
    let mut policy = snapshot_decision_policy(1_000);
    policy.disconnect = AgentMemorySnapshotPolicyAction::Keep;
    let host = snapshot_decision_test_host(&root, SnapshotDecisionTestCallback::Pending, policy);
    seed_snapshot_decision_test(&host, "restart-keep");
    let requested = persisted_requested_snapshot_audit(&host, "restart-keep").await;
    let request_id = requested.request.request_id.clone();

    let restarted = restarted_snapshot_decision_test_session(host.clone(), vec![requested]);
    let applied = wait_for_snapshot_decision_status(
        &restarted,
        &request_id,
        AgentMemorySnapshotDecisionStatus::Applied,
    )
    .await;

    assert_eq!(applied.decision, Some(AgentMemoryMergeChoice::Keep));
    assert_eq!(
        applied.source,
        Some(AgentMemorySnapshotDecisionSource::DisconnectPolicy)
    );
    assert!(matches!(
        crate::memory::snapshot_decision::read_agent_memory_snapshot_marker(
            &host.layout,
            "restart-keep",
        )
        .await
        .unwrap(),
        Some(AgentMemorySnapshotMarkerRead::Current(marker))
            if marker.request_id == request_id
                && marker.decision == AgentMemoryMergeChoice::Keep
                && marker.source == AgentMemorySnapshotDecisionSource::DisconnectPolicy
    ));

    restarted.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn restart_records_persisted_requested_merge_until_real_reconcile_consumes_prompt() {
    use crate::memory::snapshot_decision::AgentMemorySnapshotPolicyAction;

    let root = temp_store_dir();
    let mut policy = snapshot_decision_policy(1_000);
    policy.disconnect = AgentMemorySnapshotPolicyAction::Merge;
    let host = snapshot_decision_test_host(&root, SnapshotDecisionTestCallback::Pending, policy);
    seed_snapshot_decision_test(&host, "restart-merge");
    let requested = persisted_requested_snapshot_audit(&host, "restart-merge").await;
    let request_id = requested.request.request_id.clone();

    let restarted = restarted_snapshot_decision_test_session(host.clone(), vec![requested]);
    let recorded = wait_for_snapshot_decision_status(
        &restarted,
        &request_id,
        AgentMemorySnapshotDecisionStatus::DecisionRecorded,
    )
    .await;
    assert_eq!(recorded.decision, Some(AgentMemoryMergeChoice::Merge));
    assert_eq!(
        recorded.source,
        Some(AgentMemorySnapshotDecisionSource::DisconnectPolicy)
    );
    assert_snapshot_marker_absent(&host, "restart-merge").await;

    let expected_delivery = AgentMemorySnapshotMergeDelivery {
        prefix: crate::memory::agent_memory::build_merge_prompt_prefix(),
        request_id: request_id.clone(),
    };
    assert_eq!(
        restarted
            .reconcile_agent_memory_snapshot(
                host.as_ref(),
                "restart-merge",
                &CancellationToken::new(),
            )
            .await,
        Ok(Some(expected_delivery.clone()))
    );
    let still_recorded = restarted
        .agent_memory_snapshot_decision(&request_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        still_recorded.status,
        AgentMemorySnapshotDecisionStatus::DecisionRecorded
    );
    assert_eq!(
        still_recorded.source,
        Some(AgentMemorySnapshotDecisionSource::DisconnectPolicy)
    );
    assert_snapshot_marker_absent(&host, "restart-merge").await;

    // A pre-provider shutdown cannot consume the delivery. Cold recovery
    // must present the same correlated prefix again.
    restarted.shutdown_and_wait().await.unwrap();
    let second_restart =
        restarted_snapshot_decision_test_session(host.clone(), vec![still_recorded]);
    assert_eq!(
        second_restart
            .reconcile_agent_memory_snapshot(
                host.as_ref(),
                "restart-merge",
                &CancellationToken::new(),
            )
            .await,
        Ok(Some(expected_delivery))
    );
    assert_snapshot_marker_absent(&host, "restart-merge").await;

    let applied = second_restart
        .apply_agent_memory_snapshot_merge_after_provider_admission(&request_id)
        .await
        .unwrap();
    assert_eq!(applied.status, AgentMemorySnapshotDecisionStatus::Applied);
    assert!(
        crate::memory::snapshot_decision::read_agent_memory_snapshot_marker(
            &host.layout,
            "restart-merge",
        )
        .await
        .unwrap()
        .is_some()
    );

    second_restart.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn restart_and_shutdown_do_not_eagerly_ack_persisted_recorded_merge_without_consumer() {
    use crate::memory::snapshot_decision::{
        AgentMemorySnapshotPolicyAction, AgentMemorySnapshotTransitionResult,
    };

    let root = temp_store_dir();
    let mut policy = snapshot_decision_policy(1_000);
    // A previously recorded decision is authoritative; even a different
    // disconnect fallback must not replace or eagerly acknowledge it.
    policy.disconnect = AgentMemorySnapshotPolicyAction::Keep;
    let host = snapshot_decision_test_host(&root, SnapshotDecisionTestCallback::Pending, policy);
    seed_snapshot_decision_test(&host, "recorded-merge");
    let requested = persisted_requested_snapshot_audit(&host, "recorded-merge").await;
    let request_id = requested.request.request_id.clone();
    let registry = AgentMemorySnapshotDecisionRegistry::default();
    registry.restore(requested).unwrap();
    let AgentMemorySnapshotTransitionResult {
        audit: recorded, ..
    } = registry
        .record_decision(
            &request_id,
            AgentMemorySnapshotDecisionStatus::Requested,
            AgentMemoryMergeChoice::Merge,
            AgentMemorySnapshotDecisionSource::Host,
            0,
        )
        .unwrap();

    let first_restart =
        restarted_snapshot_decision_test_session(host.clone(), vec![recorded.clone()]);
    first_restart.shutdown_and_wait().await.unwrap();
    let after_shutdown = first_restart
        .agent_memory_snapshot_decision(&request_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after_shutdown.status,
        AgentMemorySnapshotDecisionStatus::DecisionRecorded
    );
    assert_eq!(after_shutdown.decision, Some(AgentMemoryMergeChoice::Merge));
    assert_eq!(
        after_shutdown.source,
        Some(AgentMemorySnapshotDecisionSource::Host)
    );
    assert_snapshot_marker_absent(&host, "recorded-merge").await;

    let second_restart =
        restarted_snapshot_decision_test_session(host.clone(), vec![after_shutdown]);
    second_restart.shutdown_and_wait().await.unwrap();
    let after_second_restart = second_restart
        .agent_memory_snapshot_decision(&request_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after_second_restart.status,
        AgentMemorySnapshotDecisionStatus::DecisionRecorded
    );
    assert_eq!(
        after_second_restart.decision,
        Some(AgentMemoryMergeChoice::Merge)
    );
    assert_snapshot_marker_absent(&host, "recorded-merge").await;

    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn restart_finishes_applied_when_merge_marker_outlived_audit_publish() {
    use crate::memory::snapshot_decision::{
        write_agent_memory_snapshot_marker_atomic, AgentMemorySnapshotSyncMarker,
        AgentMemorySnapshotTransitionResult,
    };

    let root = temp_store_dir();
    let host = snapshot_decision_test_host(
        &root,
        SnapshotDecisionTestCallback::Pending,
        snapshot_decision_policy(1_000),
    );
    let agent_type = "recorded-merge-marker";
    seed_snapshot_decision_test(&host, agent_type);
    let requested = persisted_requested_snapshot_audit(&host, agent_type).await;
    let request_id = requested.request.request_id.clone();
    let registry = AgentMemorySnapshotDecisionRegistry::default();
    registry.restore(requested.clone()).unwrap();
    let AgentMemorySnapshotTransitionResult {
        audit: recorded, ..
    } = registry
        .record_decision(
            &request_id,
            AgentMemorySnapshotDecisionStatus::Requested,
            AgentMemoryMergeChoice::Merge,
            AgentMemorySnapshotDecisionSource::Host,
            11,
        )
        .unwrap();
    let local = host.layout.dir_for(agent_type, AgentMemoryScope::User);
    write_agent_memory_snapshot_marker_atomic(
        &local,
        &AgentMemorySnapshotSyncMarker {
            schema_version: 2,
            synced_from: requested.request.snapshot_timestamp.clone(),
            snapshot_id: requested.request.snapshot_id.clone(),
            request_id: request_id.clone(),
            decision: AgentMemoryMergeChoice::Merge,
            source: AgentMemorySnapshotDecisionSource::Host,
            applied_at: 12,
        },
    )
    .await
    .unwrap();

    let restarted = restarted_snapshot_decision_test_session(host.clone(), vec![recorded]);
    let applied = wait_for_snapshot_decision_status(
        &restarted,
        &request_id,
        AgentMemorySnapshotDecisionStatus::Applied,
    )
    .await;
    assert_eq!(applied.ended_at, Some(12));
    assert_eq!(applied.decision, Some(AgentMemoryMergeChoice::Merge));
    assert_eq!(
        applied.source,
        Some(AgentMemorySnapshotDecisionSource::Host)
    );

    restarted.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn resolve_config_rejects_unknown_keys_in_main_resolver() {
    let err = resolve_config(serde_json::json!({
        "model": "claude-sonnet-test",
        "madeUpFeature": true,
    }))
    .expect_err("unknown top-level config keys should fail fast");

    assert!(err.contains("madeUpFeature"), "{err}");
    assert!(err.contains("unknown"), "{err}");
}

#[test]
fn resolve_config_defaults_and_validates_nested_dream_thresholds() {
    let cfg = resolve_config(serde_json::json!({
        "agentMemory": true,
        "autoDream": true,
        "dream": { "minHours": 0.5, "minSessions": 9 }
    }))
    .expect("Desktop dream settings should resolve through normal config");
    assert_eq!(cfg.dream.min_hours, 0.5);
    assert_eq!(cfg.dream.min_sessions, 9);

    let partial = resolve_config(serde_json::json!({
        "dream": { "minSessions": 3 }
    }))
    .expect("nested defaults should survive a partial override");
    assert_eq!(partial.dream.min_hours, 24.0);
    assert_eq!(partial.dream.min_sessions, 3);

    for invalid in [
        serde_json::json!({ "dream": { "minHours": 0.0 } }),
        serde_json::json!({ "dream": { "minSessions": 101 } }),
        serde_json::json!({ "dream": { "minSessions": 1.5 } }),
    ] {
        assert!(resolve_config(invalid).is_err());
    }
}

/// Safety net for waits whose assertion is LIVENESS ("the other thread does
/// eventually make progress"), never latency. A saturated host can starve a
/// freshly spawned OS thread for many seconds, so these waits must be generous:
/// a real deadlock still fails the test, just later. Negative assertions ("must
/// NOT complete yet") keep their own short, deliberate windows.
const CROSS_THREAD_LIVENESS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

/// Liveness deadline for tests that RE-EXEC this whole test binary as a child
/// process. Under a saturated full-suite run the suite itself has been observed
/// to slow down ~5x, and the child pays binary startup + test filtering on top,
/// so it gets a far larger budget than in-process liveness waits. The assertions
/// behind it are pure liveness ("the process exits at all"), never latency.
#[cfg(unix)]
const SUBPROCESS_REEXEC_LIVENESS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(900);

/// Wall-clock patience for waiting on a memory-maintenance fork to reach the
/// provider. Everything behind that wait touches the filesystem and a
/// cross-process consolidation lock, so on a saturated machine it can take far
/// longer than the purely in-process waits elsewhere in this file. Only the
/// patience is longer — a fork that never arrives still fails the test.
const MEMORY_MAINTENANCE_ENTRY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

fn temp_store_dir() -> PathBuf {
    static SEQUENCE: AtomicUsize = AtomicUsize::new(0);
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "klaus-session-tool-lifecycle-{}-{}-{}",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::SeqCst),
        (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos())
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn default_config_initializes_agent_memory_host() {
    let session = mk_session_with(vec![], default_config(), Vec::new());
    assert!(session.agent_memory_host.is_some());
    assert!(session.memory_roots.main().is_some());

    let mut config = default_config();
    config.agent_memory = false;
    let disabled = mk_session_with(vec![], config, Vec::new());
    assert!(disabled.agent_memory_host.is_none());
    assert!(disabled.memory_roots.main().is_none());
}

#[tokio::test]
async fn slash_command_preserves_exact_arguments_and_unknown_non_scalar_name_as_model_input() {
    let requests: Arc<Mutex<Vec<ModelRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_args: Arc<Mutex<Vec<RuntimeString>>> = Arc::new(Mutex::new(Vec::new()));
    let command_seen_args = seen_args.clone();
    let command = Command::Prompt(PromptCommand::new(
        "plugin-cmd",
        CommandMeta::default(),
        move |args: RuntimeString| {
            let command_seen_args = command_seen_args.clone();
            async move {
                command_seen_args.lock().unwrap().push(args.clone());
                Ok(vec![ContentBlock::Text(TextBlock {
                    text: RuntimeString::from("expanded:").concat(&args),
                    citations: None,
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                })])
            }
        },
    ));
    let session = Session::new(SessionInit {
        durable_permissions: None,
        process_context: crate::runtime_process::test_runtime_process_context(),
        user_config_dir: None,
        id: Some("s1".into()),
        session_role: None,
        parent_thread_id: None,
        provider: Arc::new(fake_provider()),
        deps: recording_deps(
            requests.clone(),
            vec![
                text_turn("high"),
                text_turn("low"),
                text_turn("replacement"),
                text_turn("unknown"),
                text_turn("media"),
            ],
        ),
        config: default_config(),
        network_allowed: None,
        trusted_windows_sandbox_context: None,
        context_window_override: None,
        pricing: None,
        tools: Vec::new(),
        refresh_tools: None,
        cwd: runtime_path_from_host_path(&std::env::temp_dir()),
        approval_handler: None,
        approval_reviewer: None,
        elicit_handler: None,
        mcp_elicitation_presenter: None,
        mcp_authorize_handler: None,
        mcp_oauth_redirect_uri: None,
        credential_store: None,
        hook_bus: None,
        rollout: None,
        store: None,
        mcp_clients: Vec::new(),
        plugin_mcp_servers: Vec::new(),
        slash_commands: vec![command],
        initial_history: None,
        initial_approval_context: None,
        initial_approval_reviews: None,
        initial_tool_result_budget_records: None,
        hidden_context_message_ids: None,
        initial_usage_ledger: None,
        initial_subagent_lifecycle: None,
        initial_background_processes: None,
        initial_memory_maintenance: None,
        initial_setup_lifecycle: None,
        initial_agent_memory_snapshot_decisions: None,
        forked_from_id: None,
        initial_checkpoints: None,
        system_prompt_provider: None,
        base_instructions: None,
        developer_instructions: None,
        root_agent_definition: None,
        observer: Some(None),
        logger: None,
        agents: None,
        skills: None,
        output_styles: None,
        skill_gate: None,
        current_worktree: None,
        prior_workflow_journal: None,
        initial_surfaced_tools: None,
        swarm: None,
        lsp_servers: None,
        shell_env: None,
        agent_memory: None,
        agent_memory_snapshot_policy: None,
        browser_runtime: None,
        runtime_capabilities: RuntimeCapabilityRegistry::new(),
        selected_capability_roots: None,
        extension_runtime: ExtensionRuntimeSnapshot::default(),
    });

    let arguments = vec![
        RuntimeString::from_utf16(vec![0xd800]),
        RuntimeString::from_utf16(vec![0xdc00]),
        RuntimeString::from("\u{fffd}"),
    ];
    for argument in &arguments {
        let input = RuntimeString::from("/plugin-cmd ").concat(argument);
        let result = session
            .run(Input::Text(input), TurnOptions::default())
            .await;
        assert_eq!(result.stop_reason, TerminalReason::Completed);
    }

    let unknown_name_input = RuntimeString::from("/")
        .concat(&RuntimeString::from_utf16(vec![0xd800]))
        .concat(&RuntimeString::from(" untouched"));
    let result = session
        .run(
            Input::Text(unknown_name_input.clone()),
            TurnOptions::default(),
        )
        .await;
    assert_eq!(result.stop_reason, TerminalReason::Completed);

    let media_argument = RuntimeString::from("with-image");
    let result = session
        .run(
            Input::Parts(vec![
                UserContentPart::Text {
                    text: RuntimeString::from("/plugin-cmd ").concat(&media_argument),
                },
                UserContentPart::Image {
                    base64: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScLbtAAAAABJRU5ErkJggg==".into(),
                    mime_type: "image/png".into(),
                    source_path: None,
                },
            ]),
            TurnOptions::default(),
        )
        .await;
    assert_eq!(result.stop_reason, TerminalReason::Completed);

    let mut expected_arguments = arguments.clone();
    expected_arguments.push(media_argument.clone());
    assert_eq!(*seen_args.lock().unwrap(), expected_arguments);
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 5);
    for (request, argument) in requests.iter().take(3).zip(&arguments) {
        let Message::User(user) = request.messages.last().expect("user message") else {
            panic!("expected user message");
        };
        assert_eq!(
            user.content,
            vec![ContentBlock::Text(TextBlock {
                text: RuntimeString::from("expanded:").concat(argument),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })]
        );
    }
    let Message::User(user) = requests[3].messages.last().expect("user message") else {
        panic!("expected user message");
    };
    assert_eq!(
        user.content,
        vec![ContentBlock::Text(TextBlock {
            text: unknown_name_input,
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })]
    );
    let Message::User(user) = requests[4].messages.last().expect("user message") else {
        panic!("expected user message");
    };
    assert_eq!(user.content.len(), 2);
    assert!(matches!(
        &user.content[0],
        ContentBlock::Text(block)
            if block.text == RuntimeString::from("expanded:").concat(&media_argument)
    ));
    assert!(matches!(&user.content[1], ContentBlock::Image(_)));
}

#[tokio::test]
async fn host_display_metadata_stays_separate_from_model_text_and_in_history() {
    let requests: Arc<Mutex<Vec<ModelRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let mut config = default_config();
    config.memory.enabled = false;
    let session = mk_session_with_deps(
        recording_deps(requests.clone(), vec![text_turn("done")]),
        config,
        Vec::new(),
    );
    let metadata = exact_json!({
        "displayText": "/push-code",
        "slashCommand": { "name": "push-code", "kind": "skill" }
    });

    let result = session
        .run(
            Input::Text("expanded skill body".into()),
            TurnOptions {
                display_metadata: Some(metadata.clone()),
                ..TurnOptions::default()
            },
        )
        .await;

    assert_eq!(result.stop_reason, TerminalReason::Completed);
    let request = requests
        .lock()
        .unwrap()
        .first()
        .cloned()
        .expect("provider request");
    let request_user = request
        .messages
        .iter()
        .find_map(|message| match message {
            Message::User(user) => Some(user),
            _ => None,
        })
        .expect("provider user message");
    assert_eq!(
        request_user.content,
        vec![ContentBlock::Text(TextBlock {
            text: "expanded skill body".into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })]
    );
    let history = session.history();
    let display = history
        .iter()
        .find_map(|message| match message {
            Message::System(system) if system.subtype == SystemSubtype::UserDisplay => Some(system),
            _ => None,
        })
        .expect("display metadata record");
    let display_envelope = runtime_json::parse(&display.text).unwrap();
    assert_eq!(display_envelope.get("localRender"), Some(&metadata));
    let history_user_uuid = history
        .iter()
        .find_map(|message| match message {
            Message::User(user) => Some(user.uuid.clone()),
            _ => None,
        })
        .expect("history user message");
    assert_eq!(
        display_envelope.get("messageUuid"),
        Some(&RuntimeJson::String(history_user_uuid))
    );
    let user_index = history
        .iter()
        .position(|message| matches!(message, Message::User(_)))
        .unwrap();
    let display_index = history
        .iter()
        .position(|message| {
            matches!(
                message,
                Message::System(system) if system.subtype == SystemSubtype::UserDisplay
            )
        })
        .unwrap();
    assert_eq!(
        display_index,
        user_index + 1,
        "rollback drops the user and its following metadata together"
    );
}

#[tokio::test]
async fn main_memory_prompt_runs_memory_sync_plan_on_the_real_turn_path() {
    let dir = temp_store_dir();
    let host = Arc::new(SyncTestMemoryHost {
        layout: SyncTestMemoryLayout {
            base: dir
                .to_str()
                .expect("test memory directory is a scalar host path")
                .to_owned(),
        },
        plans: std::sync::Mutex::new(Vec::new()),
        applies: std::sync::Mutex::new(Vec::new()),
        apply_results: std::sync::Mutex::new(Vec::new()),
    });
    let session = Session::new(SessionInit {
        durable_permissions: None,
        process_context: crate::runtime_process::test_runtime_process_context(),
        user_config_dir: None,
        id: Some("s1".into()),
        session_role: None,
        parent_thread_id: None,
        provider: Arc::new(fake_provider()),
        deps: mk_deps(vec![text_turn("done")]),
        config: default_config(),
        network_allowed: None,
        trusted_windows_sandbox_context: None,
        context_window_override: None,
        pricing: None,
        tools: Vec::new(),
        refresh_tools: None,
        cwd: runtime_path_from_host_path(&std::env::temp_dir()),
        approval_handler: None,
        approval_reviewer: None,
        elicit_handler: None,
        mcp_elicitation_presenter: None,
        mcp_authorize_handler: None,
        mcp_oauth_redirect_uri: None,
        credential_store: None,
        hook_bus: None,
        rollout: None,
        store: None,
        mcp_clients: Vec::new(),
        plugin_mcp_servers: Vec::new(),
        slash_commands: Vec::new(),
        initial_history: None,
        initial_approval_context: None,
        initial_approval_reviews: None,
        initial_tool_result_budget_records: None,
        hidden_context_message_ids: None,
        initial_usage_ledger: None,
        initial_subagent_lifecycle: None,
        initial_background_processes: None,
        initial_memory_maintenance: None,
        initial_setup_lifecycle: None,
        initial_agent_memory_snapshot_decisions: None,
        forked_from_id: None,
        initial_checkpoints: None,
        system_prompt_provider: None,
        base_instructions: None,
        developer_instructions: None,
        root_agent_definition: None,
        observer: Some(None),
        logger: None,
        agents: None,
        skills: None,
        output_styles: None,
        skill_gate: None,
        current_worktree: None,
        prior_workflow_journal: None,
        initial_surfaced_tools: None,
        swarm: None,
        lsp_servers: None,
        shell_env: None,
        agent_memory: Some(host.clone()),
        agent_memory_snapshot_policy: None,
        browser_runtime: None,
        runtime_capabilities: RuntimeCapabilityRegistry::new(),
        selected_capability_roots: None,
        extension_runtime: ExtensionRuntimeSnapshot::default(),
    });

    let result = session
        .run(Input::Text("go".into()), TurnOptions::default())
        .await;
    assert_eq!(result.stop_reason, TerminalReason::Completed);

    let plans = host.plans.lock().unwrap();
    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].0.agent_type, "main");
    assert_eq!(plans[0].0.scope, AgentMemoryScope::Project);
    assert_eq!(
        plans[0].1.as_ref().unwrap().operations,
        vec![crate::memory::sync::MemorySyncOperation::KeepLocal {
            key: "alpha".into()
        }]
    );
    let applies = host.applies.lock().unwrap();
    assert_eq!(applies.len(), 1);
    assert_eq!(applies[0].0.agent_type, "main");
    assert_eq!(
        applies[0].1.operations,
        vec![crate::memory::sync::MemorySyncOperation::KeepLocal {
            key: "alpha".into()
        }]
    );
    let apply_results = host.apply_results.lock().unwrap();
    assert_eq!(apply_results.len(), 1);
    assert_eq!(apply_results[0].1.as_ref().unwrap().applied_operations, 1);
    std::fs::remove_dir_all(&dir).ok();
}

fn text_turn(text: &str) -> Vec<crate::types::provider::ModelStreamEvent> {
    vec![
        ev::message_start(None),
        ev::text(text),
        ev::stop(Some(ModelStopReason::EndTurn), Some(make_usage(None, None))),
    ]
}

fn provider_error_turn(message: &str) -> Vec<crate::types::provider::ModelStreamEvent> {
    vec![ev::error(crate::types::errors::ProviderError {
        kind: crate::types::errors::ProviderErrorKind::InvalidRequest,
        message: message.into(),
        status: Some(400),
        code: None,
        error_type: None,
        media_origins: None,
        media_repair_complete: false,
        credential_recovery: None,
        retry_after_ms: None,
        request_id: None,
    })]
}

fn dream_session(root: &Path, auto_dream: bool, turns: ScriptedTurns) -> (Arc<Session>, PathBuf) {
    dream_session_with_deps(root, auto_dream, mk_deps(turns), None)
}

fn dream_session_with_deps(
    root: &Path,
    auto_dream: bool,
    mut deps: EngineDeps,
    stream_idle_timeout_ms: Option<u64>,
) -> (Arc<Session>, PathBuf) {
    let mut config = default_config();
    config.agent_memory = true;
    config.auto_dream = auto_dream;
    config.max_retries = 0;
    deps.now = Arc::new(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    });
    dream_session_with_config_and_store(root, config, deps, stream_idle_timeout_ms, None)
}

fn dream_session_with_config_and_store(
    root: &Path,
    mut config: ResolvedConfig,
    deps: EngineDeps,
    stream_idle_timeout_ms: Option<u64>,
    store: Option<Arc<dyn SessionStore>>,
) -> (Arc<Session>, PathBuf) {
    config.agent_memory = true;
    config.max_retries = 0;
    if let Some(timeout) = stream_idle_timeout_ms {
        config.stream_idle_timeout_ms = timeout;
    }
    let host: Arc<dyn AgentMemoryHost> = Arc::new(SyncTestMemoryHost {
        layout: SyncTestMemoryLayout {
            base: root
                .to_str()
                .expect("test memory root is a scalar host path")
                .to_owned(),
        },
        plans: Mutex::new(Vec::new()),
        applies: Mutex::new(Vec::new()),
        apply_results: Mutex::new(Vec::new()),
    });
    let memory_dir = PathBuf::from(host.layout().dir_for("main", AgentMemoryScope::Project));
    std::fs::create_dir_all(&memory_dir).unwrap();
    let session = mk_session_with_provider_memory_and_store(
        Arc::new(fake_provider()),
        deps,
        config,
        vec![create_write_tool()],
        Some(host),
        None,
        store,
        None,
    );
    (session, memory_dir)
}

async fn seed_dream_store(store: &Arc<JsonlSessionStore>, id: &str, created_at: i64) {
    let rollout = store
        .create(SessionMeta {
            id: id.into(),
            created_at,
            cwd: "/tmp/dream-threshold-test".into(),
            model: "test-model".into(),
            title: None,
            originator: None,
            cli_version: None,
            source: None,
            role: None,
            parent_thread_id: None,
            forked_from_id: None,
            git: None,
            host_metadata: None,
        })
        .await
        .unwrap();
    rollout.shutdown().await.unwrap();
}

fn counting_dream_deps(calls: Arc<AtomicUsize>, now: i64) -> EngineDeps {
    let mut deps = mk_deps(Vec::new());
    deps.now = Arc::new(move || now);
    deps.call_model = Box::new(move |_request, signal| {
        calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(
                futures::stream::iter(text_turn(
                    "done\n<dream-summary>the dream sharpened: durable constraints emerged</dream-summary>",
                ))
                    .take_while(move |_| futures::future::ready(!signal.is_cancelled())),
            ) as ModelStream
    });
    deps
}

fn cooperative_hanging_memory_deps(now: i64, entered: Arc<AtomicBool>) -> EngineDeps {
    let mut deps = mk_deps(Vec::new());
    deps.now = Arc::new(move || now);
    deps.call_model = Box::new(move |_request, signal| {
        entered.store(true, Ordering::SeqCst);
        Box::pin(stream! {
            yield ev::message_start(None);
            signal.cancelled().await;
        }) as ModelStream
    });
    deps
}

struct MaintenanceStreamDropMarker(Arc<AtomicBool>);

impl Drop for MaintenanceStreamDropMarker {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn uncooperative_hanging_memory_deps(
    now: i64,
    entered: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    dropped: Arc<AtomicBool>,
) -> EngineDeps {
    let mut deps = mk_deps(Vec::new());
    deps.now = Arc::new(move || now);
    deps.call_model = Box::new(move |_request, _signal| {
        let entered = entered.clone();
        let dropped = dropped.clone();
        Box::pin(stream! {
            let _drop_marker = MaintenanceStreamDropMarker(dropped);
            if let Some(entered) = entered.lock().unwrap().take() {
                let _ = entered.send(());
            }
            yield ev::message_start(None);
            futures::future::pending::<()>().await;
        }) as ModelStream
    });
    deps
}

fn panicking_memory_deps(now: i64) -> EngineDeps {
    let mut deps = mk_deps(Vec::new());
    deps.now = Arc::new(move || now);
    deps.call_model = Box::new(move |_request, _signal| {
        panic!("maintenance provider panic");
    });
    deps
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[tokio::test]
async fn automatic_dream_obeys_configured_interval_gate() {
    let root = temp_store_dir();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    seed_dream_store(&store, "other-session", 1).await;
    let now = now_ms();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut config = default_config();
    config.auto_dream = true;
    config.dream.min_hours = 2.0;
    config.dream.min_sessions = 1;
    let (session, memory_dir) = dream_session_with_config_and_store(
        &root,
        config,
        counting_dream_deps(calls.clone(), now),
        None,
        Some(store.clone()),
    );
    let memory_dir = memory_dir.to_str().unwrap();
    let lock = try_acquire_consolidation_lock(
        memory_dir,
        std::process::id() as i64,
        (now - 60 * 60 * 1000) as f64,
    )
    .await
    .unwrap()
    .expect("seed consolidation owner");
    record_consolidation(memory_dir, (now - 60 * 60 * 1000) as f64, lock)
        .await
        .unwrap();
    session
        .memory_scheduler
        .last_dream_scan_at
        .store(now, Ordering::SeqCst);

    session.run_auto_dream(memory_dir, store, now).await;

    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let snapshot = session.memory_maintenance_snapshot().unwrap();
    assert_eq!(snapshot.auto_dream_runs.len(), 1);
    assert_eq!(
        snapshot.auto_dream_runs[0].status,
        MemoryMaintenanceStatus::Skipped
    );
    assert_eq!(
        snapshot.auto_dream_runs[0].reason,
        Some(AutoDreamRunReason::TimeGate)
    );
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn automatic_dream_rechecks_the_authoritative_epoch_after_lock_acquisition() {
    let root = temp_store_dir();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    seed_dream_store(&store, "other-session", 1).await;
    let now = now_ms();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut config = default_config();
    config.auto_dream = true;
    config.dream.min_hours = 2.0;
    config.dream.min_sessions = 1;

    // Both Sessions bind the same physical memory root at construction.
    // B pauses after its stale optimistic scan, while A advances the
    // epoch through its independently pinned boundary.
    let (session_b, memory_dir) = dream_session_with_config_and_store(
        &root,
        config.clone(),
        counting_dream_deps(calls.clone(), now),
        None,
        Some(store.clone()),
    );
    let (session_a, session_a_memory_dir) = dream_session_with_config_and_store(
        &root,
        config,
        counting_dream_deps(Arc::new(AtomicUsize::new(0)), now),
        None,
        Some(store.clone()),
    );
    assert_eq!(memory_dir, session_a_memory_dir);

    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *session_b.dream_before_lock_hook.lock().unwrap() = Some((entered.clone(), release.clone()));
    let memory_dir_text = memory_dir.to_string_lossy().into_owned();
    let session_b_task = session_b.clone();
    let store_b = store.clone();
    let memory_dir_b = memory_dir_text.clone();
    let run_b = tokio::spawn(async move {
        session_b_task
            .run_auto_dream(&memory_dir_b, store_b, now)
            .await;
    });
    entered.notified().await;

    let boundary_a = session_a
        .memory_roots
        .pinned_root
        .clone()
        .expect("Session A construction must pin the shared memory root");
    let lock_a =
        try_acquire_consolidation_lock_pinned(boundary_a, std::process::id() as i64, now as f64)
            .await
            .unwrap()
            .expect("Session A owns the shared consolidation epoch");
    record_consolidation(&memory_dir_text, now as f64, lock_a)
        .await
        .unwrap();

    release.notify_one();
    run_b.await.unwrap();

    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let snapshot = session_b.memory_maintenance_snapshot().unwrap();
    assert_eq!(snapshot.auto_dream_runs.len(), 1);
    assert_eq!(
        snapshot.auto_dream_runs[0].status,
        MemoryMaintenanceStatus::Skipped
    );
    assert_eq!(
        snapshot.auto_dream_runs[0].reason,
        Some(AutoDreamRunReason::TimeGate)
    );
    assert_eq!(snapshot.auto_dream_runs[0].last_consolidated_at, Some(now));
    assert_eq!(
        read_last_consolidated_at(&memory_dir_text).await,
        now as f64,
        "Session B must roll back its speculative acquisition without changing A's epoch"
    );
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn automatic_dream_recounts_sessions_when_the_locked_epoch_changed() {
    let root = temp_store_dir();
    let sessions_dir = root.join("sessions");
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(sessions_dir.to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    seed_dream_store(&store, "pre-epoch-session", 1).await;
    let now = now_ms();
    let mut config = default_config();
    config.auto_dream = true;
    config.dream.min_hours = 0.01;
    config.dream.min_sessions = 1;
    let advanced_epoch = now - config.dream.min_interval_ms() - 1_000;
    let old_session_time =
        std::time::UNIX_EPOCH + std::time::Duration::from_millis((advanced_epoch - 1_000) as u64);
    for entry in std::fs::read_dir(&sessions_dir).unwrap() {
        let path = entry.unwrap().path();
        if path
            .extension()
            .is_some_and(|extension| extension == "jsonl")
        {
            // Opened for write: setting timestamps needs write access on Windows
            // (FILE_WRITE_ATTRIBUTES), while a read-only handle is enough on unix.
            std::fs::OpenOptions::new()
                .write(true)
                .open(path)
                .unwrap()
                .set_times(std::fs::FileTimes::new().set_modified(old_session_time))
                .unwrap();
        }
    }

    let calls = Arc::new(AtomicUsize::new(0));
    let (session_b, memory_dir) = dream_session_with_config_and_store(
        &root,
        config.clone(),
        counting_dream_deps(calls.clone(), now),
        None,
        Some(store.clone()),
    );
    let (session_a, session_a_memory_dir) = dream_session_with_config_and_store(
        &root,
        config,
        counting_dream_deps(Arc::new(AtomicUsize::new(0)), now),
        None,
        Some(store.clone()),
    );
    assert_eq!(memory_dir, session_a_memory_dir);

    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *session_b.dream_before_lock_hook.lock().unwrap() = Some((entered.clone(), release.clone()));
    let memory_dir_text = memory_dir.to_string_lossy().into_owned();
    let session_b_task = session_b.clone();
    let store_b = store.clone();
    let memory_dir_b = memory_dir_text.clone();
    let run_b = tokio::spawn(async move {
        session_b_task
            .run_auto_dream(&memory_dir_b, store_b, now)
            .await;
    });
    entered.notified().await;

    let lock_a = try_acquire_consolidation_lock_pinned(
        session_a.memory_roots.pinned_root().unwrap(),
        std::process::id() as i64,
        advanced_epoch as f64,
    )
    .await
    .unwrap()
    .expect("Session A advances the shared epoch");
    record_consolidation(&memory_dir_text, advanced_epoch as f64, lock_a)
        .await
        .unwrap();
    release.notify_one();
    run_b.await.unwrap();

    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let snapshot = session_b.memory_maintenance_snapshot().unwrap();
    assert_eq!(snapshot.auto_dream_runs.len(), 1);
    assert_eq!(
        snapshot.auto_dream_runs[0].reason,
        Some(AutoDreamRunReason::EmptyInput),
        "the session that was eligible before A's epoch must not count after it"
    );
    assert_eq!(snapshot.auto_dream_runs[0].sessions_reviewing, 0);
    assert_eq!(
        snapshot.auto_dream_runs[0].last_consolidated_at,
        Some(advanced_epoch)
    );
    assert!(
        (read_last_consolidated_at(&memory_dir_text).await - advanced_epoch as f64).abs() < 1_000.0
    );
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn automatic_dream_obeys_configured_session_count_gate() {
    let root = temp_store_dir();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    seed_dream_store(&store, "other-session", 1).await;
    let now = now_ms();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut config = default_config();
    config.auto_dream = true;
    config.dream.min_hours = 0.01;
    config.dream.min_sessions = 2;
    let (session, memory_dir) = dream_session_with_config_and_store(
        &root,
        config,
        counting_dream_deps(calls.clone(), now),
        None,
        Some(store.clone()),
    );

    session
        .run_auto_dream(memory_dir.to_str().unwrap(), store, now)
        .await;

    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(!memory_dir.join(".consolidate-lock").exists());
    let snapshot = session.memory_maintenance_snapshot().unwrap();
    assert_eq!(
        snapshot.auto_dream_runs[0].reason,
        Some(AutoDreamRunReason::SessionGate)
    );
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn automatic_dream_runs_only_after_both_configured_gates_pass() {
    let root = temp_store_dir();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    seed_dream_store(&store, "other-session", 1).await;
    let now = now_ms();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut config = default_config();
    config.auto_dream = true;
    config.dream.min_hours = 720.0;
    config.dream.min_sessions = 1;
    let (session, memory_dir) = dream_session_with_config_and_store(
        &root,
        config,
        counting_dream_deps(calls.clone(), now),
        None,
        Some(store.clone()),
    );

    session
        .run_auto_dream(memory_dir.to_str().unwrap(), store, now)
        .await;

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(
        (read_last_consolidated_at(memory_dir.to_str().unwrap()).await - now as f64).abs() < 1000.0
    );
    let snapshot = session.memory_maintenance_snapshot().unwrap();
    assert_eq!(snapshot.auto_dream_runs.len(), 1);
    assert_eq!(
        snapshot.auto_dream_runs[0].status,
        MemoryMaintenanceStatus::Completed
    );
    assert_eq!(snapshot.auto_dream_runs[0].sessions_reviewing, 1);
    assert_eq!(snapshot.auto_dream_runs[0].last_consolidated_at, Some(now));
    assert_eq!(snapshot.auto_dream_runs[0].turn_count, 1);
    assert_eq!(snapshot.auto_dream_runs[0].turns.len(), 1);
    assert_eq!(
        snapshot.auto_dream_runs[0].summary,
        Some("the dream sharpened: durable constraints emerged".into())
    );
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn automatic_dream_reports_empty_lock_shutdown_and_concurrency_gates() {
    let root = temp_store_dir();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let now = now_ms();
    let mut config = default_config();
    config.auto_dream = true;
    config.dream.min_hours = 0.01;
    config.dream.min_sessions = 1;
    let (empty_session, empty_memory_dir) = dream_session_with_config_and_store(
        &root,
        config.clone(),
        counting_dream_deps(Arc::new(AtomicUsize::new(0)), now),
        None,
        Some(store.clone()),
    );
    empty_session
        .run_auto_dream(empty_memory_dir.to_str().unwrap(), store.clone(), now)
        .await;
    assert_eq!(
        empty_session
            .memory_maintenance_snapshot()
            .unwrap()
            .auto_dream_runs[0]
            .reason,
        Some(AutoDreamRunReason::EmptyInput)
    );

    let held_lock = try_acquire_consolidation_lock(
        empty_memory_dir.to_str().unwrap(),
        std::process::id() as i64,
        (now - 60_000) as f64,
    )
    .await
    .unwrap()
    .expect("test lock owner");
    seed_dream_store(
        &store,
        "eligible-session",
        now + DREAM_SCAN_INTERVAL_MS + 30_000,
    )
    .await;
    empty_session
        .run_auto_dream(
            empty_memory_dir.to_str().unwrap(),
            store.clone(),
            now + DREAM_SCAN_INTERVAL_MS + 60_000,
        )
        .await;
    assert_eq!(
        empty_session
            .memory_maintenance_snapshot()
            .unwrap()
            .auto_dream_runs
            .last()
            .unwrap()
            .reason,
        Some(AutoDreamRunReason::LockHeld)
    );
    drop(held_lock);

    let (concurrent, _) = dream_session_with_config_and_store(
        &root.join("concurrent"),
        config.clone(),
        counting_dream_deps(Arc::new(AtomicUsize::new(0)), now),
        None,
        Some(store),
    );
    concurrent
        .memory_scheduler
        .dream_in_progress
        .store(true, Ordering::SeqCst);
    concurrent.maybe_auto_dream();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if concurrent
                .memory_maintenance_snapshot()
                .unwrap()
                .auto_dream_runs
                .first()
                .is_some_and(|run| run.reason == Some(AutoDreamRunReason::AlreadyRunning))
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("standalone AutoDream skip must cross its durability barrier");
    assert_eq!(
        concurrent
            .memory_maintenance_snapshot()
            .unwrap()
            .auto_dream_runs[0]
            .reason,
        Some(AutoDreamRunReason::AlreadyRunning)
    );

    let (shutdown, _) = dream_session_with_config_and_store(
        &root.join("shutdown"),
        config,
        counting_dream_deps(Arc::new(AtomicUsize::new(0)), now),
        None,
        None,
    );
    shutdown.lifecycle.mark_disposed();
    shutdown.maybe_auto_dream();
    assert!(
        shutdown
            .memory_maintenance_snapshot()
            .unwrap()
            .auto_dream_runs
            .is_empty(),
        "shutdown closes maintenance admission; a late scheduler call is not a real attempt"
    );
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn automatic_dream_failure_is_authoritative_and_rolls_back_lock() {
    let root = temp_store_dir();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    seed_dream_store(&store, "eligible-session", 1).await;
    let now = now_ms();
    let mut config = default_config();
    config.auto_dream = true;
    config.dream.min_hours = 0.01;
    config.dream.min_sessions = 1;
    let (session, memory_dir) = dream_session_with_config_and_store(
        &root,
        config,
        mk_deps(vec![provider_error_turn("dream provider failed")]),
        None,
        Some(store.clone()),
    );

    session
        .run_auto_dream(memory_dir.to_str().unwrap(), store, now)
        .await;

    let snapshot = session.memory_maintenance_snapshot().unwrap();
    assert_eq!(snapshot.auto_dream_runs.len(), 1);
    let run = &snapshot.auto_dream_runs[0];
    assert_eq!(run.status, MemoryMaintenanceStatus::Failed);
    assert_eq!(run.reason, Some(AutoDreamRunReason::ForkFailure));
    assert!(run
        .error
        .as_ref()
        .is_some_and(|error| error.contains("dream provider failed")));
    assert_eq!(
        read_last_consolidated_at(memory_dir.to_str().unwrap()).await,
        0.0
    );
    let reacquired = try_acquire_consolidation_lock(
        memory_dir.to_str().unwrap(),
        std::process::id() as i64,
        (now + 1) as f64,
    )
    .await
    .unwrap();
    assert!(
        reacquired.is_some(),
        "failed Dream must release its OS lock"
    );
    drop(reacquired);
    std::fs::remove_dir_all(root).ok();
}

/// A consolidation pass over a memory that has never been consolidated probes an
/// index that is not there yet (MEMORY.md is created BY a pass, not before one).
/// That probe is normal exploration: when the turn then reaches its own terminal
/// completion the run is COMPLETED, and the lock's mtime advances so the
/// configured interval starts governing the next pass.
///
/// Failing the run on the probe instead made every pass over an empty memory fail
/// permanently — the pass that would have grown the index could never finish, so
/// the mtime stayed 0, which left the interval gate disabled (`last > 0` never
/// held) and re-fired a doomed fork after every single turn.
#[tokio::test]
async fn automatic_dream_completes_when_a_missing_index_probe_fails_mid_turn() {
    let root = temp_store_dir();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    seed_dream_store(&store, "probe-miss-session", 1).await;
    let now = now_ms();
    let mut config = default_config();
    config.agent_memory = true;
    config.auto_dream = true;
    config.dream.min_hours = 0.01;
    config.dream.min_sessions = 1;
    config.max_retries = 0;
    let host: Arc<dyn AgentMemoryHost> = Arc::new(SyncTestMemoryHost {
        layout: SyncTestMemoryLayout {
            base: root.to_string_lossy().into_owned(),
        },
        plans: Mutex::new(Vec::new()),
        applies: Mutex::new(Vec::new()),
        apply_results: Mutex::new(Vec::new()),
    });
    let memory_dir = PathBuf::from(host.layout().dir_for("main", AgentMemoryScope::Project));
    std::fs::create_dir_all(&memory_dir).unwrap();
    // The memory is EMPTY: no index for the orient phase to read.
    assert!(!memory_dir.join("MEMORY.md").exists());
    // Turn 1 touches the index and the tool reports a failure; turn 2 finishes.
    let mut probing_turn = vec![ev::message_start(None)];
    probing_turn.extend(exact_tool_use_events(
        "orient-probe",
        "write",
        &exact_json!({"path": "MEMORY.md", "content": "index"}),
    ));
    probing_turn.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let session = mk_session_with_provider_memory_and_store(
        Arc::new(fake_provider()),
        mk_deps(vec![probing_turn, text_turn("nothing to consolidate")]),
        config,
        vec![Arc::new(FailedMemoryWriteTool {
            schema: JsonSchema::new(),
        })],
        Some(host),
        None,
        Some(store.clone()),
        None,
    );

    session
        .run_auto_dream(memory_dir.to_str().unwrap(), store, now)
        .await;

    let snapshot = session.memory_maintenance_snapshot().unwrap();
    assert_eq!(snapshot.auto_dream_runs.len(), 1);
    let run = &snapshot.auto_dream_runs[0];
    assert_eq!(
        run.status,
        MemoryMaintenanceStatus::Completed,
        "a probe that missed must not fail a pass that finished, got reason {:?} error {:?}",
        run.reason,
        run.error
    );
    assert_eq!(run.reason, None);
    assert_eq!(run.error, None);
    // The successful pass advances the time gate, so the configured interval now
    // governs the next one instead of every turn re-firing a doomed fork.
    assert!(
        read_last_consolidated_at(memory_dir.to_str().unwrap()).await > 0.0,
        "a completed consolidation must advance the last-consolidated mtime"
    );
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn automatic_dream_provider_panic_is_terminal_and_releases_single_flight_and_lock() {
    let root = temp_store_dir();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let now = 2_000_000;
    seed_dream_store(&store, "panic-eligible-session", now).await;
    let mut config = default_config();
    config.auto_dream = true;
    config.dream.min_hours = 0.01;
    config.dream.min_sessions = 1;
    let (session, memory_dir) = dream_session_with_config_and_store(
        &root,
        config,
        panicking_memory_deps(now),
        None,
        Some(store),
    );

    session.maybe_auto_dream();
    tokio::time::timeout(CROSS_THREAD_LIVENESS_TIMEOUT, async {
        while session
            .memory_scheduler
            .dream_in_progress
            .load(Ordering::SeqCst)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("panicking automatic Dream worker must terminate");

    let snapshot = session.memory_maintenance_snapshot().unwrap();
    assert_eq!(snapshot.auto_dream_runs.len(), 1);
    let run = &snapshot.auto_dream_runs[0];
    assert_eq!(run.status, MemoryMaintenanceStatus::Failed);
    assert_eq!(run.reason, Some(AutoDreamRunReason::ForkFailure));
    assert!(
        run.error
            .as_ref()
            .is_some_and(|error| error.contains("panic")),
        "provider panic must remain visible in the terminal lifecycle: {:?}",
        run.error
    );
    assert!(!session
        .memory_scheduler
        .dream_in_progress
        .load(Ordering::SeqCst));
    let reacquired = try_acquire_consolidation_lock(
        memory_dir.to_str().unwrap(),
        std::process::id() as i64,
        (now + 1) as f64,
    )
    .await
    .unwrap();
    assert!(reacquired.is_some());
    drop(reacquired);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn shutdown_cancels_automatic_dream_and_restores_the_prior_lock_epoch() {
    let root = temp_store_dir();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let now = 2_000_000;
    let prior = now - 100_000;
    seed_dream_store(&store, "eligible-shutdown-session", now).await;
    let entered = Arc::new(AtomicBool::new(false));
    let mut config = default_config();
    config.auto_dream = true;
    config.dream.min_hours = 0.01;
    config.dream.min_sessions = 1;
    let (session, memory_dir) = dream_session_with_config_and_store(
        &root,
        config,
        cooperative_hanging_memory_deps(now, entered.clone()),
        None,
        Some(store),
    );
    let memory_dir_text = memory_dir.to_str().unwrap();
    let seed_lock =
        try_acquire_consolidation_lock(memory_dir_text, std::process::id() as i64, prior as f64)
            .await
            .unwrap()
            .expect("seed consolidation owner");
    record_consolidation(memory_dir_text, prior as f64, seed_lock)
        .await
        .unwrap();
    let live_events = Arc::new(Mutex::new(Vec::new()));
    session.set_ordered_memory_maintenance_event_sink(Some({
        let live_events = live_events.clone();
        Arc::new(move |event| live_events.lock().unwrap().push(event))
    }));

    session.maybe_auto_dream();
    tokio::time::timeout(MEMORY_MAINTENANCE_ENTRY_TIMEOUT, async {
        loop {
            let started = session
                .memory_maintenance_snapshot()
                .unwrap()
                .auto_dream_runs
                .iter()
                .any(|run| run.status == MemoryMaintenanceStatus::Started);
            if started && entered.load(Ordering::SeqCst) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("automatic Dream should acquire its lock and enter the provider");
    let started_id = session
        .memory_maintenance_snapshot()
        .unwrap()
        .auto_dream_runs[0]
        .dream_run_id
        .clone();

    session.shutdown_and_wait().await.unwrap();

    let snapshot = session.memory_maintenance_snapshot().unwrap();
    assert_eq!(snapshot.auto_dream_runs.len(), 1);
    let terminal = &snapshot.auto_dream_runs[0];
    assert_eq!(terminal.dream_run_id, started_id);
    assert_eq!(terminal.status, MemoryMaintenanceStatus::Failed);
    assert_eq!(terminal.reason, Some(AutoDreamRunReason::Shutdown));
    assert_eq!(
        read_last_consolidated_at(memory_dir_text).await,
        prior as f64
    );
    let reacquired = try_acquire_consolidation_lock(
        memory_dir_text,
        std::process::id() as i64,
        (now + 1) as f64,
    )
    .await
    .unwrap();
    assert!(
        reacquired.is_some(),
        "shutdown must release the Dream OS lock"
    );
    drop(reacquired);
    let events = live_events.lock().unwrap();
    assert_eq!(events.len(), 2);
    let MemoryMaintenanceEvent::AutoDream { run: started } = &events[0] else {
        panic!("expected automatic Dream start");
    };
    let MemoryMaintenanceEvent::AutoDream { run: failed } = &events[1] else {
        panic!("expected automatic Dream shutdown terminal");
    };
    assert_eq!(started.dream_run_id, failed.dream_run_id);
    assert_eq!(started.status, MemoryMaintenanceStatus::Started);
    assert_eq!(failed.status, MemoryMaintenanceStatus::Failed);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test(start_paused = true)]
async fn shutdown_force_aborts_uncooperative_auto_dream_before_publishing_terminal() {
    let root = temp_store_dir();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let now = 2_000_000;
    let prior = now - 100_000;
    seed_dream_store(&store, "hard-timeout-eligible-session", now).await;
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let provider_dropped = Arc::new(AtomicBool::new(false));
    let mut config = default_config();
    config.auto_dream = true;
    config.dream.min_hours = 0.01;
    config.dream.min_sessions = 1;
    let (session, memory_dir) = dream_session_with_config_and_store(
        &root,
        config,
        uncooperative_hanging_memory_deps(
            now,
            Arc::new(Mutex::new(Some(entered_tx))),
            provider_dropped.clone(),
        ),
        None,
        Some(store),
    );
    let memory_dir_text = memory_dir.to_str().unwrap();
    let seed_lock =
        try_acquire_consolidation_lock(memory_dir_text, std::process::id() as i64, prior as f64)
            .await
            .unwrap()
            .expect("seed consolidation owner");
    record_consolidation(memory_dir_text, prior as f64, seed_lock)
        .await
        .unwrap();
    let live_events = Arc::new(Mutex::new(Vec::new()));
    let terminal_observed_released_lock = Arc::new(AtomicBool::new(false));
    session.set_ordered_memory_maintenance_event_sink(Some({
        let live_events = live_events.clone();
        let lock_path = memory_dir.join(".consolidate-lock");
        let terminal_observed_released_lock = terminal_observed_released_lock.clone();
        Arc::new(move |event| {
            if matches!(
                &event,
                MemoryMaintenanceEvent::AutoDream { run }
                    if run.status == MemoryMaintenanceStatus::Failed
            ) {
                let file = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&lock_path)
                    .expect("terminal callback can open the real lock file");
                let acquired = fs2::FileExt::try_lock_exclusive(&file).is_ok();
                if acquired {
                    let _ = fs2::FileExt::unlock(&file);
                }
                terminal_observed_released_lock.store(acquired, Ordering::SeqCst);
            }
            live_events.lock().unwrap().push(event);
        })
    }));

    session.maybe_auto_dream();
    entered_rx
        .await
        .expect("automatic Dream should hold its real lock inside the provider stream");
    assert_eq!(
        read_last_consolidated_at(memory_dir_text).await,
        prior as f64,
        "acquisition must preserve the previous successful epoch while held"
    );

    let closing_session = session.clone();
    let close = tokio::spawn(async move { closing_session.shutdown_and_wait().await });
    while !session.maintenance_is_closing() {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(std::time::Duration::from_millis(
        SESSION_SHUTDOWN_GRACE_MS + 1,
    ))
    .await;
    let error = close
        .await
        .expect("shutdown owner task")
        .expect_err("an uncooperative provider must report the hard-timeout path");
    assert!(error.contains(MAINTENANCE_SHUTDOWN_ERROR), "{error}");
    assert!(
        terminal_observed_released_lock.load(Ordering::SeqCst),
        "the registered Dream worker must drop its real lock before publishing terminal state"
    );
    assert!(
        provider_dropped.load(Ordering::SeqCst),
        "dropping the restricted fork must abort and join its owned provider query"
    );

    let snapshot = session.memory_maintenance_snapshot().unwrap();
    assert_eq!(snapshot.auto_dream_runs.len(), 1);
    let terminal = &snapshot.auto_dream_runs[0];
    assert_eq!(terminal.status, MemoryMaintenanceStatus::Failed);
    assert_eq!(terminal.reason, Some(AutoDreamRunReason::Shutdown));
    assert_eq!(
        read_last_consolidated_at(memory_dir_text).await,
        prior as f64
    );
    let reacquired = try_acquire_consolidation_lock(
        memory_dir_text,
        std::process::id() as i64,
        (now + 1) as f64,
    )
    .await
    .unwrap();
    assert!(
        reacquired.is_some(),
        "force-abort must drop the real ConsolidationLock before shutdown completion"
    );
    drop(reacquired);

    let events = live_events.lock().unwrap();
    assert_eq!(events.len(), 2, "exactly Started plus one terminal event");
    let MemoryMaintenanceEvent::AutoDream { run: started } = &events[0] else {
        panic!("expected automatic Dream start");
    };
    let MemoryMaintenanceEvent::AutoDream { run: failed } = &events[1] else {
        panic!("expected automatic Dream shutdown terminal");
    };
    assert_eq!(started.dream_run_id, failed.dream_run_id);
    assert_eq!(started.status, MemoryMaintenanceStatus::Started);
    assert_eq!(failed.status, MemoryMaintenanceStatus::Failed);
    assert_eq!(failed.reason, Some(AutoDreamRunReason::Shutdown));
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn automatic_dream_store_scan_errors_are_authoritative_failures() {
    let root = temp_store_dir();
    let sessions_file = root.join("sessions-is-a-file");
    std::fs::write(&sessions_file, "not a directory").unwrap();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(sessions_file.to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let now = now_ms();
    let mut config = default_config();
    config.auto_dream = true;
    config.dream.min_hours = 0.01;
    config.dream.min_sessions = 1;
    let (session, memory_dir) = dream_session_with_config_and_store(
        &root,
        config,
        counting_dream_deps(Arc::new(AtomicUsize::new(0)), now),
        None,
        Some(store.clone()),
    );

    session
        .run_auto_dream(memory_dir.to_str().unwrap(), store, now)
        .await;

    let snapshot = session.memory_maintenance_snapshot().unwrap();
    assert_eq!(snapshot.auto_dream_runs.len(), 1);
    let run = &snapshot.auto_dream_runs[0];
    assert_eq!(run.status, MemoryMaintenanceStatus::Failed);
    assert_eq!(run.reason, Some(AutoDreamRunReason::StoreError));
    assert!(run.error.as_ref().is_some_and(|error| !error.is_empty()));
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn manual_dream_bypasses_configured_time_and_session_gates() {
    let root = temp_store_dir();
    let now = now_ms();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut config = default_config();
    config.auto_dream = true;
    config.dream.min_hours = 720.0;
    config.dream.min_sessions = 100;
    let (session, memory_dir) = dream_session_with_config_and_store(
        &root,
        config,
        counting_dream_deps(calls.clone(), now),
        None,
        None,
    );

    let mut task = session.dream_now().await.unwrap();
    let completion = task.done.take().unwrap().await.unwrap();

    assert!(completion.ok, "{:?}", completion.reason);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(memory_dir.join(".consolidate-lock").exists());
    std::fs::remove_dir_all(root).ok();
}

struct RecordedMemoryHttpProvider {
    inner: Arc<dyn ModelProvider>,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
}

impl ModelProvider for RecordedMemoryHttpProvider {
    fn info(&self) -> &ProviderInfo {
        self.inner.info()
    }

    fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
        self.inner.capabilities(model)
    }

    fn model_metadata(&self, model: &RuntimeString) -> crate::types::provider::ModelCapabilities {
        self.inner.model_metadata(model)
    }

    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        self.requests.lock().unwrap().push(request.clone());
        self.inner.stream(request, signal)
    }

    fn stream_with_model_metadata(
        &self,
        request: ModelRequest,
        metadata: crate::types::provider::ModelCapabilities,
        raw_effort: Option<RuntimeString>,
        total_attempts: u32,
        signal: AbortSignal,
    ) -> ModelStream {
        self.requests.lock().unwrap().push(request.clone());
        self.inner
            .stream_with_model_metadata(request, metadata, raw_effort, total_attempts, signal)
    }
}

#[tokio::test]
async fn memory_feature_target_rides_own_http_provider_and_actual_limits() {
    use super::{FeatureSlot, FeatureTargets, InferenceTarget, InferenceTargetState};
    use crate::provider::anthropic::{AnthropicProvider, AnthropicProviderOptions};
    use crate::types::provider::ModelCapabilities;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::<serde_json::Value>::new()));
    let captured = requests.clone();
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (socket, _) = listener.accept().await.unwrap();
            let mut reader = tokio::io::BufReader::new(socket);
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            let mut auth = String::new();
            let mut length = 0usize;
            loop {
                line.clear();
                reader.read_line(&mut line).await.unwrap();
                if line == "\r\n" {
                    break;
                }
                if let Some((name, value)) = line.split_once(':') {
                    if name.eq_ignore_ascii_case("content-length") {
                        length = value.trim().parse().unwrap();
                    }
                    if name.eq_ignore_ascii_case("x-api-key") {
                        auth = value.trim().to_string();
                    }
                }
            }
            let mut body = vec![0u8; length];
            reader.read_exact(&mut body).await.unwrap();
            let mut value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            value["_wireAuth"] = serde_json::json!(auth);
            captured.lock().unwrap().push(value.clone());
            let events = [
                serde_json::json!({"type":"message_start","message":{"id":"memory","type":"message","role":"assistant","model":value["model"],"content":[],"usage":{"input_tokens":11,"output_tokens":0}}}),
                serde_json::json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
                serde_json::json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Memory maintenance completed."}}),
                serde_json::json!({"type":"content_block_stop","index":0}),
                serde_json::json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":7}}),
                serde_json::json!({"type":"message_stop"}),
            ];
            let body = events
                .iter()
                .map(|event| {
                    format!(
                        "event: {}\ndata: {}\n\n",
                        event["type"].as_str().unwrap(),
                        event
                    )
                })
                .collect::<String>();
            let reply = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",body.len(),body);
            reader.get_mut().write_all(reply.as_bytes()).await.unwrap();
            reader.get_mut().shutdown().await.unwrap();
        }
    });
    let root = temp_store_dir();
    let mut config = default_config();
    config.auto_dream = true;
    config.context_window = 100_000;
    config.fallback_models = vec!["main-fallback".into()];
    let (session, memory_dir) =
        dream_session_with_config_and_store(&root, config, mk_deps(Vec::new()), None, None);
    let main_config_before = session.inference_snapshot().config.clone();
    assert_eq!(main_config_before.context_window, 100_000);
    let main_target_before = session.target_for_selection(None).unwrap();
    assert_eq!(
        main_target_before.config.context_window,
        main_target_before.provider.capabilities(&main_target_before.model).context_window,
        "the fixture has no explicit window override; the resolved main target follows its provider"
    );
    let low: Arc<dyn ModelProvider> = Arc::new(AnthropicProvider::new(AnthropicProviderOptions {
        api_key: Some("memory-low-key".into()),
        base_url: Some(url),
        default_model: Some("memory-low-wire".into()),
        capabilities: Some(ModelCapabilities {
            tools: Some(true),
            context_window: Some(20_000),
            max_output_tokens: Some(333),
            image_limits: None,
            max_media_blocks: None,
            ..Default::default()
        }),
        ..Default::default()
    }));
    let model_requests = Arc::new(Mutex::new(Vec::new()));
    let low: Arc<dyn ModelProvider> = Arc::new(RecordedMemoryHttpProvider {
        inner: low,
        requests: model_requests.clone(),
    });
    session
        .install_feature_targets(FeatureTargets {
            memory: Some(InferenceTargetState::Ready(InferenceTarget {
                source: "custom".into(),
                model_id: "memory-low-entry".into(),
                model: "memory-low-wire".into(),
                provider: low,
                pricing: Vec::new(),
                review_requirements: None,
            })),
            ..Default::default()
        })
        .unwrap();
    let target = session
        .inference_snapshot()
        .for_feature(FeatureSlot::Memory)
        .unwrap();
    assert_eq!(target.config.context_window, 20_000);
    assert_eq!(target.config.model_context_window, Some(20_000));
    assert_eq!(
        crate::context::compact::policy::hard_block_threshold(&target.config),
        19_000.0
    );
    assert_eq!(
        session.target_for_selection(None).unwrap().config,
        main_target_before.config,
        "the memory feature target must leave the resolved main target unchanged"
    );
    assert_eq!(
        &session.inference_snapshot().config,
        &main_config_before,
        "the memory feature target must leave the raw main configuration unchanged"
    );
    assert_eq!(target.config.max_output_tokens, Some(333));
    assert!(target.config.fallback_models.is_empty());
    assert_eq!(
        session
            .target_for_selection(None)
            .unwrap()
            .config
            .fallback_models,
        vec![RuntimeString::from("main-fallback")]
    );
    let extracted = session
        .run_extraction_fork(memory_dir.to_str().unwrap(), &[], AbortSignal::new())
        .await;
    assert!(extracted.is_ok(), "{extracted:?}");
    assert_eq!(extracted.unwrap().turn_count, 1);
    let mut task = session.dream_now().await.unwrap();
    let completion = task.done.take().unwrap().await.unwrap();
    assert!(completion.ok, "{completion:?}");
    assert_eq!(completion.turn_count, Some(1));
    tokio::time::timeout(std::time::Duration::from_secs(10), server)
        .await
        .unwrap()
        .unwrap();
    {
        let observed = model_requests.lock().unwrap();
        assert_eq!(observed.len(), 2, "one actual request per maintenance fork");
        for request in observed.iter() {
            let prefix =
                crate::context::tokens::estimate_prefix_tokens(&request.system, &request.tools);
            let used = crate::context::tokens::estimate_context_tokens_with_prefix(
                &request.messages,
                prefix,
            );
            let limit = crate::context::compact::policy::usable_context_window(&target.config);
            assert!(used < limit, "the actual maintenance payload must fit below its declared 20k model's admission threshold: prefix={prefix} total={used} limit={limit}");
        }
    }
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests
        .iter()
        .all(|request| request["model"] == "memory-low-wire"
            && request["_wireAuth"] == "memory-low-key"
            && request["max_tokens"] == 333));
    assert!(requests[0]
        .to_string()
        .contains("Distill only high-signal, DURABLE facts worth"));
    assert!(requests[1].to_string().contains("MEMORY.md"));
    assert!(session
        .cost_snapshot()
        .models
        .iter()
        .any(|model| model.totals.model == "custom:memory-low-entry"));
    drop(requests);
    assert_eq!(
        session.target_for_selection(None).unwrap().config,
        main_target_before.config,
        "the memory feature target must leave the resolved main target unchanged"
    );
    assert_eq!(
        &session.inference_snapshot().config,
        &main_config_before,
        "the memory feature target must leave the raw main configuration unchanged"
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn restricted_memory_feature_target_reports_provider_window_rejections_without_fallback() {
    use super::{FeatureSlot, FeatureTargets, InferenceTarget, InferenceTargetState};
    use crate::provider::anthropic::{AnthropicProvider, AnthropicProviderOptions};
    use crate::types::provider::ModelCapabilities;

    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let http_requests = Arc::new(Mutex::new(Vec::<serde_json::Value>::new()));
    let captured = http_requests.clone();
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (socket, _) = listener.accept().await.unwrap();
            let mut reader = tokio::io::BufReader::new(socket);
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            let mut length = 0usize;
            let mut auth = String::new();
            loop {
                line.clear();
                reader.read_line(&mut line).await.unwrap();
                if line == "\r\n" {
                    break;
                }
                if let Some((name, value)) = line.split_once(':') {
                    if name.eq_ignore_ascii_case("content-length") {
                        length = value.trim().parse().unwrap();
                    }
                    if name.eq_ignore_ascii_case("x-api-key") {
                        auth = value.trim().to_string();
                    }
                }
            }
            assert!(length < 4_000_000);
            let mut bytes = vec![0; length];
            reader.read_exact(&mut bytes).await.unwrap();
            let mut body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            body["_wireAuth"] = serde_json::json!(auth);
            captured.lock().unwrap().push(body);
            let body = serde_json::json!({"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: fixture low-tier context rejected"}}).to_string();
            let reply = format!("HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body);
            reader.get_mut().write_all(reply.as_bytes()).await.unwrap();
            reader.get_mut().shutdown().await.unwrap();
        }
    });
    let root = temp_store_dir();
    let mut config = default_config();
    config.auto_dream = true;
    // Isolate ordinary provider admission from automatic summarization.
    config.auto_compaction = false;
    config.context_window = 100_000;
    config.fallback_models = vec!["main-fallback".into()];
    let (session, memory_dir) =
        dream_session_with_config_and_store(&root, config, mk_deps(Vec::new()), None, None);
    let main_config_before = session.inference_snapshot().config.clone();
    assert_eq!(main_config_before.context_window, 100_000);
    let main_target_before = session.target_for_selection(None).unwrap();
    assert_eq!(
        main_target_before.config.context_window,
        main_target_before.provider.capabilities(&main_target_before.model).context_window,
        "the fixture has no explicit window override; the resolved main target follows its provider"
    );
    let requests = Arc::new(Mutex::new(Vec::new()));
    let low: Arc<dyn ModelProvider> = Arc::new(RecordedMemoryHttpProvider {
        inner: Arc::new(AnthropicProvider::new(AnthropicProviderOptions {
            api_key: Some("memory-low-key".into()),
            base_url: Some(url),
            default_model: Some("memory-low-wire".into()),
            capabilities: Some(ModelCapabilities {
                tools: Some(true),
                context_window: Some(8_000),
                max_output_tokens: Some(333),
                ..Default::default()
            }),
            ..Default::default()
        })),
        requests: requests.clone(),
    });
    session
        .install_feature_targets(FeatureTargets {
            memory: Some(InferenceTargetState::Ready(InferenceTarget {
                source: "custom".into(),
                model_id: "memory-low-entry".into(),
                model: "memory-low-wire".into(),
                provider: low,
                pricing: Vec::new(),
                review_requirements: None,
            })),
            ..Default::default()
        })
        .unwrap();
    let low = session
        .inference_snapshot()
        .for_feature(FeatureSlot::Memory)
        .unwrap();
    assert_eq!(low.config.context_window, 8_000);
    assert_eq!(low.config.model_context_window, Some(8_000));
    assert_eq!(low.config.max_output_tokens, Some(333));
    assert_eq!(
        crate::context::compact::policy::actual_input_window(&low.config),
        7_600.0
    );
    assert_eq!(
        crate::context::compact::policy::hard_block_threshold(&low.config),
        7_600.0
    );
    assert!(low.config.fallback_models.is_empty());

    // A real provider refusal, rather than a local rough estimate, owns this failure.
    let rejected = session
        .run_extraction_fork(memory_dir.to_str().unwrap(), &[], AbortSignal::new())
        .await
        .unwrap_err();
    assert_eq!(rejected.kind, super::MemoryForkFailureKind::Fork);
    assert!(
        rejected
            .message
            .contains("fixture low-tier context rejected"),
        "{rejected:?}"
    );
    assert_eq!(rejected.turn_count, 0);
    assert!(rejected.files_touched.is_empty());
    // The public maintenance API must enforce the same actual low-tier capacity.
    let mut task = session.dream_now().await.unwrap();
    assert!(task.ok, "maintenance registration itself remains available");
    let completion = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        task.done.take().unwrap(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(!completion.ok, "{completion:?}");
    assert!(
        completion
            .reason
            .as_ref()
            .is_some_and(|reason| reason.contains("fixture low-tier context rejected")),
        "{completion:?}"
    );
    assert_eq!(
        completion.turn_count, None,
        "failed public completion keeps its established shape"
    );
    tokio::time::timeout(std::time::Duration::from_secs(10), server)
        .await
        .unwrap()
        .unwrap();
    {
        let recorded = requests.lock().unwrap();
        assert_eq!(
            recorded.len(),
            2,
            "each fork reaches its captured provider once"
        );
        for request in recorded.iter() {
            assert_eq!(request.model, "memory-low-wire");
            assert_eq!(request.max_output_tokens, 333);
            let estimate = crate::context::tokens::estimate_messages_tokens(&request.messages)
                + crate::context::tokens::estimate_prefix_tokens(&request.system, &request.tools);
            assert!(estimate > low.config.context_window);
        }
    }
    {
        let recorded = http_requests.lock().unwrap();
        assert_eq!(recorded.len(), 2);
        assert!(recorded
            .iter()
            .all(|request| request["model"] == "memory-low-wire"
                && request["max_tokens"] == 333
                && request["_wireAuth"] == "memory-low-key"));
    }
    assert!(!memory_dir.join("MEMORY.md").exists());
    assert_eq!(
        session.target_for_selection(None).unwrap().config,
        main_target_before.config,
        "the memory feature target must leave the resolved main target unchanged"
    );
    assert_eq!(
        &session.inference_snapshot().config,
        &main_config_before,
        "the memory feature target must leave the raw main configuration unchanged"
    );
    let costs = session.cost_snapshot();
    assert_eq!(costs.total_cost_usd, 0.0);
    assert!(costs
        .models
        .iter()
        .all(|model| model.totals.input_tokens == 0
            && model.totals.output_tokens == 0
            && model.totals.cache_read_input_tokens == 0
            && model.totals.cache_creation_input_tokens == 0));
    session.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

fn validated_main_image(session: &Session) -> super::ValidatedInput {
    let input = Input::Parts(vec![UserContentPart::Image {
        base64: "QUJD".into(),
        mime_type: "image/png".into(),
        source_path: None,
    }]);
    session
        .validate_input_for_turn(input, &TurnOptions::default())
        .expect("the vision-capable main provider admits the image")
}

/// A pure tier save bumps the bundle CAS revision but must NOT invalidate an
/// image already validated against the unchanged main provider. Before the
/// scoped revision split this comparison used the single bundle revision, so a
/// tier-only edit wrongly rejected the pending main-model attachment.
#[test]
fn global_model_tiers_pure_tier_save_keeps_validated_main_attachment_admissible() {
    use super::InferenceTargets;
    let session = mk_session(Vec::new());
    let validated = validated_main_image(&session);
    assert!(validated.has_images);
    let before = session.model_revision();
    let after = session
        .reconfigure_tier_targets(InferenceTargets::default(), before)
        .unwrap();
    assert_eq!(after, before + 1, "the bundle CAS revision advances");
    let target = session.target_for_turn(&TurnOptions::default()).unwrap();
    assert_eq!(
        validated.provider_revision, target.revision,
        "the main-model attachment stays admissible after a tier-only save"
    );
}

/// A real main-model replacement still invalidates an attachment validated
/// against the previous main provider: the safety check the tier fix must keep.
#[test]
fn global_model_tiers_main_model_change_still_rejects_stale_validated_attachment() {
    let session = mk_session(Vec::new());
    let validated = validated_main_image(&session);
    let mut config = default_config();
    config.model = "replacement".into();
    session
        .reconfigure_inference(Arc::new(fake_provider()), config, None, None, 0)
        .unwrap();
    let target = session.target_for_turn(&TurnOptions::default()).unwrap();
    assert_ne!(
        validated.provider_revision, target.revision,
        "a genuine main-model change must reject the stale attachment"
    );
}

/// Main and tier revisions are scoped tokens, so a main-validated attachment
/// never collides with an explicit tier selection even when both counters read
/// the same number. This is why the revision cannot be a bare `u64`.
#[test]
fn global_model_tiers_scoped_revisions_never_collide_between_main_and_explicit_tier() {
    use super::InferenceTargets;
    let session = mk_session(Vec::new());
    let validated = validated_main_image(&session);
    session
        .install_inference_targets(Some(InferenceTargets::default()), None, None)
        .unwrap();
    assert_eq!(
        session.model_revision(),
        0,
        "installing targets does not advance the bundle revision"
    );
    let high = session
        .target_for_selection(Some(&RuntimeString::from("high")))
        .unwrap();
    assert_ne!(
        validated.provider_revision, high.revision,
        "a main-scoped attachment must not be admitted as an explicit tier run"
    );
}

#[tokio::test]
async fn dream_now_reports_gate_closed_when_auto_dream_is_disabled() {
    let root = temp_store_dir();
    let (session, _) = dream_session(&root, false, Vec::new());

    let task = session.dream_now().await.unwrap();

    assert!(!task.ok);
    assert_eq!(task.reason, Some(DreamNowReason::GateClosed));
    assert!(task.done.is_none());
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn dream_now_reports_lock_held_without_starting_a_fork() {
    let root = temp_store_dir();
    let (session, memory_dir) = dream_session(&root, true, Vec::new());
    let now = (session.deps.now)();
    let held_lock = try_acquire_consolidation_lock(
        memory_dir
            .to_str()
            .expect("test memory directory is a scalar host path"),
        1,
        now as f64,
    )
    .await
    .unwrap()
    .unwrap();

    let task = session.dream_now().await.unwrap();

    assert!(!task.ok);
    assert_eq!(task.reason, Some(DreamNowReason::LockHeld));
    assert!(task.done.is_none());
    drop(held_lock);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn dream_now_reports_already_running_for_an_in_process_fork() {
    let root = temp_store_dir();
    let mut deps = mk_deps(Vec::new());
    deps.call_model = Box::new(move |_request, signal| {
        Box::pin(stream! {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            if signal.is_cancelled() {
                return;
            }
            yield ev::message_start(None);
            yield ev::text("done");
            yield ev::stop(Some(ModelStopReason::EndTurn), None);
        }) as ModelStream
    });
    let (session, _) = dream_session_with_deps(&root, true, deps, None);

    let mut first = session.dream_now().await.unwrap();
    let second = session.dream_now().await.unwrap();

    assert!(first.ok);
    assert!(!second.ok);
    assert_eq!(second.reason, Some(DreamNowReason::AlreadyRunning));
    assert!(second.done.is_none());
    assert!(first.done.take().unwrap().await.unwrap().ok);
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn dream_single_flight_reservation_has_one_concurrent_winner() {
    let root = temp_store_dir();
    let (session, _) = dream_session(&root, true, Vec::new());
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let mut threads = Vec::new();
    for _ in 0..8 {
        let session = session.clone();
        let barrier = barrier.clone();
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            session.try_begin_dream().is_some()
        }));
    }

    let winners = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .filter(|won| *won)
        .count();

    assert_eq!(winners, 1);
    session.force_clear_dream_owner();
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn aborted_stale_dream_guard_cannot_clear_a_newer_owner() {
    let root = temp_store_dir();
    let (session, _) = dream_session(&root, true, Vec::new());
    let first_owner = session.try_begin_dream().expect("first owner");
    let first_session = session.clone();
    let first_worker = tokio::spawn(async move {
        let _owner = DreamInProgressGuard::new(&first_session, first_owner);
        futures::future::pending::<()>().await;
    });
    tokio::task::yield_now().await;

    // Registration/error cleanup may release the reservation before the
    // spawned future is aborted and its RAII guard gets a chance to drop.
    session.finish_dream_owner(first_owner);
    let second_owner = session.try_begin_dream().expect("newer owner");
    first_worker.abort();
    assert!(first_worker.await.unwrap_err().is_cancelled());

    assert!(session
        .memory_scheduler
        .dream_in_progress
        .load(Ordering::SeqCst));
    assert_eq!(
        session
            .memory_scheduler
            .active_dream_owner
            .load(Ordering::SeqCst),
        second_owner,
        "the stale guard must not clear the newer token"
    );
    session.finish_dream_owner(second_owner);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn dream_now_completion_reports_touched_files_and_turn_count() {
    let root = temp_store_dir();
    let write_turn = |id: &str, path: &str, content: &str| {
        let mut turn = vec![ev::message_start(None)];
        turn.extend(exact_tool_use_events(id, "write", &{
            let mut input = exact_json!({ "path": "", "content": "" });
            input["path"] = RuntimeJson::String(path.into());
            input["content"] = RuntimeJson::String(content.into());
            input
        }));
        turn.push(ev::stop(Some(ModelStopReason::ToolUse), None));
        turn
    };
    let (session, memory_dir) = dream_session(
        &root,
        true,
        vec![
            write_turn("write-z", "z-consolidated.md", "fact z\n"),
            write_turn("write-a", "a-consolidated.md", "fact a\n"),
            text_turn("done"),
        ],
    );

    let mut task = session.dream_now().await.unwrap();
    assert!(task.ok);
    assert_eq!(task.reason, None);
    let completion = task.done.take().unwrap().await.unwrap();

    assert!(completion.ok);
    assert_eq!(completion.reason, None);
    let memory_dir = memory_dir.canonicalize().unwrap();
    // `filesTouched` is folded from the `file_change` events the write tool emitted, and the
    // engine spells the paths it reports POSIX-rendered on every host.
    assert_eq!(
        completion.files_touched,
        Some(vec![
            posix_rendered_runtime(&runtime_path_from_host_path(
                &memory_dir.join("z-consolidated.md")
            ))
            .into_runtime_string(),
            posix_rendered_runtime(&runtime_path_from_host_path(
                &memory_dir.join("a-consolidated.md")
            ))
            .into_runtime_string(),
        ])
    );
    assert!(completion.turn_count.unwrap_or_default() > 0);
    assert_eq!(
        std::fs::read_to_string(memory_dir.join("z-consolidated.md")).unwrap(),
        "fact z\n"
    );
    assert_eq!(
        std::fs::read_to_string(memory_dir.join("a-consolidated.md")).unwrap(),
        "fact a\n"
    );
    assert!(memory_dir.join(".consolidate-lock").is_file());
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn dream_now_completion_reports_failure_and_rolls_back_new_lock() {
    let root = temp_store_dir();
    let (session, memory_dir) = dream_session(
        &root,
        true,
        vec![provider_error_turn("boom: provider down")],
    );

    let mut task = session.dream_now().await.unwrap();
    assert!(task.ok);
    let completion = task.done.take().unwrap().await.unwrap();

    assert!(!completion.ok);
    assert!(completion
        .reason
        .as_ref()
        .is_some_and(|reason| reason.contains("memory fork failed: boom: provider down")));
    assert_eq!(
        read_last_consolidated_at(memory_dir.to_str().unwrap()).await,
        0.0
    );
    let reacquired = try_acquire_consolidation_lock(
        memory_dir.to_str().unwrap(),
        std::process::id() as i64,
        (session.deps.now)() as f64,
    )
    .await
    .unwrap();
    assert!(reacquired.is_some());
    drop(reacquired);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn manual_dream_provider_panic_completes_waiter_and_releases_lock() {
    let root = temp_store_dir();
    let (session, memory_dir) =
        dream_session_with_deps(&root, true, panicking_memory_deps(2_000_000), None);

    let mut task = session.dream_now().await.unwrap();
    let completion =
        tokio::time::timeout(std::time::Duration::from_secs(5), task.done.take().unwrap())
            .await
            .expect("manual Dream panic must complete its waiter")
            .unwrap();

    assert!(!completion.ok);
    assert!(
        completion
            .reason
            .as_ref()
            .is_some_and(|reason| reason.contains("maintenance provider panic")),
        "provider panic must remain visible to the manual waiter: {:?}",
        completion.reason
    );
    assert!(!session
        .memory_scheduler
        .dream_in_progress
        .load(Ordering::SeqCst));
    let reacquired = try_acquire_consolidation_lock(
        memory_dir.to_str().unwrap(),
        std::process::id() as i64,
        2_000_001.0,
    )
    .await
    .unwrap();
    assert!(reacquired.is_some());
    drop(reacquired);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn failed_memory_extraction_does_not_advance_its_cursor() {
    let root = temp_store_dir();
    let (session, memory_dir) =
        dream_session(&root, true, vec![provider_error_turn("extraction failed")]);
    let recent = vec![Message::User(UserMessage {
        question_answer: None,
        uuid: "memory-message-1".into(),
        content: vec![ContentBlock::Text(TextBlock {
            text: "remember this".into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })],
        is_meta: None,
    })];

    let error = session
        .run_extraction_fork(
            memory_dir
                .to_str()
                .expect("test memory directory is a scalar host path"),
            &recent,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("memory fork failed: extraction failed"));
    assert_eq!(
        *session
            .memory_scheduler
            .last_memory_message_uuid
            .lock()
            .unwrap(),
        None
    );
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn shutdown_cancels_auto_memory_with_one_terminal_and_keeps_the_cursor() {
    let root = temp_store_dir();
    let host: Arc<dyn AgentMemoryHost> = Arc::new(SyncTestMemoryHost {
        layout: SyncTestMemoryLayout {
            base: root.to_string_lossy().into_owned(),
        },
        plans: Mutex::new(Vec::new()),
        applies: Mutex::new(Vec::new()),
        apply_results: Mutex::new(Vec::new()),
    });
    let memory_dir = PathBuf::from(host.layout().dir_for("main", AgentMemoryScope::Project));
    std::fs::create_dir_all(&memory_dir).unwrap();
    let entered = Arc::new(AtomicBool::new(false));
    let mut config = default_config();
    config.agent_memory = true;
    config.auto_memory = true;
    config.max_retries = 0;
    let session = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        cooperative_hanging_memory_deps(50_000, entered.clone()),
        config,
        vec![create_write_tool()],
        Some(host),
        None,
        None,
        Some(vec![
            fork_user("memory-cursor-before-shutdown"),
            fork_user("memory-after-cursor"),
        ]),
        None,
    );
    session.record_auto_memory_skip(
        Some(
            memory_dir
                .to_str()
                .expect("test memory directory is a scalar host path"),
        ),
        &[fork_user("memory-cursor-before-shutdown")],
        0,
        AutoMemoryRunReason::DirectMemoryWrite,
        Some("memory-cursor-before-shutdown".into()),
    );
    let live_events = Arc::new(Mutex::new(Vec::new()));
    session.set_ordered_memory_maintenance_event_sink(Some({
        let live_events = live_events.clone();
        Arc::new(move |event| live_events.lock().unwrap().push(event))
    }));

    session.maybe_extract_memories();
    tokio::time::timeout(MEMORY_MAINTENANCE_ENTRY_TIMEOUT, async {
        while !entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("automatic memory should enter the provider");
    let started_id = session
        .memory_maintenance_snapshot()
        .unwrap()
        .auto_memory_runs
        .last()
        .unwrap()
        .run_id
        .clone();

    session.shutdown_and_wait().await.unwrap();

    assert_eq!(
        *session
            .memory_scheduler
            .last_memory_message_uuid
            .lock()
            .unwrap(),
        Some("memory-cursor-before-shutdown".into())
    );
    let snapshot = session.memory_maintenance_snapshot().unwrap();
    assert_eq!(snapshot.auto_memory_runs.len(), 2);
    let terminal = snapshot
        .auto_memory_runs
        .iter()
        .find(|run| run.run_id == started_id)
        .unwrap();
    assert_eq!(terminal.run_id, started_id);
    assert_eq!(terminal.status, MemoryMaintenanceStatus::Failed);
    assert_eq!(terminal.reason, Some(AutoMemoryRunReason::Shutdown));
    assert_eq!(
        terminal.cursor_message_uuid,
        Some("memory-cursor-before-shutdown".into())
    );
    assert!(!memory_dir.join("MEMORY.md").exists());
    let events = live_events.lock().unwrap();
    assert_eq!(events.len(), 2);
    let MemoryMaintenanceEvent::AutoMemory { run: started } = &events[0] else {
        panic!("expected automatic memory start");
    };
    let MemoryMaintenanceEvent::AutoMemory { run: failed } = &events[1] else {
        panic!("expected automatic memory shutdown terminal");
    };
    assert_eq!(started.run_id, failed.run_id);
    assert_eq!(started.status, MemoryMaintenanceStatus::Started);
    assert_eq!(failed.status, MemoryMaintenanceStatus::Failed);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn auto_memory_provider_panic_is_terminal_and_releases_single_flight_for_retry() {
    let root = temp_store_dir();
    let host: Arc<dyn AgentMemoryHost> = Arc::new(SyncTestMemoryHost {
        layout: SyncTestMemoryLayout {
            base: root.to_string_lossy().into_owned(),
        },
        plans: Mutex::new(Vec::new()),
        applies: Mutex::new(Vec::new()),
        apply_results: Mutex::new(Vec::new()),
    });
    std::fs::create_dir_all(host.layout().dir_for("main", AgentMemoryScope::Project)).unwrap();
    let mut config = default_config();
    config.agent_memory = true;
    config.auto_memory = true;
    config.max_retries = 0;
    let session = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        panicking_memory_deps(50_000),
        config,
        vec![create_write_tool()],
        Some(host),
        None,
        None,
        Some(vec![fork_user("panic-memory-input")]),
        None,
    );

    for expected_runs in 1..=2 {
        session.maybe_extract_memories();
        tokio::time::timeout(CROSS_THREAD_LIVENESS_TIMEOUT, async {
            while session
                .memory_scheduler
                .extraction_in_progress
                .load(Ordering::SeqCst)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("panicking automatic memory worker must terminate");
        let snapshot = session.memory_maintenance_snapshot().unwrap();
        assert_eq!(snapshot.auto_memory_runs.len(), expected_runs);
        let run = snapshot.auto_memory_runs.last().unwrap();
        assert_eq!(run.status, MemoryMaintenanceStatus::Failed);
        assert_eq!(run.reason, Some(AutoMemoryRunReason::ForkFailure));
        assert!(
            run.error
                .as_ref()
                .is_some_and(|error| error.contains("panic")),
            "provider panic must remain visible in the terminal lifecycle: {:?}",
            run.error
        );
        assert_eq!(
            *session
                .memory_scheduler
                .last_memory_message_uuid
                .lock()
                .unwrap(),
            None
        );
    }
    assert!(!session
        .memory_scheduler
        .extraction_in_progress
        .load(Ordering::SeqCst));
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn recovered_maintenance_terminal_stays_hidden_until_process_local_flush_receipt() {
    let rollout = ControlledMaintenanceRollout::new(1);
    let rollout_sink: RolloutSink = rollout.clone();
    let mut initial = MemoryMaintenanceSnapshot::default();
    let started = AutoDreamRun {
        dream_run_id: "recovered-maintenance-owner".into(),
        session_id: "setup-session".into(),
        trigger: MemoryMaintenanceTrigger::Auto,
        status: MemoryMaintenanceStatus::Started,
        started_at: 1,
        ended_at: None,
        reason: None,
        error: None,
        last_consolidated_at: None,
        sessions_reviewing: 1,
        files_touched: Vec::new(),
        turns: Vec::new(),
        turn_count: 0,
        summary: None,
    };
    assert!(initial.apply(&MemoryMaintenanceEvent::AutoDream {
        run: started.clone(),
    }));
    let bus = Arc::new(HookBus::new(
        crate::runtime_process::test_runtime_process_context(),
        Vec::new(),
    ));

    // Construct without a Tokio handle. The first query inside this test's
    // runtime must lazily register the retained recovery owner.
    let (send, receive) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let session = mk_setup_session_with_maintenance(
            mk_deps(Vec::new()),
            bus,
            Some(rollout_sink),
            None,
            Some(initial),
        );
        send.send(session).unwrap();
    })
    .join()
    .unwrap();
    let session = receive.recv().unwrap();
    assert_eq!(rollout.flush_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        session
            .memory_maintenance_snapshot()
            .unwrap()
            .auto_dream_runs[0]
            .status,
        MemoryMaintenanceStatus::Started
    );
    let barrier_session = session.clone();
    let recovery_barrier = tokio::spawn(async move {
        barrier_session
            .await_pending_memory_maintenance_recovery_owners()
            .await
    });
    rollout.wait_for_flush(1).await;
    assert_eq!(
        session
            .memory_maintenance_snapshot()
            .unwrap()
            .auto_dream_runs[0]
            .status,
        MemoryMaintenanceStatus::Started,
        "a synthesized recovery terminal is provisional until flush succeeds"
    );
    assert!(
        !recovery_barrier.is_finished(),
        "the resume barrier must retain the constructor recovery owner"
    );
    rollout.release_flush();
    recovery_barrier
        .await
        .unwrap()
        .expect("the process-local maintenance recovery owner should become durable");
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let snapshot = session.memory_maintenance_snapshot().unwrap();
            if snapshot.auto_dream_runs[0].status == MemoryMaintenanceStatus::Failed {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("recovery durability owner should promote its terminal");
    let recovered = &session
        .memory_maintenance_snapshot()
        .unwrap()
        .auto_dream_runs[0];
    assert_eq!(recovered.reason, Some(AutoDreamRunReason::Interrupted));
    assert_eq!(rollout.events().len(), 1);
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn maintenance_recovery_barrier_all_settles_before_aggregating_failure() {
    let rollout = ControlledMaintenanceRollout::configured(usize::MAX, None, Some(1));
    let rollout_sink: RolloutSink = rollout.clone();
    let mut initial = MemoryMaintenanceSnapshot::default();
    assert!(initial.apply(&MemoryMaintenanceEvent::AutoMemory {
        run: AutoMemoryRun {
            run_id: "recovered-memory-owner".into(),
            session_id: "setup-session".into(),
            trigger: MemoryMaintenanceTrigger::Auto,
            scope: AgentMemoryScope::Project,
            target_dir: "/tmp/recovered-memory-owner".into(),
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
    }));
    assert!(initial.apply(&MemoryMaintenanceEvent::AutoDream {
        run: AutoDreamRun {
            dream_run_id: "recovered-dream-owner".into(),
            session_id: "setup-session".into(),
            trigger: MemoryMaintenanceTrigger::Auto,
            status: MemoryMaintenanceStatus::Started,
            started_at: 1,
            ended_at: None,
            reason: None,
            error: None,
            last_consolidated_at: None,
            sessions_reviewing: 1,
            files_touched: Vec::new(),
            turns: Vec::new(),
            turn_count: 0,
            summary: None,
        },
    }));
    let bus = Arc::new(HookBus::new(
        crate::runtime_process::test_runtime_process_context(),
        Vec::new(),
    ));
    let session = mk_setup_session_with_maintenance(
        mk_deps(Vec::new()),
        bus,
        Some(rollout_sink),
        None,
        Some(initial),
    );

    let barrier_session = session.clone();
    let recovery_barrier = tokio::spawn(async move {
        barrier_session
            .await_pending_memory_maintenance_recovery_owners()
            .await
    });
    rollout.wait_for_flush(1).await;
    let error = recovery_barrier
        .await
        .unwrap()
        .expect_err("the first recovery flush panic must fail the aggregate barrier");
    assert!(error
        .try_to_utf8()
        .unwrap()
        .contains("memory maintenance recovery durability failed"));
    assert!(session
        .maintenance_tasks
        .lock()
        .unwrap()
        .recovery_receipts
        .values()
        .all(|receipt| receipt.borrow().is_some()));

    let outcome = session.shutdown_and_wait_detailed().await;
    assert!(!outcome.timed_out, "{outcome:?}");
    assert!(outcome
        .errors
        .iter()
        .any(|error| error.contains("flush adapter panicked")));
}

#[tokio::test]
async fn maintenance_started_flush_precedes_visibility_and_worker_effect() {
    let rollout = ControlledMaintenanceRollout::new(1);
    let rollout_sink: RolloutSink = rollout.clone();
    let session = maintenance_durability_test_session(rollout_sink);
    let started = maintenance_test_started(&session, "started-before-effect");
    let (started_send, started_receive) = tokio::sync::oneshot::channel();
    let effects = Arc::new(AtomicUsize::new(0));
    let worker_effects = effects.clone();
    let worker_session = session.clone();
    let mut terminal = started.clone();
    let handle = tokio::spawn(async move {
        if started_receive.await != Ok(true) {
            return;
        }
        worker_effects.fetch_add(1, Ordering::SeqCst);
        terminal.status = MemoryMaintenanceStatus::Completed;
        terminal.ended_at = Some(2);
        terminal.cursor_message_uuid = Some("cursor-after-effect".into());
        terminal.turn_count = Some(1);
        let _ = worker_session
            .commit_memory_maintenance_event_durable(
                MemoryMaintenanceEvent::AutoMemory { run: terminal },
                Some("cursor-after-effect".into()),
                true,
            )
            .await;
    });
    let task = MaintenanceTask {
        run_id: started.run_id.clone(),
        kind: MaintenanceTaskKind::AutoMemory,
        cancel: CancellationToken::new(),
        handle,
        manual_completion: None,
    };
    assert!(session
        .register_durable_auto_maintenance_task(
            task,
            MemoryMaintenanceEvent::AutoMemory {
                run: started.clone(),
            },
            started_send,
        )
        .is_ok());

    rollout.wait_for_flush(1).await;
    assert!(session
        .memory_maintenance_snapshot()
        .unwrap()
        .auto_memory_runs
        .is_empty());
    assert_eq!(effects.load(Ordering::SeqCst), 0);

    rollout.release_flush();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if session
                .memory_maintenance_snapshot()
                .unwrap()
                .auto_memory_runs
                .iter()
                .any(|run| run.status == MemoryMaintenanceStatus::Completed)
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal receipt should become visible");
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(
        session.memory_message_cursor(),
        Some("cursor-after-effect".into())
    );
    assert_eq!(
        rollout
            .events()
            .iter()
            .map(memory_maintenance_event_status)
            .collect::<Vec<_>>(),
        vec![
            MemoryMaintenanceStatus::Started,
            MemoryMaintenanceStatus::Completed,
        ]
    );
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn shutdown_during_blocked_maintenance_started_flush_never_admits_effect() {
    let rollout = ControlledMaintenanceRollout::new(1);
    let rollout_sink: RolloutSink = rollout.clone();
    let session = maintenance_durability_test_session(rollout_sink);
    let started = maintenance_test_started(&session, "shutdown-before-started-receipt");
    let (started_send, started_receive) = tokio::sync::oneshot::channel();
    let effects = Arc::new(AtomicUsize::new(0));
    let worker_effects = effects.clone();
    let handle = tokio::spawn(async move {
        if started_receive.await == Ok(true) {
            worker_effects.fetch_add(1, Ordering::SeqCst);
        }
    });
    let task = MaintenanceTask {
        run_id: started.run_id.clone(),
        kind: MaintenanceTaskKind::AutoMemory,
        cancel: CancellationToken::new(),
        handle,
        manual_completion: None,
    };
    assert!(session
        .register_durable_auto_maintenance_task(
            task,
            MemoryMaintenanceEvent::AutoMemory {
                run: started.clone(),
            },
            started_send,
        )
        .is_ok());
    rollout.wait_for_flush(1).await;
    assert!(session
        .memory_maintenance_snapshot()
        .unwrap()
        .auto_memory_runs
        .is_empty());

    let closing_session = session.clone();
    let shutdown = tokio::spawn(async move { closing_session.shutdown_and_wait_detailed().await });
    while !session.maintenance_is_closing() {
        tokio::task::yield_now().await;
    }
    rollout.release_flush();
    let outcome = shutdown.await.unwrap();

    assert!(!outcome.timed_out, "{outcome:?}");
    assert!(outcome.errors.is_empty(), "{outcome:?}");
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    let run = &session
        .memory_maintenance_snapshot()
        .unwrap()
        .auto_memory_runs[0];
    assert_eq!(run.status, MemoryMaintenanceStatus::Failed);
    assert_eq!(run.reason, Some(AutoMemoryRunReason::Shutdown));
    assert_eq!(
        rollout
            .events()
            .iter()
            .map(memory_maintenance_event_status)
            .collect::<Vec<_>>(),
        vec![
            MemoryMaintenanceStatus::Started,
            MemoryMaintenanceStatus::Failed,
        ]
    );
}

#[tokio::test]
async fn initiate_shutdown_synchronously_fences_republication_and_turn_admission() {
    let session = mk_session(Vec::new());
    assert!(!session.is_disposed());

    session.initiate_shutdown();

    // This current-thread task has not yielded, so the spawned owner has
    // not received its first poll. The public teardown fence must already
    // be authoritative for protocol rebinding and core turn admission.
    assert!(session.is_disposed());
    assert!(session
        .lifecycle
        .shutdown_completion
        .get()
        .expect("shutdown completion initialized synchronously")
        .borrow()
        .is_none());
    assert!(ActiveTurnLease::try_new(
        session.clone(),
        "must-not-enter-after-initiate-shutdown".into(),
        CancellationToken::new(),
    )
    .is_none());

    let first_completion = session.lifecycle.shutdown_completion.get().unwrap().clone();
    session.initiate_shutdown();
    let duplicate_completion = session.lifecycle.shutdown_completion.get().unwrap().clone();
    assert!(first_completion.same_channel(&duplicate_completion));

    let outcome = session.shutdown_and_wait_detailed().await;
    assert!(!outcome.timed_out, "{outcome:?}");
    assert!(outcome.errors.is_empty(), "{outcome:?}");
}

#[tokio::test]
async fn cancelled_terminal_waiter_cannot_publish_cursor_before_durable_flush() {
    let rollout = ControlledMaintenanceRollout::new(2);
    let rollout_sink: RolloutSink = rollout.clone();
    let session = maintenance_durability_test_session(rollout_sink);
    let started = maintenance_test_started(&session, "terminal-receipt-owner");
    assert!(
        session
            .commit_memory_maintenance_event_durable(
                MemoryMaintenanceEvent::AutoMemory {
                    run: started.clone(),
                },
                None,
                false,
            )
            .await
    );
    let live = Arc::new(Mutex::new(Vec::new()));
    session.set_ordered_memory_maintenance_event_sink(Some({
        let live = live.clone();
        Arc::new(move |event| live.lock().unwrap().push(event))
    }));

    let mut terminal = started;
    terminal.status = MemoryMaintenanceStatus::Completed;
    terminal.ended_at = Some(2);
    terminal.cursor_message_uuid = Some("durable-terminal-cursor".into());
    terminal.turn_count = Some(1);
    let terminal_session = session.clone();
    let waiter = tokio::spawn(async move {
        terminal_session
            .commit_memory_maintenance_event_durable(
                MemoryMaintenanceEvent::AutoMemory { run: terminal },
                Some("durable-terminal-cursor".into()),
                true,
            )
            .await
    });
    rollout.wait_for_flush(2).await;
    assert_eq!(
        session
            .memory_maintenance_snapshot()
            .unwrap()
            .auto_memory_runs[0]
            .status,
        MemoryMaintenanceStatus::Started
    );
    assert_eq!(session.memory_message_cursor(), None);
    assert!(live.lock().unwrap().is_empty());

    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    rollout.release_flush();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if session.memory_message_cursor()
                == Some(RuntimeString::from("durable-terminal-cursor"))
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Session-owned terminal owner survives waiter cancellation");
    assert_eq!(
        session
            .memory_maintenance_snapshot()
            .unwrap()
            .auto_memory_runs[0]
            .status,
        MemoryMaintenanceStatus::Completed
    );
    assert_eq!(live.lock().unwrap().len(), 1);
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn closed_maintenance_writer_keeps_its_owner_failure_and_never_promotes_started() {
    let rollout = ControlledMaintenanceRollout::new(usize::MAX);
    let sink: RolloutSink = rollout.clone();
    let session = maintenance_durability_test_session(sink);
    let started = maintenance_test_started(&session, "closed-maintenance-writer");
    let visible = Arc::new(AtomicUsize::new(0));
    session.set_ordered_memory_maintenance_event_sink(Some({
        let visible = visible.clone();
        Arc::new(move |_| {
            visible.fetch_add(1, Ordering::SeqCst);
        })
    }));
    rollout.fail_prepare.store(true, Ordering::SeqCst);
    assert!(
        !session
            .commit_memory_maintenance_event_durable(
                MemoryMaintenanceEvent::AutoMemory { run: started },
                None,
                false,
            )
            .await
    );
    assert!(session.memory_maintenance_snapshot().is_err());
    assert!(session
        .maintenance
        .snapshot
        .lock()
        .unwrap()
        .auto_memory_runs
        .is_empty());
    assert!(rollout.events().is_empty());
    assert_eq!(visible.load(Ordering::SeqCst), 0);
    assert_eq!(rollout.flush_calls.load(Ordering::SeqCst), 0);
    rollout.fail_prepare.store(false, Ordering::SeqCst);
    let outcome = session.shutdown_and_wait_detailed().await;
    assert!(!outcome.errors.is_empty());
}

#[tokio::test]
async fn maintenance_append_panic_blocks_effect_and_makes_shutdown_partial() {
    let rollout = ControlledMaintenanceRollout::panicking_append(1);
    let rollout_sink: RolloutSink = rollout.clone();
    let session = maintenance_durability_test_session(rollout_sink);
    let started = maintenance_test_started(&session, "append-panic-no-effect");
    let (started_send, started_receive) = tokio::sync::oneshot::channel();
    let effects = Arc::new(AtomicUsize::new(0));
    let worker_effects = effects.clone();
    let handle = tokio::spawn(async move {
        if started_receive.await == Ok(true) {
            worker_effects.fetch_add(1, Ordering::SeqCst);
        }
    });
    let task = MaintenanceTask {
        run_id: started.run_id.clone(),
        kind: MaintenanceTaskKind::AutoMemory,
        cancel: CancellationToken::new(),
        handle,
        manual_completion: None,
    };
    assert!(session
        .register_durable_auto_maintenance_task(
            task,
            MemoryMaintenanceEvent::AutoMemory { run: started },
            started_send,
        )
        .is_ok());
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if session
                .maintenance_tasks
                .lock()
                .unwrap()
                .durability_tasks
                .is_empty()
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("contained append panic retires its owner");
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    let query_error = session
        .memory_maintenance_snapshot()
        .expect_err("an uncertain Started append must fail the authoritative query");
    assert_eq!(
        query_error.detail.code,
        crate::memory::maintenance::MemoryMaintenanceQueryErrorCode::PersistenceInconsistent
    );
    assert_eq!(
        query_error.detail.run_kind,
        Some(MemoryMaintenanceRunKind::AutoMemory)
    );
    assert_eq!(
        query_error.detail.run_id,
        Some("append-panic-no-effect".into())
    );
    let outcome = session.shutdown_and_wait_detailed().await;
    assert!(!outcome.timed_out);
    assert!(outcome
        .errors
        .iter()
        .any(|error| error.contains("append_event adapter panicked")));
    assert_eq!(rollout.shutdown_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn failed_auto_dream_started_receipt_releases_its_single_flight_owner() {
    let rollout = ControlledMaintenanceRollout::panicking_append(1);
    let rollout_sink: RolloutSink = rollout.clone();
    let session = maintenance_durability_test_session(rollout_sink);
    let started = maintenance_test_dream_started(&session, "dream-start-receipt-fails");
    let owner = session
        .try_begin_dream()
        .expect("test reserves the automatic Dream single-flight owner");
    let (started_send, started_receive) = tokio::sync::oneshot::channel();
    let effects = Arc::new(AtomicUsize::new(0));
    let worker_effects = effects.clone();
    let worker_session = session.clone();
    let handle = tokio::spawn(async move {
        let _owner = DreamInProgressGuard::new(&worker_session, owner);
        if started_receive.await != Ok(true) {
            return;
        }
        worker_effects.fetch_add(1, Ordering::SeqCst);
    });
    let task = MaintenanceTask {
        run_id: started.dream_run_id.clone(),
        kind: MaintenanceTaskKind::AutoDream,
        cancel: CancellationToken::new(),
        handle,
        manual_completion: None,
    };
    assert!(session
        .register_durable_auto_maintenance_task(
            task,
            MemoryMaintenanceEvent::AutoDream { run: started },
            started_send,
        )
        .is_ok());

    tokio::time::timeout(CROSS_THREAD_LIVENESS_TIMEOUT, async {
        while session
            .memory_scheduler
            .dream_in_progress
            .load(Ordering::SeqCst)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("a false Started receipt must drop the Dream owner guard");
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert_eq!(
        session
            .memory_scheduler
            .active_dream_owner
            .load(Ordering::SeqCst),
        0
    );

    let retry_owner = session
        .try_begin_dream()
        .expect("Dream must be immediately reentrant after the failed receipt");
    session.finish_dream_owner(retry_owner);
    let outcome = session.shutdown_and_wait_detailed().await;
    assert!(!outcome.timed_out);
    assert!(outcome
        .errors
        .iter()
        .any(|error| error.contains("append_event adapter panicked")));
}

#[tokio::test]
async fn maintenance_terminal_flush_panic_never_promotes_terminal_or_cursor() {
    let rollout = ControlledMaintenanceRollout::panicking_flush(2);
    let rollout_sink: RolloutSink = rollout.clone();
    let session = maintenance_durability_test_session(rollout_sink);
    let started = maintenance_test_started(&session, "terminal-flush-panic");
    assert!(
        session
            .commit_memory_maintenance_event_durable(
                MemoryMaintenanceEvent::AutoMemory {
                    run: started.clone(),
                },
                None,
                false,
            )
            .await
    );
    let mut terminal = started;
    terminal.status = MemoryMaintenanceStatus::Completed;
    terminal.ended_at = Some(2);
    terminal.cursor_message_uuid = Some("must-not-promote".into());
    terminal.turn_count = Some(1);
    assert!(
        !session
            .commit_memory_maintenance_event_durable(
                MemoryMaintenanceEvent::AutoMemory { run: terminal },
                Some("must-not-promote".into()),
                true,
            )
            .await
    );
    let first_error = session
        .memory_maintenance_snapshot()
        .expect_err("a failed terminal receipt makes the snapshot non-authoritative");
    assert_eq!(
        first_error.detail.run_kind,
        Some(MemoryMaintenanceRunKind::AutoMemory)
    );
    assert_eq!(
        first_error.detail.run_id,
        Some("terminal-flush-panic".into())
    );
    assert_eq!(session.memory_message_cursor(), None);
    let outcome = session.shutdown_and_wait_detailed().await;
    assert!(!outcome.timed_out);
    assert!(outcome
        .errors
        .iter()
        .any(|error| error.contains("flush adapter panicked")));
    assert_eq!(
        session
            .memory_maintenance_snapshot()
            .expect_err("the first typed failure must remain sticky"),
        first_error
    );
    assert_eq!(session.memory_message_cursor(), None);
}

#[tokio::test(start_paused = true)]
async fn held_maintenance_persistence_gate_is_typed_and_skips_rollout_finalization() {
    let rollout = ControlledMaintenanceRollout::new(usize::MAX);
    let rollout_sink: RolloutSink = rollout.clone();
    let session = maintenance_durability_test_session(rollout_sink);
    let held = session
        .maintenance
        .persistence
        .serial
        .clone()
        .lock_owned()
        .await;
    let closing = session.clone();
    let shutdown = tokio::spawn(async move { closing.shutdown_and_wait_detailed().await });
    tokio::task::yield_now().await;
    tokio::time::advance(std::time::Duration::from_millis(
        SESSION_SHUTDOWN_PERSISTENCE_MS + 1,
    ))
    .await;
    tokio::task::yield_now().await;
    let outcome = tokio::time::timeout(std::time::Duration::from_millis(1), shutdown)
        .await
        .expect("maintenance persistence fence must be bounded")
        .expect("Session-owned shutdown task");
    assert!(outcome.timed_out);
    assert!(outcome.errors.iter().any(|error| {
        error.contains("memory maintenance persistence fence timed out")
            && error.contains("final rollout flush/shutdown was skipped")
    }));
    assert_eq!(rollout.flush_calls.load(Ordering::SeqCst), 0);
    assert_eq!(rollout.shutdown_calls.load(Ordering::SeqCst), 0);
    drop(held);
    assert!(session
        .append_memory_maintenance_event(&MemoryMaintenanceEvent::AutoMemory {
            run: AutoMemoryRun {
                run_id: "after-maintenance-fence".into(),
                session_id: session.id.clone(),
                trigger: MemoryMaintenanceTrigger::Auto,
                scope: AgentMemoryScope::Project,
                target_dir: "/tmp/after-maintenance-fence".into(),
                status: MemoryMaintenanceStatus::Skipped,
                started_at: 1,
                ended_at: Some(1),
                reason: Some(AutoMemoryRunReason::Shutdown),
                error: None,
                message_count: 0,
                cursor_message_uuid: None,
                files_touched: Vec::new(),
                turn_count: None,
            },
        })
        .await
        .is_err());
}

#[tokio::test]
async fn isolated_memory_maintenance_sink_panic_preserves_snapshot_and_shutdown() {
    let session = mk_session(Vec::new());
    let callback_calls = Arc::new(AtomicUsize::new(0));
    session.set_memory_maintenance_event_sink(Some({
        let callback_calls = callback_calls.clone();
        Arc::new(move |_event| {
            callback_calls.fetch_add(1, Ordering::SeqCst);
            panic!("Host maintenance callback panic");
        })
    }));
    let run = AutoMemoryRun {
        run_id: "isolated-sink-panic".into(),
        session_id: session.id.clone(),
        trigger: MemoryMaintenanceTrigger::Auto,
        scope: AgentMemoryScope::Project,
        target_dir: "/tmp/isolated-sink-panic".into(),
        status: MemoryMaintenanceStatus::Skipped,
        started_at: 1,
        ended_at: Some(2),
        reason: Some(AutoMemoryRunReason::EmptyInput),
        error: None,
        message_count: 1,
        cursor_message_uuid: None,
        files_touched: Vec::new(),
        turn_count: None,
    };

    session
        .record_memory_maintenance_event(MemoryMaintenanceEvent::AutoMemory { run: run.clone() });
    let snapshot = session.memory_maintenance_snapshot().unwrap();
    assert_eq!(snapshot.auto_memory_runs, vec![run]);
    session
        .shutdown_and_wait()
        .await
        .expect("a panicking isolated callback must not fail shutdown");
    assert_eq!(
        callback_calls.load(Ordering::SeqCst),
        1,
        "the serial dispatcher must invoke the callback exactly once"
    );
    assert_eq!(session.memory_maintenance_snapshot().unwrap(), snapshot);
}

#[test]
fn ordered_maintenance_query_cannot_overtake_a_blocked_started_event() {
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Observation {
        Event(MemoryMaintenanceStatus),
        Query(Vec<MemoryMaintenanceStatus>),
    }

    let session = mk_session(Vec::new());
    let observations = Arc::new(Mutex::new(Vec::new()));
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let (entered_send, entered_recv) = std::sync::mpsc::channel();
    let entered_send = Arc::new(Mutex::new(Some(entered_send)));
    let (completed_send, completed_recv) = std::sync::mpsc::channel();
    let completed_send = Arc::new(Mutex::new(Some(completed_send)));
    session.set_ordered_memory_maintenance_event_sink(Some({
        let observations = observations.clone();
        let release = release.clone();
        let entered_send = entered_send.clone();
        let completed_send = completed_send.clone();
        Arc::new(move |event| {
            let status = match event {
                MemoryMaintenanceEvent::AutoMemory { run } => run.status,
                MemoryMaintenanceEvent::AutoDream { run } => run.status,
            };
            observations
                .lock()
                .unwrap()
                .push(Observation::Event(status));
            if status == MemoryMaintenanceStatus::Started {
                if let Some(entered) = entered_send.lock().unwrap().take() {
                    let _ = entered.send(());
                }
                let (released, ready) = &*release;
                let mut released = released.lock().unwrap();
                while !*released {
                    released = ready.wait(released).unwrap();
                }
            } else if status == MemoryMaintenanceStatus::Completed {
                if let Some(completed) = completed_send.lock().unwrap().take() {
                    let _ = completed.send(());
                }
            }
        })
    }));
    let started = AutoMemoryRun {
        run_id: "ordered-query-race".into(),
        session_id: session.id.clone(),
        trigger: MemoryMaintenanceTrigger::Auto,
        scope: AgentMemoryScope::Project,
        target_dir: "/tmp/ordered-query-race".into(),
        status: MemoryMaintenanceStatus::Started,
        started_at: 1,
        ended_at: None,
        reason: None,
        error: None,
        message_count: 1,
        cursor_message_uuid: None,
        files_touched: Vec::new(),
        turn_count: None,
    };
    let event_session = session.clone();
    let event_started = started.clone();
    let event_thread = std::thread::spawn(move || {
        event_session.record_memory_maintenance_event(MemoryMaintenanceEvent::AutoMemory {
            run: event_started,
        });
    });
    entered_recv
        .recv_timeout(CROSS_THREAD_LIVENESS_TIMEOUT)
        .expect("started event reached the ordered sink while holding the gate");

    let query_session = session.clone();
    let query_observations = observations.clone();
    let (query_started_send, query_started_recv) = std::sync::mpsc::channel();
    let (query_done_send, query_done_recv) = std::sync::mpsc::channel();
    let query_thread = std::thread::spawn(move || {
        query_started_send.send(()).unwrap();
        query_session
            .with_ordered_memory_maintenance_snapshot(|snapshot| {
                query_observations.lock().unwrap().push(Observation::Query(
                    snapshot
                        .auto_memory_runs
                        .iter()
                        .map(|run| run.status)
                        .collect(),
                ));
            })
            .unwrap();
        query_done_send.send(()).unwrap();
    });
    query_started_recv.recv().unwrap();
    assert!(query_done_recv
        .recv_timeout(std::time::Duration::from_millis(50))
        .is_err());

    {
        let (released, ready) = &*release;
        *released.lock().unwrap() = true;
        ready.notify_all();
    }
    event_thread.join().unwrap();
    query_done_recv
        .recv_timeout(CROSS_THREAD_LIVENESS_TIMEOUT)
        .expect("query enqueues after the blocked started event");
    query_thread.join().unwrap();

    let mut completed = started;
    completed.status = MemoryMaintenanceStatus::Completed;
    completed.ended_at = Some(2);
    completed.cursor_message_uuid = Some("ordered-query-race-cursor".into());
    completed.turn_count = Some(1);
    session.record_memory_maintenance_event(MemoryMaintenanceEvent::AutoMemory { run: completed });
    completed_recv
        .recv_timeout(CROSS_THREAD_LIVENESS_TIMEOUT)
        .expect("completed event reaches the ordered sink");
    assert_eq!(
        *observations.lock().unwrap(),
        vec![
            Observation::Event(MemoryMaintenanceStatus::Started),
            Observation::Query(vec![MemoryMaintenanceStatus::Started]),
            Observation::Event(MemoryMaintenanceStatus::Completed),
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn blocking_isolated_memory_maintenance_sink_times_out_and_cannot_reopen() {
    let session = mk_session(Vec::new());
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let entered_tx = Arc::new(Mutex::new(Some(entered_tx)));
    let (exited_tx, exited_rx) = tokio::sync::oneshot::channel();
    let exited_tx = Arc::new(Mutex::new(Some(exited_tx)));
    let callback_calls = Arc::new(AtomicUsize::new(0));
    session.set_memory_maintenance_event_sink(Some({
        let gate = gate.clone();
        let entered_tx = entered_tx.clone();
        let exited_tx = exited_tx.clone();
        let callback_calls = callback_calls.clone();
        Arc::new(move |_event| {
            callback_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(entered) = entered_tx.lock().unwrap().take() {
                let _ = entered.send(());
            }
            let (released, ready) = &*gate;
            let mut released = released.lock().unwrap();
            while !*released {
                released = ready.wait(released).unwrap();
            }
            if let Some(exited) = exited_tx.lock().unwrap().take() {
                let _ = exited.send(());
            }
        })
    }));
    session.record_memory_maintenance_event(MemoryMaintenanceEvent::AutoMemory {
        run: AutoMemoryRun {
            run_id: "blocking-isolated-sink".into(),
            session_id: session.id.clone(),
            trigger: MemoryMaintenanceTrigger::Auto,
            scope: AgentMemoryScope::Project,
            target_dir: "/tmp/blocking-isolated-sink".into(),
            status: MemoryMaintenanceStatus::Skipped,
            started_at: 1,
            ended_at: Some(2),
            reason: Some(AutoMemoryRunReason::EmptyInput),
            error: None,
            message_count: 1,
            cursor_message_uuid: None,
            files_touched: Vec::new(),
            turn_count: None,
        },
    });
    entered_rx.await.expect("isolated callback entered");

    let closing_session = session.clone();
    let close = tokio::spawn(async move { closing_session.shutdown_and_wait().await });
    while !session.lifecycle.is_disposed() {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(std::time::Duration::from_millis(
        SESSION_SHUTDOWN_CLEANUP_MS + 1,
    ))
    .await;
    let error = close
        .await
        .expect("shutdown owner task")
        .expect_err("a blocked Host callback must make cleanup incomplete");
    assert!(
        error.contains("memory maintenance event dispatcher timed out during shutdown"),
        "{error}"
    );
    assert_eq!(callback_calls.load(Ordering::SeqCst), 1);

    let post_close_calls = Arc::new(AtomicUsize::new(0));
    session.set_memory_maintenance_event_sink(Some({
        let post_close_calls = post_close_calls.clone();
        Arc::new(move |_event| {
            post_close_calls.fetch_add(1, Ordering::SeqCst);
        })
    }));
    session.record_memory_maintenance_event(MemoryMaintenanceEvent::AutoMemory {
        run: AutoMemoryRun {
            run_id: "isolated-sink-after-close".into(),
            session_id: session.id.clone(),
            trigger: MemoryMaintenanceTrigger::Auto,
            scope: AgentMemoryScope::Project,
            target_dir: "/tmp/isolated-sink-after-close".into(),
            status: MemoryMaintenanceStatus::Skipped,
            started_at: 3,
            ended_at: Some(4),
            reason: Some(AutoMemoryRunReason::Shutdown),
            error: None,
            message_count: 1,
            cursor_message_uuid: None,
            files_touched: Vec::new(),
            turn_count: None,
        },
    });
    tokio::task::yield_now().await;
    assert_eq!(
        post_close_calls.load(Ordering::SeqCst),
        0,
        "permanent dispatcher closing must reject post-shutdown callback admission"
    );
    {
        let state = session.maintenance.isolated_sink.lock().unwrap();
        assert!(state.closing);
        assert!(state.dispatcher.is_none());
    }

    {
        let (released, ready) = &*gate;
        *released.lock().unwrap() = true;
        ready.notify_all();
    }
    exited_rx
        .await
        .expect("the released callback must leave its dispatcher thread cleanly");
}

#[cfg(unix)]
#[tokio::test]
async fn team_create_collision_uses_a_fresh_cc_list_and_bound_create_is_rejected() {
    let root = temp_store_dir();
    let _config = ConfigEnvGuard::set(root.to_str().unwrap());
    let collision = RuntimeString::from("Existing_Team");
    swarm_write_team_config(&TeamConfigInput {
        team_name: collision.clone(),
        description: Some("existing".into()),
        agent_type: None,
        session_id: "legacy-session".into(),
        model: "legacy-model".into(),
        cwd: "/legacy".into(),
        created_at: 1,
        joined_at: 1,
    })
    .await
    .unwrap();
    swarm_register_peer(
        &collision,
        &RuntimeString::from("legacy-peer"),
        Some(&RuntimeString::from("legacy-session")),
        1,
    )
    .await
    .unwrap();
    let collision_list = team_task_list_id(&collision);
    crate::tasks::task_store::create_task(
        &collision_list,
        crate::tasks::task_store::CreateTaskInput {
            subject: "legacy task".into(),
            description: "must survive".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let mut config = default_config();
    config.swarm = true;
    let session = mk_session_with(Vec::new(), config, Vec::new());
    let controller = session
        .swarm_controller(session.main_swarm_cell())
        .expect("swarm enabled");
    let created = controller
        .create(collision.clone(), None, Some("researcher".into()))
        .await;
    assert_ne!(created.identity.team, collision);
    assert_eq!(created.identity.peer, "team-lead");
    assert_eq!(
        created.lead_agent_id,
        RuntimeString::from("team-lead@").concat(&created.identity.team)
    );
    let fresh_list = team_task_list_id(&created.identity.team);
    assert_eq!(session.task_list_id(), fresh_list);
    let fresh = session.get_task_list(Default::default()).await.unwrap();
    assert!(fresh.tasks.is_empty());
    assert_eq!(fresh.task_list_id, fresh_list);
    assert_eq!(
        crate::tasks::task_store::list_tasks(&collision_list)
            .await
            .unwrap()[0]
            .subject,
        "legacy task",
        "a name collision must not reset or join the pre-existing team list"
    );

    let duplicate =
        std::panic::AssertUnwindSafe(controller.create(created.identity.team.clone(), None, None))
            .catch_unwind()
            .await;
    assert!(
        duplicate.is_err(),
        "CC rejects TeamCreate whenever the leader is already bound, including the same name"
    );

    let removed = controller.remove(&created.identity.team, true).await;
    assert!(removed.removed);
    assert!(controller.current().is_none());
    assert_eq!(session.task_list_id(), session.id);
    let recreated = controller
        .create(created.identity.team.clone(), None, None)
        .await;
    assert_eq!(recreated.identity.team, created.identity.team);
    assert!(session
        .get_task_list(Default::default())
        .await
        .unwrap()
        .tasks
        .is_empty());
    assert!(
        controller
            .remove(&recreated.identity.team, true)
            .await
            .removed
    );
    assert_eq!(session.task_list_id(), session.id);
    assert!(session.shutdown_and_wait().await.is_ok());
    std::fs::remove_dir_all(root).ok();
}

#[cfg(unix)]
#[tokio::test]
async fn team_create_overwrites_a_malformed_same_name_config_without_randomizing() {
    let root = temp_store_dir();
    let _config = ConfigEnvGuard::set(root.to_str().unwrap());
    let team_dir = root.join("teams/malformed-team");
    std::fs::create_dir_all(&team_dir).unwrap();
    std::fs::write(team_dir.join("config.json"), "{not-json").unwrap();

    let mut config = default_config();
    config.swarm = true;
    let session = mk_session_with(Vec::new(), config, Vec::new());
    let controller = session
        .swarm_controller(session.main_swarm_cell())
        .expect("swarm enabled");
    let created = controller.create("Malformed_Team".into(), None, None).await;
    assert_eq!(created.identity.team, "Malformed_Team");
    assert_eq!(created.lead_agent_id, "team-lead@Malformed_Team");
    assert!(crate::tasks::swarm_store::team_config_exists(&created.identity.team).await);

    assert!(session.shutdown_and_wait().await.is_ok());
    assert!(!team_dir.exists());
    std::fs::remove_dir_all(root).ok();
}

#[cfg(unix)]
#[tokio::test]
async fn team_create_writes_cc_config_and_shutdown_cleans_team_and_task_directories() {
    let root = temp_store_dir();
    let _config = ConfigEnvGuard::set(root.to_str().unwrap());
    let mut config = default_config();
    config.swarm = true;
    let expected_model = config.model.clone();
    let session = mk_session_with(Vec::new(), config, Vec::new());
    let controller = session
        .swarm_controller(session.main_swarm_cell())
        .expect("swarm enabled");
    let created = controller
        .create(
            "My_Team".into(),
            Some("Purpose".into()),
            Some("researcher".into()),
        )
        .await;

    let expected_team_dir = root.join("teams/my-team");
    let expected_team_file = expected_team_dir.join("config.json");
    assert_eq!(
        created.team_file_path,
        RuntimeString::from(expected_team_file.to_str().unwrap())
    );
    assert_eq!(created.identity.peer, "team-lead");
    assert_eq!(created.lead_agent_id, "team-lead@My_Team");
    let persisted = runtime_json::parse(&RuntimeString::from(
        std::fs::read_to_string(&expected_team_file).unwrap(),
    ))
    .unwrap();
    assert_eq!(persisted["name"], RuntimeJson::String("My_Team".into()));
    assert_eq!(
        persisted["description"],
        RuntimeJson::String("Purpose".into())
    );
    assert_eq!(
        persisted["leadAgentId"],
        RuntimeJson::String("team-lead@My_Team".into())
    );
    assert_eq!(
        persisted["leadSessionId"],
        RuntimeJson::String(session.id.clone())
    );
    assert_eq!(
        persisted["members"][0]["name"],
        RuntimeJson::String("team-lead".into())
    );
    assert_eq!(
        persisted["members"][0]["agentType"],
        RuntimeJson::String("researcher".into())
    );
    assert_eq!(
        persisted["members"][0]["model"],
        RuntimeJson::String(expected_model)
    );
    assert!(root.join("tasks/my-team").is_dir());

    assert!(session.shutdown_and_wait().await.is_ok());
    assert!(!expected_team_dir.exists());
    assert!(!root.join("tasks/my-team").exists());
    std::fs::remove_dir_all(root).ok();
}

#[cfg(unix)]
#[tokio::test]
async fn team_create_reset_failure_stays_unbound_and_defers_registered_cleanup_to_shutdown() {
    let root = temp_store_dir();
    let _config = ConfigEnvGuard::set(root.to_str().unwrap());
    let mut config = default_config();
    config.swarm = true;
    let session = mk_session_with(Vec::new(), config, Vec::new());
    let controller = session
        .swarm_controller(session.main_swarm_cell())
        .expect("swarm enabled");
    let team = RuntimeString::from("Reset_Fails");
    let list_id = team_task_list_id(&team);
    crate::tasks::task_store::fail_next_task_list_ledger_write(&list_id);

    let failed = std::panic::AssertUnwindSafe(controller.create(team.clone(), None, None))
        .catch_unwind()
        .await;
    assert!(failed.is_err());
    assert!(controller.current().is_none());
    assert_eq!(session.task_list_id(), session.id);
    assert!(
        !swarm_team_confirmed_gone(&team).await,
        "CC keeps the written team file registered for session-end cleanup"
    );
    assert!(crate::tasks::swarm_store::team_config_exists(&team).await);
    assert!(session.shutdown_and_wait().await.is_ok());
    assert!(swarm_team_confirmed_gone(&team).await);
    assert!(!root.join("tasks/reset-fails").exists());
    std::fs::remove_dir_all(root).ok();
}

#[cfg(unix)]
#[tokio::test]
async fn team_create_never_resets_an_env_overridden_unrelated_task_list() {
    let root = temp_store_dir();
    let _config = ConfigEnvGuard::set(root.to_str().unwrap());
    crate::runtime_process::set_process_environment_var("KLAUS_TASK_LIST_ID", "forced");
    crate::tasks::task_store::create_task(
        &RuntimeString::from("forced"),
        crate::tasks::task_store::CreateTaskInput {
            subject: "forced survives".into(),
            description: "unrelated shared list".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut config = default_config();
    config.swarm = true;
    let session = mk_session_with(Vec::new(), config, Vec::new());
    let controller = session
        .swarm_controller(session.main_swarm_cell())
        .expect("swarm enabled");
    let created = controller.create("New_Team".into(), None, None).await;
    assert_eq!(created.identity.team, "New_Team");
    assert_eq!(team_task_list_id(&created.identity.team), "new-team");
    assert_eq!(session.task_list_id(), "forced");
    assert_eq!(
        crate::tasks::task_store::list_tasks(&RuntimeString::from("forced"))
            .await
            .unwrap()[0]
            .subject,
        "forced survives"
    );
    assert!(
        crate::tasks::task_store::list_tasks(&RuntimeString::from("new-team"))
            .await
            .unwrap()
            .is_empty()
    );
    crate::runtime_process::remove_process_environment_var("KLAUS_TASK_LIST_ID");
    assert!(
        controller
            .remove(&created.identity.team, true)
            .await
            .removed
    );
    assert!(session.shutdown_and_wait().await.is_ok());
    std::fs::remove_dir_all(root).ok();
}

#[cfg(unix)]
#[tokio::test(start_paused = true)]
async fn blocking_todo_callback_reports_typed_cleanup_timeout_before_release_then_joins() {
    let root = temp_store_dir();
    let _config = ConfigEnvGuard::set(root.to_str().unwrap());
    let session = mk_session(Vec::new());
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let entered_tx = Arc::new(Mutex::new(Some(entered_tx)));
    let (exited_tx, mut exited_rx) = tokio::sync::oneshot::channel();
    let exited_tx = Arc::new(Mutex::new(Some(exited_tx)));
    let callback_calls = Arc::new(AtomicUsize::new(0));

    // Install the same registry topology as subscribe_task_list_changes,
    // while retaining the concrete subscription so the test can prove the
    // detached deadline path eventually joins after the Host unblocks.
    let (retiring_tx, retiring_rx) = tokio::sync::mpsc::unbounded_channel();
    {
        let mut registry = session.task_list_subscriptions.lock().unwrap();
        registry.retiring_tx = Some(retiring_tx);
        registry.reaper = Some(TaskListSubscriptionReaper::Tokio(tokio::spawn(
            reap_task_list_subscriptions(retiring_rx),
        )));
    }
    let source_session = Arc::downgrade(&session);
    let subscription = subscribe_task_states(
        Arc::new(move || {
            source_session
                .upgrade()
                .filter(|session| !session.lifecycle.is_disposed())
                .map(|session| {
                    let (list_id, generation) = session.task_list_identity_snapshot();
                    TaskListBinding {
                        session_id: session.id.clone(),
                        list_id,
                        generation,
                    }
                })
        }),
        {
            let gate = gate.clone();
            let entered_tx = entered_tx.clone();
            let exited_tx = exited_tx.clone();
            let callback_calls = callback_calls.clone();
            Arc::new(move |_, _, _| {
                callback_calls.fetch_add(1, Ordering::SeqCst);
                if let Some(entered) = entered_tx.lock().unwrap().take() {
                    let _ = entered.send(());
                }
                let (released, ready) = &*gate;
                let mut released = released.lock().unwrap();
                while !*released {
                    released = ready.wait(released).unwrap();
                }
                if let Some(exited) = exited_tx.lock().unwrap().take() {
                    let _ = exited.send(());
                }
            })
        },
    );
    {
        let mut registry = session.task_list_subscriptions.lock().unwrap();
        registry.active.insert(0, subscription.clone());
        registry.next_id = 1;
    }
    entered_rx.await.expect("TodoV2 baseline callback entered");

    let closing_session = session.clone();
    let close = tokio::spawn(async move { closing_session.shutdown_and_wait_detailed().await });
    while !session.lifecycle.is_disposed() {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(std::time::Duration::from_millis(
        SESSION_SHUTDOWN_CLEANUP_MS + 1,
    ))
    .await;
    let outcome = close.await.expect("shutdown owner task");
    assert!(outcome.timed_out, "{outcome:?}");
    assert!(
        outcome
            .errors
            .iter()
            .any(|error| error.contains("TodoV2 watcher cleanup timed out")),
        "{outcome:?}"
    );
    assert_eq!(callback_calls.load(Ordering::SeqCst), 1);
    assert!(
        matches!(
            exited_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ),
        "shutdown must return its typed deadline before the Host callback is released"
    );

    {
        let (released, ready) = &*gate;
        *released.lock().unwrap() = true;
        ready.notify_all();
    }
    exited_rx
        .await
        .expect("released TodoV2 callback must leave its dispatcher");
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        subscription.close_and_wait(),
    )
    .await
    .expect("TodoV2 callback dispatcher did not join after release")
    .unwrap();
    assert!(subscription.is_finished());
    std::fs::remove_dir_all(root).ok();
}

#[cfg(unix)]
#[tokio::test]
async fn blocked_todo_callback_cannot_keep_the_runtime_process_alive() {
    let config = temp_store_dir();
    let executable = std::env::current_exe().unwrap();
    let mut child = std::process::Command::new(executable)
        .arg("--exact")
        .arg("engine::session::turn_tests::todo_blocking_callback_process_helper")
        .arg("--ignored")
        .arg("--test-threads=1")
        .env("KLAUS_CONFIG_DIR", &config)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("launch TodoV2 blocked-callback process helper");
    // Deliberately generous: the helper re-execs this whole test binary and filters
    // down to one case, so on a saturated host its startup alone can take minutes.
    // The assertion is that the runtime process exits at all, never how fast.
    let status = tokio::time::timeout(SUBPROCESS_REEXEC_LIVENESS_TIMEOUT, async {
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await;
    if status.is_err() {
        let _ = child.kill();
    }
    let output = child.wait_with_output().unwrap();
    assert!(
        status.is_ok() && output.status.success(),
        "blocked TodoV2 callback kept the runtime process alive: \
         waited_out={} exit={:?} stdout={} stderr={}",
        status.is_err(),
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    std::fs::remove_dir_all(config).ok();
}

#[cfg(unix)]
#[tokio::test]
async fn pre_runtime_todo_subscription_cannot_keep_the_runtime_process_alive() {
    let config = temp_store_dir();
    let executable = std::env::current_exe().unwrap();
    let mut child = std::process::Command::new(executable)
        .arg("--exact")
        .arg("engine::session::turn_tests::todo_blocking_callback_pre_runtime_process_helper")
        .arg("--ignored")
        .arg("--test-threads=1")
        .env("KLAUS_CONFIG_DIR", &config)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("launch pre-runtime TodoV2 blocked-callback process helper");
    // Deliberately generous: the helper re-execs this whole test binary and filters
    // down to one case, so on a saturated host its startup alone can take minutes.
    // The assertion is that the runtime process exits at all, never how fast.
    let status = tokio::time::timeout(SUBPROCESS_REEXEC_LIVENESS_TIMEOUT, async {
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await;
    if status.is_err() {
        let _ = child.kill();
    }
    let output = child.wait_with_output().unwrap();
    assert!(
        status.is_ok() && output.status.success(),
        "pre-runtime TodoV2 reaper kept the process alive: \
         waited_out={} exit={:?} stdout={} stderr={}",
        status.is_err(),
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    std::fs::remove_dir_all(config).ok();
}

#[cfg(unix)]
#[tokio::test(start_paused = true)]
#[ignore = "subprocess helper invoked by blocked_todo_callback_cannot_keep_the_runtime_process_alive"]
async fn todo_blocking_callback_process_helper() {
    let session = mk_session(Vec::new());
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let entered_tx = Arc::new(Mutex::new(Some(entered_tx)));
    let _unsubscribe = session.subscribe_task_list_changes(Arc::new(move |_| {
        if let Some(entered) = entered_tx.lock().unwrap().take() {
            let _ = entered.send(());
        }
        loop {
            std::thread::park();
        }
    }));
    entered_rx.await.expect("TodoV2 callback entered");

    let closing_session = session.clone();
    let close = tokio::spawn(async move { closing_session.shutdown_and_wait_detailed().await });
    while !session.lifecycle.is_disposed() {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(std::time::Duration::from_millis(
        SESSION_SHUTDOWN_CLEANUP_MS + 1,
    ))
    .await;
    let outcome = close.await.expect("shutdown owner task");
    assert!(outcome.timed_out, "{outcome:?}");
    assert!(outcome
        .errors
        .iter()
        .any(|error| error.contains("TodoV2 watcher cleanup timed out")));
    // Intentionally do not release the Host callback. Returning from this
    // current-thread runtime proves no Tokio blocking-pool waiter was left
    // behind; the detached OS callback thread cannot keep stdio alive.
}

#[cfg(unix)]
#[test]
#[ignore = "subprocess helper invoked by pre_runtime_todo_subscription_cannot_keep_the_runtime_process_alive"]
fn todo_blocking_callback_pre_runtime_process_helper() {
    // Construct and subscribe before any Tokio runtime exists. This forces
    // both the watcher and the Session subscription reaper onto their
    // dedicated OS-thread variants.
    let session = mk_session(Vec::new());
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let _unsubscribe = session.subscribe_task_list_changes(Arc::new(move |_| {
        let _ = entered_tx.try_send(());
        loop {
            std::thread::park();
        }
    }));
    entered_rx
        .recv_timeout(CROSS_THREAD_LIVENESS_TIMEOUT)
        .expect("pre-runtime TodoV2 callback entered");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .expect("construct current-thread TodoV2 shutdown runtime");
    runtime.block_on(async {
        let closing_session = session.clone();
        let close = tokio::spawn(async move { closing_session.shutdown_and_wait_detailed().await });
        while !session.lifecycle.is_disposed() {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(std::time::Duration::from_millis(
            SESSION_SHUTDOWN_CLEANUP_MS + 1,
        ))
        .await;
        let outcome = close.await.expect("pre-runtime shutdown owner task");
        assert!(outcome.timed_out, "{outcome:?}");
        assert!(outcome
            .errors
            .iter()
            .any(|error| error.contains("TodoV2 watcher cleanup timed out")));
    });
    // Intentionally never release the callback. Dropping this runtime and
    // returning proves there is no hidden spawn_blocking join waiter.
}

#[tokio::test(start_paused = true)]
async fn shutdown_force_aborts_uncooperative_maintenance_and_completes_manual_waiters() {
    struct DropMarker(Arc<AtomicBool>);
    impl Drop for DropMarker {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    let session = mk_session(Vec::new());
    let live_events = Arc::new(Mutex::new(Vec::new()));
    session.set_ordered_memory_maintenance_event_sink(Some({
        let live_events = live_events.clone();
        Arc::new(move |event| live_events.lock().unwrap().push(event))
    }));
    let run = AutoMemoryRun {
        run_id: "hard-timeout-auto-memory".into(),
        session_id: session.id.clone(),
        trigger: MemoryMaintenanceTrigger::Auto,
        scope: AgentMemoryScope::Project,
        target_dir: "/tmp/maintenance-timeout".into(),
        status: MemoryMaintenanceStatus::Started,
        started_at: 0,
        ended_at: None,
        reason: None,
        error: None,
        message_count: 1,
        cursor_message_uuid: None,
        files_touched: Vec::new(),
        turn_count: None,
    };
    let late_write = Arc::new(AtomicBool::new(false));
    let late_write_task = late_write.clone();
    let worker_dropped = Arc::new(AtomicBool::new(false));
    let worker_drop_task = worker_dropped.clone();
    let auto_task = MaintenanceTask {
        run_id: run.run_id.clone(),
        kind: MaintenanceTaskKind::AutoMemory,
        cancel: CancellationToken::new(),
        handle: tokio::spawn(async move {
            let _drop_marker = DropMarker(worker_drop_task);
            tokio::time::sleep(std::time::Duration::from_millis(
                SESSION_SHUTDOWN_GRACE_MS * 2,
            ))
            .await;
            late_write_task.store(true, Ordering::SeqCst);
        }),
        manual_completion: None,
    };
    assert!(session
        .register_maintenance_task(
            auto_task,
            Some(MemoryMaintenanceEvent::AutoMemory { run: run.clone() }),
        )
        .is_ok());
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let manual_slot = Arc::new(Mutex::new(Some(done_tx)));
    let manual_task = MaintenanceTask {
        run_id: "hard-timeout-manual-dream".into(),
        kind: MaintenanceTaskKind::ManualDream,
        cancel: CancellationToken::new(),
        handle: tokio::spawn(async move {
            futures::future::pending::<()>().await;
        }),
        manual_completion: Some(manual_slot),
    };
    assert!(session.register_maintenance_task(manual_task, None).is_ok());

    let closing_session = session.clone();
    let first_caller = tokio::spawn(async move { closing_session.shutdown_and_wait().await });
    while !session.maintenance_is_closing() {
        tokio::task::yield_now().await;
    }
    first_caller.abort();
    let _ = first_caller.await;
    let retry_session = session.clone();
    let close = tokio::spawn(async move { retry_session.shutdown_and_wait().await });
    tokio::time::advance(std::time::Duration::from_millis(
        SESSION_SHUTDOWN_GRACE_MS + 1,
    ))
    .await;
    let error = close.await.unwrap().unwrap_err();
    assert!(error.contains(MAINTENANCE_SHUTDOWN_ERROR));
    assert!(
        worker_dropped.load(Ordering::SeqCst),
        "worker must be dropped before shutdown completion is observable"
    );
    let manual = done_rx
        .await
        .expect("manual Dream waiter must be completed");
    assert!(!manual.ok);
    assert_eq!(manual.reason, Some(MANUAL_DREAM_SHUTDOWN_ERROR.into()));

    let snapshot = session.memory_maintenance_snapshot().unwrap();
    assert_eq!(snapshot.auto_memory_runs.len(), 1);
    let terminal = &snapshot.auto_memory_runs[0];
    assert_eq!(terminal.run_id, run.run_id);
    assert_eq!(terminal.status, MemoryMaintenanceStatus::Failed);
    assert_eq!(terminal.reason, Some(AutoMemoryRunReason::Shutdown));
    assert_eq!(terminal.cursor_message_uuid, None);
    assert!(!late_write.load(Ordering::SeqCst));
    tokio::time::advance(std::time::Duration::from_millis(
        SESSION_SHUTDOWN_GRACE_MS * 2,
    ))
    .await;
    tokio::task::yield_now().await;
    assert!(!late_write.load(Ordering::SeqCst));
    assert_eq!(live_events.lock().unwrap().len(), 2);

    let rejected_task = MaintenanceTask {
        run_id: "registration-after-close".into(),
        kind: MaintenanceTaskKind::ManualDream,
        cancel: CancellationToken::new(),
        handle: tokio::spawn(async move {
            futures::future::pending::<()>().await;
        }),
        manual_completion: None,
    };
    let Err(mut rejected_task) = session.register_maintenance_task(rejected_task, None) else {
        panic!("maintenance admission reopened after shutdown");
    };
    rejected_task.handle.abort();
    let _ = (&mut rejected_task.handle).await;
}

#[test]
fn auto_memory_completion_requires_the_matching_live_cursor_commit() {
    let session = mk_session(Vec::new());
    let started = AutoMemoryRun {
        run_id: "cursor-commit-token".into(),
        session_id: session.id.clone(),
        trigger: MemoryMaintenanceTrigger::Auto,
        scope: AgentMemoryScope::Project,
        target_dir: "/tmp/cursor-commit-token".into(),
        status: MemoryMaintenanceStatus::Started,
        started_at: 1,
        ended_at: None,
        reason: None,
        error: None,
        message_count: 1,
        cursor_message_uuid: None,
        files_touched: Vec::new(),
        turn_count: None,
    };
    session.record_memory_maintenance_event(MemoryMaintenanceEvent::AutoMemory {
        run: started.clone(),
    });
    let mut completed = started;
    completed.status = MemoryMaintenanceStatus::Completed;
    completed.ended_at = Some(2);
    completed.cursor_message_uuid = Some("cursor-after".into());
    completed.turn_count = Some(1);

    assert!(!session.finish_memory_maintenance_if_started(
        MemoryMaintenanceEvent::AutoMemory {
            run: completed.clone(),
        },
        None,
    ));
    assert_eq!(
        session
            .memory_maintenance_snapshot()
            .unwrap()
            .auto_memory_runs[0]
            .status,
        MemoryMaintenanceStatus::Started
    );
    assert_eq!(
        *session
            .memory_scheduler
            .last_memory_message_uuid
            .lock()
            .unwrap(),
        None
    );

    assert!(session.finish_memory_maintenance_if_started(
        MemoryMaintenanceEvent::AutoMemory { run: completed },
        Some("cursor-after".into()),
    ));
    assert_eq!(
        session
            .memory_maintenance_snapshot()
            .unwrap()
            .auto_memory_cursor_message_uuid,
        Some("cursor-after".into())
    );
    assert_eq!(
        *session
            .memory_scheduler
            .last_memory_message_uuid
            .lock()
            .unwrap(),
        Some("cursor-after".into())
    );
}

#[test]
fn direct_memory_write_cursor_commits_before_the_ordered_event() {
    let session = mk_session(Vec::new());
    let observed_cursor = Arc::new(Mutex::new(None));
    let weak_session = Arc::downgrade(&session);
    session.set_ordered_memory_maintenance_event_sink(Some({
        let observed_cursor = observed_cursor.clone();
        Arc::new(move |_event| {
            *observed_cursor.lock().unwrap() = weak_session
                .upgrade()
                .and_then(|session| session.memory_message_cursor());
        })
    }));
    let recent = vec![fork_user("direct-memory-write-cursor")];

    session.record_auto_memory_skip(
        Some("/tmp/direct-memory-write-cursor"),
        &recent,
        1,
        AutoMemoryRunReason::DirectMemoryWrite,
        Some("direct-memory-write-cursor".into()),
    );

    assert_eq!(
        *observed_cursor.lock().unwrap(),
        Some("direct-memory-write-cursor".into()),
        "the ordered event must not overtake its live cursor commit"
    );
    let snapshot = session.memory_maintenance_snapshot().unwrap();
    assert_eq!(
        snapshot.auto_memory_cursor_message_uuid,
        Some("direct-memory-write-cursor".into())
    );
    assert_eq!(snapshot.auto_memory_runs.len(), 1);
    assert_eq!(
        snapshot.auto_memory_runs[0].reason,
        Some(AutoMemoryRunReason::DirectMemoryWrite)
    );
}

#[tokio::test]
async fn rejected_maintenance_start_is_not_registered_or_published() {
    let session = mk_session(Vec::new());
    let terminal = AutoMemoryRun {
        run_id: "duplicate-maintenance-id".into(),
        session_id: session.id.clone(),
        trigger: MemoryMaintenanceTrigger::Auto,
        scope: AgentMemoryScope::Project,
        target_dir: "/tmp/duplicate-maintenance-id".into(),
        status: MemoryMaintenanceStatus::Skipped,
        started_at: 1,
        ended_at: Some(1),
        reason: Some(AutoMemoryRunReason::EmptyInput),
        error: None,
        message_count: 0,
        cursor_message_uuid: None,
        files_touched: Vec::new(),
        turn_count: None,
    };
    session.record_memory_maintenance_event(MemoryMaintenanceEvent::AutoMemory {
        run: terminal.clone(),
    });
    let mut started = terminal.clone();
    started.status = MemoryMaintenanceStatus::Started;
    started.ended_at = None;
    started.reason = None;
    let task = MaintenanceTask {
        run_id: started.run_id.clone(),
        kind: MaintenanceTaskKind::AutoMemory,
        cancel: CancellationToken::new(),
        handle: tokio::spawn(futures::future::pending()),
        manual_completion: None,
    };

    let Err(mut rejected) = session.register_maintenance_task(
        task,
        Some(MemoryMaintenanceEvent::AutoMemory { run: started }),
    ) else {
        panic!("a rejected lifecycle start must reject its worker registration");
    };
    assert!(session.maintenance_tasks.lock().unwrap().tasks.is_empty());
    assert_eq!(
        session
            .memory_maintenance_snapshot()
            .unwrap()
            .auto_memory_runs,
        vec![terminal]
    );
    rejected.handle.abort();
    let _ = (&mut rejected.handle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unjoined_synchronous_maintenance_is_not_falsely_made_terminal() {
    let session = mk_session(Vec::new());
    session
        .memory_scheduler
        .extraction_in_progress
        .store(true, Ordering::SeqCst);
    let run = AutoMemoryRun {
        run_id: "sync-blocking-auto-memory".into(),
        session_id: session.id.clone(),
        trigger: MemoryMaintenanceTrigger::Auto,
        scope: AgentMemoryScope::Project,
        target_dir: "/tmp/sync-blocking-maintenance".into(),
        status: MemoryMaintenanceStatus::Started,
        started_at: 0,
        ended_at: None,
        reason: None,
        error: None,
        message_count: 1,
        cursor_message_uuid: None,
        files_touched: Vec::new(),
        turn_count: None,
    };
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (exited_tx, exited_rx) = tokio::sync::oneshot::channel();
    let (done_tx, mut done_rx) = tokio::sync::oneshot::channel();
    let completion_slot = Arc::new(Mutex::new(Some(done_tx)));
    let worker_completion = completion_slot.clone();
    let worker_gate = gate.clone();
    let worker_session = session.clone();
    let mut completed_run = run.clone();
    let handle = tokio::spawn(async move {
        let _ = entered_tx.send(());
        let (released, ready) = &*worker_gate;
        let mut released = released.lock().unwrap();
        while !*released {
            released = ready.wait(released).unwrap();
        }
        completed_run.status = MemoryMaintenanceStatus::Completed;
        completed_run.ended_at = Some(1);
        completed_run.cursor_message_uuid = Some("cursor-after-sync-block".into());
        let _ = worker_session.finish_memory_maintenance_if_started(
            MemoryMaintenanceEvent::AutoMemory { run: completed_run },
            Some("cursor-after-sync-block".into()),
        );
        worker_session
            .memory_scheduler
            .extraction_in_progress
            .store(false, Ordering::SeqCst);
        if let Some(done) = worker_completion.lock().unwrap().take() {
            let _ = done.send(DreamNowCompletion {
                ok: true,
                reason: None,
                files_touched: Some(Vec::new()),
                turn_count: Some(0),
            });
        }
        let _ = exited_tx.send(());
    });
    let task = MaintenanceTask {
        run_id: run.run_id.clone(),
        kind: MaintenanceTaskKind::AutoMemory,
        cancel: CancellationToken::new(),
        handle,
        // Synthetic shared waiter exercises the same ownership rule as a
        // manual Dream without occupying another runtime worker.
        manual_completion: Some(completion_slot),
    };
    assert!(session
        .register_maintenance_task(
            task,
            Some(MemoryMaintenanceEvent::AutoMemory { run: run.clone() }),
        )
        .is_ok());
    entered_rx
        .await
        .expect("worker entered synchronous section");

    let grace_deadline = tokio::time::Instant::now();
    let force_deadline = grace_deadline + std::time::Duration::from_millis(50);
    let error = session
        .drain_maintenance_tasks(
            grace_deadline,
            force_deadline,
            force_deadline,
            force_deadline,
        )
        .await
        .expect_err("unabortable synchronous worker makes cleanup partial");
    assert!(error.contains(MAINTENANCE_SHUTDOWN_ERROR), "{error}");
    assert!(error.contains(MAINTENANCE_FORCE_ABORT_ERROR), "{error}");
    let snapshot = session.memory_maintenance_snapshot().unwrap();
    assert_eq!(snapshot.auto_memory_runs.len(), 1);
    assert_eq!(
        snapshot.auto_memory_runs[0].status,
        MemoryMaintenanceStatus::Started,
        "shutdown must not claim writes/locks ended before the worker joins"
    );
    assert!(session
        .memory_scheduler
        .extraction_in_progress
        .load(Ordering::SeqCst));
    assert!(matches!(
        done_rx.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));

    {
        let (released, ready) = &*gate;
        *released.lock().unwrap() = true;
        ready.notify_all();
    }
    tokio::time::timeout(std::time::Duration::from_secs(2), exited_rx)
        .await
        .expect("released worker exits")
        .expect("worker exit signal remains connected");
    let completion = done_rx.await.expect("actual worker owns completion");
    assert!(completion.ok);
    assert_eq!(
        session
            .memory_maintenance_snapshot()
            .unwrap()
            .auto_memory_runs[0]
            .status,
        MemoryMaintenanceStatus::Completed
    );
    assert!(!session
        .memory_scheduler
        .extraction_in_progress
        .load(Ordering::SeqCst));
}

#[test]
fn only_core_resolved_project_memory_root_gets_internal_authorization() {
    // This test resolves the memory root TWICE against `config_home()`: once
    // inside the session it builds, and once for the expectation below. Any
    // concurrent test that repoints `KLAUS_CONFIG_DIR` between the two reads
    // makes them disagree, so hold the shared config-env lock for the span
    // instead of assuming nobody else moves the home.
    let _config_env = crate::test_support::CONFIG_ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut config = default_config();
    config.agent_memory = true;
    let default_session = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        mk_deps(Vec::new()),
        config.clone(),
        Vec::new(),
        None,
        None,
        None,
        None,
        None,
    );
    let expected_default_root = default_session.memory_roots.main().and_then(|dir| {
        canonical_descendant_scope(
            &resolve_abs(&RuntimePath::from(config_home())),
            &resolve_abs(&RuntimePath::from(dir.clone())),
        )
    });
    assert_eq!(
        default_session.memory_roots.internal_root(),
        expected_default_root
    );

    let root = temp_store_dir().join(".klaus/custom-host-memory");
    let host: Arc<dyn AgentMemoryHost> = Arc::new(SyncTestMemoryHost {
        layout: SyncTestMemoryLayout {
            base: root.to_string_lossy().into_owned(),
        },
        plans: Mutex::new(Vec::new()),
        applies: Mutex::new(Vec::new()),
        apply_results: Mutex::new(Vec::new()),
    });
    let custom_session = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        mk_deps(Vec::new()),
        config,
        Vec::new(),
        Some(host),
        None,
        None,
        None,
        None,
    );
    assert!(custom_session.memory_roots.main().is_some());
    assert_eq!(custom_session.memory_roots.internal_root(), None);
    std::fs::remove_dir_all(root.parent().unwrap().parent().unwrap()).ok();
}

#[cfg(unix)]
#[tokio::test]
async fn symlinked_default_project_memory_root_never_authorizes_external_sensitive_writes() {
    let root = temp_store_dir();
    let config_home = root.join("config");
    let outside = root.join("outside");
    std::fs::create_dir_all(&config_home).unwrap();
    std::fs::create_dir_all(outside.join(".klaus")).unwrap();
    let sentinel = outside.join(".klaus/settings.json");
    std::fs::write(&sentinel, "external sentinel\n").unwrap();
    let _config = ConfigEnvGuard::set(config_home.to_string_lossy().as_ref());

    let cwd = std::env::temp_dir();
    let default_layout = default_agent_memory_layout(None, Some(cwd.to_string_lossy().as_ref()));
    let memory_dir = PathBuf::from(default_layout.dir_for("main", AgentMemoryScope::Project));
    std::fs::create_dir_all(memory_dir.parent().unwrap()).unwrap();
    std::os::unix::fs::symlink(&outside, &memory_dir).unwrap();
    let attack_path = RuntimeString::from(
        memory_dir
            .join(".klaus/settings.json")
            .to_string_lossy()
            .into_owned(),
    );
    let mut attack_input = RuntimeObject::new();
    attack_input.insert("path", RuntimeJson::String(attack_path));
    attack_input.insert(
        "content",
        RuntimeJson::String("overwritten through memory symlink\n".into()),
    );

    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "write-through-root-symlink",
        "write",
        &RuntimeJson::Object(attack_input),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let mut config = default_config();
    config.agent_memory = true;
    config.auto_memory = true;
    config.max_retries = 0;
    let session = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        mk_deps(vec![first, text_turn("done")]),
        config,
        vec![create_write_tool()],
        None,
        None,
        None,
        Some(vec![fork_user("symlinked-default-memory-input")]),
        None,
    );

    session.maybe_extract_memories();
    tokio::time::timeout(CROSS_THREAD_LIVENESS_TIMEOUT, async {
        while session
            .memory_scheduler
            .extraction_in_progress
            .load(Ordering::SeqCst)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("memory extraction should terminate");

    assert_eq!(
        session.memory_roots.internal_root(),
        None,
        "a default memory path that is itself a symlink is not a core-owned scope"
    );
    assert_eq!(
        std::fs::read_to_string(&sentinel).unwrap(),
        "external sentinel\n",
        "the restricted fork must not follow the memory-root symlink"
    );
    assert_eq!(
        *session
            .memory_scheduler
            .last_memory_message_uuid
            .lock()
            .unwrap(),
        None
    );
    let run = &session
        .memory_maintenance_snapshot()
        .unwrap()
        .auto_memory_runs[0];
    assert_eq!(run.status, MemoryMaintenanceStatus::Failed);
    assert_eq!(run.reason, Some(AutoMemoryRunReason::ToolFailure));
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn successful_auto_memory_run_records_terminal_state_and_advances_cursor() {
    let root = temp_store_dir();
    let host: Arc<dyn AgentMemoryHost> = Arc::new(SyncTestMemoryHost {
        layout: SyncTestMemoryLayout {
            base: root.to_string_lossy().into_owned(),
        },
        plans: Mutex::new(Vec::new()),
        applies: Mutex::new(Vec::new()),
        apply_results: Mutex::new(Vec::new()),
    });
    let memory_dir = PathBuf::from(host.layout().dir_for("main", AgentMemoryScope::Project));
    std::fs::create_dir_all(&memory_dir).unwrap();
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "write-memory",
        "write",
        &exact_json!({"path": "MEMORY.md", "content": "# Durable\n"}),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let mut config = default_config();
    config.agent_memory = true;
    config.auto_memory = true;
    config.max_retries = 0;
    let recent = Message::User(UserMessage {
        question_answer: None,
        uuid: "memory-success-input".into(),
        content: vec![ContentBlock::Text(TextBlock {
            text: "remember the durable fact".into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })],
        is_meta: None,
    });
    let session = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        mk_deps(vec![first, text_turn("done")]),
        config,
        vec![create_write_tool()],
        Some(host),
        None,
        None,
        Some(vec![recent]),
        None,
    );

    session.maybe_extract_memories();
    tokio::time::timeout(CROSS_THREAD_LIVENESS_TIMEOUT, async {
        while session
            .memory_scheduler
            .extraction_in_progress
            .load(Ordering::SeqCst)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("memory extraction should terminate");

    assert_eq!(
        *session
            .memory_scheduler
            .last_memory_message_uuid
            .lock()
            .unwrap(),
        Some("memory-success-input".into())
    );
    assert_eq!(
        std::fs::read_to_string(memory_dir.join("MEMORY.md")).unwrap(),
        "# Durable\n"
    );
    let snapshot = session.memory_maintenance_snapshot().unwrap();
    assert_eq!(snapshot.auto_memory_runs.len(), 1);
    let run = &snapshot.auto_memory_runs[0];
    assert_eq!(run.status, MemoryMaintenanceStatus::Completed);
    assert_eq!(run.cursor_message_uuid, Some("memory-success-input".into()));
    assert_eq!(run.turn_count, Some(2));
    assert_eq!(run.files_touched.len(), 1);
    std::fs::remove_dir_all(root).ok();
}

/// An extraction over a memory that has never been written probes an index that
/// is not there yet (MEMORY.md is created BY a pass, not before one — and every
/// projectless session starts exactly there). That probe is normal exploration:
/// when the fork then lands its note and reaches terminal completion, the run
/// is COMPLETED and the cursor advances. Only a failed write/edit — a note that
/// never landed — or a fork that never finished fails the run.
///
/// Failing the run on the probe instead surfaced a spurious
/// `memory fork failed: tool read: File not found: …/MEMORY.md` warning for the
/// first extraction of every fresh memory directory, and held the cursor back
/// from messages whose facts HAD landed.
#[tokio::test]
async fn extraction_completes_when_a_missing_index_probe_fails_mid_turn() {
    let root = temp_store_dir();
    let host: Arc<dyn AgentMemoryHost> = Arc::new(SyncTestMemoryHost {
        layout: SyncTestMemoryLayout {
            base: root.to_string_lossy().into_owned(),
        },
        plans: Mutex::new(Vec::new()),
        applies: Mutex::new(Vec::new()),
        apply_results: Mutex::new(Vec::new()),
    });
    let memory_dir = PathBuf::from(host.layout().dir_for("main", AgentMemoryScope::Project));
    std::fs::create_dir_all(&memory_dir).unwrap();
    // The memory is EMPTY: no index for the fork to read.
    assert!(!memory_dir.join("MEMORY.md").exists());
    // Turn 1 probes the missing index (a REAL read, a REAL not-found failure);
    // turn 2 lands the note; turn 3 finishes.
    let mut probe_turn = vec![ev::message_start(None)];
    probe_turn.extend(exact_tool_use_events(
        "index-probe",
        "read",
        &exact_json!({"path": "MEMORY.md"}),
    ));
    probe_turn.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let mut write_turn = vec![ev::message_start(None)];
    write_turn.extend(exact_tool_use_events(
        "write-note",
        "write",
        &exact_json!({"path": "MEMORY.md", "content": "index"}),
    ));
    write_turn.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let mut config = default_config();
    config.agent_memory = true;
    config.auto_memory = true;
    config.max_retries = 0;
    let session = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        mk_deps(vec![probe_turn, write_turn, text_turn("done")]),
        config,
        vec![create_read_tool(), create_write_tool()],
        Some(host),
        None,
        None,
        Some(vec![fork_user("probe-miss-extraction-input")]),
        None,
    );

    session.maybe_extract_memories();
    tokio::time::timeout(CROSS_THREAD_LIVENESS_TIMEOUT, async {
        while session
            .memory_scheduler
            .extraction_in_progress
            .load(Ordering::SeqCst)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("probing extraction should terminate");

    let snapshot = session.memory_maintenance_snapshot().unwrap();
    assert_eq!(snapshot.auto_memory_runs.len(), 1);
    let run = &snapshot.auto_memory_runs[0];
    assert_eq!(
        run.status,
        MemoryMaintenanceStatus::Completed,
        "a probe that missed must not fail a pass that landed its note, got reason {:?} error {:?}",
        run.reason,
        run.error
    );
    assert_eq!(run.reason, None);
    assert_eq!(run.error, None);
    // The landed note and the advanced cursor are the pass doing its job.
    assert_eq!(
        std::fs::read_to_string(memory_dir.join("MEMORY.md")).unwrap(),
        "index"
    );
    assert_eq!(
        *session
            .memory_scheduler
            .last_memory_message_uuid
            .lock()
            .unwrap(),
        Some("probe-miss-extraction-input".into())
    );
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn auto_memory_turn_count_excludes_transient_provider_retries() {
    let root = temp_store_dir();
    let host: Arc<dyn AgentMemoryHost> = Arc::new(SyncTestMemoryHost {
        layout: SyncTestMemoryLayout {
            base: root.to_string_lossy().into_owned(),
        },
        plans: Mutex::new(Vec::new()),
        applies: Mutex::new(Vec::new()),
        apply_results: Mutex::new(Vec::new()),
    });
    let memory_dir = PathBuf::from(host.layout().dir_for("main", AgentMemoryScope::Project));
    std::fs::create_dir_all(&memory_dir).unwrap();
    let retryable_error = vec![ev::error(crate::types::errors::ProviderError {
        kind: crate::types::errors::ProviderErrorKind::Network,
        message: "temporary reset".into(),
        status: None,
        code: None,
        error_type: None,
        media_origins: None,
        media_repair_complete: false,
        credential_recovery: None,
        retry_after_ms: Some(0),
        request_id: None,
    })];
    let mut write_turn = vec![ev::message_start(None)];
    write_turn.extend(exact_tool_use_events(
        "retry-memory-write",
        "write",
        &exact_json!({"path": "MEMORY.md", "content": "after retry\n"}),
    ));
    write_turn.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let mut config = default_config();
    config.agent_memory = true;
    config.auto_memory = true;
    config.max_retries = 1;
    let session = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        mk_deps(vec![retryable_error, write_turn, text_turn("done")]),
        config,
        vec![create_write_tool()],
        Some(host),
        None,
        None,
        Some(vec![fork_user("retry-count-input")]),
        None,
    );

    session.maybe_extract_memories();
    tokio::time::timeout(CROSS_THREAD_LIVENESS_TIMEOUT, async {
        while session
            .memory_scheduler
            .extraction_in_progress
            .load(Ordering::SeqCst)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("retried extraction should terminate");

    let run = &session
        .memory_maintenance_snapshot()
        .unwrap()
        .auto_memory_runs[0];
    assert_eq!(run.status, MemoryMaintenanceStatus::Completed);
    assert_eq!(run.turn_count, Some(2));
    assert_eq!(
        std::fs::read_to_string(memory_dir.join("MEMORY.md")).unwrap(),
        "after retry\n"
    );
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn auto_memory_fails_at_five_turn_budget_without_advancing_cursor() {
    let root = temp_store_dir();
    let host: Arc<dyn AgentMemoryHost> = Arc::new(SyncTestMemoryHost {
        layout: SyncTestMemoryLayout {
            base: root.to_string_lossy().into_owned(),
        },
        plans: Mutex::new(Vec::new()),
        applies: Mutex::new(Vec::new()),
        apply_results: Mutex::new(Vec::new()),
    });
    let memory_dir = PathBuf::from(host.layout().dir_for("main", AgentMemoryScope::Project));
    std::fs::create_dir_all(&memory_dir).unwrap();
    let mut turns = Vec::new();
    for index in 0..5 {
        let mut input = RuntimeObject::new();
        input.insert(
            "path",
            RuntimeJson::String(format!("topic-{index}.md").into()),
        );
        input.insert(
            "content",
            RuntimeJson::String(format!("fact {index}\n").into()),
        );
        let mut turn = vec![ev::message_start(None)];
        turn.extend(exact_tool_use_events(
            &format!("budget-write-{index}"),
            "write",
            &RuntimeJson::Object(input),
        ));
        turn.push(ev::stop(Some(ModelStopReason::ToolUse), None));
        turns.push(turn);
    }
    let mut config = default_config();
    config.agent_memory = true;
    config.auto_memory = true;
    config.max_retries = 0;
    let session = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        mk_deps(turns),
        config,
        vec![create_write_tool()],
        Some(host),
        None,
        None,
        Some(vec![fork_user("five-turn-budget-input")]),
        None,
    );

    session.maybe_extract_memories();
    tokio::time::timeout(CROSS_THREAD_LIVENESS_TIMEOUT, async {
        while session
            .memory_scheduler
            .extraction_in_progress
            .load(Ordering::SeqCst)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("budgeted extraction should terminate");

    assert_eq!(
        *session
            .memory_scheduler
            .last_memory_message_uuid
            .lock()
            .unwrap(),
        None
    );
    let run = &session
        .memory_maintenance_snapshot()
        .unwrap()
        .auto_memory_runs[0];
    assert_eq!(run.status, MemoryMaintenanceStatus::Failed);
    assert_eq!(run.reason, Some(AutoMemoryRunReason::ForkFailure));
    assert_eq!(run.turn_count, Some(5));
    assert_eq!(run.files_touched.len(), 5);
    assert!(run
        .error
        .as_ref()
        .is_some_and(|error| error.contains("turn_limit_exhausted")));
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn failed_direct_memory_write_does_not_skip_the_extraction_retry() {
    let root = temp_store_dir();
    let host: Arc<dyn AgentMemoryHost> = Arc::new(SyncTestMemoryHost {
        layout: SyncTestMemoryLayout {
            base: root.to_string_lossy().into_owned(),
        },
        plans: Mutex::new(Vec::new()),
        applies: Mutex::new(Vec::new()),
        apply_results: Mutex::new(Vec::new()),
    });
    let memory_dir = PathBuf::from(host.layout().dir_for("main", AgentMemoryScope::Project));
    std::fs::create_dir_all(&memory_dir).unwrap();
    let call_id: RuntimeString = "failed-direct-memory-write".into();
    let mut failed_write_input = RuntimeObject::new();
    failed_write_input.insert(
        "path",
        RuntimeJson::String(
            memory_dir
                .join("MEMORY.md")
                .to_string_lossy()
                .into_owned()
                .into(),
        ),
    );
    failed_write_input.insert("content", RuntimeJson::String("not persisted".into()));
    let failed_assistant = Message::Assistant(AssistantMessage {
        uuid: "failed-direct-assistant".into(),
        content: vec![ContentBlock::ToolUse(
            crate::schemas::content::ToolUseBlock {
                id: call_id.clone(),
                name: "write".into(),
                input: RuntimeJson::Object(failed_write_input),
                tool_kind: None,
                thought_signature: None,
            },
        )],
        model: "parent-model".into(),
        usage: None,
    });
    let failed_result = Message::ToolResult(crate::schemas::message::ToolResultMessage {
        uuid: "failed-direct-result".into(),
        tool_use_id: call_id,
        content: vec![crate::schemas::content::OutputBlock::Text(TextBlock {
            text: "disk full".into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })],
        is_error: true,
        tool_kind: None,
        native_tool_output: None,
        output: None,
        extra_user_blocks: None,
    });
    let recent = fork_user("retry-after-failed-direct-write");
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "extraction-retry-write",
        "write",
        &exact_json!({"path": "MEMORY.md", "content": "retry persisted\n"}),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let mut config = default_config();
    config.agent_memory = true;
    config.auto_memory = true;
    config.max_retries = 0;
    let session = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        mk_deps(vec![first, text_turn("done")]),
        config,
        vec![create_write_tool()],
        Some(host),
        None,
        None,
        Some(vec![failed_assistant, failed_result, recent]),
        None,
    );

    session.maybe_extract_memories();
    tokio::time::timeout(CROSS_THREAD_LIVENESS_TIMEOUT, async {
        while session
            .memory_scheduler
            .extraction_in_progress
            .load(Ordering::SeqCst)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("failed direct write must be retried by extraction");

    let snapshot = session.memory_maintenance_snapshot().unwrap();
    assert_eq!(snapshot.auto_memory_runs.len(), 1);
    assert_eq!(
        snapshot.auto_memory_runs[0].status,
        MemoryMaintenanceStatus::Completed
    );
    assert_eq!(
        *session
            .memory_scheduler
            .last_memory_message_uuid
            .lock()
            .unwrap(),
        Some("retry-after-failed-direct-write".into())
    );
    assert_eq!(
        std::fs::read_to_string(memory_dir.join("MEMORY.md")).unwrap(),
        "retry persisted\n"
    );
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn arbitrary_host_memory_path_does_not_receive_internal_sensitive_bypass() {
    let root = temp_store_dir();
    let host_base = root.join(".klaus/host-selected-memory");
    let host: Arc<dyn AgentMemoryHost> = Arc::new(SyncTestMemoryHost {
        layout: SyncTestMemoryLayout {
            base: host_base.to_string_lossy().into_owned(),
        },
        plans: Mutex::new(Vec::new()),
        applies: Mutex::new(Vec::new()),
        apply_results: Mutex::new(Vec::new()),
    });
    let memory_dir = PathBuf::from(host.layout().dir_for("main", AgentMemoryScope::Project));
    std::fs::create_dir_all(&memory_dir).unwrap();
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "write-host-memory",
        "write",
        &exact_json!({"path": "MEMORY.md", "content": "must ask"}),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let mut config = default_config();
    config.agent_memory = true;
    config.auto_memory = true;
    config.max_retries = 0;
    let recent = Message::User(UserMessage {
        question_answer: None,
        uuid: "host-memory-input".into(),
        content: vec![ContentBlock::Text(TextBlock {
            text: "remember this".into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })],
        is_meta: None,
    });
    let session = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        mk_deps(vec![first, text_turn("done")]),
        config,
        vec![create_write_tool()],
        Some(host),
        None,
        None,
        Some(vec![recent]),
        None,
    );
    assert_eq!(session.memory_roots.internal_root(), None);

    session.maybe_extract_memories();
    tokio::time::timeout(CROSS_THREAD_LIVENESS_TIMEOUT, async {
        while session
            .memory_scheduler
            .extraction_in_progress
            .load(Ordering::SeqCst)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("memory extraction should terminate");

    assert!(!memory_dir.join("MEMORY.md").exists());
    assert_eq!(
        *session
            .memory_scheduler
            .last_memory_message_uuid
            .lock()
            .unwrap(),
        None
    );
    let snapshot = session.memory_maintenance_snapshot().unwrap();
    assert_eq!(snapshot.auto_memory_runs.len(), 1);
    assert_eq!(
        snapshot.auto_memory_runs[0].status,
        MemoryMaintenanceStatus::Failed
    );
    assert_eq!(
        snapshot.auto_memory_runs[0].reason,
        Some(AutoMemoryRunReason::ToolFailure)
    );
    std::fs::remove_dir_all(root).ok();
}

#[cfg(unix)]
#[tokio::test]
async fn custom_host_memory_fork_cannot_escape_through_missing_symlink_descendants() {
    let root = temp_store_dir();
    let host: Arc<dyn AgentMemoryHost> = Arc::new(SyncTestMemoryHost {
        layout: SyncTestMemoryLayout {
            base: root.join("host-memory").to_string_lossy().into_owned(),
        },
        plans: Mutex::new(Vec::new()),
        applies: Mutex::new(Vec::new()),
        apply_results: Mutex::new(Vec::new()),
    });
    let memory_dir = PathBuf::from(host.layout().dir_for("main", AgentMemoryScope::Project));
    let outside = root.join("outside");
    std::fs::create_dir_all(&memory_dir).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    std::os::unix::fs::symlink(&outside, memory_dir.join("escape")).unwrap();

    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "write-symlink-escape",
        "write",
        &exact_json!({"path": "escape/new/MEMORY.md", "content": "escaped"}),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let mut config = default_config();
    config.agent_memory = true;
    config.auto_memory = true;
    config.max_retries = 0;
    let session = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        mk_deps(vec![first, text_turn("done")]),
        config,
        vec![create_write_tool()],
        Some(host),
        None,
        None,
        Some(vec![fork_user("symlink-escape-input")]),
        None,
    );

    session.maybe_extract_memories();
    tokio::time::timeout(CROSS_THREAD_LIVENESS_TIMEOUT, async {
        while session
            .memory_scheduler
            .extraction_in_progress
            .load(Ordering::SeqCst)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("memory extraction should terminate");

    assert!(!outside.join("new/MEMORY.md").exists());
    assert_eq!(
        *session
            .memory_scheduler
            .last_memory_message_uuid
            .lock()
            .unwrap(),
        None
    );
    let run = &session
        .memory_maintenance_snapshot()
        .unwrap()
        .auto_memory_runs[0];
    assert_eq!(run.status, MemoryMaintenanceStatus::Failed);
    assert_eq!(run.reason, Some(AutoMemoryRunReason::ToolFailure));
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn tool_reported_memory_failure_is_terminal_evented_and_keeps_cursor() {
    let root = temp_store_dir();
    let host: Arc<dyn AgentMemoryHost> = Arc::new(SyncTestMemoryHost {
        layout: SyncTestMemoryLayout {
            base: root.to_string_lossy().into_owned(),
        },
        plans: Mutex::new(Vec::new()),
        applies: Mutex::new(Vec::new()),
        apply_results: Mutex::new(Vec::new()),
    });
    let memory_dir = host.layout().dir_for("main", AgentMemoryScope::Project);
    std::fs::create_dir_all(&memory_dir).unwrap();
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "write-failed",
        "write",
        &exact_json!({"path": "MEMORY.md", "content": "durable"}),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let mut config = default_config();
    config.agent_memory = true;
    config.auto_memory = true;
    config.max_retries = 0;
    let recent = Message::User(UserMessage {
        question_answer: None,
        uuid: "memory-tool-failure-input".into(),
        content: vec![ContentBlock::Text(TextBlock {
            text: "remember this".into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })],
        is_meta: None,
    });
    let prior = fork_user("memory-tool-failure-prior");
    let non_visible = fork_tool_result("memory-tool-failure-tool-result");
    let session = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        mk_deps(vec![first, text_turn("done")]),
        config,
        vec![Arc::new(FailedMemoryWriteTool {
            schema: JsonSchema::new(),
        })],
        Some(host),
        None,
        None,
        Some(vec![prior, recent, non_visible]),
        None,
    );
    session.record_auto_memory_skip(
        Some(&memory_dir),
        &[fork_user("memory-tool-failure-prior")],
        0,
        AutoMemoryRunReason::DirectMemoryWrite,
        Some("memory-tool-failure-prior".into()),
    );
    let live_events = Arc::new(Mutex::new(Vec::new()));
    session.set_ordered_memory_maintenance_event_sink(Some({
        let live_events = live_events.clone();
        Arc::new(move |event| live_events.lock().unwrap().push(event))
    }));

    session.maybe_extract_memories();
    tokio::time::timeout(CROSS_THREAD_LIVENESS_TIMEOUT, async {
        while session
            .memory_scheduler
            .extraction_in_progress
            .load(Ordering::SeqCst)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("memory extraction should terminate");

    assert_eq!(
        *session
            .memory_scheduler
            .last_memory_message_uuid
            .lock()
            .unwrap(),
        Some("memory-tool-failure-prior".into())
    );
    let snapshot = session.memory_maintenance_snapshot().unwrap();
    assert_eq!(snapshot.auto_memory_runs.len(), 2);
    let run = snapshot.auto_memory_runs.last().unwrap();
    assert_eq!(run.status, MemoryMaintenanceStatus::Failed);
    assert_eq!(run.reason, Some(AutoMemoryRunReason::ToolFailure));
    assert!(run
        .error
        .as_ref()
        .is_some_and(|error| error.contains("tool write") && error.contains("disk full")));
    let live_events = live_events.lock().unwrap();
    assert_eq!(live_events.len(), 2);
    let MemoryMaintenanceEvent::AutoMemory { run: started } = &live_events[0] else {
        panic!("expected auto-memory start");
    };
    let MemoryMaintenanceEvent::AutoMemory { run: failed } = &live_events[1] else {
        panic!("expected auto-memory failure");
    };
    assert_eq!(started.run_id, failed.run_id);
    assert_eq!(started.status, MemoryMaintenanceStatus::Started);
    assert_eq!(failed.status, MemoryMaintenanceStatus::Failed);
    assert_eq!(started.message_count, 1);
    assert_eq!(failed.message_count, 1);
    assert_eq!(
        started.cursor_message_uuid,
        Some("memory-tool-failure-prior".into())
    );
    assert_eq!(failed.cursor_message_uuid, started.cursor_message_uuid);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn partial_memory_writes_are_reported_when_a_later_required_write_fails() {
    let root = temp_store_dir();
    let host: Arc<dyn AgentMemoryHost> = Arc::new(SyncTestMemoryHost {
        layout: SyncTestMemoryLayout {
            base: root.to_string_lossy().into_owned(),
        },
        plans: Mutex::new(Vec::new()),
        applies: Mutex::new(Vec::new()),
        apply_results: Mutex::new(Vec::new()),
    });
    let memory_dir = host.layout().dir_for("main", AgentMemoryScope::Project);
    std::fs::create_dir_all(&memory_dir).unwrap();
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "first-memory-write",
        "write",
        &exact_json!({"path": "first.md", "content": "first"}),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let mut second = vec![ev::message_start(None)];
    second.extend(exact_tool_use_events(
        "second-memory-write",
        "write",
        &exact_json!({"path": "second.md", "content": "second"}),
    ));
    second.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let mut config = default_config();
    config.agent_memory = true;
    config.auto_memory = true;
    config.max_retries = 0;
    let session = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        mk_deps(vec![first, second, text_turn("done")]),
        config,
        vec![Arc::new(PartialMemoryWriteTool {
            schema: JsonSchema::new(),
            calls: AtomicUsize::new(0),
        })],
        Some(host),
        None,
        None,
        Some(vec![fork_user("partial-memory-input")]),
        None,
    );

    session.maybe_extract_memories();
    tokio::time::timeout(CROSS_THREAD_LIVENESS_TIMEOUT, async {
        while session
            .memory_scheduler
            .extraction_in_progress
            .load(Ordering::SeqCst)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("partial failure run should terminate");

    assert_eq!(
        *session
            .memory_scheduler
            .last_memory_message_uuid
            .lock()
            .unwrap(),
        None
    );
    let run = &session
        .memory_maintenance_snapshot()
        .unwrap()
        .auto_memory_runs[0];
    assert_eq!(run.status, MemoryMaintenanceStatus::Failed);
    assert_eq!(run.reason, Some(AutoMemoryRunReason::ToolFailure));
    assert_eq!(run.files_touched, vec![RuntimeString::from("first.md")]);
    assert!(run
        .error
        .as_ref()
        .is_some_and(|error| error.contains("disk full")));
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn provider_eof_without_message_stop_fails_memory_run_and_keeps_cursor() {
    let root = temp_store_dir();
    let host: Arc<dyn AgentMemoryHost> = Arc::new(SyncTestMemoryHost {
        layout: SyncTestMemoryLayout {
            base: root.to_string_lossy().into_owned(),
        },
        plans: Mutex::new(Vec::new()),
        applies: Mutex::new(Vec::new()),
        apply_results: Mutex::new(Vec::new()),
    });
    let memory_dir = host.layout().dir_for("main", AgentMemoryScope::Project);
    std::fs::create_dir_all(&memory_dir).unwrap();
    let mut config = default_config();
    config.agent_memory = true;
    config.auto_memory = true;
    config.max_retries = 0;
    let session = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        mk_deps(vec![Vec::new()]),
        config,
        Vec::new(),
        Some(host),
        None,
        None,
        Some(vec![fork_user("provider-eof-memory-input")]),
        None,
    );

    session.maybe_extract_memories();
    tokio::time::timeout(CROSS_THREAD_LIVENESS_TIMEOUT, async {
        while session
            .memory_scheduler
            .extraction_in_progress
            .load(Ordering::SeqCst)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("memory extraction should terminate");

    assert_eq!(
        *session
            .memory_scheduler
            .last_memory_message_uuid
            .lock()
            .unwrap(),
        None
    );
    let run = &session
        .memory_maintenance_snapshot()
        .unwrap()
        .auto_memory_runs[0];
    assert_eq!(run.status, MemoryMaintenanceStatus::Failed);
    assert_eq!(run.reason, Some(AutoMemoryRunReason::ForkFailure));
    assert!(run
        .error
        .as_ref()
        .is_some_and(|error| error.contains("message_stop")));
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn dream_memory_fork_uses_a_background_idle_timeout() {
    let root = temp_store_dir();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut deps = mk_deps(Vec::new());
    deps.call_model = {
        let calls = calls.clone();
        Box::new(move |_request, signal| {
            let call = calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(stream! {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                if signal.is_cancelled() {
                    return;
                }
                yield ev::message_start(None);
                if call == 0 {
                    for event in exact_tool_use_events(
                        "write-slow",
                        "write",
                        &exact_json!({
                            "path": "consolidated.md",
                            "content": "slow consolidation\n"
                        }),
                    ) {
                        yield event;
                    }
                    yield ev::stop(Some(ModelStopReason::ToolUse), None);
                } else {
                    yield ev::text("done");
                    yield ev::stop(Some(ModelStopReason::EndTurn), None);
                }
            }) as ModelStream
        })
    };
    let (session, memory_dir) = dream_session_with_deps(&root, true, deps, Some(5));

    let mut task = session.dream_now().await.unwrap();
    let completion = task.done.take().unwrap().await.unwrap();

    assert!(completion.ok, "{:?}", completion.reason);
    assert_eq!(
        std::fs::read_to_string(memory_dir.join("consolidated.md")).unwrap(),
        "slow consolidation\n"
    );
    std::fs::remove_dir_all(root).ok();
}

struct CaptureRetryTool {
    schema: JsonSchema,
    seen: Arc<Mutex<Option<Option<bool>>>>,
}

struct FailedMemoryWriteTool {
    schema: JsonSchema,
}

struct PartialMemoryWriteTool {
    schema: JsonSchema,
    calls: AtomicUsize,
}

#[async_trait::async_trait]
impl Tool for PartialMemoryWriteTool {
    fn static_name(&self) -> &str {
        "write"
    }

    fn static_description(&self) -> &str {
        "test partial memory write failure"
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
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
        _input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, crate::types::errors::ToolError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            (ctx.emit)(ToolDomainEvent::FileChange {
                change: crate::types::events::FileChange {
                    turn_diff: None,
                    path: "first.md".into(),
                    kind: crate::types::events::FileChangeKind::Create,
                    diff: None,
                    added: Some(1),
                    removed: Some(0),
                },
            })
            .await;
            Ok(text_result(Json::Null, "first persisted", false))
        } else {
            Ok(text_result(Json::Null, "disk full", true))
        }
    }
}

#[async_trait::async_trait]
impl Tool for FailedMemoryWriteTool {
    fn static_name(&self) -> &str {
        "write"
    }

    fn static_description(&self) -> &str {
        "test memory write failure"
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
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
        _input: Json,
        _ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, crate::types::errors::ToolError> {
        Ok(text_result(Json::Null, "disk full", true))
    }
}

struct NoopBrowserRuntime;

#[async_trait::async_trait]
unsafe impl BrowserExecutor for NoopBrowserRuntime {
    async fn execute(
        &self,
        _request: crate::browser::BrowserRuntimeRequest,
        _signal: AbortSignal,
        _emit: crate::browser::BrowserEventSink,
    ) -> Result<crate::browser::BrowserRuntimeResponse, crate::browser::BrowserRuntimeError> {
        Ok(crate::browser::BrowserRuntimeResponse::default())
    }
}

fn spawn_free<T>(executor: Arc<T>) -> Arc<dyn BrowserRuntime>
where
    T: BrowserExecutor + 'static,
{
    // SAFETY: this no-op test executor only returns an in-memory fixture
    // response and has no local process authority.
    unsafe { attest_spawn_free(executor) }
}

#[async_trait::async_trait]
impl Tool for CaptureRetryTool {
    fn static_name(&self) -> &str {
        "capture"
    }

    fn static_description(&self) -> &str {
        "capture context"
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
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
        _input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        *self.seen.lock().unwrap() = Some(ctx.sandbox_retry_on_denial);
        Ok(text_result(Json::Null, "captured".to_string(), false))
    }
}

#[tokio::test]
async fn run_produces_the_models_final_text_and_completes() {
    let session = mk_session(vec![text_turn("done")]);
    let result = session
        .run(Input::Text("go".into()), TurnOptions::default())
        .await;
    assert_eq!(result.stop_reason, TerminalReason::Completed);
    assert_eq!(result.final_text, "done");
    assert!(result.error.is_none());
    // The user turn + the assistant reply are in the history.
    let hist = session.history();
    assert!(matches!(hist.first(), Some(Message::User(_))));
    assert!(matches!(hist.last(), Some(Message::Assistant(_))));
}

#[tokio::test]
async fn run_parses_lone_surrogate_structured_output() {
    let raw = r#"{"value":"\ud800"}"#;
    let session = mk_session(vec![text_turn(raw)]);

    let result = session
        .run(
            Input::Text("return structured JSON".into()),
            TurnOptions {
                output_schema: Some(JsonSchema::new()),
                ..Default::default()
            },
        )
        .await;

    let structured = result
        .structured_output
        .expect("JSON.parse accepts escaped lone surrogates");
    assert_eq!(runtime_json::stringify(&structured), raw);
}

#[test]
fn generated_title_parser_accepts_model_json_with_a_lone_surrogate() {
    let title = parse_generated_title(&RuntimeString::from(r#"{"title":"\ud800"}"#))
        .expect("title JSON should use the model JSON parser");

    assert_eq!(runtime_json::stringify(&Json::String(title)), r#""\ud800""#);
}

/// One recorded `turn_lifecycle` append: the turn it belongs to, the status, and
/// the failure message when the status carries one.
type RecordedLifecycle = (Option<RuntimeString>, RuntimeString, Option<RuntimeString>);

/// Records turn_context boundaries and turn_lifecycle appends, nothing else.
#[derive(Debug, Default)]
struct RecordingTurnRollout {
    turn_contexts: Mutex<Vec<TurnContextSnapshot>>,
    messages: Mutex<Vec<Message>>,
    lifecycle: Mutex<Vec<RecordedLifecycle>>,
    lifecycle_times: Mutex<Vec<Option<i64>>>,
    lifecycle_final_messages: Mutex<Vec<Option<RuntimeString>>>,
    file_changes: Mutex<Vec<crate::types::events::FileChange>>,
}

impl RecordingTurnRollout {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

#[async_trait::async_trait]
impl LiveRollout for RecordingTurnRollout {
    async fn prepare_batch(
        &self,
        maximum_records: usize,
    ) -> std::io::Result<crate::store::session_store::PreparedRolloutBatch<'_>> {
        crate::testing::immediate_rollout::prepare_immediate_test_rollout_batch(
            self,
            maximum_records,
        )
    }

    async fn append_message(&self, message: Message) {
        self.messages.lock().unwrap().push(message);
    }

    async fn append_turn_context(&self, ctx: TurnContextSnapshot) {
        self.turn_contexts.lock().unwrap().push(ctx);
    }

    async fn append_compaction(&self, _record: CompactionRecord) {}

    async fn append_event(&self, event: RuntimeString, opts: Option<AppendEventOptions>) {
        if event == super::FILE_CHANGE_ROLLOUT_EVENT {
            let payload = opts
                .and_then(|opts| opts.payload)
                .expect("file change payload");
            self.file_changes.lock().unwrap().push(
                crate::runtime_values::runtime_codec::decode_value(&payload)
                    .expect("the file change payload is the event's own encoding"),
            );
            return;
        }
        if event != super::TURN_LIFECYCLE_ROLLOUT_EVENT {
            return;
        }
        let opts = opts.expect("turn lifecycle options");
        let payload = opts.payload.expect("turn lifecycle payload");
        let object = payload.as_object().expect("turn lifecycle payload object");
        let status = object
            .get("status")
            .and_then(|s| s.as_str())
            .expect("turn lifecycle status")
            .clone();
        let error = object.get("error").and_then(|e| e.as_str()).cloned();
        let finished_at_ms = object
            .get("finishedAtMs")
            .and_then(|value| value.as_f64())
            .map(|value| value as i64);
        let final_message_id = object
            .get("finalMessageId")
            .and_then(|value| value.as_str())
            .cloned();
        self.lifecycle_times.lock().unwrap().push(finished_at_ms);
        self.lifecycle_final_messages
            .lock()
            .unwrap()
            .push(final_message_id);
        self.lifecycle
            .lock()
            .unwrap()
            .push((opts.turn_id, status, error));
    }

    async fn append_rollback(&self, _record: RollbackRecord) {}

    async fn append_checkpoint(&self, _record: CheckpointRecord) {}

    async fn append_usage(&self, _record: UsageLedgerItem) {}

    async fn append_subagent_lifecycle(&self, _record: SubagentLifecycleRecord) {}

    async fn append_background_process_lifecycle(
        &self,
        _record: crate::schemas::background_process::BackgroundProcessRecord,
    ) {
    }

    async fn flush(&self) -> std::io::Result<()> {
        Ok(())
    }

    async fn shutdown(&self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn a_completed_turn_persists_its_final_assistant_message_identity() {
    let rollout = RecordingTurnRollout::new();
    let rollout_sink: RolloutSink = rollout.clone();
    let session = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        mk_deps(vec![text_turn("complete reply")]),
        default_config(),
        Vec::new(),
        None,
        Some(rollout_sink),
        None,
        None,
        None,
    );

    let result = session
        .run(Input::Text("hi".into()), TurnOptions::default())
        .await;
    assert_eq!(result.stop_reason, TerminalReason::Completed);
    let turn_id = rollout.turn_contexts.lock().unwrap()[0]
        .turn_id
        .clone()
        .expect("turn id");
    let final_message_id = rollout.lifecycle_final_messages.lock().unwrap()[0]
        .clone()
        .expect("completed lifecycle final message id");
    assert_eq!(
        session
            .turn_index_snapshot()
            .turn_final_message
            .get(&turn_id),
        Some(&final_message_id),
    );
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn tool_preface_narration_is_not_recorded_as_a_final_reply() {
    let rollout = RecordingTurnRollout::new();
    let rollout_sink: RolloutSink = rollout.clone();
    let mut tool_only_turn = vec![ev::message_start(None)];
    tool_only_turn.push(crate::types::provider::ModelStreamEvent::TextDelta {
        text: "process".into(),
    });
    tool_only_turn.extend(exact_tool_use_events("call-1", "read", &exact_json!({})));
    tool_only_turn.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let session = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        mk_deps(vec![tool_only_turn]),
        default_config(),
        Vec::new(),
        None,
        Some(rollout_sink),
        None,
        None,
        None,
    );

    let _ = session
        .run(Input::Text("hi".into()), TurnOptions::default())
        .await;
    let turn_id = rollout.turn_contexts.lock().unwrap()[0]
        .turn_id
        .clone()
        .expect("turn id");
    // The harness terminator now closes with text, so the turn DOES get a final
    // reply; what must not happen is the tool-preface narration being the one.
    // Compare identities: the recorded final must not be the preface message.
    let preface_uuid = rollout
        .messages
        .lock()
        .unwrap()
        .iter()
        .find_map(|message| match message {
            Message::Assistant(assistant) => assistant
                .content
                .iter()
                .any(|block| matches!(block, ContentBlock::Text(text) if text.text == "process"))
                .then(|| assistant.uuid.clone()),
            _ => None,
        })
        .expect("the scripted preface message is recorded");
    let recorded_final = session
        .turn_index_snapshot()
        .turn_final_message
        .get(&turn_id)
        .cloned();
    assert!(
        recorded_final.is_some(),
        "the closing response is the turn's final reply",
    );
    assert_ne!(
        recorded_final.as_ref(),
        Some(&preface_uuid),
        "tool-preface narration must remain intermediate",
    );
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn a_provider_failed_turn_persists_turn_lifecycle_failed_with_the_error() {
    let rollout = RecordingTurnRollout::new();
    let rollout_sink: RolloutSink = rollout.clone();
    let session = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        mk_deps(vec![provider_error_turn("404 url.not_found")]),
        default_config(),
        Vec::new(),
        None,
        Some(rollout_sink),
        None,
        None,
        None,
    );

    let result = session
        .run(Input::Text("hi".into()), TurnOptions::default())
        .await;

    assert_eq!(result.stop_reason, TerminalReason::Error);
    let turn_id = {
        let contexts = rollout.turn_contexts.lock().unwrap();
        assert_eq!(contexts.len(), 1, "one turn_context boundary");
        assert!(
            contexts[0].started_at_ms.is_some(),
            "turn_context persists the sent time"
        );
        contexts[0].turn_id.clone().expect("turn id")
    };
    // The failed turn must persist its terminal state AND reason: without the
    // record the rollout reader classes the turn "interrupted" on resume and a
    // provider error (e.g. a 404 from a misconfigured BYO endpoint) vanishes
    // from history.
    assert_eq!(
        rollout.lifecycle.lock().unwrap().as_slice(),
        &[(
            Some(turn_id.clone()),
            RuntimeString::from("failed"),
            Some(RuntimeString::from("404 url.not_found")),
        )],
    );
    let index = session.turn_index_snapshot();
    let started_at_ms = rollout.turn_contexts.lock().unwrap()[0].started_at_ms;
    let finished_at_ms = rollout.lifecycle_times.lock().unwrap()[0];
    assert!(
        finished_at_ms.is_some(),
        "turn_lifecycle persists the completion time"
    );
    assert!(finished_at_ms >= started_at_ms);
    assert_eq!(
        index.turn_status.get(&turn_id),
        Some(&RuntimeString::from("failed"))
    );
    assert_eq!(
        index.turn_errors.get(&turn_id),
        Some(&RuntimeString::from("404 url.not_found")),
    );
    assert!(
        index.turn_duration_ms.contains_key(&turn_id),
        "every terminal Turn records a duration for the history renderer"
    );
    assert_eq!(
        index.turn_started_at_ms.get(&turn_id).copied(),
        started_at_ms
    );
    assert_eq!(
        index.turn_finished_at_ms.get(&turn_id).copied(),
        finished_at_ms
    );
    session.shutdown_and_wait().await.unwrap();
}

/// A tool that reports two edits to the same file plus one to another, the way
/// the real edit tools do.
struct EditingTool {
    schema: JsonSchema,
}

#[async_trait::async_trait]
impl Tool for EditingTool {
    fn static_name(&self) -> &str {
        "edit"
    }

    fn static_description(&self) -> &str {
        "test file edits"
    }

    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn is_concurrency_safe(&self, _input: &Json) -> bool {
        true
    }

    fn is_read_only(&self, _input: &Json) -> bool {
        // The fixture reports read-only so the turn needs no approval handler:
        // what is under test is how the session folds the reported edits, not
        // whether an edit is permitted.
        true
    }

    async fn call(
        &self,
        _input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, crate::types::errors::ToolError> {
        let change = |path: &str, kind, added, removed| crate::types::events::FileChange {
            turn_diff: Some(format!("turn-diff:{path}:{added}").into()),
            path: path.into(),
            kind,
            diff: None,
            added: Some(added),
            removed: Some(removed),
        };
        (ctx.emit)(ToolDomainEvent::FileChange {
            change: change(
                "src/new.rs",
                crate::types::events::FileChangeKind::Create,
                10,
                0,
            ),
        })
        .await;
        (ctx.emit)(ToolDomainEvent::FileChange {
            change: change(
                "src/new.rs",
                crate::types::events::FileChangeKind::Modify,
                2,
                1,
            ),
        })
        .await;
        // A sub-agent's edit arrives wrapped in the spawning tool's progress
        // stream, not as a top-level file_change.
        (ctx.emit)(ToolDomainEvent::AgentProgress {
            agent_id: Json::String("A1".into()),
            event: Box::new(EngineEvent::ToolEvent {
                turn_id: "child-turn".into(),
                tool_use_id: "child-call".into(),
                event: ToolDomainEvent::FileChange {
                    change: change(
                        "docs/notes.md",
                        crate::types::events::FileChangeKind::Modify,
                        4,
                        3,
                    ),
                },
            }),
        })
        .await;
        Ok(text_result(Json::Null, "edited", false))
    }
}

#[tokio::test]
async fn a_turns_edits_record_per_file_within_that_turn_replacing_not_summing() {
    let rollout = RecordingTurnRollout::new();
    let rollout_sink: RolloutSink = rollout.clone();
    let mut editing_turn = vec![ev::message_start(None)];
    editing_turn.extend(exact_tool_use_events(
        "edit-1",
        "edit",
        &exact_json!({"path": "src/new.rs"}),
    ));
    editing_turn.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let session = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        mk_deps(vec![editing_turn, text_turn("done")]),
        default_config(),
        vec![Arc::new(EditingTool {
            schema: JsonSchema::new(),
        })],
        None,
        Some(rollout_sink),
        None,
        None,
        None,
    );

    session
        .run(Input::Text("edit it".into()), TurnOptions::default())
        .await;

    // One group for the one turn that ran, and inside it one row per file in
    // first-touched order. The second report for src/new.rs REPLACES the first
    // (each report already measures the turn's whole effect on that file, so
    // summing 10 and 2 would count the same lines twice), while the file the turn
    // CREATED keeps that verdict even though the later report was a modify. The
    // sub-agent's edit lands in the same turn.
    let changes = session.file_changes_snapshot();
    assert_eq!(changes.turns.len(), 1, "one group per turn");
    assert!(
        changes.turns[0].turn_id.is_some(),
        "a live turn attributes its own changes"
    );
    assert_eq!(
        changes.turns[0]
            .files
            .iter()
            .map(|file| (file.path.clone(), file.kind, file.added, file.removed))
            .collect::<Vec<_>>(),
        vec![
            (
                RuntimeString::from("src/new.rs"),
                crate::types::events::FileChangeKind::Create,
                Some(2),
                Some(1)
            ),
            (
                RuntimeString::from("docs/notes.md"),
                crate::types::events::FileChangeKind::Modify,
                Some(4),
                Some(3)
            ),
        ],
    );
    assert_eq!(
        changes.turns[0].files[0].turn_diff,
        Some("turn-diff:src/new.rs:2".into())
    );
    assert_eq!(
        changes.turns[0].files[1].turn_diff,
        Some("turn-diff:docs/notes.md:4".into()),
        "forwarded child event is grouped under the parent turn"
    );
    // Persisted per edit (three, including the sub-agent's), so a resume replays
    // the same fold instead of reading a stale total. Read into locals so the
    // guard is gone before the shutdown await.
    let (persisted_count, last_path) = {
        let persisted = rollout.file_changes.lock().unwrap();
        (
            persisted.len(),
            persisted.last().map(|change| change.path.clone()),
        )
    };
    assert_eq!(persisted_count, 3);
    assert_eq!(last_path, Some(RuntimeString::from("docs/notes.md")));
    assert_eq!(
        rollout
            .file_changes
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .turn_diff,
        Some("turn-diff:docs/notes.md:4".into())
    );
    session.shutdown_and_wait().await.unwrap();
}

#[test]
fn a_delete_wins_over_earlier_edits_and_a_recreate_takes_it_back() {
    use crate::engine::session::SessionFileChanges;
    use crate::types::events::FileChangeKind;

    let mut changes = SessionFileChanges::default();
    let turn = RuntimeString::from("turn-1");
    let path = RuntimeString::from("a.txt");
    changes.record(Some(&turn), &path, FileChangeKind::Modify, Some(3), Some(1));
    changes.record(Some(&turn), &path, FileChangeKind::Delete, Some(0), Some(0));
    assert_eq!(changes.turns[0].files[0].kind, FileChangeKind::Delete);
    changes.record(Some(&turn), &path, FileChangeKind::Create, Some(5), Some(0));
    assert_eq!(changes.turns[0].files[0].kind, FileChangeKind::Create);
    assert_eq!(
        (
            changes.turns[0].files[0].added,
            changes.turns[0].files[0].removed
        ),
        (Some(5), Some(0)),
        "the newest measurement stands on its own"
    );
    assert_eq!(changes.turns[0].files.len(), 1, "one row per path per turn");
}

/// A file edited across two turns has two answers, not one running total, and an
/// unmeasurable count stays unknown instead of being folded to zero.
#[test]
fn each_turn_keeps_its_own_row_and_an_unknown_count_stays_unknown() {
    use crate::engine::session::SessionFileChanges;
    use crate::types::events::FileChangeKind;

    let mut changes = SessionFileChanges::default();
    let path = RuntimeString::from("a.txt");
    let first = RuntimeString::from("turn-1");
    let second = RuntimeString::from("turn-2");
    changes.record(
        Some(&first),
        &path,
        FileChangeKind::Modify,
        Some(3),
        Some(1),
    );
    changes.record(Some(&second), &path, FileChangeKind::Modify, None, None);
    assert_eq!(
        changes
            .turns
            .iter()
            .map(|turn| (
                turn.turn_id.clone(),
                turn.files[0].added,
                turn.files[0].removed
            ))
            .collect::<Vec<_>>(),
        vec![(Some(first), Some(3), Some(1)), (Some(second), None, None),],
    );
}

#[tokio::test]
async fn run_streamed_emits_thread_and_turn_lifecycle_events() {
    let session = mk_session(vec![text_turn("hi")]);
    let turn = session.run_streamed(Input::Text("go".into()), TurnOptions::default());
    let mut kinds: Vec<&'static str> = Vec::new();
    let mut events = turn.events;
    while let Some(ev) = events.next().await {
        kinds.push(match ev {
            EngineEvent::ThreadStarted { .. } => "thread.started",
            EngineEvent::TurnStarted { .. } => "turn.started",
            EngineEvent::TurnCompleted { .. } => "turn.completed",
            EngineEvent::ItemCompleted { .. } => "item.completed",
            _ => "other",
        });
    }
    assert!(kinds.contains(&"thread.started"));
    assert!(kinds.contains(&"turn.started"));
    assert!(kinds.contains(&"turn.completed"));
    // thread.started is only emitted on the FIRST turn.
    let turn2 = session.run_streamed(Input::Text("again".into()), TurnOptions::default());
    let mut events2 = turn2.events;
    let mut saw_thread_started = false;
    while let Some(ev) = events2.next().await {
        if matches!(ev, EngineEvent::ThreadStarted { .. }) {
            saw_thread_started = true;
        }
    }
    assert!(!saw_thread_started);
}

#[tokio::test]
async fn a_turn_started_with_no_input_continues_without_adding_a_user_message() {
    // The desktop client's resume-after-stop entry starts a turn with an empty
    // input list: the thread already holds the user's message and the partial
    // work, so the point is to let the model pick it back up. Recording an empty
    // user message for that turn would leave a contentless message in history and
    // the rollout, which every provider mapper then has to special-case.
    let session = mk_session(vec![text_turn("first"), text_turn("continued")]);
    let first = session
        .run(Input::Text("go".into()), TurnOptions::default())
        .await;
    assert_eq!(first.stop_reason, TerminalReason::Completed);
    let users_after_first = session
        .history()
        .iter()
        .filter(|message| matches!(message, Message::User(_)))
        .count();
    assert_eq!(users_after_first, 1);

    let resumed = session
        .run(Input::Parts(Vec::new()), TurnOptions::default())
        .await;
    assert_eq!(resumed.stop_reason, TerminalReason::Completed);
    let users_after_resume = session
        .history()
        .iter()
        .filter(|message| matches!(message, Message::User(_)))
        .count();
    assert_eq!(users_after_resume, 1);
}

#[tokio::test]
async fn tool_context_derives_sandbox_retry_from_approval_policy() {
    let seen = Arc::new(Mutex::new(None));
    let tool: AnyTool = Arc::new(CaptureRetryTool {
        schema: JsonSchema::new(),
        seen: seen.clone(),
    });
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events("t1", "capture", &exact_json!({})));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let mut config = default_config();
    config.approval_policy = Some(crate::types::permission::ApprovalPolicy::UnlessTrusted);
    let session = mk_session_with(vec![first], config, vec![tool]);

    let result = session
        .run(Input::Text("capture".into()), TurnOptions::default())
        .await;

    assert_eq!(result.stop_reason, TerminalReason::Completed);
    assert_eq!(*seen.lock().unwrap(), Some(Some(true)));
}

#[test]
fn turn_selected_capability_roots_override_session_default_root() {
    let plugin_runtime: Arc<dyn BrowserRuntime> = spawn_free(Arc::new(NoopBrowserRuntime));
    let turn_runtime: Arc<dyn BrowserRuntime> = spawn_free(Arc::new(NoopBrowserRuntime));
    let session = Session::new(SessionInit {
        durable_permissions: None,
        process_context: crate::runtime_process::test_runtime_process_context(),
        user_config_dir: None,
        id: Some("s1".into()),
        session_role: None,
        parent_thread_id: None,
        provider: Arc::new(fake_provider()),
        deps: mk_deps(Vec::new()),
        config: default_config(),
        network_allowed: None,
        trusted_windows_sandbox_context: None,
        context_window_override: None,
        pricing: None,
        tools: Vec::new(),
        refresh_tools: None,
        cwd: runtime_path_from_host_path(&std::env::temp_dir()),
        approval_handler: None,
        approval_reviewer: None,
        elicit_handler: None,
        mcp_elicitation_presenter: None,
        mcp_authorize_handler: None,
        mcp_oauth_redirect_uri: None,
        credential_store: None,
        hook_bus: None,
        rollout: None,
        store: None,
        mcp_clients: Vec::new(),
        plugin_mcp_servers: Vec::new(),
        slash_commands: Vec::new(),
        initial_history: None,
        initial_approval_context: None,
        initial_approval_reviews: None,
        initial_tool_result_budget_records: None,
        hidden_context_message_ids: None,
        initial_usage_ledger: None,
        initial_subagent_lifecycle: None,
        initial_background_processes: None,
        initial_memory_maintenance: None,
        initial_setup_lifecycle: None,
        initial_agent_memory_snapshot_decisions: None,
        forked_from_id: None,
        initial_checkpoints: None,
        system_prompt_provider: None,
        base_instructions: None,
        developer_instructions: None,
        root_agent_definition: None,
        observer: Some(None),
        logger: None,
        agents: None,
        skills: None,
        output_styles: None,
        skill_gate: None,
        current_worktree: None,
        prior_workflow_journal: None,
        initial_surfaced_tools: None,
        swarm: None,
        lsp_servers: None,
        shell_env: None,
        agent_memory: None,
        agent_memory_snapshot_policy: None,
        browser_runtime: None,
        runtime_capabilities: crate::capabilities::RuntimeCapabilityRegistry::from_roots(vec![
            crate::capabilities::RuntimeCapabilityRoot::plugin("docs", Some("1"), "/plugins/docs")
                .with_browser_runtime(plugin_runtime.clone()),
            crate::capabilities::RuntimeCapabilityRoot::host("turn.browser")
                .with_browser_runtime(turn_runtime.clone()),
        ]),
        selected_capability_roots: Some(vec![
            crate::capabilities::SelectedRuntimeCapabilityRoot::plugin("docs"),
        ]),
        extension_runtime: ExtensionRuntimeSnapshot::default(),
    });

    let base = session.runtime_capability_snapshot_for_turn(&TurnOptions::default());
    assert!(Arc::ptr_eq(
        base.capabilities.browser_runtime.as_ref().unwrap(),
        &plugin_runtime
    ));

    let turn = session.runtime_capability_snapshot_for_turn(&TurnOptions {
        selected_capability_roots: Some(vec![
            crate::capabilities::SelectedRuntimeCapabilityRoot::host("turn.browser"),
        ]),
        ..TurnOptions::default()
    });
    assert!(Arc::ptr_eq(
        turn.capabilities.browser_runtime.as_ref().unwrap(),
        &turn_runtime
    ));
}

#[tokio::test]
async fn run_persists_tool_lifecycle_events_to_rollout() {
    let dir = temp_store_dir();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(
            dir.to_str()
                .expect("test store directory is a scalar host path")
                .to_owned(),
        ),
        archive_dir: None,
    }));
    let rollout = Arc::from(
        store
            .create(SessionMeta {
                id: "s1".into(),
                created_at: 1,
                cwd: runtime_path_from_host_path(&dir).into_runtime_string(),
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
            })
            .await
            .unwrap(),
    );
    let tool: AnyTool = Arc::new(CaptureRetryTool {
        schema: JsonSchema::new(),
        seen: Arc::new(Mutex::new(None)),
    });
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "t1",
        "capture",
        &exact_json!({"x": 1}),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let mut config = default_config();
    config.approval_policy = Some(crate::types::permission::ApprovalPolicy::UnlessTrusted);
    let session = Session::new(SessionInit {
        durable_permissions: None,
        process_context: crate::runtime_process::test_runtime_process_context(),
        user_config_dir: None,
        id: Some("s1".into()),
        session_role: None,
        parent_thread_id: None,
        provider: Arc::new(fake_provider()),
        deps: mk_deps(vec![first]),
        config,
        network_allowed: None,
        trusted_windows_sandbox_context: None,
        context_window_override: None,
        pricing: None,
        tools: vec![tool],
        refresh_tools: None,
        cwd: runtime_path_from_host_path(&dir),
        approval_handler: None,
        approval_reviewer: None,
        elicit_handler: None,
        mcp_elicitation_presenter: None,
        mcp_authorize_handler: None,
        mcp_oauth_redirect_uri: None,
        credential_store: None,
        hook_bus: None,
        rollout: Some(rollout),
        store: Some(store.clone()),
        mcp_clients: Vec::new(),
        plugin_mcp_servers: Vec::new(),
        slash_commands: Vec::new(),
        initial_history: None,
        initial_approval_context: None,
        initial_approval_reviews: None,
        initial_tool_result_budget_records: None,
        hidden_context_message_ids: None,
        initial_usage_ledger: None,
        initial_subagent_lifecycle: None,
        initial_background_processes: None,
        initial_memory_maintenance: None,
        initial_setup_lifecycle: None,
        initial_agent_memory_snapshot_decisions: None,
        forked_from_id: None,
        initial_checkpoints: None,
        system_prompt_provider: None,
        base_instructions: None,
        developer_instructions: None,
        root_agent_definition: None,
        observer: Some(None),
        logger: None,
        agents: None,
        skills: None,
        output_styles: None,
        skill_gate: None,
        current_worktree: None,
        prior_workflow_journal: None,
        initial_surfaced_tools: None,
        swarm: None,
        lsp_servers: None,
        shell_env: None,
        agent_memory: None,
        agent_memory_snapshot_policy: None,
        browser_runtime: None,
        runtime_capabilities: RuntimeCapabilityRegistry::new(),
        selected_capability_roots: None,
        extension_runtime: ExtensionRuntimeSnapshot::default(),
    });

    let result = session
        .run(Input::Text("capture".into()), TurnOptions::default())
        .await;
    assert_eq!(result.stop_reason, TerminalReason::Completed);
    session.dispose().await;

    let resumed = store.resume("s1").await.unwrap();
    let phases = resumed
        .raw_items
        .iter()
        .filter_map(|item| match item {
            RolloutItem::Event(event) if event.event == "tool_lifecycle" => event
                .payload
                .as_ref()
                .and_then(|payload| payload.get("phase"))
                .and_then(Json::as_str),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(phases, vec!["requested", "started", "completed"]);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn turn_active_is_cleared_after_a_turn_so_rollback_is_allowed() {
    let session = mk_session(vec![text_turn("a"), text_turn("b")]);
    let _ = session
        .run(Input::Text("first".into()), TurnOptions::default())
        .await;
    let _ = session
        .run(Input::Text("second".into()), TurnOptions::default())
        .await;
    // Two user turns recorded; rollback of one leaves one.
    let out = session
        .rollback(1)
        .await
        .expect("rollback allowed between turns");
    assert_eq!(out.remaining_turns, 1);
    assert_eq!(out.dropped_turns, 1);
}

#[tokio::test]
async fn an_unconsumed_turn_is_active_until_its_stream_is_dropped() {
    let session = mk_session(vec![text_turn("x")]);
    let turn = session.run_streamed(Input::Text("go".into()), TurnOptions::default());
    assert!(session.rollback(1).await.is_err());
    drop(turn);
    assert!(session.rollback(1).await.is_ok());
}

#[tokio::test]
async fn a_fully_consumed_stream_releases_its_turn_before_the_stream_is_dropped() {
    let session = mk_session(vec![text_turn("x")]);
    let mut events = session
        .run_streamed(Input::Text("go".into()), TurnOptions::default())
        .events;
    while events.next().await.is_some() {}
    assert!(session.rollback(1).await.is_ok());
    drop(events);
}

#[tokio::test]
async fn dropping_an_older_overlapping_turn_keeps_the_newer_turn_active() {
    let session = mk_session(vec![text_turn("older"), text_turn("newer")]);
    let older = session.run_streamed(
        Input::Text("older".into()),
        TurnOptions {
            model: Some("older-model".into()),
            ..Default::default()
        },
    );
    let newer = session.run_streamed(
        Input::Text("newer".into()),
        TurnOptions {
            model: Some("newer-model".into()),
            ..Default::default()
        },
    );

    assert_eq!(session.steering_model(), "newer-model");
    drop(older);
    assert_eq!(session.steering_model(), "newer-model");
    assert!(session.rollback(1).await.is_err());
    drop(newer);
    assert!(session.rollback(1).await.is_ok());
}

fn fork_user(id: &str) -> Message {
    Message::User(UserMessage {
        question_answer: None,
        uuid: id.into(),
        content: vec![ContentBlock::Text(TextBlock {
            text: id.into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })],
        is_meta: None,
    })
}

fn fork_assistant(id: &str) -> Message {
    Message::Assistant(AssistantMessage {
        uuid: id.into(),
        content: vec![ContentBlock::Text(TextBlock {
            text: id.into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })],
        model: "parent-model".into(),
        usage: None,
    })
}

fn fork_tool_result(id: &str) -> Message {
    Message::ToolResult(crate::schemas::message::ToolResultMessage {
        uuid: id.into(),
        tool_use_id: format!("call-{id}").into(),
        content: vec![crate::schemas::content::OutputBlock::Text(TextBlock {
            text: id.into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })],
        is_error: false,
        tool_kind: None,
        native_tool_output: None,
        output: None,
        extra_user_blocks: None,
    })
}

fn three_turn_fork_history() -> Vec<Message> {
    let assistant = |id: &str, result_id: &str| {
        let mut message = fork_assistant(id);
        if let Message::Assistant(message) = &mut message {
            message.content.push(ContentBlock::ToolUse(
                crate::schemas::content::ToolUseBlock {
                    id: format!("call-{result_id}").into(),
                    name: "write".into(),
                    input: crate::runtime_values::RuntimeJson::Object(
                        crate::runtime_values::RuntimeObject::new(),
                    ),
                    tool_kind: None,
                    thought_signature: None,
                },
            ));
        }
        message
    };
    vec![
        fork_user("u1"),
        assistant("a1", "t1"),
        fork_tool_result("t1"),
        fork_user("u2"),
        assistant("a2", "t2"),
        fork_tool_result("t2"),
        fork_user("u3"),
        assistant("a3", "t3"),
        fork_tool_result("t3"),
    ]
}

fn fork_history_ids(messages: &[Message]) -> Vec<RuntimeString> {
    messages.iter().map(message_uuid).cloned().collect()
}

fn fork_snapshot_call_cases() -> Vec<(&'static str, bool, SpawnSubAgentOptions, usize)> {
    vec![
        (
            "legacy flag does not disable default inheritance",
            false,
            SpawnSubAgentOptions::default(),
            1,
        ),
        (
            "resume",
            true,
            SpawnSubAgentOptions {
                prompt: "Resume the snapshot precedence probe.".into(),
                resume: Some("child".into()),
                ..Default::default()
            },
            1,
        ),
        (
            "agent type",
            true,
            SpawnSubAgentOptions {
                agent_type: Some("reviewer".into()),
                ..Default::default()
            },
            1,
        ),
        (
            "trusted non-string resume",
            true,
            SpawnSubAgentOptions {
                task_resume_raw: Some(exact_json!(true)),
                ..Default::default()
            },
            1,
        ),
        (
            "trusted non-string agent type",
            true,
            SpawnSubAgentOptions {
                task_agent_type_raw: Some(exact_json!({})),
                ..Default::default()
            },
            1,
        ),
        (
            "memory directory",
            true,
            SpawnSubAgentOptions {
                memory_dir: Some("/tmp/fork-memory".into()),
                ..Default::default()
            },
            0,
        ),
        (
            "explicit none",
            true,
            SpawnSubAgentOptions {
                fork_context: Some(ForkContext::None),
                ..Default::default()
            },
            0,
        ),
        ("fresh omitted", true, SpawnSubAgentOptions::default(), 1),
        (
            "fresh all",
            true,
            SpawnSubAgentOptions {
                fork_context: Some(ForkContext::All),
                ..Default::default()
            },
            1,
        ),
        (
            "fresh recent",
            true,
            SpawnSubAgentOptions {
                fork_context: Some(ForkContext::Recent(NonZeroUsize::new(1).unwrap())),
                ..Default::default()
            },
            1,
        ),
        (
            "fresh positive fraction below one",
            true,
            SpawnSubAgentOptions {
                fork_context: Some(ForkContext::PositiveNumber(
                    PositiveForkNumber::new(0.5).unwrap(),
                )),
                ..Default::default()
            },
            1,
        ),
        (
            "model does not block recent",
            true,
            SpawnSubAgentOptions {
                model: Some("child-model".into()),
                fork_context: Some(ForkContext::Recent(NonZeroUsize::new(1).unwrap())),
                ..Default::default()
            },
            1,
        ),
        (
            "persistent fresh",
            true,
            SpawnSubAgentOptions {
                persistent: Some(true),
                ..Default::default()
            },
            1,
        ),
    ]
}

fn fork_precedence_session(turns: ScriptedTurns, config: ResolvedConfig) -> Arc<Session> {
    mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        mk_deps(turns),
        config,
        Vec::new(),
        None,
        None,
        None,
        None,
        Some(vec![AgentDefinition {
            name: "reviewer".into(),
            ..Default::default()
        }]),
    )
}

fn counted_fork_conversation(calls: Arc<AtomicUsize>) -> GetConversation {
    Arc::new(move || {
        calls.fetch_add(1, Ordering::SeqCst);
        three_turn_fork_history()
    })
}

#[tokio::test]
async fn foreground_spawn_defers_conversation_snapshot_until_after_precedence() {
    for (label, enabled, options, expected_calls) in fork_snapshot_call_cases() {
        let mut config = default_config();
        config.fork = enabled;
        config.agent_memory = false;
        config.memory_recall = false;
        let session = fork_precedence_session(Vec::new(), config);
        let calls = Arc::new(AtomicUsize::new(0));
        let spawn = session.make_spawn_sub_agent(
            0,
            CancellationToken::new(),
            session.permission_context(),
            counted_fork_conversation(calls.clone()),
            false,
            None,
            None,
        );

        let resuming = forwarded_resume(&options).is_present();
        let invalid_role = options.task_agent_type_raw.is_some() && options.agent_type.is_none();
        let result = spawn(options).await;
        if resuming {
            let error = match result {
                Err(error) => error,
                Ok(_) => panic!("an unknown resume identity must be refused"),
            };
            assert!(
                error.contains("resume target") || error.contains("history is unavailable"),
                "{label}: {error:?}"
            );
        } else if invalid_role {
            assert!(
                result.is_err(),
                "{label}: non-string roles must remain rejected"
            );
        } else {
            drop(
                result
                    .expect("fresh child creation must be accepted")
                    .events,
            );
        }

        assert_eq!(calls.load(Ordering::SeqCst), expected_calls, "{label}");
        session.shutdown_and_wait().await.unwrap();
    }
}

#[tokio::test]
async fn background_spawn_defers_conversation_snapshot_until_after_precedence() {
    for (label, enabled, options, expected_calls) in fork_snapshot_call_cases() {
        let mut config = default_config();
        config.fork = enabled;
        config.agent_memory = false;
        config.memory_recall = false;
        let session = fork_precedence_session(
            vec![provider_error_turn("stop background snapshot probe")],
            config,
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let spawn = session.make_spawn_background_agent(
            0,
            None,
            session.permission_context(),
            counted_fork_conversation(calls.clone()),
            false,
            None,
            None,
        );

        let resuming = forwarded_resume(&options).is_present();
        let invalid_role = options.task_agent_type_raw.is_some() && options.agent_type.is_none();
        let result = spawn(options).await;
        let spawned = if resuming {
            let error = match result {
                Err(error) => error,
                Ok(_) => panic!("an unknown resume identity must be refused"),
            };
            assert!(
                error.contains("resume target") || error.contains("history is unavailable"),
                "{label}: {error:?}"
            );
            None
        } else if invalid_role {
            assert!(
                result.is_err(),
                "{label}: non-string roles must remain rejected"
            );
            None
        } else {
            Some(result.expect("fresh child creation must be accepted"))
        };

        assert_eq!(calls.load(Ordering::SeqCst), expected_calls, "{label}");
        session.shutdown_and_wait().await.unwrap();
        if let Some(spawned) = spawned {
            std::fs::remove_file(runtime_path_to_file_io_host(&spawned.output_file)).ok();
        }
    }
}

#[test]
fn fork_snapshot_obeys_memory_and_explicit_context_while_legacy_flags_do_not_block_capture() {
    use crate::types::tool::{ForkContext, PositiveForkNumber};

    let history = three_turn_fork_history();
    let recent_two = ForkContext::Recent(NonZeroUsize::new(2).unwrap());
    let fractional_half = ForkContext::PositiveNumber(PositiveForkNumber::new(0.5).unwrap());
    let fractional_one = ForkContext::PositiveNumber(PositiveForkNumber::new(1.5).unwrap());

    for (label, fork_context, expected) in [
        ("omitted", None, Some(history.clone())),
        ("all", Some(ForkContext::All), Some(history.clone())),
        ("recent", Some(recent_two), Some(history[3..].to_vec())),
        (
            "fraction below one",
            Some(fractional_half),
            Some(Vec::new()),
        ),
        (
            "fraction truncates",
            Some(fractional_one),
            Some(history[6..].to_vec()),
        ),
        ("none", Some(ForkContext::None), None),
    ] {
        let options = SpawnSubAgentOptions {
            fork_context,
            ..Default::default()
        };
        assert_eq!(
            fork_snapshot(true, &options, || history.clone()),
            expected,
            "fresh {label}"
        );
    }

    for fork_context in [
        None,
        Some(ForkContext::All),
        Some(ForkContext::None),
        Some(recent_two),
        Some(fractional_half),
    ] {
        for (label, enabled, memory_dir) in [
            ("feature disabled", false, None),
            ("memory", true, Some("/tmp/memory")),
        ] {
            let options = SpawnSubAgentOptions {
                memory_dir: memory_dir.map(RuntimePath::from),
                fork_context,
                ..Default::default()
            };
            let expected = if memory_dir.is_some() {
                None
            } else {
                fork_snapshot(true, &options, || history.clone())
            };
            assert_eq!(
                fork_snapshot(enabled, &options, || history.clone()),
                expected,
                "{label} with {fork_context:?}"
            );
        }

        let fresh = SpawnSubAgentOptions {
            fork_context,
            ..Default::default()
        };
        let expected = fork_snapshot(true, &fresh, || history.clone());
        for (label, options) in [
            (
                "resume",
                SpawnSubAgentOptions {
                    resume: Some("child".into()),
                    fork_context,
                    ..Default::default()
                },
            ),
            (
                "agent type",
                SpawnSubAgentOptions {
                    agent_type: Some("reviewer".into()),
                    fork_context,
                    ..Default::default()
                },
            ),
        ] {
            assert_eq!(
                fork_snapshot(true, &options, || history.clone()),
                expected,
                "{label} with {fork_context:?}"
            );
        }
    }

    let options = SpawnSubAgentOptions {
        model: Some("child-model".into()),
        fork_context: Some(recent_two),
        ..Default::default()
    };
    assert_eq!(
        fork_history_ids(&fork_snapshot(true, &options, || history.clone()).unwrap()),
        vec!["u2", "a2", "t2", "u3", "a3", "t3"]
    );

    let mut prefixed = vec![fork_assistant("startup-prefix")];
    prefixed.extend(history.clone());
    let huge = SpawnSubAgentOptions {
        fork_context: Some(ForkContext::PositiveNumber(
            PositiveForkNumber::new(1e20).unwrap(),
        )),
        ..Default::default()
    };
    assert_eq!(
        fork_snapshot(true, &huge, || prefixed).unwrap(),
        history,
        "a huge recent count still drops the pre-user startup prefix"
    );
}

async fn persisted_fork_session(
    history: Vec<Message>,
) -> (
    Arc<Session>,
    Arc<JsonlSessionStore>,
    Arc<Mutex<Vec<ModelRequest>>>,
    PathBuf,
) {
    let dir = temp_store_dir();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(
            dir.to_str()
                .expect("test store directory is a scalar host path")
                .to_owned(),
        ),
        archive_dir: None,
    }));
    let rollout: RolloutSink = Arc::from(
        store
            .create(SessionMeta {
                id: "s1".into(),
                created_at: 1,
                cwd: runtime_path_from_host_path(&dir).into_runtime_string(),
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
            })
            .await
            .unwrap(),
    );
    let requests = Arc::new(Mutex::new(Vec::new()));
    let mut config = default_config();
    config.fork = true;
    config.agent_memory = false;
    config.memory_recall = false;
    config.memory.enabled = false;
    let session = mk_session_with_provider_memory_and_store(
        Arc::new(fake_provider()),
        unique_subagent_test_deps(recording_deps(
            requests.clone(),
            vec![provider_error_turn("stop after fork persistence capture")],
        )),
        config,
        vec![create_write_tool()],
        None,
        Some(rollout),
        Some(store.clone()),
        Some(history),
    );
    (session, store, requests, dir)
}

fn fork_persistence_labels(messages: &[Message]) -> Vec<RuntimeString> {
    messages
        .iter()
        .map(|message| {
            let is_child_directive = match message {
                Message::User(user) => user.content.iter().any(|block| {
                    matches!(
                        block,
                        ContentBlock::Text(text) if text.text.contains("Answer CHILD_RECENT.")
                    )
                }),
                _ => false,
            };
            if is_child_directive {
                "child-directive".into()
            } else {
                message_uuid(message).clone()
            }
        })
        .collect()
}

fn assert_partial_fork_capture_and_sidechain(
    requests: &Arc<Mutex<Vec<ModelRequest>>>,
    sidechain: &[Message],
) {
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(is_in_fork_child(&requests[0].messages));
    assert_eq!(requests[0].cache_key, Some("s1".into()));
    assert_eq!(requests[0].skip_cache_write, Some(true));
    assert!(requests[0].tools.iter().any(|tool| tool.name == "write"));
    assert_eq!(
        fork_persistence_labels(&requests[0].messages),
        vec!["u2", "a2", "t2", "u3", "a3", "t3", "child-directive"]
    );
    assert_eq!(fork_persistence_labels(sidechain), vec!["child-directive"]);
    for parent_id in ["u2", "a2", "t2", "u3", "a3", "t3"] {
        assert!(
            !sidechain
                .iter()
                .any(|message| message_uuid(message) == parent_id),
            "parent message {parent_id} leaked into the child sidechain"
        );
    }
}

fn assert_empty_fork_capture_and_sidechain(
    requests: &Arc<Mutex<Vec<ModelRequest>>>,
    sidechain: &[Message],
) {
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(is_in_fork_child(&requests[0].messages));
    assert_eq!(requests[0].cache_key, Some("s1".into()));
    assert_eq!(requests[0].skip_cache_write, Some(true));
    assert!(requests[0].tools.iter().any(|tool| tool.name == "write"));
    assert_eq!(
        fork_persistence_labels(&requests[0].messages),
        vec!["child-directive"]
    );
    assert_eq!(fork_persistence_labels(sidechain), vec!["child-directive"]);
}

#[tokio::test]
async fn partial_fork_persists_only_child_directive() {
    let history = three_turn_fork_history();
    let (session, store, requests, dir) = persisted_fork_session(history.clone()).await;
    let snapshot = Arc::new(move || history.clone()) as GetConversation;
    let spawn = session.make_spawn_sub_agent(
        0,
        CancellationToken::new(),
        session.permission_context(),
        snapshot,
        false,
        None,
        None,
    );
    let run = spawn(SpawnSubAgentOptions {
        prompt: "Answer CHILD_RECENT.".into(),
        fork_context: Some(ForkContext::Recent(NonZeroUsize::new(2).unwrap())),
        ..Default::default()
    })
    .await
    .expect("child creation must be accepted");
    let agent_id = run
        .agent_id
        .as_str()
        .map(runtime_forwarded_slot_to_typed_host)
        .expect("normal foreground id is a scalar store key");
    let mut events = run.events;
    while events.next().await.is_some() {}

    let sidechain = store
        .load_sub_agent("s1", &agent_id)
        .await
        .unwrap()
        .messages;
    assert_partial_fork_capture_and_sidechain(&requests, &sidechain);

    session.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn partial_fork_background_persists_only_child_directive() {
    let history = three_turn_fork_history();
    let (session, store, requests, dir) = persisted_fork_session(history.clone()).await;
    let snapshot = Arc::new(move || history.clone()) as GetConversation;
    let spawn = session.make_spawn_background_agent(
        0,
        None,
        session.permission_context(),
        snapshot,
        false,
        None,
        None,
    );
    let spawned = spawn(SpawnSubAgentOptions {
        prompt: "Answer CHILD_RECENT.".into(),
        fork_context: Some(ForkContext::Recent(NonZeroUsize::new(2).unwrap())),
        ..Default::default()
    })
    .await
    .expect("child creation must be accepted");
    assert!(
        spawned.agent_id.starts_with("subagent-test-"),
        "background outward identity must remain the fresh UUID"
    );
    let pending = std::mem::take(&mut session.background_tasks.lock().unwrap().handles);
    for handle in pending {
        handle.await.unwrap();
    }

    let sidechain = store
        .load_sub_agent(
            "s1",
            &runtime_forwarded_slot_to_typed_host(&spawned.agent_id),
        )
        .await
        .unwrap()
        .messages;
    assert_partial_fork_capture_and_sidechain(&requests, &sidechain);

    std::fs::remove_file(runtime_path_to_file_io_host(&spawned.output_file)).ok();
    session.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn fractional_empty_foreground_still_uses_the_fork_child_path() {
    let history = three_turn_fork_history();
    let (session, store, requests, dir) = persisted_fork_session(history.clone()).await;
    let snapshot = Arc::new(move || history.clone()) as GetConversation;
    let spawn = session.make_spawn_sub_agent(
        0,
        CancellationToken::new(),
        session.permission_context(),
        snapshot,
        false,
        None,
        None,
    );
    let run = spawn(SpawnSubAgentOptions {
        prompt: "Answer CHILD_RECENT.".into(),
        fork_context: Some(ForkContext::PositiveNumber(
            PositiveForkNumber::new(0.5).unwrap(),
        )),
        ..Default::default()
    })
    .await
    .expect("child creation must be accepted");
    let agent_id = run
        .agent_id
        .as_str()
        .map(runtime_forwarded_slot_to_typed_host)
        .expect("normal foreground id is a scalar store key");
    let mut events = run.events;
    while events.next().await.is_some() {}

    let sidechain = store
        .load_sub_agent("s1", &agent_id)
        .await
        .unwrap()
        .messages;
    assert_empty_fork_capture_and_sidechain(&requests, &sidechain);

    session.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn fractional_empty_background_still_uses_the_fork_child_path() {
    let history = three_turn_fork_history();
    let (session, store, requests, dir) = persisted_fork_session(history.clone()).await;
    let snapshot = Arc::new(move || history.clone()) as GetConversation;
    let spawn = session.make_spawn_background_agent(
        0,
        None,
        session.permission_context(),
        snapshot,
        false,
        None,
        None,
    );
    let spawned = spawn(SpawnSubAgentOptions {
        prompt: "Answer CHILD_RECENT.".into(),
        fork_context: Some(ForkContext::PositiveNumber(
            PositiveForkNumber::new(0.5).unwrap(),
        )),
        ..Default::default()
    })
    .await
    .expect("child creation must be accepted");
    let pending = std::mem::take(&mut session.background_tasks.lock().unwrap().handles);
    for handle in pending {
        handle.await.unwrap();
    }

    let sidechain = store
        .load_sub_agent(
            "s1",
            &runtime_forwarded_slot_to_typed_host(&spawned.agent_id),
        )
        .await
        .unwrap()
        .messages;
    assert_empty_fork_capture_and_sidechain(&requests, &sidechain);

    std::fs::remove_file(runtime_path_to_file_io_host(&spawned.output_file)).ok();
    session.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(dir).ok();
}

fn session_with_known_agent(turns: ScriptedTurns) -> Arc<Session> {
    let mut config = default_config();
    config.fork = true;
    config.agent_memory = false;
    config.memory_recall = false;
    mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        unique_subagent_test_deps(mk_deps(turns)),
        config,
        Vec::new(),
        None,
        None,
        None,
        None,
        Some(vec![
            AgentDefinition {
                name: "known".into(),
                ..Default::default()
            },
            AgentDefinition {
                // A raw object stringifies to this text for the error, but
                // JS Map lookup still cannot match an object to this key.
                name: "[object Object]".into(),
                ..Default::default()
            },
        ]),
    )
}

async fn foreground_failure(mut events: EngineEventStream) -> Option<RuntimeString> {
    let mut failure = None;
    while let Some(event) = events.next().await {
        if let EngineEvent::TurnFailed { error, .. } = event {
            failure = Some(error.message);
        }
    }
    failure
}

#[tokio::test]
async fn trusted_non_string_agent_type_foreground_reports_unknown_instead_of_forking() {
    let history = three_turn_fork_history();
    let snapshot = Arc::new(move || history.clone()) as GetConversation;
    let session = session_with_known_agent(vec![text_turn("fork path must not run")]);
    let spawn = session.make_spawn_sub_agent(
        0,
        CancellationToken::new(),
        session.permission_context(),
        snapshot,
        false,
        None,
        None,
    );

    let error = spawn(SpawnSubAgentOptions {
        prompt: "trusted rewrite".into(),
        task_agent_type_raw: Some(exact_json!({ "bad": 1 })),
        ..Default::default()
    })
    .await
    .map(|_| ())
    .expect_err("invalid creation must be refused");
    assert_eq!(
        error,
        RuntimeString::from(
            "unknown agent type \"[object Object]\" (available: known, [object Object])"
        )
    );
    assert!(session.background_task_registry().list().is_empty());
    assert!(session.subagent_lifecycle_snapshot().agents.is_empty());
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn trusted_non_string_agent_type_background_reports_unknown_instead_of_forking() {
    let history = three_turn_fork_history();
    let snapshot = Arc::new(move || history.clone()) as GetConversation;
    let session = session_with_known_agent(vec![text_turn("fork path must not run")]);
    let spawn = session.make_spawn_background_agent(
        0,
        None,
        session.permission_context(),
        snapshot,
        false,
        None,
        None,
    );

    let error = spawn(SpawnSubAgentOptions {
        prompt: "trusted rewrite".into(),
        task_agent_type_raw: Some(exact_json!({ "bad": 1 })),
        ..Default::default()
    })
    .await
    .map(|_| ())
    .expect_err("invalid creation must be refused");
    assert_eq!(
        error,
        RuntimeString::from(
            "unknown agent type \"[object Object]\" (available: known, [object Object])"
        )
    );
    assert!(session.background_task_registry().list().is_empty());
    assert!(session.subagent_lifecycle_snapshot().agents.is_empty());
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn trusted_non_string_resume_foreground_fails_instead_of_forking() {
    let history = three_turn_fork_history();
    let (session, _store, _requests, dir) = persisted_fork_session(history.clone()).await;
    let snapshot = Arc::new(move || history.clone()) as GetConversation;
    let spawn = session.make_spawn_sub_agent(
        0,
        CancellationToken::new(),
        session.permission_context(),
        snapshot,
        false,
        None,
        None,
    );

    let error = spawn(SpawnSubAgentOptions {
        prompt: "trusted rewrite".into(),
        task_resume_raw: Some(exact_json!(true)),
        ..Default::default()
    })
    .await
    .map(|_| ())
    .expect_err("invalid creation must be refused");
    assert_eq!(
        error,
        RuntimeString::from("value.replace is not a function")
    );
    assert!(session.background_task_registry().list().is_empty());
    assert!(session.subagent_lifecycle_snapshot().agents.is_empty());
    session.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn raw_resume_is_rejected_at_the_string_keyed_session_store_boundary() {
    assert_eq!(
        persistent_resume_key(&JsForwardedRawSlot::Raw(exact_json!(true))),
        Err(RAW_PERSISTED_RESUME_ERROR)
    );
    assert_eq!(
        persistent_resume_key(&JsForwardedRawSlot::String("agent-1".into())),
        Ok(Some("agent-1".to_string()))
    );
}

#[tokio::test]
async fn trusted_non_string_resume_background_fails_instead_of_forking() {
    let history = three_turn_fork_history();
    let (session, _store, _requests, dir) = persisted_fork_session(history.clone()).await;
    let snapshot = Arc::new(move || history.clone()) as GetConversation;
    let spawn = session.make_spawn_background_agent(
        0,
        None,
        session.permission_context(),
        snapshot,
        false,
        None,
        None,
    );

    let error = spawn(SpawnSubAgentOptions {
        prompt: "trusted rewrite".into(),
        task_resume_raw: Some(exact_json!([])),
        ..Default::default()
    })
    .await
    .map(|_| ())
    .expect_err("invalid creation must be refused");
    assert_eq!(
        error,
        RuntimeString::from("value.replace is not a function")
    );
    assert!(session.background_task_registry().list().is_empty());
    assert!(session.subagent_lifecycle_snapshot().agents.is_empty());
    session.shutdown_and_wait().await.unwrap();
    std::fs::remove_dir_all(dir).ok();
}

fn no_store_recording_session(
    turns: ScriptedTurns,
) -> (Arc<Session>, Arc<Mutex<Vec<ModelRequest>>>) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let mut config = default_config();
    config.fork = false;
    config.agent_memory = false;
    config.memory_recall = false;
    let session = mk_session_with_deps(
        unique_subagent_test_deps(recording_deps(Arc::clone(&requests), turns)),
        config,
        Vec::new(),
    );
    (session, requests)
}

fn foreground_spawn(session: &Arc<Session>) -> SpawnForegroundSubAgent {
    session.make_spawn_sub_agent(
        0,
        CancellationToken::new(),
        session.permission_context(),
        Arc::new(Vec::new) as GetConversation,
        false,
        None,
        None,
    )
}

/// A spawned agent must own the plan it writes while retaining shared durable
/// task access. Stable resume IDs recover that private plan across child runs.
#[cfg(unix)]
#[tokio::test]
async fn spawned_agents_write_private_plans_without_replacing_or_staling_the_parent() {
    use crate::tasks::task_store::{list_plan_tasks, query_task_list, TaskPlanBinding, TaskStatus};
    use crate::tools::builtins::task_tools::{
        create_task_create_tool, create_task_list_tool, create_task_write_tool,
    };
    use crate::tools::builtins::test_ctx::make_ctx;
    let root =
        std::env::temp_dir().join(format!("klaus-agent-plan-scope-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let _env = ConfigEnvGuard::set(root.to_str().unwrap());
    for parent_has_plan in [false, true] {
        let tool_turn = |id: &str, name: &str, input: RuntimeJson| {
            let mut turn = vec![ev::message_start(None)];
            turn.extend(exact_tool_use_events(id, name, &input));
            turn.push(ev::stop(Some(ModelStopReason::ToolUse), None));
            turn
        };
        let mut config = default_config();
        config.fork = false;
        config.permission_mode =
            crate::types::permission::ExternalPermissionMode::BypassPermissions;
        config.allow.push("TaskWrite".into());
        config.agent_memory = false;
        config.memory_recall = false;
        let session = mk_session_with_deps(
            unique_subagent_test_deps(mk_deps(vec![
                tool_turn(
                    "a-write",
                    "TaskWrite",
                    exact_json!({"tasks":[{"id":"step","subject":"agent A plan"}]}),
                ),
                text_turn("A planned"),
                tool_turn(
                    "b-write",
                    "TaskWrite",
                    exact_json!({"tasks":[{"id":"step","subject":"agent B plan"}]}),
                ),
                text_turn("B planned"),
                tool_turn(
                    "a-resume",
                    "TaskWrite",
                    exact_json!({"merge":true,"tasks":[{"id":"step","status":"completed"}]}),
                ),
                text_turn("A complete"),
            ])),
            config,
            vec![create_task_write_tool(), create_task_list_tool()],
        );
        let list_id = session.task_list_id();
        let mut parent = make_ctx("/tmp");
        parent.task_list_id = Some(list_id.clone());
        parent.task_plan = Some(TaskPlanBinding::capture(&list_id));
        if parent_has_plan {
            create_task_write_tool()
                .call(
                    exact_json!({"tasks":[{"subject":"parent initial plan"}]}),
                    &parent,
                    None,
                )
                .await
                .unwrap();
        }
        let before = query_task_list(&session.id, &list_id, Default::default())
            .await
            .unwrap();
        let spawn = session.make_spawn_sub_agent(
            0,
            CancellationToken::new(),
            session.permission_context(),
            Arc::new(Vec::new) as GetConversation,
            false,
            None,
            parent.task_plan.clone(),
        );
        let a = spawn(SpawnSubAgentOptions {
            prompt: "Plan agent A's assigned work".into(),
            ..Default::default()
        })
        .await
        .expect("child creation must be accepted");
        let a_id = a.agent_id.as_str().unwrap().clone();
        let a_events: Vec<_> = a.events.collect().await;
        assert_eq!(
            query_task_list(&session.id, &list_id, Default::default())
                .await
                .unwrap(),
            before
        );
        let a_scope = TaskPlanBinding::capture(&list_id).for_agent(&a_id);
        let a_steps = list_plan_tasks(&list_id, Some(&a_scope)).await.unwrap();
        assert_eq!(
            a_steps.len(),
            1,
            "spawned TaskWrite must execute successfully: {a_events:?}"
        );
        assert_eq!(a_steps[0].subject, "agent A plan");
        // This also covers a private ledger created before the root's first plan.
        create_task_write_tool()
            .call(
                exact_json!({"tasks":[{"subject":"parent next plan"}]}),
                &parent,
                None,
            )
            .await
            .unwrap();
        let parent_next = query_task_list(&session.id, &list_id, Default::default())
            .await
            .unwrap();
        let b = spawn(SpawnSubAgentOptions {
            prompt: "Plan agent B's assigned work".into(),
            ..Default::default()
        })
        .await
        .expect("child creation must be accepted");
        let b_id = b.agent_id.as_str().unwrap().clone();
        drain_foreground(b).await;
        drain_foreground(
            spawn(SpawnSubAgentOptions {
                prompt: "Complete agent A's plan".into(),
                resume: Some(a_id.clone()),
                ..Default::default()
            })
            .await
            .expect("child creation must be accepted"),
        )
        .await;
        assert_eq!(
            query_task_list(&session.id, &list_id, Default::default())
                .await
                .unwrap(),
            parent_next
        );
        let restored = TaskPlanBinding::capture(&list_id).for_agent(&a_id);
        let restored_steps = list_plan_tasks(&list_id, Some(&restored)).await.unwrap();
        assert_eq!(restored_steps[0].id, a_steps[0].id);
        assert_eq!(restored_steps[0].status, TaskStatus::Completed);
        let sibling = TaskPlanBinding::capture(&list_id).for_agent(&b_id);
        assert_eq!(
            list_plan_tasks(&list_id, Some(&sibling)).await.unwrap()[0].subject,
            "agent B plan"
        );
        create_task_create_tool()
            .call(
                exact_json!({"subject":"parent continuation","description":"append deliberately"}),
                &parent,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            query_task_list(&session.id, &list_id, Default::default())
                .await
                .unwrap()
                .tasks
                .len(),
            2
        );
        session.shutdown_and_wait().await.unwrap();
        std::fs::remove_dir_all(root.join("tasks")).unwrap();
    }
    std::fs::remove_dir_all(&root).ok();
}

/// Legacy planning initialization restores recorded document bindings only.
/// New planning artifacts never allocate a historical file permission exception.
#[tokio::test]
async fn legacy_planning_initialization_does_not_allocate_a_document_binding() {
    let mut config = default_config();
    config.permission_mode = crate::types::permission::ExternalPermissionMode::Plan;
    let session = mk_session_with(vec![text_turn("planned")], config, Vec::new());

    let result = session
        .run(Input::Text("draft a plan".into()), TurnOptions::default())
        .await;
    assert_eq!(result.stop_reason, TerminalReason::Completed);

    // A new planning conversation has no historical document binding.
    assert_eq!(
        session.plan_name(),
        None,
        "entering plan mode must not allocate a plan document"
    );
    assert_eq!(
        session.plan_file_context(None),
        None,
        "no historical plan file may be allocated during mode migration"
    );
    // Historical document paths remain inspectable for older transcripts.
    assert!(!session.plans_directory_now().is_empty());

    // Simulate restoration of a name recorded in an older transcript.
    session.set_plan_name(RuntimeString::from("align-queue-bar"));
    let plan_file = session
        .plan_file_context(None)
        .expect("plan-file context must exist once a name is bound");
    let path =
        String::from_utf8_lossy(&plan_file.path.as_runtime_string().to_node_utf8()).into_owned();
    assert!(
        path.ends_with("align-queue-bar.md"),
        "plan file must be <name>.md, got {path}"
    );
    assert!(
        crate::fs::plan_files::is_session_plan_file(
            &path,
            &String::from_utf8_lossy(&plan_file.plans_dir.as_runtime_string().to_node_utf8()),
            "align-queue-bar",
        ),
        "the bound path must satisfy the exemption predicate, got {path}"
    );

    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn legacy_planning_input_is_separate_from_canonical_permissions() {
    use crate::types::permission::{ApprovalPolicy, ExternalPermissionMode, SandboxMode};
    let session = mk_session_with(Vec::new(), default_config(), Vec::new());
    session
        .request_permission_mode_change(ExternalPermissionMode::Plan)
        .unwrap();
    session
        .set_runtime_policy(super::RuntimePolicyUpdate {
            approval_policy: Some(ApprovalPolicy::Never),
            sandbox_mode: Some(SandboxMode::DangerFullAccess),
            approvals_reviewer: None,
            network_allowed: None,
            sandbox_workspace_write: None,
        })
        .unwrap();
    assert_eq!(
        session.collaboration_mode(),
        crate::planning::CollaborationMode::Plan
    );
    let policy = session.runtime_policy_state();
    assert_eq!(policy.sandbox_mode, Some(SandboxMode::DangerFullAccess));
    assert_eq!(policy.approval_policy, Some(ApprovalPolicy::Never));
    assert!(policy.network_allowed);
    session.shutdown_and_wait().await.unwrap();
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn task_resource_scope_global_live_policy_cannot_drop_directory_boundary() {
    let mut config = default_config();
    config.task_resource_scope = Some(crate::schemas::config::TaskResourceScope {
        task_id: "scope-policy".into(),
        mode: crate::schemas::config::TaskResourceMode::Manual,
        network_allowed: false,
        read_roots: Vec::new(),
        write_roots: Vec::new(),
    });
    config.sandbox_mode = Some(crate::types::permission::SandboxMode::DangerFullAccess);
    let session = mk_session_with(Vec::new(), config, Vec::new());
    let scope = session
        .permission_engine
        .bound_task_resource_scope()
        .unwrap();
    session
        .set_runtime_policy(super::RuntimePolicyUpdate {
            approval_policy: Some(crate::types::permission::ApprovalPolicy::Never),
            sandbox_mode: Some(crate::types::permission::SandboxMode::DangerFullAccess),
            approvals_reviewer: None,
            network_allowed: Some(true),
            sandbox_workspace_write: None,
        })
        .unwrap();
    assert_eq!(
        session.runtime_policy_state().sandbox_mode,
        Some(crate::types::permission::SandboxMode::WorkspaceWrite)
    );
    assert!(Arc::ptr_eq(
        &scope,
        &session
            .permission_engine
            .bound_task_resource_scope()
            .unwrap()
    ));
    assert_eq!(
        session.permission_context().additional_writable_roots,
        scope.writable_roots()
    );
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn deprecated_session_preset_translates_to_canonical_policy() {
    let session = mk_session_with(Vec::new(), default_config(), Vec::new());
    session
        .request_permission_mode_change(
            crate::types::permission::ExternalPermissionMode::BypassPermissions,
        )
        .unwrap();
    let policy = session.runtime_policy_state();
    assert_eq!(
        policy.sandbox_mode,
        Some(crate::types::permission::SandboxMode::DangerFullAccess)
    );
    assert_eq!(
        policy.approval_policy,
        Some(crate::types::permission::ApprovalPolicy::Never)
    );
    assert_eq!(
        session.collaboration_mode(),
        crate::planning::CollaborationMode::Default
    );
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn deprecated_per_turn_preset_is_rejected_before_execution() {
    let session = mk_session_with(Vec::new(), default_config(), Vec::new());
    let before = session.runtime_policy_state();
    let result = session
        .run(
            super::Input::Text("Inspect the project".into()),
            super::TurnOptions {
                permission_mode_override: Some(
                    crate::types::permission::ExternalPermissionMode::BypassPermissions,
                ),
                ..Default::default()
            },
        )
        .await;
    assert!(result.error.is_some());
    assert_eq!(session.runtime_policy_state(), before);
    session.shutdown_and_wait().await.unwrap();
}

async fn drain_foreground(run: ForegroundSubAgentRun) {
    let mut events = run.events;
    while events.next().await.is_some() {}
}

#[tokio::test]
async fn trusted_raw_resume_without_history_is_refused_before_publication() {
    let (session, requests) = no_store_recording_session(vec![text_turn("must not run")]);
    let error = foreground_spawn(&session)(SpawnSubAgentOptions {
        prompt: "raw id".into(),
        task_resume_raw: Some(exact_json!(true)),
        ..Default::default()
    })
    .await
    .err()
    .expect("missing raw history must refuse creation");

    assert_eq!(
        error,
        RuntimeString::from("The sub-agent history is unavailable.")
    );
    assert!(requests.lock().unwrap().is_empty());
    assert!(session.subagent_lifecycle_snapshot().agents.is_empty());
    assert!(session.governor.resident_ids().is_empty());
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn no_store_raw_primitive_resume_does_not_alias_the_same_text_string_id() {
    let (session, requests) = no_store_recording_session(vec![text_turn("done")]);
    session.subagents.transcripts.lock().unwrap().insert(
        SubAgentMapKey::String("true".into()),
        vec![fork_user("string-seed")],
    );

    let error = foreground_spawn(&session)(SpawnSubAgentOptions {
        prompt: "raw bool".into(),
        task_resume_raw: Some(exact_json!(true)),
        ..Default::default()
    })
    .await
    .err()
    .expect("raw primitive must not reuse a same-text string history");
    assert_eq!(
        error,
        RuntimeString::from("The sub-agent history is unavailable.")
    );
    assert!(requests.lock().unwrap().is_empty());
    assert_eq!(
        session
            .subagents
            .transcripts
            .lock()
            .unwrap()
            .get(&SubAgentMapKey::String("true".into())),
        Some(&vec![fork_user("string-seed")])
    );
    assert!(!session
        .subagents
        .transcripts
        .lock()
        .unwrap()
        .contains_key(&SubAgentMapKey::Bool(true)));
    session.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn no_store_missing_raw_resume_never_creates_primitive_or_object_history() {
    for raw in [exact_json!(7), exact_json!({ "same": true })] {
        let (session, requests) = no_store_recording_session(vec![text_turn("must not run")]);
        for prompt in ["first attempt", "second attempt"] {
            let error = foreground_spawn(&session)(SpawnSubAgentOptions {
                prompt: prompt.into(),
                task_resume_raw: Some(raw.clone()),
                ..Default::default()
            })
            .await
            .err()
            .expect("a resume call must not invent its missing history");
            assert_eq!(
                error,
                RuntimeString::from("The sub-agent history is unavailable.")
            );
        }
        assert!(requests.lock().unwrap().is_empty());
        assert!(session.subagents.transcripts.lock().unwrap().is_empty());
        assert!(session.subagent_lifecycle_snapshot().agents.is_empty());
        assert!(session.governor.resident_ids().is_empty());
        session.shutdown_and_wait().await.unwrap();
    }
}

#[tokio::test]
async fn run_sub_agent_runs_a_typeless_child_to_completion() {
    // With no agent registry, a type-less delegation is a parent-clone child.
    let session = mk_session(vec![text_turn("child-done")]);
    let permission = session.permission_context();
    let opts = SpawnSubAgentOptions {
        prompt: "do the thing".into(),
        ..Default::default()
    };
    let mut gen = session.run_sub_agent(
        opts,
        1,
        CancellationToken::new(),
        permission,
        "child-1".into(),
        RunSubAgentOpts::default(),
    );
    let mut final_text = RuntimeString::default();
    let mut completed = false;
    while let Some(ev) = gen.next().await {
        if let EngineEvent::ItemCompleted {
            inference: _,
            item: ThreadItem::AgentMessage { text, .. },
        } = &ev
        {
            final_text = text.clone();
        }
        if let EngineEvent::TurnCompleted { stop_reason, .. } = &ev {
            completed = *stop_reason == TerminalReason::Completed;
        }
    }
    assert_eq!(final_text, "child-done");
    assert!(completed);
    // No store → the child transcript is retained in the in-memory map.
    assert!(session
        .subagents
        .transcripts
        .lock()
        .unwrap()
        .contains_key(&SubAgentMapKey::String("child-1".into())));
}

#[tokio::test]
async fn run_sub_agent_records_lifecycle_on_the_real_subagent_path() {
    let session = mk_session(vec![text_turn("child-done")]);
    let permission = session.permission_context();
    let opts = SpawnSubAgentOptions {
        prompt: "inspect engine state".into(),
        ..Default::default()
    };
    let mut gen = session.run_sub_agent(
        opts,
        1,
        CancellationToken::new(),
        permission,
        "child-1".into(),
        RunSubAgentOpts::default(),
    );
    while gen.next().await.is_some() {}

    let snapshot = session.subagent_lifecycle_snapshot();
    assert_eq!(snapshot.agents.len(), 1);
    let child = &snapshot.agents[0];
    assert_eq!(child.agent_id, "child-1");
    assert_eq!(
        child.kind,
        crate::engine::subagent_lifecycle::SubagentKind::Foreground
    );
    assert_eq!(
        child.status,
        crate::engine::subagent_lifecycle::SubagentStatus::Completed
    );
    assert_eq!(child.context.session_id, "s1");
    assert_eq!(child.context.parent_agent_id, None);
    assert_eq!(child.context.description, "inspect engine state");
    assert_eq!(child.context.depth, 1);
    assert_eq!(child.last_result, Some("child-done".into()));
    assert!(child.completed_at_ms.is_some());
    // No tier token and no definition model: the row names the parent's own
    // model as the one the child actually ran on.
    assert_eq!(child.model.as_ref(), Some(&session.config.model));
    assert_eq!(child.model_source, None);
}

#[tokio::test]
async fn run_sub_agent_stamps_the_tier_resolved_model_on_the_lifecycle_row() {
    let mut config = default_config();
    config.model_tiers.high = Some("tier-high-model".into());
    config.model_source = Some(crate::types::events::InferenceSource {
        source: Some("codex".into()),
        model_id: Some("main-model".into()),
        tier: None,
        selection: Some("codex:main-model".into()),
    });
    let session = mk_session_with(vec![text_turn("child-done")], config, Vec::new());
    let permission = session.permission_context();
    let opts = SpawnSubAgentOptions {
        prompt: "hard reasoning".into(),
        model: Some("high".into()),
        ..Default::default()
    };
    let mut gen = session.run_sub_agent(
        opts,
        1,
        CancellationToken::new(),
        permission,
        "child-tier".into(),
        RunSubAgentOpts::default(),
    );
    while gen.next().await.is_some() {}

    let snapshot = session.subagent_lifecycle_snapshot();
    assert_eq!(snapshot.agents.len(), 1);
    let child = &snapshot.agents[0];
    assert_eq!(child.agent_id, "child-tier");
    // The tier token the parent passed is resolved against the session's tier
    // map before the child starts; the row carries the concrete model, not
    // the token, plus the model mode that model belongs to.
    assert_eq!(
        child.model.as_ref(),
        Some(&RuntimeString::from("tier-high-model"))
    );
    assert_eq!(
        child.model_source.as_ref(),
        Some(&RuntimeString::from("codex"))
    );
}

#[tokio::test]
async fn run_sub_agent_records_lifecycle_failures_on_the_real_subagent_path() {
    let session = mk_session(vec![provider_error_turn("bad request")]);
    let permission = session.permission_context();
    let opts = SpawnSubAgentOptions {
        prompt: "fail the child".into(),
        inherited_context: Some(Arc::new(crate::types::tool::InheritedSubagentContext {
            system_prompt: Vec::new(),
            tools: Vec::new(),
            mcp: crate::engine::session::SubagentMcpCapture::capture(&[], &[]),
        })),
        ..Default::default()
    };
    let mut gen = session.run_sub_agent(
        opts,
        2,
        CancellationToken::new(),
        permission,
        "child-failed".into(),
        RunSubAgentOpts {
            parent_agent_id: Some("parent-agent".to_string()),
            ..Default::default()
        },
    );
    while gen.next().await.is_some() {}

    let snapshot = session.subagent_lifecycle_snapshot();
    assert_eq!(snapshot.agents.len(), 1);
    let child = &snapshot.agents[0];
    assert_eq!(child.agent_id, "child-failed");
    assert_eq!(
        child.status,
        crate::engine::subagent_lifecycle::SubagentStatus::Failed
    );
    assert_eq!(
        child.context.parent_agent_id.as_ref(),
        Some(&RuntimeString::from("parent-agent"))
    );
    assert_eq!(child.context.depth, 2);
    assert_eq!(child.last_error, Some("bad request".into()));
    assert!(child.completed_at_ms.is_some());
}

#[tokio::test]
async fn run_sub_agent_accepts_descendants_beyond_the_legacy_depth_limit() {
    let session = mk_session(vec![text_turn("deep child completed")]);
    let permission = session.permission_context();
    let mut gen = session.run_sub_agent(
        SpawnSubAgentOptions {
            prompt: "x".into(),
            ..Default::default()
        },
        1_000,
        CancellationToken::new(),
        permission,
        "deep".into(),
        RunSubAgentOpts::default(),
    );
    let mut failed_msg = None;
    while let Some(ev) = gen.next().await {
        if let EngineEvent::TurnFailed { error, .. } = &ev {
            failed_msg = Some(error.message.clone());
        }
    }
    assert_eq!(failed_msg, None);
}

// ---------------------------------------------------------------------------
// Review task mode: isolated review turns (session_review.rs).
// ---------------------------------------------------------------------------

const REVIEW_FIXTURE_JSON: &str = r#"{
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

// A review runs on an isolated delegate transcript: the main conversation
// history is not read into it and nothing the reviewer produced is written
// back; the structured verdict is the only artifact.
#[tokio::test]
async fn review_turn_is_isolated_from_main_history_and_parses_findings() {
    let turns = vec![
        // Turn 1: a regular conversation turn that seeds main history.
        vec![
            ev::message_start(None),
            ev::text("regular reply"),
            ev::stop(Some(ModelStopReason::EndTurn), None),
        ],
        // Turn 2: the review delegate's structured verdict.
        vec![
            ev::message_start(None),
            ev::text(REVIEW_FIXTURE_JSON),
            ev::stop(
                Some(ModelStopReason::EndTurn),
                Some(make_usage(Some(7), Some(3))),
            ),
        ],
    ];
    let session = mk_session(turns);
    session
        .run(Input::Text("hello".into()), TurnOptions::default())
        .await;
    let history_before = session.history_snapshot();

    let result = session
        .run_review(ReviewRequest {
            target: ReviewTarget::UncommittedChanges,
            user_facing_hint: None,
        })
        .await
        .expect("review completes");

    // Findings parsed from the reviewer's final message.
    assert_eq!(result.output.findings.len(), 1);
    let finding = &result.output.findings[0];
    assert_eq!(finding.title, "[P1] Off-by-one in range check");
    assert_eq!(finding.priority, Some(1));
    assert_eq!(finding.code_location.absolute_file_path, "/repo/src/lib.rs");
    assert_eq!(finding.code_location.line_range.start, 10);
    assert_eq!(result.output.overall_correctness, "patch is incorrect");
    assert_eq!(result.user_facing_hint, "current changes");
    assert_eq!(result.stop_reason, TerminalReason::Completed);
    assert!(result.usage.is_some());

    // Isolation: the review turn left the main history untouched.
    assert_eq!(session.history_snapshot(), history_before);
}

// Unstructured reviewer output degrades to the raw text carried in
// overall_explanation, and a host-provided hint overrides the derived one.
#[tokio::test]
async fn review_turn_falls_back_to_plain_text_and_honors_the_hint_override() {
    let turns = vec![vec![
        ev::message_start(None),
        ev::text("Looks good overall."),
        ev::stop(Some(ModelStopReason::EndTurn), None),
    ]];
    let session = mk_session(turns);
    let history_before = session.history_snapshot();

    let result = session
        .run_review(ReviewRequest {
            target: ReviewTarget::Commit {
                sha: "abc1234def".into(),
                title: None,
            },
            user_facing_hint: Some("release candidate".into()),
        })
        .await
        .expect("review completes");

    assert!(result.output.findings.is_empty());
    assert_eq!(result.output.overall_explanation, "Looks good overall.");
    assert_eq!(result.user_facing_hint, "release candidate");
    assert_eq!(session.history_snapshot(), history_before);
}

// An invalid target is rejected before any provider call is made.
#[tokio::test]
async fn review_turn_rejects_an_empty_custom_target() {
    let session = mk_session(vec![]);
    let error = session
        .run_review(ReviewRequest {
            target: ReviewTarget::Custom {
                instructions: "   ".into(),
            },
            user_facing_hint: None,
        })
        .await
        .expect_err("empty custom target is invalid");
    assert_eq!(error, "review prompt cannot be empty");
}

// ---- goal mode ------------------------------------------------------------
//
// Core runs one actual turn per admission. Goal continuations return through
// idle admission so queued work and controls can take priority between turns.

#[cfg(unix)]
fn goal_temp_home(tag: &str) -> (ConfigEnvGuard, std::path::PathBuf) {
    static SEQ: AtomicUsize = AtomicUsize::new(0);
    let mut home = std::env::temp_dir();
    home.push(format!(
        "klaus-goal-session-{tag}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&home).unwrap();
    let guard = ConfigEnvGuard::set(&home.to_string_lossy());
    (guard, home)
}

#[cfg(unix)]
#[tokio::test]
async fn persistent_goal_turns_use_distinct_idle_admissions_and_one_budget_wrapup() {
    let (_env, home) = goal_temp_home("continue");
    let mut config = default_config();
    config.goal = true;
    // Each scripted turn reports 10 input + 5 output = 15 goal tokens, so a
    // budget of 20 is crossed by the SECOND turn and not the first.
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(home.join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let rollout: RolloutSink = Arc::from(
        store
            .create(SessionMeta {
                id: "s1".into(),
                created_at: 1,
                cwd: runtime_path_from_host_path(&home).into_runtime_string(),
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
            })
            .await
            .unwrap(),
    );
    let session = mk_session_with_provider_memory_and_store(
        Arc::new(fake_provider()),
        mk_deps(vec![
            text_turn("first pass"),
            text_turn("second pass"),
            text_turn("Budget wrap-up without more substantive work."),
        ]),
        config,
        Vec::new(),
        None,
        Some(rollout),
        Some(store),
        None,
    );
    session
        .set_thread_goal(Some("ship the thing".into()), None, Some(Some(20)))
        .await
        .expect("the host creates the goal");

    let mut events: Vec<EngineEvent> = session
        .run_streamed(Input::Text("go".into()), TurnOptions::default())
        .events
        .collect()
        .await;
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, EngineEvent::TurnStarted { .. }))
            .count(),
        1,
        "one foreground admission must release ownership after its actual turn"
    );
    let first = session.thread_goal().await.unwrap().unwrap();
    assert_eq!(first.status, crate::goal::types::ThreadGoalStatus::Active);
    assert_eq!(first.tokens_used, 15);
    let continuation = session
        .run_goal_idle_streamed(TurnOptions::default())
        .await
        .unwrap()
        .expect("a separate idle admission continues the active goal");
    events.extend(continuation.events.collect::<Vec<_>>().await);
    let limited = session.thread_goal().await.unwrap().unwrap();
    assert_eq!(
        limited.status,
        crate::goal::types::ThreadGoalStatus::BudgetLimited
    );
    assert_eq!(limited.tokens_used, 30);
    let wrapup = session
        .run_goal_idle_streamed(TurnOptions::default())
        .await
        .unwrap()
        .expect("the budget boundary permits exactly one admitted wrap-up");
    events.extend(wrapup.events.collect::<Vec<_>>().await);
    assert!(session
        .run_goal_idle_streamed(TurnOptions::default())
        .await
        .unwrap()
        .is_none());

    let started = events
        .iter()
        .filter(|event| matches!(event, EngineEvent::TurnStarted { .. }))
        .count();
    let completed = events
        .iter()
        .filter(|event| matches!(event, EngineEvent::TurnCompleted { .. }))
        .count();
    assert_eq!(
        (started, completed),
        (3, 3),
        "the requested turn, idle continuation and single wrap-up have distinct ownership"
    );

    // The continuation turn's input is a META message carrying the instruction,
    // so a UI does not render it as something the user typed.
    let history = session.history_snapshot();
    let continuation = history
        .iter()
        .filter_map(|message| match message {
            Message::User(user) => Some(user),
            _ => None,
        })
        .find(|user| {
            user.content.iter().any(|block| match block {
                ContentBlock::Text(text) => text.text.try_to_utf8().is_ok_and(|text| {
                    text.contains("Continue working toward the active thread goal.")
                }),
                _ => false,
            })
        })
        .expect("the continuation instruction is in the transcript");
    assert_eq!(continuation.is_meta, Some(true));

    let goal = session
        .thread_goal()
        .await
        .expect("storage is readable")
        .expect("the goal survives the run");
    assert_eq!(
        goal.status,
        crate::goal::types::ThreadGoalStatus::BudgetLimited,
        "reaching the budget is what ends the run"
    );
    assert_eq!(
        goal.tokens_used, 45,
        "the permitted wrap-up remains charged"
    );
    session.shutdown_and_wait().await.unwrap();
    let _ = std::fs::remove_dir_all(&home);
}

#[cfg(unix)]
#[tokio::test]
async fn goal_mode_off_runs_exactly_the_one_turn_the_host_asked_for() {
    let (_env, home) = goal_temp_home("off");
    let session = mk_session_with(
        vec![text_turn("first pass"), text_turn("second pass")],
        default_config(),
        Vec::new(),
    );
    // The goal surface is refused outright rather than silently doing nothing.
    assert!(session
        .set_thread_goal(Some("ship the thing".into()), None, None)
        .await
        .is_err());

    let events: Vec<EngineEvent> = session
        .run_streamed(Input::Text("go".into()), TurnOptions::default())
        .events
        .collect()
        .await;
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, EngineEvent::TurnStarted { .. }))
            .count(),
        1
    );
    let _ = std::fs::remove_dir_all(&home);
}

/// Title generation is background maintenance: it runs on the session's LOW
/// tier when the tier map has one, not on the main model.
#[tokio::test]
async fn title_generation_runs_on_the_low_tier_model() {
    let requests: Arc<Mutex<Vec<ModelRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let mut config = default_config();
    config.model = "main-model".into();
    config.model_tiers.low = Some("low-model".into());
    config.agent_memory = false;
    let session = mk_session_with_provider_memory_and_store(
        Arc::new(fake_provider()),
        recording_deps(
            requests.clone(),
            vec![text_turn(r#"{"title":"A test title"}"#)],
        ),
        config,
        Vec::new(),
        None,
        None,
        None,
        Some(vec![Message::User(UserMessage {
            question_answer: None,
            uuid: "title-tier-input".into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: "refactor the parser".into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_meta: None,
        })]),
    );

    let result = session.generate_title(None, None, false).await.unwrap();

    assert_eq!(result.title, Some(RuntimeString::from("A test title")));
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].model, "low-model");
}

/// The memory-maintenance fork (extraction / consolidation / dream) is pure
/// background work: it runs on the LOW tier.
/// The memory model setting's default (no configured target) keeps maintenance
/// on the session's main model even when model tiers are configured.
#[tokio::test]
async fn memory_extraction_fork_follows_the_main_model_when_no_memory_target_is_set() {
    let root = temp_store_dir();
    let host: Arc<dyn AgentMemoryHost> = Arc::new(SyncTestMemoryHost {
        layout: SyncTestMemoryLayout {
            base: root.to_string_lossy().into_owned(),
        },
        plans: Mutex::new(Vec::new()),
        applies: Mutex::new(Vec::new()),
        apply_results: Mutex::new(Vec::new()),
    });
    let requests: Arc<Mutex<Vec<ModelRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let mut config = default_config();
    config.model = "main-model".into();
    config.model_tiers.low = Some("low-model".into());
    config.agent_memory = true;
    config.auto_memory = true;
    config.max_retries = 0;
    let session = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        recording_deps(requests.clone(), vec![text_turn("done")]),
        config,
        Vec::new(),
        Some(host),
        None,
        None,
        Some(vec![Message::User(UserMessage {
            question_answer: None,
            uuid: "tier-fork-input".into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: "remember something".into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_meta: None,
        })]),
        None,
    );

    session.maybe_extract_memories();
    tokio::time::timeout(CROSS_THREAD_LIVENESS_TIMEOUT, async {
        while session
            .memory_scheduler
            .extraction_in_progress
            .load(Ordering::SeqCst)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("tiered extraction should terminate");

    let requests = requests.lock().unwrap();
    assert!(!requests.is_empty());
    assert!(
        requests.iter().all(|req| req.model == "main-model"),
        "an unset memory model setting follows the session's main model, not a configured tier"
    );
    std::fs::remove_dir_all(root).ok();
}

/// A configured memory model (ready feature target) must reach the maintenance
/// fork: the admission policy for user-requested subagent model selections must
/// not intercept internal feature-slot selections.
#[tokio::test]
async fn memory_extraction_fork_runs_on_the_configured_memory_feature_target() {
    use super::{FeatureTargets, InferenceTarget, InferenceTargetState};
    use crate::provider::anthropic::{AnthropicProvider, AnthropicProviderOptions};
    use crate::types::provider::ModelCapabilities;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let captured = Arc::new(Mutex::new(Vec::<serde_json::Value>::new()));
    let server_capture = captured.clone();
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (socket, _) = listener.accept().await.unwrap();
            let mut reader = tokio::io::BufReader::new(socket);
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            let mut length = 0usize;
            loop {
                line.clear();
                reader.read_line(&mut line).await.unwrap();
                if line == "\r\n" {
                    break;
                }
                if let Some((name, value)) = line.split_once(':') {
                    if name.eq_ignore_ascii_case("content-length") {
                        length = value.trim().parse().unwrap();
                    }
                }
            }
            let mut bytes = vec![0; length];
            reader.read_exact(&mut bytes).await.unwrap();
            let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            server_capture.lock().unwrap().push(body);
            let error = serde_json::json!({"type":"error","error":{"type":"invalid_request_error","message":"fixture memory target rejected"}}).to_string();
            let reply = format!("HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", error.len(), error);
            reader.get_mut().write_all(reply.as_bytes()).await.unwrap();
            reader.get_mut().shutdown().await.unwrap();
        }
    });
    let root = temp_store_dir();
    let host: Arc<dyn AgentMemoryHost> = Arc::new(SyncTestMemoryHost {
        layout: SyncTestMemoryLayout {
            base: root.to_string_lossy().into_owned(),
        },
        plans: Mutex::new(Vec::new()),
        applies: Mutex::new(Vec::new()),
        apply_results: Mutex::new(Vec::new()),
    });
    let mut config = default_config();
    config.model = "main-model".into();
    config.agent_memory = true;
    config.auto_memory = true;
    config.max_retries = 0;
    let session = mk_session_with_provider_memory_store_and_agents(
        Arc::new(fake_provider()),
        recording_deps(Arc::new(Mutex::new(Vec::new())), vec![text_turn("done")]),
        config,
        Vec::new(),
        Some(host),
        None,
        None,
        Some(vec![Message::User(UserMessage {
            question_answer: None,
            uuid: "feature-target-fork-input".into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: "remember something".into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_meta: None,
        })]),
        None,
    );
    session
        .install_feature_targets(FeatureTargets {
            memory: Some(InferenceTargetState::Ready(InferenceTarget {
                source: "custom".into(),
                model_id: "memory-target-entry".into(),
                model: "memory-target-wire".into(),
                provider: Arc::new(AnthropicProvider::new(AnthropicProviderOptions {
                    api_key: Some("memory-target-key".into()),
                    base_url: Some(url),
                    default_model: Some("memory-target-wire".into()),
                    capabilities: Some(ModelCapabilities {
                        tools: Some(true),
                        context_window: Some(20_000),
                        max_output_tokens: Some(333),
                        ..Default::default()
                    }),
                    ..Default::default()
                })),
                pricing: Vec::new(),
                review_requirements: None,
            })),
            ..Default::default()
        })
        .unwrap();

    session.maybe_extract_memories();
    tokio::time::timeout(CROSS_THREAD_LIVENESS_TIMEOUT, async {
        while session
            .memory_scheduler
            .extraction_in_progress
            .load(Ordering::SeqCst)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("feature-target extraction should terminate");
    server.abort();

    let bodies = captured.lock().unwrap();
    assert!(
        !bodies.is_empty(),
        "the configured memory model must receive the maintenance fork's requests"
    );
    assert!(
        bodies
            .iter()
            .all(|body| body["model"].as_str() == Some("memory-target-wire")),
        "the maintenance fork must ride the configured memory model"
    );
    std::fs::remove_dir_all(root).ok();
}

/// A spawn-time `model` selection resolves against the session tier map:
/// configured tiers hit their model, unconfigured tiers inherit the main
/// model, and concrete ids pass through untouched.
#[tokio::test]
async fn spawn_sub_agent_resolves_tier_selections() {
    let requests: Arc<Mutex<Vec<ModelRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let mut config = default_config();
    config.model = "main-model".into();
    config.model_tiers.low = Some("low-model".into());
    config.fork = false;
    config.agent_memory = false;
    config.memory_recall = false;
    let session = mk_session_with_deps(
        unique_subagent_test_deps(recording_deps(
            requests.clone(),
            vec![text_turn("one"), text_turn("two"), text_turn("three")],
        )),
        config,
        Vec::new(),
    );
    let spawn = foreground_spawn(&session);

    for selection in ["low", "high", "concrete-id"] {
        let run = spawn(SpawnSubAgentOptions {
            prompt: "tier probe".into(),
            model: Some(selection.into()),
            ..Default::default()
        })
        .await
        .expect("child creation must be accepted");
        assert_eq!(foreground_failure(run.events).await, None, "{selection}");
    }
    session.shutdown_and_wait().await.unwrap();

    let models: Vec<String> = requests
        .lock()
        .unwrap()
        .iter()
        .map(|req| req.model.try_to_utf8().unwrap())
        .collect();
    assert_eq!(models, vec!["low-model", "main-model", "concrete-id"]);
}

#[tokio::test]
async fn background_spawn_sub_agent_resolves_tier_selections() {
    let requests: Arc<Mutex<Vec<ModelRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let mut config = default_config();
    config.model = "main-model".into();
    config.model_tiers.low = Some("low-model".into());
    config.fork = false;
    config.agent_memory = false;
    config.memory_recall = false;
    let session = mk_session_with_deps(
        unique_subagent_test_deps(recording_deps(
            requests.clone(),
            vec![text_turn("one"), text_turn("two"), text_turn("three")],
        )),
        config,
        Vec::new(),
    );
    let spawn = session.make_spawn_background_agent(
        0,
        None,
        session.permission_context(),
        Arc::new(Vec::new),
        false,
        None,
        None,
    );

    for (selection, expected_output) in [("low", "one"), ("high", "two"), ("concrete-id", "three")]
    {
        let spawned = spawn(SpawnSubAgentOptions {
            prompt: "tier probe".into(),
            model: Some(selection.into()),
            ..Default::default()
        })
        .await
        .expect("child creation must be accepted");
        let pending = std::mem::take(&mut session.background_tasks.lock().unwrap().handles);
        for handle in pending {
            handle.await.unwrap();
        }
        assert_eq!(
            std::fs::read_to_string(runtime_path_to_file_io_host(&spawned.output_file)).unwrap(),
            expected_output,
            "{selection}"
        );
        std::fs::remove_file(runtime_path_to_file_io_host(&spawned.output_file)).unwrap();
    }
    session.shutdown_and_wait().await.unwrap();

    let models: Vec<String> = requests
        .lock()
        .unwrap()
        .iter()
        .map(|req| req.model.try_to_utf8().unwrap())
        .collect();
    assert_eq!(models, vec!["low-model", "main-model", "concrete-id"]);
}

#[tokio::test]
async fn ordinary_foreground_keeps_its_parent_inference_capture() {
    let requests: Arc<Mutex<Vec<ModelRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let mut config = default_config();
    config.fork = false;
    config.agent_memory = false;
    config.memory_recall = false;
    let session = mk_session_with_deps(
        unique_subagent_test_deps(recording_deps(
            requests.clone(),
            vec![text_turn("captured")],
        )),
        config,
        Vec::new(),
    );
    let selected_model = RuntimeString::from("captured-concrete-model");
    let capture = Arc::new(
        session
            .begin_inference_activity_for(Some(&selected_model))
            .unwrap(),
    );
    session.set_model("later-live-model").unwrap();
    let spawn = session.make_spawn_sub_agent_with_capture(
        0,
        CancellationToken::new(),
        session.permission_context(),
        Arc::new(Vec::new),
        false,
        None,
        None,
        Some(capture),
        None,
    );
    let run = spawn(SpawnSubAgentOptions {
        prompt: "captured child".into(),
        ..Default::default()
    })
    .await
    .expect("child creation must be accepted");
    assert_eq!(foreground_failure(run.events).await, None);
    drop(spawn);
    session.shutdown_and_wait().await.unwrap();

    let models: Vec<RuntimeString> = requests
        .lock()
        .unwrap()
        .iter()
        .map(|request| request.model.clone())
        .collect();
    assert_eq!(models, vec![selected_model]);
}

fn ordinary_parent_turn_lease(session: &Arc<Session>, turn_id: &str) -> ActiveTurnLease {
    let lease = ActiveTurnLease::try_new(
        session.clone(),
        session.config.model.clone(),
        CancellationToken::new(),
    )
    .unwrap();
    session
        .turn
        .active_stack
        .lock()
        .unwrap()
        .last_mut()
        .unwrap()
        .turn_id = Some(turn_id.into());
    lease
}

struct CaptureBackgroundNoticeTool {
    schema: JsonSchema,
    seen: Arc<Mutex<Option<crate::types::tool::EnqueueBackgroundNoticeFn>>>,
}

#[async_trait::async_trait]
impl Tool for CaptureBackgroundNoticeTool {
    fn static_name(&self) -> &str {
        "capture_background_notice"
    }
    fn static_description(&self) -> &str {
        "Capture the background notice sink."
    }
    fn static_input_schema(&self) -> &JsonSchema {
        &self.schema
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
        _input: Json,
        ctx: &ToolContext,
        _on_progress: Option<ToolProgress>,
    ) -> Result<ToolResult, ToolError> {
        *self.seen.lock().unwrap() = ctx.enqueue_background_notice.clone();
        Ok(text_result(Json::Null, "captured".to_string(), false))
    }
}

#[tokio::test]
async fn internal_shell_notice_retains_harness_provenance_in_the_queue() {
    let seen = Arc::new(Mutex::new(None));
    let tool: AnyTool = Arc::new(CaptureBackgroundNoticeTool {
        schema: JsonSchema::new(),
        seen: seen.clone(),
    });
    let mut first = vec![ev::message_start(None)];
    first.extend(exact_tool_use_events(
        "capture-notice",
        "capture_background_notice",
        &exact_json!({}),
    ));
    first.push(ev::stop(Some(ModelStopReason::ToolUse), None));
    let session = mk_session_with(vec![first, text_turn("done")], default_config(), vec![tool]);
    session
        .run(Input::Text("capture".into()), TurnOptions::default())
        .await;
    let enqueue = seen
        .lock()
        .unwrap()
        .clone()
        .expect("the real tool context has a notice sink");
    enqueue("Background shell fixture-shell has finished. Read its final output.".into());
    let queued = session.conversation.queue().snapshot();
    let notice = queued
        .iter()
        .find(|item| {
            item.origin
                .as_ref()
                .is_some_and(|origin| origin == "background-shell")
        })
        .expect("the completion notice remains pending");
    let blocks = crate::engine::message_queue::queued_value_to_content_blocks(&notice.value);
    assert!(
        matches!(blocks.as_slice(), [ContentBlock::Text(text)] if text.harness_envelope == Some(crate::schemas::content::HarnessEnvelope::SystemReminder) && text.text.contains("fixture-shell"))
    );
    session.dispose().await;
}
