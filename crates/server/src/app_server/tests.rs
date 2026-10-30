//! End-to-end tests: a real scripted `Agent` behind the app-server facade,
//! driven over an in-memory duplex exactly like a stdio client would.

#[path = "../../tests/kiro_source.rs"]
mod kiro_source;

#[path = "text_completion_facade_tests.rs"]
mod text_completion;

#[path = "output_control_tests.rs"]
mod output_control_tests;

#[path = "../../tests/rab6b0c00_stop_immediate_submit.rs"]
mod rab6b0c00_stop_immediate_submit;

#[path = "../../tests/r20261024_atomic_turn_submission.rs"]
mod r20261024_atomic_turn_submission;
#[path = "../../tests/r20261024_durable_thread_queue.rs"]
mod r20261024_durable_thread_queue;
#[path = "../../../core/tests/regressions/r20261025_empty_continuation.rs"]
mod r20261025_empty_continuation;
#[path = "../../../core/tests/regressions/r20261027_task_identity.rs"]
mod r20261027_task_identity;

#[path = "../../../core/tests/regressions/r9d6e3284_cursor_tool_catalog_capacity.rs"]
mod r9d6e3284_cursor_tool_catalog_capacity;

#[path = "../../../core/tests/regressions/r20261020_cursor_account_completion.rs"]
mod r20261020_cursor_account_completion;

#[path = "../../../core/tests/regressions/r20261020_cursor_model_resolution.rs"]
mod r20261020_cursor_model_resolution;

#[path = "../../tests/r20261019_antigravity_browser_login.rs"]
mod r20261019_antigravity_browser_login;

#[path = "../../tests/r20261019_approval_review_contract.rs"]
mod r20261019_approval_review_contract;

#[path = "../../tests/r20261018_cursor_metadata_discovery.rs"]
mod r20261018_cursor_metadata_discovery;

#[path = "../../tests/r20261017_goal_close_facade.rs"]
mod r20261017_goal_close_facade;

#[path = "../../tests/r20261018_runtime_settings_facade.rs"]
mod r20261018_runtime_settings_facade;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use klaus_agent_core::engine::agent::{Agent, AgentOptions, StartSessionOptions};
use klaus_agent_core::engine::deps::EngineDepsOverrides;
use klaus_agent_core::engine::session::Session;
use klaus_agent_core::testing::{ev, fake_provider, scripted_call_model, ScriptedTurns};
use klaus_agent_core::types::permission::ExternalPermissionMode;
use klaus_agent_core::types::provider::{ModelProvider, ModelStopReason, ModelStreamEvent};

use crate::lifecycle::{BoxError, CreateSession, SessionContext};

use super::{run_app_server, schema_bundle, AppServerOptions};

#[path = "../../tests/r20261010_turn_error_details_history.rs"]
mod r20261010_turn_error_details_history;

#[path = "../../../core/tests/regressions/r20261019_subscription_error_details.rs"]
mod r20261019_subscription_error_details;

#[path = "../../../core/tests/regressions/r20261019_workbuddy_instruction_roles.rs"]
mod r20261019_workbuddy_instruction_roles;

#[path = "../../../core/tests/regressions/r20261019_workbuddy_default_effort.rs"]
mod r20261019_workbuddy_default_effort;

#[path = "../../../core/tests/regressions/r20261019_workbuddy_catalog_audience.rs"]
mod r20261019_workbuddy_catalog_audience;

#[path = "../../../core/tests/regressions/rb712de41_kiro_rate_multiplier.rs"]
mod rb712de41_kiro_rate_multiplier;

#[path = "../../../core/tests/regressions/r20261019_workbuddy_explicit_selection.rs"]
mod r20261019_workbuddy_explicit_selection;

#[path = "../../../core/tests/regressions/r20261020_video_geometry_adaptation.rs"]
mod r20261020_video_geometry_adaptation;

#[path = "../../tests/r20261015_instruction_provenance.rs"]
mod r20261015_instruction_provenance;

#[path = "../../tests/r20261017_goal_lifecycle_facade.rs"]
mod r20261017_goal_lifecycle_facade;

const REPLY_TIMEOUT: Duration = Duration::from_secs(10);

fn text_turn(t: &str) -> Vec<ModelStreamEvent> {
    vec![
        ev::message_start(None),
        ev::text(t),
        ev::stop(Some(ModelStopReason::EndTurn), None),
    ]
}

fn make_create_session(replies: &[&str]) -> CreateSession {
    let turns: ScriptedTurns = replies.iter().map(|r| text_turn(r)).collect();
    create_session_for_agent_tests(make_agent_for_tests(turns))
}

fn make_agent_for_tests(turns: ScriptedTurns) -> Arc<Agent> {
    make_agent_with_call_model(scripted_call_model(turns))
}

fn make_agent_with_call_model(
    call_model: klaus_agent_core::types::provider::CallModel,
) -> Arc<Agent> {
    make_agent_with_call_model_and_config(call_model, None)
}

fn make_agent_with_call_model_and_config(
    call_model: klaus_agent_core::types::provider::CallModel,
    config: Option<Value>,
) -> Arc<Agent> {
    make_agent_with_provider_and_config(Arc::new(fake_provider()), Some(call_model), config)
}

fn make_agent_with_provider_and_config(
    provider: Arc<dyn ModelProvider>,
    call_model: Option<klaus_agent_core::types::provider::CallModel>,
    config: Option<Value>,
) -> Arc<Agent> {
    Arc::new(Agent::new(AgentOptions {
        process_context: crate::test_process_context(),
        provider,
        config,
        tools: None,
        web_fetch_page: None,
        browser_runtime: None,
        capability_roots: Vec::new(),
        cwd: Some(".".to_string()),
        deps: EngineDepsOverrides {
            call_model,
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
        hooks: Vec::new(),
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

#[path = "../../tests/r20260905_assistant_message_phase.rs"]
mod r20260905_assistant_message_phase;

#[path = "../../tests/r20260905_pin_during_active_turn.rs"]
mod r20260905_pin_during_active_turn;

#[path = "../../tests/thread_read_facade.rs"]
mod thread_read_facade;

#[path = "../../tests/thread_history_facade.rs"]
mod thread_history_facade;

#[path = "../../tests/r20261003_original_follow.rs"]
mod r20261003_original_follow;

#[path = "../../tests/r20261010_subagent_message_facade.rs"]
mod r20261010_subagent_message_facade;

#[path = "../../tests/re3f8d86_session_summary_facade.rs"]
mod re3f8d86_session_summary_facade;

#[path = "../../tests/r20261018_profile_activity_facade.rs"]
mod r20261018_profile_activity_facade;

#[path = "../../tests/r20260906_goal_input_provenance.rs"]
mod r20260906_goal_input_provenance;

#[path = "../../tests/r20260922_root_agent_definition.rs"]
mod r20260922_root_agent_definition;

#[path = "../../tests/r20260922_readonly_side_question.rs"]
mod r20260922_readonly_side_question;

#[path = "../../tests/r20260907_model_selection_source.rs"]
mod r20260907_model_selection_source;

#[path = "../../tests/r20260908_mcp_control_dispatch.rs"]
mod r20260908_mcp_control_dispatch;

#[path = "../../tests/r20260908_live_side_task_fork.rs"]
mod r20260908_live_side_task_fork;

#[path = "../../tests/r20261004_atomic_side_task_start.rs"]
mod r20261004_atomic_side_task_start;

#[path = "../../tests/r20260908_live_model_reconfigure.rs"]
mod r20260908_live_model_reconfigure;

#[path = "../../tests/r20261013_session_start_init_ack_budget.rs"]
mod r20261013_session_start_init_ack_budget;

#[path = "../../tests/r20260908_permission_rule_prefix_roundtrip.rs"]
mod r20260908_permission_rule_prefix_roundtrip;

#[path = "../../tests/r20260908_live_permission_reload.rs"]
mod r20260908_live_permission_reload;

#[path = "../../tests/r20260908_external_session_import.rs"]
mod r20260908_external_session_import;

#[path = "../../tests/r20261004_async_question_facade.rs"]
mod r20261004_async_question_facade;

#[path = "../../tests/rd958d89_turn_admission_closed_transport.rs"]
mod rd958d89_turn_admission_closed_transport;

#[path = "maintenance_tests.rs"]
mod maintenance;
#[path = "preparation_tests.rs"]
mod preparation;

fn create_session_for_agent_tests(agent: Arc<Agent>) -> CreateSession {
    Arc::new(move |ctx: SessionContext| {
        let agent = agent.clone();
        Box::pin(async move {
            let session = agent.start_session(StartSessionOptions {
                // Mirrors the production host factory: a supplied thread id is
                // adopted as the session's thread identity.
                id: ctx.init.thread_id.clone(),
                durable_permissions: Some(ctx.durable_permissions),
                approval_handler: Some(ctx.approval_handler),
                elicit_handler: Some(ctx.elicit_handler),
                mcp_elicitation_presenter: Some(ctx.mcp_elicitation_presenter),
                permission_mode: Some(ExternalPermissionMode::BypassPermissions),
                ..Default::default()
            });
            Ok::<Arc<Session>, BoxError>(session)
        }) as Pin<Box<dyn Future<Output = Result<Arc<Session>, BoxError>> + Send>>
    })
}

/// A test client speaking JSON-RPC lines over the duplex to the app-server.
struct TestClient {
    writer: tokio::io::WriteHalf<tokio::io::DuplexStream>,
    lines: tokio::io::Lines<BufReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>>,
    server: tokio::task::JoinHandle<()>,
    diagnostic: Option<Arc<r20260908_live_side_task_fork::FixtureDiagnostic>>,
}

#[cfg(unix)]
#[path = "../../../core/tests/regressions/r01b91dae_queue_sqlite_locks.rs"]
mod r01b91dae_queue_sqlite_locks;

#[path = "claude_usage_tests.rs"]
mod claude_usage;

#[path = "../../tests/r20261014_ark_console_usage.rs"]
mod r20261014_ark_console_usage;

// The allowance facade belongs to the server; keep its defect case with the
// regression sources while running it through the facade's public test client.
#[path = "../../../core/tests/regressions/r20261019_qwenwork_usage_refresh.rs"]
mod r20261019_qwenwork_usage_refresh;

#[path = "../../../core/tests/regressions/r20261021_model_catalog_membership.rs"]
mod r20261021_model_catalog_membership;

#[path = "../../../core/tests/regressions/r20261027_trae_max_context.rs"]
mod r20261027_trae_max_context;

#[path = "../../../core/tests/regressions/r01a16397_model_picker_visibility.rs"]
mod r01a16397_model_picker_visibility;

#[path = "../../../core/tests/regressions/r01a16397_conversation_model_purpose.rs"]
mod r01a16397_conversation_model_purpose;

impl TestClient {
    fn start(create_session: CreateSession) -> Self {
        Self::start_with(create_session, None)
    }

    fn start_with(
        create_session: CreateSession,
        lifecycle: Option<klaus_agent_protocol::ProtocolLifecycle>,
    ) -> Self {
        Self::start_full(create_session, lifecycle, None, None)
    }

    /// `permission_rules_path` and `config_path` are explicit here because the
    /// default for both is the real config home: a test that edits rules or
    /// config would otherwise write the developer's own `~/.klaus` files.
    fn start_full(
        create_session: CreateSession,
        lifecycle: Option<klaus_agent_protocol::ProtocolLifecycle>,
        permission_rules_path: Option<std::path::PathBuf>,
        config_path: Option<std::path::PathBuf>,
    ) -> Self {
        let (client_side, server_side) = tokio::io::duplex(1 << 20);
        let (server_read, server_write) = tokio::io::split(server_side);
        let (client_read, client_write) = tokio::io::split(client_side);
        let server = tokio::spawn(run_app_server(
            server_read,
            server_write,
            AppServerOptions {
                windows_sandbox_context: None,
                create_session,
                credential_store: None,
                lifecycle,
                plugin_admin: None,
                permission_rules_path,
                config_path,
                startup_init_ack_budget: None,
            },
        ));
        Self {
            writer: client_write,
            lines: BufReader::new(client_read).lines(),
            server,
            diagnostic: None,
        }
    }

    /// [`start_full`] with an endpoint credential store, for the model-selection
    /// path that reads a BYO key back out of it at session assembly.
    fn start_full_with_credentials(
        create_session: CreateSession,
        config_path: Option<std::path::PathBuf>,
        credential_store: Option<Arc<dyn klaus_agent_core::credential_store::CredentialStore>>,
    ) -> Self {
        let (client_side, server_side) = tokio::io::duplex(1 << 20);
        let (server_read, server_write) = tokio::io::split(server_side);
        let (client_read, client_write) = tokio::io::split(client_side);
        let server = tokio::spawn(run_app_server(
            server_read,
            server_write,
            AppServerOptions {
                windows_sandbox_context: None,
                create_session,
                credential_store,
                lifecycle: None,
                plugin_admin: None,
                permission_rules_path: None,
                config_path,
                startup_init_ack_budget: None,
            },
        ));
        Self {
            writer: client_write,
            lines: BufReader::new(client_read).lines(),
            server,
            diagnostic: None,
        }
    }

    async fn send(&mut self, message: Value) {
        if let Some(diagnostic) = &self.diagnostic {
            diagnostic.rpc("rpc_send", &message);
        }
        let mut line = serde_json::to_vec(&message).unwrap();
        line.push(b'\n');
        self.writer.write_all(&line).await.unwrap();
        self.writer.flush().await.unwrap();
        if let Some(diagnostic) = &self.diagnostic {
            diagnostic.rpc("rpc_flushed", &message);
        }
    }

    async fn next_message(&mut self) -> Value {
        self.next_message_with_timeout(REPLY_TIMEOUT).await
    }

    async fn next_message_with_timeout(&mut self, reply_timeout: Duration) -> Value {
        let pending = tokio::time::timeout(reply_timeout, self.lines.next_line()).await;
        if let (Err(_), Some(diagnostic)) = (&pending, &self.diagnostic) {
            diagnostic.dump("rpc_reply_timeout");
        }
        let line = pending
            .expect("app-server reply within timeout")
            .expect("readable stream")
            .expect("stream still open");
        let message = serde_json::from_str(&line).expect("valid JSON line");
        if let Some(diagnostic) = &self.diagnostic {
            diagnostic.rpc("rpc_receive", &message);
        }
        message
    }

    /// Read messages until the response for `id` arrives; returns it and the
    /// notifications observed on the way.
    async fn response_for(&mut self, id: u64) -> (Value, Vec<Value>) {
        self.response_for_with_timeout(id, REPLY_TIMEOUT).await
    }

    async fn response_for_with_timeout(
        &mut self,
        id: u64,
        reply_timeout: Duration,
    ) -> (Value, Vec<Value>) {
        let mut seen = Vec::new();
        loop {
            let message = self.next_message_with_timeout(reply_timeout).await;
            if message.get("id").and_then(Value::as_u64) == Some(id)
                && message.get("method").is_none()
            {
                return (message, seen);
            }
            seen.push(message);
        }
    }

    async fn initialize(&mut self) {
        self.send(json!({
            "id": 1,
            "method": "initialize",
            "params": { "clientInfo": { "name": "test-client" } }
        }))
        .await;
        let (response, _) = self.response_for(1).await;
        // Bare name, no `/<version>` suffix: the engine is unversioned, so the
        // identity it reports must not carry a number a host could read as a
        // release. Asserting equality (rather than a prefix) is what keeps a
        // version from creeping back in.
        assert_eq!(response["result"]["userAgent"], json!("klaus-app-server"));
        assert_eq!(
            response["result"]["capabilities"]["taskResourceScope"],
            json!(1)
        );
        assert_eq!(
            response["result"]["capabilities"]["composerPredictions"],
            true
        );
        self.send(json!({ "method": "initialized" })).await;
    }

    async fn close(mut self) {
        self.writer
            .shutdown()
            .await
            .expect("the client must close its request direction");
        drop(self.writer);
        drop(self.lines);
        tokio::time::timeout(REPLY_TIMEOUT, self.server)
            .await
            .expect("the endpoint must finish teardown before the client closes")
            .expect("the endpoint task must finish without a panic");
    }
}

#[tokio::test]
async fn rejects_requests_before_initialize_and_double_initialize() {
    let mut client = TestClient::start(make_create_session(&[]));
    client
        .send(json!({ "id": 5, "method": "session/start", "params": { "init": {} } }))
        .await;
    let (response, _) = client.response_for(5).await;
    assert_eq!(response["error"]["message"], "Not initialized");

    client.initialize().await;
    client
        .send(json!({ "id": 6, "method": "initialize", "params": {} }))
        .await;
    let (response, _) = client.response_for(6).await;
    assert_eq!(response["error"]["message"], "Already initialized");
    client.close().await;
}

/// The durable rule store answers and edits WITHOUT a session, and an edit comes
/// back as the store's new content: a host that had to re-read the file (or worse,
/// keep its own copy) is the second owner this move exists to remove.
#[tokio::test]
async fn permission_rules_are_read_and_edited_without_a_session() {
    let dir = std::env::temp_dir().join(format!(
        "klaus-app-server-rules-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("permission-rules.json");

    let mut client =
        TestClient::start_full(make_create_session(&[]), None, Some(path.clone()), None);
    client.initialize().await;

    client
        .send(json!({ "id": 2, "method": "engine/permissionRules", "params": {} }))
        .await;
    let (response, _) = client.response_for(2).await;
    assert_eq!(response["result"]["allow"], json!([]));

    client
        .send(json!({
            "id": 3,
            "method": "engine/addPermissionRules",
            "params": { "rules": [{ "toolName": "Bash", "argumentPrefix": "git status" }] }
        }))
        .await;
    let (response, _) = client.response_for(3).await;
    assert_eq!(response["result"]["allow"], json!(["Bash(git status)"]));

    client
        .send(json!({
            "id": 4,
            "method": "engine/removePermissionRule",
            "params": { "rule": "Bash(git status)" }
        }))
        .await;
    let (response, _) = client.response_for(4).await;
    assert_eq!(response["result"]["allow"], json!([]));

    // The file is the engine's own: what the replies claimed is what is on disk.
    let raw = std::fs::read_to_string(&path).unwrap();
    assert!(raw.contains("\"allow\": []"), "{raw}");

    client.close().await;
    std::fs::remove_dir_all(&dir).ok();
}

/// The config file answers and is edited WITHOUT a session, and the reply says
/// what a session would resolve: a host that had to re-derive that view is the
/// second owner this move exists to remove.
#[tokio::test]
async fn config_is_read_and_written_without_a_session() {
    let dir = std::env::temp_dir().join(format!(
        "klaus-app-server-config-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("config.toml");

    let mut client =
        TestClient::start_full(make_create_session(&[]), None, None, Some(path.clone()));
    client.initialize().await;

    // An absent file reads as no stored values, but the resolved view is already
    // the engine's defaults: "nothing configured" and "every default" are the
    // same state, and a settings page has to render the second one.
    client
        .send(json!({ "id": 2, "method": "config/read", "params": {} }))
        .await;
    let (response, _) = client.response_for(2).await;
    assert_eq!(response["result"]["values"], json!({}));
    assert_eq!(response["result"]["sources"]["workflow"], json!("defaults"));
    assert_eq!(
        response["result"]["path"],
        json!(path.display().to_string())
    );
    let version = response["result"]["version"].as_str().unwrap().to_string();

    client
        .send(json!({
            "id": 3,
            "method": "config/value/write",
            "params": { "keyPath": "maxToolConcurrency", "value": 12, "expectedVersion": version }
        }))
        .await;
    let (response, _) = client.response_for(3).await;
    assert_eq!(
        response["result"]["values"]["maxToolConcurrency"],
        json!(12)
    );
    assert_eq!(
        response["result"]["resolved"]["maxToolConcurrency"],
        json!(12)
    );
    assert_eq!(
        response["result"]["sources"]["maxToolConcurrency"],
        json!("user")
    );

    client
        .send(json!({
            "id": 4,
            "method": "config/batchWrite",
            "params": { "edits": [
                { "keyPath": "webSearch.mode", "value": "client" },
                { "keyPath": "desktop.dockIconPreference", "value": "hidden" },
                { "keyPath": "maxTurns", "value": null }
            ] }
        }))
        .await;
    let (response, _) = client.response_for(4).await;
    assert_eq!(
        response["result"]["values"]["webSearch"]["mode"],
        json!("client")
    );
    assert_eq!(
        response["result"]["values"]["desktop"]["dockIconPreference"],
        json!("hidden")
    );
    assert!(response["result"]["values"].get("maxTurns").is_none());
    // The desktop table is stored but never resolved as engine config.
    assert!(response["result"]["resolved"].get("desktop").is_none());

    // The file is the engine's own: what the replies claimed is what is on disk.
    let raw = std::fs::read_to_string(&path).unwrap();
    assert!(raw.contains("[webSearch]"), "{raw}");
    assert!(raw.contains("[desktop]"), "{raw}");
    assert!(!raw.contains("maxTurns"), "{raw}");

    client.close().await;
    std::fs::remove_dir_all(&dir).ok();
}

/// A BYO `modelSelection` is resolved by the ENGINE at session assembly: it reads
/// the definition from `[modelProviders]` and the API key from the credential
/// store, then binds a provider — the host never sends the key. A successful bind
/// proves resolve_model_provider found the entry and the stored-api-key auth read
/// the key without error, and the hand-written config comment is untouched.
#[tokio::test]
async fn a_model_selection_binds_a_provider_from_config_and_credential_store() {
    use klaus_agent_core::credential_store::{model_credential_key, CredentialStore};
    use klaus_agent_core::mcp::credential_store::InMemoryCredentialStore;

    let dir = std::env::temp_dir().join(format!(
        "klaus-app-server-modelsel-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("config.toml");
    // The definition lives in config (with a hand-written comment); the KEY lives
    // ONLY in the credential store.
    std::fs::write(
        &path,
        "# hand-written\n[modelProviders.m1]\nname = \"My GPT\"\nprovider = \"openai\"\nmodel = \"gpt-4o\"\ndefault = true\n",
    )
    .unwrap();
    let store: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
    store
        .set(&model_credential_key("m1"), "sk-test")
        .await
        .unwrap();

    let agent = make_agent_for_tests(vec![text_turn("ok")]);
    let captured: Arc<std::sync::Mutex<Option<bool>>> = Arc::new(std::sync::Mutex::new(None));
    let create: CreateSession = {
        let captured = captured.clone();
        Arc::new(move |ctx: SessionContext| {
            *captured.lock().unwrap() = Some(ctx.provider.is_some());
            let agent = agent.clone();
            Box::pin(async move {
                let session = agent.start_session(StartSessionOptions {
                    id: ctx.init.thread_id.clone(),
                    durable_permissions: Some(ctx.durable_permissions),
                    approval_handler: Some(ctx.approval_handler),
                    elicit_handler: Some(ctx.elicit_handler),
                    mcp_elicitation_presenter: Some(ctx.mcp_elicitation_presenter),
                    permission_mode: Some(ExternalPermissionMode::BypassPermissions),
                    ..Default::default()
                });
                Ok::<Arc<Session>, BoxError>(session)
            }) as Pin<Box<dyn Future<Output = Result<Arc<Session>, BoxError>> + Send>>
        })
    };

    let mut client =
        TestClient::start_full_with_credentials(create, Some(path.clone()), Some(store));
    client.initialize().await;
    client
        .send(json!({
            "id": 2,
            "method": "session/start",
            "params": { "init": { "modelSelection": "custom:m1" } }
        }))
        .await;
    let (response, _) = client.response_for(2).await;
    assert!(
        response.get("error").is_none(),
        "session must bind: {response}"
    );
    let session_id = response["result"]["sessionId"]
        .as_str()
        .expect("sessionId")
        .to_string();
    // A turn forces the session to bind (create_session runs), so the captured
    // provider reflects what resolve_model_provider + the stored-api-key auth
    // built from the selection.
    client
        .send(json!({
            "id": 3,
            "method": "turn/start",
            "params": { "sessionId": session_id, "input": "hi" }
        }))
        .await;
    let _ = client.response_for(3).await;
    assert_eq!(
        *captured.lock().unwrap(),
        Some(true),
        "the engine built a provider from the selection + stored key"
    );
    // The resolution is read-only over config: the hand-written comment survives.
    let raw = std::fs::read_to_string(&path).unwrap();
    assert!(raw.contains("# hand-written"), "{raw}");

    client.close().await;
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn the_subscription_catalog_exposes_the_current_haiku_with_its_effort_ladder() {
    let mut client = TestClient::start(make_create_session(&[]));
    client.initialize().await;
    client
        .send(json!({ "id": 2, "method": "subscription/models", "params": {} }))
        .await;
    let (response, _) = client.response_for(2).await;
    let models = response["result"]["models"].as_array().expect("models");
    let haiku = models
        .iter()
        .find(|entry| entry["alias"] == "haiku")
        .expect("haiku entry");
    assert_eq!(haiku["model"], json!("claude-haiku-5-5"));
    assert_eq!(haiku["label"], json!("Haiku 5.5"));
    assert_eq!(haiku["tier"], json!("low"));
    assert_eq!(
        haiku["effortLevels"],
        json!(["low", "medium", "high", "xhigh", "max"])
    );
    assert_eq!(haiku["thinking"], json!(true));
    let sonnet = models
        .iter()
        .find(|entry| entry["alias"] == "sonnet")
        .expect("sonnet entry");
    assert_eq!(sonnet["model"], json!("claude-sonnet-5-5"));
    assert_eq!(sonnet["label"], json!("Sonnet 5.5"));
    assert_eq!(sonnet["tier"], json!("medium"));
    assert_eq!(sonnet["effortLevels"], haiku["effortLevels"]);
    client.close().await;
}

/// A subscription selection arrives as a selection and a set of tier PICKS, and
/// the engine answers the model knowledge itself: the session's typed init carries
/// the capability set and the resolved tier map, and neither app-server-only field
/// survives into it. Without this the session would run on the minimal set — which
/// is what refused image input on a subscription while the model could see fine.
#[tokio::test]
async fn a_subscription_selection_lands_capabilities_and_tiers_on_the_typed_init() {
    let dir = std::env::temp_dir().join(format!(
        "klaus-app-server-subcaps-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("config.toml");
    std::fs::write(&path, "# hand-written\n").unwrap();
    // The credential the resolver reads lives beside the config file. Under
    // cfg(test) the keychain leg is off, so this file IS the store.
    std::fs::write(
        dir.join(".credentials.json"),
        json!({
            "claudeAiOauth": {
                "accessToken": "at-sub",
                "refreshToken": "rt-sub",
                "expiresAt": 32_503_680_000_000i64,
            }
        })
        .to_string(),
    )
    .unwrap();

    let agent = make_agent_for_tests(vec![text_turn("ok")]);
    let captured: Arc<std::sync::Mutex<Option<Value>>> = Arc::new(std::sync::Mutex::new(None));
    let create: CreateSession = {
        let captured = captured.clone();
        Arc::new(move |ctx: SessionContext| {
            let tier = |name: &str| {
                ctx.init
                    .config
                    .as_ref()
                    .and_then(|config| config.get("modelTiers"))
                    .and_then(|tiers| tiers.get(name))
                    .and_then(|model| model.as_str())
                    .and_then(|model| model.try_to_utf8().ok())
            };
            *captured.lock().unwrap() = Some(json!({
                "vision": ctx.init.capabilities.as_ref().map(|caps| caps.vision),
                "nativePdf": ctx.init.capabilities.as_ref().map(|caps| caps.native_pdf),
                "low": tier("low"),
                "high": tier("high"),
            }));
            let agent = agent.clone();
            Box::pin(async move {
                let session = agent.start_session(StartSessionOptions {
                    id: ctx.init.thread_id.clone(),
                    durable_permissions: Some(ctx.durable_permissions),
                    approval_handler: Some(ctx.approval_handler),
                    elicit_handler: Some(ctx.elicit_handler),
                    mcp_elicitation_presenter: Some(ctx.mcp_elicitation_presenter),
                    permission_mode: Some(ExternalPermissionMode::BypassPermissions),
                    ..Default::default()
                });
                Ok::<Arc<Session>, BoxError>(session)
            }) as Pin<Box<dyn Future<Output = Result<Arc<Session>, BoxError>> + Send>>
        })
    };

    let store: Arc<dyn klaus_agent_core::credential_store::CredentialStore> =
        Arc::new(klaus_agent_core::mcp::credential_store::InMemoryCredentialStore::new());
    let mut client =
        TestClient::start_full_with_credentials(create, Some(path.clone()), Some(store));
    client.initialize().await;
    client
        .send(json!({
            "id": 2,
            "method": "session/start",
            "params": { "init": {
                "modelSelection": "subscription:opus",
                "modelTierSelections": { "low": "haiku" },
            } }
        }))
        .await;
    let (response, _) = client.response_for(2).await;
    assert!(
        response.get("error").is_none(),
        "the subscription must bind: {response}"
    );
    let session_id = response["result"]["sessionId"]
        .as_str()
        .expect("sessionId")
        .to_string();
    client
        .send(json!({
            "id": 3,
            "method": "turn/start",
            "params": { "sessionId": session_id, "input": "hi" }
        }))
        .await;
    let _ = client.response_for(3).await;

    let init = captured.lock().unwrap().clone().expect("the session bound");
    assert_eq!(init["vision"], json!(true), "{init}");
    assert_eq!(init["nativePdf"], json!(true), "{init}");
    assert_eq!(init["low"], json!("claude-haiku-5-5"), "{init}");
    assert_eq!(init["high"], json!("claude-opus-5-5"), "{init}");

    client.close().await;
    std::fs::remove_dir_all(&dir).ok();
}

/// A rejected write must fail the whole batch and say why in a form the host can
/// branch on — a settings page retries a stale version and shows a bad value.
#[tokio::test]
async fn a_rejected_config_write_reports_its_reason_and_writes_nothing() {
    let dir = std::env::temp_dir().join(format!(
        "klaus-app-server-config-reject-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("config.toml");
    std::fs::write(&path, "# hand written\nmaxTurns = 3\n").unwrap();

    let mut client =
        TestClient::start_full(make_create_session(&[]), None, None, Some(path.clone()));
    client.initialize().await;

    client
        .send(json!({
            "id": 2,
            "method": "config/batchWrite",
            "params": { "edits": [
                { "keyPath": "worktree", "value": false },
                { "keyPath": "maxTurnss", "value": 9 }
            ] }
        }))
        .await;
    let (response, _) = client.response_for(2).await;
    assert_eq!(response["error"]["data"]["reason"], json!("rejected"));
    assert!(
        response["error"]["message"]
            .as_str()
            .unwrap()
            .contains("maxTurnss"),
        "{response}"
    );
    // All or nothing: the valid edit in the same batch did not land either.
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        "# hand written\nmaxTurns = 3\n"
    );

    client
        .send(json!({
            "id": 3,
            "method": "config/value/write",
            "params": { "keyPath": "maxTurns", "value": 4, "expectedVersion": "stale" }
        }))
        .await;
    let (response, _) = client.response_for(3).await;
    assert_eq!(
        response["error"]["data"]["reason"],
        json!("versionConflict")
    );

    client.close().await;
    std::fs::remove_dir_all(&dir).ok();
}

/// `engine/builtinSkills` answers WITHOUT a session: the roster is compiled
/// into the binary, and a host that had to start a session to learn it would
/// hand-copy the list instead (and then drift from it).
#[tokio::test]
async fn builtin_skills_answer_the_compiled_roster_without_a_session() {
    let mut client = TestClient::start(make_create_session(&[]));
    client.initialize().await;
    client
        .send(json!({ "id": 2, "method": "engine/builtinSkills", "params": {} }))
        .await;
    let (response, _) = client.response_for(2).await;
    let rows = response["result"]["builtinSkills"]
        .as_array()
        .expect("builtinSkills is an array")
        .clone();
    let expected = klaus_agent_core::extensions::skills::builtin_skills::builtin_skills();
    assert_eq!(
        rows.len(),
        expected.len(),
        "the reply must mirror builtin_skills(), not a restated list"
    );
    for (row, skill) in rows.iter().zip(expected.iter()) {
        assert_eq!(row["name"], json!(skill.name.try_to_utf8().unwrap()));
        assert_eq!(
            row["description"],
            json!(skill.description.try_to_utf8().unwrap())
        );
        assert_eq!(row["userInvocable"], json!(true));
    }
    client.close().await;
}

/// `engine/builtinAgents` answers WITHOUT a session, mirroring the skill
/// roster: each built-in role's declared model tier and effort come from the
/// compiled `BUILTIN_AGENTS`, so a host listing agents shows the engine's own
/// routing instead of a hand-copied table that drifts from it.
#[tokio::test]
async fn builtin_agents_answer_the_compiled_roster_without_a_session() {
    let mut client = TestClient::start(make_create_session(&[]));
    client.initialize().await;
    client
        .send(json!({ "id": 2, "method": "engine/builtinAgents", "params": {} }))
        .await;
    let (response, _) = client.response_for(2).await;
    let rows = response["result"]["builtinAgents"]
        .as_array()
        .expect("builtinAgents is an array")
        .clone();
    let expected = klaus_agent_core::engine::BUILTIN_AGENTS.clone();
    assert_eq!(
        rows.len(),
        expected.len(),
        "the reply must mirror BUILTIN_AGENTS, not a restated list"
    );
    for (row, agent) in rows.iter().zip(expected.iter()) {
        assert_eq!(row["name"], json!(agent.name.try_to_utf8().unwrap()));
        assert_eq!(
            row["description"],
            json!(agent.description.try_to_utf8().unwrap())
        );
        match agent.model.as_ref() {
            Some(model) => assert_eq!(row["model"], json!(model.try_to_utf8().unwrap())),
            // A role that declares no tier leaves the field out entirely: the
            // delegating model picks the tier per task for those.
            None => assert!(
                row.as_object().unwrap().get("model").is_none(),
                "an undeclared model must not surface as a chip value"
            ),
        }
        match agent.effort.as_ref() {
            Some(klaus_agent_core::schemas::effort::EffortValue::Level(level)) => {
                assert_eq!(row["effort"], json!(level.as_str()))
            }
            Some(klaus_agent_core::schemas::effort::EffortValue::Number(number)) => {
                assert_eq!(row["effort"], json!(number.to_string()))
            }
            Some(klaus_agent_core::schemas::effort::EffortValue::Custom(value)) => {
                assert_eq!(row["effort"], json!(value.try_to_utf8().unwrap()))
            }
            None => assert!(row.as_object().unwrap().get("effort").is_none()),
        }
    }
    // Roles retain either inherited selection or their explicitly declared tier.
    let tier_of = |name: &str| {
        rows.iter()
            .find(|row| row["name"] == json!(name))
            .and_then(|row| row.get("model"))
            .and_then(|model| model.as_str())
            .map(str::to_string)
    };
    assert_eq!(tier_of("Explore"), None);
    assert_eq!(tier_of("Plan").as_deref(), Some("high"));
    assert_eq!(tier_of("researcher").as_deref(), Some("medium"));
    assert_eq!(tier_of("general-purpose"), None);
    client.close().await;
}

/// The session handle IS the thread id. Three legs of that one contract: the id
/// the host names in the init comes back verbatim, a second start on a live
/// thread is refused instead of silently taking over its map slot, and a request
/// that names two different threads (resume vs init) is refused rather than
/// resolved one way. Before this the handle was a per-connection counter, which
/// is why every host carried a handle→thread translation table.
#[tokio::test]
async fn the_session_handle_is_the_thread_id() {
    let mut client = TestClient::start(make_create_session(&["ok"]));
    client.initialize().await;

    client
        .send(json!({
            "id": 2,
            "method": "session/start",
            "params": { "init": { "threadId": "host-named-thread" } }
        }))
        .await;
    let (response, _) = client.response_for(2).await;
    assert_eq!(
        response["result"]["sessionId"],
        json!("host-named-thread"),
        "the handle is the thread the host named, {response}"
    );

    client
        .send(json!({
            "id": 3,
            "method": "session/start",
            "params": { "init": { "threadId": "host-named-thread" } }
        }))
        .await;
    let (response, _) = client.response_for(3).await;
    assert_eq!(
        response["error"]["code"],
        json!(super::wire::SESSION_ALREADY_LIVE),
        "a live thread cannot be started twice, {response}"
    );

    client
        .send(json!({
            "id": 4,
            "method": "session/start",
            "params": { "init": { "threadId": "one" }, "resume": "another" }
        }))
        .await;
    let (response, _) = client.response_for(4).await;
    assert_eq!(
        response["error"]["code"],
        json!(super::wire::INVALID_PARAMS),
        "one session is one thread, {response}"
    );

    // The first session is untouched by either refusal.
    client
        .send(json!({
            "id": 5,
            "method": "turn/start",
            "params": { "sessionId": "host-named-thread", "input": "hi" }
        }))
        .await;
    let (response, _) = client.response_for(5).await;
    assert!(
        response["result"]["turnId"].is_string(),
        "the live session still answers, {response}"
    );

    client.close().await;
}

#[tokio::test]
async fn full_turn_round_trip_streams_events_and_completes() {
    let mut client = TestClient::start(make_create_session(&["hello from engine"]));
    client.initialize().await;

    client
        .send(json!({ "id": 2, "method": "session/start", "params": { "init": {} } }))
        .await;
    let (response, _) = client.response_for(2).await;
    let session_id = response["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    // An init that named no thread gets a minted uuid, not a connection counter:
    // the handle IS the thread id.
    assert_eq!(session_id.len(), 36, "a uuid handle, got {session_id}");
    assert_eq!(session_id.matches('-').count(), 4, "{session_id}");

    client
        .send(json!({
            "id": 3,
            "method": "turn/start",
            "params": { "sessionId": session_id, "input": "hi" }
        }))
        .await;
    let (response, _) = client.response_for(3).await;
    let turn_id = response["result"]["turnId"].as_str().unwrap().to_string();

    // Drain notifications until turn/completed; collect streamed text deltas.
    let mut text = String::new();
    let completed = loop {
        let message = client.next_message().await;
        match message.get("method").and_then(Value::as_str) {
            Some("session/event") => {
                let event = &message["params"]["event"];
                assert_eq!(message["params"]["sessionId"], json!(session_id));
                if event["event"]["type"] == json!("text.delta") {
                    text.push_str(event["event"]["text"].as_str().unwrap_or(""));
                }
            }
            Some("turn/completed") => break message,
            _ => {}
        }
    };
    assert_eq!(completed["params"]["turnId"], json!(turn_id));
    assert_eq!(completed["params"]["sessionId"], json!(session_id));
    assert!(
        text.contains("hello from engine"),
        "expected streamed text, got: {text:?}"
    );

    // session/close tears the session down and acks.
    client
        .send(json!({
            "id": 4,
            "method": "session/close",
            "params": { "sessionId": session_id }
        }))
        .await;
    let (response, _) = client.response_for(4).await;
    assert!(response.get("result").is_some());
    client.close().await;
}

#[tokio::test]
async fn session_unsubscribe_gates_the_event_stream_without_touching_replies() {
    let mut client = TestClient::start(make_create_session(&["gated"]));
    client.initialize().await;
    client
        .send(json!({ "id": 2, "method": "session/start", "params": { "init": {} } }))
        .await;
    let (response, _) = client.response_for(2).await;
    let session_id = response["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();

    client
        .send(json!({
            "id": 3,
            "method": "session/unsubscribe",
            "params": { "sessionId": session_id }
        }))
        .await;
    let (response, _) = client.response_for(3).await;
    assert_eq!(response["result"]["subscribed"], json!(false));
    assert_eq!(response["result"]["sessionId"], json!(session_id));

    // Requests still answer while gated: only the event stream is suppressed.
    client
        .send(json!({
            "id": 4,
            "method": "session/history",
            "params": { "sessionId": session_id }
        }))
        .await;
    let (response, seen) = client.response_for(4).await;
    assert!(response["result"].get("history").is_some());
    for message in &seen {
        let method = message.get("method").and_then(Value::as_str);
        assert!(
            method != Some("session/event") && method != Some("session/rawEvent"),
            "a gated session must not stream events: {message}"
        );
    }

    // Re-arming restores the stream.
    client
        .send(json!({
            "id": 5,
            "method": "session/subscribe",
            "params": { "sessionId": session_id }
        }))
        .await;
    let (response, _) = client.response_for(5).await;
    assert_eq!(response["result"]["subscribed"], json!(true));

    // Unknown sessions are rejected with the shared code.
    client
        .send(json!({
            "id": 6,
            "method": "session/unsubscribe",
            "params": { "sessionId": "sess_404" }
        }))
        .await;
    let (response, _) = client.response_for(6).await;
    assert_eq!(
        response["error"]["code"],
        json!(super::wire::UNKNOWN_SESSION)
    );
    client.close().await;
}

#[tokio::test]
async fn session_follow_atomically_snapshots_and_replays_revisioned_events() {
    let mut client = TestClient::start(make_create_session(&["followed"]));
    client
        .send(json!({
            "id": 1,
            "method": "initialize",
            "params": {
                "clientInfo": { "name": "follow-test" },
                "capabilities": { "revisionedSessionEvents": true }
            }
        }))
        .await;
    let (initialized, _) = client.response_for(1).await;
    assert_eq!(
        initialized["result"]["userAgent"],
        json!("klaus-app-server")
    );
    client.send(json!({ "method": "initialized" })).await;
    client
        .send(json!({ "id": 2, "method": "session/start", "params": { "init": {} } }))
        .await;
    let (started, _) = client.response_for(2).await;
    let session_id = started["result"]["sessionId"].as_str().unwrap().to_string();

    client
        .send(json!({
            "id": 3,
            "method": "session/follow",
            "params": { "sessionId": session_id }
        }))
        .await;
    let (snapshot, _) = client.response_for(3).await;
    let stream_id = snapshot["result"]["streamId"].as_str().unwrap().to_string();
    assert_eq!(snapshot["result"]["mode"], json!("snapshot"));
    assert_eq!(snapshot["result"]["revision"], json!(0));
    assert!(snapshot["result"]["snapshot"]["history"].is_array());

    client
        .send(json!({
            "id": 4,
            "method": "turn/start",
            "params": { "sessionId": session_id, "input": "hi" }
        }))
        .await;
    let (_, _) = client.response_for(4).await;
    let mut observed = Vec::new();
    loop {
        let message = client.next_message().await;
        if message.get("method").and_then(Value::as_str) == Some("session/event") {
            assert_eq!(message["params"]["streamId"], json!(stream_id));
            observed.push(message["params"]["streamRevision"].as_u64().unwrap());
        }
        if message.get("method").and_then(Value::as_str) == Some("turn/completed") {
            assert_eq!(message["params"]["streamId"], json!(stream_id));
            break;
        }
    }
    assert!(!observed.is_empty());
    let latest = observed.iter().copied().max().unwrap();

    client
        .send(json!({
            "id": 5,
            "method": "session/follow",
            "params": {
                "sessionId": session_id,
                "streamId": stream_id,
                "afterRevision": 0
            }
        }))
        .await;
    let (replay, _) = client.response_for(5).await;
    assert_eq!(replay["result"]["mode"], json!("replay"));
    assert!(replay["result"]["revision"].as_u64().unwrap() >= latest);
    let revisions = replay["result"]["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|event| event["revision"].as_u64().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(revisions.first().copied(), Some(1));
    assert!(revisions.windows(2).all(|pair| pair[1] == pair[0] + 1));

    // A cursor from another stream epoch cannot be trusted. The facade returns
    // a new atomic snapshot instead of pretending no events were missed.
    client
        .send(json!({
            "id": 6,
            "method": "session/follow",
            "params": {
                "sessionId": session_id,
                "streamId": "stale-stream-epoch",
                "afterRevision": latest
            }
        }))
        .await;
    let (replacement, _) = client.response_for(6).await;
    assert_eq!(replacement["result"]["mode"], json!("snapshot"));
    assert_eq!(replacement["result"]["streamId"], json!(stream_id));
    assert!(replacement["result"]["snapshot"]["history"].is_array());

    // A half cursor is ambiguous and rejected rather than guessed.
    client
        .send(json!({
            "id": 7,
            "method": "session/follow",
            "params": { "sessionId": session_id, "streamId": stream_id }
        }))
        .await;
    let (invalid, _) = client.response_for(7).await;
    assert_eq!(invalid["error"]["code"], json!(super::wire::INVALID_PARAMS));
    client.close().await;
}

#[tokio::test]
async fn session_start_adopts_a_host_supplied_thread_id() {
    let mut client = TestClient::start(make_create_session(&["adopted"]));
    client.initialize().await;
    client
        .send(json!({
            "id": 2,
            "method": "session/start",
            "params": { "init": { "threadId": "host-owned-thread" } }
        }))
        .await;
    let (response, _) = client.response_for(2).await;
    assert!(
        response.get("error").is_none(),
        "a host-supplied threadId must be accepted: {response}"
    );
    let session_id = response["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();

    // The engine adopted the id as the thread identity: the first turn's
    // thread.started reports it verbatim instead of a minted one.
    client
        .send(json!({
            "id": 3,
            "method": "turn/start",
            "params": { "sessionId": session_id, "input": "hi" }
        }))
        .await;
    let (_, _) = client.response_for(3).await;
    let mut adopted = None;
    for _ in 0..40 {
        let message = client.next_message().await;
        let thread_id = match message.get("method").and_then(Value::as_str) {
            Some("thread/started") => message["params"]["threadId"].as_str().map(str::to_string),
            Some("session/event") => message["params"]["event"]["event"]
                .get("threadId")
                .and_then(Value::as_str)
                .map(str::to_string),
            _ => None,
        };
        if let Some(thread_id) = thread_id {
            adopted = Some(thread_id);
            break;
        }
    }
    assert_eq!(adopted.as_deref(), Some("host-owned-thread"));
    client.close().await;
}

#[tokio::test]
async fn acked_method_forwards_op_with_ack() {
    let mut client = TestClient::start(make_create_session(&["one"]));
    client.initialize().await;
    client
        .send(json!({ "id": 2, "method": "session/start", "params": { "init": {} } }))
        .await;
    let (response, _) = client.response_for(2).await;
    let session_id = response["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();

    // Ack-promoted write op: SubmissionAck first, reply envelope streams later
    // on session/event with the server-assigned submission id.
    client
        .send(json!({
            "id": 3,
            "method": "session/updateTitle",
            "params": { "sessionId": session_id, "threadId": "thread_404", "title": "x" }
        }))
        .await;
    let (response, _) = client.response_for(3).await;
    let submission_id = response["result"]["submissionId"]
        .as_str()
        .unwrap()
        .to_string();

    loop {
        let message = client.next_message().await;
        if message.get("method").and_then(Value::as_str) == Some("session/event") {
            let event = &message["params"]["event"];
            if event["submissionId"] == json!(submission_id) {
                assert!(
                    event.get("event").is_none(),
                    "expected a terminal reply envelope, got a stream event: {event}"
                );
                break;
            }
        }
    }
    client.close().await;
}

#[tokio::test]
async fn unknown_session_and_unknown_method_error() {
    let mut client = TestClient::start(make_create_session(&[]));
    client.initialize().await;

    client
        .send(json!({
            "id": 2,
            "method": "turn/start",
            "params": { "sessionId": "sess_404", "input": "hi" }
        }))
        .await;
    let (response, _) = client.response_for(2).await;
    assert_eq!(
        response["error"]["code"],
        json!(super::wire::UNKNOWN_SESSION)
    );

    client
        .send(json!({ "id": 3, "method": "no/such/method", "params": {} }))
        .await;
    let (response, _) = client.response_for(3).await;
    assert_eq!(
        response["error"]["code"],
        json!(super::wire::METHOD_NOT_FOUND)
    );
    client.close().await;
}

#[tokio::test]
async fn notification_opt_out_suppresses_session_event_but_not_turn_completed() {
    let mut client = TestClient::start(make_create_session(&["quiet"]));
    client
        .send(json!({
            "id": 1,
            "method": "initialize",
            "params": {
                "clientInfo": { "name": "test-client" },
                "capabilities": { "optOutNotificationMethods": ["session/event", "no/such/notify"] }
            }
        }))
        .await;
    let (response, _) = client.response_for(1).await;
    assert!(response.get("result").is_some());

    client
        .send(json!({ "id": 2, "method": "session/start", "params": { "init": {} } }))
        .await;
    let (response, _) = client.response_for(2).await;
    let session_id = response["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();

    client
        .send(json!({
            "id": 3,
            "method": "turn/start",
            "params": { "sessionId": session_id, "input": "hi" }
        }))
        .await;
    let (_, _) = client.response_for(3).await;

    // Only turn/completed may arrive; any session/event proves the opt-out leaked.
    loop {
        let message = client.next_message().await;
        match message.get("method").and_then(Value::as_str) {
            Some("session/event") => panic!("session/event must be suppressed: {message}"),
            Some("turn/completed") => break,
            _ => {}
        }
    }
    client.close().await;
}

#[tokio::test]
async fn acked_method_honors_client_submission_id_and_rejects_empty() {
    let mut client = TestClient::start(make_create_session(&["one"]));
    client.initialize().await;
    client
        .send(json!({ "id": 2, "method": "session/start", "params": { "init": {} } }))
        .await;
    let (response, _) = client.response_for(2).await;
    let session_id = response["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();

    client
        .send(json!({
            "id": 3,
            "method": "session/updateTitle",
            "params": {
                "sessionId": session_id,
                "threadId": "thread_404",
                "title": "x",
                "submissionId": "shim-h1"
            }
        }))
        .await;
    let (response, _) = client.response_for(3).await;
    assert_eq!(response["result"]["submissionId"], json!("shim-h1"));
    loop {
        let message = client.next_message().await;
        if message.get("method").and_then(Value::as_str) == Some("session/event") {
            let event = &message["params"]["event"];
            if event["submissionId"] == json!("shim-h1") {
                break;
            }
        }
    }

    client
        .send(json!({
            "id": 4,
            "method": "turn/interrupt",
            "params": { "sessionId": session_id, "submissionId": "" }
        }))
        .await;
    let (response, _) = client.response_for(4).await;
    assert_eq!(
        response["error"]["code"],
        json!(super::wire::INVALID_PARAMS)
    );

    // Interaction methods are bound-only: omitting sessionId is a params
    // error, never a silent fall-through to the control session.
    client
        .send(json!({
            "id": 5,
            "method": "session/steer",
            "params": { "input": "hi" }
        }))
        .await;
    let (response, _) = client.response_for(5).await;
    assert_eq!(
        response["error"]["code"],
        json!(super::wire::INVALID_PARAMS)
    );
    client.close().await;
}

#[tokio::test]
async fn overloaded_session_start_is_rejected_with_retryable_error() {
    // A session factory that parks until released, holding heavy slots open.
    let gate = Arc::new(tokio::sync::Notify::new());
    let release = gate.clone();
    let inner = make_create_session(&[]);
    let blocked: CreateSession = Arc::new(move |ctx: SessionContext| {
        let gate = gate.clone();
        let inner = inner.clone();
        Box::pin(async move {
            gate.notified().await;
            inner(ctx).await
        }) as Pin<Box<dyn Future<Output = Result<Arc<Session>, BoxError>> + Send>>
    });

    let mut client = TestClient::start(blocked);
    client.initialize().await;
    for i in 0..super::service::MAX_INFLIGHT_HEAVY_REQUESTS {
        client
            .send(json!({
                "id": 100 + i,
                "method": "session/start",
                "params": { "init": {} }
            }))
            .await;
    }
    client
        .send(json!({ "id": 999, "method": "session/start", "params": { "init": {} } }))
        .await;
    let (response, _) = client.response_for(999).await;
    assert_eq!(response["error"]["code"], json!(super::wire::OVERLOADED));
    assert_eq!(
        response["error"]["message"],
        json!("Server overloaded; retry later.")
    );

    // Release the parked creations so teardown does not hang.
    release.notify_waiters();
    client.close().await;
}

#[tokio::test]
async fn product_events_capability_lifts_thread_turn_item_notifications() {
    let mut client = TestClient::start(make_create_session(&["product surface"]));
    client
        .send(json!({
            "id": 1,
            "method": "initialize",
            "params": {
                "capabilities": {
                    "productEvents": true,
                    "optOutNotificationMethods": ["session/event"]
                }
            }
        }))
        .await;
    let (response, _) = client.response_for(1).await;
    assert!(response.get("result").is_some());

    client
        .send(json!({ "id": 2, "method": "session/start", "params": { "init": {} } }))
        .await;
    let (response, _) = client.response_for(2).await;
    let session_id = response["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();

    client
        .send(json!({
            "id": 3,
            "method": "turn/start",
            "params": { "sessionId": session_id, "input": "hi" }
        }))
        .await;
    let (_, _) = client.response_for(3).await;

    let mut saw = std::collections::HashSet::new();
    let mut delta = String::new();
    loop {
        let message = client.next_message().await;
        let Some(method) = message.get("method").and_then(Value::as_str) else {
            continue;
        };
        assert_ne!(method, "session/event", "session/event was opted out");
        saw.insert(method.to_string());
        if method == "item/agentMessage/delta" {
            delta.push_str(message["params"]["delta"].as_str().unwrap_or(""));
            assert_eq!(message["params"]["sessionId"], json!(session_id));
        }
        if method == "turn/completed" {
            break;
        }
    }
    for expected in ["thread/started", "turn/started", "item/agentMessage/delta"] {
        assert!(
            saw.contains(expected),
            "missing product notification {expected}; saw {saw:?}"
        );
    }
    assert!(delta.contains("product surface"));
    client.close().await;
}

#[tokio::test]
async fn promoted_lifecycle_methods_reach_wired_lifecycle() {
    let store_dir = std::env::temp_dir().join(format!(
        "klaus-app-server-lifecycle-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&store_dir).expect("temp session dir");
    let store: Arc<dyn klaus_agent_core::store::session_store::SessionStore> = Arc::new(
        klaus_agent_core::store::jsonl_store::JsonlSessionStore::new(
            klaus_agent_core::store::jsonl_store::JsonlSessionStoreOptions {
                dir: Some(store_dir.to_string_lossy().into_owned()),
                archive_dir: None,
            },
        ),
    );
    let turns: ScriptedTurns = vec![text_turn("lifecycle")];
    let agent = make_agent_for_tests(turns);
    let lifecycle = crate::lifecycle::protocol_lifecycle(agent.clone(), Some(store));
    let mut client = TestClient::start_with(create_session_for_agent_tests(agent), Some(lifecycle));
    client.initialize().await;
    client
        .send(json!({ "id": 2, "method": "session/start", "params": { "init": {} } }))
        .await;
    let (response, _) = client.response_for(2).await;
    let session_id = response["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();

    // A promoted ack lifecycle op must reach the wired lifecycle instead of
    // failing with "requires a session store", exactly like native stdio.
    client
        .send(json!({
            "id": 3,
            "method": "session/updateTitle",
            "params": {
                "sessionId": session_id,
                "threadId": "thread_404",
                "title": "renamed",
                "submissionId": "lc-1"
            }
        }))
        .await;
    let (response, _) = client.response_for(3).await;
    assert_eq!(response["result"]["submissionId"], json!("lc-1"));
    loop {
        let message = client.next_message().await;
        if message.get("method").and_then(Value::as_str) == Some("session/event") {
            let event = &message["params"]["event"];
            if event["submissionId"] == json!("lc-1") {
                let error = event.get("error").and_then(Value::as_str).unwrap_or("");
                assert!(
                    !error.contains("requires a session store"),
                    "lifecycle must be reached when wired: {event}"
                );
                break;
            }
        }
    }

    // Endpoint-level lifecycle reachability WITHOUT a bound session rides the
    // formal engine/listSessions request.
    client
        .send(json!({
            "id": 4,
            "method": "engine/listSessions",
            "params": { "options": { "limit": 5 } }
        }))
        .await;
    let (response, _) = client.response_for(4).await;
    assert!(
        response["result"].get("sessions").is_some(),
        "expected sessions payload: {response}"
    );

    // Endpoint scope: a promoted session method WITHOUT sessionId is routed to
    // the control session instead of failing at the RPC layer — the exact shape
    // the desktop host uses before any session is bound. A per-session getter
    // then answers with the control channel's own refusal INSIDE the result,
    // which is the reachability proof: the request got there.
    client
        .send(json!({
            "id": 5,
            "method": "session/history",
            "params": {}
        }))
        .await;
    let (response, _) = client.response_for(5).await;
    assert!(
        response.get("error").is_none(),
        "endpoint-scoped promoted getter must not fail at the RPC layer: {response}"
    );
    assert!(
        response["result"].is_object(),
        "the control session's answer must ride the result: {response}"
    );
    client.close().await;
}

#[tokio::test]
async fn a_committed_delete_broadcasts_thread_deleted() {
    let store_dir = std::env::temp_dir().join(format!(
        "klaus-app-server-delete-notify-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&store_dir).expect("temp session dir");
    let store: Arc<dyn klaus_agent_core::store::session_store::SessionStore> = Arc::new(
        klaus_agent_core::store::jsonl_store::JsonlSessionStore::new(
            klaus_agent_core::store::jsonl_store::JsonlSessionStoreOptions {
                dir: Some(store_dir.to_string_lossy().into_owned()),
                archive_dir: None,
            },
        ),
    );
    let agent = make_agent_for_tests(vec![text_turn("delete notify")]);
    let lifecycle = crate::lifecycle::protocol_lifecycle(agent.clone(), Some(store.clone()));
    let mut client = TestClient::start_full(
        create_session_for_agent_tests(agent),
        Some(lifecycle),
        None,
        Some(store_dir.join("config.json")),
    );
    client.initialize().await;

    // Persist a thread, then delete it through the endpoint-scoped method.
    let live = store
        .create(klaus_agent_core::schemas::rollout::SessionMeta {
            id: "thread-delete-notify".into(),
            created_at: 1_700_000_000,
            cwd: "/tmp".into(),
            model: "m".into(),
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
        .expect("persist thread");
    live.shutdown().await.expect("close writer");

    client
        .send(json!({
            "id": 2,
            "method": "session/deleteSession",
            "params": { "threadId": "thread-delete-notify", "submissionId": "del-1" }
        }))
        .await;
    let (response, _) = client.response_for(2).await;
    assert_eq!(response["result"]["submissionId"], json!("del-1"));

    // The commit is announced to the whole connection, not just carried inside
    // the requester's own reply envelope.
    let mut announced = None;
    for _ in 0..40 {
        let message = client.next_message().await;
        if message.get("method").and_then(Value::as_str) == Some("thread/deleted") {
            announced = message["params"]["threadId"].as_str().map(str::to_string);
            break;
        }
    }
    assert_eq!(announced.as_deref(), Some("thread-delete-notify"));
    client.close().await;
    std::fs::remove_dir_all(&store_dir).ok();
}

#[tokio::test]
async fn product_events_reroutes_unprojected_envelopes_to_raw_event() {
    // With productEvents on and session/event opted out, escape-hatch op
    // replies must still arrive — on session/rawEvent — while projected kinds
    // must NEVER be duplicated there.
    let mut client = TestClient::start(make_create_session(&["raw split"]));
    client
        .send(json!({
            "id": 1,
            "method": "initialize",
            "params": {
                "capabilities": {
                    "productEvents": true,
                    "optOutNotificationMethods": ["session/event"]
                }
            }
        }))
        .await;
    let (response, _) = client.response_for(1).await;
    assert!(response.get("result").is_some());

    client
        .send(json!({ "id": 2, "method": "session/start", "params": { "init": {} } }))
        .await;
    let (response, _) = client.response_for(2).await;
    let session_id = response["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();

    // Ack-promoted write op: the reply envelope must arrive via
    // session/rawEvent with the client-supplied submission id intact.
    client
        .send(json!({
            "id": 3,
            "method": "session/updateTitle",
            "params": {
                "sessionId": session_id,
                "threadId": "thread_404",
                "title": "x",
                "submissionId": "shim-raw-1"
            }
        }))
        .await;
    let (response, _) = client.response_for(3).await;
    assert_eq!(response["result"]["submissionId"], json!("shim-raw-1"));
    loop {
        let message = client.next_message().await;
        match message.get("method").and_then(Value::as_str) {
            Some("session/event") => panic!("session/event must be suppressed: {message}"),
            Some("session/rawEvent") => {
                let event = &message["params"]["event"];
                assert_eq!(message["params"]["sessionId"], json!(session_id));
                if event["submissionId"] == json!("shim-raw-1") {
                    break;
                }
            }
            _ => {}
        }
    }

    // A full turn: projected kinds must arrive as product notifications ONLY;
    // any session/rawEvent seen along the way must carry unprojected kinds.
    client
        .send(json!({
            "id": 4,
            "method": "turn/start",
            "params": { "sessionId": session_id, "input": "hi" }
        }))
        .await;
    let (_, _) = client.response_for(4).await;

    let projected = [
        "thread.started",
        "turn.started",
        "turn.failed",
        "item.started",
        "item.updated",
        "item.completed",
        "text.delta",
        "thinking.delta",
    ];
    let mut saw_product_delta = false;
    loop {
        let message = client.next_message().await;
        match message.get("method").and_then(Value::as_str) {
            Some("session/event") => panic!("session/event must be suppressed: {message}"),
            Some("session/rawEvent") => {
                let kind = message["params"]["event"]["event"]["type"].clone();
                if let Some(kind) = kind.as_str() {
                    assert!(
                        !projected.contains(&kind),
                        "projected kind {kind} leaked onto session/rawEvent"
                    );
                }
            }
            Some("item/agentMessage/delta") => saw_product_delta = true,
            Some("turn/completed") => break,
            _ => {}
        }
    }
    assert!(
        saw_product_delta,
        "expected item/agentMessage/delta product notification"
    );
    client.close().await;
}

#[test]
fn rejected_turn_input_emits_terminal_product_failure() {
    std::thread::Builder::new()
        .name("rejected-turn-input-test".into())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime")
                .block_on(async {
                    let mut client = TestClient::start(make_create_session(&[]));
                    client
                        .send(json!({
                            "id": 1,
                            "method": "initialize",
                            "params": {
                                "capabilities": {
                                    "productEvents": true,
                                    "optOutNotificationMethods": ["session/event"]
                                }
                            }
                        }))
                        .await;
                    let (response, _) = client.response_for(1).await;
                    assert!(response.get("result").is_some());
                    client.send(json!({ "method": "initialized" })).await;

                    client
                        .send(
                            json!({ "id": 2, "method": "session/start", "params": { "init": {} } }),
                        )
                        .await;
                    let (response, _) = client.response_for(2).await;
                    let session_id = response["result"]["sessionId"]
                        .as_str()
                        .expect("session id")
                        .to_string();

                    client
                        .send(json!({
                            "id": 3,
                            "method": "turn/start",
                            "params": {
                                "sessionId": session_id,
                                "input": [{
                                    "type": "document",
                                    "base64": "%%%not-base64%%%",
                                    "mimeType": "application/pdf",
                                    "filename": "bad.pdf"
                                }]
                            }
                        }))
                        .await;
                    let (response, mut seen) = client.response_for(3).await;
                    assert!(response["result"]["turnId"].is_string());

                    let failure = loop {
                        if let Some(message) = seen
                            .iter()
                            .find(|message| {
                                message.get("method").and_then(Value::as_str) == Some("turn/failed")
                            })
                            .cloned()
                        {
                            break message;
                        }
                        seen.push(client.next_message().await);
                    };
                    assert_eq!(failure["params"]["sessionId"], json!(session_id));
                    assert_eq!(failure["params"]["detail"]["type"], json!("turn.failed"));
                    assert_eq!(
                        failure["params"]["detail"]["error"]["code"],
                        json!("invalid_input")
                    );
                    assert!(failure["params"]["detail"]["error"]["message"]
                        .as_str()
                        .is_some_and(|message| message.contains("valid standard base64")));
                    client.close().await;
                });
        })
        .expect("test thread starts")
        .join()
        .expect("test thread completes");
}

#[tokio::test]
async fn promoted_session_request_returns_reply_as_result() {
    let mut client = TestClient::start(make_create_session(&["promoted"]));
    client.initialize().await;
    client
        .send(json!({ "id": 2, "method": "session/start", "params": { "init": {} } }))
        .await;
    let (response, _) = client.response_for(2).await;
    let session_id = response["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();

    // The promoted getter answers in the JSON-RPC response itself — no ack,
    // no reply envelope on the event stream.
    client
        .send(json!({
            "id": 3,
            "method": "session/history",
            "params": { "sessionId": session_id }
        }))
        .await;
    let (response, seen) = client.response_for(3).await;
    assert!(
        response["result"].get("history").is_some(),
        "expected history payload in the result: {response}"
    );
    assert!(
        response["result"].get("submissionId").is_none(),
        "submissionId must be stripped from the result"
    );
    for message in &seen {
        if message.get("method").and_then(Value::as_str) == Some("session/event") {
            assert!(
                message["params"]["event"].get("history").is_none(),
                "promoted reply must not also stream on session/event: {message}"
            );
        }
    }

    // Parameterized promoted getter: taskList with inner options.
    client
        .send(json!({
            "id": 4,
            "method": "session/taskList",
            "params": { "sessionId": session_id, "options": { "includeCompleted": true } }
        }))
        .await;
    let (response, _) = client.response_for(4).await;
    assert!(
        response.get("error").is_none(),
        "session/taskList must not error: {response}"
    );

    // Background processes use the same named direct-result facade. These empty
    // and missing-process responses pin the exact envelopes the desktop unwraps;
    // the core registry tests cover live spawn/output/stop state transitions.
    client
        .send(json!({
            "id": 41,
            "method": "session/backgroundProcesses",
            "params": { "sessionId": session_id }
        }))
        .await;
    let (response, _) = client.response_for(41).await;
    assert_eq!(
        response["result"]["backgroundProcesses"]["processes"],
        json!([])
    );

    client
        .send(json!({
            "id": 42,
            "method": "session/backgroundProcessOutput",
            "params": { "sessionId": session_id, "shellId": "missing", "afterOffset": 7 }
        }))
        .await;
    let (response, _) = client.response_for(42).await;
    assert_eq!(
        response["result"]["backgroundProcessOutput"]["found"],
        json!(false)
    );
    assert_eq!(
        response["result"]["backgroundProcessOutput"]["nextOffset"],
        json!(7)
    );

    client
        .send(json!({
            "id": 43,
            "method": "session/stopBackgroundProcess",
            "params": { "sessionId": session_id, "shellId": "missing" }
        }))
        .await;
    let (response, _) = client.response_for(43).await;
    assert_eq!(
        response["result"]["backgroundProcessStop"]["found"],
        json!(false)
    );
    assert_eq!(
        response["result"]["backgroundProcessStop"]["accepted"],
        json!(false)
    );

    // Unknown session fails with the shared UNKNOWN_SESSION code.
    client
        .send(json!({
            "id": 5,
            "method": "session/history",
            "params": { "sessionId": "sess_404" }
        }))
        .await;
    let (response, _) = client.response_for(5).await;
    assert_eq!(
        response["error"]["code"],
        json!(super::wire::UNKNOWN_SESSION)
    );
    client.close().await;
}

#[tokio::test]
async fn turn_start_honors_client_submission_id_and_rejects_collisions() {
    let mut client = TestClient::start(make_create_session(&["correlated"]));
    client.initialize().await;
    client
        .send(json!({ "id": 2, "method": "session/start", "params": { "init": {} } }))
        .await;
    let (response, _) = client.response_for(2).await;
    let session_id = response["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();

    client
        .send(json!({
            "id": 3,
            "method": "turn/start",
            "params": { "sessionId": session_id, "input": "hi", "submissionId": "shim-run-1" }
        }))
        .await;
    let (response, _) = client.response_for(3).await;
    assert_eq!(response["result"]["turnId"], json!("shim-run-1"));

    // While the turn is in flight, the same id collides on any promoted
    // method that carries a client-supplied submission id.
    client
        .send(json!({
            "id": 4,
            "method": "turn/interrupt",
            "params": {
                "sessionId": session_id,
                "submissionId": "shim-run-1"
            }
        }))
        .await;
    let (response, _) = client.response_for(4).await;
    assert_eq!(
        response["error"]["code"],
        json!(super::wire::DUPLICATE_SUBMISSION)
    );

    // The tracked turn still terminates with turn/completed on the client id.
    loop {
        let message = client.next_message().await;
        if message.get("method").and_then(Value::as_str) == Some("turn/completed") {
            assert_eq!(message["params"]["turnId"], json!("shim-run-1"));
            break;
        }
    }

    // Empty submission id is rejected.
    client
        .send(json!({
            "id": 5,
            "method": "turn/start",
            "params": { "sessionId": session_id, "input": "hi", "submissionId": "" }
        }))
        .await;
    let (response, _) = client.response_for(5).await;
    assert_eq!(
        response["error"]["code"],
        json!(super::wire::INVALID_PARAMS)
    );
    client.close().await;
}

#[tokio::test]
async fn engine_list_sessions_promoted_request_returns_sessions() {
    let store_dir = std::env::temp_dir().join(format!(
        "klaus-app-server-promoted-lifecycle-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&store_dir).expect("temp session dir");
    let store: Arc<dyn klaus_agent_core::store::session_store::SessionStore> = Arc::new(
        klaus_agent_core::store::jsonl_store::JsonlSessionStore::new(
            klaus_agent_core::store::jsonl_store::JsonlSessionStoreOptions {
                dir: Some(store_dir.to_string_lossy().into_owned()),
                archive_dir: None,
            },
        ),
    );
    let turns: ScriptedTurns = vec![text_turn("lifecycle")];
    let agent = make_agent_for_tests(turns);
    let lifecycle = crate::lifecycle::protocol_lifecycle(agent.clone(), Some(store));
    let mut client = TestClient::start_with(create_session_for_agent_tests(agent), Some(lifecycle));
    client.initialize().await;

    // No bound session required: the promoted endpoint-level request answers
    // directly, and the reply must NOT leak onto engine/event.
    client
        .send(json!({
            "id": 2,
            "method": "engine/listSessions",
            "params": { "options": { "limit": 5 } }
        }))
        .await;
    let (response, seen) = client.response_for(2).await;
    assert!(
        response["result"].get("sessions").is_some(),
        "expected sessions payload in the result: {response}"
    );
    for message in &seen {
        if message.get("method").and_then(Value::as_str) == Some("engine/event") {
            assert!(
                message["params"]["event"].get("sessions").is_none(),
                "promoted reply must not also stream on engine/event: {message}"
            );
        }
    }
    client.close().await;
}

#[tokio::test]
async fn session_shutdown_promoted_request_returns_completion_and_unregisters() {
    let mut client = TestClient::start(make_create_session(&["bye"]));
    client.initialize().await;
    client
        .send(json!({ "id": 2, "method": "session/start", "params": { "init": {} } }))
        .await;
    let (response, _) = client.response_for(2).await;
    let session_id = response["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();

    client
        .send(json!({
            "id": 3,
            "method": "session/shutdown",
            "params": { "sessionId": session_id }
        }))
        .await;
    let (response, _) = client.response_for(3).await;
    assert!(
        response.get("error").is_none(),
        "session/shutdown must not error: {response}"
    );
    assert!(
        response["result"].get("shutdownCompleted").is_some(),
        "expected the native shutdownCompleted payload: {response}"
    );

    // The session is gone afterwards.
    client
        .send(json!({
            "id": 4,
            "method": "session/history",
            "params": { "sessionId": session_id }
        }))
        .await;
    let (response, _) = client.response_for(4).await;
    assert_eq!(
        response["error"]["code"],
        json!(super::wire::UNKNOWN_SESSION)
    );
    client.close().await;
}

#[tokio::test]
async fn side_task_promoted_requests_answer_directly() {
    let store_dir = std::env::temp_dir().join(format!(
        "klaus-app-server-side-task-promoted-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&store_dir).expect("temp session dir");
    let store: Arc<dyn klaus_agent_core::store::session_store::SessionStore> = Arc::new(
        klaus_agent_core::store::jsonl_store::JsonlSessionStore::new(
            klaus_agent_core::store::jsonl_store::JsonlSessionStoreOptions {
                dir: Some(store_dir.to_string_lossy().into_owned()),
                archive_dir: None,
            },
        ),
    );
    let turns: ScriptedTurns = vec![text_turn("side")];
    let agent = make_agent_for_tests(turns);
    let lifecycle = crate::lifecycle::protocol_lifecycle(agent.clone(), Some(store));
    let mut client = TestClient::start_with(create_session_for_agent_tests(agent), Some(lifecycle));
    client.initialize().await;
    client
        .send(json!({ "id": 2, "method": "session/start", "params": { "init": {} } }))
        .await;
    let (response, _) = client.response_for(2).await;
    let session_id = response["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();

    // The promoted side-task request answers in the JSON-RPC response itself and
    // must not also stream on session/event.
    client
        .send(json!({
            "id": 3,
            "method": "session/createSideTask",
            "params": { "sessionId": session_id, "threadId": "thread_side" }
        }))
        .await;
    let (response, seen) = client.response_for(3).await;
    assert!(
        response.get("error").is_none(),
        "session/createSideTask must not error: {response}"
    );
    assert!(
        response["result"].get("submissionId").is_none(),
        "submissionId must be stripped from the result"
    );
    for message in &seen {
        if message.get("method").and_then(Value::as_str) == Some("session/event") {
            assert!(
                message["params"]["event"].get("threadId").is_none(),
                "promoted reply must not also stream on session/event: {message}"
            );
        }
    }

    // Unknown session fails with the shared UNKNOWN_SESSION code.
    client
        .send(json!({
            "id": 6,
            "method": "session/createSideTask",
            "params": { "sessionId": "sess_404", "threadId": "thread_side" }
        }))
        .await;
    let (response, _) = client.response_for(6).await;
    assert_eq!(
        response["error"]["code"],
        json!(super::wire::UNKNOWN_SESSION)
    );
    client.close().await;
}

/// The published bundle describes the whole declared surface, and only it.
///
/// This used to be a hand-written list of method names, which is exactly the
/// failure the declaration table was introduced to end: the list was a fourth
/// place that had to agree with the other three by hand, and by the time anyone
/// looked it had silently fallen 17 methods behind. Walking
/// [`super::methods::ALL_METHODS`] instead means a new row is published and
/// asserted by the act of declaring it.
#[test]
fn advisor_target_schema_extends_existing_session_methods() {
    let bundle = schema_bundle();
    for method in ["session/start", "session/reconfigureModel"] {
        let params = &bundle["requests"][method]["params"];
        let init = &params["properties"]["init"];
        let init = match init["$ref"].as_str() {
            Some(reference) => params
                .pointer(reference.strip_prefix('#').unwrap())
                .unwrap(),
            None => init,
        };
        let advisor = &init["properties"]["advisorTarget"];
        assert!(advisor.is_object(), "{method}: {params}");
        assert!(advisor.to_string().contains("ModelTierTarget"), "{advisor}");
        assert!(advisor.to_string().contains("null"), "{advisor}");
        assert!(!init["required"]
            .as_array()
            .is_some_and(|fields| { fields.iter().any(|field| field == "advisorTarget") }));
        let target = &params["definitions"]["ModelTierTarget"];
        for kind in ["source", "resolved", "unavailable"] {
            assert!(target.to_string().contains(kind), "{target}");
        }
    }
    assert!(
        bundle["requests"]["session/modelRuntime"]["result"]["properties"]
            .get("advisorTarget")
            .is_some()
    );
    assert!(!super::methods::ALL_METHODS
        .iter()
        .any(|method| method.name.to_ascii_lowercase().contains("advisor")));
}

#[test]
fn schema_bundle_covers_the_api_surface() {
    let bundle = schema_bundle();
    let published = bundle["requests"]
        .as_object()
        .expect("the bundle publishes a requests object");
    for spec in super::methods::ALL_METHODS {
        let entry = published
            .get(spec.name)
            .unwrap_or_else(|| panic!("missing request schema: {}", spec.name));
        assert!(
            entry.get("params").is_some() && entry.get("result").is_some(),
            "{} is published without both halves of its contract",
            spec.name
        );
    }
    // And nothing beyond it: an entry the table does not declare could not be
    // called, so publishing one would advertise a method that does not exist.
    let declared: std::collections::BTreeSet<&str> = super::methods::ALL_METHODS
        .iter()
        .map(|spec| spec.name)
        .collect();
    let undeclared: Vec<&String> = published
        .keys()
        .filter(|name| !declared.contains(name.as_str()))
        .collect();
    assert!(
        undeclared.is_empty(),
        "the bundle publishes request schemas for methods nothing declares: {undeclared:?}"
    );

    assert!(bundle["serverRequests"].get("approval/request").is_some());
    assert!(bundle["serverRequests"]
        .get("elicitation/request")
        .is_some());
    let elicitation = &bundle["serverRequests"]["elicitation/request"];
    assert_eq!(
        elicitation["params"]["properties"]["elicitation"]["properties"]["request"]["properties"]
            ["attachmentsSupported"]["type"],
        json!("boolean")
    );
    let attachments = &elicitation["result"]["properties"]["response"]["oneOf"][0]["properties"]
        ["attachments"]["additionalProperties"];
    assert_eq!(attachments["maxItems"], json!(20));
    assert_eq!(
        attachments["items"]["properties"]["type"]["const"],
        json!("image")
    );
    assert_eq!(
        attachments["items"]["properties"]["base64"]["maxLength"],
        json!(13981016)
    );
    assert!(bundle["notifications"].get("session/event").is_some());
    assert!(bundle["notifications"].get("session/rawEvent").is_some());
    assert!(bundle["notifications"].get("taskList/changed").is_some());
    assert!(bundle["notifications"]
        .get("backgroundProcess/changed")
        .is_some());
    assert_eq!(
        bundle["notifications"]["taskList/changed"]["requiresCapability"],
        json!("productEvents")
    );
    assert_eq!(
        bundle["notifications"]["backgroundProcess/changed"]["requiresCapability"],
        json!("productEvents")
    );
    assert!(bundle["requests"]
        .get("session/backgroundProcesses")
        .is_some());
    assert!(bundle["requests"]
        .get("session/backgroundProcessOutput")
        .is_some());
    assert!(bundle["requests"]
        .get("session/stopBackgroundProcess")
        .is_some());
    assert_eq!(
        bundle["notifications"]["session/rawEvent"]["requiresCapability"],
        json!("productEvents")
    );
    assert!(bundle["notifications"].get("turn/completed").is_some());
    assert!(bundle["requests"].get("session/follow").is_some());
    assert!(bundle["requests"]["session/follow"]["result"]["properties"]["streamId"].is_object());
    assert!(bundle["requests"]["session/follow"]["result"]["properties"]["revision"].is_object());
    for method in [
        "session/event",
        "session/rawEvent",
        "taskList/changed",
        "backgroundProcess/changed",
        "turn/completed",
        "thread/started",
        "turn/started",
        "turn/failed",
        "item/started",
        "item/updated",
        "item/completed",
        "item/agentMessage/delta",
        "item/reasoning/delta",
    ] {
        let properties = &bundle["notifications"][method]["params"]["properties"];
        assert!(
            properties["streamId"].is_object() && properties["streamRevision"].is_object(),
            "{method} must publish its optional revision cursor"
        );
    }
    for method in [
        "session/beginModelUpdate",
        "session/failModelUpdate",
        "session/reconfigureModel",
    ] {
        let request = &bundle["requests"][method];
        assert!(request["params"]["properties"]["changeId"].is_object());
        assert!(request["result"]["properties"]["revision"].is_object());
        assert!(request["result"]["properties"]["changeId"].is_object());
    }
    assert_eq!(
        bundle["requests"]["session/failModelUpdate"]["params"]["properties"]["error"]["type"],
        json!("string")
    );
    for field in [
        "changeId",
        "pendingChangeId",
        "modelUpdateError",
        "maxOutputRetryTokens",
    ] {
        assert!(
            bundle["requests"]["session/modelRuntime"]["result"]["properties"][field].is_object()
        );
    }
    for method in [
        "item/started",
        "item/updated",
        "item/completed",
        "item/agentMessage/delta",
        "item/reasoning/delta",
    ] {
        let params = &bundle["notifications"][method]["params"];
        assert!(params["properties"]["inference"].is_object());
        assert_eq!(
            params["definitions"]["PublicInference"]["properties"]["revision"]["type"],
            json!("integer")
        );
    }
    let skills = &bundle["requests"]["session/setSkills"];
    assert_eq!(skills["params"]["properties"]["skills"]["type"], "array");
    assert_eq!(
        skills["params"]["properties"]["sessionId"]["type"],
        "string"
    );
    assert_eq!(
        skills["result"]["properties"]["submissionId"]["type"],
        "string"
    );
    // Params schemas must be objects with camelCase properties.
    assert!(bundle["requests"]["turn/start"]["params"]["properties"]["sessionId"].is_object());
}

/// Every declared method has a dispatch arm.
///
/// The gap this closes: declaring a method makes it publishable and CALLABLE —
/// `request::<M>` compiles the moment the row exists — but not answerable. A row
/// with no arm in [`handle_request`] type-checks on both sides and then returns
/// `METHOD_NOT_FOUND` at runtime, which is the one failure a typed surface is
/// supposed to make impossible.
///
/// Read off the dispatch source rather than probed over a connection, and that is
/// a deliberate trade. Probing means sending each method for real, and several of
/// them are endpoint-level writes: `hooks/write` with empty params would write an
/// empty hooks file, and its target is the developer's own config home, not
/// something `AppServerOptions` can redirect. A test that edits the machine it
/// runs on to prove a match arm exists is not worth the extra fidelity.
#[test]
fn every_declared_method_is_dispatched() {
    let dispatch = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/app_server/service.rs"),
    )
    .expect("the dispatch source is readable");
    let missing: Vec<&str> = super::methods::ALL_METHODS
        .iter()
        .map(|spec| spec.name)
        .filter(|name| {
            // `=>` is the common case; `|` catches the arms that share a body,
            // such as `"session/subscribe" | "session/unsubscribe"`.
            !dispatch
                .match_indices(&format!("\"{name}\""))
                .any(|(index, literal)| {
                    let following = dispatch[index + literal.len()..].trim_start();
                    following.starts_with("=>") || following.starts_with('|')
                })
        })
        .collect();
    assert!(
        missing.is_empty(),
        "these methods are declared and publishable but fall through to METHOD_NOT_FOUND, so a \
         client that calls them compiles and then fails: {missing:?}"
    );
}

#[path = "../../tests/r20260905_structured_user_questions.rs"]
mod r20260905_structured_user_questions;

#[path = "../../tests/r20260913_pending_resume.rs"]
mod r20260913_pending_resume;

/// Defect 20261003: steering raced a stopped/completed turn and left an orphan waiter.
/// Pin the public facade's identity fence, durable queue ID and explicit continuation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn followup_turn_fence_preserves_stopped_queue_and_explicit_resume_once() {
    use klaus_agent_core::schemas::message::Message;
    use klaus_agent_core::types::abort::AbortSignal;
    use klaus_agent_core::types::provider::{ModelRequest, ModelStream};
    use std::sync::atomic::{AtomicUsize, Ordering};

    let started = Arc::new(tokio::sync::Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let agent = make_agent_with_call_model_and_config(
        {
            let started = started.clone();
            let calls = calls.clone();
            Box::new(move |_: ModelRequest, signal: AbortSignal| -> ModelStream {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    started.notify_one();
                    Box::pin(futures::stream::once(async move {
                        signal.cancelled().await;
                        ev::stop(Some(ModelStopReason::EndTurn), None)
                    }))
                } else {
                    Box::pin(futures::stream::iter(text_turn("continued")))
                }
            })
        },
        Some(json!({"prewarm":false,"goal":false,"autoMemory":false,"autoDream":false})),
    );
    let live = Arc::new(std::sync::Mutex::new(None::<Arc<Session>>));
    let create = create_session_for_agent_tests(agent);
    let factory: CreateSession = {
        let live = live.clone();
        Arc::new(move |ctx| {
            let create = create.clone();
            let live = live.clone();
            Box::pin(async move {
                let session = create(ctx).await?;
                *live.lock().unwrap() = Some(session.clone());
                Ok(session)
            })
        })
    };
    let mut client = TestClient::start(factory);
    client.initialize().await;
    client
        .send(json!({"id":2,"method":"session/start","params":{"init":{}}}))
        .await;
    let (response, _) = client.response_for(2).await;
    let session_id = response["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    client
        .send(json!({"id":3,"method":"turn/start","params":{
            "sessionId":session_id,"input":"block until stopped","submissionId":"foreground-one"
        }}))
        .await;
    let (response, _) = client.response_for(3).await;
    assert_eq!(response["result"]["turnId"], "foreground-one");
    tokio::time::timeout(REPLY_TIMEOUT, started.notified())
        .await
        .unwrap();

    let queue_id = "c9a02ab1-092b-4206-8b14-bbb7d37d5f20";
    client
        .send(json!({"id":4,"method":"session/steer","params":{
            "sessionId":session_id,"input":"wrong turn","expectedTurnId":"another-turn",
            "queueMessageId":"674b3696-1006-4354-a6c1-582939fbaaa4","submissionId":"wrong-steer"
        }}))
        .await;
    let (_, seen) = client.response_for(4).await;
    let mut seen = std::collections::VecDeque::from(seen);
    loop {
        let message = match seen.pop_front() {
            Some(message) => message,
            None => client.next_message().await,
        };
        if message["params"]["event"]["submissionId"] == "wrong-steer" {
            assert!(message["params"]["event"]["error"]
                .as_str()
                .unwrap()
                .contains("steer turn mismatch"));
            break;
        }
    }
    client
        .send(json!({"id":5,"method":"session/queueSnapshot","params":{"sessionId":session_id}}))
        .await;
    let (response, _) = client.response_for(5).await;
    assert!(response["result"]["queueSnapshot"]["items"]
        .as_array()
        .unwrap()
        .is_empty());

    client
        .send(json!({"id":6,"method":"session/steer","params":{
            "sessionId":session_id,"input":"keep me after stop","expectedTurnId":"foreground-one",
            "queueMessageId":queue_id,"delivery":"user","submissionId":"valid-steer",
            "displayMetadata":{"displayText":"Inspect Orders","browserTabs":[{"tabId":42,"title":"Orders"}]}
        }}))
        .await;
    let (response, _) = client.response_for(6).await;
    assert_eq!(response["result"]["submissionId"], "valid-steer");
    client
        .send(json!({"id":7,"method":"session/queueSnapshot","params":{"sessionId":session_id}}))
        .await;
    let (response, _) = client.response_for(7).await;
    assert_eq!(
        response["result"]["queueSnapshot"]["items"][0]["message"]["uuid"],
        queue_id
    );
    assert_eq!(
        response["result"]["queueSnapshot"]["items"][0]["message"]["displayMetadata"],
        json!({"displayText":"Inspect Orders","browserTabs":[{"tabId":42,"title":"Orders"}]})
    );

    client
        .send(json!({"id":8,"method":"turn/interrupt","params":{"sessionId":session_id}}))
        .await;
    let (_, seen) = client.response_for(8).await;
    let mut seen = std::collections::VecDeque::from(seen);
    loop {
        let message = match seen.pop_front() {
            Some(message) => message,
            None => client.next_message().await,
        };
        if message["method"] == "turn/completed" && message["params"]["turnId"] == "foreground-one"
        {
            break;
        }
    }
    client
        .send(json!({"id":9,"method":"session/steer","params":{
            "sessionId":session_id,"input":"too late","expectedTurnId":"foreground-one",
            "queueMessageId":"674b3696-1006-4354-a6c1-582939fbaaa4","submissionId":"late-steer"
        }}))
        .await;
    let (_, seen) = client.response_for(9).await;
    let mut seen = std::collections::VecDeque::from(seen);
    loop {
        let message = match seen.pop_front() {
            Some(message) => message,
            None => client.next_message().await,
        };
        if message["params"]["event"]["submissionId"] == "late-steer" {
            assert!(message["params"]["event"]["error"]
                .as_str()
                .unwrap()
                .contains("no longer active"));
            break;
        }
    }
    client.send(json!({"id":10,"method":"session/resumePending","params":{
        "sessionId":session_id,"afterSubmissionId":"foreground-one","submissionId":"automatic-after-stop"
    }})).await;
    let (_, seen) = client.response_for(10).await;
    let mut seen = std::collections::VecDeque::from(seen);
    loop {
        let message = match seen.pop_front() {
            Some(message) => message,
            None => client.next_message().await,
        };
        if message["params"]["event"]["submissionId"] == "automatic-after-stop" {
            assert_eq!(
                message["params"]["event"]["pendingResume"],
                json!({"started":false,"reason":"not_completed"})
            );
            break;
        }
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    client
        .send(json!({"id":11,"method":"session/resumeQueued","params":{
            "sessionId":session_id,"submissionId":"explicit-resume"
        }}))
        .await;
    let (response, seen) = client.response_for(11).await;
    assert_eq!(response["result"]["turnId"], "explicit-resume");
    let mut seen = std::collections::VecDeque::from(seen);
    let mut started_resume = false;
    let mut applied = false;
    loop {
        let message = match seen.pop_front() {
            Some(message) => message,
            None => client.next_message().await,
        };
        let event = &message["params"]["event"];
        if event["submissionId"] == "explicit-resume" && event["pendingResume"]["started"] == true {
            started_resume = true;
        }
        if event["submissionId"] == "valid-steer" && event["steered"]["applied"] == true {
            applied = true;
        }
        if message["method"] == "turn/completed" && message["params"]["turnId"] == "explicit-resume"
        {
            break;
        }
    }
    assert!(started_resume && applied);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    client
        .send(json!({"id":12,"method":"session/resumeQueued","params":{
            "sessionId":session_id,"submissionId":"explicit-resume-again"
        }}))
        .await;
    let (_, seen) = client.response_for(12).await;
    let mut seen = std::collections::VecDeque::from(seen);
    loop {
        let message = match seen.pop_front() {
            Some(message) => message,
            None => client.next_message().await,
        };
        if message["params"]["event"]["submissionId"] == "explicit-resume-again" {
            assert_eq!(
                message["params"]["event"]["pendingResume"],
                json!({"started":false,"reason":"empty"})
            );
            break;
        }
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let session = live.lock().unwrap().clone().unwrap();
    assert!(session.queue_snapshot().items.is_empty());
    assert_eq!(
        session
            .history()
            .iter()
            .filter(|message| matches!(message, Message::User(user) if user.uuid == queue_id))
            .count(),
        1
    );
    let history = session.history();
    let display = history
        .iter()
        .find_map(|message| match message {
            Message::System(system)
                if system.subtype
                    == klaus_agent_core::schemas::message::SystemSubtype::UserDisplay =>
            {
                Some(klaus_agent_core::runtime_values::runtime_json::parse(&system.text).unwrap())
            }
            _ => None,
        })
        .expect("resuming a stopped human steer preserves the composer rendering metadata");
    assert_eq!(
        display.get("messageUuid"),
        Some(&klaus_agent_core::runtime_values::RuntimeJson::String(
            queue_id.into()
        ))
    );
    assert_eq!(
        display.get("localRender").unwrap().get("displayText"),
        Some(&klaus_agent_core::runtime_values::RuntimeJson::String(
            "Inspect Orders".into()
        ))
    );
    client.close().await;
}

#[path = "../../tests/r20261011_lifecycle_row_replay.rs"]
mod r20261011_lifecycle_row_replay;

#[tokio::test]
async fn live_skill_publication_answers_after_application_and_rejects_duplicate_names() {
    let agent = make_agent_for_tests(vec![text_turn("done")]);
    let factory = crate::lifecycle::engine_sessions(agent.clone(), None).create_session;
    let loaded = Arc::new(std::sync::Mutex::new(None::<Arc<Session>>));
    let capture = loaded.clone();
    let create: CreateSession = Arc::new(move |context| {
        let factory = factory.clone();
        let capture = capture.clone();
        Box::pin(async move {
            let session = factory(context).await?;
            *capture.lock().unwrap() = Some(session.clone());
            Ok(session)
        })
    });
    let mut client = TestClient::start_with(
        create,
        Some(crate::lifecycle::protocol_lifecycle(agent, None)),
    );
    client.initialize().await;
    client
        .send(json!({ "id": 2, "method": "session/start", "params": { "init": {} } }))
        .await;
    let (started, _) = client.response_for(2).await;
    let session_id = started["result"]["sessionId"].as_str().unwrap();
    let record = json!({ "name": "live-draft", "description": "Draft text", "body": "Draft instructions", "path": "/skills/live-draft/SKILL.md" });
    client
        .send(json!({ "id": 3, "method": "session/setSkills", "params": {
        "sessionId": session_id, "submissionId": "skills-added", "skills": [record.clone()]
    } }))
        .await;
    let (ack, mut seen) = client.response_for(3).await;
    assert_eq!(ack["result"]["submissionId"], "skills-added");
    while !seen.iter().any(|message| {
        message["params"]["event"]["submissionId"] == "skills-added"
            && message["params"]["event"]["skillsUpdated"] == true
    }) {
        seen.push(client.next_message().await);
    }
    assert!(loaded
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .visible_skill_names()
        .iter()
        .any(|name| name == "live-draft"));
    client.send(json!({ "id": 4, "method": "session/setSkills", "params": {
        "sessionId": session_id, "submissionId": "skills-invalid", "skills": [record.clone(), record]
    } })).await;
    let (_, mut seen) = client.response_for(4).await;
    while !seen.iter().any(|message| {
        message["params"]["event"]["submissionId"] == "skills-invalid"
            && message["params"]["event"]["error"].is_string()
    }) {
        seen.push(client.next_message().await);
    }
    assert!(loaded
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .visible_skill_names()
        .iter()
        .any(|name| name == "live-draft"));
    client
        .send(json!({ "id": 5, "method": "session/setSkills", "params": {
        "sessionId": session_id, "submissionId": "skills-removed", "skills": []
    } }))
        .await;
    let (_, mut seen) = client.response_for(5).await;
    while !seen.iter().any(|message| {
        message["params"]["event"]["submissionId"] == "skills-removed"
            && message["params"]["event"]["skillsUpdated"] == true
    }) {
        seen.push(client.next_message().await);
    }
    assert!(!loaded
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .visible_skill_names()
        .iter()
        .any(|name| name == "live-draft"));
    client.close().await;
}

#[path = "prediction_tests.rs"]
mod prediction_tests;

#[path = "../../tests/r20261016_cursor_source.rs"]
mod r20261016_cursor_source;

#[path = "../../tests/r20261016_realtime_facade.rs"]
mod r20261016_realtime_facade;

#[path = "../../tests/r20261017_planning_facade.rs"]
mod r20261017_planning_facade;

#[path = "../../tests/r20261018_exact_planning_wire.rs"]
mod r20261018_exact_planning_wire;

#[path = "../../tests/r20261017_qwenwork_capabilities.rs"]
mod r20261017_qwenwork_capabilities;

#[tokio::test]
async fn lazy_runtime_policy_ack_reaches_the_first_materialized_turn() {
    use klaus_agent_core::planning::CollaborationMode;
    use klaus_agent_core::runtime_values::{runtime_codec, RuntimePath};
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    for (label, requested_mode, pre_plan_mode) in [
        ("ordinary", ExternalPermissionMode::AcceptEdits, None),
        (
            "legacy-plan",
            ExternalPermissionMode::Plan,
            Some(ExternalPermissionMode::DontAsk),
        ),
    ] {
        let root = tempfile::tempdir().unwrap();
        let initial_config = json!({
            "prewarm": false,
            "goal": false,
            "autoMemory": false,
            "autoDream": false,
            "memoryRecall": false,
            "memory": {"enabled": false},
            "sandbox_mode": "workspace-write",
            "sandbox_workspace_write": {"network_access": true}
        });
        let factory_calls = Arc::new(AtomicUsize::new(0));
        let provider_calls = Arc::new(AtomicUsize::new(0));
        let created = Arc::new(Mutex::new(None::<Arc<Session>>));
        let raw_init = Arc::new(Mutex::new(None));
        let observed_policy = Arc::new(Mutex::new(None));
        let provider_session = created.clone();
        let provider_count = provider_calls.clone();
        let provider_observation = observed_policy.clone();
        let agent = make_agent_with_call_model_and_config(
            Box::new(move |_, _| {
                assert_eq!(provider_count.fetch_add(1, Ordering::SeqCst), 0);
                let session = provider_session.lock().unwrap().clone().unwrap();
                *provider_observation.lock().unwrap() =
                    Some((session.runtime_policy_state(), session.collaboration_mode()));
                Box::pin(futures::stream::iter(text_turn("policy observed")))
            }),
            Some(initial_config.clone()),
        );
        let real_factory = crate::lifecycle::engine_sessions_in_test_root(
            agent,
            None,
            RuntimePath::from(root.path().to_string_lossy().into_owned()),
        )
        .create_session;
        let factory_count = factory_calls.clone();
        let factory_session = created.clone();
        let factory_init = raw_init.clone();
        let create: CreateSession = Arc::new(move |ctx| {
            let real_factory = real_factory.clone();
            let factory_count = factory_count.clone();
            let factory_session = factory_session.clone();
            let factory_init = factory_init.clone();
            Box::pin(async move {
                assert_eq!(factory_count.fetch_add(1, Ordering::SeqCst), 0);
                *factory_init.lock().unwrap() = Some((
                    ctx.init.permission_mode,
                    ctx.init.pre_plan_permission_mode,
                    ctx.init.config.clone(),
                    ctx.init.network_allowed,
                ));
                let session = real_factory(ctx).await?;
                *factory_session.lock().unwrap() = Some(session.clone());
                Ok::<_, BoxError>(session)
            })
        });
        let mut client = TestClient::start_full(
            create,
            None,
            Some(root.path().join("rules.json")),
            Some(root.path().join("config.toml")),
        );
        client.initialize().await;
        let session_id = format!("lazy-policy-{label}");
        client
            .send(
                json!({"id": 2, "method": "session/start", "params": {"init": {
                    "threadId": session_id,
                    "cwd": root.path(),
                    "permissionMode": "bypassPermissions",
                    "collaborationMode": "plan",
                    "config": initial_config
                }}}),
            )
            .await;
        let (start, _) = client.response_for(2).await;
        assert!(start.get("error").is_none(), "{start}");
        assert_eq!(start["result"]["sessionId"], session_id);
        assert_eq!(factory_calls.load(Ordering::SeqCst), 0);
        assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
        assert!(raw_init.lock().unwrap().is_none());

        let mut policy = json!({
            "sessionId": session_id,
            "submissionId": "policy-before-first-turn",
            "permissionMode": requested_mode,
            "sandboxMode": "read-only",
            "networkAllowed": true,
            "sandboxWorkspaceWrite": {
                "writable_roots": [], "network_access": false,
                "exclude_tmpdir_env_var": true, "exclude_slash_tmp": true
            }
        });
        if let Some(mode) = pre_plan_mode {
            policy["prePlanPermissionMode"] = json!(mode);
        }
        client
            .send(json!({"id": 3, "method": "session/setRuntimePolicy", "params": policy}))
            .await;
        let (ack, _) = client.response_for(3).await;
        assert!(ack.get("error").is_none(), "{ack}");
        assert_eq!(ack["result"]["submissionId"], "policy-before-first-turn");
        assert_eq!(factory_calls.load(Ordering::SeqCst), 0);
        assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
        assert!(raw_init.lock().unwrap().is_none());

        client
            .send(json!({"id": 4, "method": "turn/start", "params": {
                "sessionId": session_id,
                "submissionId": "first-policy-turn",
                "input": "Observe the admitted runtime policy."
            }}))
            .await;
        let (turn, seen) = client.response_for(4).await;
        assert!(turn.get("error").is_none(), "{turn}");
        assert_eq!(turn["result"]["turnId"], "first-policy-turn");
        let mut pending: VecDeque<_> = seen.into();
        let completed = tokio::time::timeout(REPLY_TIMEOUT, async {
            loop {
                let message = match pending.pop_front() {
                    Some(message) => message,
                    None => client.next_message().await,
                };
                assert!(
                    message["method"] != "turn/failed",
                    "the first policy turn must succeed: {message}"
                );
                if message["method"] == "session/event" {
                    assert!(
                        message["params"]["event"]["error"].is_null(),
                        "unexpected protocol error: {message}"
                    );
                }
                if message["method"] == "turn/completed"
                    && message["params"]["turnId"] == "first-policy-turn"
                {
                    break message;
                }
            }
        })
        .await
        .expect("the first policy turn completes within the existing reply deadline");
        assert_eq!(completed["params"]["sessionId"], session_id);
        assert_eq!(factory_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider_calls.load(Ordering::SeqCst), 1);

        let (raw_mode, raw_pre_plan, raw_config, raw_network) =
            raw_init.lock().unwrap().clone().unwrap();
        assert_eq!(
            raw_mode,
            (requested_mode == ExternalPermissionMode::Plan)
                .then_some(ExternalPermissionMode::Plan)
        );
        assert!(raw_pre_plan.is_none());
        let raw_config: Value = serde_json::from_str(
            &runtime_codec::encode_text(&raw_config.unwrap())
                .unwrap()
                .try_to_utf8()
                .unwrap(),
        )
        .unwrap();
        let mut expected_config = initial_config;
        expected_config["sandbox_mode"] = json!("read-only");
        expected_config["sandbox_workspace_write"] = json!({
            "writable_roots": [], "network_access": false,
            "exclude_tmpdir_env_var": true, "exclude_slash_tmp": true
        });
        assert_eq!(raw_network, Some(true));
        assert_eq!(raw_config, expected_config);
        assert!(raw_config.get("networkAllowed").is_none());
        let session = created.lock().unwrap().clone().unwrap();
        let policy = session.runtime_policy_state();
        assert_eq!(policy.generation, 0);
        assert_eq!(
            policy.sandbox_mode,
            Some(klaus_agent_core::types::permission::SandboxMode::ReadOnly)
        );
        assert!(
            policy.network_allowed,
            "explicit read-only network selection survives materialization"
        );
        assert_eq!(
            *observed_policy.lock().unwrap(),
            Some((policy, CollaborationMode::Plan))
        );
        assert_eq!(session.collaboration_mode(), CollaborationMode::Plan);
        client.close().await;
    }
}

#[path = "../../tests/r20261019_sandbox_facade_contract.rs"]
mod r20261019_sandbox_facade_contract;

#[path = "../../tests/r20261019_windows_preparation_facade.rs"]
mod r20261019_windows_preparation_facade;
