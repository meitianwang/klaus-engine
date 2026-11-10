//! Defect: a live side task could not become a durable ordinary thread.
//! Root cause: the only fork facade resumed its source from a store, while side tasks have no rollout.
//! Fixed for the 2026-09-08 Electron side-task promotion work item.
//! This public facade regression belongs to server; core cannot depend on its own facade.

use super::*;
use crate::app_server::wire;
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeEncode};
use klaus_agent_core::schemas::rollout::SessionRole;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::rollout::parse_rollout_line;
use klaus_agent_core::store::session_store::SessionStore;
use std::sync::Mutex;
use tokio::sync::Notify;

fn json_value<T: RuntimeEncode>(value: &T) -> Value {
    serde_json::from_str(
        &runtime_codec::encode_text(value)
            .unwrap()
            .try_to_utf8()
            .unwrap(),
    )
    .unwrap()
}

pub(super) async fn request(
    client: &mut TestClient,
    id: u64,
    method: &str,
    params: Value,
) -> Value {
    client
        .send(json!({"id":id,"method":method,"params":params}))
        .await;
    client.response_for(id).await.0
}

async fn history(client: &mut TestClient, id: u64, session: &str) -> Value {
    let reply = request(client, id, "session/history", json!({"sessionId":session})).await;
    assert!(reply.get("error").is_none(), "{reply}");
    reply["result"]["history"].clone()
}

pub(super) async fn run_turn(client: &mut TestClient, id: u64, session: &str, input: &str) {
    client
        .send(json!({"id":id,"method":"turn/start","params":{"sessionId":session,"input":input}}))
        .await;
    let (reply, seen) = client.response_for(id).await;
    assert!(reply["result"]["turnId"].is_string(), "{reply}");
    for event in &seen {
        assert!(event["params"]["event"].get("error").is_none(), "{event}");
        assert_ne!(
            event["params"]["event"]["event"]["type"], "turn.failed",
            "{event}"
        );
        if event["method"] == "turn/completed" && event["params"]["sessionId"] == session {
            assert!(event["params"].get("error").is_none(), "{event}");
        }
    }
    if seen
        .iter()
        .any(|event| event["method"] == "turn/completed" && event["params"]["sessionId"] == session)
    {
        return;
    }
    loop {
        let event = client.next_message().await;
        assert!(event["params"]["event"].get("error").is_none(), "{event}");
        assert_ne!(
            event["params"]["event"]["event"]["type"], "turn.failed",
            "{event}"
        );
        if event["method"] == "turn/completed" && event["params"]["sessionId"] == session {
            assert!(event["params"].get("error").is_none(), "{event}");
            return;
        }
    }
}

struct BootstrapProvider(Arc<dyn ModelProvider>);

impl ModelProvider for BootstrapProvider {
    fn info(&self) -> &klaus_agent_core::types::provider::ProviderInfo {
        self.0.info()
    }

    fn capabilities(
        &self,
        model: &klaus_agent_core::runtime_values::RuntimeString,
    ) -> klaus_agent_core::types::provider::ProviderCapabilities {
        self.0.capabilities(model)
    }

    fn stream(
        &self,
        _: klaus_agent_core::types::provider::ModelRequest,
        _: klaus_agent_core::types::abort::AbortSignal,
    ) -> klaus_agent_core::types::provider::ModelStream {
        Box::pin(futures::stream::iter(text_turn("fixture answer")))
    }
}

const DIAGNOSTIC_LIMIT: usize = 8 * 1024;

#[derive(Clone, Copy)]
pub(super) enum DiagnosticPeer {
    Main,
    High,
    Medium,
    Low,
    Next,
}

impl DiagnosticPeer {
    fn label(self) -> &'static str {
        match self {
            Self::Main => "main",
            Self::High => "high",
            Self::Medium => "medium",
            Self::Low => "low",
            Self::Next => "next",
        }
    }
}

#[derive(Default)]
struct DiagnosticBuffer {
    lines: std::collections::VecDeque<String>,
    bytes: usize,
    dropped: usize,
}

pub(super) struct FixtureDiagnostic {
    label: &'static str,
    started: std::time::Instant,
    buffer: Mutex<DiagnosticBuffer>,
    source: Mutex<Option<std::sync::Weak<Session>>>,
    http: [std::sync::atomic::AtomicU64; 5],
    approvals_started: std::sync::atomic::AtomicU64,
    approvals_finished: std::sync::atomic::AtomicU64,
    approvals_active: std::sync::atomic::AtomicU64,
    dumped: std::sync::atomic::AtomicBool,
}

fn diagnostic_token(value: &str) -> String {
    value
        .chars()
        .take(48)
        .map(|c| {
            if c.is_ascii_alphanumeric() || "-_.:/".contains(c) {
                c
            } else {
                '?'
            }
        })
        .collect()
}

fn diagnostic_runtime_token(value: &klaus_agent_core::runtime_values::RuntimeString) -> String {
    value
        .as_utf16()
        .iter()
        .take(48)
        .map(|unit| {
            let c = char::from_u32(u32::from(*unit)).unwrap_or('?');
            if c.is_ascii_alphanumeric() || "-_.:/".contains(c) {
                c
            } else {
                '?'
            }
        })
        .collect()
}

impl FixtureDiagnostic {
    pub(super) fn new(label: &'static str) -> Arc<Self> {
        Arc::new(Self {
            label,
            started: std::time::Instant::now(),
            buffer: Mutex::new(DiagnosticBuffer::default()),
            source: Mutex::new(None),
            http: std::array::from_fn(|_| std::sync::atomic::AtomicU64::new(0)),
            approvals_started: std::sync::atomic::AtomicU64::new(0),
            approvals_finished: std::sync::atomic::AtomicU64::new(0),
            approvals_active: std::sync::atomic::AtomicU64::new(0),
            dumped: std::sync::atomic::AtomicBool::new(false),
        })
    }

    pub(super) fn record(&self, stage: &'static str, details: String) {
        let mut line = format!(
            "{}ms {} {}\n",
            self.started.elapsed().as_millis(),
            stage,
            details
        );
        line.truncate(512);
        let mut buffer = self
            .buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while buffer.bytes + line.len() > DIAGNOSTIC_LIMIT / 2 {
            if let Some(old) = buffer.lines.pop_front() {
                buffer.bytes -= old.len();
                buffer.dropped += 1;
            } else {
                break;
            }
        }
        buffer.bytes += line.len();
        buffer.lines.push_back(line);
    }

    pub(super) fn rpc(&self, stage: &'static str, message: &Value) {
        let id = message
            .get("id")
            .and_then(Value::as_u64)
            .map(|v| v.to_string())
            .or_else(|| {
                message
                    .get("id")
                    .and_then(Value::as_str)
                    .map(diagnostic_token)
            })
            .unwrap_or_else(|| "none".to_string());
        let method = diagnostic_token(
            message
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or("none"),
        );
        let session = diagnostic_token(
            message
                .pointer("/params/sessionId")
                .and_then(Value::as_str)
                .unwrap_or("none"),
        );
        let event = diagnostic_token(
            message
                .pointer("/params/event/event/type")
                .or_else(|| message.pointer("/params/event/type"))
                .and_then(Value::as_str)
                .unwrap_or("none"),
        );
        self.record(
            stage,
            format!(
                "id={id} method={method} session={session} event={event} error_field={}",
                message.get("error").is_some()
            ),
        );
    }

    pub(super) fn http_enter(&self, peer: DiagnosticPeer, path: &str) {
        let count = self.http[peer as usize].fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        self.record(
            "http_enter",
            format!(
                "peer={} count={count} path={}",
                peer.label(),
                diagnostic_token(path)
            ),
        );
    }

    pub(super) fn http_body(&self, peer: DiagnosticPeer, value: &Value) {
        self.record(
            "http_body",
            format!(
                "peer={} model={}",
                peer.label(),
                diagnostic_token(value.get("model").and_then(Value::as_str).unwrap_or("none"))
            ),
        );
    }

    fn bind(&self, source: &Arc<Session>) {
        *self
            .source
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::downgrade(source));
        self.checkpoint("side_bound");
    }

    pub(super) fn checkpoint(&self, stage: &'static str) {
        let source = self
            .source
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(std::sync::Weak::upgrade);
        if let Some(source) = source {
            let policy = source.runtime_policy_state();
            let planning = source.planning_runtime();
            self.record(stage, format!("filesystem={:?} approval={:?} reviewer={:?} policy_generation={} collaboration_active={:?} collaboration_desired={:?} children={}", policy.sandbox_mode, policy.approval_policy, policy.approvals_reviewer, policy.generation, planning.mode(), planning.desired_mode(), source.subagent_lifecycle_snapshot().agents.len()));
        } else {
            self.record(stage, "source=unbound".to_string());
        }
    }

    pub(super) fn dump(&self, reason: &'static str) {
        use std::io::Write;
        if self.dumped.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        self.checkpoint("final_snapshot");
        let mut output = format!("FIXTURE_DIAGNOSTIC label={} reason={} elapsed_ms={} http_main={} http_high={} http_medium={} http_low={} http_next={} approvals_started={} approvals_finished={} approvals_active={}\n", self.label, reason, self.started.elapsed().as_millis(), self.http[0].load(std::sync::atomic::Ordering::SeqCst), self.http[1].load(std::sync::atomic::Ordering::SeqCst), self.http[2].load(std::sync::atomic::Ordering::SeqCst), self.http[3].load(std::sync::atomic::Ordering::SeqCst), self.http[4].load(std::sync::atomic::Ordering::SeqCst), self.approvals_started.load(std::sync::atomic::Ordering::SeqCst), self.approvals_finished.load(std::sync::atomic::Ordering::SeqCst), self.approvals_active.load(std::sync::atomic::Ordering::SeqCst));
        let buffer = self
            .buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        output.push_str(&format!(
            "retained_bytes={} dropped_records={}\n",
            buffer.bytes, buffer.dropped
        ));
        for line in &buffer.lines {
            output.push_str(line);
        }
        output.truncate(DIAGNOSTIC_LIMIT - 1);
        output.push('\n');
        let _ = std::io::stderr().lock().write_all(output.as_bytes());
    }

    pub(super) fn scope(self: &Arc<Self>) -> DiagnosticScope {
        DiagnosticScope(self.clone())
    }
}

pub(super) struct DiagnosticScope(Arc<FixtureDiagnostic>);

impl Drop for DiagnosticScope {
    fn drop(&mut self) {
        self.0.dump(if std::thread::panicking() {
            "panic"
        } else {
            "scope_exit"
        });
    }
}

struct DiagnosticApprovalAttempt {
    diagnostic: Arc<FixtureDiagnostic>,
    request_id: String,
    returned: bool,
}

impl Drop for DiagnosticApprovalAttempt {
    fn drop(&mut self) {
        self.diagnostic
            .approvals_active
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        if !self.returned {
            self.diagnostic
                .record("approval_future_dropped", format!("id={}", self.request_id));
        }
    }
}

struct DiagnosticApprovalHandler {
    inner: Arc<dyn klaus_agent_core::types::permission::ApprovalHandler>,
    diagnostic: Arc<FixtureDiagnostic>,
}

#[async_trait::async_trait]
impl klaus_agent_core::types::permission::ApprovalHandler for DiagnosticApprovalHandler {
    async fn approve(
        &self,
        request: klaus_agent_core::types::permission::ApprovalRequest,
        signal: klaus_agent_core::types::abort::AbortSignal,
    ) -> klaus_agent_core::types::permission::ApprovalResponse {
        self.diagnostic
            .approvals_started
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.diagnostic.record(
            "approval_enter",
            format!(
                "id={} tool={}",
                diagnostic_runtime_token(&request.id),
                diagnostic_runtime_token(&request.tool_name)
            ),
        );
        self.diagnostic
            .approvals_active
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut attempt = DiagnosticApprovalAttempt {
            diagnostic: self.diagnostic.clone(),
            request_id: diagnostic_runtime_token(&request.id),
            returned: false,
        };
        self.diagnostic.checkpoint("approval_snapshot");
        let response = self.inner.approve(request, signal).await;
        self.diagnostic
            .approvals_finished
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let outcome = match &response {
            klaus_agent_core::types::permission::ApprovalResponse::Allow { .. } => "allow",
            klaus_agent_core::types::permission::ApprovalResponse::Deny { .. } => "deny",
        };
        self.diagnostic
            .record("approval_return", format!("outcome={outcome}"));
        attempt.returned = true;
        drop(attempt);
        response
    }
}

pub(super) struct Fixture {
    pub(super) client: TestClient,
    pub(super) source: Arc<Session>,
    pub(super) store: Arc<dyn SessionStore>,
    pub(super) root: tempfile::TempDir,
    config_home_guard: FixtureConfigHome,
}

struct FixtureConfigHome(Option<std::ffi::OsString>);

impl FixtureConfigHome {
    fn enter(root: &std::path::Path) -> Self {
        let previous = std::env::var_os("KLAUS_CONFIG_DIR");
        klaus_agent_core::runtime_process::set_process_environment_var("KLAUS_CONFIG_DIR", root);
        Self(previous)
    }
}

impl Drop for FixtureConfigHome {
    fn drop(&mut self) {
        match self.0.take() {
            Some(previous) => klaus_agent_core::runtime_process::set_process_environment_var(
                "KLAUS_CONFIG_DIR",
                previous,
            ),
            None => klaus_agent_core::runtime_process::remove_process_environment_var(
                "KLAUS_CONFIG_DIR",
            ),
        }
    }
}

impl Fixture {
    pub(super) async fn new(
        ignore_snapshot: bool,
        pause: Option<(Arc<Notify>, Arc<Notify>)>,
    ) -> Self {
        Self::with_credentials(ignore_snapshot, pause, None).await
    }

    pub(super) async fn with_credentials(
        ignore_snapshot: bool,
        pause: Option<(Arc<Notify>, Arc<Notify>)>,
        credentials: Option<Arc<dyn klaus_agent_core::credential_store::CredentialStore>>,
    ) -> Self {
        Self::with_bootstrap_transport(ignore_snapshot, pause, credentials, true).await
    }

    pub(super) async fn with_provider_transport() -> Self {
        Self::with_provider_transport_and_credentials(None).await
    }

    pub(super) async fn with_provider_transport_and_credentials(
        credentials: Option<Arc<dyn klaus_agent_core::credential_store::CredentialStore>>,
    ) -> Self {
        Self::with_bootstrap_transport(false, None, credentials, false).await
    }

    pub(super) async fn with_diagnostic(diagnostic: Arc<FixtureDiagnostic>) -> Self {
        Self::with_bootstrap_transport_and_diagnostic(
            false,
            None,
            None,
            true,
            Some(diagnostic),
            None,
        )
        .await
    }

    pub(super) async fn with_auto_compact_window(window: u64) -> Self {
        Self::with_bootstrap_transport_and_diagnostic(false, None, None, true, None, Some(window))
            .await
    }

    async fn with_bootstrap_transport(
        ignore_snapshot: bool,
        pause: Option<(Arc<Notify>, Arc<Notify>)>,
        credentials: Option<Arc<dyn klaus_agent_core::credential_store::CredentialStore>>,
        injected: bool,
    ) -> Self {
        Self::with_bootstrap_transport_and_diagnostic(
            ignore_snapshot,
            pause,
            credentials,
            injected,
            None,
            None,
        )
        .await
    }

    async fn with_bootstrap_transport_and_diagnostic(
        ignore_snapshot: bool,
        pause: Option<(Arc<Notify>, Arc<Notify>)>,
        credentials: Option<Arc<dyn klaus_agent_core::credential_store::CredentialStore>>,
        injected: bool,
        diagnostic: Option<Arc<FixtureDiagnostic>>,
        auto_compact_window: Option<u64>,
    ) -> Self {
        let root = tempfile::tempdir().unwrap();
        let config_home_guard = FixtureConfigHome::enter(root.path());
        let user_config_dir = klaus_agent_core::runtime_values::RuntimePath::from(
            root.path().to_string_lossy().into_owned(),
        );
        let dir = root.path().join("sessions");
        std::fs::create_dir_all(&dir).unwrap();
        let rows = [
            json!({"kind":"session_meta","schemaVersion":2,"meta":{"id":"parent","createdAt":1700000000000_i64,"cwd":root.path(),"model":"m","role":"main"}}),
            json!({"kind":"turn_context","ctx":{"cwd":root.path(),"model":"m","permissionMode":"default","turnId":"parent-turn","startedAtMs":1700000000100_i64}}),
            json!({"kind":"message","message":{"type":"user","uuid":"parent-user","content":[{"type":"text","text":"hidden parent prompt"}]}}),
            json!({"kind":"message","message":{"type":"assistant","uuid":"parent-answer","content":[{"type":"text","text":"hidden parent answer"}],"model":"m"}}),
            json!({"kind":"event","event":"turn_lifecycle","turnId":"parent-turn","payload":{"status":"completed","finalMessageId":"parent-answer","startedAtMs":1700000000100_i64,"finishedAtMs":1700000000200_i64,"durationMs":100}}),
        ];
        for row in &rows {
            assert!(parse_rollout_line(&row.to_string()).is_some(), "{row}");
        }
        std::fs::write(
            dir.join("1700000000000-parent.jsonl"),
            rows.iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n")
                + "\n",
        )
        .unwrap();
        let store: Arc<dyn SessionStore> =
            Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
                dir: Some(dir.to_string_lossy().into_owned()),
                archive_dir: None,
            }));
        let mut config = json!({"autoMemory":false,"autoDream":false,"memoryRecall":false,"memory":{"enabled":false}});
        if let Some(window) = auto_compact_window {
            config["autoCompactWindow"] = json!(window);
        }
        let config = Some(config);
        let agent = if injected {
            make_agent_with_call_model_and_config(
                Box::new(|_, _| Box::pin(futures::stream::iter(text_turn("fixture answer")))),
                config,
            )
        } else {
            make_agent_with_provider_and_config(
                Arc::new(BootstrapProvider(Arc::new(fake_provider()))),
                None,
                config,
            )
        };
        let captured = Arc::new(Mutex::new(None));
        let mut lifecycle = crate::lifecycle::protocol_lifecycle_in_test_root(
            agent.clone(),
            Some(store.clone()),
            user_config_dir.clone(),
        );
        let side_factory = lifecycle.create_side_task.take().unwrap();
        lifecycle.create_side_task = Some(Arc::new({
            let captured = captured.clone();
            let diagnostic = diagnostic.clone();
            move |mut handlers: klaus_agent_protocol::CreateSessionArgs, parent, id, cut| {
                if let Some(diagnostic) = &diagnostic {
                    diagnostic.record("side_factory", "entered".to_string());
                    handlers.approval_handler = Arc::new(DiagnosticApprovalHandler {
                        inner: handlers.approval_handler.clone(),
                        diagnostic: diagnostic.clone(),
                    });
                }
                let original = side_factory(handlers, parent, id, cut);
                let captured = captured.clone();
                let diagnostic = diagnostic.clone();
                Box::pin(async move {
                    let session = original.await?;
                    if let Some(diagnostic) = &diagnostic {
                        diagnostic.bind(&session);
                    }
                    *captured.lock().unwrap() = Some(session.clone());
                    Ok(session)
                })
            }
        }));
        let production = crate::lifecycle::engine_sessions_in_test_root(
            agent.clone(),
            Some(store.clone()),
            user_config_dir.clone(),
        )
        .create_session;
        let create: CreateSession = Arc::new(move |ctx| {
            let production = production.clone();
            let agent = agent.clone();
            let pause = pause.clone();
            let user_config_dir = user_config_dir.clone();
            Box::pin(async move {
                if ctx.fork_live.is_some() {
                    if let Some((entered, release)) = pause {
                        entered.notify_one();
                        release.notified().await;
                    }
                    if ignore_snapshot {
                        return Ok::<Arc<Session>, BoxError>(agent.start_session(
                            StartSessionOptions {
                                id: ctx.init.thread_id.clone(),
                                user_config_dir: Some(user_config_dir),
                                ..Default::default()
                            },
                        ));
                    }
                }
                production(ctx).await
            })
        });
        let (client_side, server_side) = tokio::io::duplex(1 << 20);
        let (server_read, server_write) = tokio::io::split(server_side);
        let (client_read, writer) = tokio::io::split(client_side);
        let server = tokio::spawn(run_app_server(
            server_read,
            server_write,
            AppServerOptions {
                windows_sandbox_context: None,
                create_session: create,
                credential_store: credentials,
                lifecycle: Some(lifecycle),
                plugin_admin: None,
                permission_rules_path: Some(root.path().join("rules.json")),
                config_path: Some(root.path().join("config.toml")),
                startup_init_ack_budget: None,
                session_unload_delay: None,
            },
        ));
        let mut client = TestClient {
            writer,
            lines: BufReader::new(client_read).lines(),
            server,
            diagnostic,
        };
        client.initialize().await;
        let start = request(
            &mut client,
            2,
            "session/start",
            json!({"init":{"threadId":"side","cwd":root.path()}}),
        )
        .await;
        assert_eq!(start["result"]["sessionId"], "side", "{start}");
        let side = request(
            &mut client,
            3,
            "session/createSideTask",
            json!({"sessionId":"side","threadId":"parent","newThreadId":"side"}),
        )
        .await;
        assert!(side.get("error").is_none(), "{side}");
        let source = captured
            .lock()
            .unwrap()
            .clone()
            .expect("side task was bound");
        assert_eq!(source.meta.role, Some(SessionRole::SideTask));
        assert_eq!(source.history().len(), 2);
        assert!(source.visible_history().is_empty());
        run_turn(&mut client, 4, "side", "side question").await;
        Self {
            client,
            source,
            store,
            root,
            config_home_guard,
        }
    }

    fn init(&self, target: &str) -> Value {
        json!({"threadId":target,"cwd":self.root.path()})
    }

    async fn fork(&mut self, id: u64, target: &str, cut: Option<&str>) -> Value {
        let mut params = json!({"sourceSessionId":"side","init":self.init(target)});
        if let Some(cut) = cut {
            params["beforeMessageUuid"] = json!(cut);
        }
        request(&mut self.client, id, "session/forkLive", params).await
    }

    pub(super) async fn close(self) {
        let mut client = self.client;
        client.writer.shutdown().await.unwrap();
        drop(client.writer);
        tokio::time::timeout(REPLY_TIMEOUT, client.server)
            .await
            .expect("endpoint teardown completes")
            .unwrap();
        let mut source_thread = self
            .store
            .read_thread("side")
            .await
            .expect("a persisted side task keeps its own thread");
        assert_eq!(
            source_thread.meta.role,
            Some(klaus_agent_core::schemas::rollout::SessionRole::SideTask),
            "the source thread keeps the side-task role"
        );
        let _ = &mut source_thread;
        drop(self.config_home_guard);
    }
}

#[tokio::test]
async fn live_fork_promotes_hidden_prefix_only_in_the_durable_destination() {
    let mut fixture = Fixture::new(false, None).await;
    let source_history = fixture.source.history();
    let visible = fixture.source.visible_history();
    let source_meta = fixture.source.meta.clone();
    let forked = fixture.fork(20, "branch", None).await;
    assert_eq!(forked["result"]["sessionId"], "branch", "{forked}");
    assert!(forked["result"]["initialized"].is_object(), "{forked}");
    let persisted = fixture.store.resume("branch").await.unwrap();
    assert_eq!(
        persisted.messages, source_history,
        "reply requires the entire transcript to be flushed"
    );
    assert_eq!(persisted.meta.role, Some(SessionRole::Main));
    assert_eq!(persisted.meta.parent_thread_id, None);
    assert_eq!(persisted.meta.forked_from_id, Some("side".into()));
    let expected = json_value(&source_history);
    assert_eq!(history(&mut fixture.client, 21, "branch").await, expected);
    assert_eq!(
        history(&mut fixture.client, 22, "side").await,
        json_value(&visible)
    );
    assert_eq!(fixture.source.history(), source_history);
    assert_eq!(fixture.source.meta, source_meta);
    let closed = request(
        &mut fixture.client,
        23,
        "session/close",
        json!({"sessionId":"branch"}),
    )
    .await;
    assert!(closed.get("error").is_none(), "{closed}");
    let init = fixture.init("branch");
    let resumed = request(
        &mut fixture.client,
        24,
        "session/start",
        json!({"init":init,"resume":"branch"}),
    )
    .await;
    assert_eq!(resumed["result"]["sessionId"], "branch", "{resumed}");
    assert_eq!(history(&mut fixture.client, 25, "branch").await, expected);
    run_turn(
        &mut fixture.client,
        26,
        "side",
        "source continues independently",
    )
    .await;
    assert!(fixture.source.history().len() > source_history.len());
    assert_eq!(history(&mut fixture.client, 27, "branch").await, expected);
    assert_eq!(fixture.source.meta, source_meta);
    fixture.close().await;
}

#[tokio::test]
async fn live_fork_cut_points_and_target_failures_leave_source_and_existing_threads_unchanged() {
    let mut fixture = Fixture::new(false, None).await;
    let source_history = fixture.source.history();
    let source_visible = fixture.source.visible_history();
    let source_meta = fixture.source.meta.clone();
    let original_parent = fixture.store.read_thread("parent").await.unwrap();
    let full = json_value(&source_history);
    let cut = full[2]["uuid"].as_str().unwrap();
    let branch = fixture.fork(20, "cut-branch", Some(cut)).await;
    assert_eq!(branch["result"]["sessionId"], "cut-branch", "{branch}");
    let expected = json_value(&source_history[..2].to_vec());
    assert_eq!(
        history(&mut fixture.client, 21, "cut-branch").await,
        expected
    );
    assert_eq!(
        fixture.store.resume("cut-branch").await.unwrap().messages,
        source_history[..2]
    );
    request(
        &mut fixture.client,
        22,
        "session/close",
        json!({"sessionId":"cut-branch"}),
    )
    .await;
    let init = fixture.init("cut-branch");
    request(
        &mut fixture.client,
        23,
        "session/start",
        json!({"init":init,"resume":"cut-branch"}),
    )
    .await;
    assert_eq!(
        history(&mut fixture.client, 24, "cut-branch").await,
        expected
    );
    for (id, target, cut) in [
        (30, "unknown-cut", Some("not-a-message")),
        (31, "side", None),
        (32, "cut-branch", None),
        (33, "parent", None),
    ] {
        let failed = fixture.fork(id, target, cut).await;
        assert!(failed.get("error").is_some(), "{failed}");
    }
    fixture.store.archive("parent").await.unwrap();
    let archived = fixture.store.read_thread("parent").await.unwrap();
    assert!(archived.archived);
    assert!(fixture
        .fork(34, "parent", None)
        .await
        .get("error")
        .is_some());
    assert_eq!(fixture.store.read_thread("parent").await.unwrap(), archived);
    assert_eq!(archived.raw_items, original_parent.raw_items);
    for (id, init) in [
        (40, json!(5)),
        (
            41,
            json!({"threadId":"invalid-init","permissionMode":"invalid-mode"}),
        ),
        (
            42,
            json!({"threadId":"invalid-provider","modelSelection":"custom:missing"}),
        ),
        (
            43,
            json!({"threadId":"invalid-provider-kind","provider":{"type":"invalid"}}),
        ),
        (
            44,
            json!({"threadId":"injected-history","initialHistory":[]}),
        ),
        (45, json!({"threadId":" padded-id "})),
        (46, json!({"threadId":""})),
    ] {
        let failed = request(
            &mut fixture.client,
            id,
            "session/forkLive",
            json!({"sourceSessionId":"side","init":init}),
        )
        .await;
        assert!(failed.get("error").is_some(), "{failed}");
    }
    assert_eq!(fixture.source.history(), source_history);
    assert_eq!(fixture.source.visible_history(), source_visible);
    assert_eq!(fixture.source.meta, source_meta);
    for target in [
        "unknown-cut",
        "invalid-init",
        "invalid-provider",
        "invalid-provider-kind",
        "injected-history",
        "padded-id",
    ] {
        assert!(
            fixture.store.read_thread(target).await.is_err(),
            "no durable failed target {target}"
        );
    }
    fixture.close().await;
}

#[tokio::test]
async fn live_fork_rejects_real_turn_admission_and_closing_sources() {
    let mut fixture = Fixture::new(false, None).await;
    // An unpolled real turn already owns ActiveTurnLease. Checking only the
    // streamed isRunning notification would accept this overlapping capture.
    let held = fixture
        .source
        .run_streamed(Input::Text("unpolled turn".into()), TurnOptions::default());
    let before = fixture.source.history();
    let busy = fixture.fork(20, "busy-branch", None).await;
    assert_eq!(busy["error"]["code"], wire::ENGINE_ERROR, "{busy}");
    assert!(
        busy["error"]["message"]
            .as_str()
            .unwrap()
            .contains("running"),
        "{busy}"
    );
    drop(held);
    assert_eq!(fixture.source.history(), before);
    let ready = fixture.fork(21, "ready-branch", None).await;
    assert_eq!(ready["result"]["sessionId"], "ready-branch", "{ready}");
    fixture.source.initiate_shutdown();
    let closing = fixture.fork(22, "closing-branch", None).await;
    assert!(closing.get("error").is_some(), "{closing}");
    assert!(fixture.store.read_thread("busy-branch").await.is_err());
    assert!(fixture.store.read_thread("closing-branch").await.is_err());
    fixture.close().await;
}

#[tokio::test]
async fn live_fork_capture_is_independent_of_source_turns_during_target_initialization() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut fixture = Fixture::new(false, Some((entered.clone(), release.clone()))).await;
    let before = fixture.source.history();
    let init = fixture.init("delayed-branch");
    fixture.client.send(json!({"id":20,"method":"session/forkLive","params":{"sourceSessionId":"side","init":init}})).await;
    tokio::time::timeout(REPLY_TIMEOUT, entered.notified())
        .await
        .unwrap();
    run_turn(&mut fixture.client, 21, "side", "after the capture").await;
    assert!(fixture.source.history().len() > before.len());
    release.notify_one();
    let forked = fixture.client.response_for(20).await.0;
    assert_eq!(forked["result"]["sessionId"], "delayed-branch", "{forked}");
    assert_eq!(
        fixture
            .store
            .resume("delayed-branch")
            .await
            .unwrap()
            .messages,
        before
    );
    fixture.close().await;
}

#[tokio::test]
async fn live_fork_rejects_an_endpoint_factory_that_ignores_persistence() {
    let mut fixture = Fixture::new(true, None).await;
    let before = fixture.source.history();
    let reply = fixture.fork(20, "ignored-branch", None).await;
    assert_eq!(reply["error"]["code"], wire::ENGINE_ERROR, "{reply}");
    assert!(
        reply["error"]["message"]
            .as_str()
            .unwrap()
            .contains("persistent ordinary live fork"),
        "{reply}"
    );
    assert!(fixture.store.read_thread("ignored-branch").await.is_err());
    assert_eq!(fixture.source.history(), before);
    let gone = request(
        &mut fixture.client,
        21,
        "session/history",
        json!({"sessionId":"ignored-branch"}),
    )
    .await;
    assert_eq!(gone["error"]["code"], wire::UNKNOWN_SESSION, "{gone}");
    fixture.close().await;
}
