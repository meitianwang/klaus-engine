//! Exact runtime content at the compiled CLI's JSON process boundary.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, AgentOptions};
use klaus_agent_core::engine::deps::EngineDepsOverrides;
use klaus_agent_core::runtime_process::{RuntimeProcessContext, RuntimeProcessSupervisor};
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimeString};
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::Message;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::testing::{ev, fake_provider};
use klaus_agent_core::types::provider::{ModelStopReason, ModelStreamEvent};
use klaus_agent_server::app_server::methods::{
    Initialize, SessionClose, SessionHistory, SessionQueueSnapshot, SessionSetGoal, SessionStart,
    SessionSteer, TurnStart,
};
use klaus_agent_server::app_server::runtime_wire::{RawJson, RuntimeValue};
use klaus_agent_server::app_server::wire;
use klaus_agent_server::{
    connect_facade, engine_sessions, protocol_lifecycle, serve, AppServerClient, ConnectOptions,
    FacadeConnection, ServeOptions,
};
use serde_json::json;

const DEADLINE: Duration = Duration::from_secs(45);
const WORKER_DEADLINE: Duration = Duration::from_secs(240);
const STDOUT_LIMIT: usize = 1024 * 1024;
const STDERR_LIMIT: usize = 8 * 1024;
const FIXTURE_ROOT: &str = "KLAUS_EXACT_CLI_FIXTURE_ROOT";
const WORKER_TEST: &str = "r20261018_exact_facade_cli::owned_fixture_endpoint";
const THREAD_ID: &str = "exact-cli-saved-thread";
const QUEUE_ID: &str = "c7064fb2-d1ab-4ceb-ae49-d82eb8553064";

struct OwnedRoot(PathBuf);

impl OwnedRoot {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("klaus-exact-cli-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(root.join("config")).expect("create a private config directory");
        Self(root)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for OwnedRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[derive(Debug)]
struct Capture {
    bytes: Vec<u8>,
    overflow: bool,
}

fn drain(mut pipe: impl Read, limit: usize, tail: bool) -> Capture {
    let mut capture = Capture {
        bytes: Vec::with_capacity(limit),
        overflow: false,
    };
    let mut buffer = [0_u8; 4096];
    loop {
        let read = match pipe.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => read,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => panic!("read a child output pipe: {error}"),
        };
        let excess = capture
            .bytes
            .len()
            .saturating_add(read)
            .saturating_sub(limit);
        capture.overflow |= excess > 0;
        if tail {
            if excess > 0 {
                capture.bytes.drain(..excess);
            }
            capture.bytes.extend_from_slice(&buffer[..read]);
        } else {
            let keep = read.min(limit.saturating_sub(capture.bytes.len()));
            capture.bytes.extend_from_slice(&buffer[..keep]);
        }
    }
    capture
}

/// Own the process before polling readiness or interpreting any of its output.
/// Both pipes are drained concurrently with a fixed retention ceiling.
struct OwnedChild {
    child: Child,
    stdout: Option<JoinHandle<Capture>>,
    stderr: Option<JoinHandle<Capture>>,
}

struct ChildOutput {
    status: ExitStatus,
    stdout: Capture,
    stderr: Capture,
}

impl OwnedChild {
    fn spawn(command: &mut Command) -> Self {
        let mut child = command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn the owned child");
        let stdout = child.stdout.take().expect("stdout is piped");
        let stderr = child.stderr.take().expect("stderr is piped");
        Self {
            child,
            stdout: Some(std::thread::spawn(move || {
                drain(stdout, STDOUT_LIMIT, false)
            })),
            stderr: Some(std::thread::spawn(move || {
                drain(stderr, STDERR_LIMIT, true)
            })),
        }
    }

    async fn finish(mut self) -> ChildOutput {
        let deadline = Instant::now() + DEADLINE;
        let status = loop {
            if let Some(status) = self.child.try_wait().expect("poll the owned child") {
                break status;
            }
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                let status = self.child.wait().expect("reap the timed out child");
                let output = self.collect(status);
                panic!(
                    "child exceeded {DEADLINE:?}; status {}; stderr tail:\n{}",
                    output.status,
                    String::from_utf8_lossy(&output.stderr.bytes)
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        self.collect(status)
    }

    fn collect(&mut self, status: ExitStatus) -> ChildOutput {
        ChildOutput {
            status,
            stdout: self
                .stdout
                .take()
                .unwrap()
                .join()
                .expect("join stdout drain"),
            stderr: self
                .stderr
                .take()
                .unwrap()
                .join()
                .expect("join stderr drain"),
        }
    }
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(reader) = self.stdout.take() {
            let _ = reader.join();
        }
        if let Some(reader) = self.stderr.take() {
            let _ = reader.join();
        }
    }
}

struct Fixture {
    child: OwnedChild,
    url: String,
    root: PathBuf,
}

impl Fixture {
    async fn start(root: &Path) -> Self {
        let mut command =
            Command::new(std::env::current_exe().expect("find the integration binary"));
        command
            .args(["--ignored", "--exact", WORKER_TEST, "--nocapture"])
            .current_dir(root)
            .env(FIXTURE_ROOT, root)
            .env("HOME", root)
            .env("KLAUS_CONFIG_DIR", root.join("config"))
            .env("KLAUS_PLUGIN_HOME", root.join("plugins"));
        let mut child = OwnedChild::spawn(&mut command);
        let ready = root.join("endpoint-url");
        let deadline = Instant::now() + DEADLINE;
        loop {
            if let Ok(url) = std::fs::read_to_string(&ready) {
                assert!(url.starts_with("ws://127.0.0.1:"), "{url}");
                return Self {
                    child,
                    url,
                    root: root.to_owned(),
                };
            }
            if let Some(status) = child.child.try_wait().expect("poll fixture startup") {
                let output = child.collect(status);
                panic!(
                    "fixture exited before readiness: {}; stderr tail:\n{}; stdout:\n{}",
                    output.status,
                    String::from_utf8_lossy(&output.stderr.bytes),
                    String::from_utf8_lossy(&output.stdout.bytes)
                );
            }
            assert!(Instant::now() < deadline, "fixture did not become ready");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn calls(&self) -> usize {
        std::fs::read_to_string(self.root.join("provider-calls"))
            .expect("read the fixture's model call count")
            .parse()
            .expect("the fixture writes an integer call count")
    }

    async fn finish(self) {
        std::fs::write(self.root.join("shutdown"), b"close").expect("request fixture shutdown");
        let output = self.child.finish().await;
        assert_success(&output);
    }
}

fn exact_content() -> RuntimeString {
    RuntimeString::from_utf16(vec![
        0xd800, 0x7c, 0xdc00, 0x7c, 0xd83e, 0xdd80, 0x7c, 0xfffd,
    ])
}

fn goal_objective() -> RuntimeString {
    RuntimeString::from("目标｜🦀｜\u{fffd}")
}

fn exact_occurrences(value: &RuntimeJson, expected: &RuntimeString) -> usize {
    match value {
        RuntimeJson::String(text) => usize::from(text == expected),
        RuntimeJson::Array(values) => values
            .iter()
            .map(|value| exact_occurrences(value, expected))
            .sum(),
        RuntimeJson::Object(object) => object
            .values()
            .map(|value| exact_occurrences(value, expected))
            .sum(),
        _ => 0,
    }
}

fn stream_chunks() -> Vec<RuntimeString> {
    vec![
        RuntimeString::from_utf16(vec![0xd800, 0x7c, 0xdc00, 0x7c]),
        RuntimeString::from_utf16(vec![0xd83e]),
        RuntimeString::from_utf16(vec![0xdd80]),
        RuntimeString::from_utf16(vec![0x7c, 0xfffd]),
    ]
}

fn field<'a>(value: &'a RuntimeJson, key: &str) -> &'a RuntimeJson {
    value
        .get(key)
        .unwrap_or_else(|| panic!("missing {key}: {value:?}"))
}

fn string<'a>(value: &'a RuntimeJson, key: &str) -> &'a RuntimeString {
    field(value, key)
        .as_string()
        .expect("expected an exact JSON string")
}

fn scope(session_id: &str) -> wire::SessionRequestParams {
    wire::SessionRequestParams {
        session_id: Some(session_id.to_owned()),
        submission_id: None,
    }
}

async fn initialize(client: &AppServerClient) {
    tokio::time::timeout(DEADLINE, client.request::<Initialize>(Default::default()))
        .await
        .expect("initialize deadline")
        .expect("initialize the named facade");
}

async fn connect(fixture: &Fixture) -> FacadeConnection {
    tokio::time::timeout(
        DEADLINE,
        connect_facade(&fixture.url, ConnectOptions::default()),
    )
    .await
    .expect("connect deadline")
    .expect("connect to the local named facade")
}

async fn seed_turn(client: &AppServerClient, session: &str, input: RuntimeString) {
    let mut notifications = client.notifications();
    let admitted = tokio::time::timeout(
        DEADLINE,
        client.request::<TurnStart>(wire::TurnStartParams {
            session_id: session.to_owned(),
            input: RuntimeValue(RuntimeJson::String(input)),
            options: None,
            submission_id: Some("exact-cli-seed-turn".to_owned()),
        }),
    )
    .await
    .expect("seed admission deadline")
    .expect("admit the seed turn");
    tokio::time::timeout(DEADLINE, async {
        while let Some(notification) = notifications.next().await {
            if notification.session_id() != Some(session) {
                continue;
            }
            let Some(event) = notification.event().expect("decode the seed event") else {
                continue;
            };
            if string(&event.0, "submissionId") != admitted.turn_id.as_str() {
                continue;
            }
            if let Some(result) = event.0.get("result") {
                assert!(event.0.get("error").is_none(), "{event:?}");
                assert_eq!(string(result, "stopReason"), "completed");
                assert_eq!(
                    string(result, "finalText").as_utf16(),
                    exact_content().as_utf16()
                );
                return;
            }
        }
        panic!("the seed event stream closed before its terminal result");
    })
    .await
    .expect("seed completion deadline");
}

fn cli(fixture: &Fixture, args: &[&str]) -> OwnedChild {
    let mut command = Command::new(env!("CARGO_BIN_EXE_klaus"));
    command
        .current_dir(&fixture.root)
        .env("HOME", &fixture.root)
        .env("KLAUS_CONFIG_DIR", fixture.root.join("config"))
        .env("KLAUS_PLUGIN_HOME", fixture.root.join("plugins"))
        .args(["--no-discovery", "--remote", &fixture.url, "--json"])
        .args([
            "--config-json",
            r#"{"goal":false,"autoMemory":false,"autoDream":false}"#,
        ])
        .args(args);
    OwnedChild::spawn(&mut command)
}

fn assert_success(output: &ChildOutput) {
    assert_eq!(
        output.status.code(),
        Some(0),
        "child status {}; stderr tail:\n{}; stdout:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr.bytes),
        String::from_utf8_lossy(&output.stdout.bytes)
    );
    assert!(
        !output.stdout.overflow,
        "stdout exceeded the fixture's 1 MiB limit"
    );
}

fn json_lines(output: &ChildOutput) -> Vec<RuntimeJson> {
    assert_success(output);
    let text = std::str::from_utf8(&output.stdout.bytes).expect("JSON stdout is UTF-8");
    assert!(text.ends_with('\n'), "stdout must end a complete JSON line");
    let values: Vec<_> = text
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| {
            let value = runtime_json::parse(&RuntimeString::from(line)).expect("native JSON line");
            assert!(value.is_object(), "a JSON line must be an event object");
            assert!(field(&value, "submissionId").is_string(), "{value:?}");
            value
        })
        .collect();
    assert!(!values.is_empty(), "the CLI printed no JSON result");
    values
}

fn one_result(output: &ChildOutput) -> RuntimeJson {
    let mut values = json_lines(output);
    assert_eq!(
        values.len(),
        1,
        "a direct read prints one terminal response"
    );
    values.pop().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_resumed_json_reads_preserve_saved_goal_queue_and_history_units() {
    let root = OwnedRoot::new();
    let fixture = Fixture::start(root.path()).await;
    let connection = connect(&fixture).await;
    let client = &connection.client;
    initialize(client).await;
    let started = tokio::time::timeout(
        DEADLINE,
        client.request::<SessionStart>(wire::SessionStartParams {
            init: RawJson::from_serialize(&json!({
                "threadId": THREAD_ID,
                "cwd": root.path(),
                "config": {"goal":true,"autoMemory":false,"autoDream":false}
            }))
            .unwrap(),
            resume: None,
            fork: None,
        }),
    )
    .await
    .expect("seed session deadline")
    .expect("start a real persisted seed session");
    seed_turn(client, &started.session_id, exact_content()).await;
    let goal = tokio::time::timeout(
        DEADLINE,
        client.request::<SessionSetGoal>(wire::SessionSetGoalParams {
            owned_resources: None,
            client_request_id: Some("exact-cli-seed-goal".to_owned()),
            submit: None,
            display_text: None,
            target: wire::SessionGoalParams {
                session_id: Some(started.session_id.clone()),
                thread_id: None,
                expected_goal_id: Some(None),
                expected_goal_revision: None,
                origin: Some(wire::GoalOrigin::User),
                submission_id: None,
            },
            objective: Some(goal_objective()),
            status: Some("paused".to_owned()),
            token_budget: None,
        }),
    )
    .await
    .expect("seed goal deadline")
    .expect("persist the exact paused goal");
    let saved_goal = goal.thread_goal.expect("the goal was persisted");
    assert_eq!(saved_goal.objective.as_utf16(), goal_objective().as_utf16());
    assert_eq!(saved_goal.status, wire::ThreadGoalStatus::Paused);
    tokio::time::timeout(
        DEADLINE,
        client.request::<SessionSteer>(wire::SessionSteerParams {
            session_id: started.session_id.clone(),
            expected_turn_id: None,
            queue_message_id: Some(QUEUE_ID.to_owned()),
            input: RuntimeValue(RuntimeJson::String(exact_content())),
            priority: Some("later".to_owned()),
            delivery: None,
            display_metadata: None,
            submission_id: Some("exact-cli-seed-queue".to_owned()),
        }),
    )
    .await
    .expect("seed queue deadline")
    .expect("enqueue exact content through the named facade");
    let saved_queue = tokio::time::timeout(
        DEADLINE,
        client.request::<SessionQueueSnapshot>(scope(&started.session_id)),
    )
    .await
    .expect("seed snapshot deadline")
    .expect("read the persisted queue snapshot");
    let saved_queue = saved_queue
        .get("queueSnapshot")
        .unwrap()
        .expect("the queue reply carries its native snapshot")
        .to_runtime()
        .unwrap();
    let snapshot = &saved_queue.0;
    let message = snapshot
        .pointer("/items/0/message")
        .expect("one queued message");
    assert_eq!(
        string(message, "value").as_utf16(),
        exact_content().as_utf16()
    );
    assert_eq!(string(message, "uuid"), QUEUE_ID);
    assert_eq!(string(message, "sessionId"), THREAD_ID);
    let saved_history = tokio::time::timeout(
        DEADLINE,
        client.request::<SessionHistory>(scope(&started.session_id)),
    )
    .await
    .expect("seed history deadline")
    .expect("read exact completed history");
    let saved_history = saved_history
        .get("history")
        .unwrap()
        .expect("the history reply carries its native rows")
        .to_runtime()
        .unwrap();
    let history = &saved_history.0;
    assert!(history
        .as_array()
        .is_some_and(|messages| messages.len() >= 2));
    assert_eq!(
        exact_occurrences(history, &exact_content()),
        2,
        "both the human input and the completed assistant message retain all units"
    );
    tokio::time::timeout(
        DEADLINE,
        client.request::<SessionClose>(wire::SessionCloseParams {
            session_id: started.session_id,
        }),
    )
    .await
    .expect("seed flush deadline")
    .expect("close the seed session and release its rollout writer");
    tokio::time::timeout(DEADLINE, connection.close_and_wait())
        .await
        .expect("close the seed transport");
    drop(connection);
    assert_eq!(
        fixture.calls(),
        1,
        "only the seed turn invokes the provider"
    );

    let observed_goal = one_result(
        &cli(&fixture, &["--resume", THREAD_ID, "goal", "get"])
            .finish()
            .await,
    );
    let observed = field(&observed_goal, "threadGoal");
    assert_eq!(
        string(observed, "objective").as_utf16(),
        goal_objective().as_utf16()
    );
    assert_eq!(string(observed, "goalId"), saved_goal.goal_id.as_str());
    assert_eq!(string(observed, "threadId"), THREAD_ID);
    assert_eq!(string(observed, "status"), "paused");
    assert_eq!(
        field(&observed_goal, "goalRevision").as_i64(),
        Some(goal.goal_revision)
    );
    let observed_queue = one_result(
        &cli(&fixture, &["--resume", THREAD_ID, "queue", "snapshot"])
            .finish()
            .await,
    );
    assert_eq!(field(&observed_queue, "queueSnapshot"), snapshot);
    let observed_list = one_result(
        &cli(&fixture, &["--resume", THREAD_ID, "queue", "list"])
            .finish()
            .await,
    );
    assert_eq!(
        field(&observed_list, "queued"),
        &RuntimeJson::Array(vec![message.clone()])
    );
    let observed_history = one_result(
        &cli(&fixture, &["--resume", THREAD_ID, "history"])
            .finish()
            .await,
    );
    assert_eq!(field(&observed_history, "history"), history);
    assert_eq!(
        fixture.calls(),
        1,
        "disabled saved reads must not invoke a provider"
    );
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_exec_json_keeps_each_native_delta_and_one_exact_terminal_result() {
    let root = OwnedRoot::new();
    let fixture = Fixture::start(root.path()).await;
    let output = cli(&fixture, &["exec", "stream the exact fixture"])
        .finish()
        .await;
    let events = json_lines(&output);
    let deltas: Vec<_> = events
        .iter()
        .filter_map(|envelope| envelope.get("event"))
        .filter(|event| {
            event
                .get("type")
                .and_then(RuntimeJson::as_string)
                .is_some_and(|kind| kind == "text.delta")
        })
        .map(|event| string(event, "text").clone())
        .collect();
    assert_eq!(
        deltas,
        stream_chunks(),
        "every normalized delta retains its own units"
    );
    assert_eq!(deltas[1].as_utf16(), &[0xd83e]);
    assert_eq!(deltas[2].as_utf16(), &[0xdd80]);
    let terminals: Vec<_> = events
        .iter()
        .filter(|event| event.get("result").is_some())
        .collect();
    assert_eq!(
        terminals.len(),
        1,
        "one admitted exec has one terminal result"
    );
    let terminal = terminals[0];
    let result = field(terminal, "result");
    assert_eq!(string(result, "stopReason"), "completed");
    assert_eq!(
        string(result, "finalText").as_utf16(),
        exact_content().as_utf16()
    );
    assert!(terminal.get("error").is_none(), "{terminal:?}");
    assert!(!string(terminal, "submissionId").is_empty());
    for envelope in &events {
        if envelope
            .get("event")
            .and_then(|event| event.get("type"))
            .and_then(RuntimeJson::as_string)
            .is_some_and(|kind| kind == "text.delta")
        {
            assert_eq!(
                string(envelope, "submissionId"),
                string(terminal, "submissionId")
            );
        }
    }
    assert_eq!(
        fixture.calls(),
        1,
        "exec must not repeat provider execution"
    );
    fixture.finish().await;
}

/// A separate process owns all environment-sensitive engine roots. The parent
/// invokes only this ignored worker, and owns its process even if an assertion
/// fails before the normal endpoint close handshake.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "private endpoint worker invoked only by the compiled CLI regressions"]
async fn owned_fixture_endpoint() {
    let root = PathBuf::from(std::env::var_os(FIXTURE_ROOT).expect("worker needs its owned root"));
    assert!(root.is_absolute());
    assert_eq!(std::env::var_os("HOME").as_deref(), Some(root.as_os_str()));
    assert_eq!(
        std::env::var_os("KLAUS_CONFIG_DIR").as_deref(),
        Some(root.join("config").as_os_str())
    );
    let config = json!({
        "model":"fake-model", "goal":false, "autoMemory":false,
        "agentMemory":false, "autoDream":false, "loopDetection":false, "prewarm":false
    });
    std::fs::write(
        root.join("config/config.json"),
        serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();
    std::fs::write(root.join("provider-calls"), b"0").unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let provider_calls = calls.clone();
    let call_root = root.clone();
    let supervisor =
        RuntimeProcessSupervisor::initialize(tokio::runtime::Handle::current()).unwrap();
    let process_context = RuntimeProcessContext::initialize(supervisor.clone()).unwrap();
    let agent = Arc::new(Agent::new(AgentOptions {
        process_context: process_context.clone(),
        provider: Arc::new(fake_provider()),
        config: Some(config),
        cwd: Some(root.to_str().unwrap().to_owned()),
        tools: None,
        web_fetch_page: None,
        browser_runtime: None,
        capability_roots: Vec::new(),
        deps: EngineDepsOverrides {
            call_model: Some(Box::new(move |request, signal| {
                let count = provider_calls.fetch_add(1, Ordering::SeqCst) + 1;
                let temporary = call_root.join("provider-calls.next");
                std::fs::write(&temporary, count.to_string()).unwrap();
                std::fs::rename(temporary, call_root.join("provider-calls")).unwrap();
                assert_eq!(count, 1, "unexpected provider request in the owned fixture");
                assert!(request.messages.iter().any(|message| matches!(message,
                    Message::User(user) if user.content.iter().any(|block| matches!(block,
                        ContentBlock::Text(text) if text.text == exact_content() || text.text == "stream the exact fixture"
                    ))
                )), "the provider must receive the actual admitted human input");
                let mut events = vec![ev::message_start(None)];
                events.extend(
                    stream_chunks()
                        .into_iter()
                        .map(|text| ModelStreamEvent::TextDelta { text }),
                );
                events.push(ev::stop(Some(ModelStopReason::EndTurn), None));
                Box::pin(futures::stream::iter(events).take_until(async move {
                    signal.cancelled().await;
                }))
            })),
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
    }));
    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.join("sessions").to_str().unwrap().to_owned()),
        archive_dir: None,
    }));
    let sessions = engine_sessions(agent.clone(), Some(store.clone()));
    let server = serve(ServeOptions {
        windows_sandbox_context: None,
        create_session: sessions.create_session,
        credential_store: None,
        list_threads: sessions.list_threads,
        lifecycle: Some(protocol_lifecycle(agent, Some(store))),
        plugin_admin: None,
        host: Some("127.0.0.1".to_owned()),
        port: Some(0),
        token: None,
        allowed_origins: None,
        silent: true,
    })
    .await
    .expect("start the genuine local facade endpoint");
    std::fs::write(root.join("endpoint-url.next"), &server.url).unwrap();
    std::fs::rename(root.join("endpoint-url.next"), root.join("endpoint-url")).unwrap();
    tokio::time::timeout(WORKER_DEADLINE, async {
        while !root.join("shutdown").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the parent must finish or kill its private worker");
    tokio::time::timeout(DEADLINE, server.close())
        .await
        .expect("close all endpoint sessions and sockets");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    tokio::time::timeout(DEADLINE, process_context.shutdown())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(DEADLINE, supervisor.shutdown())
        .await
        .unwrap()
        .unwrap();
}
