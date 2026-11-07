//! End-to-end tests for both network transports (WebSocket and HTTP+SSE), driven
//! offline with the scripted fake provider. Each connection gets its own session;
//! these cover the happy path over BOTH transports, health probes, the auth
//! boundary (fail-closed non-loopback, empty-host collapse, bearer token, Origin
//! rejection), a control-op round-trip, extended ops, and lifecycle
//! (list/resume/fork) over the wire.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Liveness deadline for every cross-task wait here (teardown started, close
/// finished, request answered, run produced its events). A full-suite run
/// saturates the host, and these tests each drive a real server over a real
/// transport, so a freshly spawned task can be starved for a long time. The
/// assertions behind these waits are pure LIVENESS ("it does eventually happen"),
/// never latency: a genuine hang still fails, just later.
const TRANSPORT_LIVENESS_TIMEOUT: Duration = Duration::from_secs(120);

/// Poll budget for the retry loop that waits for a resumed session's history to
/// become readable — same liveness-not-latency reasoning as above.
const HISTORY_READABLE_ATTEMPTS: usize = 2_000;
const HISTORY_READABLE_INTERVAL: Duration = Duration::from_millis(20);

use async_trait::async_trait;
use futures::StreamExt;
use serde_json::json;

use klaus_agent_core::engine::agent::{Agent, AgentOptions, StartSessionOptions};
use klaus_agent_core::engine::deps::EngineDepsOverrides;
use klaus_agent_core::engine::message_queue::{QueuePriority, QueuedValue};
use klaus_agent_core::engine::session::{Input, Session};
use klaus_agent_core::hooks::types::{
    Hook, HookEvent, HookFn, HookHandler, HookInput, HookOutcome,
};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimeString};
use klaus_agent_core::schemas::message::Message;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::{ListOptions, SessionStore, ThreadPage};
use klaus_agent_core::testing::{ev, fake_provider, scripted_call_model, ScriptedTurns};
use klaus_agent_core::types::errors::TerminalReason;
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::permission::ExternalPermissionMode;
use klaus_agent_core::types::provider::{ModelProvider, ModelStopReason, ModelStreamEvent};
use tokio::sync::Notify;

use klaus_agent_protocol::{
    ProtocolClient, ProtocolEvent, RewindScope, RunOpOptions, SteerPriority,
};

use crate::app_server::{methods, wire};
use crate::connect::{connect, connect_facade, ClientConnection, ConnectOptions};
use crate::connect_http::{connect_http, list_threads, ConnectHttpOptions};
use crate::endpoint_activity::{EndpointTestGate, EndpointTestGates};
use crate::lifecycle::{engine_sessions, BoxError, CreateSession, SessionContext};
use crate::serve::{serve, ServeError, ServeOptions};

// --- fixtures -------------------------------------------------------------

fn runtime_value(source: &str) -> RuntimeJson {
    runtime_json::parse(&RuntimeString::from_utf8(source)).expect("valid runtime JSON fixture")
}

fn text_turn(t: &str) -> Vec<ModelStreamEvent> {
    vec![
        ev::message_start(None),
        ev::text(t),
        ev::stop(Some(ModelStopReason::EndTurn), None),
    ]
}

fn make_agent(
    process_context: RuntimeProcessContext,
    cwd: &str,
    turns: ScriptedTurns,
) -> Arc<Agent> {
    make_agent_with_hooks(process_context, cwd, turns, Vec::new())
}

fn make_agent_with_hooks(
    process_context: RuntimeProcessContext,
    cwd: &str,
    turns: ScriptedTurns,
    hooks: Vec<Hook>,
) -> Arc<Agent> {
    Arc::new(Agent::new(AgentOptions {
        process_context,
        provider: Arc::new(fake_provider()) as Arc<dyn ModelProvider>,
        config: None,
        tools: None,
        web_fetch_page: None,
        browser_runtime: None,
        capability_roots: Vec::new(),
        cwd: Some(cwd.to_string()),
        deps: EngineDepsOverrides {
            call_model: Some(scripted_call_model(turns)),
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

/// A session factory backed by an agent scripted with `replies` (one text turn
/// each), running under bypassPermissions so turns never park on an approval.
fn make_create_session(replies: &[&str]) -> CreateSession {
    let turns: ScriptedTurns = replies.iter().map(|r| text_turn(r)).collect();
    let agent = make_agent(crate::test_process_context(), ".", turns);
    create_session_for_agent(agent)
}

fn create_session_for_agent(agent: Arc<Agent>) -> CreateSession {
    Arc::new(move |ctx: SessionContext| {
        let agent = agent.clone();
        Box::pin(async move {
            let session = agent.start_session(StartSessionOptions {
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

fn make_gated_create_session(
    reply: &str,
    entered: Arc<Notify>,
    release: Arc<Notify>,
) -> CreateSession {
    let agent = make_agent_with_hooks(
        crate::test_process_context(),
        ".",
        vec![text_turn(reply)],
        vec![Hook {
            event: HookEvent::SessionEnd,
            matcher: None,
            handler: HookHandler::Fn(Arc::new(GatedSessionEndHook { entered, release })),
            is_async: false,
        }],
    );
    create_session_for_agent(agent)
}

fn serve_opts(create_session: CreateSession) -> ServeOptions {
    ServeOptions {
        windows_sandbox_context: None,
        create_session,
        credential_store: None,
        list_threads: None,
        lifecycle: None,
        plugin_admin: None,
        host: None,
        port: None,
        token: None,
        allowed_origins: None,
        silent: true,
        teardown_test_gates: EndpointTestGates::default(),
    }
}

async fn assert_retained_close_waits_for_gate(
    server: Arc<crate::serve::RunningServer>,
    gate: Arc<EndpointTestGate>,
    label: &'static str,
) {
    tokio::time::timeout(TRANSPORT_LIVENESS_TIMEOUT, gate.wait_until_entered())
        .await
        .unwrap_or_else(|_| panic!("{label} gate was never entered"));

    let first_server = server.clone();
    let mut first_close = tokio::spawn(async move { first_server.close().await });
    tokio::time::timeout(TRANSPORT_LIVENESS_TIMEOUT, server.wait_for_close_start())
        .await
        .unwrap_or_else(|_| panic!("{label} close never started"));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut first_close)
            .await
            .is_err(),
        "first close returned while {label} was gated"
    );

    let second_server = server.clone();
    let mut second_close = tokio::spawn(async move { second_server.close().await });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut second_close)
            .await
            .is_err(),
        "second close returned while {label} was gated"
    );

    first_close.abort();
    assert!(first_close.await.unwrap_err().is_cancelled());
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut second_close)
            .await
            .is_err(),
        "aborting the initiating close orphaned {label}"
    );

    gate.release();
    tokio::time::timeout(TRANSPORT_LIVENESS_TIMEOUT, second_close)
        .await
        .unwrap_or_else(|_| panic!("{label} close timed out after release"))
        .unwrap_or_else(|error| panic!("{label} close task panicked: {error}"));
}

fn unique_temp_dir(tag: &str) -> std::path::PathBuf {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "klaus-server-{tag}-{}-{}-{}",
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

/// Subscribe, run one turn, and collect engine events up to (and including)
/// `turn.completed`. Subscribing before the run avoids racing the first events.
async fn run_turn(client: &ProtocolClient, input: &str) -> Vec<Arc<ProtocolEvent>> {
    tokio::time::timeout(TRANSPORT_LIVENESS_TIMEOUT, async {
        let mut events = client.events();
        client.run(Input::Text(input.into()), None).await;
        let mut received = Vec::new();
        while let Some(evt) = events.next().await {
            let stop = is_completed(&evt);
            received.push(evt);
            if stop {
                break;
            }
        }
        received
    })
    .await
    .expect("run_turn timed out")
}

fn delta_text(events: &[Arc<ProtocolEvent>]) -> String {
    events
        .iter()
        .filter_map(|e| match &e.event {
            Some(EngineEvent::TextDelta { text, .. }) => Some(
                text.try_to_utf8()
                    .expect("integration-test text deltas must be scalar"),
            ),
            _ => None,
        })
        .collect()
}

fn has_completed(events: &[Arc<ProtocolEvent>]) -> bool {
    events.iter().any(|e| is_completed(e))
}

fn is_retained_skill_catalog(message: &Message) -> bool {
    matches!(message, Message::User(user)
        if user.is_meta == Some(true) && user.content.len() == 1
            && matches!(&user.content[0],
                klaus_agent_core::schemas::content::ContentBlock::Text(text)
                if text.harness_envelope == Some(klaus_agent_core::schemas::content::HarnessEnvelope::SystemReminder)
                    && text.text.starts_with("<skill-catalog:")))
}

fn assert_retained_skill_catalog(history: &[Message]) {
    assert_eq!(
        history
            .iter()
            .filter(|message| is_retained_skill_catalog(message))
            .count(),
        1,
        "raw transport history retains one trusted skill catalog separately from user turns"
    );
}

fn user_turns(history: &[Message]) -> usize {
    history
        .iter()
        .filter(|message| {
            matches!(message, Message::User(_)) && !is_retained_skill_catalog(message)
        })
        .count()
}

// --- WebSocket transport --------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_close_waits_for_an_accepted_http_connection_task() {
    let gate = Arc::new(EndpointTestGate::default());
    let mut opts = serve_opts(make_create_session(&[]));
    opts.teardown_test_gates.http_after_admission = Some(gate.clone());
    let server = Arc::new(serve(opts).await.unwrap());

    let socket = tokio::net::TcpStream::connect((server.host.as_str(), server.port))
        .await
        .expect("raw HTTP connection");
    assert_retained_close_waits_for_gate(server, gate, "accepted HTTP connection task").await;
    drop(socket);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_close_finishes_an_in_flight_http_handler_before_returning() {
    let gate = Arc::new(EndpointTestGate::default());
    let handler_gate = gate.clone();
    let mut opts = serve_opts(make_create_session(&[]));
    opts.list_threads = Some(Arc::new(move |_options| {
        let handler_gate = handler_gate.clone();
        Box::pin(async move {
            handler_gate.pause().await;
            Ok(ThreadPage {
                items: Vec::new(),
                next_cursor: None,
                membership_fingerprint: None,
                metadata_fingerprint: None,
            })
        }) as Pin<Box<dyn Future<Output = Result<ThreadPage, BoxError>> + Send>>
    }));
    let server = Arc::new(serve(opts).await.unwrap());
    let url = format!("http://127.0.0.1:{}/threads", server.port);
    let request = tokio::spawn(async move { reqwest::get(url).await });

    assert_retained_close_waits_for_gate(server, gate, "in-flight HTTP handler").await;
    let response = tokio::time::timeout(TRANSPORT_LIVENESS_TIMEOUT, request)
        .await
        .expect("in-flight HTTP request timed out after handler release")
        .expect("in-flight HTTP request task panicked")
        .expect("in-flight HTTP request failed during graceful shutdown");
    assert_eq!(response.status().as_u16(), 200);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_drives_a_session_end_to_end() {
    let server = serve(serve_opts(make_create_session(&["hello over ws"])))
        .await
        .unwrap();
    let conn = connect(&server.url, ConnectOptions::default())
        .await
        .unwrap();
    let events = run_turn(&conn.client, "hi").await;
    assert!(delta_text(&events).contains("hello over ws"));
    assert!(has_completed(&events));
    conn.close();
    server.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_facade_drives_a_session_over_the_socket() {
    let server = serve(serve_opts(make_create_session(&["hello over the facade"])))
        .await
        .unwrap();
    let conn = connect_facade(&server.url, ConnectOptions::default())
        .await
        .unwrap();

    conn.client
        .request::<methods::Initialize>(wire::InitializeParams::default())
        .await
        .expect("handshake");
    // Subscribe before starting the turn: the id names a live stream, not a
    // value to fetch later.
    let mut events = conn.client.notifications();
    let started = conn
        .client
        .request::<methods::SessionStart>(wire::SessionStartParams {
            init: crate::app_server::RawJson::from_serialize(&json!({})).unwrap(),
            resume: None,
            fork: None,
        })
        .await
        .expect("session/start");
    let turn = conn
        .client
        .request::<methods::TurnStart>(wire::TurnStartParams {
            session_id: started.session_id.clone(),
            input: json!("hi").into(),
            options: None,
            submission_id: None,
        })
        .await
        .expect("turn/start");

    let mut text = String::new();
    let completed = tokio::time::timeout(TRANSPORT_LIVENESS_TIMEOUT, async {
        while let Some(notification) = events.next().await {
            if let Some(event) = notification.event().expect("valid facade event") {
                let event: ProtocolEvent =
                    klaus_agent_core::runtime_values::runtime_codec::decode_value(&event.0)
                        .expect("session/event must carry a valid protocol envelope");
                if let Some(EngineEvent::TextDelta { text: delta, .. }) = event.event {
                    text.push_str(&delta.try_to_utf8().expect("fixture text is scalar"));
                }
            }
            if notification.method == "turn/completed" {
                return Some(notification);
            }
        }
        None
    })
    .await
    .expect("the facade streamed the turn to completion")
    .expect("the notification stream ended before turn/completed");

    assert_eq!(
        completed.params.control_string("turnId").unwrap(),
        Some(turn.turn_id)
    );
    assert_eq!(
        completed.params.control_string("sessionId").unwrap(),
        Some(started.session_id.clone())
    );
    assert!(
        text.contains("hello over the facade"),
        "expected streamed text, got: {text:?}"
    );

    conn.client
        .request::<methods::SessionClose>(wire::SessionCloseParams {
            session_id: started.session_id,
        })
        .await
        .expect("session/close");
    conn.close_and_wait().await;
    server.close().await;
}

/// The two surfaces are chosen, not guessed: a client that asks for one it does
/// not get would look like an engine that answers nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unknown_surface_is_rejected_at_the_upgrade() {
    let server = serve(serve_opts(make_create_session(&[]))).await.unwrap();
    let outcome = connect(
        &format!("{}/?surface=nope", server.url),
        ConnectOptions::default(),
    )
    .await;
    let err = match outcome {
        Ok(_) => panic!("an unknown surface must not open"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("400"),
        "expected an HTTP 400 rejection, got: {err}"
    );
    server.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_server_close_waits_for_protocol_session_teardown() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let server = Arc::new(
        serve(serve_opts(make_gated_create_session(
            "closing ws",
            entered.clone(),
            release.clone(),
        )))
        .await
        .unwrap(),
    );
    let conn = connect(&server.url, ConnectOptions::default())
        .await
        .unwrap();
    assert!(has_completed(&run_turn(&conn.client, "hi").await));

    let first_server = server.clone();
    let mut first_close = tokio::spawn(async move { first_server.close().await });
    tokio::time::timeout(TRANSPORT_LIVENESS_TIMEOUT, entered.notified())
        .await
        .expect("WebSocket SessionEnd teardown started");
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut first_close)
            .await
            .is_err(),
        "RunningServer::close returned before WebSocket Session teardown"
    );
    let second_server = server.clone();
    let mut second_close = tokio::spawn(async move { second_server.close().await });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut second_close)
            .await
            .is_err(),
        "concurrent WebSocket close did not join the retained teardown"
    );
    first_close.abort();
    assert!(first_close.await.unwrap_err().is_cancelled());
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut second_close)
            .await
            .is_err(),
        "cancelling the initiating close orphaned WebSocket teardown"
    );
    release.notify_one();
    tokio::time::timeout(TRANSPORT_LIVENESS_TIMEOUT, second_close)
        .await
        .expect("WebSocket server close timed out")
        .expect("WebSocket server close task panicked");
    drop(conn);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_close_waits_for_websocket_upgrade_before_registration() {
    let gate = Arc::new(EndpointTestGate::default());
    let mut opts = serve_opts(make_create_session(&[]));
    opts.teardown_test_gates.websocket_before_registration = Some(gate.clone());
    let server = Arc::new(serve(opts).await.unwrap());
    let conn = connect(&server.url, ConnectOptions::default())
        .await
        .expect("WebSocket upgrade");

    assert_retained_close_waits_for_gate(server, gate, "WebSocket pre-registration task").await;
    drop(conn);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_carries_a_control_op_and_response_rewind() {
    let server = serve(serve_opts(make_create_session(&["one"])))
        .await
        .unwrap();
    let conn = connect(&server.url, ConnectOptions::default())
        .await
        .unwrap();
    let rewound = tokio::time::timeout(TRANSPORT_LIVENESS_TIMEOUT, async {
        let mut events = conn.client.events();
        conn.client.run(Input::Text("hello".into()), None).await;
        let mut completed = 0;
        let mut outcome = None;
        while let Some(evt) = events.next().await {
            if is_completed(&evt) {
                completed += 1;
                if completed == 1 {
                    conn.client.rewind(1, Some(RewindScope::Conversation)).await;
                }
            }
            if let Some(r) = &evt.rewound {
                outcome = Some(r.clone());
                break;
            }
        }
        outcome
    })
    .await
    .expect("rewind round-trip timed out");
    assert_eq!(rewound.unwrap().dropped_turns, 1);
    conn.close();
    server.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_enforces_bearer_token() {
    let mut opts = serve_opts(make_create_session(&["ok"]));
    opts.token = Some("sekret".to_string());
    let server = serve(opts).await.unwrap();

    assert!(connect(&server.url, ConnectOptions::default())
        .await
        .is_err());
    assert!(connect(
        &server.url,
        ConnectOptions {
            token: Some("nope".into()),
            ..Default::default()
        }
    )
    .await
    .is_err());

    let conn = connect(
        &server.url,
        ConnectOptions {
            token: Some("sekret".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(has_completed(&run_turn(&conn.client, "go").await));
    conn.close();
    server.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_rejects_disallowed_origin() {
    let guarded = serve(serve_opts(make_create_session(&[]))).await.unwrap();
    assert!(connect(
        &guarded.url,
        ConnectOptions {
            origin: Some("http://evil.example".into()),
            ..Default::default()
        }
    )
    .await
    .is_err());
    guarded.close().await;

    let mut opts = serve_opts(make_create_session(&["allowed"]));
    opts.allowed_origins = Some(vec!["http://app.example".to_string()]);
    let permissive = serve(opts).await.unwrap();
    let conn = connect(
        &permissive.url,
        ConnectOptions {
            origin: Some("http://app.example".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(has_completed(&run_turn(&conn.client, "go").await));
    conn.close();
    permissive.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_run_to_completion_returns_structured_output() {
    let server = serve(serve_opts(make_create_session(&["{\"ok\":true}"])))
        .await
        .unwrap();
    let conn = connect(&server.url, ConnectOptions::default())
        .await
        .unwrap();
    let result = conn
        .client
        .run_to_completion(
            Input::Text("json".into()),
            Some(RunOpOptions {
                output_schema: Some(runtime_value(r#"{"type":"object"}"#)),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    assert_eq!(result.final_text, "{\"ok\":true}");
    assert_eq!(
        result.structured_output,
        Some(runtime_value(r#"{"ok":true}"#))
    );
    assert_eq!(result.stop_reason, TerminalReason::Completed);
    conn.close();
    server.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_steer_enqueues_and_take_pending_drains() {
    let server = serve(serve_opts(make_create_session(&["ok"])))
        .await
        .unwrap();
    let conn = connect(&server.url, ConnectOptions::default())
        .await
        .unwrap();
    conn.client
        .steer(Input::Text("go now".into()), Some(SteerPriority::Now))
        .await;
    let queued = conn.client.get_queued().await.unwrap();
    assert_eq!(queued.len(), 1);
    assert!(matches!(&queued[0].value, QueuedValue::Text(t) if t == "go now"));
    assert!(matches!(queued[0].priority, Some(QueuePriority::Now)));
    assert_eq!(conn.client.take_pending().await.unwrap().len(), 1);
    assert_eq!(conn.client.get_queued().await.unwrap().len(), 0);
    conn.close();
    server.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_history_cost_and_subagents_snapshots() {
    let server = serve(serve_opts(make_create_session(&["hi there"])))
        .await
        .unwrap();
    let conn = connect(&server.url, ConnectOptions::default())
        .await
        .unwrap();
    conn.client
        .run_to_completion(Input::Text("hello".into()), None)
        .await
        .unwrap();
    let history = conn.client.get_history().await.unwrap();
    assert_retained_skill_catalog(&history);
    assert_eq!(user_turns(&history), 1);
    assert!(conn.client.get_cost().await.unwrap().total_cost_usd >= 0.0);
    assert_eq!(conn.client.get_subagents().await.unwrap().len(), 0);
    conn.close();
    server.close().await;
}

// --- HTTP + SSE transport -------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_sse_drives_a_session_end_to_end() {
    let server = serve(serve_opts(make_create_session(&["hello over sse"])))
        .await
        .unwrap();
    let base = format!("http://127.0.0.1:{}", server.port);
    let conn = connect_http(&base, ConnectHttpOptions::default())
        .await
        .unwrap();
    let events = run_turn(&conn.client, "hi").await;
    assert!(delta_text(&events).contains("hello over sse"));
    assert!(has_completed(&events));
    conn.close();
    server.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_sse_server_close_waits_for_protocol_session_teardown() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let server = Arc::new(
        serve(serve_opts(make_gated_create_session(
            "closing sse",
            entered.clone(),
            release.clone(),
        )))
        .await
        .unwrap(),
    );
    let base = format!("http://127.0.0.1:{}", server.port);
    let conn = connect_http(&base, ConnectHttpOptions::default())
        .await
        .unwrap();
    assert!(has_completed(&run_turn(&conn.client, "hi").await));

    let first_server = server.clone();
    let mut first_close = tokio::spawn(async move { first_server.close().await });
    tokio::time::timeout(TRANSPORT_LIVENESS_TIMEOUT, entered.notified())
        .await
        .expect("SSE SessionEnd teardown started");
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut first_close)
            .await
            .is_err(),
        "RunningServer::close returned before SSE Session teardown"
    );
    let second_server = server.clone();
    let mut second_close = tokio::spawn(async move { second_server.close().await });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut second_close)
            .await
            .is_err(),
        "concurrent SSE close did not join the retained teardown"
    );
    first_close.abort();
    assert!(first_close.await.unwrap_err().is_cancelled());
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut second_close)
            .await
            .is_err(),
        "cancelling the initiating close orphaned SSE teardown"
    );
    release.notify_one();
    tokio::time::timeout(TRANSPORT_LIVENESS_TIMEOUT, second_close)
        .await
        .expect("SSE server close timed out")
        .expect("SSE server close task panicked");
    drop(conn);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_close_waits_for_sse_pre_registration_cleanup() {
    let gate = Arc::new(EndpointTestGate::default());
    let mut opts = serve_opts(make_create_session(&[]));
    opts.teardown_test_gates.sse_before_registration = Some(gate.clone());
    let server = Arc::new(serve(opts).await.unwrap());
    let base = format!("http://127.0.0.1:{}", server.port);
    let connect_task =
        tokio::spawn(async move { connect_http(&base, ConnectHttpOptions::default()).await });

    assert_retained_close_waits_for_gate(server, gate, "SSE pre-registration cleanup").await;
    let result = tokio::time::timeout(TRANSPORT_LIVENESS_TIMEOUT, connect_task)
        .await
        .expect("SSE connect task timed out")
        .expect("SSE connect task panicked");
    assert!(
        result.is_err(),
        "SSE request admitted after endpoint close began"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_sse_serves_event_stream_content_type() {
    let server = serve(serve_opts(make_create_session(&[]))).await.unwrap();
    let base = format!("http://127.0.0.1:{}", server.port);
    let http = reqwest::Client::new();
    let res = tokio::time::timeout(
        TRANSPORT_LIVENESS_TIMEOUT,
        http.get(format!("{base}/sessions/probe/events"))
            .header("accept", "text/event-stream")
            .send(),
    )
    .await
    .expect("SSE handshake timed out")
    .unwrap();
    assert_eq!(res.status().as_u16(), 200);
    assert!(res
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap()
        .contains("text/event-stream"));
    drop(res); // close the stream (server tears the session down on disconnect)
    server.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_rejects_oversized_op_body_413() {
    let server = serve(serve_opts(make_create_session(&[]))).await.unwrap();
    let base = format!("http://127.0.0.1:{}", server.port);
    let conn = connect_http(&base, ConnectHttpOptions::default())
        .await
        .unwrap();
    let big = "x".repeat(2 * 1024 * 1024); // 2 MiB, over the 1 MiB cap
    let http = reqwest::Client::new();
    let res = http
        .post(format!("{base}/sessions/{}/ops", conn.session_id))
        .header("content-type", "application/json")
        .body(big)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status().as_u16(), 413);
    conn.close();
    server.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_404_for_ops_on_unopened_session() {
    let server = serve(serve_opts(make_create_session(&[]))).await.unwrap();
    let base = format!("http://127.0.0.1:{}", server.port);
    let http = reqwest::Client::new();
    let res = http
        .post(format!("{base}/sessions/never-opened/ops"))
        .header("content-type", "application/json")
        .body(json!({ "id": "x", "op": { "type": "interrupt" } }).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(res.status().as_u16(), 404);
    server.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_malformed_session_id_is_a_clean_400() {
    let server = serve(serve_opts(make_create_session(&["still alive"])))
        .await
        .unwrap();
    let base = format!("http://127.0.0.1:{}", server.port);
    let http = reqwest::Client::new();
    let bad = http
        .get(format!("{base}/sessions/%zz/events"))
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status().as_u16(), 400);
    let bad_ops = http
        .post(format!("{base}/sessions/%zz/ops"))
        .header("content-type", "application/json")
        .body(json!({ "id": "x", "op": { "type": "interrupt" } }).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(bad_ops.status().as_u16(), 400);
    // The server survived: a normal session still works afterwards.
    let conn = connect_http(&base, ConnectHttpOptions::default())
        .await
        .unwrap();
    assert!(has_completed(&run_turn(&conn.client, "go").await));
    conn.close();
    server.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_run_to_completion_structured_output() {
    let server = serve(serve_opts(make_create_session(&["{\"n\":7}"])))
        .await
        .unwrap();
    let base = format!("http://127.0.0.1:{}", server.port);
    let conn = connect_http(&base, ConnectHttpOptions::default())
        .await
        .unwrap();
    let result = conn
        .client
        .run_to_completion(
            Input::Text("json".into()),
            Some(RunOpOptions {
                output_schema: Some(runtime_value(r#"{"type":"object"}"#)),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    assert_eq!(result.structured_output, Some(runtime_value(r#"{"n":7}"#)));
    conn.close();
    server.close().await;
}

// --- server basics --------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn answers_http_health_probes_with_200_and_404s_others() {
    let server = serve(serve_opts(make_create_session(&[]))).await.unwrap();
    let base = format!("http://127.0.0.1:{}", server.port);
    let http = reqwest::Client::new();
    for path in ["/healthz", "/readyz"] {
        let res = http.get(format!("{base}{path}")).send().await.unwrap();
        assert_eq!(res.status().as_u16(), 200);
        assert_eq!(res.text().await.unwrap(), "ok");
    }
    let other = http.get(format!("{base}/whatever")).send().await.unwrap();
    assert_eq!(other.status().as_u16(), 404);
    server.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refuses_non_loopback_listener_without_a_token() {
    let mut opts = serve_opts(make_create_session(&[]));
    opts.host = Some("0.0.0.0".to_string());
    match serve(opts).await {
        Err(ServeError::BindPolicy(msg)) => assert!(msg.to_lowercase().contains("token")),
        Err(e) => panic!("expected a BindPolicy error, got {e:?}"),
        Ok(_) => panic!("expected a BindPolicy error, but the server started"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn collapses_an_empty_host_to_loopback() {
    let mut opts = serve_opts(make_create_session(&[]));
    opts.host = Some(String::new());
    let server = serve(opts).await.unwrap();
    assert_eq!(server.host, "127.0.0.1");
    server.close().await;
}

// --- lifecycle over the wire (engineSessions + store) ---------------------

/// Poll the store until a persisted thread appears, returning its id.
async fn wait_for_thread(store: &Arc<dyn SessionStore>) -> String {
    for _ in 0..HISTORY_READABLE_ATTEMPTS {
        let page = store.list(ListOptions::default()).await.unwrap();
        if let Some(first) = page.items.first() {
            return first
                .id
                .try_to_utf8()
                .expect("persisted integration-test thread id must be scalar");
        }
        tokio::time::sleep(HISTORY_READABLE_INTERVAL).await;
    }
    panic!("no persisted thread ever appeared");
}

/// Connect with `opts`, read the history's user-turn count, retrying while the
/// prior connection's writer lock is still being released after its close.
async fn user_turns_via(url: &str, opts: ConnectOptions) -> usize {
    for _ in 0..HISTORY_READABLE_ATTEMPTS {
        let conn = connect(url, opts.clone()).await.unwrap();
        match conn.client.get_history().await {
            Ok(history) => {
                assert_retained_skill_catalog(&history);
                let n = user_turns(&history);
                conn.close();
                return n;
            }
            Err(_) => {
                conn.close();
                tokio::time::sleep(HISTORY_READABLE_INTERVAL).await;
            }
        }
    }
    panic!("history never became readable");
}

/// True when the store refused an op only because a connection that has already
/// closed is still handing back the thread's rollout writer lock.
///
/// Teardown is asynchronous, so an op aimed at a thread a just-closed connection
/// was bound to can land inside that window. Retrying only THIS error is what
/// keeps the retries below from also papering over a wiring failure.
fn rollout_lock_contention(error: &str) -> bool {
    error.contains("is locked by another writer")
}

/// Resume `thread_id` as an OP on a fresh connection, and count the user turns
/// that connection can then read.
///
/// Retried like [`user_turns_via`], because [`wait_for_thread`] only proves the
/// rollout FILE appeared: the seeding connection may still hold its writer lock.
async fn user_turns_via_resume_op(url: &str, thread_id: &str) -> usize {
    for _ in 0..HISTORY_READABLE_ATTEMPTS {
        let conn = connect(url, ConnectOptions::default()).await.unwrap();
        match conn.client.resume_session(thread_id.to_string()).await {
            Ok(bound) => {
                assert_eq!(
                    bound.thread_id, *thread_id,
                    "resume_session bound a thread other than the one asked for"
                );
                // Proof it is the SAME thread and not a fresh one wearing its id.
                if let Ok(history) = conn.client.get_history().await {
                    assert_retained_skill_catalog(&history);
                    let turns = user_turns(&history);
                    conn.close();
                    return turns;
                }
            }
            Err(error) => {
                let error = error.to_string();
                assert!(
                    rollout_lock_contention(&error),
                    "resume_session must reach the store: {error}"
                );
            }
        }
        conn.close();
        tokio::time::sleep(HISTORY_READABLE_INTERVAL).await;
    }
    panic!("resume by op never succeeded");
}

/// Retitle `thread_id` as an OP, riding out the same writer-lock handback.
async fn title_via_op(conn: &ClientConnection, thread_id: &str, title: &str) {
    for _ in 0..HISTORY_READABLE_ATTEMPTS {
        match conn
            .client
            .update_title(thread_id.to_string(), title.to_string())
            .await
        {
            Ok(_) => return,
            Err(error) => {
                let error = error.to_string();
                assert!(
                    rollout_lock_contention(&error),
                    "update_title must reach the store: {error}"
                );
                tokio::time::sleep(HISTORY_READABLE_INTERVAL).await;
            }
        }
    }
    panic!("update_title never got the rollout writer lock");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lifecycle_list_resume_and_fork_over_the_wire() {
    let dir = unique_temp_dir("lifecycle");
    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let agent = make_agent(crate::test_process_context(), ".", vec![text_turn("one")]);
    let sessions = engine_sessions(agent.clone(), Some(store.clone()));
    let server = serve(ServeOptions {
        windows_sandbox_context: None,
        create_session: sessions.create_session,
        credential_store: None,
        list_threads: sessions.list_threads,
        // Wired: this exercises the served transport reaching the same lifecycle
        // the in-process one does, alongside the connect-time resume below.
        lifecycle: Some(crate::protocol_lifecycle(agent, Some(store.clone()))),
        plugin_admin: None,
        host: None,
        port: None,
        token: None,
        allowed_origins: None,
        silent: true,
        teardown_test_gates: EndpointTestGates::default(),
    })
    .await
    .unwrap();

    // Fresh persisted session: run once, then close so the rollout is flushed.
    let c1 = connect(&server.url, ConnectOptions::default())
        .await
        .unwrap();
    c1.client
        .run_to_completion(Input::Text("first".into()), None)
        .await
        .unwrap();
    c1.close();

    let thread_id = wait_for_thread(&store).await;

    // list over the wire (GET /threads) surfaces the persisted thread.
    let base = format!("http://127.0.0.1:{}", server.port);
    let page = list_threads(&base, None).await.unwrap();
    let items = page
        .get("items")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(items
        .iter()
        .any(|t| t.get("id").and_then(|v| v.as_str()) == Some(thread_id.as_str())));

    // resume: the prior turn's history carries over.
    let resumed = user_turns_via(
        &server.url,
        ConnectOptions {
            resume: Some(thread_id.clone()),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(resumed, 1);

    // fork: a NEW session seeded from that thread's history.
    let forked = user_turns_via(
        &server.url,
        ConnectOptions {
            fork: Some(thread_id.clone()),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(forked, 1);
    let fork_summary = {
        let mut found = None;
        for _ in 0..HISTORY_READABLE_ATTEMPTS {
            let page = store.list(ListOptions::default()).await.unwrap();
            if let Some(summary) = page.items.into_iter().find(|item| item.id != thread_id) {
                found = Some(summary);
                break;
            }
            tokio::time::sleep(HISTORY_READABLE_INTERVAL).await;
        }
        found.expect("the forked thread never appeared in the store")
    };
    assert_eq!(
        fork_summary.forked_from_id.as_ref(),
        Some(&RuntimeString::from(thread_id.as_str()))
    );
    let wire_page = list_threads(&base, None).await.unwrap();
    let fork_id = fork_summary
        .id
        .try_to_utf8()
        .expect("persisted integration-test fork id must be scalar");
    let wire_fork = wire_page["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"].as_str() == Some(fork_id.as_str()))
        .unwrap();
    assert_eq!(wire_fork["forkedFromId"], thread_id);

    server.close().await;
    std::fs::remove_dir_all(&dir).ok();
}

/// A served connection must reach the lifecycle as OPS, not just through the
/// connect URL and `GET /threads`.
///
/// The connect-time form settles identity before the socket exists, which is what
/// a browser needs and all a browser can do. It is not enough for a client that
/// is already connected: enumerating threads, or moving to another one, or
/// renaming the current one, are things a session asks for mid-flight. Wiring
/// only the URL form left every one of those answering "requires a session
/// store" against an endpoint that owns one, which made `--remote` a quietly
/// lesser client than the same code reached in process.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lifecycle_ops_reach_the_store_over_the_wire() {
    let dir = unique_temp_dir("lifecycle-ops");
    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let agent = make_agent(crate::test_process_context(), ".", vec![text_turn("one")]);
    let sessions = engine_sessions(agent.clone(), Some(store.clone()));
    let server = serve(ServeOptions {
        windows_sandbox_context: None,
        create_session: sessions.create_session,
        credential_store: None,
        list_threads: sessions.list_threads,
        lifecycle: Some(crate::protocol_lifecycle(agent, Some(store.clone()))),
        plugin_admin: None,
        host: None,
        port: None,
        token: None,
        allowed_origins: None,
        silent: true,
        teardown_test_gates: EndpointTestGates::default(),
    })
    .await
    .unwrap();

    // Persist a thread the plain way, so the ops below have something real to
    // find rather than an empty store that would pass every assertion vacuously.
    let seed = connect(&server.url, ConnectOptions::default())
        .await
        .unwrap();
    seed.client
        .run_to_completion(Input::Text("first".into()), None)
        .await
        .unwrap();
    seed.close();
    let thread_id = wait_for_thread(&store).await;

    // Connections from here on carry NO lifecycle intent in their URL, so every
    // assertion below is about the ops alone.
    let conn = connect(&server.url, ConnectOptions::default())
        .await
        .unwrap();

    let page = conn.client.list_sessions(None).await.unwrap();
    assert!(
        page.items.iter().any(|item| item.id == *thread_id),
        "list_sessions must see the persisted thread"
    );

    title_via_op(&conn, &thread_id, "titled over the wire").await;

    conn.close();

    assert_eq!(
        user_turns_via_resume_op(&server.url, &thread_id).await,
        1,
        "resume_session must bind the persisted thread, seeded turn and all"
    );

    server.close().await;
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lifecycle_resume_of_a_nonexistent_thread_rejects() {
    let dir = unique_temp_dir("lifecycle");
    let store: Arc<dyn SessionStore> = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let agent = make_agent(
        crate::test_process_context(),
        ".",
        vec![text_turn("unused")],
    );
    let sessions = engine_sessions(agent, Some(store));
    let server = serve(ServeOptions {
        windows_sandbox_context: None,
        create_session: sessions.create_session,
        credential_store: None,
        list_threads: sessions.list_threads,
        lifecycle: None,
        plugin_admin: None,
        host: None,
        port: None,
        token: None,
        allowed_origins: None,
        silent: true,
        teardown_test_gates: EndpointTestGates::default(),
    })
    .await
    .unwrap();

    // No such thread → createSession rejects. The query must REJECT with the
    // correlated error, not hang forever awaiting a reply that never comes.
    let conn = connect(
        &server.url,
        ConnectOptions {
            resume: Some("does-not-exist".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let err = conn.client.get_history().await.unwrap_err();
    let msg = format!("{err}").to_lowercase();
    assert!(msg.contains("not found") || msg.contains("does-not-exist"));
    conn.close();
    server.close().await;
    std::fs::remove_dir_all(&dir).ok();
}
