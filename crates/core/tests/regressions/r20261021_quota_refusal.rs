//! Defect: a quota refusal appeared as an authentication failure.
//! Root cause: the pre-consumption quota code fell through to HTTP 403 auth.
//! Work item: 20261021 preserve quota refusal reasons in live and saved errors.

use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::deps::NoopCompactor;
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::provider::openai::{OpenAIProvider, OpenAIProviderOptions, OpenAiApi};
use klaus_agent_core::provider::retry::RetryOptions;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::runtime_codec;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::store::thread_history::project_thread_history;
use klaus_agent_core::types::errors::TerminalReason;
use klaus_agent_core::types::events::EngineEvent;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const QUOTA_CODE: &str = "pre_consume_token_quota_failed";
const QUOTA_MESSAGE: &str =
    "token quota is not enough, token remain quota: $0.449224, need quota: $0.476426";

struct Server {
    base_url: String,
    calls: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn server(status: u16, code: &str, message: &str, streamed: bool) -> Server {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let error = json!({
        "error_code": status,
        "error": {"message": message, "code": code, "type": "new_api_error", "request_id": "req_quota"}
    });
    let (http_status, content_type, body) = if streamed {
        (200, "text/event-stream", format!("data: {error}\n\n"))
    } else {
        (status, "application/json", error.to_string())
    };
    let response = format!(
        "HTTP/1.1 {http_status} Fixture\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut received = Vec::new();
            let mut buffer = [0; 4096];
            loop {
                let count = socket.read(&mut buffer).await.unwrap();
                assert!(count > 0, "the complete request must arrive");
                received.extend_from_slice(&buffer[..count]);
                let Some(end) = received.windows(4).position(|bytes| bytes == b"\r\n\r\n") else {
                    continue;
                };
                let headers = String::from_utf8_lossy(&received[..end]);
                let length = headers
                    .lines()
                    .filter_map(|line| line.split_once(':'))
                    .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                    .unwrap()
                    .1
                    .trim()
                    .parse::<usize>()
                    .unwrap();
                if received.len() >= end + 4 + length {
                    break;
                }
            }
            seen.fetch_add(1, Ordering::SeqCst);
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.shutdown().await;
        }
    });
    Server {
        base_url,
        calls,
        task,
    }
}

async fn run(server: &Server) -> (Vec<EngineEvent>, Value) {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let root = super::context_fixture::tempdir().unwrap();
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
    options.cwd = Some(root.path().to_string_lossy().into_owned());
    options.compactor = Some(Arc::new(NoopCompactor));
    options
        .config
        .as_mut()
        .unwrap()
        .as_object_mut()
        .unwrap()
        .extend(
            json!({
                "model":"quota-fixture", "contextWindow":1_000_000, "maxOutputTokens":1000,
                "maxRetries":2, "prewarm":false, "autoMemory":false, "agentMemory":false,
                "idleMicrocompact":false, "earlyMicrocompact":false,
                "speculativeCompaction":false, "loopDetection":false, "goal":false
            })
            .as_object()
            .unwrap()
            .clone(),
        );
    options.provider = Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
        api_key: Some("fixture-key".into()),
        base_url: Some(server.base_url.clone()),
        api: Some(OpenAiApi::Chat),
        retry: Some(RetryOptions {
            max_attempts: Some(3),
            base_delay_ms: Some(1),
            jitter: Some(0.0),
            congestion_hold_ms: Some(0),
            unbounded_network_retries: Some(false),
            ..Default::default()
        }),
        ..Default::default()
    }));
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.path().join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let session = Agent::new(options)
        .open_session(
            store.clone(),
            StartSessionOptions {
                user_config_dir: Some(root.path().to_string_lossy().into_owned().into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let events = tokio::time::timeout(
        Duration::from_secs(10),
        session
            .run_streamed(
                Input::Text("Answer briefly.".into()),
                TurnOptions::default(),
            )
            .events
            .collect(),
    )
    .await
    .expect("permanent refusals must finish without waiting for recovery");
    let live_history: Value = serde_json::from_slice(
        &runtime_codec::encode_bytes(&session.turn_index_snapshot()).unwrap(),
    )
    .unwrap();
    session.flush_rollout().await.unwrap();
    let snapshot = store
        .read_thread(&session.id.try_to_utf8().unwrap())
        .await
        .unwrap();
    let history: Value = serde_json::from_slice(
        &runtime_codec::encode_bytes(&project_thread_history(snapshot.raw_items)).unwrap(),
    )
    .unwrap();
    assert_eq!(history["turnErrors"], live_history["turnErrors"]);
    assert_eq!(
        history["turnErrorDetails"],
        live_history["turnErrorDetails"]
    );
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    (events, history)
}

#[tokio::test]
async fn quota_refusals_keep_the_original_reason_in_live_and_saved_errors() {
    for streamed in [false, true] {
        let server = server(403, QUOTA_CODE, QUOTA_MESSAGE, streamed).await;
        let (events, history) = run(&server).await;
        assert_eq!(server.calls.load(Ordering::SeqCst), 1);
        assert!(!events.iter().any(|event| matches!(
            event,
            EngineEvent::TurnCompleted {
                stop_reason: TerminalReason::AuthFailed,
                ..
            }
        )));
        let error: Value = events
            .iter()
            .find_map(|event| match event {
                EngineEvent::TurnFailed { error, .. } => Some(
                    serde_json::from_slice(&runtime_codec::encode_bytes(error).unwrap()).unwrap(),
                ),
                _ => None,
            })
            .expect("quota refusal must retain a typed failure instead of an auth terminal");
        assert_eq!(error["kind"], "quota");
        assert_eq!(error["message"], QUOTA_MESSAGE);
        assert_eq!(error["providerError"]["kind"], "quota");
        assert_eq!(error["providerError"]["status"], 403);
        assert_eq!(error["providerError"]["code"], QUOTA_CODE);
        assert_eq!(error["providerError"]["message"], QUOTA_MESSAGE);
        assert_eq!(error["requestId"], "req_quota");
        assert_eq!(error["recoveryReason"], "not-retryable");
        let saved = history["turnErrorDetails"].as_object().unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved.values().next().unwrap(), &error);
        assert_eq!(
            history["turnErrors"][saved.keys().next().unwrap()],
            QUOTA_MESSAGE
        );
    }
}

#[tokio::test]
async fn genuine_auth_refusals_keep_the_auth_terminal() {
    for (status, code) in [(401, "invalid_api_key"), (403, "permission_error")] {
        let server = server(status, code, "Invalid credentials", false).await;
        let (events, _) = run(&server).await;
        assert_eq!(server.calls.load(Ordering::SeqCst), 1);
        assert!(events.iter().any(|event| matches!(
            event,
            EngineEvent::TurnCompleted {
                stop_reason: TerminalReason::AuthFailed,
                ..
            }
        )));
    }
}
