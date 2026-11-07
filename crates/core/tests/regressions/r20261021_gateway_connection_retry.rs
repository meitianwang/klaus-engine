//! Defect: gateway connection failures ended the turn without another request.
//! Root cause: status 408 became Unknown and bypassed both retry policies.
//! Work item: 20261021 bounded gateway connection recovery.

use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::deps::NoopCompactor;
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::provider::observability::ProviderDecisionEventKind;
use klaus_agent_core::provider::openai::{OpenAIProvider, OpenAIProviderOptions, OpenAiApi};
use klaus_agent_core::provider::retry::RetryOptions;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::runtime_codec;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::TerminalReason;
use klaus_agent_core::types::events::EngineEvent;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MESSAGES: [&str; 3] = [
    "connection unknown error",
    "cygnet return illegal packet",
    "connection closed",
];

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

async fn server(responses: Vec<String>) -> Server {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut received = Vec::new();
            let mut buffer = [0; 4096];
            loop {
                let count = socket.read(&mut buffer).await.unwrap();
                assert!(count > 0, "the full request body must arrive");
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
            let index = seen.fetch_add(1, Ordering::SeqCst);
            let response = &responses[index.min(responses.len() - 1)];
            // Cancellation can close an in-flight request before its reply.
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

fn response(status: u16, content_type: &str, body: String) -> String {
    format!(
        "HTTP/1.1 {status} Fixture\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

fn failure(status: u16, code: &str, message: &str, streamed: bool) -> String {
    let error = json!({
        "error_code": status,
        "error": {"message": message, "code": code, "type": code, "request_id":"req_gateway"}
    });
    if streamed {
        let prefix = if message == MESSAGES[1] {
            format!(
                "data: {}\n\n",
                json!({"choices":[{"delta":{"role":"assistant","content":"Partial"},"finish_reason":null}]})
            )
        } else {
            String::new()
        };
        response(
            200,
            "text/event-stream",
            format!("{prefix}data: {error}\n\n"),
        )
    } else {
        response(status, "application/json", error.to_string())
    }
}

fn success() -> String {
    let chunks = [
        json!({"model":"gateway-retry-fixture","choices":[{"delta":{"role":"assistant","content":"Recovered"},"finish_reason":null}]}),
        json!({"choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":1,"total_tokens":11}}),
    ];
    response(
        200,
        "text/event-stream",
        chunks
            .iter()
            .map(|chunk| format!("data: {chunk}\n\n"))
            .collect::<String>()
            + "data: [DONE]\n\n",
    )
}

async fn run(
    server: &Server,
    max_retries: u32,
    establishment_attempts: u32,
    cancel_retry: bool,
) -> Vec<EngineEvent> {
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let root = std::env::temp_dir().join(format!("klaus-gateway-retry-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
    options.cwd = Some(root.to_string_lossy().into_owned());
    options.compactor = Some(Arc::new(NoopCompactor));
    options
        .config
        .as_mut()
        .unwrap()
        .as_object_mut()
        .unwrap()
        .extend(
            json!({
                "model":"gateway-retry-fixture", "contextWindow":1_000_000,
                "maxOutputTokens":1000, "maxRetries":max_retries,
                "prewarm":false, "autoMemory":false, "agentMemory":false,
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
            max_attempts: Some(establishment_attempts),
            base_delay_ms: Some(1),
            jitter: Some(0.0),
            congestion_hold_ms: Some(0),
            unbounded_network_retries: Some(false),
            ..Default::default()
        }),
        ..Default::default()
    }));
    let session = Agent::new(options).start_session(StartSessionOptions {
        user_config_dir: Some(root.to_string_lossy().into_owned().into()),
        ..Default::default()
    });
    let signal = AbortSignal::new();
    let mut events = session
        .run_streamed(
            Input::Text("Return a short answer.".into()),
            TurnOptions {
                signal: Some(signal.clone()),
                ..Default::default()
            },
        )
        .events;
    let captured = tokio::time::timeout(Duration::from_secs(10), async {
        let mut captured = Vec::new();
        while let Some(event) = events.next().await {
            if cancel_retry
                && matches!(&event, EngineEvent::ProviderDecision { event, .. }
                    if event.kind == ProviderDecisionEventKind::RetryScheduled)
            {
                signal.cancel();
            }
            captured.push(event);
        }
        captured
    })
    .await
    .expect("the bounded retry turn must finish");
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
    captured
}

fn terminal_error(events: &[EngineEvent]) -> Value {
    events
        .iter()
        .find_map(|event| match event {
            EngineEvent::TurnFailed { error, .. } => {
                Some(serde_json::from_slice(&runtime_codec::encode_bytes(error).unwrap()).unwrap())
            }
            _ => None,
        })
        .expect("the failed turn retains its diagnostics")
}

fn assert_completed(events: &[EngineEvent]) {
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
    assert!(
        events.iter().any(|event| matches!(event,
            EngineEvent::TurnCompleted { stop_reason, .. }
                if *stop_reason == TerminalReason::Completed
        )),
        "{events:?}"
    );
    assert!(
        events.iter().any(|event| matches!(event,
            EngineEvent::TextDelta { text, .. } if text == "Recovered"
        )),
        "{events:?}"
    );
}

#[tokio::test]
async fn gateway_http_and_stream_errors_issue_real_requests_until_recovered() {
    for streamed in [false, true] {
        for message in MESSAGES {
            let error = failure(408, "model_proxy_ultra_inner_error", message, streamed);
            let server = server(vec![error.clone(), error, success()]).await;
            let events = run(&server, 2, 1, false).await;
            assert_eq!(
                server.calls.load(Ordering::SeqCst),
                3,
                "{message}; streamed={streamed}"
            );
            assert_completed(&events);
            if streamed && message == MESSAGES[1] {
                assert_eq!(
                    events
                        .iter()
                        .filter(|event| matches!(event, EngineEvent::Tombstone { .. }))
                        .count(),
                    2
                );
            }
        }
    }
}

#[tokio::test]
async fn gateway_http_establishment_retries_reach_the_server() {
    let error = failure(408, "model_proxy_inner_error", "connection closed", false);
    let server = server(vec![error.clone(), error, success()]).await;
    let events = run(&server, 0, 3, false).await;
    assert_eq!(server.calls.load(Ordering::SeqCst), 3);
    assert_completed(&events);
}

#[tokio::test]
async fn persistent_gateway_errors_stop_at_the_retry_budget_and_keep_details() {
    let server = server(vec![failure(
        408,
        "model_proxy_ultra_inner_error",
        "connection closed",
        true,
    )])
    .await;
    let events = run(&server, 2, 1, false).await;
    assert_eq!(server.calls.load(Ordering::SeqCst), 3);
    let error = terminal_error(&events);
    assert_eq!(error["providerError"]["kind"], "timeout");
    assert_eq!(error["providerError"]["status"], 408);
    assert_eq!(
        error["providerError"]["code"],
        "model_proxy_ultra_inner_error"
    );
    assert_eq!(error["requestId"], "req_gateway");
    assert_eq!(error["recoveryReason"], "retries-exhausted");
}

#[tokio::test]
async fn persistent_http_failures_respect_both_retry_limits() {
    let server = server(vec![failure(
        408,
        "model_proxy_inner_error",
        "connection closed",
        false,
    )])
    .await;
    let events = run(&server, 2, 2, false).await;
    assert_eq!(server.calls.load(Ordering::SeqCst), 6);
    assert_eq!(
        terminal_error(&events)["recoveryReason"],
        "retries-exhausted"
    );
}

#[tokio::test]
async fn disabled_retries_are_reported_without_extra_requests() {
    let server = server(vec![failure(
        408,
        "model_proxy_ultra_inner_error",
        "connection closed",
        true,
    )])
    .await;
    let events = run(&server, 0, 1, false).await;
    assert_eq!(server.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        terminal_error(&events)["recoveryReason"],
        "retries-disabled"
    );
}

#[tokio::test]
async fn permanent_errors_do_not_retry_or_claim_retry_exhaustion() {
    for (status, code) in [
        (400, "invalid_request_error"),
        (401, "invalid_api_key"),
        (403, "permission_error"),
        (429, "insufficient_quota"),
        (408, "invalid_api_key"),
        (408, "insufficient_quota"),
    ] {
        let server = server(vec![
            failure(status, code, "Fixture refusal", false),
            success(),
        ])
        .await;
        let events = run(&server, 2, 3, false).await;
        assert_eq!(server.calls.load(Ordering::SeqCst), 1, "{status}: {code}");
        if !matches!(code, "invalid_api_key" | "permission_error") {
            assert_eq!(terminal_error(&events)["recoveryReason"], "not-retryable");
        }
    }
}

#[tokio::test]
async fn cancelling_a_scheduled_retry_sends_no_further_requests() {
    let error = failure(
        408,
        "model_proxy_ultra_inner_error",
        "connection closed",
        false,
    )
    .replace("Content-Type:", "Retry-After: 10\r\nContent-Type:");
    for establishment_attempts in [1, 3] {
        let server = server(vec![error.clone(), success()]).await;
        let events = run(&server, 2, establishment_attempts, true).await;
        assert_eq!(server.calls.load(Ordering::SeqCst), 1);
        assert!(
            events.iter().any(|event| matches!(event,
                EngineEvent::TurnCompleted { stop_reason, .. }
                    if *stop_reason == TerminalReason::Aborted
            )),
            "{events:?}"
        );
    }
}
