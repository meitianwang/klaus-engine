//! Defect: (1) Anthropic silent/encrypted thinking exceeding `streamIdleTimeoutMs`
//! canceled live SSE streams and surfaced `ProviderErrorKind::Network` instead of
//! `ProviderErrorKind::Timeout`; (2) `CONTEXT_HINT_BETA` HTTP 400 rejections only
//! stripped the header for one physical retry and re-uploaded it on every later request.
//! Root cause: `RequestTiming::next_event` only renewed `idle_deadline` on emitted
//! `ModelStreamEvent`s while `AnthropicProvider` dropped SSE `ping` and encrypted
//! thinking frames, `streaming.rs` tagged `stream_idle_timeout` as `Network`, and
//! `StripAndRetry` did not record endpoint/session beta refusal state.
//! Fixed by: 2026-11-21 Anthropic SSE liveness renewal, timeout kind, and hint-beta learning.

use klaus_agent_core::context::context_hints::CONTEXT_HINT_BETA;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::provider::anthropic::{AnthropicProvider, AnthropicProviderOptions};
use klaus_agent_core::provider::retry::RetryOptions;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::types::errors::ProviderErrorKind;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn read_http_request(socket: &mut TcpStream) -> Option<(String, String)> {
    let mut bytes = Vec::new();
    loop {
        let mut buffer = [0u8; 8192];
        let count = socket.read(&mut buffer).await.ok()?;
        if count == 0 {
            return None;
        }
        bytes.extend_from_slice(&buffer[..count]);
        if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&bytes[..end]).into_owned();
            let length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or(0);
            if bytes.len() < end + 4 + length {
                continue;
            }
            let body = String::from_utf8_lossy(&bytes[end + 4..end + 4 + length]).into_owned();
            return Some((headers, body));
        }
    }
}

fn is_messages_post(headers: &str) -> bool {
    headers
        .lines()
        .next()
        .is_some_and(|line| line.starts_with("POST /v1/messages"))
}

async fn reply_prewarm(socket: &mut TcpStream) {
    let _ = socket
        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        .await;
    let _ = socket.shutdown().await;
}

async fn write_chunk(socket: &mut TcpStream, text: &str) -> std::io::Result<()> {
    let header = format!("{:x}\r\n", text.len());
    socket.write_all(header.as_bytes()).await?;
    socket.write_all(text.as_bytes()).await?;
    socket.write_all(b"\r\n").await?;
    socket.flush().await
}

async fn finish_chunked(socket: &mut TcpStream) -> std::io::Result<()> {
    socket.write_all(b"0\r\n\r\n").await?;
    socket.flush().await
}

#[tokio::test]
async fn public_session_survives_silent_thinking_exceeding_idle_timeout_while_sse_is_alive() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let requests = Arc::new(AtomicUsize::new(0));
    let server_requests = requests.clone();

    let server = tokio::spawn(async move {
        while let Ok(Ok((mut socket, _))) =
            tokio::time::timeout(Duration::from_millis(800), listener.accept()).await
        {
            let Some((headers, _)) = read_http_request(&mut socket).await else {
                continue;
            };
            if !is_messages_post(&headers) {
                reply_prewarm(&mut socket).await;
                continue;
            }
            server_requests.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let _ = socket
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
                    )
                    .await;
                let _ = write_chunk(
                    &mut socket,
                    "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-opus-5-5\",\"usage\":{\"input_tokens\":100,\"output_tokens\":0}}}\n\n",
                )
                .await;
                let _ = write_chunk(
                    &mut socket,
                    "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\",\"signature\":\"\"}}\n\n",
                )
                .await;

                // Stream keepalive pings and encrypted signature deltas for ~225ms total,
                // well past `streamIdleTimeoutMs = 90ms`, without emitting plaintext deltas.
                for step in 0..5 {
                    tokio::time::sleep(Duration::from_millis(45)).await;
                    let frame = if step == 3 {
                        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"signature_delta\",\"signature\":\"opaque-sig-part\"}}\n\n"
                    } else {
                        "event: ping\ndata: {\"type\":\"ping\"}\n\n"
                    };
                    if write_chunk(&mut socket, frame).await.is_err() {
                        return;
                    }
                }

                let _ = write_chunk(
                    &mut socket,
                    concat!(
                        "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
                        "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
                        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"text_delta\",\"text\":\"Silent thinking completed.\"}}\n\n",
                        "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":1}\n\n",
                        "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":12}}\n\n",
                        "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
                    ),
                )
                .await;
                let _ = finish_chunked(&mut socket).await;
            });
        }
    });

    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let root = std::env::temp_dir().join(format!(
        "klaus-silent-thinking-sse-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&root).unwrap();

    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
    options.cwd = Some(root.to_string_lossy().into_owned());
    options.config.as_mut().unwrap()["model"] = json!("claude-opus-5-5");
    options.config.as_mut().unwrap()["streamIdleTimeoutMs"] = json!(90);
    options.config.as_mut().unwrap()["streamTotalTimeoutMs"] = json!(2_000);
    options.config.as_mut().unwrap()["maxRetries"] = json!(0);
    options.config.as_mut().unwrap()["agentMemory"] = json!(false);
    options.config.as_mut().unwrap()["autoMemory"] = json!(false);
    options.provider = Arc::new(AnthropicProvider::new(AnthropicProviderOptions {
        base_url: Some(base),
        api_key: Some("fixture-only".into()),
        default_model: Some("claude-opus-5-5".into()),
        retry: Some(RetryOptions {
            max_attempts: Some(1),
            congestion_hold_ms: Some(0),
            unbounded_network_retries: Some(false),
            ..Default::default()
        }),
        ..Default::default()
    }));

    let session = Agent::new(options).start_session(StartSessionOptions {
        config: Some(json!({
            "streamIdleTimeoutMs": 90,
            "streamTotalTimeoutMs": 2_000,
            "maxRetries": 0,
        })),
        ..StartSessionOptions::default()
    });
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        session.run(
            Input::Text("Think silently.".into()),
            TurnOptions::default(),
        ),
    )
    .await
    .expect("public turn must settle");
    assert!(
        result.error.is_none(),
        "unexpected error: {:?}",
        result.error
    );
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    assert_eq!(result.final_text, "Silent thinking completed.");

    session.shutdown_and_wait().await.unwrap();
    let _ = server.await;
    process.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn public_session_reports_timeout_kind_and_honors_idle_retry_budget_on_dead_stream() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let requests = Arc::new(AtomicUsize::new(0));
    let server_requests = requests.clone();

    let server = tokio::spawn(async move {
        while let Ok(Ok((mut socket, _))) =
            tokio::time::timeout(Duration::from_millis(1_500), listener.accept()).await
        {
            let Some((headers, _)) = read_http_request(&mut socket).await else {
                continue;
            };
            if !is_messages_post(&headers) {
                reply_prewarm(&mut socket).await;
                continue;
            }
            server_requests.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let _ = socket
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
                    )
                    .await;
                let _ = write_chunk(
                    &mut socket,
                    "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-opus-5-5\",\"usage\":{\"input_tokens\":50,\"output_tokens\":0}}}\n\n",
                )
                .await;
                tokio::time::sleep(Duration::from_millis(500)).await;
                drop(socket);
            });
        }
    });

    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let root = std::env::temp_dir().join(format!(
        "klaus-dead-stream-timeout-kind-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&root).unwrap();

    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
    options.cwd = Some(root.to_string_lossy().into_owned());
    options.config.as_mut().unwrap()["model"] = json!("claude-opus-5-5");
    options.config.as_mut().unwrap()["streamIdleTimeoutMs"] = json!(50);
    options.config.as_mut().unwrap()["streamTotalTimeoutMs"] = json!(5_000);
    options.config.as_mut().unwrap()["maxRetries"] = json!(2);
    options.config.as_mut().unwrap()["agentMemory"] = json!(false);
    options.config.as_mut().unwrap()["autoMemory"] = json!(false);
    options.provider = Arc::new(AnthropicProvider::new(AnthropicProviderOptions {
        base_url: Some(base),
        api_key: Some("fixture-only".into()),
        default_model: Some("claude-opus-5-5".into()),
        retry: Some(RetryOptions {
            max_attempts: Some(1),
            congestion_hold_ms: Some(0),
            unbounded_network_retries: Some(false),
            ..Default::default()
        }),
        ..Default::default()
    }));

    let session = Agent::new(options).start_session(StartSessionOptions {
        config: Some(json!({
            "streamIdleTimeoutMs": 50,
            "streamTotalTimeoutMs": 5_000,
            "maxRetries": 2,
        })),
        ..StartSessionOptions::default()
    });
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        session.run(
            Input::Text("Stall after start.".into()),
            TurnOptions::default(),
        ),
    )
    .await
    .expect("public turn must settle");
    let error = result.error.expect("truly silent stream must fail");
    assert!(
        error.message.contains("stream idle timeout"),
        "expected stream idle timeout message, got {:?}",
        error.message
    );
    assert_eq!(error.provider_kind, Some(ProviderErrorKind::Timeout));
    // Idle timeouts retain the full `maxRetries` budget (1 initial + 2 retries = 3),
    // unlike full-turn total timeouts which cap retries at 1.
    assert_eq!(requests.load(Ordering::SeqCst), 3);

    session.shutdown_and_wait().await.unwrap();
    let _ = server.await;
    process.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn public_session_persists_unsupported_context_hint_beta_across_turns() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let model_id = format!("claude-opus-5-5-hint-{}", uuid::Uuid::new_v4());
    let server_model = model_id.clone();

    let server = tokio::spawn(async move {
        let mut beta_flags = Vec::new();
        while let Ok(Ok((mut socket, _))) =
            tokio::time::timeout(Duration::from_millis(600), listener.accept()).await
        {
            let Some((headers, _body)) = read_http_request(&mut socket).await else {
                continue;
            };
            if !is_messages_post(&headers) {
                reply_prewarm(&mut socket).await;
                continue;
            }
            let has_hint_beta = headers
                .to_ascii_lowercase()
                .contains(&CONTEXT_HINT_BETA.to_ascii_lowercase());
            beta_flags.push(has_hint_beta);
            if has_hint_beta {
                let err_body = format!(
                    "{{\"type\":\"error\",\"error\":{{\"type\":\"invalid_request_error\",\"message\":\"Unexpected value '{CONTEXT_HINT_BETA}' for 'anthropic-beta' header\"}}}}"
                );
                let resp = format!(
                    "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{err_body}",
                    err_body.len()
                );
                socket.write_all(resp.as_bytes()).await.unwrap();
            } else {
                let ok_body = format!(
                    concat!(
                        "event: message_start\ndata: {{\"type\":\"message_start\",\"message\":{{\"model\":\"{}\",\"usage\":{{\"input_tokens\":20,\"output_tokens\":0}}}}}}\n\n",
                        "event: content_block_start\ndata: {{\"type\":\"content_block_start\",\"index\":0,\"content_block\":{{\"type\":\"text\",\"text\":\"\"}}}}\n\n",
                        "event: content_block_delta\ndata: {{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{{\"type\":\"text_delta\",\"text\":\"ok\"}}}}\n\n",
                        "event: content_block_stop\ndata: {{\"type\":\"content_block_stop\",\"index\":0}}\n\n",
                        "event: message_delta\ndata: {{\"type\":\"message_delta\",\"delta\":{{\"stop_reason\":\"end_turn\"}},\"usage\":{{\"output_tokens\":1}}}}\n\n",
                        "event: message_stop\ndata: {{\"type\":\"message_stop\"}}\n\n",
                    ),
                    server_model
                );
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{ok_body}",
                    ok_body.len()
                );
                socket.write_all(resp.as_bytes()).await.unwrap();
            }
            let _ = socket.shutdown().await;
        }
        beta_flags
    });

    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let root = std::env::temp_dir().join(format!(
        "klaus-context-hint-persist-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&root).unwrap();

    let mut provider = AnthropicProvider::new(AnthropicProviderOptions {
        base_url: Some(base),
        api_key: Some("fixture-only".into()),
        default_model: Some(model_id.clone()),
        retry: Some(RetryOptions {
            max_attempts: Some(2),
            congestion_hold_ms: Some(0),
            unbounded_network_retries: Some(false),
            sleep: Some(Arc::new(|_, _| Box::pin(async { Ok(()) }))),
            ..Default::default()
        }),
        ..Default::default()
    });
    provider.testing_set_first_party(true);

    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
    options.cwd = Some(root.to_string_lossy().into_owned());
    options.config.as_mut().unwrap()["model"] = json!(model_id);
    options.config.as_mut().unwrap()["contextHints"] = json!(true);
    options.config.as_mut().unwrap()["agentMemory"] = json!(false);
    options.config.as_mut().unwrap()["autoMemory"] = json!(false);
    options.provider = Arc::new(provider);

    let agent = Agent::new(options);
    let session = agent.start_session(StartSessionOptions {
        config: Some(json!({ "contextHints": true })),
        ..StartSessionOptions::default()
    });

    let turn1 = session
        .run(Input::Text("Turn one.".into()), TurnOptions::default())
        .await;
    assert!(turn1.error.is_none(), "turn 1 failed: {:?}", turn1.error);

    let turn2 = session
        .run(Input::Text("Turn two.".into()), TurnOptions::default())
        .await;
    assert!(turn2.error.is_none(), "turn 2 failed: {:?}", turn2.error);

    // A fresh session against the same endpoint + model also skips the rejected beta.
    let session2 = agent.start_session(StartSessionOptions {
        config: Some(json!({ "contextHints": true })),
        ..StartSessionOptions::default()
    });
    let turn3 = session2
        .run(Input::Text("Turn three.".into()), TurnOptions::default())
        .await;
    assert!(turn3.error.is_none(), "turn 3 failed: {:?}", turn3.error);

    session.shutdown_and_wait().await.unwrap();
    session2.shutdown_and_wait().await.unwrap();
    let beta_flags = server.await.unwrap();
    // Only the very first `/v1/messages` attempt should declare CONTEXT_HINT_BETA:
    // Turn 1: [true -> 400, false -> 200]; Turn 2: [false -> 200]; Session 2 Turn 3: [false -> 200].
    assert_eq!(beta_flags, vec![true, false, false, false]);

    process.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(root);
}
