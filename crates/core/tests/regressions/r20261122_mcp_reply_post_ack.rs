//! Defect: a streamable HTTP reply POST decoded any successful response body
//! and dispatched it to the shared message router. Both peers assign JSON-RPC
//! ids independently, so a server message riding on the acknowledgement of a
//! reply could falsely resolve an unrelated pending request with a colliding
//! id; a `204 No Content` acknowledgement also fell into empty-body parsing
//! and reported a spurious transport error.
//! Fixed by: teaching `HttpTransport::send` to recognize outgoing JSON-RPC
//! responses and accept only `202`/`204` acknowledgements for them, rejecting
//! any other successful status before decoding its body.

use klaus_agent_core::mcp::client::{Client, RequestOptions};
use klaus_agent_core::mcp::transports::{HttpTransport, Transport, TransportEvent};
use klaus_agent_core::runtime_values::{runtime_json, RuntimeString};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// One queued HTTP response: status, content type, body.
type Stub = (u16, &'static str, &'static str);

/// A minimal streamable HTTP endpoint that answers each accepted POST with
/// the next queued response, one request per connection.
struct StubServer {
    url: String,
    task: JoinHandle<()>,
}

impl StubServer {
    async fn start(stubs: Vec<Stub>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/mcp", listener.local_addr().unwrap());
        let queue = Arc::new(Mutex::new(stubs.into_iter()));
        let task = tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    break;
                };
                let next = queue.lock().unwrap().next();
                let Some((status, content_type, body)) = next else {
                    break;
                };
                tokio::spawn(async move {
                    let (read, mut write) = socket.into_split();
                    let mut reader = BufReader::new(read);
                    let mut line = String::new();
                    if reader.read_line(&mut line).await.is_err() {
                        return;
                    }
                    let mut length = 0usize;
                    loop {
                        line.clear();
                        match reader.read_line(&mut line).await {
                            Ok(0) | Err(_) => return,
                            Ok(_) => {}
                        }
                        if line == "\r\n" || line == "\n" || line.is_empty() {
                            break;
                        }
                        if let Some((name, value)) = line.split_once(':') {
                            if name.eq_ignore_ascii_case("content-length") {
                                length = value.trim().parse().unwrap_or(0);
                            }
                        }
                    }
                    let mut body_in = vec![0; length];
                    if length > 0 && reader.read_exact(&mut body_in).await.is_err() {
                        return;
                    }
                    let reason = match status {
                        202 => "Accepted",
                        204 => "No Content",
                        _ => "OK",
                    };
                    let mut head = format!("HTTP/1.1 {status} {reason}\r\n");
                    if status != 204 {
                        head.push_str(&format!(
                            "content-type: {content_type}\r\ncontent-length: {}\r\n",
                            body.len()
                        ));
                    }
                    head.push_str("connection: close\r\n\r\n");
                    if write.write_all(head.as_bytes()).await.is_err() {
                        return;
                    }
                    if status != 204 && !body.is_empty() {
                        let _ = write.write_all(body.as_bytes()).await;
                    }
                    let _ = write.shutdown().await;
                });
            }
        });
        Self { url, task }
    }
}

impl Drop for StubServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn message(text: &str) -> klaus_agent_core::runtime_values::RuntimeJson {
    runtime_json::parse(&RuntimeString::from(text)).unwrap()
}

async fn started_transport(url: &str) -> (HttpTransport, mpsc::UnboundedReceiver<TransportEvent>) {
    let transport = HttpTransport::new(url.to_string(), HashMap::new());
    let (tx, rx) = mpsc::unbounded_channel();
    transport.start(tx).await.unwrap();
    (transport, rx)
}

/// The reply carried a `200` JSON body whose response id collides with a
/// hypothetical pending request id: the body must be rejected, not routed.
#[tokio::test]
async fn a_reply_post_rejects_a_json_body_before_dispatch() {
    let server = StubServer::start(vec![(
        200,
        "application/json",
        r#"{"jsonrpc":"2.0","id":1,"result":{"poison":"collides"}}"#,
    )])
    .await;
    let (transport, mut rx) = started_transport(&server.url).await;
    let outcome = transport
        .send(message(
            r#"{"jsonrpc":"2.0","id":7,"result":{"action":"accept"}}"#,
        ))
        .await;
    let error = outcome.expect_err("a reply POST body must be rejected");
    assert!(
        format!("{error}").contains("reply POST"),
        "unexpected error: {error}"
    );
    assert!(
        rx.try_recv().is_err(),
        "the rejected payload must never reach the message router"
    );
}

/// The same collision delivered as an event stream must also be rejected
/// before the stream is decoded or handed to the router.
#[tokio::test]
async fn a_reply_post_rejects_an_event_stream_body_before_dispatch() {
    let server = StubServer::start(vec![(
        200,
        "text/event-stream",
        "event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"poison\":\"streamed\"}}\n\n",
    )])
    .await;
    let (transport, mut rx) = started_transport(&server.url).await;
    let outcome = transport
        .send(message(
            r#"{"jsonrpc":"2.0","id":7,"error":{"code":-32603,"message":"declined"}}"#,
        ))
        .await;
    outcome.expect_err("a reply POST event stream must be rejected");
    // The pre-fix code decoded streams on a spawned task; give any straggler
    // a chance to surface before asserting the router saw nothing.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        rx.try_recv().is_err(),
        "the rejected stream must never reach the message router"
    );
}

/// `202 Accepted` and `204 No Content` are both valid acknowledgements for a
/// reply POST and must succeed without touching a body.
#[tokio::test]
async fn a_reply_post_accepts_both_acknowledgement_statuses() {
    let server = StubServer::start(vec![
        (202, "application/json", ""),
        (204, "application/json", ""),
    ])
    .await;
    let (transport, mut rx) = started_transport(&server.url).await;
    transport
        .send(message(
            r#"{"jsonrpc":"2.0","id":3,"result":{"action":"accept"}}"#,
        ))
        .await
        .expect("202 must acknowledge a reply POST");
    transport
        .send(message(
            r#"{"jsonrpc":"2.0","id":"shared-4","error":{"code":-32800,"message":"cancelled"}}"#,
        ))
        .await
        .expect("204 must acknowledge a reply POST");
    assert!(rx.try_recv().is_err(), "acknowledgements carry no messages");
}

/// Ordinary request POSTs keep the existing behavior: a `200` JSON body is
/// the response to the request and must be decoded and dispatched.
#[tokio::test]
async fn a_request_post_still_delivers_the_response_body() {
    let server = StubServer::start(vec![(
        200,
        "application/json",
        r#"{"jsonrpc":"2.0","id":1,"result":{"ok":true}}"#,
    )])
    .await;
    let (transport, mut rx) = started_transport(&server.url).await;
    transport
        .send(message(r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#))
        .await
        .expect("a request POST accepts a JSON response body");
    let event = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("the response body must be dispatched")
        .unwrap();
    match event {
        TransportEvent::Message(decoded) => {
            assert_eq!(
                decoded,
                message(r#"{"jsonrpc":"2.0","id":1,"result":{"ok":true}}"#)
            );
        }
        other => panic!("unexpected transport event: {other:?}"),
    }
}

/// A JSON-RPC 2.0 error reply carrying `"id": null` is still a one-way reply
/// POST and must reject any `200` body before it can reach the message router.
#[tokio::test]
async fn a_null_id_error_reply_post_rejects_a_response_body_before_dispatch() {
    let server = StubServer::start(vec![(
        200,
        "application/json",
        r#"{"jsonrpc":"2.0","id":1,"result":{"poison":"null-id-collides"}}"#,
    )])
    .await;
    let (transport, mut rx) = started_transport(&server.url).await;
    let outcome = transport
        .send(message(
            r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"Invalid Request"}}"#,
        ))
        .await;
    let error = outcome.expect_err("a null-id error reply POST body must be rejected");
    assert!(
        format!("{error}").contains("reply POST"),
        "unexpected error: {error}"
    );
    assert!(
        rx.try_recv().is_err(),
        "the rejected null-id reply payload must never reach the message router"
    );
}

/// Outbound notifications (`notifications/initialized`, `notifications/cancelled`)
/// must succeed on `204 No Content` and drop unexpected `200` JSON or SSE
/// payloads without routing them into in-flight request waiters.
#[tokio::test]
async fn outbound_notifications_accept_204_and_drop_200_bodies_without_dispatch() {
    let handshake_server = StubServer::start(vec![
        (
            200,
            "application/json",
            r#"{"jsonrpc":"2.0","id":0,"result":{"protocolVersion":"2025-11-25","capabilities":{},"serverInfo":{"name":"stub","version":"1"}}}"#,
        ),
        (204, "application/json", ""),
    ])
    .await;
    let client = Client::new(None);
    let handshake_transport = Arc::new(HttpTransport::new(
        handshake_server.url.clone(),
        HashMap::new(),
    ));
    client
        .connect(
            handshake_transport,
            RequestOptions {
                timeout_ms: 5_000,
                ..Default::default()
            },
        )
        .await
        .expect("notifications/initialized answered with 204 No Content must succeed");

    let notif_server = StubServer::start(vec![
        (
            200,
            "application/json",
            r#"{"jsonrpc":"2.0","id":1,"result":{"poison":"cancelled-json"}}"#,
        ),
        (
            200,
            "text/event-stream",
            "event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"poison\":\"cancelled-sse\"}}\n\n",
        ),
    ])
    .await;
    let (transport, mut rx) = started_transport(&notif_server.url).await;
    transport
        .send(message(
            r#"{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":9}}"#,
        ))
        .await
        .expect("notification POST with 200 JSON ack must succeed");
    transport
        .send(message(
            r#"{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":10}}"#,
        ))
        .await
        .expect("notification POST with 200 SSE ack must succeed");
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        rx.try_recv().is_err(),
        "notification acknowledgements must never dispatch frames into the router"
    );
}
