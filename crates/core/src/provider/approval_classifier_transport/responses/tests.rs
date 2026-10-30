use super::*;
use crate::provider::approval_classifier_transport::{
    create_classifier_transport, test_request, ClassifierHttpClientFactory,
    ClassifierTransportEvent,
};
use crate::provider::auth::{AuthSource, StaticBearerAuth};
use crate::provider::oauth::{OAuthAuth, OAuthAuthOptions, OAuthTokens};
use crate::provider::openai::{
    OpenAICompatibilityOptions, OpenAIProvider, OpenAIProviderOptions, OpenAiApi,
};
use crate::types::provider::{ModelCapabilities, ModelStreamEvent};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Complete,
    IdlePing,
    InterruptAccepted,
    InterruptRejected,
    WrongAcknowledgement,
}

#[derive(Clone)]
struct Record {
    websocket: bool,
    connection: usize,
    headers: HashMap<String, String>,
    body: RuntimeJson,
}

struct Fixture {
    url: String,
    opens: Arc<AtomicUsize>,
    allowed_opens: Arc<AtomicUsize>,
    closed: Arc<AtomicUsize>,
    pongs: Arc<AtomicUsize>,
    records: Arc<Mutex<Vec<Record>>>,
    changed: Arc<Notify>,
    stop: AbortSignal,
    task: tokio::task::JoinHandle<()>,
}

struct PrefixedIo<S> {
    prefix: Vec<u8>,
    offset: usize,
    inner: S,
}

impl<S: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for PrefixedIo<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        out: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.offset < self.prefix.len() {
            let count = out.remaining().min(self.prefix.len() - self.offset);
            out.put_slice(&self.prefix[self.offset..self.offset + count]);
            self.offset += count;
            std::task::Poll::Ready(Ok(()))
        } else {
            Pin::new(&mut self.inner).poll_read(cx, out)
        }
    }
}

impl<S: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for PrefixedIo<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, bytes)
    }
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl Fixture {
    async fn start(mode: Mode) -> Self {
        Self::start_with_tls(mode, None).await
    }

    async fn start_tls(mode: Mode) -> Self {
        use rustls::pki_types::pem::PemObject;
        let certificate = rustls::pki_types::CertificateDer::from_pem_slice(include_bytes!(
            "fixtures/server.pem"
        ))
        .unwrap();
        let key = rustls::pki_types::PrivateKeyDer::from_pem_slice(include_bytes!(
            "fixtures/server-key.pem"
        ))
        .unwrap();
        let config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![certificate], key)
        .unwrap();
        Self::start_with_tls(mode, Some(Arc::new(config))).await
    }

    async fn start_with_tls(mode: Mode, tls: Option<Arc<rustls::ServerConfig>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fixture");
        let address = listener.local_addr().expect("fixture address");
        let url = if tls.is_some() {
            format!("https://localhost:{}", address.port())
        } else {
            format!("http://{address}")
        };
        let opens = Arc::new(AtomicUsize::new(0));
        let allowed_opens = Arc::new(AtomicUsize::new(usize::MAX));
        let closed = Arc::new(AtomicUsize::new(0));
        let pongs = Arc::new(AtomicUsize::new(0));
        let records = Arc::new(Mutex::new(Vec::new()));
        let changed = Arc::new(Notify::new());
        let stop = AbortSignal::new();
        let task = {
            let opens = Arc::clone(&opens);
            let allowed = Arc::clone(&allowed_opens);
            let closed = Arc::clone(&closed);
            let pongs = Arc::clone(&pongs);
            let records = Arc::clone(&records);
            let changed = Arc::clone(&changed);
            let stop = stop.clone();
            tokio::spawn(async move {
                let mut connections = JoinSet::new();
                loop {
                    let accepted = tokio::select! {
                        _ = stop.cancelled() => break,
                        joined = connections.join_next(), if !connections.is_empty() => {
                            joined.expect("fixture connection was registered").expect("fixture connection completed without panic");
                            continue;
                        },
                        accepted = listener.accept() => accepted,
                    };
                    let Ok((mut stream, _)) = accepted else {
                        break;
                    };
                    let opens = Arc::clone(&opens);
                    let allowed = Arc::clone(&allowed);
                    let closed = Arc::clone(&closed);
                    let pongs = Arc::clone(&pongs);
                    let records = Arc::clone(&records);
                    let changed = Arc::clone(&changed);
                    let tls = tls.clone();
                    connections.spawn(async move {
                        if let Some(tls) = tls {
                            let Ok(mut stream) =
                                tokio_rustls::TlsAcceptor::from(tls).accept(stream).await
                            else {
                                return;
                            };
                            let mut method = [0; 4];
                            if stream.read_exact(&mut method).await.is_err() {
                                return;
                            }
                            let mut stream = PrefixedIo {
                                prefix: method.to_vec(),
                                offset: 0,
                                inner: stream,
                            };
                            if &method == b"GET " {
                                let connection = opens.fetch_add(1, Ordering::SeqCst) + 1;
                                changed.notify_waiters();
                                if connection > allowed.load(Ordering::SeqCst) {
                                    let _ =
                                        tokio::io::copy(&mut stream, &mut tokio::io::sink()).await;
                                } else {
                                    serve_websocket(
                                        stream, connection, mode, records, changed, pongs,
                                    )
                                    .await;
                                }
                                closed.fetch_add(1, Ordering::SeqCst);
                            } else {
                                serve_http(stream, records, changed).await;
                            }
                            return;
                        }
                        let mut method = [0; 4];
                        if stream.peek(&mut method).await.unwrap_or(0) < 4 {
                            return;
                        }
                        if &method == b"GET " {
                            let connection = opens.fetch_add(1, Ordering::SeqCst) + 1;
                            changed.notify_waiters();
                            if connection > allowed.load(Ordering::SeqCst) {
                                let _ = tokio::io::copy(&mut stream, &mut tokio::io::sink()).await;
                            } else {
                                serve_websocket(stream, connection, mode, records, changed, pongs)
                                    .await;
                            }
                            closed.fetch_add(1, Ordering::SeqCst);
                        } else {
                            serve_http(stream, records, changed).await;
                        }
                    });
                }
                connections.abort_all();
                while let Some(result) = connections.join_next().await {
                    if let Err(error) = result {
                        assert!(error.is_cancelled(), "fixture connection panicked: {error}");
                    }
                }
            })
        };
        Self {
            url,
            opens,
            allowed_opens,
            closed,
            pongs,
            records,
            changed,
            stop,
            task,
        }
    }

    async fn wait_opens(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let notified = self.changed.notified();
                if self.opens.load(Ordering::SeqCst) >= count {
                    return;
                }
                notified.await;
            }
        })
        .await
        .expect("open count reached");
    }

    async fn wait_records(&self, count: usize) -> Vec<Record> {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let notified = self.changed.notified();
                let records = self.records.lock().unwrap().clone();
                if records.len() >= count {
                    return records;
                }
                notified.await;
            }
        })
        .await
        .expect("request records reached")
    }

    async fn shutdown(self) {
        self.stop.cancel();
        self.task.await.expect("fixture stopped");
    }
}

fn record(records: &Mutex<Vec<Record>>, changed: &Notify, record: Record) {
    let mut records = records.lock().unwrap();
    assert!(records.len() < 64, "fixture request log is bounded");
    records.push(record);
    drop(records);
    changed.notify_waiters();
}

fn events(id: &str) -> [RuntimeJson; 4] {
    let item = serde_json::json!({
        "id": format!("msg-{id}"), "type": "message", "role": "assistant",
        "content": [{"type":"output_text","text":"low"}]
    });
    [
        serde_json::json!({"type":"response.created","response":{"id":id,"model":"gpt-5.6-luna"}}),
        serde_json::json!({"type":"response.output_text.delta","delta":"low"}),
        serde_json::json!({"type":"response.output_item.done","item":item.clone()}),
        serde_json::json!({"type":"response.completed","response":{
            "id":id,"model":"gpt-5.6-luna","status":"completed","output":[item],
            "usage":{"input_tokens":40,"output_tokens":2,"total_tokens":42}
        }}),
    ]
    .map(|value| {
        runtime_json::parse(&RuntimeString::from(value.to_string())).expect("fixture JSON")
    })
}

async fn serve_websocket<S>(
    stream: S,
    connection: usize,
    mode: Mode,
    records: Arc<Mutex<Vec<Record>>>,
    changed: Arc<Notify>,
    pongs: Arc<AtomicUsize>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin,
{
    let captured = Arc::new(Mutex::new(HashMap::new()));
    let callback_headers = Arc::clone(&captured);
    let callback = move |request: &Request, response: Response| {
        let headers = request
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.as_str().to_owned(), value.to_owned()))
            })
            .collect();
        *callback_headers.lock().unwrap() = headers;
        Ok(response)
    };
    let Ok(mut socket) = tokio_tungstenite::accept_hdr_async(stream, callback).await else {
        return;
    };
    if mode == Mode::IdlePing {
        if socket
            .send(Message::Ping(b"idle-keepalive".to_vec()))
            .await
            .is_err()
        {
            return;
        }
        let pong = match tokio::time::timeout(Duration::from_secs(1), socket.next()).await {
            Ok(Some(Ok(Message::Close(_)))) => return,
            Ok(Some(Ok(pong))) => pong,
            // Refilled idle sockets can be retired before their keepalive finishes.
            Ok(None) | Ok(Some(Err(_))) => return,
            Err(error) => panic!("idle keepalive timed out: {error}"),
        };
        assert_eq!(pong, Message::Pong(b"idle-keepalive".to_vec()));
        pongs.fetch_add(1, Ordering::SeqCst);
        changed.notify_waiters();
    }
    let mut sequence = 0;
    while let Some(Ok(Message::Text(text))) = socket.next().await {
        let body = runtime_json::parse(&RuntimeString::from(text)).expect("WebSocket request JSON");
        if body
            .get("type")
            .and_then(RuntimeJson::as_str)
            .is_none_or(|kind| kind != "response.create")
        {
            break;
        }
        sequence += 1;
        record(
            &records,
            &changed,
            Record {
                websocket: true,
                connection,
                headers: captured.lock().unwrap().clone(),
                body,
            },
        );
        let id = format!("response-{connection}-{sequence}");
        let response = events(&id);
        for event in &response[..2] {
            socket
                .send(Message::Text(
                    runtime_json::stringify(event).try_to_utf8().unwrap(),
                ))
                .await
                .expect("fixture event");
        }
        if sequence == 1
            && matches!(
                mode,
                Mode::InterruptAccepted | Mode::InterruptRejected | Mode::WrongAcknowledgement
            )
        {
            let Some(Ok(Message::Text(interrupt))) = socket.next().await else {
                return;
            };
            let interrupt =
                runtime_json::parse(&RuntimeString::from(interrupt)).expect("interrupt JSON");
            assert_eq!(
                interrupt["type"],
                RuntimeJson::String("response.interrupt".into())
            );
            assert_eq!(
                interrupt["response_id"],
                RuntimeJson::String(id.clone().into())
            );
            assert_eq!(
                interrupt["mode"],
                RuntimeJson::String("discard_partial_items".into())
            );
            let acknowledgement = serde_json::json!({
                "type": if mode == Mode::InterruptRejected {"response.interrupt.failed"} else {"response.interrupt.accepted"},
                "response_id": if mode == Mode::WrongAcknowledgement { "another-response" } else { &id },
            });
            // Terminal usage can precede an invalid or rejected acknowledgement.
            socket
                .send(Message::Text(
                    runtime_json::stringify(&response[3]).try_to_utf8().unwrap(),
                ))
                .await
                .expect("fixture completion");
            socket
                .send(Message::Text(acknowledgement.to_string()))
                .await
                .expect("fixture acknowledgement");
        } else {
            for event in &response[2..] {
                socket
                    .send(Message::Text(
                        runtime_json::stringify(event).try_to_utf8().unwrap(),
                    ))
                    .await
                    .expect("fixture event");
            }
        }
    }
}

async fn serve_http<S>(mut stream: S, records: Arc<Mutex<Vec<Record>>>, changed: Arc<Notify>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin,
{
    let mut header = Vec::new();
    while !header.ends_with(b"\r\n\r\n") {
        assert!(header.len() < 32 * 1024, "bounded fixture headers");
        let mut byte = [0];
        if stream.read_exact(&mut byte).await.is_err() {
            return;
        }
        header.push(byte[0]);
    }
    let header = String::from_utf8(header).unwrap();
    let headers: HashMap<_, _> = header
        .lines()
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_owned()))
        .collect();
    let length: usize = headers
        .get("content-length")
        .expect("content length")
        .parse()
        .unwrap();
    assert!(length < 1024 * 1024, "bounded fixture body");
    let mut body = vec![0; length];
    stream.read_exact(&mut body).await.expect("fixture body");
    record(
        &records,
        &changed,
        Record {
            websocket: false,
            connection: 0,
            headers,
            body: runtime_json::parse(&RuntimeString::from(String::from_utf8(body).unwrap()))
                .expect("HTTP body JSON"),
        },
    );
    let body = events("http-response")
        .iter()
        .map(|event| {
            format!(
                "data: {}\n\n",
                runtime_json::stringify(event).try_to_utf8().unwrap()
            )
        })
        .collect::<String>();
    stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes())
        .await.expect("HTTP fixture response");
}

fn transport(
    url: &str,
    auth: Option<Arc<dyn AuthSource>>,
    extra_headers: HashMap<String, String>,
) -> Arc<dyn ApprovalClassifierTransport> {
    transport_with_credentials(url, auth, None, extra_headers)
}

fn transport_with_credentials(
    url: &str,
    auth: Option<Arc<dyn AuthSource>>,
    api_key: Option<String>,
    extra_headers: HashMap<String, String>,
) -> Arc<dyn ApprovalClassifierTransport> {
    let factory: ClassifierHttpClientFactory = Arc::new(|restricted| {
        let builder = reqwest::Client::builder().no_proxy().http1_only();
        let builder = if restricted {
            builder.redirect(reqwest::redirect::Policy::none())
        } else {
            builder
        };
        builder.build().map_err(network_error)
    });
    let provider = Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
        base_url: Some(url.to_owned()),
        api: Some(OpenAiApi::Responses),
        api_key,
        auth_source: auth,
        classifier_http_client_factory: Some(factory),
        capabilities: Some(ModelCapabilities::default()),
        compatibility: Some(OpenAICompatibilityOptions::try_new(extra_headers, None).unwrap()),
        ..Default::default()
    }));
    create_classifier_transport(provider, test_request().attribution)
}

async fn drain(mut stream: ClassifierStream) -> Vec<ClassifierTransportEvent> {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut events = Vec::new();
        while let Some(event) = stream.next().await {
            assert!(events.len() < 64, "fixture event collection is bounded");
            events.push(event);
        }
        events
    })
    .await
    .expect("classifier stream drained")
}

#[tokio::test]
async fn cold_classification_uses_http_without_waiting_for_websocket_open() {
    let server = Fixture::start(Mode::Complete).await;
    server.allowed_opens.store(0, Ordering::SeqCst);
    let transport = transport(&server.url, None, HashMap::new());
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        drain(transport.stream(test_request(), AbortSignal::new())),
    )
    .await
    .expect("HTTP must finish while WebSocket establishment is stalled");
    assert!(result.iter().any(|event| matches!(event, ClassifierTransportEvent::Model(ModelStreamEvent::TextDelta { text }) if text == "low")));
    let records = server.wait_records(1).await;
    assert!(!records[0].websocket);
    assert_eq!(
        records[0].headers.get("session-id").map(String::as_str),
        Some("owning-session")
    );
    assert!(!records[0]
        .body
        .as_object()
        .unwrap()
        .contains_key("max_output_tokens"));
    assert!(!records[0].body.as_object().unwrap().contains_key("tools"));
    assert_eq!(
        records[0].body["reasoning"]["context"],
        RuntimeJson::String("all_turns".into())
    );
    transport.shutdown().await;
    server.shutdown().await;
}

#[tokio::test]
async fn warmed_websockets_preserve_attribution_native_items_and_completed_reuse() {
    let server = Fixture::start(Mode::Complete).await;
    let transport = transport(&server.url, None, HashMap::new());
    transport.prewarm().await;
    assert_eq!(server.opens.load(Ordering::SeqCst), INITIAL_CONNECTIONS);
    for _ in 0..3 {
        let events = drain(transport.stream(test_request(), AbortSignal::new())).await;
        assert!(events.iter().any(|event| matches!(event, ClassifierTransportEvent::OutputItem(item) if item.get("id").is_some())));
        assert!(events.iter().any(|event| matches!(
            event,
            ClassifierTransportEvent::Model(ModelStreamEvent::MessageStop { .. })
        )));
    }
    let records = server.wait_records(3).await;
    assert!(records.iter().all(|record| record.websocket));
    assert!(records
        .iter()
        .enumerate()
        .any(|(index, record)| records[..index]
            .iter()
            .any(|earlier| earlier.connection == record.connection)));
    for record in records {
        assert_eq!(
            record.headers.get("openai-beta").map(String::as_str),
            Some("responses_websockets=2026-02-06")
        );
        assert!(record.headers.contains_key("thread-id"));
        assert_eq!(
            record.body["client_metadata"]["parent_turn_id"],
            RuntimeJson::String("parent-turn".into())
        );
        assert_eq!(
            record.body["client_metadata"]["root_turn_id"],
            RuntimeJson::String("root-turn".into())
        );
    }
    transport.shutdown().await;
    server.shutdown().await;
}

#[tokio::test]
async fn subscription_backend_proxies_keep_classifier_attribution_scoped_to_oauth() {
    let oauth: Arc<dyn AuthSource> = Arc::new(OAuthAuth::new(OAuthAuthOptions {
        tokens: Some(OAuthTokens {
            access_token: "subscription-token".into(),
            ..Default::default()
        }),
        ..Default::default()
    }));
    let bearer: Arc<dyn AuthSource> = Arc::new(StaticBearerAuth::new(
        "custom-bearer-token",
        None,
        HashMap::new(),
    ));
    for (backend_route, auth, api_key, classifier_route) in [
        (true, Some(Arc::clone(&oauth)), None, true),
        (false, Some(Arc::clone(&oauth)), None, false),
        (true, Some(bearer), None, false),
        (true, None, Some("api-token".to_owned()), false),
        (
            true,
            Some(Arc::clone(&oauth)),
            Some("api-token".to_owned()),
            false,
        ),
    ] {
        let server = Fixture::start(Mode::Complete).await;
        let base_url = if backend_route {
            format!("{}/backend-api/codex/", server.url)
        } else {
            server.url.clone()
        };
        let transport = transport_with_credentials(&base_url, auth, api_key, HashMap::new());
        let _ = drain(transport.stream(test_request(), AbortSignal::new())).await;
        transport.prewarm().await;
        let _ = drain(transport.stream(test_request(), AbortSignal::new())).await;
        let records = server.wait_records(2).await;
        assert!(!records[0].websocket);
        assert!(records[1].websocket);
        for record in records {
            assert_eq!(
                record.headers.get("x-codex-guardian").map(String::as_str),
                classifier_route.then_some("classifier"),
            );
            assert_eq!(
                record.body["client_metadata"]
                    .get("parent_response_id")
                    .and_then(RuntimeJson::as_str)
                    .and_then(|value| value.try_to_utf8().ok()),
                classifier_route.then(|| "parent-response".to_owned()),
            );
        }
        transport.shutdown().await;
        server.shutdown().await;
    }
}

#[tokio::test]
async fn idle_connections_answer_keepalives_before_any_classification_is_sent() {
    let server = Fixture::start(Mode::IdlePing).await;
    let transport = transport(&server.url, None, HashMap::new());
    transport.prewarm().await;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let notified = server.changed.notified();
            if server.pongs.load(Ordering::SeqCst) >= INITIAL_CONNECTIONS {
                return;
            }
            notified.await;
        }
    })
    .await
    .expect("idle socket pumps answered pings");
    let events = drain(transport.stream(test_request(), AbortSignal::new())).await;
    assert!(events.iter().any(|event| matches!(
        event,
        ClassifierTransportEvent::Model(ModelStreamEvent::MessageStop { .. })
    )));
    assert!(server.wait_records(1).await[0].websocket);
    transport.shutdown().await;
    server.shutdown().await;
}

#[tokio::test]
async fn cancellation_drains_terminal_usage_and_keeps_acknowledged_sockets_open() {
    let server = Fixture::start(Mode::InterruptAccepted).await;
    let transport = transport(&server.url, None, HashMap::new());
    transport.prewarm().await;
    let signal = AbortSignal::new();
    let mut stream = transport.stream(test_request(), signal.clone());
    while let Some(event) = stream.next().await {
        if matches!(
            event,
            ClassifierTransportEvent::Model(ModelStreamEvent::TextDelta { .. })
        ) {
            break;
        }
    }
    signal.cancel();
    let events = drain(stream).await;
    assert!(events.iter().any(|event| matches!(event, ClassifierTransportEvent::Model(ModelStreamEvent::UsageSnapshot { usage }) if usage.total_tokens == Some(42))));
    assert_eq!(server.closed.load(Ordering::SeqCst), 0);
    transport.shutdown().await;
    server.shutdown().await;
}

#[tokio::test]
async fn invalid_interrupt_acknowledgements_retire_sockets_and_preserve_usage() {
    for mode in [Mode::InterruptRejected, Mode::WrongAcknowledgement] {
        let server = Fixture::start(mode).await;
        let transport = transport(&server.url, None, HashMap::new());
        transport.prewarm().await;
        let signal = AbortSignal::new();
        let mut stream = transport.stream(test_request(), signal.clone());
        while let Some(event) = stream.next().await {
            if matches!(
                event,
                ClassifierTransportEvent::Model(ModelStreamEvent::TextDelta { .. })
            ) {
                break;
            }
        }
        signal.cancel();
        let events = drain(stream).await;
        assert!(events.iter().any(|event| matches!(event, ClassifierTransportEvent::Model(ModelStreamEvent::UsageSnapshot { usage }) if usage.total_tokens == Some(42))));
        assert!(events.iter().any(|event| matches!(
            event,
            ClassifierTransportEvent::Model(ModelStreamEvent::Error { .. })
        )));
        transport.shutdown().await;
        server.shutdown().await;
    }
}

#[tokio::test]
async fn account_replacement_cancels_active_work_and_discards_old_idle_authentication() {
    let server = Fixture::start(Mode::InterruptAccepted).await;
    let auth = Arc::new(OAuthAuth::new(OAuthAuthOptions {
        tokens: Some(OAuthTokens {
            access_token: "token-a".into(),
            ..Default::default()
        }),
        ..Default::default()
    }));
    let transport = transport(&server.url, Some(auth.clone()), HashMap::new());
    transport.prewarm().await;
    server
        .allowed_opens
        .store(INITIAL_CONNECTIONS, Ordering::SeqCst);
    let mut stream = transport.stream(test_request(), AbortSignal::new());
    while let Some(event) = stream.next().await {
        if matches!(
            event,
            ClassifierTransportEvent::Model(ModelStreamEvent::TextDelta { .. })
        ) {
            break;
        }
    }
    auth.set_tokens(OAuthTokens {
        access_token: "token-b".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let events = drain(stream).await;
    assert!(events.iter().any(|event| matches!(event, ClassifierTransportEvent::Model(ModelStreamEvent::Error { error }) if error.code.as_ref().is_some_and(|code| code == "classifier_auth_changed"))));
    let _ = drain(transport.stream(test_request(), AbortSignal::new())).await;
    let records = server.wait_records(2).await;
    assert!(!records[1].websocket);
    assert_eq!(
        records[1].headers.get("authorization").map(String::as_str),
        Some("Bearer token-b")
    );
    transport.shutdown().await;
    server.shutdown().await;
}

#[tokio::test]
async fn shutdown_cancels_and_joins_stalled_openers() {
    let server = Fixture::start(Mode::Complete).await;
    server.allowed_opens.store(0, Ordering::SeqCst);
    let transport = transport(&server.url, None, HashMap::new());
    let prewarm_transport = Arc::clone(&transport);
    let prewarm = tokio::spawn(async move {
        prewarm_transport.prewarm().await;
    });
    server.wait_opens(INITIAL_CONNECTIONS).await;
    tokio::time::timeout(Duration::from_secs(1), transport.shutdown())
        .await
        .expect("all openers joined");
    prewarm.await.expect("prewarm cancelled");
    server.shutdown().await;
}

struct DirectConnector {
    url: String,
    client: reqwest::Client,
    headers: Mutex<HashMap<String, String>>,
}

#[async_trait::async_trait]
impl ClassifierConnector for DirectConnector {
    async fn resolve(&self, signal: &AbortSignal) -> Result<ClassifierEndpoint, ProviderError> {
        if signal.is_cancelled() {
            return Err(cancelled_error());
        }
        Ok(ClassifierEndpoint {
            url: format!("{}/responses", self.url),
            headers: self.headers.lock().unwrap().clone(),
            client: self.client.clone(),
            websocket_client: self.client.clone(),
            auth_changes: None,
            auth_state: None,
            classifier_route: false,
            connect_timeout: Duration::from_secs(15),
            idle_timeout: Duration::from_secs(300),
            attempt: None,
        })
    }

    async fn auth_recovery(
        &self,
        _signal: &AbortSignal,
    ) -> Option<Box<dyn ClassifierAuthRecovery>> {
        None
    }

    fn prepare(
        &self,
        request: &ClassifierRequest,
        thread_id: &str,
        endpoint: &ClassifierEndpoint,
    ) -> Result<RuntimeJson, ProviderError> {
        assert!(!endpoint.url.is_empty());
        Ok(runtime_json::parse(&RuntimeString::from(serde_json::json!({
            "model":request.request.model.try_to_utf8().unwrap(), "input":[], "thread_id":thread_id,
        }).to_string())).unwrap())
    }

    fn map_stream(
        &self,
        request: ClassifierRequest,
        mut stream: RawClassifierStream,
    ) -> ClassifierStream {
        assert!(!request.attribution.session_id.is_empty());
        Box::pin(async_stream::stream! {
            while let Some(event) = stream.next().await {
                match event {
                    Ok(event) => yield ClassifierTransportEvent::OutputItem(event),
                    Err(error) => yield ClassifierTransportEvent::Model(ModelStreamEvent::Error { error }),
                }
            }
        })
    }
}

fn direct_pool(server: &Fixture) -> (ResponsesClassifierTransport, Arc<DirectConnector>) {
    let connector = Arc::new(DirectConnector {
        url: server.url.clone(),
        client: reqwest::Client::builder()
            .http1_only()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap(),
        headers: Mutex::new(HashMap::new()),
    });
    (
        ResponsesClassifierTransport::new(connector.clone(), test_request().attribution),
        connector,
    )
}

#[tokio::test]
async fn transport_admission_holds_sixteen_requests_and_releases_on_lease_completion() {
    let server = Fixture::start(Mode::Complete).await;
    let (transport, _) = direct_pool(&server);
    let signal = AbortSignal::new();
    let mut leases = Vec::new();
    for _ in 0..MAX_CONCURRENT_REQUESTS {
        leases.push(transport.pool.lease(&signal).await.unwrap());
    }
    let pending = transport.pool.lease(&signal);
    tokio::pin!(pending);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut pending)
            .await
            .is_err()
    );
    drop(leases.pop());
    let last = tokio::time::timeout(Duration::from_secs(1), &mut pending)
        .await
        .expect("released admission permit")
        .unwrap();
    drop(last);
    drop(leases);
    transport.shutdown().await;
    server.shutdown().await;
}

#[tokio::test]
async fn expired_and_differently_routed_idle_connections_fall_back_to_http() {
    let server = Fixture::start(Mode::Complete).await;
    let (transport, connector) = direct_pool(&server);
    transport.prewarm().await;
    server
        .allowed_opens
        .store(INITIAL_CONNECTIONS, Ordering::SeqCst);
    connector.headers.lock().unwrap().insert(
        "x-openai-account-routing-override".into(),
        "workspace-b".into(),
    );
    let lease = transport.pool.lease(&AbortSignal::new()).await.unwrap();
    assert!(matches!(&lease.connection, Connection::Http { .. }));
    drop(lease);
    transport.shutdown().await;
    server.shutdown().await;

    let server = Fixture::start(Mode::Complete).await;
    let (transport, _) = direct_pool(&server);
    transport.prewarm().await;
    server
        .allowed_opens
        .store(INITIAL_CONNECTIONS, Ordering::SeqCst);
    for connection in transport.pool.idle.lock().unwrap().iter_mut() {
        connection.expires_at = Instant::now() - Duration::from_secs(1);
    }
    let lease = transport.pool.lease(&AbortSignal::new()).await.unwrap();
    assert!(matches!(&lease.connection, Connection::Http { .. }));
    drop(lease);
    transport.shutdown().await;
    server.shutdown().await;
}

#[tokio::test]
async fn http_and_websocket_share_custom_trust_and_connect_proxy_configuration() {
    let server = Fixture::start_tls(Mode::Complete).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_url = format!("http://{}", listener.local_addr().unwrap());
    let proxy_stop = AbortSignal::new();
    let stop = proxy_stop.clone();
    let connects = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&connects);
    let proxy = tokio::spawn(async move {
        let mut tunnels = JoinSet::new();
        loop {
            let accepted = tokio::select! {
                _ = stop.cancelled() => break,
                joined = tunnels.join_next(), if !tunnels.is_empty() => {
                    joined.expect("proxy tunnel was registered").expect("proxy tunnel completed without panic");
                    continue;
                },
                accepted = listener.accept() => accepted,
            };
            let Ok((mut incoming, _)) = accepted else {
                break;
            };
            let count = Arc::clone(&count);
            tunnels.spawn(async move {
                let mut header = Vec::new();
                while !header.ends_with(b"\r\n\r\n") {
                    assert!(header.len() < 32 * 1024);
                    let mut byte = [0];
                    if incoming.read_exact(&mut byte).await.is_err() {
                        return;
                    }
                    header.push(byte[0]);
                }
                let header = String::from_utf8(header).unwrap();
                let mut first = header.lines().next().unwrap().split_whitespace();
                assert_eq!(first.next(), Some("CONNECT"));
                let target = first.next().unwrap();
                let Ok(mut outgoing) = TcpStream::connect(target).await else {
                    return;
                };
                count.fetch_add(1, Ordering::SeqCst);
                if incoming
                    .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                    .await
                    .is_err()
                {
                    return;
                }
                let _ = tokio::io::copy_bidirectional(&mut incoming, &mut outgoing).await;
            });
        }
        tunnels.abort_all();
        while let Some(result) = tunnels.join_next().await {
            if let Err(error) = result {
                assert!(error.is_cancelled(), "proxy tunnel panicked: {error}");
            }
        }
    });
    let factory: ClassifierHttpClientFactory = Arc::new(move |restricted| {
        let certificate =
            reqwest::Certificate::from_pem(include_bytes!("fixtures/root.pem")).unwrap();
        let proxy = reqwest::Proxy::all(&proxy_url).unwrap();
        let builder = reqwest::Client::builder()
            .no_proxy()
            .proxy(proxy)
            .http1_only()
            .add_root_certificate(certificate);
        let builder = if restricted {
            builder.redirect(reqwest::redirect::Policy::none())
        } else {
            builder
        };
        builder.build().map_err(network_error)
    });
    let provider = Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
        base_url: Some(server.url.clone()),
        api: Some(OpenAiApi::Responses),
        classifier_http_client_factory: Some(factory),
        ..Default::default()
    }));
    let transport = create_classifier_transport(provider, test_request().attribution);
    let cold = drain(transport.stream(test_request(), AbortSignal::new())).await;
    assert!(cold.iter().any(|event| matches!(event, ClassifierTransportEvent::Model(ModelStreamEvent::TextDelta { text }) if text == "low")));
    transport.prewarm().await;
    let warm = drain(transport.stream(test_request(), AbortSignal::new())).await;
    assert!(warm.iter().any(|event| matches!(
        event,
        ClassifierTransportEvent::Model(ModelStreamEvent::MessageStop { .. })
    )));
    let records = server.wait_records(2).await;
    assert!(!records[0].websocket);
    assert!(records[1].websocket);
    assert!(connects.load(Ordering::SeqCst) >= 3);
    transport.shutdown().await;
    proxy_stop.cancel();
    proxy.await.unwrap();
    server.shutdown().await;
}

#[tokio::test]
async fn account_routed_http_rejects_redirects_without_sending_routing_headers_to_the_destination()
{
    let destination = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let destination_url = format!("http://{}/responses", destination.local_addr().unwrap());
    let gateway = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gateway_url = format!("http://{}", gateway.local_addr().unwrap());
    let stop = AbortSignal::new();
    let gateway_stop = stop.clone();
    let task = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            let accepted = tokio::select! {
                _ = gateway_stop.cancelled() => break,
                joined = connections.join_next(), if !connections.is_empty() => {
                    joined.expect("gateway connection was registered").expect("gateway connection completed without panic");
                    continue;
                },
                accepted = gateway.accept() => accepted,
            };
            let Ok((mut stream, _)) = accepted else {
                break;
            };
            let destination_url = destination_url.clone();
            connections.spawn(async move {
                let mut header = Vec::new();
                while !header.ends_with(b"\r\n\r\n") {
                    assert!(header.len() < 32 * 1024);
                    let mut byte = [0];
                    if stream.read_exact(&mut byte).await.is_err() { return; }
                    header.push(byte[0]);
                }
                let _ = stream.write_all(format!("HTTP/1.1 307 Temporary Redirect\r\nLocation: {destination_url}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await;
            });
        }
        connections.abort_all();
        while let Some(result) = connections.join_next().await {
            if let Err(error) = result {
                assert!(error.is_cancelled(), "gateway connection panicked: {error}");
            }
        }
    });
    let transport = transport(
        &gateway_url,
        None,
        HashMap::from([(
            "x-openai-account-routing-override".into(),
            "workspace-a".into(),
        )]),
    );
    let events = drain(transport.stream(test_request(), AbortSignal::new())).await;
    assert!(events.iter().any(|event| matches!(
        event,
        ClassifierTransportEvent::Model(ModelStreamEvent::Error { .. })
    )));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), destination.accept())
            .await
            .is_err()
    );
    transport.shutdown().await;
    stop.cancel();
    task.await.unwrap();
}
