//! Standalone exec-server JSON-RPC transport.
//!
//! This intentionally does not reuse the agent Op/Event protocol served by
//! `serve.rs`. Each WebSocket frame is one JSON-RPC request or notification, and
//! responses/notifications are sent back as JSON text frames.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use futures::{SinkExt, StreamExt};
use hyper::body::Incoming;
use hyper::header::{CONNECTION, CONTENT_TYPE, UPGRADE};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::{watch, Mutex as AsyncMutex};
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::{Message, Role};
use tokio_tungstenite::WebSocketStream;

use klaus_agent_core::command_runtime::exec_server::backend::ExecBackend;
use klaus_agent_core::command_runtime::exec_server::handler::ExecServerHandler;
use klaus_agent_core::command_runtime::exec_server::protocol::JsonRpcRequest;
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeEncode, RuntimeString};

use crate::auth::{bearer_allowed, bind_policy_error, is_loopback_host, origin_allowed};
use crate::http_util::{empty_body, full_body, text_response, ResponseBody};

pub struct ExecServeOptions {
    pub backend: Arc<dyn ExecBackend>,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub token: Option<String>,
    pub allowed_origins: Option<Vec<String>>,
    pub silent: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum ExecServeError {
    #[error("{0}")]
    BindPolicy(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub struct RunningExecServer {
    pub url: String,
    pub host: String,
    pub port: u16,
    inner: Arc<ExecServerHandle>,
}

struct ExecServerState {
    backend: Arc<dyn ExecBackend>,
    token: Option<String>,
    allowed_origins: Option<Vec<String>>,
    conns: Arc<Mutex<HashMap<u64, watch::Sender<bool>>>>,
    conn_id: AtomicU64,
}

struct ExecServerHandle {
    shutdown: watch::Sender<bool>,
    conns: Arc<Mutex<HashMap<u64, watch::Sender<bool>>>>,
    accept: AsyncMutex<Option<tokio::task::JoinHandle<()>>>,
}

impl RunningExecServer {
    pub async fn close(&self) {
        let _ = self.inner.shutdown.send(true);
        let conns = {
            let mut conns = self.inner.conns.lock().unwrap();
            conns.drain().map(|(_, close)| close).collect::<Vec<_>>()
        };
        for close in conns {
            let _ = close.send(true);
        }
        let mut accept = self.inner.accept.lock().await;
        if let Some(handle) = accept.take() {
            let _ = handle.await;
        }
    }
}

pub async fn serve_exec_server(
    opts: ExecServeOptions,
) -> Result<RunningExecServer, ExecServeError> {
    let host = match &opts.host {
        Some(host) if !host.is_empty() => host.clone(),
        _ => "127.0.0.1".to_string(),
    };
    let requested_port = opts.port.unwrap_or(0);
    let token = opts.token.clone();
    if let Some(message) = bind_policy_error(&host, token.as_deref()) {
        return Err(ExecServeError::BindPolicy(message));
    }

    let listener = TcpListener::bind((host.as_str(), requested_port)).await?;
    let port = listener.local_addr()?.port();
    let conns = Arc::new(Mutex::new(HashMap::new()));
    let state = Arc::new(ExecServerState {
        backend: opts.backend,
        token: token.clone(),
        allowed_origins: opts.allowed_origins,
        conns: Arc::clone(&conns),
        conn_id: AtomicU64::new(0),
    });
    let (shutdown_tx, _shutdown_rx0) = watch::channel(false);
    let accept = {
        let state = Arc::clone(&state);
        let mut shutdown_rx = shutdown_tx.subscribe();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => break,
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else {
                            continue;
                        };
                        let io = TokioIo::new(stream);
                        let state = Arc::clone(&state);
                        let service = service_fn(move |req: Request<Incoming>| {
                            let state = Arc::clone(&state);
                            async move { Ok::<_, Infallible>(handle_exec_request(req, state).await) }
                        });
                        tokio::spawn(async move {
                            let _ = http1::Builder::new()
                                .serve_connection(io, service)
                                .with_upgrades()
                                .await;
                        });
                    }
                }
            }
        })
    };

    let url = format!("ws://{}:{}", format_host(&host), port);
    if !opts.silent {
        print_exec_banner(&host, port, token.as_deref());
    }
    Ok(RunningExecServer {
        url,
        host,
        port,
        inner: Arc::new(ExecServerHandle {
            shutdown: shutdown_tx,
            conns,
            accept: AsyncMutex::new(Some(accept)),
        }),
    })
}

async fn handle_exec_request(
    mut req: Request<Incoming>,
    state: Arc<ExecServerState>,
) -> Response<ResponseBody> {
    if !is_ws_upgrade(&req) {
        return text_response(404, "not found");
    }
    if !origin_allowed(req.headers(), state.allowed_origins.as_deref()) {
        return reject_upgrade(403, "origin not allowed");
    }
    if !bearer_allowed(req.headers(), state.token.as_deref()) {
        return reject_upgrade(401, "invalid or missing bearer token");
    }
    let accept = match req.headers().get("sec-websocket-key") {
        Some(key) => derive_accept_key(key.as_bytes()),
        None => return reject_upgrade(400, "bad request"),
    };
    let id = state.conn_id.fetch_add(1, Ordering::Relaxed);
    let backend = Arc::clone(&state.backend);
    let conns = Arc::clone(&state.conns);
    let on_upgrade = hyper::upgrade::on(&mut req);
    tokio::spawn(async move {
        if let Ok(upgraded) = on_upgrade.await {
            let io = TokioIo::new(upgraded);
            let ws = WebSocketStream::from_raw_socket(io, Role::Server, None).await;
            run_exec_ws(ws, backend, conns, id).await;
        }
    });

    Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header(CONNECTION, "Upgrade")
        .header(UPGRADE, "websocket")
        .header("sec-websocket-accept", accept)
        .body(empty_body())
        .unwrap()
}

async fn run_exec_ws<S>(
    ws: WebSocketStream<S>,
    backend: Arc<dyn ExecBackend>,
    conns: Arc<Mutex<HashMap<u64, watch::Sender<bool>>>>,
    id: u64,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let handler = ExecServerHandler::new_shared(backend);
    let (close_tx, mut close_rx) = watch::channel(false);
    conns.lock().unwrap().insert(id, close_tx);
    let (mut sink, mut stream) = ws.split();

    'ws: loop {
        tokio::select! {
            _ = close_rx.changed() => break,
            frame = stream.next() => {
                let Some(Ok(Message::Text(text))) = frame else {
                    break;
                };
                let Ok(value) = runtime_json::parse(&RuntimeString::from_utf8(&text)) else {
                    continue;
                };
                let Ok(request) = runtime_codec::decode_value::<JsonRpcRequest>(&value) else {
                    continue;
                };
                if let Some(response) = handler.handle_request(request).await {
                    let Some(text) = encode_json_rpc_frame(&response) else {
                        break;
                    };
                    if sink.send(Message::Text(text)).await.is_err() {
                        break;
                    }
                }
                for notification in handler.drain_notifications() {
                    let Some(text) = encode_json_rpc_frame(&notification) else {
                        break 'ws;
                    };
                    if sink.send(Message::Text(text)).await.is_err() {
                        break 'ws;
                    }
                }
            }
        }
    }
    let _ = sink.close().await;
    conns.lock().unwrap().remove(&id);
}

fn encode_json_rpc_frame(value: &impl RuntimeEncode) -> Option<String> {
    let value = runtime_codec::encode_value(value).ok()?;
    runtime_json::stringify(&value).try_to_utf8().ok()
}

fn is_ws_upgrade(req: &Request<Incoming>) -> bool {
    let headers = req.headers();
    let is_upgrade = headers
        .get(UPGRADE)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.eq_ignore_ascii_case("websocket"))
        .unwrap_or(false);
    is_upgrade && headers.contains_key("sec-websocket-key")
}

fn reject_upgrade(code: u16, message: &str) -> Response<ResponseBody> {
    Response::builder()
        .status(StatusCode::from_u16(code).unwrap())
        .header(CONNECTION, "close")
        .header(CONTENT_TYPE, "text/plain")
        .body(full_body(message.to_string()))
        .unwrap()
}

fn format_host(host: &str) -> String {
    if host.contains(':') {
        format!("[{host}]")
    } else {
        host.to_string()
    }
}

fn print_exec_banner(host: &str, port: u16, token: Option<&str>) {
    let h = format_host(host);
    let mut lines = vec![
        "klaus-agent exec-server (WebSocket JSON-RPC)".to_string(),
        format!("  listening on: ws://{h}:{port}"),
    ];
    if is_loopback_host(host) {
        lines.push("  note: loopback only".to_string());
    } else {
        let has_token = token.map(|token| !token.is_empty()).unwrap_or(false);
        lines.push(format!(
            "  note: reachable off-box; bearer token {}",
            if has_token { "required" } else { "MISSING" }
        ));
    }
    eprintln!("{}", lines.join("\n"));
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{SinkExt, StreamExt};
    use klaus_agent_core::command_runtime::exec_server::backend::LocalExecBackend;
    use klaus_agent_core::command_runtime::exec_server::protocol::{
        ExecParams, PathUri, RequestId, HTTP_REQUEST_BODY_DELTA_METHOD, HTTP_REQUEST_METHOD,
        INITIALIZED_METHOD, INITIALIZE_METHOD, PROCESS_READ_METHOD, PROCESS_START_METHOD,
    };
    use klaus_agent_core::runtime_process::RuntimeEnv;
    use serde_json::json;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::Notify;
    use tokio_tungstenite::connect_async;
    use tokio_tungstenite::tungstenite::protocol::Message;

    async fn open_initialized_ws(
        server: &RunningExecServer,
    ) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>
    {
        let (mut ws, _) = connect_async(&server.url).await.unwrap();
        ws.send(Message::Text(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": INITIALIZE_METHOD,
                "params": { "clientName": "ws-test" }
            })
            .to_string(),
        ))
        .await
        .unwrap();
        let init = ws.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(
            serde_json::from_str::<serde_json::Value>(&init).unwrap()["result"]["sessionId"]
                .as_str()
                .unwrap()
                .starts_with("exec-")
        );

        ws.send(Message::Text(
            json!({
                "jsonrpc": "2.0",
                "method": INITIALIZED_METHOD,
                "params": {}
            })
            .to_string(),
        ))
        .await
        .unwrap();
        ws
    }

    async fn spawn_http_server() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let mut buf = vec![0; 1024];
            let _ = socket.read(&mut buf).await;
            let _ = socket
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 5\r\n\r\nhello")
                .await;
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn concurrent_close_waits_for_the_shared_accept_task() {
        let release = Arc::new(Notify::new());
        let accept_started = Arc::new(Notify::new());
        let shutdown_seen = Arc::new(Notify::new());
        let (shutdown, mut shutdown_rx) = watch::channel(false);
        let accept = tokio::spawn({
            let release = Arc::clone(&release);
            let accept_started = Arc::clone(&accept_started);
            let shutdown_seen = Arc::clone(&shutdown_seen);
            async move {
                accept_started.notify_one();
                shutdown_rx.changed().await.unwrap();
                shutdown_seen.notify_one();
                release.notified().await;
            }
        });
        accept_started.notified().await;

        let server = Arc::new(RunningExecServer {
            url: "ws://127.0.0.1:0".to_owned(),
            host: "127.0.0.1".to_owned(),
            port: 0,
            inner: Arc::new(ExecServerHandle {
                shutdown,
                conns: Arc::new(Mutex::new(HashMap::new())),
                accept: AsyncMutex::new(Some(accept)),
            }),
        });

        let first = tokio::spawn({
            let server = Arc::clone(&server);
            async move { server.close().await }
        });
        shutdown_seen.notified().await;

        let mut second = tokio::spawn({
            let server = Arc::clone(&server);
            async move { server.close().await }
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut second)
                .await
                .is_err(),
            "a concurrent close returned before the shared accept task finished"
        );

        release.notify_waiters();
        tokio::time::timeout(std::time::Duration::from_secs(1), first)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), second)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn websocket_exec_server_drives_process_json_rpc() {
        let server = serve_exec_server(ExecServeOptions {
            backend: Arc::new(LocalExecBackend::new(crate::test_process_context())),
            host: None,
            port: None,
            token: None,
            allowed_origins: None,
            silent: true,
        })
        .await
        .unwrap();

        let mut ws = open_initialized_ws(&server).await;

        let cwd = PathUri::from_host_native_path(std::env::current_dir().unwrap()).unwrap();
        let params = ExecParams {
            process_id: "ws-proc".to_string(),
            argv: vec!["sh".into(), "-lc".into(), "printf ws-ok".into()],
            cwd: cwd.into_runtime_path(),
            env_policy: None,
            env: RuntimeEnv::new(),
            tty: false,
            pipe_stdin: false,
            arg0: None,
            sandbox: None,
            enforce_managed_network: false,
            managed_network: None,
        };
        let request = JsonRpcRequest::request(
            RequestId::Number(2),
            PROCESS_START_METHOD,
            runtime_codec::encode_value(&params).unwrap(),
        );
        ws.send(Message::Text(encode_json_rpc_frame(&request).unwrap()))
            .await
            .unwrap();
        let started = ws.next().await.unwrap().unwrap().into_text().unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&started).unwrap()["result"]["processId"],
            "ws-proc"
        );

        ws.send(Message::Text(
            json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": PROCESS_READ_METHOD,
                "params": { "processId": "ws-proc", "afterSeq": 0, "waitMs": 1000 }
            })
            .to_string(),
        ))
        .await
        .unwrap();
        let read = ws.next().await.unwrap().unwrap().into_text().unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&read).unwrap()["result"]["chunks"][0]
                ["chunk"],
            "d3Mtb2s="
        );

        server.close().await;
    }

    #[tokio::test]
    async fn websocket_exec_server_delivers_streamed_http_notifications() {
        let server = serve_exec_server(ExecServeOptions {
            backend: Arc::new(LocalExecBackend::new(crate::test_process_context())),
            host: None,
            port: None,
            token: None,
            allowed_origins: None,
            silent: true,
        })
        .await
        .unwrap();

        let mut ws = open_initialized_ws(&server).await;
        let base = spawn_http_server().await;
        ws.send(Message::Text(
            json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": HTTP_REQUEST_METHOD,
                "params": {
                    "method": "GET",
                    "url": format!("{base}/ok"),
                    "headers": [],
                    "timeoutMs": 1000,
                    "redirectPolicy": "follow",
                    "requestId": "ws-http",
                    "streamResponse": true
                }
            })
            .to_string(),
        ))
        .await
        .unwrap();

        let response = ws.next().await.unwrap().unwrap().into_text().unwrap();
        let response = serde_json::from_str::<serde_json::Value>(&response).unwrap();
        assert_eq!(response["result"]["status"], 200);
        assert_eq!(response["result"]["bodyBase64"], "");

        let notification = ws.next().await.unwrap().unwrap().into_text().unwrap();
        let notification = serde_json::from_str::<serde_json::Value>(&notification).unwrap();
        assert_eq!(notification["method"], HTTP_REQUEST_BODY_DELTA_METHOD);
        assert_eq!(notification["params"]["requestId"], "ws-http");
        assert_eq!(notification["params"]["deltaBase64"], "aGVsbG8=");

        server.close().await;
    }
}
