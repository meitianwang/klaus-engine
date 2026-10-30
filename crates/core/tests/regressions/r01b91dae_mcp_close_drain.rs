//! Defect: retiring an idle session could leave every later send blocked on failed shutdown.
//! Root cause: MCP remote DELETE and server-request response writes could wait indefinitely.
//! Fixed by: 01b91dae4f0e4b591cb17863ad8fc24a1c3a1077.

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::mcp::client::{Client, RequestOptions};
use klaus_agent_core::mcp::transports::HttpTransport;
use klaus_agent_core::mcp::types::{McpServerConfig, McpServerOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::types::provider::{ModelStopReason, ModelStreamEvent};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::task::{JoinHandle, JoinSet};

const LIMIT: Duration = Duration::from_secs(8);

struct SilentCloseServer {
    url: String,
    deletes: Arc<AtomicUsize>,
    delete_started: Arc<Notify>,
    response_started: Arc<Notify>,
    stream_closed: Arc<Notify>,
    task: JoinHandle<()>,
}

impl SilentCloseServer {
    async fn start(ask_roots: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/mcp", listener.local_addr().unwrap());
        let deletes = Arc::new(AtomicUsize::new(0));
        let response_started = Arc::new(Notify::new());
        let delete_started = Arc::new(Notify::new());
        let stream_closed = Arc::new(Notify::new());
        let counted = deletes.clone();
        let observed = response_started.clone();
        let deleting = delete_started.clone();
        let streaming = stream_closed.clone();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (socket, _) = accepted.unwrap();
                        let counted = counted.clone();
                        let observed = observed.clone();
                        let deleting = deleting.clone();
                        let streaming = streaming.clone();
                        connections.spawn(async move {
                            let mut reader = BufReader::new(socket);
                            let mut line = String::new();
                            reader.read_line(&mut line).await.unwrap();
                            let delete = line.starts_with("DELETE ");
                            let mut length = 0;
                            loop {
                                line.clear();
                                reader.read_line(&mut line).await.unwrap();
                                if line == "\r\n" || line.is_empty() { break; }
                                if let Some((name, value)) = line.split_once(':') {
                                    if name.eq_ignore_ascii_case("content-length") {
                                        length = value.trim().parse().unwrap();
                                    }
                                }
                            }
                            let mut body = vec![0; length];
                            reader.read_exact(&mut body).await.unwrap();
                            if delete {
                                counted.fetch_add(1, Ordering::SeqCst);
                                deleting.notify_one();
                                // Keep the socket alive without acknowledging the remote deletion.
                                let mut byte = [0];
                                let _ = reader.read(&mut byte).await;
                                return;
                            }
                            let frame: Value = serde_json::from_slice(&body).unwrap();
                            if frame.get("method").is_none() {
                                observed.notify_one();
                                // A roots/list response POST must be cancellable during close.
                                let mut byte = [0];
                                let _ = reader.read(&mut byte).await;
                                return;
                            }
                            let method = frame["method"].as_str().unwrap();
                            let result = match method {
                                "initialize" => Some(json!({
                                    "protocolVersion":"2025-11-25","capabilities":{},
                                    "serverInfo":{"name":"silent-close","version":"1"}
                                })),
                                "resources/list" => Some(json!({"resources":[]})),
                                "resources/templates/list" => Some(json!({"resourceTemplates":[]})),
                                "prompts/list" => Some(json!({"prompts":[]})),
                                "tools/list" => Some(json!({"tools":[]})),
                                _ => None,
                            };
                            let response = result.map(|result| json!({
                                "jsonrpc":"2.0","id":frame["id"],"result":result
                            }).to_string());
                            let socket = reader.get_mut();
                            if method == "notifications/initialized" && ask_roots {
                                let event = format!("data: {}\n\n", json!({"jsonrpc":"2.0","id":41,"method":"roots/list","params":{}}));
                                socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n").await.unwrap();
                                socket.write_all(event.as_bytes()).await.unwrap();
                                socket.flush().await.unwrap();
                                // An open response stream also has to be released locally.
                                let mut byte = [0];
                                let _ = reader.read(&mut byte).await;
                                streaming.notify_one();
                            } else if let Some(body) = response {
                                let headers = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nmcp-session-id: fixture-session\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
                                socket.write_all(headers.as_bytes()).await.unwrap();
                                socket.write_all(body.as_bytes()).await.unwrap();
                            } else {
                                socket.write_all(b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
                            }
                        });
                    }
                    Some(result) = connections.join_next(), if !connections.is_empty() => result.unwrap(),
                }
            }
        });
        Self {
            url,
            deletes,
            delete_started,
            response_started,
            stream_closed,
            task,
        }
    }

    fn config(&self) -> McpServerConfig {
        McpServerConfig::Http {
            name: "silent-close".to_string(),
            url: self.url.clone(),
            headers: None,
            oauth: None,
            options: McpServerOptions {
                startup_timeout_ms: Some(5_000),
                ..Default::default()
            },
        }
    }
}

impl Drop for SilentCloseServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
async fn session_close_releases_the_writer_and_allows_resume_when_remote_delete_stalls() {
    let server = SilentCloseServer::start(false).await;
    let directory = tempfile::tempdir().unwrap();
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut agent_config = agent_options(process.clone());
    agent_config.deps.call_model = Some(Box::new(|_, _| {
        Box::pin(futures::stream::iter([
            ModelStreamEvent::MessageStart {
                model: "fixture".into(),
            },
            ModelStreamEvent::TextDelta {
                text: "received".into(),
            },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: Usage {
                    input_tokens: 1,
                    output_tokens: 1,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: None,
                },
            },
        ]))
    }));
    let agent = Agent::new(agent_config);
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(
            directory
                .path()
                .join("sessions")
                .to_string_lossy()
                .into_owned(),
        ),
        archive_dir: None,
    }));
    let options = || StartSessionOptions {
        cwd: Some(directory.path().to_string_lossy().into_owned().into()),
        user_config_dir: Some(
            directory
                .path()
                .join("config")
                .to_string_lossy()
                .into_owned()
                .into(),
        ),
        ..Default::default()
    };
    let session = agent.open_session(store.clone(), options()).await.unwrap();
    session
        .reload_mcp_servers(vec![server.config()])
        .await
        .unwrap();
    let before = session
        .run(
            Input::Text("before retirement".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(before.error.is_none(), "{:?}", before.error);
    let thread_id = session.id.try_to_utf8().unwrap();
    tokio::time::timeout(LIMIT, session.shutdown_and_wait())
        .await
        .expect("remote DELETE must not consume the session shutdown deadline")
        .unwrap();
    // A second caller receives the same successful close receipt.
    session.shutdown_and_wait().await.unwrap();
    assert_eq!(server.deletes.load(Ordering::SeqCst), 1);
    let resumed = agent
        .resume_session(store, &thread_id, options())
        .await
        .expect("the retired session must release its real rollout writer lease");
    let after = resumed
        .run(
            Input::Text("after retirement".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(after.error.is_none(), "{:?}", after.error);
    let history = resumed.history();
    let text: Vec<_> = history
        .iter()
        .filter_map(|message| match message {
            Message::User(user) => Some(format!("{:?}", user.content)),
            _ => None,
        })
        .collect();
    assert!(text.iter().any(|text| text.contains("before retirement")));
    assert!(text.iter().any(|text| text.contains("after retirement")));
    resumed.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn client_close_cancels_a_blocked_server_response_and_open_event_stream() {
    let server = SilentCloseServer::start(true).await;
    let client = Client::new(None);
    let transport = Arc::new(HttpTransport::new(server.url.clone(), HashMap::new()));
    client
        .connect(
            transport,
            RequestOptions {
                timeout_ms: 5_000,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    tokio::time::timeout(LIMIT, server.response_started.notified())
        .await
        .unwrap();
    tokio::time::timeout(LIMIT, client.close())
        .await
        .expect("a server response POST must not retain the closing client")
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), client.close())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), server.stream_closed.notified())
        .await
        .expect("closing the client must release the open HTTP event stream");
    assert_eq!(server.deletes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancelling_the_first_close_waiter_does_not_cancel_connection_cleanup() {
    let server = SilentCloseServer::start(false).await;
    let client = Client::new(None);
    let transport = Arc::new(HttpTransport::new(server.url.clone(), HashMap::new()));
    client
        .connect(
            transport,
            RequestOptions {
                timeout_ms: 5_000,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let first_client = client.clone();
    let first = tokio::spawn(async move { first_client.close().await });
    tokio::time::timeout(LIMIT, server.delete_started.notified())
        .await
        .unwrap();
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let mut retry = Box::pin(client.close());
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut retry)
            .await
            .is_err(),
        "a concurrent close must join the retained remote cleanup"
    );
    tokio::time::timeout(LIMIT, retry).await.unwrap().unwrap();
    assert_eq!(server.deletes.load(Ordering::SeqCst), 1);
}
