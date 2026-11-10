//! Regression: live subscription dictation must publish before audio ends and drain final revisions.
use super::super::service::{self, AppServerOptions};
use super::*;
use klaus_agent_core::mcp::credential_store::InMemoryCredentialStore;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

fn seed(root: &Path, account: &str) {
    let expiry = chrono::Utc::now().timestamp_millis() + 3_600_000;
    std::fs::write(super::super::model_sources::codex_store_path(root),
        json!({"defaultAccountId":account,"accounts":{account:{"accountId":account,"accessToken":"fixture-access","accessTokenExpiresAt":expiry}}}).to_string()
    ).unwrap();
}
struct Client {
    writer: tokio::io::WriteHalf<tokio::io::DuplexStream>,
    lines: tokio::io::Lines<BufReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>>,
    events: VecDeque<Value>,
    task: tokio::task::JoinHandle<()>,
}
impl Client {
    async fn start(root: &Path, endpoint: String) -> Self {
        let (client, server) = tokio::io::duplex(128 * 1024);
        let (input, output) = tokio::io::split(server);
        let options = AppServerOptions {
            windows_sandbox_context: None,
            create_session: Arc::new(|_| {
                Box::pin(async { Err("dictation never opens a model turn".into()) })
            }),
            credential_store: Some(Arc::new(InMemoryCredentialStore::new())),
            lifecycle: None,
            plugin_admin: None,
            permission_rules_path: Some(root.join("rules.json")),
            config_path: Some(root.join("config.toml")),
            startup_init_ack_budget: None,
            session_unload_delay: None,
        };
        let mut speech_options = subscription_speech::Options::default();
        speech_options.codex_stream_url = endpoint;
        let task = tokio::spawn(service::run_app_server_with_provider_options(
            input,
            output,
            options,
            Default::default(),
            speech_options,
        ));
        let (reader, writer) = tokio::io::split(client);
        let mut client = Self {
            writer,
            lines: BufReader::new(reader).lines(),
            events: VecDeque::new(),
            task,
        };
        client.request(1, "initialize", json!({})).await;
        client
    }
    async fn next(&mut self) -> Value {
        let line = tokio::time::timeout(Duration::from_secs(5), self.lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        serde_json::from_str(&line).unwrap()
    }
    async fn request(&mut self, id: u64, method: &str, params: Value) -> Value {
        self.writer
            .write_all(format!("{}\n", json!({"id":id,"method":method,"params":params})).as_bytes())
            .await
            .unwrap();
        loop {
            let value = self.next().await;
            if value["id"] == id {
                return value;
            }
            if value["method"] == "engine/dictationEvent" {
                self.events.push_back(value["params"].clone());
            }
        }
    }
    async fn event(&mut self) -> Value {
        if let Some(value) = self.events.pop_front() {
            return value;
        }
        loop {
            let value = self.next().await;
            if value["method"] == "engine/dictationEvent" {
                return value["params"].clone();
            }
        }
    }
    async fn close(mut self) {
        self.writer.shutdown().await.unwrap();
        drop(self.lines);
        self.task.await.unwrap();
    }
}
async fn socket(listener: TcpListener) -> WebSocketStream<TcpStream> {
    let (tcp, _) = listener.accept().await.unwrap();
    tokio_tungstenite::accept_hdr_async(
        tcp,
        |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
         mut response: tokio_tungstenite::tungstenite::handshake::server::Response| {
            assert_eq!(request.uri().path(), "/backend-api/dictation/stream");
            assert_eq!(
                request.uri().query(),
                Some("dictation_surface=composer&client_type=desktop")
            );
            assert_eq!(request.headers()["authorization"], "Bearer fixture-access");
            assert_eq!(request.headers()["chatgpt-account-id"], "fixture-codex");
            assert_eq!(request.headers()["originator"], "Codex Desktop");
            assert_eq!(
                request.headers()["sec-websocket-protocol"],
                "chatgpt-dictation, codex-desktop"
            );
            response.headers_mut().insert(
                "sec-websocket-protocol",
                "chatgpt-dictation".parse().unwrap(),
            );
            Ok(response)
        },
    )
    .await
    .unwrap()
}
async fn receive(socket: &mut WebSocketStream<TcpStream>) -> Value {
    let message = tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_str(message.to_text().unwrap()).unwrap()
}
async fn send(socket: &mut WebSocketStream<TcpStream>, event: Value) {
    socket.send(Message::Text(event.to_string())).await.unwrap();
}
fn started() -> Value {
    json!({"type":"session.started","sequence_no":1,"session":{"status":"active","session_id":"fixture-session","config":{"provider_mode":"streaming_sse","transcript_delivery_mode":"segment"}}})
}
async fn fixture() -> (tempfile::TempDir, TcpListener, Client) {
    let root = tempfile::tempdir().unwrap();
    seed(root.path(), "fixture-codex");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "ws://{}/backend-api/dictation/stream?dictation_surface=composer&client_type=desktop",
        listener.local_addr().unwrap()
    );
    let client = Client::start(root.path(), endpoint).await;
    (root, listener, client)
}

#[tokio::test]
async fn named_facade_streams_pcm_and_revises_utterances_before_audio_ends() {
    let (_root, listener, mut client) = fixture().await;
    let peer = tokio::spawn(async move {
        let mut socket = socket(listener).await;
        let start = receive(&mut socket).await;
        assert_eq!(start, start_message());
        assert_eq!(start["config"]["sample_rate_hz"], 16000);
        send(&mut socket, started()).await;
        let audio = receive(&mut socket).await;
        assert_eq!(audio["type"], "audio.append");
        assert_eq!(
            STANDARD.decode(audio["audio"].as_str().unwrap()).unwrap(),
            [1, 0, 2, 0]
        );
        send(
            &mut socket,
            json!({"type":"speech.started","sequence_no":2,"utterance_id":"a"}),
        )
        .await;
        send(&mut socket, json!({"type":"transcript.segment","sequence_no":3,"utterance_id":"a","revision":1,"text":"interim"})).await;
        assert_eq!(
            receive(&mut socket).await,
            json!({"type":"audio.append","audio":"AwAEAA=="})
        );
        send(&mut socket, json!({"type":"transcript.segment","sequence_no":4,"utterance_id":"a","revision":2,"text":"corrected interim"})).await;
        assert_eq!(receive(&mut socket).await, json!({"type":"session.close"}));
        for event in [
            json!({"type":"transcript.final","sequence_no":5,"utterance_id":"a","revision":1,"text":"First sentence."}),
            json!({"type":"transcript.final","sequence_no":6,"utterance_id":"b","revision":1,"text":"First sentence."}),
            json!({"type":"session.updated","sequence_no":7,"session":{"status":"closed"}}),
        ] {
            send(&mut socket, event).await;
        }
        let _ = socket.next().await;
    });
    let opened = client
        .request(
            2,
            "engine/dictationStart",
            json!({"source":"codex","operationId":"live"}),
        )
        .await;
    assert_eq!(opened["result"]["streaming"], true, "{opened}");
    assert_eq!(
        opened["result"]["credentialGeneration"]
            .as_str()
            .unwrap()
            .len(),
        64
    );
    assert!(client
        .request(
            3,
            "engine/dictationAppendAudio",
            json!({"operationId":"live","seq":1,"audioBase64":"AQACAA=="})
        )
        .await
        .get("error")
        .is_none());
    let first = client.event().await;
    assert_eq!(first["type"], "partial");
    assert_eq!(first["text"], "interim");
    client
        .request(
            4,
            "engine/dictationAppendAudio",
            json!({"operationId":"live","seq":2,"audioBase64":"AwAEAA=="}),
        )
        .await;
    assert_eq!(client.event().await["text"], "corrected interim");
    client
        .request(5, "engine/dictationEnd", json!({"operationId":"live"}))
        .await;
    loop {
        let event = client.event().await;
        if event["type"] == "final" {
            assert_eq!(event["text"], "First sentence. First sentence.");
            assert_eq!(event["rawText"], "First sentence. First sentence.");
            break;
        }
    }
    peer.await.unwrap();
    client.close().await;
}

#[tokio::test]
async fn close_acknowledgment_cannot_publish_an_unfinalized_preview() {
    let (_root, listener, mut client) = fixture().await;
    let peer = tokio::spawn(async move {
        let mut socket = socket(listener).await;
        receive(&mut socket).await;
        send(&mut socket, started()).await;
        receive(&mut socket).await;
        send(&mut socket, json!({"type":"transcript.segment","sequence_no":2,"utterance_id":"a","revision":1,"text":"unfinished"})).await;
        receive(&mut socket).await;
        send(
            &mut socket,
            json!({"type":"session.updated","sequence_no":3,"session":{"status":"closed"}}),
        )
        .await;
    });
    client
        .request(
            2,
            "engine/dictationStart",
            json!({"source":"codex","operationId":"tail"}),
        )
        .await;
    client
        .request(
            3,
            "engine/dictationAppendAudio",
            json!({"operationId":"tail","seq":1,"audioBase64":"AQACAA=="}),
        )
        .await;
    assert_eq!(client.event().await["type"], "partial");
    client
        .request(4, "engine/dictationEnd", json!({"operationId":"tail"}))
        .await;
    let event = client.event().await;
    assert_eq!(event["type"], "error");
    assert_eq!(event["code"], "codex_transcription_incomplete");
    peer.await.unwrap();
    client.close().await;
}

#[tokio::test]
async fn account_change_closes_the_live_socket_and_fences_retained_audio() {
    let (root, listener, mut client) = fixture().await;
    let peer = tokio::spawn(async move {
        let mut socket = socket(listener).await;
        receive(&mut socket).await;
        send(&mut socket, started()).await;
        let _ = tokio::time::timeout(Duration::from_secs(3), socket.next())
            .await
            .unwrap();
    });
    let started = client
        .request(
            2,
            "engine/dictationStart",
            json!({"source":"codex","operationId":"bound"}),
        )
        .await;
    let generation = started["result"]["credentialGeneration"].clone();
    seed(root.path(), "replacement-account");
    let event = client.event().await;
    assert_eq!(event["type"], "error");
    assert_eq!(event["code"], "codex_transcription_credential_changed");
    let recovery = client.request(3, "codex/transcribeAudio", json!({"operationId":"retained","expectedCredentialGeneration":generation,"mimeType":"audio/wav","audioBase64":"AQACAA=="})).await;
    assert_eq!(
        recovery["error"]["message"],
        "codex_transcription_credential_changed"
    );
    peer.await.unwrap();
    client.close().await;
}

#[tokio::test]
async fn cancellation_drains_the_socket_without_a_late_final() {
    let (_root, listener, mut client) = fixture().await;
    let peer = tokio::spawn(async move {
        let mut socket = socket(listener).await;
        receive(&mut socket).await;
        send(&mut socket, started()).await;
        let _ = tokio::time::timeout(Duration::from_secs(3), socket.next())
            .await
            .unwrap();
    });
    client
        .request(
            2,
            "engine/dictationStart",
            json!({"source":"codex","operationId":"cancel"}),
        )
        .await;
    let ended = client
        .request(
            3,
            "engine/dictationEnd",
            json!({"operationId":"cancel","cancel":true}),
        )
        .await;
    assert_eq!(ended["result"]["complete"], true);
    assert!(client.events.is_empty());
    let appended = client
        .request(
            4,
            "engine/dictationAppendAudio",
            json!({"operationId":"cancel","seq":1,"audioBase64":"AQA="}),
        )
        .await;
    assert_eq!(appended["error"]["message"], "dictation-session-not-owned");
    peer.await.unwrap();
    client.close().await;
}

#[tokio::test]
async fn startup_refusal_and_buffered_negotiation_do_not_admit_audio() {
    for refuse in [true, false] {
        let (_root, listener, mut client) = fixture().await;
        let peer = tokio::spawn(async move {
            let mut socket = socket(listener).await;
            receive(&mut socket).await;
            let event = if refuse {
                json!({"type":"session.error","sequence_no":1,"fatal":true,"error":{"code":"rate_limited","message":"fixture refusal","retryable":false}})
            } else {
                let mut event = started();
                event["session"]["config"]["provider_mode"] = json!("buffered");
                event
            };
            send(&mut socket, event).await;
        });
        let response = client
            .request(
                2,
                "engine/dictationStart",
                json!({"source":"codex","operationId":"refused"}),
            )
            .await;
        assert!(response.get("error").is_some(), "{response}");
        let event = client.event().await;
        assert_eq!(event["type"], "error");
        assert_eq!(
            event["code"],
            if refuse {
                "codex_transcription_rate_limited"
            } else {
                "codex_transcription_response_invalid"
            }
        );
        peer.await.unwrap();
        client.close().await;
    }
}

#[tokio::test(start_paused = true)]
async fn stalled_audio_and_close_writes_have_a_short_recoverable_deadline() {
    let (stream, _peer) = tokio::io::duplex(32);
    let mut socket = WebSocketStream::from_raw_socket(
        stream,
        tokio_tungstenite::tungstenite::protocol::Role::Client,
        None,
    )
    .await;
    let began = tokio::time::Instant::now();
    let failure = send_frame(&mut socket, Message::Text("x".repeat(65_536)))
        .await
        .unwrap_err();
    assert_eq!(failure, "codex_transcription_transport_failed");
    assert_eq!(tokio::time::Instant::now() - began, Duration::from_secs(5));
}

#[test]
fn revision_snapshots_preserve_repeated_utterances_and_reject_stale_sequences() {
    let mut transcript = Transcript::default();
    for (sequence, kind, id, revision, text) in [
        (1, "transcript.segment", "a", 2, "draft"),
        (2, "transcript.segment", "a", 1, "stale"),
        (3, "transcript.final", "a", 1, "repeat"),
        (4, "transcript.segment", "a", 4, "late preview"),
        (5, "transcript.final", "b", 1, "repeat"),
    ] {
        transcript.accept(&json!({"type":kind,"sequence_no":sequence,"utterance_id":id,"revision":revision,"text":text})).unwrap();
    }
    assert_eq!(transcript.complete_text().unwrap(), "repeat repeat");
    assert_eq!(
        transcript
            .accept(&json!({"type":"speech.stopped","sequence_no":5}))
            .unwrap_err(),
        "codex_transcription_sequence_invalid"
    );
    assert_eq!(
        transcript
            .accept(
                &json!({"type":"transcript.failed","sequence_no":6,"error":{"code":"anything"}})
            )
            .unwrap_err(),
        "codex_transcription_provider_failed"
    );
}
