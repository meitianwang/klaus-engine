use super::super::service::{self, AppServerOptions};
use super::*;
use klaus_agent_core::mcp::credential_store::InMemoryCredentialStore;
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

fn wav(pcm: &[u8]) -> Vec<u8> {
    let mut bytes = b"RIFF".to_vec();
    bytes.extend_from_slice(&(36u32 + pcm.len() as u32).to_le_bytes());
    bytes.extend_from_slice(b"WAVEfmt ");
    bytes.extend_from_slice(&16u32.to_le_bytes());
    bytes.extend_from_slice(&1u16.to_le_bytes());
    bytes.extend_from_slice(&1u16.to_le_bytes());
    bytes.extend_from_slice(&16_000u32.to_le_bytes());
    bytes.extend_from_slice(&32_000u32.to_le_bytes());
    bytes.extend_from_slice(&2u16.to_le_bytes());
    bytes.extend_from_slice(&16u16.to_le_bytes());
    bytes.extend_from_slice(b"data");
    bytes.extend_from_slice(&(pcm.len() as u32).to_le_bytes());
    bytes.extend_from_slice(pcm);
    bytes
}

fn seed(root: &Path, source: Source) {
    let expiry = chrono::Utc::now().timestamp_millis() + 3_600_000;
    let (path, value) = match source {
        Source::Claude => (
            model_sources::claude_subscription_path(root),
            json!({"version":1,"accounts":[{"id":"fixture-claude","tokens":{"accessToken":"fixture-claude-access","expiresAt":expiry}}]}),
        ),
        Source::Codex => (
            model_sources::codex_store_path(root),
            json!({"defaultAccountId":"fixture-codex","accounts":{"fixture-codex":{"accountId":"fixture-codex","accessToken":"fixture-codex-access","accessTokenExpiresAt":expiry}}}),
        ),
    };
    std::fs::write(path, serde_json::to_vec(&value).unwrap()).unwrap();
}

struct Client {
    writer: tokio::io::WriteHalf<tokio::io::DuplexStream>,
    lines: tokio::io::Lines<BufReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Client {
    async fn start(root: &Path, options: Options) -> Self {
        let (client, server) = tokio::io::duplex(128 * 1024);
        let (input, output) = tokio::io::split(server);
        let config_path = root.join("config.toml");
        let task = tokio::spawn(service::run_app_server_with_provider_options(
            input,
            output,
            AppServerOptions {
                windows_sandbox_context: None,
                create_session: Arc::new(|_| {
                    Box::pin(async { Err("no model session in dictation fixture".into()) })
                }),
                credential_store: Some(Arc::new(InMemoryCredentialStore::new())),
                lifecycle: None,
                plugin_admin: None,
                permission_rules_path: Some(root.join("rules.json")),
                config_path: Some(config_path),
                startup_init_ack_budget: None,
                session_unload_delay: None,
            },
            Default::default(),
            options,
        ));
        let (reader, writer) = tokio::io::split(client);
        let mut client = Self {
            writer,
            lines: BufReader::new(reader).lines(),
            task,
        };
        client
            .send(json!({"id":1,"method":"initialize","params":{}}))
            .await;
        let initialized = client.next().await;
        assert!(initialized.get("error").is_none(), "{initialized}");
        client
    }
    async fn send(&mut self, value: Value) {
        let mut line = serde_json::to_vec(&value).unwrap();
        line.push(b'\n');
        self.writer.write_all(&line).await.unwrap();
        self.writer.flush().await.unwrap();
    }
    async fn next(&mut self) -> Value {
        let line = tokio::time::timeout(Duration::from_secs(5), self.lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        serde_json::from_str(&line).unwrap()
    }
    async fn close(mut self) {
        self.writer.shutdown().await.unwrap();
        drop(self.lines);
        self.task.await.unwrap();
    }
}

#[test]
fn pcm_parser_requires_complete_mono_pcm16_16khz_and_never_reinterprets_a_container() {
    let pcm = [1, 0, 2, 0];
    let bytes = wav(&pcm);
    assert_eq!(pcm_audio(&bytes, "audio/wav").unwrap(), pcm);
    assert_eq!(pcm_audio(&pcm, "audio/pcm").unwrap(), pcm);
    assert!(pcm_audio(&pcm[..3], "audio/pcm").is_err());
    for offset in [20, 22, 24, 28, 32, 34] {
        let mut invalid = bytes.clone();
        invalid[offset] ^= 1;
        assert!(pcm_audio(&invalid, "audio/wav").is_err());
    }
    assert!(pcm_audio(&bytes[..bytes.len() - 1], "audio/wav").is_err());
    assert!(mime(Some("audio/pcm;rate=8000")).is_err());
    assert!(mime(Some("audio/L16")).is_err());
    assert!(mime(Some("audio/wav\r\nAuthorization: bad")).is_err());
    assert!(language(Some("zh-CN")).is_err());
    assert_eq!(language(Some("ZH")).unwrap(), Some("zh".into()));
}

#[tokio::test]
async fn claude_named_facade_sends_only_pcm_and_assembles_finalized_native_transcript() {
    let root = tempfile::tempdir().unwrap();
    seed(root.path(), Source::Claude);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "ws://{}/api/ws/speech_to_text/voice_stream",
        listener.local_addr().unwrap()
    );
    let expected = vec![10, 0, 20, 0, 30, 0];
    let pcm = expected.clone();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_hdr_async(
            socket,
            |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                assert!(request.uri().query().unwrap().contains("language=zh"));
                assert!(request.uri().query().unwrap().contains("sample_rate=16000"));
                assert_eq!(
                    request.headers()["authorization"],
                    "Bearer fixture-claude-access"
                );
                assert_eq!(request.headers()["x-app"], "cli");
                assert_eq!(
                    request.headers()["anthropic-client-platform"],
                    "claude_code_cli"
                );
                Ok(response)
            },
        )
        .await
        .unwrap();
        let mut received = Vec::new();
        loop {
            match socket.next().await.unwrap().unwrap() {
                Message::Binary(bytes) => {
                    received.extend_from_slice(&bytes);
                    assert!(received.len() <= 100);
                }
                Message::Text(text) if text.contains("CloseStream") => break,
                Message::Text(text) => assert!(text.contains("KeepAlive")),
                other => panic!("unexpected fixture frame: {other:?}"),
            }
        }
        assert_eq!(received, expected);
        for event in [
            json!({"type":"TranscriptInterim","data":"wrong interim"}),
            json!({"type":"TranscriptText","data":"正确的听写。"}),
            json!({"type":"TranscriptEndpoint"}),
        ] {
            socket.send(Message::Text(event.to_string())).await.unwrap();
        }
        // The request owns no persistent upstream voice conversation after publication.
        let closed = tokio::time::timeout(Duration::from_secs(5), socket.next())
            .await
            .unwrap();
        assert!(!matches!(
            closed,
            Some(Ok(Message::Text(_) | Message::Binary(_)))
        ));
    });
    let mut client = Client::start(
        root.path(),
        Options {
            claude_url: url,
            ..Default::default()
        },
    )
    .await;
    client.send(json!({"id":2,"method":"claude/transcribeAudio","params":{"audioBase64":base64::engine::general_purpose::STANDARD.encode(wav(&pcm)),"mimeType":"audio/wav","language":"zh","operationId":"claude-native"}})).await;
    let response = client.next().await;
    assert_eq!(
        response["result"],
        json!({"text":"正确的听写。"}),
        "{response}"
    );
    server.await.unwrap();
    client.close().await;
}

async fn dictation_response(
    client: &mut Client,
    id: u64,
    events: &mut std::collections::VecDeque<Value>,
) -> Value {
    loop {
        let value = client.next().await;
        if value["id"] == id {
            return value;
        }
        if value["method"] == "engine/dictationEvent" {
            events.push_back(value["params"].clone());
        }
    }
}

async fn dictation_event(
    client: &mut Client,
    events: &mut std::collections::VecDeque<Value>,
) -> Value {
    if let Some(event) = events.pop_front() {
        return event;
    }
    loop {
        let value = client.next().await;
        if value["method"] == "engine/dictationEvent" {
            return value["params"].clone();
        }
    }
}

#[tokio::test]
async fn claude_live_named_facade_publishes_before_stop_and_preserves_every_utterance() {
    for language in [None, Some("zh")] {
        let root = tempfile::tempdir().unwrap();
        seed(root.path(), Source::Claude);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "ws://{}/api/ws/speech_to_text/voice_stream",
            listener.local_addr().unwrap()
        );
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_hdr_async(
                socket,
                |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                    let query = request.uri().query().unwrap();
                    assert!(query.contains("sample_rate=16000"));
                    assert!(query.contains("forward_interims=typed"));
                    assert!(query.contains(&format!("language={}", language.unwrap_or("en"))));
                    assert_eq!(
                        request.headers()["authorization"],
                        "Bearer fixture-claude-access"
                    );
                    Ok(response)
                },
            )
            .await
            .unwrap();
            assert!(socket
                .next()
                .await
                .unwrap()
                .unwrap()
                .to_text()
                .unwrap()
                .contains("KeepAlive"));
            assert_eq!(
                socket.next().await.unwrap().unwrap(),
                Message::Binary(vec![1, 0])
            );
            socket
                .send(Message::Text(
                    json!({"type":"TranscriptInterim","data":"draft"}).to_string(),
                ))
                .await
                .unwrap();
            assert_eq!(
                socket.next().await.unwrap().unwrap(),
                Message::Binary(vec![2, 0])
            );
            socket
                .send(Message::Text(
                    json!({"type":"TranscriptText","data":"First."}).to_string(),
                ))
                .await
                .unwrap();
            assert!(socket
                .next()
                .await
                .unwrap()
                .unwrap()
                .to_text()
                .unwrap()
                .contains("CloseStream"));
            for value in [
                json!({"type":"TranscriptEndpoint"}),
                json!({"type":"TranscriptText","data":"First."}),
                json!({"type":"TranscriptEndpoint"}),
            ] {
                socket.send(Message::Text(value.to_string())).await.unwrap();
            }
            socket.close(None).await.unwrap();
        });
        let mut client = Client::start(
            root.path(),
            Options {
                claude_url: url,
                ..Default::default()
            },
        )
        .await;
        let mut events = std::collections::VecDeque::new();
        client.send(json!({"id":2,"method":"engine/dictationStart","params":{"source":"claude","operationId":"claude-live","language":language}})).await;
        let response = dictation_response(&mut client, 2, &mut events).await;
        assert_eq!(response["result"]["streaming"], true, "{response}");
        assert_eq!(
            response["result"]["credentialGeneration"]
                .as_str()
                .unwrap()
                .len(),
            64
        );
        client.send(json!({"id":3,"method":"engine/dictationAppendAudio","params":{"operationId":"claude-live","seq":1,"audioBase64":"AQA="}})).await;
        assert!(dictation_response(&mut client, 3, &mut events)
            .await
            .get("error")
            .is_none());
        let first = dictation_event(&mut client, &mut events).await;
        assert_eq!(first["type"], "partial");
        assert_eq!(first["text"], "draft");
        client.send(json!({"id":4,"method":"engine/dictationAppendAudio","params":{"operationId":"claude-live","seq":2,"audioBase64":"AgA="}})).await;
        dictation_response(&mut client, 4, &mut events).await;
        assert_eq!(
            dictation_event(&mut client, &mut events).await["text"],
            "First."
        );
        client
        .send(json!({"id":5,"method":"engine/dictationEnd","params":{"operationId":"claude-live"}}))
        .await;
        dictation_response(&mut client, 5, &mut events).await;
        loop {
            let event = dictation_event(&mut client, &mut events).await;
            if event["type"] == "final" {
                assert_eq!(event["text"], "First. First.");
                break;
            }
            assert_eq!(event["type"], "partial", "{event}");
        }
        server.await.unwrap();
        client.close().await;
    }
}

#[tokio::test]
async fn claude_live_cancellation_drains_the_native_socket_without_a_final_event() {
    let root = tempfile::tempdir().unwrap();
    seed(root.path(), Source::Claude);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/voice", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(socket).await.unwrap();
        socket.next().await.unwrap().unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(3), socket.next())
            .await
            .unwrap();
    });
    let mut client = Client::start(
        root.path(),
        Options {
            claude_url: url,
            ..Default::default()
        },
    )
    .await;
    let mut events = std::collections::VecDeque::new();
    client.send(json!({"id":2,"method":"engine/dictationStart","params":{"source":"claude","operationId":"cancel-live"}})).await;
    assert!(dictation_response(&mut client, 2, &mut events)
        .await
        .get("error")
        .is_none());
    client.send(json!({"id":3,"method":"engine/dictationEnd","params":{"operationId":"cancel-live","cancel":true}})).await;
    assert_eq!(
        dictation_response(&mut client, 3, &mut events).await["result"]["complete"],
        true
    );
    assert!(events.is_empty());
    server.await.unwrap();
    client.close().await;
}

#[tokio::test]
async fn claude_recovery_checks_the_original_login_generation_before_opening_a_socket() {
    let root = tempfile::tempdir().unwrap();
    seed(root.path(), Source::Claude);
    let store = Arc::new(InMemoryCredentialStore::new());
    let credential = claude_credential(root.path(), Some(store.clone()), &Default::default(), None)
        .await
        .unwrap();
    assert_eq!(credential.generation.len(), 64);
    let expiry = chrono::Utc::now().timestamp_millis() + 3_600_000;
    std::fs::write(model_sources::claude_subscription_path(root.path()),
        json!({"version":1,"accounts":[{"id":"replacement","tokens":{"accessToken":"replacement-access","expiresAt":expiry}}]}).to_string()).unwrap();
    assert!(!(credential.current)());
    let params = serde_json::from_value(json!({"operationId":"retained","audioBase64":base64::engine::general_purpose::STANDARD.encode(wav(&[1,0])),"mimeType":"audio/wav","expectedCredentialGeneration":credential.generation})).unwrap();
    let failure = transcribe(
        root.path().to_path_buf(),
        Some(store),
        Source::Claude,
        params,
        AbortSignal::new(),
        Options::default(),
        Default::default(),
    )
    .await
    .err()
    .unwrap();
    assert_eq!(failure, "claude_transcription_credential_changed");
}

async fn receive_http(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    loop {
        let mut buffer = [0u8; 4096];
        let read = socket.read(&mut buffer).await.unwrap();
        assert!(read > 0);
        bytes.extend_from_slice(&buffer[..read]);
        assert!(bytes.len() < 256 * 1024);
        if let Some(end) = bytes
            .windows(4)
            .position(|v| v == b"\r\n\r\n")
            .map(|i| i + 4)
        {
            let head = std::str::from_utf8(&bytes[..end]).unwrap();
            let length = head
                .lines()
                .find_map(|line| {
                    let (k, v) = line.split_once(':')?;
                    k.eq_ignore_ascii_case("content-length")
                        .then(|| v.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            if bytes.len() >= end + length {
                return bytes;
            }
        }
    }
}

#[tokio::test]
async fn codex_named_facade_uses_subscription_transcription_without_model_or_public_api_path() {
    let root = tempfile::tempdir().unwrap();
    seed(root.path(), Source::Codex);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/transcribe", listener.local_addr().unwrap());
    let audio = wav(&[0, 0, 1, 0]);
    let expected = audio.clone();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let bytes = receive_http(&mut socket).await;
        let text = String::from_utf8_lossy(&bytes);
        assert!(text.starts_with("POST /transcribe HTTP/1.1"));
        assert!(text.contains("Bearer fixture-codex-access"));
        assert!(text
            .to_ascii_lowercase()
            .contains("user-agent: codex desktop/"));
        assert!(text
            .to_ascii_lowercase()
            .contains("accept: application/json"));
        assert!(text
            .to_ascii_lowercase()
            .contains("chatgpt-account-id: fixture-codex"));
        assert!(text.contains("name=\"language\"\r\n\r\nzh"));
        assert!(!text.contains("name=\"model\""));
        assert!(bytes
            .windows(expected.len())
            .any(|window| window == expected));
        let body=json!({"text":"standalone dictation","asset_pointer":"not-published","access_token":"not-published"}).to_string();
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
    });
    let mut client = Client::start(
        root.path(),
        Options {
            codex_url: url,
            ..Default::default()
        },
    )
    .await;
    client.send(json!({"id":2,"method":"codex/transcribeAudio","params":{"audioBase64":base64::engine::general_purpose::STANDARD.encode(audio),"mimeType":"audio/wav","language":"zh"}})).await;
    let response = client.next().await;
    assert_eq!(
        response["result"],
        json!({"text":"standalone dictation"}),
        "{response}"
    );
    server.await.unwrap();
    client.close().await;
}

#[tokio::test]
async fn cancel_transcription_closes_its_real_http_request_and_does_not_cancel_other_source_ids() {
    let root = tempfile::tempdir().unwrap();
    seed(root.path(), Source::Codex);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/transcribe", listener.local_addr().unwrap());
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        receive_http(&mut socket).await;
        entered_tx.send(()).unwrap();
        let mut byte = [0];
        let closed = tokio::time::timeout(Duration::from_secs(5), socket.read(&mut byte))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(closed, 0);
    });
    let mut client = Client::start(
        root.path(),
        Options {
            codex_url: url,
            ..Default::default()
        },
    )
    .await;
    client.send(json!({"id":2,"method":"codex/transcribeAudio","params":{"audioBase64":"AAAAAA==","operationId":"active"}})).await;
    entered_rx.await.unwrap();
    client
        .send(
            json!({"id":3,"method":"claude/cancelTranscription","params":{"operationId":"active"}}),
        )
        .await;
    assert_eq!(client.next().await["result"]["cancelled"], false);
    client
        .send(
            json!({"id":4,"method":"codex/cancelTranscription","params":{"operationId":"active"}}),
        )
        .await;
    let a = client.next().await;
    let b = client.next().await;
    let response = if a["id"] == 2 { a } else { b };
    assert_eq!(response["error"]["message"], "provider_operation_cancelled");
    server.await.unwrap();
    client.close().await;
}

#[tokio::test]
async fn credential_replacement_closes_inflight_transcription_without_releasing_text() {
    let root = tempfile::tempdir().unwrap();
    seed(root.path(), Source::Codex);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/transcribe", listener.local_addr().unwrap());
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        receive_http(&mut socket).await;
        entered_tx.send(()).unwrap();
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), socket.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    });
    let mut client = Client::start(
        root.path(),
        Options {
            codex_url: url,
            ..Default::default()
        },
    )
    .await;
    client
        .send(json!({"id":2,"method":"codex/transcribeAudio","params":{"audioBase64":"AAAAAA=="}}))
        .await;
    entered_rx.await.unwrap();
    std::fs::remove_file(model_sources::codex_store_path(root.path())).unwrap();
    let response = client.next().await;
    assert_eq!(
        response["error"]["message"],
        "codex_transcription_credential_changed"
    );
    server.await.unwrap();
    client.close().await;
}

#[tokio::test]
async fn upstream_error_bodies_and_oversized_transcripts_do_not_escape() {
    for (status, body, expected) in [
        (
            403,
            "fixture-private-token-and-audio".to_owned(),
            "codex_transcription_permission_denied",
        ),
        (
            200,
            json!({"text":"x".repeat(MAX_TEXT+1)}).to_string(),
            "codex_transcription_response_invalid",
        ),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/transcribe", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            receive_http(&mut socket).await;
            socket.write_all(format!("HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
        });
        let error = codex_request(
            &url,
            "fixture-token",
            "fixture-account",
            vec![0, 0],
            "audio/wav",
            "wav",
            None,
        )
        .await
        .unwrap_err();
        assert_eq!(error, expected);
        server.await.unwrap();
    }
}

#[test]
fn desktop_version_metadata_is_bounded_and_header_safe() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let write = |version: &str| {
        std::fs::write(file.path(), format!("<plist><dict><key>CFBundleShortVersionString</key><string>{version}</string></dict></plist>")).unwrap()
    };
    write("1.2.345");
    assert_eq!(desktop_bundle_version(file.path()), Some("1.2.345".into()));
    for version in ["1.2", "1.2.3\r\nInjected: true", "<script>", ""] {
        write(version);
        assert_eq!(desktop_bundle_version(file.path()), None);
    }
    std::fs::write(file.path(), "x".repeat(131_073)).unwrap();
    assert_eq!(desktop_bundle_version(file.path()), None);
}

#[tokio::test]
async fn codex_edge_challenge_is_not_classified_as_account_permission() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/transcribe", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        receive_http(&mut socket).await;
        socket.write_all(b"HTTP/1.1 403 Forbidden\r\ncf-mitigated: challenge\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
    });
    let error = codex_request(
        &url,
        "fixture-token",
        "fixture-account",
        vec![0, 0],
        "audio/wav",
        "wav",
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(error, "codex_transcription_network_challenge");
    server.await.unwrap();
}
