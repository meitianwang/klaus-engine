//! Native dictation preserves live snapshots, account identity and complete final results.
use super::*;
use serde_json::json;
use std::sync::Arc;
use tokio_tungstenite::tungstenite::protocol::Role;

struct Fixture {
    _root: tempfile::TempDir,
    credential_path: std::path::PathBuf,
    session: Arc<NativeSession>,
}
impl Fixture {
    async fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        super::super::set_config_root(root.path());
        let credential_path = root.path().join("workbuddy-subscription-oauth.json");
        std::fs::write(&credential_path, json!({"deviceId":"fixture-device","userInfo":{
            "access_token":"fixture-token", "uid":"fixture-user", "enterprise_id":"fixture-tenant",
            "domain":"https://example.com/prefix", "login_method":"adopted"
        }}).to_string()).unwrap();
        let session = Arc::new(NativeSession::start().await.unwrap());
        Self {
            _root: root,
            credential_path,
            session,
        }
    }
}

type Socket = WebSocketStream<tokio::io::DuplexStream>;
async fn sockets() -> (Socket, Socket) {
    let (client, server) = tokio::io::duplex(128 * 1024);
    (
        WebSocketStream::from_raw_socket(client, Role::Client, None).await,
        WebSocketStream::from_raw_socket(server, Role::Server, None).await,
    )
}
async fn event(socket: &mut Socket, value: Value) {
    socket.send(Message::Text(value.to_string())).await.unwrap();
}
async fn frame(socket: &mut Socket) -> Message {
    tokio::time::timeout(Duration::from_secs(2), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}
fn callbacks() -> (TextCallback, mpsc::UnboundedReceiver<String>) {
    let (sender, receiver) = mpsc::unbounded_channel();
    let callback: TextCallback = Arc::new(move |text| {
        let sender = sender.clone();
        Box::pin(async move {
            let _ = sender.send(text);
        })
    });
    (callback, receiver)
}

#[tokio::test]
async fn request_preserves_subscription_headers_endpoint_prefix_and_opaque_generation() {
    let _lock = super::super::test_env_lock().lock().await;
    let fixture = Fixture::new().await;
    let request = stream_request(&fixture.session).unwrap();
    assert_eq!(
        request.uri().to_string(),
        "wss://example.com/prefix/clientcap/v2/asr/stream?source=desktop&interim_results=1"
    );
    assert_eq!(request.headers()["authorization"], "Bearer fixture-token");
    assert_eq!(request.headers()["x-user-id"], "fixture-user");
    assert_eq!(request.headers()["x-enterprise-id"], "fixture-tenant");
    assert_eq!(request.headers()["x-tenant-id"], "fixture-tenant");
    assert_eq!(request.headers()["x-domain"], "https://example.com/prefix");
    let generation = fixture.session.generation();
    assert_eq!(generation.len(), 64);
    assert!(!generation.contains("fixture"));
    assert_eq!(
        NativeSession::start_expected(Some(&generation))
            .await
            .unwrap()
            .generation(),
        generation
    );
}

#[tokio::test]
async fn continuous_pcm_publishes_revisable_previews_before_end_and_waits_for_native_final() {
    let _lock = super::super::test_env_lock().lock().await;
    let fixture = Fixture::new().await;
    let (client, mut server) = sockets().await;
    let (audio, input) = mpsc::channel(4);
    let (ready_tx, ready) = oneshot::channel();
    let (publish, mut previews) = callbacks();
    let session = fixture.session.clone();
    let worker =
        tokio::spawn(async move { recognize(client, input, ready_tx, publish, &session).await });
    event(&mut server, json!({"code":0,"voice_id":"voice-one"})).await;
    assert_eq!(ready.await.unwrap(), fixture.session.generation());
    audio.send(vec![1, 0, 2, 0]).await.unwrap();
    assert_eq!(frame(&mut server).await, Message::Binary(vec![1, 0, 2, 0]));
    event(
        &mut server,
        json!({"code":0,"voice_id":"voice-one","text":"provisional","stable":false,"final":false}),
    )
    .await;
    assert_eq!(previews.recv().await.unwrap(), "provisional");
    assert!(!worker.is_finished());
    audio.send(vec![3, 0]).await.unwrap();
    assert_eq!(frame(&mut server).await, Message::Binary(vec![3, 0]));
    event(&mut server, json!({"code":0,"voice_id":"voice-one","text":"revised preview","stable":true,"final":false})).await;
    assert_eq!(previews.recv().await.unwrap(), "revised preview");
    drop(audio);
    assert_eq!(frame(&mut server).await, Message::Binary(Vec::new()));
    assert!(!worker.is_finished());
    event(
        &mut server,
        json!({"code":0,"voice_id":"voice-one","text":"Complete recording text.","final":true}),
    )
    .await;
    assert_eq!(previews.recv().await.unwrap(), "Complete recording text.");
    assert!(matches!(frame(&mut server).await, Message::Close(_)));
    assert_eq!(worker.await.unwrap().unwrap(), "Complete recording text.");
}

#[tokio::test]
async fn disconnect_or_unsolicited_final_never_commits_an_incomplete_preview() {
    let _lock = super::super::test_env_lock().lock().await;
    for premature_final in [false, true] {
        let fixture = Fixture::new().await;
        let (client, mut server) = sockets().await;
        let (audio, input) = mpsc::channel(4);
        let (ready_tx, ready) = oneshot::channel();
        let (publish, mut previews) = callbacks();
        let session = fixture.session.clone();
        let worker =
            tokio::spawn(
                async move { recognize(client, input, ready_tx, publish, &session).await },
            );
        event(&mut server, json!({"code":0,"voice_id":"voice-one"})).await;
        ready.await.unwrap();
        audio.send(vec![1, 0]).await.unwrap();
        frame(&mut server).await;
        event(
            &mut server,
            json!({"code":0,"text":"unfinished","final":false}),
        )
        .await;
        assert_eq!(previews.recv().await.unwrap(), "unfinished");
        if premature_final {
            event(&mut server, json!({"code":0,"text":"early","final":true})).await;
        } else {
            server.close(None).await.unwrap();
        }
        assert_eq!(
            worker.await.unwrap().unwrap_err(),
            "workbuddy_transcription_incomplete"
        );
        assert!(previews.try_recv().is_err());
    }
}

#[tokio::test]
async fn cancellation_and_account_replacement_drop_the_socket_without_publication() {
    let _lock = super::super::test_env_lock().lock().await;
    for cancelled in [true, false] {
        let fixture = Fixture::new().await;
        let (client, mut server) = sockets().await;
        let (_audio, input) = mpsc::channel(4);
        let (ready_tx, ready) = oneshot::channel();
        let (publish, mut previews) = callbacks();
        let session = fixture.session.clone();
        let signal = AbortSignal::new();
        let worker_signal = signal.clone();
        let worker = tokio::spawn(async move {
            native::bounded(
                &worker_signal,
                360,
                recognize(client, input, ready_tx, publish, &session),
            )
            .await
        });
        event(&mut server, json!({"code":0,"voice_id":"voice-one"})).await;
        ready.await.unwrap();
        if cancelled {
            signal.cancel();
        } else {
            std::fs::remove_file(&fixture.credential_path).unwrap();
        }
        let result = tokio::time::timeout(Duration::from_secs(1), worker)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            result.unwrap_err(),
            if cancelled {
                "workbuddy_operation_cancelled"
            } else {
                "workbuddy_credential_changed"
            }
        );
        assert!(previews.try_recv().is_err());
    }
}

#[tokio::test]
async fn retained_audio_is_refused_before_network_access_after_the_login_changes() {
    let _lock = super::super::test_env_lock().lock().await;
    let fixture = Fixture::new().await;
    let generation = fixture.session.generation();
    std::fs::write(
        &fixture.credential_path,
        json!({"deviceId":"other-device","userInfo":{
            "access_token":"new-token", "uid":"other-user", "domain":"https://example.com/prefix"
        }})
        .to_string(),
    )
    .unwrap();
    let result = super::super::voice::transcribe_audio(
        crate::app_server::wire::WorkBuddyTranscribeParams {
            audio_base64: "UklGRiYAAABXQVZFZm10IBAAAAABAAEAgD4AAAB9AAACABAAZGF0YQIAAAAAAA==".into(),
            mime_type: Some("audio/wav".into()),
            language: None,
            operation_id: Some("retained-audio".into()),
            expected_credential_generation: Some(generation),
        },
        AbortSignal::new(),
    )
    .await;
    assert_eq!(result.err().unwrap(), "workbuddy_credential_changed");
}

#[tokio::test(start_paused = true)]
async fn socket_backpressure_has_a_short_recoverable_timeout() {
    let (stream, _peer) = tokio::io::duplex(32);
    let mut socket = WebSocketStream::from_raw_socket(stream, Role::Client, None).await;
    let began = tokio::time::Instant::now();
    assert_eq!(
        send_frame(&mut socket, Message::Binary(vec![0; 65_536]))
            .await
            .unwrap_err(),
        "workbuddy_transcription_transport_failed"
    );
    assert_eq!(tokio::time::Instant::now() - began, WRITE_BUDGET);
}

#[test]
fn event_bounds_and_native_capability_gate_remain_explicit() {
    assert!(parse_event("[]").is_err());
    assert!(parse_event(&json!({"code":0,"text":"x".repeat(MAX_TEXT+1)}).to_string()).is_err());
    assert!(
        parse_event(&json!({"code":11140,"message":"private upstream detail"}).to_string())
            .unwrap_err()
            .contains("service_rejected")
    );
    assert_eq!(
        super::super::media::project_capabilities(&json!({}))["transcription"]["streaming"],
        true
    );
    assert_eq!(
        super::super::media::project_capabilities(
            &json!({"productFeatures":{"DisableVoiceInput":true}})
        )["transcription"]["streaming"],
        false
    );
}
