use super::*;
use serde_json::json;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;

#[test]
fn interim_snapshots_revise_only_the_active_utterance_and_preserve_repetitions() {
    let mut transcript = Transcript::default();
    assert_eq!(transcript.update("wrong").unwrap(), Some("wrong".into()));
    assert_eq!(
        transcript.update("First sentence.").unwrap(),
        Some("First sentence.".into())
    );
    assert_eq!(transcript.commit().unwrap(), None);
    assert_eq!(
        transcript.update("First sentence.").unwrap(),
        Some("First sentence. First sentence.".into())
    );
    transcript.commit().unwrap();
    assert_eq!(
        transcript.finish().unwrap(),
        "First sentence. First sentence."
    );
    transcript.update("unfinished").unwrap();
    assert_eq!(
        transcript.finish().unwrap_err(),
        "claude_transcription_incomplete"
    );
}

#[tokio::test(start_paused = true)]
async fn stalled_pcm_writes_fail_with_a_recoverable_transport_deadline() {
    let (stream, _peer) = tokio::io::duplex(32);
    let mut socket = WebSocketStream::from_raw_socket(
        stream,
        tokio_tungstenite::tungstenite::protocol::Role::Client,
        None,
    )
    .await;
    let started = tokio::time::Instant::now();
    assert_eq!(
        send(&mut socket, Message::Binary(vec![0; 65_536]))
            .await
            .unwrap_err(),
        "claude_transcription_transport_failed"
    );
    assert_eq!(
        tokio::time::Instant::now() - started,
        Duration::from_secs(5)
    );
}

#[tokio::test]
async fn live_pcm_publishes_revisions_before_end_and_commits_the_trailing_utterance() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/voice", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let first = socket.next().await.unwrap().unwrap();
        assert!(first.to_text().unwrap().contains("KeepAlive"));
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
        let close = socket.next().await.unwrap().unwrap();
        assert!(close.to_text().unwrap().contains("CloseStream"));
        for event in [
            json!({"type":"TranscriptEndpoint"}),
            json!({"type":"TranscriptText","data":"Tail."}),
            json!({"type":"TranscriptEndpoint"}),
        ] {
            socket.send(Message::Text(event.to_string())).await.unwrap();
        }
        socket.close(None).await.unwrap();
    });
    let socket = tokio_tungstenite::connect_async(url).await.unwrap().0;
    let (audio, input) = mpsc::channel(4);
    let (ready, ready_rx) = oneshot::channel();
    let snapshots = Arc::new(Mutex::new(Vec::new()));
    let captured = snapshots.clone();
    let publish: TextCallback = Arc::new(move |text| {
        let captured = captured.clone();
        Box::pin(async move {
            captured.lock().unwrap().push(text);
        })
    });
    let recognition = tokio::spawn(recognize(
        socket,
        input,
        ready,
        "selected-login".into(),
        publish,
    ));
    assert_eq!(ready_rx.await.unwrap(), "selected-login");
    audio.send(vec![1, 0]).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while snapshots.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(snapshots.lock().unwrap()[0], "draft");
    assert!(
        !recognition.is_finished(),
        "preview must not finish an open audio input"
    );
    audio.send(vec![2, 0]).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !snapshots
            .lock()
            .unwrap()
            .iter()
            .any(|text| text == "First.")
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(audio);
    assert_eq!(recognition.await.unwrap().unwrap(), "First. Tail.");
    assert_eq!(snapshots.lock().unwrap().last().unwrap(), "First. Tail.");
    server.await.unwrap();
}

#[tokio::test]
async fn normal_socket_close_cannot_promote_an_unfinalized_preview() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/voice", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        socket.next().await.unwrap().unwrap();
        socket.next().await.unwrap().unwrap();
        socket
            .send(Message::Text(
                json!({"type":"TranscriptInterim","data":"incomplete"}).to_string(),
            ))
            .await
            .unwrap();
        while let Some(Ok(message)) = socket.next().await {
            if matches!(message, Message::Text(ref text) if text.contains("CloseStream")) {
                break;
            }
        }
        socket.close(None).await.unwrap();
    });
    let socket = tokio_tungstenite::connect_async(url).await.unwrap().0;
    let (audio, input) = mpsc::channel(4);
    let (ready, ready_rx) = oneshot::channel();
    let publish: TextCallback = Arc::new(|_| Box::pin(async {}));
    let recognition = tokio::spawn(recognize(
        socket,
        input,
        ready,
        "selected-login".into(),
        publish,
    ));
    ready_rx.await.unwrap();
    audio.send(vec![1, 0]).await.unwrap();
    drop(audio);
    assert_eq!(
        recognition.await.unwrap().unwrap_err(),
        "claude_transcription_incomplete"
    );
    server.await.unwrap();
}
