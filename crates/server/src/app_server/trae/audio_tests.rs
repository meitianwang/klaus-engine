use super::*;

fn response(sequence: i32, final_result: bool, value: Value) -> Vec<u8> {
    let payload = value.to_string();
    let mut bytes = vec![0x11, 0x91 | if final_result { 2 } else { 0 }, 0x10, 0];
    bytes.extend_from_slice(&sequence.to_be_bytes());
    bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    bytes.extend_from_slice(payload.as_bytes());
    bytes
}

#[test]
fn transcript_requires_native_final_flag_and_preserves_full_text_and_sequence() {
    let value = json!({"result":{"text":"A complete sentence.","utterances":[{"definite":true}]}});
    let interim = parse_event(&response(32, false, value.clone())).unwrap();
    assert!(!interim.final_result);
    let complete = parse_event(&response(33, true, value)).unwrap();
    assert!(complete.final_result && complete.definite);
    assert_eq!(complete.sequence, Some(33));
    assert_eq!(complete.text, "A complete sentence.");
}

#[test]
fn transcript_rejects_truncation_oversize_and_native_errors() {
    let mut bytes = response(1, false, json!({"result":{"text":""}}));
    bytes.pop();
    assert!(parse_event(&bytes).is_err());
    let error = [0x11, 0xf0, 0x10, 0, 0, 0, 0x0f, 0xa1];
    assert_eq!(
        parse_event(&error).err().as_deref(),
        Some("trae_transcription_refused (4001)")
    );
    let huge = response(1, true, json!({"result":{"text":"x".repeat(MAX_TEXT + 1)}}));
    assert!(parse_event(&huge).is_err());
}

#[test]
fn recognition_frames_carry_exact_payload_length_and_end_of_audio_marker() {
    assert_eq!(
        frame(2, false, &[1, 2]),
        [0x11, 0x20, 0x10, 0, 0, 0, 0, 2, 1, 2]
    );
    assert_eq!(frame(2, true, &[]), [0x11, 0x22, 0x10, 0, 0, 0, 0, 0]);
}

#[test]
fn audio_admission_accepts_only_bounded_mono_pcm_and_matching_wav() {
    let pcm = STANDARD.encode([1, 2, 3, 4]);
    assert_eq!(decode_audio(&pcm, Some("audio/pcm")).unwrap(), [1, 2, 3, 4]);
    assert!(decode_audio(&pcm, Some("audio/mp3")).is_err());
    assert!(decode_audio(&STANDARD.encode([1]), Some("audio/pcm")).is_err());
    let mut wav = b"RIFF".to_vec();
    wav.extend_from_slice(&40u32.to_le_bytes());
    wav.extend_from_slice(b"WAVEfmt ");
    wav.extend_from_slice(&16u32.to_le_bytes());
    for bytes in [1u16.to_le_bytes(), 1u16.to_le_bytes()] {
        wav.extend_from_slice(&bytes);
    }
    wav.extend_from_slice(&16000u32.to_le_bytes());
    wav.extend_from_slice(&32000u32.to_le_bytes());
    wav.extend_from_slice(&2u16.to_le_bytes());
    wav.extend_from_slice(&16u16.to_le_bytes());
    wav.extend_from_slice(b"data");
    wav.extend_from_slice(&4u32.to_le_bytes());
    wav.extend_from_slice(&[1, 2, 3, 4]);
    assert_eq!(
        decode_audio(&STANDARD.encode(&wav), Some("audio/wav")).unwrap(),
        [1, 2, 3, 4]
    );
    wav[24..28].copy_from_slice(&48000u32.to_le_bytes());
    assert!(decode_audio(&STANDARD.encode(&wav), Some("audio/wav")).is_err());
}

#[tokio::test]
async fn live_recognition_waits_for_final_instead_of_returning_interim_text() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let first = socket.next().await.unwrap().unwrap();
        assert!(matches!(first, Message::Binary(ref bytes) if bytes[1] == 0x10));
        socket
            .send(Message::Binary(response(
                1,
                false,
                json!({"result":{"text":""}}),
            )))
            .await
            .unwrap();
        loop {
            let message = socket.next().await.unwrap().unwrap();
            if let Message::Binary(bytes) = message {
                if bytes[1] & 2 != 0 {
                    break;
                }
            }
        }
        socket
            .send(Message::Binary(response(
                2,
                false,
                json!({"result":{"text":"Partial"}}),
            )))
            .await
            .unwrap();
        socket
            .send(Message::Binary(response(
                3,
                true,
                json!({"result":{"text":"Complete recording.","utterances":[{"definite":true}]}}),
            )))
            .await
            .unwrap();
    });
    let (socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}"))
        .await
        .unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(2);
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    tx.send(vec![0; 3200]).await.unwrap();
    drop(tx);
    let transcript = recognize(
        socket,
        Feed::Live(rx),
        Some((ready_tx, "recording-generation".into())),
        None,
    )
    .await
    .unwrap();
    assert_eq!(ready_rx.await.unwrap(), "recording-generation");
    assert_eq!(transcript.text, "Complete recording.");
    assert_eq!(transcript.sequence, Some(3));
    assert!(transcript.definite);
    server.await.unwrap();
}

#[tokio::test]
async fn live_recognition_awaits_each_partial_before_reading_the_final_result() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        socket.next().await.unwrap().unwrap();
        socket
            .send(Message::Binary(response(
                1,
                false,
                json!({"result":{"text":""}}),
            )))
            .await
            .unwrap();
        while let Some(Ok(message)) = socket.next().await {
            if matches!(message, Message::Binary(ref bytes) if bytes[1] & 2 != 0) {
                break;
            }
        }
        socket
            .send(Message::Binary(response(
                2,
                false,
                json!({"result":{"text":"Partial"}}),
            )))
            .await
            .unwrap();
        socket
            .send(Message::Binary(response(
                3,
                true,
                json!({"result":{"text":"Complete recording.","utterances":[{"definite":true}]}}),
            )))
            .await
            .unwrap();
    });
    let (socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}"))
        .await
        .unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
    let publish: crate::app_server::qoder::speech::TextCallback = {
        let entered = entered.clone();
        let release = release.clone();
        let observed = observed.clone();
        Arc::new(move |text| {
            let entered = entered.clone();
            let release = release.clone();
            let observed = observed.clone();
            Box::pin(async move {
                entered.notify_one();
                release.acquire().await.unwrap().forget();
                observed.lock().unwrap().push(text);
            })
        })
    };
    let task = tokio::spawn(async move {
        recognize(socket, Feed::Buffered(&[0, 0]), None, Some(publish)).await
    });
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    assert!(!task.is_finished());
    assert!(observed.lock().unwrap().is_empty());
    release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    assert!(!task.is_finished());
    assert_eq!(*observed.lock().unwrap(), vec!["Partial"]);
    release.add_permits(1);
    let transcript = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        *observed.lock().unwrap(),
        vec!["Partial", "Complete recording."]
    );
    assert_eq!(transcript.text, "Complete recording.");
    assert_eq!(transcript.sequence, Some(3));
    assert!(transcript.definite);
    server.await.unwrap();
}

#[tokio::test]
async fn disconnect_without_final_is_not_a_success() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        socket.next().await.unwrap().unwrap();
        socket
            .send(Message::Binary(response(
                1,
                false,
                json!({"result":{"text":""}}),
            )))
            .await
            .unwrap();
        socket.next().await.unwrap().unwrap();
        socket
            .send(Message::Binary(response(
                2,
                false,
                json!({"result":{"text":"Incomplete"}}),
            )))
            .await
            .unwrap();
        socket.close(None).await.unwrap();
    });
    let (socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}"))
        .await
        .unwrap();
    let result = recognize(socket, Feed::Buffered(&[0, 0]), None, None).await;
    assert_eq!(
        result.err().as_deref(),
        Some("trae_transcription_incomplete")
    );
    server.await.unwrap();
}
