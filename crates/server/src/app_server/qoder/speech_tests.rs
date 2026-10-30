use super::*;
use tokio::net::{TcpListener, TcpStream};

async fn live_transcript_fixture(
    source: Source,
    mut events: Vec<Value>,
) -> (Result<String, String>, Vec<String>) {
    events.push(json!({"type":"speech_done"}));
    let (socket, server) = fixture_socket(events).await;
    let (input, receiver) = tokio::sync::mpsc::channel(1);
    input.send(vec![0, 1]).await.unwrap();
    drop(input);
    let (ready, connected) = tokio::sync::oneshot::channel();
    let (updates, mut snapshots) = tokio::sync::mpsc::unbounded_channel();
    let result = transcribe_feed(
        source,
        socket,
        AudioFeed::Live(receiver),
        "test-session",
        Some(LiveCallbacks {
            recognized: None,
            ready,
            generation: "original-account".into(),
            publish: Arc::new(move |value| {
                let _ = updates.send(value);
                Box::pin(std::future::ready(()))
            }),
            valid: Arc::new(|| true),
        }),
    )
    .await;
    assert_eq!(connected.await.unwrap(), "original-account");
    assert_eq!(server.await.unwrap(), vec![0, 1]);
    let mut published = Vec::new();
    while let Some(value) = snapshots.recv().await {
        published.push(value);
    }
    (result, published)
}

// Multi-sentence live dictation must retain completed utterances. Treating each
// utterance as the complete recording discarded all but the last sentence.
#[tokio::test]
async fn live_multisentence_revisions_keep_completed_utterances() {
    for source in [Source::Qoder, Source::QoderWork, Source::QwenWork] {
        let (result, snapshots) = live_transcript_fixture(
            source,
            vec![
                json!({"type":"speech_delta","message":"第一局"}),
                json!({"type":"speech_processing","message":"第一句"}),
                json!({"type":"speech_completed","message":"第一句。"}),
                json!({"type":"speech_delta","message":"第二局"}),
                json!({"type":"speech_processing","message":"第二句"}),
                json!({"type":"speech_completed","message":"第二句。"}),
                json!({"type":"speech_completed","message":"第三句。"}),
            ],
        )
        .await;
        assert_eq!(result.unwrap(), "第一句。第二句。第三句。", "{source:?}");
        assert_eq!(
            snapshots,
            [
                "第一局",
                "第一句",
                "第一句。",
                "第一句。第二局",
                "第一句。第二句",
                "第一句。第二句。",
                "第一句。第二句。第三句。"
            ],
            "{source:?}"
        );
    }
}

#[tokio::test]
async fn live_multisentence_tail_revision_preserves_completed_text() {
    let (result, snapshots) = live_transcript_fixture(
        Source::QwenWork,
        vec![
            json!({"type":"speech_completed","message":"第一句。"}),
            json!({"type":"speech_delta","message":"最后一局"}),
            json!({"type":"speech_processing","message":"最后一句。"}),
        ],
    )
    .await;
    assert_eq!(result.unwrap(), "第一句。最后一句。");
    assert_eq!(
        snapshots,
        ["第一句。", "第一句。最后一局", "第一句。最后一句。"]
    );
}

#[tokio::test]
async fn live_multisentence_repeated_utterances_are_not_deduplicated() {
    let (result, snapshots) = live_transcript_fixture(
        Source::QwenWork,
        vec![
            json!({"type":"speech_completed","message":"再说一次。"}),
            json!({"type":"speech_processing","message":"再说一次"}),
            json!({"type":"speech_completed","message":"再说一次。"}),
            json!({"type":"speech_processing","message":""}),
        ],
    )
    .await;
    assert_eq!(result.unwrap(), "再说一次。再说一次。");
    assert_eq!(snapshots.last().unwrap(), "再说一次。再说一次。");
}

#[tokio::test]
async fn live_multisentence_total_is_bounded_for_completed_and_interim_text() {
    for kind in ["speech_completed", "speech_processing"] {
        let (result, _) = live_transcript_fixture(
            Source::QwenWork,
            vec![
                json!({"type":"speech_completed","message":"x".repeat(MAX_RESULT_BYTES / 2 + 1)}),
                json!({"type":kind,"message":"y".repeat(MAX_RESULT_BYTES / 2 + 1)}),
            ],
        )
        .await;
        assert_eq!(
            result.as_ref().err().map(String::as_str),
            Some("qoder_transcription_result_too_large"),
            "{kind}"
        );
    }
}

#[test]
fn websocket_admission_errors_keep_actionable_codes_without_provider_body_text() {
    for (status, code) in [
        (401, "authentication_required"),
        (402, "billing_required"),
        (403, "permission_denied"),
        (429, "rate_limited"),
    ] {
        let response = http::Response::builder()
            .status(status)
            .body(Some(b"private provider response".to_vec()))
            .unwrap();
        assert_eq!(
            connection_error(
                Source::QwenWork,
                tokio_tungstenite::tungstenite::Error::Http(response)
            ),
            format!("qoder_transcription_{code}")
        );
    }
}

#[tokio::test]
async fn retained_audio_rejects_a_replaced_credential_before_decoding_or_connecting() {
    let root = tempfile::tempdir().unwrap();
    super::super::set_config_root(root.path());
    super::super::auth::adopt_credential(
        json!({"uid":"fixture-user","access_token":"first","expire_time":4102444800i64}),
    )
    .unwrap();
    let generation = credential_identity(&Source::Qoder.credential().unwrap());
    super::super::auth::adopt_credential(
        json!({"uid":"fixture-user","access_token":"replacement","expire_time":4102444800i64}),
    )
    .unwrap();
    let params = serde_json::from_value(
        json!({"audioBase64":"invalid audio","expectedCredentialGeneration":generation}),
    )
    .unwrap();
    let error = match transcribe_audio(Source::Qoder, params, AbortSignal::new()).await {
        Ok(_) => panic!("replacement credential must be rejected"),
        Err(error) => error,
    };
    assert_eq!(error, "qoder_transcription_credential_changed");
}

// Work item 20261022: live input must publish interim revisions before input ends.
// Buffered-only upload previously hid all recognition events until completion.
#[tokio::test]
async fn live_feed_publishes_revisions_before_stop_and_preserves_the_final_tail() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        assert_eq!(
            socket.next().await.unwrap().unwrap(),
            Message::Binary(vec![1, 0])
        );
        for (kind, message) in [("speech_delta", "fir"), ("speech_processing", "first")] {
            socket
                .send(Message::Text(
                    json!({"type":kind,"asr_session_id":"live-test","message":message}).to_string(),
                ))
                .await
                .unwrap();
        }
        socket
            .send(Message::Text(
                json!({"type":"speech_completed","message":"First "}).to_string(),
            ))
            .await
            .unwrap();
        assert_eq!(
            socket.next().await.unwrap().unwrap(),
            Message::Binary(vec![2, 0])
        );
        let Message::Text(end) = socket.next().await.unwrap().unwrap() else {
            panic!("missing input end")
        };
        assert_eq!(
            serde_json::from_str::<Value>(&end).unwrap()["type"],
            "voice_completed"
        );
        for event in [
            json!({"type":"speech_completed","message":"last."}),
            json!({"type":"speech_done"}),
        ] {
            socket.send(Message::Text(event.to_string())).await.unwrap();
        }
    });
    let (socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}"))
        .await
        .unwrap();
    let (input, receiver) = tokio::sync::mpsc::channel(4);
    let (ready, connected) = tokio::sync::oneshot::channel();
    let (updates, mut snapshots) = tokio::sync::mpsc::unbounded_channel();
    let client = tokio::spawn(transcribe_feed(
        Source::QwenWork,
        socket,
        AudioFeed::Live(receiver),
        "live-test",
        Some(LiveCallbacks {
            recognized: None,
            ready,
            generation: "original-account".into(),
            publish: Arc::new(move |value| {
                let _ = updates.send(value);
                Box::pin(std::future::ready(()))
            }),
            valid: Arc::new(|| true),
        }),
    ));
    assert_eq!(connected.await.unwrap(), "original-account");
    input.send(vec![1, 0]).await.unwrap();
    for expected in ["fir", "first", "First "] {
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), snapshots.recv())
                .await
                .unwrap()
                .unwrap(),
            expected
        );
    }
    assert!(!client.is_finished());
    input.send(vec![2, 0]).await.unwrap();
    drop(input);
    assert_eq!(client.await.unwrap().unwrap(), "First last.");
    assert_eq!(snapshots.recv().await.unwrap(), "First last.");
    server.await.unwrap();
}

#[tokio::test]
async fn live_feed_rejects_an_account_change_before_sending_the_next_frame() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (first_frame, first_seen) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        assert_eq!(
            socket.next().await.unwrap().unwrap(),
            Message::Binary(vec![1, 0])
        );
        let _ = first_frame.send(());
        assert!(!matches!(socket.next().await, Some(Ok(Message::Binary(_)))));
    });
    let (socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}"))
        .await
        .unwrap();
    let (input, receiver) = tokio::sync::mpsc::channel(4);
    let (ready, connected) = tokio::sync::oneshot::channel();
    let valid = Arc::new(AtomicBool::new(true));
    let check = valid.clone();
    let client = tokio::spawn(async move {
        transcribe_feed(
            Source::Qoder,
            socket,
            AudioFeed::Live(receiver),
            "live-test",
            Some(LiveCallbacks {
                recognized: None,
                ready,
                generation: "original".into(),
                publish: Arc::new(|_| Box::pin(std::future::ready(()))),
                valid: Arc::new(move || check.load(Ordering::Acquire)),
            }),
        )
        .await
    });
    connected.await.unwrap();
    input.send(vec![1, 0]).await.unwrap();
    first_seen.await.unwrap();
    valid.store(false, Ordering::Release);
    input.send(vec![2, 0]).await.unwrap();
    assert_eq!(
        client.await.unwrap().unwrap_err(),
        "qoder_transcription_credential_changed"
    );
    drop(input);
    server.await.unwrap();
}

fn wav(pcm: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"RIFF");
    bytes.extend_from_slice(&(36 + pcm.len() as u32).to_le_bytes());
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

#[test]
fn wav_requires_the_declared_pcm_format_and_complete_chunks() {
    let pcm = [0, 1, 2, 3];
    let valid = wav(&pcm);
    assert_eq!(decode_audio(&STANDARD.encode(&valid), None).unwrap(), pcm);
    assert_eq!(
        decode_audio(&STANDARD.encode(pcm), Some("audio/pcm")).unwrap(),
        pcm
    );
    for (offset, value) in [(20, 3), (22, 2), (24, 1), (28, 1), (32, 4), (34, 8)] {
        let mut invalid = valid.clone();
        invalid[offset] = value;
        assert!(wav_pcm(&invalid).is_err(), "format offset {offset}");
    }
    assert!(wav_pcm(&valid[..valid.len() - 1]).is_err());
    let mut duplicate = valid.clone();
    duplicate.extend_from_slice(b"data\x02\x00\x00\x00\x00\x00");
    let size = (duplicate.len() as u32 - 8).to_le_bytes();
    duplicate[4..8].copy_from_slice(&size);
    assert!(wav_pcm(&duplicate).is_err());
}

#[test]
fn audio_limits_apply_before_allocation_and_to_decoded_duration() {
    assert_eq!(
        decode_audio(&"!".repeat(MAX_AUDIO_BYTES.div_ceil(3) * 4 + 1), None).unwrap_err(),
        "qoder_transcription_audio_too_large"
    );
    assert!(decode_audio("not base64", None).is_err());
    assert!(decode_audio(&STANDARD.encode([0, 1]), Some("audio/webm")).is_err());
    assert!(decode_audio(&STANDARD.encode([0]), Some("audio/pcm")).is_err());
    assert!(decode_audio("", Some("audio/pcm")).is_err());
    assert_eq!(
        decode_audio(
            &STANDARD.encode(vec![0; MAX_PCM_BYTES + 2]),
            Some("audio/pcm")
        )
        .unwrap_err(),
        "qoder_transcription_audio_too_large"
    );
}

#[test]
fn audio_origin_accepts_only_trusted_https_origins() {
    assert!(valid_origin("https://api1.qoder.sh"));
    for origin in [
        "http://api1.qoder.sh",
        "https://api1.qoder.sh/extra",
        "https://api1.qoder.sh?x=1",
        "https://api1.qoder.sh.evil.test",
        "https://user@api1.qoder.sh",
        "https://127.0.0.1",
        "https://api1.qoder.sh:8443",
    ] {
        assert!(!valid_origin(origin), "{origin}");
    }
}

#[test]
fn audio_auth_is_bodyless_and_never_exposes_plaintext_credentials() {
    let profile = json!({"uid":"test-user","security_oauth_token":"fixture-secret-token","expire_time":4102444800i64});
    let request = audio_request(
        Source::Qoder,
        "https://api1.qoder.sh",
        &profile,
        "test-session",
    )
    .unwrap();
    assert_eq!(
        request.uri().to_string(),
        "wss://api1.qoder.sh/api/v2/service/ws/asr"
    );
    assert_eq!(request.headers()["samplerate"], "16000");
    assert_eq!(request.headers()["channels"], "1");
    assert_eq!(request.headers()["x-asr-session-id"], "test-session");
    assert!(!format!("{request:?}").contains("fixture-secret-token"));
    let business: Value =
        serde_json::from_str(request.headers()["x-business"].to_str().unwrap()).unwrap();
    assert_eq!(business["type"], "asr_chat");
    assert_eq!(business["product"], "ide");
    assert!(usable_profile(&profile).is_ok());
    assert_eq!(
        usable_profile(
            &json!({"uid":"test-user","access_token":"fixture-secret-token","expire_time":1})
        )
        .unwrap_err(),
        "qoder_transcription_login_expired"
    );
}

async fn fixture_socket(
    events: Vec<Value>,
) -> (
    tokio_tungstenite::WebSocketStream<TcpStream>,
    tokio::task::JoinHandle<Vec<u8>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let mut pcm = Vec::new();
        while let Some(message) = socket.next().await {
            match message.unwrap() {
                Message::Binary(frame) => {
                    assert!(frame.len() <= FRAME_BYTES);
                    pcm.extend(frame);
                }
                Message::Text(text) => {
                    assert_eq!(
                        serde_json::from_str::<Value>(&text).unwrap(),
                        json!({"type":"voice_completed","message":"close by user"})
                    );
                    for event in events {
                        if socket.send(Message::Text(event.to_string())).await.is_err() {
                            break;
                        }
                    }
                    let _ = socket.close(None).await;
                    break;
                }
                _ => {}
            }
        }
        pcm
    });
    let stream = TcpStream::connect(address).await.unwrap();
    let (client, _) = tokio_tungstenite::client_async(format!("ws://{address}"), stream)
        .await
        .unwrap();
    (client, server)
}

#[tokio::test]
async fn live_feed_ignores_unknown_messages_and_retains_only_transcript_events() {
    for transcript_type in [None, Some("speech_completed"), Some("speech_delta")] {
        let mut events = vec![json!({"type":"session_ready","message":"Control status"})];
        if let Some(kind) = transcript_type {
            events.push(json!({"type":kind,"message":"Spoken text."}));
        }
        events.extend([
            json!({"type":"unrecognized_metadata","message":"Private metadata"}),
            json!({"message":"Missing event type"}),
            json!({"type":"speech_done"}),
        ]);
        let (socket, server) = fixture_socket(events).await;
        let (input, receiver) = tokio::sync::mpsc::channel(1);
        input.send(vec![0, 1]).await.unwrap();
        drop(input);
        let (ready, connected) = tokio::sync::oneshot::channel();
        let (updates, mut snapshots) = tokio::sync::mpsc::unbounded_channel();
        let result = transcribe_feed(
            Source::QwenWork,
            socket,
            AudioFeed::Live(receiver),
            "test-session",
            Some(LiveCallbacks {
                recognized: None,
                ready,
                generation: "original-account".into(),
                publish: Arc::new(move |value| {
                    let _ = updates.send(value);
                    Box::pin(std::future::ready(()))
                }),
                valid: Arc::new(|| true),
            }),
        )
        .await;
        assert_eq!(connected.await.unwrap(), "original-account");
        if transcript_type.is_some() {
            assert_eq!(result.unwrap(), "Spoken text.");
            assert_eq!(snapshots.recv().await.unwrap(), "Spoken text.");
        } else {
            assert_eq!(result.unwrap_err(), "qoder_transcription_no_speech");
        }
        assert!(snapshots.recv().await.is_none());
        assert_eq!(server.await.unwrap(), vec![0, 1]);
    }
}

#[tokio::test]
async fn live_feed_rechecks_account_identity_after_the_last_transcript_event() {
    let (socket, server) = fixture_socket(vec![
        json!({"type":"speech_completed","message":"Spoken text."}),
        json!({"type":"speech_done"}),
    ])
    .await;
    let (input, receiver) = tokio::sync::mpsc::channel(1);
    input.send(vec![0, 1]).await.unwrap();
    drop(input);
    let (ready, connected) = tokio::sync::oneshot::channel();
    let current = Arc::new(AtomicBool::new(true));
    let check = current.clone();
    let result = transcribe_feed(
        Source::QoderWork,
        socket,
        AudioFeed::Live(receiver),
        "test-session",
        Some(LiveCallbacks {
            recognized: None,
            ready,
            generation: "original-account".into(),
            publish: Arc::new(move |_| {
                current.store(false, Ordering::Release);
                Box::pin(std::future::ready(()))
            }),
            valid: Arc::new(move || check.load(Ordering::Acquire)),
        }),
    )
    .await;
    assert_eq!(connected.await.unwrap(), "original-account");
    assert_eq!(
        result.unwrap_err(),
        "qoder_transcription_credential_changed"
    );
    assert_eq!(server.await.unwrap(), vec![0, 1]);
}

#[tokio::test]
async fn audio_frames_end_once_and_only_final_matching_segments_are_published() {
    let (socket, server) = fixture_socket(vec![
        json!({"type":"speech_completed","asr_session_id":"other-session","message":"stale"}),
        json!({"type":"speech_processing","message":"interim"}),
        json!({"type":"speech_completed","message":"First "}),
        json!({"type":"speech_cost","message":"metadata"}),
        json!({"type":"speech_completed","message":"second."}),
        json!({"type":"speech_done"}),
    ])
    .await;
    let pcm = vec![5; FRAME_BYTES * 2 + 2];
    assert_eq!(
        transcribe_socket(Source::Qoder, socket, &pcm, "test-session")
            .await
            .unwrap(),
        "First second."
    );
    assert_eq!(server.await.unwrap(), pcm);
}

#[tokio::test]
async fn partial_errors_empty_output_and_unbounded_output_fail_without_echoing_service_text() {
    for (events, expected) in [
        (
            vec![json!({"type":"speech_completed","message":"partial"})],
            "qoder_transcription_incomplete",
        ),
        (
            vec![json!({"type":"speech_done"})],
            "qoder_transcription_no_speech",
        ),
        (
            vec![json!({"type":"speech_err","message":"sensitive-service-content"})],
            "qoder_transcription_refused",
        ),
        (
            vec![json!({"type":"speech_completed","message":"x".repeat(MAX_RESULT_BYTES+1)})],
            "qoder_transcription_result_too_large",
        ),
    ] {
        let (socket, server) = fixture_socket(events).await;
        let error = transcribe_socket(Source::Qoder, socket, &[0, 1], "test-session")
            .await
            .unwrap_err();
        assert_eq!(error, expected);
        server.await.unwrap();
    }
}

#[tokio::test]
async fn cancellation_drops_in_flight_work_and_timeout_is_bounded() {
    let signal = AbortSignal::new();
    let cancelling = signal.clone();
    let task = tokio::spawn(async move {
        cancellable(
            &signal,
            Duration::from_secs(5),
            std::future::pending::<Result<(), String>>(),
        )
        .await
    });
    cancelling.cancel();
    assert_eq!(
        task.await.unwrap().unwrap_err(),
        "qoder_transcription_cancelled"
    );
    assert_eq!(
        cancellable(
            &AbortSignal::new(),
            Duration::from_millis(1),
            std::future::pending::<Result<(), String>>()
        )
        .await
        .unwrap_err(),
        "qoder_transcription_timed_out"
    );
}

#[tokio::test]
async fn prepared_source_uses_its_gateway_identity_and_final_segments() {
    assert!(source_origin(
        Source::QwenWork,
        "https://gateway.qwenwork.cn"
    ));
    assert!(!source_origin(Source::QwenWork, "https://api1.qoder.sh"));
    let profile = json!({"uid":"quota-user", "security_oauth_token":"fixture-host-token"});
    let request = audio_request(
        Source::QwenWork,
        "https://gateway.qwenwork.cn",
        &profile,
        "test-session",
    )
    .unwrap();
    assert!(!request.headers().contains_key("x-machine-id"));
    let business: Value =
        serde_json::from_str(request.headers()["x-business"].to_str().unwrap()).unwrap();
    assert_eq!(business["product"], "qoder_work");
    let (socket, server) = fixture_socket(vec![
        json!({"type":"speech_completed","message":"First "}),
        json!({"type":"speech_completed","message":"second."}),
        json!({"type":"speech_done"}),
    ])
    .await;
    assert_eq!(
        transcribe_socket(Source::QwenWork, socket, &[0, 1], "test-session")
            .await
            .unwrap(),
        "First second."
    );
    server.await.unwrap();
}

#[tokio::test]
async fn qwen_audio_follows_the_pcm_clock_and_retains_every_final_segment() {
    let (socket, server) = fixture_socket(vec![
        json!({"type":"speech_completed","message":"First "}),
        json!({"type":"speech_completed","message":"second."}),
        json!({"type":"speech_done"}),
    ])
    .await;
    let pcm = vec![0; FRAME_BYTES * 3];
    let started = tokio::time::Instant::now();
    assert_eq!(
        transcribe_socket(Source::QwenWork, socket, &pcm, "test-session")
            .await
            .unwrap(),
        "First second."
    );
    assert!(started.elapsed() >= pcm_clock(pcm.len()));
    assert_eq!(server.await.unwrap(), pcm);
}

#[tokio::test]
async fn qwen_final_segment_total_remains_bounded() {
    let (socket, server) = fixture_socket(vec![
        json!({"type":"speech_completed","message":"x".repeat(MAX_RESULT_BYTES / 2 + 1)}),
        json!({"type":"speech_completed","message":"y".repeat(MAX_RESULT_BYTES / 2 + 1)}),
        json!({"type":"speech_done"}),
    ])
    .await;
    assert_eq!(
        transcribe_socket(Source::QwenWork, socket, &[0, 1], "test-session")
            .await
            .unwrap_err(),
        "qoder_transcription_result_too_large"
    );
    server.await.unwrap();
}

#[test]
fn shared_audio_failures_keep_the_selected_source_identity() {
    for source in [Source::Qoder, Source::QoderWork, Source::QwenWork] {
        assert_eq!(
            source.source_error("qoder_transcription_login_expired"),
            format!("{}_transcription_login_expired", source.name())
        );
        assert_eq!(
            source.source_error("qoder_transcription_credential_changed"),
            format!("{}_transcription_credential_changed", source.name())
        );
        assert_eq!(
            source.source_error("qoder_transcription_cancelled"),
            format!("{}_transcription_cancelled", source.name())
        );
        assert_eq!(
            source.source_error("private transport details"),
            format!("{}_transcription_failed", source.name())
        );
    }
}
