use super::*;
use crate::app_server::wire::{
    DictationRefinementMessage, DictationRefinementRole, DictationStartParams,
};
use tokio::net::{TcpListener, TcpStream};

fn config() -> DictationRefinementConfig {
    DictationRefinementConfig {
        messages: vec![DictationRefinementMessage {
            role: DictationRefinementRole::User,
            content: "Preferred spelling: Klaus.".into(),
        }],
    }
}

async fn recording(
    events: impl FnOnce(&str) -> Vec<Value> + Send + 'static,
    valid: Arc<AtomicBool>,
    invalidate_at_boundary: bool,
) -> Result<SpeechResult, String> {
    recording_with_raw(events, valid, invalidate_at_boundary, None).await
}

async fn recording_with_raw(
    events: impl FnOnce(&str) -> Vec<Value> + Send + 'static,
    valid: Arc<AtomicBool>,
    invalidate_at_boundary: bool,
    recognized: Option<crate::app_server::qoder::speech::TextCallback>,
) -> Result<SpeechResult, String> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (boundary_tx, boundary) = tokio::sync::oneshot::channel();
    let (resume, resume_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let first = socket.next().await.unwrap().unwrap();
        let Message::Text(first) = first else {
            panic!("The configuration must precede the first audio frame");
        };
        let first: Value = serde_json::from_str(&first).unwrap();
        assert_eq!(first["type"], "asr_polish_config");
        assert_eq!(first["enabled"], true);
        assert_eq!(first["client_type"], 6);
        assert_eq!(first["source"], "chat_input");
        assert_eq!(
            first["messages"],
            json!([{"role":"user", "content":"Preferred spelling: Klaus."}])
        );
        let request_id = first["request_id"].as_str().unwrap().to_owned();
        assert!(uuid::Uuid::parse_str(&request_id).is_ok());
        let mut audio = Vec::new();
        while let Some(message) = socket.next().await {
            match message.unwrap() {
                Message::Binary(bytes) => audio.extend(bytes),
                Message::Text(text) => {
                    assert_eq!(
                        serde_json::from_str::<Value>(&text).unwrap()["type"],
                        "voice_completed"
                    );
                    break;
                }
                _ => {}
            }
        }
        assert_eq!(audio, [1, 0, 2, 0]);
        let mut boundary_tx = Some(boundary_tx);
        let mut resume_rx = Some(resume_rx);
        for event in events(&request_id) {
            if event["type"] == "test_boundary" {
                let _ = boundary_tx
                    .take()
                    .unwrap()
                    .send(event["wait_ms"].as_u64().unwrap_or(20));
                let _ = resume_rx.take().unwrap().await;
            } else if socket.send(Message::Text(event.to_string())).await.is_err() {
                break;
            }
        }
        let _ = socket.close(None).await;
    });
    let stream = TcpStream::connect(address).await.unwrap();
    let (socket, _) = tokio_tungstenite::client_async(format!("ws://{address}"), stream)
        .await
        .unwrap();
    let (input, receiver) = tokio::sync::mpsc::channel(1);
    input.send(vec![1, 0, 2, 0]).await.unwrap();
    drop(input);
    let (ready, connected) = tokio::sync::oneshot::channel();
    let check = valid.clone();
    let result = tokio::spawn(async move {
        transcribe_feed_complete(
            Source::QwenWork,
            socket,
            AudioFeed::Live(receiver),
            "test-session",
            Some(LiveCallbacks {
                ready,
                generation: "original".into(),
                publish: Arc::new(|_| Box::pin(std::future::ready(()))),
                recognized,
                valid: Arc::new(move || check.load(Ordering::Acquire)),
            }),
            Some(config()),
        )
        .await
    });
    assert_eq!(connected.await.unwrap(), "original");
    let wait_ms = boundary.await.unwrap();
    tokio::time::sleep(Duration::from_millis(wait_ms)).await;
    assert!(
        !result.is_finished(),
        "A recording cannot publish an incomplete result"
    );
    if invalidate_at_boundary {
        valid.store(false, Ordering::Release);
    }
    resume.send(()).unwrap();
    let result = result.await.unwrap();
    server.await.unwrap();
    result
}

fn ack(request_id: &str, enabled: bool) -> Value {
    json!({"type":"asr_polish_config_ack", "request_id":request_id, "enabled":enabled})
}
fn polished(request_id: &str) -> Value {
    json!({"type":"polish_completed", "request_id":request_id, "asr_session_id":"test-session",
        "raw_text":"First sentance. Last sentance.", "message":"First sentence. Last sentence.", "polished":true})
}
fn raw() -> Value {
    json!({"type":"speech_completed", "message":"First sentance. Last sentance."})
}
fn done() -> Value {
    json!({"type":"speech_done"})
}
fn boundary() -> Value {
    json!({"type":"test_boundary"})
}

#[tokio::test]
async fn recognized_text_is_complete_once_and_only_for_enabled_native_refinement() {
    for enabled in [true, false] {
        let updates = Arc::new(std::sync::Mutex::new(Vec::new()));
        let capture = updates.clone();
        let result = recording_with_raw(
            move |request_id| {
                vec![
                    ack(request_id, enabled),
                    raw(),
                    boundary(),
                    done(),
                    done(),
                    polished(request_id),
                ]
            },
            Arc::new(AtomicBool::new(true)),
            false,
            Some(Arc::new(move |text| {
                capture.lock().unwrap().push(text);
                Box::pin(std::future::ready(()))
            })),
        )
        .await
        .unwrap();
        let updates = updates.lock().unwrap();
        if enabled {
            assert_eq!(*updates, vec!["First sentance. Last sentance."]);
            assert!(result.refinement_complete);
        } else {
            assert!(updates.is_empty());
            assert!(!result.refinement_complete);
        }
    }
}

#[tokio::test]
async fn complete_refinement_waits_for_both_recording_and_native_result_in_either_order() {
    for first in [true, false] {
        let result = recording(
            move |request_id| {
                let mut events = vec![ack(request_id, true), raw()];
                events.push(if first { polished(request_id) } else { done() });
                events.push(boundary());
                events.push(if first { done() } else { polished(request_id) });
                events
            },
            Arc::new(AtomicBool::new(true)),
            false,
        )
        .await
        .unwrap();
        assert_eq!(result.text, "First sentence. Last sentence.");
        assert_eq!(result.raw_text, "First sentance. Last sentance.");
        assert!(result.polished);
        assert!(result.refinement_complete);
    }
}

#[tokio::test]
async fn unsupported_or_failed_refinement_returns_complete_raw_for_host_refinement() {
    for mode in 0..3 {
        let result = recording(
            move |request_id| {
                let mut events = if mode == 2 {
                    vec![raw(), boundary()]
                } else {
                    vec![ack(request_id, mode == 1), raw(), boundary()]
                };
                if mode == 1 {
                    let mut error = polished(request_id);
                    error["error"] = json!("unavailable");
                    events.push(error);
                }
                events.push(done());
                events
            },
            Arc::new(AtomicBool::new(true)),
            false,
        )
        .await
        .unwrap();
        assert_eq!(result.text, "First sentance. Last sentance.");
        assert_eq!(result.raw_text, result.text);
        assert!(!result.polished);
        assert!(!result.refinement_complete);
    }
}

#[tokio::test]
async fn recording_ignores_foreign_results_and_keeps_an_unchanged_native_result_complete() {
    let result = recording(
        |request_id| {
            let mut unchanged = polished(request_id);
            unchanged["asr_session_id"] = json!("provider-assigned-session");
            unchanged["message"] = json!("First sentance. Last sentance.");
            unchanged["polished"] = json!(false);
            vec![
                ack("other-request", true),
                ack(request_id, true),
                raw(),
                done(),
                polished("other-request"),
                boundary(),
                unchanged,
            ]
        },
        Arc::new(AtomicBool::new(true)),
        false,
    )
    .await
    .unwrap();
    assert!(result.refinement_complete);
    assert!(!result.polished);
    assert_eq!(result.text, result.raw_text);
}

#[tokio::test]
async fn slow_native_refinement_is_not_replaced_by_a_short_raw_text_deadline() {
    let result = recording(
        |request_id| {
            let mut wait = boundary();
            wait["wait_ms"] = json!(2000);
            vec![
                ack(request_id, true),
                raw(),
                done(),
                wait,
                polished(request_id),
            ]
        },
        Arc::new(AtomicBool::new(true)),
        false,
    )
    .await
    .unwrap();
    assert_eq!(result.text, "First sentence. Last sentence.");
    assert!(result.refinement_complete);
    assert!(result.polished);
}

#[tokio::test]
async fn missing_native_completion_is_an_error_instead_of_a_successful_raw_result() {
    let result = recording(
        |request_id| vec![ack(request_id, true), raw(), done(), boundary()],
        Arc::new(AtomicBool::new(true)),
        false,
    )
    .await;
    assert_eq!(result.unwrap_err(), "qoder_transcription_incomplete");
}

#[tokio::test]
async fn recording_rechecks_the_account_before_publishing_native_refinement() {
    let result = recording(
        |request_id| {
            vec![
                ack(request_id, true),
                raw(),
                done(),
                boundary(),
                polished(request_id),
            ]
        },
        Arc::new(AtomicBool::new(true)),
        true,
    )
    .await;
    assert_eq!(
        result.unwrap_err(),
        "qoder_transcription_credential_changed"
    );
}

#[tokio::test]
async fn native_raw_and_final_text_have_independent_bounds_with_a_bounded_combined_event() {
    for oversized in [false, true] {
        let result = recording(
            move |request_id| {
                let mut completion = polished(request_id);
                completion["raw_text"] = json!("r".repeat(MAX_RESULT_BYTES / 2 + 128));
                completion["message"] = json!("f".repeat(if oversized {
                    MAX_RESULT_BYTES + 1
                } else {
                    MAX_RESULT_BYTES / 2 + 128
                }));
                vec![ack(request_id, true), raw(), done(), boundary(), completion]
            },
            Arc::new(AtomicBool::new(true)),
            false,
        )
        .await;
        if oversized {
            assert_eq!(result.unwrap_err(), "qoder_transcription_result_too_large");
        } else {
            let result = result.unwrap();
            assert_eq!(result.text.len(), MAX_RESULT_BYTES / 2 + 128);
            assert_eq!(result.raw_text.len(), MAX_RESULT_BYTES / 2 + 128);
            assert!(result.refinement_complete);
        }
    }
}

#[test]
fn public_dictation_parameters_reject_unknown_fields_roles_and_unbounded_context() {
    let parse = |value| serde_json::from_value::<DictationStartParams>(value);
    assert!(parse(json!({"operationId":"recording", "source":"qwenwork"})).is_ok());
    assert!(parse(
        json!({"operationId":"recording", "source":"qwenwork", "refinement":{"messages":[]}})
    )
    .is_ok());
    for refinement in [
        json!({"messages":[{"role":"system","content":"text"}]}),
        json!({"messages":[{"role":"user","content":"text","extra":true}]}),
        json!({"messages":[], "enabled":true}),
        json!({"messages":[{"role":"user","content":"x".repeat(8193)}]}),
        json!({"messages":[{"role":"user","content":"\0"}]}),
        json!({"messages":vec![json!({"role":"user","content":"x"});41]}),
        json!({"messages":vec![json!({"role":"user","content":"x".repeat(8192)});5]}),
    ] {
        assert!(parse(
            json!({"operationId":"recording", "source":"qwenwork", "refinement":refinement})
        )
        .is_err());
    }
}
