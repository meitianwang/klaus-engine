use super::*;
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::{Response, StatusCode};
use hyper_util::rt::TokioIo;

#[derive(Clone)]
pub(crate) enum NativeReply {
    Success,
    Status(u16, Value),
    Json(Value),
    Hang,
}

pub(crate) struct NativeFixture {
    pub origin: String,
    pub requests: Arc<Mutex<Vec<(String, Value)>>>,
    pub reply: Arc<Mutex<NativeReply>>,
    pub started: Arc<tokio::sync::Notify>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for NativeFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub(crate) async fn native_fixture() -> NativeFixture {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fixture");
    let origin = format!("http://{}", listener.local_addr().expect("address"));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let reply = Arc::new(Mutex::new(NativeReply::Success));
    let started = Arc::new(tokio::sync::Notify::new());
    let (captured, selected, notified) = (
        Arc::clone(&requests),
        Arc::clone(&reply),
        Arc::clone(&started),
    );
    let task = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                socket = listener.accept() => {
                    let (socket, _) = socket.expect("accept fixture");
                    let (captured, selected, notified) = (Arc::clone(&captured), Arc::clone(&selected), Arc::clone(&notified));
                    connections.spawn(async move {
                        let service = hyper::service::service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
                            let (captured, selected, notified) = (Arc::clone(&captured), Arc::clone(&selected), Arc::clone(&notified));
                            async move {
                                let path = request.uri().path().to_string();
                                assert_eq!(request.headers().get("authorization").expect("bearer"), "Bearer fixture-access-token");
                                let body = Limited::new(request.into_body(), 16 * 1024 * 1024).collect().await.expect("bounded fixture request").to_bytes();
                                let body: Value = serde_json::from_slice(&body).expect("JSON request");
                                {
                                    let mut requests = captured.lock().unwrap();
                                    assert!(requests.len() < 64, "bounded fixture request list");
                                    requests.push((path.clone(), body));
                                }
                                if path.ends_with("/AvailableModels") {
                                    return Ok::<_, std::convert::Infallible>(Response::new(Full::new(Bytes::from(json!({"models":[{"name":"fixture","serverModelName":"fixture","clientDisplayName":"Fixture","supportsAgent":true,"variants":[
                                        {"displayName":"Fixture High Fast","legacySlug":"fixture-high-fast","isDefaultNonMaxConfig":true,"parameterValues":[{"id":"effort","value":"high"},{"id":"fast","value":"true"},{"id":"context","value":"200k"}]},
                                        {"displayName":"Fixture Max","legacySlug":"fixture-max","isMaxMode":true,"parameterValues":[{"id":"effort","value":"max"},{"id":"fast","value":"false"},{"id":"context","value":"1m"}]}
                                    ]}]}).to_string()))));
                                }
                                notified.notify_one();
                                let reply = selected.lock().unwrap().clone();
                                let (status, value) = match reply {
                                    NativeReply::Status(status, value) => (status, value),
                                    NativeReply::Json(value) => (200, value),
                                    NativeReply::Hang => {
                                        tokio::time::sleep(Duration::from_secs(30)).await;
                                        (200, json!({}))
                                    }
                                    NativeReply::Success => {
                                        let value = match path.rsplit('/').next().expect("method") {
                                            "RunGenerateImage" => json!({"success":{"imageData":general_purpose::STANDARD.encode(b"\x89PNG\r\n\x1a\nfixture-image"),"mimeType":"image/png"}}),
                                            "TextToSpeech" => json!({"audio":general_purpose::STANDARD.encode(b"ID3fixture-audio"),"mimeType":"audio/mpeg","synthesisTimeMs":"10"}),
                                            "RunGenerateVideo" => json!({"success":{"videoData":general_purpose::STANDARD.encode(b"\0\0\0\x14ftypisomfixture-video"),"mimeType":"video/mp4","durationSeconds":4}}),
                                            "RunWebSearch" => json!({"answer":"A bounded search answer","documents":[{"title":"Example","url":"https://example.com/page","text":"A result snippet"}]}),
                                            _ => panic!("unexpected native method"),
                                        };
                                        (200, value)
                                    }
                                };
                                Ok::<_, std::convert::Infallible>(Response::builder().status(StatusCode::from_u16(status).expect("status"))
                                    .header("content-type", "application/json")
                                    .body(Full::new(Bytes::from(value.to_string()))).expect("response"))
                            }
                        });
                        let _ = hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(socket), service).await;
                    });
                }
                _ = connections.join_next(), if !connections.is_empty() => {}
            }
        }
    });
    NativeFixture {
        origin,
        requests,
        reply,
        started,
        task,
    }
}

fn adopt(fixture: &NativeFixture, root: &std::path::Path) {
    super::super::set_config_root(root);
    assert_eq!(
        super::super::adopt_credential(
            json!({"accessToken":"fixture-access-token","clientIdentity":"fixture-device","backendUrl":fixture.origin,"inferenceUrl":fixture.origin}),
            None
        )["ok"],
        true
    );
}

fn image(value: Value) -> CursorImageParams {
    serde_json::from_value(value).expect("image params")
}
fn audio(value: Value) -> CursorAudioParams {
    serde_json::from_value(value).expect("audio params")
}
fn video(value: Value) -> CursorVideoParams {
    serde_json::from_value(value).expect("video params")
}
fn search_params(value: Value) -> CursorSearchParams {
    serde_json::from_value(value).expect("search params")
}

#[tokio::test]
async fn image_maps_real_serving_identity_references_and_aspect_without_pixel_promises() {
    let _guard = super::super::test_lock().lock().await;
    let root = tempfile::tempdir().expect("root");
    let fixture = native_fixture().await;
    adopt(&fixture, root.path());
    let reference = general_purpose::STANDARD.encode(b"\x89PNG\r\n\x1a\nreference");
    let result = generate_image(image(json!({"prompt":"Blue square","size":"1280x720","referenceImages":[{"dataBase64":reference,"mimeType":"image/png"}]})), AbortSignal::new()).await.expect("image").value;
    assert_eq!(result["mimeType"], "image/png");
    assert_eq!(result["pixelSize"], "provider-controlled");
    assert_eq!(result["aspectRatio"], "16:9");
    let captured = fixture.requests.lock().unwrap();
    let (_, body) = captured
        .iter()
        .find(|(path, _)| path.ends_with("RunGenerateImage"))
        .expect("image request");
    assert_eq!(
        body,
        &json!({"description":"Blue square","referenceImages":[{"data":reference,"mimeType":"image/png"}],"modelId":"fixture-high-fast","maxMode":false,"aspectRatio":"16:9"})
    );
}

#[tokio::test]
async fn speech_is_a_direct_model_independent_operation_and_preserves_voice_and_speed() {
    let _guard = super::super::test_lock().lock().await;
    let root = tempfile::tempdir().expect("root");
    let fixture = native_fixture().await;
    adopt(&fixture, root.path());
    let result = generate_audio(
        audio(json!({"text":"Hello","voice":"ara","speed":1.25})),
        AbortSignal::new(),
    )
    .await
    .expect("speech")
    .value;
    assert_eq!(
        result,
        json!({"audioBase64":general_purpose::STANDARD.encode(b"ID3fixture-audio"),"mimeType":"audio/mpeg"})
    );
    let captured = fixture.requests.lock().unwrap();
    assert_eq!(
        captured.as_slice(),
        &[(
            "/aiserver.v1.AiService/TextToSpeech".into(),
            json!({"text":"Hello","voiceId":"ara","speed":1.25})
        )]
    );
}

#[tokio::test]
async fn video_consumes_seconds_and_size_and_keeps_omitted_defaults_with_the_provider() {
    let _guard = super::super::test_lock().lock().await;
    let root = tempfile::tempdir().expect("root");
    let fixture = native_fixture().await;
    adopt(&fixture, root.path());
    let result = generate_video(
        video(json!({"prompt":"A moving square","seconds":4,"size":"720x1280"})),
        AbortSignal::new(),
    )
    .await
    .expect("video")
    .value;
    assert_eq!(result["seconds"], 4);
    assert_eq!(result["mimeType"], "video/mp4");
    generate_video(
        video(json!({"prompt":"A moving square"})),
        AbortSignal::new(),
    )
    .await
    .expect("provider defaults");
    let captured = fixture.requests.lock().unwrap();
    let calls: Vec<_> = captured
        .iter()
        .filter(|(path, _)| path.ends_with("RunGenerateVideo"))
        .collect();
    assert_eq!(
        calls[0].1,
        json!({"prompt":"A moving square","referenceImages":[],"modelId":"fixture-high-fast","maxMode":false,"durationSeconds":4,"aspectRatio":"9:16","resolution":"720p"})
    );
    assert!(calls[1].1.get("durationSeconds").is_none());
    assert!(calls[1].1.get("resolution").is_none());
    assert!(calls[1].1.get("aspectRatio").is_none());
}

#[tokio::test]
async fn search_returns_canonical_hits_and_observes_proto_empty_list_defaults() {
    let _guard = super::super::test_lock().lock().await;
    let root = tempfile::tempdir().expect("root");
    let fixture = native_fixture().await;
    adopt(&fixture, root.path());
    let result = search(
        search_params(json!({"query":"public documentation"})),
        AbortSignal::new(),
    )
    .await
    .expect("search")
    .value;
    assert_eq!(
        result,
        json!({"answer":"A bounded search answer","results":[{"title":"Example","url":"https://example.com/page","snippet":"A result snippet"}]})
    );
    *fixture.reply.lock().unwrap() = NativeReply::Json(json!({}));
    assert_eq!(
        search(
            search_params(json!({"query":"no matches"})),
            AbortSignal::new()
        )
        .await
        .expect("empty result")
        .value,
        json!({"results":[]})
    );
    *fixture.reply.lock().unwrap() = NativeReply::Json(json!({"documents":null}));
    assert_eq!(
        search(
            search_params(json!({"query":"malformed"})),
            AbortSignal::new()
        )
        .await
        .unwrap_err(),
        "cursor_search_response_invalid"
    );
}

#[tokio::test]
async fn configured_native_search_uses_the_engine_owned_account_root_and_standard_hits() {
    let _guard = super::super::test_lock().lock().await;
    let root = tempfile::tempdir().expect("root");
    let fixture = native_fixture().await;
    adopt(&fixture, root.path());
    let factory = crate::app_server::native_web_search::source_backend_factory();
    let backend = factory(&klaus_agent_core::schemas::config::WebSearchConfig {
        source: Some("cursor".into()),
        ..Default::default()
    })
    .expect("native account backend");
    let hits = backend("public docs".into(), AbortSignal::new())
        .await
        .expect("search");
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].title, "Example");
    assert_eq!(hits[0].url, "https://example.com/page");
    assert_eq!(
        hits[0].snippet.as_ref().expect("snippet"),
        "A result snippet"
    );
}

#[tokio::test]
async fn explicit_model_selections_preserve_exact_variants_and_never_substitute_an_alias() {
    let _guard = super::super::test_lock().lock().await;
    let root = tempfile::tempdir().expect("root");
    let fixture = native_fixture().await;
    adopt(&fixture, root.path());
    generate_image(image(json!({"prompt":"Square","modelSelection":"cursor:fixture[context=1m,effort=max,fast=false]@max"})), AbortSignal::new()).await.expect("exact max variant");
    let body = fixture
        .requests
        .lock()
        .unwrap()
        .iter()
        .find(|(path, _)| path.ends_with("RunGenerateImage"))
        .expect("image call")
        .1
        .clone();
    assert_eq!(body["modelId"], "fixture-max");
    assert_eq!(body["maxMode"], true);
    for selection in [
        "cursor:auto",
        "cursor:fixture",
        "cursor:missing",
        "codex:fixture",
    ] {
        assert_eq!(
            generate_image(
                image(json!({"prompt":"Square","modelSelection":selection})),
                AbortSignal::new()
            )
            .await
            .unwrap_err(),
            "cursor_model_unavailable"
        );
    }
    assert_eq!(
        fixture
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(path, _)| path.ends_with("RunGenerateImage"))
            .count(),
        1
    );
}

#[tokio::test]
async fn unsupported_controls_and_invalid_inputs_fail_before_authentication_or_network() {
    assert!(
        serde_json::from_value::<CursorAudioParams>(json!({"text":"Hello","emotion":"happy"}))
            .is_err()
    );
    assert!(
        serde_json::from_value::<CursorVideoParams>(json!({"prompt":"Square","seconds":1.5}))
            .is_err()
    );
    for size in ["1024x1536", "0x0", "8193x8193", "square", "16:10"] {
        assert_eq!(
            generate_image(
                image(json!({"prompt":"Square","size":size})),
                AbortSignal::new()
            )
            .await
            .unwrap_err(),
            "cursor_image_size_unsupported"
        );
    }
    for seconds in [0, 16] {
        assert_eq!(
            generate_video(
                video(json!({"prompt":"Square","seconds":seconds})),
                AbortSignal::new()
            )
            .await
            .unwrap_err(),
            "cursor_video_seconds_unsupported"
        );
    }
    for size in ["1920x1080", "0x480", "1024x1024", "16:10"] {
        assert_eq!(
            generate_video(
                video(json!({"prompt":"Square","size":size})),
                AbortSignal::new()
            )
            .await
            .unwrap_err(),
            "cursor_video_size_unsupported"
        );
    }
    for speed in [0.5, 2.1] {
        assert_eq!(
            generate_audio(
                audio(json!({"text":"Hello","speed":speed})),
                AbortSignal::new()
            )
            .await
            .unwrap_err(),
            "cursor_audio_speed_invalid"
        );
    }
    assert_eq!(
        generate_audio(audio(json!({"text":" ","voice":"ara"})), AbortSignal::new())
            .await
            .unwrap_err(),
        "cursor_audio_text_invalid"
    );
    assert_eq!(generate_image(image(json!({"prompt":"Square","referenceImages":[{"dataBase64":"AAAA","mimeType":"image/png"}]})), AbortSignal::new()).await.unwrap_err(), "cursor_reference_images_invalid");
}

#[test]
fn binary_bounds_mime_and_sizes_are_checked_before_publication() {
    assert!(decode_base64("====", 16).is_err());
    assert!(decode_base64(&general_purpose::STANDARD.encode([1u8; 17]), 16).is_err());
    assert_eq!(decode_base64("SUQz", 16).expect("base64"), b"ID3");
    assert!(!valid_image(b"<svg>payload</svg>", "image/png"));
    assert!(!valid_audio(b"fixture-token", "audio/mpeg"));
    assert_eq!(
        video_size(Some("1280x720")).expect("video size"),
        (Some("16:9".into()), Some("720p"))
    );
    assert_eq!(video_size(None).expect("default size"), (None, None));
}

#[test]
fn native_errors_use_safe_stable_auth_quota_capacity_and_gate_classes() {
    let gate = json!({"code":"permission_denied","message":GATE_REASON});
    assert_eq!(
        native_error(403, Some(&gate)),
        "cursor_native_capability_not_enabled"
    );
    assert_eq!(
        native_error(
            403,
            Some(&json!({"code":"permission_denied","message":"Bearer private-token"}))
        ),
        "cursor_permission_denied"
    );
    assert_eq!(native_error(401, None), credential::NEEDS_REIMPORT);
    assert_eq!(native_error(429, None), "cursor_rate_limit");
    assert_eq!(native_error(503, None), "cursor_overloaded");
    assert_eq!(
        native_error(
            200,
            Some(&json!({"error":{"error":"private-token","contentSafetyBlocked":true}}))
        ),
        "cursor_content_filter"
    );
    assert_eq!(
        native_error(
            200,
            Some(&json!({"error":{"error":"private-token","modelRestricted":true}}))
        ),
        "cursor_model_capability_unsupported"
    );
    assert_eq!(
        native_error(
            200,
            Some(&json!({"error":{"error":"private-token","providerStatusCode":429}}))
        ),
        "cursor_rate_limit"
    );
}

#[tokio::test]
async fn native_gate_keeps_valid_credentials_while_auth_failure_marks_import_for_reimport() {
    let _guard = super::super::test_lock().lock().await;
    let root = tempfile::tempdir().expect("root");
    let fixture = native_fixture().await;
    adopt(&fixture, root.path());
    *fixture.reply.lock().unwrap() = NativeReply::Status(
        403,
        json!({"code":"permission_denied","message":GATE_REASON}),
    );
    assert_eq!(
        generate_audio(audio(json!({"text":"Hello"})), AbortSignal::new())
            .await
            .unwrap_err(),
        "cursor_native_capability_not_enabled"
    );
    assert_eq!(super::super::status()["loggedIn"], true);
    *fixture.reply.lock().unwrap() = NativeReply::Status(
        401,
        json!({"code":"unauthenticated","message":"Bearer fixture-access-token"}),
    );
    assert_eq!(
        generate_audio(audio(json!({"text":"Hello"})), AbortSignal::new())
            .await
            .unwrap_err(),
        credential::NEEDS_REIMPORT
    );
    assert_eq!(super::super::status()["needsReimport"], true);
}

#[tokio::test]
async fn cancellation_drops_inflight_native_request_and_deadline_has_no_retry() {
    let _guard = super::super::test_lock().lock().await;
    let root = tempfile::tempdir().expect("root");
    let fixture = native_fixture().await;
    adopt(&fixture, root.path());
    *fixture.reply.lock().unwrap() = NativeReply::Hang;
    let signal = AbortSignal::new();
    let cancel = signal.clone();
    let request = tokio::spawn(generate_audio(audio(json!({"text":"Hello"})), signal));
    tokio::time::timeout(Duration::from_secs(1), fixture.started.notified())
        .await
        .expect("request started");
    cancel.cancel();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), request)
            .await
            .expect("bounded cancel")
            .expect("task")
            .unwrap_err(),
        "cursor_operation_cancelled"
    );
    assert_eq!(fixture.requests.lock().unwrap().len(), 1);
    assert_eq!(
        cancellable(
            &AbortSignal::new(),
            Duration::from_millis(5),
            std::future::pending::<Result<(), String>>()
        )
        .await
        .unwrap_err(),
        "cursor_operation_timed_out"
    );
}

#[tokio::test]
async fn credential_rotation_interrupts_inflight_operations_without_rejecting_the_new_account() {
    let _guard = super::super::test_lock().lock().await;
    let root = tempfile::tempdir().expect("root");
    let fixture = native_fixture().await;
    adopt(&fixture, root.path());
    *fixture.reply.lock().unwrap() = NativeReply::Hang;
    let request = tokio::spawn(generate_audio(
        audio(json!({"text":"Hello"})),
        AbortSignal::new(),
    ));
    tokio::time::timeout(Duration::from_secs(1), fixture.started.notified())
        .await
        .expect("request started");
    let latest = super::super::adopt_credential(
        json!({"accessToken":"newer-account-token","clientIdentity":"newer-device"}),
        None,
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), request)
            .await
            .expect("bounded rotation")
            .expect("task")
            .unwrap_err(),
        "cursor_credential_changed"
    );
    assert_eq!(
        super::super::status()["credentialId"],
        latest["credentialId"]
    );
    assert_eq!(super::super::status()["loggedIn"], true);
}

#[tokio::test]
async fn response_size_and_malformed_media_are_rejected_without_retry() {
    let _guard = super::super::test_lock().lock().await;
    let root = tempfile::tempdir().expect("root");
    let fixture = native_fixture().await;
    adopt(&fixture, root.path());
    let session = NativeSession::start().await.expect("session");
    assert_eq!(
        session
            .request("TextToSpeech", json!({"text":"Hello"}), 16)
            .await
            .unwrap_err(),
        "cursor_response_too_large"
    );
    *fixture.reply.lock().unwrap() =
        NativeReply::Json(json!({"audio":"AAAA","mimeType":"audio/mpeg"}));
    assert_eq!(
        generate_audio(audio(json!({"text":"Hello"})), AbortSignal::new())
            .await
            .unwrap_err(),
        "cursor_native_media_invalid"
    );
    assert_eq!(fixture.requests.lock().unwrap().len(), 2);
}

#[test]
fn operation_ids_are_connection_owned_bounded_and_cancel_before_async_work() {
    let operations = Arc::new(Operations::default());
    let first = operations.register(Some("operation-1")).expect("register");
    assert_eq!(
        operations
            .register(Some("operation-1"))
            .err()
            .expect("duplicate"),
        "cursor_operation_id_in_use"
    );
    assert_eq!(
        operations.cancel("operation-1").expect("cancel")["cancelled"],
        true
    );
    assert!(first.signal().is_cancelled());
    assert_eq!(
        Operations::default()
            .cancel("operation-1")
            .expect("foreign connection")["cancelled"],
        false
    );
    assert!(operations.register(Some("invalid\noperation")).is_err());
    let mut guards = vec![first];
    for index in 1..MAX_OPERATIONS {
        guards.push(
            operations
                .register(Some(&format!("operation-{index}-extra")))
                .expect("slot"),
        );
    }
    assert_eq!(
        operations.register(None).err().expect("limit"),
        "cursor_operation_limit"
    );
    operations.cancel_all();
    assert!(guards
        .iter()
        .all(|operation| operation.signal().is_cancelled()));
    drop(guards);
    assert!(operations.register(Some("operation-1")).is_ok());
}
