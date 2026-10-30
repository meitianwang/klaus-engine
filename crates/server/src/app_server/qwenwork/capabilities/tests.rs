use super::*;
use bytes::Bytes;
use futures::SinkExt;
use http_body_util::{BodyExt, Full, Limited};
use hyper::{Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::sync::Mutex;

#[derive(Clone, Copy, Default)]
pub(crate) enum Reply {
    #[default]
    Success,
    Status(u16),
    Hang,
    WrongSession,
    UntrustedBase,
    BlankBase,
    SpacedSession,
    WrongTask,
    PollUnauthorized,
    Oversized,
}
pub(crate) struct Fixture {
    pub origin: String,
    pub requests: Arc<Mutex<Vec<(String, Value)>>>,
    pub reply: Arc<Mutex<Reply>>,
    pub started: Arc<tokio::sync::Notify>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
pub(crate) async fn fixture() -> Fixture {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let reply = Arc::new(Mutex::new(Reply::Success));
    let started = Arc::new(tokio::sync::Notify::new());
    let (base, captured, selected, notified) = (
        origin.clone(),
        requests.clone(),
        reply.clone(),
        started.clone(),
    );
    let task = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                socket = listener.accept() => {
                    let (socket,_) = socket.unwrap();
                    let (base,captured,selected,notified)=(base.clone(),captured.clone(),selected.clone(),notified.clone());
                    connections.spawn(async move {
                        let service = hyper::service::service_fn(move |mut request: hyper::Request<hyper::body::Incoming>| {
                            let (base,captured,selected,notified)=(base.clone(),captured.clone(),selected.clone(),notified.clone());
                            async move {
                                let path=request.uri().path().to_string();
                                let is_get=request.method()==hyper::Method::GET;
                                if path == "/api/v2/service/ws/asr" {
                                    assert!(request.headers()["authorization"].to_str().unwrap().starts_with("Bearer COSY."));
                                    assert_eq!(request.headers()["cosy-user"],"quota-user");
                                    assert_eq!(serde_json::from_str::<Value>(request.headers()["x-business"].to_str().unwrap()).unwrap()["product"],"qoder_work");
                                    let accept = tokio_tungstenite::tungstenite::handshake::derive_accept_key(request.headers()["sec-websocket-key"].as_bytes());
                                    let upgrade = hyper::upgrade::on(&mut request);
                                    tokio::spawn(async move {
                                        let socket = upgrade.await.unwrap();
                                        let mut socket = tokio_tungstenite::WebSocketStream::from_raw_socket(TokioIo::new(socket),tokio_tungstenite::tungstenite::protocol::Role::Server,None).await;
                                        let mut frames=0;
                                        while let Some(message)=socket.next().await {
                                            match message.unwrap() {
                                                tokio_tungstenite::tungstenite::Message::Binary(bytes)=>{assert!(!bytes.is_empty() && bytes.len() <=3200);frames+=1;assert!(frames<=3600);},
                                                tokio_tungstenite::tungstenite::Message::Text(text)=>{
                                                    assert_eq!(serde_json::from_str::<Value>(&text).unwrap(),json!({"type":"voice_completed","message":"close by user"}));
                                                    socket.send(tokio_tungstenite::tungstenite::Message::Text(json!({"type":"speech_completed","message":"First "}).to_string())).await.unwrap();
                                                    socket.send(tokio_tungstenite::tungstenite::Message::Text(json!({"type":"speech_completed","message":"A green circle"}).to_string())).await.unwrap();
                                                    socket.send(tokio_tungstenite::tungstenite::Message::Text(json!({"type":"speech_done"}).to_string())).await.unwrap();break;
                                                }
                                                _=>{}
                                            }
                                        }
                                    });
                                    return Ok::<_,std::convert::Infallible>(Response::builder().status(101).header("connection","Upgrade").header("upgrade","websocket").header("sec-websocket-accept",accept).body(Full::new(Bytes::new())).unwrap());
                                }
                                let expected=if path=="/api/desktop-sessions" || path.starts_with("/api/v1/adapter/user/account-context") {"Bearer fixture-host-token"} else {"Bearer fixture-service-token"};
                                if !path.starts_with("/artifact") { assert_eq!(request.headers()["authorization"],expected); }
                                else { assert!(!request.headers().contains_key("authorization")); }
                                let data=Limited::new(request.into_body(),16*1024*1024).collect().await.unwrap().to_bytes();
                                let body:Value=if data.is_empty(){Value::Null}else{serde_json::from_slice(&data).unwrap()};
                                let number={let mut rows=captured.lock().unwrap(); assert!(rows.len()<64); rows.push((path.clone(),body.clone()));rows.iter().filter(|(p,_)|p==&path).count()};
                                let mode=*selected.lock().unwrap();
                                let (status,value)=if path.starts_with("/api/v1/adapter/user/account-context") {
                                    (200,json!({"data":{"user":{"id":"quota-user"},"quota":{"remaining":20}}}))
                                } else if path=="/api/desktop-sessions" {
                                    (200,json!({"access_token":"fixture-service-token","session_id":if matches!(mode,Reply::WrongSession){json!(uuid::Uuid::new_v4().to_string())}else if matches!(mode,Reply::SpacedSession){json!(format!(" {} ",body["session_id"].as_str().unwrap()))}else{body["session_id"].clone()},"base_url":if matches!(mode,Reply::UntrustedBase){"https://foreign.example"}else if matches!(mode,Reply::BlankBase){"  "}else{&base},"expires_at_ms":chrono::Utc::now().timestamp_millis()+3_600_000,"ttl_ms":3_600_000}))
                                } else if path.starts_with("/artifact") {
                                    let artifact = if path.ends_with("/video") { b"\0\0\0\x14ftypisomfixture-video".as_slice() } else if path.ends_with("/music") { b"ID3fixture-music".as_slice() } else { b"\x89PNG\r\n\x1a\nfixture-image".as_slice() };
                                    return Ok::<_,std::convert::Infallible>(Response::new(Full::new(Bytes::copy_from_slice(artifact))));
                                } else {
                                    notified.notify_one();
                                    match mode {
                                        Reply::Status(status)=>(status,json!({"error":"fixture-host-token fixture-service-token https://secret.example/?token=private"})),
                                        Reply::Hang=>{tokio::time::sleep(Duration::from_secs(30)).await;(200,json!({}))},
                                        Reply::Oversized=>(200,json!({"oversized":"x".repeat(MAX_JSON)})),
                                        Reply::PollUnauthorized if is_get && number==1=>(401,json!({})),
                                        _ if path.ends_with("/tools/web-search")=>(200,json!({"provider":"fixture","results":[{"title":"IANA","url":"https://www.iana.org/domains/reserved","content":"Official example domains"}]})),
                                        _ if is_get=>{
                                            let mut value=json!({"task_info":{"id":if matches!(mode,Reply::WrongTask){"foreign-task"}else{"task-1"},"status":"completed"}});
                                            let (key,suffix)=if path.contains("/bytedance/") || path.contains("/alibaba/"){("videos","/video")}else if path.contains("/minimax/"){("audios","/music")}else{("images","")};
                                            value[key]=json!([format!("{base}/artifact{suffix}")]);(200,value)
                                        },
                                        _=>(200,json!({"task_info":{"id":"task-1","status":"running"}})),
                                    }
                                };
                                Ok::<_,std::convert::Infallible>(Response::builder().status(StatusCode::from_u16(status).unwrap()).header("content-type","application/json").body(Full::new(Bytes::from(value.to_string()))).unwrap())
                            }
                        });
                        let _=hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(socket),service).with_upgrades().await;
                    });
                }
                _=connections.join_next(),if !connections.is_empty()=>{}
            }
        }
    });
    Fixture {
        origin,
        requests,
        reply,
        started,
        task,
    }
}
pub(crate) struct Setup {
    previous: Option<std::ffi::OsString>,
}
impl Drop for Setup {
    fn drop(&mut self) {
        match &self.previous {
            Some(value) => klaus_agent_core::runtime_process::set_process_environment_var(
                "QWENWORK_API_BASE",
                value,
            ),
            None => klaus_agent_core::runtime_process::remove_process_environment_var(
                "QWENWORK_API_BASE",
            ),
        };
    }
}
pub(crate) fn setup(fixture: &Fixture, root: &std::path::Path) -> Setup {
    super::super::set_config_root(root);
    std::fs::write(root.join("qwenwork-subscription-oauth.json"),json!({"machineId":"fixture-device","userInfo":{"access_token":"fixture-host-token","security_oauth_token":"fixture-host-token","expire_time":chrono::Utc::now().timestamp()+86400}}).to_string()).unwrap();
    let previous = std::env::var_os("QWENWORK_API_BASE");
    klaus_agent_core::runtime_process::set_process_environment_var(
        "QWENWORK_API_BASE",
        &fixture.origin,
    );
    Setup { previous }
}
fn image(value: Value) -> QwenWorkImageParams {
    serde_json::from_value(value).unwrap()
}
fn video(value: Value) -> QwenWorkVideoParams {
    serde_json::from_value(value).unwrap()
}
fn music(value: Value) -> QwenWorkMusicParams {
    serde_json::from_value(value).unwrap()
}
fn search_params(value: Value) -> QwenWorkSearchParams {
    serde_json::from_value(value).unwrap()
}

#[test]
fn requests_use_native_edit_references_and_explicit_media_controls() {
    let reference = STANDARD.encode(b"\x89PNG\r\n\x1a\nreference");
    let request=image_request(image(json!({"prompt":"画绿色圆形","size":"1024x1024","referenceImages":[{"dataBase64":reference,"mimeType":"image/png"}]}))).unwrap();
    assert_eq!(request.path, "/vendors/openai/v1/gpt-image-2/edit");
    assert_eq!(
        request.body["images"][0],
        format!("data:image/png;base64,{reference}")
    );
    assert_eq!(request.body["prompt"], "画绿色圆形");
    let request = video_request(video(
        json!({"prompt":"A moving circle","seconds":4,"size":"720x1280"}),
    ))
    .unwrap();
    assert_eq!(
        request.body,
        json!({"prompt":"A moving circle","duration":4,"resolution":"720p","aspect_ratio":"9:16"})
    );
    let request = music_request(music(
        json!({"prompt":"A quiet piano song","lyrics":"[verse] A quiet day"}),
    ))
    .unwrap();
    assert_eq!(request.body["lyrics_optimizer"], false);
    assert_eq!(request.body["lyrics_prompt"], "[verse] A quiet day");
    assert_eq!(
        request.body["audio_setting"],
        json!({"format":"mp3","sample_rate":44100,"bitrate":256000})
    );
}
#[test]
fn unsupported_or_unbound_media_parameters_fail_before_network() {
    assert!(image_request(image(json!({"prompt":"x","size":"8192x8192"}))).is_err());
    assert!(image_request(image(json!({"prompt":"x","model":"auto"}))).is_err());
    assert!(image_request(image(json!({"prompt":"x","sessionId":""}))).is_err());
    assert!(video_request(video(json!({"prompt":"x","seconds":3}))).is_err());
    assert!(video_request(video(
        json!({"prompt":"x","model":"happy-horse-1.0","size":"854x480"})
    ))
    .is_err());
    assert!(music_request(music(json!({"prompt":"x","lyrics":"short"}))).is_err());
    assert!(serde_json::from_value::<QwenWorkMusicParams>(
        json!({"prompt":"x","instrumental":true})
    )
    .is_err());
    assert!(serde_json::from_value::<QwenWorkImageParams>(
        json!({"prompt":"x","callback":"ignored"})
    )
    .is_err());
    assert!(image_request(image(json!({"prompt":"x","referenceImages":[{"dataBase64":STANDARD.encode(b"\x89PNG\r\n\x1a\nreference"),"mimeType":"image/jpeg"}]}))).is_err());
}

#[test]
fn resolution_only_video_models_adapt_pixel_dimensions_without_aspect_overrides() {
    for (size, resolution) in [
        ("1280x720", "720P"),
        ("720x1280", "720P"),
        ("1920x1080", "1080P"),
        ("1080x1920", "1080P"),
        ("720p", "720P"),
        ("1080p", "1080P"),
    ] {
        let request = video_request(video(
            json!({"prompt":"A moving circle","model":"happy-horse-1.0","size":size,"seconds":5}),
        ))
        .unwrap();
        assert_eq!(
            request.body,
            json!({"prompt":"A moving circle","resolution":resolution,"duration":5})
        );
    }
    for size in ["480p", "4k", "0x720", "1280x0", "1280xx720"] {
        assert!(video_request(video(
            json!({"prompt":"A moving circle","model":"happy-horse-1.0","size":size})
        ))
        .is_err());
    }
}
#[test]
fn service_and_artifact_origins_reject_secret_and_private_routes() {
    for value in [
        "http://maas.xiaoluozi.cn",
        "https://maas.xiaoluozi.cn.evil.example",
        "https://user@maas.xiaoluozi.cn",
        "https://maas.xiaoluozi.cn/?token=secret",
        "https://maas.xiaoluozi.cn/arbitrary",
    ] {
        assert!(checked_base(value).is_err(), "{value}");
    }
    assert_eq!(
        checked_base("https://maas.xiaoluozi.cn/v1/").unwrap(),
        "https://maas.xiaoluozi.cn/v1"
    );
    for value in [
        "http://example.com/file",
        "https://127.0.0.2/image",
        "https://[::ffff:127.0.0.1]/image",
        "https://user:secret@example.com/image",
        "https://metadata.internal/image",
    ] {
        assert!(public_url(value, true).is_err());
    }
    assert!(public_url("http://www.iana.org/domains/reserved", false).is_ok());
}
#[test]
fn search_maps_only_real_supported_fields_and_retains_citations() {
    let (body, count) = search_request(&search_params(
        json!({"query":" IANA domains ","count":11,"timeRange":"week"}),
    ))
    .unwrap();
    assert_eq!(
        body,
        json!({"query":"IANA domains","count":20,"recency":"oneWeek"})
    );
    assert_eq!(count, 11);
    assert!(search_request(&search_params(json!({"query":"x".repeat(71)}))).is_err());
    assert!(search_request(&search_params(json!({"query":"x","timeRange":"year"}))).is_err());
    assert!(search_request(&search_params(json!({"query":"x","count":0}))).is_err());
    assert_eq!(search_result(json!({"results":[{"title":"IANA","url":"https://www.iana.org/","content":"source text"}]}),5).unwrap()["results"][0]["snippet"],"source text");
}
#[tokio::test]
async fn native_image_mints_bound_grant_polls_same_task_and_downloads_without_auth_leak() {
    let _guard = super::super::test_env_lock().lock().await;
    let temp = tempfile::tempdir().unwrap();
    let fixture = fixture().await;
    let _setup = setup(&fixture, temp.path());
    let response = generate_image(
        image(json!({"prompt":"A green circle"})),
        AbortSignal::new(),
    )
    .await
    .ok()
    .unwrap();
    assert_eq!(response.value["mimeType"], "image/png");
    assert_eq!(
        STANDARD
            .decode(response.value["imageBase64"].as_str().unwrap())
            .unwrap(),
        b"\x89PNG\r\n\x1a\nfixture-image"
    );
    let requests = fixture.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 4);
    assert_eq!(requests[0].0, "/api/desktop-sessions");
    assert!(uuid::Uuid::parse_str(requests[0].1["session_id"].as_str().unwrap()).is_ok());
    assert_eq!(requests[1].0, "/vendors/openai/v1/gpt-image-2/generation");
    assert_eq!(requests[2].0, format!("{}/task-1", requests[1].0));
}
#[tokio::test]
async fn unsafe_grants_task_swaps_bounds_and_upstream_errors_never_publish_payloads() {
    let _guard = super::super::test_env_lock().lock().await;
    for (mode, expected) in [
        (Reply::WrongSession, "qwenwork_session_mismatch"),
        (Reply::UntrustedBase, "qwenwork_service_origin_invalid"),
        (Reply::WrongTask, "qwenwork_task_mismatch"),
        (Reply::Status(401), "qwenwork_login_expired"),
        (Reply::Status(403), "qwenwork_permission_denied"),
        (Reply::Status(429), "qwenwork_rate_limit"),
        (Reply::Status(503), "qwenwork_service_unavailable"),
        (Reply::Oversized, "qwenwork_response_too_large"),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let fixture = fixture().await;
        let _setup = setup(&fixture, temp.path());
        *fixture.reply.lock().unwrap() = mode;
        let error = generate_image(image(json!({"prompt":"A circle"})), AbortSignal::new())
            .await
            .err()
            .unwrap();
        assert_eq!(error, expected);
        assert!(!error.contains("fixture-host-token"));
        assert!(!error.contains("fixture-service-token"));
        assert!(
            fixture
                .requests
                .lock()
                .unwrap()
                .iter()
                .filter(|(path, _)| path.ends_with("/generation"))
                .count()
                <= 1,
            "paid submissions never retry"
        );
    }
}
#[tokio::test]
async fn search_and_readonly_poll_retry_one_unauthorized_grant_without_resubmitting_media() {
    let _guard = super::super::test_env_lock().lock().await;
    let temp = tempfile::tempdir().unwrap();
    let fixture = fixture().await;
    let _setup = setup(&fixture, temp.path());
    *fixture.reply.lock().unwrap() = Reply::PollUnauthorized;
    generate_image(image(json!({"prompt":"A circle"})), AbortSignal::new())
        .await
        .ok()
        .unwrap();
    let requests = fixture.requests.lock().unwrap().clone();
    assert_eq!(
        requests
            .iter()
            .filter(|(path, _)| path.ends_with("/generation"))
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|(path, _)| path == "/api/desktop-sessions")
            .count(),
        2
    );
    drop(requests);
    assert_eq!(
        search(
            search_params(json!({"query":"IANA example domains"})),
            AbortSignal::new()
        )
        .await
        .ok()
        .unwrap()
        .value["results"][0]["title"],
        "IANA"
    );
}
#[tokio::test]
async fn cancellation_and_credential_changes_interrupt_inflight_requests() {
    let _guard = super::super::test_env_lock().lock().await;
    for credential_change in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let fixture = fixture().await;
        let _setup = setup(&fixture, temp.path());
        *fixture.reply.lock().unwrap() = Reply::Hang;
        let signal = AbortSignal::new();
        let copy = signal.clone();
        let work =
            tokio::spawn(
                async move { generate_image(image(json!({"prompt":"A circle"})), copy).await },
            );
        tokio::time::timeout(Duration::from_secs(2), fixture.started.notified())
            .await
            .unwrap();
        if credential_change {
            credential_changed();
        } else {
            signal.cancel();
        }
        let error = tokio::time::timeout(Duration::from_secs(2), work)
            .await
            .unwrap()
            .unwrap()
            .err()
            .unwrap();
        assert_eq!(
            error,
            if credential_change {
                "qwenwork_credential_changed"
            } else {
                "qwenwork_operation_cancelled"
            }
        );
    }
}

#[tokio::test]
async fn qwenwork_transcription_resolves_own_quota_uid_and_retains_final_speech_segments() {
    let _guard = super::super::test_env_lock().lock().await;
    let temp = tempfile::tempdir().unwrap();
    let fixture = fixture().await;
    let _setup = setup(&fixture, temp.path());
    let params = serde_json::from_value(
        json!({"audioBase64":STANDARD.encode([0,0,1,0]),"mimeType":"audio/pcm"}),
    )
    .unwrap();
    let response = transcribe_audio(params, AbortSignal::new())
        .await
        .ok()
        .unwrap();
    assert_eq!(response.value, json!({"text":"First A green circle"}));
    assert!(fixture
        .requests
        .lock()
        .unwrap()
        .iter()
        .all(|(path, _)| path != "/api/desktop-sessions"));
}

#[tokio::test]
async fn mint_without_nonempty_base_uses_the_trusted_native_default() {
    let _guard = super::super::test_env_lock().lock().await;
    let temp = tempfile::tempdir().unwrap();
    let fixture = fixture().await;
    let _setup = setup(&fixture, temp.path());
    *fixture.reply.lock().unwrap() = Reply::BlankBase;
    let session = NativeSession::start(uuid::Uuid::new_v4().to_string())
        .await
        .ok()
        .unwrap();
    assert_eq!(session.base, "https://maas.xiaoluozi.cn/v1");
    assert_eq!(fixture.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn native_video_and_music_return_actual_formats_without_inventing_duration() {
    let _guard = super::super::test_env_lock().lock().await;
    let temp = tempfile::tempdir().unwrap();
    let fixture = fixture().await;
    let _setup = setup(&fixture, temp.path());
    let video = generate_video(
        video(json!({"prompt":"A moving circle","seconds":4,"size":"720p"})),
        AbortSignal::new(),
    )
    .await
    .ok()
    .unwrap();
    assert_eq!(video.value["mimeType"], "video/mp4");
    assert!(video.value.get("seconds").is_none());
    let music = generate_music(
        music(json!({"prompt":"A piano melody"})),
        AbortSignal::new(),
    )
    .await
    .ok()
    .unwrap();
    assert_eq!(music.value["mimeType"], "audio/mpeg");
    assert!(music.value["audioBase64"]
        .as_str()
        .is_some_and(|value| !value.is_empty()));
}
#[tokio::test]
async fn mint_session_identity_is_trimmed_and_case_insensitive_without_changing_caller_attribution()
{
    let _guard = super::super::test_env_lock().lock().await;
    let temp = tempfile::tempdir().unwrap();
    let fixture = fixture().await;
    let _setup = setup(&fixture, temp.path());
    *fixture.reply.lock().unwrap() = Reply::SpacedSession;
    let session = NativeSession::start("owned-session".into())
        .await
        .ok()
        .unwrap();
    assert_eq!(session.session, "owned-session");
}

#[test]
fn artifact_dns_supports_tunnel_answers_without_allowing_private_or_literal_targets() {
    let ip = |value: &str| value.parse::<std::net::IpAddr>().unwrap();
    assert!(artifact_dns_address("assets.example.com", ip("198.18.0.1")));
    assert!(artifact_dns_address(
        "assets.example.com",
        ip("198.19.255.254")
    ));
    assert!(!artifact_dns_address("198.18.0.1", ip("198.18.0.1")));
    assert!(!artifact_dns_address("localhost", ip("198.18.0.1")));
    for address in [
        "127.0.0.1",
        "10.0.0.1",
        "169.254.169.254",
        "100.64.0.1",
        "::1",
        "fc00::1",
    ] {
        assert!(!artifact_dns_address("assets.example.com", ip(address)));
    }
    assert!(public_url("https://198.18.0.1/image", true).is_err());
}
