use super::*;
#[test]
fn catalog_visibility_and_feature_gates_control_real_media_models() {
    let v = json!({"models":[{"id":"shared","name":"Image","tags":["text-to-image","image-to-image"]},{"id":"hidden","tags":["text-to-image"]},{"id":"video","tags":["text-to-video","image-to-video"]}],"availableModels":["shared","video"],"productFeatures":{"ImageGen":true,"ImageEdit":false,"VideoGen":true,"DisableVoiceInput":true}});
    let c = project_capabilities(&v);
    assert_eq!(c["image"]["models"].as_array().unwrap().len(), 1);
    assert_eq!(c["imageEdit"]["available"], true);
    assert_eq!(c["video"]["models"].as_array().unwrap().len(), 1);
    assert_eq!(c["transcription"]["available"], false);
    assert_eq!(c["audio"]["voice"], false);
    assert!(select(&v, "text-to-image", "ImageGen", Some("hidden")).is_err());
    assert_eq!(
        select(&v, "image-to-image", "ImageEdit", Some("shared")).unwrap()["id"],
        "shared"
    );
    let mut disabled = v;
    disabled["productFeatures"]["DisableMultimodalGeneration"] = json!(true);
    assert_eq!(project_capabilities(&disabled)["image"]["available"], false);
}
#[test]
fn video_dimensions_use_catalog_resolution_or_the_model_default() {
    let model =
        json!({"videoGeneration":{"resolutions":["480P","720P"],"defaultResolution":"720P"}});
    assert_eq!(
        video_size(Some("1280x720"), &model, false).unwrap()["resolution"],
        "720P"
    );
    assert_eq!(
        video_size(Some("3840x2160"), &model, false).unwrap(),
        json!({"enable_audio":true,"resolution":"720P","aspect_ratio":"16:9"})
    );
    let quality = json!({"videoGeneration":{"resolutions":["standard","professional"],"defaultResolution":"professional"}});
    assert_eq!(
        video_size(Some("720x1280"), &quality, false).unwrap(),
        json!({"enable_audio":true,"resolution":"professional","aspect_ratio":"9:16"})
    );
    assert_eq!(
        video_size(Some("1024x1024"), &json!({}), false).unwrap(),
        json!({"enable_audio":true,"aspect_ratio":"1:1"})
    );
    assert_eq!(
        video_size(
            Some("1280x720"),
            &json!({"videoGeneration":{"resolutions":["standard","professional"]}}),
            false
        )
        .unwrap(),
        json!({"enable_audio":true,"aspect_ratio":"16:9"})
    );
    assert_eq!(
        video_size(None, &model, false).unwrap(),
        json!({"enable_audio":true,"resolution":"720P","aspect_ratio":"16:9"})
    );
}
#[test]
fn reference_video_geometry_remains_image_derived() {
    let model =
        json!({"videoGeneration":{"resolutions":["480P","720P"],"defaultResolution":"480P"}});
    for size in ["1280x720", "720x1280", "1000x720", "720P"] {
        assert_eq!(
            video_size(Some(size), &model, true).unwrap(),
            json!({"enable_audio":true,"resolution":"720P"})
        );
    }
    assert_eq!(
        video_size(Some("16:9"), &model, true).unwrap(),
        json!({"enable_audio":true,"resolution":"480P"})
    );
    assert_eq!(
        video_size(Some("1280x720"), &json!({}), true).unwrap(),
        json!({"enable_audio":true})
    );
}
#[test]
fn malformed_video_geometry_is_rejected_before_submission() {
    let model = json!({"videoGeneration":{"defaultResolution":"720P"}});
    for size in [
        "",
        "0x720",
        "1280x0",
        "1280xx720",
        "+1280x720",
        "1280x720x1",
        "invalid",
        "16385x16385",
    ] {
        for has_reference in [false, true] {
            assert_eq!(
                video_size(Some(size), &model, has_reference).unwrap_err(),
                "workbuddy_video_size_unsupported",
                "{size}"
            );
        }
    }
    assert!(video_size(Some("1000x720"), &model, false).is_err());
    assert!(video_size(Some("4294967295x1"), &model, false).is_err());
}
#[test]
fn references_require_matching_binary_types_and_aggregate_bound() {
    let invalid = WorkBuddyReferenceImage {
        data_base64: STANDARD.encode(b"not image"),
        mime_type: "image/png".into(),
    };
    assert!(reference_images(&[invalid], 4).is_err());
    let wrong = WorkBuddyReferenceImage {
        data_base64: STANDARD.encode(b"\x89PNG\r\n\x1a\n"),
        mime_type: "image/jpeg".into(),
    };
    assert!(reference_images(&[wrong], 4).is_err());
}
#[tokio::test]
async fn finite_image_route_uses_account_identity_and_sparse_model_overlay() {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    let _lock = super::super::test_env_lock().lock().await;
    let root = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    super::super::set_config_root(root.path());
    std::fs::write(root.path().join("workbuddy-subscription-oauth.json"),json!({"deviceId":"fixture-device","userInfo":{"access_token":"fixture-secret","uid":"fixture-user","domain":base,"login_method":"adopted"}}).to_string()).unwrap();
    let task = tokio::spawn(async move {
        for route in ["/v3/config", "/v2/images/generations"] {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buf = [0u8; 8192];
            loop {
                let n = socket.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                request.extend_from_slice(&buf[..n]);
                assert!(request.len() < 65536);
                if let Some(end) = request.windows(4).position(|v| v == b"\r\n\r\n") {
                    let head = String::from_utf8_lossy(&request[..end]);
                    let len = head
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(str::trim)
                                .and_then(|v| v.parse::<usize>().ok())
                        })
                        .unwrap_or(0);
                    if request.len() >= end + 4 + len {
                        break;
                    }
                }
            }
            let text = String::from_utf8(request).unwrap();
            assert!(text.lines().next().unwrap().contains(route));
            assert!(text
                .to_ascii_lowercase()
                .contains("authorization: bearer fixture-secret"));
            assert!(text
                .to_ascii_lowercase()
                .contains("x-user-id: fixture-user"));
            let response=if route=="/v3/config"{json!({"code":0,"data":{"models":[{"id":"gpt-image-2.5-sunburst"}],"productFeatures":{"ImageGen":true,"DisableMultimodalGeneration":false}}})}else{let body=text.split("\r\n\r\n").nth(1).unwrap();let body:Value=serde_json::from_str(body).unwrap();assert_eq!(body["model"],"gpt-image-2.5-sunburst");assert_eq!(body["response_format"],"b64_json");json!({"code":0,"data":{"data":[{"b64_json":STANDARD.encode(b"\x89PNG\r\n\x1a\n")} ]}})}.to_string();
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{}",response.len(),response).as_bytes()).await.unwrap();
        }
    });
    let result = generate_image(
        WorkBuddyImageParams {
            prompt: "A blue square".into(),
            size: None,
            session_id: None,
            reference_images: vec![],
            model: None,
            operation_id: None,
        },
        AbortSignal::new(),
    )
    .await
    .unwrap();
    assert_eq!(result.value["mimeType"], "image/png");
    assert!(!result.value.to_string().contains("fixture-secret"));
    task.await.unwrap();
}

#[test]
fn media_facade_accepts_host_session_and_rejects_unknown_options() {
    let params:WorkBuddyImageParams=serde_json::from_value(json!({"prompt":"A blue square","sessionId":"owned-session","model":"image","referenceImages":[],"operationId":"owned-op"})).unwrap();
    assert_eq!(params.session_id.as_deref(), Some("owned-session"));
    assert!(serde_json::from_value::<WorkBuddyImageParams>(
        json!({"prompt":"A blue square","callback":"ignored"})
    )
    .is_err());
    let video:WorkBuddyVideoParams=serde_json::from_value(json!({"prompt":"A short scene","sessionId":"owned-session","seconds":5,"referenceImages":[]})).unwrap();
    assert_eq!(video.seconds, Some(5));
}

#[test]
fn video_tasks_preserve_id_type_and_poll_bounded_pending_states() {
    for id in [json!("task-1"), json!(42)] {
        assert_eq!(video_task_id(&json!({"data":{"id":id}})).unwrap(), id);
    }
    for id in [
        json!(0),
        json!(null),
        json!({}),
        json!(" "),
        json!("x".repeat(513)),
    ] {
        assert!(video_task_id(&json!({"data":{"id":id}})).is_err());
    }
    assert!(!video_task_completed(&json!({"data":{"status":"rendering_frames"}})).unwrap());
    assert!(video_task_completed(&json!({"data":{"status":"completed"}})).unwrap());
    assert!(video_task_completed(&json!({"data":{"status":"failed"}})).is_err());
    assert!(video_task_completed(&json!({"data":{"status":"x".repeat(65)}})).is_err());
}

#[test]
fn enabled_media_uses_named_service_defaults_when_the_catalog_contains_only_chat() {
    let config = json!({"models":[{"id":"chat","supportsToolCall":true}],"productFeatures":{"ImageGen":true,"VideoGen":true}});
    assert_eq!(
        select(&config, "text-to-image", "ImageGen", None).unwrap()["id"],
        "gpt-image-2.5-sunburst"
    );
    assert_eq!(
        select(&config, "image-to-image", "ImageEdit", None).unwrap()["id"],
        "gpt-image-2.5-sunburst"
    );
    let video = select(&config, "text-to-video", "VideoGen", None).unwrap();
    assert_eq!(video["id"], "seedance-2.5");
    assert_eq!(video["videoGeneration"]["maxSeconds"], 30);
    let disabled = json!({"models":[{"id":"disabled","tags":["text-to-image"],"disabled":true}],"productFeatures":{"ImageGen":true}});
    assert!(select(&disabled, "text-to-image", "ImageGen", None).is_err());
    assert!(select(
        &json!({"models":[],"productFeatures":{"ImageGen":false}}),
        "text-to-image",
        "ImageGen",
        None
    )
    .is_err());
}
