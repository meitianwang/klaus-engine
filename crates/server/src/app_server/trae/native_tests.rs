use super::*;

#[test]
fn search_uses_native_request_fields_and_keeps_service_default_model() {
    let body = search_body(&json!({"query":"Rust documentation","maxResults":2})).unwrap();
    assert_eq!(body["user_input"], "Rust documentation");
    assert_eq!(body["top_k"], 2);
    assert_eq!(body["access_type"], 0);
    assert!(body.get("ModelName").is_none());
    assert_ne!(body["session_id"], body["conversation_id"]);
    assert!(search_body(&json!({"query":"","maxResults":2})).is_err());
    assert!(search_body(&json!({"query":"Rust","maxResults":21})).is_err());
}

#[test]
fn native_references_are_bounded_and_domain_filters_use_host_boundaries() {
    let value = json!({"references":[
        {"uri":"https://rust-lang.org/","title":"Rust","snippets":["One","Two"]},
        {"uri":"https://doc.rust-lang.org/book/","title":"Book","snippets":["Documentation"]},
        {"uri":"https://rust-lang.org.evil.example/","title":"Other","snippets":["Unrelated"]}
    ]});
    let all = search_results(&value, &json!({}), 1).unwrap();
    assert_eq!(all["results"].as_array().unwrap().len(), 1);
    assert_eq!(all["results"][0]["snippet"], "One\n\nTwo");
    let filtered = search_results(
        &value,
        &json!({"allowedDomains":["rust-lang.org"],"blockedDomains":["doc.rust-lang.org"]}),
        5,
    )
    .unwrap();
    assert_eq!(filtered["results"].as_array().unwrap().len(), 1);
    assert_eq!(filtered["results"][0]["url"], "https://rust-lang.org/");
    assert!(domains(
        &json!({"allowedDomains":["https://rust-lang.org"]}),
        "allowedDomains"
    )
    .is_err());
    assert!(public_url("http://127.0.0.1/internal").is_err());
    assert!(public_url("https://user:password@example.com/").is_err());
}

#[test]
fn service_refusals_preserve_codes_without_echoing_sensitive_messages() {
    assert_eq!(
        service_error(&json!({"error_code":4001,"error_msg":"private credential"}))
            .err()
            .as_deref(),
        Some("trae_request_rejected (service code 4001)")
    );
    assert_eq!(
        service_error(&json!({"success":false})).err().as_deref(),
        Some("trae_request_rejected")
    );
    assert!(service_error(&json!({"code":0,"results":[]})).is_ok());
}

#[tokio::test]
async fn cancellation_interrupts_a_pending_operation() {
    let signal = AbortSignal::new();
    let pending = async { std::future::pending::<Result<(), String>>().await };
    signal.cancel();
    assert_eq!(
        bounded(&signal, None, 30, pending).await.err().as_deref(),
        Some("trae_operation_cancelled")
    );
}

#[test]
fn media_stream_reassembles_split_utf8_and_requires_a_successful_terminal_event() {
    let stream = "event: heartbeat\ndata: {\"status\":\"processing\"}\n\nevent: result\ndata: {\"uri\":\"tos-cn-test/image\",\"label\":\"完整\"}\n\nevent: done\ndata: {\"finish_reason\":\"stop\"}\n\n";
    let mut parser = MediaStream::default();
    for byte in stream.as_bytes() {
        parser.push(&[*byte]).unwrap();
    }
    assert!(parser.done);
    assert_eq!(parser.results.len(), 1);
    assert_eq!(parser.results[0]["label"], "完整");
    let mut invalid = MediaStream::default();
    assert!(invalid
        .push(b"event: done\ndata: {\"finish_reason\":\"stop\"}\n\n")
        .is_err());
    let mut error = MediaStream::default();
    assert_eq!(
        error
            .push(b"event: error\ndata: {\"code\":3003,\"message\":\"private prompt\"}\n\n")
            .err()
            .as_deref(),
        Some("trae_service_unavailable (service code 3003)")
    );
}

#[test]
fn media_requests_use_verified_service_defaults_and_native_reference_fields() {
    let image = media_body(&json!({"prompt":"Blue circle"}), false).unwrap();
    assert_eq!(image["image_size"], "2048x2048");
    assert!(image.get("model").is_none());
    let edit = media_body(
        &json!({"prompt":"Red circle","imageUrls":["https://cdn.example/image.jpeg"]}),
        false,
    )
    .unwrap();
    assert_eq!(edit["image_urls"][0], "https://cdn.example/image.jpeg");
    let video = media_body(
        &json!({"prompt":"Moving circle","seconds":5,"size":"16:9"}),
        true,
    )
    .unwrap();
    assert!(video.get("resolution").is_none());
    assert_eq!(video["ratio"], "16:9");
    assert_eq!(video["duration"], 5);
    let jpeg = STANDARD.encode([255, 216, 255, 224]);
    let local_edit = media_body(&json!({"prompt":"Green circle","referenceImages":[{"dataBase64":jpeg,"mimeType":"image/jpeg"}]}), false).unwrap();
    assert!(local_edit["image_urls"][0]
        .as_str()
        .unwrap()
        .starts_with("data:image/jpeg;base64,"));
    assert!(media_body(&json!({"prompt":"Circle","referenceImages":[{"dataBase64":STANDARD.encode([1, 2, 3]),"mimeType":"image/jpeg"}]}), false).is_err());
    assert!(media_body(
        &json!({"prompt":"Circle","model":"unverified-model"}),
        false
    )
    .is_err());
    assert!(media_body(
        &json!({"prompt":"Circle","imageUrls":["http://127.0.0.1/image"]}),
        false
    )
    .is_err());
    assert!(media_body(&json!({"prompt":"Circle","seconds":100}), true).is_err());
}

#[test]
fn image_requests_adapt_small_dimensions_before_the_first_submission() {
    let square = media_body(&json!({"prompt":"Kitten","size":"1024x1024"}), false).unwrap();
    assert_eq!(square["image_size"], "2048x2048");
    for size in [
        "512x512", "768x1024", "1024x768", "1280x720", "720x1280", "1x1",
    ] {
        let request = media_body(&json!({"prompt":"Kitten","size":size}), false).unwrap();
        let dimensions = |value: &str| {
            let (width, height) = value.split_once('x').unwrap();
            (
                width.parse::<u64>().unwrap(),
                height.parse::<u64>().unwrap(),
            )
        };
        let (width, height) = dimensions(request["image_size"].as_str().unwrap());
        let (requested_width, requested_height) = dimensions(size);
        assert!(width * height >= 3_686_400, "{size}");
        assert!(
            width >= requested_width && height >= requested_height,
            "{size}"
        );
        let ratio_error =
            (width as f64 / height as f64 - requested_width as f64 / requested_height as f64).abs();
        assert!(ratio_error < 0.002, "{size}: {}", request["image_size"]);
    }
    let edit = media_body(
        &json!({"prompt":"Kitten","size":"1024x1024","imageUrls":["https://cdn.example/image.jpeg"]}),
        false,
    ).unwrap();
    assert_eq!(edit["image_size"], "2048x2048");
    assert_eq!(edit["image_urls"][0], "https://cdn.example/image.jpeg");
}

#[test]
fn image_size_adaptation_preserves_sufficient_dimensions_and_native_defaults() {
    for size in [
        "1920x1920",
        "2048x2048",
        "2560x1440",
        "1440x2560",
        "4096x4096",
        "2K",
    ] {
        let request = media_body(&json!({"prompt":"Kitten","size":size}), false).unwrap();
        assert_eq!(request["image_size"], size);
    }
    assert_eq!(
        media_body(&json!({"prompt":"Kitten","size":"auto"}), false).unwrap()["image_size"],
        "2048x2048"
    );
    let alias = media_body(&json!({"prompt":"Kitten","image_size":"1024x1024"}), false).unwrap();
    assert_eq!(alias["image_size"], "2048x2048");
    let precedence = media_body(
        &json!({"prompt":"Kitten","image_size":"2560x1440","size":"1024x1024"}),
        false,
    )
    .unwrap();
    assert_eq!(precedence["image_size"], "2560x1440");
    for size in [
        "0x1024",
        "1024x0",
        "4294967296x1024",
        "1024x1024x2",
        "-1024x1024",
        "",
    ] {
        assert_eq!(
            media_body(&json!({"prompt":"Kitten","size":size}), false)
                .err()
                .as_deref(),
            Some("trae_image_size_invalid"),
            "{size}"
        );
    }
    assert!(media_body(&json!({"prompt":"Kitten","size":1024}), false).is_err());
}

#[test]
fn public_video_pixel_sizes_select_native_resolution_and_reduced_aspect_ratio() {
    for (size, resolution, ratio) in [
        ("1280x720", "720p", "16:9"),
        ("720x1280", "720p", "9:16"),
        ("640x480", "480p", "4:3"),
        ("1920x1080", "1080p", "16:9"),
        ("3840x2160", "4k", "16:9"),
        ("720x720", "720p", "1:1"),
    ] {
        let body = media_body(&json!({"prompt":"Moving circle","size":size}), true).unwrap();
        assert_eq!(body["resolution"], resolution);
        assert_eq!(body["ratio"], ratio);
        assert!(body.get("size").is_none());
    }
    let defaults = media_body(&json!({"prompt":"Moving circle"}), true).unwrap();
    assert!(defaults.get("resolution").is_none());
    assert!(defaults.get("ratio").is_none());
    let auto = media_body(&json!({"prompt":"Moving circle","size":"auto"}), true).unwrap();
    assert!(auto.get("resolution").is_none());
    assert!(auto.get("ratio").is_none());
    let ratio_only = media_body(&json!({"prompt":"Moving circle","size":"32:18"}), true).unwrap();
    assert_eq!(ratio_only["ratio"], "16:9");
    assert!(ratio_only.get("resolution").is_none());
}

#[test]
fn explicit_video_sizes_reject_unsupported_or_conflicting_dimensions() {
    for size in [
        "2560x1440",
        "1024x1024",
        "0x720",
        "1280x0",
        "1280x720x2",
        "1920X1080",
        "",
        "-720x1280",
    ] {
        assert!(
            media_body(&json!({"prompt":"Moving circle","size":size}), true).is_err(),
            "{size}"
        );
    }
    assert!(media_body(&json!({"prompt":"Moving circle","size":1280}), true).is_err());
    assert!(media_body(
        &json!({"prompt":"Moving circle","size":"1280x720","resolution":"1080p"}),
        true
    )
    .is_err());
    assert!(media_body(
        &json!({"prompt":"Moving circle","size":"1280x720","ratio":"4:3"}),
        true
    )
    .is_err());
    let matching = media_body(
        &json!({"prompt":"Moving circle","size":"1280x720","resolution":"720p","ratio":"32:18"}),
        true,
    )
    .unwrap();
    assert_eq!(matching["ratio"], "16:9");
    assert_eq!(matching["resolution"], "720p");
}

#[test]
fn image_results_preserve_each_output_without_repeating_the_single_image_payload() {
    let first = json!({"imageBase64":"first-image","mimeType":"image/png"});
    let second = json!({"imageBase64":"second-image","mimeType":"image/jpeg"});
    assert_eq!(media_result(vec![first.clone()], false).unwrap(), first);
    let multiple = media_result(vec![first.clone(), second.clone()], false).unwrap();
    assert_eq!(multiple, json!({"images":[first,second]}));
    assert!(multiple.get("imageBase64").is_none());
    assert!(multiple.get("outputs").is_none());
    assert!(media_result(Vec::new(), false).is_err());
}
