use super::*;
fn args(step: &ImageStep) -> Value {
    let mut args = json!({"description":step.prompt,"filePath":step.filename});
    if let Some(ratio) = &step.ratio {
        args["aspectRatio"] = json!(ratio);
    }
    if !step.reference_order.is_empty() {
        args["referenceImagePaths"] = json!(step.reference_order);
    }
    args
}
fn started(step: &ImageStep) -> Value {
    json!({"interactionUpdate":{"toolCallStarted":{"callId":step.native_id,"modelCallId":"fixture","toolCall":{"toolCallId":step.native_id,"generateImageToolCall":{"args":args(step)}}}}})
}
fn write(step: &ImageStep) -> Value {
    json!({"execServerMessage":{"writeArgs":{"path":step.folder.join("assets").join(&step.filename),"fileBytes":STANDARD.encode(b"\x89PNG\r\n\x1a\nfixture-image")}}})
}
fn reference() -> Value {
    json!({"data":STANDARD.encode(b"\x89PNG\r\n\x1a\nreference-image"),"mimeType":"image/png"})
}

#[test]
fn short_and_non_whitespace_descriptions_keep_original_visual_content_in_explicit_generation_request(
) {
    for prompt in ["Square", "蓝色圆形，白色背景。", "a stop sign"] {
        let step = ImageStep::new(prompt, None, &[]).unwrap();
        assert!(step.prompt.ends_with(prompt));
        assert!(step.prompt.split_whitespace().count() > 20);
        let key = step.history.state["rootPromptMessagesJson"][1]
            .as_str()
            .unwrap();
        let user: Value = serde_json::from_slice(step.history.blobs.get(key).unwrap()).unwrap();
        assert_eq!(user["content"], prompt);
    }
}

#[test]
fn image_seed_binds_agent_mode_in_state_and_actual_user_protobuf_without_caller_environment() {
    let step = ImageStep::new("caller image prompt", Some("1:1"), &[reference()]).unwrap();
    assert_eq!(step.history.state["mode"], 1);
    let turn = step
        .history
        .blobs
        .get(step.history.state["turns"][0].as_str().unwrap())
        .unwrap();
    assert_eq!(&turn[..4], &[10, 34, 10, 32]);
    let user = step
        .history
        .blobs
        .get(&STANDARD.encode(&turn[4..]))
        .unwrap();
    assert_eq!(&user[user.len() - 2..], &[32, 1]);
    assert!(!step.folder.exists());
    assert_eq!(step.context["tools"], json!([]));
    assert_eq!(step.reference_order.len(), 1);
    assert!(step.reference_order[0].ends_with("reference-0.png"));
}

#[test]
fn image_references_use_only_supplied_memory_and_all_must_be_read_before_held_write() {
    let mut step = ImageStep::new("prompt", Some("1:1"), &[reference()]).unwrap();
    step.receive(&started(&step)).unwrap();
    assert!(step.receive(&write(&step)).is_err());
    let path = step.reference_order[0].clone();
    let read = json!({"execServerMessage":{"readArgs":{"path":path,"toolCallId":step.native_id}}});
    let replies = step.receive(&read).unwrap();
    assert_eq!(replies[0]["execClientMessage"]["id"], 0);
    assert_eq!(
        replies[0]["execClientMessage"]["readResult"]["success"]["data"],
        reference()["data"]
    );
    assert!(step.receive(&read).is_err());
    assert!(step.receive(&write(&step)).unwrap().is_empty());
    let result = step.take_result().unwrap();
    assert_eq!(result["mimeType"], "image/png");
    assert!(!step.folder.exists());
}

#[test]
fn image_rejects_unowned_reads_writes_changed_intents_and_model_continuation() {
    for change in ["foreign-path", "text", "extra", "bytes", "before-start"] {
        let mut step = ImageStep::new("prompt", None, &[]).unwrap();
        if change != "before-start" {
            step.receive(&started(&step)).unwrap();
        }
        let mut packet = write(&step);
        match change {
            "foreign-path" => {
                packet["execServerMessage"]["writeArgs"]["path"] = json!("/private/other.png")
            }
            "text" => packet["execServerMessage"]["writeArgs"]["fileText"] = json!("unexpected"),
            "extra" => {
                packet["execServerMessage"]["writeArgs"]["returnFileContentAfterWrite"] =
                    json!(true)
            }
            "bytes" => {
                packet["execServerMessage"]["writeArgs"]["fileBytes"] =
                    json!(STANDARD.encode(b"not-an-image"))
            }
            _ => {}
        }
        assert!(step.receive(&packet).is_err());
        assert!(step.take_result().is_none());
    }
    for packet in [
        json!({"execServerMessage":{"readArgs":{"path":"/private/file","toolCallId":"foreign"}}}),
        json!({"interactionUpdate":{"thinkingDelta":{"text":"model"}}}),
        json!({"interactionUpdate":{"stepStarted":{}}}),
        json!({"conversationCheckpointUpdate":{}}),
        json!({"interactionUpdate":{"heartbeat":{}},"unknownRoot":{}}),
    ] {
        let mut step = ImageStep::new("prompt", None, &[]).unwrap();
        assert!(step.receive(&packet).is_err());
    }
    // Retyped or unknown timing telemetry is discarded with the packet
    // sidecar instead of aborting the image step.
    for packet in [
        json!({"interactionUpdate":{"heartbeat":{}},"ttftBreakdown":{"clientClock":"true"}}),
        json!({"interactionUpdate":{"heartbeat":{}},"ttftBreakdown":{"unknown":1}}),
    ] {
        let mut step = ImageStep::new("prompt", None, &[]).unwrap();
        assert!(step.receive(&packet).is_ok());
    }
    let mut step = ImageStep::new("prompt", None, &[]).unwrap();
    let mut packet = started(&step);
    packet["interactionUpdate"]["toolCallStarted"]["toolCall"]["generateImageToolCall"]["args"]
        ["description"] = json!("different");
    assert!(step.receive(&packet).is_err());
}

#[tokio::test]
async fn image_http2_serves_reference_bytes_then_closes_unacknowledged_write_before_return() {
    let _guard = crate::app_server::cursor::test_lock().lock().await;
    let root = tempfile::tempdir().unwrap();
    crate::app_server::cursor::set_config_root(root.path());
    let fixture = super::super::tests::fixture(false, true).await;
    crate::app_server::cursor::adopt_credential(
        json!({"accessToken":"fixture","clientIdentity":"fixture","inferenceUrl":fixture.origin}),
        None,
    );
    let (credential, generation) = credential::snapshot().unwrap();
    let result=tokio::time::timeout(std::time::Duration::from_secs(5),generate(&credential,generation,&json!({"legacySlug":"fixture-high","requestedModel":{"modelId":"fixture","maxMode":false,"parameters":[]}}),"prompt",Some("1:1"),&[reference()])).await.unwrap().unwrap();
    assert_eq!(result["mimeType"], "image/png");
    tokio::time::timeout(std::time::Duration::from_secs(2), fixture.closed.notified())
        .await
        .unwrap();
}
