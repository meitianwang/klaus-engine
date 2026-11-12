//! Defect: compliant-resolution but byte-heavy images and cumulative historical
//! media reached the request without independent image and aggregate budgets;
//! rejected-media HTTP400 failures also ended without request-view remediation.
//! Root cause: dimensions-only admission and no normalized media policy or
//! coordinate-aware repair at the provider request boundary.
//! Work item: 2026-10-10 image and media request budgets; public provider surface.

use std::collections::HashMap;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use futures::StreamExt;
use image::GenericImageView as _;
use klaus_agent_core::context::images::{
    gate_image_block, gate_image_block_with_limits, image_dimensions, GatedImage, ImageDimensions,
    ImageLimits, IMAGE_BEST_EFFORT_RAW_BYTES, IMAGE_MAX_BASE64_CHARS, IMAGE_TARGET_RAW_BYTES,
};
use klaus_agent_core::context::media_budget::{
    DIRECT_MEDIA_BASE64_BYTES, MEDIA_REMOVED_MARKER, OTHER_MEDIA_BASE64_BYTES,
};
use klaus_agent_core::provider::anthropic::{AnthropicProvider, AnthropicProviderOptions};
use klaus_agent_core::provider::retry::RetryOptions;
use klaus_agent_core::schemas::content::{
    ContentBlock, DocumentBlock, ImageBlock, OutputBlock, TextBlock, ToolResultBlock,
};
use klaus_agent_core::schemas::message::{
    AssistantMessage, Message, ToolResultMessage, UserMessage,
};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::ProviderErrorKind;
use klaus_agent_core::types::provider::{
    ModelCapabilities, ModelProvider, ModelRequest, ModelStreamEvent,
};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const MODEL: &str = "media-budget-fixture";
const WAIT: Duration = Duration::from_secs(15);

fn text(text: &str) -> TextBlock {
    TextBlock {
        text: text.into(),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    }
}

fn image(id: usize) -> ImageBlock {
    ImageBlock {
        mime_type: "image/png".into(),
        base64: format!("{id:04}").into(),
        detail: None,

        source_path: None,
    }
}

fn user(content: Vec<ContentBlock>) -> Message {
    Message::User(UserMessage {
        uuid: "user".into(),
        question_answer: None,
        content,
        is_meta: None,
    })
}

fn assistant() -> Message {
    Message::Assistant(AssistantMessage {
        uuid: "assistant".into(),
        model: MODEL.into(),
        content: vec![ContentBlock::Text(text("Previous turn completed."))],
        usage: None,
    })
}

fn request(messages: Vec<Message>) -> ModelRequest {
    ModelRequest {
        supports_image_detail_original: false,
        model_verbosity: None,
        reasoning_summary: Default::default(),
        model: MODEL.into(),
        image_limits: None,
        max_media_blocks: None,
        system: vec![],
        messages,
        tools: vec![],
        native_deferred_tools: None,
        deferred_tools: Vec::new(),
        tool_choice: None,
        max_output_tokens: 64,
        task_budget: None,
        output_schema: None,
        web_search: None,
        thinking: None,
        effort: None,
        skip_cache_write: None,
        cache_ttl: None,
        cache_domain: None,
        cache_key: None,
        context_management: None,
        context_hints: None,
        thinking_context: None,
    }
}

async fn capture_server() -> (String, tokio::task::JoinHandle<Value>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        tokio::time::timeout(WAIT, async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut received = Vec::new();
            let mut buffer = [0; 8192];
            let body = loop {
                let count = stream.read(&mut buffer).await.unwrap();
                assert!(count > 0, "request body must arrive before the peer closes");
                received.extend_from_slice(&buffer[..count]);
                let Some(end) = received.windows(4).position(|part| part == b"\r\n\r\n") else { continue; };
                let headers = std::str::from_utf8(&received[..end]).unwrap();
                assert!(headers.starts_with("POST /v1/messages HTTP/1.1"));
                let length: usize = headers.lines().filter_map(|line| line.split_once(':'))
                    .find(|(key, _)| key.eq_ignore_ascii_case("content-length"))
                    .expect("the actual provider must send a serialized body").1.trim().parse().unwrap();
                if received.len() >= end + 4 + length {
                    break serde_json::from_slice::<Value>(&received[end + 4..end + 4 + length]).unwrap();
                }
            };
            let events = concat!(
                "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"fixture\",\"model\":\"media-budget-fixture\",\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\n",
                "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\n",
                "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
            );
            let response = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", events.len(), events);
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.shutdown().await.unwrap();
            body
        }).await.expect("local provider fixture must finish promptly")
    });
    (base, task)
}

struct FixtureReply {
    status: u16,
    error: Option<String>,
    code: Option<String>,
}

impl FixtureReply {
    fn reject(message: impl Into<String>) -> Self {
        Self {
            status: 400,
            error: Some(message.into()),
            code: None,
        }
    }

    fn accept() -> Self {
        Self {
            status: 200,
            error: None,
            code: None,
        }
    }
}

async fn scripted_capture_server(
    replies: Vec<FixtureReply>,
) -> (String, tokio::task::JoinHandle<Vec<Value>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        tokio::time::timeout(WAIT, async move {
            let mut requests = Vec::new();
            for reply in replies {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut received = Vec::new();
                let mut buffer = [0; 8192];
                let body = loop {
                    let count = stream.read(&mut buffer).await.unwrap();
                    assert!(count > 0, "request must contain its complete body");
                    received.extend_from_slice(&buffer[..count]);
                    let Some(end) = received.windows(4).position(|bytes| bytes == b"\r\n\r\n") else { continue; };
                    let headers = std::str::from_utf8(&received[..end]).unwrap();
                    assert!(headers.starts_with("POST /v1/messages HTTP/1.1"));
                    let length: usize = headers.lines().filter_map(|line| line.split_once(':'))
                        .find(|(key, _)| key.eq_ignore_ascii_case("content-length"))
                        .unwrap().1.trim().parse().unwrap();
                    if received.len() >= end + 4 + length {
                        break serde_json::from_slice::<Value>(&received[end + 4..end + 4 + length]).unwrap();
                    }
                };
                requests.push(body);
                let (content_type, payload) = if let Some(message) = reply.error {
                    ("application/json", serde_json::json!({"type":"error", "error": {
                        "type":"invalid_request_error", "message":message, "code":reply.code,
                    }}).to_string())
                } else {
                    ("text/event-stream", concat!(
                        "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"fixture\",\"model\":\"media-budget-fixture\",\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\n",
                        "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\n",
                        "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
                    ).to_owned())
                };
                let response = format!("HTTP/1.1 {} Fixture\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    reply.status, content_type, payload.len(), payload);
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.shutdown().await.unwrap();
            }
            assert!(tokio::time::timeout(Duration::from_millis(100), listener.accept()).await.is_err(),
                "no-change, duplicate, or exhausted remediation must not resubmit the body");
            requests
        }).await.expect("scripted adapter fixture must finish promptly")
    });
    (base, task)
}

fn tool_carrier(id: &str, images: &[usize], extra: Option<Vec<ContentBlock>>) -> Message {
    let mut content = vec![OutputBlock::Text(text("Keep tool text."))];
    content.extend(images.iter().map(|id| OutputBlock::Image(image(*id))));
    Message::ToolResult(ToolResultMessage {
        uuid: id.into(),
        tool_use_id: id.into(),
        content,
        is_error: false,
        tool_kind: None,
        native_tool_output: None,
        output: None,
        extra_user_blocks: extra,
    })
}

fn document(id: usize) -> ContentBlock {
    ContentBlock::Document(DocumentBlock {
        mime_type: "application/pdf".into(),
        base64: format!("{id:04}").into(),
        filename: Some(format!("fixture-{id}.pdf").into()),
    })
}

fn assert_no_generic_retry(events: &[ModelStreamEvent]) {
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::RetryScheduled { .. })),
        "media repairs must not consume generic retry attempts: {events:?}"
    );
}

#[tokio::test]
async fn public_http400_repairs_the_normalized_selected_tool_carrier_for_all_path_forms() {
    for path in [
        "messages.0.content.1.content.1.image",
        "messages[0].content[1].content[1].image",
        "messages.0.content.1.tool_result.content.1.image",
        "messages[0].content[1].tool_result.content[1].image",
    ] {
        let (base, server) = scripted_capture_server(vec![
            FixtureReply::reject(format!("{path}: Could not process image")),
            FixtureReply::accept(),
        ])
        .await;
        let provider = AnthropicProvider::new(options(base));
        let req = request(vec![
            tool_carrier("first-tool", &[1], Some(vec![document(4)])),
            tool_carrier(
                "selected-tool",
                &[2, 3],
                Some(vec![ContentBlock::Image(image(5))]),
            ),
        ]);
        let original = req.messages.clone();
        let events = send(&provider, req.clone()).await;
        assert_completed(&events);
        assert_no_generic_retry(&events);
        let bodies = server.await.unwrap();
        assert_eq!(bodies.len(), 2);
        assert_eq!(
            bodies[0]["messages"].as_array().unwrap().len(),
            1,
            "two persisted tool messages must be coalesced before coordinate remediation"
        );
        let content = bodies[1]["messages"][0]["content"].as_array().unwrap();
        assert_eq!(content[0]["tool_use_id"], "first-tool");
        assert_eq!(content[0]["content"][1]["type"], "image");
        assert_eq!(content[1]["tool_use_id"], "selected-tool");
        assert_eq!(content[1]["content"][0]["text"], "Keep tool text.");
        assert_eq!(content[1]["content"][1]["type"], "text");
        assert_eq!(content[1]["content"][2]["type"], "text");
        assert_eq!(content[2]["type"], "document");
        assert_eq!(content[3]["type"], "image");
        assert_eq!(bodies[0]["model"], bodies[1]["model"]);
        assert_eq!(req.messages, original);
    }
}

#[tokio::test]
async fn public_http400_pdf_path_replaces_only_the_selected_top_level_document() {
    let (base, server) = scripted_capture_server(vec![
        FixtureReply::reject("messages[0].content[2].pdf.source: The PDF specified was not valid"),
        FixtureReply::accept(),
    ])
    .await;
    let provider = AnthropicProvider::new(options(base));
    let req = request(vec![
        tool_carrier("first-tool", &[1], Some(vec![document(4)])),
        tool_carrier("second-tool", &[2], Some(vec![document(5)])),
    ]);
    let original = req.messages.clone();
    let events = send(&provider, req.clone()).await;
    assert_completed(&events);
    let bodies = server.await.unwrap();
    let content = bodies[1]["messages"][0]["content"].as_array().unwrap();
    assert_eq!(content[0]["tool_use_id"], "first-tool");
    assert_eq!(content[1]["tool_use_id"], "second-tool");
    assert_eq!(content[2]["type"], "text");
    assert!(content[2]["text"]
        .as_str()
        .unwrap()
        .contains("Document removed"));
    assert_eq!(content[3]["source"]["data"], "0005");
    assert_eq!(req.messages, original);
}

#[tokio::test]
async fn public_http400_untargeted_repair_is_limited_to_three_latest_carriers() {
    let (base, server) = scripted_capture_server(
        (0..4)
            .map(|_| FixtureReply::reject("Could not process image"))
            .collect(),
    )
    .await;
    let provider = AnthropicProvider::new(options(base));
    let mut messages = Vec::new();
    for id in 0..4 {
        if id > 0 {
            messages.push(assistant());
        }
        messages.push(user(vec![
            ContentBlock::Image(image(id)),
            document(id + 10),
        ]));
    }
    let req = request(messages);
    let original = req.messages.clone();
    let events = send(&provider, req.clone()).await;
    assert_no_generic_retry(&events);
    assert!(events
        .iter()
        .any(|event| matches!(event, ModelStreamEvent::Error { error }
        if error.status == Some(400) && error.message.contains("Could not process image"))));
    let bodies = server.await.unwrap();
    assert_eq!(
        bodies.len(),
        4,
        "one initial HTTP request plus exactly three untargeted repairs"
    );
    for (attempt, body) in bodies.iter().enumerate() {
        let carriers: Vec<_> = body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|message| message["role"] == "user")
            .collect();
        for (index, carrier) in carriers.iter().enumerate() {
            assert_eq!(
                carrier["content"][0]["type"],
                if index >= 4 - attempt {
                    "text"
                } else {
                    "image"
                }
            );
            assert_eq!(
                carrier["content"][1]["type"], "document",
                "the other kind must remain"
            );
        }
    }
    assert_eq!(req.messages, original);
}

#[tokio::test]
async fn public_http400_unique_targeted_coordinates_are_not_counted_as_untargeted_repairs() {
    let mut replies: Vec<_> = (0..4)
        .map(|index| {
            FixtureReply::reject(format!(
                "messages.0.content.{index}.image: Could not process image"
            ))
        })
        .collect();
    replies.push(FixtureReply::accept());
    let (base, server) = scripted_capture_server(replies).await;
    let provider = AnthropicProvider::new(options(base));
    let events = send(
        &provider,
        request(vec![user(
            (0..4).map(|id| ContentBlock::Image(image(id))).collect(),
        )]),
    )
    .await;
    assert_completed(&events);
    assert_no_generic_retry(&events);
    let bodies = server.await.unwrap();
    assert_eq!(
        bodies.len(),
        5,
        "four distinct targeted changes must be admitted"
    );
    for (attempt, body) in bodies.iter().enumerate() {
        for (index, block) in body["messages"][0]["content"]
            .as_array()
            .unwrap()
            .iter()
            .enumerate()
        {
            assert_eq!(
                block["type"],
                if index < attempt { "text" } else { "image" }
            );
        }
    }
    assert_eq!(
        bodies[4]["messages"][0]["content"][3]["cache_control"]["type"],
        "ephemeral"
    );
}

#[tokio::test]
async fn public_http400_duplicate_target_falls_back_then_stops_without_unchanged_resends() {
    let message = "messages.0.content.0.image: Could not process image";
    let (base, server) = scripted_capture_server(vec![
        FixtureReply::reject(message),
        FixtureReply::reject(message),
        FixtureReply::reject(message),
    ])
    .await;
    let provider = AnthropicProvider::new(options(base));
    let events = send(
        &provider,
        request(vec![user(vec![
            ContentBlock::Image(image(1)),
            ContentBlock::Image(image(2)),
        ])]),
    )
    .await;
    assert_no_generic_retry(&events);
    assert!(events.iter().any(
        |event| matches!(event, ModelStreamEvent::Error { error } if error.status == Some(400))
    ));
    let bodies = server.await.unwrap();
    assert_eq!(bodies.len(), 3);
    assert!(bodies.windows(2).all(|pair| pair[0] != pair[1]));
    assert_eq!(bodies[1]["messages"][0]["content"][0]["type"], "text");
    assert_eq!(bodies[1]["messages"][0]["content"][1]["type"], "image");
    assert_eq!(bodies[2]["messages"][0]["content"][1]["type"], "text");
    assert!(events.iter().any(
        |event| matches!(event, ModelStreamEvent::Error { error } if error.media_repair_complete)
    ));
}

#[tokio::test]
async fn public_http400_no_change_ordinary_errors_and_413_do_not_resubmit() {
    for (status, message, code) in [
        (
            400,
            "messages.0.content.0.image: Could not process image",
            None,
        ),
        (400, "Invalid tool choice", None),
        (400, "Input is too long.", None),
        (
            413,
            "messages.0.content.1.image: Could not process image",
            None,
        ),
    ] {
        let (base, server) = scripted_capture_server(vec![FixtureReply {
            status,
            error: Some(message.into()),
            code: code.map(str::to_owned),
        }])
        .await;
        let mut opts = options(base);
        opts.retry = Some(RetryOptions {
            max_attempts: Some(5),
            congestion_hold_ms: Some(0),
            ..Default::default()
        });
        let provider = AnthropicProvider::new(opts);
        let events = send(
            &provider,
            request(vec![user(vec![
                ContentBlock::Text(text("Keep text.")),
                ContentBlock::Text(text("No repairable image in this carrier.")),
            ])]),
        )
        .await;
        assert_no_generic_retry(&events);
        assert!(events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::Error { error }
            if error.status == Some(status) && error.message.contains(message))));
        let bodies = server.await.unwrap();
        assert_eq!(bodies.len(), 1);
        assert_eq!(bodies[0]["messages"][0]["content"][1]["type"], "text");
    }
}

#[test]
fn explicit_request_policy_outranks_native_defaults_and_unknown_counts_remain_conservative() {
    let provider = AnthropicProvider::new(AnthropicProviderOptions {
        capabilities: Some(ModelCapabilities {
            context_window: Some(1_000_000),
            one_million_context: true,
            ..Default::default()
        }),
        protected_first_party_contract: true,
        max_media_blocks: Some(600),
        image_limits: Some(ImageLimits::default().with_base64_cap(20)),
        ..Default::default()
    });
    let image_snapshot = ImageLimits {
        max_width: 320,
        max_height: 240,
        max_base64_chars: 8,
        target_raw_bytes: 6,
    };
    let (images, media) =
        provider.resolve_request_media_policy(&MODEL.into(), Some(image_snapshot), Some(100));
    assert_eq!(images, image_snapshot);
    assert_eq!(media.max_blocks, 100);
    let unknown = AnthropicProvider::new(AnthropicProviderOptions {
        capabilities: Some(ModelCapabilities {
            context_window: Some(1_000_000),
            one_million_context: true,
            ..Default::default()
        }),
        protected_first_party_contract: true,
        ..Default::default()
    });
    assert_eq!(
        unknown
            .media_budget_for(&"model-with-1m-in-its-name".into())
            .max_blocks,
        100
    );
}

fn options(base: String) -> AnthropicProviderOptions {
    AnthropicProviderOptions {
        api_key: Some("fixture-key".into()),
        base_url: Some(base),
        retry: Some(RetryOptions {
            max_attempts: Some(1),
            congestion_hold_ms: Some(0),
            ..Default::default()
        }),
        ..Default::default()
    }
}

async fn send(provider: &AnthropicProvider, req: ModelRequest) -> Vec<ModelStreamEvent> {
    tokio::time::timeout(WAIT, provider.stream(req, AbortSignal::new()).collect())
        .await
        .expect("public stream must finish promptly")
}

fn assert_completed(events: &[ModelStreamEvent]) {
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::Error { .. })),
        "{events:?}"
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::MessageStop { .. })),
        "{events:?}"
    );
}

fn count_media(messages: &[Value]) -> usize {
    let mut count = 0;
    for message in messages {
        if message["role"] != "user" {
            continue;
        }
        for block in message["content"].as_array().unwrap() {
            if matches!(block["type"].as_str(), Some("image" | "document")) {
                count += 1;
            }
            if block["type"] == "tool_result" {
                count += block["content"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|media| matches!(media["type"].as_str(), Some("image" | "document")))
                    .count();
            }
        }
    }
    count
}

#[tokio::test]
async fn public_send_sheds_nested_media_before_top_level_and_does_not_rewrite_history() {
    let (base, server) = capture_server().await;
    let provider = AnthropicProvider::new(options(base));
    let req = request(vec![user(vec![
        ContentBlock::Image(image(100)),
        ContentBlock::ToolResult(ToolResultBlock {
            tool_use_id: "tool".into(),
            is_error: None,
            content: (0..100).map(|id| OutputBlock::Image(image(id))).collect(),
        }),
    ])]);
    let original = req.messages.clone();
    assert_completed(&send(&provider, req.clone()).await);
    let body = server.await.unwrap();
    let messages = body["messages"].as_array().unwrap();
    assert_eq!(
        count_media(messages),
        80,
        "count excess plus twenty blocks must be removed"
    );
    let content = messages[0]["content"].as_array().unwrap();
    assert_eq!(content[0]["source"]["data"], "0100");
    assert_eq!(content[1]["content"][0]["source"]["data"], "0021");
    assert_eq!(content[1]["content"].as_array().unwrap().len(), 79);
    assert_eq!(req.messages, original);
    assert_eq!(
        content.last().unwrap()["cache_control"]["type"],
        "ephemeral"
    );
}

#[tokio::test]
async fn public_send_uses_explicit_media_counts_not_configured_token_windows() {
    for (total, window, media_cap, expected) in [
        (100, 200_000, None, 100),
        (101, 200_000, None, 80),
        (101, 1_000_000, None, 80),
        (101, 200_000, Some(600), 101),
        (600, 200_000, Some(600), 600),
        (601, 1_000_000, Some(600), 580),
    ] {
        let (base, server) = capture_server().await;
        let mut opts = options(base);
        opts.max_media_blocks_by_model =
            media_cap.map(|cap| HashMap::from([(MODEL.to_owned(), cap)]));
        opts.capabilities_by_model = Some(HashMap::from([(
            MODEL.to_owned(),
            ModelCapabilities {
                context_window: Some(window),
                ..Default::default()
            },
        )]));
        let provider = AnthropicProvider::new(opts);
        let req = request(vec![user(
            (0..total)
                .map(|id| ContentBlock::Image(image(id)))
                .collect(),
        )]);
        assert_completed(&send(&provider, req).await);
        let body = server.await.unwrap();
        assert_eq!(count_media(body["messages"].as_array().unwrap()), expected);
    }
}

#[tokio::test]
async fn public_send_trims_byte_heavy_documents_oldest_first_and_caches_the_marker() {
    let (base, server) = capture_server().await;
    let provider = AnthropicProvider::new(options(base));
    let req = request(vec![
        user(vec![ContentBlock::Document(DocumentBlock {
            mime_type: "application/pdf".into(),
            base64: "A".repeat(OTHER_MEDIA_BASE64_BYTES + 4).into(),
            filename: Some("old.pdf".into()),
        })]),
        assistant(),
        user(vec![ContentBlock::Image(image(7))]),
    ]);
    let original = req.messages.clone();
    let mut req_for_send = req.clone();
    // The penultimate message is not markable media, so make the cleared oldest
    // message the final cache target in a separate empty-tail request below.
    assert_completed(&send(&provider, req_for_send.clone()).await);
    let body = server.await.unwrap();
    assert_eq!(
        body["messages"][0]["content"][0]["text"],
        MEDIA_REMOVED_MARKER
    );
    assert_eq!(body["messages"][2]["content"][0]["source"]["data"], "0007");
    assert_eq!(count_media(body["messages"].as_array().unwrap()), 1);
    assert_eq!(req.messages, original);

    let (base, server) = capture_server().await;
    let provider = AnthropicProvider::new(options(base));
    req_for_send.messages.truncate(1);
    assert_completed(&send(&provider, req_for_send).await);
    let body = server.await.unwrap();
    assert_eq!(
        body["messages"][0]["content"][0]["text"],
        MEDIA_REMOVED_MARKER
    );
    assert_eq!(
        body["messages"][0]["content"][0]["cache_control"]["type"],
        "ephemeral"
    );
}

#[tokio::test]
async fn public_send_checks_final_images_before_aggregate_count_shedding_or_http() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let mut opts = options(base);
    opts.image_limits_by_model = Some(HashMap::from([(
        MODEL.to_owned(),
        ImageLimits::default().with_base64_cap(4),
    )]));
    let provider = AnthropicProvider::new(opts);
    let mut nested: Vec<OutputBlock> = (0..101).map(|id| OutputBlock::Image(image(id))).collect();
    nested[0] = OutputBlock::Image(ImageBlock {
        base64: "AAAAAAAA".into(),
        ..image(0)
    });
    let req = request(vec![user(vec![ContentBlock::ToolResult(
        ToolResultBlock {
            tool_use_id: "tool".into(),
            content: nested,
            is_error: None,
        },
    )])]);
    let events = send(&provider, req).await;
    assert!(
        matches!(events.as_slice(), [ModelStreamEvent::Error { error }]
        if error.kind == ProviderErrorKind::InvalidRequest
            && error.message.contains("messages[0].content[0].content[0]")
            && error.message.contains("4 base64-byte limit"))
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err(),
        "validation must refuse before a transport attempt"
    );
}

#[test]
fn endpoint_identity_and_model_source_constraints_are_resolved_without_model_name_heuristics() {
    for (base, expected) in [
        (None, DIRECT_MEDIA_BASE64_BYTES),
        (Some("https://api.anthropic.com"), DIRECT_MEDIA_BASE64_BYTES),
        (
            Some("https://gateway.example.test"),
            OTHER_MEDIA_BASE64_BYTES,
        ),
    ] {
        let provider = AnthropicProvider::new(AnthropicProviderOptions {
            base_url: base.map(str::to_owned),
            protected_first_party_contract: base.is_none(),
            image_limits: Some(ImageLimits::default().with_base64_cap(100)),
            image_limits_by_model: Some(HashMap::from([(
                MODEL.to_owned(),
                ImageLimits::default().with_base64_cap(200),
            )])),
            ..Default::default()
        });
        assert_eq!(
            provider.media_budget_for(&MODEL.into()).max_base64_bytes,
            expected
        );
        assert_eq!(
            provider.image_limits_for(&MODEL.into()).max_base64_chars,
            200
        );
        assert_eq!(
            provider
                .image_limits_for(&"other-model".into())
                .max_base64_chars,
            100
        );
    }
}

fn noisy_image(width: u32, height: u32) -> image::RgbImage {
    let mut state = 0x9185_104bu32;
    image::RgbImage::from_fn(width, height, |_, _| {
        let mut pixel = [0; 3];
        for channel in &mut pixel {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            *channel = state as u8;
        }
        image::Rgb(pixel)
    })
}

fn encoded_block(bytes: &[u8], mime: &str) -> ImageBlock {
    ImageBlock {
        mime_type: mime.into(),
        base64: BASE64.encode(bytes).into(),
        detail: None,

        source_path: Some("/synthetic/source-image".into()),
    }
}

#[test]
fn dimension_compliant_high_entropy_png_is_processed_even_above_the_original_base64_cap() {
    let pixels = noisy_image(1200, 1200);
    let mut bytes = Vec::new();
    pixels
        .write_with_encoder(image::codecs::png::PngEncoder::new(&mut bytes))
        .unwrap();
    assert!(bytes.len() > IMAGE_TARGET_RAW_BYTES);
    let block = encoded_block(&bytes, "image/png");
    assert!(block.base64.len_utf16() > IMAGE_MAX_BASE64_CHARS);
    let GatedImage::Downscaled(prepared) = gate_image_block(&block) else {
        panic!("byte-heavy but dimension-compliant PNG must be processed, not rejected or kept");
    };
    let output = BASE64
        .decode(prepared.base64.try_to_utf8().unwrap())
        .unwrap();
    assert!(output.len() < bytes.len());
    assert!(prepared.base64.len_utf16() <= IMAGE_MAX_BASE64_CHARS);
    assert_eq!(
        image_dimensions(&prepared),
        Some(ImageDimensions {
            width: 1200,
            height: 1200
        })
    );
    assert_eq!(prepared.mime_type, "image/jpeg");
    assert!(prepared.source_path.is_none());
}

#[test]
fn dimension_compliant_high_entropy_jpeg_uses_the_quality_budget_without_resizing() {
    let pixels = noisy_image(1200, 800);
    let mut bytes = Vec::new();
    pixels
        .write_with_encoder(image::codecs::jpeg::JpegEncoder::new_with_quality(
            &mut bytes, 95,
        ))
        .unwrap();
    assert!(bytes.len() > IMAGE_BEST_EFFORT_RAW_BYTES);
    let block = encoded_block(&bytes, "image/jpeg");
    let GatedImage::Downscaled(prepared) = gate_image_block(&block) else {
        panic!("byte-heavy JPEG must be re-encoded");
    };
    let output = BASE64
        .decode(prepared.base64.try_to_utf8().unwrap())
        .unwrap();
    assert!(output.len() < bytes.len());
    assert!(prepared.base64.len_utf16() <= IMAGE_MAX_BASE64_CHARS);
    assert_eq!(
        image::load_from_memory(&output).unwrap().dimensions(),
        (1200, 800)
    );
}

#[test]
fn per_axis_limits_follow_encoded_dimensions_without_implicit_exif_rotation() {
    let mut bytes = Vec::new();
    image::RgbImage::from_pixel(128, 64, image::Rgb([20, 40, 60]))
        .write_with_encoder(image::codecs::jpeg::JpegEncoder::new_with_quality(
            &mut bytes, 90,
        ))
        .unwrap();
    let exif = b"Exif\0\0II\x2a\0\x08\0\0\0\x01\0\x12\x01\x03\0\x01\0\0\0\x06\0\0\0\0\0\0\0";
    let mut oriented = bytes[..2].to_vec();
    oriented.extend_from_slice(&[0xff, 0xe1]);
    oriented.extend_from_slice(&((exif.len() + 2) as u16).to_be_bytes());
    oriented.extend_from_slice(exif);
    oriented.extend_from_slice(&bytes[2..]);
    let block = encoded_block(&oriented, "image/jpeg");
    assert_eq!(
        image_dimensions(&block),
        Some(ImageDimensions {
            width: 128,
            height: 64
        })
    );
    let limits = ImageLimits {
        max_width: 32,
        max_height: 24,
        ..Default::default()
    };
    let GatedImage::Downscaled(prepared) = gate_image_block_with_limits(&block, limits) else {
        panic!("per-axis limit must resize the oriented source");
    };
    assert_eq!(
        image_dimensions(&prepared),
        Some(ImageDimensions {
            width: 32,
            height: 16
        })
    );
}
