//! Defect: aggregate image and captured request policies lacked real HTTP-body
//! acceptance coverage, so helper-only fixtures could miss adapter binding.
//! Root cause: normalized request assembly and public streaming were not tested
//! together with valid image bytes and a refreshed capability catalog.
//! Work item: 2026-10-10 media budgets; two public-stream acceptance fixtures.
//!
//! Mounted inside the provider under cfg(test) solely to replace its physical
//! destination after official-endpoint classification. No production transport,
//! authentication, beta policy, or limit declaration is replaced.

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use futures::StreamExt;
use image::GenericImageView as _;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::{AnthropicProvider, AnthropicProviderOptions};
use crate::context::images::{
    gate_image_block_with_limits, image_dimensions, GatedImage, ImageDimensions,
    ImageLimits as ResolvedImageLimits, IMAGE_MAX_BASE64_CHARS,
};
use crate::context::media_budget::{
    DIRECT_MEDIA_BASE64_BYTES, MEDIA_BASE64_HEADROOM, OTHER_MEDIA_BASE64_BYTES,
};
use crate::provider::model_catalog::{
    capture_model_provider, with_model_catalog, CatalogRefresh, ModelCatalog,
};
use crate::provider::retry::RetryOptions;
use crate::runtime_values::RuntimeString;
use crate::schemas::content::{ContentBlock, ImageBlock, OutputBlock, TextBlock, ToolResultBlock};
use crate::schemas::message::{AssistantMessage, Message, UserMessage};
use crate::types::abort::AbortSignal;
use crate::types::errors::ProviderErrorKind;
use crate::types::provider::{
    ImageLimits as DeclaredImageLimits, ModelCapabilities, ModelProvider, ModelRequest,
    ModelStream, ModelStreamEvent, ProviderCapabilities, ProviderInfo,
};

const MAIN: &str = "image-policy-main";
const SUMMARY: &str = "image-policy-summary";
const WAIT: Duration = Duration::from_secs(15);
const HISTORY_RAW_BYTES: usize = 483_840;
const HISTORY_BASE64_BYTES: usize = HISTORY_RAW_BYTES / 3 * 4;
const PNG_ID_PREFIX: &[u8] = b"fixture-id\0";

fn text(value: &str) -> TextBlock {
    TextBlock {
        text: value.into(),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    }
}

fn user(id: &str, content: Vec<ContentBlock>) -> Message {
    Message::User(UserMessage {
        uuid: id.into(),
        question_answer: None,
        content,
        is_meta: None,
    })
}

fn request(model: &str, messages: Vec<Message>) -> ModelRequest {
    ModelRequest {
        supports_image_detail_original: false,
        model_verbosity: None,
        reasoning_summary: Default::default(),
        model: model.into(),
        image_limits: None,
        max_media_blocks: None,
        system: vec![],
        messages,
        tools: vec![],
        tool_choice: None,
        max_output_tokens: 64,
        task_budget: None,
        output_schema: None,
        web_search: None,
        thinking: None,
        effort: None,
        skip_cache_write: None,
        cache_ttl: None,
        cache_key: None,
        context_management: None,
        context_hints: None,
        thinking_context: None,
    }
}

fn options(base: String) -> AnthropicProviderOptions {
    AnthropicProviderOptions {
        api_key: Some("fixture-key".into()),
        base_url: Some(base),
        default_model: Some(MAIN.into()),
        retry: Some(RetryOptions {
            max_attempts: Some(1),
            congestion_hold_ms: Some(0),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn local_provider(opts: AnthropicProviderOptions) -> AnthropicProvider {
    let mut provider = AnthropicProvider::new(opts);
    let endpoint = url::Url::parse(&provider.messages_url).unwrap();
    assert_eq!(endpoint.host_str(), Some("127.0.0.1"));
    assert!(!provider.direct_media_endpoint);
    assert_eq!(
        provider.media_budget_for(&MAIN.into()).max_base64_bytes,
        OTHER_MEDIA_BASE64_BYTES
    );
    provider.client = reqwest::Client::builder().no_proxy().build().unwrap();
    provider
}

fn official_provider_routed_locally(base: &str) -> AnthropicProvider {
    let mut opts = options("https://api.anthropic.com".into());
    opts.protected_first_party_contract = true;
    opts.capabilities = Some(ModelCapabilities {
        context_window: Some(1_000_000),
        one_million_context: true,
        ..Default::default()
    });
    let mut provider = AnthropicProvider::new(opts);
    assert_eq!(
        provider.messages_url,
        "https://api.anthropic.com/v1/messages"
    );
    assert!(provider.direct_media_endpoint);
    assert!(provider.is_first_party);
    assert_eq!(provider.media_budget_for(&MAIN.into()).max_blocks, 100);
    assert_eq!(
        provider.media_budget_for(&MAIN.into()).max_base64_bytes,
        DIRECT_MEDIA_BASE64_BYTES
    );
    // This cfg(test)-only route changes the physical destination, not the
    // endpoint predicate's input, admission limits, auth, or beta behavior.
    provider.messages_url = format!("{base}/v1/messages");
    provider.count_tokens_url = format!("{base}/v1/messages/count_tokens");
    provider.client = reqwest::Client::builder().no_proxy().build().unwrap();
    provider
}

struct SourceDirectory(PathBuf);

impl SourceDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "klaus-image-policy-fixture-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for SourceDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn padded_png(id: usize, width: u32, height: u32, raw_bytes: usize) -> Vec<u8> {
    let pixels = image::RgbImage::from_fn(width, height, |x, y| {
        if (x + y) % 2 == 0 {
            image::Rgb([24, 48, 72])
        } else {
            image::Rgb([72, 48, 24])
        }
    });
    let mut encoded = std::io::Cursor::new(Vec::new());
    image::DynamicImage::ImageRgb8(pixels)
        .write_to(&mut encoded, image::ImageFormat::Png)
        .unwrap();
    let original = encoded.into_inner();
    assert_eq!(&original[original.len() - 8..original.len() - 4], b"IEND");
    let chunk_size = raw_bytes.checked_sub(original.len() + 12).unwrap();
    let mut chunk = Vec::with_capacity(chunk_size + 4);
    chunk.extend_from_slice(b"tEXt");
    chunk.extend_from_slice(PNG_ID_PREFIX);
    chunk.extend_from_slice(format!("{id:08}").as_bytes());
    assert!(chunk.len() <= chunk_size + 4);
    chunk.resize(chunk_size + 4, b'p');
    let mut bytes = Vec::with_capacity(raw_bytes);
    bytes.extend_from_slice(&original[..original.len() - 12]);
    bytes.extend_from_slice(&u32::try_from(chunk_size).unwrap().to_be_bytes());
    bytes.extend_from_slice(&chunk);
    bytes.extend_from_slice(&crc32fast::hash(&chunk).to_be_bytes());
    bytes.extend_from_slice(&original[original.len() - 12..]);
    assert_eq!(bytes.len(), raw_bytes);
    assert_eq!(
        image::guess_format(&bytes).unwrap(),
        image::ImageFormat::Png
    );
    assert_eq!(
        image::load_from_memory(&bytes).unwrap().dimensions(),
        (width, height)
    );
    bytes
}

fn png_id(bytes: &[u8]) -> usize {
    assert_eq!(&bytes[..8], b"\x89PNG\r\n\x1a\n");
    let mut at = 8;
    let mut id = None;
    while at < bytes.len() {
        let length = u32::from_be_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
        let end = at + 8 + length;
        let chunk = &bytes[at + 4..end];
        let crc = u32::from_be_bytes(bytes[end..end + 4].try_into().unwrap());
        assert_eq!(
            crc32fast::hash(chunk),
            crc,
            "PNG chunks must have valid CRCs"
        );
        if &chunk[..4] == b"tEXt" && chunk[4..].starts_with(PNG_ID_PREFIX) {
            let begin = 4 + PNG_ID_PREFIX.len();
            id = Some(
                std::str::from_utf8(&chunk[begin..begin + 8])
                    .unwrap()
                    .parse()
                    .unwrap(),
            );
        }
        at = end + 4;
    }
    assert_eq!(at, bytes.len());
    id.expect("fixture image must retain its valid identification chunk")
}

fn image_block(bytes: &[u8], source_path: Option<RuntimeString>) -> ImageBlock {
    ImageBlock {
        mime_type: "image/png".into(),
        base64: BASE64.encode(bytes).into(),
        source_path,
        detail: None,
    }
}

fn tiny_image(id: usize) -> ImageBlock {
    image_block(&padded_png(id, 2, 2, 256), None)
}

fn images_request(model: &str, total: usize) -> ModelRequest {
    request(
        model,
        vec![user(
            "images",
            std::iter::once(ContentBlock::Text(text("Keep request text.")))
                .chain((0..total).map(|id| ContentBlock::Image(tiny_image(id))))
                .collect(),
        )],
    )
}

fn historical_request(images: &[ImageBlock]) -> ModelRequest {
    let nested = |tool: &str, range: std::ops::Range<usize>| {
        ContentBlock::ToolResult(ToolResultBlock {
            tool_use_id: tool.into(),
            is_error: None,
            content: std::iter::once(OutputBlock::Text(text(&format!("Keep {tool} text."))))
                .chain(range.map(|id| OutputBlock::Image(images[id].clone())))
                .collect(),
        })
    };
    request(
        MAIN,
        vec![
            user(
                "old-user",
                vec![
                    ContentBlock::Text(text("Keep oldest text.")),
                    ContentBlock::Image(images[0].clone()),
                    ContentBlock::Image(images[1].clone()),
                    nested("old-tool", 2..29),
                ],
            ),
            Message::Assistant(AssistantMessage {
                uuid: "previous-assistant".into(),
                model: MAIN.into(),
                content: vec![ContentBlock::Text(text("Keep assistant text."))],
                usage: None,
            }),
            user(
                "new-user",
                vec![
                    ContentBlock::Text(text("Keep newest text.")),
                    ContentBlock::Image(images[29].clone()),
                    ContentBlock::Image(images[30].clone()),
                    nested("new-tool", 31..55),
                ],
            ),
        ],
    )
}

fn retain_images(req: &ModelRequest, images: &[ImageBlock], keep: &[usize]) -> ModelRequest {
    let retained = |image: &ImageBlock| keep.iter().any(|id| images[*id].base64 == image.base64);
    let mut compliant = req.clone();
    for message in &mut compliant.messages {
        if let Message::User(message) = message {
            for block in &mut message.content {
                if let ContentBlock::ToolResult(result) = block {
                    result.content.retain(|block| match block {
                        OutputBlock::Image(image) => retained(image),
                        _ => true,
                    });
                }
            }
            message.content.retain(|block| match block {
                ContentBlock::Image(image) => retained(image),
                _ => true,
            });
        }
    }
    compliant
}

async fn read_request(stream: &mut TcpStream, expected_line: &str) -> (String, Value) {
    let mut received = Vec::new();
    let mut buffer = [0; 65_536];
    loop {
        let count = stream.read(&mut buffer).await.unwrap();
        assert!(count > 0, "the complete HTTP body must arrive before EOF");
        received.extend_from_slice(&buffer[..count]);
        let Some(end) = received.windows(4).position(|part| part == b"\r\n\r\n") else {
            continue;
        };
        let headers = std::str::from_utf8(&received[..end]).unwrap();
        assert!(headers.starts_with(expected_line), "{headers}");
        let length = headers
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find(|(key, _)| key.eq_ignore_ascii_case("content-length"))
            .map(|(_, value)| value.trim().parse::<usize>().unwrap())
            .unwrap_or(0);
        assert!(
            length <= OTHER_MEDIA_BASE64_BYTES + 1024 * 1024,
            "fixture request is bounded"
        );
        if received.len() >= end + 4 + length {
            let body = if length == 0 {
                Value::Null
            } else {
                serde_json::from_slice(&received[end + 4..end + 4 + length]).unwrap()
            };
            return (headers.to_owned(), body);
        }
    }
}

async fn respond(stream: &mut TcpStream, content_type: &str, payload: &str) {
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
        payload.len()
    );
    stream.write_all(response.as_bytes()).await.unwrap();
    stream.shutdown().await.unwrap();
}

async fn capture_server(count: usize) -> (String, tokio::task::JoinHandle<Vec<Value>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        tokio::time::timeout(WAIT, async move {
            let mut bodies = Vec::new();
            for _ in 0..count {
                let (mut stream, _) = listener.accept().await.unwrap();
                let (headers, body) = read_request(&mut stream, "POST /v1/messages HTTP/1.1").await;
                assert!(headers.lines().any(|line| line.eq_ignore_ascii_case("x-api-key: fixture-key")));
                assert!(headers.lines().any(|line| line.eq_ignore_ascii_case("anthropic-version: 2023-06-01")));
                assert!(!headers.lines().any(|line| line.to_ascii_lowercase().starts_with("authorization:")));
                let start = json!({"type":"message_start", "message":{
                    "id":"fixture", "model":body["model"],
                    "usage":{"input_tokens":1,"output_tokens":0}
                }});
                let events = format!(concat!(
                    "event: message_start\ndata: {}\n\n",
                    "event: message_delta\ndata: {{\"type\":\"message_delta\",\"delta\":{{\"stop_reason\":\"end_turn\"}},\"usage\":{{\"output_tokens\":1}}}}\n\n",
                    "event: message_stop\ndata: {{\"type\":\"message_stop\"}}\n\n",
                ), start);
                bodies.push(body);
                respond(&mut stream, "text/event-stream", &events).await;
            }
            assert!(tokio::time::timeout(Duration::from_millis(50), listener.accept()).await.is_err(),
                "completed fixtures must not leave retries or extra requests");
            bodies
        }).await.expect("loopback capture must finish within its request timeout")
    });
    (base, task)
}

async fn send(provider: &dyn ModelProvider, req: ModelRequest) -> Vec<ModelStreamEvent> {
    tokio::time::timeout(WAIT, provider.stream(req, AbortSignal::new()).collect())
        .await
        .expect("public provider stream must finish promptly")
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
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::RetryScheduled { .. })),
        "{events:?}"
    );
}

fn assert_no_policy_leak(value: &Value) {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                assert!(
                    !matches!(
                        key.as_str(),
                        "imageLimits" | "image_limits" | "maxMediaBlocks" | "max_media_blocks"
                    ),
                    "internal request policy leaked into HTTP: {key}"
                );
                assert_no_policy_leak(value);
            }
        }
        Value::Array(values) => values.iter().for_each(assert_no_policy_leak),
        _ => {}
    }
}

fn wire_images(body: &Value) -> Vec<&Value> {
    let mut images = Vec::new();
    for message in body["messages"].as_array().unwrap() {
        if message["role"] != "user" {
            continue;
        }
        for block in message["content"].as_array().unwrap() {
            if block["type"] == "image" {
                images.push(block);
            }
            if block["type"] == "tool_result" {
                images.extend(
                    block["content"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .filter(|block| block["type"] == "image"),
                );
            }
        }
    }
    images
}

fn validate_wire_images(body: &Value, max_base64: usize) -> (Vec<usize>, usize, usize) {
    assert_no_policy_leak(body);
    let mut ids = Vec::new();
    let mut base64_bytes = 0;
    let mut raw_bytes = 0;
    for image in wire_images(body) {
        assert_eq!(image["source"]["type"], "base64");
        assert_eq!(image["source"]["media_type"], "image/png");
        let encoded = image["source"]["data"].as_str().unwrap();
        assert!(encoded.len() <= max_base64);
        let bytes = BASE64
            .decode(encoded)
            .expect("captured base64 must decode strictly");
        assert_eq!(BASE64.encode(&bytes), encoded, "base64 must be canonical");
        assert_eq!(
            image::guess_format(&bytes).unwrap(),
            image::ImageFormat::Png
        );
        let dimensions = image::load_from_memory(&bytes).unwrap().dimensions();
        assert!(
            dimensions.0 > 0 && dimensions.1 > 0,
            "captured PNG dimensions must be valid"
        );
        ids.push(png_id(&bytes));
        base64_bytes += encoded.len();
        raw_bytes += bytes.len();
    }
    (ids, base64_bytes, raw_bytes)
}

#[tokio::test]
async fn official_55_valid_images_use_actual_stream_bytes_and_stable_compliant_cache_prefix() {
    let source = SourceDirectory::new();
    let mut files = Vec::new();
    let images: Vec<ImageBlock> = (0..55)
        .map(|id| {
            let bytes = padded_png(id, 2, 2, HISTORY_RAW_BYTES);
            let path = source.0.join(format!("{id:02}.png"));
            std::fs::write(&path, &bytes).unwrap();
            files.push((path.clone(), Sha256::digest(&bytes).to_vec()));
            let image = image_block(&bytes, Some(path.to_str().unwrap().into()));
            assert_eq!(image.base64.len_utf16(), HISTORY_BASE64_BYTES);
            assert!(image.base64.len_utf16() < IMAGE_MAX_BASE64_CHARS);
            assert!(
                matches!(gate_image_block_with_limits(&image, ResolvedImageLimits::default()),
            GatedImage::Keep(ref kept) if kept == &image),
                "every historical image is individually valid"
            );
            image
        })
        .collect();
    assert!(
        images
            .iter()
            .map(|image| image.base64.len_utf16())
            .sum::<usize>()
            > DIRECT_MEDIA_BASE64_BYTES
    );
    let (base, server) = capture_server(3).await;
    let provider = official_provider_routed_locally(&base);
    let req = historical_request(&images);
    let original = req.messages.clone();
    assert_completed(&send(&provider, req.clone()).await);

    // Independent expected traversal: nested old media, old top-level media,
    // then nested new media. The newest top-level pair must survive.
    let expected: Vec<usize> = [29, 30].into_iter().chain(35..55).collect();
    let compliant = retain_images(&req, &images, &expected);
    assert_completed(&send(&provider, compliant.clone()).await);
    assert_completed(&send(&provider, compliant.clone()).await);
    let bodies = server.await.unwrap();
    assert_eq!(bodies.len(), 3);
    let (ids, base64_bytes, raw_bytes) = validate_wire_images(&bodies[0], IMAGE_MAX_BASE64_CHARS);
    assert_eq!(
        ids, expected,
        "actual wire body must shed nested-first and oldest-first"
    );
    assert_eq!(ids.len(), 22);
    assert_eq!(base64_bytes, 22 * HISTORY_BASE64_BYTES);
    assert_eq!(raw_bytes, 22 * HISTORY_RAW_BYTES);
    assert!(base64_bytes <= DIRECT_MEDIA_BASE64_BYTES - MEDIA_BASE64_HEADROOM);
    assert!(
        (ids.len() + 1) * HISTORY_BASE64_BYTES > DIRECT_MEDIA_BASE64_BYTES - MEDIA_BASE64_HEADROOM,
        "only the minimum whole-image removal needed for byte headroom is allowed"
    );
    let old = bodies[0]["messages"][0]["content"].as_array().unwrap();
    assert_eq!(old.len(), 2);
    assert_eq!(old[0]["text"], "Keep oldest text.");
    assert_eq!(old[1]["tool_use_id"], "old-tool");
    assert_eq!(
        old[1]["content"],
        json!([{"type":"text", "text":"Keep old-tool text."}])
    );
    assert_eq!(
        bodies[0]["messages"][1]["content"][0]["text"],
        "Keep assistant text."
    );
    let newest = bodies[0]["messages"][2]["content"].as_array().unwrap();
    assert_eq!(newest[0]["text"], "Keep newest text.");
    assert_eq!(newest.last().unwrap()["tool_use_id"], "new-tool");
    assert_eq!(
        newest.last().unwrap()["content"][0]["text"],
        "Keep new-tool text."
    );
    assert!(
        old.last().unwrap().get("cache_control").is_none(),
        "only the final surviving carrier receives the conversation cache breakpoint"
    );
    assert_eq!(newest.last().unwrap()["cache_control"]["type"], "ephemeral");
    assert!(
        newest.last().unwrap()["content"]
            .as_array()
            .unwrap()
            .iter()
            .all(|block| block.get("cache_control").is_none()),
        "cache assignment must target surviving normalized carriers, not removed nested indices"
    );
    assert_eq!(
        bodies[0], bodies[1],
        "a compliant replay must preserve the normalized cached prefix"
    );
    assert_eq!(
        bodies[1], bodies[2],
        "a second compliant replay must remain byte-for-byte stable"
    );
    assert_eq!(
        req.messages, original,
        "sending must not rewrite stored history"
    );
    for (path, digest) in files {
        assert_eq!(
            Sha256::digest(std::fs::read(path).unwrap()).to_vec(),
            digest,
            "sending must not rewrite the original source image file"
        );
    }
    eprintln!("official public stream: original_count=55 original_base64_bytes={} final_count={} final_base64_bytes={} final_raw_bytes={} direct_limit={} byte_headroom={} compliant_replays=2",
        55 * HISTORY_BASE64_BYTES, ids.len(), base64_bytes, raw_bytes, DIRECT_MEDIA_BASE64_BYTES, MEDIA_BASE64_HEADROOM);
}

fn resolved_policy(provider: &AnthropicProvider, req: &ModelRequest) -> ResolvedImageLimits {
    provider
        .resolve_request_media_policy(
            &req.model,
            req.image_limits.as_ref().map(|limits| {
                provider
                    .image_limits_for(&req.model)
                    .with_declaration(Some(limits))
            }),
            req.max_media_blocks.map(|count| count.get() as usize),
        )
        .0
}

async fn snapshot_override_and_fallback_cases() {
    let (base, server) = capture_server(2).await;
    let mut opts = options(base);
    opts.image_limits = Some(ResolvedImageLimits {
        max_width: 2,
        max_height: 2,
        max_base64_chars: 32,
        target_raw_bytes: 24,
    });
    opts.max_media_blocks = Some(2);
    let provider = local_provider(opts);
    let mut req = images_request(MAIN, 30);
    req.image_limits = Some(DeclaredImageLimits {
        max_width: NonZeroU32::new(7),
        max_height: NonZeroU32::new(5),
        max_base64_size: NonZeroU32::new(768),
        target_raw_size: NonZeroU32::new(300),
    });
    req.max_media_blocks = NonZeroU32::new(28);
    let effective = resolved_policy(&provider, &req);
    assert_eq!(
        effective,
        ResolvedImageLimits {
            max_width: 7,
            max_height: 5,
            max_base64_chars: 768,
            target_raw_bytes: 300,
        }
    );
    let selected_axis = image_block(&padded_png(777, 7, 5, 256), None);
    assert_eq!(
        image_dimensions(&selected_axis),
        Some(ImageDimensions {
            width: 7,
            height: 5
        })
    );
    assert!(
        matches!(gate_image_block_with_limits(&selected_axis, effective), GatedImage::Keep(ref image) if image == &selected_axis)
    );
    assert!(
        matches!(
            gate_image_block_with_limits(&selected_axis, provider.image_limits_for(&MAIN.into())),
            GatedImage::Rejected(_)
        ),
        "native options must reject a payload that the selected request snapshot admits"
    );
    if let Message::User(message) = &mut req.messages[0] {
        *message.content.last_mut().unwrap() = ContentBlock::Image(selected_axis);
    }
    let original = req.clone();
    assert_completed(&send(&provider, req.clone()).await);
    assert_eq!(
        req, original,
        "snapshot binding must not mutate the supplied request"
    );
    if let Message::User(message) = &mut req.messages[0] {
        *message.content.last_mut().unwrap() = ContentBlock::Image(tiny_image(29));
    }

    let partial = DeclaredImageLimits {
        max_base64_size: NonZeroU32::new(1024),
        ..Default::default()
    };
    req.image_limits = Some(partial);
    req.max_media_blocks = NonZeroU32::new(27);
    assert_eq!(
        resolved_policy(&provider, &req),
        ResolvedImageLimits {
            max_width: 2,
            max_height: 2,
            max_base64_chars: 1024,
            target_raw_bytes: 768,
        }
    );
    assert_completed(&send(&provider, req).await);
    let bodies = server.await.unwrap();
    assert_eq!(
        validate_wire_images(&bodies[0], 768).0,
        (22..29).chain(std::iter::once(777)).collect::<Vec<_>>()
    );
    let selected_bytes = BASE64
        .decode(
            wire_images(&bodies[0]).last().unwrap()["source"]["data"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
    assert_eq!(
        image::load_from_memory(&selected_bytes)
            .unwrap()
            .dimensions(),
        (7, 5),
        "the selected-axis image must reach the real HTTP body unchanged"
    );
    assert_eq!(
        validate_wire_images(&bodies[1], 1024).0,
        (23..30).collect::<Vec<_>>()
    );
    assert_eq!(
        bodies[0]["messages"][0]["content"][0]["text"],
        "Keep request text."
    );
    assert_eq!(
        original
            .image_limits
            .unwrap()
            .target_raw_size
            .unwrap()
            .get(),
        300
    );
    assert_eq!(original.max_media_blocks.unwrap().get(), 28);

    let (base, server) = capture_server(2).await;
    let mut opts = options(base);
    opts.image_limits_by_model = Some(HashMap::from([(
        MAIN.to_owned(),
        ResolvedImageLimits {
            max_width: 8,
            max_height: 6,
            max_base64_chars: 768,
            target_raw_bytes: 576,
        },
    )]));
    opts.max_media_blocks_by_model = Some(HashMap::from([(MAIN.to_owned(), 27)]));
    opts.capabilities = Some(ModelCapabilities {
        context_window: Some(1_000_000),
        one_million_context: true,
        ..Default::default()
    });
    let provider = local_provider(opts);
    let native = images_request(MAIN, 30);
    assert_eq!(native.image_limits, None);
    assert_eq!(native.max_media_blocks, None);
    assert_eq!(resolved_policy(&provider, &native).max_base64_chars, 768);
    assert_completed(&send(&provider, native).await);
    let unknown = images_request("unknown-one-million-model", 101);
    assert_eq!(provider.media_budget_for(&unknown.model).max_blocks, 100);
    assert_completed(&send(&provider, unknown).await);
    let bodies = server.await.unwrap();
    assert_eq!(
        validate_wire_images(&bodies[0], 768).0,
        (23..30).collect::<Vec<_>>()
    );
    assert_eq!(
        validate_wire_images(&bodies[1], IMAGE_MAX_BASE64_CHARS).0,
        (21..101).collect::<Vec<_>>()
    );
}

struct RecordingProvider {
    inner: AnthropicProvider,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
}

impl ModelProvider for RecordingProvider {
    fn info(&self) -> &ProviderInfo {
        self.inner.info()
    }

    fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
        self.inner.capabilities(model)
    }

    fn stream(&self, req: ModelRequest, signal: AbortSignal) -> ModelStream {
        self.requests.lock().unwrap().push(req.clone());
        self.inner.stream(req, signal)
    }
}

async fn refresh_catalog(catalog: &ModelCatalog, document: Value) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/catalog", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        tokio::time::timeout(WAIT, async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            read_request(&mut stream, "GET /catalog HTTP/1.1").await;
            respond(&mut stream, "application/json", &document.to_string()).await;
        })
        .await
        .expect("catalog fixture must complete promptly")
    });
    let result = tokio::time::timeout(
        WAIT,
        catalog.refresh(
            &reqwest::Client::builder().no_proxy().build().unwrap(),
            &url,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result, CatalogRefresh::Updated);
    server.await.unwrap();
}

async fn captured_catalog_main_and_summary_cases() {
    let (base, server) = capture_server(2).await;
    let mut opts = options(base);
    opts.image_limits = Some(ResolvedImageLimits::default().with_base64_cap(32));
    opts.max_media_blocks = Some(2);
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let native = Arc::new(RecordingProvider {
        inner: local_provider(opts),
        requests: recorded.clone(),
    });
    let catalog = ModelCatalog::empty();
    refresh_catalog(&catalog, json!({"schemaVersion":1, "entries":[
        {"provider":"anthropic", "model":MAIN, "capabilities":{
            "imageLimits":{"maxWidth":7,"maxHeight":5,"maxBase64Size":768}, "maxMediaBlocks":27}},
        {"provider":"anthropic", "model":SUMMARY, "capabilities":{
            "imageLimits":{"maxWidth":5,"maxHeight":7,"maxBase64Size":1024}, "maxMediaBlocks":26}}
    ]})).await;
    let live = with_model_catalog(native, catalog.clone());
    let captured = capture_model_provider(live.clone(), &[MAIN.into(), SUMMARY.into()]);
    let main_before = captured.capabilities(&MAIN.into());
    let summary_before = captured.capabilities(&SUMMARY.into());
    assert_eq!(main_before.image_limits.unwrap().target_raw_size, None);
    assert_eq!(summary_before.image_limits.unwrap().target_raw_size, None);
    assert_eq!(
        ResolvedImageLimits::default()
            .with_declaration(main_before.image_limits.as_ref())
            .target_raw_bytes,
        576,
        "capture must not freeze the old default raw target into a partial cap declaration"
    );
    assert_eq!(
        ResolvedImageLimits::default()
            .with_declaration(summary_before.image_limits.as_ref())
            .target_raw_bytes,
        768
    );
    refresh_catalog(
        &catalog,
        json!({"schemaVersion":1, "entries":[
            {"provider":"anthropic", "model":MAIN, "capabilities":{
                "imageLimits":{"maxWidth":1,"maxHeight":1,"maxBase64Size":64}, "maxMediaBlocks":1}},
            {"provider":"anthropic", "model":SUMMARY, "capabilities":{
                "imageLimits":{"maxWidth":1,"maxHeight":1,"maxBase64Size":64}, "maxMediaBlocks":1}}
        ]}),
    )
    .await;
    assert_eq!(
        live.capabilities(&MAIN.into())
            .max_media_blocks
            .unwrap()
            .get(),
        1
    );
    assert_eq!(
        live.capabilities(&SUMMARY.into())
            .image_limits
            .unwrap()
            .max_base64_size
            .unwrap()
            .get(),
        64
    );
    assert_eq!(captured.capabilities(&MAIN.into()), main_before);
    assert_eq!(captured.capabilities(&SUMMARY.into()), summary_before);
    for model in [MAIN, SUMMARY] {
        let mut req = images_request(model, 30);
        req.image_limits = Some(DeclaredImageLimits {
            max_width: NonZeroU32::new(99),
            ..Default::default()
        });
        req.max_media_blocks = NonZeroU32::new(99);
        assert_completed(&send(captured.as_ref(), req).await);
    }
    let bodies = server.await.unwrap();
    assert_eq!(bodies[0]["model"], MAIN);
    assert_eq!(bodies[1]["model"], SUMMARY);
    assert_eq!(
        validate_wire_images(&bodies[0], 768).0,
        (23..30).collect::<Vec<_>>()
    );
    assert_eq!(
        validate_wire_images(&bodies[1], 1024).0,
        (24..30).collect::<Vec<_>>()
    );
    let requests = recorded.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].image_limits, main_before.image_limits);
    assert_eq!(requests[0].max_media_blocks, main_before.max_media_blocks);
    assert_eq!(requests[1].image_limits, summary_before.image_limits);
    assert_eq!(
        requests[1].max_media_blocks,
        summary_before.max_media_blocks
    );
    assert_eq!(requests[0].image_limits.unwrap().target_raw_size, None);
    assert_eq!(requests[1].image_limits.unwrap().target_raw_size, None);
}

async fn invalid_snapshot_image_precedes_aggregate_case() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut opts = options(format!("http://{}", listener.local_addr().unwrap()));
    opts.max_media_blocks = Some(600);
    let provider = local_provider(opts);
    let mut nested = vec![OutputBlock::Image(image_block(
        &padded_png(999, 2, 2, 600),
        None,
    ))];
    nested.extend((0..30).map(|id| OutputBlock::Image(tiny_image(id))));
    let mut req = request(
        MAIN,
        vec![user(
            "invalid-first",
            vec![ContentBlock::ToolResult(ToolResultBlock {
                tool_use_id: "invalid-old-tool".into(),
                is_error: None,
                content: nested,
            })],
        )],
    );
    req.image_limits = Some(DeclaredImageLimits {
        max_base64_size: NonZeroU32::new(768),
        ..Default::default()
    });
    req.max_media_blocks = NonZeroU32::new(28);
    let original = req.messages.clone();
    let events = send(&provider, req.clone()).await;
    assert!(
        matches!(events.as_slice(), [ModelStreamEvent::Error { error }]
        if error.kind == ProviderErrorKind::InvalidRequest
            && error.message.contains("messages[0].content[0].content[0]")
            && error.message.contains("768 base64-byte limit")
            && error.message.contains("800 base64 bytes")),
        "{events:?}"
    );
    assert!(tokio::time::timeout(Duration::from_millis(50), listener.accept()).await.is_err(),
        "the invalid oldest image would be trimmed by aggregate count shedding; it must instead refuse before HTTP");
    assert_eq!(req.messages, original);
}

#[tokio::test]
async fn complete_request_snapshot_policy_is_bound_on_public_stream_and_frozen_after_catalog_refresh(
) {
    snapshot_override_and_fallback_cases().await;
    captured_catalog_main_and_summary_cases().await;
    invalid_snapshot_image_precedes_aggregate_case().await;
    eprintln!("request snapshot public stream: full override=8 images; partial cap=7; native fallback=7; unknown 1M=80; refreshed catalog frozen main=7 summary=6; invalid oldest image=refused before HTTP; internal policy leak=none");
}
