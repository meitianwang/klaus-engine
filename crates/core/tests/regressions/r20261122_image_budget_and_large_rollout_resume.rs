//! Defect: image-heavy editing sessions grew into >100 MB rollouts, hit
//! excessive per-step thinking latency on unpicked Opus 5.5 reasoning effort,
//! and took seconds to reopen or render history once checkpoints accumulated.
//! Root cause: `read` injected multi-megabyte raw images up to 5 MB without
//! compression or cold-cache reclamation, first-party adaptive requests had no
//! model-declared default effort fallback, and resume / thread-index scanners
//! decoded superseded image rows and repair snapshots in full.
//! Work item: 20261122 image read budget, cold-cache image reclaim, and
//! streaming skip for superseded rollout snapshots.

use base64::Engine as _;
use futures::StreamExt;
use image::{ImageBuffer, ImageFormat, Rgba};
use klaus_agent_core::context::context_hints::CONTEXT_HINT_CLEARED_MARKER;
use klaus_agent_core::context::images::{
    estimate_image_tokens, IMAGE_BEST_EFFORT_RAW_BYTES, READ_IMAGE_MAX_TOKENS,
};
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::questions::QuestionAnswer;
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::provider::anthropic::{AnthropicProvider, AnthropicProviderOptions};
use klaus_agent_core::provider::retry::RetryOptions;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{
    runtime_codec, runtime_json, RuntimeEncode, RuntimePath, RuntimeString,
};
use klaus_agent_core::schemas::content::{ContentBlock, ImageBlock, OutputBlock, TextBlock};
use klaus_agent_core::schemas::effort::EffortLevel;
use klaus_agent_core::schemas::message::{
    CompactBoundaryMessage, Message, ToolResultMessage, Usage, UserMessage,
};
use klaus_agent_core::schemas::rollout::{CompactedItem, EventItem, RolloutItem};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::tools::builtins::read::create_read_tool;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    EffortCapability, ModelCapabilities, ModelProvider, ModelRequest, ModelStopReason, ModelStream,
    ModelStreamEvent, ProviderCapabilities, ProviderInfo, ThinkingCapability, ThinkingConfig,
};
use serde_json::{json, Value};
use std::io::Cursor;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

fn make_text(value: &str) -> TextBlock {
    TextBlock {
        text: RuntimeString::from(value),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    }
}

fn noisy_png_bytes(width: u32, height: u32) -> Vec<u8> {
    let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
    let img = ImageBuffer::from_fn(width, height, |x, y| {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(((x as u64) << 16) ^ (y as u64) ^ 1442695040888963407);
        Rgba([
            (state >> 40) as u8,
            (state >> 32) as u8,
            (state >> 24) as u8,
            255,
        ])
    });
    let mut cursor = Cursor::new(Vec::new());
    image::DynamicImage::ImageRgba8(img)
        .write_to(&mut cursor, ImageFormat::Png)
        .expect("encode synthetic high-entropy PNG");
    cursor.into_inner()
}

struct MultiFrameReadProvider {
    info: ProviderInfo,
    requests: Mutex<Vec<ModelRequest>>,
    frame_paths: Vec<String>,
    empty_path: String,
    step: AtomicUsize,
}

impl ModelProvider for MultiFrameReadProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: true,
            thinking: true,
            structured_output: false,
            web_search: false,
            prompt_cache_control: true,
            context_window: 200_000,
            max_output_tokens: 16_384,
            native_pdf: false,
            vision: true,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn stream(&self, req: ModelRequest, _: AbortSignal) -> ModelStream {
        self.requests.lock().unwrap().push(req.clone());
        let call_index = self.step.fetch_add(1, Ordering::SeqCst);
        if call_index == 0 {
            let mut events = vec![ModelStreamEvent::MessageStart {
                model: req.model.clone(),
            }];
            let empty_args = json!({ "path": self.empty_path }).to_string();
            events.push(ModelStreamEvent::ToolUseStart {
                id: "call-empty".into(),
                name: "read".into(),
                thought_signature: None,
                tool_kind: None,
                input_format: None,
            });
            events.push(ModelStreamEvent::ToolUseInputDelta {
                id: "call-empty".into(),
                partial_json: empty_args.into(),
            });
            events.push(ModelStreamEvent::ToolUseStop {
                id: "call-empty".into(),
            });
            for (idx, path) in self.frame_paths.iter().enumerate() {
                let id = RuntimeString::from(format!("call-frame-{idx}"));
                let args = json!({ "path": path }).to_string();
                events.push(ModelStreamEvent::ToolUseStart {
                    id: id.clone(),
                    name: "read".into(),
                    thought_signature: None,
                    tool_kind: None,
                    input_format: None,
                });
                events.push(ModelStreamEvent::ToolUseInputDelta {
                    id,
                    partial_json: args.into(),
                });
                events.push(ModelStreamEvent::ToolUseStop {
                    id: format!("call-frame-{idx}").into(),
                });
            }
            events.push(ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::ToolUse,
                usage: Usage {
                    input_tokens: 1024,
                    output_tokens: 64,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: None,
                },
            });
            return Box::pin(futures::stream::iter(events));
        }

        Box::pin(futures::stream::iter(vec![
            ModelStreamEvent::MessageStart {
                model: req.model.clone(),
            },
            ModelStreamEvent::TextDelta {
                text: format!("done-{call_index}").into(),
            },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: Usage {
                    input_tokens: 1024,
                    output_tokens: 12,
                    cache_read_input_tokens: 512,
                    cache_creation_input_tokens: 0,
                    total_tokens: None,
                },
            },
        ]))
    }
}

fn image_blocks_in(req: &ModelRequest) -> Vec<ImageBlock> {
    let mut found = Vec::new();
    for msg in &req.messages {
        match msg {
            Message::ToolResult(tr) => {
                for block in &tr.content {
                    if let OutputBlock::Image(img) = block {
                        found.push(img.clone());
                    }
                }
            }
            Message::User(u) => {
                for block in &u.content {
                    if let ContentBlock::ToolResult(tr) = block {
                        for out in &tr.content {
                            if let OutputBlock::Image(img) = out {
                                found.push(img.clone());
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
    found
}

fn cleared_markers_in(req: &ModelRequest) -> usize {
    let mut count = 0;
    for msg in &req.messages {
        match msg {
            Message::ToolResult(tr) => {
                if tr.content.iter().any(
                    |b| matches!(b, OutputBlock::Text(t) if t.text == CONTEXT_HINT_CLEARED_MARKER),
                ) {
                    count += 1;
                }
            }
            Message::User(u) => {
                for block in &u.content {
                    if let ContentBlock::ToolResult(tr) = block {
                        if tr.content.iter().any(|b| {
                            matches!(b, OutputBlock::Text(t) if t.text == CONTEXT_HINT_CLEARED_MARKER)
                        }) {
                            count += 1;
                        }
                    }
                }
            }
            _ => {}
        }
    }
    count
}

#[tokio::test]
async fn read_tool_compresses_large_frames_and_cold_cache_turn_reclaims_aged_images_once() {
    let temp = tempdir().unwrap();
    let user_cfg = temp.path().join("klaus-cfg");
    std::fs::create_dir_all(&user_cfg).unwrap();

    // A high-entropy 2200x2200 PNG (>5 MB on disk) exercises both the raised 32 MiB
    // source gate and the <= 512 KB / <= 25,000 token read-time compression pipeline.
    let raw_png = noisy_png_bytes(2200, 2200);
    assert!(
        raw_png.len() > 5 * 1024 * 1024,
        "synthetic frame must exceed the historical 5 MB gate, got {} bytes",
        raw_png.len()
    );
    let mut frame_paths = Vec::new();
    for idx in 0..8 {
        let p = temp.path().join(format!("frame-{idx}.png"));
        std::fs::write(&p, &raw_png).unwrap();
        frame_paths.push(p.to_string_lossy().to_string());
    }
    let empty_path = temp.path().join("empty.png");
    std::fs::write(&empty_path, b"").unwrap();

    let clock_ms = Arc::new(AtomicI64::new(1_000_000));
    let provider = Arc::new(MultiFrameReadProvider {
        info: ProviderInfo {
            name: "anthropic".into(),
            default_model: "claude-opus-5-5".into(),
            env_key: None,
            base_url: Some("https://api.anthropic.com".into()),
        },
        requests: Mutex::new(Vec::new()),
        frame_paths,
        empty_path: empty_path.to_string_lossy().to_string(),
        step: AtomicUsize::new(0),
    });

    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut opts = agent_options(process);
    opts.provider = provider.clone();
    {
        let clock_ms = clock_ms.clone();
        opts.deps.now = Some(Arc::new(move || clock_ms.load(Ordering::SeqCst)));
    }
    opts.tools = Some(vec![create_read_tool()]);
    opts.config = Some(json!({
        "model": "claude-opus-5-5",
        "prewarm": false,
        "autoMemory": false,
        "autoDream": false,
        "memoryRecall": false,
        "checkpointing": false,
        "contextWindow": 200000
    }));

    let agent = Agent::new(opts);
    let session = agent.start_session(StartSessionOptions {
        cwd: Some(RuntimePath::from(temp.path().to_string_lossy().as_ref())),
        user_config_dir: Some(RuntimePath::from(user_cfg.to_string_lossy().as_ref())),
        ..Default::default()
    });

    // Turn 1: reads empty.png (error) + 8 oversized frames (compressed into budget).
    let turn1 = session
        .run(
            Input::Text("Inspect all 8 video frames".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(turn1.error.is_none(), "{:?}", turn1.error);

    // Turn 2 (10 seconds later — cache still warm): all 8 compressed frames stay intact.
    clock_ms.fetch_add(10_000, Ordering::SeqCst);
    let turn2 = session
        .run(Input::Text("Warm follow-up".into()), TurnOptions::default())
        .await;
    assert!(turn2.error.is_none(), "{:?}", turn2.error);

    // Turn 3 (65 minutes later — past prompt-cache lifetime): cold reclaim clears the
    // 3 oldest image tool results while keeping the 5 most recent intact.
    clock_ms.fetch_add(65 * 60 * 1_000, Ordering::SeqCst);
    let turn3 = session
        .run(Input::Text("Cold follow-up".into()), TurnOptions::default())
        .await;
    assert!(turn3.error.is_none(), "{:?}", turn3.error);

    // Turn 4 (5 seconds after Turn 3 — cache warm again): frozen set stays at 3 cleared, 5 kept.
    clock_ms.fetch_add(5_000, Ordering::SeqCst);
    let turn4 = session
        .run(
            Input::Text("Second warm follow-up".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(turn4.error.is_none(), "{:?}", turn4.error);

    let requests = provider.requests.lock().unwrap();
    assert_eq!(requests.len(), 5);

    let turn1_images = image_blocks_in(&requests[1]);
    assert_eq!(turn1_images.len(), 8);
    for img in &turn1_images {
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(img.base64.try_to_utf8().unwrap().as_bytes())
            .unwrap();
        assert!(
            decoded.len() <= IMAGE_BEST_EFFORT_RAW_BYTES,
            "compressed frame must fit within {IMAGE_BEST_EFFORT_RAW_BYTES} bytes, got {}",
            decoded.len()
        );
        let tokens = estimate_image_tokens(img);
        assert!(
            tokens <= READ_IMAGE_MAX_TOKENS,
            "compressed frame must fit within {READ_IMAGE_MAX_TOKENS} tokens, got {tokens}"
        );
        assert!(
            img.source_path
                .as_ref()
                .and_then(|p| p.try_to_utf8().ok())
                .is_some_and(|p| p.contains("frame-")),
            "expected local source_path on read image block"
        );
    }

    assert_eq!(image_blocks_in(&requests[2]).len(), 8);
    assert_eq!(cleared_markers_in(&requests[2]), 0);

    assert_eq!(image_blocks_in(&requests[3]).len(), 5);
    assert_eq!(cleared_markers_in(&requests[3]), 4);

    assert_eq!(image_blocks_in(&requests[4]).len(), 5);
    assert_eq!(cleared_markers_in(&requests[4]), 4);
}

async fn read_http_request(socket: &mut TcpStream) -> Option<(String, String)> {
    let mut bytes = Vec::new();
    loop {
        let mut buffer = [0u8; 8192];
        let count = socket.read(&mut buffer).await.ok()?;
        if count == 0 {
            return None;
        }
        bytes.extend_from_slice(&buffer[..count]);
        if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&bytes[..end]).into_owned();
            let length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or(0);
            if bytes.len() < end + 4 + length {
                continue;
            }
            let body = String::from_utf8_lossy(&bytes[end + 4..end + 4 + length]).into_owned();
            return Some((headers, body));
        }
    }
}

#[tokio::test]
async fn first_party_adaptive_requests_fall_back_to_declared_default_effort_only_when_thinking_active(
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let captured = Arc::new(Mutex::new(Vec::<(String, Value)>::new()));
    let captured_server = captured.clone();
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let (headers, body) = read_http_request(&mut socket).await.unwrap();
            let parsed: Value = serde_json::from_str(&body).unwrap();
            captured_server
                .lock()
                .unwrap()
                .push((headers.to_ascii_lowercase(), parsed));
            let sse = concat!(
                "event: message_start\n",
                "data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-opus-5-5\",\"usage\":{\"input_tokens\":10,\"output_tokens\":1}}}\n\n",
                "event: content_block_start\n",
                "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
                "event: content_block_delta\n",
                "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"ok\"}}\n\n",
                "event: content_block_stop\n",
                "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
                "event: message_delta\n",
                "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\n",
                "event: message_stop\n",
                "data: {\"type\":\"message_stop\"}\n\n",
            );
            let reply = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                sse.len(),
                sse
            );
            socket.write_all(reply.as_bytes()).await.unwrap();
        }
    });

    let mut provider = AnthropicProvider::new(AnthropicProviderOptions {
        base_url: Some(base),
        api_key: Some("sk-ant-test".into()),
        default_model: Some("claude-opus-5-5".into()),
        capabilities: Some(ModelCapabilities {
            thinking: Some(ThinkingCapability {
                adaptive: true,
                interleaved: true,
                can_disable_thinking: false,
            }),
            effort: Some(EffortCapability {
                levels: vec![
                    EffortLevel::Low,
                    EffortLevel::Medium,
                    EffortLevel::High,
                    EffortLevel::Xhigh,
                    EffortLevel::Max,
                ],
                default: Some(EffortLevel::Medium),
            }),
            ..Default::default()
        }),
        retry: Some(RetryOptions {
            max_attempts: Some(1),
            congestion_hold_ms: Some(0),
            unbounded_network_retries: Some(false),
            ..Default::default()
        }),
        ..Default::default()
    });
    provider.testing_set_first_party(true);

    let make_req = |thinking: Option<ThinkingConfig>| -> ModelRequest {
        let raw = runtime_json::parse(
            &json!({
                "model": "claude-opus-5-5",
                "system": [],
                "messages": [{
                    "type": "user",
                    "uuid": "u-1",
                    "content": [{ "type": "text", "text": "hello" }]
                }],
                "tools": [],
                "maxOutputTokens": 4096
            })
            .to_string()
            .into(),
        )
        .unwrap();
        let mut req: ModelRequest = runtime_codec::decode_value(&raw).unwrap();
        req.thinking = thinking;
        req.effort = None;
        req
    };

    let _: Vec<_> = provider
        .stream(make_req(Some(ThinkingConfig::Adaptive)), AbortSignal::new())
        .collect()
        .await;
    let _: Vec<_> = provider
        .stream(make_req(Some(ThinkingConfig::Disabled)), AbortSignal::new())
        .collect()
        .await;
    server.await.unwrap();

    let seen = captured.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert!(
        seen[0].0.contains("effort-2025-11-24"),
        "adaptive default effort must attach effort beta header: {}",
        seen[0].0
    );
    assert_eq!(
        seen[0].1["output_config"]["effort"],
        json!("medium"),
        "adaptive request with omitted effort must apply declared model default"
    );
    assert!(
        seen[1].1["output_config"]["effort"].is_null(),
        "disabled-thinking auxiliary requests must not attach output_config.effort"
    );
}

fn encode_line(value: &impl RuntimeEncode) -> String {
    String::from_utf8(runtime_codec::encode_bytes(value).unwrap()).unwrap()
}

#[tokio::test]
async fn resume_and_paged_history_stream_past_superseded_and_repair_image_payloads() {
    let dir = tempdir().unwrap();
    let thread_id = "synthetic-stream-skip";
    let rollout_path = dir.path().join(format!("1700000000000-{thread_id}.jsonl"));

    let big_b64 = "A".repeat(512 * 1024);

    let meta_row = json!({
        "kind": "session_meta",
        "schemaVersion": 2,
        "meta": {
            "id": thread_id,
            "cwd": dir.path().to_string_lossy(),
            "createdAt": 1700000000000_i64,
            "model": "claude-opus-5-5"
        }
    })
    .to_string();
    let turn_1_ctx = json!({
        "kind": "turn_context",
        "ctx": {
            "cwd": dir.path().to_string_lossy(),
            "model": "claude-opus-5-5",
            "permissionMode": "default",
            "turnId": "turn-1",
            "startedAtMs": 1000
        }
    })
    .to_string();

    let u1 = RolloutItem::Message {
        message: Message::User(UserMessage {
            uuid: "u-1".into(),
            content: vec![ContentBlock::Text(make_text("first prompt"))],
            is_meta: None,
            question_answer: None,
        }),
    };
    let qa_answer: QuestionAnswer = runtime_codec::decode_value(
        &runtime_json::parse(
            &r#"{"requestId":"req-1","turnId":"turn-1","toolCallId":"tc-qa","clientResponseId":"cr-1","questions":[],"answers":{}}"#.into(),
        )
        .unwrap(),
    )
    .unwrap();
    let qa_msg = RolloutItem::Message {
        message: Message::User(UserMessage {
            uuid: "u-qa".into(),
            content: vec![ContentBlock::Text(make_text("chosen answer"))],
            is_meta: None,
            question_answer: Some(Box::new(qa_answer)),
        }),
    };
    let tr_big_msg = Message::ToolResult(ToolResultMessage {
        uuid: "tr-big-1".into(),
        tool_use_id: "tu-big-1".into(),
        content: vec![OutputBlock::Image(ImageBlock {
            base64: RuntimeString::from(big_b64.clone()),
            mime_type: "image/png".into(),
            detail: None,
            source_path: None,
        })],
        is_error: false,
        tool_kind: None,
        native_tool_output: None,
        output: None,
        extra_user_blocks: None,
    });
    let obsolete_compacted = RolloutItem::Compacted(CompactedItem {
        summary: "older summary".into(),
        replaced_range: (0, 2),
        replacement_history: Some(vec![
            Message::CompactBoundary(CompactBoundaryMessage {
                uuid: "cb-old".into(),
                summary: "older summary".into(),
                native_compaction: None,
            }),
            tr_big_msg.clone(),
        ]),
        source_message_ids: Some(vec!["u-1".into(), "tr-big-1".into()]),
        approval_parent_history: None,
    });

    let u2 = Message::User(UserMessage {
        uuid: "u-2".into(),
        content: vec![ContentBlock::Text(make_text("after compaction"))],
        is_meta: None,
        question_answer: None,
    });
    let authoritative_compacted = RolloutItem::Compacted(CompactedItem {
        summary: "latest summary".into(),
        replaced_range: (0, 2),
        replacement_history: Some(vec![
            Message::CompactBoundary(CompactBoundaryMessage {
                uuid: "cb-latest".into(),
                summary: "latest summary".into(),
                native_compaction: None,
            }),
            u2.clone(),
        ]),
        source_message_ids: Some(vec!["cb-old".into(), "tr-big-1".into()]),
        approval_parent_history: None,
    });

    let repair_compacted = RolloutItem::Compacted(CompactedItem {
        summary: "".into(),
        replaced_range: (0, 2),
        replacement_history: Some(vec![
            Message::CompactBoundary(CompactBoundaryMessage {
                uuid: "cb-latest".into(),
                summary: "latest summary".into(),
                native_compaction: None,
            }),
            u2,
            Message::ToolResult(ToolResultMessage {
                uuid: "tr-post".into(),
                tool_use_id: "tu-post".into(),
                content: vec![OutputBlock::Image(ImageBlock {
                    base64: RuntimeString::from(big_b64),
                    mime_type: "image/png".into(),
                    detail: None,
                    source_path: None,
                })],
                is_error: false,
                tool_kind: None,
                native_tool_output: None,
                output: None,
                extra_user_blocks: None,
            }),
        ]),
        source_message_ids: Some(vec!["cb-latest".into(), "u-2".into(), "tr-post".into()]),
        approval_parent_history: None,
    });

    let lifecycle = RolloutItem::Event(EventItem {
        event: "turn_lifecycle".into(),
        turn_id: Some("turn-1".into()),
        payload: Some(
            runtime_json::parse(&r#"{"status":"completed","finishedAtMs":3000}"#.into()).unwrap(),
        ),
    });

    let lines = [
        meta_row,
        turn_1_ctx,
        encode_line(&u1),
        encode_line(&qa_msg),
        encode_line(&RolloutItem::Message {
            message: tr_big_msg,
        }),
        encode_line(&obsolete_compacted),
        encode_line(&authoritative_compacted),
        encode_line(&repair_compacted),
        encode_line(&lifecycle),
    ];
    std::fs::write(&rollout_path, lines.join("\n") + "\n").unwrap();

    let store = JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.path().to_string_lossy().into_owned()),
        archive_dir: None,
    });
    let resumed = store
        .resume_runtime_for_write(thread_id)
        .await
        .unwrap()
        .resume;
    assert!(resumed
        .persisted_user_uuids
        .contains(&RuntimeString::from("u-1")));
    assert!(resumed
        .persisted_user_uuids
        .contains(&RuntimeString::from("u-qa")));
    assert_eq!(
        resumed
            .turn_index
            .message_turn
            .get(&RuntimeString::from("tr-big-1")),
        Some(&RuntimeString::from("turn-1")),
        "skimmed superseded tool results must still populate message_turn index"
    );
    assert!(
        resumed
            .messages
            .iter()
            .any(|m| matches!(m, Message::User(u) if u.uuid == "u-qa")),
        "typed questionAnswer message before checkpoint must survive fast pre-checkpoint skip"
    );

    let page = store
        .paged_thread_history(thread_id, None, Some(50), None, None)
        .await
        .unwrap()
        .expect("thread history page");
    let boundaries: Vec<_> = page
        .history
        .history
        .iter()
        .filter_map(|m| match m {
            Message::CompactBoundary(cb) => Some(cb.summary.try_to_utf8().unwrap()),
            _ => None,
        })
        .collect();
    assert_eq!(
        boundaries,
        vec![
            "older summary".to_string(),
            "latest summary".to_string()
        ],
        "streaming compaction_info must skip history-repair records while preserving real boundaries"
    );
}
