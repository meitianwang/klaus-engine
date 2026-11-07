//! Defect: payload-size failures were reported as token overflow, summary media
//! recovery was unconditional, and rejected media had no bounded request repair.
//! Root cause: one overflow branch conflated three independent recovery states.
//! Work item 20261010: classify request bytes independently and preserve failures.

use async_trait::async_trait;
use futures::StreamExt;
use klaus_agent_core::context::compact::{create_full_compactor, FullCompactorOptions};
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::deps::{CompactionResult, Compactor, NoopCompactor};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::hooks::types::{
    Hook, HookDecision, HookEvent, HookFn, HookHandler, HookInput, HookOutcome,
};
use klaus_agent_core::provider::anthropic::{AnthropicProvider, AnthropicProviderOptions};
use klaus_agent_core::provider::classify::{classify_error, RawProviderError};
use klaus_agent_core::provider::retry::{is_retryable, RetryOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeString};
use klaus_agent_core::schemas::config::{default_config, ResolvedConfig};
use klaus_agent_core::schemas::content::{
    ContentBlock, DocumentBlock, ImageBlock, OutputBlock, TextBlock, ToolUseBlock,
};
use klaus_agent_core::schemas::message::{
    AssistantMessage, CompactBoundaryMessage, Message, ToolResultMessage, UserMessage,
};
use klaus_agent_core::schemas::rollout::RolloutItem;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{
    EngineError, ProviderError, ProviderErrorKind, ProviderMediaOrigin,
};
use klaus_agent_core::types::provider::{
    CallModel, ModelCapabilities, ModelProvider, ModelRequest, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn text(value: &str) -> ContentBlock {
    ContentBlock::Text(TextBlock {
        text: value.into(),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    })
}
fn image() -> ImageBlock {
    ImageBlock { mime_type: "image/png".into(), base64: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a5X8AAAAASUVORK5CYII=".into(), detail: None,  source_path: None }
}
fn user(id: &str, content: Vec<ContentBlock>) -> Message {
    Message::User(UserMessage {
        uuid: id.into(),
        content,
        is_meta: None,
        question_answer: None,
    })
}
fn assistant(id: &str) -> Message {
    Message::Assistant(AssistantMessage {
        uuid: id.into(),
        content: vec![text(&"task progress ".repeat(300))],
        model: "fixture".into(),
        usage: None,
    })
}
fn history() -> Vec<Message> {
    let mut messages = vec![user(
        "oldest",
        vec![
            text("Preserve the original constraints."),
            ContentBlock::Image(image()),
        ],
    )];
    for index in 0..7 {
        messages.push(assistant(&format!("assistant-{index}")));
    }
    messages.push(user("tail", vec![ContentBlock::Image(image())]));
    messages
}
fn error(kind: ProviderErrorKind, message: &str, status: Option<u16>) -> ProviderError {
    ProviderError {
        kind,
        message: message.into(),
        status,
        code: None,
        error_type: None,
        media_origins: None,
        media_repair_complete: false,
        credential_recovery: None,
        retry_after_ms: None,
        request_id: Some("req_fixture".into()),
    }
}
fn summary_options(call_model: CallModel) -> FullCompactorOptions {
    FullCompactorOptions {
        call_model,
        model: "fixture".into(),
        max_output_tokens: Some(1000),
        uuid: Box::new(|| "summary-boundary".into()),
    }
}

#[test]
fn token_and_request_byte_signals_keep_distinct_kinds_and_metadata() {
    for (status, code, body_type, message, expected) in [
        (
            Some(413),
            None,
            Some("invalid_request_error"),
            "gateway body limit is 12 MB",
            ProviderErrorKind::RequestTooLarge,
        ),
        (
            Some(413),
            None,
            Some("request_too_large"),
            "input exceeds context-window",
            ProviderErrorKind::PromptTooLong,
        ),
        (
            Some(413),
            Some("context_length_exceeded"),
            Some("request_too_large"),
            "over capacity",
            ProviderErrorKind::PromptTooLong,
        ),
        (
            None,
            None,
            Some("request_too_large"),
            "SSE payload exceeds body limit",
            ProviderErrorKind::RequestTooLarge,
        ),
        (
            Some(400),
            Some("request_too_large"),
            Some("invalid_request_error"),
            "gateway rejected the payload",
            ProviderErrorKind::RequestTooLarge,
        ),
        (
            Some(400),
            None,
            Some("invalid_request_error"),
            "input length and `max_tokens` exceed context limit",
            ProviderErrorKind::PromptTooLong,
        ),
    ] {
        let raw = RawProviderError {
            status,
            code: code.map(str::to_owned),
            body_type: body_type.map(str::to_owned),
            message: message.into(),
            normalized_request_id: Some("req_fixture".into()),
            ..Default::default()
        };
        let provider = classify_error(&raw);
        assert_eq!(provider.kind, expected);
        assert_eq!(provider.status, status);
        assert_eq!(provider.message, message);
        assert_eq!(
            provider
                .code
                .as_ref()
                .map(|code| code.try_to_utf8().unwrap()),
            code.map(str::to_owned)
        );
        assert_eq!(
            provider
                .error_type
                .as_ref()
                .map(|kind| kind.try_to_utf8().unwrap()),
            body_type.map(str::to_owned)
        );
        assert!(!is_retryable(&provider));
        let terminal = EngineError::from_provider(provider.clone(), "reactive-compaction-disabled");
        let encoded = runtime_codec::encode_value(&terminal).unwrap();
        let decoded: EngineError = runtime_codec::decode_value(&encoded).unwrap();
        assert_eq!(decoded.message, provider.message);
        assert_eq!(decoded.provider_error(), Some(&provider));
        assert_eq!(
            decoded.recovery_reason(),
            Some(&RuntimeString::from("reactive-compaction-disabled"))
        );
    }
    let wrapped_size_error = classify_error(&RawProviderError {
        error_type: Some("api_error".into()),
        body_type: Some("request_too_large".into()),
        message: "stream body limit".into(),
        ..Default::default()
    });
    assert_eq!(wrapped_size_error.kind, ProviderErrorKind::RequestTooLarge);
    assert!(!is_retryable(&wrapped_size_error));
    let network_413 = error(ProviderErrorKind::Network, "request rejected", Some(413));
    assert!(
        !is_retryable(&network_413),
        "a coarse kind must not force a repeated 413 body"
    );
}

#[tokio::test]
async fn a_large_summary_request_uses_only_its_configured_reconnect_budget() {
    let requests = Arc::new(AtomicUsize::new(0));
    let seen = requests.clone();
    let compactor = create_full_compactor(summary_options(Box::new(move |_, _| {
        seen.fetch_add(1, Ordering::SeqCst);
        Box::pin(futures::stream::iter(vec![ModelStreamEvent::Error {
            error: error(
                ProviderErrorKind::RequestTooLarge,
                "large system prefix",
                Some(413),
            ),
        }]))
    })));
    let original = vec![
        user("u", vec![text("constraints")]),
        assistant("a1"),
        assistant("a2"),
    ];
    let mut config = default_config();
    config.compaction_stream_max_retries = 0;
    let result = compactor
        .reactive_compact(original.clone(), &config, None)
        .await;
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    assert!(!result.compacted);
    assert_eq!(result.messages, original);
    assert!(result.diagnostics[0]
        .message
        .as_ref()
        .unwrap()
        .contains("large system prefix"));
}

const SSE_SUCCESS: &str = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"model\":\"fixture\",\"usage\":{\"input_tokens\":10,\"output_tokens\":0}}}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Recovered.\"}}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";

#[derive(Clone, Copy)]
enum FinalStream {
    Normal,
    Stall,
    Truncated,
}

async fn fixture_stream(
    responses: Vec<(u16, String, String)>,
    final_stream: FinalStream,
) -> (String, Arc<Mutex<Vec<Value>>>, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let seen = requests.clone();
    let task = tokio::spawn(async move {
        let last = responses.len() - 1;
        for (index, (status, content_type, body)) in responses.into_iter().enumerate() {
            let (mut socket, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let mut bytes = Vec::new();
            let mut buf = [0; 4096];
            let (header_end, length) = loop {
                let read = socket.read(&mut buf).await.unwrap();
                assert!(read > 0);
                bytes.extend_from_slice(&buf[..read]);
                if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&bytes[..end]);
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            let (key, value) = line.split_once(':')?;
                            key.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap();
                    break (end + 4, length);
                }
            };
            while bytes.len() < header_end + length {
                let read = socket.read(&mut buf).await.unwrap();
                assert!(read > 0);
                bytes.extend_from_slice(&buf[..read]);
            }
            seen.lock()
                .unwrap()
                .push(serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap());
            let length = body.len()
                + usize::from(index == last && matches!(final_stream, FinalStream::Truncated)) * 10;
            let response = format!("HTTP/1.1 {status} Fixture\r\nContent-Type: {content_type}\r\nContent-Length: {length}\r\nrequest-id: req_fixture\r\nConnection: close\r\n\r\n{body}");
            socket.write_all(response.as_bytes()).await.unwrap();
            if index == last && matches!(final_stream, FinalStream::Stall) {
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            socket.shutdown().await.unwrap();
        }
    });
    (format!("http://{address}"), requests, task)
}
fn rejection(status: u16, kind: &str, message: &str) -> (u16, String, String) {
    (
        status,
        "application/json".into(),
        json!({"error":{"type":kind,"code":"fixture_size_code","message":message}}).to_string(),
    )
}
fn accepted() -> (u16, String, String) {
    (200, "text/event-stream".into(), SSE_SUCCESS.into())
}

struct FilteredProvider(Arc<AnthropicProvider>);

impl ModelProvider for FilteredProvider {
    fn info(&self) -> &ProviderInfo {
        self.0.info()
    }
    fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities {
        self.0.capabilities(model)
    }
    fn stream(&self, request: ModelRequest, signal: AbortSignal) -> ModelStream {
        let mut actual = request.clone();
        let Message::User(user) = &mut actual.messages[0] else {
            panic!("fixture starts with media")
        };
        user.content.remove(0);
        let origins = if matches!(user.content.first(), Some(ContentBlock::Image(_))) {
            vec![ProviderMediaOrigin {
                wire_message_index: 0,
                wire_content_index: 0,
                wire_output_index: None,
                message_index: 0,
                content_index: Some(1),
                extra_user_blocks: false,
                output_index: None,
            }]
        } else {
            Vec::new()
        };
        Box::pin(self.0.stream(actual, signal).map(move |mut event| {
            if let ModelStreamEvent::Error { error } = &mut event {
                error.media_origins = Some(origins.clone());
            }
            event
        }))
    }
}

#[tokio::test]
async fn rejection_coordinates_follow_the_final_filtered_provider_body() {
    let original = vec![user(
        "media",
        vec![ContentBlock::Image(image()), ContentBlock::Image(image())],
    )];
    let (result, requests) = run_fixture_with_projection(
        vec![
            rejection(
                400,
                "invalid_request_error",
                "messages.0.content.0.image: Could not process image",
            ),
            accepted(),
        ],
        original,
        Some(Arc::new(NoopCompactor)),
        Vec::new(),
        true,
    )
    .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0]["messages"][0]["content"][0]["type"], "image");
    assert_eq!(
        requests[1]["messages"][0]["content"][0]["type"], "text",
        "the source block surviving wire pruning, not the already-pruned block, must change"
    );
}

async fn run_fixture(
    responses: Vec<(u16, String, String)>,
    history: Vec<Message>,
    compactor: Option<Arc<dyn Compactor>>,
    hooks: Vec<Hook>,
) -> (klaus_agent_core::engine::session::TurnResult, Vec<Value>) {
    run_fixture_with_projection(responses, history, compactor, hooks, false).await
}

async fn run_fixture_with_projection(
    responses: Vec<(u16, String, String)>,
    history: Vec<Message>,
    compactor: Option<Arc<dyn Compactor>>,
    hooks: Vec<Hook>,
    prune_oldest: bool,
) -> (klaus_agent_core::engine::session::TurnResult, Vec<Value>) {
    run_fixture_stream(
        responses,
        history,
        compactor,
        hooks,
        prune_oldest,
        FinalStream::Normal,
    )
    .await
}

async fn run_fixture_stream(
    responses: Vec<(u16, String, String)>,
    history: Vec<Message>,
    compactor: Option<Arc<dyn Compactor>>,
    hooks: Vec<Hook>,
    prune_oldest: bool,
    final_stream: FinalStream,
) -> (klaus_agent_core::engine::session::TurnResult, Vec<Value>) {
    let (base_url, captured, server) = fixture_stream(responses, final_stream).await;
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let root =
        std::env::temp_dir().join(format!("klaus-request-recovery-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let provider = Arc::new(AnthropicProvider::new(AnthropicProviderOptions {
        api_key: Some("fixture-only".into()),
        base_url: Some(base_url),
        default_model: Some("fixture".into()),
        retry: Some(RetryOptions {
            max_attempts: Some(5),
            congestion_hold_ms: Some(0),
            unbounded_network_retries: Some(false),
            ..Default::default()
        }),
        capabilities: Some(ModelCapabilities {
            vision: true,
            native_pdf: true,
            context_window: Some(1_000_000),
            max_output_tokens: Some(1000),
            ..Default::default()
        }),
        ..Default::default()
    }));
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
    options.provider = if prune_oldest {
        Arc::new(FilteredProvider(provider))
    } else {
        provider
    };
    options.compactor = compactor;
    if matches!(final_stream, FinalStream::Stall) {
        options
            .config
            .as_mut()
            .unwrap()
            .as_object_mut()
            .unwrap()
            .extend(
                json!({"streamIdleTimeoutMs":100, "streamTotalTimeoutMs":1000})
                    .as_object()
                    .unwrap()
                    .clone(),
            );
    }
    options.hooks = hooks;
    options.cwd = Some(root.to_string_lossy().into_owned());
    options
        .config
        .as_mut()
        .unwrap()
        .as_object_mut()
        .unwrap()
        .extend(
        json!({
            "model":"fixture", "contextWindow":1_000_000, "maxOutputTokens":1000, "maxRetries":5,
            "autoMemory":false, "agentMemory":false, "prewarm":false, "idleMicrocompact":false,
            "earlyMicrocompact":false, "speculativeCompaction":false, "loopDetection":false
        })
        .as_object()
        .unwrap()
        .clone(),
    );
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(root.join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let session = Agent::new(options)
        .open_session(
            store.clone(),
            StartSessionOptions {
                cwd: Some(root.to_string_lossy().into_owned().into()),
                user_config_dir: Some(root.to_string_lossy().into_owned().into()),
                initial_history: Some(history),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        session.run(Input::Text("Continue.".into()), TurnOptions::default()),
    )
    .await
    .expect("the recovery fixture must settle within its bounded request sequence");
    server.await.unwrap();
    let requests = captured.lock().unwrap().clone();
    session.flush_rollout().await.unwrap();
    if let Some(error) = &result.error {
        if let Some(provider) = error.provider_error() {
            let durable = store
                .read_thread(&session.id.try_to_utf8().unwrap())
                .await
                .unwrap();
            let payload = durable
                .raw_items
                .iter()
                .rev()
                .find_map(|item| match item {
                    RolloutItem::Event(event) if event.event == "turn_lifecycle" => {
                        event.payload.as_ref()
                    }
                    _ => None,
                })
                .expect("failed turn lifecycle remains durable");
            assert_eq!(
                payload.get("providerError"),
                Some(&runtime_codec::encode_value(provider).unwrap())
            );
            assert_eq!(
                payload.get("error").and_then(|message| message.as_string()),
                Some(&error.message)
            );
        }
    }
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
    (result, requests)
}

#[tokio::test]
async fn repaired_media_is_not_restored_after_eof_transport_failure_or_watchdog_timeout() {
    let prefix = SSE_SUCCESS.split("event: message_delta").next().unwrap();
    for final_stream in [
        FinalStream::Normal,
        FinalStream::Truncated,
        FinalStream::Stall,
    ] {
        let (result, requests) = run_fixture_stream(
            vec![
                rejection(400, "invalid_request_error", "Could not process image"),
                (200, "text/event-stream".into(), prefix.into()),
            ],
            vec![user("media", vec![ContentBlock::Image(image())])],
            Some(Arc::new(NoopCompactor)),
            Vec::new(),
            false,
            final_stream,
        )
        .await;
        let error = result
            .error
            .expect("incomplete wire stream must not synthesize success");
        assert!(error.provider_error().unwrap().media_repair_complete);
        assert_eq!(
            error.recovery_reason(),
            Some(&RuntimeString::from("request-media-recovery-exhausted"))
        );
        assert_eq!(
            requests.len(),
            2,
            "upper retries must not restore the original image"
        );
        assert_eq!(requests[0]["messages"][0]["content"][0]["type"], "image");
        assert_eq!(requests[1]["messages"][0]["content"][0]["type"], "text");
        assert_ne!(requests[0], requests[1]);
    }
}

#[tokio::test(start_paused = true)]
async fn a_summary_reconnects_without_starting_another_media_repair_policy() {
    let count = Arc::new(AtomicUsize::new(0));
    let seen = count.clone();
    let compactor = create_full_compactor(summary_options(Box::new(move |_, _| {
        seen.fetch_add(1, Ordering::SeqCst);
        let mut failure = error(
            ProviderErrorKind::RequestTooLarge,
            "media episode already exhausted",
            Some(413),
        );
        failure.media_repair_complete = true;
        Box::pin(futures::stream::iter(vec![ModelStreamEvent::Error {
            error: failure,
        }]))
    })));
    let original = history();
    let result = compactor
        .reactive_compact(original.clone(), &default_config(), None)
        .await;
    assert_eq!(count.load(Ordering::SeqCst), 6);
    assert!(!result.compacted);
    assert_eq!(result.messages, original);
    assert_eq!(
        result.diagnostics[0].message.as_ref(),
        Some(&RuntimeString::from("media episode already exhausted"))
    );
}

#[tokio::test]
async fn actual_http_413_is_terminal_without_repeating_the_body_or_losing_metadata() {
    let message = "gateway request body exceeds 12 MB";
    let (result, requests) = run_fixture(
        vec![rejection(413, "request_too_large", message)],
        Vec::new(),
        Some(Arc::new(NoopCompactor)),
        Vec::new(),
    )
    .await;
    assert_eq!(requests.len(), 1);
    let error = result.error.unwrap();
    assert_eq!(error.message, message);
    assert_eq!(
        error.provider_kind,
        Some(ProviderErrorKind::RequestTooLarge)
    );
    let provider = error.provider_error().unwrap();
    assert_eq!(provider.status, Some(413));
    assert_eq!(provider.code, Some("fixture_size_code".into()));
    assert_eq!(provider.error_type, Some("request_too_large".into()));
    assert_eq!(provider.request_id, Some("req_fixture".into()));
    assert_eq!(
        error.recovery_reason(),
        Some(&RuntimeString::from("reactive-compaction-disabled"))
    );
}

#[tokio::test]
async fn actual_sse_request_size_code_uses_the_typed_terminal_branch() {
    let body = "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"request_too_large\",\"message\":\"stream payload too large\",\"code\":\"fixture_size_code\"}}\n\n";
    let (result, requests) = run_fixture(
        vec![(200, "text/event-stream".into(), body.into())],
        Vec::new(),
        Some(Arc::new(NoopCompactor)),
        Vec::new(),
    )
    .await;
    assert_eq!(requests.len(), 1);
    let error = result.error.unwrap();
    assert_eq!(
        error.provider_kind,
        Some(ProviderErrorKind::RequestTooLarge)
    );
    assert_eq!(error.message, "stream payload too large");
    assert_eq!(
        error.provider_error().unwrap().code,
        Some("fixture_size_code".into())
    );
}

#[tokio::test]
async fn http400_targets_the_actual_coalesced_messages_content_coordinate() {
    let original = vec![
        user("u1", vec![ContentBlock::Image(image())]),
        user("u2", vec![ContentBlock::Image(image())]),
    ];
    let (result, requests) = run_fixture(
        vec![
            rejection(
                400,
                "invalid_request_error",
                "messages.0.content.1.image.source.media_type: unsupported format",
            ),
            accepted(),
        ],
        original,
        Some(Arc::new(NoopCompactor)),
        Vec::new(),
    )
    .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0]["messages"][0]["content"][0]["type"], "image");
    assert_eq!(requests[1]["messages"][0]["content"][0]["type"], "image");
    assert_eq!(requests[1]["messages"][0]["content"][1]["type"], "text");
    assert_ne!(requests[0], requests[1]);
}

#[tokio::test]
async fn targeted_media_retries_do_not_consume_the_three_carrier_budget() {
    let original = vec![user(
        "media",
        (0..5).map(|_| ContentBlock::Image(image())).collect(),
    )];
    let mut responses = (0..5)
        .map(|index| {
            rejection(
                400,
                "invalid_request_error",
                &format!("messages[0].content[{index}].image: Could not process image"),
            )
        })
        .collect::<Vec<_>>();
    responses.push(accepted());
    let (result, requests) = run_fixture(
        responses,
        original,
        Some(Arc::new(NoopCompactor)),
        Vec::new(),
    )
    .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    assert_eq!(requests.len(), 6);
    assert!(requests.windows(2).all(|pair| pair[0] != pair[1]));
}

#[tokio::test]
async fn pathless_media_recovery_removes_only_three_latest_media_carriers() {
    let mut original = Vec::new();
    for index in 0..4 {
        original.push(user(
            &format!("u-{index}"),
            vec![ContentBlock::Image(image())],
        ));
        original.push(assistant(&format!("a-{index}")));
    }
    let (result, requests) = run_fixture(
        (0..4)
            .map(|_| rejection(400, "invalid_request_error", "Could not process image"))
            .collect(),
        original,
        Some(Arc::new(NoopCompactor)),
        Vec::new(),
    )
    .await;
    assert!(result.error.is_some());
    assert_eq!(requests.len(), 4);
    assert!(requests.windows(2).all(|pair| pair[0] != pair[1]));
    assert_eq!(
        requests[3]["messages"][0]["content"][0]["type"], "image",
        "the fourth carrier must remain, not an unbounded media purge"
    );
}

#[derive(Clone)]
struct RecoveryCompactor {
    attempts: Arc<AtomicUsize>,
    unchanged: bool,
}
impl RecoveryCompactor {
    fn rewrite(&self, messages: &[Message], id: &str) -> CompactionResult {
        let next = if self.unchanged {
            messages.to_vec()
        } else {
            let start = messages
                .iter()
                .rposition(|message| matches!(message, Message::Assistant(_)))
                .unwrap_or(messages.len() - 1);
            let mut next = vec![Message::CompactBoundary(CompactBoundaryMessage {
                native_compaction: None,
                uuid: id.into(),
                summary: format!("Preserve constraints and resume the task after {id}.").into(),
            })];
            next.extend_from_slice(&messages[start..]);
            next
        };
        CompactionResult {
            messages: next,
            freed_tokens: 100,
            compacted: true,
            reason: None,
            diagnostics: Vec::new(),
        }
    }
}
#[async_trait]
impl Compactor for RecoveryCompactor {
    fn rebind_summary(
        &self,
        _: Arc<dyn ModelProvider>,
        _: &ResolvedConfig,
    ) -> Result<Option<Arc<dyn Compactor>>, String> {
        Ok(Some(Arc::new(self.clone())))
    }
    async fn maybe_compact(
        &self,
        messages: &[Message],
        _: u64,
        _: &ResolvedConfig,
        _: Option<&RuntimeString>,
    ) -> CompactionResult {
        CompactionResult {
            messages: messages.to_vec(),
            freed_tokens: 0,
            compacted: false,
            reason: None,
            diagnostics: Vec::new(),
        }
    }

    async fn reactive_compact(
        &self,
        messages: &[Message],
        _: &ResolvedConfig,
        _: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        self.rewrite(messages, "fresh")
    }
}

#[tokio::test]
async fn a_reported_compaction_without_changed_content_does_not_resend_the_body() {
    let compactor = Arc::new(RecoveryCompactor {
        attempts: Arc::new(AtomicUsize::new(0)),
        unchanged: true,
    });
    let (result, requests) = run_fixture(
        vec![rejection(413, "request_too_large", "body too large")],
        history(),
        Some(compactor),
        Vec::new(),
    )
    .await;
    assert_eq!(requests.len(), 1);
    assert_eq!(
        result.error.unwrap().recovery_reason(),
        Some(&RuntimeString::from("reactive-compaction-no-progress"))
    );
}

struct RejectCompact;
#[async_trait]
impl HookFn for RejectCompact {
    async fn run(&self, _: HookInput) -> HookOutcome {
        HookOutcome {
            decision: Some(HookDecision::Block),
            ..Default::default()
        }
    }
}

#[tokio::test]
async fn a_pre_compact_hook_veto_aborts_without_a_replacement_or_retry() {
    let compactor = Arc::new(RecoveryCompactor {
        attempts: Arc::new(AtomicUsize::new(0)),
        unchanged: false,
    });
    let hooks = vec![Hook {
        event: HookEvent::PreCompact,
        matcher: None,
        handler: HookHandler::Fn(Arc::new(RejectCompact)),
        is_async: false,
    }];
    let (result, requests) = run_fixture(
        vec![rejection(
            413,
            "request_too_large",
            "gateway refused original body",
        )],
        history(),
        Some(compactor.clone()),
        hooks,
    )
    .await;
    assert_eq!(requests.len(), 1);
    assert_eq!(compactor.attempts.load(Ordering::SeqCst), 0);
    assert!(result.error.is_none());
    assert_eq!(
        result.stop_reason,
        klaus_agent_core::types::errors::TerminalReason::Aborted
    );
}

#[tokio::test]
async fn tool_result_and_deferred_document_coordinates_preserve_unrejected_media() {
    let call = Message::Assistant(AssistantMessage {
        uuid: "call".into(),
        model: "fixture".into(),
        usage: None,
        content: vec![ContentBlock::ToolUse(ToolUseBlock {
            id: "tool".into(),
            name: "read".into(),
            input: klaus_agent_core::runtime_values::RuntimeObject::new().into(),
            tool_kind: None,
            thought_signature: None,
        })],
    });
    let original = vec![
        user("u", vec![text("read")]),
        call,
        Message::ToolResult(ToolResultMessage {
            uuid: "result".into(),
            tool_use_id: "tool".into(),
            tool_kind: None,
            output: None,
            native_tool_output: None,
            is_error: false,
            content: vec![OutputBlock::Image(image())],
            extra_user_blocks: Some(vec![ContentBlock::Document(DocumentBlock {
                mime_type: "application/pdf".into(),
                base64: "JVBERi0xLjQ=".into(),
                filename: Some("fixture.pdf".into()),
            })]),
        }),
    ];
    let responses = vec![
        rejection(
            400,
            "invalid_request_error",
            "messages.2.content.1.document: Could not process document",
        ),
        rejection(
            400,
            "invalid_request_error",
            "messages[2].content[0].content[0].image: Could not process image",
        ),
        accepted(),
    ];
    let (result, requests) = run_fixture(
        responses,
        original,
        Some(Arc::new(NoopCompactor)),
        Vec::new(),
    )
    .await;
    assert!(result.error.is_none(), "{:?}", result.error);
    assert_eq!(requests.len(), 3);
    assert_eq!(
        requests[1]["messages"][2]["content"][0]["content"][0]["type"],
        "image"
    );
    assert_eq!(requests[1]["messages"][2]["content"][1]["type"], "text");
    assert_eq!(
        requests[2]["messages"][2]["content"][0]["content"][0]["type"],
        "text"
    );
}

#[tokio::test]
async fn a_second_rejection_after_the_fresh_attempt_is_a_specific_terminal_failure() {
    let compactor = Arc::new(RecoveryCompactor {
        attempts: Arc::new(AtomicUsize::new(0)),
        unchanged: false,
    });
    let (result, requests) = run_fixture(
        vec![
            rejection(413, "request_too_large", "original body"),
            rejection(
                413,
                "request_too_large",
                "retained tail still exceeds 12 MB",
            ),
        ],
        history(),
        Some(compactor.clone()),
        Vec::new(),
    )
    .await;
    assert_eq!(requests.len(), 2);
    assert_eq!(compactor.attempts.load(Ordering::SeqCst), 1);
    let error = result.error.unwrap();
    assert_eq!(error.message, "retained tail still exceeds 12 MB");
    assert_eq!(
        error.recovery_reason(),
        Some(&RuntimeString::from("reactive-attempt-exhausted"))
    );
}

#[tokio::test]
async fn a_cancelled_summary_does_not_enter_the_stripped_retry() {
    let request = super::rbd0a6abc_empty_tool_choice::record_engine_request(false).await;
    let signal = klaus_agent_core::types::abort::AbortSignal::new();
    let cancel = signal.clone();
    let requests = Arc::new(AtomicUsize::new(0));
    let seen = requests.clone();
    let compactor = create_full_compactor(summary_options(Box::new(move |_, _| {
        seen.fetch_add(1, Ordering::SeqCst);
        cancel.cancel();
        Box::pin(futures::stream::iter(vec![ModelStreamEvent::Error {
            error: error(
                ProviderErrorKind::RequestTooLarge,
                "summary media too large",
                Some(413),
            ),
        }]))
    })));
    compactor.prepare_request(&request, &signal);
    let original = history();
    let result = compactor
        .reactive_compact(original.clone(), &default_config(), None)
        .await;
    assert!(!result.compacted);
    assert_eq!(result.messages, original);
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    assert!(result.diagnostics[0]
        .message
        .as_ref()
        .unwrap()
        .contains("interrupted"));
}

#[tokio::test]
async fn repeated_target_coordinates_never_resend_unchanged_content() {
    let original = vec![user("media", vec![ContentBlock::Image(image())])];
    let responses = (0..2)
        .map(|_| {
            rejection(
                400,
                "invalid_request_error",
                "messages.0.content.0.image: Could not process image",
            )
        })
        .collect();
    let (result, requests) = run_fixture(
        responses,
        original,
        Some(Arc::new(NoopCompactor)),
        Vec::new(),
    )
    .await;
    assert!(result.error.is_some());
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0]["messages"][0]["content"][0]["type"], "image");
    assert_eq!(requests[1]["messages"][0]["content"][0]["type"], "text");
}

#[derive(Clone, Default)]
struct AbortRecovery(Arc<Mutex<Option<klaus_agent_core::types::abort::AbortSignal>>>);

#[async_trait]
impl Compactor for AbortRecovery {
    fn rebind_summary(
        &self,
        _: Arc<dyn ModelProvider>,
        _: &ResolvedConfig,
    ) -> Result<Option<Arc<dyn Compactor>>, String> {
        Ok(Some(Arc::new(self.clone())))
    }
    fn prepare_request(
        &self,
        _: &ModelRequest,
        signal: &klaus_agent_core::types::abort::AbortSignal,
    ) {
        *self.0.lock().unwrap() = Some(signal.clone());
    }
    async fn maybe_compact(
        &self,
        messages: &[Message],
        _: u64,
        _: &ResolvedConfig,
        _: Option<&RuntimeString>,
    ) -> CompactionResult {
        CompactionResult {
            messages: messages.to_vec(),
            freed_tokens: 0,
            compacted: false,
            reason: None,
            diagnostics: Vec::new(),
        }
    }
    async fn reactive_compact(
        &self,
        messages: &[Message],
        _: &ResolvedConfig,
        _: Option<&RuntimeString>,
    ) -> CompactionResult {
        self.0.lock().unwrap().as_ref().unwrap().cancel();
        CompactionResult {
            messages: messages.to_vec(),
            freed_tokens: 0,
            compacted: false,
            reason: None,
            diagnostics: Vec::new(),
        }
    }
}

#[tokio::test]
async fn cancellation_of_recovery_keeps_the_already_received_provider_rejection() {
    let (result, requests) = run_fixture(
        vec![rejection(
            413,
            "request_too_large",
            "original 12 MB gateway rejection",
        )],
        history(),
        Some(Arc::new(AbortRecovery::default())),
        Vec::new(),
    )
    .await;
    assert_eq!(requests.len(), 1);
    let error = result
        .error
        .expect("cancelling recovery must not discard the original size rejection");
    assert_eq!(error.message, "original 12 MB gateway rejection");
    assert_eq!(
        error.provider_kind,
        Some(ProviderErrorKind::RequestTooLarge)
    );
    assert_eq!(
        error.recovery_reason(),
        Some(&RuntimeString::from("reactive-compaction-aborted"))
    );
}
