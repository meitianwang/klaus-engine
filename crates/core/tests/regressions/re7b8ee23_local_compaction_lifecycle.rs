//! Defect: local checkpoints retained tool tails and shared mutable trimming state.
//! Root cause: summary reconstruction, overflow recovery and reconnects used separate policies.
//! Fix: e7b8ee23; create and commit source-preserving context checkpoints.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use klaus_agent_core::context::compact::engine::Compactor;
use klaus_agent_core::context::compact::full_compact::FullCompactor;
use klaus_agent_core::context::compact::{
    create_compactor, create_full_compactor, CompactorOptions, FullCompactorOptions,
};
use klaus_agent_core::context::tokens::estimate_messages_tokens;
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeJson, RuntimeObject, RuntimeString};
use klaus_agent_core::schemas::config::{EngineConfig, ResolvedConfig};
use klaus_agent_core::schemas::content::{ContentBlock, OutputBlock, TextBlock, ToolUseBlock};
use klaus_agent_core::schemas::message::{
    AssistantMessage, Message, ToolResultMessage, Usage, UserMessage,
};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{ProviderError, ProviderErrorKind};
use klaus_agent_core::types::provider::{
    CallModel, ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo, RemoteCompactionSupport, SystemPromptPart, ThinkingConfig,
    ToolChoice, ToolSpec,
};
use tokio::time::Instant;

const MODEL: &str = "local-lifecycle";
const SUMMARY: &str = "Keep the task constraints, completed work and next steps.";
type Requests = Arc<Mutex<Vec<(Instant, ModelRequest)>>>;

fn text(value: impl Into<RuntimeString>) -> TextBlock {
    TextBlock {
        text: value.into(),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    }
}

fn user(id: &str, value: impl Into<RuntimeString>) -> Message {
    Message::User(UserMessage {
        uuid: id.into(),
        content: vec![ContentBlock::Text(text(value))],
        is_meta: None,
        question_answer: None,
    })
}

fn assistant(id: &str, value: &str) -> Message {
    Message::Assistant(AssistantMessage {
        uuid: id.into(),
        content: vec![ContentBlock::Text(text(value))],
        model: MODEL.into(),
        usage: None,
    })
}

fn pair(id: &str) -> [Message; 2] {
    [
        Message::Assistant(AssistantMessage {
            uuid: format!("call-{id}").into(),
            model: MODEL.into(),
            usage: None,
            content: vec![ContentBlock::ToolUse(ToolUseBlock {
                id: id.into(),
                name: "fixture_tool".into(),
                input: RuntimeJson::Object(RuntimeObject::new()),
                tool_kind: None,
                thought_signature: None,
            })],
        }),
        Message::ToolResult(ToolResultMessage {
            uuid: format!("result-{id}").into(),
            tool_use_id: id.into(),
            content: vec![OutputBlock::Text(text("Recorded tool output."))],
            is_error: false,
            tool_kind: None,
            native_tool_output: None,
            output: None,
            extra_user_blocks: None,
        }),
    ]
}

fn old_boundary() -> Message {
    runtime_codec::decode_text(&RuntimeString::from(
        r#"{"type":"compact_boundary","uuid":"old-boundary","summary":"OLD_CHECKPOINT"}"#,
    ))
    .unwrap()
}

fn id(message: &Message) -> String {
    match message {
        Message::User(value) => &value.uuid,
        Message::Assistant(value) => &value.uuid,
        Message::ToolResult(value) => &value.uuid,
        Message::System(value) => &value.uuid,
        Message::CompactBoundary(value) => &value.uuid,
    }
    .try_to_utf8()
    .unwrap()
}

fn body(message: &Message) -> String {
    let Message::User(user) = message else {
        panic!("expected retained user text");
    };
    user.content
        .iter()
        .map(|block| match block {
            ContentBlock::Text(value) => value.text.try_to_utf8().unwrap(),
            _ => panic!("retained user content must be text"),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn usage() -> Usage {
    Usage {
        input_tokens: 100,
        output_tokens: 20,
        cache_read_input_tokens: 0,
        cache_creation_input_tokens: 0,
        total_tokens: None,
    }
}

fn stop(reason: ModelStopReason) -> ModelStreamEvent {
    ModelStreamEvent::MessageStop {
        stop_reason: reason,
        usage: usage(),
    }
}

fn success() -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::TextDelta {
            text: SUMMARY.into(),
        },
        stop(ModelStopReason::EndTurn),
    ]
}

fn error(kind: ProviderErrorKind, retry_after_ms: Option<u64>) -> ModelStreamEvent {
    ModelStreamEvent::Error {
        error: ProviderError {
            kind,
            message: "Controlled local summary failure.".into(),
            status: None,
            code: None,
            error_type: None,
            media_origins: None,
            media_repair_complete: false,
            credential_recovery: None,
            retry_after_ms,
            request_id: None,
        },
    }
}

fn config() -> ResolvedConfig {
    EngineConfig {
        model: MODEL.into(),
        context_window: 200_000,
        compaction_stream_max_retries: 5,
        stream_idle_timeout_ms: 300_000,
        ..EngineConfig::default()
    }
}

fn request(messages: Vec<Message>) -> ModelRequest {
    ModelRequest {
        supports_image_detail_original: false,
        model_verbosity: None,
        reasoning_summary: Default::default(),
        model: MODEL.into(),
        image_limits: None,
        max_media_blocks: None,
        system: vec![SystemPromptPart {
            name: "workspace-policy".into(),
            text: "Preserve the approved workspace policy.".into(),
            cacheable: None,
            cache_scope: None,
        }],
        messages,
        tools: vec![ToolSpec {
            name: "fixture_tool".into(),
            description: "A declaration that summaries must not execute.".into(),
            input_schema: RuntimeJson::Object(RuntimeObject::new()),
            kind: Default::default(),
            format: None,
        }],
        native_deferred_tools: None,
        deferred_tools: Vec::new(),
        tool_choice: Some(ToolChoice::Auto),
        max_output_tokens: 7_777,
        task_budget: None,
        output_schema: None,
        web_search: None,
        thinking: Some(ThinkingConfig::Adaptive),
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

fn full(call_model: CallModel) -> FullCompactor {
    create_full_compactor(FullCompactorOptions {
        call_model,
        model: MODEL.into(),
        max_output_tokens: Some(7_777),
        uuid: Box::new(|| uuid::Uuid::new_v4().to_string()),
    })
}

fn recording(
    requests: &Requests,
    reply: impl Fn(usize, &ModelRequest) -> Vec<ModelStreamEvent> + Send + Sync + 'static,
) -> CallModel {
    let requests = requests.clone();
    Box::new(move |request, _| {
        let index = {
            let mut seen = requests.lock().unwrap();
            let index = seen.len();
            seen.push((Instant::now(), request.clone()));
            index
        };
        Box::pin(futures::stream::iter(reply(index, &request)))
    })
}

fn summary_is_last(messages: &[Message]) {
    assert!(
        matches!(messages.last(), Some(Message::CompactBoundary(value)) if value.summary == SUMMARY)
    );
    assert_eq!(
        messages
            .iter()
            .filter(|message| matches!(message, Message::CompactBoundary(_)))
            .count(),
        1
    );
    assert!(messages[..messages.len() - 1]
        .iter()
        .all(|message| matches!(message, Message::User(value) if value.is_meta != Some(true))));
}

#[tokio::test]
async fn recent_real_users_share_one_text_budget_and_the_completed_summary_is_last() {
    let requests = Requests::default();
    let compactor = full(recording(&requests, |_, _| success()));
    let middle = format!(
        "MIDDLE_START{}MIDDLE_SECRET{}MIDDLE_END",
        "m".repeat(30_000),
        "m".repeat(30_000)
    );
    let newest = format!("NEWEST_START{}NEWEST_END", "n".repeat(28_000));
    let mut meta = user("meta", "DO_NOT_RETAIN_META");
    if let Message::User(value) = &mut meta {
        value.is_meta = Some(true);
    }
    let original = vec![
        user(
            "oldest",
            "An older user turn outside the shared retained budget.",
        ),
        old_boundary(),
        user("middle", middle),
        assistant("old-answer", "Do not replay this assistant tail."),
        pair("older-tool")[0].clone(),
        pair("older-tool")[1].clone(),
        meta,
        user("newest", newest.clone()),
        assistant("latest-answer", "Do not replay this assistant tail either."),
    ];
    let unchanged = original.clone();
    let result = compactor
        .full_compact(original.clone(), &config(), None, false)
        .await;
    assert!(result.compacted);
    assert_eq!(original, unchanged);
    summary_is_last(&result.messages);
    assert_eq!(result.messages.len(), 3);
    assert_eq!(id(&result.messages[0]), "middle");
    assert_eq!(id(&result.messages[1]), "newest");
    assert_eq!(body(&result.messages[1]), newest);
    let truncated = body(&result.messages[0]);
    assert!(truncated.starts_with("MIDDLE_START"));
    assert!(truncated.ends_with("MIDDLE_END"));
    assert!(!truncated.contains("MIDDLE_SECRET"));
    let retained_tokens = estimate_messages_tokens(&result.messages[..2]);
    assert!((20_000..=20_016).contains(&retained_tokens));
    assert_eq!(requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn an_oversized_newest_user_keeps_its_identity_and_both_ends() {
    let requests = Requests::default();
    let compactor = full(recording(&requests, |_, _| success()));
    let huge = format!(
        "NEWEST_HEAD{}REMOVE_THIS_MIDDLE{}NEWEST_TAIL",
        "x".repeat(50_000),
        "x".repeat(50_000)
    );
    let result = compactor
        .full_compact(
            vec![user("older", "Older context."), user("latest", huge)],
            &config(),
            None,
            false,
        )
        .await;
    assert!(result.compacted);
    summary_is_last(&result.messages);
    assert_eq!(result.messages.len(), 2);
    assert_eq!(id(&result.messages[0]), "latest");
    let retained = body(&result.messages[0]);
    assert!(retained.starts_with("NEWEST_HEAD"));
    assert!(retained.ends_with("NEWEST_TAIL"));
    assert!(!retained.contains("REMOVE_THIS_MIDDLE"));
    assert!(estimate_messages_tokens(&result.messages[..1]) <= 20_016);
}

#[tokio::test(start_paused = true)]
async fn repeated_context_rejections_trim_only_request_copies_and_reset_reconnect_budget() {
    let original = vec![
        user(
            "oldest",
            "Keep this original user constraint after recovery.",
        ),
        pair("a")[0].clone(),
        pair("a")[1].clone(),
        user("second", "Second requirement."),
        assistant("second-answer", "Second progress."),
        user("third", "Third requirement."),
        pair("b")[0].clone(),
        pair("b")[1].clone(),
        user("latest", "The latest request."),
        assistant("latest-answer", "Latest progress."),
    ];
    let requests = Requests::default();
    let compactor = full(recording(&requests, |index, _| {
        let phase = index / 6;
        if index % 6 < 5 {
            vec![error(ProviderErrorKind::Network, Some(0))]
        } else if phase < 5 {
            vec![error(ProviderErrorKind::PromptTooLong, None)]
        } else {
            success()
        }
    }));
    let result = compactor
        .full_compact(original.clone(), &config(), None, false)
        .await;
    assert!(
        result.compacted,
        "five context rejections must not exhaust a three-attempt cap"
    );
    summary_is_last(&result.messages);
    assert!(result
        .messages
        .iter()
        .any(|message| id(message) == "oldest"));
    let seen = requests.lock().unwrap();
    assert_eq!(
        seen.len(),
        36,
        "each reduced request gets five reconnects of its own"
    );
    let expected = [
        vec![
            "oldest",
            "call-a",
            "result-a",
            "second",
            "second-answer",
            "third",
            "call-b",
            "result-b",
            "latest",
            "latest-answer",
        ],
        vec![
            "call-a",
            "result-a",
            "second",
            "second-answer",
            "third",
            "call-b",
            "result-b",
            "latest",
            "latest-answer",
        ],
        vec![
            "second",
            "second-answer",
            "third",
            "call-b",
            "result-b",
            "latest",
            "latest-answer",
        ],
        vec![
            "second-answer",
            "third",
            "call-b",
            "result-b",
            "latest",
            "latest-answer",
        ],
        vec!["third", "call-b", "result-b", "latest", "latest-answer"],
        vec!["call-b", "result-b", "latest", "latest-answer"],
    ];
    for (phase, expected) in expected.iter().enumerate() {
        let request = &seen[phase * 6].1;
        let actual: Vec<_> = request
            .messages
            .iter()
            .filter(
                |message| !matches!(message, Message::User(value) if value.is_meta == Some(true)),
            )
            .map(id)
            .collect();
        assert_eq!(&actual, expected, "trim phase {phase}");
        for retry in 1..6 {
            assert_eq!(
                seen[phase * 6 + retry].1,
                *request,
                "reconnects resend the same reduced request"
            );
        }
    }
    assert_eq!(original.len(), 10);
    assert!(matches!(&original[0], Message::User(value) if value.uuid == "oldest"));
}

#[tokio::test(start_paused = true)]
async fn five_configured_retries_follow_server_waits_before_a_complete_summary() {
    let requests = Requests::default();
    let compactor = full(recording(&requests, |index, _| {
        if index < 5 {
            let kinds = [
                ProviderErrorKind::Auth,
                ProviderErrorKind::InvalidRequest,
                ProviderErrorKind::RequestTooLarge,
                ProviderErrorKind::Quota,
                ProviderErrorKind::Unknown,
            ];
            vec![error(kinds[index], Some(1_250))]
        } else {
            success()
        }
    }));
    let started = Instant::now();
    let result = compactor
        .full_compact(
            vec![user("task", "Complete the task.")],
            &config(),
            None,
            false,
        )
        .await;
    assert!(result.compacted);
    assert_eq!(started.elapsed(), Duration::from_millis(6_250));
    let seen = requests.lock().unwrap();
    assert_eq!(seen.len(), 6);
    for calls in seen.windows(2) {
        assert_eq!(
            calls[1].0.duration_since(calls[0].0),
            Duration::from_millis(1_250)
        );
    }
}

#[tokio::test(start_paused = true)]
async fn failed_or_unterminated_text_never_replaces_original_history() {
    let original = vec![
        user("task", "Original constraints."),
        old_boundary(),
        pair("read")[0].clone(),
        pair("read")[1].clone(),
    ];
    for failure in [
        "network",
        "unterminated",
        "refusal",
        "empty-limit",
        "empty-tool",
    ] {
        let requests = Requests::default();
        let compactor = full(recording(&requests, move |_, _| {
            let mut events = if failure.starts_with("empty-") {
                Vec::new()
            } else {
                vec![ModelStreamEvent::TextDelta {
                    text: "UNCOMMITTED_PARTIAL_TEXT".into(),
                }]
            };
            match failure {
                "network" => events.push(error(ProviderErrorKind::Network, Some(0))),
                "unterminated" => {}
                "refusal" => events.push(stop(ModelStopReason::Refusal)),
                "empty-limit" => events.push(stop(ModelStopReason::MaxTokens)),
                "empty-tool" => events.push(stop(ModelStopReason::ToolUse)),
                _ => unreachable!(),
            }
            events
        }));
        let mut config = config();
        config.compaction_stream_max_retries = 0;
        let result = compactor
            .full_compact(original.clone(), &config, None, false)
            .await;
        assert!(!result.compacted, "{failure}: {result:?}");
        assert_eq!(result.messages, original, "{failure}");
        assert_eq!(result.freed_tokens, 0, "{failure}");
        assert!(!result.diagnostics.is_empty(), "{failure}");
        assert_eq!(requests.lock().unwrap().len(), 1, "{failure}");
    }
}

#[tokio::test]
async fn output_limited_text_replaces_history_with_an_explicit_degradation() {
    use klaus_agent_core::context::compact::outcome::CompactionDiagnosticKind;

    let requests = Requests::default();
    let compactor = full(recording(&requests, |_, _| {
        vec![
            ModelStreamEvent::TextDelta {
                text: SUMMARY.into(),
            },
            stop(ModelStopReason::MaxTokens),
        ]
    }));
    let original = vec![
        user("task", "Original constraints."),
        old_boundary(),
        pair("read")[0].clone(),
        pair("read")[1].clone(),
    ];
    let result = compactor
        .full_compact(original, &config(), None, false)
        .await;
    assert!(result.compacted, "{result:?}");
    summary_is_last(&result.messages);
    assert_eq!(body(&result.messages[0]), "Original constraints.");
    assert_eq!(result.diagnostics.len(), 1);
    assert_eq!(
        result.diagnostics[0].kind,
        CompactionDiagnosticKind::OutputTruncated
    );
    assert_eq!(requests.lock().unwrap().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn cancelling_a_pending_summary_preserves_history_and_a_fresh_manual_attempt_can_finish() {
    let signals = Arc::new(Mutex::new(Vec::<AbortSignal>::new()));
    let captured = signals.clone();
    let compactor = full(Box::new(move |_, signal| {
        let first = {
            let mut signals = captured.lock().unwrap();
            signals.push(signal);
            signals.len() == 1
        };
        if first {
            Box::pin(futures::stream::pending())
        } else {
            Box::pin(futures::stream::iter(success()))
        }
    }));
    let original = vec![user("task", "Keep these constraints.")];
    let config = config();
    let cancelled = AbortSignal::new();
    compactor.prepare_request(&request(original.clone()), &cancelled);
    let operation = compactor.manual_compact(original.clone(), &config, None);
    tokio::pin!(operation);
    tokio::select! {
        _ = &mut operation => panic!("the controlled provider must still be pending"),
        _ = tokio::time::sleep(Duration::from_millis(1)) => {},
    }
    cancelled.cancel();
    let result = operation.await;
    assert!(!result.compacted);
    assert_eq!(result.messages, original);
    assert!(signals.lock().unwrap()[0].is_cancelled());
    let fresh = AbortSignal::new();
    compactor.prepare_request(&request(original.clone()), &fresh);
    let result = compactor.manual_compact(original, &config, None).await;
    assert!(result.compacted);
    summary_is_last(&result.messages);
    assert!(!fresh.is_cancelled());
}

#[tokio::test]
async fn custom_summary_instructions_keep_the_live_policy_and_do_not_enable_tools() {
    let requests = Requests::default();
    let compactor = full(recording(&requests, |_, _| success()));
    let original = vec![user("task", "Continue the task.")];
    let captured = request(original.clone());
    compactor.prepare_request(&captured, &AbortSignal::new());
    let mut config = config();
    config.compact_prompt = Some("Keep pending work and user constraints.".into());
    let instructions = RuntimeString::from("Emphasize the failing test and its reproduction.");
    let result = compactor
        .manual_compact(original, &config, Some(&instructions))
        .await;
    assert!(result.compacted);
    let seen = requests.lock().unwrap();
    assert_eq!(seen.len(), 1);
    let request = &seen[0].1;
    assert_eq!(request.system, captured.system);
    assert_eq!(request.max_output_tokens, 7_777);
    assert_eq!(request.thinking, Some(ThinkingConfig::Adaptive));
    assert!(request.tools.is_empty());
    assert!(request.tool_choice.is_none());
    let last = request.messages.last().unwrap();
    assert!(matches!(last, Message::User(value) if value.is_meta == Some(true)));
    assert_eq!(body(last), "Keep pending work and user constraints.\n\nEmphasize the failing test and its reproduction.");
}

struct PlainProvider {
    info: ProviderInfo,
    requests: Requests,
}

impl ModelProvider for PlainProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }
    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: true,
            tools: false,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: 200_000,
            max_output_tokens: 8_000,
            vision: false,
            native_pdf: false,
            image_limits: None,
            max_media_blocks: None,
            supports_image_detail_original: false,
        }
    }
    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        self.requests
            .lock()
            .unwrap()
            .push((Instant::now(), request));
        Box::pin(futures::stream::iter(success()))
    }
}

#[tokio::test]
async fn a_provider_without_native_compaction_uses_text_summary_without_account_credentials() {
    let requests = Requests::default();
    let provider = Arc::new(PlainProvider {
        info: ProviderInfo {
            name: "plain-api".into(),
            default_model: MODEL.into(),
            base_url: None,
            env_key: None,
        },
        requests: requests.clone(),
    });
    assert_eq!(
        provider.remote_compaction_support(&MODEL.into()),
        RemoteCompactionSupport::Unsupported
    );
    let transport = provider.clone();
    let compactor = create_compactor(CompactorOptions {
        call_model: Box::new(move |request, signal| transport.stream(request, signal)),
        model: MODEL.into(),
        max_output_tokens: Some(8_000),
        uuid: Box::new(|| uuid::Uuid::new_v4().to_string()),
    });
    compactor.bind_native_provider(provider);
    let result = compactor
        .manual_compact(
            vec![user("task", "Preserve the local task.")],
            &config(),
            None,
        )
        .await;
    assert!(result.compacted);
    summary_is_last(&result.messages);
    assert_eq!(requests.lock().unwrap().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn idle_wait_after_partial_text_cancels_only_the_summary_request() {
    use futures::StreamExt;
    let signals = Arc::new(Mutex::new(Vec::<AbortSignal>::new()));
    let captured = signals.clone();
    let compactor = full(Box::new(move |_, signal| {
        captured.lock().unwrap().push(signal);
        Box::pin(
            futures::stream::iter([ModelStreamEvent::TextDelta {
                text: "Partial checkpoint.".into(),
            }])
            .chain(futures::stream::pending()),
        )
    }));
    let original = vec![user("task", "Preserve the original constraints.")];
    let parent = AbortSignal::new();
    compactor.prepare_request(&request(original.clone()), &parent);
    let mut config = config();
    config.stream_idle_timeout_ms = 40;
    config.compaction_stream_max_retries = 0;
    let started = Instant::now();
    let result = compactor
        .full_compact(original.clone(), &config, None, false)
        .await;
    assert!(!result.compacted);
    assert_eq!(result.messages, original);
    assert_eq!(started.elapsed(), Duration::from_millis(40));
    assert!(signals.lock().unwrap()[0].is_cancelled());
    assert!(!parent.is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn regular_stream_events_can_finish_after_the_normal_request_total_timeout() {
    let compactor = full(Box::new(move |_, _| {
        Box::pin(futures::stream::unfold(0, |index| async move {
            if index > 5 {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            let event = if index == 5 {
                stop(ModelStopReason::EndTurn)
            } else {
                ModelStreamEvent::TextDelta {
                    text: "Checkpoint detail. ".into(),
                }
            };
            Some((event, index + 1))
        }))
    }));
    let mut config = config();
    config.stream_idle_timeout_ms = 20;
    config.stream_total_timeout_ms = 1;
    config.compaction_stream_max_retries = 0;
    let started = Instant::now();
    let result = compactor
        .full_compact(vec![user("task", "Keep the task.")], &config, None, false)
        .await;
    assert!(result.compacted);
    assert_eq!(started.elapsed(), Duration::from_millis(60));
}

#[tokio::test]
async fn multilingual_user_retention_counts_utf8_bytes_without_splitting_surrogate_pairs() {
    let requests = Requests::default();
    let compactor = full(recording(&requests, |_, _| success()));
    let huge = format!(
        "多语言_HEAD🌍{}HIDDEN_MIDDLE{}多语言_TAIL🧪",
        "汉🙂".repeat(15_000),
        "汉🙂".repeat(15_000)
    );
    let result = compactor
        .full_compact(vec![user("unicode-user", huge)], &config(), None, false)
        .await;
    assert!(result.compacted);
    summary_is_last(&result.messages);
    assert_eq!(id(&result.messages[0]), "unicode-user");
    let retained = body(&result.messages[0]);
    assert!(retained.starts_with("多语言_HEAD🌍"));
    assert!(retained.ends_with("多语言_TAIL🧪"));
    assert!(!retained.contains("HIDDEN_MIDDLE"));
    assert!(
        !retained.contains('\u{fffd}'),
        "valid astral characters must not become replacement characters"
    );
    assert!(
        retained.len() <= 80_064,
        "retained head and tail use the shared byte budget plus one bounded truncation marker"
    );
    assert!(
        retained.len() >= 79_900,
        "char-safe truncation should use the available budget"
    );
}
