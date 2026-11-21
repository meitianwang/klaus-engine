//! Defect: switching from a server-pruned large-window source to a tighter
//! compaction-limit model with CJK-heavy tool outputs underestimated context
//! below the auto-compact threshold, launched parallel background precompute on
//! a cold model switch, and left `/compact` unable to recover when the full
//! cache-safe summary fork hit HTTP 429 rate limiting.
//! Root cause: `tokens.rs` divided raw UTF-8 bytes by 4 (undercounting CJK at
//! 0.75 tokens/char) and trusted foreign-model usage anchors; `turn.rs` armed
//! precompute before the active model had completed any turn; and
//! `full_compact.rs` did not fall back from the cache-safe fork to a lean
//! pruned summary on `ProviderErrorKind::RateLimit`.
//! Work item: 2026-11-21 cross-source CJK estimation, cold-switch precompute
//! guard, and 429 lean-compaction fallback.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use klaus_agent_core::context::compact::full_compact::{
    create_full_compactor, FullCompactorOptions,
};
use klaus_agent_core::context::tokens::{
    estimate_context_tokens, estimate_context_tokens_for_model, estimate_message_tokens,
    estimate_prefix_tokens,
};
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{resolve_config, Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeJson, RuntimeString};
use klaus_agent_core::schemas::content::{ContentBlock, ImageBlock, OutputBlock, TextBlock};
use klaus_agent_core::schemas::message::{
    AssistantMessage, Message, ToolResultMessage, Usage, UserMessage,
};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{ProviderError, ProviderErrorKind};
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo, ReasoningSummary, SystemPromptPart, ThinkingConfig,
    ToolSpec, ToolSpecKind,
};
use serde_json::json;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

fn msg(value: serde_json::Value) -> Message {
    runtime_codec::decode_text(&value.to_string().into()).unwrap()
}

fn text_block(text: impl Into<RuntimeString>) -> TextBlock {
    TextBlock {
        text: text.into(),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    }
}

fn source_config(name: &str, model: &str, window: u64, precompute: bool) -> serde_json::Value {
    json!({
        "model": model,
        "contextWindow": window,
        "maxOutputTokens": 2_000,
        "autoCompaction": true,
        "precomputeCompaction": precompute,
        "prewarm": false,
        "thinking": false,
        "loopDetection": false,
        "autoMemory": false,
        "autoDream": false,
        "memoryRecall": false,
        "maxRetries": 0,
        "compactionStreamMaxRetries": 0,
        "approval_policy": "never",
        "sandbox_mode": "danger-full-access",
        "modelSource": {
            "source": name,
            "modelId": model,
            "selection": format!("{name}:{model}")
        }
    })
}

#[test]
fn cjk_estimation_and_cross_model_usage_floor_prevent_undercounting() {
    let cjk_100 = "中".repeat(100);
    let cjk_msg = Message::User(UserMessage {
        uuid: "u-cjk".into(),
        content: vec![ContentBlock::Text(text_block(cjk_100))],
        is_meta: None,
        question_answer: None,
    });
    assert_eq!(
        estimate_message_tokens(&cjk_msg),
        100,
        "100 CJK characters must count at least 1 token per character rather than 300/4=75"
    );

    // Local history holds 30k CJK characters of tool output, while a prior model
    // with server-side tool-result clearing reported only 10k active tokens.
    let history = vec![
        Message::User(UserMessage {
            uuid: "u-1".into(),
            content: vec![ContentBlock::Text(text_block("开始剪辑"))],
            is_meta: None,
            question_answer: None,
        }),
        Message::ToolResult(ToolResultMessage {
            uuid: "tr-1".into(),
            tool_use_id: "call-1".into(),
            content: vec![OutputBlock::Text(text_block("字".repeat(30_000)))],
            is_error: false,
            tool_kind: None,
            native_tool_output: None,
            output: None,
            extra_user_blocks: None,
        }),
        Message::Assistant(AssistantMessage {
            uuid: "a-1".into(),
            content: vec![ContentBlock::Text(text_block("已完成"))],
            model: "source-a-model".into(),
            usage: Some(Usage {
                input_tokens: 9_800,
                output_tokens: 200,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
                total_tokens: None,
            }),
        }),
    ];

    let same_model =
        estimate_context_tokens_for_model(&history, 4_000, &RuntimeString::from("source-a-model"));
    assert_eq!(
        same_model, 10_000,
        "same-model turns still trust the provider's authoritative usage anchor"
    );

    let switched_model =
        estimate_context_tokens_for_model(&history, 4_000, &RuntimeString::from("source-b-model"));
    assert!(
        switched_model >= 34_000,
        "switching models must not hide unpruned local history behind a smaller foreign usage anchor; got {switched_model}"
    );
}

struct RecordingTargetProvider {
    info: ProviderInfo,
    window: u64,
    auto_compact_limit: Option<u64>,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
    calls: Arc<AtomicUsize>,
    reported_input_tokens: u64,
}

impl RecordingTargetProvider {
    fn new(
        name: &str,
        model: &str,
        window: u64,
        auto_compact_limit: Option<u64>,
        reported_input_tokens: u64,
        requests: Arc<Mutex<Vec<ModelRequest>>>,
        calls: Arc<AtomicUsize>,
    ) -> Arc<Self> {
        Arc::new(Self {
            info: ProviderInfo {
                name: name.into(),
                default_model: model.into(),
                base_url: None,
                env_key: None,
            },
            window,
            auto_compact_limit,
            requests,
            calls,
            reported_input_tokens,
        })
    }
}

impl ModelProvider for RecordingTargetProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn model_auto_compact_token_limit(&self, _: &RuntimeString) -> Option<u64> {
        self.auto_compact_limit
    }

    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: true,
            tools: true,
            thinking: true,
            structured_output: false,
            web_search: false,
            prompt_cache_control: true,
            context_window: self.window,
            max_output_tokens: 4_000,
            vision: true,
            native_pdf: false,
            image_limits: None,
            max_media_blocks: None,
            supports_image_detail_original: false,
        }
    }

    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let is_summary = request.tools.is_empty() || request.skip_cache_write == Some(true);
        let model = request.model.clone();
        self.requests.lock().unwrap().push(request);
        let text = if is_summary {
            "Condensed summary of the video editing progress."
        } else {
            "Continued cleanly on the target model."
        };
        let input_tokens = if is_summary {
            1_500
        } else {
            self.reported_input_tokens
        };
        Box::pin(futures::stream::iter(vec![
            ModelStreamEvent::MessageStart { model },
            ModelStreamEvent::TextDelta { text: text.into() },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: Usage {
                    input_tokens,
                    output_tokens: 120,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: None,
                },
            },
        ]))
    }
}

#[tokio::test]
async fn cjk_heavy_history_triggers_downshift_compaction_on_model_switch() {
    // 26,000 CJK chars = 78,000 UTF-8 bytes. Under raw bytes/4 that was only
    // 19,500 tokens (below a 24,000 target auto-compact ceiling); with CJK
    // accounting and cross-model floor it is >= 26,000 tokens and must compact
    // before issuing the main turn.
    let dir = tempfile::tempdir().unwrap();
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.path().join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));

    let old_requests = Arc::new(Mutex::new(Vec::new()));
    let old_calls = Arc::new(AtomicUsize::new(0));
    let old_provider = RecordingTargetProvider::new(
        "prior-source",
        "prior-large-model",
        500_000,
        None,
        12_000,
        old_requests,
        old_calls.clone(),
    );

    let mut initial_history = vec![msg(json!({
        "type": "user",
        "uuid": "u-0",
        "content": [{"type": "text", "text": "剪辑这 6 段口播视频并核对字幕"}]
    }))];
    for idx in 0..13 {
        let call_id = format!("call_{idx}");
        initial_history.push(msg(json!({
            "type": "assistant",
            "uuid": format!("a-{idx}"),
            "model": "prior-large-model",
            "content": [{"type": "tool_use", "id": call_id, "name": "bash", "input": {"command": "echo ok"}}],
            "usage": {"inputTokens": 9_000, "outputTokens": 50, "cacheReadInputTokens": 0, "cacheCreationInputTokens": 0}
        })));
        initial_history.push(msg(json!({
            "type": "tool_result",
            "uuid": format!("tr-{idx}"),
            "toolUseId": format!("call_{idx}"),
            "content": [{"type": "text", "text": "词".repeat(2_000)}],
            "isError": false
        })));
    }
    initial_history.push(msg(json!({
        "type": "assistant",
        "uuid": "a-final",
        "model": "prior-large-model",
        "content": [{"type": "text", "text": "准备导出最终成片"}],
        "usage": {"inputTokens": 12_000, "outputTokens": 80, "cacheReadInputTokens": 0, "cacheCreationInputTokens": 0}
    })));

    let mut options = agent_options(process);
    options.provider = old_provider;
    options.config = Some(source_config(
        "prior-source",
        "prior-large-model",
        500_000,
        false,
    ));
    options.cwd = Some(dir.path().to_string_lossy().into_owned());
    options.include_default_agents = Some(false);

    let session = Agent::new(options)
        .open_session(
            store,
            StartSessionOptions {
                initial_history: Some(initial_history),
                user_config_dir: Some(dir.path().to_string_lossy().into_owned().into()),
                base_instructions: Some("Assistant instructions.".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let target_requests = Arc::new(Mutex::new(Vec::new()));
    let target_calls = Arc::new(AtomicUsize::new(0));
    let target_provider = RecordingTargetProvider::new(
        "target-source",
        "target-tight-model",
        1_048_576,
        Some(24_000),
        2_000,
        target_requests.clone(),
        target_calls.clone(),
    );
    let mut target_cfg = resolve_config(source_config(
        "target-source",
        "target-tight-model",
        1_048_576,
        true,
    ))
    .unwrap();
    target_cfg.model_auto_compact_token_limit = Some(24_000);

    session
        .reconfigure_inference(
            target_provider,
            target_cfg,
            None,
            None,
            session.model_revision(),
        )
        .unwrap();

    let events: Vec<_> = tokio::time::timeout(
        Duration::from_secs(15),
        session
            .run_streamed(Input::Text("继续".into()), TurnOptions::default())
            .events
            .collect(),
    )
    .await
    .unwrap();

    assert_eq!(old_calls.load(Ordering::SeqCst), 0);
    assert!(
        events
            .iter()
            .any(|ev| matches!(ev, EngineEvent::Compaction { .. })),
        "CJK-heavy switched history must trigger downshift compaction before sending the main turn: {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|ev| matches!(ev, EngineEvent::TurnCompleted { .. })),
        "turn must complete cleanly after target-side downshift compaction: {events:?}"
    );

    let recorded = target_requests.lock().unwrap();
    assert!(
        recorded.len() >= 2,
        "expected a lean compaction summary request followed by the main turn request"
    );
    assert!(
        recorded[0].tools.is_empty(),
        "first request on the target model must be the lean summary pass, not an uncompacted full turn"
    );
    let main_req = recorded.last().unwrap();
    let main_tokens = estimate_context_tokens(&main_req.messages)
        + estimate_prefix_tokens(&main_req.system, &main_req.tools);
    assert!(
        main_tokens < 24_000,
        "main turn after downshift compaction must fit below target ceiling; got {main_tokens}"
    );
}

#[tokio::test]
async fn cold_model_switch_in_precompute_lead_window_does_not_fire_parallel_precompute() {
    // History sits inside the precompute lead window (80%-100% of a 24,000
    // auto-compact limit -> ~21,000 tokens) after switching from `prior-model`
    // to `target-model`, with zero prior assistant turns from `target-model`.
    let dir = tempfile::tempdir().unwrap();
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.path().join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));

    let old_provider = RecordingTargetProvider::new(
        "prior-source",
        "prior-model",
        500_000,
        None,
        21_000,
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(AtomicUsize::new(0)),
    );

    let initial_history = vec![
        msg(json!({
            "type": "user",
            "uuid": "u-0",
            "content": [{"type": "text", "text": "a".repeat(84_000)}]
        })),
        msg(json!({
            "type": "assistant",
            "uuid": "a-0",
            "model": "prior-model",
            "content": [{"type": "text", "text": "ok"}],
            "usage": {"inputTokens": 21_000, "outputTokens": 50, "cacheReadInputTokens": 0, "cacheCreationInputTokens": 0}
        })),
    ];

    let mut options = agent_options(process);
    options.provider = old_provider;
    options.config = Some(source_config("prior-source", "prior-model", 500_000, true));
    options.cwd = Some(dir.path().to_string_lossy().into_owned());
    options.include_default_agents = Some(false);

    let session = Agent::new(options)
        .open_session(
            store,
            StartSessionOptions {
                initial_history: Some(initial_history),
                user_config_dir: Some(dir.path().to_string_lossy().into_owned().into()),
                base_instructions: Some("Base prompt.".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let target_requests = Arc::new(Mutex::new(Vec::new()));
    let target_calls = Arc::new(AtomicUsize::new(0));
    let target_provider = RecordingTargetProvider::new(
        "target-source",
        "target-model",
        500_000,
        Some(24_000),
        21_000,
        target_requests.clone(),
        target_calls.clone(),
    );
    let mut target_cfg = resolve_config(source_config(
        "target-source",
        "target-model",
        500_000,
        true,
    ))
    .unwrap();
    target_cfg.model_auto_compact_token_limit = Some(24_000);

    session
        .reconfigure_inference(
            target_provider,
            target_cfg,
            None,
            None,
            session.model_revision(),
        )
        .unwrap();

    let events: Vec<_> = tokio::time::timeout(
        Duration::from_secs(15),
        session
            .run_streamed(Input::Text("继续".into()), TurnOptions::default())
            .events
            .collect(),
    )
    .await
    .unwrap();
    assert!(
        events
            .iter()
            .any(|ev| matches!(ev, EngineEvent::TurnCompleted { .. })),
        "first turn on switched model must complete: {events:?}"
    );
    // Give any mistakenly spawned background precompute task time to hit the provider.
    tokio::time::sleep(Duration::from_millis(80)).await;

    assert_eq!(
        target_calls.load(Ordering::SeqCst),
        1,
        "cold first turn after model switch must not arm a parallel background precompute request"
    );

    // On the second turn, `target-model` has now completed a turn in history
    // and its usage sits inside the lead window, so background precompute may arm.
    let events2: Vec<_> = tokio::time::timeout(
        Duration::from_secs(15),
        session
            .run_streamed(Input::Text("再看一眼".into()), TurnOptions::default())
            .events
            .collect(),
    )
    .await
    .unwrap();
    assert!(
        events2
            .iter()
            .any(|ev| matches!(ev, EngineEvent::TurnCompleted { .. })),
        "second turn on warmed model must complete: {events2:?}"
    );
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(
        target_calls.load(Ordering::SeqCst) >= 3,
        "once the active model has completed a turn, precompute should arm normally on subsequent turns; total calls = {}",
        target_calls.load(Ordering::SeqCst)
    );
}

#[tokio::test]
async fn cache_safe_compaction_fork_falls_back_to_pruned_lean_summary_on_rate_limit() {
    let captured = Arc::new(Mutex::new(Vec::<ModelRequest>::new()));
    let seen = captured.clone();
    let compactor = create_full_compactor(FullCompactorOptions {
        call_model: Box::new(move |request, _| {
            let idx = {
                let mut guard = seen.lock().unwrap();
                let idx = guard.len();
                guard.push(request);
                idx
            };
            if idx == 0 {
                // Stage 1 (full cache-safe fork) is rejected with HTTP 429 rate limit.
                Box::pin(futures::stream::iter(vec![ModelStreamEvent::Error {
                    error: ProviderError {
                        kind: ProviderErrorKind::RateLimit,
                        status: Some(429),
                        message: "antigravity_rate_limited [RESOURCE_EXHAUSTED]: Resource has been exhausted (e.g. check quota).".into(),
                        error_type: None,
                        retry_after_ms: None,
                        request_id: None,
                        code: Some("antigravity_rate_limited".into()),
                        media_origins: None,
                        media_repair_complete: false,
                        credential_recovery: None,
                    },
                }]))
            } else {
                Box::pin(futures::stream::iter(vec![
                    ModelStreamEvent::TextMessageCompleted {
                        id: "summary".into(),
                        phase: Some("final_answer".into()),
                        text: "Recovered lean summary after 429 fallback.".into(),
                    },
                    ModelStreamEvent::MessageStop {
                        stop_reason: ModelStopReason::EndTurn,
                        usage: Usage {
                            input_tokens: 1_500,
                            output_tokens: 100,
                            cache_read_input_tokens: 0,
                            cache_creation_input_tokens: 0,
                            total_tokens: None,
                        },
                    },
                ]))
            }
        }),
        model: "gemini-4-neon-xhigh".into(),
        max_output_tokens: Some(4_096),
        uuid: Box::new(|| "summary-prompt-id".to_owned()),
    });

    // Build a long history with an aged 12 KB tool output + an image + recent turns.
    let mut history = vec![
        Message::User(UserMessage {
            uuid: "u-0".into(),
            content: vec![
                ContentBlock::Text(text_block("Start editing")),
                ContentBlock::Image(ImageBlock {
                    mime_type: "image/png".into(),
                    base64: "AAAA".repeat(200).into(),
                    detail: None,
                    source_path: None,
                }),
            ],
            is_meta: None,
            question_answer: None,
        }),
        Message::ToolResult(ToolResultMessage {
            uuid: "tr-aged".into(),
            tool_use_id: "call-aged".into(),
            content: vec![OutputBlock::Text(text_block("x".repeat(12_000)))],
            is_error: false,
            tool_kind: None,
            native_tool_output: None,
            output: None,
            extra_user_blocks: None,
        }),
    ];
    for idx in 1..=7 {
        history.push(Message::Assistant(AssistantMessage {
            uuid: format!("a-{idx}").into(),
            content: vec![ContentBlock::Text(text_block(format!("step {idx}")))],
            model: "gemini-4-neon-xhigh".into(),
            usage: None,
        }));
    }

    let live_request = ModelRequest {
        supports_image_detail_original: false,
        model_verbosity: None,
        reasoning_summary: ReasoningSummary::Auto,
        model: "gemini-4-neon-xhigh".into(),
        image_limits: None,
        max_media_blocks: None,
        system: vec![SystemPromptPart {
            name: "base".into(),
            text: "Very large primary system prompt.".into(),
            cacheable: Some(true),
            cache_scope: None,
        }],
        messages: history.clone(),
        tools: vec![ToolSpec {
            name: "bash".into(),
            description: "Execute a shell command".into(),
            input_schema: RuntimeJson::Object(Default::default()),
            kind: ToolSpecKind::Function,
            format: None,
            namespace: None,
        }],
        native_deferred_tools: None,
        deferred_tools: Vec::new(),
        surfaced_deferred_names: Vec::new(),
        wire_caps: None,
        frozen_tools: None,
        frozen_instructions: None,
        tool_inventory_events: Vec::new(),
        tool_choice: None,
        max_output_tokens: 32_000,
        task_budget: None,
        output_schema: None,
        web_search: None,
        thinking: Some(ThinkingConfig::Enabled {
            budget_tokens: Some(16_000),
        }),
        effort: None,
        skip_cache_write: None,
        cache_ttl: None,
        cache_domain: None,
        cache_key: Some("session-429-fallback".into()),
        context_management: None,
        context_hints: None,
        thinking_context: None,
    };

    let signal = AbortSignal::new();
    compactor.prepare_request(&live_request, &signal);

    let mut config = klaus_agent_core::schemas::config::default_config();
    config.compaction_stream_max_retries = 5;
    let result = compactor.manual_compact(history, &config, None).await;
    assert!(
        result.compacted,
        "manual /compact must recover via lean fallback when cache-safe fork hits 429: {:?}",
        result.diagnostics
    );

    let requests = captured.lock().unwrap();
    assert_eq!(
        requests.len(),
        2,
        "cache-safe fork must fall back immediately on 429 without burning stream retries"
    );
    assert!(
        !requests[0].tools.is_empty(),
        "stage 1 should still attempt the cache-safe fork with live tools"
    );
    let fallback = &requests[1];
    assert!(fallback.tools.is_empty(), "stage 2 must strip tool schemas");
    assert_eq!(fallback.thinking, Some(ThinkingConfig::Disabled));
    assert_eq!(fallback.system.len(), 1);
    assert_eq!(fallback.system[0].name, "compact");
    let aged_tr = fallback
        .messages
        .iter()
        .find_map(|m| match m {
            Message::ToolResult(tr) if tr.uuid == "tr-aged" => Some(tr),
            _ => None,
        })
        .expect("aged tool result kept in summary request");
    let aged_len: usize = aged_tr
        .content
        .iter()
        .map(|b| match b {
            OutputBlock::Text(t) => t.text.to_node_utf8().len(),
            OutputBlock::Image(_) => 0,
        })
        .sum();
    assert!(
        aged_len < 2_500,
        "stage 2 rate-limit fallback must truncate aged tool outputs; got {aged_len} bytes"
    );
}
