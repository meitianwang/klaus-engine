//! Defect: after an earlier compaction left a `CompactBoundary` in history,
//! switching models wrote in-place `persist_history_repair` records that copied
//! the old summary and projected duplicate `CompactBoundary` dividers in the UI;
//! cold model switches with large uncached tool histories below the target
//! `auto_compact_token_limit` failed with HTTP 429 `RateLimit` without running
//! reactive compaction; and speculative composer `predict_message_with_signal`
//! queries retried across establishments/accounts and fired immediately against
//! turns that had just failed with `RateLimit`.
//! Root cause: `persist_checkpoint` populated `summary` even when
//! `retires_context == false` while `is_history_repair` / `thread_index::scan`
//! treated any non-empty summary as a new compaction; `turn.rs` did not treat
//! cold-switch oversized `RateLimit` rejections as reactive compaction recovery
//! candidates; and `run_prediction_query` lacked single-attempt provider
//! streaming, retry/rotation exclusions, and post-capacity-failure suppression.
//! Work item: 2026-11-22 repair boundary, cold-switch 429 reactive compaction,
//! and prediction retry-storm fixes.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use klaus_agent_core::context::tokens::{
    estimate_context_tokens, estimate_message_tokens, estimate_prefix_tokens,
};
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{resolve_config, Input, TurnOptions};
use klaus_agent_core::provider::anthropic::{AnthropicProvider, AnthropicProviderOptions};
use klaus_agent_core::provider::retry::RetryOptions;
use klaus_agent_core::provider::rotation::RotateOutcome;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeString};
use klaus_agent_core::schemas::message::{CompactBoundaryMessage, Message, Usage};
use klaus_agent_core::schemas::rollout::{CompactedItem, RolloutItem};
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::store::thread_history::project_thread_history;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{ProviderError, ProviderErrorKind};
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

fn msg(value: serde_json::Value) -> Message {
    runtime_codec::decode_text(&value.to_string().into()).unwrap()
}

fn test_config(
    source: &str,
    model: &str,
    window: u64,
    max_retries: u32,
    context_hints: bool,
) -> serde_json::Value {
    json!({
        "model": model,
        "contextWindow": window,
        "maxOutputTokens": 2_000,
        "autoCompaction": true,
        "precomputeCompaction": false,
        "prewarm": false,
        "thinking": false,
        "loopDetection": false,
        "autoMemory": false,
        "autoDream": false,
        "memoryRecall": false,
        "contextHints": context_hints,
        "maxRetries": max_retries,
        "compactionStreamMaxRetries": 1,
        "approval_policy": "never",
        "sandbox_mode": "danger-full-access",
        "modelSource": {
            "source": source,
            "modelId": model,
            "selection": format!("{source}:{model}")
        }
    })
}

struct StaticReplyProvider {
    info: ProviderInfo,
    window: u64,
}

impl StaticReplyProvider {
    fn new(name: &str, model: &str, window: u64) -> Arc<Self> {
        Arc::new(Self {
            info: ProviderInfo {
                name: name.into(),
                default_model: model.into(),
                base_url: None,
                env_key: None,
            },
            window,
        })
    }
}

impl ModelProvider for StaticReplyProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: true,
            tools: true,
            thinking: false,
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
        let model = request.model;
        Box::pin(futures::stream::iter(vec![
            ModelStreamEvent::MessageStart { model },
            ModelStreamEvent::TextDelta {
                text: "Done.".into(),
            },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: Usage {
                    input_tokens: 1_200,
                    output_tokens: 32,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: None,
                },
            },
        ]))
    }
}

#[tokio::test]
async fn model_switch_history_repair_after_prior_compaction_does_not_project_fake_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.path().join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));

    let prior_summary = "Prior compacted video timeline checkpoint summary.";
    let initial_history = vec![
        Message::CompactBoundary(CompactBoundaryMessage {
            uuid: "cb-prior-001".into(),
            summary: prior_summary.into(),
            native_compaction: None,
        }),
        msg(json!({
            "type": "user",
            "uuid": "u-after-cb",
            "content": [{"type": "text", "text": "继续校对第三段字幕"}]
        })),
        msg(json!({
            "type": "assistant",
            "uuid": "a-after-cb",
            "model": "model-alpha",
            "content": [{"type": "text", "text": "第三段字幕校对完成"}],
            "usage": {"inputTokens": 4_000, "outputTokens": 60, "cacheReadInputTokens": 0, "cacheCreationInputTokens": 0}
        })),
    ];

    let mut options = agent_options(process.clone());
    options.provider = StaticReplyProvider::new("source-a", "model-alpha", 200_000);
    options.config = Some(test_config("source-a", "model-alpha", 200_000, 0, true));
    options.cwd = Some(dir.path().to_string_lossy().into_owned());
    options.include_default_agents = Some(false);

    let session = Agent::new(options)
        .open_session(
            store.clone(),
            StartSessionOptions {
                initial_history: Some(initial_history),
                user_config_dir: Some(dir.path().to_string_lossy().into_owned().into()),
                base_instructions: Some("Video editing assistant.".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Run one turn on model-alpha to bind context_hint_state to model-alpha.
    let first_events: Vec<_> = session
        .run_streamed(Input::Text("检查音频响度".into()), TurnOptions::default())
        .events
        .collect()
        .await;
    assert!(first_events
        .iter()
        .any(|ev| matches!(ev, EngineEvent::TurnCompleted { .. })));

    // Switch twice (model-beta, then model-gamma) without crossing compaction threshold.
    for (source, model) in [("source-b", "model-beta"), ("source-c", "model-gamma")] {
        session
            .reconfigure_inference(
                StaticReplyProvider::new(source, model, 256_000),
                resolve_config(test_config(source, model, 256_000, 0, true)).unwrap(),
                None,
                None,
                session.model_revision(),
            )
            .unwrap();
        let events: Vec<_> = session
            .run_streamed(
                Input::Text(format!("继续使用 {model} 处理").into()),
                TurnOptions::default(),
            )
            .events
            .collect()
            .await;
        assert!(events
            .iter()
            .any(|ev| matches!(ev, EngineEvent::TurnCompleted { .. })));
        assert!(
            !events
                .iter()
                .any(|ev| matches!(ev, EngineEvent::Compaction { .. })),
            "model switch below window limit must not emit Compaction"
        );
    }

    session.flush_rollout().await.unwrap();
    let thread_id = session.id.try_to_utf8().unwrap();
    let persisted = store.read_thread(&thread_id).await.unwrap();
    let repairs: Vec<_> = persisted
        .raw_items
        .iter()
        .filter_map(|item| match item {
            RolloutItem::Compacted(c) => Some(c),
            _ => None,
        })
        .collect();
    assert!(
        repairs.len() >= 2,
        "expected model switches to persist history-repair Compacted records"
    );
    for repair in &repairs {
        assert!(
            repair.summary.is_empty(),
            "in-place history repair must not copy prior CompactBoundary summary"
        );
        assert!(
            repair.is_history_repair(),
            "model-switch repair after an earlier compaction must remain classified as repair"
        );
    }

    let projected = project_thread_history(persisted.raw_items.clone());
    let projected_boundaries: Vec<_> = projected
        .history
        .iter()
        .filter_map(|m| match m {
            Message::CompactBoundary(b) => Some(b.uuid.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        projected_boundaries,
        vec![RuntimeString::from("cb-prior-001")],
        "only the original CompactBoundary may survive projection"
    );

    let page = store
        .paged_thread_history(&thread_id, None, Some(100), None, None)
        .await
        .unwrap()
        .expect("paged history");
    let paged_boundaries: Vec<_> = page
        .history
        .history
        .iter()
        .filter_map(|m| match m {
            Message::CompactBoundary(b) => Some(b.uuid.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        paged_boundaries,
        vec![RuntimeString::from("cb-prior-001")],
        "thread_index must not materialize fake CompactBoundary items for post-compaction repairs"
    );

    // Also verify backward-compatible healing of historical rollout lines that
    // previously copied `cb-prior-001`'s summary onto an in-place repair record.
    let legacy_tainted_repair = CompactedItem {
        summary: prior_summary.into(),
        replaced_range: (0, 2),
        replacement_history: Some(vec![
            Message::CompactBoundary(CompactBoundaryMessage {
                uuid: "cb-prior-001".into(),
                summary: prior_summary.into(),
                native_compaction: None,
            }),
            msg(json!({
                "type": "user",
                "uuid": "u-after-cb",
                "content": [{"type": "text", "text": "继续"}]
            })),
        ]),
        source_message_ids: Some(vec!["cb-prior-001".into(), "u-after-cb".into()]),
        approval_parent_history: None,
    };
    assert!(
        legacy_tainted_repair.is_history_repair(),
        "legacy repair record carrying only an existing prior boundary's summary must heal as repair"
    );

    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}

struct ColdSwitchRateLimitProvider {
    info: ProviderInfo,
    uncompacted_calls: Arc<AtomicUsize>,
    summary_requests: Arc<Mutex<Vec<ModelRequest>>>,
    turn_requests: Arc<Mutex<Vec<ModelRequest>>>,
}

impl ColdSwitchRateLimitProvider {
    fn new(
        name: &str,
        model: &str,
        uncompacted_calls: Arc<AtomicUsize>,
        summary_requests: Arc<Mutex<Vec<ModelRequest>>>,
        turn_requests: Arc<Mutex<Vec<ModelRequest>>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            info: ProviderInfo {
                name: name.into(),
                default_model: model.into(),
                base_url: None,
                env_key: None,
            },
            uncompacted_calls,
            summary_requests,
            turn_requests,
        })
    }
}

impl ModelProvider for ColdSwitchRateLimitProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn model_auto_compact_token_limit(&self, _: &RuntimeString) -> Option<u64> {
        Some(256_000)
    }

    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: true,
            tools: true,
            thinking: true,
            structured_output: false,
            web_search: false,
            prompt_cache_control: true,
            context_window: 1_048_576,
            max_output_tokens: 8_000,
            vision: true,
            native_pdf: false,
            image_limits: None,
            max_media_blocks: None,
            supports_image_detail_original: false,
        }
    }

    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        let estimated = estimate_context_tokens(&request.messages)
            + estimate_prefix_tokens(&request.system, &request.tools);
        let is_summary = request.tools.is_empty();
        if is_summary {
            let model = request.model.clone();
            self.summary_requests.lock().unwrap().push(request);
            return Box::pin(futures::stream::iter(vec![
                ModelStreamEvent::MessageStart { model },
                ModelStreamEvent::TextDelta {
                    text: "Compacted cold-switch summary of prior editing steps.".into(),
                },
                ModelStreamEvent::MessageStop {
                    stop_reason: ModelStopReason::EndTurn,
                    usage: Usage {
                        input_tokens: 1_800,
                        output_tokens: 90,
                        cache_read_input_tokens: 0,
                        cache_creation_input_tokens: 0,
                        total_tokens: None,
                    },
                },
            ]));
        }
        if estimated >= 32_000 {
            self.uncompacted_calls.fetch_add(1, Ordering::SeqCst);
            return Box::pin(futures::stream::once(async move {
                ModelStreamEvent::Error {
                    error: ProviderError {
                        kind: ProviderErrorKind::RateLimit,
                        message: "RESOURCE_EXHAUSTED: cold prefill budget exceeded".into(),
                        status: Some(429),
                        code: Some("RESOURCE_EXHAUSTED".into()),
                        error_type: None,
                        request_id: Some("req-cold-429".into()),
                        retry_after_ms: Some(10),
                        media_origins: None,
                        media_repair_complete: false,
                        credential_recovery: None,
                    },
                }
            }));
        }
        let model = request.model.clone();
        self.turn_requests.lock().unwrap().push(request);
        Box::pin(futures::stream::iter(vec![
            ModelStreamEvent::MessageStart { model },
            ModelStreamEvent::TextDelta {
                text: "Recovered after reactive compaction on cold model switch.".into(),
            },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: Usage {
                    input_tokens: 3_200,
                    output_tokens: 64,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: None,
                },
            },
        ]))
    }
}

#[tokio::test]
async fn cold_model_switch_rate_limit_runs_pruned_reactive_compaction_and_completes_turn() {
    let dir = tempfile::tempdir().unwrap();
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.path().join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));

    // Build ~50k estimated tokens of foreign-model history (< 256k auto_compact_token_limit,
    // >= 32k cold-prefill floor) so preflight compaction does not trigger.
    let mut initial_history = vec![msg(json!({
        "type": "user",
        "uuid": "u-0",
        "content": [{"type": "text", "text": "剪辑整条视频并输出字幕轴"}]
    }))];
    for idx in 0..16 {
        let call_id = format!("call_cold_{idx}");
        initial_history.push(msg(json!({
            "type": "assistant",
            "uuid": format!("a-cold-{idx}"),
            "model": "prior-foreign-model",
            "content": [{"type": "tool_use", "id": call_id, "name": "bash", "input": {"command": "echo segment"}}],
            "usage": {"inputTokens": 48_000, "outputTokens": 60, "cacheReadInputTokens": 0, "cacheCreationInputTokens": 0}
        })));
        initial_history.push(msg(json!({
            "type": "tool_result",
            "uuid": format!("tr-cold-{idx}"),
            "toolUseId": format!("call_cold_{idx}"),
            "content": [{"type": "text", "text": "segment_frame_data ".repeat(650)}],
            "isError": false
        })));
    }
    initial_history.push(msg(json!({
        "type": "assistant",
        "uuid": "a-cold-final",
        "model": "prior-foreign-model",
        "content": [{"type": "text", "text": "已完成初剪，等待下一步指令"}],
        "usage": {"inputTokens": 50_000, "outputTokens": 50, "cacheReadInputTokens": 0, "cacheCreationInputTokens": 0}
    })));

    let mut options = agent_options(process.clone());
    options.provider = StaticReplyProvider::new("prior-source", "prior-foreign-model", 500_000);
    options.config = Some(test_config(
        "prior-source",
        "prior-foreign-model",
        500_000,
        4,
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
                base_instructions: Some("Video assistant.".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let uncompacted_calls = Arc::new(AtomicUsize::new(0));
    let summary_requests = Arc::new(Mutex::new(Vec::new()));
    let turn_requests = Arc::new(Mutex::new(Vec::new()));
    let target_provider = ColdSwitchRateLimitProvider::new(
        "target-source",
        "target-cold-model",
        uncompacted_calls.clone(),
        summary_requests.clone(),
        turn_requests.clone(),
    );
    let mut target_cfg = resolve_config(test_config(
        "target-source",
        "target-cold-model",
        1_048_576,
        4,
        false,
    ))
    .unwrap();
    target_cfg.model_auto_compact_token_limit = Some(256_000);

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
            .run_streamed(Input::Text("继续导出字幕".into()), TurnOptions::default())
            .events
            .collect(),
    )
    .await
    .unwrap();

    assert_eq!(
        uncompacted_calls.load(Ordering::SeqCst),
        1,
        "cold-switch oversized 429 must not burn max_retries=4 repeating the full uncompacted payload"
    );
    assert!(
        events
            .iter()
            .any(|ev| matches!(ev, EngineEvent::Compaction { .. })),
        "cold-switch oversized 429 must trigger reactive compaction: {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|ev| matches!(ev, EngineEvent::TurnCompleted { .. })),
        "turn must succeed after reactive compaction shrinks the cold payload: {events:?}"
    );

    {
        let summaries = summary_requests.lock().unwrap();
        assert_eq!(
            summaries.len(),
            1,
            "reactive recovery after 429 must go straight to a single pruned lean summary request"
        );
        assert!(summaries[0].tools.is_empty());
        assert_eq!(summaries[0].system.len(), 1);
        let pruned_body_tokens: u64 = summaries[0]
            .messages
            .iter()
            .map(estimate_message_tokens)
            .sum();
        assert!(
            pruned_body_tokens < 20_000,
            "pruned lean summary must prune aged tool outputs instead of resending ~50k raw history (got {pruned_body_tokens})"
        );
    }

    {
        let turns = turn_requests.lock().unwrap();
        assert_eq!(turns.len(), 1);
        let post_compact_tokens = estimate_context_tokens(&turns[0].messages)
            + estimate_prefix_tokens(&turns[0].system, &turns[0].tools);
        assert!(post_compact_tokens < 32_000);
    }

    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}

struct MultiAccountRateLimitProvider {
    info: ProviderInfo,
    active_account: Mutex<String>,
    stream_calls: Arc<AtomicUsize>,
    rotations: Arc<AtomicUsize>,
    fail_all_with_429: std::sync::atomic::AtomicBool,
}

impl MultiAccountRateLimitProvider {
    fn new(
        name: &str,
        model: &str,
        stream_calls: Arc<AtomicUsize>,
        rotations: Arc<AtomicUsize>,
    ) -> Arc<Self> {
        Arc::new(Self {
            info: ProviderInfo {
                name: name.into(),
                default_model: model.into(),
                base_url: None,
                env_key: None,
            },
            active_account: Mutex::new("acc-primary".to_string()),
            stream_calls,
            rotations,
            fail_all_with_429: std::sync::atomic::AtomicBool::new(false),
        })
    }
}

#[async_trait::async_trait]
impl ModelProvider for MultiAccountRateLimitProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: true,
            tools: true,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: true,
            context_window: 200_000,
            max_output_tokens: 4_000,
            vision: false,
            native_pdf: false,
            image_limits: None,
            max_media_blocks: None,
            supports_image_detail_original: false,
        }
    }

    fn credential_account_id(&self) -> Option<String> {
        Some(self.active_account.lock().unwrap().clone())
    }

    async fn rotate_credential(&self, failed_id: &str) -> Option<RotateOutcome> {
        self.rotations.fetch_add(1, Ordering::SeqCst);
        let mut current = self.active_account.lock().unwrap();
        if current.as_str() == failed_id && current.as_str() == "acc-primary" {
            *current = "acc-backup".to_string();
            Some(RotateOutcome::Rotated("acc-backup".to_string()))
        } else {
            Some(RotateOutcome::Exhausted)
        }
    }

    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        self.stream_calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_all_with_429.load(Ordering::SeqCst) {
            return Box::pin(futures::stream::once(async move {
                ModelStreamEvent::Error {
                    error: ProviderError {
                        kind: ProviderErrorKind::RateLimit,
                        message: "rate limited".into(),
                        status: Some(429),
                        code: Some("rate_limit_error".into()),
                        error_type: None,
                        request_id: Some("req-pred-429".into()),
                        retry_after_ms: Some(5),
                        media_origins: None,
                        media_repair_complete: false,
                        credential_recovery: None,
                    },
                }
            }));
        }
        let model = request.model;
        Box::pin(futures::stream::iter(vec![
            ModelStreamEvent::MessageStart { model },
            ModelStreamEvent::TextDelta {
                text: r#"{"suggestion":"导出高清成片"}"#.into(),
            },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: Usage {
                    input_tokens: 400,
                    output_tokens: 20,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: None,
                },
            },
        ]))
    }
}

#[tokio::test]
async fn prediction_never_retries_or_rotates_accounts_and_skips_capacity_failed_turn_until_switched(
) {
    // 1. Verify transport-level `stream_single_attempt` on a real HTTP provider
    //    configured with multi-attempt retries + congestion hold issues at most 1 HTTP call.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let http_hits = Arc::new(AtomicUsize::new(0));
    let http_hits_server = http_hits.clone();
    let server = tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            http_hits_server.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 4096];
            let _ = sock.read(&mut buf).await;
            let body =
                r#"{"type":"error","error":{"type":"rate_limit_error","message":"throttled"}}"#;
            let resp = format!(
                "HTTP/1.1 429 Too Many Requests\r\nContent-Type: application/json\r\nRetry-After: 0\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = sock.write_all(resp.as_bytes()).await;
        }
    });

    let dir = tempfile::tempdir().unwrap();
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.path().join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));

    let http_provider = Arc::new(AnthropicProvider::new(AnthropicProviderOptions {
        api_key: Some("test-key".into()),
        base_url: Some(format!("http://{addr}")),
        default_model: Some("http-throttled-model".into()),
        retry: Some(RetryOptions {
            max_attempts: Some(4),
            base_delay_ms: Some(1),
            max_delay_ms: Some(5),
            congestion_hold_ms: Some(30_000),
            ..Default::default()
        }),
        ..Default::default()
    }));

    let stream_calls = Arc::new(AtomicUsize::new(0));
    let rotations = Arc::new(AtomicUsize::new(0));
    let pool_provider = MultiAccountRateLimitProvider::new(
        "pool-source",
        "pool-model",
        stream_calls.clone(),
        rotations.clone(),
    );

    let mut options = agent_options(process.clone());
    options.provider = pool_provider.clone();
    options.config = Some(test_config("pool-source", "pool-model", 200_000, 4, false));
    options.cwd = Some(dir.path().to_string_lossy().into_owned());
    options.include_default_agents = Some(false);

    let session = Agent::new(options)
        .open_session(
            store,
            StartSessionOptions {
                user_config_dir: Some(dir.path().to_string_lossy().into_owned().into()),
                base_instructions: Some("Assistant.".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Complete turn 1 normally so we have a valid prediction source turn.
    let ok_events: Vec<_> = session
        .run_streamed(Input::Text("第一轮成功".into()), TurnOptions::default())
        .events
        .collect()
        .await;
    assert!(ok_events
        .iter()
        .any(|ev| matches!(ev, EngineEvent::TurnCompleted { .. })));
    let completed_turn_id = session
        .prediction_context()
        .await
        .unwrap()
        .turn_id
        .expect("completed turn id")
        .try_to_utf8()
        .unwrap();

    // Flip pool_provider into 429 mode and run a speculative prediction:
    // must issue ONLY 1 attempt and 0 credential rotations despite maxRetries=4!
    pool_provider
        .fail_all_with_429
        .store(true, Ordering::SeqCst);
    stream_calls.store(0, Ordering::SeqCst);
    rotations.store(0, Ordering::SeqCst);

    let pred_after_ok = session
        .predict_message_with_signal(
            "Predict next user prompt as JSON {\"prompt\":\"...\"}".to_string(),
            completed_turn_id.clone(),
            None,
            AbortSignal::new(),
        )
        .await;
    assert!(
        pred_after_ok.is_err(),
        "rate-limited prediction must return Err so host treats it as retryable prediction_failed"
    );
    assert_eq!(
        stream_calls.load(Ordering::SeqCst),
        1,
        "prediction query must stop after 1 attempt without turn-level retries"
    );
    assert_eq!(
        rotations.load(Ordering::SeqCst),
        0,
        "background prediction must never rotate the user's subscription account pool"
    );
    assert_eq!(
        pool_provider.credential_account_id().as_deref(),
        Some("acc-primary")
    );

    // Also switch to the real HTTP provider (with max_attempts=4 + congestion_hold_ms=30_000)
    // and verify `stream_single_attempt` makes strictly 1 HTTP call with zero congestion hold.
    session
        .reconfigure_inference(
            http_provider,
            resolve_config(test_config(
                "anthropic",
                "http-throttled-model",
                200_000,
                4,
                false,
            ))
            .unwrap(),
            None,
            None,
            session.model_revision(),
        )
        .unwrap();
    let http_pred = tokio::time::timeout(
        Duration::from_secs(5),
        session.predict_message_with_signal(
            "Predict next user prompt".to_string(),
            completed_turn_id,
            None,
            AbortSignal::new(),
        ),
    )
    .await
    .expect("single-attempt HTTP prediction must not hang in congestion hold");
    assert!(http_pred.is_err());
    assert_eq!(
        http_hits.load(Ordering::SeqCst),
        1,
        "provider establishment retry must be capped to 1 HTTP request for predictions"
    );

    // 2. Now run a foreground turn on `pool_provider` that fails with 429 RateLimit,
    //    and verify immediate prediction against that failed turn is suppressed with 0 calls
    //    until the user switches to a healthy model.
    session
        .reconfigure_inference(
            pool_provider.clone(),
            resolve_config(test_config("pool-source", "pool-model", 200_000, 0, false)).unwrap(),
            None,
            None,
            session.model_revision(),
        )
        .unwrap();
    let fail_events: Vec<_> = session
        .run_streamed(Input::Text("这轮被限流".into()), TurnOptions::default())
        .events
        .collect()
        .await;
    assert!(fail_events
        .iter()
        .any(|ev| matches!(ev, EngineEvent::TurnFailed { .. })));

    let failed_turn_id = session
        .prediction_context()
        .await
        .unwrap()
        .turn_id
        .expect("failed turn id")
        .try_to_utf8()
        .unwrap();
    stream_calls.store(0, Ordering::SeqCst);

    let suppressed = session
        .predict_message_with_signal(
            "Predict next user prompt".to_string(),
            failed_turn_id.clone(),
            None,
            AbortSignal::new(),
        )
        .await;
    assert!(
        suppressed.is_err(),
        "prediction against a turn that just failed with RateLimit on the same model/account must fail fast"
    );
    assert_eq!(
        stream_calls.load(Ordering::SeqCst),
        0,
        "suppressed post-429 prediction must issue zero upstream calls"
    );

    // Switching to a healthy model lifts suppression for that turn so UI recheck succeeds.
    let healthy_calls = Arc::new(AtomicUsize::new(0));
    let healthy_rotations = Arc::new(AtomicUsize::new(0));
    let healthy_provider = MultiAccountRateLimitProvider::new(
        "healthy-source",
        "healthy-model",
        healthy_calls.clone(),
        healthy_rotations,
    );
    session
        .reconfigure_inference(
            healthy_provider,
            resolve_config(test_config(
                "healthy-source",
                "healthy-model",
                200_000,
                0,
                false,
            ))
            .unwrap(),
            None,
            None,
            session.model_revision(),
        )
        .unwrap();

    let recovered = session
        .predict_message_with_signal(
            "Predict next user prompt".to_string(),
            failed_turn_id,
            None,
            AbortSignal::new(),
        )
        .await
        .unwrap();
    assert_eq!(recovered, Some(RuntimeString::from("导出高清成片")));
    assert_eq!(healthy_calls.load(Ordering::SeqCst), 1);

    server.abort();
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}
