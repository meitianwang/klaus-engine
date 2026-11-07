//! Defect: context snapshots exposed stale capacity and unrelated response reservations.
//! Root cause: snapshot classification did not consume the bound context policy.
//! Work item: expose actual model capacity and the same warning/admission edges.

use std::sync::Arc;

use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::ContextWindowState;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeString};
use klaus_agent_core::schemas::content::{ContentBlock, TextBlock};
use klaus_agent_core::schemas::message::{AssistantMessage, Message, Usage};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStream, ProviderCapabilities, ProviderInfo,
};

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

struct SnapshotProvider {
    info: ProviderInfo,
}
impl ModelProvider for SnapshotProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }
    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: false,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: 200_000,
            max_output_tokens: 40_000,
            native_pdf: true,
            vision: true,
            image_limits: None,
            max_media_blocks: None,
        }
    }
    fn stream(&self, _: ModelRequest, _: AbortSignal) -> ModelStream {
        panic!("a context snapshot must not call the model")
    }
}

async fn assert_snapshot(
    used: u64,
    configured_window: u64,
    automatic: bool,
    auto_window: Option<u64>,
    expected_window: u64,
    expected_threshold: u64,
    expected_state: ContextWindowState,
) {
    assert_snapshot_with_buffer(
        used,
        configured_window,
        automatic,
        auto_window,
        expected_window,
        (expected_threshold, expected_state),
        13_000,
    )
    .await;
}

async fn assert_snapshot_with_buffer(
    used: u64,
    configured_window: u64,
    automatic: bool,
    auto_window: Option<u64>,
    expected_window: u64,
    expected: (u64, ContextWindowState),
    compact_buffer: u64,
) {
    let (expected_threshold, expected_state) = expected;
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut options = agent_options(process.clone());
    options.provider = Arc::new(SnapshotProvider {
        info: ProviderInfo {
            name: "context-snapshot".into(),
            default_model: "context-snapshot".into(),
            base_url: None,
            env_key: None,
        },
    });
    let mut config = serde_json::json!({
        "model":"context-snapshot",
        "contextWindow":configured_window,
        "maxOutputTokens":1_000,
        "outputReserveTokens":20_000,
        "compactBufferTokens":compact_buffer,
        "compactThreshold":null,
        "autoCompaction":automatic,
        "prewarm":false,
    });
    if let Some(window) = auto_window {
        config["autoCompactWindow"] = serde_json::json!(window);
    }
    options.config = Some(config);
    let agent = Agent::new(options);
    let initial_history = if used == 0 {
        Vec::new()
    } else {
        vec![Message::Assistant(AssistantMessage {
            uuid: "usage-anchor".into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: "Anchored response.".into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            model: "context-snapshot".into(),
            usage: Some(Usage {
                input_tokens: used,
                output_tokens: 0,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
                total_tokens: None,
            }),
        })]
    };
    let session = agent.start_session(StartSessionOptions {
        initial_history: Some(initial_history),
        ..Default::default()
    });
    let snapshot = session.context_snapshot();
    let encoded = runtime_codec::encode_text(&snapshot).expect("snapshot encodes");
    assert!(
        snapshot.compact_threshold.is_finite(),
        "non-finite context gauge: ratio={:?}, window={}, reserve={}, threshold={}, wire={}",
        snapshot.compact_threshold,
        snapshot.context_window,
        snapshot.output_reserve_tokens,
        snapshot.compact_threshold_tokens,
        encoded.try_to_utf8().expect("snapshot JSON is scalar text")
    );
    if expected_threshold == 0 {
        assert_eq!(snapshot.compact_threshold, 0.0);
    }
    let decoded: klaus_agent_core::engine::session::SessionContextSnapshot =
        runtime_codec::decode_text(&encoded).expect("snapshot must round trip through wire text");
    assert_eq!(decoded.compact_threshold, snapshot.compact_threshold);
    assert_eq!(decoded.context_window, snapshot.context_window);
    assert_eq!(
        decoded.output_reserve_tokens,
        snapshot.output_reserve_tokens
    );
    assert_eq!(
        snapshot.total_tokens, used,
        "the authoritative usage anchor must remain unchanged"
    );
    assert_eq!(
        snapshot.context_window, expected_window,
        "the snapshot must report the bound capacity"
    );
    assert_eq!(
        snapshot.output_reserve_tokens,
        expected_window - expected_window * 95 / 100,
        "model-proportional headroom is independent of response and retired buffer preferences"
    );
    assert_eq!(snapshot.compact_threshold_tokens, expected_threshold);
    assert_eq!(
        snapshot.window_state, expected_state,
        "used={used}, automatic={automatic}, auto_window={auto_window:?}"
    );
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn snapshots_use_bound_model_capacity_and_proportional_admission_edges() {
    for (used, state) in [
        (0, ContextWindowState::Empty),
        (179_999, ContextWindowState::Normal),
        (180_000, ContextWindowState::NearLimit),
        (189_999, ContextWindowState::NearLimit),
        (190_000, ContextWindowState::OverLimit),
    ] {
        assert_snapshot(used, 800_000, true, None, 200_000, 180_000, state).await;
    }
    assert_snapshot(
        90_000,
        100_000,
        true,
        None,
        100_000,
        90_000,
        ContextWindowState::NearLimit,
    )
    .await;
}

#[tokio::test]
async fn snapshots_honor_an_earlier_automatic_window_at_exact_edges() {
    for (used, state) in [
        (99_999, ContextWindowState::Normal),
        (100_000, ContextWindowState::NearLimit),
        (189_999, ContextWindowState::NearLimit),
        (190_000, ContextWindowState::OverLimit),
    ] {
        assert_snapshot(used, 800_000, true, Some(100_000), 200_000, 100_000, state).await;
    }
}

#[tokio::test]
async fn disabled_automatic_compaction_ignores_its_window_but_keeps_actual_admission_limits() {
    for auto_window in [None, Some(100_000)] {
        for (used, state) in [
            (100_000, ContextWindowState::Normal),
            (180_000, ContextWindowState::Normal),
            (189_999, ContextWindowState::Normal),
            (190_000, ContextWindowState::OverLimit),
        ] {
            assert_snapshot(
                used,
                800_000,
                false,
                auto_window,
                200_000,
                auto_window.unwrap_or(180_000),
                state,
            )
            .await;
        }
    }
}

#[tokio::test]
async fn retired_absolute_buffers_do_not_move_snapshot_boundaries() {
    for compact_buffer in [0, 13_000, 200_000] {
        for (used, state) in [
            (179_999, ContextWindowState::Normal),
            (180_000, ContextWindowState::NearLimit),
            (190_000, ContextWindowState::OverLimit),
        ] {
            assert_snapshot_with_buffer(
                used,
                800_000,
                true,
                None,
                200_000,
                (180_000, state),
                compact_buffer,
            )
            .await;
        }
    }
}

#[tokio::test]
async fn small_windows_keep_finite_proportional_thresholds_and_wire_round_trips() {
    for (window, auto_window) in [
        (2_048, None),
        (20_000, None),
        (20_001, None),
        (200_000, Some(10_000)),
        (33_000, None),
    ] {
        let threshold = auto_window.unwrap_or(window * 90 / 100);
        assert_snapshot(
            0,
            window,
            true,
            auto_window,
            window,
            threshold,
            ContextWindowState::Empty,
        )
        .await;
        assert_snapshot(
            1,
            window,
            true,
            auto_window,
            window,
            threshold,
            ContextWindowState::Normal,
        )
        .await;
    }
}
