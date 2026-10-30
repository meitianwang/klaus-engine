//! Fixing commit: 0a2ef4598881a6dcdbcb8a78254b6e792229f578.
//! Repeated immutable prefix concatenation copied the full text for each delta.
//! Preserve exact code units and message identity while accumulating small deltas.
//! Measurements exercise public streamed sessions rather than a replacement accumulator.

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};
use futures::StreamExt;
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{Input, TurnOptions};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::RuntimeString;
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::{Message, Usage};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::events::{EngineEvent, ThreadItem};
use klaus_agent_core::types::provider::{
    ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo,
};
use serde_json::json;
use std::sync::Arc;
use std::time::Instant;

struct ChunkProvider {
    info: ProviderInfo,
    chunks: Vec<RuntimeString>,
}

impl ModelProvider for ChunkProvider {
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
            context_window: 2_000_000,
            max_output_tokens: 1_000_000,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    fn stream(&self, _: ModelRequest, _: AbortSignal) -> ModelStream {
        let mut events = vec![
            ModelStreamEvent::MessageStart {
                model: "text-buffer-fixture".into(),
            },
            ModelStreamEvent::TextMessage {
                id: "provider-text".into(),
                phase: Some("final_answer".into()),
            },
        ];
        events.extend(
            self.chunks
                .iter()
                .cloned()
                .map(|text| ModelStreamEvent::TextDelta { text }),
        );
        events.push(ModelStreamEvent::MessageStop {
            stop_reason: ModelStopReason::EndTurn,
            usage: Usage {
                input_tokens: 10,
                output_tokens: 20,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
                total_tokens: None,
            },
        });
        Box::pin(futures::stream::iter(events))
    }
}

async fn stream(chunks: Vec<RuntimeString>) {
    let expected: Vec<u16> = chunks
        .iter()
        .flat_map(|chunk| chunk.as_utf16().iter().copied())
        .collect();
    let chunk_count = chunks.len();
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let root = tempfile::tempdir().unwrap();
    let mut options = agent_options(process.clone());
    options.provider = Arc::new(ChunkProvider {
        info: ProviderInfo {
            name: "text-buffer-fixture".into(),
            default_model: "text-buffer-fixture".into(),
            base_url: None,
            env_key: None,
        },
        chunks,
    });
    options.tools = Some(Vec::new());
    options.config = Some(json!({
        "model": "text-buffer-fixture", "prewarm": false,
        "goal": false, "autoMemory": false, "autoDream": false,
        "approval_policy": "never", "sandbox_mode": "danger-full-access"
    }));
    let directory = root.path().to_string_lossy().into_owned();
    let session = Agent::new(options).start_session(StartSessionOptions {
        cwd: Some(directory.clone().into()),
        user_config_dir: Some(directory.into()),
        ..Default::default()
    });
    let started = Instant::now();
    let events: Vec<_> = session
        .run_streamed(
            Input::Text("Produce the requested text".into()),
            TurnOptions::default(),
        )
        .events
        .collect()
        .await;
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::TurnFailed { .. })),
        "the public turn must complete"
    );
    let deltas: Vec<u16> = events
        .iter()
        .filter_map(|event| match event {
            EngineEvent::TextDelta { text, .. } => Some(text.as_utf16()),
            _ => None,
        })
        .flatten()
        .copied()
        .collect();
    assert_eq!(deltas, expected, "live deltas retain exact code units");
    let completed = events
        .iter()
        .find_map(|event| match event {
            EngineEvent::ItemCompleted {
                item: ThreadItem::AgentMessage { id, text, phase },
                ..
            } => Some((id, text, phase)),
            _ => None,
        })
        .expect("the displayed message must complete");
    assert_eq!(completed.1.as_utf16(), expected);
    assert_eq!(completed.2.as_ref().unwrap(), "final_answer");
    for event in &events {
        if let EngineEvent::TextDelta { message_id, .. } = event {
            assert_eq!(message_id.as_ref(), Some(completed.0));
        }
    }
    let history = session.history();
    let saved = history
        .iter()
        .rev()
        .find_map(|message| match message {
            Message::Assistant(message) => Some(message),
            _ => None,
        })
        .expect("the assistant message must be persisted in session history");
    let text: Vec<u16> = saved
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text(text) => Some(text.text.as_utf16()),
            _ => None,
        })
        .flatten()
        .copied()
        .collect();
    assert_eq!(
        text, expected,
        "saved content agrees with the displayed message"
    );
    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
    eprintln!(
        "STREAM_TEXT_MEASUREMENT {}",
        json!({ "chunks": chunk_count,
        "codeUnits": expected.len(), "elapsedMs": elapsed_ms, "events": events.len() })
    );
}

#[tokio::test]
async fn streamed_text_preserves_split_and_unpaired_surrogates() {
    stream(vec![
        RuntimeString::from_utf16(vec![0x61, 0xd83e]),
        RuntimeString::from_utf16(vec![0xdded, 0xd800]),
        RuntimeString::from_utf16(vec![0x7a, 0xdc00]),
    ])
    .await;
}

#[tokio::test]
async fn streamed_text_small_chunks_preserve_full_public_output() {
    let text = (0..4096)
        .map(|index| format!("line-{index:08}-文本-🧭\n"))
        .collect::<String>();
    let units: Vec<u16> = text.encode_utf16().collect();
    stream(
        units
            .chunks(16)
            .map(|chunk| RuntimeString::from_utf16(chunk.to_vec()))
            .collect(),
    )
    .await;
}

#[tokio::test]
async fn streamed_text_large_chunks_preserve_full_public_output() {
    let text = (0..4096)
        .map(|index| format!("line-{index:08}-文本-🧭\n"))
        .collect::<String>();
    let units: Vec<u16> = text.encode_utf16().collect();
    stream(
        units
            .chunks(1024)
            .map(|chunk| RuntimeString::from_utf16(chunk.to_vec()))
            .collect(),
    )
    .await;
}
