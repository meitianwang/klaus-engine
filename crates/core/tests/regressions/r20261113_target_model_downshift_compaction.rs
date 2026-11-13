//! Defect: switching from an exhausted large-window model source to a smaller-window
//! model source invoked the exhausted old source for compaction and stalled the chat.
//! Root cause: `InferenceBundle` retained the deselected source and `model_context_migration`
//! summarized on it (and explicitly refused fallback on 429 quota errors).
//! Work item: 2026-11-13 target-only downshift compaction with pre-pruning, chunking,
//! comp_hash local expansion, and historical document/image projection.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use klaus_agent_core::context::tokens::{estimate_context_tokens, estimate_prefix_tokens};
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::{resolve_config, Input, TurnOptions, UserContentPart};
use klaus_agent_core::provider::openai::{OpenAIProvider, OpenAIProviderOptions, OpenAiApi};
use klaus_agent_core::provider::review_target::with_review_model_catalog;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeString};
use klaus_agent_core::schemas::content::ContentBlock;
use klaus_agent_core::schemas::message::{Message, NativeCompactionCheckpoint, Usage};
use klaus_agent_core::schemas::rollout::RolloutItem;
use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
use klaus_agent_core::store::session_store::SessionStore;
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{EngineErrorCode, ProviderError, ProviderErrorKind};
use klaus_agent_core::types::events::EngineEvent;
use klaus_agent_core::types::provider::{
    ModelCapabilities, ModelProvider, ModelRequest, ModelStopReason, ModelStream, ModelStreamEvent,
    ProviderCapabilities, ProviderInfo, RemoteCompactionSupport,
};
use serde_json::json;

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};

const LARGE_WINDOW: u64 = 500_000;
const SMALL_WINDOW: u64 = 32_000;
const TINY_WINDOW: u64 = 16_000;

fn msg(value: serde_json::Value) -> Message {
    runtime_codec::decode_text(&value.to_string().into()).unwrap()
}

fn encode_messages(messages: &[Message]) -> String {
    let encoded = runtime_codec::encode_bytes(&messages.to_vec()).unwrap();
    String::from_utf8(encoded).unwrap()
}

struct ExhaustedOldProvider {
    info: ProviderInfo,
    error_kind: ProviderErrorKind,
    calls: Arc<AtomicUsize>,
}

impl ExhaustedOldProvider {
    fn new(error_kind: ProviderErrorKind, calls: Arc<AtomicUsize>) -> Arc<Self> {
        Arc::new(Self {
            info: ProviderInfo {
                name: "exhausted-large-source".into(),
                default_model: "large-model".into(),
                base_url: None,
                env_key: None,
            },
            error_kind,
            calls,
        })
    }
}

#[async_trait]
impl ModelProvider for ExhaustedOldProvider {
    fn info(&self) -> &ProviderInfo {
        &self.info
    }

    fn capabilities(&self, _: &RuntimeString) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: true,
            tools: true,
            thinking: true,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: LARGE_WINDOW,
            max_output_tokens: 4_000,
            vision: true,
            native_pdf: true,
            image_limits: None,
            max_media_blocks: None,
            supports_image_detail_original: false,
        }
    }

    fn stream(&self, _: ModelRequest, _: AbortSignal) -> ModelStream {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(futures::stream::iter([ModelStreamEvent::Error {
            error: ProviderError {
                kind: self.error_kind,
                message: "Old model source quota is completely exhausted".into(),
                status: Some(429),
                code: Some("insufficient_quota".into()),
                error_type: None,
                retry_after_ms: None,
                request_id: None,
                media_origins: None,
                media_repair_complete: false,
                credential_recovery: None,
            },
        }]))
    }
}

struct SmallTargetProvider {
    info: ProviderInfo,
    window: u64,
    vision: bool,
    native_pdf: bool,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
}

impl SmallTargetProvider {
    fn new(
        window: u64,
        vision: bool,
        native_pdf: bool,
        requests: Arc<Mutex<Vec<ModelRequest>>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            info: ProviderInfo {
                name: "small-target-source".into(),
                default_model: "small-model".into(),
                base_url: None,
                env_key: None,
            },
            window,
            vision,
            native_pdf,
            requests,
        })
    }
}

impl ModelProvider for SmallTargetProvider {
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
            prompt_cache_control: false,
            context_window: self.window,
            max_output_tokens: 1_000,
            vision: self.vision,
            native_pdf: self.native_pdf,
            image_limits: None,
            max_media_blocks: None,
            supports_image_detail_original: false,
        }
    }

    fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
        let total_tokens = estimate_context_tokens(&request.messages)
            + estimate_prefix_tokens(&request.system, &request.tools);
        if total_tokens >= self.window {
            return Box::pin(futures::stream::iter([ModelStreamEvent::Error {
                error: ProviderError {
                    kind: ProviderErrorKind::PromptTooLong,
                    message: format!(
                        "Request of {total_tokens} tokens exceeds context window {}",
                        self.window
                    )
                    .into(),
                    status: Some(400),
                    code: Some("context_length_exceeded".into()),
                    error_type: None,
                    retry_after_ms: None,
                    request_id: None,
                    media_origins: None,
                    media_repair_complete: false,
                    credential_recovery: None,
                },
            }]));
        }
        let is_summary = request.skip_cache_write == Some(true);
        self.requests.lock().unwrap().push(request);
        let reply = if is_summary {
            "Structured checkpoint: preserved ORIGINAL_GOAL, src/engine.rs edits, and verified state."
        } else {
            "Small target model answered using the compacted checkpoint."
        };
        Box::pin(futures::stream::iter([
            ModelStreamEvent::MessageStart {
                model: "small-model".into(),
            },
            ModelStreamEvent::TextDelta { text: reply.into() },
            ModelStreamEvent::MessageStop {
                stop_reason: ModelStopReason::EndTurn,
                usage: Usage {
                    input_tokens: 200,
                    output_tokens: 30,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    total_tokens: None,
                },
            },
        ]))
    }
}

fn source_config(name: &str, model: &str, window: u64) -> serde_json::Value {
    json!({
        "model": model,
        "contextWindow": window,
        "maxOutputTokens": 1_000,
        "autoCompaction": true,
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

#[tokio::test]
async fn quota_and_auth_exhausted_old_source_never_blocks_downshift_to_smaller_model() {
    for old_error in [ProviderErrorKind::Quota, ProviderErrorKind::Auth] {
        let dir = tempfile::tempdir().unwrap();
        let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
        let old_calls = Arc::new(AtomicUsize::new(0));
        let target_requests = Arc::new(Mutex::new(Vec::new()));
        let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
            dir: Some(dir.path().join("sessions").to_string_lossy().into_owned()),
            archive_dir: None,
        }));

        // Build ~55K tokens of aged tool logs + reasoning (> 32K target window),
        // which pre-pruning shrinks below the 32K target window for a 1-pass summary.
        let mut history = vec![
            msg(
                json!({"type":"user","uuid":"u-goal","content":[{"type":"text","text":"ORIGINAL_GOAL: refactor src/engine.rs without breaking callers."}]}),
            ),
            msg(
                json!({"type":"assistant","uuid":"a-call-1","model":"large-model","content":[
                    {"type":"thinking","text": "r".repeat(30_000), "signature":"sig-old"},
                    {"type":"tool_use","id":"call-read-1","name":"read","input":{"path":"src/engine.rs"}}
                ]}),
            ),
            msg(
                json!({"type":"tool_result","uuid":"tr-1","toolUseId":"call-read-1","content":[{"type":"text","text": format!("HEAD_MARKER\n{}\nTAIL_MARKER", "x".repeat(160_000))}],"isError":false}),
            ),
            msg(
                json!({"type":"assistant","uuid":"a-call-2","model":"large-model","content":[
                    {"type":"tool_use","id":"call-edit-1","name":"edit","input":{"path":"src/engine.rs","oldString":"a","newString":"b"}}
                ]}),
            ),
            msg(
                json!({"type":"tool_result","uuid":"tr-2","toolUseId":"call-edit-1","content":[{"type":"text","text":"Edited src/engine.rs"}],"isError":false}),
            ),
        ];
        for i in 0..8 {
            history.push(msg(json!({"type":"user","uuid":format!("u-recent-{i}"),"content":[{"type":"text","text":format!("Follow-up check {i}")}]})));
            history.push(msg(json!({"type":"assistant","uuid":format!("a-recent-{i}"),"model":"large-model","content":[{"type":"text","text":format!("Verified check {i}")}]})));
        }

        let mut options = agent_options(process.clone());
        options.provider = ExhaustedOldProvider::new(old_error, old_calls.clone());
        options.config = Some(source_config(
            "exhausted-large-source",
            "large-model",
            LARGE_WINDOW,
        ));
        options.cwd = Some(dir.path().to_string_lossy().into_owned());
        options.include_default_agents = Some(false);

        let session = Agent::new(options)
            .open_session(
                store.clone(),
                StartSessionOptions {
                    initial_history: Some(history),
                    user_config_dir: Some(dir.path().to_string_lossy().into_owned().into()),
                    base_instructions: Some("System instructions for the coding session.".into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // Switch to the smaller-window model after the old source ran out of quota.
        session
            .reconfigure_inference(
                SmallTargetProvider::new(SMALL_WINDOW, true, true, target_requests.clone()),
                resolve_config(source_config(
                    "small-target-source",
                    "small-model",
                    SMALL_WINDOW,
                ))
                .unwrap(),
                None,
                None,
                session.model_revision(),
            )
            .unwrap();

        let events: Vec<_> = tokio::time::timeout(
            Duration::from_secs(15),
            session
                .run_streamed(
                    Input::Text("Continue with the final verification step.".into()),
                    TurnOptions::default(),
                )
                .events
                .collect(),
        )
        .await
        .unwrap();

        assert!(
            !events
                .iter()
                .any(|e| matches!(e, EngineEvent::TurnFailed { .. })),
            "downshift must succeed without error: {events:?}"
        );
        assert!(events
            .iter()
            .any(|e| matches!(e, EngineEvent::Compaction { .. })));
        assert_eq!(
            old_calls.load(Ordering::SeqCst),
            0,
            "the exhausted deselected model source must receive zero calls"
        );

        {
            let recorded = target_requests.lock().unwrap();
            // Pre-pruning capped the 160KB aged tool output and dropped the 30KB
            // old thinking block, so the target model needed only 1 summary call
            // plus 1 normal turn call, with tools stripped on the summary call.
            assert_eq!(
                recorded.len(),
                2,
                "expected 1 lean summary + 1 turn request"
            );
            assert_eq!(recorded[0].skip_cache_write, Some(true));
            assert!(recorded[0].tools.is_empty());
            let summary_payload = encode_messages(&recorded[0].messages);
            assert!(summary_payload.contains("HEAD_MARKER"));
            assert!(summary_payload.contains("TAIL_MARKER"));
            assert!(
                summary_payload.contains("Modified files in this segment: src/engine.rs"),
                "file-operation tracking note must accompany the summary prompt: {summary_payload}"
            );
            assert!(!summary_payload.contains("sig-old"));

            assert_ne!(recorded[1].skip_cache_write, Some(true));
            let turn_payload = encode_messages(&recorded[1].messages);
            assert!(turn_payload.contains("Structured checkpoint"));
            assert!(turn_payload.contains("Continue with the final verification step."));
        }

        // Verify the durable raw rollout still retains the untouched 160KB tool output.
        session.flush_rollout().await.unwrap();
        let persisted = store
            .read_thread(&session.id.try_to_utf8().unwrap())
            .await
            .unwrap();
        assert!(persisted.raw_items.iter().any(|item| matches!(
            item,
            RolloutItem::Message {
                message: Message::ToolResult(tr)
            } if tr.uuid == "tr-1" && encode_messages(&[Message::ToolResult(tr.clone())]).len() > 150_000
        )));

        session.shutdown_and_wait().await.unwrap();
        process.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn dense_conversational_history_chunks_at_closed_tool_boundaries_on_target_model() {
    let dir = tempfile::tempdir().unwrap();
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let old_calls = Arc::new(AtomicUsize::new(0));
    let target_requests = Arc::new(Mutex::new(Vec::new()));

    // Build dense user/assistant/tool exchanges whose prose exceeds TINY_WINDOW (16K)
    // even after pre-pruning, requiring multi-chunk summarization on the target model.
    let mut history = vec![msg(json!({
        "type": "user",
        "uuid": "u-first",
        "content": [{"type": "text", "text": "ORIGINAL_REQUIREMENT_ANCHOR: keep every architectural constraint."}]
    }))];
    for i in 0..18 {
        let call_id = format!("paired-call-{i}");
        history.push(msg(json!({
            "type": "assistant",
            "uuid": format!("a-{i}"),
            "model": "large-model",
            "content": [
                {"type": "text", "text": format!("ROUND_{i}: {}", "Detailed architectural analysis sentence. ".repeat(80))},
                {"type": "tool_use", "id": call_id, "name": "read", "input": {"path": format!("src/mod_{i}.rs")}}
            ]
        })));
        history.push(msg(json!({
            "type": "tool_result",
            "uuid": format!("tr-{i}"),
            "toolUseId": call_id,
            "content": [{"type": "text", "text": format!("RESULT_{i}: module verified.")}],
            "isError": false
        })));
    }

    let mut options = agent_options(process.clone());
    options.provider = ExhaustedOldProvider::new(ProviderErrorKind::Quota, old_calls.clone());
    options.config = Some(source_config(
        "exhausted-large-source",
        "large-model",
        LARGE_WINDOW,
    ));
    options.cwd = Some(dir.path().to_string_lossy().into_owned());
    options.include_default_agents = Some(false);

    let session = Agent::new(options).start_session(StartSessionOptions {
        initial_history: Some(history),
        user_config_dir: Some(dir.path().to_string_lossy().into_owned().into()),
        base_instructions: Some("Continue the multi-module architecture task.".into()),
        ..Default::default()
    });

    session
        .reconfigure_inference(
            SmallTargetProvider::new(TINY_WINDOW, true, true, target_requests.clone()),
            resolve_config(source_config(
                "small-target-source",
                "small-model",
                TINY_WINDOW,
            ))
            .unwrap(),
            None,
            None,
            session.model_revision(),
        )
        .unwrap();

    let events: Vec<_> = session
        .run_streamed(
            Input::Text("Summarize all 18 modules and proceed.".into()),
            TurnOptions::default(),
        )
        .events
        .collect()
        .await;

    assert!(
        !events
            .iter()
            .any(|e| matches!(e, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
    assert_eq!(old_calls.load(Ordering::SeqCst), 0);

    {
        let recorded = target_requests.lock().unwrap();
        assert!(
            recorded.len() >= 3,
            "expected multiple bounded chunk summaries plus continuation, got {}",
            recorded.len()
        );
        let mut all_summary_inputs = String::new();
        for req in &recorded[..recorded.len() - 1] {
            assert_eq!(req.skip_cache_write, Some(true));
            let tokens = estimate_context_tokens(&req.messages)
                + estimate_prefix_tokens(&req.system, &req.tools);
            assert!(
                tokens < TINY_WINDOW,
                "chunk request ({tokens} tokens) exceeded target window {TINY_WINDOW}"
            );
            // Verify no tool_use was separated from its matching tool_result.
            let mut open = std::collections::HashSet::new();
            for message in &req.messages {
                match message {
                    Message::Assistant(a) => {
                        for block in &a.content {
                            if let ContentBlock::ToolUse(call) = block {
                                open.insert(call.id.clone());
                            }
                        }
                    }
                    Message::ToolResult(tr) => {
                        assert!(
                            open.remove(&tr.tool_use_id),
                            "orphaned tool_result {:?} in chunk",
                            tr.tool_use_id
                        );
                    }
                    _ => {}
                }
            }
            assert!(open.is_empty(), "unclosed tool_use in chunk: {open:?}");
            all_summary_inputs.push_str(&encode_messages(&req.messages));
        }
        assert!(
            all_summary_inputs.contains("ORIGINAL_REQUIREMENT_ANCHOR"),
            "earliest requirement must be summarized rather than dropped"
        );
        for i in 0..18 {
            assert!(
                all_summary_inputs.contains(&format!("ROUND_{i}:")),
                "round {i} was dropped instead of summarized"
            );
        }
    }

    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn historical_images_and_pdfs_project_to_text_notes_while_fresh_pdf_is_still_validated() {
    use base64::Engine;
    let dir = tempfile::tempdir().unwrap();
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let old_calls = Arc::new(AtomicUsize::new(0));
    let target_requests = Arc::new(Mutex::new(Vec::new()));

    let mut png = Vec::new();
    image::RgbImage::from_pixel(16, 16, image::Rgb([10, 20, 30]))
        .write_with_encoder(image::codecs::png::PngEncoder::new(&mut png))
        .unwrap();
    let img_b64 = base64::engine::general_purpose::STANDARD.encode(png);

    let history = vec![
        msg(json!({
            "type": "user",
            "uuid": "u-multimodal",
            "content": [
                {"type": "text", "text": "Inspect this screenshot and architecture spec."},
                {"type": "image", "mimeType": "image/png", "base64": img_b64},
                {"type": "document", "mimeType": "application/pdf", "base64": "JVBERi0xLjcK", "filename": "arch.pdf"}
            ]
        })),
        msg(json!({
            "type": "assistant",
            "uuid": "a-multimodal",
            "model": "large-model",
            "content": [{"type": "text", "text": "d".repeat(160_000)}]
        })),
    ];

    let mut options = agent_options(process.clone());
    options.provider = ExhaustedOldProvider::new(ProviderErrorKind::Quota, old_calls.clone());
    options.config = Some(source_config(
        "exhausted-large-source",
        "large-model",
        LARGE_WINDOW,
    ));
    options.cwd = Some(dir.path().to_string_lossy().into_owned());
    options.include_default_agents = Some(false);

    let session = Agent::new(options).start_session(StartSessionOptions {
        initial_history: Some(history),
        user_config_dir: Some(dir.path().to_string_lossy().into_owned().into()),
        base_instructions: Some("Continue multimodal session on text-only model.".into()),
        ..Default::default()
    });

    // Switch to a text-only smaller model (vision=false, native_pdf=false).
    session
        .reconfigure_inference(
            SmallTargetProvider::new(SMALL_WINDOW, false, false, target_requests.clone()),
            resolve_config(source_config(
                "small-target-source",
                "small-model",
                SMALL_WINDOW,
            ))
            .unwrap(),
            None,
            None,
            session.model_revision(),
        )
        .unwrap();

    let events: Vec<_> = session
        .run_streamed(
            Input::Text("Continue with plain text.".into()),
            TurnOptions::default(),
        )
        .events
        .collect()
        .await;

    assert!(
        !events
            .iter()
            .any(|e| matches!(e, EngineEvent::TurnFailed { .. })),
        "historical images/PDFs must degrade to notes instead of rejecting the turn: {events:?}"
    );
    assert_eq!(old_calls.load(Ordering::SeqCst), 0);
    {
        let recorded = target_requests.lock().unwrap();
        for req in recorded.iter() {
            let serialized = encode_messages(&req.messages);
            assert!(!serialized.contains("\"type\":\"image\""));
            assert!(!serialized.contains("\"type\":\"document\""));
        }
        let summary_input = encode_messages(&recorded[0].messages);
        assert!(summary_input.contains("[image attachment]"));
        assert!(summary_input.contains("[PDF document: arch.pdf]"));
        let final_turn = encode_messages(&recorded.last().unwrap().messages);
        assert!(final_turn.contains("Inspect this screenshot and architecture spec."));
        assert!(final_turn.contains("Continue with plain text."));
    }

    // Also verify uncompacted multimodal history switched directly onto a
    // blind/non-PDF model omits both image and PDF blocks on the live request.
    let direct_requests = Arc::new(Mutex::new(Vec::new()));
    let mut direct_opts = agent_options(process.clone());
    direct_opts.provider = ExhaustedOldProvider::new(ProviderErrorKind::Quota, old_calls.clone());
    direct_opts.config = Some(source_config(
        "exhausted-large-source",
        "large-model",
        LARGE_WINDOW,
    ));
    direct_opts.cwd = Some(dir.path().to_string_lossy().into_owned());
    direct_opts.include_default_agents = Some(false);
    let direct_session = Agent::new(direct_opts).start_session(StartSessionOptions {
        initial_history: Some(vec![msg(json!({
            "type": "user",
            "uuid": "u-fitting-mm",
            "content": [
                {"type": "text", "text": "Short multimodal turn."},
                {"type": "image", "mimeType": "image/png", "base64": img_b64},
                {"type": "document", "mimeType": "application/pdf", "base64": "JVBERi0xLjcK", "filename": "spec.pdf"}
            ]
        }))]),
        user_config_dir: Some(dir.path().to_string_lossy().into_owned().into()),
        base_instructions: Some("Continue.".into()),
        ..Default::default()
    });
    direct_session
        .reconfigure_inference(
            SmallTargetProvider::new(SMALL_WINDOW, false, false, direct_requests.clone()),
            resolve_config(source_config(
                "small-target-source",
                "small-model",
                SMALL_WINDOW,
            ))
            .unwrap(),
            None,
            None,
            direct_session.model_revision(),
        )
        .unwrap();
    let direct_events: Vec<_> = direct_session
        .run_streamed(Input::Text("Follow up.".into()), TurnOptions::default())
        .events
        .collect()
        .await;
    assert!(!direct_events
        .iter()
        .any(|e| matches!(e, EngineEvent::TurnFailed { .. })));
    {
        let direct_recorded = direct_requests.lock().unwrap();
        assert_eq!(direct_recorded.len(), 1);
        let payload = encode_messages(&direct_recorded[0].messages);
        assert!(payload.contains("image was omitted"));
        assert!(payload.contains("PDF document was omitted"));
    }
    direct_session.shutdown_and_wait().await.unwrap();

    // Sending a NEW PDF directly to the PDF-incapable model must still be rejected explicitly.
    let fresh_pdf_events: Vec<_> = session
        .run_streamed(
            Input::Parts(vec![UserContentPart::Document {
                mime_type: "application/pdf".into(),
                base64: "JVBERi0xLjcK".into(),
                filename: Some("fresh.pdf".into()),
            }]),
            TurnOptions::default(),
        )
        .events
        .collect()
        .await;
    assert!(
        fresh_pdf_events.iter().any(|e| matches!(
            e,
            EngineEvent::TurnFailed { error, .. } if error.code == EngineErrorCode::InvalidInput
        )),
        "fresh PDF on PDF-incapable model must still fail validation: {fresh_pdf_events:?}"
    );

    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn openai_native_checkpoint_rejects_mismatched_comp_hash_on_same_endpoint() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let base = OpenAIProvider::new(OpenAIProviderOptions {
        api: Some(OpenAiApi::Responses),
        api_key: Some("fixture-key".into()),
        base_url: Some("http://127.0.0.1:9/v1".into()),
        default_model: Some("model-a".into()),
        remote_compaction: RemoteCompactionSupport::ResponsesV2,
        ..Default::default()
    });
    let cap_a: ModelCapabilities = runtime_codec::decode_text(
        &json!({"contextWindow": 200_000, "maxOutputTokens": 4_096, "compHash": "hash-v1"})
            .to_string()
            .into(),
    )
    .unwrap();
    let cap_b: ModelCapabilities = runtime_codec::decode_text(
        &json!({"contextWindow": 200_000, "maxOutputTokens": 4_096, "compHash": "hash-v2"})
            .to_string()
            .into(),
    )
    .unwrap();
    let cap_unhashed: ModelCapabilities = runtime_codec::decode_text(
        &json!({"contextWindow": 200_000, "maxOutputTokens": 4_096})
            .to_string()
            .into(),
    )
    .unwrap();
    let provider: Arc<dyn ModelProvider> = with_review_model_catalog(
        Arc::new(base),
        Arc::new({
            let cap_a = cap_a.clone();
            let cap_b = cap_b.clone();
            let cap_unhashed = cap_unhashed.clone();
            move || {
                vec![
                    ("model-a".into(), cap_a.clone()),
                    ("model-b".into(), cap_b.clone()),
                    ("model-unhashed".into(), cap_unhashed.clone()),
                ]
            }
        }),
    );

    let scope = provider
        .native_compaction_context(&"model-a".into())
        .unwrap()
        .auth_scope;
    let checkpoint = NativeCompactionCheckpoint {
        source: "http://127.0.0.1:9/v1".into(),
        auth_scope: scope,
        model: "model-a".into(),
        comp_hash: Some("hash-v1".into()),
        response_id: "resp-1".into(),
        encrypted_content: "opaque-v1".into(),
        retained_input: Vec::new(),
        retained_input_metadata: Vec::new(),
    };

    assert!(provider
        .native_checkpoint_compatibility(&checkpoint, &"model-a".into(), &AbortSignal::new())
        .await
        .unwrap());
    assert!(
        !provider
            .native_checkpoint_compatibility(&checkpoint, &"model-b".into(), &AbortSignal::new())
            .await
            .unwrap(),
        "a different comp_hash on the same endpoint must trigger portable local recovery"
    );
    assert!(
        provider
            .native_checkpoint_compatibility(
                &checkpoint,
                &"model-unhashed".into(),
                &AbortSignal::new()
            )
            .await
            .unwrap(),
        "missing target comp_hash keeps existing endpoint compatibility"
    );

    // End-to-end Session test: compact natively on model-a (hash-v1), switch to
    // model-b (hash-v2) on the same endpoint, and verify Session expands the
    // portable raw history from local rollout instead of forwarding opaque-v1.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/v1", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut bodies = Vec::new();
        let responses = [
            "data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"compaction\",\"encrypted_content\":\"opaque-hash-v1\"}}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r-compact\",\"status\":\"completed\"}}\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"Continued cleanly on model-b.\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r-turn\",\"status\":\"completed\",\"usage\":{\"input_tokens\":50,\"output_tokens\":10}}}\n\n",
        ];
        for sse in responses {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let (header_end, length) = loop {
                let mut buf = [0u8; 4096];
                let n = socket.read(&mut buf).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buf[..n]);
                if let Some(pos) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let head = String::from_utf8_lossy(&bytes[..pos]);
                    let len = head
                        .lines()
                        .find_map(|l| {
                            let (k, v) = l.split_once(':')?;
                            k.eq_ignore_ascii_case("content-length")
                                .then(|| v.trim().parse::<usize>().unwrap())
                        })
                        .unwrap();
                    break (pos + 4, len);
                }
            };
            while bytes.len() < header_end + length {
                let mut buf = [0u8; 4096];
                let n = socket.read(&mut buf).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buf[..n]);
            }
            bodies.push(
                serde_json::from_slice::<serde_json::Value>(
                    &bytes[header_end..header_end + length],
                )
                .unwrap(),
            );
            let reply = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{sse}",
                sse.len()
            );
            socket.write_all(reply.as_bytes()).await.unwrap();
        }
        bodies
    });

    let dir = tempfile::tempdir().unwrap();
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.path().join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));
    let live_endpoint = with_review_model_catalog(
        Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
            api: Some(OpenAiApi::Responses),
            api_key: Some("fixture-key".into()),
            base_url: Some(endpoint),
            default_model: Some("model-a".into()),
            remote_compaction: RemoteCompactionSupport::ResponsesV2,
            ..Default::default()
        })),
        Arc::new(move || {
            vec![
                ("model-a".into(), cap_a.clone()),
                ("model-b".into(), cap_b.clone()),
            ]
        }),
    );
    let mut opts = agent_options(process.clone());
    opts.provider = live_endpoint.clone();
    opts.config = Some(source_config("same-endpoint", "model-a", 200_000));
    opts.cwd = Some(dir.path().to_string_lossy().into_owned());
    opts.include_default_agents = Some(false);
    let session = Agent::new(opts)
        .open_session(
            store,
            StartSessionOptions {
                initial_history: Some(vec![msg(json!({
                    "type": "user",
                    "uuid": "u-hash-switch",
                    "content": [{"type": "text", "text": "PORTABLE_RAW_TASK: survive comp_hash switch."}]
                }))]),
                user_config_dir: Some(dir.path().to_string_lossy().into_owned().into()),
                base_instructions: Some("Continue task.".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert!(session.compact(None).await.unwrap().compacted);
    session
        .reconfigure_inference(
            live_endpoint,
            resolve_config(source_config("same-endpoint", "model-b", 200_000)).unwrap(),
            None,
            None,
            session.model_revision(),
        )
        .unwrap();
    let events: Vec<_> = session
        .run_streamed(
            Input::Text("Proceed on model-b.".into()),
            TurnOptions::default(),
        )
        .events
        .collect()
        .await;
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
    let bodies = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bodies.len(), 2);
    let model_b_payload = bodies[1].to_string();
    assert!(
        !model_b_payload.contains("opaque-hash-v1"),
        "incompatible comp_hash checkpoint must not be forwarded to model-b: {model_b_payload}"
    );
    assert!(model_b_payload.contains("PORTABLE_RAW_TASK"));

    session.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn cold_resume_onto_smaller_window_model_chunks_without_dropping_earliest_requirement() {
    let dir = tempfile::tempdir().unwrap();
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let old_calls = Arc::new(AtomicUsize::new(0));
    let target_requests = Arc::new(Mutex::new(Vec::new()));
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(dir.path().join("sessions").to_string_lossy().into_owned()),
        archive_dir: None,
    }));

    let mut history = vec![msg(json!({
        "type": "user",
        "uuid": "u-cold-0",
        "content": [{"type": "text", "text": "COLD_RESUME_INITIAL_SPEC: never lose this constraint."}]
    }))];
    for i in 0..14 {
        history.push(msg(json!({
            "type": "assistant",
            "uuid": format!("a-cold-{i}"),
            "model": "large-model",
            "content": [{"type": "text", "text": format!("COLD_STEP_{i}: {}", "Completed verification detail. ".repeat(300))}]
        })));
    }

    let mut old_options = agent_options(process.clone());
    old_options.provider = ExhaustedOldProvider::new(ProviderErrorKind::Quota, old_calls.clone());
    old_options.config = Some(source_config(
        "exhausted-large-source",
        "large-model",
        LARGE_WINDOW,
    ));
    old_options.cwd = Some(dir.path().to_string_lossy().into_owned());
    old_options.include_default_agents = Some(false);

    let session = Agent::new(old_options)
        .open_session(
            store.clone(),
            StartSessionOptions {
                initial_history: Some(history),
                user_config_dir: Some(dir.path().to_string_lossy().into_owned().into()),
                base_instructions: Some("Resume task.".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let session_id = session.id.try_to_utf8().unwrap();
    session.flush_rollout().await.unwrap();
    session.shutdown_and_wait().await.unwrap();

    // Cold-resume the persisted session directly onto a smaller-window model.
    let mut new_options = agent_options(process.clone());
    new_options.provider =
        SmallTargetProvider::new(TINY_WINDOW, true, true, target_requests.clone());
    new_options.config = Some(source_config(
        "small-target-source",
        "small-model",
        TINY_WINDOW,
    ));
    new_options.cwd = Some(dir.path().to_string_lossy().into_owned());
    new_options.include_default_agents = Some(false);

    let resumed = Agent::new(new_options)
        .resume_session(
            store,
            &session_id,
            StartSessionOptions {
                user_config_dir: Some(dir.path().to_string_lossy().into_owned().into()),
                base_instructions: Some("Resume task.".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let events: Vec<_> = resumed
        .run_streamed(
            Input::Text("Continue after restart on smaller model.".into()),
            TurnOptions::default(),
        )
        .events
        .collect()
        .await;

    assert!(
        !events
            .iter()
            .any(|e| matches!(e, EngineEvent::TurnFailed { .. })),
        "{events:?}"
    );
    assert_eq!(old_calls.load(Ordering::SeqCst), 0);
    {
        let recorded = target_requests.lock().unwrap();
        assert!(recorded.len() >= 3);
        let combined = recorded[..recorded.len() - 1]
            .iter()
            .map(|req| encode_messages(&req.messages))
            .collect::<String>();
        assert!(combined.contains("COLD_RESUME_INITIAL_SPEC"));
    }

    resumed.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}
