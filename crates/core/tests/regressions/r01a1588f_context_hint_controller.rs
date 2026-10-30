//! Defect: optional server hints shared destructive fallback behavior with
//! unrelated provider errors, or exposed request edits as durable history.
//! Root cause: hint negotiation and transcript ownership were not independent.
//! Work item: 01a1588f pin the hint failure matrix and request-only replacements.

use futures::StreamExt;
use klaus_agent_core::context::context_hints::{
    project_hint_edits, stream_fallback_request, ContextHintController, ContextHintRequest,
    ContextHintState, HintErrorAction, CONTEXT_HINT_CLEARED_MARKER, CONTEXT_HINT_MIN_SAVINGS,
    CONTEXT_HINT_STREAM_FALLBACK_CODE, CONTEXT_HINT_TARGET_TOKENS,
};
use klaus_agent_core::provider::anthropic::{AnthropicProvider, AnthropicProviderOptions};
use klaus_agent_core::provider::retry::RetryOptions;
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimePath, RuntimeString};
use klaus_agent_core::schemas::content::{
    ContentBlock, ImageBlock, OutputBlock, TextBlock, ToolResultBlock, ToolUseBlock,
};
use klaus_agent_core::schemas::message::{
    AssistantMessage, Message, ToolResultMessage, UserMessage,
};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::errors::{ProviderError, ProviderErrorKind};
use klaus_agent_core::types::provider::{ModelProvider, ModelRequest, ModelStreamEvent};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn text(value: impl Into<RuntimeString>) -> TextBlock {
    TextBlock {
        text: value.into(),
        citations: None,
        harness_envelope: None,
        message_id: None,
        phase: None,
    }
}

fn intent() -> ContextHintRequest {
    ContextHintRequest {
        enabled: true,
        main_thread: true,
        artifact_dir: None,
        state: std::sync::Arc::new(ContextHintState::new()),
    }
}

fn history(tokens: usize, nested: bool) -> Vec<Message> {
    (0..6)
        .flat_map(|index| {
            let id = RuntimeString::from(format!("hint-tool-{index}"));
            let content = vec![OutputBlock::Text(text(if index == 0 {
                "x".repeat(tokens * 4)
            } else {
                format!("recent result {index}")
            }))];
            [
                Message::Assistant(AssistantMessage {
                    uuid: format!("hint-assistant-{index}").into(),
                    content: vec![ContentBlock::ToolUse(ToolUseBlock {
                        id: id.clone(),
                        name: "read".into(),
                        input: runtime_json::parse(&"{}".into()).unwrap(),
                        tool_kind: None,
                        thought_signature: None,
                    })],
                    model: "hint-model".into(),
                    usage: None,
                }),
                if nested {
                    Message::User(UserMessage {
                        uuid: format!("hint-result-{index}").into(),
                        content: vec![ContentBlock::ToolResult(ToolResultBlock {
                            tool_use_id: id,
                            content,
                            is_error: None,
                        })],
                        is_meta: None,
                        question_answer: None,
                    })
                } else {
                    Message::ToolResult(ToolResultMessage {
                        uuid: format!("hint-result-{index}").into(),
                        tool_use_id: id,
                        content,
                        is_error: false,
                        tool_kind: None,
                        native_tool_output: None,
                        output: None,
                        extra_user_blocks: None,
                    })
                },
            ]
        })
        .collect()
}

fn request() -> ModelRequest {
    let value = runtime_json::parse(
        &json!({
            "model": "hint-model",
            "system": [],
            "messages": [],
            "tools": [],
            "maxOutputTokens": 128
        })
        .to_string()
        .into(),
    )
    .unwrap();
    let mut request: ModelRequest = runtime_codec::decode_value(&value).unwrap();
    request.messages = history(CONTEXT_HINT_MIN_SAVINGS as usize, false);
    request.context_hints = Some(intent());
    request
}

fn error(status: Option<u16>, message: &str, kind: Option<&str>) -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::InvalidRequest,
        message: message.into(),
        status,
        code: None,
        error_type: kind.map(Into::into),
        media_origins: None,
        media_repair_complete: false,
        retry_after_ms: None,
        request_id: None,
    }
}

#[test]
fn hints_require_native_opt_in_and_main_thread_first_party_scope() {
    let messages = history(20_000, false);
    for (value, first_party) in [
        (None, true),
        (
            Some(ContextHintRequest {
                enabled: false,
                ..intent()
            }),
            true,
        ),
        (
            Some(ContextHintRequest {
                main_thread: false,
                ..intent()
            }),
            true,
        ),
        (Some(intent()), false),
    ] {
        let mut controller = ContextHintController::new(value.as_ref(), first_party);
        assert!(controller.build_request(&messages).is_none());
        assert!(!controller.beta_active());
        assert_eq!(
            controller.on_request_error(&error(Some(422), "cannot apply hint", None)),
            HintErrorAction::None
        );
    }
    let encoded = runtime_codec::encode_value(&request()).unwrap();
    assert!(encoded.get("contextHints").is_none());
    let decoded: ModelRequest = runtime_codec::decode_value(&encoded).unwrap();
    assert!(decoded.context_hints.is_none());
}

#[test]
fn hint_body_starts_at_twenty_thousand_old_tokens_and_targets_seventy_five_thousand() {
    for tokens in [19_999, 20_000] {
        let mut controller = ContextHintController::new(Some(&intent()), true);
        let params = controller.build_request(&history(tokens, false)).unwrap();
        assert_eq!(params.enabled, tokens >= 20_000);
        assert_eq!(params.target_tokens_saved, CONTEXT_HINT_TARGET_TOKENS);
        assert!(controller.beta_active());
    }
}

#[test]
fn hint_error_status_matrix_disables_only_the_appropriate_episode() {
    let rows = [
        (Some(422), "hint rejected", HintErrorAction::ClearAndRetry),
        (Some(424), "hint failed", HintErrorAction::ClearAndRetry),
        (
            Some(400),
            "Unexpected value for anthropic-beta",
            HintErrorAction::StripAndRetry,
        ),
        (Some(409), "server busy", HintErrorAction::Strip),
        (Some(529), "overloaded", HintErrorAction::Strip),
        (Some(400), "bad input", HintErrorAction::None),
        (Some(401), "invalid authentication", HintErrorAction::None),
        (Some(429), "rate limited", HintErrorAction::None),
        (Some(500), "server error", HintErrorAction::None),
        (None, "connection reset", HintErrorAction::None),
    ];
    for (status, message, expected) in rows {
        let mut controller = ContextHintController::new(Some(&intent()), true);
        controller.build_request(&history(20_000, false));
        let failure = error(status, message, None);
        assert_eq!(
            controller.on_request_error(&failure),
            expected,
            "{status:?}"
        );
        if expected == HintErrorAction::None {
            assert!(controller.beta_active());
        } else {
            assert!(!controller.beta_active());
            assert_eq!(controller.on_request_error(&failure), HintErrorAction::None);
            assert!(controller.build_request(&history(20_000, false)).is_none());
        }
    }
}

#[tokio::test]
async fn local_hint_projection_keeps_five_recent_results_and_preserves_the_transcript() {
    for nested in [false, true] {
        let original = history(20_000, nested);
        let projection = project_hint_edits(&original, None).await;
        assert_eq!(projection.replacements.len(), 1);
        assert_eq!(projection.estimated_tokens_saved, 20_000);
        assert_eq!(
            projection.replacements[&RuntimeString::from("hint-tool-0")],
            CONTEXT_HINT_CLEARED_MARKER
        );
        let mut projected = original.clone();
        projection.apply(&mut projected);
        assert_ne!(projected[1], original[1]);
        assert_eq!(&projected[2..], &original[2..]);
        assert_eq!(original, history(20_000, nested));
        assert!(project_hint_edits(&projected, None)
            .await
            .replacements
            .is_empty());
    }
    assert!(project_hint_edits(&history(19_999, false), None)
        .await
        .replacements
        .is_empty());
    let mut nonreconstructible = history(20_000, false);
    let Message::Assistant(assistant) = &mut nonreconstructible[0] else {
        panic!("assistant tool call");
    };
    let ContentBlock::ToolUse(tool) = &mut assistant.content[0] else {
        panic!("tool call");
    };
    tool.name = "task".into();
    assert!(project_hint_edits(&nonreconstructible, None)
        .await
        .replacements
        .is_empty());
}

#[tokio::test]
async fn hint_projection_persists_complete_text_but_uses_a_marker_for_media() {
    let directory = super::context_fixture::tempdir().unwrap();
    let path = RuntimePath::from(directory.path().to_string_lossy().into_owned());
    let original = history(20_000, false);
    let projection = project_hint_edits(&original, Some(&path)).await;
    assert!(projection.replacements[&RuntimeString::from("hint-tool-0")]
        .contains("Tool result saved to:"));
    let files = std::fs::read_dir(directory.path())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(
        std::fs::read_to_string(files[0].path()).unwrap(),
        "x".repeat(80_000)
    );
    let mut media = original.clone();
    let Message::ToolResult(result) = &mut media[1] else {
        panic!("tool result");
    };
    result.content.push(OutputBlock::Image(ImageBlock {
        mime_type: "image/png".into(),
        base64: "AAAA".into(),
        detail: None,
        source_path: None,
    }));
    let projection = project_hint_edits(&media, Some(&path)).await;
    assert_eq!(
        projection.replacements[&RuntimeString::from("hint-tool-0")],
        CONTEXT_HINT_CLEARED_MARKER
    );
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
    let invalid_directory = directory.path().join("not-a-directory");
    std::fs::write(&invalid_directory, "keep existing artifact").unwrap();
    let unavailable = RuntimePath::from(invalid_directory.to_string_lossy().into_owned());
    let failed = project_hint_edits(&original, Some(&unavailable)).await;
    assert_eq!(
        failed.replacements[&RuntimeString::from("hint-tool-0")],
        CONTEXT_HINT_CLEARED_MARKER
    );
    assert_eq!(
        std::fs::read_to_string(invalid_directory).unwrap(),
        "keep existing artifact"
    );
    assert_eq!(original, history(20_000, false));
}

#[tokio::test]
async fn statusless_invalid_request_fallback_is_explicit_and_cannot_repeat() {
    for (status, kind, accepted) in [
        (None, Some("invalid_request_error"), true),
        (Some(400), Some("invalid_request_error"), false),
        (None, Some("overloaded_error"), false),
        (None, None, false),
    ] {
        let mut controller = ContextHintController::new(Some(&intent()), true);
        controller.build_request(&history(20_000, false));
        let mut failure = error(status, "stream failed", kind);
        assert_eq!(controller.classify_stream_error(&failure), accepted);
        if accepted {
            assert!(!controller.classify_stream_error(&failure));
            failure.code = Some(CONTEXT_HINT_STREAM_FALLBACK_CODE.into());
            let original = request();
            let retried = stream_fallback_request(&original, &failure).await.unwrap();
            assert!(retried.context_hints.is_none());
            assert_ne!(retried.messages, original.messages);
            assert_eq!(original.messages, history(20_000, false));
            assert!(stream_fallback_request(&retried, &failure).await.is_none());
        } else {
            assert!(stream_fallback_request(&request(), &failure)
                .await
                .is_none());
        }
    }
}

#[tokio::test]
async fn frozen_hint_results_survive_continuation_but_not_model_changes_or_retirement() {
    let original = request();
    let intent = original.context_hints.as_ref().unwrap().clone();
    let mut failure = error(None, "hint stream failed", Some("invalid_request_error"));
    failure.code = Some(CONTEXT_HINT_STREAM_FALLBACK_CODE.into());
    let projected = stream_fallback_request(&original, &failure).await.unwrap();
    assert_eq!(intent.state.records().len(), 1);

    let mut continuation = history(20_000, false);
    intent
        .state
        .apply_frozen(&original.model, &mut continuation);
    assert_eq!(continuation, projected.messages);
    assert_eq!(original.messages, history(20_000, false));

    let independent = ContextHintState::new();
    let mut separate = history(20_000, false);
    independent.apply_frozen(&original.model, &mut separate);
    assert_eq!(separate, original.messages);

    let mut changed_model = history(20_000, false);
    intent
        .state
        .apply_frozen(&"other-model".into(), &mut changed_model);
    assert_eq!(changed_model, original.messages);
    assert!(intent.state.records().is_empty());
    intent
        .state
        .apply_frozen(&original.model, &mut changed_model);
    let projection = project_hint_edits(&original.messages, None).await;
    intent.state.record_projection(&original.model, &projection);
    assert_eq!(intent.state.records().len(), 1);
    intent.state.apply_frozen(&original.model, &mut []);
    assert!(intent.state.records().is_empty());
}

#[tokio::test]
async fn custom_provider_endpoint_never_declares_or_recovers_a_context_hint() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let provider = AnthropicProvider::new(AnthropicProviderOptions {
        api_key: Some("fixture-only".into()),
        base_url: Some(format!("http://{}", listener.local_addr().unwrap())),
        retry: Some(RetryOptions {
            max_attempts: Some(1),
            congestion_hold_ms: Some(0),
            unbounded_network_retries: Some(false),
            ..Default::default()
        }),
        ..Default::default()
    });
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        loop {
            let mut buffer = [0u8; 8192];
            let count = socket.read(&mut buffer).await.unwrap();
            assert!(count > 0);
            bytes.extend_from_slice(&buffer[..count]);
            if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&bytes[..end]);
                let length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                if bytes.len() < end + 4 + length {
                    continue;
                }
                assert!(!headers.contains("context-hint-2026-04-09"));
                let body: serde_json::Value =
                    serde_json::from_slice(&bytes[end + 4..end + 4 + length]).unwrap();
                assert!(body.get("context_hint").is_none());
                assert!(body.to_string().contains(&"x".repeat(80_000)));
                break;
            }
        }
        let body = r#"{"error":{"type":"invalid_request_error","message":"hint unavailable"}}"#;
        let response = format!("HTTP/1.1 422 Unprocessable Entity\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
        socket.write_all(response.as_bytes()).await.unwrap();
    });
    let original = request();
    let events = provider
        .stream(original.clone(), AbortSignal::new())
        .collect::<Vec<_>>()
        .await;
    server.await.unwrap();
    assert!(
        matches!(events.as_slice(), [ModelStreamEvent::Error { error }] if error.status == Some(422))
    );
    assert_eq!(original.messages, history(20_000, false));
}

#[tokio::test]
async fn persisted_hint_records_are_lossless_and_never_become_prompt_messages() {
    use klaus_agent_core::context::context_hints::CONTEXT_HINT_ROLLOUT_EVENT;
    use klaus_agent_core::schemas::rollout::{EventItem, RolloutItem};
    use klaus_agent_core::store::projection::project_for_prompt;
    let model = RuntimeString::from("hint-model");
    let id = RuntimeString::from_utf16(vec![0xd800, 65]);
    let body = RuntimeString::from_utf16(vec![0xdc00, 66]);
    let state = ContextHintState::new();
    state.restore_records(&model, vec![(id.clone(), body.clone())]);
    let event = RolloutItem::Event(EventItem {
        event: CONTEXT_HINT_ROLLOUT_EVENT.into(),
        turn_id: None,
        payload: state.take_pending_record(),
    });
    assert!(state.take_pending_record().is_none());
    let encoded = runtime_codec::encode_value(&event).unwrap();
    let decoded: RolloutItem = runtime_codec::decode_value(&encoded).unwrap();
    let mut items = history(20_000, false)
        .into_iter()
        .map(|message| RolloutItem::Message { message })
        .collect::<Vec<_>>();
    items.push(decoded);
    assert_eq!(project_for_prompt(&items), history(20_000, false));
    let restored = ContextHintState::new();
    restored.restore_rollout(&items);
    assert_eq!(restored.records(), vec![(id, body)]);
    assert!(restored.take_pending_record().is_none());
}

#[tokio::test]
async fn session_hint_fallback_tombstones_partial_text_and_restores_frozen_records_on_resume() {
    use klaus_agent_core::context::context_hints::CONTEXT_HINT_ROLLOUT_EVENT;
    use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
    use klaus_agent_core::engine::session::{Input, TurnOptions};
    use klaus_agent_core::runtime_process::RuntimeProcessContext;
    use klaus_agent_core::schemas::message::Usage;
    use klaus_agent_core::schemas::rollout::RolloutItem;
    use klaus_agent_core::store::jsonl_store::{JsonlSessionStore, JsonlSessionStoreOptions};
    use klaus_agent_core::store::projection::project_for_prompt;
    use klaus_agent_core::store::session_store::SessionStore;
    use klaus_agent_core::types::events::EngineEvent;
    use klaus_agent_core::types::provider::{
        ModelStopReason, ModelStream, ProviderCapabilities, ProviderInfo,
    };
    use std::sync::{Arc, Mutex};
    struct Provider {
        info: ProviderInfo,
        requests: Mutex<Vec<ModelRequest>>,
    }
    impl ModelProvider for Provider {
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
                max_output_tokens: 1_000,
                native_pdf: false,
                vision: false,
                image_limits: None,
                max_media_blocks: None,
            }
        }
        fn stream(&self, request: ModelRequest, _: AbortSignal) -> ModelStream {
            let mut requests = self.requests.lock().unwrap();
            let index = requests.len();
            requests.push(request);
            drop(requests);
            let events = if index == 0 {
                let mut failure =
                    error(None, "hint stream rejected", Some("invalid_request_error"));
                failure.code = Some(CONTEXT_HINT_STREAM_FALLBACK_CODE.into());
                vec![
                    ModelStreamEvent::TextDelta {
                        text: "temporary hint prefix".into(),
                    },
                    ModelStreamEvent::Error { error: failure },
                ]
            } else {
                vec![
                    ModelStreamEvent::TextDelta {
                        text: "completed".into(),
                    },
                    ModelStreamEvent::MessageStop {
                        stop_reason: ModelStopReason::EndTurn,
                        usage: Usage {
                            input_tokens: 100,
                            output_tokens: 10,
                            cache_read_input_tokens: 0,
                            cache_creation_input_tokens: 0,
                            total_tokens: None,
                        },
                    },
                ]
            };
            Box::pin(futures::stream::iter(events))
        }
    }
    let directory = super::context_fixture::tempdir().unwrap();
    let process = RuntimeProcessContext::initialize(
        super::r7c876d49_spec_restores_latest_permission_policy::process_supervisor(),
    )
    .unwrap();
    let provider = Arc::new(Provider {
        info: ProviderInfo {
            name: "hint-fixture".into(),
            default_model: "hint-model".into(),
            base_url: None,
            env_key: None,
        },
        requests: Mutex::new(Vec::new()),
    });
    let mut options =
        super::r7c876d49_spec_restores_latest_permission_policy::agent_options(process.clone());
    options.provider = provider.clone();
    options.config = Some(
        json!({"model":"hint-model","contextHints":true,"thinking":false,"prewarm":false,"autoCompaction":false,"autoMemory":false,"permissionMode":"bypassPermissions","sandbox":"process","sandboxNetworkAllowed":true,"requireRealSandbox":false,"sandboxRetryOnDenial":false,"loopDetection":false}),
    );
    let agent = Agent::new(options);
    let store = Arc::new(JsonlSessionStore::new(JsonlSessionStoreOptions {
        dir: Some(
            directory
                .path()
                .join("sessions")
                .to_string_lossy()
                .into_owned(),
        ),
        archive_dir: None,
    }));
    let start = || StartSessionOptions {
        cwd: Some(directory.path().to_string_lossy().into_owned().into()),
        user_config_dir: Some(directory.path().to_string_lossy().into_owned().into()),
        system_prompt_provider: Some(Arc::new(|| Box::pin(async { Vec::new() }))),
        ..Default::default()
    };
    let mut initial = start();
    initial.initial_history = Some(history(20_000, false));
    let session = agent.open_session(store.clone(), initial).await.unwrap();
    let events = session
        .run_streamed(Input::Text("continue".into()), TurnOptions::default())
        .events
        .collect::<Vec<_>>()
        .await;
    assert!(events
        .iter()
        .any(|event| matches!(event, EngineEvent::Tombstone { .. })));
    assert!(!events
        .iter()
        .any(|event| matches!(event, EngineEvent::TurnFailed { .. })));
    for message in history(20_000, false) {
        assert!(session.history().contains(&message));
    }
    let rendered =
        runtime_json::stringify(&runtime_codec::encode_value(&session.history()).unwrap());
    assert!(!rendered.contains("temporary hint prefix"));
    session.flush_rollout().await.unwrap();
    let id = session.id.try_to_utf8().unwrap();
    let persisted = store.read_thread(&id).await.unwrap();
    assert!(persisted.raw_items.iter().any(|item| matches!(item, RolloutItem::Event(event) if event.event == CONTEXT_HINT_ROLLOUT_EVENT)));
    assert_eq!(project_for_prompt(&persisted.raw_items), session.history());
    session.shutdown_and_wait().await.unwrap();
    let resumed = agent.resume_session(store, &id, start()).await.unwrap();
    let outcome = resumed
        .run(
            Input::Text("continue after resume".into()),
            TurnOptions::default(),
        )
        .await;
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    let requests = provider.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 3);
    assert!(requests[0].context_hints.is_some());
    assert!(requests[1].context_hints.is_none());
    assert!(requests[2].context_hints.is_some());
    let first =
        runtime_json::stringify(&runtime_codec::encode_value(&requests[0].messages).unwrap());
    let retry =
        runtime_json::stringify(&runtime_codec::encode_value(&requests[1].messages).unwrap());
    let restored =
        runtime_json::stringify(&runtime_codec::encode_value(&requests[2].messages).unwrap());
    assert!(first.contains(&"x".repeat(80_000)));
    assert!(!retry.contains(&"x".repeat(80_000)));
    assert!(!restored.contains(&"x".repeat(80_000)));
    assert!(restored.contains("Tool result saved to:"));
    resumed.shutdown_and_wait().await.unwrap();
    process.shutdown().await.unwrap();
}
