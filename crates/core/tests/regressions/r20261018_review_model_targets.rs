//! Defect: review reused the main model and omitted internal catalog metadata.
//! Root cause: picker visibility and a captured main-model allowlist also controlled review targets.
//! Work item: 20261018 independent account-bound approval review model capture.

use futures::StreamExt;
use klaus_agent_core::provider::openai::{OpenAIProvider, OpenAIProviderOptions, OpenAiApi};
use klaus_agent_core::provider::retry::RetryOptions;
use klaus_agent_core::provider::review_target::{
    capture_review_model_target, capture_review_model_target_with_effort,
    capture_review_model_target_with_retries, resolve_approval_review_target,
    with_review_model_catalog,
};
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeString};
use klaus_agent_core::schemas::effort::{EffortLevel, EffortValue};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::{
    ModelCapabilities, ModelProvider, ModelRequest, ModelStreamEvent, ReasoningSummary,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn metadata(levels: &[&str], default: &str) -> ModelCapabilities {
    runtime_codec::decode_text(&RuntimeString::from(json!({"effort":{"levels":levels,"default":default},"contextWindow":272000,"maxOutputTokens":4096,"supportsReasoningSummaryParameter":false}).to_string())).unwrap()
}

fn provider(parent: ModelCapabilities) -> Arc<dyn ModelProvider> {
    Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
        api: Some(OpenAiApi::Responses),
        api_key: Some("fixture-only".into()),
        default_model: Some("parent".into()),
        capabilities: Some(parent),
        ..Default::default()
    }))
}

#[test]
fn preferred_override_and_parent_fallback_keep_exact_model_and_effort_choices() {
    let parent = metadata(&["medium", "high"], "medium");
    let live = Arc::new(Mutex::new(vec![(
        RuntimeString::from("gpt-5.6-luna"),
        metadata(&["low", "medium"], "medium"),
    )]));
    let catalog = live.clone();
    let source = with_review_model_catalog(
        provider(parent.clone()),
        Arc::new(move || catalog.lock().unwrap().clone()),
    );
    let target = resolve_approval_review_target(
        source.clone(),
        &"parent".into(),
        Some(EffortValue::Level(EffortLevel::High)),
    );
    assert_eq!(target.model, "gpt-5.6-luna");
    assert_eq!(target.effort, Some(EffortValue::Level(EffortLevel::Low)));
    assert!(target.catalog_contains_preferred);
    assert!(!target.provider.has_model_capabilities(&"parent".into()));
    assert!(target.provider.has_model_capabilities(&target.model));
    *live.lock().unwrap() = vec![("gpt-5.6-luna".into(), metadata(&["medium", "high"], "high"))];
    let changed = resolve_approval_review_target(source.clone(), &"parent".into(), None);
    assert_eq!(changed.effort, Some(EffortValue::Level(EffortLevel::High)));
    assert_eq!(target.effort, Some(EffortValue::Level(EffortLevel::Low)));
    live.lock().unwrap().clear();
    let fallback = resolve_approval_review_target(
        source,
        &"parent".into(),
        Some(EffortValue::Level(EffortLevel::High)),
    );
    assert_eq!(fallback.model, "parent");
    assert_eq!(fallback.effort, Some(EffortValue::Level(EffortLevel::High)));
    assert_eq!(fallback.metadata, parent);
    let mut overridden = metadata(&["low", "high"], "high");
    overridden.auto_review_model_override = Some("explicit-review".into());
    let selected = resolve_approval_review_target(provider(overridden), &"parent".into(), None);
    assert_eq!(selected.model, "explicit-review");
    assert!(selected.model_overridden);
    assert_eq!(selected.effort, Some(EffortValue::Level(EffortLevel::Low)));
}

#[test]
fn required_review_metadata_and_nested_policy_survive_exact_codecs() {
    let wire = json!({"modelSpecialty":"cyber","supportedReasoningEfforts":["none","minimal","persistent","custom/Exact"],"defaultReasoningEffort":"custom/Exact","effectiveContextWindowPercent":0,"usedFallbackModelMetadata":true,"autoReviewModelOverride":"private-review","requiresSandboxedReview":true,"nodeReplAutoReviewRequired":true,"guardian":{"shell":"adaptive","network":"synchronous"},"guardianV2":{"review_threshold":0.5},"modelMessages":{"guardian_v2":{"risk_score_template":"fixture"}},"compHash":"revision-2","supportsReasoningSummaryParameter":false,"defaultReasoningSummary":"none"});
    let decoded: ModelCapabilities =
        runtime_codec::decode_text(&RuntimeString::from(wire.to_string())).unwrap();
    assert_eq!(decoded.model_specialty.as_ref().unwrap(), "cyber");
    assert_eq!(decoded.effective_context_window_percent, Some(0));
    assert!(decoded.used_fallback_model_metadata);
    assert_eq!(
        decoded.default_reasoning_effort.as_ref().unwrap(),
        "custom/Exact"
    );
    assert!(decoded.requires_sandboxed_review);
    assert!(decoded.node_repl_auto_review_required);
    assert_eq!(
        decoded.auto_review_model_override.as_ref().unwrap(),
        "private-review"
    );
    assert_eq!(
        decoded.default_reasoning_summary,
        Some(ReasoningSummary::None)
    );
    let encoded = runtime_codec::encode_value(&decoded).unwrap();
    assert_eq!(
        runtime_codec::decode_value::<ModelCapabilities>(&encoded).unwrap(),
        decoded
    );
    assert!(decoded.guardian.is_some());
    assert!(decoded.guardian_v2.is_some());
    assert!(decoded.model_messages.is_some());
}

async fn read_request(socket: &mut TcpStream) -> Value {
    let mut bytes = Vec::new();
    loop {
        let mut chunk = [0_u8; 8192];
        let count = socket.read(&mut chunk).await.unwrap();
        assert_ne!(count, 0);
        assert!(bytes.len() + count <= 128 * 1024);
        bytes.extend_from_slice(&chunk[..count]);
        if let Some(end) = bytes.windows(4).position(|value| value == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&bytes[..end]);
            let length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            if bytes.len() >= end + 4 + length {
                return serde_json::from_slice(&bytes[end + 4..end + 4 + length]).unwrap();
            }
        }
    }
}

fn request(model: &RuntimeString) -> ModelRequest {
    runtime_codec::decode_text(&RuntimeString::from(json!({"model":model.try_to_utf8().unwrap(),"system":[],"messages":[],"tools":[],"maxOutputTokens":128,"reasoningSummary":"none","effort":"low"}).to_string())).unwrap()
}

#[tokio::test]
async fn selected_review_metadata_reaches_actual_responses_wire_without_rebinding_parent() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let root: Arc<dyn ModelProvider> = Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
        api: Some(OpenAiApi::Responses),
        api_key: Some("fixture-only".into()),
        base_url: Some(format!("http://{}", listener.local_addr().unwrap())),
        default_model: Some("parent".into()),
        capabilities: Some(metadata(&["high"], "high")),
        capabilities_by_model: Some(HashMap::new()),
        ..Default::default()
    }));
    let source = with_review_model_catalog(
        root.clone(),
        Arc::new(|| {
            vec![(
                "gpt-5.6-luna".into(),
                metadata(&["low", "medium"], "medium"),
            )]
        }),
    );
    let target = resolve_approval_review_target(source, &"parent".into(), None);
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let body = read_request(&mut socket).await;
        let stream="data: {\"type\":\"response.output_text.delta\",\"delta\":\"done\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"output\":[]}}\n\n";
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{stream}",stream.len()).as_bytes()).await.unwrap();
        body
    });
    let events = tokio::time::timeout(
        Duration::from_secs(5),
        target
            .provider
            .stream(request(&target.model), AbortSignal::new())
            .collect::<Vec<_>>(),
    )
    .await
    .unwrap();
    assert!(
        events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::MessageStop { .. })),
        "{events:?}"
    );
    let body = server.await.unwrap();
    assert_eq!(body["model"], json!("gpt-5.6-luna"));
    assert_eq!(body["reasoning"]["effort"], json!("low"));
    assert!(body["reasoning"].get("summary").is_none());
    assert_eq!(
        root.model_metadata(&"parent".into()).effort.unwrap().levels,
        vec![EffortLevel::High]
    );
    let wrong = request(&"parent".into());
    let rejected = target
        .provider
        .stream(wrong, AbortSignal::new())
        .collect::<Vec<_>>()
        .await;
    assert!(matches!(&rejected[..], [ModelStreamEvent::Error { .. }]));
}

#[tokio::test]
async fn sync_and_classifier_transports_keep_distinct_retry_counts() {
    for total in [2, 3] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let root: Arc<dyn ModelProvider> = Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
            api: Some(OpenAiApi::Responses),
            api_key: Some("fixture-only".into()),
            base_url: Some(format!("http://{}", listener.local_addr().unwrap())),
            capabilities: Some(metadata(&["low"], "low")),
            retry: Some(RetryOptions {
                max_attempts: Some(8),
                base_delay_ms: Some(1),
                congestion_hold_ms: Some(0),
                ..Default::default()
            }),
            ..Default::default()
        }));
        let selected = if total == 2 {
            capture_review_model_target(root, "review".into(), metadata(&["low"], "low"))
        } else {
            capture_review_model_target_with_retries(
                root,
                "review".into(),
                metadata(&["low"], "low"),
                total,
            )
        };
        let server = tokio::spawn(async move {
            let mut models = Vec::new();
            for _ in 0..total {
                let (mut socket, _) = listener.accept().await.unwrap();
                let body = read_request(&mut socket).await;
                models.push(body["model"].clone());
                socket.write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").await.unwrap();
            }
            models
        });
        let events = tokio::time::timeout(
            Duration::from_secs(10),
            selected
                .stream(request(&"review".into()), AbortSignal::new())
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, ModelStreamEvent::Error { .. })),
            "{events:?}"
        );
        let models = tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(models, vec![json!("review"); total as usize]);
    }
}

#[test]
fn complete_review_effort_vocabulary_and_absent_default_remain_distinct() {
    for default in [
        None,
        Some("minimal"),
        Some("persistent"),
        Some("custom/Exact"),
    ] {
        let metadata = ModelCapabilities {
            supported_reasoning_efforts: vec![
                "minimal".into(),
                "persistent".into(),
                "custom/Exact".into(),
            ],
            default_reasoning_effort: default.map(RuntimeString::from),
            ..Default::default()
        };
        let source = with_review_model_catalog(
            provider(ModelCapabilities::default()),
            Arc::new(move || vec![("gpt-5.6-luna".into(), metadata.clone())]),
        );
        let target = resolve_approval_review_target(source, &"parent".into(), None);
        assert_eq!(target.raw_effort, default.map(RuntimeString::from));
        assert_eq!(target.effort, None);
    }
}

#[tokio::test]
async fn review_only_raw_efforts_reach_actual_chat_and_responses_requests() {
    for api in [OpenAiApi::Chat, OpenAiApi::Responses] {
        for effort in ["none", "minimal", "persistent", "custom/Exact", "ultra"] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let root: Arc<dyn ModelProvider> =
                Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
                    api: Some(api),
                    api_key: Some("fixture-only".into()),
                    base_url: Some(format!("http://{}", listener.local_addr().unwrap())),
                    ..Default::default()
                }));
            let selected = capture_review_model_target_with_effort(
                root,
                "review".into(),
                ModelCapabilities::default(),
                Some(effort.into()),
                2,
            );
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let body = read_request(&mut socket).await;
                let response = if api == OpenAiApi::Responses {
                    "data: {\"type\":\"response.completed\",\"response\":{\"output\":[]}}\n\n"
                } else {
                    "data: [DONE]\n\n"
                };
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",response.len(),response).as_bytes()).await.unwrap();
                body
            });
            tokio::time::timeout(
                Duration::from_secs(5),
                selected
                    .stream(request(&"review".into()), AbortSignal::new())
                    .collect::<Vec<_>>(),
            )
            .await
            .unwrap();
            let body = server.await.unwrap();
            let actual = if api == OpenAiApi::Responses {
                &body["reasoning"]["effort"]
            } else {
                &body["reasoning_effort"]
            };
            assert_eq!(
                actual,
                &json!(if effort == "ultra" { "xhigh" } else { effort })
            );
        }
    }
}

#[tokio::test]
async fn unrepresentable_review_effort_is_an_explicit_provider_error() {
    use klaus_agent_core::provider::anthropic::{AnthropicProvider, AnthropicProviderOptions};
    let source: Arc<dyn ModelProvider> =
        Arc::new(AnthropicProvider::new(AnthropicProviderOptions {
            api_key: Some("fixture-only".into()),
            ..Default::default()
        }));
    let selected = capture_review_model_target_with_effort(
        source,
        "review".into(),
        ModelCapabilities::default(),
        Some("persistent".into()),
        2,
    );
    let result = selected
        .stream(request(&"review".into()), AbortSignal::new())
        .collect::<Vec<_>>()
        .await;
    assert!(
        matches!(&result[..], [ModelStreamEvent::Error { error }] if error.code.as_ref().is_some_and(|code| code == "unsupported_review_effort"))
    );
}

#[tokio::test]
async fn reviewer_http_retry_classes_keep_rate_limits_in_outer_attempts() {
    use klaus_agent_core::types::errors::ProviderFailurePhase;
    for (status, inner_calls) in [(429_u16, 1_usize), (503_u16, 2_usize)] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let root: Arc<dyn ModelProvider> = Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
            api: Some(OpenAiApi::Responses),
            api_key: Some("fixture-only".into()),
            base_url: Some(format!("http://{}", listener.local_addr().unwrap())),
            retry: Some(RetryOptions {
                max_attempts: Some(9),
                congestion_hold_ms: Some(60000),
                unbounded_network_retries: Some(true),
                ..Default::default()
            }),
            ..Default::default()
        }));
        let target = capture_review_model_target_with_effort(
            root,
            "review".into(),
            metadata(&["low"], "low"),
            Some("low".into()),
            2,
        );
        let server = tokio::spawn(async move {
            for _ in 0..3 * inner_calls {
                let (mut socket, _) = listener.accept().await.unwrap();
                read_request(&mut socket).await;
                socket.write_all(format!("HTTP/1.1 {status} Rejected\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}").as_bytes()).await.unwrap();
            }
            assert!(
                tokio::time::timeout(Duration::from_millis(50), listener.accept())
                    .await
                    .is_err()
            );
            3 * inner_calls
        });
        for _ in 0..3 {
            let events = tokio::time::timeout(
                Duration::from_secs(5),
                target
                    .stream(request(&"review".into()), AbortSignal::new())
                    .collect::<Vec<_>>(),
            )
            .await
            .unwrap();
            let error = events
                .iter()
                .find_map(|event| {
                    if let ModelStreamEvent::Error { error } = event {
                        Some(error)
                    } else {
                        None
                    }
                })
                .unwrap();
            assert_eq!(error.status, Some(status));
            assert_eq!(error.failure_phase(), ProviderFailurePhase::Establishment);
        }
        assert_eq!(server.await.unwrap(), 3 * inner_calls);
    }
}

#[tokio::test]
async fn interrupted_review_stream_has_a_distinct_phase_and_no_http_retry() {
    use klaus_agent_core::types::errors::ProviderFailurePhase;
    for after_text in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let root: Arc<dyn ModelProvider> = Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
            api: Some(OpenAiApi::Responses),
            api_key: Some("fixture-only".into()),
            base_url: Some(format!("http://{}", listener.local_addr().unwrap())),
            ..Default::default()
        }));
        let target = capture_review_model_target(root, "review".into(), metadata(&["low"], "low"));
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            let fragment = if after_text {
                "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n"
            } else {
                ""
            };
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 65536\r\nConnection: close\r\n\r\n{fragment}").as_bytes()).await.unwrap();
            socket.shutdown().await.unwrap();
            drop(socket);
            assert!(
                tokio::time::timeout(Duration::from_millis(50), listener.accept())
                    .await
                    .is_err()
            );
        });
        let events = tokio::time::timeout(
            Duration::from_secs(5),
            target
                .stream(request(&"review".into()), AbortSignal::new())
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
        let error = events
            .iter()
            .find_map(|event| {
                if let ModelStreamEvent::Error { error } = event {
                    Some(error)
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(error.failure_phase(), ProviderFailurePhase::Streaming);
        assert_eq!(error.status, None);
        assert!(!events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::MessageStop { .. })));
        server.await.unwrap();
    }
}

#[test]
fn interruption_marker_preserves_genuine_upstream_diagnostics() {
    use klaus_agent_core::types::errors::{ProviderError, ProviderFailurePhase};
    let error: ProviderError = runtime_codec::decode_text(&RuntimeString::from(r#"{"kind":"network","message":"upstream failure","code":"upstream-code","type":"upstream-type","status":503}"#)).unwrap();
    let marked = error.clone().mark_stream_interrupted();
    assert_eq!(marked, error);
    assert_eq!(marked.failure_phase(), ProviderFailurePhase::Establishment);
}

#[tokio::test]
async fn review_connection_failures_are_bounded_before_stream_establishment() {
    use klaus_agent_core::types::errors::ProviderFailurePhase;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let root: Arc<dyn ModelProvider> = Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
        api: Some(OpenAiApi::Responses),
        api_key: Some("fixture-only".into()),
        base_url: Some(format!("http://{}", listener.local_addr().unwrap())),
        ..Default::default()
    }));
    let target = capture_review_model_target(root, "review".into(), metadata(&["low"], "low"));
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            socket.shutdown().await.unwrap();
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(50), listener.accept())
                .await
                .is_err()
        );
    });
    let events = tokio::time::timeout(
        Duration::from_secs(5),
        target
            .stream(request(&"review".into()), AbortSignal::new())
            .collect::<Vec<_>>(),
    )
    .await
    .unwrap();
    let error = events
        .iter()
        .find_map(|event| {
            if let ModelStreamEvent::Error { error } = event {
                Some(error)
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(error.failure_phase(), ProviderFailurePhase::Establishment);
    assert_eq!(
        error.kind,
        klaus_agent_core::types::errors::ProviderErrorKind::Network
    );
    server.await.unwrap();
}

#[tokio::test]
async fn normal_provider_requests_retain_their_existing_rate_limit_retry_policy() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let source = OpenAIProvider::new(OpenAIProviderOptions {
        api: Some(OpenAiApi::Responses),
        api_key: Some("fixture-only".into()),
        base_url: Some(format!("http://{}", listener.local_addr().unwrap())),
        retry: Some(RetryOptions {
            max_attempts: Some(2),
            base_delay_ms: Some(1),
            congestion_hold_ms: Some(0),
            unbounded_network_retries: Some(false),
            ..Default::default()
        }),
        ..Default::default()
    });
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            socket.write_all(b"HTTP/1.1 429 Rejected\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").await.unwrap();
        }
    });
    let events = tokio::time::timeout(
        Duration::from_secs(5),
        source
            .stream(request(&"ordinary".into()), AbortSignal::new())
            .collect::<Vec<_>>(),
    )
    .await
    .unwrap();
    assert!(events
        .iter()
        .any(|event| matches!(event,ModelStreamEvent::Error{error} if error.status == Some(429))));
    server.await.unwrap();
}

fn managed_identity_token(user: &str, account: &str, plan: &str) -> RuntimeString {
    use base64::Engine;
    let claims=json!({"https://api.openai.com/auth":{"chatgpt_user_id":user,"chatgpt_account_id":account,"chatgpt_plan_type":plan}}).to_string();
    format!(
        "header.{}.signature",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(claims.as_bytes())
    )
    .into()
}

#[tokio::test]
async fn managed_cloud_source_uses_bound_oauth_identity_through_captured_targets() {
    use klaus_agent_core::engine::approval_requirements_cloud::ManagedReviewCloudAuthRecovery;
    use klaus_agent_core::provider::oauth::{OAuthAuth, OAuthAuthOptions, OAuthTokens};
    let auth = Arc::new(OAuthAuth::new(OAuthAuthOptions {
        tokens: Some(OAuthTokens {
            access_token: "actual-access".into(),
            id_token: Some(managed_identity_token("user-a", "account-a", "ent26")),
            expires_at: Some(4_000_000_000_000),
            ..Default::default()
        }),
        base_url: Some("https://chatgpt.com/backend-api/codex".into()),
        extra_headers: Some(HashMap::from([
            ("ChatGPT-Account-Id".into(), "account-a".into()),
            ("version".into(), "fixture-version".into()),
        ])),
        ..Default::default()
    }));
    let root: Arc<dyn ModelProvider> = Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
        api: Some(OpenAiApi::Responses),
        auth_source: Some(auth.clone()),
        ..Default::default()
    }));
    let captured = capture_review_model_target(
        with_review_model_catalog(root, Arc::new(Vec::new)),
        "review".into(),
        ModelCapabilities::default(),
    );
    let source = captured
        .managed_review_cloud_auth_source()
        .expect("bound subscription source");
    let before = source.current().await.unwrap().unwrap();
    assert_eq!(before.identity.chatgpt_user_id.as_deref(), Some("user-a"));
    assert_eq!(before.identity.account_id.as_deref(), Some("account-a"));
    assert_eq!(before.identity.plan_type.as_deref(), Some("ent26"));
    assert_eq!(
        before.headers.get("Authorization").map(String::as_str),
        Some("Bearer actual-access")
    );
    assert_eq!(
        before.headers.get("version").map(String::as_str),
        Some("fixture-version")
    );
    assert_eq!(
        before.identity.backend_base_url,
        "https://chatgpt.com/backend-api/codex"
    );
    assert!(matches!(
        source.recover_unauthorized(2, &before.identity).await,
        ManagedReviewCloudAuthRecovery::Unavailable
    ));
    auth.set_tokens(OAuthTokens {
        access_token: "new-access".into(),
        id_token: Some(managed_identity_token("user-b", "account-b", "business")),
        ..Default::default()
    })
    .await
    .unwrap();
    assert!(matches!(
        source.recover_unauthorized(1, &before.identity).await,
        ManagedReviewCloudAuthRecovery::PermanentFailure(_)
    ));
    let after = source.current().await.unwrap().unwrap();
    assert_ne!(
        before.identity.owner_generation,
        after.identity.owner_generation
    );
    assert_eq!(after.identity.chatgpt_user_id.as_deref(), Some("user-b"));
    assert_eq!(
        after.headers.get("Authorization").map(String::as_str),
        Some("Bearer new-access")
    );
}

#[test]
fn managed_cloud_source_rejects_api_keys_static_bearers_and_custom_oauth_routes() {
    use klaus_agent_core::provider::auth::StaticBearerAuth;
    use klaus_agent_core::provider::oauth::{OAuthAuth, OAuthAuthOptions, OAuthTokens};
    let backend = "https://chatgpt.com/backend-api/codex";
    let api = OpenAIProvider::new(OpenAIProviderOptions {
        api: Some(OpenAiApi::Responses),
        api_key: Some("fixture".into()),
        base_url: Some(backend.into()),
        ..Default::default()
    });
    assert!(api.managed_review_cloud_auth_source().is_none());
    let bearer = OpenAIProvider::new(OpenAIProviderOptions {
        api: Some(OpenAiApi::Responses),
        auth_source: Some(Arc::new(StaticBearerAuth::new(
            "fixture",
            Some(backend.into()),
            HashMap::new(),
        ))),
        ..Default::default()
    });
    assert!(bearer.managed_review_cloud_auth_source().is_none());
    let oauth = Arc::new(OAuthAuth::new(OAuthAuthOptions {
        tokens: Some(OAuthTokens {
            access_token: managed_identity_token("user", "account", "business"),
            ..Default::default()
        }),
        base_url: Some("https://gateway.example/v1".into()),
        ..Default::default()
    }));
    let custom = OpenAIProvider::new(OpenAIProviderOptions {
        api: Some(OpenAiApi::Responses),
        auth_source: Some(oauth),
        base_url: Some("https://gateway.example/v1".into()),
        ..Default::default()
    });
    assert!(custom.managed_review_cloud_auth_source().is_none());
}

#[tokio::test]
async fn native_subscription_backend_proxy_preserves_scoped_reviewer_headers() {
    use klaus_agent_core::provider::auth::StaticBearerAuth;
    use klaus_agent_core::provider::oauth::{OAuthAuth, OAuthAuthOptions, OAuthTokens};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend = format!(
        "http://{}/proxy/backend-api/codex/",
        listener.local_addr().unwrap()
    );
    let auth = Arc::new(OAuthAuth::new(OAuthAuthOptions {
        tokens: Some(OAuthTokens {
            access_token: "oauth-fixture".into(),
            id_token: Some(managed_identity_token("user", "account", "business")),
            ..Default::default()
        }),
        base_url: Some(backend.clone()),
        ..Default::default()
    }));
    let root: Arc<dyn ModelProvider> = Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
        api: Some(OpenAiApi::Responses),
        auth_source: Some(auth),
        capabilities: Some(metadata(&["low"], "low")),
        ..Default::default()
    }));
    let cloud = root
        .managed_review_cloud_auth_source()
        .expect("configured native backend proxy");
    let current = cloud.current().await.unwrap().unwrap();
    assert_eq!(current.identity.backend_base_url, backend);
    assert_eq!(current.identity.plan_type.as_deref(), Some("business"));
    let preferred: RuntimeString = "codex-auto-review".into();
    let targets = [
        (
            capture_review_model_target(root.clone(), preferred.clone(), metadata(&["low"], "low")),
            preferred.clone(),
        ),
        (
            capture_review_model_target(
                root.clone(),
                "explicit-review".into(),
                metadata(&["low"], "low"),
            ),
            "explicit-review".into(),
        ),
        (
            capture_review_model_target(
                Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
                    api: Some(OpenAiApi::Responses),
                    api_key: Some("api-fixture".into()),
                    base_url: Some(backend.clone()),
                    ..Default::default()
                })),
                preferred.clone(),
                metadata(&["low"], "low"),
            ),
            preferred.clone(),
        ),
        (
            capture_review_model_target(
                Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
                    api: Some(OpenAiApi::Responses),
                    auth_source: Some(Arc::new(StaticBearerAuth::new(
                        "static-fixture",
                        Some(backend),
                        HashMap::new(),
                    ))),
                    ..Default::default()
                })),
                preferred.clone(),
                metadata(&["low"], "low"),
            ),
            preferred.clone(),
        ),
        (root, preferred),
    ];
    let server = tokio::spawn(async move {
        let mut captured = Vec::new();
        for _ in 0..5 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let header = loop {
                let mut chunk = [0u8; 8192];
                let read = socket.read(&mut chunk).await.unwrap();
                assert_ne!(read, 0);
                assert!(bytes.len() + read <= 128 * 1024);
                bytes.extend_from_slice(&chunk[..read]);
                if let Some(end) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
                    let header = String::from_utf8(bytes[..end].to_vec()).unwrap();
                    let length = header
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap();
                    if bytes.len() >= end + 4 + length {
                        break header.to_ascii_lowercase();
                    }
                }
            };
            captured.push(header);
            let body = "data: {\"type\":\"response.completed\",\"response\":{\"output\":[]}}\n\n";
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",body.len(),body).as_bytes()).await.unwrap();
        }
        captured
    });
    for (provider, model) in targets {
        tokio::time::timeout(
            Duration::from_secs(5),
            provider
                .stream(request(&model), AbortSignal::new())
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
    }
    let headers = server.await.unwrap();
    assert!(headers[0].starts_with("post /proxy/backend-api/codex/responses "));
    assert!(headers[0].contains("x-codex-guardian: reviewer"));
    for header in &headers[1..] {
        assert!(!header.contains("x-codex-guardian"), "{header}");
    }
    assert!(headers[0].contains("authorization: bearer oauth-fixture"));
    assert!(headers[2].contains("authorization: bearer api-fixture"));
    assert!(headers[3].contains("authorization: bearer static-fixture"));
}

#[tokio::test]
async fn declared_native_routes_control_attribution_independently_from_auth_routing() {
    use klaus_agent_core::provider::auth::{AuthSource, StaticBearerAuth};
    use klaus_agent_core::provider::oauth::{OAuthAuth, OAuthAuthOptions, OAuthTokens};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let runtime_root = format!("http://{address}/auth-runtime-route");
    let server = tokio::spawn(async move {
        let mut requests = Vec::new();
        for _ in 0..5 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let captured = loop {
                let mut chunk = [0u8; 8192];
                let read = socket.read(&mut chunk).await.unwrap();
                assert_ne!(read, 0);
                assert!(bytes.len() + read <= 128 * 1024);
                bytes.extend_from_slice(&chunk[..read]);
                if let Some(end) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
                    let header = String::from_utf8(bytes[..end].to_vec()).unwrap();
                    let length = header
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap();
                    if bytes.len() >= end + 4 + length {
                        let body: Value =
                            serde_json::from_slice(&bytes[end + 4..end + 4 + length]).unwrap();
                        break (header.to_ascii_lowercase(), body);
                    }
                }
            };
            requests.push(captured);
            let body = "data: {\"type\":\"response.completed\",\"response\":{\"output\":[]}}\n\n";
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",body.len(),body).as_bytes()).await.unwrap();
        }
        requests
    });
    let mut estimates = Vec::new();
    for (declared, api_key, subscription, expected) in [
        (None, None, true, true),
        (
            Some(format!("http://{address}/declared/backend-api/codex/")),
            None,
            true,
            true,
        ),
        (
            Some(format!("http://{address}/custom/v1")),
            None,
            true,
            false,
        ),
        (
            Some(format!("http://{address}/declared/backend-api/codex/")),
            Some("fixture-key".to_string()),
            true,
            false,
        ),
        (
            Some(format!("http://{address}/declared/backend-api/codex/")),
            None,
            false,
            false,
        ),
    ] {
        let auth: Arc<dyn AuthSource> = if subscription {
            Arc::new(OAuthAuth::new(OAuthAuthOptions {
                tokens: Some(OAuthTokens {
                    access_token: "native-oauth-fixture".into(),
                    ..Default::default()
                }),
                base_url: Some(runtime_root.clone()),
                ..Default::default()
            }))
        } else {
            Arc::new(StaticBearerAuth::new(
                "static-fixture",
                Some(runtime_root.clone()),
                HashMap::new(),
            ))
        };
        let root: Arc<dyn ModelProvider> = Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
            api: Some(OpenAiApi::Responses),
            base_url: declared,
            api_key,
            auth_source: Some(auth),
            ..Default::default()
        }));
        assert_eq!(root.managed_review_cloud_auth_source().is_some(), expected);
        let target = capture_review_model_target(
            root,
            "codex-auto-review".into(),
            metadata(&["low"], "low"),
        );
        let mut request = request(&"codex-auto-review".into());
        request.tools = runtime_codec::decode_text(&RuntimeString::from(json!([
            {"name":"search_a","description":"Search local evidence A.","kind":"tool_search","inputSchema":{"type":"object","properties":{}}},
            {"name":"search_b","description":"Search local evidence B.","kind":"tool_search","inputSchema":{"type":"object","properties":{}}}
        ]).to_string())).unwrap();
        estimates.push(target.approval_request_tokens(&request).unwrap());
        let events = tokio::time::timeout(
            Duration::from_secs(5),
            target
                .stream(request, AbortSignal::new())
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, ModelStreamEvent::MessageStop { .. })),
            "{events:?}"
        );
    }
    let requests = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
    for (index, ((header, body), estimate)) in requests.iter().zip(estimates).enumerate() {
        assert!(header.starts_with("post /auth-runtime-route/responses "));
        let native = index < 2;
        assert_eq!(header.contains("x-codex-guardian: reviewer"), native);
        assert_eq!(body.get("max_output_tokens").is_none(), native);
        for tool in body["tools"].as_array().unwrap() {
            // Every Responses target speaks OpenAI's tool_search enum now.
            assert_eq!(tool["execution"], json!("client"));
        }
        let input = body["input"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| serde_json::to_vec(item).unwrap().len().div_ceil(4))
            .sum::<usize>();
        let instructions = body
            .get("instructions")
            .map(|value| serde_json::to_vec(value).unwrap().len().div_ceil(4))
            .unwrap_or_default();
        let framing = serde_json::to_vec(&json!([body.get("tools"), body.get("text")]))
            .unwrap()
            .len()
            .div_ceil(4);
        assert_eq!(estimate, input + instructions + framing);
    }
}

#[tokio::test]
async fn ordinary_and_private_review_terminal_semantics_remain_independent() {
    use klaus_agent_core::types::provider::ModelStopReason;
    for interrupted in [false, true] {
        for review in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let root: Arc<dyn ModelProvider> =
                Arc::new(OpenAIProvider::new(OpenAIProviderOptions {
                    api: Some(OpenAiApi::Responses),
                    api_key: Some("fixture-only".into()),
                    base_url: Some(format!("http://{}", listener.local_addr().unwrap())),
                    ..Default::default()
                }));
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                read_request(&mut socket).await;
                let response = if interrupted {
                    json!({"type":"response.incomplete","response":{"output":[],"incomplete_details":{"reason":"interrupted"}}})
                } else {
                    json!({"type":"response.completed","response":{"output":[],"end_turn":false}})
                };
                let body = format!("data: {response}\n\n");
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            });
            let provider = if review {
                capture_review_model_target(root, "review".into(), ModelCapabilities::default())
            } else {
                root
            };
            let events = tokio::time::timeout(
                Duration::from_secs(5),
                provider
                    .stream(request(&"review".into()), AbortSignal::new())
                    .collect::<Vec<_>>(),
            )
            .await
            .unwrap();
            let expected = if review {
                ModelStopReason::Continue
            } else {
                ModelStopReason::EndTurn
            };
            assert!(
                matches!(events.last(),Some(ModelStreamEvent::MessageStop{stop_reason,..}) if *stop_reason==expected),
                "review={review}, interrupted={interrupted}: {events:?}"
            );
            assert!(
                !events
                    .iter()
                    .any(|event| matches!(event, ModelStreamEvent::Error { .. })),
                "{events:?}"
            );
            tokio::time::timeout(Duration::from_secs(5), server)
                .await
                .unwrap()
                .unwrap();
        }
    }
}
