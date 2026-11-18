//! Product-owned direct-cloud transport and account-isolation fixtures.

use super::*;
use futures::StreamExt;
use klaus_agent_core::runtime_process::{
    remove_process_environment_var, set_process_environment_var,
};
use klaus_agent_core::runtime_values::runtime_codec;
use klaus_agent_core::types::provider::{ModelRequest, ModelStreamEvent};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Fixture {
    endpoint: String,
    requests: Arc<Mutex<Vec<Value>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn fixture() -> Fixture {
    // Pin the protocol identity so fixture-backed tests never spawn the
    // installed app's helper processes and see a deterministic value.
    set_process_environment_var("KLAUS_ANTIGRAVITY_HUB_IDENTITY", "9.9.9:123456");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = requests.clone();
    let task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let captured = captured.clone();
            tokio::spawn(async move {
                let mut bytes = Vec::new();
                let (header_end, length) = loop {
                    let mut chunk = [0u8; 4096];
                    let count = socket.read(&mut chunk).await.unwrap();
                    if count == 0 {
                        return;
                    }
                    bytes.extend_from_slice(&chunk[..count]);
                    assert!(bytes.len() < 2 * 1024 * 1024);
                    if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&bytes[..index]);
                        let length = header
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .and_then(|value| value.trim().parse::<usize>().ok())
                            })
                            .unwrap_or(0);
                        break (index + 4, length);
                    }
                };
                while bytes.len() < header_end + length {
                    let mut chunk = [0u8; 4096];
                    let count = socket.read(&mut chunk).await.unwrap();
                    if count == 0 {
                        return;
                    }
                    bytes.extend_from_slice(&chunk[..count]);
                }
                let header = String::from_utf8_lossy(&bytes[..header_end]);
                let path = header
                    .lines()
                    .next()
                    .unwrap()
                    .split_whitespace()
                    .nth(1)
                    .unwrap()
                    .to_owned();
                let body: Value = serde_json::from_slice(&bytes[header_end..header_end + length])
                    .unwrap_or(Value::Null);
                captured
                    .lock()
                    .unwrap()
                    .push(json!({"path":path,"body":body,"userAgent":header.lines().find_map(|line|line.to_ascii_lowercase().strip_prefix("user-agent:").map(|value|value.trim().to_string()))}));
                let payload = if path == "/token" {
                    json!({"access_token":"fixture-renewed","refresh_token":"fixture-rotated","expires_in":3600})
                } else if path.ends_with(":loadCodeAssist") {
                    json!({"cloudaicompanionProject":"fixture-project","currentTier":{"id":"free","name":"Free"}})
                } else if path.ends_with(":fetchAvailableModels") {
                    json!({
                        "defaultAgentModelId":"fixture-level",
                        "models":{
                            "fixture-level":{"displayName":"Level Model","supportsImages":true,"supportedMimeTypes":{"audio/wav":true},"supportsThinking":true,"thinkingLevel":1,"maxTokens":100000,"maxOutputTokens":8192},
                            "fixture-budget":{"displayName":"Budget Model","supportsThinking":true,"thinkingBudget":4000,"minThinkingBudget":32,"maxTokens":200000,"maxOutputTokens":8192},
                            "fixture-plain":{"displayName":"Plain Model","supportsTools":false,"maxTokens":50000,"maxOutputTokens":4096},
                            "fixture-locked":{"displayName":"Locked Model","disabled":true,"supportsThinking":true,"thinkingBudget":4000,"minThinkingBudget":32,"maxTokens":200000,"maxOutputTokens":8192},
                            "fixture-stream-locked":{"displayName":"Stream Locked Model","disabled":true},
                            "fixture-http-disabled":{"displayName":"Disabled Account Model"},
                            "fixture-stream-disabled":{"displayName":"Disabled Stream Model"},
                            "fixture-http-restricted":{"displayName":"Restricted Account Model"},
                            "fixture-stream-restricted":{"displayName":"Restricted Stream Model"},
                            "fixture-stream-quota":{"displayName":"Stream Quota Model"},
                            "fixture-http-quota":{"displayName":"HTTP Quota Model"},
                            "fixture-notfound-listed":{"displayName":"Listed NotFound Model"},
                            "fixture-image":{"displayName":"Image Model"},
                            "fixture-completion":{"displayName":"Completion Model"},
                        },
                        "tabModelIds":["fixture-completion"],
                        "imageGenerationModelIds":["fixture-image"],"webSearchModelIds":["fixture-search"],"audioTranscriptionModelIds":["fixture-audio"],
                    })
                } else if path.ends_with(":retrieveUserQuota") {
                    json!({"buckets":[{"modelId":"fixture-level","remainingFraction":0.75,"resetTime":"2027-01-01T00:00:00Z"}]})
                } else if path.ends_with(":retrieveUserQuotaSummary") {
                    json!({"groups":[{"displayName":"Model allowance","buckets":[{"bucketId":"five-hour","remainingAmount":"30","window":"FIVE_HOUR"}]}]})
                } else if path.ends_with(":generateContent") && body["requestType"] == "image_gen" {
                    json!({"response":{"candidates":[{"content":{"parts":[{"inlineData":{"mimeType":"image/png","data":"aW1hZ2U="}}]},"finishReason":"STOP"}]}})
                } else if path.ends_with(":generateContent") && body["requestType"] == "web_search"
                {
                    json!({"response":{"candidates":[{"content":{"parts":[{"text":"Grounded answer"}]},"groundingMetadata":{"webSearchQueries":["fixture"],"groundingChunks":[{"web":{"uri":"https://example.com/source","title":"Source"}}]}}]}})
                } else {
                    Value::Null
                };
                let (content_type, response) = if path.contains(":streamGenerateContent") {
                    let model = body["model"].as_str().unwrap_or("fixture");
                    let first = json!({"response":{"modelVersion":model,"candidates":[{"content":{"parts":[{"thought":true,"text":"Thinking"},{"text":"OK"}]}}]}});
                    let last = json!({"response":{"candidates":[{"finishReason":"STOP","content":{"parts":[{"text":"","thoughtSignature":"fixture-signature"}]}}],"usageMetadata":{"promptTokenCount":4,"candidatesTokenCount":2}}});
                    (
                        "text/event-stream",
                        if model == "fixture-notice" {
                            format!("data: {first}\n\n")
                        } else {
                            format!("data: {first}\n\ndata: {last}\n\n")
                        },
                    )
                } else {
                    ("application/json", payload.to_string())
                };
                let model = body["model"].as_str().unwrap_or("");
                let not_found = json!({"error":{"code":404,"status":"NOT_FOUND","message":"Requested entity was not found."}});
                let refusal = if model.ends_with("-disabled") {
                    json!({"error":{"code":403,"status":"PERMISSION_DENIED","message":"This service has been disabled in this account for violation of Terms of Service."}})
                } else if model.ends_with("-restricted") {
                    json!({"error":{"code":403,"status":"PERMISSION_DENIED","message":"Antigravity is not available for this account."}})
                } else if model.contains("quota") {
                    json!({"error":{"code":429,"status":"RESOURCE_EXHAUSTED","message":"Your model quota is exhausted.","requestId":"fixture-request-123","details":[{"reason":"QUOTA_EXHAUSTED"},{"retryDelay":"2s"}]},"privatePrompt":"not-public"})
                } else {
                    json!({"error":{"code":403,"status":"PERMISSION_DENIED","message":"Your subscription does not permit access to this model.","details":[{"reason":"MODEL_ACCESS_DENIED"}]},"privatePrompt":"not-public"})
                };
                let (status, content_type, response) = if (body["project"] == "stale-project"
                    && (path.contains(":streamGenerateContent")
                        || path.contains(":fetchAvailableModels")))
                    || matches!(
                        model,
                        "fixture-notfound" | "fixture-notfound-listed" | "fixture-vanished"
                    ) {
                    (404, "application/json", not_found.to_string())
                } else if model.starts_with("fixture-stream-") {
                    (200, "text/event-stream", format!("data: {refusal}\n\n"))
                } else if matches!(
                    model,
                    "fixture-locked"
                        | "claude-sonnet-5-5-max"
                        | "gemini-4-argon-high"
                        | "fixture-http-quota"
                        | "fixture-http-disabled"
                        | "fixture-http-restricted"
                ) {
                    (
                        if model.contains("quota") { 429 } else { 403 },
                        "application/json",
                        refusal.to_string(),
                    )
                } else {
                    (200, content_type, response)
                };
                let reply = format!(
                    "HTTP/1.1 {status} Fixture\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",
                    response.len()
                );
                let _ = socket.write_all(reply.as_bytes()).await;
            });
        }
    });
    Fixture {
        endpoint,
        requests,
        task,
    }
}

fn decode<T: klaus_agent_core::runtime_values::RuntimeDecode>(value: Value) -> T {
    runtime_codec::decode_text(&RuntimeString::from(value.to_string())).unwrap()
}

struct OAuthFixture {
    endpoint: String,
    started: tokio::sync::oneshot::Receiver<()>,
    release: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

async fn oauth_fixture(status: u16, payload: &'static str) -> OAuthFixture {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/token", listener.local_addr().unwrap());
    let (started_tx, started) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0u8; 8192];
        let _ = socket.read(&mut request).await.unwrap();
        let _ = started_tx.send(());
        let _ = released.await;
        let response = format!(
            "HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
            payload.len()
        );
        let _ = socket.write_all(response.as_bytes()).await;
    });
    OAuthFixture {
        endpoint,
        started,
        release,
        task,
    }
}

fn expiring_credential() -> auth::Credential {
    serde_json::from_value(json!({
        "accessToken":"fixture-expiring", "refreshToken":"fixture-refresh",
        "expiresAt":chrono::Utc::now().timestamp_millis()+60_000,
        "authMethod":"consumer",
    }))
    .unwrap()
}

// Work item 20261022: retained audio must never use a replacement account.
// The recovery request previously acquired the current credential unconditionally.
#[tokio::test]
async fn retained_audio_rejects_a_replaced_account_before_any_provider_request() {
    let root = tempfile::tempdir().unwrap();
    set_config_root(root.path());
    let fixture = fixture().await;
    set_process_environment_var("KLAUS_ANTIGRAVITY_API_URL", &fixture.endpoint);
    let mut credential = expiring_credential();
    credential.expires_at = Some(chrono::Utc::now().timestamp_millis() + 3_600_000);
    credential.refresh_token = None;
    auth::adopt(credential.clone()).await.unwrap();
    let original = auth::generation().unwrap();
    // Re-authenticating the same account still creates a new credential identity.
    auth::adopt(credential).await.unwrap();
    assert_ne!(auth::generation().as_deref(), Some(original.as_str()));
    let params = serde_json::from_value(json!({
        "audioBase64":"AAAA", "mimeType":"audio/wav",
        "expectedCredentialGeneration":original,
    }))
    .unwrap();
    let result = transcribe_audio(params, AbortSignal::new()).await;
    remove_process_environment_var("KLAUS_ANTIGRAVITY_API_URL");
    assert_eq!(result.unwrap_err(), "antigravity_credential_changed");
    assert!(fixture.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn refresh_denials_require_login_only_for_permanent_oauth_errors() {
    let root = tempfile::tempdir().unwrap();
    set_config_root(root.path());
    for (status, payload, permanent) in [
        (
            400,
            r#"{"error":"invalid_grant","error_description":"private detail"}"#,
            true,
        ),
        (401, r#"{"error":"unauthorized"}"#, true),
        (429, r#"{"error":"invalid_grant"}"#, false),
        (503, r#"{"error":"temporarily_unavailable"}"#, false),
    ] {
        let fixture = oauth_fixture(status, payload).await;
        set_process_environment_var("KLAUS_ANTIGRAVITY_OAUTH_URL", &fixture.endpoint);
        auth::adopt(expiring_credential()).await.unwrap();
        let refresh = tokio::spawn(async { auth::credential(None).await });
        fixture.started.await.unwrap();
        let _ = fixture.release.send(());
        let error = match refresh.await.unwrap() {
            Ok(_) => panic!("refresh must fail"),
            Err(error) => error,
        };
        assert_eq!(
            error,
            if permanent {
                "antigravity_login_expired"
            } else {
                "antigravity_refresh_unavailable"
            }
        );
        let status = auth::status();
        assert_eq!(status["needsLogin"], permanent);
        assert_eq!(status["canRefresh"], !permanent);
        assert!(!status.to_string().contains("private detail"));
        if permanent {
            assert!(
                matches!(auth::credential(None).await,Err(error) if error=="antigravity_login_expired")
            );
        }
        fixture.task.await.unwrap();
    }
    auth::logout().await.unwrap();
    remove_process_environment_var("KLAUS_ANTIGRAVITY_OAUTH_URL");
}

#[tokio::test]
async fn a_late_permanent_refresh_denial_cannot_poison_a_replacement_account() {
    let root = tempfile::tempdir().unwrap();
    set_config_root(root.path());
    let fixture = oauth_fixture(400, r#"{"error":"invalid_grant"}"#).await;
    set_process_environment_var("KLAUS_ANTIGRAVITY_OAUTH_URL", &fixture.endpoint);
    auth::adopt(expiring_credential()).await.unwrap();
    let refresh = tokio::spawn(async { auth::credential(None).await });
    fixture.started.await.unwrap();
    auth::adopt(serde_json::from_value(json!({
        "accessToken":"fixture-replacement", "account":"replacement@example.test", "authMethod":"consumer"
    })).unwrap()).await.unwrap();
    let _ = fixture.release.send(());
    assert!(matches!(refresh.await.unwrap(),Err(error) if error=="antigravity_credential_changed"));
    let status = auth::status();
    assert_eq!(status["account"], "replacement@example.test");
    assert_eq!(status["needsLogin"], false);
    assert!(auth::credential(None).await.is_ok());
    auth::logout().await.unwrap();
    fixture.task.await.unwrap();
    remove_process_environment_var("KLAUS_ANTIGRAVITY_OAUTH_URL");
}

#[tokio::test]
async fn direct_cloud_account_refresh_catalog_native_media_and_mixed_models() {
    let fixture = fixture().await;
    let root = tempfile::tempdir().unwrap();
    set_config_root(root.path());
    set_process_environment_var("KLAUS_ANTIGRAVITY_API_URL", &fixture.endpoint);
    set_process_environment_var(
        "KLAUS_ANTIGRAVITY_OAUTH_URL",
        format!("{}/token", fixture.endpoint),
    );
    let adopted = auth::adopt(auth::Credential {
        access_token: "fixture-expired".into(),
        refresh_token: Some("fixture-refresh".into()),
        expires_at: Some(1),
        account: Some("fixture@example.test".into()),
        auth_method: "consumer".into(),
        project: None,
        client_id: None,
        client_secret: None,
    })
    .await
    .unwrap();
    refresh_catalog(&auth::generation().unwrap()).await.unwrap();
    assert_eq!(adopted["ok"], true);
    assert_eq!(adopted["loggedIn"], true);
    assert!(!adopted.to_string().contains("fixture-expired"));
    let listed = models(true).await.unwrap();
    assert_eq!(listed["models"][0]["key"], "fixture-level");
    assert_eq!(
        main_model("auto", &serde_json::Map::new()).await.unwrap(),
        "fixture-level"
    );
    let allowance = usage(false).await.unwrap();
    assert!(allowance["windows"]
        .as_array()
        .unwrap()
        .iter()
        .any(|window| window["usedPercent"] == 25.0));
    let available = capabilities().await.unwrap();
    assert_eq!(available["image"]["available"], true);
    assert_eq!(available["imageEdit"]["available"], true);
    assert_eq!(available["search"]["available"], true);
    assert_eq!(available["transcription"]["available"], true);
    let image = generate_image(
        super::super::wire::AntigravityImageParams {
            prompt: "A blue square".into(),
            model: None,
            size: Some("1024x1024".into()),
            reference_images: vec![super::super::wire::AntigravityReferenceImage {
                data_base64: "aW1hZ2U=".into(),
                mime_type: "image/png".into(),
            }],
            operation_id: None,
            session_id: None,
        },
        AbortSignal::new(),
    )
    .await
    .unwrap();
    assert_eq!(image["imageBase64"], "aW1hZ2U=");
    let search = search(
        super::super::wire::AntigravitySearchParams {
            query: "fixture".into(),
            model: None,
            allowed_domains: Vec::new(),
            blocked_domains: Vec::new(),
            freshness: None,
            operation_id: None,
            session_id: None,
        },
        AbortSignal::new(),
    )
    .await
    .unwrap();
    assert_eq!(search["results"][0]["url"], "https://example.com/source");
    for model in ["fixture-level", "fixture-budget", "fixture-plain"] {
        let resolved = resolve(model, &serde_json::Map::new()).await.unwrap();
        assert_eq!(resolved.auth, Value::Null);
        let init: ProtocolSessionInit = decode(
            json!({"provider":resolved.provider,"capabilities":resolved.capabilities,"capabilitiesByModel":resolved.capabilities_by_model,"modelSource":{"source":"antigravity","modelId":model}}),
        );
        let base: Arc<dyn ModelProvider> = Arc::new(GeminiProvider::new(GeminiProviderOptions {
            default_model: Some(model.into()),
            capabilities: Some(decode(resolved.capabilities)),
            ..Default::default()
        }));
        let provider = decorate(base, &init);
        assert!(provider.uses_model_tool_budget(&RuntimeString::from(model)));
        assert_eq!(
            provider.capabilities(&RuntimeString::from(model)).tools,
            model != "fixture-plain"
        );
        let mut request: ModelRequest = decode(
            json!({"model":model,"system":[],"messages":[],"tools":[],"maxOutputTokens":100}),
        );
        if model == "fixture-level" {
            request
                .messages
                .push(klaus_agent_core::schemas::message::Message::User(
                    klaus_agent_core::schemas::message::UserMessage {
                        uuid: "native-audio".into(),
                        is_meta: None,
                        question_answer: None,
                        content: vec![klaus_agent_core::schemas::content::ContentBlock::Media(
                            klaus_agent_core::schemas::content::MediaBlock {
                                mime_type: "audio/wav".into(),
                                base64: "UklGRg==".into(),
                                filename: Some("fixture.wav".into()),
                            },
                        )],
                    },
                ));
        }
        let events: Vec<_> = provider.stream(request, AbortSignal::new()).collect().await;
        assert!(events
            .iter()
            .any(|event| matches!(event,ModelStreamEvent::TextDelta {text} if text=="OK")));
        assert!(events.iter().any(|event| matches!(event,ModelStreamEvent::ThinkingBlock {signature:Some(signature),..} if signature.starts_with("gemini-thought:"))));
        assert!(!events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::Error { .. })));
    }
    let auxiliary = resolve("fixture-budget", &serde_json::Map::new())
        .await
        .unwrap();
    let target=klaus_agent_protocol::server::resolved_model_target(&json!({
        "type":"resolved","source":"antigravity","modelId":"fixture-budget",
        "provider":auxiliary.provider,"auth":auxiliary.auth,"capabilities":auxiliary.capabilities,
    }),None,&super::super::model_sources::provider_decorator(root.path())).await.unwrap();
    let klaus_agent_core::engine::session::InferenceTargetState::Ready(target) = target else {
        panic!("auxiliary target must resolve");
    };
    let request: ModelRequest = decode(
        json!({"model":"fixture-budget","system":[],"messages":[],"tools":[],"maxOutputTokens":100,"thinking":{"type":"adaptive"}}),
    );
    let events: Vec<_> = target
        .provider
        .stream(request, AbortSignal::new())
        .collect()
        .await;
    assert!(!events
        .iter()
        .any(|event| matches!(event, ModelStreamEvent::Error { .. })));
    let locked = resolve("fixture-locked", &serde_json::Map::new())
        .await
        .unwrap();
    assert_eq!(locked.provider["model"], "fixture-locked");
    {
        let requests = fixture.requests.lock().unwrap();
        let streams: Vec<_> = requests
            .iter()
            .filter(|request| {
                request["path"]
                    .as_str()
                    .unwrap()
                    .contains("streamGenerateContent")
            })
            .collect();
        assert_eq!(streams.len(), 4);
        assert_eq!(
            streams[0]["body"]["request"]["contents"][0]["parts"][0]["inlineData"],
            json!({"mimeType":"audio/wav","data":"UklGRg=="})
        );
        let auxiliary_request = streams.last().unwrap();
        assert_eq!(
            auxiliary_request["body"]["request"]["generationConfig"]["thinkingConfig"]
                ["thinkingBudget"],
            4000
        );
        assert!(
            auxiliary_request["body"]["request"]["generationConfig"]["thinkingConfig"]
                .get("thinkingLevel")
                .is_none()
        );
        for request in streams {
            assert_eq!(request["body"]["userAgent"], "antigravity");
            assert_eq!(request["body"]["requestType"], "agent");
            let req_id = request["body"]["requestId"].as_str().unwrap();
            assert!(
                req_id.starts_with("agent/"),
                "expected scoped requestId, got {req_id}"
            );
            assert!(request["body"].get("userPromptId").is_none());
            assert!(request["body"]["request"]["sessionId"]
                .as_str()
                .is_some_and(|sid| sid.parse::<i64>().is_ok()));
            assert!(request["body"].get("enabledCreditTypes").is_none());
        }
        assert_eq!(
            requests
                .iter()
                .filter(|request| request["path"] == "/token")
                .count(),
            1
        );
    }
    auth::logout().await.unwrap();
    assert_eq!(auth::status()["signedIn"], false);
    remove_process_environment_var("KLAUS_ANTIGRAVITY_API_URL");
    remove_process_environment_var("KLAUS_ANTIGRAVITY_OAUTH_URL");
}

// A declared conversation model must reach its cloud source even when the
// catalog cannot establish account access. Its refusal belongs to the turn.
#[tokio::test]
async fn declared_models_reach_the_service_and_preserve_structured_refusals() {
    use klaus_agent_core::types::errors::{EngineError, ProviderErrorKind};
    let fixture = fixture().await;
    let root = tempfile::tempdir().unwrap();
    set_config_root(root.path());
    set_process_environment_var("KLAUS_ANTIGRAVITY_API_URL", &fixture.endpoint);
    auth::adopt(
        serde_json::from_value(json!({"accessToken":"fixture-token","authMethod":"consumer"}))
            .unwrap(),
    )
    .await
    .unwrap();
    refresh_catalog(&auth::generation().unwrap()).await.unwrap();
    let listed = models(true).await.unwrap();
    let paid = listed["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["key"] == "claude-sonnet-5-5-max")
        .unwrap();
    assert_eq!(paid["available"], false);
    assert_eq!(paid["entitlementUnknown"], true);
    for (model, code, kind, status) in [
        (
            "fixture-locked",
            "antigravity_model_access_denied",
            ProviderErrorKind::InvalidRequest,
            403,
        ),
        (
            "claude-sonnet-5-5-max",
            "antigravity_model_access_denied",
            ProviderErrorKind::InvalidRequest,
            403,
        ),
        (
            "gemini-4-argon-high",
            "antigravity_model_access_denied",
            ProviderErrorKind::InvalidRequest,
            403,
        ),
        (
            "fixture-stream-locked",
            "antigravity_model_access_denied",
            ProviderErrorKind::InvalidRequest,
            403,
        ),
        (
            "fixture-http-quota",
            "antigravity_quota_exhausted",
            ProviderErrorKind::Quota,
            429,
        ),
        (
            "fixture-stream-quota",
            "antigravity_quota_exhausted",
            ProviderErrorKind::Quota,
            429,
        ),
        (
            "fixture-http-disabled",
            "antigravity_account_disabled",
            ProviderErrorKind::InvalidRequest,
            403,
        ),
        (
            "fixture-stream-disabled",
            "antigravity_account_disabled",
            ProviderErrorKind::InvalidRequest,
            403,
        ),
        (
            "fixture-http-restricted",
            "antigravity_account_restricted",
            ProviderErrorKind::InvalidRequest,
            403,
        ),
        (
            "fixture-stream-restricted",
            "antigravity_account_restricted",
            ProviderErrorKind::InvalidRequest,
            403,
        ),
    ] {
        let tiers = serde_json::from_value(json!({"high":"claude-sonnet-5-5-max"})).unwrap();
        let resolved = resolve(model, &tiers).await.unwrap();
        assert_eq!(resolved.provider["model"], model);
        assert_eq!(resolved.model_tiers["high"], "claude-sonnet-5-5-max");
        let init: ProtocolSessionInit = decode(
            json!({"provider":resolved.provider,"capabilities":resolved.capabilities,"capabilitiesByModel":resolved.capabilities_by_model,"modelSource":{"source":"antigravity","modelId":model}}),
        );
        let base: Arc<dyn ModelProvider> = Arc::new(GeminiProvider::new(GeminiProviderOptions {
            default_model: Some(model.into()),
            capabilities: Some(decode(resolved.capabilities)),
            ..Default::default()
        }));
        let provider = decorate(base, &init);
        let request: ModelRequest = decode(
            json!({"model":model,"system":[],"messages":[],"tools":[],"maxOutputTokens":100,"thinking":{"type":"adaptive"}}),
        );
        let events: Vec<_> = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            provider.stream(request, AbortSignal::new()).collect(),
        )
        .await
        .expect("terminal refusals must not back off");
        let errors: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                ModelStreamEvent::Error { error } => Some(error),
                _ => None,
            })
            .collect();
        assert_eq!(errors.len(), 1, "{model}: {events:?}");
        let error = errors[0];
        assert_eq!(error.kind, kind);
        assert_eq!(error.status, Some(status));
        assert_eq!(error.code.as_ref().unwrap(), code);
        assert_eq!(
            error.error_type.as_ref().unwrap(),
            if status == 403 {
                "PERMISSION_DENIED"
            } else {
                "RESOURCE_EXHAUSTED"
            }
        );
        assert!(error
            .message
            .contains(if code == "antigravity_account_disabled" {
                "service has been disabled"
            } else if code == "antigravity_account_restricted" {
                "not available for this account"
            } else if status == 403 {
                "subscription does not permit access"
            } else {
                "quota is exhausted"
            }));
        assert!(!error.message.contains("not-public"));
        if status == 429 {
            assert_eq!(error.retry_after_ms, Some(2000));
            assert_eq!(error.request_id.as_ref().unwrap(), "fixture-request-123");
        }
        let public = runtime_codec::encode_text(&EngineError::from_provider(error.clone(), ""))
            .unwrap()
            .try_to_utf8()
            .unwrap();
        assert!(public.contains(code));
        assert!(public.contains("providerError"));
        assert!(!events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::MessageStop { .. })));
    }
    assert_eq!(auth::status()["loggedIn"], true);
    assert_eq!(auth::status()["needsLogin"], false);
    for model in [
        "unknown-unregistered",
        "fixture-image",
        "fixture-completion",
    ] {
        let error = resolve(model, &serde_json::Map::new()).await.unwrap_err();
        assert!(matches!(
            error.as_str(),
            "antigravity_model_unavailable" | "antigravity_model_purpose_unsupported"
        ));
    }
    {
        let captured = fixture.requests.lock().unwrap();
        let streams: Vec<_> = captured
            .iter()
            .filter(|request| {
                request["path"]
                    .as_str()
                    .unwrap()
                    .contains("streamGenerateContent")
            })
            .collect();
        assert_eq!(
            streams.len(),
            10,
            "no local fallback, retry, or structural-invalid request"
        );
        assert_eq!(streams[0]["body"]["model"], "fixture-locked");
        assert_eq!(
            streams[0]["body"]["request"]["generationConfig"]["thinkingConfig"]["thinkingBudget"],
            4000
        );
        for (index, level) in [(1, "MAX"), (2, "HIGH")] {
            let config = &streams[index]["body"]["request"]["generationConfig"]["thinkingConfig"];
            assert_eq!(config["thinkingLevel"], level);
            assert!(config.get("thinkingBudget").is_none());
        }
    }
    auth::logout().await.unwrap();
    remove_process_environment_var("KLAUS_ANTIGRAVITY_API_URL");
}

#[test]
fn facade_schemas_publish_secret_input_and_closed_native_shapes() {
    let bundle = super::super::schema_bundle();
    for method in [
        "status",
        "adoptCredential",
        "switchAccount",
        "logout",
        "models",
        "usage",
        "capabilities",
        "generateImage",
        "search",
        "transcribeAudio",
        "cancelOperation",
    ] {
        assert!(bundle["requests"]
            .get(format!("antigravity/{method}"))
            .is_some());
    }
    assert!(serde_json::from_value::<super::super::wire::AntigravityAdoptParams>(json!({
        "credential":{"accessToken":"fixture","authMethod":"consumer","unexpectedSecret":"refuse"}
    })).is_err());
}

#[tokio::test]
async fn buffered_cloud_frames_respect_credentials_and_clean_eof_preserves_output() {
    use klaus_agent_core::provider::gemini::GeminiClient;
    let fixture = fixture().await;
    let root = tempfile::tempdir().unwrap();
    set_config_root(root.path());
    set_process_environment_var("KLAUS_ANTIGRAVITY_API_URL", &fixture.endpoint);
    auth::adopt(auth::Credential {
        access_token: "fixture".into(),
        refresh_token: None,
        expires_at: None,
        account: None,
        auth_method: "consumer".into(),
        project: Some("fixture-project".into()),
        client_id: None,
        client_secret: None,
    })
    .await
    .unwrap();
    let client = cloud::CloudClient {
        generation: auth::generation().unwrap(),
        session: None,
    };
    let params =
        |model: &str| decode(json!({"model":model,"contents":[],"config":{"maxOutputTokens":100}}));
    let mut stream = client
        .generate_content_stream(
            params("fixture-level"),
            None,
            Vec::new(),
            AbortSignal::new(),
        )
        .await
        .unwrap();
    assert!(stream.next().await.unwrap().is_ok());
    auth::logout().await.unwrap();
    let error = stream.next().await.unwrap().unwrap_err();
    assert_eq!(error.message, "antigravity_credential_changed");
    assert!(stream.next().await.is_none());
    auth::adopt(auth::Credential {
        access_token: "fixture".into(),
        refresh_token: None,
        expires_at: None,
        account: None,
        auth_method: "consumer".into(),
        project: Some("fixture-project".into()),
        client_id: None,
        client_secret: None,
    })
    .await
    .unwrap();
    let client = cloud::CloudClient {
        generation: auth::generation().unwrap(),
        session: None,
    };
    let events: Vec<_> = client
        .generate_content_stream(
            params("fixture-notice"),
            None,
            Vec::new(),
            AbortSignal::new(),
        )
        .await
        .unwrap()
        .collect()
        .await;
    // A clean HTTP EOF preserves emitted candidates even without finishReason.
    // Abrupt transport truncation is covered by the stream recovery regressions.
    assert_eq!(events.len(), 1);
    assert!(events[0].is_ok());
    auth::logout().await.unwrap();
    remove_process_environment_var("KLAUS_ANTIGRAVITY_API_URL");
}

#[tokio::test]
async fn logout_invalidates_a_refresh_without_waiting_for_the_network() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/token", listener.local_addr().unwrap());
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0u8; 8192];
        let _ = socket.read(&mut request).await.unwrap();
        let _ = started_tx.send(());
        let _ = finish_rx.await;
        let payload = r#"{"access_token":"late-token","expires_in":3600}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
            payload.len()
        );
        let _ = socket.write_all(response.as_bytes()).await;
    });
    let root = tempfile::tempdir().unwrap();
    set_config_root(root.path());
    set_process_environment_var("KLAUS_ANTIGRAVITY_OAUTH_URL", endpoint);
    auth::adopt(auth::Credential {
        access_token: "expired".into(),
        refresh_token: Some("refresh".into()),
        expires_at: Some(1),
        account: None,
        auth_method: "consumer".into(),
        project: None,
        client_id: None,
        client_secret: None,
    })
    .await
    .unwrap();
    let refresh = tokio::spawn(async { auth::credential(None).await });
    started_rx.await.unwrap();
    tokio::time::timeout(std::time::Duration::from_millis(250), auth::logout())
        .await
        .expect("logout must not wait for refresh")
        .unwrap();
    let _ = finish_tx.send(());
    assert!(matches!(refresh.await.unwrap(),Err(error) if error=="antigravity_credential_changed"));
    assert!(!auth::path().exists());
    server.await.unwrap();
    remove_process_environment_var("KLAUS_ANTIGRAVITY_OAUTH_URL");
}

#[tokio::test]
async fn a_stale_cached_project_heals_before_a_not_found_refusal_is_trusted() {
    use klaus_agent_core::provider::gemini::GeminiClient;
    let fixture = fixture().await;
    let root = tempfile::tempdir().unwrap();
    set_config_root(root.path());
    set_process_environment_var("KLAUS_ANTIGRAVITY_API_URL", &fixture.endpoint);
    auth::adopt(auth::Credential {
        access_token: "fixture".into(),
        refresh_token: None,
        expires_at: None,
        account: None,
        auth_method: "consumer".into(),
        project: Some("stale-project".into()),
        client_id: None,
        client_secret: None,
    })
    .await
    .unwrap();
    let client = cloud::CloudClient {
        generation: auth::generation().unwrap(),
        session: None,
    };
    let events: Vec<_> = client
        .generate_content_stream(
            decode(json!({"model":"fixture-level","contents":[],"config":{"maxOutputTokens":100}})),
            None,
            Vec::new(),
            AbortSignal::new(),
        )
        .await
        .unwrap()
        .collect()
        .await;
    assert!(
        events.iter().all(|event| event.is_ok()),
        "the healed replay must stream every frame"
    );
    let (paths, first_project, replay_project) = {
        let requests = fixture.requests.lock().unwrap();
        let paths: Vec<String> = requests
            .iter()
            .map(|request| request["path"].as_str().unwrap_or_default().to_string())
            .collect();
        let first = requests[0]["body"]["project"].as_str().map(str::to_string);
        let replay = requests[2]["body"]["project"].as_str().map(str::to_string);
        (paths, first, replay)
    };
    assert_eq!(paths.len(), 3, "{paths:?}");
    assert!(paths[0].contains(":streamGenerateContent"));
    assert!(paths[1].ends_with(":loadCodeAssist"));
    assert!(paths[2].contains(":streamGenerateContent"));
    assert_eq!(first_project.as_deref(), Some("stale-project"));
    assert_eq!(replay_project.as_deref(), Some("fixture-project"));
    let healed = auth::credential(None).await.unwrap();
    assert_eq!(healed.project.as_deref(), Some("fixture-project"));
    auth::logout().await.unwrap();
    remove_process_environment_var("KLAUS_ANTIGRAVITY_API_URL");
}

#[tokio::test]
async fn a_persistent_not_found_reports_the_account_verdict_only_for_unlisted_models() {
    use klaus_agent_core::provider::classify::classify_error;
    use klaus_agent_core::provider::gemini::GeminiClient;
    use klaus_agent_core::types::errors::ProviderErrorKind;
    let fixture = fixture().await;
    let root = tempfile::tempdir().unwrap();
    set_config_root(root.path());
    set_process_environment_var("KLAUS_ANTIGRAVITY_API_URL", &fixture.endpoint);
    auth::adopt(auth::Credential {
        access_token: "fixture".into(),
        refresh_token: None,
        expires_at: None,
        account: None,
        auth_method: "consumer".into(),
        project: Some("fixture-project".into()),
        client_id: None,
        client_secret: None,
    })
    .await
    .unwrap();
    let client = cloud::CloudClient {
        generation: auth::generation().unwrap(),
        session: None,
    };
    let params =
        |model: &str| decode(json!({"model":model,"contents":[],"config":{"maxOutputTokens":100}}));
    let unlisted = client
        .generate_content_stream(
            params("fixture-notfound"),
            None,
            Vec::new(),
            AbortSignal::new(),
        )
        .await
        .err()
        .unwrap();
    assert!(
        unlisted
            .message
            .starts_with("antigravity_model_unlisted [NOT_FOUND]"),
        "{}",
        unlisted.message
    );
    let listed = client
        .generate_content_stream(
            params("fixture-notfound-listed"),
            None,
            Vec::new(),
            AbortSignal::new(),
        )
        .await
        .err()
        .unwrap();
    assert!(
        listed
            .message
            .starts_with("antigravity_request_rejected [NOT_FOUND]"),
        "{}",
        listed.message
    );
    for error in [&unlisted, &listed] {
        let classified = classify_error(&client.error_details(error));
        assert_eq!(classified.kind, ProviderErrorKind::InvalidRequest);
        assert_eq!(classified.status, Some(404));
    }
    assert_eq!(
        client.error_details(&unlisted).code.as_deref(),
        Some("antigravity_model_unlisted")
    );
    auth::logout().await.unwrap();
    remove_process_environment_var("KLAUS_ANTIGRAVITY_API_URL");
}

#[test]
fn identity_ua_carries_the_full_hub_shape() {
    let ua = cloud::identity_ua("9.9.9", "123456");
    assert!(ua.starts_with("antigravity/hub/9.9.9 (aidev_client; os_type="));
    assert!(ua.ends_with("; cl=123456)"));
    set_process_environment_var("KLAUS_ANTIGRAVITY_HUB_IDENTITY", "9.9.9:123456");
    assert_eq!(cloud::client_identity(), ua);
    remove_process_environment_var("KLAUS_ANTIGRAVITY_HUB_IDENTITY");
}

#[tokio::test]
async fn startup_surfaces_mirror_the_hub_control_plane_calls() {
    let fixture = fixture().await;
    set_process_environment_var("KLAUS_ANTIGRAVITY_API_URL", &fixture.endpoint);
    let root = tempfile::tempdir().unwrap();
    set_config_root(root.path());
    auth::adopt(auth::Credential {
        access_token: "fixture".into(),
        refresh_token: None,
        expires_at: None,
        account: None,
        auth_method: "consumer".into(),
        project: Some("fixture-project".into()),
        client_id: None,
        client_secret: None,
    })
    .await
    .unwrap();
    let result = models(true).await.unwrap();
    assert!(result["models"]
        .as_array()
        .unwrap()
        .iter()
        .any(|row| row["id"] == "gemini-4-argon-medium"));
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let done = fixture.requests.lock().unwrap().iter().any(|request| {
                request["path"]
                    .as_str()
                    .unwrap_or_default()
                    .ends_with(":buildWithGooglePlugins?page_size=1000")
            });
            if done {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the one-shot startup surfaces must complete");
    let (experiments_body, scoped_body) = {
        let requests = fixture.requests.lock().unwrap();
        let path_of = |request: &Value| request["path"].as_str().unwrap_or_default().to_string();
        let experiments = requests
            .iter()
            .find(|request| path_of(request).ends_with(":listExperiments"))
            .expect("listExperiments fires once per generation");
        let scoped = requests
            .iter()
            .find(|request| {
                path_of(request).ends_with(":loadCodeAssist")
                    && request["body"]["cloudaicompanionProject"] == "fixture-project"
            })
            .expect("the project-scoped loadCodeAssist fires");
        (experiments["body"].clone(), scoped["body"].clone())
    };
    assert_eq!(
        experiments_body,
        json!({"project":"fixture-project","metadata":{"updateChannel":"stable"}})
    );
    assert_eq!(
        scoped_body,
        json!({"cloudaicompanionProject":"fixture-project","metadata":{"ideType":"ANTIGRAVITY"}})
    );
    auth::logout().await.unwrap();
    remove_process_environment_var("KLAUS_ANTIGRAVITY_API_URL");
}

#[tokio::test]
async fn heartbeat_repeats_the_paired_control_plane_poll() {
    let fixture = fixture().await;
    set_process_environment_var("KLAUS_ANTIGRAVITY_API_URL", &fixture.endpoint);
    set_process_environment_var("KLAUS_ANTIGRAVITY_HEARTBEAT_SECS", "1");
    let root = tempfile::tempdir().unwrap();
    set_config_root(root.path());
    auth::adopt(auth::Credential {
        access_token: "fixture".into(),
        refresh_token: None,
        expires_at: None,
        account: None,
        auth_method: "consumer".into(),
        project: Some("fixture-project".into()),
        client_id: None,
        client_secret: None,
    })
    .await
    .unwrap();
    let _ = models(true).await.unwrap();
    let fetches = || {
        fixture
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| {
                request["path"]
                    .as_str()
                    .unwrap_or_default()
                    .ends_with(":fetchAvailableModels")
            })
            .count()
    };
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        while fetches() < 3 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the heartbeat must keep the paired poll alive");
    let (minimal, duplicate_scoped) = {
        let requests = fixture.requests.lock().unwrap();
        let minimal = requests
            .iter()
            .filter(|request| {
                request["path"]
                    .as_str()
                    .unwrap_or_default()
                    .ends_with(":loadCodeAssist")
                    && request["body"] == json!({"metadata":{"ideType":"ANTIGRAVITY"}})
            })
            .count();
        let scoped = requests
            .iter()
            .filter(|request| {
                request["path"]
                    .as_str()
                    .unwrap_or_default()
                    .ends_with(":loadCodeAssist")
                    && request["body"]["cloudaicompanionProject"] == "fixture-project"
            })
            .count();
        (minimal, scoped)
    };
    assert!(
        minimal >= 2,
        "each heartbeat tick revalidates the account: {minimal}"
    );
    assert!(
        duplicate_scoped <= 1,
        "heartbeat ticks must not emit a duplicate project-scoped loadCodeAssist: {duplicate_scoped}"
    );
    auth::logout().await.unwrap();
    remove_process_environment_var("KLAUS_ANTIGRAVITY_HEARTBEAT_SECS");
    remove_process_environment_var("KLAUS_ANTIGRAVITY_API_URL");
}

#[tokio::test]
async fn catalog_identity_changes_discard_legacy_snapshots_before_model_selection() {
    let fixture = fixture().await;
    set_process_environment_var("KLAUS_ANTIGRAVITY_API_URL", &fixture.endpoint);
    for old_identity in [None, Some("legacy-client")] {
        let root = tempfile::tempdir().unwrap();
        set_config_root(root.path());
        auth::adopt(auth::Credential {
            access_token: "fixture".into(),
            refresh_token: None,
            expires_at: None,
            account: None,
            auth_method: "consumer".into(),
            project: Some("fixture-project".into()),
            client_id: None,
            client_secret: None,
        })
        .await
        .unwrap();
        let mut snapshot = json!({
            "generation":auth::generation().unwrap(),
            "payload":{"models":{"legacy-only":{"displayName":"Legacy snapshot"}}},
            "fetchedAt":chrono::Utc::now().timestamp_millis(),
        });
        if let Some(identity) = old_identity {
            snapshot["clientIdentity"] = json!(identity);
        }
        let snapshot_path = root.path().join("antigravity-models.json");
        std::fs::write(&snapshot_path, snapshot.to_string()).unwrap();
        let result = models(true).await.unwrap();
        let rows = result["models"].as_array().unwrap();
        assert!(rows.iter().any(|row| row["id"] == "gemini-4-argon-medium"));
        assert!(!rows.iter().any(|row| row["id"] == "legacy-only"));
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let refreshed: Value =
                    serde_json::from_slice(&std::fs::read(&snapshot_path).unwrap()).unwrap();
                if refreshed["payload"]["models"]["fixture-level"].is_object() {
                    return refreshed;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the rejected snapshot must revalidate in the background");
        let refreshed: Value =
            serde_json::from_slice(&std::fs::read(snapshot_path).unwrap()).unwrap();
        assert_eq!(refreshed["clientIdentity"], cloud::client_identity());
        auth::logout().await.unwrap();
    }
    let requests = fixture.requests.lock().unwrap();
    let fetches = requests
        .iter()
        .filter(|request| {
            request["path"]
                .as_str()
                .unwrap_or_default()
                .ends_with(":fetchAvailableModels")
        })
        .count();
    assert_eq!(
        fetches, 2,
        "each identity generation refreshes the catalog exactly once"
    );
    for request in requests.iter() {
        let ua = request["userAgent"].as_str().unwrap();
        assert!(
            ua.starts_with("antigravity/hub/")
                && ua.contains(" (aidev_client; os_type=")
                && ua.contains("; cl=")
                && ua.ends_with(')'),
            "the service catalog is scoped to the hub protocol: {ua}"
        );
    }
    remove_process_environment_var("KLAUS_ANTIGRAVITY_API_URL");
}

#[tokio::test]
async fn a_model_the_live_catalog_dropped_reports_the_account_verdict() {
    use klaus_agent_core::provider::gemini::GeminiClient;
    let fixture = fixture().await;
    let root = tempfile::tempdir().unwrap();
    set_config_root(root.path());
    set_process_environment_var("KLAUS_ANTIGRAVITY_API_URL", &fixture.endpoint);
    auth::adopt(auth::Credential {
        access_token: "fixture".into(),
        refresh_token: None,
        expires_at: None,
        account: None,
        auth_method: "consumer".into(),
        project: Some("fixture-project".into()),
        client_id: None,
        client_secret: None,
    })
    .await
    .unwrap();
    let generation = auth::generation().unwrap();
    let snapshot = json!({
        "generation": generation,
        "payload": {"models": {"fixture-vanished": {"displayName": "Vanished Model"}}},
        "fetchedAt": 1,
        "clientIdentity": cloud::client_identity(),
    });
    std::fs::write(
        root.path().join("antigravity-models.json"),
        snapshot.to_string(),
    )
    .unwrap();
    let client = cloud::CloudClient {
        generation,
        session: None,
    };
    let error = client
        .generate_content_stream(
            decode(
                json!({"model":"fixture-vanished","contents":[],"config":{"maxOutputTokens":100}}),
            ),
            None,
            Vec::new(),
            AbortSignal::new(),
        )
        .await
        .err()
        .unwrap();
    assert!(
        error
            .message
            .starts_with("antigravity_model_not_offered [NOT_FOUND]"),
        "{}",
        error.message
    );
    auth::logout().await.unwrap();
    remove_process_environment_var("KLAUS_ANTIGRAVITY_API_URL");
}

/// A transport failure after the request was uploaded may already have
/// reached the service, so neither catalog reads nor inference may replay it.
#[tokio::test]
async fn a_post_upload_transport_failure_is_never_replayed() {
    for mode in ["catalog", "inference"] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let connections = Arc::new(Mutex::new(0usize));
        let counted = connections.clone();
        let server = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let counted = counted.clone();
                tokio::spawn(async move {
                    let mut bytes = Vec::new();
                    let mut chunk = [0u8; 4096];
                    // Read until the full request arrived, then close without a
                    // response: the client has provably uploaded the request.
                    loop {
                        let count = socket.read(&mut chunk).await.unwrap_or(0);
                        if count == 0 {
                            return;
                        }
                        bytes.extend_from_slice(&chunk[..count]);
                        if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
                            break;
                        }
                    }
                    *counted.lock().unwrap() += 1;
                    drop(socket);
                });
            }
        });
        let root = tempfile::tempdir().unwrap();
        set_config_root(root.path());
        set_process_environment_var("KLAUS_ANTIGRAVITY_API_URL", &endpoint);
        auth::adopt(auth::Credential {
            access_token: "fixture".into(),
            refresh_token: None,
            expires_at: None,
            account: None,
            auth_method: "consumer".into(),
            project: Some("fixture-project".into()),
            client_id: None,
            client_secret: None,
        })
        .await
        .unwrap();
        let generation = auth::generation().unwrap();
        let message = if mode == "catalog" {
            cloud::rpc(
                "fetchAvailableModels",
                json!({"requestId": "fixture"}),
                Some(&generation),
                &AbortSignal::new(),
            )
            .await
            .err()
            .unwrap()
        } else {
            use klaus_agent_core::provider::gemini::GeminiClient;
            let client = cloud::CloudClient {
                generation: generation.clone(),
                session: None,
            };
            client
                .generate_content_stream(
                    decode(json!({"model":"fixture-model","contents":[],"config":{"maxOutputTokens":100}})),
                    None,
                    Vec::new(),
                    AbortSignal::new(),
                )
                .await
                .err()
                .unwrap()
                .message
        };
        assert!(
            message.contains("antigravity_transport_unavailable (sent)"),
            "{mode}: {message}"
        );
        // No replay: the connection count stays at one, and the error keeps the
        // sent-stage diagnostic. (No wall-clock assert here — a loaded parallel
        // suite can stretch one localhost request past any tight bound.)
        assert_eq!(
            *connections.lock().unwrap(),
            1,
            "{mode}: the uploaded request must not be replayed, message {message}"
        );
        server.abort();
        remove_process_environment_var("KLAUS_ANTIGRAVITY_API_URL");
        auth::logout().await.unwrap();
    }
}

/// A connect-stage failure is the one transport stage that proves the request
/// bytes never left the client, so the replay runs once and the final error
/// keeps the connect diagnostic for correct network classification.
#[tokio::test]
async fn a_connect_stage_failure_replays_once_and_reports_the_stage() {
    for mode in ["catalog", "inference"] {
        // Bind a port to learn a free one, then drop it so connections are
        // refused: the canonical connect-stage failure.
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", probe.local_addr().unwrap());
        drop(probe);
        let root = tempfile::tempdir().unwrap();
        set_config_root(root.path());
        set_process_environment_var("KLAUS_ANTIGRAVITY_API_URL", &endpoint);
        auth::adopt(auth::Credential {
            access_token: "fixture".into(),
            refresh_token: None,
            expires_at: None,
            account: None,
            auth_method: "consumer".into(),
            project: Some("fixture-project".into()),
            client_id: None,
            client_secret: None,
        })
        .await
        .unwrap();
        let generation = auth::generation().unwrap();
        let started = std::time::Instant::now();
        let message = if mode == "catalog" {
            cloud::rpc(
                "fetchAvailableModels",
                json!({"requestId": "fixture"}),
                Some(&generation),
                &AbortSignal::new(),
            )
            .await
            .err()
            .unwrap()
        } else {
            use klaus_agent_core::provider::gemini::GeminiClient;
            let client = cloud::CloudClient {
                generation: generation.clone(),
                session: None,
            };
            client
                .generate_content_stream(
                    decode(json!({"model":"fixture-model","contents":[],"config":{"maxOutputTokens":100}})),
                    None,
                    Vec::new(),
                    AbortSignal::new(),
                )
                .await
                .err()
                .unwrap()
                .message
        };
        assert!(
            message.contains("antigravity_transport_unavailable (connect)"),
            "{mode}: {message}"
        );
        // Both attempts refuse instantly, so the replay's 800 ms wait dominates
        // the elapsed time and proves the second attempt actually ran.
        assert!(
            started.elapsed() >= std::time::Duration::from_millis(700),
            "{mode}: the connect-stage replay must have waited before the second attempt"
        );
        remove_process_environment_var("KLAUS_ANTIGRAVITY_API_URL");
        auth::logout().await.unwrap();
    }
}

#[test]
fn recommended_sort_section_curates_the_menu_inventory() {
    let payload = json!({
        "defaultAgentModelId": "m-b-medium",
        "agentModelSorts": [
            {"displayName": "Recommended", "groups": [{"modelIds": ["m-b-medium", "m-b-high"]}]},
            {"displayName": "Archive", "groups": [{"modelIds": ["m-c-medium"]}]}
        ],
        "models": {
            "m-b-medium": {"displayName": "M B (Medium)"},
            "m-b-high": {"displayName": "M B (High)"},
            "m-a-low": {"displayName": "M A (Low)"},
            "m-c-medium": {"displayName": "M C (Medium)"}
        }
    });
    let flagged = |rows: &[Value], key: &str| {
        rows.iter()
            .find(|row| row.get("key").and_then(Value::as_str) == Some(key))
            .is_some_and(|row| row.get("inRecommendedSort").and_then(Value::as_bool) == Some(true))
    };
    let rows = super::catalog::normalize_models(&payload);
    // The recommended section's first group is the curated menu inventory.
    assert!(flagged(&rows, "m-b-medium"));
    assert!(flagged(&rows, "m-b-high"));
    // Ungrouped models and later sections carry no curation marker.
    assert!(!flagged(&rows, "m-a-low"));
    assert!(!flagged(&rows, "m-c-medium"));
    // A payload without sort sections carries no curation at all.
    let bare = super::catalog::normalize_models(&json!({
        "models": {"m-b-medium": {"displayName": "M B (Medium)"}}
    }));
    assert!(!flagged(&bare, "m-b-medium"));
}

#[tokio::test]
async fn legacy_single_account_file_reads_as_queue_and_pool_mutations_preserve_order() {
    let root = tempfile::tempdir().unwrap();
    set_config_root(root.path());
    let raw_legacy = json!({
        "generation": "legacy-gen-1",
        "credential": {
            "accessToken": "tok-alice",
            "refreshToken": "ref-alice",
            "account": "alice@example.invalid",
            "authMethod": "consumer",
            "project": "proj-alice"
        },
        "needsLogin": false
    })
    .to_string();
    std::fs::write(auth::path(), &raw_legacy).unwrap();

    let initial = auth::status();
    assert_eq!(initial["loggedIn"], true);
    assert_eq!(initial["account"], "alice@example.invalid");
    assert_eq!(initial["accounts"].as_array().unwrap().len(), 1);
    assert_eq!(initial["accounts"][0]["id"], "legacy");
    assert_eq!(initial["accounts"][0]["isCurrent"], true);
    assert_eq!(
        std::fs::read_to_string(auth::path()).unwrap(),
        raw_legacy,
        "reading a legacy file must not rewrite it"
    );

    let bob = auth::adopt(auth::Credential {
        access_token: "tok-bob".into(),
        refresh_token: Some("ref-bob".into()),
        expires_at: None,
        account: Some("bob@example.invalid".into()),
        auth_method: "consumer".into(),
        project: Some("proj-bob".into()),
        client_id: None,
        client_secret: None,
    })
    .await
    .unwrap();
    assert_eq!(bob["updatedExisting"], false);
    assert_eq!(bob["previousCount"], 1);

    let carol = auth::adopt(auth::Credential {
        access_token: "tok-carol".into(),
        refresh_token: Some("ref-carol".into()),
        expires_at: None,
        account: Some("carol@example.invalid".into()),
        auth_method: "consumer".into(),
        project: Some("proj-carol".into()),
        client_id: None,
        client_secret: None,
    })
    .await
    .unwrap();
    assert_eq!(carol["accounts"].as_array().unwrap().len(), 3);
    let alice_id = carol["accounts"][0]["id"].as_str().unwrap().to_string();
    let bob_id = carol["accounts"][1]["id"].as_str().unwrap().to_string();
    let carol_id = carol["accounts"][2]["id"].as_str().unwrap().to_string();
    assert_ne!(
        alice_id, "legacy",
        "legacy id upgrades when persisting a queue"
    );

    // Re-adopting Bob refreshes in place without duplicating or reordering.
    let bob_refreshed = auth::adopt(auth::Credential {
        access_token: "tok-bob-v2".into(),
        refresh_token: None,
        expires_at: None,
        account: Some("BOB@example.invalid".into()),
        auth_method: "consumer".into(),
        project: None,
        client_id: None,
        client_secret: None,
    })
    .await
    .unwrap();
    assert_eq!(bob_refreshed["updatedExisting"], true);
    assert_eq!(bob_refreshed["accountId"], bob_id);
    let emails: Vec<&str> = bob_refreshed["accounts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["email"].as_str().unwrap())
        .collect();
    assert_eq!(
        emails,
        vec![
            "alice@example.invalid",
            "BOB@example.invalid",
            "carol@example.invalid"
        ]
    );

    // Promote Carol to head; Alice and Bob keep their relative order behind her.
    let switched = auth::switch_account(&carol_id).await.unwrap();
    assert_eq!(switched["ok"], true);
    assert_eq!(switched["account"], "carol@example.invalid");
    assert_eq!(switched["project"], "proj-carol");
    let ids: Vec<&str> = switched["accounts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(
        ids,
        vec![carol_id.as_str(), alice_id.as_str(), bob_id.as_str()]
    );
    assert_eq!(switched["accounts"][0]["isCurrent"], true);
    assert_eq!(switched["accounts"][1]["isCurrent"], false);

    // Preserved Bob project & refresh token survive when Bob becomes head.
    let bob_head = auth::switch_account(&bob_id).await.unwrap();
    assert_eq!(bob_head["project"], "proj-bob");
    assert_eq!(bob_head["canRefresh"], true);

    assert_eq!(
        auth::switch_account("missing-id").await.unwrap()["error"],
        "antigravity_account_not_found"
    );
    assert_eq!(
        auth::logout_account(Some("missing-id")).await.unwrap()["error"],
        "antigravity_account_not_found"
    );

    let after_remove = auth::logout_account(Some(&bob_id)).await.unwrap();
    assert_eq!(after_remove["accounts"].as_array().unwrap().len(), 2);
    assert_eq!(after_remove["account"], "carol@example.invalid");

    let signed_out = auth::logout_account(None).await.unwrap();
    assert_eq!(signed_out["signedIn"], false);
    assert!(!auth::path().exists());
}

#[tokio::test]
async fn pooled_stream_rotates_across_accounts_with_isolated_projects_and_safe_parallel_streams() {
    use klaus_agent_core::types::provider::{ModelProvider, ModelRequest, ModelStreamEvent};
    use std::collections::VecDeque;

    #[derive(Clone)]
    enum StreamPlan {
        Http429,
        Sse429,
        Http401,
        Http403Disabled,
        Success(&'static str),
        HoldSuccess {
            first: &'static str,
            second: &'static str,
            entered: Arc<tokio::sync::Notify>,
            release: Arc<tokio::sync::Notify>,
        },
    }

    let plans: Arc<Mutex<HashMap<String, VecDeque<StreamPlan>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let oauth_status: Arc<Mutex<HashMap<String, (u16, Value)>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let trace: Arc<Mutex<Vec<(String, String, String)>>> = Arc::new(Mutex::new(Vec::new()));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let plans_srv = plans.clone();
    let oauth_srv = oauth_status.clone();
    let trace_srv = trace.clone();
    let server = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            let plans = plans_srv.clone();
            let oauth = oauth_srv.clone();
            let trace = trace_srv.clone();
            tokio::spawn(async move {
                let mut bytes = Vec::new();
                let (header_end, length) = loop {
                    let mut chunk = [0u8; 4096];
                    let Ok(count) = socket.read(&mut chunk).await else {
                        return;
                    };
                    if count == 0 {
                        return;
                    }
                    bytes.extend_from_slice(&chunk[..count]);
                    if let Some(index) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&bytes[..index]);
                        let len = header
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .and_then(|v| v.trim().parse::<usize>().ok())
                            })
                            .unwrap_or(0);
                        break (index + 4, len);
                    }
                };
                while bytes.len() < header_end + length {
                    let mut chunk = [0u8; 4096];
                    let Ok(count) = socket.read(&mut chunk).await else {
                        return;
                    };
                    if count == 0 {
                        return;
                    }
                    bytes.extend_from_slice(&chunk[..count]);
                }
                let header = String::from_utf8_lossy(&bytes[..header_end]);
                let path = header
                    .lines()
                    .next()
                    .unwrap()
                    .split_whitespace()
                    .nth(1)
                    .unwrap()
                    .to_string();
                if path == "/token" {
                    let form: HashMap<String, String> =
                        url::form_urlencoded::parse(&bytes[header_end..header_end + length])
                            .into_owned()
                            .collect();
                    let rt = form.get("refresh_token").cloned().unwrap_or_default();
                    trace
                        .lock()
                        .unwrap()
                        .push(("oauth".into(), rt.clone(), String::new()));
                    let (code, body) = oauth
                        .lock()
                        .unwrap()
                        .get(&rt)
                        .cloned()
                        .unwrap_or((400, json!({"error": "invalid_grant"})));
                    let text = body.to_string();
                    let resp = format!(
                        "HTTP/1.1 {code} Reply\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{text}",
                        text.len()
                    );
                    let _ = socket.write_all(resp.as_bytes()).await;
                    return;
                }
                let bearer = header
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("authorization: bearer ")
                            .map(|_| {
                                line.split_whitespace()
                                    .nth(2)
                                    .unwrap_or_default()
                                    .to_string()
                            })
                    })
                    .unwrap_or_default();
                let body: Value = serde_json::from_slice(&bytes[header_end..header_end + length])
                    .unwrap_or(Value::Null);
                let project = body["project"].as_str().unwrap_or_default().to_string();
                if path.contains(":streamGenerateContent") {
                    trace
                        .lock()
                        .unwrap()
                        .push(("stream".into(), bearer.clone(), project));
                    let plan = plans
                        .lock()
                        .unwrap()
                        .get_mut(&bearer)
                        .and_then(|q| q.pop_front())
                        .unwrap_or(StreamPlan::Http429);
                    match plan {
                        StreamPlan::Http429 => {
                            let err = json!({"error":{"code":429,"status":"RESOURCE_EXHAUSTED","message":"Rate limit exceeded"}}).to_string();
                            let resp = format!("HTTP/1.1 429 Too Many Requests\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{err}", err.len());
                            let _ = socket.write_all(resp.as_bytes()).await;
                        }
                        StreamPlan::Sse429 => {
                            let frame = format!(
                                "data: {}\n\n",
                                json!({"error":{"code":429,"status":"RESOURCE_EXHAUSTED","message":"Quota exceeded for quota metric"}})
                            );
                            let resp = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{frame}", frame.len());
                            let _ = socket.write_all(resp.as_bytes()).await;
                        }
                        StreamPlan::Http401 => {
                            let err = json!({"error":{"code":401,"status":"UNAUTHENTICATED","message":"Invalid Credentials"}}).to_string();
                            let resp = format!("HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{err}", err.len());
                            let _ = socket.write_all(resp.as_bytes()).await;
                        }
                        StreamPlan::Http403Disabled => {
                            let err = json!({"error":{"code":403,"status":"PERMISSION_DENIED","message":"This account has been disabled."}}).to_string();
                            let resp = format!("HTTP/1.1 403 Forbidden\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{err}", err.len());
                            let _ = socket.write_all(resp.as_bytes()).await;
                        }
                        StreamPlan::Success(text) => {
                            let sse = format!(
                                "data: {}\n\n",
                                json!({"response":{"candidates":[{"finishReason":"STOP","content":{"parts":[{"text":text}]}}]}})
                            );
                            let resp = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{sse}", sse.len());
                            let _ = socket.write_all(resp.as_bytes()).await;
                        }
                        StreamPlan::HoldSuccess {
                            first,
                            second,
                            entered,
                            release,
                        } => {
                            let f1 = format!(
                                "data: {}\n\n",
                                json!({"response":{"candidates":[{"content":{"parts":[{"text":first}]}}]}})
                            );
                            let f2 = format!(
                                "data: {}\n\n",
                                json!({"response":{"candidates":[{"finishReason":"STOP","content":{"parts":[{"text":second}]}}]}})
                            );
                            let _ = socket
                                .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n")
                                .await;
                            let _ = socket.write_all(f1.as_bytes()).await;
                            let _ = socket.flush().await;
                            entered.notify_one();
                            release.notified().await;
                            let _ = socket.write_all(f2.as_bytes()).await;
                        }
                    }
                }
            });
        }
    });

    let root = tempfile::tempdir().unwrap();
    set_config_root(root.path());
    set_process_environment_var("KLAUS_ANTIGRAVITY_API_URL", &endpoint);
    set_process_environment_var("KLAUS_ANTIGRAVITY_OAUTH_URL", format!("{endpoint}/token"));

    for (email, tok, rf, proj) in [
        ("a@example.invalid", "tok-a", "ref-a", "proj-a"),
        ("b@example.invalid", "tok-b", "ref-b", "proj-b"),
        ("c@example.invalid", "tok-c", "ref-c", "proj-c"),
    ] {
        auth::adopt(auth::Credential {
            access_token: tok.into(),
            refresh_token: Some(rf.into()),
            expires_at: Some(chrono::Utc::now().timestamp_millis() + 3_600_000),
            account: Some(email.into()),
            auth_method: "consumer".into(),
            project: Some(proj.into()),
            client_id: None,
            client_secret: None,
        })
        .await
        .unwrap();
    }

    let resolved = resolve("gemini-3.8-flash-medium", &serde_json::Map::new())
        .await
        .unwrap();
    let init_json = json!({
        "provider": resolved.provider,
        "capabilities": resolved.capabilities,
        "capabilitiesByModel": resolved.capabilities_by_model,
        "modelSource": {"source": "antigravity"},
    });
    let proto_init: ProtocolSessionInit =
        klaus_agent_core::runtime_values::runtime_codec::decode_text(&RuntimeString::from(
            init_json.to_string(),
        ))
        .unwrap();
    let base_provider: Arc<dyn ModelProvider> =
        Arc::new(GeminiProvider::new(GeminiProviderOptions {
            default_model: Some("gemini-3.8-flash-medium".into()),
            ..Default::default()
        }));
    let provider = decorate(base_provider, &proto_init);

    let run_request = |provider: Arc<dyn ModelProvider>| async move {
        let req: ModelRequest = decode(json!({
            "model": "gemini-3.8-flash-medium",
            "system": [],
            "messages": [],
            "tools": [],
            "maxOutputTokens": 256
        }));
        let events: Vec<ModelStreamEvent> =
            provider.stream(req, AbortSignal::new()).collect().await;
        events
    };

    // 1. HTTP 429 on A -> first-SSE-frame 429 on B -> HTTP 200 on C; persisted head becomes C.
    {
        let mut p = plans.lock().unwrap();
        p.insert("tok-a".into(), VecDeque::from([StreamPlan::Http429]));
        p.insert("tok-b".into(), VecDeque::from([StreamPlan::Sse429]));
        p.insert(
            "tok-c".into(),
            VecDeque::from([
                StreamPlan::Success("from-c"),
                StreamPlan::Success("next-turn-c"),
            ]),
        );
    }
    let events = run_request(provider.clone()).await;
    let text: String = events
        .iter()
        .filter_map(|ev| match ev {
            ModelStreamEvent::TextDelta { text } => text.try_to_utf8().ok(),
            _ => None,
        })
        .collect();
    assert_eq!(text, "from-c", "{events:?}");
    assert_eq!(
        trace.lock().unwrap().clone(),
        vec![
            ("stream".into(), "tok-a".into(), "proj-a".into()),
            ("stream".into(), "tok-b".into(), "proj-b".into()),
            ("stream".into(), "tok-c".into(), "proj-c".into()),
        ],
        "each rotated account must authenticate with its own token and Cloud Code Assist project"
    );
    assert_eq!(auth::status()["account"], "c@example.invalid");

    // Next turn immediately uses C without re-trying A or B.
    trace.lock().unwrap().clear();
    let events2 = run_request(provider.clone()).await;
    let text2: String = events2
        .iter()
        .filter_map(|ev| match ev {
            ModelStreamEvent::TextDelta { text } => text.try_to_utf8().ok(),
            _ => None,
        })
        .collect();
    assert_eq!(text2, "next-turn-c");
    assert_eq!(
        trace.lock().unwrap().clone(),
        vec![("stream".into(), "tok-c".into(), "proj-c".into())]
    );

    // 2. Premature 401 on C attempts one OAuth refresh; on invalid_grant marks only C needsLogin and rotates to A.
    trace.lock().unwrap().clear();
    {
        let mut p = plans.lock().unwrap();
        p.insert("tok-c".into(), VecDeque::from([StreamPlan::Http401]));
        p.insert(
            "tok-a".into(),
            VecDeque::from([StreamPlan::Success("recovered-on-a")]),
        );
    }
    let events3 = run_request(provider.clone()).await;
    let text3: String = events3
        .iter()
        .filter_map(|ev| match ev {
            ModelStreamEvent::TextDelta { text } => text.try_to_utf8().ok(),
            _ => None,
        })
        .collect();
    assert_eq!(text3, "recovered-on-a");
    assert_eq!(
        trace.lock().unwrap().clone(),
        vec![
            ("stream".into(), "tok-c".into(), "proj-c".into()),
            ("oauth".into(), "ref-c".into(), String::new()),
            ("stream".into(), "tok-a".into(), "proj-a".into()),
        ]
    );
    let st = auth::status();
    assert_eq!(st["account"], "a@example.invalid");
    assert_eq!(st["needsLogin"], false);
    assert_eq!(st["accounts"][2]["email"], "c@example.invalid");
    assert_eq!(st["accounts"][2]["needsLogin"], true);

    // 3. Account-disabled 403 is InvalidRequest, trips the per-generation circuit breaker,
    //    and never rotates the queue onto standby accounts.
    let gen_a = auth::generation().unwrap();
    trace.lock().unwrap().clear();
    plans.lock().unwrap().insert(
        "tok-a".into(),
        VecDeque::from([StreamPlan::Http403Disabled]),
    );
    let disabled_events = run_request(provider.clone()).await;
    assert!(disabled_events.iter().any(|ev| matches!(
        ev,
        ModelStreamEvent::Error { error } if error.code.as_ref().is_some_and(|code| code == "antigravity_account_disabled")
    )));
    assert_eq!(auth::status()["account"], "a@example.invalid");
    assert!(auth::is_generation_disabled(&gen_a));
    auth::clear_generation_disabled(&gen_a);

    // 4. Parallel subagent safety: Stream 1 streaming on A stays alive while Stream 2 rotates A -> B.
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    {
        let mut p = plans.lock().unwrap();
        p.insert(
            "tok-a".into(),
            VecDeque::from([
                StreamPlan::HoldSuccess {
                    first: "part1-",
                    second: "part2",
                    entered: entered.clone(),
                    release: release.clone(),
                },
                StreamPlan::Http429,
            ]),
        );
        p.insert(
            "tok-b".into(),
            VecDeque::from([StreamPlan::Success("subagent-b")]),
        );
    }
    let p1 = provider.clone();
    let stream1 = tokio::spawn(async move { run_request(p1).await });
    entered.notified().await;
    let events_sub2 = run_request(provider.clone()).await;
    let text_sub2: String = events_sub2
        .iter()
        .filter_map(|ev| match ev {
            ModelStreamEvent::TextDelta { text } => text.try_to_utf8().ok(),
            _ => None,
        })
        .collect();
    assert_eq!(text_sub2, "subagent-b");
    assert_eq!(auth::status()["account"], "b@example.invalid");
    release.notify_one();
    let events_sub1 = stream1.await.unwrap();
    let text_sub1: String = events_sub1
        .iter()
        .filter_map(|ev| match ev {
            ModelStreamEvent::TextDelta { text } => text.try_to_utf8().ok(),
            _ => None,
        })
        .collect();
    assert_eq!(
        text_sub1, "part1-part2",
        "rotation by a sibling stream must not kill an active stream on a still-stored account: {events_sub1:?}"
    );

    server.abort();
    auth::logout().await.unwrap();
    remove_process_environment_var("KLAUS_ANTIGRAVITY_API_URL");
    remove_process_environment_var("KLAUS_ANTIGRAVITY_OAUTH_URL");
}

#[tokio::test]
async fn multi_account_usage_isolates_failures_and_native_media_search_rotate_on_quota() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let mut bytes = Vec::new();
                let (header_end, length) = loop {
                    let mut chunk = [0u8; 4096];
                    let Ok(count) = socket.read(&mut chunk).await else {
                        return;
                    };
                    if count == 0 {
                        return;
                    }
                    bytes.extend_from_slice(&chunk[..count]);
                    if let Some(index) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&bytes[..index]);
                        let len = header
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .and_then(|v| v.trim().parse::<usize>().ok())
                            })
                            .unwrap_or(0);
                        break (index + 4, len);
                    }
                };
                while bytes.len() < header_end + length {
                    let mut chunk = [0u8; 4096];
                    let Ok(count) = socket.read(&mut chunk).await else {
                        return;
                    };
                    if count == 0 {
                        return;
                    }
                    bytes.extend_from_slice(&chunk[..count]);
                }
                let header = String::from_utf8_lossy(&bytes[..header_end]);
                let path = header
                    .lines()
                    .next()
                    .unwrap()
                    .split_whitespace()
                    .nth(1)
                    .unwrap()
                    .to_string();
                let bearer = header
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("authorization: bearer ")
                            .map(|_| {
                                line.split_whitespace()
                                    .nth(2)
                                    .unwrap_or_default()
                                    .to_string()
                            })
                    })
                    .unwrap_or_default();
                let body: Value = serde_json::from_slice(&bytes[header_end..header_end + length])
                    .unwrap_or(Value::Null);
                let (status, payload) = if bearer == "tok-broken" {
                    (
                        401,
                        json!({"error":{"code":401,"status":"UNAUTHENTICATED","message":"Revoked"}}),
                    )
                } else if path.ends_with(":fetchAvailableModels") {
                    (
                        200,
                        json!({
                            "models":{"fixture-image":{},"fixture-search":{}},
                            "imageGenerationModelIds":["fixture-image"],
                            "webSearchModelIds":["fixture-search"]
                        }),
                    )
                } else if path.ends_with(":retrieveUserQuota") {
                    (200, json!({"buckets":[]}))
                } else if path.ends_with(":retrieveUserQuotaSummary") {
                    (
                        200,
                        json!({"groups":[{"displayName":"Gemini Models","buckets":[{"bucketId":"gemini-5h","remainingFraction":0.8,"window":"5h","description":"Resets every 5h"}]}]}),
                    )
                } else if path.ends_with(":loadCodeAssist") {
                    (
                        200,
                        json!({"cloudaicompanionProject":"proj-ok","paidTier":{"name":"Google AI Ultra"}}),
                    )
                } else if path.ends_with(":generateContent") && bearer == "tok-limited" {
                    (
                        429,
                        json!({"error":{"code":429,"status":"RESOURCE_EXHAUSTED","message":"Rate limit"}}),
                    )
                } else if path.ends_with(":generateContent") && body["requestType"] == "image_gen" {
                    (
                        200,
                        json!({"response":{"candidates":[{"content":{"parts":[{"inlineData":{"mimeType":"image/png","data":"aW1hZ2U="}}]},"finishReason":"STOP"}]}}),
                    )
                } else {
                    (
                        200,
                        json!({"response":{"candidates":[{"content":{"parts":[{"text":"Search ok"}]},"groundingMetadata":{"groundingChunks":[{"web":{"uri":"https://example.com","title":"Example"}}]}} ]}}),
                    )
                };
                let text = payload.to_string();
                let resp = format!(
                    "HTTP/1.1 {status} Reply\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{text}",
                    text.len()
                );
                let _ = socket.write_all(resp.as_bytes()).await;
            });
        }
    });

    let root = tempfile::tempdir().unwrap();
    set_config_root(root.path());
    set_process_environment_var("KLAUS_ANTIGRAVITY_API_URL", &endpoint);

    auth::adopt(auth::Credential {
        access_token: "tok-limited".into(),
        refresh_token: None,
        expires_at: None,
        account: Some("limited@example.invalid".into()),
        auth_method: "consumer".into(),
        project: Some("proj-limited".into()),
        client_id: None,
        client_secret: None,
    })
    .await
    .unwrap();
    auth::adopt(auth::Credential {
        access_token: "tok-broken".into(),
        refresh_token: None,
        expires_at: None,
        account: Some("broken@example.invalid".into()),
        auth_method: "consumer".into(),
        project: Some("proj-broken".into()),
        client_id: None,
        client_secret: None,
    })
    .await
    .unwrap();
    auth::adopt(auth::Credential {
        access_token: "tok-healthy".into(),
        refresh_token: None,
        expires_at: None,
        account: Some("healthy@example.invalid".into()),
        auth_method: "consumer".into(),
        project: Some("proj-healthy".into()),
        client_id: None,
        client_secret: None,
    })
    .await
    .unwrap();

    // Multi-account usage queries only the active head account (`limited@`), never polling standby accounts.
    let quota = usage(false).await.unwrap();
    assert_eq!(quota["signedIn"], true);
    let rows = quota["accounts"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["label"], "limited@example.invalid");
    assert_eq!(rows[0]["isCurrent"], true);
    assert_eq!(rows[0]["state"], "ok");
    assert_eq!(
        auth::status()["account"],
        "limited@example.invalid",
        "usage queries must never advance the rotation queue"
    );

    // Seed catalog so generate_image and search can pick native models, then verify rotation past limited@ and broken@ to healthy@.
    let head_gen = auth::generation().unwrap();
    std::fs::write(
        root.path().join("antigravity-models.json"),
        json!({
            "generation": head_gen,
            "clientIdentity": cloud_test_identity(),
            "fetchedAt": chrono::Utc::now().timestamp_millis(),
            "payload": {
                "models": {"fixture-image": {}, "fixture-search": {}},
                "imageGenerationModelIds": ["fixture-image"],
                "webSearchModelIds": ["fixture-search"]
            }
        })
        .to_string(),
    )
    .unwrap();

    let img = generate_image(
        serde_json::from_value(json!({"prompt": "draw a circle"})).unwrap(),
        AbortSignal::new(),
    )
    .await
    .unwrap();
    assert_eq!(img["mimeType"], "image/png");
    assert_eq!(auth::status()["account"], "healthy@example.invalid");

    let srch = search(
        serde_json::from_value(json!({"query": "example query"})).unwrap(),
        AbortSignal::new(),
    )
    .await
    .unwrap();
    assert_eq!(srch["results"][0]["url"], "https://example.com");

    server.abort();
    auth::logout().await.unwrap();
    remove_process_environment_var("KLAUS_ANTIGRAVITY_API_URL");
}

#[tokio::test]
async fn title_generation_steps_down_high_thinking_variant_while_turns_keep_selected_variant() {
    use klaus_agent_core::engine::agent::{Agent, AgentOptions, StartSessionOptions};
    use klaus_agent_core::engine::deps::EngineDepsOverrides;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let recorded = Arc::new(Mutex::new(Vec::<Value>::new()));
    let captured = recorded.clone();
    let server = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            let captured = captured.clone();
            tokio::spawn(async move {
                let mut bytes = Vec::new();
                let (header_end, length) = loop {
                    let mut chunk = [0u8; 4096];
                    let Ok(count) = socket.read(&mut chunk).await else {
                        return;
                    };
                    if count == 0 {
                        return;
                    }
                    bytes.extend_from_slice(&chunk[..count]);
                    if let Some(index) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&bytes[..index]);
                        let len = header
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .and_then(|v| v.trim().parse::<usize>().ok())
                            })
                            .unwrap_or(0);
                        break (index + 4, len);
                    }
                };
                while bytes.len() < header_end + length {
                    let mut chunk = [0u8; 4096];
                    let Ok(count) = socket.read(&mut chunk).await else {
                        return;
                    };
                    if count == 0 {
                        return;
                    }
                    bytes.extend_from_slice(&chunk[..count]);
                }
                let header = String::from_utf8_lossy(&bytes[..header_end]);
                let path = header
                    .lines()
                    .next()
                    .unwrap()
                    .split_whitespace()
                    .nth(1)
                    .unwrap()
                    .to_string();
                let body: Value =
                    serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap();
                captured
                    .lock()
                    .unwrap()
                    .push(json!({ "path": path, "body": body }));
                if path.contains("streamGenerateContent") {
                    let payload = format!(
                        "data: {}\n\n",
                        json!({
                            "response": {
                                "candidates": [{
                                    "content": {
                                        "role": "model",
                                        "parts": [{"text": "{\"title\":\"同步新会话工作模式\"}"}]
                                    },
                                    "finishReason": "STOP"
                                }],
                                "usageMetadata": {
                                    "promptTokenCount": 42,
                                    "candidatesTokenCount": 8
                                }
                            }
                        })
                    );
                    let reply = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        payload.len(),
                        payload
                    );
                    let _ = socket.write_all(reply.as_bytes()).await;
                }
            });
        }
    });

    let root = tempfile::tempdir().unwrap();
    set_config_root(root.path());
    set_process_environment_var("KLAUS_ANTIGRAVITY_API_URL", &endpoint);
    auth::adopt(auth::Credential {
        access_token: "fixture-token".into(),
        refresh_token: None,
        expires_at: None,
        account: Some("user@example.invalid".into()),
        auth_method: "consumer".into(),
        project: Some("fixture-project".into()),
        client_id: None,
        client_secret: None,
    })
    .await
    .unwrap();
    let generation = auth::generation().unwrap();
    std::fs::write(
        root.path().join("antigravity-models.json"),
        json!({
            "generation": generation,
            "clientIdentity": cloud_test_identity(),
            "fetchedAt": chrono::Utc::now().timestamp_millis(),
            "payload": {
                "defaultAgentModelId": "gemini-4-carbon-high",
                "models": {
                    "gemini-4-carbon-high": {
                        "displayName": "Gemini 4 Carbon (High)",
                        "supportsThinking": true,
                        "thinkingLevel": 3,
                        "minThinkingBudget": 128,
                        "maxTokens": 1048576,
                        "maxOutputTokens": 65536,
                        "modelProvider": "MODEL_PROVIDER_GOOGLE"
                    },
                    "gemini-4-carbon-low": {
                        "displayName": "Gemini 4 Carbon (Low)",
                        "supportsThinking": true,
                        "thinkingLevel": 1,
                        "minThinkingBudget": 128,
                        "maxTokens": 1048576,
                        "maxOutputTokens": 65536,
                        "modelProvider": "MODEL_PROVIDER_GOOGLE"
                    }
                }
            }
        })
        .to_string(),
    )
    .unwrap();

    let resolved = resolve("gemini-4-carbon-high", &serde_json::Map::new())
        .await
        .unwrap();
    let init: ProtocolSessionInit = runtime_codec::decode_text(&RuntimeString::from(
        json!({
            "provider": resolved.provider,
            "capabilities": resolved.capabilities,
            "capabilitiesByModel": resolved.capabilities_by_model,
            "modelSource": { "source": "antigravity", "modelId": "gemini-4-carbon-high" }
        })
        .to_string(),
    ))
    .unwrap();
    let base: Arc<dyn ModelProvider> = Arc::new(GeminiProvider::new(GeminiProviderOptions {
        default_model: Some("gemini-4-carbon-high".into()),
        capabilities: Some(
            runtime_codec::decode_text(&RuntimeString::from(resolved.capabilities.to_string()))
                .unwrap(),
        ),
        ..Default::default()
    }));
    let provider = decorate(base, &init);

    let turn_req: ModelRequest = runtime_codec::decode_text(&RuntimeString::from(
        json!({
            "model": "gemini-4-carbon-high",
            "system": [],
            "messages": [],
            "tools": [],
            "maxOutputTokens": 256,
            "thinking": { "type": "adaptive" }
        })
        .to_string(),
    ))
    .unwrap();
    let turn_events: Vec<_> = provider
        .stream(turn_req, AbortSignal::new())
        .collect()
        .await;
    assert!(!turn_events
        .iter()
        .any(|ev| matches!(ev, ModelStreamEvent::Error { .. })));

    let process = crate::test_process_context();
    let root_str = root.path().to_string_lossy().into_owned();
    let session = Agent::new(AgentOptions {
        process_context: process,
        provider: provider.clone(),
        config: Some(json!({ "model": "gemini-4-carbon-high" })),
        tools: Some(Vec::new()),
        web_fetch_page: None,
        browser_runtime: None,
        capability_roots: Vec::new(),
        cwd: Some(root_str.clone()),
        deps: EngineDepsOverrides::default(),
        compactor: None,
        approval_handler: None,
        approval_reviewer: None,
        elicit_handler: None,
        credential_store: None,
        hooks: Vec::new(),
        observer: Some(None),
        logger: None,
        pricing: None,
        agents: None,
        include_default_agents: None,
        skills: None,
        output_styles: None,
        workflows: None,
        mcp_catalog: None,
        plugin_catalog: None,
        lsp_servers: None,
        agent_memory: None,
        mcp_clients: None,
        mcp_server_configs: Vec::new(),
        mcp_tools: None,
        refresh_tools: None,
        system_prompt_provider: None,
        plugins: Vec::new(),
        plugin_load_report: None,
    })
    .start_session(StartSessionOptions {
        cwd: Some(root_str.clone().into()),
        user_config_dir: Some(root_str.into()),
        config: Some(json!({
            "model": "gemini-4-carbon-high",
            "thinking": true,
            "effort": "high"
        })),
        ..Default::default()
    });

    let title = session
        .generate_title(
            Some("Keep the title within 36 characters.".into()),
            Some("同步后续新会话的编排模式与并行模式".into()),
            false,
        )
        .await
        .unwrap();
    assert_eq!(title.title, Some(RuntimeString::from("同步新会话工作模式")));

    {
        let streams: Vec<_> = recorded
            .lock()
            .unwrap()
            .iter()
            .filter(|entry| {
                entry["path"]
                    .as_str()
                    .is_some_and(|p| p.contains("streamGenerateContent"))
            })
            .cloned()
            .collect();
        assert_eq!(
            streams.len(),
            2,
            "one ordinary turn + one single-shot title request"
        );
        assert_eq!(streams[0]["body"]["model"], "gemini-4-carbon-high");
        assert_eq!(
            streams[0]["body"]["request"]["generationConfig"]["thinkingConfig"]["thinkingLevel"],
            "HIGH"
        );
        assert_eq!(streams[1]["body"]["model"], "gemini-4-carbon-low");
        assert_eq!(
            streams[1]["body"]["request"]["generationConfig"]["thinkingConfig"]["thinkingLevel"],
            "LOW"
        );
        let sys = streams[1]["body"]["request"]["systemInstruction"].to_string();
        assert!(sys.contains("Keep the title within 36 characters."));
        let user_msg = streams[1]["body"]["request"]["contents"][0]["parts"][0]["text"]
            .as_str()
            .unwrap();
        assert!(user_msg
            .contains("<conversation>\n同步后续新会话的编排模式与并行模式\n</conversation>"));
        assert!(!user_msg.contains("Keep the title within 36 characters."));
    }

    session.shutdown_and_wait().await.unwrap();
    server.abort();
    auth::logout().await.unwrap();
    remove_process_environment_var("KLAUS_ANTIGRAVITY_API_URL");
}

#[tokio::test]
async fn premature_401_deduplicates_refresh_preserves_oauth_verdicts_and_retries_hop_drops() {
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone)]
    enum ReplyPlan {
        InvalidAuth401,
        MissingCred401,
        SseMissingCred401,
        MidStream401ThenResume {
            checkpoint_text: &'static str,
            continuation: &'static str,
        },
        OkText(&'static str),
    }

    #[derive(Clone)]
    enum OAuthPlan {
        Success {
            access: &'static str,
            refresh: &'static str,
        },
        InvalidGrant400,
        Unavailable503,
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let stream_plans: Arc<Mutex<VecDeque<ReplyPlan>>> = Arc::new(Mutex::new(VecDeque::new()));
    let oauth_plans: Arc<Mutex<VecDeque<OAuthPlan>>> = Arc::new(Mutex::new(VecDeque::new()));
    let oauth_calls = Arc::new(AtomicUsize::new(0));
    let stream_calls = Arc::new(AtomicUsize::new(0));

    let sp = stream_plans.clone();
    let op = oauth_plans.clone();
    let oc = oauth_calls.clone();
    let sc = stream_calls.clone();
    let server = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            let sp = sp.clone();
            let op = op.clone();
            let oc = oc.clone();
            let sc = sc.clone();
            tokio::spawn(async move {
                let mut bytes = Vec::new();
                let (header_end, length) = loop {
                    let mut chunk = [0u8; 4096];
                    let Ok(count) = socket.read(&mut chunk).await else {
                        return;
                    };
                    if count == 0 {
                        return;
                    }
                    bytes.extend_from_slice(&chunk[..count]);
                    if let Some(index) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&bytes[..index]);
                        let len = header
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .and_then(|v| v.trim().parse::<usize>().ok())
                            })
                            .unwrap_or(0);
                        break (index + 4, len);
                    }
                };
                while bytes.len() < header_end + length {
                    let mut chunk = [0u8; 4096];
                    let Ok(count) = socket.read(&mut chunk).await else {
                        return;
                    };
                    if count == 0 {
                        return;
                    }
                    bytes.extend_from_slice(&chunk[..count]);
                }
                let header = String::from_utf8_lossy(&bytes[..header_end]);
                let path = header
                    .lines()
                    .next()
                    .unwrap()
                    .split_whitespace()
                    .nth(1)
                    .unwrap()
                    .to_string();
                let bearer = header
                    .lines()
                    .find_map(|line| {
                        let (k, v) = line.split_once(':')?;
                        k.eq_ignore_ascii_case("authorization")
                            .then(|| v.trim().strip_prefix("Bearer ").unwrap_or("").to_string())
                    })
                    .unwrap_or_default();

                if path == "/token" {
                    oc.fetch_add(1, Ordering::SeqCst);
                    let plan = op
                        .lock()
                        .unwrap()
                        .pop_front()
                        .unwrap_or(OAuthPlan::Success {
                            access: "tok-renewed",
                            refresh: "ref-renewed",
                        });
                    let (status, body) = match plan {
                        OAuthPlan::Success { access, refresh } => (
                            200,
                            json!({"access_token": access, "refresh_token": refresh, "expires_in": 3600})
                                .to_string(),
                        ),
                        OAuthPlan::InvalidGrant400 => (
                            400,
                            json!({"error": "invalid_grant"}).to_string(),
                        ),
                        OAuthPlan::Unavailable503 => (
                            503,
                            json!({"error": "temporarily_unavailable"}).to_string(),
                        ),
                    };
                    let resp = format!(
                        "HTTP/1.1 {status} Reply\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(resp.as_bytes()).await;
                    return;
                }

                if !path.contains(":streamGenerateContent") {
                    let body = json!({
                        "cloudaicompanionProject": "proj-1",
                        "models": {
                            "gemini-3.8-flash-medium": {
                                "displayName": "Gemini 3.8 Flash Medium",
                                "maxTokens": 100000,
                                "maxOutputTokens": 8192
                            }
                        }
                    })
                    .to_string();
                    let resp = format!(
                        "HTTP/1.1 {status} Reply\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len(),
                        status = 200
                    );
                    let _ = socket.write_all(resp.as_bytes()).await;
                    return;
                }

                sc.fetch_add(1, Ordering::SeqCst);
                if bearer.starts_with("stale-") {
                    let err = json!({
                        "error": {
                            "code": 401,
                            "status": "UNAUTHENTICATED",
                            "message": "Request had invalid authentication credentials."
                        }
                    })
                    .to_string();
                    let resp = format!(
                        "HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{err}",
                        err.len()
                    );
                    let _ = socket.write_all(resp.as_bytes()).await;
                    return;
                }

                let plan = sp
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or(ReplyPlan::OkText("ok"));
                match plan {
                    ReplyPlan::InvalidAuth401 => {
                        let err = json!({
                            "error": {
                                "code": 401,
                                "status": "UNAUTHENTICATED",
                                "message": "Request had invalid authentication credentials."
                            }
                        })
                        .to_string();
                        let resp = format!(
                            "HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{err}",
                            err.len()
                        );
                        let _ = socket.write_all(resp.as_bytes()).await;
                    }
                    ReplyPlan::MissingCred401 => {
                        let err = json!({
                            "error": {
                                "code": 401,
                                "status": "UNAUTHENTICATED",
                                "message": "Request is missing required authentication credential. Expected OAuth 2 access token, login cookie or other valid authentication credential."
                            }
                        })
                        .to_string();
                        let resp = format!(
                            "HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{err}",
                            err.len()
                        );
                        let _ = socket.write_all(resp.as_bytes()).await;
                    }
                    ReplyPlan::SseMissingCred401 => {
                        let sse = format!(
                            "data: {}\n\n",
                            json!({
                                "error": {
                                    "code": 401,
                                    "status": "UNAUTHENTICATED",
                                    "message": "Request is missing required authentication credential."
                                }
                            })
                        );
                        let resp = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{sse}",
                            sse.len()
                        );
                        let _ = socket.write_all(resp.as_bytes()).await;
                    }
                    ReplyPlan::MidStream401ThenResume {
                        checkpoint_text,
                        continuation,
                    } => {
                        let f1 = json!({
                            "response": {
                                "candidates": [{
                                    "continuationToken": continuation,
                                    "content": { "parts": [{ "text": checkpoint_text }] }
                                }]
                            }
                        });
                        let f2 = json!({
                            "error": {
                                "code": 401,
                                "status": "UNAUTHENTICATED",
                                "message": "Request had invalid authentication credentials."
                            }
                        });
                        let sse = format!("data: {f1}\n\ndata: {f2}\n\n");
                        let resp = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{sse}",
                            sse.len()
                        );
                        let _ = socket.write_all(resp.as_bytes()).await;
                    }
                    ReplyPlan::OkText(text) => {
                        let sse = format!(
                            "data: {}\n\n",
                            json!({"response":{"candidates":[{"finishReason":"STOP","content":{"parts":[{"text":text}]}}]}})
                        );
                        let resp = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{sse}",
                            sse.len()
                        );
                        let _ = socket.write_all(resp.as_bytes()).await;
                    }
                }
            });
        }
    });

    let root = tempfile::tempdir().unwrap();
    set_config_root(root.path());
    set_process_environment_var("KLAUS_ANTIGRAVITY_API_URL", &endpoint);
    set_process_environment_var("KLAUS_ANTIGRAVITY_OAUTH_URL", format!("{endpoint}/token"));

    auth::adopt(auth::Credential {
        access_token: "stale-initial".into(),
        refresh_token: Some("ref-1".into()),
        expires_at: Some(chrono::Utc::now().timestamp_millis() + 3_600_000),
        account: Some("user@example.invalid".into()),
        auth_method: "consumer".into(),
        project: Some("proj-1".into()),
        client_id: None,
        client_secret: None,
    })
    .await
    .unwrap();

    let resolved = resolve("gemini-3.8-flash-medium", &serde_json::Map::new())
        .await
        .unwrap();
    let init_json = json!({
        "provider": resolved.provider,
        "capabilities": resolved.capabilities,
        "capabilitiesByModel": resolved.capabilities_by_model,
        "modelSource": {"source": "antigravity"},
    });
    let proto_init: ProtocolSessionInit =
        runtime_codec::decode_text(&RuntimeString::from(init_json.to_string())).unwrap();
    let base_provider: Arc<dyn ModelProvider> =
        Arc::new(GeminiProvider::new(GeminiProviderOptions {
            default_model: Some("gemini-3.8-flash-medium".into()),
            ..Default::default()
        }));
    let provider = decorate(base_provider, &proto_init);

    let run = |provider: Arc<dyn ModelProvider>| async move {
        let req: ModelRequest = decode(json!({
            "model": "gemini-3.8-flash-medium",
            "system": [],
            "messages": [],
            "tools": [],
            "maxOutputTokens": 256
        }));
        provider
            .stream(req, AbortSignal::new())
            .collect::<Vec<_>>()
            .await
    };

    // 1. Concurrent premature 401s share one OAuth refresh; even a staggered
    // caller that rejected the same stale token reuses the renewed credential.
    oauth_plans.lock().unwrap().push_back(OAuthPlan::Success {
        access: "fresh-1",
        refresh: "ref-2",
    });
    for _ in 0..4 {
        stream_plans
            .lock()
            .unwrap()
            .push_back(ReplyPlan::OkText("healed"));
    }
    let batch = futures::future::join_all((0..4).map(|_| run(provider.clone()))).await;
    for events in &batch {
        let text: String = events
            .iter()
            .filter_map(|ev| match ev {
                ModelStreamEvent::TextDelta { text } => text.try_to_utf8().ok(),
                _ => None,
            })
            .collect();
        assert_eq!(text, "healed", "{events:?}");
    }
    let reused = auth::force_refresh(&auth::generation().unwrap(), Some("stale-initial"))
        .await
        .unwrap();
    assert_eq!(reused.access_token, "fresh-1");
    assert_eq!(oauth_calls.load(Ordering::SeqCst), 1);

    // 2. Upstream hop missing-credential 401 (HTTP and SSE) retries as service overload
    // without touching /token or asking the user to sign in again.
    oauth_calls.store(0, Ordering::SeqCst);
    {
        let mut sp_guard = stream_plans.lock().unwrap();
        sp_guard.push_back(ReplyPlan::MissingCred401);
        sp_guard.push_back(ReplyPlan::SseMissingCred401);
        sp_guard.push_back(ReplyPlan::OkText("after-hop-drop"));
    }
    let hop_events = run(provider.clone()).await;
    let hop_text: String = hop_events
        .iter()
        .filter_map(|ev| match ev {
            ModelStreamEvent::TextDelta { text } => text.try_to_utf8().ok(),
            _ => None,
        })
        .collect();
    assert_eq!(hop_text, "after-hop-drop", "{hop_events:?}");
    assert_eq!(oauth_calls.load(Ordering::SeqCst), 0);

    // 3. Mid-stream in-band 401 after checkpointed output renews the token and resumes cleanly.
    oauth_plans.lock().unwrap().push_back(OAuthPlan::Success {
        access: "fresh-2",
        refresh: "ref-3",
    });
    {
        let mut sp_guard = stream_plans.lock().unwrap();
        sp_guard.push_back(ReplyPlan::MidStream401ThenResume {
            checkpoint_text: "hello-",
            continuation: "cont-1",
        });
        sp_guard.push_back(ReplyPlan::OkText("world"));
    }
    let mid_events = run(provider.clone()).await;
    let mid_text: String = mid_events
        .iter()
        .filter_map(|ev| match ev {
            ModelStreamEvent::TextDelta { text } => text.try_to_utf8().ok(),
            _ => None,
        })
        .collect();
    assert_eq!(mid_text, "hello-world", "{mid_events:?}");
    assert_eq!(oauth_calls.load(Ordering::SeqCst), 1);

    // 4. Transient OAuth 503 during reactive renewal surfaces `antigravity_refresh_unavailable`
    // (preserving the saved login) and recovers when the next bounded attempt succeeds.
    oauth_calls.store(0, Ordering::SeqCst);
    {
        let mut op_guard = oauth_plans.lock().unwrap();
        op_guard.push_back(OAuthPlan::Unavailable503);
        op_guard.push_back(OAuthPlan::Success {
            access: "fresh-3",
            refresh: "ref-4",
        });
    }
    {
        let mut sp_guard = stream_plans.lock().unwrap();
        sp_guard.push_back(ReplyPlan::InvalidAuth401);
        sp_guard.push_back(ReplyPlan::InvalidAuth401);
        sp_guard.push_back(ReplyPlan::OkText("recovered-after-oauth-503"));
    }
    let transient_oauth_events = run(provider.clone()).await;
    let transient_oauth_text: String = transient_oauth_events
        .iter()
        .filter_map(|ev| match ev {
            ModelStreamEvent::TextDelta { text } => text.try_to_utf8().ok(),
            _ => None,
        })
        .collect();
    assert_eq!(
        transient_oauth_text, "recovered-after-oauth-503",
        "{transient_oauth_events:?}"
    );
    assert_eq!(auth::status()["needsLogin"], false);

    // 5. Terminal OAuth `invalid_grant` on reactive 401 surfaces `antigravity_login_expired`.
    oauth_plans
        .lock()
        .unwrap()
        .push_back(OAuthPlan::InvalidGrant400);
    stream_plans
        .lock()
        .unwrap()
        .push_back(ReplyPlan::InvalidAuth401);
    let expired_events = run(provider.clone()).await;
    assert!(
        expired_events.iter().any(|ev| matches!(
            ev,
            ModelStreamEvent::Error { error }
                if error.code.as_ref().is_some_and(|code| code == "antigravity_login_expired")
                    && error.kind == klaus_agent_core::types::errors::ProviderErrorKind::Auth
        )),
        "{expired_events:?}"
    );
    assert_eq!(auth::status()["needsLogin"], true);

    server.abort();
    auth::logout().await.unwrap();
    remove_process_environment_var("KLAUS_ANTIGRAVITY_API_URL");
    remove_process_environment_var("KLAUS_ANTIGRAVITY_OAUTH_URL");
}

#[tokio::test]
async fn antigravity_wire_projection_rewrites_prompt_and_tools_only_for_antigravity_and_restores_calls(
) {
    use klaus_agent_core::provider::gemini::{GeminiProvider, GeminiProviderOptions};
    use klaus_agent_core::types::provider::{ModelProvider, ModelRequest, ModelStreamEvent};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let captured_bodies: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_wire_bytes: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let captured_clone = captured_bodies.clone();
    let captured_wire_clone = captured_wire_bytes.clone();
    let server = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            let captured = captured_clone.clone();
            let captured_wire = captured_wire_clone.clone();
            tokio::spawn(async move {
                let mut raw = Vec::new();
                let mut buf = [0u8; 4096];
                let header_end = loop {
                    let n = socket.read(&mut buf).await.unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    raw.extend_from_slice(&buf[..n]);
                    if let Some(pos) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                        break pos + 4;
                    }
                };
                let head = String::from_utf8_lossy(&raw[..header_end]).to_string();
                let content_len = head
                    .lines()
                    .find_map(|l| {
                        let (k, v) = l.split_once(':')?;
                        k.trim()
                            .eq_ignore_ascii_case("content-length")
                            .then(|| v.trim().parse::<usize>().ok())?
                    })
                    .unwrap_or(0);
                while raw.len() < header_end + content_len {
                    let n = socket.read(&mut buf).await.unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    raw.extend_from_slice(&buf[..n]);
                }
                let path = head
                    .lines()
                    .next()
                    .and_then(|l| l.split_whitespace().nth(1))
                    .unwrap_or("");
                if let Ok(parsed) =
                    serde_json::from_slice::<Value>(&raw[header_end..header_end + content_len])
                {
                    if path.contains("streamGenerateContent") || path.contains("generateContent") {
                        captured.lock().unwrap().push(parsed);
                    }
                }
                if path.contains("streamGenerateContent") {
                    captured_wire
                        .lock()
                        .unwrap()
                        .push(raw[header_end..header_end + content_len].to_vec());
                    let sse = concat!(
                        "data: {\"response\":{\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"Chart:\\n::inline-\"}]}}],\"usageMetadata\":{\"promptTokenCount\":10,\"cachedContentTokenCount\":0}}}\n\n",
                        "data: {\"response\":{\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"vis-frame{path=\\\"/tmp/x.html\\\"}\"},{\"functionCall\":{\"name\":\"app_state_query\",\"args\":{\"key\":\"docs\"}}}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":10,\"candidatesTokenCount\":5,\"totalTokenCount\":15}}}\n\n"
                    );
                    let reply = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        sse.len(),
                        sse
                    );
                    let _ = socket.write_all(reply.as_bytes()).await;
                } else {
                    let body = json!({
                        "candidates": [{
                            "content": {
                                "role": "model",
                                "parts": [{ "text": "plain-gemini-ok" }]
                            },
                            "finishReason": "STOP"
                        }]
                    })
                    .to_string();
                    let reply = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = socket.write_all(reply.as_bytes()).await;
                }
            });
        }
    });

    let root = tempfile::tempdir().unwrap();
    set_config_root(root.path());
    set_process_environment_var("KLAUS_ANTIGRAVITY_API_URL", &endpoint);
    auth::adopt(auth::Credential {
        access_token: "tok-sanitize".into(),
        refresh_token: None,
        expires_at: None,
        account: Some("sanitize@example.invalid".into()),
        auth_method: "consumer".into(),
        project: Some("proj-sanitize".into()),
        client_id: None,
        client_secret: None,
    })
    .await
    .unwrap();

    let proto_init = ProtocolSessionInit {
        thread_id: Some(RuntimeString::from("thread-fixed-42")),
        ..Default::default()
    };
    let base_provider: Arc<dyn ModelProvider> =
        Arc::new(GeminiProvider::new(GeminiProviderOptions {
            api_key: Some("dummy-key".into()),
            base_url: Some(endpoint.clone()),
            default_model: Some("gemini-3.8-flash-medium".into()),
            capabilities: Some(decode(json!({ "supportsTools": true }))),
            ..Default::default()
        }));
    let agy_provider = decorate(base_provider.clone(), &proto_init);

    let make_req = || -> ModelRequest {
        let mut req: ModelRequest = decode(json!({
            "model": "gemini-3.8-flash-medium",
            "system": [{
                "name": "base",
                "text": "You are Klaus. You and the user share one workspace, and your job is to collaborate with them until their intended goal is completely handled.\nUse `klaus_query` in /Users/dev/workspace/klaus."
            }],
            "messages": [],
            "tools": [{
                "name": "klaus_query",
                "description": "Read Klaus documentation and live state.",
                "inputSchema": { "type": "object", "properties": { "key": { "type": "string" } } }
            }],
            "maxOutputTokens": 256
        }));
        req.messages
            .push(klaus_agent_core::schemas::message::Message::User(
                klaus_agent_core::schemas::message::UserMessage {
                    uuid: "u-1".into(),
                    is_meta: None,
                    question_answer: None,
                    content: vec![klaus_agent_core::schemas::content::ContentBlock::Text(
                        klaus_agent_core::schemas::content::TextBlock {
                            text: "<!-- klaus-steered-message -->\nInspect status".into(),
                            citations: None,
                            harness_envelope: None,
                            message_id: None,
                            phase: None,
                        },
                    )],
                },
            ));
        req
    };

    let diagnostic_session = format!("wire-fixture-{}", uuid::Uuid::new_v4());
    let events: Vec<_> =
        klaus_agent_core::observability::provider_diagnostics::scoped_trace_stream(
            diagnostic_session.clone().into(),
            agy_provider.stream(make_req(), AbortSignal::new()),
        )
        .collect()
        .await;
    let combined_text: String = events
        .iter()
        .filter_map(|ev| match ev {
            ModelStreamEvent::TextDelta { text } => text.try_to_utf8().ok(),
            _ => None,
        })
        .collect();
    assert_eq!(
        combined_text, "Chart:\n::klaus-inline-vis{path=\"/tmp/x.html\"}",
        "split inline-vis marker must be restored across chunks: {events:?}"
    );
    assert!(
        events.iter().any(|ev| matches!(
            ev,
            ModelStreamEvent::ToolUseStart { name, .. } if name == "klaus_query"
        )),
        "wire tool call `app_state_query` must restore to `klaus_query`: {events:?}"
    );

    let sent = captured_bodies.lock().unwrap().clone();
    assert_eq!(sent.len(), 1);
    let wire_body = &sent[0];
    assert!(wire_body.get("userPromptId").is_none());
    let req_id = wire_body["requestId"].as_str().unwrap();
    assert!(req_id.starts_with("agent/") && req_id.ends_with("/thread-fixed-42/0"));
    assert_eq!(
        wire_body["request"]["sessionId"].as_str().unwrap(),
        cloud::compute_session_id("thread-fixed-42")
    );
    let wire_sys = wire_body["request"]["systemInstruction"]["parts"][0]["text"]
        .as_str()
        .unwrap();
    assert!(!wire_sys.contains("You are Klaus"));
    assert!(wire_sys.contains("/Users/dev/workspace/klaus"));
    assert_eq!(
        wire_body["request"]["tools"][0]["functionDeclarations"][0]["name"],
        "app_state_query"
    );
    let diagnostics_path = klaus_agent_core::observability::provider_diagnostics::log_path_named(
        "antigravity-cache.log",
    );
    let records: Vec<Value> = std::fs::read_to_string(&diagnostics_path)
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|record| record["upstreamRequestId"].as_str() == Some(req_id))
        .collect();
    let request_record = records
        .iter()
        .find(|record| record["kind"] == "wire_request")
        .unwrap();
    use sha2::{Digest, Sha256};
    let mut wire_hash = Sha256::new();
    wire_hash.update(b"wire-body\0");
    wire_hash.update(&captured_wire_bytes.lock().unwrap()[0]);
    assert_eq!(
        request_record["wireBodyHash"],
        format!("{:x}", wire_hash.finalize())
    );
    assert_eq!(
        request_record["wireMessageCount"],
        wire_body["request"]["contents"].as_array().unwrap().len()
    );
    let mut account_hash = Sha256::new();
    account_hash.update(b"account-generation\0");
    account_hash.update(auth::generation().unwrap().as_bytes());
    assert_eq!(
        request_record["accountGenerationHash"],
        format!("{:x}", account_hash.finalize())
    );
    let usage_records: Vec<_> = records
        .iter()
        .filter(|record| record["kind"] == "wire_usage")
        .collect();
    assert_eq!(usage_records.len(), 2);
    let first = &usage_records[0]["rawUsage"]["fields"]["cachedContentTokenCount"];
    let last = &usage_records[1]["rawUsage"]["fields"]["cachedContentTokenCount"];
    assert_eq!(first["present"], true);
    assert_eq!(first["value"], 0);
    assert_eq!(last["present"], false);
    assert!(last["value"].is_null());
    assert!(records
        .iter()
        .any(|record| record["kind"] == "wire_response_end"
            && record["usageMetadataFrames"] == 2
            && record["cachedTokenCountPresentFrames"] == 1));
    let serialized_records = serde_json::to_string(&records).unwrap();
    for private in [
        "tok-sanitize",
        "proj-sanitize",
        "You are Klaus",
        "Inspect status",
    ] {
        assert!(!serialized_records.contains(private));
    }

    // Non-Antigravity GeminiProvider preserves original system prompt and tool names verbatim.
    captured_bodies.lock().unwrap().clear();
    let _plain_events: Vec<_> =
        klaus_agent_core::observability::provider_diagnostics::scoped_trace_stream(
            diagnostic_session.clone().into(),
            base_provider.stream(make_req(), AbortSignal::new()),
        )
        .collect()
        .await;
    let plain_sent = captured_bodies.lock().unwrap().clone();
    assert_eq!(plain_sent.len(), 1);
    let plain_sys = plain_sent[0]["systemInstruction"]["parts"][0]["text"]
        .as_str()
        .unwrap();
    assert!(plain_sys.starts_with("You are Klaus."));
    assert_eq!(
        plain_sent[0]["tools"][0]["functionDeclarations"][0]["name"],
        "klaus_query"
    );
    let scoped_wire_requests = std::fs::read_to_string(&diagnostics_path)
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|record| {
            record["sessionId"].as_str() == Some(diagnostic_session.as_str())
                && record["kind"] == "wire_request"
        })
        .count();
    assert_eq!(
        scoped_wire_requests, 1,
        "plain Gemini must not emit source-specific diagnostics"
    );

    // Sub-sessions (`:sub:` / `:readonly:`) sharing the parent's pooled
    // `CloudClient` must stamp distinct wire `sessionId`s so they never overwrite
    // the parent chat's upstream KV cache slot, while Stage 1 `skip_cache_write`
    // compaction forks keep the parent's `sessionId` to reuse the hot prefix.
    captured_bodies.lock().unwrap().clear();
    let mut sub_req = make_req();
    sub_req.cache_key = Some("thread-fixed-42:sub:agent-alpha".into());
    let _sub_events: Vec<_> = agy_provider
        .stream(sub_req, AbortSignal::new())
        .collect()
        .await;
    let mut readonly_req = make_req();
    readonly_req.cache_key = Some("thread-fixed-42:readonly:side-1".into());
    let _readonly_events: Vec<_> = agy_provider
        .stream(readonly_req, AbortSignal::new())
        .collect()
        .await;
    let mut compact_fork_req = make_req();
    compact_fork_req.cache_key = Some("thread-fixed-42".into());
    compact_fork_req.skip_cache_write = Some(true);
    let _compact_events: Vec<_> = agy_provider
        .stream(compact_fork_req, AbortSignal::new())
        .collect()
        .await;
    let isolated_sent = captured_bodies.lock().unwrap().clone();
    assert_eq!(isolated_sent.len(), 3);
    assert_eq!(
        isolated_sent[0]["request"]["sessionId"].as_str().unwrap(),
        cloud::compute_session_id("thread-fixed-42:sub:agent-alpha")
    );
    assert_ne!(
        isolated_sent[0]["request"]["sessionId"],
        wire_body["request"]["sessionId"]
    );
    assert_eq!(
        isolated_sent[1]["request"]["sessionId"].as_str().unwrap(),
        cloud::compute_session_id("thread-fixed-42:readonly:side-1")
    );
    assert_ne!(
        isolated_sent[1]["request"]["sessionId"],
        wire_body["request"]["sessionId"]
    );
    assert_eq!(
        isolated_sent[2]["request"]["sessionId"],
        wire_body["request"]["sessionId"]
    );

    server.abort();
    auth::logout().await.unwrap();
    remove_process_environment_var("KLAUS_ANTIGRAVITY_API_URL");
}

#[tokio::test]
async fn heartbeat_runs_only_for_active_head_account_and_concurrent_streams_run_in_parallel() {
    use klaus_agent_core::provider::gemini::{GeminiProvider, GeminiProviderOptions};
    use klaus_agent_core::types::provider::{ModelProvider, ModelRequest};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let active_in_flight = Arc::new(AtomicUsize::new(0));
    let peak_in_flight = Arc::new(AtomicUsize::new(0));
    let heartbeat_tokens: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let (release_tx, release_rx) = tokio::sync::watch::channel(false);
    let all_entered = Arc::new(tokio::sync::Notify::new());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let active_clone = active_in_flight.clone();
    let peak_clone = peak_in_flight.clone();
    let hb_clone = heartbeat_tokens.clone();
    let all_entered_clone = all_entered.clone();

    let server = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            let active = active_clone.clone();
            let peak = peak_clone.clone();
            let hb = hb_clone.clone();
            let mut release = release_rx.clone();
            let all_in = all_entered_clone.clone();
            tokio::spawn(async move {
                let mut raw = Vec::new();
                let mut buf = [0u8; 4096];
                let header_end = loop {
                    let n = socket.read(&mut buf).await.unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    raw.extend_from_slice(&buf[..n]);
                    if let Some(pos) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                        break pos + 4;
                    }
                };
                let head = String::from_utf8_lossy(&raw[..header_end]).to_string();
                let path = head
                    .lines()
                    .next()
                    .and_then(|l| l.split_whitespace().nth(1))
                    .unwrap_or("")
                    .to_string();
                let bearer = head
                    .lines()
                    .find_map(|l| {
                        let (k, v) = l.split_once(':')?;
                        k.trim()
                            .eq_ignore_ascii_case("authorization")
                            .then(|| v.trim().to_string())
                    })
                    .unwrap_or_default();

                if path.contains("streamGenerateContent") {
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    if now == 4 {
                        all_in.notify_one();
                    }
                    let _ = release.wait_for(|&open| open).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    let sse = "data: {\"response\":{\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"ok\"}]},\"finishReason\":\"STOP\"}]}}\n\n";
                    let reply = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        sse.len(),
                        sse
                    );
                    let _ = socket.write_all(reply.as_bytes()).await;
                } else {
                    if path.contains("loadCodeAssist") || path.contains("fetchAvailableModels") {
                        hb.lock().unwrap().push(format!("{bearer}:{path}"));
                    }
                    let body = if path.contains("fetchAvailableModels") {
                        json!({"models":{"gemini-3.8-flash-medium":{"displayName":"Flash"}}})
                            .to_string()
                    } else {
                        json!({"cloudaicompanionProject":"proj-pool"}).to_string()
                    };
                    let reply = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = socket.write_all(reply.as_bytes()).await;
                }
            });
        }
    });

    let root = tempfile::tempdir().unwrap();
    set_config_root(root.path());
    set_process_environment_var("KLAUS_ANTIGRAVITY_API_URL", &endpoint);
    set_process_environment_var("KLAUS_ANTIGRAVITY_HEARTBEAT_SECS", "1");

    auth::adopt(auth::Credential {
        access_token: "head-tok".into(),
        refresh_token: None,
        expires_at: None,
        account: Some("head@example.invalid".into()),
        auth_method: "consumer".into(),
        project: Some("proj-head".into()),
        client_id: None,
        client_secret: None,
    })
    .await
    .unwrap();
    let head_gen = auth::generation().unwrap();

    auth::adopt(auth::Credential {
        access_token: "standby-tok".into(),
        refresh_token: None,
        expires_at: None,
        account: Some("standby@example.invalid".into()),
        auth_method: "consumer".into(),
        project: Some("proj-standby".into()),
        client_id: None,
        client_secret: None,
    })
    .await
    .unwrap();
    let standby_gen = auth::generations()
        .into_iter()
        .find(|g| g != &head_gen)
        .unwrap();

    spawn_heartbeat(&standby_gen);
    refresh_catalog(&head_gen).await.unwrap();
    heartbeat_tokens.lock().unwrap().clear();
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    let recorded_hb = heartbeat_tokens.lock().unwrap().clone();
    assert!(
        recorded_hb
            .iter()
            .any(|entry| entry.starts_with("Bearer head-tok:")),
        "expected active head heartbeat RPCs, got {recorded_hb:?}"
    );
    assert!(
        !recorded_hb
            .iter()
            .any(|entry| entry.starts_with("Bearer standby-tok:")),
        "standby account must never emit background heartbeat RPCs: {recorded_hb:?}"
    );

    let proto_init = ProtocolSessionInit::default();
    let base_provider: Arc<dyn ModelProvider> =
        Arc::new(GeminiProvider::new(GeminiProviderOptions {
            default_model: Some("gemini-3.8-flash-medium".into()),
            ..Default::default()
        }));
    let provider = decorate(base_provider, &proto_init);
    let launch_one = |p: Arc<dyn ModelProvider>| {
        tokio::spawn(async move {
            let req: ModelRequest = decode(json!({
                "model": "gemini-3.8-flash-medium",
                "system": [],
                "messages": [],
                "tools": [],
                "maxOutputTokens": 64
            }));
            p.stream(req, AbortSignal::new()).collect::<Vec<_>>().await
        })
    };
    let tasks: Vec<_> = (0..4).map(|_| launch_one(provider.clone())).collect();
    tokio::time::timeout(std::time::Duration::from_secs(5), all_entered.notified())
        .await
        .unwrap();
    assert_eq!(active_in_flight.load(Ordering::SeqCst), 4);
    assert_eq!(peak_in_flight.load(Ordering::SeqCst), 4);
    let _ = release_tx.send(true);
    for task in tasks {
        let evs = tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
        assert!(!evs.is_empty());
    }

    server.abort();
    auth::logout().await.unwrap();
    remove_process_environment_var("KLAUS_ANTIGRAVITY_API_URL");
    remove_process_environment_var("KLAUS_ANTIGRAVITY_HEARTBEAT_SECS");
}

#[test]
fn source_compaction_ceiling_preserves_lower_declared_limits() {
    use klaus_agent_core::types::provider::ModelCapabilities;

    for (declared, expected) in [
        (Some(900_000), 256_000),
        (Some(100_000), 100_000),
        (None, 256_000),
    ] {
        let mut caps = ModelCapabilities {
            context_window: Some(1_048_576),
            max_output_tokens: Some(262_144),
            auto_compact_token_limit: declared,
            ..Default::default()
        };
        apply_source_compaction_cap(None, &mut caps, None);
        assert_eq!(caps.auto_compact_token_limit, Some(expected));
        assert_eq!(caps.context_window, Some(1_048_576));
    }
}

#[tokio::test]
async fn sticky_conversations_shard_across_accounts_and_fail_over_on_503_overload() {
    use klaus_agent_core::provider::gemini::{GeminiProvider, GeminiProviderOptions};
    use klaus_agent_core::types::provider::{ModelProvider, ModelRequest, ModelStreamEvent};
    use std::collections::VecDeque;

    #[derive(Clone)]
    enum ReplyPlan {
        Http503Overloaded,
        ModelAccessDenied,
        Ok(&'static str),
    }

    let plans: Arc<Mutex<HashMap<String, VecDeque<ReplyPlan>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let seen: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let plans_srv = plans.clone();
    let seen_srv = seen.clone();
    let server = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            let plans = plans_srv.clone();
            let seen = seen_srv.clone();
            tokio::spawn(async move {
                let mut raw = Vec::new();
                let mut buf = [0u8; 4096];
                let (header_end, content_len) = loop {
                    let Ok(n) = socket.read(&mut buf).await else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    raw.extend_from_slice(&buf[..n]);
                    if let Some(pos) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                        let hdr = String::from_utf8_lossy(&raw[..pos]);
                        let len = hdr
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .and_then(|v| v.trim().parse::<usize>().ok())
                            })
                            .unwrap_or(0);
                        break (pos + 4, len);
                    }
                };
                while raw.len() < header_end + content_len {
                    let Ok(n) = socket.read(&mut buf).await else {
                        return;
                    };
                    if n == 0 {
                        break;
                    }
                    raw.extend_from_slice(&buf[..n]);
                }
                let header = String::from_utf8_lossy(&raw[..header_end]);
                let path = header
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap_or("")
                    .to_string();
                let bearer = header
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("authorization: bearer ")
                            .map(|idx| line[line.len() - idx.len()..].trim().to_string())
                    })
                    .unwrap_or_default();
                if !path.contains("streamGenerateContent") {
                    let body = if path.contains("fetchAvailableModels") {
                        r#"{"models":{"gemini-3.8-flash-medium":{"displayName":"Flash","maxTokens":1048576,"maxOutputTokens":65536}}}"#
                    } else {
                        "{}"
                    };
                    let reply = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(reply.as_bytes()).await;
                    return;
                }
                let body_json: Value =
                    serde_json::from_slice(&raw[header_end..header_end + content_len])
                        .unwrap_or(Value::Null);
                let proj = body_json["project"].as_str().unwrap_or("").to_string();
                seen.lock().unwrap().push((bearer.clone(), proj));
                let step = plans
                    .lock()
                    .unwrap()
                    .get_mut(&bearer)
                    .and_then(VecDeque::pop_front)
                    .unwrap_or(ReplyPlan::Ok("default-ok"));
                match step {
                    ReplyPlan::ModelAccessDenied => {
                        let err = json!({"error": {
                            "code": 403, "status": "PERMISSION_DENIED",
                            "message": "Your account does not have access to this model.",
                            "details": [{"reason": "MODEL_ACCESS_DENIED"}]
                        }})
                        .to_string();
                        let reply = format!(
                            "HTTP/1.1 403 Forbidden\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{err}",
                            err.len()
                        );
                        let _ = socket.write_all(reply.as_bytes()).await;
                    }
                    ReplyPlan::Http503Overloaded => {
                        let err = json!({
                            "error": {
                                "code": 503,
                                "status": "UNAVAILABLE",
                                "message": "overloaded prefill queue",
                                "details": [{"reason": "PREFILL_QUEUE_OVERLOADED"}]
                            }
                        })
                        .to_string();
                        let reply = format!(
                            "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{err}",
                            err.len()
                        );
                        let _ = socket.write_all(reply.as_bytes()).await;
                    }
                    ReplyPlan::Ok(text) => {
                        let sse = format!(
                            "data: {{\"response\":{{\"candidates\":[{{\"content\":{{\"role\":\"model\",\"parts\":[{{\"text\":\"{text}\"}}]}},\"finishReason\":\"STOP\"}}]}}}}\n\n"
                        );
                        let reply = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{sse}",
                            sse.len()
                        );
                        let _ = socket.write_all(reply.as_bytes()).await;
                    }
                }
            });
        }
    });

    let root = tempfile::tempdir().unwrap();
    set_config_root(root.path());
    set_process_environment_var("KLAUS_ANTIGRAVITY_API_URL", &endpoint);
    set_process_environment_var("KLAUS_ANTIGRAVITY_ROTATION_COOLDOWN_MS", "0");

    let first = auth::adopt(auth::Credential {
        access_token: "acct-1-tok".into(),
        refresh_token: None,
        expires_at: None,
        account: Some("acct1@example.invalid".into()),
        auth_method: "consumer".into(),
        project: Some("proj-1".into()),
        client_id: None,
        client_secret: None,
    })
    .await
    .unwrap();
    let acct1_id = first["accountId"].as_str().unwrap().to_string();

    auth::adopt(auth::Credential {
        access_token: "acct-2-tok".into(),
        refresh_token: None,
        expires_at: None,
        account: Some("acct2@example.invalid".into()),
        auth_method: "consumer".into(),
        project: Some("proj-2".into()),
        client_id: None,
        client_secret: None,
    })
    .await
    .unwrap();

    let make_session_provider = |thread_id: &str| {
        let init: ProtocolSessionInit = decode(json!({
            "threadId": thread_id,
            "modelSource": {"source": "antigravity"},
        }));
        let base: Arc<dyn ModelProvider> = Arc::new(GeminiProvider::new(GeminiProviderOptions {
            default_model: Some("gemini-3.8-flash-medium".into()),
            ..Default::default()
        }));
        decorate(base, &init)
    };
    let ask = |provider: Arc<dyn ModelProvider>| async move {
        let req: ModelRequest = decode(json!({
            "model": "gemini-3.8-flash-medium",
            "system": [],
            "messages": [],
            "tools": [],
            "maxOutputTokens": 64
        }));
        let events: Vec<ModelStreamEvent> =
            provider.stream(req, AbortSignal::new()).collect().await;
        events
            .into_iter()
            .filter_map(|ev| match ev {
                ModelStreamEvent::TextDelta { text } => text.try_to_utf8().ok(),
                _ => None,
            })
            .collect::<String>()
    };

    let chat_a = make_session_provider("thread-alpha");
    let chat_b = make_session_provider("thread-beta");
    let chat_c = make_session_provider("thread-gamma");
    let chat_d = make_session_provider("thread-delta");
    assert_eq!(
        chat_a
            .model_metadata(&RuntimeString::from("gemini-3.8-flash-medium"))
            .auto_compact_token_limit,
        Some(256_000)
    );
    assert!(auth::status().get("accountSharding").is_none());

    // All new threads stick to the active head account without sharding across standby accounts.
    {
        let mut p = plans.lock().unwrap();
        p.insert(
            "acct-1-tok".into(),
            VecDeque::from([
                ReplyPlan::Ok("a1"),
                ReplyPlan::Ok("b1"),
                ReplyPlan::Ok("c1"),
                ReplyPlan::Ok("d1"),
            ]),
        );
    }
    assert_eq!(ask(chat_a.clone()).await, "a1");
    assert_eq!(ask(chat_b.clone()).await, "b1");
    assert_eq!(ask(chat_c.clone()).await, "c1");
    assert_eq!(ask(chat_d.clone()).await, "d1");
    assert_eq!(
        seen.lock().unwrap().clone(),
        vec![
            ("acct-1-tok".into(), "proj-1".into()),
            ("acct-1-tok".into(), "proj-1".into()),
            ("acct-1-tok".into(), "proj-1".into()),
            ("acct-1-tok".into(), "proj-1".into()),
        ],
        "new threads always bind to the active head account without sharding"
    );

    // Pre-response HTTP 503 overload on acct-1 fails over immediately to acct-2 and rebinds chat_a.
    seen.lock().unwrap().clear();
    {
        let mut p = plans.lock().unwrap();
        p.insert(
            "acct-1-tok".into(),
            VecDeque::from([ReplyPlan::Http503Overloaded]),
        );
        p.insert(
            "acct-2-tok".into(),
            VecDeque::from([ReplyPlan::Ok("failover-2"), ReplyPlan::Ok("sticky-2")]),
        );
    }
    assert_eq!(ask(chat_a.clone()).await, "failover-2");
    assert_eq!(ask(chat_a.clone()).await, "sticky-2");
    assert_eq!(
        seen.lock().unwrap().clone(),
        vec![
            ("acct-1-tok".into(), "proj-1".into()),
            ("acct-2-tok".into(), "proj-2".into()),
            ("acct-2-tok".into(), "proj-2".into()),
        ]
    );

    // Explicit manual account switch resets sticky leases so existing chats honor the user's pick.
    seen.lock().unwrap().clear();
    let switched = auth::switch_account(&acct1_id).await.unwrap();
    assert!(switched.get("accountSharding").is_none());
    plans.lock().unwrap().insert(
        "acct-1-tok".into(),
        VecDeque::from([ReplyPlan::Ok("manual-1")]),
    );
    assert_eq!(ask(chat_a.clone()).await, "manual-1");
    assert_eq!(
        seen.lock().unwrap().clone(),
        vec![("acct-1-tok".into(), "proj-1".into())]
    );

    // Different account entitlements still fail over cleanly on model access denial.
    seen.lock().unwrap().clear();
    {
        let mut p = plans.lock().unwrap();
        p.insert(
            "acct-1-tok".into(),
            VecDeque::from([ReplyPlan::ModelAccessDenied]),
        );
        p.insert(
            "acct-2-tok".into(),
            VecDeque::from([ReplyPlan::Ok("entitled-2")]),
        );
    }
    assert_eq!(ask(chat_a.clone()).await, "entitled-2");
    assert_eq!(
        seen.lock().unwrap().clone(),
        vec![
            ("acct-1-tok".into(), "proj-1".into()),
            ("acct-2-tok".into(), "proj-2".into()),
        ]
    );
    assert_eq!(auth::usable_generations().len(), 2);

    server.abort();
    auth::logout().await.unwrap();
    remove_process_environment_var("KLAUS_ANTIGRAVITY_API_URL");
    remove_process_environment_var("KLAUS_ANTIGRAVITY_ROTATION_COOLDOWN_MS");
}
