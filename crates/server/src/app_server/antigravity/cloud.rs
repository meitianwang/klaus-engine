//! Direct cloud requests and bounded streamed response decoding.

use super::auth;
use futures::StreamExt;
use klaus_agent_core::provider::classify::RawProviderError;
use klaus_agent_core::provider::gemini::{
    generate_content_request, Candidate, Content, GeminiApiError, GeminiChunkStream, GeminiClient,
    GenerateContentParameters, GenerateContentResponse, UsageMetadata,
};
use klaus_agent_core::provider::rotation::RotateOutcome;
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeJson, RuntimeString};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::AuthHeaders;
use serde_json::{json, Value};

const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;
const MAX_SSE_FRAME_BYTES: usize = 8 * 1024 * 1024;
const MAX_CONTINUATION_ATTEMPTS: usize = 32;
const MAX_MID_STREAM_RETRIES: usize = 3;
const MID_STREAM_RETRY_BASE_BACKOFF_MS: u64 = 100;
const MID_STREAM_RETRY_MAX_BACKOFF_MS: u64 = 5_000;
const MID_STREAM_RETRY_MAX_DELAY_MS: u64 = 60_000;

pub(super) fn validate_endpoint(endpoint: &str) -> Result<(), String> {
    let url = url::Url::parse(endpoint).map_err(|_| "antigravity_endpoint_invalid")?;
    let local = matches!(
        url.host_str(),
        Some("127.0.0.1" | "localhost" | "[::1]" | "::1")
    );
    let service = matches!(
        url.host_str(),
        Some("daily-cloudcode-pa.googleapis.com" | "oauth2.googleapis.com")
    );
    if (url.scheme() != "https" && !(url.scheme() == "http" && local))
        || !(local || service)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("antigravity_endpoint_invalid".into());
    }
    Ok(())
}

pub(super) fn client_identity() -> String {
    let os = match std::env::consts::OS {
        "macos" => "darwin",
        other => other,
    };
    let arch = match std::env::consts::ARCH {
        "aarch64" => "arm64",
        "x86_64" => "amd64",
        other => other,
    };
    format!("antigravity/hub/2.24.0 (aidev_client; os_type={os}; arch={arch})")
}

pub(super) fn http_client() -> Result<reqwest::Client, String> {
    // Model discovery and inference share the hub protocol identity, and one
    // pooled client keeps connections warm across calls. No total request
    // timeout is set here: control-plane RPCs cap each request themselves, and
    // inference streams stay open so the engine-level idle/total stream budgets
    // govern them instead of an adapter-side deadline cutting them short.
    static CLIENT: std::sync::OnceLock<Result<reqwest::Client, String>> =
        std::sync::OnceLock::new();
    CLIENT
        .get_or_init(|| {
            reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(15))
                .pool_idle_timeout(std::time::Duration::from_secs(30))
                .redirect(reqwest::redirect::Policy::none())
                .user_agent(client_identity())
                .build()
                .map_err(|_| "antigravity_transport_unavailable".into())
        })
        .clone()
}

pub(super) fn api_base() -> Result<String, String> {
    let base = std::env::var("KLAUS_ANTIGRAVITY_API_URL")
        .unwrap_or_else(|_| "https://daily-cloudcode-pa.googleapis.com".into());
    validate_endpoint(&base)?;
    Ok(base.trim_end_matches('/').to_string())
}

pub(super) async fn read_body(
    response: reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, String> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err("antigravity_response_too_large".into());
    }
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| "antigravity_response_interrupted")?;
        if bytes.len().saturating_add(chunk.len()) > limit {
            return Err("antigravity_response_too_large".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

const ERROR_TAGS: [&str; 13] = [
    "antigravity_model_access_denied",
    "antigravity_model_not_offered",
    "antigravity_model_unlisted",
    "antigravity_quota_exhausted",
    "antigravity_request_rejected",
    "antigravity_region_unsupported",
    "antigravity_account_disabled",
    "antigravity_account_restricted",
    "antigravity_permission_denied",
    "antigravity_auth_required",
    "antigravity_rate_limited",
    "antigravity_service_unavailable",
    "model_context_window_exceeded",
];

fn safe_machine_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn service_error(
    http_status: Option<u16>,
    bytes: &[u8],
    mut headers: std::collections::HashMap<String, String>,
) -> GeminiApiError {
    let parsed: Value = serde_json::from_slice(bytes).unwrap_or(Value::Null);
    let error = parsed.get("error").unwrap_or(&parsed);
    let status = http_status.or_else(|| {
        error
            .get("code")
            .and_then(Value::as_u64)
            .filter(|code| (100..600).contains(code))
            .map(|code| code as u16)
    });
    let service_code = error
        .get("status")
        .and_then(Value::as_str)
        .filter(|code| safe_machine_code(code));
    let reason: String = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("")
        .chars()
        .filter(|character| !character.is_control())
        .take(2048)
        .collect();
    let lower = reason.to_ascii_lowercase();
    let details: Vec<&Value> = error
        .get("details")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .take(16)
        .collect();
    let reasons: Vec<&str> = details
        .iter()
        .filter_map(|detail| detail.get("reason").and_then(Value::as_str))
        .collect();
    let context_overflow = status == Some(400)
        && lower.contains("input token count")
        && lower.contains("exceeds")
        && lower.contains("maximum");
    let explicit_rate = reasons.iter().any(|reason| {
        matches!(
            *reason,
            "RATE_LIMIT_EXCEEDED" | "REQUEST_RATE_LIMIT_EXCEEDED"
        )
    });
    let rate_wording = ["per minute", "per second", "requests per", "rate limit"]
        .iter()
        .any(|word| lower.contains(word));
    let explicit_quota = !explicit_rate
        && (reasons.iter().any(|reason| {
            matches!(
                *reason,
                "QUOTA_EXCEEDED"
                    | "QUOTA_EXHAUSTED"
                    | "CREDIT_BALANCE_EXHAUSTED"
                    | "INSUFFICIENT_CREDITS"
            )
        }) || (!rate_wording
            && ["quota", "allowance", "credit balance", "credits"]
                .iter()
                .any(|word| {
                    lower.contains(word)
                        && (lower.contains("exhaust")
                            || lower.contains("exceed")
                            || lower.contains("insufficient"))
                })));
    let auth_reason = reasons.iter().any(|reason| {
        matches!(
            *reason,
            "ACCESS_TOKEN_EXPIRED"
                | "ACCESS_TOKEN_INVALID"
                | "CREDENTIALS_MISSING"
                | "UNAUTHENTICATED"
                | "INSUFFICIENT_AUTHENTICATION_SCOPES"
        )
    });
    let model_access = reasons.iter().any(|reason| {
        matches!(
            *reason,
            "MODEL_ACCESS_DENIED"
                | "MODEL_NOT_ALLOWED"
                | "MODEL_NOT_ENTITLED"
                | "SUBSCRIPTION_REQUIRED"
        )
    }) || (!auth_reason
        && ([
            "access to this model",
            "access to the model",
            "model access",
            "use this model",
            "use the model",
            "model is not allowed",
            "model is not available",
            "model requires a subscription",
        ]
        .iter()
        .any(|word| lower.contains(word))
            || (lower.contains("model")
                && lower.contains("your account")
                && [
                    "not available",
                    "not enabled",
                    "not allowed",
                    "not entitled",
                ]
                .iter()
                .any(|word| lower.contains(word)))));
    // A region eligibility verdict rejects the request's egress location, not
    // its content: the localized sentence must point at the network path
    // instead of inviting the user to rewrite the message. The verdict flips
    // with the egress route (proxy or VPN up/down), so it arrives through the
    // same refusal shape as content rejections while needing its own wording.
    let region_unsupported = lower.contains("location is not supported")
        && matches!(
            service_code,
            Some("FAILED_PRECONDITION" | "PERMISSION_DENIED")
        );
    // An account-level ban is not a stale token. Re-login with the same Google
    // account cannot clear it, so it must not be reported as an auth refresh.
    let account_disabled = reasons.iter().any(|reason| {
        matches!(
            *reason,
            "ACCOUNT_DISABLED" | "ACCOUNT_SUSPENDED" | "USER_DISABLED" | "CONSUMER_SUSPENDED"
        )
    }) || [
        "account has been disabled",
        "account is disabled",
        "account has been suspended",
        "account is suspended",
        "banned from antigravity",
        "this service has been disabled in this account",
        "this service has been disabled for this account",
    ]
    .iter()
    .any(|phrase| lower.contains(phrase));
    // Eligibility restrictions do not establish that an account was banned.
    let account_restricted = reasons.iter().any(|reason| {
        matches!(
            *reason,
            "ACCOUNT_RESTRICTED" | "ACCOUNT_INELIGIBLE" | "USER_NOT_ELIGIBLE"
        )
    }) || [
        "not eligible to use antigravity",
        "antigravity is not available for this account",
    ]
    .iter()
    .any(|phrase| lower.contains(phrase));
    // Upstream capacity verdicts arrive shaped like ordinary refusals but clear
    // on their own, so the retry schedule can wait them out instead of failing
    // the turn fast. HTTP 408 is the service's own request-timeout verdict.
    let transient_overload = status == Some(408)
        || matches!(
            service_code,
            Some(
                "PREFILL_QUEUE_OVERLOADED"
                    | "DECODE_PREEMPTED"
                    | "TPU_EXECUTE_ERROR"
                    | "MODEL_CAPACITY_EXHAUSTED"
                    | "THROTTLED_TASK_LIMIT"
            )
        )
        || reasons.iter().any(|reason| {
            matches!(
                *reason,
                "PREFILL_QUEUE_OVERLOADED"
                    | "DECODE_PREEMPTED"
                    | "TPU_EXECUTE_ERROR"
                    | "MODEL_CAPACITY_EXHAUSTED"
                    | "THROTTLED_TASK_LIMIT"
            )
        })
        || (!explicit_quota
            && [
                "overloaded prefill queue",
                "thread is cancelled before we get prefill results",
                "preempted out of decode queue",
                "tpu device returned error",
            ]
            .iter()
            .any(|phrase| lower.contains(phrase)));
    let tag = if context_overflow {
        "model_context_window_exceeded"
    } else if account_disabled {
        "antigravity_account_disabled"
    } else if region_unsupported {
        "antigravity_region_unsupported"
    } else if status == Some(401) || service_code == Some("UNAUTHENTICATED") || auth_reason {
        "antigravity_auth_required"
    } else if explicit_quota {
        "antigravity_quota_exhausted"
    } else if (status == Some(403) || service_code == Some("PERMISSION_DENIED")) && model_access {
        "antigravity_model_access_denied"
    } else if account_restricted {
        "antigravity_account_restricted"
    } else if status == Some(403) || service_code == Some("PERMISSION_DENIED") {
        "antigravity_permission_denied"
    } else if status.is_some_and(|status| status >= 500)
        || transient_overload
        || matches!(
            service_code,
            Some("UNAVAILABLE" | "INTERNAL" | "DEADLINE_EXCEEDED")
        )
    {
        "antigravity_service_unavailable"
    } else if explicit_rate || status == Some(429) || service_code == Some("RESOURCE_EXHAUSTED") {
        "antigravity_rate_limited"
    } else {
        "antigravity_request_rejected"
    };
    for detail in details {
        if let Some(seconds) = detail
            .get("retryDelay")
            .and_then(Value::as_str)
            .and_then(|delay| delay.strip_suffix('s'))
            .and_then(|delay| delay.parse::<f64>().ok())
            .filter(|delay| delay.is_finite() && *delay >= 0.0 && *delay <= 86400.0)
        {
            headers
                .entry("retry-after".into())
                .or_insert_with(|| seconds.to_string());
        }
    }
    if let Some(request_id) = error
        .get("requestId")
        .or_else(|| parsed.get("requestId"))
        .and_then(Value::as_str)
        .filter(|id| {
            safe_machine_code(id)
                || (id.len() <= 128
                    && id
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_'))
        })
    {
        headers
            .entry("x-request-id".into())
            .or_insert_with(|| request_id.to_string());
    }
    // Do not forward arbitrary body fields, prompts, or agent configuration.
    let reason = if context_overflow {
        "context window exceeded"
    } else {
        &reason
    };
    GeminiApiError {
        status,
        message: match (service_code, reason.is_empty()) {
            (Some(code), false) => format!("{tag} [{code}]: {reason}"),
            (Some(code), true) => format!("{tag} [{code}]"),
            (None, false) => format!("{tag}: {reason}"),
            (None, true) => tag.into(),
        },
        headers: Some(headers),
    }
}

fn error_details(error: &GeminiApiError) -> RawProviderError {
    let code = ERROR_TAGS.into_iter().find(|tag| {
        error.message == *tag
            || error
                .message
                .strip_prefix(tag)
                .is_some_and(|suffix| suffix.starts_with(":") || suffix.starts_with(" ["))
    });
    let error_type = code
        .and_then(|code| error.message.strip_prefix(code))
        .and_then(|suffix| suffix.strip_prefix(" ["))
        .and_then(|suffix| suffix.split_once(']'))
        .map(|(code, _)| code)
        .filter(|code| safe_machine_code(code));
    // Preserve the localized key while exposing a transport signal understood
    // by the engine's bounded stream recovery. Local refusals remain terminal.
    let message = match (error.status, error.message.as_str()) {
        (None, "antigravity_stream_incomplete") => format!(
            "{}: network stream ended before its terminal event",
            error.message
        ),
        (None, "antigravity_stream_interrupted") => format!(
            "{}: network stream interrupted while reading response",
            error.message
        ),
        // A transport failure below produced no response bytes, so no output or
        // tool side effect was consumed: it re-enters the engine retry schedule
        // with network semantics, and the stage label stays as a URL-free
        // diagnostic.
        (None, "antigravity_transport_unavailable (connect)") => format!(
            "{}: network connection failed before the request was delivered",
            error.message
        ),
        (None, "antigravity_transport_unavailable (sent)") => format!(
            "{}: network transport failed while awaiting the first response byte",
            error.message
        ),
        _ => error.message.clone(),
    };
    RawProviderError {
        status: error.status,
        message,
        code: code.map(str::to_string),
        error_type: error_type.map(str::to_string),
        headers: error.headers.clone().unwrap_or_default(),
        ..Default::default()
    }
}

async fn read_refusal_body(
    response: reqwest::Response,
    generation: Option<&str>,
    signal: &AbortSignal,
) -> Result<Vec<u8>, GeminiApiError> {
    let read = read_body(response, 64 * 1024);
    tokio::pin!(read);
    let bytes = loop {
        tokio::select! {
            _ = signal.cancelled() => return Err(api_error("antigravity_request_cancelled")),
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                if generation.is_some_and(|generation| !auth::is_current(generation)) {
                    return Err(api_error("antigravity_credential_changed"));
                }
            },
            result = &mut read => break result.unwrap_or_default(),
        }
    };
    if generation.is_some_and(|generation| !auth::is_current(generation)) {
        return Err(api_error("antigravity_credential_changed"));
    }
    Ok(bytes)
}

/// Where a transport failure happened. Only `Connect` proves the request
/// bytes never left the client, so it is the only stage a bounded replay may
/// trust: a failure after upload may already have reached the service, and
/// replaying it could duplicate an inference generation or a quota charge.
#[derive(Clone, Copy, PartialEq, Eq)]
enum TransportStage {
    Connect,
    Sent,
}

impl TransportStage {
    /// Safe diagnostic suffix: names the stage without echoing the URL.
    fn label(self) -> &'static str {
        match self {
            TransportStage::Connect => "connect",
            TransportStage::Sent => "sent",
        }
    }
}

fn transport_error(stage: TransportStage) -> GeminiApiError {
    api_error(format!(
        "antigravity_transport_unavailable ({})",
        stage.label()
    ))
}

/// Replays the request once, and only for a connect-stage transport failure —
/// the one stage that proves the request bytes never reached the service.
/// Post-upload failures surface immediately so an inference request is never
/// duplicated, and the stage stays in the error message as a URL-free
/// diagnostic so callers can classify the network verdict correctly.
async fn send(
    method: &str,
    body: Vec<u8>,
    generation: Option<&str>,
    signal: &AbortSignal,
    extra_headers: Vec<(String, String)>,
) -> Result<reqwest::Response, GeminiApiError> {
    let first = send_once(
        method,
        body.clone(),
        generation,
        signal,
        extra_headers.clone(),
    )
    .await;
    let Err(error) = first else { return first };
    let replayable =
        error.status.is_none() && error.message == transport_error(TransportStage::Connect).message;
    if !replayable {
        return Err(error);
    }
    tokio::select! {
        _ = signal.cancelled() => Err(api_error("antigravity_request_cancelled")),
        _ = tokio::time::sleep(std::time::Duration::from_millis(800)) => {
            send_once(method, body, generation, signal, extra_headers).await
        }
    }
}

async fn send_once(
    method: &str,
    body: Vec<u8>,
    generation: Option<&str>,
    signal: &AbortSignal,
    extra_headers: Vec<(String, String)>,
) -> Result<reqwest::Response, GeminiApiError> {
    let credential = auth::credential(generation).await.map_err(api_error)?;
    let base = api_base().map_err(api_error)?;
    let suffix = if method == "streamGenerateContent" {
        "?alt=sse"
    } else {
        ""
    };
    let url = format!("{base}/v1internal:{method}{suffix}");
    let http = http_client().map_err(api_error)?;
    let mut request = http.post(url).header("content-type", "application/json");
    if method != "streamGenerateContent" {
        // Control-plane RPCs stay bounded. The inference stream carries no
        // adapter-side total deadline: engine-level idle/total stream budgets
        // own it, and a long prefill or generation must not be cut short here.
        request = request.timeout(std::time::Duration::from_secs(180));
    }
    let mut request = request.body(body);
    for (name, value) in extra_headers {
        if name.eq_ignore_ascii_case("authorization") || name.eq_ignore_ascii_case("host") {
            continue;
        }
        request = request.header(name, value);
    }
    request = request.bearer_auth(&credential.access_token);
    let response = tokio::select! {
        _ = signal.cancelled() => return Err(api_error("antigravity_request_cancelled")),
        response = request.send() => match response {
            Ok(response) => response,
            // A connect error is the only reqwest stage that proves the
            // request was never uploaded; every other transport failure is
            // treated as possibly delivered and must not be replayed.
            Err(error) => {
                let stage = if error.is_connect() {
                    TransportStage::Connect
                } else {
                    TransportStage::Sent
                };
                return Err(transport_error(stage));
            }
        },
    };
    if generation.is_some_and(|generation| !auth::is_current(generation)) {
        return Err(api_error("antigravity_credential_changed"));
    }
    if !response.status().is_success() {
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.to_string(), value.to_string()))
            })
            .collect();
        let bytes = read_refusal_body(response, generation, signal).await?;
        return Err(service_error(Some(status), &bytes, headers));
    }
    Ok(response)
}

pub(super) async fn rpc(
    method: &str,
    body: Value,
    generation: Option<&str>,
    signal: &AbortSignal,
) -> Result<Value, String> {
    let bytes = serde_json::to_vec(&body).map_err(|_| "antigravity_request_invalid")?;
    let response = send(method, bytes, generation, signal, Vec::new())
        .await
        .map_err(|e| e.message)?;
    let bytes = tokio::select! {
        _ = signal.cancelled() => return Err("antigravity_request_cancelled".into()),
        result = read_body(response,MAX_BODY_BYTES) => result?,
    };
    let result: Value =
        serde_json::from_slice(&bytes).map_err(|_| "antigravity_response_invalid")?;
    if let Some(generation) = generation {
        super::record_credits(generation, &result);
    }
    Ok(result)
}

pub(super) fn envelope(project: &str, model: &str, request: Value, request_type: &str) -> Value {
    json!({
        "project":project,
        "model":model,
        "requestId":uuid::Uuid::new_v4().to_string(),
        "userPromptId":uuid::Uuid::new_v4().to_string(),
        "userAgent":"antigravity",
        "requestType":request_type,
        "request":request,
    })
}

fn api_error(message: impl Into<String>) -> GeminiApiError {
    GeminiApiError {
        status: None,
        message: message.into(),
        headers: None,
    }
}

fn is_not_found(error: &GeminiApiError) -> bool {
    error.status == Some(404) && error.message.contains("[NOT_FOUND]")
}

fn retag(error: GeminiApiError, tag: &str) -> GeminiApiError {
    let mut error = error;
    for known in ERROR_TAGS {
        if let Some(suffix) = error.message.strip_prefix(known) {
            error.message = format!("{tag}{suffix}");
            return error;
        }
    }
    error
}

/// A NOT_FOUND refusal that survives a fresh project resolution is read against
/// the live account catalog only: a model the service listed before and no
/// longer lists is an entitlement verdict, while an identity the live catalog
/// never carried is a stale selection the picker must re-resolve, not an
/// account limitation. The hardcoded known-model inventory must not count as
/// listing, or every stale identity would read as entitled.
async fn not_offered_or_original(
    generation: &str,
    model: &str,
    error: GeminiApiError,
) -> GeminiApiError {
    let was_listed = super::cached_payload(generation).is_some_and(|cached| {
        cached
            .payload
            .get("models")
            .and_then(|models| models.get(model))
            .is_some()
    });
    let listed_now = match super::refresh_catalog(generation).await {
        Ok(payload) => payload
            .get("models")
            .and_then(|models| models.get(model))
            .is_some(),
        Err(_) => return error,
    };
    if listed_now {
        return error;
    }
    retag(
        error,
        if was_listed {
            "antigravity_model_not_offered"
        } else {
            "antigravity_model_unlisted"
        },
    )
}

#[derive(Clone)]
pub(super) struct CloudClient {
    pub generation: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum FinishReasonKind {
    #[default]
    Unspecified,
    Continuation,
    MaxTokens,
    OtherTerminal,
}

struct DecodedCloudFrame {
    response: GenerateContentResponse,
    continuation_token: Option<RuntimeString>,
    finish_reason_kind: FinishReasonKind,
    prompt_feedback_blocked: bool,
}

fn classify_finish_reason_value(
    value: Option<&RuntimeJson>,
) -> (FinishReasonKind, Option<Option<&'static str>>) {
    let Some(value) = value else {
        return (FinishReasonKind::Unspecified, None);
    };
    if let Some(reason) = value.as_string() {
        if reason.is_empty() || reason == "FINISH_REASON_UNSPECIFIED" {
            (FinishReasonKind::Unspecified, None)
        } else if reason == "FINISH_REASON_CONTINUATION" || reason == "CONTINUATION" {
            (
                FinishReasonKind::Continuation,
                Some(Some("FINISH_REASON_CONTINUATION")),
            )
        } else if reason == "MAX_TOKENS" || reason == "FINISH_REASON_MAX_DECODED_TOKENS" {
            (FinishReasonKind::MaxTokens, Some(Some("MAX_TOKENS")))
        } else if reason == "STOP"
            || reason == "FINISH_REASON_STOP"
            || reason == "FINISH_REASON_NATURAL_STOP"
        {
            (FinishReasonKind::OtherTerminal, Some(Some("STOP")))
        } else {
            (FinishReasonKind::OtherTerminal, None)
        }
    } else if let Some(number) = value.as_i64() {
        match number {
            0 => (FinishReasonKind::Unspecified, Some(None)),
            1 => (FinishReasonKind::OtherTerminal, Some(Some("STOP"))),
            2 => (FinishReasonKind::MaxTokens, Some(Some("MAX_TOKENS"))),
            18 => (
                FinishReasonKind::Continuation,
                Some(Some("FINISH_REASON_CONTINUATION")),
            ),
            _ => (FinishReasonKind::OtherTerminal, Some(Some("OTHER"))),
        }
    } else {
        (FinishReasonKind::OtherTerminal, None)
    }
}

fn decode_raw_frame(
    bytes: &[u8],
    generation: Option<&str>,
) -> Result<DecodedCloudFrame, GeminiApiError> {
    let payload = runtime_json::parse(&RuntimeString::from(
        String::from_utf8_lossy(bytes).as_ref(),
    ))
    .map_err(|_| api_error("antigravity_stream_invalid"))?;
    if payload.get("error").is_some()
        || (payload.get("code").is_some()
            && payload.get("status").is_some()
            && payload.get("message").is_some())
    {
        return Err(service_error(None, bytes, Default::default()));
    }
    if let Some(generation) = generation {
        if payload.get("remainingCredits").is_some() || payload.get("consumedCredits").is_some() {
            if let Ok(credits) = serde_json::from_slice(bytes) {
                super::record_credits(generation, &credits);
            }
        }
    }
    let mut inner = payload.get("response").unwrap_or(&payload).clone();
    let mut continuation_token = None;
    let mut finish_reason_kind = FinishReasonKind::Unspecified;
    if let Some(cand) = inner
        .get_mut("candidates")
        .and_then(RuntimeJson::as_array_mut)
        .and_then(|candidates| candidates.first_mut())
        .and_then(RuntimeJson::as_object_mut)
    {
        continuation_token = cand
            .get("continuationToken")
            .or_else(|| cand.get("continuation_token"))
            .and_then(RuntimeJson::as_string)
            .filter(|token| !token.is_empty())
            .cloned();
        let raw_reason = cand
            .get("finishReason")
            .or_else(|| cand.get("finish_reason"));
        let (kind, normalized) = classify_finish_reason_value(raw_reason);
        finish_reason_kind = kind;
        if let Some(replacement) = normalized {
            cand.remove("finish_reason");
            match replacement {
                Some(text) => {
                    cand.insert("finishReason", RuntimeJson::String(text.into()));
                }
                None => {
                    cand.remove("finishReason");
                }
            }
        }
    }
    let prompt_feedback_blocked = inner
        .get("promptFeedback")
        .or_else(|| inner.get("prompt_feedback"))
        .and_then(|feedback| {
            feedback
                .get("blockReason")
                .or_else(|| feedback.get("block_reason"))
        })
        .is_some_and(|reason| {
            if let Some(value) = reason.as_string() {
                !value.is_empty() && value != "BLOCK_REASON_UNSPECIFIED"
            } else if let Some(value) = reason.as_i64() {
                value != 0
            } else {
                false
            }
        });
    let response =
        runtime_codec::decode_value(&inner).map_err(|_| api_error("antigravity_stream_invalid"))?;
    Ok(DecodedCloudFrame {
        response,
        continuation_token,
        finish_reason_kind,
        prompt_feedback_blocked,
    })
}

#[cfg(test)]
fn decode_frame(
    bytes: &[u8],
    generation: Option<&str>,
) -> Result<GenerateContentResponse, GeminiApiError> {
    decode_raw_frame(bytes, generation).map(|frame| frame.response)
}

fn candidate_has_content(candidate: &Candidate) -> bool {
    candidate.content.as_ref().is_some_and(|content| {
        content.parts.iter().any(|part| {
            part.text.as_ref().is_some_and(|text| !text.is_empty())
                || part.function_call.is_some()
                || part.inline_data.is_some()
                || part.function_response.is_some()
        })
    })
}

fn is_continuation_budget_exhausted_error(error: &GeminiApiError) -> bool {
    let invalid_arg = error.status == Some(400) || error.message.contains("[INVALID_ARGUMENT]");
    invalid_arg
        && error
            .message
            .to_ascii_lowercase()
            .contains("more than the max tokens limit allowed")
}

#[derive(Default)]
struct LegState {
    thinking_tokens: u64,
    response_tokens: u64,
    checkpoint_thinking_tokens: u64,
    checkpoint_response_tokens: u64,
    last_finish_reason: FinishReasonKind,
    has_checkpoint: bool,
    saw_candidate: bool,
}

enum LegEndAction {
    Continue {
        is_mid_stream_retry: bool,
        retry_delay_ms: Option<u64>,
    },
    EmitSyntheticMaxTokens,
    FinishCleanly,
    Fail(GeminiApiError),
}

enum DialErrorAction {
    Retry { retry_delay_ms: Option<u64> },
    EmitSyntheticMaxTokens,
    Fail(GeminiApiError),
}

struct StreamContinuationTracker {
    latest_continuation_token: Option<RuntimeString>,
    initial_input_tokens: u64,
    initial_cache_read_tokens: u64,
    prior_legs_thinking_tokens: u64,
    prior_legs_response_tokens: u64,
    leg: LegState,
    last_usage: Option<UsageMetadata>,
    max_continuation_attempts: usize,
    continuation_count: usize,
    max_mid_stream_retries: usize,
    mid_stream_retry_count: usize,
    mid_stream_retry_base_backoff_ms: u64,
    initial_max_output_tokens: Option<u64>,
    last_mid_stream_err: Option<GeminiApiError>,
    has_prior_leg: bool,
    has_initial_input_tokens: bool,
    has_last_usage: bool,
    suppressed_max_tokens_reason: bool,
    has_uncheckpointed_chunks: bool,
    resuming_from_mid_stream_error: bool,
    last_model_version: Option<RuntimeString>,
}

impl StreamContinuationTracker {
    fn new(initial_max_output_tokens: Option<u64>) -> Self {
        Self {
            latest_continuation_token: None,
            initial_input_tokens: 0,
            initial_cache_read_tokens: 0,
            prior_legs_thinking_tokens: 0,
            prior_legs_response_tokens: 0,
            leg: LegState::default(),
            last_usage: None,
            max_continuation_attempts: MAX_CONTINUATION_ATTEMPTS,
            continuation_count: 0,
            max_mid_stream_retries: MAX_MID_STREAM_RETRIES,
            mid_stream_retry_count: 0,
            mid_stream_retry_base_backoff_ms: MID_STREAM_RETRY_BASE_BACKOFF_MS,
            initial_max_output_tokens,
            last_mid_stream_err: None,
            has_prior_leg: false,
            has_initial_input_tokens: false,
            has_last_usage: false,
            suppressed_max_tokens_reason: false,
            has_uncheckpointed_chunks: false,
            resuming_from_mid_stream_error: false,
            last_model_version: None,
        }
    }

    fn has_remaining_output_token_budget(&self) -> bool {
        match self.initial_max_output_tokens {
            None | Some(0) => true,
            Some(max_tokens) => {
                let total = self
                    .prior_legs_thinking_tokens
                    .saturating_add(self.prior_legs_response_tokens)
                    .saturating_add(self.leg.thinking_tokens)
                    .saturating_add(self.leg.response_tokens);
                total < max_tokens
            }
        }
    }

    fn should_continue_on_leg_end(&self) -> bool {
        self.leg.has_checkpoint
            && !self.has_uncheckpointed_chunks
            && self
                .latest_continuation_token
                .as_ref()
                .is_some_and(|token| !token.is_empty())
            && self.continuation_count < self.max_continuation_attempts
            && matches!(
                self.leg.last_finish_reason,
                FinishReasonKind::Continuation | FinishReasonKind::MaxTokens
            )
            && self.has_remaining_output_token_budget()
    }

    fn record_candidate(
        &mut self,
        continuation_token: Option<RuntimeString>,
        has_content: bool,
        finish_reason_kind: FinishReasonKind,
        usage: Option<&UsageMetadata>,
    ) -> bool {
        self.leg.saw_candidate = true;
        self.resuming_from_mid_stream_error = false;
        self.last_mid_stream_err = None;
        if has_content && self.leg.last_finish_reason == FinishReasonKind::Unspecified {
            self.suppressed_max_tokens_reason = false;
        }
        let has_token = continuation_token
            .as_ref()
            .is_some_and(|token| !token.is_empty());
        if let Some(token) = continuation_token.filter(|token| !token.is_empty()) {
            let same = self.latest_continuation_token.as_ref() == Some(&token);
            self.latest_continuation_token = Some(token);
            self.has_uncheckpointed_chunks = false;
            self.leg.has_checkpoint = true;
            if !same {
                self.mid_stream_retry_count = 0;
            }
        } else if has_content {
            self.has_uncheckpointed_chunks = true;
            if self.leg.last_finish_reason != FinishReasonKind::Unspecified
                && finish_reason_kind == FinishReasonKind::Unspecified
            {
                self.suppressed_max_tokens_reason = false;
            }
        }
        if let Some(usage) = usage {
            let thoughts = usage.thoughts_token_count.unwrap_or(0);
            let candidates = usage.candidates_token_count.unwrap_or(0);
            self.leg.thinking_tokens = thoughts;
            self.leg.response_tokens = candidates;
            if has_token || (!self.has_uncheckpointed_chunks && self.leg.has_checkpoint) {
                self.leg.checkpoint_thinking_tokens = thoughts;
                self.leg.checkpoint_response_tokens = candidates;
            }
            let prompt = usage.prompt_token_count.unwrap_or(0);
            let cached = usage.cached_content_token_count.unwrap_or(0).min(prompt);
            let uncached_input = prompt.saturating_sub(cached);
            let (pinned_input, pinned_cached) = if self.has_prior_leg {
                if self.has_initial_input_tokens {
                    (self.initial_input_tokens, self.initial_cache_read_tokens)
                } else {
                    (uncached_input, cached)
                }
            } else {
                self.initial_input_tokens = uncached_input;
                self.initial_cache_read_tokens = cached;
                self.has_initial_input_tokens = true;
                (uncached_input, cached)
            };
            let pinned_prompt = pinned_input.saturating_add(pinned_cached);
            let total_thinking = self
                .prior_legs_thinking_tokens
                .saturating_add(self.leg.thinking_tokens);
            let total_response = self
                .prior_legs_response_tokens
                .saturating_add(self.leg.response_tokens);
            let total_output = total_thinking.saturating_add(total_response);
            let total_tokens = pinned_prompt.saturating_add(total_output);
            self.last_usage = Some(UsageMetadata {
                prompt_token_count: Some(pinned_prompt),
                cached_content_token_count: Some(pinned_cached),
                candidates_token_count: Some(total_response),
                thoughts_token_count: Some(total_thinking),
                total_token_count: Some(total_tokens),
            });
            self.has_last_usage = true;
        } else if has_token {
            self.leg.checkpoint_thinking_tokens = self.leg.thinking_tokens;
            self.leg.checkpoint_response_tokens = self.leg.response_tokens;
        }
        let mut suppress_finish_reason = false;
        if finish_reason_kind != FinishReasonKind::Unspecified {
            self.leg.last_finish_reason = finish_reason_kind;
            self.suppressed_max_tokens_reason = false;
            if self.should_continue_on_leg_end() {
                self.suppressed_max_tokens_reason = true;
                suppress_finish_reason = true;
            }
        }
        suppress_finish_reason
    }

    fn record_frame(&mut self, mut decoded: DecodedCloudFrame) -> GenerateContentResponse {
        if decoded
            .response
            .model_version
            .as_ref()
            .is_some_and(|version| !version.is_empty())
        {
            self.last_model_version = decoded.response.model_version.clone();
        }
        let has_candidates = decoded
            .response
            .candidates
            .as_ref()
            .is_some_and(|candidates| !candidates.is_empty());
        if has_candidates {
            let has_content = decoded
                .response
                .candidates
                .as_ref()
                .and_then(|candidates| candidates.first())
                .is_some_and(candidate_has_content);
            let suppress_finish_reason = self.record_candidate(
                decoded.continuation_token,
                has_content,
                decoded.finish_reason_kind,
                decoded.response.usage_metadata.as_ref(),
            );
            if decoded.response.usage_metadata.is_some() && self.has_last_usage {
                decoded.response.usage_metadata = self.last_usage.clone();
            }
            if let Some(cand) = decoded
                .response
                .candidates
                .as_mut()
                .and_then(|candidates| candidates.first_mut())
            {
                if suppress_finish_reason {
                    cand.finish_reason = None;
                    if cand.content.is_none() {
                        cand.content = Some(Content {
                            role: "model".into(),
                            parts: Vec::new(),
                        });
                    }
                } else if matches!(
                    decoded.finish_reason_kind,
                    FinishReasonKind::Continuation | FinishReasonKind::MaxTokens
                ) {
                    cand.finish_reason = Some("MAX_TOKENS".into());
                }
            }
        } else if decoded.prompt_feedback_blocked
            || (decoded.response.usage_metadata.is_some() && self.leg.saw_candidate)
        {
            let has_content = decoded.prompt_feedback_blocked;
            let kind = if decoded.prompt_feedback_blocked {
                FinishReasonKind::OtherTerminal
            } else {
                FinishReasonKind::Unspecified
            };
            let _ = self.record_candidate(
                None,
                has_content,
                kind,
                decoded.response.usage_metadata.as_ref(),
            );
            if decoded.response.usage_metadata.is_some() && self.has_last_usage {
                decoded.response.usage_metadata = self.last_usage.clone();
            }
        } else if decoded.response.usage_metadata.is_some() && self.has_prior_leg {
            if self.has_last_usage {
                decoded.response.usage_metadata = self.last_usage.clone();
            } else {
                decoded.response.usage_metadata = None;
            }
        }
        decoded.response
    }

    fn can_resume_on_error(&self, error: &GeminiApiError) -> Option<Option<u64>> {
        if self.has_uncheckpointed_chunks
            || self
                .latest_continuation_token
                .as_ref()
                .is_none_or(RuntimeString::is_empty)
            || (self.leg.last_finish_reason != FinishReasonKind::Unspecified
                && !self.suppressed_max_tokens_reason)
            || self.max_continuation_attempts == 0
            || self.continuation_count >= self.max_continuation_attempts
            || self.mid_stream_retry_count >= self.max_mid_stream_retries
        {
            return None;
        }
        if error.status == Some(429)
            || error.message.contains("[RESOURCE_EXHAUSTED]")
            || error.message.starts_with("antigravity_quota_exhausted")
            || error.message.starts_with("antigravity_rate_limited")
        {
            return None;
        }
        let classified =
            klaus_agent_core::provider::classify::classify_error(&error_details(error));
        if !klaus_agent_core::provider::retry::is_retryable(&classified)
            || matches!(
                classified.kind,
                klaus_agent_core::types::errors::ProviderErrorKind::Quota
                    | klaus_agent_core::types::errors::ProviderErrorKind::RateLimit
            )
        {
            return None;
        }
        if classified
            .retry_after_ms
            .is_some_and(|delay| delay > MID_STREAM_RETRY_MAX_DELAY_MS)
        {
            return None;
        }
        Some(classified.retry_after_ms)
    }

    fn commit_leg_boundary_tokens(&mut self) {
        self.prior_legs_thinking_tokens = self
            .prior_legs_thinking_tokens
            .saturating_add(self.leg.thinking_tokens);
        self.prior_legs_response_tokens = self
            .prior_legs_response_tokens
            .saturating_add(self.leg.response_tokens);
    }

    fn commit_checkpoint_tokens(&mut self) {
        self.prior_legs_thinking_tokens = self
            .prior_legs_thinking_tokens
            .saturating_add(self.leg.checkpoint_thinking_tokens);
        self.prior_legs_response_tokens = self
            .prior_legs_response_tokens
            .saturating_add(self.leg.checkpoint_response_tokens);
    }

    fn evaluate_leg_end(&mut self, mut leg_err: Option<GeminiApiError>) -> LegEndAction {
        if leg_err.is_none() {
            if self.resuming_from_mid_stream_error
                && !self.leg.saw_candidate
                && !self.suppressed_max_tokens_reason
            {
                leg_err = Some(
                    self.last_mid_stream_err
                        .clone()
                        .unwrap_or_else(|| api_error("antigravity_stream_interrupted")),
                );
            } else if self.should_continue_on_leg_end() {
                self.commit_leg_boundary_tokens();
                self.continuation_count += 1;
                self.has_prior_leg = true;
                self.resuming_from_mid_stream_error = false;
                self.last_mid_stream_err = None;
                self.leg = LegState::default();
                return LegEndAction::Continue {
                    is_mid_stream_retry: false,
                    retry_delay_ms: None,
                };
            } else if self.suppressed_max_tokens_reason {
                self.suppressed_max_tokens_reason = false;
                self.resuming_from_mid_stream_error = false;
                self.last_mid_stream_err = None;
                return LegEndAction::EmitSyntheticMaxTokens;
            } else {
                return LegEndAction::FinishCleanly;
            }
        }
        let err = leg_err.expect("checked Some above");
        if (self.continuation_count > 0 || self.resuming_from_mid_stream_error)
            && !self.has_uncheckpointed_chunks
            && is_continuation_budget_exhausted_error(&err)
        {
            self.suppressed_max_tokens_reason = false;
            self.resuming_from_mid_stream_error = false;
            self.last_mid_stream_err = None;
            return LegEndAction::EmitSyntheticMaxTokens;
        }
        if let Some(retry_delay_ms) = self.can_resume_on_error(&err) {
            self.mid_stream_retry_count += 1;
            self.commit_checkpoint_tokens();
            self.continuation_count += 1;
            self.has_prior_leg = true;
            self.resuming_from_mid_stream_error = true;
            self.last_mid_stream_err = Some(err);
            self.leg = LegState::default();
            return LegEndAction::Continue {
                is_mid_stream_retry: true,
                retry_delay_ms,
            };
        }
        LegEndAction::Fail(err)
    }

    fn evaluate_dial_error(&mut self, dial_err: GeminiApiError) -> DialErrorAction {
        if is_continuation_budget_exhausted_error(&dial_err) {
            self.suppressed_max_tokens_reason = false;
            self.resuming_from_mid_stream_error = false;
            self.last_mid_stream_err = None;
            return DialErrorAction::EmitSyntheticMaxTokens;
        }
        if let Some(retry_delay_ms) = self.can_resume_on_error(&dial_err) {
            self.mid_stream_retry_count += 1;
            if self.resuming_from_mid_stream_error {
                self.last_mid_stream_err = Some(dial_err);
            }
            return DialErrorAction::Retry { retry_delay_ms };
        }
        DialErrorAction::Fail(dial_err)
    }

    fn mid_stream_retry_delay_ms(&self, retry_after_ms: Option<u64>) -> u64 {
        let backoff = if self.mid_stream_retry_base_backoff_ms > 0 {
            let shift = self.mid_stream_retry_count.saturating_sub(1).min(6) as u32;
            let raw = self
                .mid_stream_retry_base_backoff_ms
                .saturating_mul(1u64 << shift)
                .min(MID_STREAM_RETRY_MAX_BACKOFF_MS);
            let half = raw / 2;
            if half > 0 {
                half.saturating_add(rand::random::<u64>() % half)
            } else {
                raw
            }
        } else {
            0
        };
        retry_after_ms.unwrap_or(0).max(backoff)
    }

    fn new_synthetic_max_tokens_response(&self) -> GenerateContentResponse {
        GenerateContentResponse {
            model_version: self.last_model_version.clone(),
            candidates: Some(vec![Candidate {
                content: Some(Content {
                    role: "model".into(),
                    parts: Vec::new(),
                }),
                finish_reason: Some("MAX_TOKENS".into()),
                grounding_metadata: None,
            }]),
            usage_metadata: if self.has_last_usage {
                self.last_usage.clone()
            } else {
                None
            },
        }
    }
}

fn build_leg_body(
    project: &str,
    model: &str,
    user_prompt_id: &str,
    base_request: &RuntimeJson,
    continuation_token: Option<&RuntimeString>,
) -> Result<Vec<u8>, GeminiApiError> {
    let mut request = base_request.clone();
    if let Some(token) = continuation_token {
        request
            .as_object_mut()
            .ok_or_else(|| api_error("antigravity_request_invalid"))?
            .insert("continuationToken", RuntimeJson::String(token.clone()));
    }
    let mut envelope_value = envelope(project, model, Value::Null, "agent");
    if let Some(obj) = envelope_value.as_object_mut() {
        obj.insert("userPromptId".into(), Value::String(user_prompt_id.into()));
    }
    let mut body: RuntimeJson =
        runtime_codec::decode_text(&RuntimeString::from(envelope_value.to_string()))
            .map_err(|_| api_error("antigravity_request_invalid"))?;
    body.as_object_mut()
        .ok_or_else(|| api_error("antigravity_request_invalid"))?
        .insert("request", request);
    let mut bytes = Vec::new();
    runtime_json::write_to(&body, &mut bytes)
        .map_err(|_| api_error("antigravity_request_invalid"))?;
    Ok(bytes)
}

#[allow(clippy::too_many_arguments)]
async fn send_stream_leg(
    generation: &str,
    model: &str,
    project: &mut String,
    user_prompt_id: &str,
    base_request: &RuntimeJson,
    continuation_token: Option<&RuntimeString>,
    signal: &AbortSignal,
    extra_headers: &[(String, String)],
) -> Result<reqwest::Response, GeminiApiError> {
    let body = build_leg_body(
        project,
        model,
        user_prompt_id,
        base_request,
        continuation_token,
    )?;
    match send(
        "streamGenerateContent",
        body,
        Some(generation),
        signal,
        extra_headers.to_vec(),
    )
    .await
    {
        Ok(response) => Ok(response),
        // A NOT_FOUND refusal can mean the cached project no longer resolves
        // rather than the request being wrong; resolve a fresh project and
        // replay the request once before trusting the refusal.
        Err(first) if is_not_found(&first) => {
            let healed = super::heal_project(generation, signal)
                .await
                .map_err(api_error)?;
            let retry_body = build_leg_body(
                &healed,
                model,
                user_prompt_id,
                base_request,
                continuation_token,
            )?;
            match send(
                "streamGenerateContent",
                retry_body,
                Some(generation),
                signal,
                extra_headers.to_vec(),
            )
            .await
            {
                Ok(response) => {
                    *project = healed;
                    Ok(response)
                }
                Err(second) if is_not_found(&second) => {
                    Err(not_offered_or_original(generation, model, second).await)
                }
                Err(second) => Err(second),
            }
        }
        Err(first) => Err(first),
    }
}

async fn sleep_for_mid_stream_retry(
    delay_ms: u64,
    generation: &str,
    signal: &AbortSignal,
) -> Result<(), GeminiApiError> {
    if !auth::is_current(generation) {
        return Err(api_error("antigravity_credential_changed"));
    }
    if signal.is_cancelled() {
        return Err(api_error("antigravity_request_cancelled"));
    }
    if delay_ms == 0 {
        return Ok(());
    }
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(delay_ms);
    loop {
        tokio::select! {
            _ = signal.cancelled() => return Err(api_error("antigravity_request_cancelled")),
            _ = tokio::time::sleep_until(deadline) => break,
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                if !auth::is_current(generation) {
                    return Err(api_error("antigravity_credential_changed"));
                }
            }
        }
    }
    if !auth::is_current(generation) {
        return Err(api_error("antigravity_credential_changed"));
    }
    Ok(())
}

// This family completes a reasoning run with a signature-only trailer. Keep
// genuine signatures opaque and apply this grouping only at its cloud boundary.
fn adapt_anthropic_parts(parts: &mut Vec<RuntimeJson>) {
    let mut completed: Vec<RuntimeJson> = Vec::with_capacity(parts.len());
    for mut part in std::mem::take(parts) {
        let signed_trailer = part.get("thought").and_then(RuntimeJson::as_bool) == Some(true)
            && part
                .get("thoughtSignature")
                .and_then(RuntimeJson::as_string)
                .is_some()
            && part
                .get("text")
                .and_then(RuntimeJson::as_string)
                .is_none_or(RuntimeString::is_empty)
            && ["inlineData", "functionCall", "functionResponse"]
                .iter()
                .all(|key| part.get(*key).is_none());
        if signed_trailer {
            let start = completed
                .iter()
                .rposition(|previous| {
                    previous.get("thought").and_then(RuntimeJson::as_bool) != Some(true)
                        || [
                            "thoughtSignature",
                            "inlineData",
                            "functionCall",
                            "functionResponse",
                        ]
                        .iter()
                        .any(|key| previous.get(*key).is_some())
                })
                .map_or(0, |index| index + 1);
            if start < completed.len() {
                let text = completed[start..]
                    .iter()
                    .filter_map(|previous| previous.get("text").and_then(RuntimeJson::as_string))
                    .fold(RuntimeString::default(), |text, delta| text.concat(delta));
                completed.truncate(start);
                part.as_object_mut()
                    .expect("thought part is an object")
                    .insert("text", RuntimeJson::String(text));
            }
        }
        if let Some(object) = part.as_object_mut() {
            if object.get("functionCall").is_some()
                && object
                    .get("thoughtSignature")
                    .and_then(RuntimeJson::as_string)
                    .is_some_and(|signature| signature == "skip_thought_signature_validator")
            {
                object.remove("thoughtSignature");
            }
        }
        completed.push(part);
    }

    // The cloud service attaches at most one `thinking.signature` per model
    // Content (on the first `thought: true` part). When a turn emits multiple
    // signed thought runs (for example, a `thinking` run followed by a
    // `narration` run), leaving a second `thought: true` part in the same
    // Content produces an unsigned `thinking` block that Anthropic rejects with
    // `thinking.signature: Field required`. Coalesce all `thought: true` parts
    // into the first signed thought part and drop unsigned thought parts when
    // no signed thought part exists.
    let mut primary_thought_index: Option<usize> = None;
    let mut pending_thought_text = RuntimeString::default();
    let mut coalesced: Vec<RuntimeJson> = Vec::with_capacity(completed.len());
    for mut part in completed {
        if part.get("thought").and_then(RuntimeJson::as_bool) == Some(true) {
            let text = part
                .get("text")
                .and_then(RuntimeJson::as_string)
                .cloned()
                .unwrap_or_default();
            let has_signature = part
                .get("thoughtSignature")
                .and_then(RuntimeJson::as_string)
                .is_some_and(|sig| !sig.is_empty());
            if primary_thought_index.is_none() && has_signature {
                let merged_text = if pending_thought_text.is_empty() {
                    text
                } else {
                    std::mem::take(&mut pending_thought_text).concat(&text)
                };
                if !merged_text.is_empty() {
                    if let Some(obj) = part.as_object_mut() {
                        obj.insert("text", RuntimeJson::String(merged_text));
                    }
                    primary_thought_index = Some(coalesced.len());
                    coalesced.push(part);
                }
            } else if let Some(idx) = primary_thought_index {
                if !text.is_empty() {
                    let existing = coalesced[idx]
                        .get("text")
                        .and_then(RuntimeJson::as_string)
                        .cloned()
                        .unwrap_or_default();
                    if let Some(obj) = coalesced[idx].as_object_mut() {
                        obj.insert("text", RuntimeJson::String(existing.concat(&text)));
                    }
                }
            } else if !text.is_empty() {
                pending_thought_text = pending_thought_text.concat(&text);
            }
        } else {
            coalesced.push(part);
        }
    }

    // Parallel tool results with inlineData/text attachments must keep all
    // `functionResponse` parts contiguous at the front of the user Content so
    // the translated Anthropic user message has all `tool_result` blocks
    // immediately after the assistant `tool_use` turn.
    if coalesced
        .iter()
        .any(|part| part.get("functionResponse").is_some())
    {
        let mut responses = Vec::with_capacity(coalesced.len());
        let mut attachments = Vec::new();
        for part in coalesced {
            if part.get("functionResponse").is_some() {
                responses.push(part);
            } else {
                attachments.push(part);
            }
        }
        responses.extend(attachments);
        *parts = responses;
    } else {
        *parts = coalesced;
    }
}

fn adapt_model_request(request: &mut RuntimeJson, details: &Value) -> Result<(), GeminiApiError> {
    let object = request
        .as_object_mut()
        .ok_or_else(|| api_error("antigravity_request_invalid"))?;
    if details.get("modelProvider").is_some_and(|provider| {
        provider.as_str() == Some("MODEL_PROVIDER_ANTHROPIC") || provider.as_i64() == Some(3)
    }) {
        for content in object
            .get_mut("contents")
            .and_then(RuntimeJson::as_array_mut)
            .into_iter()
            .flatten()
        {
            if let Some(parts) = content.get_mut("parts").and_then(RuntimeJson::as_array_mut) {
                adapt_anthropic_parts(parts);
            }
        }
    }
    if details
        .get("doesNotSupportToolChoice")
        .and_then(Value::as_bool)
        == Some(true)
        || details.get("supportsToolChoice").and_then(Value::as_bool) == Some(false)
    {
        let mode = object
            .get("toolConfig")
            .and_then(|config| config.get("functionCallingConfig"))
            .and_then(|config| config.get("mode"))
            .and_then(RuntimeJson::as_string);
        if mode.is_some_and(|mode| mode != "AUTO") {
            return Err(api_error("antigravity_tool_choice_unsupported"));
        }
        object.remove("toolConfig");
    }
    let Some(config) = object
        .get_mut("generationConfig")
        .and_then(RuntimeJson::as_object_mut)
        .and_then(|config| config.get_mut("thinkingConfig"))
        .and_then(RuntimeJson::as_object_mut)
    else {
        return Ok(());
    };
    if config.get("includeThoughts").and_then(RuntimeJson::as_bool) == Some(false) {
        return Ok(());
    }
    if let Some(level) = details
        .get("thinkingLevel")
        .and_then(Value::as_i64)
        .filter(|level| *level > 0)
    {
        let level = match level {
            1 => "LOW",
            2 => "MEDIUM",
            3 => "HIGH",
            4 => "MINIMAL",
            5 => "EXTRA_HIGH",
            6 | 200 => "MAX",
            _ => return Err(api_error("antigravity_thinking_level_unsupported")),
        };
        config.remove("thinkingBudget");
        config.insert("thinkingLevel", RuntimeJson::String(level.into()));
    } else if let Some(budget) = details
        .get("thinkingBudget")
        .and_then(Value::as_i64)
        .filter(|budget| *budget > 0 || *budget == -1)
    {
        config.remove("thinkingLevel");
        config.insert("thinkingBudget", RuntimeJson::from(budget));
    } else if details
        .get("supportsAdaptiveThinking")
        .and_then(Value::as_bool)
        == Some(true)
    {
        config.remove("thinkingBudget");
    }
    Ok(())
}

fn has_tag_prefix(message: &str, tag: &str) -> bool {
    message == tag
        || message
            .strip_prefix(tag)
            .is_some_and(|suffix| suffix.starts_with(':') || suffix.starts_with(" ["))
}

pub(super) fn is_auth_refusal_message(message: &str) -> bool {
    has_tag_prefix(message, "antigravity_auth_required")
}

pub(super) fn is_rotatable_message(message: &str) -> bool {
    [
        "antigravity_rate_limited",
        "antigravity_quota_exhausted",
        "antigravity_auth_required",
        "antigravity_login_expired",
    ]
    .iter()
    .any(|tag| has_tag_prefix(message, tag))
}

async fn establish_initial_leg_for_generation(
    generation: &str,
    params: &GenerateContentParameters,
    model: &str,
    user_prompt_id: &str,
    signal: &AbortSignal,
    extra_headers: &[(String, String)],
) -> Result<(String, RuntimeJson, Option<u64>, reqwest::Response), GeminiApiError> {
    let mut base_request = generate_content_request(params);
    if let Some(details) = super::model_details(generation, model) {
        adapt_model_request(&mut base_request, &details)?;
    }
    let initial_max_output_tokens = base_request
        .get("generationConfig")
        .and_then(|config| config.get("maxOutputTokens"))
        .and_then(RuntimeJson::as_i64)
        .filter(|value| *value > 0)
        .map(|value| value as u64);

    let mut renewed = false;
    loop {
        let mut project = match super::project(Some(generation), signal).await {
            Ok(project) => project,
            Err(err) if !renewed && is_auth_refusal_message(&err) && !signal.is_cancelled() => {
                renewed = true;
                if auth::force_refresh(generation).await.is_ok() {
                    continue;
                }
                return Err(api_error(err));
            }
            Err(err) => return Err(api_error(err)),
        };
        match send_stream_leg(
            generation,
            model,
            &mut project,
            user_prompt_id,
            &base_request,
            None,
            signal,
            extra_headers,
        )
        .await
        {
            Ok(response) => {
                return Ok((project, base_request, initial_max_output_tokens, response))
            }
            Err(err)
                if !renewed && is_auth_refusal_message(&err.message) && !signal.is_cancelled() =>
            {
                renewed = true;
                if auth::force_refresh(generation).await.is_ok() {
                    continue;
                }
                return Err(err);
            }
            Err(err) => return Err(err),
        }
    }
}

#[async_trait::async_trait]
impl GeminiClient for CloudClient {
    fn error_details(&self, error: &GeminiApiError) -> RawProviderError {
        error_details(error)
    }

    async fn generate_content_stream(
        &self,
        params: GenerateContentParameters,
        headers: Option<AuthHeaders>,
        mut extra_headers: Vec<(String, String)>,
        signal: AbortSignal,
    ) -> Result<GeminiChunkStream, GeminiApiError> {
        if let Some(headers) = headers {
            extra_headers.extend(headers);
        }
        let model = params
            .model
            .try_to_utf8()
            .map_err(|_| api_error("antigravity_model_invalid"))?;
        let user_prompt_id = uuid::Uuid::new_v4().to_string();
        let pooled = self.generation.is_empty();
        let mut attempted: Vec<String> = Vec::new();
        let (mut generation, mut project, mut base_request, initial_max_output_tokens, response) = loop {
            let candidate_gen = if pooled {
                auth::generation().ok_or_else(|| api_error("antigravity_not_signed_in"))?
            } else {
                self.generation.clone()
            };
            match establish_initial_leg_for_generation(
                &candidate_gen,
                &params,
                &model,
                &user_prompt_id,
                &signal,
                &extra_headers,
            )
            .await
            {
                Ok((project, base_request, max_tokens, response)) => {
                    break (candidate_gen, project, base_request, max_tokens, response);
                }
                Err(err)
                    if pooled
                        && !signal.is_cancelled()
                        && is_rotatable_message(&err.message)
                        && auth::account_count() > 1
                        && !attempted.contains(&candidate_gen) =>
                {
                    attempted.push(candidate_gen.clone());
                    match auth::rotate(&candidate_gen).await {
                        RotateOutcome::Rotated(next) | RotateOutcome::AlreadyAdvanced(next)
                            if !attempted.contains(&next) =>
                        {
                            continue;
                        }
                        _ => return Err(err),
                    }
                }
                Err(err) => return Err(err),
            }
        };

        Ok(Box::pin(async_stream::stream! {
            let mut tracker = StreamContinuationTracker::new(initial_max_output_tokens);
            let mut response = response;
            let mut in_stream_auth_renewed = false;
            'leg: loop {
                let mut stream = response.bytes_stream();
                let mut buffered = Vec::new();
                let mut data = Vec::new();
                let mut total_bytes = 0usize;
                let mut leg_err: Option<GeminiApiError> = None;
                'read: loop {
                    let chunk = tokio::select! {
                        _ = signal.cancelled() => return,
                        _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                            if !auth::is_current(&generation) {
                                yield Err(api_error("antigravity_credential_changed"));
                                return;
                            }
                            continue;
                        },
                        chunk = stream.next() => chunk,
                    };
                    let Some(chunk) = chunk else { break 'read; };
                    if !auth::is_current(&generation) {
                        yield Err(api_error("antigravity_credential_changed"));
                        return;
                    }
                    let chunk = match chunk {
                        Ok(chunk) => chunk,
                        Err(_) => {
                            leg_err = Some(api_error("antigravity_stream_interrupted"));
                            break 'read;
                        }
                    };
                    total_bytes = total_bytes.saturating_add(chunk.len());
                    if total_bytes > MAX_BODY_BYTES {
                        yield Err(api_error("antigravity_stream_response_too_large"));
                        return;
                    }
                    if buffered.len().saturating_add(data.len()).saturating_add(chunk.len()) > MAX_SSE_FRAME_BYTES {
                        yield Err(api_error("antigravity_stream_frame_too_large"));
                        return;
                    }
                    buffered.extend_from_slice(&chunk);
                    while let Some(index) = buffered.iter().position(|byte| *byte == b'\n') {
                        let mut line: Vec<u8> = buffered.drain(..=index).collect();
                        line.pop();
                        if line.last() == Some(&b'\r') {
                            line.pop();
                        }
                        if line.is_empty() {
                            if !data.is_empty() {
                                if data.as_slice() != b"[DONE]" {
                                    if !auth::is_current(&generation) {
                                        yield Err(api_error("antigravity_credential_changed"));
                                        return;
                                    }
                                    match decode_raw_frame(&data, Some(&generation)) {
                                        Ok(decoded) => {
                                            yield Ok(tracker.record_frame(decoded));
                                        }
                                        Err(error) => {
                                            leg_err = Some(error);
                                            break 'read;
                                        }
                                    }
                                }
                                data.clear();
                            }
                        } else if let Some(payload) = line.strip_prefix(b"data:") {
                            let payload = payload.strip_prefix(b" ").unwrap_or(payload);
                            if !data.is_empty() {
                                data.push(b'\n');
                            }
                            data.extend_from_slice(payload);
                        } else if data.is_empty() && line.trim_ascii().starts_with(b"{") {
                            if let Err(error) = decode_raw_frame(line.trim_ascii(), Some(&generation)) {
                                if error.message != "antigravity_stream_invalid" {
                                    leg_err = Some(error);
                                    break 'read;
                                }
                            }
                        }
                    }
                }
                if leg_err.is_none() {
                    if let Some(payload) = buffered.strip_prefix(b"data:") {
                        if !data.is_empty() {
                            data.push(b'\n');
                        }
                        data.extend_from_slice(payload.strip_prefix(b" ").unwrap_or(payload));
                    } else if data.is_empty() && buffered.trim_ascii().starts_with(b"{") {
                        if let Err(error) = decode_raw_frame(buffered.trim_ascii(), Some(&generation)) {
                            if error.message != "antigravity_stream_invalid" {
                                leg_err = Some(error);
                            }
                        }
                    }
                    if leg_err.is_none() && !data.is_empty() && data.as_slice() != b"[DONE]" {
                        if !auth::is_current(&generation) {
                            yield Err(api_error("antigravity_credential_changed"));
                            return;
                        }
                        match decode_raw_frame(&data, Some(&generation)) {
                            Ok(decoded) => {
                                yield Ok(tracker.record_frame(decoded));
                            }
                            Err(error) => {
                                leg_err = Some(error);
                            }
                        }
                    }
                }
                if !auth::is_current(&generation) {
                    yield Err(api_error("antigravity_credential_changed"));
                    return;
                }
                if signal.is_cancelled() {
                    return;
                }
                if !tracker.leg.saw_candidate
                    && !tracker.has_prior_leg
                    && leg_err.as_ref().is_some_and(|err| is_rotatable_message(&err.message))
                {
                    let first_err = leg_err.take().unwrap();
                    if !in_stream_auth_renewed && is_auth_refusal_message(&first_err.message) {
                        in_stream_auth_renewed = true;
                        if auth::force_refresh(&generation).await.is_ok() {
                            if let Ok((next_proj, next_base, _, next_resp)) =
                                establish_initial_leg_for_generation(
                                    &generation,
                                    &params,
                                    &model,
                                    &user_prompt_id,
                                    &signal,
                                    &extra_headers,
                                )
                                .await
                            {
                                project = next_proj;
                                base_request = next_base;
                                response = next_resp;
                                tracker = StreamContinuationTracker::new(initial_max_output_tokens);
                                continue 'leg;
                            }
                        }
                    }
                    if pooled
                        && auth::account_count() > 1
                        && !attempted.contains(&generation)
                    {
                        attempted.push(generation.clone());
                        let mut terminal_err = first_err;
                        while let RotateOutcome::Rotated(next)
                        | RotateOutcome::AlreadyAdvanced(next) =
                            auth::rotate(&generation).await
                        {
                            if attempted.contains(&next) || signal.is_cancelled() {
                                break;
                            }
                            generation = next.clone();
                            in_stream_auth_renewed = false;
                            match establish_initial_leg_for_generation(
                                &generation,
                                &params,
                                &model,
                                &user_prompt_id,
                                &signal,
                                &extra_headers,
                            )
                            .await
                            {
                                Ok((next_proj, next_base, _, next_resp)) => {
                                    project = next_proj;
                                    base_request = next_base;
                                    response = next_resp;
                                    tracker =
                                        StreamContinuationTracker::new(initial_max_output_tokens);
                                    continue 'leg;
                                }
                                Err(err)
                                    if is_rotatable_message(&err.message)
                                        && !attempted.contains(&generation) =>
                                {
                                    attempted.push(generation.clone());
                                    terminal_err = err;
                                }
                                Err(err) => {
                                    yield Err(err);
                                    return;
                                }
                            }
                        }
                        leg_err = Some(terminal_err);
                    } else {
                        leg_err = Some(first_err);
                    }
                }
                match tracker.evaluate_leg_end(leg_err) {
                    LegEndAction::FinishCleanly => return,
                    LegEndAction::EmitSyntheticMaxTokens => {
                        yield Ok(tracker.new_synthetic_max_tokens_response());
                        return;
                    }
                    LegEndAction::Fail(error) => {
                        yield Err(error);
                        return;
                    }
                    LegEndAction::Continue {
                        is_mid_stream_retry,
                        retry_delay_ms,
                    } => {
                        if is_mid_stream_retry {
                            let delay_ms = tracker.mid_stream_retry_delay_ms(retry_delay_ms);
                            if let Err(error) =
                                sleep_for_mid_stream_retry(delay_ms, &generation, &signal).await
                            {
                                if !signal.is_cancelled() {
                                    yield Err(error);
                                }
                                return;
                            }
                        }
                        loop {
                            if signal.is_cancelled() {
                                return;
                            }
                            if !auth::is_current(&generation) {
                                yield Err(api_error("antigravity_credential_changed"));
                                return;
                            }
                            let token = tracker.latest_continuation_token.clone();
                            match send_stream_leg(
                                &generation,
                                &model,
                                &mut project,
                                &user_prompt_id,
                                &base_request,
                                token.as_ref(),
                                &signal,
                                &extra_headers,
                            )
                            .await
                            {
                                Ok(next_response) => {
                                    response = next_response;
                                    continue 'leg;
                                }
                                Err(dial_err) => match tracker.evaluate_dial_error(dial_err) {
                                    DialErrorAction::EmitSyntheticMaxTokens => {
                                        yield Ok(tracker.new_synthetic_max_tokens_response());
                                        return;
                                    }
                                    DialErrorAction::Fail(error) => {
                                        yield Err(error);
                                        return;
                                    }
                                    DialErrorAction::Retry { retry_delay_ms } => {
                                        let delay_ms =
                                            tracker.mid_stream_retry_delay_ms(retry_delay_ms);
                                        if let Err(error) = sleep_for_mid_stream_retry(
                                            delay_ms,
                                            &generation,
                                            &signal,
                                        )
                                        .await
                                        {
                                            if !signal.is_cancelled() {
                                                yield Err(error);
                                            }
                                            return;
                                        }
                                    }
                                },
                            }
                        }
                    }
                }
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A truncated cloud response was classified as Unknown and skipped the
    // engine's bounded stream recovery. Response-read and transport failures
    // below the first response byte carry network semantics so the engine
    // retry schedule owns recovery; local verdicts stay terminal.
    #[test]
    fn interrupted_streams_enter_recovery_without_retrying_local_refusals() {
        use klaus_agent_core::provider::classify::classify_error;
        use klaus_agent_core::provider::retry::is_retryable;
        use klaus_agent_core::types::errors::ProviderErrorKind;

        for key in [
            "antigravity_stream_incomplete",
            "antigravity_stream_interrupted",
            "antigravity_transport_unavailable (connect)",
            "antigravity_transport_unavailable (sent)",
        ] {
            let classified = classify_error(&error_details(&api_error(key)));
            assert_eq!(classified.kind, ProviderErrorKind::Network, "{key}");
            assert_eq!(classified.status, None);
            assert!(classified.message.starts_with(key));
            assert!(is_retryable(&classified));
        }
        for key in [
            "antigravity_credential_changed",
            "antigravity_request_cancelled",
            "antigravity_stream_invalid",
            "antigravity_stream_response_too_large",
            "antigravity_stream_frame_too_large",
        ] {
            let classified = classify_error(&error_details(&api_error(key)));
            assert!(!is_retryable(&classified), "{key}");
            assert_eq!(classified.message, key);
        }
    }

    // Upstream capacity verdicts clear on their own, so they must reach the
    // engine retry schedule as overload instead of failing the turn fast as a
    // request fault — on the refusal path and inside the stream alike.
    #[test]
    fn transient_capacity_verdicts_stay_retryable() {
        use klaus_agent_core::provider::classify::classify_error;
        use klaus_agent_core::provider::retry::is_retryable;
        use klaus_agent_core::types::errors::ProviderErrorKind;

        for service_status in [
            "PREFILL_QUEUE_OVERLOADED",
            "DECODE_PREEMPTED",
            "TPU_EXECUTE_ERROR",
            "MODEL_CAPACITY_EXHAUSTED",
        ] {
            let bytes = serde_json::to_vec(&json!({
                "error": {
                    "code": 400,
                    "status": service_status,
                    "message": "Upstream capacity is temporarily busy."
                }
            }))
            .unwrap();
            for error in [
                service_error(Some(400), &bytes, Default::default()),
                decode_frame(&bytes, None).unwrap_err(),
            ] {
                let classified = classify_error(&error_details(&error));
                assert_eq!(
                    classified.kind,
                    ProviderErrorKind::Overloaded,
                    "{service_status}"
                );
                assert_eq!(
                    classified.code.as_ref().unwrap(),
                    "antigravity_service_unavailable"
                );
                assert!(is_retryable(&classified), "{service_status}");
            }
        }
        let classified = classify_error(&error_details(&service_error(
            Some(408),
            b"{}",
            Default::default(),
        )));
        assert_eq!(classified.kind, ProviderErrorKind::Overloaded);
        assert_eq!(
            classified.code.as_ref().unwrap(),
            "antigravity_service_unavailable"
        );
        assert!(is_retryable(&classified));
    }

    #[test]
    fn account_verdicts_do_not_guess_suspension_from_eligibility_or_permission() {
        use klaus_agent_core::provider::classify::classify_error;
        use klaus_agent_core::types::errors::ProviderErrorKind;
        for (message, reason, expected) in [
            (
                "This service has been disabled in this account for violation of Terms of Service.",
                None,
                "antigravity_account_disabled",
            ),
            (
                "Account access denied.",
                Some("ACCOUNT_SUSPENDED"),
                "antigravity_account_disabled",
            ),
            (
                "Account access denied.",
                Some("ACCOUNT_RESTRICTED"),
                "antigravity_account_restricted",
            ),
            (
                "You are not eligible to use Antigravity.",
                None,
                "antigravity_account_restricted",
            ),
            (
                "Access has been disabled.",
                None,
                "antigravity_permission_denied",
            ),
            (
                "This model is not available for this account.",
                None,
                "antigravity_model_access_denied",
            ),
            (
                "Credentials expired.",
                Some("ACCESS_TOKEN_EXPIRED"),
                "antigravity_auth_required",
            ),
        ] {
            let details: Vec<Value> = reason
                .into_iter()
                .map(|reason| json!({"reason": reason}))
                .collect();
            let body = serde_json::to_vec(&json!({"error": {
                "code": 403, "status": "PERMISSION_DENIED", "message": message, "details": details
            }}))
            .unwrap();
            for error in [
                service_error(Some(403), &body, Default::default()),
                decode_frame(&body, None).unwrap_err(),
            ] {
                let classified = classify_error(&error_details(&error));
                assert_eq!(classified.code.as_ref().unwrap(), expected, "{message}");
                assert_eq!(
                    classified.kind,
                    if expected == "antigravity_auth_required" {
                        ProviderErrorKind::Auth
                    } else {
                        ProviderErrorKind::InvalidRequest
                    }
                );
            }
        }
    }

    #[test]
    fn service_failures_preserve_bounded_details_and_context_classification() {
        use klaus_agent_core::provider::classify::classify_error;
        use klaus_agent_core::types::errors::ProviderErrorKind;
        let error = service_error(Some(400),br#"{"error":{"status":"INVALID_ARGUMENT","message":"The input token count exceeds the maximum number of tokens allowed 32768. private detail"}}"#,Default::default());
        let classified = classify_error(&error_details(&error));
        assert_eq!(classified.kind, ProviderErrorKind::PromptTooLong);
        assert!(!error.message.contains("private detail"));
        for (status, service_status, message, code, kind) in [
            (
                401,
                "UNAUTHENTICATED",
                "The credentials are invalid.",
                "antigravity_auth_required",
                ProviderErrorKind::Auth,
            ),
            (
                403,
                "PERMISSION_DENIED",
                "The caller does not have permission.",
                "antigravity_permission_denied",
                ProviderErrorKind::InvalidRequest,
            ),
            (
                403,
                "PERMISSION_DENIED",
                "Access to this model requires a subscription.",
                "antigravity_model_access_denied",
                ProviderErrorKind::InvalidRequest,
            ),
            (
                429,
                "RESOURCE_EXHAUSTED",
                "Quota is exhausted.",
                "antigravity_quota_exhausted",
                ProviderErrorKind::Quota,
            ),
            (
                429,
                "RESOURCE_EXHAUSTED",
                "Too many requests, try again in 2 seconds.",
                "antigravity_rate_limited",
                ProviderErrorKind::RateLimit,
            ),
            (
                503,
                "UNAVAILABLE",
                "Model capacity is unavailable.",
                "antigravity_service_unavailable",
                ProviderErrorKind::Overloaded,
            ),
            (
                403,
                "PERMISSION_DENIED",
                "User location is not supported for the API use.",
                "antigravity_region_unsupported",
                ProviderErrorKind::InvalidRequest,
            ),
            (
                400,
                "INVALID_ARGUMENT",
                "Invalid generation configuration.",
                "antigravity_request_rejected",
                ProviderErrorKind::InvalidRequest,
            ),
            (
                400,
                "FAILED_PRECONDITION",
                "User location is not supported for the API use.",
                "antigravity_region_unsupported",
                ProviderErrorKind::InvalidRequest,
            ),
        ] {
            let bytes=serde_json::to_vec(&json!({"error":{"code":status,"status":service_status,"message":message},"privatePrompt":"not-public"})).unwrap();
            for error in [
                service_error(Some(status), &bytes, Default::default()),
                decode_frame(&bytes, None).unwrap_err(),
            ] {
                let classified = classify_error(&error_details(&error));
                assert_eq!(classified.kind, kind);
                assert_eq!(classified.status, Some(status));
                assert_eq!(classified.code.as_ref().unwrap(), code);
                assert_eq!(classified.error_type.as_ref().unwrap(), service_status);
                assert!(classified.message.contains(message));
                assert!(!classified.message.contains("not-public"));
            }
        }
        let iam=json!({"error":{"code":403,"status":"PERMISSION_DENIED","message":"Permission denied on projects/fixture/publishers/google/models/fixture.","details":[{"reason":"IAM_PERMISSION_DENIED"}]}}).to_string();
        let classified = classify_error(&error_details(&service_error(
            Some(403),
            iam.as_bytes(),
            Default::default(),
        )));
        assert_eq!(classified.kind, ProviderErrorKind::InvalidRequest);
        assert_eq!(
            classified.code.as_ref().unwrap(),
            "antigravity_permission_denied"
        );
        let banned=json!({"error":{"code":403,"status":"PERMISSION_DENIED","message":"Your account has been disabled.","details":[{"reason":"ACCOUNT_DISABLED"}]}}).to_string();
        let classified = classify_error(&error_details(&service_error(
            Some(403),
            banned.as_bytes(),
            Default::default(),
        )));
        assert_eq!(classified.kind, ProviderErrorKind::InvalidRequest);
        assert_eq!(
            classified.code.as_ref().unwrap(),
            "antigravity_account_disabled"
        );
        let ineligible=br#"{"error":{"code":403,"status":"PERMISSION_DENIED","message":"Antigravity is not available for this account."}}"#;
        let classified = classify_error(&error_details(&service_error(
            Some(403),
            ineligible,
            Default::default(),
        )));
        assert_eq!(
            classified.code.as_ref().unwrap(),
            "antigravity_account_restricted"
        );
        for details in [
            json!([]),
            json!([{"reason":"RATE_LIMIT_EXCEEDED"},{"retryDelay":"2s"}]),
        ] {
            let body=json!({"error":{"code":429,"status":"RESOURCE_EXHAUSTED","message":"Quota exceeded for requests per minute.","details":details}}).to_string();
            let error = service_error(Some(429), body.as_bytes(), Default::default());
            let classified = classify_error(&error_details(&error));
            assert_eq!(classified.kind, ProviderErrorKind::RateLimit);
            assert_eq!(
                classified.code.as_ref().unwrap(),
                "antigravity_rate_limited"
            );
            assert!(klaus_agent_core::provider::retry::is_retryable(&classified));
        }
        let direct=br#"{"code":403,"status":"PERMISSION_DENIED","message":"Your model access requires a subscription."}"#;
        let classified = classify_error(&error_details(&decode_frame(direct, None).unwrap_err()));
        assert_eq!(classified.kind, ProviderErrorKind::InvalidRequest);
        assert_eq!(classified.status, Some(403));
        let long=json!({"error":{"status":"INVALID_ARGUMENT","message":format!("reason\n{}","x".repeat(9000))}}).to_string();
        let error = service_error(Some(400), long.as_bytes(), Default::default());
        assert!(error.message.len() < 2200);
        assert!(!error.message.contains('\n'));
        let invalid = service_error(
            Some(400),
            b"<html>private response</html>",
            Default::default(),
        );
        assert_eq!(invalid.message, "antigravity_request_rejected");
    }

    #[tokio::test]
    async fn a_pending_error_body_is_cancelled_and_fenced_after_account_replacement() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let root = tempfile::tempdir().unwrap();
        super::super::set_config_root(root.path());
        for cancelled in [true, false] {
            auth::adopt(
                serde_json::from_value(
                    json!({"accessToken":"fixture-token","authMethod":"consumer"}),
                )
                .unwrap(),
            )
            .await
            .unwrap();
            let generation = auth::generation().unwrap();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    let mut chunk = [0u8; 4096];
                    let count = socket.read(&mut chunk).await.unwrap();
                    assert!(count > 0);
                    request.extend_from_slice(&chunk[..count]);
                    assert!(request.len() < 16384);
                }
                socket.write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 4096\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n").await.unwrap();
                std::future::pending::<()>().await;
            });
            let signal = AbortSignal::new();
            let response = http_client()
                .unwrap()
                .post(endpoint)
                .body("{}")
                .send()
                .await
                .unwrap();
            assert_eq!(response.status().as_u16(), 403);
            let read = read_refusal_body(response, Some(&generation), &signal);
            tokio::pin!(read);
            assert!(
                futures::poll!(&mut read).is_pending(),
                "the refusal-body reader must be active before invalidation"
            );
            if cancelled {
                signal.cancel();
            } else {
                auth::adopt(
                    serde_json::from_value(
                        json!({"accessToken":"fixture-replacement","authMethod":"consumer"}),
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
            }
            let error = tokio::time::timeout(std::time::Duration::from_secs(2), read)
                .await
                .expect("a blocked refusal body must not delay cancellation")
                .unwrap_err();
            assert_eq!(
                error.message,
                if cancelled {
                    "antigravity_request_cancelled"
                } else {
                    "antigravity_credential_changed"
                }
            );
            server.abort();
        }
        auth::logout().await.unwrap();
    }

    #[test]
    fn known_inventory_metadata_drives_thinking_and_signed_tool_replay() {
        let details =
            super::super::catalog::model_details(&json!({"models":{}}), "claude-sonnet-5-5-max")
                .unwrap();
        let mut request:RuntimeJson=runtime_codec::decode_text(&RuntimeString::from(json!({
            "contents":[{"role":"model","parts":[
                {"thought":true,"text":"Reasoning"},
                {"thought":true,"text":"","thoughtSignature":"opaque-signature"},
                {"functionCall":{"name":"fixture","args":{}},"thoughtSignature":"skip_thought_signature_validator"}
            ]}],
            "generationConfig":{"thinkingConfig":{"includeThoughts":true,"thinkingBudget":8192}},
            "toolConfig":{"functionCallingConfig":{"mode":"AUTO"}}
        }).to_string())).unwrap();
        adapt_model_request(&mut request, &details).unwrap();
        let json: Value =
            serde_json::from_slice(&runtime_codec::encode_bytes(&request).unwrap()).unwrap();
        assert_eq!(
            json["generationConfig"]["thinkingConfig"]["thinkingLevel"],
            "MAX"
        );
        assert!(json["generationConfig"]["thinkingConfig"]
            .get("thinkingBudget")
            .is_none());
        assert!(json.get("toolConfig").is_none());
        let parts = json["contents"][0]["parts"].as_array().unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0]["text"], "Reasoning");
        assert_eq!(parts[0]["thoughtSignature"], "opaque-signature");
        assert!(parts[1].get("thoughtSignature").is_none());
        let live =
            json!({"models":{"claude-sonnet-5-5-max":{"thinkingLevel":1,"supportsThinking":true}}});
        assert_eq!(
            super::super::catalog::model_details(&live, "claude-sonnet-5-5-max").unwrap(),
            live["models"]["claude-sonnet-5-5-max"]
        );
        assert!(super::super::catalog::model_details(&live, "unknown-model").is_none());
    }

    #[test]
    fn consumer_endpoints_are_bound_to_the_cloud_and_oauth_services() {
        assert!(validate_endpoint("https://daily-cloudcode-pa.googleapis.com").is_ok());
        assert!(validate_endpoint("https://oauth2.googleapis.com/token").is_ok());
        assert!(validate_endpoint("http://127.0.0.1:1234").is_ok());
        assert!(validate_endpoint("https://cloudcode-pa.googleapis.com").is_err());
        assert!(validate_endpoint("https://example.com").is_err());
        assert!(validate_endpoint("https://daily-cloudcode-pa.googleapis.com@evil.test").is_err());
    }

    #[test]
    fn fixed_level_models_do_not_receive_stale_numeric_budgets() {
        let base = json!({"contents":[],"generationConfig":{"thinkingConfig":{"includeThoughts":true,"thinkingBudget":8192}}});
        for (details, expected) in [
            (
                json!({"thinkingLevel":3,"thinkingBudget":0,"supportsAdaptiveThinking":false}),
                "HIGH",
            ),
            (
                json!({"thinkingLevel":200,"thinkingBudget":0,"supportsAdaptiveThinking":true}),
                "MAX",
            ),
        ] {
            let mut request: RuntimeJson =
                runtime_codec::decode_text(&RuntimeString::from(base.to_string())).unwrap();
            adapt_model_request(&mut request, &details).unwrap();
            let config = request
                .get("generationConfig")
                .unwrap()
                .get("thinkingConfig")
                .unwrap();
            assert!(config.get("thinkingBudget").is_none());
            assert_eq!(
                config.get("thinkingLevel").unwrap().as_string().unwrap(),
                expected
            );
        }
    }

    #[test]
    fn anthropic_replay_removes_only_the_synthetic_function_call_signature() {
        let base = json!({"contents":[{"role":"model","parts":[
            {"thought":true,"text":"reason","thoughtSignature":"opaque-thought"},
            {"functionCall":{"id":"call","name":"read","args":{}},"thoughtSignature":"skip_thought_signature_validator"},
            {"functionCall":{"id":"signed","name":"read","args":{}},"thoughtSignature":"opaque-call"},
            {"text":"","thoughtSignature":"opaque-text"}
        ]}]});
        for provider in [json!("MODEL_PROVIDER_ANTHROPIC"), json!(3)] {
            let mut request: RuntimeJson =
                runtime_codec::decode_text(&RuntimeString::from(base.to_string())).unwrap();
            adapt_model_request(&mut request, &json!({"modelProvider":provider})).unwrap();
            let contents = request.get("contents").unwrap().as_array().unwrap();
            let parts = contents[0].get("parts").unwrap().as_array().unwrap();
            assert_eq!(
                parts[0]
                    .get("thoughtSignature")
                    .unwrap()
                    .as_string()
                    .unwrap(),
                "opaque-thought"
            );
            assert!(parts[1].get("thoughtSignature").is_none());
            assert_eq!(
                parts[2]
                    .get("thoughtSignature")
                    .unwrap()
                    .as_string()
                    .unwrap(),
                "opaque-call"
            );
            assert_eq!(
                parts[3]
                    .get("thoughtSignature")
                    .unwrap()
                    .as_string()
                    .unwrap(),
                "opaque-text"
            );
        }
        let mut google: RuntimeJson =
            runtime_codec::decode_text(&RuntimeString::from(base.to_string())).unwrap();
        adapt_model_request(
            &mut google,
            &json!({"modelProvider":"MODEL_PROVIDER_GOOGLE"}),
        )
        .unwrap();
        let original: RuntimeJson =
            runtime_codec::decode_text(&RuntimeString::from(base.to_string())).unwrap();
        assert_eq!(google, original);
    }

    #[test]
    fn anthropic_thought_trailers_complete_only_their_unsigned_text_run() {
        let base = json!({"contents":[{"role":"model","parts":[
            {"text":"visible prefix"},
            {"thought":true,"text":"signed thought","thoughtSignature":"previous-signature"},
            {"thought":true,"text":"New"},
            {"thought":true,"text":" reasoning"},
            {"thought":true,"text":"","thoughtSignature":"new-signature/+=="},
            {"functionCall":{"id":"call","name":"read","args":{}},"thoughtSignature":"skip_thought_signature_validator"},
            {"text":"","thoughtSignature":"nonthought-signature"}
        ]}]});
        let mut request: RuntimeJson =
            runtime_codec::decode_text(&RuntimeString::from(base.to_string())).unwrap();
        adapt_model_request(
            &mut request,
            &json!({"modelProvider":"MODEL_PROVIDER_ANTHROPIC"}),
        )
        .unwrap();
        let contents = request.get("contents").unwrap().as_array().unwrap();
        let parts = contents[0].get("parts").unwrap().as_array().unwrap();
        assert_eq!(parts.len(), 4);
        assert_eq!(
            parts[0].get("text").unwrap().as_string().unwrap(),
            "visible prefix"
        );
        assert_eq!(
            parts[1].get("text").unwrap().as_string().unwrap(),
            "signed thoughtNew reasoning"
        );
        assert_eq!(
            parts[1]
                .get("thoughtSignature")
                .unwrap()
                .as_string()
                .unwrap(),
            "previous-signature"
        );
        assert!(parts[2].get("functionCall").is_some());
        assert!(parts[2].get("thoughtSignature").is_none());
        assert_eq!(
            parts[3]
                .get("thoughtSignature")
                .unwrap()
                .as_string()
                .unwrap(),
            "nonthought-signature"
        );
        let mut google: RuntimeJson =
            runtime_codec::decode_text(&RuntimeString::from(base.to_string())).unwrap();
        adapt_model_request(
            &mut google,
            &json!({"modelProvider":"MODEL_PROVIDER_GOOGLE"}),
        )
        .unwrap();
        assert_eq!(
            google,
            runtime_codec::decode_text::<RuntimeJson>(&RuntimeString::from(base.to_string()))
                .unwrap()
        );
    }

    #[test]
    fn anthropic_parallel_function_responses_precede_inline_data_attachments() {
        let base = json!({"contents":[{"role":"user","parts":[
            {"functionResponse":{"id":"call_1","name":"read","response":{"output":""}}},
            {"inlineData":{"mimeType":"image/png","data":"aW1nMQ=="}},
            {"functionResponse":{"id":"call_2","name":"read","response":{"output":""}}},
            {"inlineData":{"mimeType":"image/png","data":"aW1nMg=="}}
        ]}]});
        let mut request: RuntimeJson =
            runtime_codec::decode_text(&RuntimeString::from(base.to_string())).unwrap();
        adapt_model_request(
            &mut request,
            &json!({"modelProvider":"MODEL_PROVIDER_ANTHROPIC"}),
        )
        .unwrap();
        let contents = request.get("contents").unwrap().as_array().unwrap();
        let parts = contents[0].get("parts").unwrap().as_array().unwrap();
        assert_eq!(parts.len(), 4);
        assert_eq!(
            parts[0]
                .get("functionResponse")
                .unwrap()
                .get("id")
                .unwrap()
                .as_string()
                .unwrap(),
            "call_1"
        );
        assert_eq!(
            parts[1]
                .get("functionResponse")
                .unwrap()
                .get("id")
                .unwrap()
                .as_string()
                .unwrap(),
            "call_2"
        );
        assert_eq!(
            parts[2]
                .get("inlineData")
                .unwrap()
                .get("data")
                .unwrap()
                .as_string()
                .unwrap(),
            "aW1nMQ=="
        );
        assert_eq!(
            parts[3]
                .get("inlineData")
                .unwrap()
                .get("data")
                .unwrap()
                .as_string()
                .unwrap(),
            "aW1nMg=="
        );
    }

    #[test]
    fn fixed_budget_variants_and_tool_choice_limits_are_explicit() {
        let base = json!({"contents":[],"generationConfig":{"thinkingConfig":{"includeThoughts":true,"thinkingLevel":"HIGH"}},"toolConfig":{"functionCallingConfig":{"mode":"AUTO"}}});
        let mut request: RuntimeJson =
            runtime_codec::decode_text(&RuntimeString::from(base.to_string())).unwrap();
        adapt_model_request(&mut request, &json!({"thinkingBudget":4000})).unwrap();
        let config = request
            .get("generationConfig")
            .unwrap()
            .get("thinkingConfig")
            .unwrap();
        assert_eq!(config.get("thinkingBudget").unwrap().as_i64(), Some(4000));
        assert!(config.get("thinkingLevel").is_none());
        adapt_model_request(&mut request, &json!({"supportsToolChoice":false})).unwrap();
        assert!(request.get("toolConfig").is_none());
        let mut forced: RuntimeJson = runtime_codec::decode_text(&RuntimeString::from(
            json!({"toolConfig":{"functionCallingConfig":{"mode":"ANY"}}}).to_string(),
        ))
        .unwrap();
        assert_eq!(
            adapt_model_request(&mut forced, &json!({"supportsToolChoice":false}))
                .unwrap_err()
                .message,
            "antigravity_tool_choice_unsupported"
        );
    }

    #[test]
    fn direct_envelope_has_camel_case_identity_without_paid_credit_opt_in() {
        let body = envelope("project", "fixture-model", json!({"contents":[]}), "agent");
        assert_eq!(body["requestType"], "agent");
        assert!(body["requestId"].as_str().is_some());
        assert!(body["userPromptId"].as_str().is_some());
        assert!(body.get("enabledCreditTypes").is_none());
        assert!(body.get("session_id").is_none());
    }

    #[test]
    fn wrapped_chunks_preserve_thought_signature_and_token_usage() {
        let chunk = decode_frame(br#"{"response":{"candidates":[{"content":{"parts":[{"functionCall":{"name":"echo","args":{"x":1}},"thoughtSignature":"signature"}]}}],"usageMetadata":{"promptTokenCount":4,"candidatesTokenCount":2}}}"#,None).unwrap();
        let candidates = chunk.candidates.unwrap();
        let part = &candidates[0].content.as_ref().unwrap().parts[0];
        assert_eq!(part.thought_signature.as_ref().unwrap(), "signature");
        assert_eq!(chunk.usage_metadata.unwrap().prompt_token_count, Some(4));
    }

    #[test]
    fn continuation_tracker_suppresses_intermediate_leg_finish_reasons_and_accumulates_usage() {
        let mut tracker = StreamContinuationTracker::new(Some(10_000));
        let leg1 = decode_raw_frame(
            br#"{"response":{"modelVersion":"gemini-4-argon-medium","candidates":[{"content":{"role":"model","parts":[{"text":"part one "}]},"finishReason":"FINISH_REASON_CONTINUATION","continuationToken":"tok-leg-1"}],"usageMetadata":{"promptTokenCount":1000,"cachedContentTokenCount":800,"thoughtsTokenCount":300,"candidatesTokenCount":200,"totalTokenCount":1500}}}"#,
            None,
        )
        .unwrap();
        let mapped1 = tracker.record_frame(leg1);
        assert!(
            mapped1.candidates.as_ref().unwrap()[0]
                .finish_reason
                .is_none(),
            "intermediate continuation finishReason must be suppressed"
        );
        assert!(matches!(
            tracker.evaluate_leg_end(None),
            LegEndAction::Continue {
                is_mid_stream_retry: false,
                ..
            }
        ));
        assert_eq!(tracker.continuation_count, 1);
        assert_eq!(
            tracker.latest_continuation_token.as_ref().unwrap(),
            "tok-leg-1"
        );

        let leg2 = decode_raw_frame(
            br#"{"response":{"modelVersion":"gemini-4-argon-medium","candidates":[{"content":{"role":"model","parts":[{"text":"part two"}]},"finishReason":"STOP","continuationToken":"tok-leg-2"}],"usageMetadata":{"promptTokenCount":1500,"cachedContentTokenCount":1200,"thoughtsTokenCount":100,"candidatesTokenCount":400,"totalTokenCount":2000}}}"#,
            None,
        )
        .unwrap();
        let mapped2 = tracker.record_frame(leg2);
        assert_eq!(
            mapped2.candidates.as_ref().unwrap()[0]
                .finish_reason
                .as_ref()
                .unwrap(),
            "STOP"
        );
        let usage = mapped2.usage_metadata.unwrap();
        assert_eq!(usage.prompt_token_count, Some(1000));
        assert_eq!(usage.cached_content_token_count, Some(800));
        assert_eq!(usage.thoughts_token_count, Some(400));
        assert_eq!(usage.candidates_token_count, Some(600));
        assert_eq!(usage.total_token_count, Some(2000));
        assert!(matches!(
            tracker.evaluate_leg_end(None),
            LegEndAction::FinishCleanly
        ));
    }

    #[test]
    fn continuation_tracker_resumes_mid_stream_errors_only_when_checkpoint_is_current() {
        let mut tracker = StreamContinuationTracker::new(None);
        let checkpointed = decode_raw_frame(
            br#"{"response":{"modelVersion":"gemini-4-argon-medium","candidates":[{"content":{"role":"model","parts":[{"text":"checkpointed"}]},"continuationToken":"tok-1"}],"usageMetadata":{"promptTokenCount":500,"cachedContentTokenCount":400,"thoughtsTokenCount":50,"candidatesTokenCount":100}}}"#,
            None,
        )
        .unwrap();
        let _ = tracker.record_frame(checkpointed);
        assert!(matches!(
            tracker.evaluate_leg_end(Some(api_error("antigravity_stream_interrupted"))),
            LegEndAction::Continue {
                is_mid_stream_retry: true,
                ..
            }
        ));
        assert_eq!(tracker.mid_stream_retry_count, 1);

        // An empty 200 EOF on a mid-stream resume leg must restore the error and retry.
        assert!(matches!(
            tracker.evaluate_leg_end(None),
            LegEndAction::Continue {
                is_mid_stream_retry: true,
                ..
            }
        ));
        assert_eq!(tracker.mid_stream_retry_count, 2);

        // Uncheckpointed content after a checkpoint prevents mid-stream resume so
        // uncheckpointed text is never duplicated.
        let uncheckpointed = decode_raw_frame(
            br#"{"response":{"candidates":[{"content":{"role":"model","parts":[{"text":"tail without token"}]}}]}}"#,
            None,
        )
        .unwrap();
        let _ = tracker.record_frame(uncheckpointed);
        assert!(matches!(
            tracker.evaluate_leg_end(Some(api_error("antigravity_stream_interrupted"))),
            LegEndAction::Fail(_)
        ));
    }

    #[test]
    fn continuation_tracker_synthesizes_max_tokens_when_continuation_budget_is_exhausted() {
        let mut tracker = StreamContinuationTracker::new(None);
        let leg1 = decode_raw_frame(
            br#"{"response":{"modelVersion":"gemini-4-argon-medium","candidates":[{"content":{"role":"model","parts":[{"text":"hello"}]},"finishReason":18,"continuationToken":"tok-1"}],"usageMetadata":{"promptTokenCount":100,"cachedContentTokenCount":20,"thoughtsTokenCount":10,"candidatesTokenCount":30}}}"#,
            None,
        )
        .unwrap();
        let _ = tracker.record_frame(leg1);
        assert!(matches!(
            tracker.evaluate_leg_end(None),
            LegEndAction::Continue {
                is_mid_stream_retry: false,
                ..
            }
        ));
        let budget_err = service_error(
            Some(400),
            br#"{"error":{"code":400,"status":"INVALID_ARGUMENT","message":"Request contains more than the max tokens limit allowed."}}"#,
            Default::default(),
        );
        assert!(matches!(
            tracker.evaluate_dial_error(budget_err),
            DialErrorAction::EmitSyntheticMaxTokens
        ));
        let synthetic = tracker.new_synthetic_max_tokens_response();
        assert_eq!(
            synthetic.candidates.as_ref().unwrap()[0]
                .finish_reason
                .as_ref()
                .unwrap(),
            "MAX_TOKENS"
        );
        assert_eq!(
            synthetic.usage_metadata.unwrap().candidates_token_count,
            Some(30)
        );
    }
}
