//! Direct cloud requests and bounded streamed response decoding.

use super::auth;
use futures::StreamExt;
use klaus_agent_core::provider::classify::RawProviderError;
use klaus_agent_core::provider::gemini::{
    generate_content_request, GeminiApiError, GeminiChunkStream, GeminiClient,
    GenerateContentParameters, GenerateContentResponse,
};
use klaus_agent_core::runtime_values::{runtime_codec, runtime_json, RuntimeJson, RuntimeString};
use klaus_agent_core::types::abort::AbortSignal;
use klaus_agent_core::types::provider::AuthHeaders;
use serde_json::{json, Value};

const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;
const MAX_SSE_FRAME_BYTES: usize = 8 * 1024 * 1024;

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

const ERROR_TAGS: [&str; 9] = [
    "antigravity_model_access_denied",
    "antigravity_model_not_offered",
    "antigravity_model_unlisted",
    "antigravity_quota_exhausted",
    "antigravity_request_rejected",
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
            "IAM_PERMISSION_DENIED"
                | "ACCESS_TOKEN_EXPIRED"
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
            )
        );
    let tag = if context_overflow {
        "model_context_window_exceeded"
    } else if status == Some(401) || service_code == Some("UNAUTHENTICATED") || auth_reason {
        "antigravity_auth_required"
    } else if explicit_quota {
        "antigravity_quota_exhausted"
    } else if (status == Some(403) || service_code == Some("PERMISSION_DENIED")) && model_access {
        "antigravity_model_access_denied"
    } else if status == Some(403) || service_code == Some("PERMISSION_DENIED") {
        "antigravity_auth_required"
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

fn decode_frame(
    bytes: &[u8],
    generation: Option<&str>,
) -> Result<GenerateContentResponse, GeminiApiError> {
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
    let inner = payload.get("response").unwrap_or(&payload);
    runtime_codec::decode_value(inner).map_err(|_| api_error("antigravity_stream_invalid"))
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
        let build_body = |project: &str| -> Result<Vec<u8>, GeminiApiError> {
            let mut request = generate_content_request(&params);
            if let Some(details) = super::model_details(&self.generation, &model) {
                adapt_model_request(&mut request, &details)?;
            }
            let mut body: RuntimeJson = runtime_codec::decode_text(&RuntimeString::from(
                envelope(project, &model, Value::Null, "agent").to_string(),
            ))
            .map_err(|_| api_error("antigravity_request_invalid"))?;
            body.as_object_mut()
                .ok_or_else(|| api_error("antigravity_request_invalid"))?
                .insert("request", request);
            let mut bytes = Vec::new();
            runtime_json::write_to(&body, &mut bytes)
                .map_err(|_| api_error("antigravity_request_invalid"))?;
            Ok(bytes)
        };
        let project = super::project(Some(&self.generation), &signal)
            .await
            .map_err(api_error)?;
        let response = match send(
            "streamGenerateContent",
            build_body(&project)?,
            Some(&self.generation),
            &signal,
            extra_headers.clone(),
        )
        .await
        {
            Ok(response) => response,
            // A NOT_FOUND refusal can mean the cached project no longer resolves
            // rather than the request being wrong; resolve a fresh project and
            // replay the request once before trusting the refusal.
            Err(first) if is_not_found(&first) => {
                let healed = super::heal_project(&self.generation, &signal)
                    .await
                    .map_err(api_error)?;
                match send(
                    "streamGenerateContent",
                    build_body(&healed)?,
                    Some(&self.generation),
                    &signal,
                    extra_headers,
                )
                .await
                {
                    Ok(response) => response,
                    Err(second) if is_not_found(&second) => {
                        return Err(not_offered_or_original(&self.generation, &model, second).await)
                    }
                    Err(second) => return Err(second),
                }
            }
            Err(first) => return Err(first),
        };
        let generation = self.generation.clone();
        Ok(Box::pin(async_stream::stream! {
            let mut stream = response.bytes_stream();
            let mut buffered = Vec::new();
            let mut data = Vec::new();
            let mut terminal = false;
            let mut total_bytes=0usize;
            loop {
                let chunk = tokio::select! {
                    _ = signal.cancelled() => return,
                    _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                        if !auth::is_current(&generation) {yield Err(api_error("antigravity_credential_changed"));return;}
                        continue;
                    },
                    chunk = stream.next() => chunk,
                };
                let Some(chunk) = chunk else { break; };
                if !auth::is_current(&generation) {
                    yield Err(api_error("antigravity_credential_changed"));
                    return;
                }
                let chunk = match chunk {
                    Ok(chunk) => chunk,
                    Err(_) => { yield Err(api_error("antigravity_stream_interrupted")); return; }
                };
                total_bytes=total_bytes.saturating_add(chunk.len());
                if total_bytes>MAX_BODY_BYTES {yield Err(api_error("antigravity_stream_response_too_large"));return;}
                if buffered.len().saturating_add(data.len()).saturating_add(chunk.len()) > MAX_SSE_FRAME_BYTES {
                    yield Err(api_error("antigravity_stream_frame_too_large"));
                    return;
                }
                buffered.extend_from_slice(&chunk);
                while let Some(index) = buffered.iter().position(|byte| *byte == b'\n') {
                    let mut line: Vec<u8> = buffered.drain(..=index).collect();
                    line.pop();
                    if line.last() == Some(&b'\r') { line.pop(); }
                    if line.is_empty() {
                        if !data.is_empty() {
                            if data.as_slice() != b"[DONE]" {
                                if !auth::is_current(&generation) {yield Err(api_error("antigravity_credential_changed"));return;}
                                match decode_frame(&data,Some(&generation)) {
                                    Ok(chunk)=>{
                                        terminal |= chunk.candidates.iter().flatten().any(|candidate| candidate.finish_reason.as_ref().is_some_and(|reason|!reason.is_empty() && reason!="FINISH_REASON_UNSPECIFIED"));
                                        yield Ok(chunk);
                                    },
                                    Err(error)=>{yield Err(error);return;},
                                }
                            }
                            data.clear();
                        }
                    } else if let Some(payload) = line.strip_prefix(b"data:") {
                        let payload = payload.strip_prefix(b" ").unwrap_or(payload);
                        if !data.is_empty() { data.push(b'\n'); }
                        data.extend_from_slice(payload);
                    }
                }
            }
            if let Some(payload) = buffered.strip_prefix(b"data:") {
                if !data.is_empty() { data.push(b'\n'); }
                data.extend_from_slice(payload.strip_prefix(b" ").unwrap_or(payload));
            }
            if !data.is_empty() && data.as_slice() != b"[DONE]" {
                if !auth::is_current(&generation) {yield Err(api_error("antigravity_credential_changed"));return;}
                match decode_frame(&data,Some(&generation)) {
                    Ok(chunk)=>{
                        terminal |= chunk.candidates.iter().flatten().any(|candidate| candidate.finish_reason.as_ref().is_some_and(|reason|!reason.is_empty() && reason!="FINISH_REASON_UNSPECIFIED"));
                        yield Ok(chunk);
                    },
                    Err(error)=>{yield Err(error);return;},
                }
            }
            if !auth::is_current(&generation) {yield Err(api_error("antigravity_credential_changed"));return;}
            if !terminal {yield Err(api_error("antigravity_stream_incomplete"));}
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
                "antigravity_auth_required",
                ProviderErrorKind::Auth,
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
                400,
                "INVALID_ARGUMENT",
                "Invalid generation configuration.",
                "antigravity_request_rejected",
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
        assert_eq!(classified.kind, ProviderErrorKind::Auth);
        assert_eq!(
            classified.code.as_ref().unwrap(),
            "antigravity_auth_required"
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
}
