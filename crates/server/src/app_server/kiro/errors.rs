//! Bounded service diagnostics with explicit source-specific refusal categories.
use futures::StreamExt;
use serde_json::{json, Value};
pub(super) fn native_code(value: &Value) -> String {
    if value.get("message").and_then(Value::as_str) == Some("Invalid token")
        || value.pointer("/error/message").and_then(Value::as_str)
            == Some("The bearer token included in the request is invalid.")
    {
        return "INVALID_TOKEN".into();
    }
    let code = value
        .get("reason")
        .and_then(Value::as_str)
        .or_else(|| value.pointer("/error/reason").and_then(Value::as_str))
        .or_else(|| value.get("code").and_then(Value::as_str))
        .or_else(|| value.get("__type").and_then(Value::as_str))
        .or_else(|| value.pointer("/error/code").and_then(Value::as_str))
        .or_else(|| value.get("type").and_then(Value::as_str))
        .unwrap_or("request_rejected");
    code.rsplit('#')
        .next()
        .unwrap_or(code)
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | ':'))
        .take(128)
        .collect()
}
pub(super) fn classify(status: u16, value: &Value) -> (&'static str, String) {
    let native = native_code(value);
    let reason = native
        .strip_prefix("kiro:")
        .unwrap_or(&native)
        .to_ascii_uppercase();
    let error_type = value
        .get("type")
        .and_then(Value::as_str)
        .or_else(|| value.pointer("/error/type").and_then(Value::as_str))
        .unwrap_or_default()
        .to_ascii_lowercase();
    let code = if matches!(
        error_type.as_str(),
        "too_many_requests" | "throttlingexception" | "rate_limit_error"
    ) || matches!(
        reason.as_str(),
        "THROTTLINGEXCEPTION" | "RATE_LIMIT_EXCEEDED" | "TOO_MANY_REQUESTS" | "RATE_LIMITED"
    ) {
        "kiro_rate_limited"
    } else if matches!(error_type.as_str(), "insufficient_quota" | "quota_exceeded")
        || matches!(
            reason.as_str(),
            "INSUFFICIENT_CREDITS"
                | "CREDIT_LIMIT_REACHED"
                | "MONTHLY_REQUEST_COUNT"
                | "MONTHLY_USAGE_LIMIT_EXCEEDED"
                | "QUOTA_EXCEEDED"
                | "USAGE_LIMIT_EXCEEDED"
                | "INSUFFICIENT_QUOTA"
                | "CREDIT_BALANCE_EXHAUSTED"
                | "CONVERSATION_LIMIT_EXCEEDED"
                | "OVERAGE_REQUEST_LIMIT_EXCEEDED"
        )
    {
        "kiro_quota_exhausted"
    } else if status == 429 {
        "kiro_rate_limited"
    } else if reason == "MODEL_REQUIRES_USER_OPT_IN" {
        "kiro_model_consent_required"
    } else if reason == "UNSUPPORTED_PARAMETER" {
        "kiro_request_parameter_unsupported"
    } else if reason == "MODEL_NOT_FOUND" {
        "kiro_model_not_found"
    } else if matches!(
        reason.as_str(),
        "MODEL_ACCESS_DENIED"
            | "MODEL_NOT_SUBSCRIBED"
            | "SUBSCRIPTION_REQUIRED"
            | "MODEL_SUBSCRIPTION_REQUIRED"
            | "MODEL_NOT_ENTITLED"
    ) {
        "kiro_model_access_denied"
    } else if matches!(
        reason.as_str(),
        "CONTEXT_LENGTH_EXCEEDED"
            | "CONTEXT_WINDOW_EXCEEDED"
            | "MODEL_CONTEXT_WINDOW_EXCEEDED"
            | "INPUT_TOO_LONG"
    ) {
        "kiro_context_length_exceeded"
    } else if matches!(
        reason.as_str(),
        "INVALID_TOKEN"
            | "EXPIRED_TOKEN"
            | "EXPIREDTOKENEXCEPTION"
            | "INVALIDBEARERTOKEN"
            | "INVALIDBEARERTOKENEXCEPTION"
    ) {
        "kiro_login_expired"
    } else {
        "kiro_request_rejected"
    };
    (code, native)
}
pub(super) async fn response(
    response: reqwest::Response,
) -> (hyper::StatusCode, Value, Option<String>, Option<String>) {
    let status = response.status();
    let request_id = response
        .headers()
        .get("x-amzn-requestid")
        .and_then(|v| v.to_str().ok())
        .filter(|v| v.len() <= 256)
        .map(str::to_owned);
    let retry = response
        .headers()
        .get("x-amzn-kiro-ratelimit-retry-after")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v <= 86400000)
        .map(|ms| format!("{}.{:03}", ms / 1000, ms % 1000))
        .or_else(|| {
            response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .filter(|v| v.len() <= 128)
                .map(str::to_owned)
        });
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(Ok(chunk)) = stream.next().await {
        if bytes.len().saturating_add(chunk.len()) > 256 * 1024 {
            break;
        }
        bytes.extend_from_slice(&chunk);
    }
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    let (code, native) = classify(status.as_u16(), &value);
    if code == "kiro_model_not_found" {
        super::catalog::forget();
    }
    let detail = diagnostic(code, &native, &value);
    (
        status,
        json!({"error":{"message":detail,"code":code,"type":native}}),
        retry,
        request_id,
    )
}
pub(super) fn diagnostic(code: &str, native: &str, value: &Value) -> String {
    // Preserve a parameter identifier, never an arbitrary echoed request value.
    let param = value
        .pointer("/error/param")
        .and_then(Value::as_str)
        .filter(|param| {
            !param.is_empty()
                && param.len() <= 128
                && param
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '[' | ']'))
        });
    match param {
        Some(param) => format!("{code} ({native}; parameter={param})"),
        None => format!("{code} ({native})"),
    }
}
