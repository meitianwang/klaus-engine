//! Source-owned service errors retain transport diagnostics and retry meaning.

use klaus_agent_core::types::errors::{ProviderError, ProviderErrorKind};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct ServiceError {
    pub category: String,
    pub native_code: Option<String>,
    pub message: String,
    pub status: Option<u16>,
    pub request_id: Option<String>,
    pub retry_after_ms: Option<u64>,
    pub required_plan: Option<u64>,
}

fn safe_message(value: &str) -> String {
    if value.len() > 2048
        || value.contains("eyJ")
        || value.to_ascii_lowercase().contains("authorization")
        || value.contains("Bearer ")
        || value.contains("Cloud-IDE-JWT ")
    {
        return "The service rejected the request.".into();
    }
    value
        .chars()
        .filter(|character| !character.is_control())
        .take(512)
        .collect()
}

impl ServiceError {
    pub fn from_native(
        value: &Value,
        status: Option<u16>,
        headers: Option<&reqwest::header::HeaderMap>,
    ) -> Self {
        let native_code = value
            .get("code")
            .or_else(|| value.get("error_code"))
            .or_else(|| value.pointer("/error/code"))
            .and_then(|code| {
                code.as_str()
                    .map(str::to_owned)
                    .or_else(|| code.as_i64().map(|code| code.to_string()))
            })
            .filter(|code| {
                code.len() <= 64
                    && code.chars().all(|character| {
                        character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.')
                    })
            });
        let code = native_code.as_deref().unwrap_or_default();
        let category = match code {
            "1001" | "invalid_token" | "token_expired" | "authentication_error" => {
                "trae_auth_required"
            }
            "1005"
            | "4400"
            | "model_access_denied"
            | "model_not_allowed"
            | "entitlement_required" => "trae_model_access_denied",
            "977"
            | "4040"
            | "4011"
            | "4021"
            | "insufficient_quota"
            | "quota_exhausted"
            | "usage_limit_reached"
            | "credit_balance_exhausted" => "trae_quota_exhausted",
            "4006" | "4022" | "4000010" | "context_length_exceeded" => "context_length_exceeded",
            "1009" | "request_too_large" => "request_too_large",
            "3004" | "4007" | "5003" | "rate_limit_exceeded" | "rate_limit_error" => {
                "trae_rate_limited"
            }
            // The service declares these model-call failures as transient and
            // retry-later; SSE error frames carry no HTTP status, so the retry
            // decision has to come from the source category alone.
            "3003" | "4053" | "4054" | "978" | "991" | "992" | "993" => "trae_service_unavailable",
            _ => match status {
                Some(401) => "trae_auth_required",
                Some(429) => "trae_rate_limited",
                Some(413) => "request_too_large",
                Some(500..=599) => "trae_service_unavailable",
                _ => "trae_request_rejected",
            },
        }
        .to_owned();
        let message = value
            .get("message")
            .or_else(|| value.pointer("/error/message"))
            .or_else(|| value.get("error"))
            .and_then(Value::as_str)
            .filter(|message| !message.trim().is_empty())
            .map(safe_message)
            .unwrap_or_else(|| "The service rejected the request.".into());
        let request_id = headers
            .and_then(|headers| {
                headers
                    .get("x-tt-logid")
                    .or_else(|| headers.get("request-id"))
                    .or_else(|| headers.get("x-request-id"))
            })
            .and_then(|value| value.to_str().ok())
            .filter(|value| {
                !value.is_empty()
                    && value.len() <= 128
                    && value.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')
                    })
            })
            .map(str::to_owned);
        let retry_after_ms = headers
            .and_then(|headers| headers.get("retry-after"))
            .and_then(|value| value.to_str().ok())
            .and_then(|value| {
                value
                    .trim()
                    .parse::<u64>()
                    .ok()
                    .and_then(|seconds| seconds.checked_mul(1000))
                    .or_else(|| {
                        chrono::DateTime::parse_from_rfc2822(value)
                            .ok()
                            .map(|time| {
                                time.timestamp_millis()
                                    .saturating_sub(super::auth::now())
                                    .max(0) as u64
                            })
                    })
            });
        let extra = value
            .get("extra")
            .and_then(Value::as_str)
            .filter(|extra| extra.len() <= 4096)
            .and_then(|extra| serde_json::from_str::<Value>(extra).ok());
        let required_plan = value
            .pointer("/extra/plan")
            .or_else(|| extra.as_ref().and_then(|extra| extra.get("plan")))
            .and_then(Value::as_u64)
            .filter(|plan| *plan <= 1000);
        Self {
            category,
            native_code,
            message,
            status,
            request_id,
            retry_after_ms,
            required_plan,
        }
    }
    pub fn provider_error(self) -> ProviderError {
        let kind = match self.category.as_str() {
            "trae_auth_required" => ProviderErrorKind::Auth,
            "trae_quota_exhausted" => ProviderErrorKind::Quota,
            "trae_rate_limited" => ProviderErrorKind::RateLimit,
            "trae_service_unavailable" => ProviderErrorKind::Overloaded,
            "context_length_exceeded" => ProviderErrorKind::PromptTooLong,
            "request_too_large" => ProviderErrorKind::RequestTooLarge,
            _ => ProviderErrorKind::InvalidRequest,
        };
        let plan = self
            .required_plan
            .map(|plan| format!(" (required plan {plan})"))
            .unwrap_or_default();
        ProviderError {
            kind,
            message: format!(
                "{}: {}{}{}",
                self.category,
                self.message,
                self.native_code
                    .as_deref()
                    .map(|code| format!(" (service code {code})"))
                    .unwrap_or_default(),
                plan
            )
            .into(),
            status: self.status,
            code: Some(self.category.into()),
            error_type: self.native_code.map(Into::into),
            request_id: self.request_id.map(Into::into),
            media_origins: None,
            media_repair_complete: false,
            retry_after_ms: self.retry_after_ms,
        }
    }
    /// A local stream interruption: the upstream event stream ended or stalled
    /// without a terminal frame. Re-establishing the request is safe, and the
    /// local code stays in `native_code` for diagnostics while the category
    /// reuses the existing transient-service wording.
    pub fn stream_interrupted(message: &str) -> Self {
        Self {
            category: "trae_service_unavailable".into(),
            native_code: Some(message.into()),
            message: "The model stream was interrupted before completion.".into(),
            status: None,
            request_id: None,
            retry_after_ms: None,
            required_plan: None,
        }
    }
    pub fn envelope(self, model: &str, account: &str) -> Value {
        serde_json::json!({"kind":"klaus.trae.error.v1","source":"trae","model":model,"account":account,"error":self})
    }
}

pub(super) async fn from_response(response: reqwest::Response) -> ServiceError {
    use futures::StreamExt;
    let status = Some(response.status().as_u16());
    let headers = response.headers().clone();
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(Ok(chunk)) = stream.next().await {
        if bytes.len().saturating_add(chunk.len()) > 64 * 1024 {
            break;
        }
        bytes.extend_from_slice(&chunk);
    }
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    ServiceError::from_native(&value, status, Some(&headers))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn account_limits_context_and_transient_rate_errors_keep_distinct_kinds() {
        for (body, status, kind, code) in [
            (
                json!({"code":1005,"message":"This mode is exclusive to Pro users."}),
                None,
                ProviderErrorKind::InvalidRequest,
                "trae_model_access_denied",
            ),
            (
                json!({"code":"insufficient_quota"}),
                Some(429),
                ProviderErrorKind::Quota,
                "trae_quota_exhausted",
            ),
            (
                json!({"code":4007}),
                None,
                ProviderErrorKind::RateLimit,
                "trae_rate_limited",
            ),
            (
                json!({"code":4006}),
                None,
                ProviderErrorKind::PromptTooLong,
                "context_length_exceeded",
            ),
            (
                json!({"code":"iam_permission_denied"}),
                Some(403),
                ProviderErrorKind::InvalidRequest,
                "trae_request_rejected",
            ),
            (
                json!({}),
                Some(401),
                ProviderErrorKind::Auth,
                "trae_auth_required",
            ),
            (
                json!({}),
                Some(503),
                ProviderErrorKind::Overloaded,
                "trae_service_unavailable",
            ),
            (
                json!({"code":3003,"message":"Model Request failed, please try again later."}),
                None,
                ProviderErrorKind::Overloaded,
                "trae_service_unavailable",
            ),
            (
                json!({"code":4054}),
                None,
                ProviderErrorKind::Overloaded,
                "trae_service_unavailable",
            ),
            (
                json!({"code":992}),
                None,
                ProviderErrorKind::Overloaded,
                "trae_service_unavailable",
            ),
        ] {
            let error = ServiceError::from_native(&body, status, None).provider_error();
            assert_eq!(error.kind, kind);
            assert_eq!(error.code.unwrap(), code);
        }
    }
    #[test]
    fn retry_and_request_diagnostics_survive_without_raw_response_or_credentials() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("retry-after", "7".parse().unwrap());
        headers.insert("x-tt-logid", "safe-request-1".parse().unwrap());
        let error = ServiceError::from_native(
            &json!({"message":"Cloud-IDE-JWT credential"}),
            Some(429),
            Some(&headers),
        )
        .provider_error();
        assert_eq!(error.retry_after_ms, Some(7000));
        assert_eq!(error.request_id.unwrap(), "safe-request-1");
        assert!(!error.message.contains("credential"));
    }
    #[test]
    fn interrupted_streams_and_transient_model_failures_are_retryable() {
        let interrupted =
            ServiceError::stream_interrupted("trae_stream_transport_failed").provider_error();
        assert_eq!(interrupted.kind, ProviderErrorKind::Overloaded);
        assert_eq!(
            interrupted.code.clone().unwrap(),
            "trae_service_unavailable"
        );
        assert_eq!(
            interrupted.error_type.clone().unwrap(),
            "trae_stream_transport_failed"
        );
        assert!(klaus_agent_core::provider::retry::is_retryable(
            &interrupted
        ));
        for code in ["3003", "4053", "4054", "978", "991", "992", "993"] {
            let error = ServiceError::from_native(
                &json!({"code": code.parse::<i64>().unwrap()}),
                None,
                None,
            )
            .provider_error();
            assert_eq!(error.kind, ProviderErrorKind::Overloaded);
            assert!(klaus_agent_core::provider::retry::is_retryable(&error));
        }
        for (body, status) in [
            (json!({"code": 1005}), None),
            (json!({"code": "insufficient_quota"}), Some(429)),
            (json!({"code": 4006}), None),
            (json!({"code": 1009}), Some(413)),
        ] {
            let error = ServiceError::from_native(&body, status, None).provider_error();
            assert!(!klaus_agent_core::provider::retry::is_retryable(&error));
        }
    }
}
