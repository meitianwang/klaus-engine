//! Read-only preservation of complete turn-failure diagnostics.

use crate::runtime_values::{RuntimeJson, RuntimeObject};

fn strip_internal(value: &RuntimeJson) -> RuntimeJson {
    match value {
        RuntimeJson::Object(object) => {
            let mut filtered = RuntimeObject::new();
            for (key, value) in object.entries() {
                if key == "media_origins" || key == "mediaOrigins" {
                    continue;
                }
                filtered.insert(key, strip_internal(value));
            }
            RuntimeJson::Object(filtered)
        }
        RuntimeJson::Array(values) => {
            RuntimeJson::Array(values.iter().map(strip_internal).collect())
        }
        other => other.clone(),
    }
}

/// Preserve unknown diagnostic fields while excluding request-local media maps.
/// Empty or non-object details are absent, not an invented failure description.
pub fn sanitize_turn_error_details(value: &RuntimeJson) -> Option<RuntimeJson> {
    let mut filtered = strip_internal(value);
    let RuntimeJson::Object(object) = &mut filtered else {
        return None;
    };
    if object.get("providerError").is_some_and(|value| {
        matches!(value, RuntimeJson::Null)
            || value.as_object().is_some_and(|value| value.is_empty())
    }) {
        object.remove("providerError");
    }
    if object.get("recoveryReason").is_some_and(|value| {
        matches!(value, RuntimeJson::Null) || value.as_str().is_some_and(|value| value.is_empty())
    }) {
        object.remove("recoveryReason");
    }
    if object.is_empty() {
        return None;
    }
    Some(filtered)
}

/// Recover diagnostics from a full lifecycle record or its older provider-detail
/// envelope. A legacy message/kind alone remains legacy, without new fields.
pub fn turn_error_details_from_lifecycle(payload: &RuntimeJson) -> Option<RuntimeJson> {
    let payload = payload.as_object()?;
    if payload
        .get("status")
        .and_then(RuntimeJson::as_str)
        .is_none_or(|status| status != "failed")
    {
        return None;
    }
    if let Some(details) = payload
        .get("errorDetails")
        .and_then(sanitize_turn_error_details)
    {
        return Some(details);
    }
    if let Some(details) = payload.get("error").and_then(sanitize_turn_error_details) {
        return Some(details);
    }
    let provider = payload
        .get("providerError")
        .and_then(sanitize_turn_error_details);
    let recovery = payload
        .get("recoveryReason")
        .and_then(RuntimeJson::as_str)
        .filter(|reason| !reason.is_empty());
    let flat_keys = [
        "code",
        "errorCode",
        "requestId",
        "retryAfterMs",
        "providerStatus",
        "httpStatus",
        "providerCode",
        "providerType",
        "type",
    ];
    let present = |value: &RuntimeJson| {
        !matches!(value, RuntimeJson::Null) && value.as_str().is_none_or(|value| !value.is_empty())
    };
    if provider.is_none()
        && recovery.is_none()
        && !flat_keys
            .iter()
            .any(|key| payload.get(*key).is_some_and(present))
    {
        return None;
    }
    let message = payload
        .get("error")
        .and_then(RuntimeJson::as_str)
        .or_else(|| provider.as_ref()?.get("message")?.as_str())?;
    let kind = payload
        .get("kind")
        .and_then(RuntimeJson::as_str)
        .or_else(|| provider.as_ref()?.get("kind")?.as_str());
    let mut details = RuntimeObject::new();
    let code = payload
        .get("errorCode")
        .or_else(|| payload.get("code"))
        .and_then(RuntimeJson::as_str)
        .cloned()
        .unwrap_or_else(|| {
            if kind.is_some_and(|kind| kind == "prompt_too_long") {
                "prompt_too_long".into()
            } else {
                "provider_error".into()
            }
        });
    details.insert("code", RuntimeJson::String(code));
    details.insert("message", RuntimeJson::String(message.clone()));
    if let Some(kind) = kind {
        details.insert("kind", RuntimeJson::String(kind.clone()));
    }
    for key in ["requestId", "retryAfterMs"] {
        let value = payload
            .get(key)
            .filter(|value| present(value))
            .or_else(|| provider.as_ref()?.get(key).filter(|value| present(value)));
        if let Some(value) = value {
            details.insert(key, value.clone());
        }
    }
    for key in [
        "providerStatus",
        "httpStatus",
        "providerCode",
        "providerType",
        "type",
    ] {
        if let Some(value) = payload.get(key).filter(|value| present(value)) {
            details.insert(key, value.clone());
        }
    }
    if let Some(provider) = provider {
        details.insert("providerError", provider);
    }
    if let Some(recovery) = recovery {
        details.insert("recoveryReason", RuntimeJson::String(recovery.clone()));
    }
    Some(RuntimeJson::Object(details))
}
