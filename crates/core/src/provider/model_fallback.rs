//! Model-fallback eligibility classifier.
//!
//! Decides whether a provider error means the active model is unusable in a way
//! a plain retry on the SAME model cannot fix — so the turn should rotate to a
//! backup model (`config.fallbackModels`) instead of failing or retrying
//! forever. This is a SEPARATE axis from the retry classifier: an error can be
//! retryable, fallback-eligible, both, or neither.

use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson,
};
use crate::types::errors::{ProviderError, ProviderErrorKind};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::sync::LazyLock;

fn decode_provider_unit_enum<T: Copy>(
    value: &RuntimeJson,
    variants: &[RuntimeEnumVariant<'_>],
    values: &[T],
) -> Result<T, RuntimeCodecError> {
    runtime_codec::decode_external(value, variants, |variant| match variant {
        RuntimeExternalVariant::Unit { variant } => values
            .get(variant)
            .copied()
            .ok_or_else(|| RuntimeCodecError::new(RuntimeCodecErrorKind::UnknownEnumVariant)),
        RuntimeExternalVariant::Payload { .. } => Err(RuntimeCodecError::new(
            RuntimeCodecErrorKind::InvalidEnumShape,
        )),
    })
}

/// Why a model is unusable in a way a plain retry on the SAME model cannot fix.
///
/// - `ModelNotFound`: the model name is unknown / retired / unavailable (4xx +
///   a model-not-found message). Switch IMMEDIATELY — retrying the same name
///   never succeeds.
/// - `PermissionDenied`: the account/key isn't allowed to use THIS model (403 +
///   a model-permission message). Switch immediately. A plain 403 with no model
///   signal (bad key) is NOT this — it stays terminal so we don't paper over an
///   auth problem by silently downgrading.
/// - `Overloaded`: the model is up but capacity-shedding (529/5xx). Unlike the
///   two above this IS worth retrying first; the loop retries `maxRetries`
///   times, then treats persistent overload as a reason to fall back.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelFallbackKind {
    ModelNotFound,
    PermissionDenied,
    Overloaded,
}

impl RuntimeEncode for ModelFallbackKind {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::ModelNotFound => "model_not_found",
            Self::PermissionDenied => "permission_denied",
            Self::Overloaded => "overloaded",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for ModelFallbackKind {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("model_not_found", &[]),
                RuntimeEnumVariant::new("permission_denied", &[]),
                RuntimeEnumVariant::new("overloaded", &[]),
            ],
            &[
                Self::ModelNotFound,
                Self::PermissionDenied,
                Self::Overloaded,
            ],
        )
    }
}

impl ModelFallbackKind {
    /// The wire/string form (`'model_not_found' | 'permission_denied' |
    /// 'overloaded'`).
    pub fn as_str(&self) -> &'static str {
        match self {
            ModelFallbackKind::ModelNotFound => "model_not_found",
            ModelFallbackKind::PermissionDenied => "permission_denied",
            ModelFallbackKind::Overloaded => "overloaded",
        }
    }
}

// Message patterns matched against the error message because in-stream errors
// often carry no HTTP status. `[\s\S]` (not `.`) so the "model" token and the
// signal word can sit on different lines of a multi-line provider message.
static MODEL_NOT_FOUND_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)\bmodel\b[\s\S]*(not found|does not exist|unknown|unsupported|retired|unavailable|not available)|(not found|does not exist|unknown|unsupported|retired|unavailable|not available)[\s\S]*\bmodel\b|invalid model",
    )
    .unwrap()
});

static MODEL_PERMISSION_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)\bmodel\b[\s\S]*(permission|access|authorized|allowed|enabled|disabled|blocked|unavailable|not available)|(permission|access|authorized|allowed|enabled|disabled|blocked|unavailable|not available)[\s\S]*\bmodel\b",
    )
    .unwrap()
});

/// Statuses that are never a model-availability problem — a model-not-found
/// regex hit on one of these is a coincidence in the message, not a retired
/// model.
fn is_not_model_status(status: u16) -> bool {
    matches!(status, 401 | 402 | 413 | 422 | 429)
}

/// Classify a provider error for MODEL FALLBACK eligibility. Returns the reason
/// to switch models, or `None` to leave the error to the normal retry/terminal
/// path.
pub fn classify_model_fallback_error(error: &ProviderError) -> Option<ModelFallbackKind> {
    // Capacity shedding — the model exists and is permitted, it's just busy.
    // Retried first by the caller; eligible as a last resort once retries are
    // spent.
    if error.kind == ProviderErrorKind::Overloaded {
        return Some(ModelFallbackKind::Overloaded);
    }

    // The fixed patterns inspect only ASCII signal words plus `\s`/`\S`. Node's UTF-8
    // projection preserves those observable matches while mapping an unpaired surrogate to the
    // same non-word/non-whitespace replacement category as JavaScript's regex engine.
    let projected_message = error.message.to_node_utf8();
    let message = std::str::from_utf8(&projected_message)
        .expect("RuntimeString Node projection always produces valid UTF-8");
    let status = error.status;

    // Not-permitted for THIS model: requires a model-specific signal in the
    // message, so a bare 403 (invalid key / org-level block) is NOT treated as a
    // model problem and stays terminal. Gate on status == 403 EXACTLY — a 401
    // (which also maps to kind `auth`) is a real authentication failure that
    // switching models can't fix, so it must stay terminal. The `kind == Auth`
    // arm only admits a STATUS-LESS in-stream permission error (status absent),
    // which `status == 403` alone would miss.
    if (status == Some(403) || (status.is_none() && error.kind == ProviderErrorKind::Auth))
        && MODEL_PERMISSION_RE.is_match(message)
    {
        return Some(ModelFallbackKind::PermissionDenied);
    }

    // Retired / unknown / unavailable model name. A 429/413/422/401/402 that
    // merely happens to contain the word "model" is excluded — those are
    // rate/size/auth problems a model switch won't fix.
    if status.is_none_or(|s| !is_not_model_status(s)) && MODEL_NOT_FOUND_RE.is_match(message) {
        return Some(ModelFallbackKind::ModelNotFound);
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a `ProviderError` from partial fields, defaulting to `kind:
    /// unknown` and an empty message (mirrors the classifier's own defaults).
    fn err(kind: ProviderErrorKind, message: &str, status: Option<u16>) -> ProviderError {
        ProviderError {
            code: None,
            error_type: None,
            media_origins: None,
            media_repair_complete: false,
            kind,
            message: message.into(),
            status,
            retry_after_ms: None,
            request_id: None,
        }
    }

    #[test]
    fn treats_overloaded_error_as_fallback_trigger() {
        assert_eq!(
            classify_model_fallback_error(&err(
                ProviderErrorKind::Overloaded,
                "overloaded_error",
                None
            )),
            Some(ModelFallbackKind::Overloaded)
        );
    }

    #[test]
    fn detects_retired_unknown_unavailable_model_name() {
        assert_eq!(
            classify_model_fallback_error(&err(
                ProviderErrorKind::Unknown,
                "model \"claude-x\" not found",
                Some(404)
            )),
            Some(ModelFallbackKind::ModelNotFound)
        );
        assert_eq!(
            classify_model_fallback_error(&err(
                ProviderErrorKind::Unknown,
                "The model claude-x has been retired",
                Some(400)
            )),
            Some(ModelFallbackKind::ModelNotFound)
        );
        assert_eq!(
            classify_model_fallback_error(&err(ProviderErrorKind::Unknown, "invalid model", None)),
            Some(ModelFallbackKind::ModelNotFound)
        );
        // status-less in-stream error still classified from the message alone
        // (signal before "model").
        assert_eq!(
            classify_model_fallback_error(&err(
                ProviderErrorKind::Unknown,
                "does not exist: model claude-x",
                None
            )),
            Some(ModelFallbackKind::ModelNotFound)
        );
    }

    #[test]
    fn detects_not_permitted_for_this_model_error() {
        assert_eq!(
            classify_model_fallback_error(&err(
                ProviderErrorKind::Auth,
                "your account is not authorized to use model claude-x",
                Some(403)
            )),
            Some(ModelFallbackKind::PermissionDenied)
        );
        assert_eq!(
            classify_model_fallback_error(&err(
                ProviderErrorKind::Unknown,
                "model claude-x is not enabled for this organization",
                Some(403)
            )),
            Some(ModelFallbackKind::PermissionDenied)
        );
    }

    #[test]
    fn does_not_fall_back_on_plain_auth_error_with_no_model_signal() {
        assert_eq!(
            classify_model_fallback_error(&err(
                ProviderErrorKind::Auth,
                "invalid x-api-key",
                Some(403)
            )),
            None
        );
        assert_eq!(
            classify_model_fallback_error(&err(ProviderErrorKind::Auth, "unauthorized", Some(401))),
            None
        );
    }

    #[test]
    fn keeps_401_terminal_even_when_message_names_a_model() {
        // status 401 (mapped to kind `auth`) with model+permission wording must
        // NOT downgrade — a real authentication failure can't be fixed by
        // switching models (permission fallback is gated on 403).
        assert_eq!(
            classify_model_fallback_error(&err(
                ProviderErrorKind::Auth,
                "model claude-x is not available with your authorization",
                Some(401)
            )),
            None
        );
        // But a STATUS-LESS in-stream permission error (kind `auth`, no status)
        // with a model signal IS a model-permission problem → still eligible.
        assert_eq!(
            classify_model_fallback_error(&err(
                ProviderErrorKind::Auth,
                "model claude-x access not allowed",
                None
            )),
            Some(ModelFallbackKind::PermissionDenied)
        );
    }

    #[test]
    fn does_not_fall_back_on_plain_rate_limit_or_network_error() {
        assert_eq!(
            classify_model_fallback_error(&err(
                ProviderErrorKind::RateLimit,
                "slow down",
                Some(429)
            )),
            None
        );
        assert_eq!(
            classify_model_fallback_error(&err(ProviderErrorKind::Network, "ECONNRESET", None)),
            None
        );
    }

    #[test]
    fn excludes_model_word_coincidence_on_rate_size_auth_status() {
        // 429 message mentions "model" + "not available" but it's a rate-limit,
        // not a retired model.
        assert_eq!(
            classify_model_fallback_error(&err(
                ProviderErrorKind::RateLimit,
                "model temporarily not available, slow down",
                Some(429)
            )),
            None
        );
    }

    #[test]
    fn preserves_runtime_string_identity_while_classifying_across_a_lone_surrogate() {
        let message = crate::runtime_values::RuntimeString::from_utf16(vec![
            u16::from(b'm'),
            u16::from(b'o'),
            u16::from(b'd'),
            u16::from(b'e'),
            u16::from(b'l'),
            u16::from(b' '),
            0xd800,
            u16::from(b' '),
            u16::from(b'n'),
            u16::from(b'o'),
            u16::from(b't'),
            u16::from(b' '),
            u16::from(b'f'),
            u16::from(b'o'),
            u16::from(b'u'),
            u16::from(b'n'),
            u16::from(b'd'),
        ]);
        let error = ProviderError {
            code: None,
            error_type: None,
            media_origins: None,
            media_repair_complete: false,
            kind: ProviderErrorKind::Unknown,
            message: message.clone(),
            status: Some(404),
            retry_after_ms: None,
            request_id: None,
        };

        assert_eq!(
            classify_model_fallback_error(&error),
            Some(ModelFallbackKind::ModelNotFound)
        );
        assert_eq!(error.message, message);
    }
}
