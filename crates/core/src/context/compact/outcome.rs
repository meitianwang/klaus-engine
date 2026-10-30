//! Stable compaction outcome metadata shared by the compactor, Session API,
//! protocol, and SDK surfaces.

use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};

fn decode_unit_enum<T: Copy>(
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

/// Why a completed compaction request intentionally kept the existing history.
///
/// This is only a no-op cause. Provider and verification degradation belongs in
/// [`CompactionDiagnostic`] even when that degradation ultimately contributes to
/// an inflated summary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionNoopReason {
    CircuitBreakerOpen,
    SummaryInflated,
    NoProgress,
    CompactorDisabled,
}

impl RuntimeEncode for CompactionNoopReason {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::CircuitBreakerOpen => "circuit-breaker-open",
            Self::SummaryInflated => "summary-inflated",
            Self::NoProgress => "no-progress",
            Self::CompactorDisabled => "compactor-disabled",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for CompactionNoopReason {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("circuit-breaker-open", &[]),
                RuntimeEnumVariant::new("summary-inflated", &[]),
                RuntimeEnumVariant::new("no-progress", &[]),
                RuntimeEnumVariant::new("compactor-disabled", &[]),
            ],
            &[
                Self::CircuitBreakerOpen,
                Self::SummaryInflated,
                Self::NoProgress,
                Self::CompactorDisabled,
            ],
        )
    }
}

/// Which model-backed compaction stage degraded and used its defined fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionDiagnosticStage {
    Summary,
    SelfCheck,
}

impl RuntimeEncode for CompactionDiagnosticStage {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(
            out,
            match self {
                Self::Summary => "summary",
                Self::SelfCheck => "self-check",
            },
        )
    }
}

impl RuntimeDecode for CompactionDiagnosticStage {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("summary", &[]),
                RuntimeEnumVariant::new("self-check", &[]),
            ],
            &[Self::Summary, Self::SelfCheck],
        )
    }
}

/// Stable classification for a model-backed compaction degradation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionDiagnosticKind {
    ProviderError,
    PromptTooLong,
    OutputTruncated,
    EmptyOutput,
}

impl RuntimeEncode for CompactionDiagnosticKind {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(
            out,
            match self {
                Self::ProviderError => "provider-error",
                Self::PromptTooLong => "prompt-too-long",
                Self::OutputTruncated => "output-truncated",
                Self::EmptyOutput => "empty-output",
            },
        )
    }
}

impl RuntimeDecode for CompactionDiagnosticKind {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("provider-error", &[]),
                RuntimeEnumVariant::new("prompt-too-long", &[]),
                RuntimeEnumVariant::new("output-truncated", &[]),
                RuntimeEnumVariant::new("empty-output", &[]),
            ],
            &[
                Self::ProviderError,
                Self::PromptTooLong,
                Self::OutputTruncated,
                Self::EmptyOutput,
            ],
        )
    }
}

/// Non-fatal provider or self-check degradation observed during compaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionDiagnostic {
    pub stage: CompactionDiagnosticStage,
    pub kind: CompactionDiagnosticKind,
    pub message: Option<RuntimeString>,
    /// Native failure identity retained for isolated request recovery; never serialized.
    pub provider_error: Option<crate::types::errors::ProviderError>,
}

impl CompactionDiagnostic {
    pub(crate) fn new(
        stage: CompactionDiagnosticStage,
        kind: CompactionDiagnosticKind,
        message: Option<RuntimeString>,
    ) -> Self {
        Self {
            stage,
            kind,
            message,
            provider_error: None,
        }
    }

    pub(crate) fn from_provider(
        stage: CompactionDiagnosticStage,
        kind: CompactionDiagnosticKind,
        error: crate::types::errors::ProviderError,
    ) -> Self {
        Self {
            stage,
            kind,
            message: Some(error.message.clone()),
            provider_error: Some(error),
        }
    }
}

impl RuntimeEncode for CompactionDiagnostic {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("stage", &self.stage)?;
        object.field("kind", &self.kind)?;
        if let Some(message) = &self.message {
            object.field("message", message)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for CompactionDiagnostic {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                stage: object.required("stage", &[])?,
                kind: object.required("kind", &[])?,
                message: object.optional("message", &[])?,
                provider_error: None,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_reason_tags_are_stable_and_round_trip() {
        let cases = [
            (
                CompactionNoopReason::CircuitBreakerOpen,
                "circuit-breaker-open",
            ),
            (CompactionNoopReason::SummaryInflated, "summary-inflated"),
            (CompactionNoopReason::NoProgress, "no-progress"),
            (
                CompactionNoopReason::CompactorDisabled,
                "compactor-disabled",
            ),
        ];
        for (reason, tag) in cases {
            let encoded = runtime_codec::encode_value(&reason).unwrap();
            assert_eq!(encoded, RuntimeJson::from(tag));
            assert_eq!(
                runtime_codec::decode_value::<CompactionNoopReason>(&encoded).unwrap(),
                reason
            );
        }
    }

    #[test]
    fn diagnostics_round_trip_without_becoming_noop_causes() {
        let diagnostic = CompactionDiagnostic {
            provider_error: None,
            stage: CompactionDiagnosticStage::SelfCheck,
            kind: CompactionDiagnosticKind::ProviderError,
            message: Some("provider unavailable".into()),
        };
        let encoded = runtime_codec::encode_value(&diagnostic).unwrap();
        assert_eq!(encoded["stage"], RuntimeJson::from("self-check"));
        assert_eq!(encoded["kind"], RuntimeJson::from("provider-error"));
        assert_eq!(
            runtime_codec::decode_value::<CompactionDiagnostic>(&encoded).unwrap(),
            diagnostic
        );
    }
}
