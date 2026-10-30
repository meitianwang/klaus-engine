//! Reasoning effort levels and values.

use serde::{Deserialize, Serialize};

use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson,
};

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

/// Known reasoning selections, including local orchestration aliases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EffortLevel {
    None,
    Minimal,
    Low,
    Medium,
    High,
    Xhigh,
    Max,
    /// Local orchestration mode; providers resolve it to a supported effort.
    Ultra,
    Persistent,
}

impl RuntimeEncode for EffortLevel {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::None => "none",
            Self::Minimal => "minimal",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Xhigh => "xhigh",
            Self::Max => "max",
            Self::Ultra => "ultra",
            Self::Persistent => "persistent",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for EffortLevel {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("none", &[]),
                RuntimeEnumVariant::new("minimal", &[]),
                RuntimeEnumVariant::new("low", &[]),
                RuntimeEnumVariant::new("medium", &[]),
                RuntimeEnumVariant::new("high", &[]),
                RuntimeEnumVariant::new("xhigh", &[]),
                RuntimeEnumVariant::new("max", &[]),
                RuntimeEnumVariant::new("ultra", &[]),
                RuntimeEnumVariant::new("persistent", &[]),
            ],
            &[
                Self::None,
                Self::Minimal,
                Self::Low,
                Self::Medium,
                Self::High,
                Self::Xhigh,
                Self::Max,
                Self::Ultra,
                Self::Persistent,
            ],
        )
    }
}

/// The known reasoning selections in declaration order.
pub const EFFORT_LEVELS: [EffortLevel; 9] = [
    EffortLevel::None,
    EffortLevel::Minimal,
    EffortLevel::Low,
    EffortLevel::Medium,
    EffortLevel::High,
    EffortLevel::Xhigh,
    EffortLevel::Max,
    EffortLevel::Ultra,
    EffortLevel::Persistent,
];

impl EffortLevel {
    /// The wire/string form of the level.
    pub fn as_str(&self) -> &'static str {
        match self {
            EffortLevel::None => "none",
            EffortLevel::Minimal => "minimal",
            EffortLevel::Low => "low",
            EffortLevel::Medium => "medium",
            EffortLevel::High => "high",
            EffortLevel::Xhigh => "xhigh",
            EffortLevel::Max => "max",
            EffortLevel::Ultra => "ultra",
            EffortLevel::Persistent => "persistent",
        }
    }

    /// Whether this local selection authorizes proactive multi-agent orchestration.
    pub fn is_ultra(self) -> bool {
        self == Self::Ultra
    }

    /// Parse a level from its string form, or `None` if unknown.
    // Keep the public Option-returning API for downstream callers.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(value: &str) -> Option<EffortLevel> {
        match value {
            "none" => Some(EffortLevel::None),
            "minimal" => Some(EffortLevel::Minimal),
            "low" => Some(EffortLevel::Low),
            "medium" => Some(EffortLevel::Medium),
            "high" => Some(EffortLevel::High),
            "xhigh" => Some(EffortLevel::Xhigh),
            "max" => Some(EffortLevel::Max),
            "ultra" => Some(EffortLevel::Ultra),
            "persistent" => Some(EffortLevel::Persistent),
            _ => None,
        }
    }
}

/// An effort value carried by config, roles and requests: a known selection,
/// a custom model-owned string, or an integer. The integer form is an opaque,
/// provider-interpreted intensity; the engine threads it through unchanged and
/// the active provider decides how to bucket it. `ultra` is a local delegation
/// preset, distinct from the keyword-triggered `ultracode` request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum EffortValue {
    Level(EffortLevel),
    Number(i64),
    Custom(#[serde(serialize_with = "serialize_custom")] crate::runtime_values::RuntimeString),
}

fn serialize_custom<S: serde::Serializer>(
    value: &crate::runtime_values::RuntimeString,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    value
        .try_to_utf8()
        .map_err(serde::ser::Error::custom)?
        .serialize(serializer)
}

impl<'de> Deserialize<'de> for EffortValue {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Text(String),
            Number(i64),
        }
        match Raw::deserialize(deserializer)? {
            Raw::Text(value) => Self::from_raw(&value.into())
                .ok_or_else(|| serde::de::Error::custom("Reasoning effort must not be empty")),
            Raw::Number(value) => Ok(Self::Number(value)),
        }
    }
}

impl EffortValue {
    /// Parse a raw reasoning selection without inventing a default.
    pub fn from_raw(value: &crate::runtime_values::RuntimeString) -> Option<Self> {
        if value.is_empty() {
            return None;
        }
        Some(
            value
                .try_to_utf8()
                .ok()
                .as_deref()
                .and_then(EffortLevel::from_str)
                .map(Self::Level)
                .unwrap_or_else(|| Self::Custom(value.clone())),
        )
    }

    pub fn is_ultra(&self) -> bool {
        matches!(self, Self::Level(EffortLevel::Ultra))
    }

    pub fn raw_name(&self) -> crate::runtime_values::RuntimeString {
        match self {
            Self::Level(level) => level.as_str().into(),
            Self::Number(number) => number.to_string().into(),
            Self::Custom(value) => value.clone(),
        }
    }
}

impl RuntimeEncode for EffortValue {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        match self {
            Self::Level(level) => level.encode_runtime(out),
            Self::Number(number) => number.encode_runtime(out),
            Self::Custom(value) => value.encode_runtime(out),
        }
    }
}

impl RuntimeDecode for EffortValue {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        match value {
            RuntimeJson::String(value) => Self::from_raw(value).ok_or_else(|| {
                RuntimeCodecError::new(RuntimeCodecErrorKind::NoMatchingUntaggedVariant)
            }),
            RuntimeJson::Number(_) => i64::decode_runtime(value).map(Self::Number),
            _ => Err(RuntimeCodecError::new(
                RuntimeCodecErrorKind::NoMatchingUntaggedVariant,
            )),
        }
    }
}

/// Normalize a frontmatter / config `effort` value:
/// - undefined / null / empty → `None`
/// - integer number → the number
/// - a known level string → that level
/// - any other string that parses as an integer → that integer
/// - any other nonempty string → a custom model-owned selection
/// - anything else → `None`
pub fn parse_effort(value: &serde_json::Value) -> Option<EffortValue> {
    match value {
        serde_json::Value::Null => None,
        serde_json::Value::Number(n) => {
            // integer number → the number; a non-integer number → undefined
            if let Some(i) = n.as_i64() {
                Some(EffortValue::Number(i))
            } else if let Some(u) = n.as_u64() {
                // Very large positive integers still round-trip as i64 where
                // possible; otherwise treat as out of range → dropped.
                i64::try_from(u).ok().map(EffortValue::Number)
            } else {
                None
            }
        }
        serde_json::Value::String(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                return None;
            }
            if let Some(level) = EffortLevel::from_str(trimmed) {
                return Some(EffortValue::Level(level));
            }
            // any other string that parses as an integer → that integer
            Some(
                trimmed
                    .parse::<i64>()
                    .map(EffortValue::Number)
                    .unwrap_or_else(|_| EffortValue::Custom(trimmed.into())),
            )
        }
        _ => None,
    }
}

/// Preserve the raw selection while filling a missing orchestration hint.
pub fn normalize_legacy_ultra(effort: &mut Option<EffortValue>, ultra_mode: &mut Option<bool>) {
    if *effort == Some(EffortValue::Level(EffortLevel::Ultra)) {
        ultra_mode.get_or_insert(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn passes_through_named_levels() {
        for level in EFFORT_LEVELS {
            assert_eq!(
                parse_effort(&json!(level.as_str())),
                Some(EffortValue::Level(level))
            );
        }
    }

    #[test]
    fn inherent_from_str_remains_an_option_without_importing_a_trait() {
        let known: Option<EffortLevel> = EffortLevel::from_str("high");
        let unknown: Option<EffortLevel> = EffortLevel::from_str("ultracode");

        assert_eq!(known, Some(EffortLevel::High));
        assert_eq!(unknown, None);
    }

    #[test]
    fn passes_through_integers_and_integer_strings() {
        assert_eq!(parse_effort(&json!(80)), Some(EffortValue::Number(80)));
        assert_eq!(parse_effort(&json!("80")), Some(EffortValue::Number(80)));
    }

    #[test]
    fn drops_empty_noninteger_and_structured_values() {
        assert_eq!(parse_effort(&json!("")), None);
        assert_eq!(parse_effort(&json!("   ")), None);
        assert_eq!(parse_effort(&json!(1.5)), None);
        assert_eq!(
            parse_effort(&json!("custom/exact")),
            Some(EffortValue::Custom("custom/exact".into()))
        );
        assert_eq!(parse_effort(&json!(null)), None);
        assert_eq!(parse_effort(&json!({})), None);
    }

    #[test]
    fn schema_accepts_nonempty_model_owned_strings() {
        assert!(serde_json::from_value::<EffortValue>(json!("high")).is_ok());
        assert!(serde_json::from_value::<EffortValue>(json!(50)).is_ok());
        assert_eq!(
            serde_json::from_value::<EffortValue>(json!("custom/exact")).unwrap(),
            EffortValue::Custom("custom/exact".into())
        );
        assert!(serde_json::from_value::<EffortValue>(json!("")).is_err());
    }
}
