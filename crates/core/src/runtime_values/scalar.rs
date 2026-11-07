use std::fmt;

use serde::{de::DeserializeOwned, Serialize};

use super::{
    RuntimeCodecError, RuntimeDecode, RuntimeEncode, RuntimeEncoder, RuntimeJson, RuntimeNumber,
    RuntimeObject, RuntimeString,
};

/// Engine-authored, validator-approved scalar-only schema JSON.
#[derive(Clone, Debug, PartialEq)]
pub struct ScalarJson(serde_json::Value);

/// Validator-approved local configuration JSON.
#[derive(Clone, Debug, PartialEq)]
pub struct ConfigJson(serde_json::Value);

/// Validator-approved ASCII/scalar provenance JSON.
#[derive(Clone, Debug, PartialEq)]
pub struct ProvenanceJson(serde_json::Value);

/// Stable scalar-domain conversion failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScalarJsonErrorKind {
    InvalidScalarNumber,
    NonScalarString,
    NonFiniteNumber,
}

/// Scalar-domain conversion failure with an exact runtime path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScalarJsonError {
    path: Vec<RuntimeString>,
    kind: ScalarJsonErrorKind,
}

impl ScalarJsonError {
    pub fn kind(&self) -> ScalarJsonErrorKind {
        self.kind
    }

    pub fn path(&self) -> &[RuntimeString] {
        &self.path
    }

    fn root(kind: ScalarJsonErrorKind) -> Self {
        Self {
            path: Vec::new(),
            kind,
        }
    }

    fn prepend(mut self, segment: impl Into<RuntimeString>) -> Self {
        self.path.insert(0, segment.into());
        self
    }
}

impl fmt::Display for ScalarJsonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "scalar JSON {:?} at {:?}", self.kind, self.path)
    }
}

impl std::error::Error for ScalarJsonError {}

impl ScalarJson {
    /// Validate the engine-authored static-schema boundary.
    pub fn validate_static_schema(value: serde_json::Value) -> Result<Self, ScalarJsonError> {
        validate_scalar_value(&value)?;
        Ok(Self(value))
    }
}

impl ConfigJson {
    /// Validate the local configuration/credential document boundary.
    pub fn validate_config_document(value: serde_json::Value) -> Result<Self, ScalarJsonError> {
        validate_scalar_value(&value)?;
        Ok(Self(value))
    }

    /// Decode a validated configuration document without exposing its serde
    /// representation to runtime-domain callers.
    pub(crate) fn deserialize_config<T: DeserializeOwned>(self) -> Result<T, serde_json::Error> {
        serde_json::from_value(self.0)
    }

    pub(crate) fn serialize_config<T: Serialize + ?Sized>(
        value: &T,
    ) -> Result<Self, RuntimeCodecError> {
        let value = serde_json::to_value(value).map_err(|error| {
            RuntimeCodecError::new(super::RuntimeCodecErrorKind::Custom(RuntimeString::from(
                error.to_string(),
            )))
        })?;
        Self::validate_config_document(value).map_err(scalar_error_to_codec)
    }

    pub(crate) fn decode_runtime_document(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_to_scalar(value)
            .map(Self)
            .map_err(scalar_error_to_codec)
    }
}

impl ProvenanceJson {
    /// Validate the package/pipeline/provenance document boundary.
    pub fn validate_provenance_document(value: serde_json::Value) -> Result<Self, ScalarJsonError> {
        validate_scalar_value(&value)?;
        Ok(Self(value))
    }
}

/// Convert exact runtime JSON only when scalar serde identity is preserved.
pub fn try_to_scalar_json(value: &RuntimeJson) -> Result<ScalarJson, ScalarJsonError> {
    runtime_to_scalar(value).map(ScalarJson)
}

macro_rules! impl_scalar_encode {
    ($($type:ty),+ $(,)?) => {
        $(
            impl RuntimeEncode for $type {
                fn encode_runtime(
                    &self,
                    out: &mut RuntimeEncoder,
                ) -> Result<(), RuntimeCodecError> {
                    out.emit(scalar_to_runtime(&self.0))
                }
            }
        )+
    };
}

impl_scalar_encode!(ScalarJson, ConfigJson, ProvenanceJson);

impl RuntimeDecode for ScalarJson {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        try_to_scalar_json(value).map_err(scalar_error_to_codec)
    }
}

fn scalar_error_to_codec(error: ScalarJsonError) -> RuntimeCodecError {
    let message = RuntimeString::from(format!("{error}"));
    let mut codec = RuntimeCodecError::new(super::RuntimeCodecErrorKind::Custom(message));
    for segment in error.path.into_iter().rev() {
        codec = codec.prepend(segment);
    }
    codec
}

fn validate_scalar_value(value: &serde_json::Value) -> Result<(), ScalarJsonError> {
    match value {
        serde_json::Value::Number(value) => value
            .as_str()
            .parse::<f64>()
            .map(|_| ())
            .map_err(|_| ScalarJsonError::root(ScalarJsonErrorKind::InvalidScalarNumber)),
        serde_json::Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                validate_scalar_value(value).map_err(|error| error.prepend(index.to_string()))?;
            }
            Ok(())
        }
        serde_json::Value::Object(values) => {
            for (key, value) in values {
                validate_scalar_value(value).map_err(|error| error.prepend(key.as_str()))?;
            }
            Ok(())
        }
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::String(_) => {
            Ok(())
        }
    }
}

fn scalar_to_runtime(value: &serde_json::Value) -> RuntimeJson {
    match value {
        serde_json::Value::Null => RuntimeJson::Null,
        serde_json::Value::Bool(value) => RuntimeJson::Bool(*value),
        serde_json::Value::Number(value) => RuntimeJson::Number(RuntimeNumber::from_f64(
            value
                .as_str()
                .parse::<f64>()
                .expect("validated serde JSON number converts through binary64"),
        )),
        serde_json::Value::String(value) => RuntimeJson::String(RuntimeString::from(value.clone())),
        serde_json::Value::Array(values) => {
            RuntimeJson::Array(values.iter().map(scalar_to_runtime).collect())
        }
        serde_json::Value::Object(values) => {
            let mut object = RuntimeObject::new();
            for (key, value) in values {
                object.insert(key.clone(), scalar_to_runtime(value));
            }
            RuntimeJson::Object(object)
        }
    }
}

fn runtime_to_scalar(value: &RuntimeJson) -> Result<serde_json::Value, ScalarJsonError> {
    match value {
        RuntimeJson::Null => Ok(serde_json::Value::Null),
        RuntimeJson::Bool(value) => Ok(serde_json::Value::Bool(*value)),
        RuntimeJson::Number(value) => {
            if !value.as_f64().is_finite() {
                return Err(ScalarJsonError::root(ScalarJsonErrorKind::NonFiniteNumber));
            }
            // Keep integer tokens valid for scalar config codecs and preserve
            // negative zero's binary64 identity at this lossless boundary.
            let token = if value.as_f64() == 0.0 && value.as_f64().is_sign_negative() {
                "-0.0".to_string()
            } else {
                value.to_json_token()
            };
            token
                .parse::<serde_json::Number>()
                .map(serde_json::Value::Number)
                .map_err(|_| ScalarJsonError::root(ScalarJsonErrorKind::InvalidScalarNumber))
        }
        RuntimeJson::String(value) => value
            .try_to_utf8()
            .map(serde_json::Value::String)
            .map_err(|_| ScalarJsonError::root(ScalarJsonErrorKind::NonScalarString)),
        RuntimeJson::Array(values) => values
            .iter()
            .enumerate()
            .map(|(index, value)| {
                runtime_to_scalar(value).map_err(|error| error.prepend(index.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(serde_json::Value::Array),
        RuntimeJson::Object(values) => {
            let mut object = serde_json::Map::with_capacity(values.len());
            for (key, value) in values.entries() {
                let exact_key = key.clone();
                let key = key.try_to_utf8().map_err(|_| {
                    ScalarJsonError::root(ScalarJsonErrorKind::NonScalarString).prepend(exact_key)
                })?;
                let converted = runtime_to_scalar(value)
                    .map_err(|error| error.prepend(RuntimeString::from(key.clone())))?;
                object.insert(key, converted);
            }
            Ok(serde_json::Value::Object(object))
        }
    }
}
