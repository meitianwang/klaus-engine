//! Scoped serde adapters for runtime-owned JSON subtrees.
//!
//! Envelope controls keep their ordinary serde types and numeric lexemes. Only
//! runtime payloads pass through the native UTF-16 parser and encoder.

use std::collections::HashSet;
use std::fmt;
use std::ops::{Deref, DerefMut};

use klaus_agent_core::runtime_values::{
    runtime_json, RuntimeCodecError, RuntimeDecode, RuntimeEncode, RuntimeEncoder, RuntimeJson,
    RuntimeString,
};
use schemars::{gen::SchemaGenerator, schema::Schema, JsonSchema};
use serde::de::{DeserializeOwned, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;

/// One owned JSON subtree, retaining its original number and string spelling.
/// Decode controls with [`Self::deserialize`]; decode runtime payloads with
/// [`Self::to_runtime`]. This is a value carrier, not a request escape hatch.
#[derive(Clone, Debug)]
pub struct RawJson(Box<RawValue>);

impl RawJson {
    pub fn from_json(text: String) -> Result<Self, serde_json::Error> {
        RawValue::from_string(text).map(Self)
    }

    pub fn from_serialize<T: Serialize + ?Sized>(value: &T) -> Result<Self, serde_json::Error> {
        serde_json::value::to_raw_value(value).map(Self)
    }

    pub fn as_str(&self) -> &str {
        self.0.get()
    }

    pub fn deserialize<T: DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_str(self.as_str())
    }

    /// A checked scalar boundary for service-owned DTOs only.
    pub fn scalar(&self) -> Result<serde_json::Value, serde_json::Error> {
        self.deserialize()
    }

    pub fn to_runtime(&self) -> Result<RuntimeValue, String> {
        runtime_json::parse(&self.as_str().into())
            .map(RuntimeValue)
            .map_err(|error| error.to_string())
    }

    /// Extract one field without decoding any sibling runtime payload.
    pub fn get(&self, name: &str) -> Result<Option<Self>, String> {
        Ok(self
            .object_fields()?
            .into_iter()
            .find_map(|(key, value)| (key == name).then_some(value)))
    }

    /// Read a scalar control field. Null is accepted for optional controls.
    pub fn control_string(&self, name: &str) -> Result<Option<String>, String> {
        self.get(name)?
            .map(|value| {
                value
                    .deserialize::<Option<String>>()
                    .map_err(|error| error.to_string())
            })
            .transpose()
            .map(Option::flatten)
    }

    pub fn control_u64(&self, name: &str) -> Result<Option<u64>, String> {
        self.get(name)?
            .map(|value| {
                value
                    .deserialize::<Option<u64>>()
                    .map_err(|error| error.to_string())
            })
            .transpose()
            .map(Option::flatten)
    }

    /// Add an absent scalar string control without re-encoding any opaque
    /// sibling value. An existing control must have its declared string type.
    pub fn with_default_string(&self, name: &str, default: &str) -> Result<Self, String> {
        struct Fields;
        impl<'de> Visitor<'de> for Fields {
            type Value = Vec<(RawJson, RawJson)>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a JSON reply object")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut fields = Vec::new();
                while let Some(key) = map.next_key::<RawJson>()? {
                    fields.push((key, map.next_value::<RawJson>()?));
                }
                Ok(fields)
            }
        }
        let mut decoder = serde_json::Deserializer::from_str(self.as_str());
        let fields = serde::de::Deserializer::deserialize_map(&mut decoder, Fields)
            .map_err(|error| error.to_string())?;
        decoder.end().map_err(|error| error.to_string())?;
        let mut control = None;
        for (key, value) in &fields {
            let key = key.to_runtime()?;
            let key = key
                .as_string()
                .ok_or("a JSON object key must be a string")?;
            if key == name {
                if control.is_some() {
                    return Err(format!("duplicate control field {name}"));
                }
                control = Some(value);
            }
        }
        if let Some(value) = control {
            value
                .deserialize::<String>()
                .map_err(|error| format!("{name}: {error}"))?;
            return Ok(self.clone());
        }
        let mut text = String::from("{");
        for (key, value) in &fields {
            if text.len() != 1 {
                text.push(',');
            }
            text.push_str(key.as_str());
            text.push(':');
            text.push_str(value.as_str());
        }
        if text.len() != 1 {
            text.push(',');
        }
        text.push_str(
            Self::from_serialize(name)
                .map_err(|error| error.to_string())?
                .as_str(),
        );
        text.push(':');
        text.push_str(
            Self::from_serialize(default)
                .map_err(|error| error.to_string())?
                .as_str(),
        );
        text.push('}');
        Self::from_json(text).map_err(|error| error.to_string())
    }

    pub(crate) fn object_fields(&self) -> Result<Vec<(String, Self)>, String> {
        struct Fields;
        impl<'de> Visitor<'de> for Fields {
            type Value = Vec<(String, RawJson)>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a JSON object with unique control fields")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut fields = Vec::new();
                let mut names = HashSet::new();
                while let Some(name) = map.next_key::<String>()? {
                    if !names.insert(name.clone()) {
                        return Err(serde::de::Error::custom(format!(
                            "duplicate control field {name}"
                        )));
                    }
                    fields.push((name, map.next_value::<RawJson>()?));
                }
                Ok(fields)
            }
        }
        let mut decoder = serde_json::Deserializer::from_str(self.as_str());
        let fields = serde::de::Deserializer::deserialize_map(&mut decoder, Fields)
            .map_err(|error| error.to_string())?;
        decoder.end().map_err(|error| error.to_string())?;
        Ok(fields)
    }
}

impl Default for RawJson {
    fn default() -> Self {
        Self(RawValue::from_string("null".into()).expect("null is valid JSON"))
    }
}

impl TryFrom<serde_json::Value> for RawJson {
    type Error = serde_json::Error;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        Self::from_serialize(&value)
    }
}

impl PartialEq for RawJson {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl JsonSchema for RawJson {
    fn schema_name() -> String {
        "RawJson".into()
    }

    fn is_referenceable() -> bool {
        false
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        RuntimeValue::json_schema(generator)
    }
}

impl Serialize for RawJson {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RawJson {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Box::<RawValue>::deserialize(deserializer).map(Self)
    }
}

/// An opaque runtime-owned result or payload, with exact UTF-16 strings.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuntimeValue(pub RuntimeJson);

impl RuntimeValue {
    pub fn into_inner(self) -> RuntimeJson {
        self.0
    }
}

impl Deref for RuntimeValue {
    type Target = RuntimeJson;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RuntimeValue {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<RuntimeJson> for RuntimeValue {
    fn from(value: RuntimeJson) -> Self {
        Self(value)
    }
}

impl From<RuntimeString> for RuntimeValue {
    fn from(value: RuntimeString) -> Self {
        Self(RuntimeJson::String(value))
    }
}

impl From<&str> for RuntimeValue {
    fn from(value: &str) -> Self {
        Self::from(RuntimeString::from(value))
    }
}

impl From<String> for RuntimeValue {
    fn from(value: String) -> Self {
        Self::from(RuntimeString::from(value))
    }
}

impl From<serde_json::Value> for RuntimeValue {
    fn from(value: serde_json::Value) -> Self {
        // An explicit scalar-to-runtime boundary; controls never use this path.
        RawJson::from_serialize(&value)
            .expect("scalar JSON encodes")
            .to_runtime()
            .expect("scalar JSON is valid runtime JSON")
    }
}

impl Serialize for RuntimeValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let bytes = runtime_json::stringify(&self.0).to_node_utf8();
        let text = String::from_utf8(bytes).expect("native JSON encoder emits UTF-8 JSON");
        RawJson::from_json(text)
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RuntimeValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        <RawJson as Deserialize>::deserialize(deserializer)?
            .to_runtime()
            .map_err(serde::de::Error::custom)
    }
}

impl JsonSchema for RuntimeValue {
    fn schema_name() -> String {
        "RuntimeValue".into()
    }

    fn is_referenceable() -> bool {
        false
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        Schema::Bool(true)
    }
}

impl RuntimeEncode for RuntimeValue {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        self.0.encode_runtime(out)
    }
}

impl RuntimeDecode for RuntimeValue {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        Ok(Self(value.clone()))
    }
}

impl fmt::Display for RuntimeValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = runtime_json::stringify(&self.0).to_node_utf8();
        formatter
            .write_str(std::str::from_utf8(&bytes).expect("native JSON encoder emits UTF-8 JSON"))
    }
}

/// A serde field adapter for one exact runtime string.
pub mod string {
    use super::*;

    pub fn serialize<S: Serializer>(
        value: &RuntimeString,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        RuntimeValue(RuntimeJson::String(value.clone())).serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<RuntimeString, D::Error> {
        RuntimeValue::deserialize(deserializer)?
            .0
            .into_string()
            .map_err(|_| serde::de::Error::custom("expected a runtime string"))
    }
}

/// A serde field adapter for an optional exact runtime string.
pub mod optional_string {
    use super::*;

    pub fn serialize<S: Serializer>(
        value: &Option<RuntimeString>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(value) => string::serialize(value, serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<RuntimeString>, D::Error> {
        Option::<RuntimeValue>::deserialize(deserializer)?
            .map(|value| {
                value
                    .0
                    .into_string()
                    .map_err(|_| serde::de::Error::custom("expected a runtime string"))
            })
            .transpose()
    }
}

/// A serde field adapter for an array of exact runtime strings.
pub mod strings {
    use super::*;

    pub fn serialize<S: Serializer>(
        value: &[RuntimeString],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        RuntimeValue(RuntimeJson::Array(
            value.iter().cloned().map(RuntimeJson::String).collect(),
        ))
        .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<RuntimeString>, D::Error> {
        Vec::<RuntimeValue>::deserialize(deserializer)?
            .into_iter()
            .map(|value| {
                value
                    .0
                    .into_string()
                    .map_err(|_| serde::de::Error::custom("expected a runtime string"))
            })
            .collect()
    }
}

/// A serde field adapter for an optional array of exact runtime strings.
pub mod optional_strings {
    use super::*;

    pub fn serialize<S: Serializer>(
        value: &Option<Vec<RuntimeString>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(value) => strings::serialize(value, serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Vec<RuntimeString>>, D::Error> {
        Option::<Vec<RuntimeValue>>::deserialize(deserializer)?
            .map(|values| {
                values
                    .into_iter()
                    .map(|value| {
                        value
                            .0
                            .into_string()
                            .map_err(|_| serde::de::Error::custom("expected a runtime string"))
                    })
                    .collect()
            })
            .transpose()
    }
}
