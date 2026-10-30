//! Exact runtime payloads share the connection's ordinary output and op queues.
//! Service-owned scalar DTOs retain their original number representation.

use klaus_agent_core::runtime_values::{runtime_json, RuntimeJson, RuntimeObject, RuntimeString};
use serde_json::Value;

use crate::app_server::runtime_wire::RawJson;

#[derive(Clone, Debug)]
pub(crate) enum Payload {
    Scalar(Value),
    Runtime(RuntimeJson),
    /// Typed serde payloads retain raw subtrees and integer control values.
    Encoded(RawJson),
    /// Envelope/control fields retain their scalar number lexemes while each
    /// runtime subtree remains authoritative and is encoded by its own codec.
    Object(Vec<(String, Payload)>),
}

impl From<Value> for Payload {
    fn from(value: Value) -> Self {
        Self::Scalar(value)
    }
}
impl From<RuntimeJson> for Payload {
    fn from(value: RuntimeJson) -> Self {
        Self::Runtime(value)
    }
}
impl From<RawJson> for Payload {
    fn from(value: RawJson) -> Self {
        Self::Encoded(value)
    }
}

impl Payload {
    pub fn bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        match self {
            Self::Scalar(value) => serde_json::to_vec(value),
            Self::Runtime(value) => Ok(runtime_json::stringify(value).to_node_utf8()),
            Self::Encoded(value) => Ok(value.as_str().as_bytes().to_vec()),
            Self::Object(fields) => {
                let mut bytes = vec![b'{'];
                for (index, (key, value)) in fields.iter().enumerate() {
                    if index != 0 {
                        bytes.push(b',');
                    }
                    serde_json::to_writer(&mut bytes, key)?;
                    bytes.push(b':');
                    bytes.extend(value.bytes()?);
                }
                bytes.push(b'}');
                Ok(bytes)
            }
        }
    }

    pub fn response_id(&self) -> Value {
        match self {
            Self::Scalar(value) => value.get("id").cloned().unwrap_or(Value::Null),
            Self::Runtime(value) => scalar(field(value, "id")).unwrap_or(Value::Null),
            Self::Encoded(value) => value
                .get("id")
                .ok()
                .flatten()
                .and_then(|id| id.deserialize().ok())
                .unwrap_or(Value::Null),
            Self::Object(fields) => {
                fields
                    .iter()
                    .find(|(name, _)| name == "id")
                    .map_or(Value::Null, |(_, value)| match value {
                        Self::Scalar(value) => value.clone(),
                        Self::Runtime(value) => scalar(value).unwrap_or(Value::Null),
                        Self::Encoded(value) => value.deserialize().unwrap_or(Value::Null),
                        Self::Object(_) => Value::Null,
                    })
            }
        }
    }
}

pub(super) fn parse(text: &str) -> Result<RuntimeJson, String> {
    runtime_json::parse(&RuntimeString::from(text)).map_err(|error| error.to_string())
}

/// This conversion is only for values already owned by a scalar service DTO.
pub(super) fn runtime(value: Value) -> RuntimeJson {
    parse(&serde_json::to_string(&value).expect("scalar JSON encodes"))
        .expect("scalar JSON is runtime JSON")
}

/// A checked control boundary. Never use this to project transcript content.
pub(super) fn scalar(value: &RuntimeJson) -> Result<Value, String> {
    serde_json::from_slice(&runtime_json::stringify(value).to_node_utf8())
        .map_err(|error| error.to_string())
}

pub(super) fn field<'a>(value: &'a RuntimeJson, name: &str) -> &'a RuntimeJson {
    static NULL: RuntimeJson = RuntimeJson::Null;
    value.get(name).unwrap_or(&NULL)
}

pub(super) fn text(value: &RuntimeJson) -> Option<String> {
    value.as_string()?.try_to_utf8().ok()
}

pub(super) fn object(fields: impl IntoIterator<Item = (&'static str, RuntimeJson)>) -> RuntimeJson {
    let mut object = RuntimeObject::new();
    for (name, value) in fields {
        object.insert(name, value);
    }
    RuntimeJson::Object(object)
}

pub(super) fn response(id: &Value, result: impl Into<Payload>) -> Payload {
    Payload::Object(vec![
        ("id".into(), id.clone().into()),
        ("result".into(), result.into()),
    ])
}

pub(super) fn runtime_fields(value: RuntimeJson) -> Vec<(String, Payload)> {
    value
        .as_object()
        .expect("runtime response object")
        .entries()
        .map(|(key, value)| {
            (
                key.try_to_utf8().expect("fixed response field"),
                Payload::Runtime(value.clone()),
            )
        })
        .collect()
}

pub(super) fn notification(method: &str, params: RuntimeJson) -> RuntimeJson {
    object([("method", method.into()), ("params", params)])
}
