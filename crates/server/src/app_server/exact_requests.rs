//! Raw parameters enter the ordinary named-method dispatcher without a scalar detour.

use super::*;
use serde::Serialize;
use std::collections::BTreeMap;

pub(super) enum Incoming {
    Request {
        id: Value,
        method: String,
        params: RawJson,
    },
    Notification,
    Response {
        id: Value,
        result: Result<RawJson, RawJson>,
    },
    Invalid,
}

/// Control fields are decoded independently so their original number tokens
/// reach their normal typed validators. Runtime content is never scalarized.
pub(super) fn classify(line: &str) -> Result<Incoming, serde_json::Error> {
    let raw = RawJson::from_json(line.to_owned())?;
    let Ok(mut fields) = raw.deserialize::<BTreeMap<String, RawJson>>() else {
        return Ok(Incoming::Invalid);
    };
    let method = fields
        .remove("method")
        .and_then(|raw| raw.deserialize::<String>().ok());
    let id = fields
        .remove("id")
        .map(|raw| raw.deserialize::<Value>())
        .transpose()?;
    let null = || RawJson::from_json("null".into()).expect("null is JSON");
    Ok(match (method, id) {
        (Some(method), Some(id)) => Incoming::Request {
            id,
            method,
            params: fields.remove("params").unwrap_or_else(null),
        },
        (Some(_), None) => Incoming::Notification,
        (None, Some(id)) => Incoming::Response {
            id,
            result: match fields.remove("error") {
                Some(error) => Err(error),
                None => Ok(fields.remove("result").unwrap_or_else(null)),
            },
        },
        (None, None) => Incoming::Invalid,
    })
}

/// Serialize a validated, named request DTO through its scoped runtime fields.
/// The transport and session identities belong to the facade, not the inner op.
pub(super) fn op<T: Serialize>(params: &T, kind: &str) -> RuntimeJson {
    let mut value = RawJson::from_serialize(params)
        .expect("validated request parameters encode")
        .to_runtime()
        .expect("validated request parameters are runtime JSON")
        .0;
    let fields = value
        .as_object_mut()
        .expect("request parameters are an object");
    fields.remove("sessionId");
    fields.remove("submissionId");
    fields.insert("type", kind.into());
    value
}

/// An init object is mixed: service controls are scalar, while untouched
/// discovery and runtime configuration values retain their exact JSON text.
pub(super) struct InitObject {
    fields: Vec<(String, RawJson)>,
}

impl InitObject {
    pub fn new(raw: &RawJson) -> Result<Self, String> {
        Ok(Self {
            fields: raw.object_fields()?,
        })
    }

    pub fn get(&self, name: &str) -> Option<&RawJson> {
        self.fields
            .iter()
            .find_map(|(key, value)| (key == name).then_some(value))
    }

    pub fn control_string(&self, name: &str) -> Result<Option<String>, String> {
        self.get(name)
            .map(|value| {
                value
                    .deserialize::<Option<String>>()
                    .map_err(|error| format!("{name}: {error}"))
            })
            .transpose()
            .map(Option::flatten)
    }

    pub fn set(&mut self, name: &str, value: RawJson) {
        if let Some((_, current)) = self.fields.iter_mut().find(|(key, _)| key == name) {
            *current = value;
        } else {
            self.fields.push((name.into(), value));
        }
    }

    pub fn remove(&mut self, name: &str) {
        self.fields.retain(|(key, _)| key != name);
    }

    pub fn raw(&self) -> RawJson {
        let mut text = String::from("{");
        for (index, (name, value)) in self.fields.iter().enumerate() {
            if index != 0 {
                text.push(',');
            }
            text.push_str(&serde_json::to_string(name).expect("scalar control key encodes"));
            text.push(':');
            text.push_str(value.as_str());
        }
        text.push('}');
        RawJson::from_json(text).expect("raw object children are valid JSON")
    }
}

#[cfg(test)]
mod selected_compaction_tests {
    use super::*;

    #[test]
    fn named_compaction_preserves_optional_selection_in_the_inner_request() {
        for direction in ["from", "upTo"] {
            let raw = RawJson::from_json(format!(
                r#"{{"sessionId":"thread","submissionId":"op","selection":{{"messageId":"id\ud800","direction":"{direction}"}},"instructions":"retain exact constraints"}}"#
            )).unwrap();
            let params: wire::SessionCompactParams = raw.deserialize().unwrap();
            let inner = op(&params, "compact_session");
            let decoded = klaus_agent_protocol::parse_op({
                let mut request = klaus_agent_core::runtime_values::RuntimeObject::new();
                request.insert("id", "op".into());
                request.insert("op", inner.clone());
                RuntimeJson::Object(request)
            })
            .unwrap();
            let klaus_agent_protocol::OpKind::CompactSession {
                selection: Some(selection),
                ..
            } = decoded.op
            else {
                panic!("named facade must forward the selected boundary");
            };
            assert_eq!(selection.message_id.as_utf16(), &[105, 100, 0xd800]);
            assert!(inner.as_object().unwrap().get("sessionId").is_none());
            assert!(inner.as_object().unwrap().get("submissionId").is_none());
        }
        for value in [
            r#"{"sessionId":"thread","selection":{"messageId":"id","direction":"backward"}}"#,
            r#"{"sessionId":"thread","selection":{"messageId":"id","direction":"from","index":1}}"#,
        ] {
            assert!(RawJson::from_json(value.into())
                .unwrap()
                .deserialize::<wire::SessionCompactParams>()
                .is_err());
        }
    }
}
