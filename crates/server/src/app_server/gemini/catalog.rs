//! Catalog projection for the public Gemini chat lineup.
//!
//! The backend serves one combined inventory through the shared transport;
//! this module keeps only the public versioned Gemini chat identities for
//! this source and drops internal task models, media models and the sibling
//! source's curated families. Model identities always come from the service
//! payload (or the sanitized bundled baseline recorded from one); the rules
//! here only decide which listed identities this source surfaces.

use serde_json::{json, Map, Value};
use std::sync::OnceLock;

/// The chat model this source selects when nothing else is named.
pub(super) const DEFAULT_MAIN_MODEL: &str = "gemini-3-flash";

/// Versioned public Gemini chat ids: `gemini-<major>[.<minor>]-<tier>[-thinking]`.
pub(super) fn is_public_chat_id(id: &str) -> bool {
    let Some(rest) = id.strip_prefix("gemini-") else {
        return false;
    };
    let Some((version, tail)) = split_version(rest) else {
        return false;
    };
    if !valid_version(version) {
        return false;
    }
    matches!(
        tail,
        "pro" | "flash" | "flash-lite" | "pro-thinking" | "flash-thinking" | "flash-lite-thinking"
    )
}

fn split_version(rest: &str) -> Option<(&str, &str)> {
    let end = rest.find(|c: char| c != '.' && !c.is_ascii_digit())?;
    (rest.as_bytes().get(end) == Some(&b'-')).then(|| (&rest[..end], &rest[end + 1..]))
}

fn valid_version(version: &str) -> bool {
    let groups: Vec<&str> = version.split('.').collect();
    (1..=2).contains(&groups.len())
        && groups
            .iter()
            .all(|group| !group.is_empty() && group.bytes().all(|b| b.is_ascii_digit()))
}

fn first_version(text: &str) -> Option<&str> {
    let bytes = text.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index].is_ascii_digit() {
            let start = index;
            while index < bytes.len() && (bytes[index].is_ascii_digit() || bytes[index] == b'.') {
                index += 1;
            }
            let candidate = &text[start..index];
            if valid_version(candidate) {
                return Some(candidate);
            }
        }
        index += 1;
    }
    None
}

fn version_tuple(version: &str) -> (u64, u64) {
    let mut parts = version.split('.');
    let major = parts.next().and_then(|part| part.parse().ok()).unwrap_or(0);
    let minor = parts.next().and_then(|part| part.parse().ok()).unwrap_or(0);
    (major, minor)
}

/// The backend retires stale public ids by re-pointing them at a newer model,
/// which shows up as a display name from a different version. Surface only
/// identities whose display name agrees with the id's own version.
pub(super) fn display_matches_id(id: &str, entry: &Value) -> bool {
    let Some(display) = entry.get("displayName").and_then(Value::as_str) else {
        return true;
    };
    let (Some(display_version), Some(id_version)) = (
        first_version(display),
        id.strip_prefix("gemini-")
            .and_then(|rest| split_version(rest).map(|(version, _)| version)),
    ) else {
        return true;
    };
    version_tuple(id_version) == version_tuple(display_version)
}

/// Reduce a recorded service payload to this source's surface: public Gemini
/// chat identities only, sanitized to model-selection metadata, with this
/// source's default main model named when the lineup carries it.
pub(super) fn project_payload(payload: &Value) -> Value {
    let mut models = Map::new();
    if let Some(entries) = payload.get("models").and_then(Value::as_object) {
        for (id, entry) in entries {
            if !is_public_chat_id(id) || !display_matches_id(id, entry) {
                continue;
            }
            if entry.get("isInternal").and_then(Value::as_bool) == Some(true) {
                continue;
            }
            models.insert(id.clone(), entry.clone());
        }
    }
    let default = if models.contains_key(DEFAULT_MAIN_MODEL) {
        DEFAULT_MAIN_MODEL
    } else {
        models
            .keys()
            .next()
            .map(String::as_str)
            .unwrap_or(DEFAULT_MAIN_MODEL)
    };
    let projected = json!({ "defaultAgentModelId": default, "models": Value::Object(models) });
    super::super::antigravity::catalog::public_payload(&projected)
}

/// The sanitized lineup shipped with the build, recorded from a live payload.
/// It carries model identities, protocol metadata and capability declarations
/// only — no account, project, quota or credential data.
pub(super) fn bundled_payload() -> Value {
    static BUNDLED: OnceLock<Value> = OnceLock::new();
    BUNDLED
        .get_or_init(|| {
            let mut payload: Value =
                serde_json::from_str(include_str!("bundled_catalog.json")).unwrap_or(Value::Null);
            if let Some(object) = payload.as_object_mut() {
                object.insert("catalogBaseline".into(), json!(true));
            }
            payload
        })
        .clone()
}

pub(super) fn model_details(payload: &Value, model: &str) -> Option<Value> {
    payload
        .get("models")
        .and_then(|models| models.get(model))
        .filter(|details| details.is_object())
        .cloned()
}

pub(super) fn is_chat_model(entry: &Value) -> bool {
    super::super::conversation_catalog::is_conversation_model("gemini", entry)
}

/// Full row set for this source. A bundled baseline carries identities and
/// capabilities, never a local account's entitlement decision, so every known
/// chat model stays selectable while the service verdict is in flight.
pub(super) fn models_rows(payload: &Value) -> Vec<Value> {
    let mut models = super::super::antigravity::catalog::normalize_models(payload);
    if payload.get("catalogBaseline").and_then(Value::as_bool) == Some(true) {
        for model in &mut models {
            if is_chat_model(model) && model.get("disabled").and_then(Value::as_bool) != Some(true)
            {
                model["available"] = json!(false);
                model["entitlementUnknown"] = json!(true);
                model["unavailableReason"] = json!(
                    "Account access is confirmed when the model service answers. The model remains selectable."
                );
            }
        }
    }
    models
}
