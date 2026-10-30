//! Public capability definitions enrich only models the account directory exposes.
use serde_json::{json, Value};
#[cfg(not(test))]
use std::{io::Read, path::PathBuf};
#[cfg(not(test))]
const LIMIT: usize = 2 * 1024 * 1024;
#[cfg(not(test))]
fn installed() -> Option<Value> {
    let mut paths = Vec::<PathBuf>::new();
    if let Some(path) = std::env::var_os("WORKBUDDY_PRODUCT_METADATA") {
        paths.push(path.into());
    }
    #[cfg(target_os = "macos")]
    for root in [
        PathBuf::from("/Applications"),
        PathBuf::from(std::env::var("HOME").unwrap_or_default()).join("Applications"),
    ] {
        for app in ["WorkBuddy AI.app", "WorkBuddy.app"] {
            paths.push(
                root.join(app)
                    .join("Contents/Resources/app.asar.unpacked/cli/product.json"),
            );
        }
    }
    for path in paths {
        let Ok(file) = std::fs::File::open(path) else {
            continue;
        };
        let mut bytes = Vec::new();
        if file
            .take((LIMIT + 1) as u64)
            .read_to_end(&mut bytes)
            .is_err()
            || bytes.len() > LIMIT
        {
            continue;
        }
        if let Ok(value) = serde_json::from_slice::<Value>(&bytes) {
            if value["models"].is_array() {
                return Some(value);
            }
        }
    }
    None
}
fn defaults() -> Value {
    let seed: Value = serde_json::from_str(include_str!("capability_seed.json"))
        .expect("valid capability definitions");
    #[cfg(not(test))]
    {
        installed().unwrap_or(seed)
    }
    #[cfg(test)]
    {
        seed
    }
}
pub(crate) fn merge_with(base: &Value, remote: &Value) -> Value {
    let source = remote.get("data").unwrap_or(remote);
    let mut result = source.clone();
    if let Some(fields) = result.as_object_mut() {
        fields.remove("endpoint");
        fields.remove("stagingEndpoint");
    }
    let mut features = base["productFeatures"]
        .as_object()
        .cloned()
        .unwrap_or_default();
    if let Some(remote) = source["productFeatures"].as_object() {
        features.extend(remote.clone());
    }
    result["productFeatures"] = Value::Object(features);
    if let Some(rows) = source["models"].as_array() {
        let base = base["models"].as_array();
        let models: Vec<Value> = rows
            .iter()
            .map(|row| {
                let mut fields = base
                    .and_then(|rows| {
                        rows.iter()
                            .find(|m| m["id"].is_string() && m["id"] == row["id"])
                    })
                    .and_then(Value::as_object)
                    .cloned()
                    .unwrap_or_default();
                if let Some(remote) = row.as_object() {
                    fields.extend(remote.clone());
                }
                Value::Object(fields)
            })
            .collect();
        result["models"] = json!(models);
    }
    result
}
fn media_models_with(base: &Value, config: &Value, tag: &str) -> Vec<Value> {
    base["models"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|model| {
            model["disabled"] != true
                && model["local"] != true
                && model["tags"]
                    .as_array()
                    .is_some_and(|tags| tags.iter().any(|value| value == tag))
                && config["models"]
                    .as_array()
                    .is_none_or(|rows| !rows.iter().any(|row| row["id"] == model["id"]))
                && config["availableModels"]
                    .as_array()
                    .filter(|ids| !ids.is_empty())
                    .is_none_or(|ids| ids.iter().any(|id| id == &model["id"]))
        })
        .cloned()
        .collect()
}
pub(crate) fn media_models(config: &Value, tag: &str) -> Vec<Value> {
    media_models_with(&defaults(), config, tag)
}
pub(crate) fn effective(remote: &Value) -> Value {
    merge_with(&defaults(), remote)
}
pub(crate) fn unresolved(config: &Value) -> Vec<Value> {
    config["models"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|m| m.get("supportsToolCall").is_none() && !m["tags"].is_array())
        .filter_map(|m| m["id"].as_str())
        .map(|id| json!({"id":id,"reason":"workbuddy_model_metadata_unavailable"}))
        .collect()
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sparse_overlay_restores_capabilities_but_never_adds_unlisted_models() {
        let base = json!({"models":[{"id":"chat","supportsToolCall":true,"supportsImages":true},{"id":"hidden","supportsToolCall":true},{"id":"image","tags":["text-to-image"]}],"productFeatures":{"DisableVoiceInput":true,"ImageGen":false}});
        let remote = json!({"data":{"models":[{"id":"chat","supportsImages":false},{"id":"image"},{"id":"unknown"}],"productFeatures":{"ImageGen":true}}});
        let v = merge_with(&base, &remote);
        assert_eq!(v["models"].as_array().unwrap().len(), 3);
        assert_eq!(v["models"][0]["supportsToolCall"], true);
        assert_eq!(v["models"][0]["supportsImages"], false);
        assert_eq!(v["models"][1]["tags"], json!(["text-to-image"]));
        assert_eq!(v["productFeatures"]["DisableVoiceInput"], true);
        assert_eq!(v["productFeatures"]["ImageGen"], true);
        assert_eq!(
            unresolved(&v),
            vec![json!({"id":"unknown","reason":"workbuddy_model_metadata_unavailable"})]
        );
    }
    #[test]
    fn media_definitions_fill_an_omitted_catalog_without_reviving_explicit_rows() {
        let base = json!({"models":[{"id":"image","tags":["text-to-image","image-to-image"]},{"id":"video","tags":["text-to-video"],"videoGeneration":{"minSeconds":4}},{"id":"chat","supportsToolCall":true}]});
        let config = json!({"models":[{"id":"chat"}]});
        assert_eq!(
            media_models_with(&base, &config, "text-to-image"),
            vec![base["models"][0].clone()]
        );
        assert_eq!(
            media_models_with(&base, &config, "text-to-video")[0]["videoGeneration"]["minSeconds"],
            4
        );
        for row in [
            json!({"id":"image","disabled":true}),
            json!({"id":"image","tags":null}),
            json!({"id":"image","local":true}),
        ] {
            assert!(media_models_with(&base, &json!({"models":[row]}), "text-to-image").is_empty());
        }
        assert!(
            media_models_with(&base, &json!({"availableModels":["chat"]}), "text-to-image")
                .is_empty()
        );
        assert!(media_models_with(&base, &config, "unknown").is_empty());
    }
    #[test]
    fn remote_metadata_cannot_replace_the_authenticated_service_origin() {
        let value = merge_with(
            &json!({}),
            &json!({"endpoint":"https://foreign.example","stagingEndpoint":"https://staging.example","models":[]}),
        );
        assert!(value.get("endpoint").is_none());
        assert!(value.get("stagingEndpoint").is_none());
    }
    #[test]
    fn explicit_null_and_false_are_not_replaced_by_defaults() {
        let v = merge_with(
            &json!({"models":[{"id":"m","supportsToolCall":true,"tags":["text-to-image"]}]}),
            &json!({"models":[{"id":"m","supportsToolCall":null,"tags":null}]}),
        );
        assert!(v["models"][0]["tags"].is_null());
        assert!(v["models"][0]["supportsToolCall"].is_null());
    }
}
