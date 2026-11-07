//! `resolve_config` — THE crate-wide config resolver.
//!
//! Folds host-provided partial config layers over `EngineConfig` defaults with
//! governance-schema key/value validation, then runs serde shape validation and
//! the cross-field dependency checks. Re-exported from `engine::session` so the
//! established `crate::engine::session::resolve_config` call path is unchanged.

use std::collections::BTreeMap;

use crate::config_governance::{
    govern_config, ConfigGovernanceLayer, ConfigGovernanceSource, GovernedConfig,
};
use crate::runtime_values::ConfigJson;
use crate::schemas::config::{
    engine_config_governance_schema, normalize_sandbox_config, normalize_sandbox_workspace_roots,
    retain_supported_sandbox_features, EngineConfig, ResolvedConfig,
};

/// Resolve a partial config into a fully-defaulted, validated [`ResolvedConfig`].
///
/// The partial is a JSON object of the keys the host set. The governance layer
/// rejects unknown top-level keys and folds the partial over `EngineConfig`
/// defaults before serde performs nested shape/enum validation and the
/// cross-field dependency checks run.
pub fn resolve_config(partial: serde_json::Value) -> Result<ResolvedConfig, String> {
    let layer = config_layer_from_value(ConfigGovernanceSource::Project, partial)?;
    resolve_config_layers(vec![layer])
}

/// Resolve already-classified config layers into a fully-defaulted config.
pub fn resolve_config_layers(layers: Vec<ConfigGovernanceLayer>) -> Result<ResolvedConfig, String> {
    let layers = layers
        .into_iter()
        .map(|layer| {
            config_layer_from_value(
                layer.source,
                serde_json::Value::Object(layer.values.into_iter().collect()),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let governed = govern_config(&engine_config_governance_schema(), &layers)
        .map_err(|error| error.to_string())?;
    let mut values = governed.to_json_object();
    derive_web_search_access(&mut values);
    gate_agent_memory_children(&governed, &mut values);
    // Accept the retired boolean in old files but publish the effective value:
    // workflow is a permanent engine capability, independent of effort mode.
    values.insert("workflow".to_string(), serde_json::Value::Bool(true));
    let cfg_value = serde_json::Value::Object(values);
    let config = ConfigJson::validate_config_document(cfg_value).map_err(|e| e.to_string())?;
    let mut cfg: EngineConfig = config.deserialize_config().map_err(|e| e.to_string())?;
    // Backstop for a legacy `effort: "ultra"` that reached the struct without
    // passing a JSON layer (the layers above normalize with key presence, so an
    // explicit `ultraMode: false` already won before this point).
    cfg.normalize_legacy_ultra();
    cfg.validate()?;
    Ok(cfg)
}

fn derive_web_search_access(values: &mut serde_json::Map<String, serde_json::Value>) {
    let Some(mode) = values
        .get("sandbox_mode")
        .and_then(serde_json::Value::as_str)
    else {
        return;
    };
    let access = if mode == "danger-full-access" {
        "live"
    } else {
        "cached"
    };
    let Some(search) = values
        .get_mut("webSearch")
        .and_then(serde_json::Value::as_object_mut)
    else {
        return;
    };
    if search.contains_key("access")
        || search
            .get("mode")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|mode| ["off", "disabled", "cached", "indexed", "live"].contains(&mode))
    {
        return;
    }
    search.insert("access".into(), serde_json::Value::String(access.into()));
}

/// `agentMemory` is the subsystem's master switch: with it off, every child goes
/// off with it. Gating rather than refusing, because the switch is a GUI row —
/// the child rows the user set earlier are still in the config file, and turning
/// the subsystem off must not turn the file into a config error.
fn gate_agent_memory_children(
    governed: &GovernedConfig,
    values: &mut serde_json::Map<String, serde_json::Value>,
) {
    let parent_off = governed
        .get("agentMemory")
        .is_some_and(|entry| entry.value == serde_json::Value::Bool(false));
    if !parent_off {
        return;
    }
    for child in [
        "memoryRecall",
        "agentMemorySnapshot",
        "autoMemory",
        "autoDream",
    ] {
        values.insert(child.to_string(), serde_json::Value::Bool(false));
    }
}

pub fn config_layer_from_value(
    source: ConfigGovernanceSource,
    value: serde_json::Value,
) -> Result<ConfigGovernanceLayer, String> {
    config_layer_from_value_with_base(source, value, None)
}

pub(crate) fn config_layer_from_value_with_base(
    source: ConfigGovernanceSource,
    value: serde_json::Value,
    base_dir: Option<&std::path::Path>,
) -> Result<ConfigGovernanceLayer, String> {
    match value {
        serde_json::Value::Object(mut map) => {
            for warning in normalize_sandbox_config(&mut map)? {
                tracing::warn!(
                    code = %warning.code,
                    keys = ?warning.keys,
                    layer = ?source,
                    "{}",
                    warning.message
                );
            }
            normalize_sandbox_workspace_roots(&mut map, base_dir)?;
            // Rewrite the retired `effort: "ultra"` spelling per layer, before
            // the layers fold: presence of an explicit `ultraMode` in THIS
            // layer decides whether the orchestration flag is filled in.
            crate::schemas::config::normalize_legacy_ultra_fields(&mut map);
            // Keep supported platform preferences; raw feature metadata stays outside execution.
            retain_supported_sandbox_features(&mut map);
            Ok(ConfigGovernanceLayer {
                source,
                values: map
                    .into_iter()
                    // Main conversations no longer have a global model-turn cap.
                    // Ignore the retired key at every config boundary so an older
                    // host or saved file cannot prevent a session from starting.
                    .filter(|(key, _)| key != "maxTurns")
                    .collect::<BTreeMap<_, _>>(),
            })
        }
        other => Err(format!(
            "config layer {source:?} must be an object, got {}",
            config_value_kind(&other)
        )),
    }
}

pub(crate) fn config_value_kind(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

/// The zero-config resolution (the common `resolveConfig()` call).
pub fn resolve_config_default() -> ResolvedConfig {
    resolve_config_layers(Vec::new()).expect("default engine configuration is valid")
}
