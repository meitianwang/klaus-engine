//! Source-qualified model choices shared by the picker facade and session startup.
//! Selection never chooses a different model just because a credential is absent.

use std::path::Path;

use klaus_agent_core::config_store;
use klaus_agent_core::engine::session::is_supported_model_source;
use serde_json::{Map, Value};

use super::model_sources;
use super::wire::{ModelSelectionResolution, ModelSelectionResolveParams};

fn scoped_value<'a>(source: &str, selection: &'a str) -> Result<&'a str, String> {
    let selection = selection.trim();
    if let Some((prefix, value)) = selection.split_once(':') {
        if is_supported_model_source(prefix) {
            if prefix != source {
                return Err(format!(
                    "model selection {selection} does not belong to source {source}"
                ));
            }
            if value.trim().is_empty() {
                return Err("a source-qualified model selection must name a model".into());
            }
            return Ok(value.trim());
        }
    }
    Ok(selection)
}

/// Resolve the identity only. Credentials remain the provider initializer's job.
pub(super) async fn resolve(
    config_path: &Path,
    params: &ModelSelectionResolveParams,
) -> Result<ModelSelectionResolution, String> {
    let source = params.source.trim();
    if !is_supported_model_source(source) {
        return Err(format!("unsupported model source: {source}"));
    }
    let value = scoped_value(source, &params.selection)?;
    let value = match source {
        "kiro" => {
            super::kiro::set_config_root(&model_sources::config_root_of(config_path));
            super::kiro::main_model(&params.selection, &params.model_tier_selections).await?
        }
        "trae" => {
            super::trae::set_config_root(&model_sources::config_root_of(config_path));
            super::trae::main_model(value, &params.model_tier_selections).await?
        }
        "antigravity" => {
            super::antigravity::set_config_root(&model_sources::config_root_of(config_path));
            super::antigravity::main_model(value, &params.model_tier_selections).await?
        }
        "custom" => {
            let document = config_store::load(config_path)
                .await
                .map_err(|error| format!("cannot read model providers: {error}"))?;
            let providers = document.product_section("modelProviders");
            let empty = Map::new();
            custom_model_id(
                providers.as_object().unwrap_or(&empty),
                &params.selection,
                &params.model_tier_selections,
            )?
            .to_string()
        }
        "codex" => {
            model_sources::codex_main_model(
                value,
                &model_sources::config_root_of(config_path),
                &params.model_tier_selections,
            )
            .await
        }
        "subscription" => subscription_choice(value).to_string(),
        "qoder" => model_sources::qoder_main_model(value, &params.model_tier_selections),
        "qwenwork" => {
            // The catalog head read follows the config root, so the facade must
            // point the source at the caller's root like the session path does.
            super::qwenwork::set_config_root(&model_sources::config_root_of(config_path));
            model_sources::qwenwork_main_model(value, &params.model_tier_selections)
        }
        "cursor" => {
            super::cursor::set_config_root(&model_sources::config_root_of(config_path));
            model_sources::cursor_main_model(value, &params.model_tier_selections).await?
        }
        "workbuddy" => {
            super::workbuddy::set_config_root(&model_sources::config_root_of(config_path));
            model_sources::workbuddy_main_model(value, &params.model_tier_selections)
        }
        "kimi" => {
            // The catalog head read follows the config root, so the facade must
            // point the source at the caller's root like the session path does.
            super::kimi::set_config_root(&model_sources::config_root_of(config_path));
            model_sources::kimi_main_model(value, &params.model_tier_selections)
        }
        "bailian" => {
            super::bailian::set_config_root(&model_sources::config_root_of(config_path));
            model_sources::bailian_main_model(value, &params.model_tier_selections)
        }
        "minimax" => {
            super::minimax::set_config_root(&model_sources::config_root_of(config_path));
            model_sources::minimax_main_model(value, &params.model_tier_selections)
        }
        "zhipu" => {
            super::zhipu::set_config_root(&model_sources::config_root_of(config_path));
            model_sources::zhipu_main_model(value, &params.model_tier_selections)
        }
        "ark" => {
            super::ark::set_config_root(&model_sources::config_root_of(config_path));
            model_sources::ark_main_model(value, &params.model_tier_selections)
        }
        _ => unreachable!("validated source"),
    };
    Ok(ModelSelectionResolution {
        source: source.to_string(),
        selection: format!("{source}:{value}"),
        model_tier_targets: None,
    })
}

/// A subscription picker persists tier aliases, so reloading a selection and
/// starting a session apply the user's tier mapping exactly once.
pub(super) fn subscription_choice(selection: &str) -> &str {
    match selection {
        // A bare default names the lineup's top tier model. The `medium` tier word
        // stays on the mid model: a tier is what auxiliary work resolves through,
        // so promoting it would re-price sub-agents and summaries nobody asked about.
        "" | "auto" => "opus",
        "high" => "opus",
        "medium" => "sonnet",
        "low" => "haiku",
        other => other,
    }
}

/// The configured ID that both the picker and provider startup select. Default
/// choice depends only on configuration, never on which keys happen to exist.
pub(super) fn custom_model_id<'a>(
    providers: &'a Map<String, Value>,
    selection: &str,
    tier_selections: &Map<String, Value>,
) -> Result<&'a str, String> {
    if let Some(id) = selection.trim().strip_prefix("custom:") {
        return providers
            .get_key_value(id.trim())
            .map(|(id, _)| id.as_str())
            .ok_or_else(|| {
                format!("no configured model has the id {id} — `custom:{id}` names nothing")
            });
    }
    let selection = scoped_value("custom", selection)?;
    // A tier name resolves through the host's per-source tier map; anything
    // else already names a provider entry id.
    let picked = match selection {
        "high" | "medium" | "low" => tier_selections
            .get(selection)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|id| !id.is_empty()),
        "" | "auto" => None,
        id => Some(id),
    };
    picked
        .and_then(|id| providers.get_key_value(id).map(|(id, _)| id.as_str()))
        .or_else(|| {
            providers
                .iter()
                .find(|(_, entry)| entry.get("default").and_then(Value::as_bool) == Some(true))
                .map(|(id, _)| id.as_str())
        })
        .or_else(|| providers.iter().next().map(|(id, _)| id.as_str()))
        .ok_or_else(|| {
            format!(
                "cannot resolve model {selection}: no models are configured in [modelProviders]"
            )
        })
}
