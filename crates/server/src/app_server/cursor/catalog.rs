use super::{credential, state};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const CATALOG_TTL: Duration = Duration::from_secs(300);
// Include refresh-lock admission and token renewal in the catalog budget, leaving
// time for the facade to report a source error before a client's read deadline.
const CATALOG_TIMEOUT: Duration = Duration::from_secs(12);
const OWNED_CATALOG_FILE: &str = "cursor-model-catalog.json";
const OWNED_PROJECTION_VERSION: u64 = 1;
const OWNED_CATALOG_LIMIT: u64 = 8 * 1024 * 1024;

fn text<'a>(value: &'a Value, field: &str) -> Option<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}

fn clean_label(label: &str) -> String {
    let mut inside = false;
    let plain: String = label
        .chars()
        .filter(|c| match c {
            '<' => {
                inside = true;
                false
            }
            '>' => {
                inside = false;
                false
            }
            _ => !inside && !matches!(c, '\u{200b}' | '\u{200c}' | '\u{200d}' | '\u{feff}'),
        })
        .collect();
    plain
        .replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn automatic(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "" | "default" | "auto" | "auto-smart"
    )
}

fn context_parameter(parameters: &[Value]) -> Option<u64> {
    let value = parameters
        .iter()
        .find(|p| p["id"] == "context")?
        .get("value")?
        .as_str()?
        .to_ascii_lowercase();
    let (number, multiplier) = if let Some(v) = value.strip_suffix('k') {
        (v, 1_000f64)
    } else if let Some(v) = value.strip_suffix('m') {
        (v, 1_000_000f64)
    } else {
        (value.as_str(), 1f64)
    };
    let tokens = number.parse::<f64>().ok()? * multiplier;
    (tokens.is_finite() && tokens > 0. && tokens <= 100_000_000.).then_some(tokens as u64)
}

fn parameter_definitions(model: &Value) -> Vec<Value> {
    let mut definitions = Vec::new();
    for definition in model["parameterDefinitions"]
        .as_array()
        .into_iter()
        .flatten()
        .take(16)
    {
        let Some(id) = text(definition, "id")
            .filter(|id| id.len() <= 128 && !id.chars().any(char::is_control))
        else {
            continue;
        };
        let Some(name) = text(definition, "name").filter(|name| name.len() <= 1024) else {
            continue;
        };
        let parameter = &definition["parameterType"];
        let (kind, values) = match (
            parameter.get("booleanParameter"),
            parameter.get("enumParameter"),
        ) {
            (Some(value), None) => ("boolean", value),
            (None, Some(value)) => ("enum", value),
            _ => continue,
        };
        let mut normalized = Vec::new();
        for value in values["values"].as_array().into_iter().flatten().take(32) {
            let Some(raw) = text(value, "value")
                .filter(|value| value.len() <= 256 && !value.chars().any(char::is_control))
            else {
                continue;
            };
            let mut item = json!({"value":raw});
            if let Some(display) =
                text(value, "displayName").filter(|display| display.len() <= 1024)
            {
                item["displayName"] = json!(clean_label(display));
            }
            for field in ["increasesModelCost", "blockedByAdminAllowlist"] {
                if let Some(flag) = value[field].as_bool() {
                    item[field] = json!(flag);
                }
            }
            normalized.push(item);
        }
        if !normalized.is_empty() {
            definitions
                .push(json!({"id":id,"name":clean_label(name),"type":kind,"values":normalized}));
        }
    }
    if serde_json::to_vec(&definitions).is_ok_and(|bytes| bytes.len() <= 16 * 1024) {
        definitions
    } else {
        Vec::new()
    }
}

/// The account catalog's image flag is not the wire contract for every model.
///
/// `supportsImages` is false for grok-4.7, but a direct image request on that
/// id reads the pixels. A declared true flag still wins for every other model.
fn reads_images(model: &Value, model_id: &str) -> bool {
    let declared = model
        .get("supportsImage")
        .or_else(|| model.get("supportsImages"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    declared || model_id == "grok-4.7"
}

/// Flatten the account catalog without inventing models, variants or routing.
pub(super) fn normalize(value: &Value) -> Vec<Value> {
    let models = value
        .get("models")
        .and_then(Value::as_array)
        .or_else(|| value.as_array());
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for model in models.into_iter().flatten() {
        let Some(model_id) = text(model, "serverModelName").or_else(|| text(model, "name")) else {
            continue;
        };
        let display = clean_label(text(model, "clientDisplayName").unwrap_or(model_id));
        if automatic(model_id) || automatic(&display) {
            continue;
        }
        let definitions = parameter_definitions(model);
        let fallback = vec![json!({})];
        let variants = model
            .get("variants")
            .and_then(Value::as_array)
            .filter(|v| !v.is_empty())
            .unwrap_or(&fallback);
        for variant in variants {
            let mut parameters: Vec<Value> = variant
                .get("parameterValues")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|p| Some(json!({"id":text(p,"id")?,"value":p.get("value")?.as_str()?})))
                .collect();
            parameters.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
            let max_mode = variant
                .get("isMaxMode")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            // Canonicalize parameter order and include max mode: legacy slugs can
            // name several context variants and cannot be an inference identity.
            let key = format!(
                "{}[{}]{}",
                model_id,
                parameters
                    .iter()
                    .map(|p| format!(
                        "{}={}",
                        p["id"].as_str().unwrap_or_default(),
                        p["value"].as_str().unwrap_or_default()
                    ))
                    .collect::<Vec<_>>()
                    .join(","),
                if max_mode { "@max" } else { "" }
            );
            if !seen.insert(key.clone()) {
                continue;
            }
            let mut label = clean_label(text(variant, "displayName").unwrap_or(&display));
            if automatic(&label) {
                continue;
            }
            if max_mode {
                label.push_str(" Max");
            }
            let reasoning = parameters.iter().any(|p| {
                matches!(
                    p["id"].as_str(),
                    Some("reasoning" | "reasoning_effort" | "effort" | "thinking")
                ) && !matches!(p["value"].as_str(), Some("false" | "none" | "off"))
            });
            let context = context_parameter(&parameters)
                .or_else(|| {
                    if max_mode {
                        model
                            .get("contextTokenLimitForMaxMode")
                            .and_then(Value::as_u64)
                    } else {
                        None
                    }
                })
                .or_else(|| model.get("contextTokenLimit").and_then(Value::as_u64));
            let mut entry = json!({
                "key":key, "modelId":model_id, "modelName":display,"parameterDefinitions":definitions,"display_name":label, "description":clean_label(text(model,"description").unwrap_or("")),
                "icon":"cursor", "is_vl":reads_images(model, model_id),
                "is_reasoning":reasoning,
                "requestedModel":{"modelId":model_id,"maxMode":max_mode,"parameters":parameters},
                "isDefault":variant.get("isDefaultNonMaxConfig").and_then(Value::as_bool).unwrap_or(false) && !max_mode,
            });
            if let Some(effort) = parameters
                .iter()
                .find(|p| {
                    matches!(
                        p["id"].as_str(),
                        Some("reasoning" | "reasoning_effort" | "effort")
                    )
                })
                .and_then(|p| p["value"].as_str())
                .filter(|value| matches!(*value, "low" | "medium" | "high" | "xhigh" | "max"))
            {
                entry["efforts"] = json!([effort]);
                entry["defaultEffort"] = json!(effort);
            }
            if let Some(value) = context {
                entry["contextTokens"] = json!([value]);
                entry["max_input_tokens"] = json!(value);
            }
            if let Some(value) = model
                .get("maxOutputTokens")
                .or_else(|| model.get("outputTokenLimit"))
                .and_then(Value::as_u64)
            {
                entry["max_output_tokens"] = json!(value);
            }
            if let Some(slug) = text(variant, "legacySlug") {
                entry["legacySlug"] = json!(slug);
            }
            if let Some(representation) = text(variant, "variantStringRepresentation") {
                entry["variantStringRepresentation"] = json!(representation);
            }
            super::super::conversation_catalog::mark_purpose(
                "cursor",
                model,
                entry.as_object_mut().unwrap(),
            );
            out.push(entry);
        }
    }
    promote_default_context(&mut out);
    out.sort_by_key(|entry| entry["isDefault"] != true);
    out
}

fn non_context_parameters(entry: &Value) -> Vec<(String, String)> {
    entry["requestedModel"]["parameters"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|parameter| {
            let id = parameter["id"].as_str()?;
            if id == "context" {
                return None;
            }
            Some((
                id.to_string(),
                parameter["value"].as_str().unwrap_or("").to_string(),
            ))
        })
        .collect()
}

fn context_tokens(entry: &Value) -> u64 {
    entry
        .get("max_input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0)
}

/// Same effort and speed, longest context tier the catalog actually offers.
pub(super) fn with_max_context<'a>(models: &'a [Value], selected: &'a Value) -> &'a Value {
    let model_id = selected["requestedModel"]["modelId"].as_str();
    let parameters = non_context_parameters(selected);
    let current = context_tokens(selected);
    models
        .iter()
        .filter(|candidate| {
            candidate["requestedModel"]["modelId"].as_str() == model_id
                && non_context_parameters(candidate) == parameters
                && context_tokens(candidate) > current
        })
        .max_by_key(|candidate| context_tokens(candidate))
        .unwrap_or(selected)
}

fn promote_default_context(entries: &mut [Value]) {
    let plan: Vec<(String, String)> = entries
        .iter()
        .filter(|entry| entry["isDefault"] == true)
        .filter_map(|entry| {
            let from = text(entry, "key")?;
            let to = text(with_max_context(entries, entry), "key")?;
            (from != to).then(|| (from.to_string(), to.to_string()))
        })
        .collect();
    for (from, to) in plan {
        if let Some(entry) = entries
            .iter_mut()
            .find(|entry| text(entry, "key") == Some(from.as_str()))
        {
            entry["isDefault"] = json!(false);
        }
        if let Some(entry) = entries
            .iter_mut()
            .find(|entry| text(entry, "key") == Some(to.as_str()))
        {
            entry["isDefault"] = json!(true);
        }
    }
}

pub async fn fetch_models(force_refresh: bool) -> Result<Vec<Value>, String> {
    super::diagnostics::result("catalog_fetch", fetch_models_timed(force_refresh)).await
}

async fn fetch_models_timed(force_refresh: bool) -> Result<Vec<Value>, String> {
    super::diagnostics::event(
        "budget",
        json!({"timeoutMs":12000,"forceRefresh":force_refresh}),
    );
    let started = Instant::now();
    tokio::time::timeout(CATALOG_TIMEOUT, fetch_models_inner(force_refresh))
        .await
        .map_err(|_| {
            super::diagnostics::event(
                "deadline",
                json!({"timeoutMs":12000,"totalMs":started.elapsed().as_millis() as u64}),
            );
            "cursor_network_error".to_string()
        })?
}

fn cached_models() -> Result<Option<(u64, Instant, Vec<Value>)>, String> {
    let (_, generation) = credential::snapshot()?;
    let state = state().lock().unwrap_or_else(|p| p.into_inner());
    if state.generation != generation {
        return Err("cursor_credential_changed".into());
    }
    Ok(state
        .catalog
        .as_ref()
        .map(|(fetched, models)| (generation, *fetched, models.clone())))
}

async fn fetch_models_inner(force_refresh: bool) -> Result<Vec<Value>, String> {
    if !force_refresh {
        if let Ok(Some((_, fetched, models))) = cached_models() {
            if fetched.elapsed() < CATALOG_TTL {
                super::diagnostics::event(
                    "cache_hit",
                    json!({"ageMs":fetched.elapsed().as_millis() as u64,"count":models.len(),"stale":false}),
                );
                return Ok(models);
            }
        }
    }
    let auth_started = Instant::now();
    super::diagnostics::event("resolve_auth_start", json!({}));
    super::auth::ensure_valid().await?;
    super::diagnostics::event(
        "resolve_auth_ready",
        json!({"waitMs":auth_started.elapsed().as_millis() as u64}),
    );
    let (_, generation) = credential::snapshot()?;
    if !force_refresh {
        let state = state().lock().unwrap_or_else(|p| p.into_inner());
        if let Some((fetched, models)) = &state.catalog {
            if fetched.elapsed() < CATALOG_TTL {
                return Ok(models.clone());
            }
        }
    }
    let (value, fetched_generation) = credential::unary(
        "/aiserver.v1.AiService/AvailableModels",
        json!({"includeLongContextModels":true,"useModelParameters":true}),
    )
    .await?;
    if generation != fetched_generation {
        return Err("cursor_credential_changed".into());
    }
    let models = normalize(&value);
    super::diagnostics::event("catalog_normalized", json!({"count":models.len()}));
    if models.is_empty() {
        return Err("cursor_models_unavailable".into());
    }
    let mut state = state().lock().unwrap_or_else(|p| p.into_inner());
    if state.generation != generation {
        return Err("cursor_credential_changed".into());
    }
    super::diagnostics::event(
        "cache_commit",
        json!({"generation":generation,"count":models.len()}),
    );
    state.catalog = Some((Instant::now(), models.clone()));
    let root = super::config_root(&state);
    let fingerprint = credential::read(&root)
        .ok()
        .map(|stored| stored.credential_id);
    drop(state);
    if let Some(fingerprint) = fingerprint {
        write_owned_catalog(&root, &models, &fingerprint);
    }
    Ok(models)
}

struct CatalogRefresh(u64);

impl Drop for CatalogRefresh {
    fn drop(&mut self) {
        let mut state = state().lock().unwrap_or_else(|p| p.into_inner());
        if state.generation == self.0 {
            state.catalog_refreshing = false;
        }
    }
}

fn owned_catalog_path(root: &Path) -> PathBuf {
    root.join(OWNED_CATALOG_FILE)
}

/// The signed-in credential this process may paint a catalog for.
///
/// The fingerprint is the stored credential id, not the in-memory generation:
/// a restart keeps the same account file and can paint its snapshot, while a
/// replaced login gets a new id and must not inherit the previous account.
fn catalog_owner() -> Result<(u64, String, PathBuf), String> {
    let (_, generation) = credential::snapshot()?;
    let state = state().lock().unwrap_or_else(|p| p.into_inner());
    if state.generation != generation {
        return Err("cursor_credential_changed".into());
    }
    let root = super::config_root(&state);
    let stored = credential::read(&root)?;
    Ok((generation, stored.credential_id, root))
}

fn read_owned_catalog(root: &Path, fingerprint: &str) -> Option<Vec<Value>> {
    let path = owned_catalog_path(root);
    let len = std::fs::metadata(&path).ok()?.len();
    if len == 0 || len > OWNED_CATALOG_LIMIT {
        return None;
    }
    let value: Value = serde_json::from_slice(&std::fs::read(path).ok()?).ok()?;
    if value.get("projectionVersion").and_then(Value::as_u64) != Some(OWNED_PROJECTION_VERSION)
        || value.get("fingerprint").and_then(Value::as_str) != Some(fingerprint)
    {
        return None;
    }
    value
        .get("models")
        .and_then(Value::as_array)
        .cloned()
        .filter(|models| !models.is_empty())
}

fn write_owned_catalog(root: &Path, models: &[Value], fingerprint: &str) {
    let Ok(bytes) = serde_json::to_vec(&json!({
        "projectionVersion": OWNED_PROJECTION_VERSION,
        "fingerprint": fingerprint,
        "models": models,
    })) else {
        return;
    };
    if bytes.len() as u64 > OWNED_CATALOG_LIMIT {
        return;
    }
    let path = owned_catalog_path(root);
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if std::fs::write(&path, bytes).is_err() {
        return;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
    }
}

fn remember_stale(generation: u64, models: Vec<Value>) -> Result<(), String> {
    let mut state = state().lock().unwrap_or_else(|p| p.into_inner());
    if state.generation != generation {
        return Err("cursor_credential_changed".into());
    }
    if state.catalog.is_none() {
        let fetched = Instant::now()
            .checked_sub(CATALOG_TTL)
            .unwrap_or_else(Instant::now);
        state.catalog = Some((fetched, models));
    }
    Ok(())
}

fn schedule_refresh(generation: u64) -> Result<(), String> {
    let refresh = {
        let mut state = state().lock().unwrap_or_else(|p| p.into_inner());
        if state.generation != generation {
            return Err("cursor_credential_changed".into());
        }
        if state.catalog_refreshing {
            None
        } else {
            state.catalog_refreshing = true;
            Some(CatalogRefresh(generation))
        }
    };
    super::diagnostics::event(
        "background_refresh",
        json!({"scheduled": refresh.is_some()}),
    );
    if let Some(refresh) = refresh {
        super::diagnostics::background(async move {
            let _refresh = refresh;
            if credential::check_generation(_refresh.0).is_ok() {
                let _ = fetch_models(true).await;
            }
        });
    }
    Ok(())
}

pub async fn read_models(
    read: super::super::wire::QoderCatalogRead,
    force_refresh: bool,
) -> Result<Vec<Value>, String> {
    super::diagnostics::result("catalog_read", read_models_inner(read, force_refresh)).await
}

async fn read_models_inner(
    read: super::super::wire::QoderCatalogRead,
    force_refresh: bool,
) -> Result<Vec<Value>, String> {
    super::diagnostics::event(
        "catalog_read_mode",
        json!({"cachedRead":read == super::super::wire::QoderCatalogRead::Cached,"forceRefresh":force_refresh}),
    );
    if !force_refresh {
        if let Ok(Some((generation, fetched, models))) = cached_models() {
            let stale = fetched.elapsed() >= CATALOG_TTL;
            super::diagnostics::event(
                "cache_hit",
                json!({"ageMs":fetched.elapsed().as_millis() as u64,"count":models.len(),"stale":stale,"generation":generation}),
            );
            if stale {
                schedule_refresh(generation)?;
            }
            return Ok(models);
        }
        match catalog_owner() {
            Ok((generation, fingerprint, root)) => {
                if let Some(models) = read_owned_catalog(&root, &fingerprint) {
                    super::diagnostics::event(
                        "owned_snapshot",
                        json!({"count":models.len(),"generation":generation,"stale":true}),
                    );
                    remember_stale(generation, models.clone())?;
                    schedule_refresh(generation)?;
                    return Ok(models);
                }
            }
            Err(error) if error == "cursor_credential_changed" => {}
            Err(error) => return Err(error),
        }
    }
    super::diagnostics::event("cache_miss", json!({}));
    fetch_models(force_refresh).await
}

pub async fn resolved_model(selection: &str) -> Result<String, String> {
    let models = read_models(super::super::wire::QoderCatalogRead::Cached, false).await?;
    let selected = if matches!(
        selection,
        "" | "auto" | "default" | "auto-smart" | "high" | "medium" | "low"
    ) {
        models
            .iter()
            .find(|m| m["isDefault"] == true)
            .or_else(|| models.first())
    } else {
        models
            .iter()
            .find(|m| m["key"] == selection)
            .or_else(|| {
                models.iter().find(|m| {
                    m["variantStringRepresentation"] == selection || m["legacySlug"] == selection
                })
            })
            .or_else(|| {
                models
                    .iter()
                    .find(|m| m["requestedModel"]["modelId"] == selection && m["isDefault"] == true)
            })
            .or_else(|| {
                models
                    .iter()
                    .find(|m| m["requestedModel"]["modelId"] == selection)
            })
    }
    .ok_or("cursor_model_unavailable")?;
    let selected = with_max_context(&models, selected);
    text(selected, "key")
        .map(str::to_owned)
        .ok_or_else(|| "cursor_model_unavailable".into())
}

pub async fn model_capabilities(key: &str) -> Value {
    let model = super::lookup_model(key).await;
    let mut capabilities = json!({"vision":model.as_ref().is_some_and(|m| m["is_vl"] == true),"tools":true,"imageLimits":{"maxBase64Size":5242880},"maxMediaBlocks":4});
    if let Some(model) = model {
        if let Some(effort) = model.get("defaultEffort") {
            capabilities["effort"] = json!({"levels":[effort],"default":effort});
        }
        if let Some(context) = model.get("max_input_tokens") {
            capabilities["contextWindow"] = context.clone();
        }
        if let Some(output) = model.get("max_output_tokens") {
            capabilities["maxOutputTokens"] = output.clone();
        }
    }
    // Reasoning and fast variants are selections, not an independent override.
    capabilities
}
