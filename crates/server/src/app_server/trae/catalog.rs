//! The source inventory retains private request routing outside public model rows.

use super::{auth, bounded_json, client, cloud_base, cloud_headers};
use serde_json::{json, Map, Value};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

#[derive(Clone)]
struct Catalog {
    generation: String,
    fetched_at: i64,
    payload: Value,
}
fn cache() -> &'static Mutex<Option<Catalog>> {
    static CACHE: OnceLock<Mutex<Option<Catalog>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(None))
}
fn refresh_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}
pub(super) fn forget() {
    *cache().lock().unwrap_or_else(|p| p.into_inner()) = None;
}

async fn payload(_warm: bool) -> Result<Value, String> {
    let generation = auth::generation().ok_or("trae_not_signed_in")?;
    let cached = cache()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .as_ref()
        .filter(|entry| entry.generation == generation && auth::now() - entry.fetched_at < 300_000)
        .cloned();
    if let Some(cached) = cached {
        return Ok(cached.payload);
    }
    let _lock = refresh_lock().lock().await;
    let cached = cache()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .as_ref()
        .filter(|entry| entry.generation == generation && auth::now() - entry.fetched_at < 300_000)
        .cloned();
    if let Some(cached) = cached {
        return Ok(cached.payload);
    }
    let credential = auth::credential(Some(&generation)).await?;
    let response = client()
        .get(format!("{}/api/ide/v1/get_detail_param", cloud_base()))
        .query(&[("function", "chat_v3"), ("need_prompt", "false")])
        .headers(cloud_headers(&credential)?)
        .timeout(Duration::from_secs(20))
        .send()
        .await
        .map_err(|_| "trae_catalog_transport_failed")?;
    let request_pin = response
        .headers()
        .get("x-request-pin")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty() && value.len() <= 4096)
        .map(str::to_owned);
    let requested_at = response
        .headers()
        .get("x-request-at")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty() && value.len() <= 64)
        .map(str::to_owned);
    let mut payload = bounded_json(response).await?;
    if let Some(value) = request_pin {
        payload["_requestPin"] = json!(value);
    }
    if let Some(value) = requested_at {
        payload["_requestedAt"] = json!(value);
    }
    if !payload["config_info_list"].is_array() {
        return Err("trae_catalog_invalid".into());
    }
    if !auth::is_current(&generation) {
        return Err("trae_credential_changed".into());
    }
    *cache().lock().unwrap_or_else(|p| p.into_inner()) = Some(Catalog {
        generation,
        fetched_at: auth::now(),
        payload: payload.clone(),
    });
    Ok(payload)
}

fn object_json(value: &Value) -> Value {
    if let Some(text) = value.as_str() {
        serde_json::from_str(text).unwrap_or(Value::Null)
    } else {
        value.clone()
    }
}
fn native_efforts(config: &Value) -> Vec<String> {
    config
        .pointer("/reasoning_effort_config/options")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter(|value| {
            !value.is_empty() && value.len() <= 64 && !value.chars().any(char::is_control)
        })
        .map(str::to_owned)
        .collect()
}
fn native_default(config: &Value) -> Option<&str> {
    config
        .pointer("/reasoning_effort_config/default_level")
        .and_then(Value::as_str)
}
fn variant_key(base: &str, effort: &str) -> String {
    format!("{base}::effort={}", urlencoding::encode(effort))
}

/// Prefer the declared Max route and retain models that only declare a dev route.
pub(super) fn request_route(config: &Value) -> Option<(&Value, &'static str)> {
    let details = config["model_detail_list"].as_array()?;
    if let Some(detail) = details.iter().find(|detail| {
        detail["model_name"]
            .as_str()
            .is_some_and(|name| name.ends_with("__max"))
    }) {
        return Some((detail, "max"));
    }
    details.first().map(|detail| (detail, "dev"))
}

pub(super) fn selection_parts(selection: &str) -> Result<(&str, Option<String>), String> {
    match selection.split_once("::effort=") {
        Some((base, level)) if !base.is_empty() && !level.is_empty() => {
            let level = urlencoding::decode(level)
                .map_err(|_| "trae_model_invalid")?
                .into_owned();
            if variant_key(base, &level) != selection {
                return Err("trae_model_invalid".into());
            }
            Ok((base, Some(level)))
        }
        Some(_) => Err("trae_model_invalid".into()),
        None => Ok((selection, None)),
    }
}

pub(super) async fn rows(warm: bool) -> Result<Vec<Value>, String> {
    Ok(normalize(&payload(warm).await?))
}

pub(super) fn normalize(payload: &Value) -> Vec<Value> {
    payload["config_info_list"].as_array().into_iter().flatten().filter_map(|config|{
        let key=config["config_name"].as_str().filter(|key|!key.is_empty()&&key.len()<=256)?;
        let (detail,context_mode)=request_route(config)?;
        let display=&config["display_config"];
        let name=display["display_name"].as_str().filter(|label|!label.is_empty()).unwrap_or(key);
        let native=object_json(&config["extra_config"]);
        let tools=native["native_function_call"].as_bool().unwrap_or(false);
        let vision=display["multimodal"].as_bool().unwrap_or(false);
        let thinking=config.pointer("/reasoning_effort_config/support_thinking").and_then(Value::as_bool)
            .unwrap_or(display["model_capability"].as_str()==Some("reasoning_model"));
        let context=config["context_window_tokens"][context_mode].as_u64().filter(|value|*value>0);
        let output=detail["max_tokens"].as_u64().filter(|value|*value>0);
        let mut capabilities=json!({"tools":tools,"vision":vision,"nativePdf":false,"structuredOutput":false,"webSearch":false});
        if let Some(context)=context{capabilities["contextWindow"]=json!(context);}
        if let Some(output)=output{capabilities["maxOutputTokens"]=json!(output);}
        if thinking{capabilities["thinking"]=json!({"adaptive":false,"interleaved":false,"canDisableThinking":false});}
        let hidden=config["is_invisible_to_user"].as_bool()==Some(true);
        let chat=config["usage"].as_str()==Some("chat_completion");
        let available=config["config_switch"].as_bool().unwrap_or(true);
        let efforts=native_efforts(config);
        let mut variants=vec![json!({"value":format!("trae:{key}"),"nativeEffort":native_default(config),
            "isDefault":true,"available":available,"contextTokens":context,"mediaInputMimeTypes":[]})];
        variants.extend(efforts.iter().map(|effort|json!({"value":format!("trae:{}",variant_key(key,effort)),"nativeEffort":effort,
            "isDefault":false,"available":available,"contextTokens":context,"mediaInputMimeTypes":[]})));
        let mut row=json!({"key":key,"id":key,"display_name":name,"displayName":name,"name":name,"label":name,
            "contextWindow":context,"maxOutputTokens":output,"supportsThinking":thinking,"supportsVision":vision,"supportsToolCalls":tools,
            "capabilities":capabilities,"isDefault":config["is_default"].as_bool().unwrap_or(false),"available":available,
            "purpose":if chat{"chat"}else{"internal"},"efforts":[],"canDisableThinking":false,
            "providerMetadata":{"configName":key,"wireModelName":detail["model_name"],"configSource":config["config_source"],
                "nativeReasoningEfforts":efforts,"nativeDefaultEffort":native_default(config)},
        });
        if hidden||!chat {row["visibility"]=json!("hidden");row["conversationModel"]=json!(false);}
        if variants.len()>1{row["traeFamilyId"]=json!(key);row["traeVariants"]=json!(variants);}
        Some(row)
    }).collect()
}

pub(super) async fn selected(selection: &str) -> Result<(Value, Value, Option<String>), String> {
    let payload = payload(true).await?;
    let (base, effort) = selection_parts(selection)?;
    let mut config = payload["config_info_list"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|config| config["config_name"].as_str() == Some(base))
        .cloned()
        .ok_or("trae_model_unavailable")?;
    for field in ["_requestPin", "_requestedAt"] {
        if let Some(value) = payload.get(field) {
            config[field] = value.clone();
        }
    }
    if effort
        .as_ref()
        .is_some_and(|effort| !native_efforts(&config).contains(effort))
    {
        return Err("trae_reasoning_effort_unsupported".into());
    }
    let detail = request_route(&config)
        .ok_or("trae_model_unavailable")?
        .0
        .clone();
    Ok((config, detail, effort))
}

pub(super) fn expanded_rows(rows: &[Value]) -> Vec<Value> {
    rows.iter()
        .flat_map(|row| {
            let mut values = vec![row.clone()];
            for variant in row["traeVariants"].as_array().into_iter().flatten() {
                if let Some(id) = variant["value"]
                    .as_str()
                    .and_then(|value| value.strip_prefix("trae:"))
                {
                    if row["key"].as_str() == Some(id) {
                        continue;
                    }
                    let mut entry = row.clone();
                    entry["key"] = json!(id);
                    values.push(entry);
                }
            }
            values
        })
        .collect()
}

fn quantity(value: &Value) -> Option<Value> {
    if let Some(number) = value.as_i64().filter(|number| *number >= 0) {
        return Some(json!(number));
    }
    if let Some(number) = value
        .as_f64()
        .filter(|number| number.is_finite() && *number >= 0.0)
    {
        return Some(json!(number));
    }
    if let Some(text) = value.as_str().filter(|text| {
        !text.is_empty() && text.len() <= 40 && text.bytes().all(|byte| byte.is_ascii_digit())
    }) {
        return Some(json!(text));
    }
    None
}

pub(super) fn normalize_usage(usage: &Value, status: &Value, account: Option<&str>) -> Value {
    let mut result =
        json!({"signedIn":true,"account":account,"plan":status["user_pay_identity_str"]});
    let mut windows = Vec::new();
    for (index, pack) in usage["user_entitlement_pack_list"]
        .as_array()
        .into_iter()
        .flatten()
        .enumerate()
    {
        let base = &pack["entitlement_base_info"];
        let id = base["entitlement_id"]
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| format!("pack-{index}"));
        let label = pack["group_name"]
            .as_str()
            .filter(|value| !value.is_empty())
            .unwrap_or("Credits");
        let mut window = Map::new();
        window.insert("id".into(), json!(id));
        window.insert("name".into(), json!(label));
        window.insert("label".into(), json!(label));
        window.insert("unit".into(), json!("credits"));
        window.insert("status".into(), pack["status"].clone());
        if let Some(limit) = quantity(&base["quota"]["credits_limit"]) {
            window.insert("total".into(), limit);
        }
        if base["quota"]["credits_limit"].as_i64() == Some(-1) {
            window.insert("unlimited".into(), json!(true));
        }
        if let Some(used) = quantity(&pack["usage"]["credits_amount"]) {
            window.insert("used".into(), used);
        }
        if let Some(expires) = pack["expire_time"]
            .as_i64()
            .filter(|time| *time > 0 && *time < i64::MAX / 1000)
        {
            window.insert("expiresAt".into(), json!(expires * 1000));
        }
        windows.push(Value::Object(window));
    }
    if !windows.is_empty() {
        result["windows"] = json!(windows);
    }
    let summary = &usage["usage_summary"];
    let mut credits = Map::new();
    if let Some(total) = quantity(&summary["total_amount"]) {
        credits.insert("total".into(), total);
    }
    if let Some(used) = quantity(&summary["consumed_amount"]) {
        credits.insert("used".into(), used);
    }
    if let (Some(total), Some(used)) = (
        summary["total_amount"].as_i64(),
        summary["consumed_amount"].as_i64(),
    ) {
        if total >= 0 && used >= 0 {
            credits.insert("remaining".into(), json!((total - used).max(0)));
        }
    } else if let (Some(total), Some(used)) = (
        summary["total_amount"].as_f64(),
        summary["consumed_amount"].as_f64(),
    ) {
        if total.is_finite()
            && used.is_finite()
            && total >= 0.0
            && used >= 0.0
            && total <= 9_007_199_254_740_991.0
            && used <= 9_007_199_254_740_991.0
        {
            credits.insert("remaining".into(), json!((total - used).max(0.0)));
        }
    }
    if !credits.contains_key("remaining") {
        if let Some(packs) = usage["user_entitlement_pack_list"]
            .as_array()
            .filter(|packs| !packs.is_empty())
        {
            let balance = packs.iter().try_fold(0.0, |balance, pack| {
                let total = pack.pointer("/entitlement_base_info/quota/credits_limit")?;
                let used = pack
                    .pointer("/usage/credits_amount")
                    .filter(|value| !value.is_null())
                    .cloned()
                    .unwrap_or(json!(0));
                let remaining = if let (Some(total), Some(used)) = (total.as_i64(), used.as_i64()) {
                    if total < 0 || used < 0 {
                        return None;
                    }
                    (total - used).max(0) as f64
                } else {
                    let (total, used) = (total.as_f64()?, used.as_f64()?);
                    if !total.is_finite()
                        || !used.is_finite()
                        || total < 0.0
                        || used < 0.0
                        || total > 9_007_199_254_740_991.0
                        || used > 9_007_199_254_740_991.0
                    {
                        return None;
                    }
                    (total - used).max(0.0)
                };
                Some(balance + remaining)
            });
            if let Some(balance) =
                balance.filter(|balance| balance.is_finite() && *balance <= 9_007_199_254_740_991.0)
            {
                credits.insert("remaining".into(), json!(balance));
            }
        }
    }
    if !credits.is_empty() {
        credits.insert("unit".into(), json!("credits"));
        result["credits"] = Value::Object(credits);
    }
    result
}
