//! Complete source inventory and declared model capabilities.
use serde_json::{json, Value};
use std::sync::{Mutex, OnceLock};
#[derive(Clone)]
struct Cache {
    generation: String,
    at: i64,
    rows: Vec<Value>,
}
fn cache() -> &'static Mutex<Option<Cache>> {
    static CACHE: OnceLock<Mutex<Option<Cache>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(None))
}
fn refresh() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}
pub(super) fn forget() {
    *cache().lock().unwrap_or_else(|p| p.into_inner()) = None;
}
pub(super) async fn rows(_warm: bool) -> Result<Vec<Value>, String> {
    let generation = super::auth::generation().ok_or("kiro_not_signed_in")?;
    let cached = cache()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .as_ref()
        .filter(|v| v.generation == generation && super::auth::now() - v.at < 300000)
        .cloned();
    if let Some(c) = cached {
        return Ok(c.rows);
    }
    let _lock = refresh().lock().await;
    let cached = cache()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .as_ref()
        .filter(|v| v.generation == generation && super::auth::now() - v.at < 300000)
        .cloned();
    if let Some(c) = cached {
        return Ok(c.rows);
    }
    let c = super::auth::credential(Some(&generation)).await?;
    let mut token = None;
    let mut models = Vec::new();
    let mut default = None;
    let mut tokens = std::collections::HashSet::new();
    for _ in 0..32 {
        let mut request = super::client()
            .get(format!(
                "{}/List-Available-Models",
                super::management_base(&c)
            ))
            .query(&[("origin", "AI_EDITOR")])
            .headers(super::headers(&c)?);
        if let Some(profile) = &c.profile_arn {
            request = request.query(&[("profileArn", profile)]);
        }
        if let Some(token) = &token {
            request = request.query(&[("nextToken", token)]);
        }
        let value = super::bounded_json(
            request
                .send()
                .await
                .map_err(|_| "kiro_catalog_transport_failed")?,
        )
        .await?;
        if default.is_none() {
            default = value["defaultModel"]["modelId"].as_str().map(str::to_owned);
        }
        let rows = value["models"].as_array().ok_or("kiro_catalog_invalid")?;
        if models.len().saturating_add(rows.len()) > 4096 {
            return Err("kiro_catalog_too_large".into());
        }
        models.extend(rows.iter().cloned());
        token = value["nextToken"]
            .as_str()
            .filter(|v| !v.is_empty())
            .map(str::to_owned);
        if let Some(t) = &token {
            if !tokens.insert(t.clone()) {
                return Err("kiro_catalog_pagination_invalid".into());
            }
        } else {
            if !super::auth::is_current(&generation) {
                return Err("kiro_credential_changed".into());
            }
            let rows = normalize(&models, default.as_deref());
            *cache().lock().unwrap_or_else(|p| p.into_inner()) = Some(Cache {
                generation,
                at: super::auth::now(),
                rows: rows.clone(),
            });
            return Ok(rows);
        }
    }
    Err("kiro_catalog_too_large".into())
}
fn schema(value: &Value) -> Value {
    value
        .as_str()
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_else(|| value.clone())
}
pub(super) fn variant_key(base: &str, effort: &str) -> String {
    format!("{base}::effort={}", urlencoding::encode(effort))
}
pub(super) fn selection_parts(selection: &str) -> Result<(&str, Option<String>), String> {
    match selection.split_once("::effort=") {
        Some((base, effort)) if !base.is_empty() && !effort.is_empty() => {
            let effort = urlencoding::decode(effort)
                .map_err(|_| "kiro_model_invalid")?
                .into_owned();
            if variant_key(base, &effort) != selection {
                return Err("kiro_model_invalid".into());
            }
            Ok((base, Some(effort)))
        }
        Some(_) => Err("kiro_model_invalid".into()),
        None => Ok((selection, None)),
    }
}
pub(super) fn is_routing_alias(selection: &str) -> bool {
    selection
        .strip_prefix("kiro:")
        .unwrap_or(selection)
        .split("::effort=")
        .next()
        .is_some_and(|base| base.eq_ignore_ascii_case("auto"))
}
pub(super) fn expanded_rows(rows: &[Value]) -> Vec<Value> {
    let mut expanded = Vec::new();
    for row in rows {
        if row["key"].as_str().is_some_and(is_routing_alias) {
            continue;
        }
        expanded.push(row.clone());
        for variant in row["kiroVariants"].as_array().into_iter().flatten() {
            if variant["isDefault"] == true {
                continue;
            }
            let Some(value) = variant["value"]
                .as_str()
                .and_then(|v| v.strip_prefix("kiro:"))
            else {
                continue;
            };
            let mut member = row.clone();
            member["key"] = json!(value);
            member["id"] = json!(value);
            member["providerMetadata"]["selectedNativeEffort"] = variant["nativeEffort"].clone();
            expanded.push(member);
        }
    }
    expanded
}
fn effort_schema(schema: &Value) -> &Value {
    schema
        .pointer("/properties/reasoning/properties/effort")
        .or_else(|| schema.pointer("/properties/output_config/properties/effort"))
        .or_else(|| schema.pointer("/properties/effort"))
        .unwrap_or(&Value::Null)
}

/// Kiro exposes the consumption multiplier as `rateMultiplier` (e.g. "1", "0.5x")
/// and `rateUnit` (e.g. "credit"). Translate the multiplier to the shared
/// `priceFactor` so the picker can render it beside the model name.
fn rate_multiplier(value: &Value) -> Option<f64> {
    let factor = match value {
        Value::Number(number) => number.as_f64()?,
        Value::String(text) => text
            .trim()
            .trim_start_matches(['x', 'X'])
            .trim_end_matches(['x', 'X'])
            .trim()
            .parse::<f64>()
            .ok()?,
        _ => return None,
    };
    (factor.is_finite() && factor > 0.0).then_some(factor)
}
pub(super) fn normalize(models: &[Value], default: Option<&str>) -> Vec<Value> {
    let mut seen = std::collections::HashSet::new();
    models.iter().filter_map(|raw| {
        let key=raw["modelId"].as_str().filter(|s|!s.is_empty()&&s.len()<=512&&!s.chars().any(char::is_control))?;
        if !seen.insert(key) {return None;}
        let name=raw["modelName"].as_str().filter(|s|!s.is_empty()).unwrap_or(key);
        let input=raw["supportedInputTypes"].as_array();
        let has=|kind:&str| input.is_some_and(|v|v.iter().any(|v|v.as_str()==Some(kind)));
        let vision=has("IMAGE");
        let pdf=has("DOCUMENT");
        let schema=schema(&raw["additionalModelRequestFieldsSchema"]);
        let efforts:Vec<Value>=effort_schema(&schema)["enum"].as_array().into_iter().flatten()
            .filter(|v|v.as_str().is_some_and(|s|!s.is_empty()&&s.len()<=64&&s!="default")).cloned().collect();
        let thinking=schema.pointer("/properties/thinking").is_some()
            || schema.pointer("/properties/reasoning").is_some() || !efforts.is_empty();
        let mut cap=json!({"tools":true,"vision":vision,"nativePdf":pdf,"structuredOutput":false,"webSearch":false});
        let context=raw.pointer("/tokenLimits/maxInputTokens").and_then(Value::as_u64).filter(|v|*v>0);
        let output=raw.pointer("/tokenLimits/maxOutputTokens").and_then(Value::as_u64).filter(|v|*v>0);
        if context.is_none()||output.is_none(){cap["usedFallbackModelMetadata"]=json!(true);}
        if let Some(value)=context {cap["contextWindow"]=json!(value);}
        if let Some(value)=output {cap["maxOutputTokens"]=json!(value);}
        if thinking {
            let enum_values=schema.pointer("/properties/thinking/properties/type/enum").or_else(||schema.pointer("/properties/reasoning/properties/type/enum")).and_then(Value::as_array);
            let adaptive=enum_values.as_ref().is_some_and(|v|v.iter().any(|v|v=="adaptive"));
            let can_disable=enum_values.as_ref().is_some_and(|v|v.iter().any(|v|v=="disabled"));
            cap["thinking"]=json!({"adaptive":adaptive,"interleaved":true,"canDisableThinking":can_disable});
        }
        let default_effort=effort_schema(&schema)["default"].as_str().filter(|v|*v!="default").map(|v|json!(v)).unwrap_or(Value::Null);
        let available=raw["available"].as_bool().unwrap_or(true);
        let rate_factor = raw.get("rateMultiplier").and_then(rate_multiplier);
        let mut variants=vec![json!({"value":format!("kiro:{key}"),"nativeEffort":default_effort,
            "isDefault":true,"available":available,"contextTokens":context,"mediaInputMimeTypes":[]})];
        variants.extend(efforts.iter().filter_map(Value::as_str).map(|effort|json!({
            "value":format!("kiro:{}",variant_key(key,effort)),"nativeEffort":effort,
            "isDefault":false,"available":available,"contextTokens":context,"mediaInputMimeTypes":[]
        })));
        if efforts.is_empty() && schema.pointer("/properties/thinking/properties/type/enum").and_then(Value::as_array).is_some_and(|v|v.iter().any(|v|v=="adaptive")) {
            if schema.pointer("/properties/thinking/properties/type/default").and_then(Value::as_str)==Some("adaptive") {variants[0]["nativeEffort"]=json!("adaptive");}
            variants.push(json!({"value":format!("kiro:{}",variant_key(key,"adaptive")),"nativeEffort":"adaptive","nativeThinkingType":"adaptive","isDefault":false,"available":available,"contextTokens":context,"mediaInputMimeTypes":[]}));
        }
        if schema.pointer("/properties/thinking/properties/type/enum").and_then(Value::as_array).is_some_and(|v|v.iter().any(|v|v=="disabled")) {
            variants.push(json!({"value":format!("kiro:{}",variant_key(key,"disabled")),"nativeEffort":"disabled","nativeThinkingType":"disabled","isDefault":false,"available":available,"contextTokens":context,"mediaInputMimeTypes":[]}));
        }
        let mut row=json!({"key":key,"id":key,"name":name,"label":name,"display_name":name,"displayName":name,
            "isDefault":Some(key)==default,"available":available,"purpose":if is_routing_alias(key) {"routing"} else {"chat"},"conversationModel":!is_routing_alias(key),"contextWindow":context,"maxOutputTokens":output,
            "supportsThinking":thinking,"supportsVision":vision,"supportsToolCalls":true,"efforts":[],"defaultEffort":null,
            "capabilities":cap,"providerMetadata":{"status":raw["status"],"consentStatus":raw["consentStatus"],
            "modelProvider":raw["modelProvider"],"rateMultiplier":raw["rateMultiplier"],"rateUnit":raw["rateUnit"],
            "supportedInputTypes":raw["supportedInputTypes"],"additionalModelRequestFieldsSchema":schema,"nativeDefaultEffort":default_effort}});
        if let Some(factor) = rate_factor { row["priceFactor"] = json!(factor); }
        if variants.len()>1 {row["kiroFamilyId"]=json!(key);row["kiroVariants"]=json!(variants);}
        Some(row)
    }).collect()
}
