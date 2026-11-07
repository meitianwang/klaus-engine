use serde_json::{json, Map, Value};
use std::collections::BTreeSet;
use std::sync::OnceLock;

/// Keep only model-selection metadata; server agent configuration is private.
pub fn public_payload(payload: &Value) -> Value {
    let mut out = Map::new();
    for key in [
        "defaultAgentModelId",
        "agentModelSorts",
        "imageGenerationModelIds",
        "webSearchModelIds",
        "audioTranscriptionModelIds",
        "tabModelIds",
    ] {
        if let Some(value) = payload.get(key) {
            out.insert(key.into(), value.clone());
        }
    }
    if let Some(models) = payload.get("models").and_then(Value::as_object) {
        out.insert(
            "models".into(),
            Value::Object(
                models
                    .iter()
                    .map(|(id, model)| (id.clone(), public_model_details(model)))
                    .collect(),
            ),
        );
    }
    if let Some(deprecated) = payload.get("deprecatedModelIds").and_then(Value::as_object) {
        out.insert(
            "deprecatedModelIds".into(),
            Value::Object(
                deprecated
                    .iter()
                    .filter_map(|(id, details)| {
                        first_string(details, &["newModelId"])
                            .map(|replacement| (id.clone(), json!({"newModelId":replacement})))
                    })
                    .collect(),
            ),
        );
    }
    Value::Object(out)
}

fn public_model_details(model: &Value) -> Value {
    let mut out = Map::new();
    for key in [
        "displayName",
        "name",
        "label",
        "recommended",
        "disabled",
        "preview",
        "maxTokens",
        "maxOutputTokens",
        "supportsImages",
        "supportsPdf",
        "supportsVideo",
        "supportsThinking",
        "supportsAdaptiveThinking",
        "supportsTools",
        "supportsToolCalls",
        "supportsFunctionCalling",
        "supportsToolChoice",
        "doesNotSupportToolChoice",
        "supportsStructuredOutput",
        "supportsWebSearch",
        "supportedMimeTypes",
        "thinkingBudget",
        "minThinkingBudget",
        "thinkingLevel",
        "canDisableThinking",
        "supportedReasoningEfforts",
        "supported_reasoning_efforts",
        "reasoningEfforts",
        "efforts",
        "defaultReasoningEffort",
        "default_reasoning_effort",
        "defaultEffort",
        "quotaInfo",
        "vertexModelId",
        "model",
        "apiProvider",
        "modelProvider",
        "deprecated",
        "deprecationWarning",
        "replacementModelId",
        "notices",
        "isInternal",
        "isInternalOnly",
        "visibility",
        "purpose",
        "taskType",
        "task_type",
        "outputModalities",
        "output_modalities",
    ] {
        if let Some(value) = model.get(key) {
            out.insert(key.into(), value.clone());
        }
    }
    Value::Object(out)
}

/// Keep the complete service inventory, including disabled account entitlements.
pub fn normalize_models(payload: &Value) -> Vec<Value> {
    let Some(models) = payload.get("models").and_then(Value::as_object) else {
        return Vec::new();
    };
    let default_model = first_string(payload, &["defaultAgentModelId"]);
    let catalog_order = catalog_model_order(payload);
    let mut entries: Vec<_> = models.iter().collect();
    entries.sort_by(|left, right| {
        let rank = |id: &str| {
            (
                default_model != Some(id),
                catalog_order
                    .iter()
                    .position(|candidate| candidate == id)
                    .unwrap_or(usize::MAX),
            )
        };
        rank(left.0)
            .cmp(&rank(right.0))
            .then_with(|| left.0.cmp(right.0))
    });
    entries
        .into_iter()
        .filter_map(|(id, raw)| {
            raw.as_object()?;
            if id.trim().is_empty() {
                return None;
            }
            let label =
                first_string(raw, &["displayName", "display_name", "name", "label"]).unwrap_or(id);
            let disabled = raw
                .get("disabled")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let completion_only = payload
                .get("tabModelIds")
                .and_then(Value::as_array)
                .is_some_and(|ids| ids.iter().any(|value| value.as_str() == Some(id)));
            let native_role=[("imageGenerationModelIds","image"),("webSearchModelIds","search"),("audioTranscriptionModelIds","transcription")].into_iter()
                .find_map(|(field,role)|payload.get(field).and_then(Value::as_array).is_some_and(|ids|ids.iter().any(|value|value.as_str()==Some(id))).then_some(role));
            let disabled = disabled || completion_only || native_role.is_some();
            let capabilities = model_capabilities(raw);
            let efforts = declared_efforts(raw);
            let default_effort = first_string(
                raw,
                &[
                    "defaultReasoningEffort",
                    "default_reasoning_effort",
                    "defaultEffort",
                ],
            )
            .map(str::to_ascii_lowercase)
            .filter(|effort| efforts.contains(effort));
            let can_disable = raw
                .get("canDisableThinking")
                .and_then(Value::as_bool)
                .or_else(|| {
                    if positive_integer(raw.get("thinkingLevel")).is_some() {
                        return None;
                    }
                    declared_thinking_budget(raw)
                        .and_then(|_| nonnegative_integer(raw.get("minThinkingBudget")))
                        .map(|min| min == 0)
                })
                .unwrap_or(false);
            let mut entry = json!({
                "key": id,
                "value": id,
                "modelId": id,
                "id": id,
                "displayName": label,
                "display_name": label,
                "name": label,
                "label": label,
                "contextWindow": capabilities.get("contextWindow"),
                "maxOutputTokens": capabilities.get("maxOutputTokens"),
                "capabilities": capabilities,
                "disabled": disabled,
                "unavailable": disabled,
                "efforts": efforts,
                "supported_reasoning_efforts": efforts,
                "canDisableThinking": can_disable,
                "isDefault": default_model == Some(id.as_str()),
                "modelSortGroups": model_sort_groups(payload, id),
                "providerMetadata": public_model_details(raw),
            });
            super::super::conversation_catalog::mark_purpose("antigravity", raw, entry.as_object_mut().unwrap());
            if let Some(order) = catalog_order.iter().position(|candidate| candidate == id) {
                entry["catalogOrder"] = json!(order);
            }
            if let Some(budget) = signed_integer(raw.get("thinkingBudget")) {
                entry["defaultThinkingBudget"] = json!(budget);
            }
            if let Some(replacement) = payload
                .get("deprecatedModelIds")
                .and_then(|models| models.get(id))
                .and_then(|replacement| first_string(replacement, &["newModelId"]))
            {
                entry["replacementModelId"] = json!(replacement);
            }
            if raw.get("disabled").and_then(Value::as_bool).is_some() {
                entry["available"] = json!(!disabled);
            }
            if completion_only {
                entry["available"] = json!(false);
                entry["purpose"] = json!("completion");
                entry["unavailableReason"] = json!(
                    "This model serves editor completion requests and cannot run a conversation."
                );
            }
            if let Some(role)=native_role {
                entry["available"]=json!(false);
                entry["purpose"]=json!(role);
                entry["unavailableReason"]=json!(format!("This model serves the {role} capability. Select it from that capability's model selector."));
            }
            if let Some(effort) = default_effort {
                entry["defaultEffort"] = json!(effort);
                entry["default_reasoning_effort"] = json!(effort);
            }
            for key in [
                "preview",
                "recommended",
                "quotaInfo",
                "vertexModelId",
                "apiProvider",
                "modelProvider",
                "isInternal",
                "tagTitle",
                "tagDescription",
                "thinkingBudget",
                "minThinkingBudget",
                "thinkingLevel",
                "supportsAdaptiveThinking",
                "supportsThinking",
                "supportsTools",
                "supportsToolChoice",
                "doesNotSupportToolChoice",
                "supportedMimeTypes",
                "supportsImages",
                "supportsVideo",
                "supportsPdf",
            ] {
                if let Some(value) = raw.get(key) {
                    entry[key] = value.clone();
                }
            }
            Some(entry)
        })
        .collect()
}

/// Preserve account availability while including the known model inventory.
pub fn models_with_unavailable_paid(payload: &Value) -> Vec<Value> {
    let mut models = normalize_models(payload);
    // A bundled snapshot carries identities and capabilities, never a local
    // account's entitlement decision. Keep every known chat model selectable so
    // the service decides access while the first live catalog is in flight.
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
    let dynamic_ids: BTreeSet<String> = payload
        .get("models")
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(|models| models.keys())
        .cloned()
        .collect();
    for mut model in normalize_models(&paid_model_inventory()) {
        let id = model["key"].as_str().unwrap_or_default();
        if dynamic_ids.contains(id) {
            continue;
        }
        model["disabled"] = json!(true);
        model["unavailable"] = json!(true);
        model["available"] = json!(false);
        model["entitlementUnknown"] = json!(true);
        model["unavailableReason"] = json!(
            "This model is not offered to the connected account. It becomes available when the service includes it in your model catalog."
        );
        models.push(model);
    }
    models
}

/// The sanitized catalog shipped with the build.
///
/// It contains only model identities, protocol metadata and capability
/// declarations. Account quotas, project identifiers, credential state and
/// per-account entitlement flags are deliberately absent: the baseline is a
/// local answer while the credential-scoped catalog is being revalidated, not a
/// substitute for the service's account decision.
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

/// Resolve exact declared identities independently of account availability.
pub(super) fn model_details(payload: &Value, model: &str) -> Option<Value> {
    payload
        .get("models")
        .and_then(|models| models.get(model))
        .filter(|details| details.is_object())
        .cloned()
        .or_else(|| paid_model_inventory().get("models")?.get(model).cloned())
}

pub(super) fn is_chat_model(entry: &Value) -> bool {
    super::super::conversation_catalog::is_conversation_model("antigravity", entry)
}

/// Project only fields accepted by the engine's capability contract.
pub fn model_capabilities(entry: &Value) -> Value {
    let raw = entry.get("providerMetadata").unwrap_or(entry);
    let mime_types = declared_mime_types(raw);
    let supports = |key: &str| raw.get(key).and_then(Value::as_bool);
    let mut capabilities = json!({
        "tools": supports("supportsTools")
            .or_else(|| supports("supportsToolCalls"))
            .or_else(|| supports("supportsToolCalls"))
            .or_else(|| supports("supportsFunctionCalling"))
            .or_else(|| supports("tools"))
            .unwrap_or(true),
        "vision": supports("supportsImages")
            .unwrap_or_else(|| mime_types.iter().any(|mime| mime.starts_with("image/"))),
        "nativePdf": supports("supportsPdf")
            .unwrap_or_else(|| mime_types.contains("application/pdf")),
        "structuredOutput": supports("supportsStructuredOutput").unwrap_or(false),
        "webSearch": supports("supportsWebSearch").unwrap_or(false),
    });
    let media_mimes: Vec<_> = mime_types
        .iter()
        .filter(|mime| {
            (mime.starts_with("audio/") || mime.starts_with("video/"))
                && (!mime.starts_with("video/") || supports("supportsVideo") != Some(false))
        })
        .cloned()
        .collect();
    if !media_mimes.is_empty() {
        capabilities["mediaInputMimeTypes"] = json!(media_mimes);
    }
    if let Some(tokens) = positive_integer(raw.get("maxTokens")) {
        capabilities["contextWindow"] = json!(tokens);
    }
    if let Some(tokens) = positive_integer(raw.get("maxOutputTokens")) {
        capabilities["maxOutputTokens"] = json!(tokens);
    }
    let budget = declared_thinking_budget(raw);
    let level = positive_integer(raw.get("thinkingLevel")).is_some();
    let adaptive = supports("supportsAdaptiveThinking").unwrap_or(budget == Some(-1));
    let thinking = supports("supportsThinking").unwrap_or(adaptive || budget.is_some() || level);
    if thinking {
        let can_disable_thinking = supports("canDisableThinking")
            .or_else(|| {
                if level {
                    return Some(false);
                }
                budget
                    .and_then(|_| nonnegative_integer(raw.get("minThinkingBudget")))
                    .map(|min| min == 0)
            })
            .unwrap_or(false);
        capabilities["thinking"] = json!({
            "adaptive": adaptive,
            "interleaved": false,
            "canDisableThinking": can_disable_thinking,
        });
        if level || (adaptive && budget.is_none()) {
            capabilities["geminiThinking"] = json!({"mode": "level"});
        } else if budget.is_some() {
            // The catalog's budget selects a model variant; it is not an upper limit.
            let min = nonnegative_integer(raw.get("minThinkingBudget"));
            let mut dialect = json!({"mode": "budget"});
            if let Some(min) = min {
                dialect["min"] = json!(min);
                dialect["canDisable"] = json!(min == 0);
            }
            if let Some(can_disable) = supports("canDisableThinking") {
                dialect["canDisable"] = json!(can_disable);
            }
            capabilities["geminiThinking"] = dialect;
        }
        let efforts = declared_efforts(raw);
        if !efforts.is_empty() {
            let mut effort = json!({"levels": efforts});
            if let Some(default) = first_string(
                raw,
                &[
                    "defaultReasoningEffort",
                    "default_reasoning_effort",
                    "defaultEffort",
                ],
            )
            .map(str::to_ascii_lowercase)
            .filter(|default| efforts.contains(default))
            {
                effort["default"] = json!(default);
            }
            capabilities["effort"] = effort;
        }
    }
    capabilities
}

/// Preserve independently enforced quota windows instead of inventing a total.
pub fn normalize_usage(
    quota: &Value,
    summary: &Value,
    account: Option<&str>,
    plan: Option<&str>,
) -> Value {
    let mut result = json!({"signedIn": true});
    if let Some(account) = account.filter(|value| !value.trim().is_empty()) {
        result["account"] = json!(account);
    }
    if let Some(plan) = plan.filter(|value| !value.trim().is_empty()) {
        result["plan"] = json!(plan);
    }
    let mut windows = Vec::new();
    if let Some(buckets) = quota.get("buckets").and_then(Value::as_array) {
        for bucket in buckets {
            if let Some(window) = usage_window("quota", None, bucket) {
                windows.push(window);
            }
        }
    }
    let mut grouped = false;
    if let Some(groups) = summary.get("groups").and_then(Value::as_array) {
        for group in groups {
            let Some(buckets) = group.get("buckets").and_then(Value::as_array) else {
                continue;
            };
            for bucket in buckets {
                if let Some(window) = usage_window("summary", Some(group), bucket) {
                    windows.push(window);
                    grouped = true;
                }
            }
        }
    }
    if !grouped {
        if let Some(buckets) = summary.get("buckets").and_then(Value::as_array) {
            for bucket in buckets {
                if let Some(window) = usage_window("summary", Some(summary), bucket) {
                    windows.push(window);
                }
            }
        }
    }
    windows.sort_by(|left, right| {
        left["id"]
            .as_str()
            .cmp(&right["id"].as_str())
            .then_with(|| left.to_string().cmp(&right.to_string()))
    });
    let mut seen = std::collections::BTreeMap::new();
    for window in &mut windows {
        let id = window["id"].as_str().unwrap_or_default().to_owned();
        let count = seen.entry(id.clone()).or_insert(0usize);
        *count += 1;
        if *count > 1 {
            window["id"] = json!(format!("{id}:duplicate:{count}"));
        }
    }
    if !windows.is_empty() {
        result["windows"] = json!(windows);
    }
    if let Some(description) = first_string(summary, &["description"]) {
        result["description"] = json!(description);
    }
    if let Some(credits) = [summary, quota].into_iter().find_map(normalize_credits) {
        result["credits"] = credits;
    }
    result
}

/// Declare only native services named by the cloud catalog.
pub fn native_capabilities(payload: &Value) -> Value {
    let mut capabilities = Map::new();
    if payload
        .get("imageGenerationModelIds")
        .and_then(Value::as_array)
        .is_some()
    {
        let mut image = native_service(payload, "imageGenerationModelIds");
        image["referenceImages"] = json!(true);
        capabilities.insert("image".into(), image.clone());
        capabilities.insert("imageEdit".into(), image);
    }
    if payload
        .get("webSearchModelIds")
        .and_then(Value::as_array)
        .is_some()
    {
        capabilities.insert(
            "search".into(),
            native_service(payload, "webSearchModelIds"),
        );
    }
    if payload
        .get("audioTranscriptionModelIds")
        .and_then(Value::as_array)
        .is_some()
    {
        let mut transcription = native_service(payload, "audioTranscriptionModelIds");
        transcription["available"] = json!(false);
        transcription["reason"] = json!("protocol_unavailable");
        capabilities.insert("transcription".into(), transcription);
    }
    Value::Object(capabilities)
}

fn paid_model_inventory() -> Value {
    let mut models = Map::new();
    for (suffix, label, level) in [
        ("low", "Low", 1),
        ("medium", "Medium", 2),
        ("high", "High", 3),
    ] {
        models.insert(
            format!("gemini-4-argon-{suffix}"),
            json!({
                "displayName": format!("Gemini 4 Argon ({label})"),
                "maxTokens": 1048576,
                "maxOutputTokens": 65536,
                "thinkingLevel": level,
                "thinkingBudget": 0,
                "minThinkingBudget": 0,
                "supportsImages": true,
                "supportsPdf": true,
                "supportsVideo": true,
                "supportsTools": true,
                "supportsThinking": true,
                "supportsAdaptiveThinking": false,
            }),
        );
    }
    for (family, name) in [("sonnet", "Sonnet"), ("opus", "Opus")] {
        for (suffix, label, level) in [
            ("low", "Low", 1),
            ("medium", "Medium", 2),
            ("high", "High", 3),
            ("max", "Max", 200),
        ] {
            models.insert(
                format!("claude-{family}-5-5-{suffix}"),
                json!({
                    "displayName": format!("Claude {name} 5.5 ({label})"),
                    "maxTokens": 250000,
                    "maxOutputTokens": 64000,
                    "thinkingLevel": level,
                    "thinkingBudget": 0,
                    "minThinkingBudget": 0,
                    "supportsImages": true,
                    "supportsPdf": false,
                    "supportsVideo": false,
                    "supportsTools": true,
                    "supportsThinking": true,
                    "supportsAdaptiveThinking": true,
                    "supportsToolChoice": false,
                    "doesNotSupportToolChoice": true,
                    "modelProvider": 3,
                }),
            );
        }
    }
    json!({"models": models})
}

fn native_service(payload: &Value, key: &str) -> Value {
    let ids: BTreeSet<_> = payload
        .get(key)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter(|id| !id.trim().is_empty())
        .collect();
    let models: Vec<_> = ids
        .into_iter()
        .map(|id| {
            let label = payload
                .get("models")
                .and_then(|models| models.get(id))
                .and_then(|model| first_string(model, &["displayName", "display_name", "name"]))
                .unwrap_or(id);
            json!({"id": id, "label": label})
        })
        .collect();
    json!({"available": !models.is_empty(), "models": models})
}

fn catalog_model_order(payload: &Value) -> Vec<String> {
    let mut order = Vec::new();
    for sort in payload
        .get("agentModelSorts")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        for group in sort
            .get("groups")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            for id in group
                .get("modelIds")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
            {
                if !id.trim().is_empty() && !order.iter().any(|existing| existing == id) {
                    order.push(id.to_owned());
                }
            }
        }
    }
    order
}

fn model_sort_groups(payload: &Value, id: &str) -> Vec<Value> {
    let mut memberships = Vec::new();
    for sort in payload
        .get("agentModelSorts")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        for group in sort
            .get("groups")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let position = group
                .get("modelIds")
                .and_then(Value::as_array)
                .and_then(|ids| {
                    ids.iter()
                        .position(|candidate| candidate.as_str() == Some(id))
                });
            if let Some(position) = position {
                let mut membership = json!({"position": position});
                if let Some(label) = first_string(sort, &["displayName"]) {
                    membership["sortLabel"] = json!(label);
                }
                if let Some(label) = first_string(group, &["displayName"]) {
                    membership["groupLabel"] = json!(label);
                }
                memberships.push(membership);
            }
        }
    }
    memberships
}

fn usage_window(source: &str, group: Option<&Value>, bucket: &Value) -> Option<Value> {
    bucket.as_object()?;
    let mut identity = vec![group
        .and_then(|group| first_string(group, &["groupId", "id", "displayName"]))
        .map(|value| json!(value))
        .unwrap_or(Value::Null)];
    identity.extend(
        ["bucketId", "modelId", "tokenType", "window", "displayName"]
            .into_iter()
            .map(|key| bucket.get(key).cloned().unwrap_or(Value::Null)),
    );
    // A JSON array preserves component boundaries without lossy identifier escaping.
    let id = format!("{source}:{}", json!(identity));
    let label = first_string(bucket, &["displayName", "modelId", "bucketId", "tokenType"])
        .or_else(|| group.and_then(|group| first_string(group, &["displayName"])))
        .unwrap_or("Quota");
    let mut window = json!({"id": id, "label": label, "source": source});
    if let Some(fraction) = finite_number(bucket.get("remainingFraction")) {
        let fraction = fraction.clamp(0.0, 1.0);
        window["remainingFraction"] = json!(fraction);
        window["remainingPercent"] = json!(fraction * 100.0);
        window["usedPercent"] = json!((1.0 - fraction) * 100.0);
    }
    if let Some(amount) =
        finite_number(bucket.get("remainingAmount")).filter(|amount| *amount >= 0.0)
    {
        window["remainingAmount"] = json!(amount);
    }
    if let Some(reset) = first_string(bucket, &["resetTime"])
        .and_then(|reset| chrono::DateTime::parse_from_rfc3339(reset).ok())
    {
        window["resetsAt"] = json!(reset.timestamp_millis());
    }
    for key in [
        "bucketId",
        "modelId",
        "tokenType",
        "window",
        "description",
        "disabled",
    ] {
        if let Some(value) = bucket.get(key) {
            window[key] = value.clone();
        }
    }
    if let Some(group) = group {
        if let Some(label) = first_string(group, &["displayName"]) {
            window["groupLabel"] = json!(label);
        }
        if let Some(description) = first_string(group, &["description"]) {
            window["groupDescription"] = json!(description);
        }
    }
    Some(window)
}

fn normalize_credits(payload: &Value) -> Option<Value> {
    let raw = payload
        .get("credits")
        .or_else(|| payload.get("availableCredits"))?;
    if let Some(pools) = raw.as_array() {
        let pools: Vec<_> = pools
            .iter()
            .filter(|pool| pool.is_object())
            .map(|pool| {
                let mut item = Map::new();
                for key in ["creditType", "creditAmount", "minimumCreditAmountForUsage"] {
                    if let Some(value) = pool.get(key) {
                        item.insert(key.into(), value.clone());
                    }
                }
                Value::Object(item)
            })
            .collect();
        if pools.is_empty() {
            return None;
        }
        let mut result = json!({"pools":pools});
        if pools.len() == 1 {
            if let Some(remaining) = finite_number(pools[0].get("creditAmount"))
                .filter(|amount| *amount >= 0.0 && *amount <= 9_007_199_254_740_991.0)
            {
                result["remaining"] = json!(remaining);
            }
            if let Some(unit) = pools[0].get("creditType") {
                result["unit"] = unit.clone();
            }
        }
        return Some(result);
    }
    let mut credits = Map::new();
    if raw.is_object() {
        for (output, keys) in [
            ("remaining", &["remaining", "remainingAmount"][..]),
            ("total", &["total", "totalAmount"][..]),
            ("used", &["used", "usedAmount"][..]),
        ] {
            if let Some(amount) = keys
                .iter()
                .find_map(|key| finite_number(raw.get(*key)))
                .filter(|amount| *amount >= 0.0)
            {
                credits.insert(output.into(), json!(amount));
            }
        }
        if let Some(unit) = first_string(raw, &["unit"]) {
            credits.insert("unit".into(), json!(unit));
        }
    } else if let Some(amount) = finite_number(Some(raw)).filter(|amount| *amount >= 0.0) {
        credits.insert("remaining".into(), json!(amount));
    }
    (!credits.is_empty()).then_some(Value::Object(credits))
}

fn declared_efforts(raw: &Value) -> Vec<String> {
    let Some(efforts) = [
        "supportedReasoningEfforts",
        "supported_reasoning_efforts",
        "efforts",
    ]
    .into_iter()
    .find_map(|key| raw.get(key).and_then(Value::as_array)) else {
        return Vec::new();
    };
    let mut result = Vec::new();
    for effort in efforts.iter().filter_map(Value::as_str) {
        let effort = effort.trim().to_ascii_lowercase();
        if matches!(effort.as_str(), "low" | "medium" | "high" | "xhigh" | "max")
            && !result.contains(&effort)
        {
            result.push(effort);
        }
    }
    result
}

fn declared_mime_types(raw: &Value) -> BTreeSet<&str> {
    raw.get("supportedMimeTypes")
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(|types| types.iter())
        .filter(|(_, supported)| supported.as_bool() == Some(true))
        .map(|(mime, _)| mime.as_str())
        .collect()
}

fn first_string<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a str> {
    keys.iter().find_map(|key| {
        value
            .get(*key)
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
    })
}

fn positive_integer(value: Option<&Value>) -> Option<u64> {
    nonnegative_integer(value).filter(|value| *value > 0)
}

fn nonnegative_integer(value: Option<&Value>) -> Option<u64> {
    match value? {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.trim().parse().ok(),
        _ => None,
    }
}

fn signed_integer(value: Option<&Value>) -> Option<i64> {
    match value? {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.trim().parse().ok(),
        _ => None,
    }
}

fn declared_thinking_budget(raw: &Value) -> Option<i64> {
    signed_integer(raw.get("thinkingBudget")).filter(|budget| *budget == -1 || *budget > 0)
}

fn finite_number(value: Option<&Value>) -> Option<f64> {
    let value = match value? {
        Value::Number(number) => number.as_f64()?,
        Value::String(text) => text.trim().parse::<f64>().ok()?,
        _ => return None,
    };
    value.is_finite().then_some(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renderer_catalog_excludes_agent_configuration_and_marks_completion_models() {
        let raw = json!({"defaultAgentModelId":"chat","tabModelIds":["completion"],"agentExperiments":{"systemPrompt":"private"},"deprecatedModelIds":{"chat":{"newModelId":"replacement","systemPrompt":"private"}},"models":{
            "chat":{"displayName":"Chat","supportsThinking":true,"thinkingBudget":4000,"modelExperiments":{"systemPrompt":"private"}},
            "completion":{"displayName":"Completion","maxTokens":1000}
        }});
        let public = public_payload(&raw);
        assert!(public.get("agentExperiments").is_none());
        assert!(public["models"]["chat"].get("modelExperiments").is_none());
        assert_eq!(
            public["deprecatedModelIds"]["chat"],
            json!({"newModelId":"replacement"})
        );
        let rows = normalize_models(&public);
        assert_eq!(
            rows.iter().find(|row| row["key"] == "chat").unwrap()["replacementModelId"],
            "replacement"
        );
        let completion = rows.iter().find(|row| row["key"] == "completion").unwrap();
        assert_eq!(completion["available"], false);
        assert_eq!(completion["disabled"], true);
        assert_eq!(completion["purpose"], "completion");
        assert!(!rows
            .iter()
            .any(|row| row.to_string().contains("systemPrompt")));
    }

    #[test]
    fn media_input_mime_types_follow_explicit_model_declarations() {
        let caps = model_capabilities(
            &json!({"supportsVideo":false,"supportedMimeTypes":{"audio/wav":true,"video/mp4":true,"audio/ogg":false,"text/plain":true}}),
        );
        assert_eq!(caps["mediaInputMimeTypes"], json!(["audio/wav"]));
    }

    #[test]
    fn credit_pools_preserve_int64_amounts_without_inventing_totals_or_resets() {
        let usage = normalize_usage(
            &Value::Null,
            &json!({"availableCredits":[{"creditType":"GOOGLE_ONE_AI","creditAmount":"9007199254740993","minimumCreditAmountForUsage":"100"}]}),
            None,
            None,
        );
        assert_eq!(
            usage["credits"]["pools"][0]["creditAmount"],
            "9007199254740993"
        );
        assert_eq!(
            usage["credits"]["pools"][0]["minimumCreditAmountForUsage"],
            "100"
        );
        assert!(usage["credits"].get("remaining").is_none());
        assert!(usage["credits"].get("total").is_none());
        assert!(usage["credits"].get("resetsAt").is_none());
        let small = normalize_usage(
            &Value::Null,
            &json!({"availableCredits":[{"creditType":"GOOGLE_ONE_AI","creditAmount":"1234"}]}),
            None,
            None,
        );
        assert_eq!(small["credits"]["remaining"], 1234.0);
    }

    #[test]
    fn catalog_retains_disabled_models_and_the_service_identity() {
        let payload = json!({"models": {
            "reasoning-premium": {"displayName": "Reasoning Premium", "disabled": true,
                "preview": true, "vertexModelId": "backend-route", "maxTokens": 1048576,
                "maxOutputTokens": 65536, "quotaInfo": {"remainingFraction": 0.0}},
            "chat-standard": {"displayName": "Chat Standard"}
        }});
        let models = normalize_models(&payload);
        assert_eq!(models.len(), 2);
        assert_eq!(models[0]["key"], "chat-standard");
        let premium = &models[1];
        for key in ["key", "value", "id", "modelId"] {
            assert_eq!(premium[key], "reasoning-premium");
        }
        assert_eq!(premium["display_name"], "Reasoning Premium");
        assert_eq!(premium["unavailable"], true);
        assert_eq!(premium["available"], false);
        assert_eq!(premium["contextWindow"], 1048576);
        assert_eq!(premium["maxOutputTokens"], 65536);
        assert_eq!(
            premium["providerMetadata"],
            payload["models"]["reasoning-premium"]
        );
        assert!(models[0]["contextWindow"].is_null());
    }

    #[test]
    fn capability_projection_uses_declarations_without_guessing_from_model_names() {
        let plain = model_capabilities(&json!({"displayName": "Vision Thinking Pro"}));
        assert_eq!(plain["tools"], true);
        assert_eq!(plain["vision"], false);
        assert_eq!(plain["nativePdf"], false);
        assert!(plain.get("thinking").is_none());
        assert!(plain.get("contextWindow").is_none());
        let declared = model_capabilities(&json!({"supportsTools": false,
            "supportedMimeTypes": {"image/png": true, "application/pdf": true, "image/webp": false},
            "supportsVideo": true, "supportsThinking": false, "thinkingBudget": 8192}));
        assert_eq!(declared["tools"], false);
        assert_eq!(declared["vision"], true);
        assert_eq!(declared["nativePdf"], true);
        assert!(declared.get("thinking").is_none());
        assert!(declared.get("supportsVideo").is_none());
    }

    #[test]
    fn explicit_media_denials_override_the_mime_list() {
        let capabilities =
            model_capabilities(&json!({"supportsImages": false, "supportsPdf": false,
            "supportedMimeTypes": {"image/png": true, "application/pdf": true}}));
        assert_eq!(capabilities["vision"], false);
        assert_eq!(capabilities["nativePdf"], false);
    }

    #[test]
    fn chosen_budgets_do_not_become_upper_limits() {
        let budget = model_capabilities(&json!({"supportsThinking": true,
            "thinkingBudget": 24576, "minThinkingBudget": 128}));
        assert_eq!(
            budget["geminiThinking"],
            json!({"mode":"budget", "min":128, "canDisable":false})
        );
        let level =
            model_capabilities(&json!({"supportsAdaptiveThinking": true, "thinkingLevel": 2}));
        assert_eq!(level["geminiThinking"], json!({"mode":"level"}));
        assert_eq!(level["thinking"]["adaptive"], true);
        assert!(level.get("effort").is_none());
    }

    #[test]
    fn explicit_effort_labels_reach_both_catalog_and_engine_shapes() {
        let models = normalize_models(&json!({"models":{"model":{"supportsThinking":true,
            "efforts":["LOW","high","LOW"],"defaultEffort":"HIGH","canDisableThinking":true}}}));
        let model = &models[0];
        assert_eq!(model["efforts"], json!(["low", "high"]));
        assert_eq!(model["supported_reasoning_efforts"], model["efforts"]);
        assert_eq!(model["default_reasoning_effort"], "high");
        assert_eq!(model["canDisableThinking"], true);
        assert_eq!(
            model["capabilities"]["effort"],
            json!({"levels":["low","high"],"default":"high"})
        );
        assert_eq!(model_capabilities(model), model["capabilities"]);
    }

    #[test]
    fn unknown_or_invalid_limits_do_not_become_zero() {
        let capabilities = model_capabilities(&json!({"maxTokens":-1, "maxOutputTokens":"invalid",
            "supportsThinking":true, "thinkingBudget": 8192, "minThinkingBudget":-8}));
        assert!(capabilities.get("contextWindow").is_none());
        assert!(capabilities.get("maxOutputTokens").is_none());
        assert!(capabilities["geminiThinking"].get("min").is_none());
        assert!(capabilities["geminiThinking"].get("canDisable").is_none());
    }

    #[test]
    fn declared_capabilities_decode_through_the_engine_contract() {
        use klaus_agent_core::runtime_values::{runtime_codec, RuntimeString};
        use klaus_agent_protocol::types::ModelCapabilitiesInit;

        let capabilities = model_capabilities(&json!({"maxTokens":1048576,
            "maxOutputTokens":65536, "supportsImages":true, "supportsPdf":true,
            "supportsThinking":true, "thinkingBudget":32768, "minThinkingBudget":0,
            "efforts":["low", "high"], "defaultEffort":"high"}));
        let decoded: ModelCapabilitiesInit =
            runtime_codec::decode_text(&RuntimeString::from(capabilities.to_string()))
                .expect("catalog capabilities must be accepted at session initialization");
        assert_eq!(decoded.context_window, Some(1048576));
        assert_eq!(decoded.max_output_tokens, Some(65536));
        let dialect = decoded.gemini_thinking.unwrap();
        assert_eq!(dialect.min, Some(0));
        assert_eq!(dialect.max, None);
    }

    #[test]
    fn default_and_curated_order_preserve_the_complete_inventory() {
        let payload = json!({
            "defaultAgentModelId":"gemini-3.8-flash-high",
            "agentModelSorts":[{"displayName":"Recommended", "groups":[{"modelIds":[
                "gemini-3.8-flash-high", "gemini-3.8-flash-medium", "gemini-3.8-flash-low"
            ]}]}],
            "models":{
                "gemini-3.8-flash-low":{"displayName":"Gemini 3.8 Flash (Low)",
                    "recommended":true, "supportsThinking":true,
                    "thinkingBudget":1000, "minThinkingBudget":32},
                "gemini-3.8-flash-medium":{"displayName":"Gemini 3.8 Flash (Medium)",
                    "recommended":true, "supportsThinking":true,
                    "thinkingBudget":4000, "minThinkingBudget":32},
                "gemini-3.8-flash-high":{"displayName":"Gemini 3.8 Flash (High)",
                    "recommended":true, "supportsThinking":true,
                    "thinkingBudget":-1, "minThinkingBudget":32},
                "unlisted-model":{"recommended":true},
                "internal-model":{"isInternal":true}
            }
        });
        let models = normalize_models(&payload);
        assert_eq!(models.len(), 5);
        assert_eq!(models[0]["key"], "gemini-3.8-flash-high");
        assert_eq!(models[1]["key"], "gemini-3.8-flash-medium");
        assert_eq!(models[2]["key"], "gemini-3.8-flash-low");
        assert_eq!(models[0]["isDefault"], true);
        assert_eq!(models[1]["isDefault"], false);
        assert_eq!(models[0]["catalogOrder"], 0);
        assert_eq!(
            models[0]["modelSortGroups"],
            json!([{"sortLabel":"Recommended", "position":0}])
        );
        let unlisted = models
            .iter()
            .find(|model| model["key"] == "unlisted-model")
            .unwrap();
        assert_eq!(unlisted["recommended"], true);
        assert!(unlisted.get("catalogOrder").is_none());
        assert_eq!(unlisted["modelSortGroups"], json!([]));
        assert!(unlisted.get("available").is_none());
    }

    #[test]
    fn fixed_model_variants_preserve_positive_and_automatic_budget_defaults() {
        let models = normalize_models(&json!({"models":{
            "low-variant":{"supportsThinking":true, "thinkingBudget":1000, "minThinkingBudget":32},
            "medium-variant":{"supportsThinking":true, "thinkingBudget":4000, "minThinkingBudget":32},
            "automatic-variant":{"supportsThinking":true, "thinkingBudget":-1, "minThinkingBudget":32}
        }}));
        for model in &models {
            assert_eq!(
                model["defaultThinkingBudget"],
                model["providerMetadata"]["thinkingBudget"]
            );
            assert_eq!(model["efforts"], json!([]));
            assert!(model["capabilities"].get("effort").is_none());
            assert_eq!(model["capabilities"]["geminiThinking"]["mode"], "budget");
            assert!(model["capabilities"]["geminiThinking"].get("max").is_none());
            assert_eq!(model["canDisableThinking"], false);
        }
        let automatic = models
            .iter()
            .find(|model| model["key"] == "automatic-variant")
            .unwrap();
        assert_eq!(automatic["defaultThinkingBudget"], -1);
        assert_eq!(automatic["capabilities"]["thinking"]["adaptive"], true);
    }

    #[test]
    fn paid_inventory_remains_informational_and_outside_the_dynamic_catalog() {
        let payload = json!({"models":{}});
        assert!(normalize_models(&payload).is_empty());
        let models = models_with_unavailable_paid(&payload);
        let expected: BTreeSet<_> = [
            "gemini-4-argon-low",
            "gemini-4-argon-medium",
            "gemini-4-argon-high",
            "claude-sonnet-5-5-low",
            "claude-sonnet-5-5-medium",
            "claude-sonnet-5-5-high",
            "claude-sonnet-5-5-max",
            "claude-opus-5-5-low",
            "claude-opus-5-5-medium",
            "claude-opus-5-5-high",
            "claude-opus-5-5-max",
        ]
        .into_iter()
        .collect();
        let actual: BTreeSet<_> = models
            .iter()
            .map(|model| model["key"].as_str().unwrap())
            .collect();
        assert_eq!(actual, expected);
        for model in &models {
            assert_eq!(model["disabled"], true);
            assert_eq!(model["unavailable"], true);
            assert_eq!(model["available"], false);
            assert_eq!(model["entitlementUnknown"], true);
            assert_eq!(
                model["unavailableReason"],
                "This model is not offered to the connected account. It becomes available when the service includes it in your model catalog."
            );
            assert_eq!(model["isDefault"], false);
            assert!(model.get("vertexModelId").is_none());
            assert!(model["providerMetadata"].get("vertexModelId").is_none());
        }
    }

    #[test]
    fn service_model_rows_win_over_informational_paid_records() {
        let payload = json!({"defaultAgentModelId":"gemini-4-argon-high", "models":{
            "gemini-4-argon-high":{"displayName":"Account model", "disabled":false,
                "maxTokens":900000, "maxOutputTokens":32000, "supportsThinking":false}
        }});
        let dynamic = normalize_models(&payload);
        let inventory = models_with_unavailable_paid(&payload);
        assert_eq!(inventory.len(), 11);
        assert_eq!(inventory[0], dynamic[0]);
        assert_eq!(inventory[0]["displayName"], "Account model");
        assert_eq!(inventory[0]["contextWindow"], 900000);
        assert_eq!(inventory[0]["available"], true);
        assert!(inventory[0].get("entitlementUnknown").is_none());
        assert!(inventory[0]["capabilities"].get("thinking").is_none());
        assert!(inventory
            .iter()
            .skip(1)
            .all(|model| model["available"] == false));
    }

    #[test]
    fn bundled_catalog_has_no_account_state_and_never_claims_entitlement() {
        let payload = bundled_payload();
        for private in ["quotaInfo", "project", "account", "email", "credential"] {
            assert!(
                !contains_key(&payload, private),
                "bundled catalog leaked {private}"
            );
        }
        assert_eq!(payload["catalogBaseline"], true);
        let rows = models_with_unavailable_paid(&payload);
        let chat = rows
            .iter()
            .find(|row| row["key"] == "gemini-4-argon-medium")
            .unwrap();
        assert_eq!(chat["disabled"], false);
        assert_eq!(chat["available"], false);
        assert_eq!(chat["entitlementUnknown"], true);
        assert_eq!(
            chat["unavailableReason"],
            "Account access is confirmed when the model service answers. The model remains selectable."
        );
    }

    fn contains_key(value: &Value, key: &str) -> bool {
        match value {
            Value::Object(object) => {
                object.contains_key(key) || object.values().any(|value| contains_key(value, key))
            }
            Value::Array(values) => values.iter().any(|value| contains_key(value, key)),
            _ => false,
        }
    }

    #[test]
    fn paid_families_keep_declared_level_and_adaptive_capabilities_without_numeric_budgets() {
        use klaus_agent_core::runtime_values::{runtime_codec, RuntimeString};
        use klaus_agent_protocol::types::ModelCapabilitiesInit;

        let models = models_with_unavailable_paid(&json!({"models":{}}));
        for model in &models {
            let id = model["key"].as_str().unwrap();
            let capabilities = &model["capabilities"];
            let decoded: ModelCapabilitiesInit =
                runtime_codec::decode_text(&RuntimeString::from(capabilities.to_string()))
                    .expect("paid metadata must decode through the engine contract");
            assert_eq!(decoded.tools, Some(true));
            assert!(decoded.vision);
            assert_eq!(capabilities["geminiThinking"], json!({"mode":"level"}));
            assert_eq!(model["defaultThinkingBudget"], 0);
            assert_eq!(model["canDisableThinking"], false);
            assert!(capabilities.get("effort").is_none());
            if id.starts_with("gemini-4-argon-") {
                assert_eq!(decoded.context_window, Some(1048576));
                assert_eq!(decoded.max_output_tokens, Some(65536));
                assert!(decoded.native_pdf);
                assert_eq!(model["supportsVideo"], true);
                assert!(!decoded.thinking.unwrap().adaptive);
            } else {
                assert_eq!(decoded.context_window, Some(250000));
                assert_eq!(decoded.max_output_tokens, Some(64000));
                assert!(!decoded.native_pdf);
                assert_eq!(model["supportsVideo"], false);
                assert!(decoded.thinking.unwrap().adaptive);
                assert_eq!(model["supportsToolChoice"], false);
                assert_eq!(model["doesNotSupportToolChoice"], true);
                assert_eq!(model["providerMetadata"]["doesNotSupportToolChoice"], true);
                assert!(capabilities.get("supportsToolChoice").is_none());
            }
        }
    }

    #[test]
    fn zero_budget_is_a_sentinel_and_does_not_enable_a_budget_dialect_or_thinking_off() {
        let models = normalize_models(&json!({"models":{"model":{
            "supportsThinking":true, "thinkingBudget":0, "minThinkingBudget":0
        }}}));
        assert_eq!(models[0]["defaultThinkingBudget"], 0);
        assert_eq!(models[0]["canDisableThinking"], false);
        assert!(models[0]["capabilities"].get("geminiThinking").is_none());
        assert_eq!(models[0]["capabilities"]["thinking"]["adaptive"], false);
    }

    #[test]
    fn quota_fractions_have_independent_resets_and_amounts_have_no_synthetic_total() {
        let usage = normalize_usage(
            &json!({"buckets":[
                {"modelId":"chat", "tokenType":"REQUESTS", "remainingFraction":0.75,
                    "resetTime":"2026-10-19T12:00:00Z"},
                {"modelId":"premium", "tokenType":"TOKENS", "remainingAmount":"4000"}
            ]}),
            &Value::Null,
            Some("account@example.com"),
            Some("Free"),
        );
        assert_eq!(usage["signedIn"], true);
        assert_eq!(usage["account"], "account@example.com");
        assert_eq!(usage["plan"], "Free");
        let windows = usage["windows"].as_array().unwrap();
        assert_eq!(windows.len(), 2);
        let chat = windows
            .iter()
            .find(|window| window["modelId"] == "chat")
            .unwrap();
        assert_eq!(chat["usedPercent"].as_f64(), Some(25.0));
        assert_eq!(chat["resetsAt"], 1792411200000i64);
        let premium = windows
            .iter()
            .find(|window| window["modelId"] == "premium")
            .unwrap();
        assert_eq!(premium["remainingAmount"].as_f64(), Some(4000.0));
        for key in ["usedPercent", "remainingFraction", "total", "resetsAt"] {
            assert!(premium.get(key).is_none());
        }
    }

    #[test]
    fn grouped_summary_preserves_labels_descriptions_and_disabled_buckets() {
        let usage = normalize_usage(
            &Value::Null,
            &json!({"groups":[{
            "displayName":"Premium models", "description":"Shared request allowance", "buckets":[{
                "bucketId":"weekly", "displayName":"Weekly", "window":"P7D",
                "description":"Account restricted", "remainingFraction":0.0, "disabled":true
            }]}]}),
            None,
            None,
        );
        let window = &usage["windows"][0];
        assert_eq!(window["label"], "Weekly");
        assert_eq!(window["groupLabel"], "Premium models");
        assert_eq!(window["groupDescription"], "Shared request allowance");
        assert_eq!(window["description"], "Account restricted");
        assert_eq!(window["usedPercent"].as_f64(), Some(100.0));
        assert_eq!(window["disabled"], true);
    }

    #[test]
    fn deprecated_summary_buckets_work_without_grouped_data() {
        let usage = normalize_usage(
            &Value::Null,
            &json!({"description":"Legacy allowance",
            "buckets":[{"bucketId":"legacy", "remainingAmount":12}]}),
            None,
            None,
        );
        assert_eq!(usage["description"], "Legacy allowance");
        assert_eq!(usage["windows"][0]["bucketId"], "legacy");
        assert_eq!(usage["windows"][0]["remainingAmount"].as_f64(), Some(12.0));
    }

    #[test]
    fn fraction_validation_clamps_finite_values_and_omits_invalid_measurements() {
        let usage = normalize_usage(
            &json!({"buckets":[
                {"modelId":"a", "remainingFraction":-0.5},
                {"modelId":"b", "remainingFraction":1.5},
                {"modelId":"c", "remainingFraction":"NaN", "remainingAmount":"Infinity", "resetTime":"bad"},
                {"modelId":"d", "remainingFraction":"0.25"}
            ]}),
            &Value::Null,
            None,
            None,
        );
        let windows = usage["windows"].as_array().unwrap();
        assert_eq!(windows[0]["usedPercent"].as_f64(), Some(100.0));
        assert_eq!(windows[1]["usedPercent"].as_f64(), Some(0.0));
        assert!(windows[2].get("usedPercent").is_none());
        assert!(windows[2].get("remainingAmount").is_none());
        assert!(windows[2].get("resetsAt").is_none());
        assert_eq!(windows[3]["usedPercent"].as_f64(), Some(75.0));
    }

    #[test]
    fn bucket_identifiers_are_stable_when_the_service_reorders_buckets() {
        let first = json!({"modelId":"first", "tokenType":"REQUESTS", "remainingFraction":0.8});
        let second = json!({"modelId":"second", "tokenType":"TOKENS", "remainingAmount":12});
        let before = normalize_usage(&json!({"buckets":[first,second]}), &Value::Null, None, None);
        let after = normalize_usage(&json!({"buckets":[second,first]}), &Value::Null, None, None);
        assert_eq!(before, after);
    }

    #[test]
    fn unknown_usage_is_absent_and_available_credits_keep_only_supplied_amounts() {
        let missing = normalize_usage(&Value::Null, &Value::Null, None, None);
        assert!(missing.get("windows").is_none());
        assert!(missing.get("credits").is_none());
        let credits = normalize_usage(
            &json!({"availableCredits":"42.5"}),
            &Value::Null,
            None,
            None,
        );
        assert_eq!(credits["credits"]["remaining"].as_f64(), Some(42.5));
        assert!(credits["credits"].get("total").is_none());
        assert!(credits["credits"].get("used").is_none());
    }

    #[test]
    fn native_inventory_exposes_observed_services_without_claiming_an_audio_protocol() {
        let capabilities = native_capabilities(
            &json!({"models":{"image-model":{"displayName":"Image Model"}},
            "imageGenerationModelIds":["image-model","image-model"], "webSearchModelIds":["search-model"],
            "audioTranscriptionModelIds":["speech-model"]}),
        );
        assert_eq!(capabilities["image"]["available"], true);
        assert_eq!(
            capabilities["image"]["models"],
            json!([{"id":"image-model","label":"Image Model"}])
        );
        assert_eq!(capabilities["image"]["referenceImages"], true);
        assert_eq!(capabilities["imageEdit"], capabilities["image"]);
        assert_eq!(capabilities["search"]["available"], true);
        assert_eq!(capabilities["transcription"]["available"], false);
        assert_eq!(
            capabilities["transcription"]["reason"],
            "protocol_unavailable"
        );
        for key in ["video", "audio", "music", "fetch"] {
            assert!(capabilities.get(key).is_none());
        }
        assert_eq!(native_capabilities(&Value::Null), json!({}));
    }

    #[test]
    fn absent_and_empty_native_model_lists_remain_distinguishable() {
        let capabilities = native_capabilities(&json!({"imageGenerationModelIds":[]}));
        assert_eq!(capabilities["image"]["available"], false);
        assert_eq!(capabilities["image"]["models"], json!([]));
        assert!(capabilities.get("search").is_none());
    }
    #[test]
    fn effort_metadata_accepts_only_native_reasoning_vocabulary() {
        assert_eq!(
            declared_efforts(&json!({"efforts": [
                "LOW", "ultra", "turbo", "", "medium", "high", "xhigh", "max", "low"
            ]})),
            vec!["low", "medium", "high", "xhigh", "max"]
        );
    }
}
