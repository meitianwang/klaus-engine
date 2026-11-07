//! The Kimi Code cloud catalog.
//!
//! Which models a membership serves is only knowable from the service: the plan
//! tier decides the list (a Go membership has no coding allowance at all, Plus
//! unlocks the coding models, Pro and above add the Highspeed one), so a
//! hardcoded table would advertise models a user's plan refuses to serve and
//! hide ones it does. The catalog is therefore read from `/v1/models` and kept
//! in two caches — a process-lifetime TTL cache and a snapshot on disk — so a
//! settings pane paints at once and refreshes behind itself.
//!
//! The snapshot is Klaus's own file, written after every successful live read
//! and bound to the account and region. A matching snapshot paints immediately
//! while a live refresh runs in the background.

use serde_json::{json, Map, Value};
use std::sync::{Mutex, OnceLock};

const MODELS_PATH: &str = "/v1/models";
const CATALOG_TTL_MS: i64 = 6 * 60 * 60 * 1000;
const FETCH_TIMEOUT_MS: u64 = 15_000;
/// A picker without a matching snapshot gives the live list this bounded window.
const WARM_TIMEOUT_MS: u64 = 1500;

/// The model a bare tier word names when nothing else can answer.
///
/// One declaration, not a second copy of the module's default: both name the
/// same fact and a duplicated constant is how a picker and a request drift onto
/// different models.
const FALLBACK_MODEL: &str = super::DEFAULT_SELECTION;

/// The catalog is the source of truth for the window; this only bounds a paint
/// that happens before any read has landed.
const FALLBACK_CONTEXT_TOKENS: i64 = 262_144;

/// Anthropic's 1M context beta. Kimi serves two models at exactly that window,
/// and the beta was measured accepted on both.
const CONTEXT_1M_TOKENS: i64 = 1_048_576;

/// The client identity every request carries.
///
/// One honest agent string, used for the catalog, the login and the allowance
/// reads alike: Kimi's terms name spoofing a client identity as a violation,
/// and measurement shows the spoof is not needed — every route here answers 200
/// to it. It says what Klaus is rather than pretending to be Kimi's CLI.
pub(crate) fn user_agent() -> String {
    std::env::var("KIMI_USER_AGENT")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| {
            format!(
                "Klaus-Desktop/{}",
                option_env!("CARGO_PKG_VERSION").unwrap_or("0.0.0")
            )
        })
}

/// Why a catalog read failed, and which of the two recovery paths applies.
///
/// `not_found` means there is nothing to list (signed out, or the plan serves no
/// coding model), and `login_expired` means the credential was refused so the
/// host should send the user back to a login rather than retry.
#[derive(Debug, Clone)]
pub struct KimiError {
    pub message: String,
    pub not_found: bool,
    pub login_expired: bool,
}

impl std::fmt::Display for KimiError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

struct CatalogCache {
    fingerprint: Option<String>,
    models: Vec<Value>,
    expires_at: i64,
}

fn catalog_cache() -> &'static Mutex<Option<CatalogCache>> {
    static CACHE: OnceLock<Mutex<Option<CatalogCache>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(None))
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn http_client() -> Result<reqwest::Client, KimiError> {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(FETCH_TIMEOUT_MS))
        .user_agent(user_agent())
        .build()
        .map_err(|error| KimiError {
            message: error.to_string(),
            not_found: false,
            login_expired: false,
        })
}

async fn fetch_live_models() -> Result<Vec<Value>, KimiError> {
    let token = super::auth::access_token().await?;
    let http = http_client()?;
    // A console key is spent as `x-api-key`; a device-flow token is a bearer.
    // `access_token` resolves to whichever is stored, and `auth_type` says which
    // header that value belongs in — sending both would leave the service to
    // choose, and then a failure could not be attributed to either credential.
    let header = match super::auth::auth_type() {
        Some(super::auth::KimiAuthType::ApiKey) => ("x-api-key", token),
        _ => ("authorization", format!("Bearer {token}")),
    };
    let response = http
        .get(format!("{}{MODELS_PATH}", super::auth::api_base()))
        .header(header.0, header.1)
        .send()
        .await
        .map_err(|error| KimiError {
            message: error.to_string(),
            not_found: false,
            login_expired: false,
        })?;
    let status_code = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    if status_code == 401 || status_code == 403 {
        return Err(KimiError {
            message: "the Kimi credential was refused".into(),
            not_found: false,
            login_expired: true,
        });
    }
    if !(200..300).contains(&status_code) {
        let detail: String = body.chars().take(200).collect();
        return Err(KimiError {
            message: format!("the Kimi catalog answered {status_code}: {detail}"),
            not_found: false,
            login_expired: false,
        });
    }
    let parsed: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    let rows = parsed
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let models: Vec<Value> = rows.iter().filter_map(catalog_entry).collect();
    if models.is_empty() {
        // A plan with no coding allowance answers with an empty list rather than
        // an error, and that is a real state worth naming: the lowest Kimi tier
        // carries no coding quota at all. Prose rather than a stable code, because
        // this sentence is what the settings tier card renders when a catalog read
        // fails — a code there would print as a code.
        return Err(KimiError {
            message: "this Kimi plan serves no coding models; Kimi Code needs a Plus plan or above"
                .into(),
            not_found: true,
            login_expired: false,
        });
    }
    Ok(models)
}

/// Project one service row onto the entry the picker and the capability reader
/// consume.
///
/// Only what the service states, plus the two things measured on the live
/// endpoint, is carried. Nothing here guesses a capability from a model id: a
/// declared field that the service later renames shows up as an absent
/// capability rather than as a model that silently stopped reasoning.
fn catalog_entry(model: &Value) -> Option<Value> {
    let key = string_of(model, "id")?;
    let mut entry = Map::new();
    entry.insert("key".into(), json!(key));
    entry.insert(
        "display_name".into(),
        json!(string_of(model, "display_name").unwrap_or_else(|| key.clone())),
    );
    entry.insert(
        "is_vl".into(),
        json!(truthy(model.get("supports_image_in"))),
    );
    entry.insert(
        "contextTokens".into(),
        json!(positive_i64(model.get("context_length")).unwrap_or(FALLBACK_CONTEXT_TOKENS)),
    );
    // `supports_dynamic_tools` is the service's own statement that this model
    // takes tool calls. The Highspeed row omits it, and a model without tools
    // cannot drive an agent session at all — so carry it through rather than let
    // the picker offer a model that fails on the first tool call.
    entry.insert(
        "tools".into(),
        json!(truthy(model.get("supports_dynamic_tools"))),
    );

    let efforts = model.get("think_efforts");
    if truthy(efforts.and_then(|group| group.get("support"))) {
        let levels: Vec<String> = efforts
            .and_then(|group| group.get("valid_efforts"))
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .filter_map(engine_effort_level)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        if !levels.is_empty() {
            entry.insert("efforts".into(), json!(levels));
            if let Some(default) = efforts
                .and_then(|group| group.get("default_effort"))
                .and_then(Value::as_str)
                .and_then(engine_effort_level)
                .filter(|default| levels.iter().any(|level| level == default))
            {
                entry.insert("defaultEffort".into(), json!(default));
            }
        }
    }
    // The service declares `supports_thinking_type: "only"` on all four models,
    // which reads as "always thinks". Measured, every one of them accepts
    // `thinking: {"type":"disabled"}` and answers with a text-only block, so the
    // honest declaration is that thinking can be turned off. Trusting the field
    // instead would hide a control that works.
    if matches!(
        key.as_str(),
        "kimi-for-coding" | "kimi-for-coding-highspeed" | "k3" | "k3-256k"
    ) {
        entry.insert("canDisableThinking".into(), json!(true));
    }
    super::super::conversation_catalog::mark_purpose("kimi", model, &mut entry);
    Some(Value::Object(entry))
}

/// The service's effort vocabulary onto the engine's five-rung ladder.
///
/// Kimi ships `low`, `high` and `max` — no `medium` and no `xhigh` — and a rung
/// the engine cannot spell must not reach the wire. Absent means "not a rung
/// this engine knows", so the entry declares a shorter ladder rather than
/// padding it out.
fn engine_effort_level(level: &str) -> Option<&'static str> {
    match level.trim().to_ascii_lowercase().as_str() {
        "low" => Some("low"),
        "medium" => Some("medium"),
        "high" => Some("high"),
        "xhigh" => Some("xhigh"),
        "max" => Some("max"),
        _ => None,
    }
}

fn string_of(source: &Value, key: &str) -> Option<String> {
    source
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn positive_i64(value: Option<&Value>) -> Option<i64> {
    let number = match value {
        Some(Value::Number(number)) => number.as_i64()?,
        Some(Value::String(text)) => text.trim().parse::<i64>().ok()?,
        _ => return None,
    };
    (number > 0).then_some(number)
}

fn truthy(value: Option<&Value>) -> bool {
    matches!(value, Some(Value::Bool(true)))
}

fn owned_catalog_path() -> std::path::PathBuf {
    super::config_root().join("kimi-model-catalog.json")
}

/// Seed the on-disk snapshot so a test resolves against a known catalog.
///
/// With a snapshot present and the TTL cache empty, [`fetch_models`] hands it
/// back at once and refreshes behind itself, so a resolution test asserts
/// against a fixed list instead of whatever the network answered.
#[cfg(test)]
pub fn seed_owned_catalog(models: &[Value]) {
    if let Some(fingerprint) = super::auth::catalog_fingerprint() {
        write_owned_catalog(models, &fingerprint);
    }
}

fn read_local_models() -> Vec<Value> {
    let Some(fingerprint) = super::auth::catalog_fingerprint() else {
        return Vec::new();
    };
    std::fs::read_to_string(owned_catalog_path())
        .ok()
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
        .filter(|value| {
            value.get("fingerprint").and_then(Value::as_str) == Some(fingerprint.as_str())
        })
        .and_then(|value| value.get("models").and_then(Value::as_array).cloned())
        .unwrap_or_default()
}

fn write_owned_catalog(models: &[Value], fingerprint: &str) {
    let path = owned_catalog_path();
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Ok(serialized) =
        serde_json::to_string(&json!({ "models": models, "fingerprint": fingerprint }))
    {
        let _ = std::fs::write(&path, serialized);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
        }
    }
}

/// Invalidate the catalog on credential changes. Memory is cleared; a disk
/// snapshot remains unreadable unless its fingerprint matches the current account
/// and region.
pub fn forget_cache() {
    *catalog_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
}

async fn refresh_live_catalog() -> Result<Vec<Value>, KimiError> {
    let fingerprint = super::auth::catalog_fingerprint();
    let models = fetch_live_models().await?;
    if fingerprint != super::auth::catalog_fingerprint() {
        return Ok(models);
    }
    let mut guard = catalog_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *guard = Some(CatalogCache {
        fingerprint: fingerprint.clone(),
        models: models.clone(),
        expires_at: now_ms() + CATALOG_TTL_MS,
    });
    drop(guard);
    if let Some(fingerprint) = fingerprint.as_deref() {
        write_owned_catalog(&models, fingerprint);
    }
    Ok(models)
}

/// A settings page's read: stale-while-revalidate, so the pane paints at once
/// and refreshes behind itself.
pub async fn fetch_models() -> Result<Vec<Value>, KimiError> {
    {
        let guard = catalog_cache()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(cache) = guard
            .as_ref()
            .filter(|cache| cache.fingerprint == super::auth::catalog_fingerprint())
        {
            if now_ms() < cache.expires_at {
                return Ok(cache.models.clone());
            }
        }
    }
    let stale = {
        let guard = catalog_cache()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match guard.as_ref() {
            Some(cache)
                if !cache.models.is_empty()
                    && cache.fingerprint == super::auth::catalog_fingerprint() =>
            {
                cache.models.clone()
            }
            _ => read_local_models(),
        }
    };
    if !stale.is_empty() {
        tokio::spawn(async {
            if let Err(error) = refresh_live_catalog().await {
                eprintln!("[Kimi] catalog refresh failed: {}", error.message);
            }
        });
        return Ok(stale);
    }
    match refresh_live_catalog().await {
        Ok(models) => Ok(models),
        Err(error) => {
            let local = read_local_models();
            if local.is_empty() {
                Err(error)
            } else {
                Ok(local)
            }
        }
    }
}

/// A picker's read: signed out is an ERROR, so the picker paints empty and
/// agrees with the settings pane instead of offering models a credential that
/// has gone cannot serve.
pub async fn fetch_models_warm() -> Result<Vec<Value>, KimiError> {
    if super::auth::credential().is_none() {
        // Phrased like the other sources rather than as a stable code, because the
        // settings pane recognizes a signed-out catalog by this sentence and hides
        // the tier-mapping card instead of reporting a load failure: the account
        // card above already says "not connected" and holds the connect control.
        return Err(KimiError {
            message: "not signed in to a Kimi membership".into(),
            not_found: true,
            login_expired: false,
        });
    }
    {
        let guard = catalog_cache()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(cache) = guard
            .as_ref()
            .filter(|cache| cache.fingerprint == super::auth::catalog_fingerprint())
        {
            if now_ms() < cache.expires_at && !cache.models.is_empty() {
                return Ok(cache.models.clone());
            }
        }
    }
    let local = read_local_models();
    if !local.is_empty() {
        tokio::spawn(async {
            if let Err(error) = refresh_live_catalog().await {
                eprintln!("[Kimi] catalog refresh failed: {}", error.message);
            }
        });
        return Ok(local);
    }
    // Without a matching snapshot, give the live list a bounded first chance.
    match tokio::time::timeout(
        std::time::Duration::from_millis(WARM_TIMEOUT_MS),
        refresh_live_catalog(),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => {
            let local = read_local_models();
            if local.is_empty() {
                Err(KimiError {
                    message: "the Kimi catalog did not answer in time".into(),
                    not_found: false,
                    login_expired: false,
                })
            } else {
                Ok(local)
            }
        }
    }
}

/// The catalog head: what a bare tier word resolves to.
///
/// Prefers [`super::DEFAULT_SELECTION`] when the catalog serves it, so a bare
/// selection lands on the plan's own default rather than on whatever order the
/// service happens to list first. The head is the fallback for a lineup that no
/// longer carries the default.
pub fn first_model_key() -> Option<String> {
    let guard = catalog_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let models = match guard.as_ref() {
        Some(cache)
            if !cache.models.is_empty()
                && cache.fingerprint == super::auth::catalog_fingerprint() =>
        {
            cache.models.clone()
        }
        _ => {
            drop(guard);
            read_local_models()
        }
    };
    models
        .iter()
        .find(|model| model.get("key").and_then(Value::as_str) == Some(FALLBACK_MODEL))
        .or_else(|| models.first())
        .and_then(|model| model.get("key"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// The catalog key a requested key actually maps to.
///
/// An unknown key falls back to the plan's default rather than going to the wire
/// as-is: the service refuses a model the plan does not list, and a picker holding
/// a stale selection should land on the model a bare selection names.
pub fn resolve_model_key(models: &[Value], requested: &str) -> String {
    if models
        .iter()
        .any(|model| model.get("key").and_then(Value::as_str) == Some(requested))
    {
        return requested.to_string();
    }
    first_key_of(models).unwrap_or_else(|| FALLBACK_MODEL.to_string())
}

fn first_key_of(models: &[Value]) -> Option<String> {
    models
        .iter()
        .find(|model| model.get("key").and_then(Value::as_str) == Some(FALLBACK_MODEL))
        .or_else(|| models.first())
        .and_then(|model| model.get("key"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

pub async fn lookup_model(key: &str) -> Option<Value> {
    let models = fetch_models().await.ok()?;
    models
        .into_iter()
        .find(|model| model.get("key").and_then(Value::as_str) == Some(key))
}

pub fn context_window(entry: Option<&Value>) -> i64 {
    entry
        .and_then(|model| model.get("contextTokens"))
        .and_then(Value::as_i64)
        .filter(|tokens| *tokens > 0)
        .unwrap_or(FALLBACK_CONTEXT_TOKENS)
}

/// What a Kimi model can do, declared from the catalog.
///
/// Every field is either the service's own statement or something measured
/// against the live endpoint; nothing is inferred from the model id. The fields
/// deliberately absent are the unmeasured ones — structured output, native PDF
/// and server-side web search — so the engine falls back to its own emulation
/// rather than being told a capability that was never verified.
pub async fn model_capabilities(key: &str) -> Value {
    capabilities_of(lookup_model(key).await.as_ref())
}

/// The projection itself, over an entry already in hand.
///
/// Split out from the async lookup so the declared capability set is testable
/// against a captured catalog row instead of against a copy of this code.
pub fn capabilities_of(entry: Option<&Value>) -> Value {
    let window = context_window(entry);
    let mut capabilities = json!({
        "vision": entry
            .and_then(|model| model.get("is_vl"))
            .and_then(Value::as_bool)
            .unwrap_or(false),
        "contextWindow": window,
        // Prompt-cache breakpoints were measured accepted: a system block with
        // `cache_control` answered 200 rather than rejecting the field.
        "promptCacheControl": true,
        // Thinking is on by default here and both shapes the engine sends were
        // measured accepted — `adaptive` and `enabled` with a budget each
        // produced a thinking block, and an explicit disable produced text only.
        "thinking": {
            "adaptive": true,
            "interleaved": false,
            "canDisableThinking": entry
                .and_then(|model| model.get("canDisableThinking"))
                .and_then(Value::as_bool)
                .unwrap_or(false),
        },
        "tools": entry
            .and_then(|model| model.get("tools"))
            .and_then(Value::as_bool)
            .unwrap_or(false),
    });
    // K3.1 Pro rejects specified tool choice while thinking is enabled, but
    // accepts automatic tool choice. Project the measured restriction here so
    // older account-bound catalog snapshots receive it before a live refresh.
    if entry
        .and_then(|model| model.get("key"))
        .and_then(Value::as_str)
        == Some("k3.1-pro")
    {
        capabilities["forcedToolChoiceWithThinking"] = json!(false);
    }
    // The 1M beta was measured accepted on the models that serve that window.
    if window >= CONTEXT_1M_TOKENS {
        capabilities["oneMillionContext"] = json!(true);
    }
    if let Some(model) = entry {
        let levels: Vec<&str> = model
            .get("efforts")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .fold(Vec::new(), |mut levels, level| {
                if !levels.contains(&level) {
                    levels.push(level);
                }
                levels
            });
        // An empty ladder declares NO effort knob, which is a different statement
        // from "the full ladder": the Highspeed model advertises none, and
        // substituting rungs would let a turn ask for a level the service never
        // offered. Omit the key so the provider reads it as absent.
        if !levels.is_empty() {
            let mut effort = json!({ "levels": levels });
            if let Some(default) = model
                .get("defaultEffort")
                .and_then(Value::as_str)
                .filter(|default| levels.contains(default))
            {
                effort["default"] = json!(default);
            }
            capabilities["effort"] = effort;
        }
    }
    capabilities
}

#[cfg(test)]
mod tests {
    use super::*;
    crate::app_server::catalog_test_support::warm_catalog_tests!("kimi", read_local_models);

    #[test]
    fn measured_forced_tool_choice_restriction_applies_to_existing_catalog_snapshots() {
        let cached = json!({"key":"k3.1-pro","tools":true});
        assert_eq!(
            capabilities_of(Some(&cached))["forcedToolChoiceWithThinking"],
            json!(false)
        );
        for key in ["k3", "future-model"] {
            assert!(capabilities_of(Some(&json!({"key":key})))
                .get("forcedToolChoiceWithThinking")
                .is_none());
        }
        assert!(capabilities_of(None)
            .get("forcedToolChoiceWithThinking")
            .is_none());
    }

    /// The live body, captured 2026-09-29 from `/v1/models` on a Pro membership.
    /// Four models; the Highspeed row carries no `think_efforts` at all.
    fn live_row(id: &str, extra: Value) -> Value {
        let mut value = json!({
            "id": id,
            "display_name": id.to_uppercase(),
            "object": "model",
            "type": "model",
            "supports_reasoning": true,
            "supports_image_in": true,
            "supports_video_in": true,
            "supports_dynamic_tools": true,
            "supports_thinking_type": "only",
        });
        if let (Some(map), Some(extra)) = (value.as_object_mut(), extra.as_object()) {
            for (key, item) in extra {
                map.insert(key.clone(), item.clone());
            }
        }
        value
    }

    #[test]
    fn the_coding_head_declares_the_three_rungs_kimi_ships() {
        let entry = catalog_entry(&live_row(
            "kimi-for-coding",
            json!({
                "display_name": "K2.8 Preview",
                "context_length": 1048576,
                "think_efforts": {
                    "support": true,
                    "valid_efforts": ["low", "high", "max"],
                    "default_effort": "max",
                },
            }),
        ))
        .expect("entry");
        assert_eq!(entry["key"], json!("kimi-for-coding"));
        assert_eq!(entry["display_name"], json!("K2.8 Preview"));
        assert_eq!(entry["efforts"], json!(["low", "high", "max"]));
        assert_eq!(entry["defaultEffort"], json!("max"));
        assert_eq!(entry["contextTokens"], json!(1048576));
        assert_eq!(entry["is_vl"], json!(true));
        assert_eq!(entry["tools"], json!(true));
    }

    #[test]
    fn a_model_advertising_no_efforts_declares_no_effort_knob() {
        let entry = catalog_entry(&live_row(
            "kimi-for-coding-highspeed",
            json!({ "display_name": "K2.7 Code Highspeed", "context_length": 262144,
                    "supports_dynamic_tools": false }),
        ))
        .expect("entry");
        assert!(entry.get("efforts").is_none(), "no ladder must be invented");
        assert!(entry.get("defaultEffort").is_none());
        assert_eq!(entry["tools"], json!(false));
        let capabilities = capabilities_of(Some(&entry));
        assert!(capabilities.get("effort").is_none());
        assert_eq!(capabilities["tools"], json!(false));
    }

    #[test]
    fn thinking_can_be_disabled_even_though_the_service_says_only() {
        // `supports_thinking_type: "only"` reads as "always thinks", but every
        // model was measured accepting an explicit disable.
        let entry =
            catalog_entry(&live_row("k3", json!({ "context_length": 1048576 }))).expect("entry");
        assert_eq!(entry["canDisableThinking"], json!(true));
    }

    #[test]
    fn an_unknown_effort_rung_is_dropped_not_padded() {
        let entry = catalog_entry(&live_row(
            "kimi-for-coding",
            json!({
                "think_efforts": {
                    "support": true,
                    "valid_efforts": ["low", "turbo", "max"],
                    "default_effort": "turbo",
                },
            }),
        ))
        .expect("entry");
        assert_eq!(entry["efforts"], json!(["low", "max"]));
        // A default the engine cannot spell must not become one.
        assert!(entry.get("defaultEffort").is_none());
    }

    #[test]
    fn a_row_without_an_id_is_not_a_model() {
        assert!(catalog_entry(&json!({ "display_name": "nameless" })).is_none());
    }

    #[test]
    fn capabilities_follow_the_catalog_window_and_declare_the_1m_beta() {
        let wide =
            catalog_entry(&live_row("k3", json!({ "context_length": 1048576 }))).expect("entry");
        let wide_capabilities = capabilities_of(Some(&wide));
        assert_eq!(wide_capabilities["contextWindow"], json!(1048576));
        assert_eq!(wide_capabilities["oneMillionContext"], json!(true));

        let narrow = catalog_entry(&live_row("k3-256k", json!({ "context_length": 262144 })))
            .expect("entry");
        let narrow_capabilities = capabilities_of(Some(&narrow));
        assert_eq!(narrow_capabilities["contextWindow"], json!(262144));
        assert!(narrow_capabilities.get("oneMillionContext").is_none());
    }

    #[test]
    fn unmeasured_capabilities_are_absent_not_assumed() {
        let entry =
            catalog_entry(&live_row("k3", json!({ "context_length": 262144 }))).expect("entry");
        let capabilities = capabilities_of(Some(&entry));
        for absent in ["structuredOutput", "nativePdf", "webSearch"] {
            assert!(
                capabilities.get(absent).is_none(),
                "{absent} was never measured and must not be declared"
            );
        }
        assert_eq!(capabilities["promptCacheControl"], json!(true));
        assert_eq!(capabilities["thinking"]["adaptive"], json!(true));
    }

    #[test]
    fn an_unknown_key_falls_back_to_the_plan_default() {
        let models = vec![
            catalog_entry(&live_row("kimi-for-coding", json!({}))).expect("entry"),
            catalog_entry(&live_row("k3", json!({}))).expect("entry"),
        ];
        assert_eq!(resolve_model_key(&models, "k3"), "k3");
        // A stale selection lands on the plan's default, the same model a bare
        // selection names, rather than on whichever row the service listed first.
        assert_eq!(resolve_model_key(&models, "retired-model"), "k3");
        assert_eq!(resolve_model_key(&[], "anything"), FALLBACK_MODEL);
    }

    #[test]
    fn a_stale_capacity_string_is_read_as_a_number() {
        assert_eq!(positive_i64(Some(&json!("1048576"))), Some(1048576));
        assert_eq!(positive_i64(Some(&json!(262144))), Some(262144));
        assert_eq!(positive_i64(Some(&json!(0))), None);
        assert_eq!(positive_i64(Some(&json!("not-a-number"))), None);
        assert_eq!(positive_i64(None), None);
    }

    #[test]
    fn an_absent_entry_declares_no_window_and_no_effort() {
        let capabilities = capabilities_of(None);
        assert_eq!(
            capabilities["contextWindow"],
            json!(FALLBACK_CONTEXT_TOKENS)
        );
        assert!(capabilities.get("effort").is_none());
        assert!(capabilities.get("oneMillionContext").is_none());
        assert_eq!(capabilities["vision"], json!(false));
        assert_eq!(capabilities["tools"], json!(false));
    }
    #[test]
    fn a_model_without_a_measured_thinking_off_override_defaults_to_required_thinking() {
        let entry = catalog_entry(&json!({"id": "k3.1-pro"})).expect("entry");
        assert!(entry.get("canDisableThinking").is_none());
        let capabilities = capabilities_of(Some(&entry));
        assert_eq!(
            capabilities["thinking"]["canDisableThinking"],
            json!(false),
            "an undeclared model must not be sent the disabled-thinking shape"
        );
    }

    #[test]
    fn new_models_do_not_inherit_a_measured_thinking_off_override() {
        let entry = catalog_entry(&json!({"id": "unmeasured-future-model"})).expect("entry");
        assert!(entry.get("canDisableThinking").is_none());
        assert!(entry.get("efforts").is_none());
        let declared = catalog_entry(&json!({"id": "unmeasured-future-model", "think_efforts": {
            "support": true, "valid_efforts": ["low"], "default_effort": "high"
        }}))
        .expect("entry");
        assert_eq!(declared["efforts"], json!(["low"]));
        assert!(declared.get("defaultEffort").is_none());
    }
}
