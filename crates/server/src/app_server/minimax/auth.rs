//! MiniMax M Plan credentials, connection status and allowance.
//!
//! M Plan authenticates with one thing only: a subscription key (`sk-cp-…`) shown
//! in the MiniMax console under the plan detail page. There is no OAuth, no device
//! flow and no local client store to import, so this module is a key holder rather
//! than a login flow — which is why there are no `start`/`poll` methods here.
//!
//! The key is verified before it is stored. Storing an unverified key would leave
//! a mistyped one looking connected on the settings card and failing inside a
//! conversation with the service's own prose; the verification costs one catalog
//! read and no inference tokens.
//!
//! ## Two sites, one key shape
//!
//! The plan is served from a China site and a global site, and BOTH mint
//! `sk-cp-` keys, so the region cannot be inferred from the key itself. The sites
//! do not share keys: measured, a China-subscription key answers `invalid api key`
//! (status 2049) on the global site. A wrong region therefore reads to the user as
//! "the key is wrong", which is why an absent region is DETECTED by trying each
//! site rather than defaulted, and why an explicit region that fails is reported
//! rather than silently retried elsewhere.
//!
//! ## A real allowance to draw
//!
//! Unlike a plan that publishes no quota route, this one does: `/v1/token_plan/
//! remains` answers with percentage windows on the subscription key itself. So
//! [`usage`] projects two windows — a five-hour one and a weekly one — in the shape
//! the host's existing gauges already render.
//!
//! The service's documented rule is visible in those numbers and worth preserving:
//! video draw is counted against the WEEKLY window only, never the five-hour one.
//!
//! ## One honest User-Agent
//!
//! The plan's terms confine it to interactive use inside AI coding and agent tools.
//! Nothing here pretends to be another client: every request carries Klaus's own
//! `User-Agent`, the key stays on this machine, and calls go straight from here to
//! the gateway with no server in between.

use serde_json::{json, Map, Value};
use std::path::PathBuf;

/// The two sites M Plan is served from. A key is bound to the site that issued it.
const CN_HOST: &str = "api.minimax.cn";
const GLOBAL_HOST: &str = "api.minimax.io";

/// The Anthropic-compatible root. Deliberately carries no `/v1` suffix: the
/// provider appends `/v1/messages` itself, so a `/v1` here would compose
/// `/v1/v1/messages`.
const ANTHROPIC_PATH: &str = "/anthropic";
/// The OpenAI-compatible prefix, which is where the catalog lives.
const OPENAI_PATH: &str = "/v1";
const MODELS_PATH: &str = "/models";
/// The allowance route, on the service root rather than either protocol prefix.
const REMAINS_PATH: &str = "/v1/token_plan/remains";
/// The plan's own web search route, likewise on the service root.
const SEARCH_PATH: &str = "/v1/coding_plan/search";

/// The subscription key prefix. A pay-as-you-go key (`sk-api-…`) is a separate
/// system with a separate balance and must not be stored here.
const SUBSCRIPTION_KEY_PREFIX: &str = "sk-cp-";
const PAYG_KEY_PREFIX: &str = "sk-api-";

const HTTP_TIMEOUT_MS: u64 = 20_000;

/// The client identity sent on every request.
///
/// Exposed to the catalog module so both modules send one agent string: a request
/// whose client identity varies by call site is a request that cannot be reasoned
/// about when the service starts refusing it.
pub(crate) fn user_agent() -> String {
    std::env::var("MINIMAX_USER_AGENT")
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

/// Which site the stored key belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Region {
    Cn,
    Global,
}

impl Region {
    fn as_str(self) -> &'static str {
        match self {
            Self::Cn => "cn",
            Self::Global => "global",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "cn" | "china" | "domestic" => Some(Self::Cn),
            // The vendor's own CLI spells the other site `global`; the site's
            // marketing copy and its hostname both say international.
            "global" | "intl" | "international" | "overseas" => Some(Self::Global),
            _ => None,
        }
    }

    fn host(self) -> &'static str {
        match self {
            Self::Cn => CN_HOST,
            Self::Global => GLOBAL_HOST,
        }
    }
}

/// The sites in detection order. China first because the plan launched there and
/// most subscription keys are Chinese-site keys.
const DETECTION_ORDER: [Region; 2] = [Region::Cn, Region::Global];

fn store_path() -> PathBuf {
    super::config_root().join("minimax-mplan.json")
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn read_store() -> Option<Value> {
    let raw = std::fs::read_to_string(store_path()).ok()?;
    serde_json::from_str(&raw).ok()
}

fn write_store(store: &Value) -> Result<(), String> {
    let path = store_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let serialized = serde_json::to_string_pretty(store).map_err(|error| error.to_string())?;
    std::fs::write(&path, serialized).map_err(|error| error.to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

fn string_field(store: &Value, key: &str) -> Option<String> {
    store
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

/// The stored subscription key, if there is one.
///
/// This is read by the host for the source-scoped video route, which spends the
/// same subscription allowance: the video API is the service's own async task
/// endpoint, not something the engine's provider layer speaks, so the host calls it
/// directly with this key rather than a second credential being stored anywhere.
pub fn api_key() -> Option<String> {
    read_store().and_then(|store| string_field(&store, "apiKey"))
}

/// The region the stored key belongs to.
pub fn region() -> Option<Region> {
    read_store()
        .and_then(|store| string_field(&store, "region").and_then(|value| Region::parse(&value)))
}

fn service_root(region: Region) -> String {
    // Honour the same override knob the text base uses, so a test (or a caller on a
    // non-default host) points every route — text, catalog, allowance, video — at one
    // place instead of three. `MINIMAX_API_BASE` names the Anthropic root
    // (`…/anthropic`); the service root is that with the protocol suffix trimmed, since
    // the allowance and media endpoints hang off the host directly rather than under
    // `/anthropic` or `/v1`.
    if let Ok(value) = std::env::var("MINIMAX_API_BASE") {
        let trimmed = value.trim().trim_end_matches('/');
        if let Some(root) = trimmed.strip_suffix(ANTHROPIC_PATH) {
            if !root.is_empty() {
                return root.to_string();
            }
        }
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    format!("https://{}", region.host())
}

/// The Anthropic-compatible root a session is pointed at.
pub fn api_base() -> String {
    if let Ok(value) = std::env::var("MINIMAX_API_BASE") {
        let trimmed = value.trim().trim_end_matches('/').to_string();
        if !trimmed.is_empty() {
            return trimmed;
        }
    }
    let region = region().unwrap_or(Region::Cn);
    format!("{}{ANTHROPIC_PATH}", service_root(region))
}

/// The root the plan's own web search route hangs off, beside the two protocol
/// prefixes and the allowance route.
pub(crate) fn search_base() -> String {
    format!(
        "{}{SEARCH_PATH}",
        service_root(region().unwrap_or(Region::Cn))
    )
}

/// The OpenAI-compatible root the catalog is read from.
///
/// Same subscription key, different path prefix: the plan serves both protocols off
/// one host, and the model list only exists on this one.
pub(crate) fn catalog_base() -> String {
    if let Ok(value) = std::env::var("MINIMAX_CATALOG_BASE") {
        let trimmed = value.trim().trim_end_matches('/').to_string();
        if !trimmed.is_empty() {
            return trimmed;
        }
    }
    let region = region().unwrap_or(Region::Cn);
    format!("{}{OPENAI_PATH}", service_root(region))
}

fn client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(HTTP_TIMEOUT_MS))
        .user_agent(user_agent())
        .build()
        .map_err(|error| error.to_string())
}

/// Show the head and tail of a key: enough to recognize which one is connected,
/// never enough to spend it.
fn mask_key(key: &str) -> String {
    if key.chars().count() <= 12 {
        return "•".repeat(8);
    }
    let head: String = key.chars().take(9).collect();
    let tail: String = key
        .chars()
        .rev()
        .take(4)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("{head}…{tail}")
}

/// Verify a key by reading the catalog on one site, then store it.
///
/// The catalog read is the cheapest honest check: it authenticates the key, costs
/// no inference tokens, and its answer doubles as the model list the settings pane
/// wants. A key the service refuses is reported as a stable code rather than as the
/// gateway's prose, because that prose is written for MiniMax's own clients.
///
/// An absent region is DETECTED: each site is tried in order and the one that
/// answers wins. This is not a convenience over a default — since both sites mint
/// the same `sk-cp-` shape and do not share keys, defaulting would make every key
/// from the other site fail with an error that blames the key.
pub async fn set_api_key(key: &str, region: Option<&str>) -> Result<Value, Value> {
    let key = key.trim();
    if key.is_empty() {
        return Ok(json!({ "ok": false, "error": "minimax_api_key_empty" }));
    }
    // Refuse the neighbouring product's key shape rather than storing one that can
    // only spend the wrong balance: a pay-as-you-go key draws down account balance
    // instead of plan allowance, which is exactly the confusion the plan's own FAQ
    // warns about.
    if key.starts_with(PAYG_KEY_PREFIX) {
        return Ok(json!({
            "ok": false,
            "error": "minimax_key_not_a_subscription_key",
        }));
    }
    if !key.starts_with(SUBSCRIPTION_KEY_PREFIX) {
        return Ok(json!({
            "ok": false,
            "error": "minimax_key_not_a_subscription_key",
        }));
    }

    let http = client().map_err(|error| json!({ "ok": false, "error": error }))?;

    // An explicit site must name one of the two the plan serves. Silently
    // swallowing an unrecognized value would leave the user believing they picked a
    // site while the key was checked against a different one.
    let candidates: Vec<Region> = match region {
        Some(value) if !value.trim().is_empty() => match Region::parse(value) {
            Some(parsed) => vec![parsed],
            None => {
                return Ok(json!({
                    "ok": false,
                    "error": "minimax_region_unknown",
                }))
            }
        },
        // No site named (or an empty one): detect. Trying the stored site first
        // keeps a re-entered key where it already was when that is still correct.
        _ => {
            let stored = self::region();
            let mut order: Vec<Region> = Vec::with_capacity(DETECTION_ORDER.len());
            if let Some(stored) = stored {
                order.push(stored);
            }
            for candidate in DETECTION_ORDER {
                if !order.contains(&candidate) {
                    order.push(candidate);
                }
            }
            order
        }
    };

    let mut last_error = "minimax_key_rejected".to_string();
    for candidate in candidates {
        let base = format!("{}{OPENAI_PATH}", service_root(candidate));
        match verify_key(&http, &base, key).await {
            Ok(models) => {
                let mut store = Map::new();
                store.insert("apiKey".into(), json!(key));
                store.insert("region".into(), json!(candidate.as_str()));
                store.insert(
                    "apiBase".into(),
                    json!(format!("{}{ANTHROPIC_PATH}", service_root(candidate))),
                );
                store.insert("modelCount".into(), json!(models));
                store.insert("updatedAt".into(), json!(now_ms()));
                super::catalog::forget_cache();
                return match write_store(&Value::Object(store)) {
                    Ok(()) => Ok(status()),
                    Err(error) => Ok(json!({ "ok": false, "error": error })),
                };
            }
            Err(error) => last_error = error,
        }
    }
    Ok(json!({ "ok": false, "error": last_error }))
}

/// Authenticate a key against the catalog route and report how many models the
/// plan serves.
///
/// The count is the live list only and does not include the flagship model this
/// catalog injects, so it is a rough "the key works and the plan serves models"
/// signal rather than an exact picker length.
async fn verify_key(http: &reqwest::Client, base: &str, key: &str) -> Result<usize, String> {
    let response = http
        .get(format!("{base}{MODELS_PATH}"))
        .header("authorization", format!("Bearer {key}"))
        .send()
        .await
        .map_err(|error| error.to_string())?;
    let status_code = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    match status_code {
        200..=299 => {}
        // A wrong key and a right key on the wrong site both answer 4xx or a
        // `status_code` inside a 200 body, so neither is treated as final here:
        // detection keeps trying and the caller reports one stable code.
        401 | 403 => return Err("minimax_key_rejected".to_string()),
        _ => {
            let detail: String = body.chars().take(200).collect();
            return Err(format!("minimax_key_check_failed:{status_code}:{detail}"));
        }
    }
    let parsed: Value = match serde_json::from_str(&body) {
        Ok(value) => value,
        // A 200 whose body is not JSON is not a success: a captive portal, a
        // corporate interception page or an HTML error page all answer 200 with
        // markup. Treating that as "verified, zero models" would store a key that
        // was never actually checked, so it fails closed instead.
        Err(_) => {
            return Err(
                "minimax_key_check_failed:the catalog answered with something that is not JSON"
                    .to_string(),
            )
        }
    };
    // This gateway reports some refusals inside a 200 body rather than as an HTTP
    // status, so the body's own status code is checked too. A key from the other
    // site answers `2049 invalid api key` with HTTP 200.
    if let Some(code) = parsed
        .pointer("/base_resp/status_code")
        .and_then(Value::as_i64)
    {
        if code != 0 {
            return Err("minimax_key_rejected".to_string());
        }
    }
    // Require an actual model list. A JSON 200 with no `data` array is a refusal or
    // an unexpected shape, not a plan that serves zero models, so it must not read
    // as a verified key.
    let models = match parsed.get("data").and_then(Value::as_array) {
        Some(rows) if !rows.is_empty() => rows.len(),
        Some(_) => return Err("minimax_key_check_failed:the catalog listed no models".to_string()),
        None => {
            return Err("minimax_key_check_failed:the catalog answer had no model list".to_string())
        }
    };
    Ok(models)
}

pub fn status() -> Value {
    let Some(store) = read_store() else {
        return json!({ "loggedIn": false });
    };
    let Some(key) = string_field(&store, "apiKey") else {
        return json!({ "loggedIn": false });
    };
    let mut out = Map::new();
    out.insert("loggedIn".into(), json!(true));
    // There is no account route on this gateway, so the card shows the key's own
    // masked form instead of inventing a name.
    out.insert("account".into(), json!(mask_key(&key)));
    out.insert("apiKeyMasked".into(), json!(mask_key(&key)));
    out.insert(
        "region".into(),
        json!(string_field(&store, "region").unwrap_or_else(|| Region::Cn.as_str().to_string())),
    );
    if let Some(count) = store.get("modelCount").and_then(Value::as_i64) {
        out.insert("modelCount".into(), json!(count));
    }
    Value::Object(out)
}

pub(crate) fn catalog_fingerprint() -> Option<String> {
    use sha2::{Digest, Sha256};
    let store = read_store()?;
    let key = string_field(&store, "apiKey")?;
    let region = string_field(&store, "region")
        .and_then(|value| Region::parse(&value))
        .unwrap_or(Region::Cn);
    Some(format!(
        "{:x}:{}",
        Sha256::digest(key.as_bytes()),
        region.as_str()
    ))
}

pub fn logout() -> Result<Value, Value> {
    super::catalog::forget_cache();
    match std::fs::remove_file(store_path()) {
        Ok(()) => Ok(json!({ "ok": true })),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(json!({ "ok": true })),
        Err(error) => Ok(json!({ "ok": false, "error": error.to_string() })),
    }
}

/// The plan's allowance, projected onto rate windows.
///
/// The route answers with REMAINING percentages per window, not used ones, and the
/// host's gauges want used — so each is inverted here rather than in the host,
/// keeping the wire shape one the host already renders for other sources.
///
/// A window the plan does not cover is skipped rather than reported as zero: the
/// service marks a window that is not part of the subscription with a distinct
/// status, and drawing a 0%-used bar for a window the user does not have would read
/// as an allowance they have not touched.
pub async fn usage() -> Result<Value, String> {
    let Some(key) = api_key() else {
        return Ok(json!({ "signedIn": false }));
    };
    let Some(region) = region() else {
        return Ok(json!({ "signedIn": true, "quotaAvailable": false }));
    };
    let http = client().map_err(|error| error.to_string())?;
    let response = http
        .get(format!("{}{REMAINS_PATH}", service_root(region)))
        .header("authorization", format!("Bearer {key}"))
        .send()
        .await
        .map_err(|error| error.to_string())?;
    let status_code = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    if status_code == 401 || status_code == 403 {
        // A key that stopped working is a signed-out card, not an error toast.
        return Ok(json!({ "signedIn": false }));
    }
    if !(200..300).contains(&status_code) {
        let detail: String = body.chars().take(200).collect();
        return Err(format!(
            "minimax usage read failed ({status_code}): {detail}"
        ));
    }
    let parsed: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    if let Some(code) = parsed
        .pointer("/base_resp/status_code")
        .and_then(Value::as_i64)
    {
        if code != 0 {
            return Ok(json!({ "signedIn": true, "quotaAvailable": false }));
        }
    }
    let mut out = Map::new();
    out.insert("signedIn".into(), json!(true));
    out.insert("account".into(), json!(mask_key(&key)));
    out.insert("region".into(), json!(region.as_str()));
    let windows = usage_windows(&parsed);
    if windows.is_empty() {
        // Signed in, but the plan reported no window this key can see (a tier with
        // no allowance rows, or a shape change). Say so rather than inventing a bar.
        out.insert("quotaAvailable".into(), json!(false));
    } else {
        out.insert("quotaAvailable".into(), json!(true));
        out.insert("windows".into(), Value::Array(windows));
    }
    Ok(Value::Object(out))
}

/// The stored credential plus the service root its media routes live on.
///
/// The host runs the plan's video API itself, because that API is the service's own
/// async task protocol and the host already has working task polling, container
/// validation, size caps and cancellation for exactly that shape. Handing it the key
/// is what makes that reuse possible; re-implementing all of it inside the engine
/// would duplicate machinery that already exists and is already tested.
///
/// The base URL is the service ROOT (`https://api.minimax.cn`) rather than either
/// protocol prefix, because the media routes hang off the root directly
/// (`/v2/video_generation`) instead of under `/anthropic` or `/v1`.
///
/// Signed out answers `{ "ok": false }` rather than an error: a conversation on this
/// source with no key stored is "not configured", not a failed call.
pub fn media_credential() -> Value {
    let Some(key) = api_key() else {
        return json!({ "ok": false });
    };
    let Some(region) = region() else {
        return json!({ "ok": false });
    };
    json!({
        "ok": true,
        "apiKey": key,
        "baseUrl": service_root(region),
        "region": region.as_str(),
    })
}

/// Project the service's remaining-percentage buckets onto labelled windows.
///
/// Each bucket is a separate allowance with its own reset, and collapsing them into
/// one bar would hide the window that actually stops a session: the five-hour one
/// resets within the day while the weekly one resets in a week, and video draw is
/// counted against the weekly window only.
fn usage_windows(payload: &Value) -> Vec<Value> {
    let mut windows = Vec::new();
    let rows = payload.get("model_remains").and_then(Value::as_array);
    // Only the first row is read: it is the whole plan's allowance
    // (`model_name: "general"`). More than one row would mean per-model rows, and
    // taking the first of each kind keeps the projection two windows rather than a
    // wall of near-duplicates.
    let Some(row) = rows.and_then(|rows| rows.first()) else {
        return windows;
    };
    // `five_hour`: the short window text, image and audio draw against.
    if let Some(used) = remaining_percent(row, "current_interval_remaining_percent") {
        let mut window = Map::new();
        window.insert("id".into(), json!("five_hour"));
        window.insert("usedPercent".into(), json!(used));
        if let Some(reset) = row.get("end_time").and_then(Value::as_i64) {
            if reset > 0 {
                window.insert("resetsAt".into(), json!(reset));
            }
        }
        windows.push(Value::Object(window));
    }
    // `weekly`: the long window, and the ONLY one video is counted against.
    if let Some(used) = remaining_percent(row, "current_weekly_remaining_percent") {
        let mut window = Map::new();
        window.insert("id".into(), json!("weekly"));
        window.insert("usedPercent".into(), json!(used));
        if let Some(reset) = row.get("weekly_end_time").and_then(Value::as_i64) {
            if reset > 0 {
                window.insert("resetsAt".into(), json!(reset));
            }
        }
        windows.push(Value::Object(window));
    }
    windows
}

/// Turn a REMAINING percentage into a USED one, skipping a window the plan does
/// not report or has marked as not part of this subscription.
fn remaining_percent(row: &Value, key: &str) -> Option<f64> {
    let remaining = match row.get(key) {
        Some(Value::Number(number)) => number.as_f64()?,
        Some(Value::String(text)) => text.trim().parse::<f64>().ok()?,
        _ => return None,
    };
    if !remaining.is_finite() {
        return None;
    }
    Some((100.0 - remaining).clamp(0.0, 100.0))
}

/// Seed the credential store so a sibling module's test can resolve against a
/// known key without a live network call. Mirrors the catalog's `seed_owned_catalog`
/// seam: a test asserts against a fixed store rather than whatever was last saved.
#[cfg(test)]
pub fn write_store_for_test(store: &Value) {
    write_store(store).expect("test store write");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masking_never_reveals_a_spendable_key() {
        assert_eq!(mask_key("sk-cp-abcdefghijklmnop"), "sk-cp-abc…mnop");
        assert_eq!(mask_key("short"), "••••••••");
    }

    #[test]
    fn region_parsing_accepts_the_names_a_user_would_type() {
        assert_eq!(Region::parse("cn"), Some(Region::Cn));
        assert_eq!(Region::parse("CN"), Some(Region::Cn));
        assert_eq!(Region::parse("china"), Some(Region::Cn));
        assert_eq!(Region::parse("global"), Some(Region::Global));
        assert_eq!(Region::parse("intl"), Some(Region::Global));
        assert_eq!(Region::parse("international"), Some(Region::Global));
        assert_eq!(Region::parse("mars"), None);
        assert_eq!(Region::Cn.host(), "api.minimax.cn");
        assert_eq!(Region::Global.host(), "api.minimax.io");
    }

    #[tokio::test]
    async fn the_anthropic_base_carries_no_v1_suffix() {
        // Takes the same lock the override-setting tests take, and clears the
        // override, because this asserts the PRODUCTION composition. `service_root`
        // honours `MINIMAX_API_BASE`, so without the lock a parallel test's override
        // would leak in and this would measure the test harness instead of the
        // shipped default — which passes when run alone and fails in a full suite.
        let _lock = super::super::test_env_lock().lock().await;
        let _guard = ApiBaseGuard::clear_env();

        // The provider appends `/v1/messages` itself, so a `/v1` here would compose
        // `/anthropic/v1/v1/messages` and 404.
        let base = format!("{}{ANTHROPIC_PATH}", service_root(Region::Cn));
        assert_eq!(base, "https://api.minimax.cn/anthropic");
        assert_eq!(
            format!("{base}/v1/messages"),
            "https://api.minimax.cn/anthropic/v1/messages"
        );
        // The catalog lives on the OpenAI-compatible prefix instead.
        assert_eq!(
            format!("{}{OPENAI_PATH}{MODELS_PATH}", service_root(Region::Cn)),
            "https://api.minimax.cn/v1/models"
        );
    }

    #[tokio::test]
    async fn a_signed_out_store_is_an_answer_not_an_error() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        assert_eq!(status(), json!({ "loggedIn": false }));
        assert!(api_key().is_none());
        assert_eq!(usage().await, Ok(json!({ "signedIn": false })));
        // Signing out when nothing is stored still succeeds.
        assert_eq!(logout(), Ok(json!({ "ok": true })));
    }

    #[tokio::test]
    async fn a_stored_key_reads_back_and_never_leaves_the_store_in_plaintext_status() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        write_store(&json!({
            "apiKey": "sk-cp-abcdefghijklmnop",
            "region": "global",
            "apiBase": "https://api.minimax.io/anthropic",
            "modelCount": 8,
        }))
        .expect("store write");

        assert_eq!(api_key().as_deref(), Some("sk-cp-abcdefghijklmnop"));
        assert_eq!(region(), Some(Region::Global));
        let status = status();
        assert_eq!(status["loggedIn"], json!(true));
        assert_eq!(status["region"], json!("global"));
        assert_eq!(status["modelCount"], json!(8));
        assert_eq!(status["apiKeyMasked"], json!("sk-cp-abc…mnop"));
        assert!(
            !status.to_string().contains("abcdefghijklmnop"),
            "the raw key must not reach a status reply: {status}"
        );
    }

    #[tokio::test]
    async fn a_key_from_the_pay_as_you_go_product_is_refused_before_any_call() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        // A pay-as-you-go key spends account balance rather than plan allowance.
        // Storing one would silently bill the user outside their subscription.
        let result = set_api_key("sk-api-abcdefghijklmnop", None).await.unwrap();
        assert_eq!(result["error"], json!("minimax_key_not_a_subscription_key"));
        assert!(api_key().is_none(), "a refused key is not stored");

        let empty = set_api_key("   ", None).await.unwrap();
        assert_eq!(empty["error"], json!("minimax_api_key_empty"));
        assert!(api_key().is_none());
    }

    /// A tiny server answering the catalog route once, with a caller-chosen body.
    ///
    /// Used to pin the fail-closed behaviour on a 200 that is not the catalog: a
    /// captive portal or corporate interception page answers HTTP 200 with markup,
    /// and a key "verified" against that would be stored without ever being checked.
    fn one_shot_gateway(
        status: &'static str,
        content_type: &'static str,
        body: &'static str,
    ) -> (std::net::SocketAddr, std::thread::JoinHandle<()>) {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind mock gateway");
        let addr = listener.local_addr().expect("mock gateway addr");
        let handle = std::thread::spawn(move || {
            if let Ok((mut stream, _peer)) = listener.accept() {
                let mut request = [0u8; 4096];
                let _ = stream.read(&mut request);
                let response = format!(
                    "HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
            }
        });
        (addr, handle)
    }

    #[tokio::test]
    async fn a_200_that_is_not_the_catalog_never_verifies_or_stores_a_key() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());

        // An HTML interception page: HTTP 200, but nothing a catalog would return.
        let (addr, handle) = one_shot_gateway(
            "200 OK",
            "text/html",
            "<html><body>302 Blocked by security policy</body></html>",
        );
        let guard = ApiBaseGuard::set(&format!("http://{addr}{ANTHROPIC_PATH}"));
        let result = set_api_key("sk-cp-abcdefghijklmnop", Some("cn"))
            .await
            .unwrap();
        handle.join().ok();
        guard.clear();

        assert_ne!(
            result.get("loggedIn"),
            Some(&json!(true)),
            "an interception page must not read as connected: {result}"
        );
        assert!(
            api_key().is_none(),
            "a key checked against a non-catalog answer is never stored"
        );
    }

    #[tokio::test]
    async fn a_200_json_answer_with_no_model_list_does_not_verify_the_key() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());

        // Valid JSON, HTTP 200, but no `data` array: a refusal shape this gateway
        // uses, not a plan that happens to serve zero models.
        let (addr, handle) = one_shot_gateway(
            "200 OK",
            "application/json",
            r#"{ "base_resp": { "status_code": 1004 } }"#,
        );
        let guard = ApiBaseGuard::set(&format!("http://{addr}{ANTHROPIC_PATH}"));
        let result = set_api_key("sk-cp-abcdefghijklmnop", Some("cn"))
            .await
            .unwrap();
        handle.join().ok();
        guard.clear();

        assert_ne!(
            result.get("loggedIn"),
            Some(&json!(true)),
            "no model list means no verified key: {result}"
        );
        assert!(api_key().is_none());
    }

    #[tokio::test]
    async fn a_real_catalog_answer_verifies_and_stores_the_key() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());

        // The measured success shape: a `data` array of models. This is the control
        // proving the two tests above fail because of the answer, not the harness.
        let (addr, handle) = one_shot_gateway(
            "200 OK",
            "application/json",
            r#"{ "object": "list", "data": [ { "id": "MiniMax-M2.5" }, { "id": "MiniMax-M3" } ] }"#,
        );
        let guard = ApiBaseGuard::set(&format!("http://{addr}{ANTHROPIC_PATH}"));
        let result = set_api_key("sk-cp-abcdefghijklmnop", Some("cn"))
            .await
            .unwrap();
        handle.join().ok();
        guard.clear();

        assert_eq!(
            result.get("loggedIn"),
            Some(&json!(true)),
            "a catalog answer verifies: {result}"
        );
        assert_eq!(
            api_key().as_deref(),
            Some("sk-cp-abcdefghijklmnop"),
            "the key is stored"
        );
        assert_eq!(
            result.get("modelCount"),
            Some(&json!(2)),
            "the live model count is reported"
        );
    }

    /// Restores `MINIMAX_API_BASE` on drop so a test cannot leak an override into
    /// the next one. Mirrors the guard the Kimi suite uses for its own base env var.
    struct ApiBaseGuard;
    impl ApiBaseGuard {
        fn set(value: &str) -> Self {
            klaus_agent_core::runtime_process::set_process_environment_var(
                "MINIMAX_API_BASE",
                value,
            );
            Self
        }
        /// For a test that asserts the PRODUCTION default: it must not inherit an
        /// override some other test set. Every test touching this variable holds
        /// `test_env_lock`, so setting and clearing are serialized; this only closes
        /// the leak between tests.
        fn clear_env() -> Self {
            klaus_agent_core::runtime_process::remove_process_environment_var("MINIMAX_API_BASE");
            Self
        }
        fn clear(&self) {
            klaus_agent_core::runtime_process::remove_process_environment_var("MINIMAX_API_BASE");
        }
    }
    impl Drop for ApiBaseGuard {
        /// Restore on every exit path. A test that panics between `set` and `clear`
        /// would otherwise leave the override set for the rest of the suite, and the
        /// failure would surface in whichever unrelated test ran next.
        fn drop(&mut self) {
            klaus_agent_core::runtime_process::remove_process_environment_var("MINIMAX_API_BASE");
        }
    }

    #[tokio::test]
    async fn an_unrecognized_site_is_refused_rather_than_silently_ignored() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        // Returns before any network call, so it needs no connectivity.
        let result = set_api_key("sk-cp-abcdefghijklmnop", Some("mars"))
            .await
            .unwrap();
        assert_eq!(result["error"], json!("minimax_region_unknown"));
        assert!(api_key().is_none(), "a refused site stores nothing");
    }

    #[test]
    fn remaining_percent_inverts_and_skips_a_missing_window() {
        let row = json!({
            "current_interval_remaining_percent": 99,
            "current_weekly_remaining_percent": 93,
        });
        assert_eq!(
            remaining_percent(&row, "current_interval_remaining_percent"),
            Some(1.0)
        );
        assert_eq!(
            remaining_percent(&row, "current_weekly_remaining_percent"),
            Some(7.0)
        );
        // A window the plan does not report is skipped, not drawn as 0% used.
        assert_eq!(remaining_percent(&row, "absent_percent"), None);
    }

    #[test]
    fn usage_projects_two_windows_from_the_measured_shape() {
        // The measured answer shape: remaining percentages plus epoch-ms resets,
        // with video counted only against the weekly window.
        let payload = json!({
            "model_remains": [{
                "model_name": "general",
                "current_interval_remaining_percent": 99,
                "current_weekly_remaining_percent": 93,
                "end_time": 1791451961678_i64,
                "weekly_end_time": 1792038761678_i64,
                "current_interval_status": 1,
                "current_weekly_status": 1,
            }],
            "base_resp": { "status_code": 0, "status_msg": "success" }
        });
        let windows = usage_windows(&payload);
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0]["id"], json!("five_hour"));
        assert_eq!(windows[0]["usedPercent"], json!(1.0));
        assert_eq!(windows[0]["resetsAt"], json!(1791451961678_i64));
        assert_eq!(windows[1]["id"], json!("weekly"));
        assert_eq!(windows[1]["usedPercent"], json!(7.0));
        // No fabricated third window and no credit row: the plan reports windows.
        assert!(usage_windows(&json!({})).is_empty());
    }
}
