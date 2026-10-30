//! Bailian Token Plan credentials and connection status.
//!
//! Token Plan authenticates with one thing only: a plan API key (`sk-sp-…`)
//! minted in the Bailian console under Token Plan → My Subscriptions. There is no
//! OAuth, no device flow and no local client store to import, so this module is a
//! key holder rather than a login flow — which is why there are no `start`/`poll`
//! methods here.
//!
//! The key is verified before it is stored. Storing an unverified key would leave
//! a mistyped one looking connected on the settings card and failing inside a
//! conversation with the service's own prose; the verification costs one catalog
//! read and no inference tokens.
//!
//! ## No quota endpoint
//!
//! Token Plan publishes usage only through OpenAPI routes that want an Aliyun
//! AccessKey pair signed with ACS3-HMAC-SHA256 — explicitly not the plan key —
//! and their shapes are seat-based, which is a team-edition concept. Every
//! candidate path under the plan gateway itself answers 404 (measured), so
//! [`usage`] reports the connection and nothing more. The settings pane sends the
//! user to the console rather than drawing a bar out of a number it cannot get.
//!
//! ## One honest User-Agent
//!
//! Bailian's terms confine this plan to interactive use inside AI coding tools and
//! agent tools, and name automation platforms, API testers and custom application
//! backends as out of scope. Nothing here pretends to be another client: every
//! request carries Klaus's own `User-Agent`, the key stays on this machine, and
//! calls go straight from here to the gateway with no server in between.

use serde_json::{json, Map, Value};
use std::path::PathBuf;

/// Token Plan serves exactly one region per site, and a key is bound to the
/// region that issued it.
const CN_HOST: &str = "token-plan.cn-beijing.maas.aliyuncs.com";
const INTL_HOST: &str = "token-plan.ap-southeast-1.maas.aliyuncs.com";

/// The Anthropic-compatible root. Deliberately carries no `/v1` suffix: the
/// provider appends `/v1/messages` itself, and Bailian's own FAQ warns that a
/// `/v1/` suffix makes model discovery compose `/v1/v1/models` and 404.
const ANTHROPIC_PATH: &str = "/apps/anthropic";
/// The OpenAI-compatible root, which is where the catalog lives: the Anthropic
/// endpoint serves no `/v1/models` at all (measured 404).
const OPENAI_PATH: &str = "/compatible-mode/v1";
const MODELS_PATH: &str = "/models";

const HTTP_TIMEOUT_MS: u64 = 20_000;

/// How long one eligibility probe's answer stays authoritative. A negative
/// state (expired, invalid key) does not change back on its own, and probing
/// on every popover refresh would turn a status read into a request storm.
const SUBSCRIPTION_STATE_TTL_MS: i64 = 6 * 60 * 60 * 1000;
/// The probe must classify, not converse: a refusal arrives in well under a
/// second, and a plan that answers 200 has windows to show anyway, so a slow
/// success may time out and simply go unclassified.
const PROBE_TIMEOUT_MS: u64 = 5_000;
/// Store keys for the cached eligibility state, beside the plan key so a
/// logout drops the cache with the credential it describes.
const KEY_SUB_STATE: &str = "subscriptionState";
const KEY_SUB_STATE_AT: &str = "subscriptionStateAt";

/// The client identity sent on every request.
///
/// Exposed to the catalog module so both modules send one agent string: Kimi's
/// terms-style restriction applies to Bailian too, and a request whose client
/// identity varies by call site is a request that cannot be reasoned about when
/// the service starts refusing it.
pub(crate) fn user_agent() -> String {
    std::env::var("BAILIAN_USER_AGENT")
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
    Intl,
}

impl Region {
    fn as_str(self) -> &'static str {
        match self {
            Self::Cn => "cn",
            Self::Intl => "intl",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "cn" | "china" | "beijing" => Some(Self::Cn),
            "intl" | "international" | "global" | "singapore" => Some(Self::Intl),
            _ => None,
        }
    }

    fn host(self) -> &'static str {
        match self {
            Self::Cn => CN_HOST,
            Self::Intl => INTL_HOST,
        }
    }
}

fn store_path() -> PathBuf {
    super::config_root().join("bailian-tokenplan.json")
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn read_store() -> Option<Value> {
    let raw = std::fs::read_to_string(store_path()).ok()?;
    serde_json::from_str(&raw).ok()
}

/// The whole store, for the console module which keeps its session credential
/// beside the plan key in this one file.
pub(crate) fn read_store_public() -> Option<serde_json::Map<String, Value>> {
    read_store().and_then(|store| store.as_object().cloned())
}

/// Merge a patch into the store, with `null` removing a key. The console
/// module uses it to store and clear its session token without rewriting the
/// plan key.
pub(crate) fn patch_store_public(patch: serde_json::Map<String, Value>) -> Result<(), String> {
    let mut store = read_store().unwrap_or(Value::Object(Map::new()));
    let object = store
        .as_object_mut()
        .ok_or_else(|| "corrupt store".to_string())?;
    for (key, value) in patch {
        if value.is_null() {
            object.remove(&key);
        } else {
            object.insert(key, value);
        }
    }
    write_store(&store)
}

pub(crate) fn write_store(store: &Value) -> Result<(), String> {
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

/// The stored plan key, if there is one.
pub fn api_key() -> Option<String> {
    read_store().and_then(|store| string_field(&store, "apiKey"))
}

/// The region the stored key belongs to.
pub fn region() -> Option<Region> {
    read_store()
        .and_then(|store| string_field(&store, "region").and_then(|value| Region::parse(&value)))
}

/// The Anthropic-compatible root a session is pointed at.
pub fn api_base() -> String {
    if let Ok(value) = std::env::var("BAILIAN_API_BASE") {
        let trimmed = value.trim().trim_end_matches('/').to_string();
        if !trimmed.is_empty() {
            return trimmed;
        }
    }
    let host = region().map(Region::host).unwrap_or(CN_HOST);
    format!("https://{host}{ANTHROPIC_PATH}")
}

/// The OpenAI-compatible root the catalog is read from.
///
/// Same plan key, different path prefix: Token Plan serves both protocols off one
/// host, and the model list only exists on this one.
pub(crate) fn catalog_base() -> String {
    if let Ok(value) = std::env::var("BAILIAN_CATALOG_BASE") {
        let trimmed = value.trim().trim_end_matches('/').to_string();
        if !trimmed.is_empty() {
            return trimmed;
        }
    }
    let host = region().map(Region::host).unwrap_or(CN_HOST);
    format!("https://{host}{OPENAI_PATH}")
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

/// Verify a key by reading the catalog, then store it.
///
/// The catalog read is the cheapest honest check: it authenticates the key, costs
/// no inference tokens, and its answer doubles as the model list the settings pane
/// wants. A key the service refuses is reported as a stable code rather than as
/// the gateway's prose, because that prose is written for Bailian's own clients.
pub async fn set_api_key(key: &str, region: Option<&str>) -> Result<Value, Value> {
    let key = key.trim();
    if key.is_empty() {
        return Ok(json!({ "ok": false, "error": "bailian_api_key_empty" }));
    }
    // An explicit site must name one of the two Token Plan serves. Silently
    // swallowing an unrecognized value would leave the user believing they picked a
    // site while the key was checked against a different one — and since the sites
    // do not share keys, that reads as "the key is wrong" rather than as a typo.
    let region = match region {
        Some(value) if !value.trim().is_empty() => match Region::parse(value) {
            Some(parsed) => parsed,
            None => {
                return Ok(json!({
                    "ok": false,
                    "error": "bailian_region_unknown",
                }))
            }
        },
        // No site named: keep whichever one the stored key already belongs to,
        // rather than moving it and failing it.
        _ => self::region().unwrap_or(Region::Cn),
    };
    // Refuse the neighbouring product's key shape rather than storing one that can
    // only fail: Token Plan and the pay-as-you-go API are separate systems and
    // their keys are not interchangeable.
    if !key.starts_with("sk-sp-") {
        return Ok(json!({
            "ok": false,
            "error": "bailian_key_not_a_plan_key",
        }));
    }

    let http = client().map_err(|error| json!({ "ok": false, "error": error }))?;
    let base = format!("https://{}{OPENAI_PATH}", region.host());
    match verify_key(&http, &base, key).await {
        Ok(models) => {
            let mut store = Map::new();
            store.insert("apiKey".into(), json!(key));
            store.insert("region".into(), json!(region.as_str()));
            store.insert(
                "apiBase".into(),
                json!(format!("https://{}{ANTHROPIC_PATH}", region.host())),
            );
            store.insert("modelCount".into(), json!(models));
            store.insert("updatedAt".into(), json!(now_ms()));
            super::catalog::forget_cache();
            match write_store(&Value::Object(store)) {
                Ok(()) => Ok(status()),
                Err(error) => Ok(json!({ "ok": false, "error": error })),
            }
        }
        Err(error) => Ok(json!({ "ok": false, "error": error })),
    }
}

/// Authenticate a key against the catalog route and report how many models the
/// plan serves.
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
        // Both a wrong key and a right key on the wrong site answer with a 4xx.
        // One stable code covers them because the fix is the same either way —
        // re-check the key and the region it was minted for — and the gateway's
        // own prose is written for Bailian's clients, not for this card.
        401 | 403 => return Err("bailian_key_rejected".to_string()),
        _ => {
            let detail: String = body.chars().take(200).collect();
            return Err(format!("bailian_key_check_failed:{status_code}:{detail}"));
        }
    }
    let parsed: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    let models = parsed
        .get("data")
        .and_then(Value::as_array)
        .map(|rows| rows.len())
        .unwrap_or(0);
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
    // Report a measured subscription state when one is still fresh. This is a
    // store read, not a probe: the settings card mounts often, and the probe
    // belongs to the usage read that has a reason to pay for it.
    if let Some((state, at)) = cached_subscription_state() {
        if subscription_state_is_fresh(at) {
            if let Some(known) = known_subscription_state(&state) {
                out.insert("subscriptionState".into(), json!(known));
            }
        }
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
    // The store file holds the console session token too; removing the file
    // drops both, which is what signing out of the plan must mean.
    match std::fs::remove_file(store_path()) {
        Ok(()) => Ok(json!({ "ok": true })),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(json!({ "ok": true })),
        Err(error) => Ok(json!({ "ok": false, "error": error.to_string() })),
    }
}

/// The allowance, when a console session can read it.
///
/// The plan key alone reaches no quota route, so without a console login this
/// answers "signed in, no allowance data" and the host renders the console
/// link. With a stored console session token the allowance comes from the
/// console's own gateway as rate windows — the same shape the Codex and Kimi
/// gauges render — and a lapsed session clears itself back to the no-data
/// answer rather than surfacing as a fetch failure.
pub async fn usage() -> Result<Value, String> {
    let Some(_) = api_key() else {
        return Ok(json!({ "signedIn": false }));
    };
    if super::console::console_token().is_none() {
        // No session token: the honest answer is a connection and no allowance
        // data — plus, when a console was ever connected on this machine, a
        // renewal URL the host can open. The authorize page posts a fresh token
        // back on its own while the browser still holds the Aliyun session, so
        // this is what keeps the allowance bars alive across token lapses
        // instead of waiting for the next manual "connect console" click.
        // A machine that never connected a console opens nothing.
        let mut answer = json!({
            "signedIn": true,
            // No `windows` and no `credits`: the gateway exposes neither to this
            // key, and the console is where the plan's Credits live.
            "quotaAvailable": false,
        });
        if let Some(state) = subscription_state().await {
            answer
                .as_object_mut()
                .expect("object literal")
                .insert("subscriptionState".into(), json!(state));
        }
        if let Some(url) = super::console::auto_renewal_url() {
            answer
                .as_object_mut()
                .expect("object literal")
                .insert("renewalUrl".into(), json!(url));
        }
        return Ok(answer);
    }
    match super::console::quota_windows().await {
        Ok(windows) if !windows.is_empty() => {
            // Allowance rows are the plan's own receipt that it is active; record
            // it so a previously measured expiry does not linger in the cache.
            write_subscription_state("active");
            Ok(json!({
                "signedIn": true,
                "consoleLoggedIn": true,
                "windows": windows,
            }))
        }
        Ok(_) => Ok(console_live_without_allowance(subscription_state().await)),
        Err(super::console::GatewayError::NotLogined) => {
            // The browser's Aliyun session usually outlives the console token,
            // so a lapsed token can renew with no user click: start the loopback
            // flow and hand the host the authorize URL to open. Throttled inside
            // the engine, and only for machines that connected a console before.
            let renewal = super::console::auto_renewal_url();
            let mut answer = json!({
                "signedIn": true,
                "consoleLoggedIn": false,
                "quotaAvailable": false,
            });
            if let Some(url) = renewal {
                answer
                    .as_object_mut()
                    .expect("object literal")
                    .insert("renewalUrl".into(), json!(url));
            }
            Ok(answer)
        }
        Err(error) => Err(match error {
            super::console::GatewayError::NotLogined => unreachable!(),
            super::console::GatewayError::Transport(detail) => {
                format!("bailian_console_gateway_transport:{detail}")
            }
            super::console::GatewayError::Business(detail) => {
                format!("bailian_console_gateway_business:{detail}")
            }
        }),
    }
}

/// A console session that reads no allowance row is the one state where "the
/// plan reports nothing to these routes" and "the plan is gone" are
/// indistinguishable from the outside. The inference gateway answers
/// eligibility to the plan key itself, so the state it reports is stated here
/// instead of the generic console hint.
fn console_live_without_allowance(state: Option<String>) -> Value {
    let mut answer = json!({
        "signedIn": true,
        "consoleLoggedIn": true,
        // A session that reads no allowance row must not look like a
        // failed fetch: say the console login is live but the plan
        // reports no pool, and let the host draw the console link.
        "quotaAvailable": false,
    });
    if let Some(state) = state {
        answer
            .as_object_mut()
            .expect("object literal")
            .insert("subscriptionState".into(), json!(state));
    }
    answer
}

/// The cached eligibility state and when it was measured, if a probe ever
/// answered one. Exposed for `console_status`, whose settings-card reader wants
/// the same state the usage read would report.
pub(crate) fn cached_subscription_state() -> Option<(String, i64)> {
    let store = read_store()?;
    let state = string_field(&store, KEY_SUB_STATE)?;
    let at = store
        .get(KEY_SUB_STATE_AT)
        .and_then(Value::as_i64)
        .unwrap_or(0);
    Some((state, at))
}

fn write_subscription_state(state: &str) {
    let mut patch = Map::new();
    patch.insert(KEY_SUB_STATE.to_string(), json!(state));
    patch.insert(KEY_SUB_STATE_AT.to_string(), json!(now_ms()));
    let _ = patch_store_public(patch);
}

fn known_subscription_state(state: &str) -> Option<String> {
    match state {
        "active" | "expired" | "invalid_key" => Some(state.to_string()),
        _ => None,
    }
}

fn subscription_state_is_fresh(at: i64) -> bool {
    now_ms() - at < SUBSCRIPTION_STATE_TTL_MS
}

/// The plan's eligibility behind the stored key: `expired` when the account no
/// longer holds Token Plan, `invalid_key` when the key was revoked, `active`
/// when the gateway still serves it. Fresh cache first; a cache miss probes
/// once and caches whatever the probe could classify.
async fn subscription_state() -> Option<String> {
    if let Some((state, at)) = cached_subscription_state() {
        if subscription_state_is_fresh(at) {
            return known_subscription_state(&state);
        }
    }
    let state = probe_subscription_state().await?;
    write_subscription_state(state);
    Some(state.to_string())
}

/// The probe endpoint. Overridable so a future fixture can point the probe at
/// a stub without also moving the inference base.
fn probe_url() -> String {
    if let Ok(value) = std::env::var("BAILIAN_PROBE_URL") {
        let trimmed = value.trim().to_string();
        if !trimmed.is_empty() {
            return trimmed;
        }
    }
    format!("{}{}", api_base(), "/v1/messages")
}

/// Map one probe reply to a state name, or `None` when it decides nothing.
fn classify_subscription_reply(status: u16, body: &str) -> Option<&'static str> {
    if (200..=299).contains(&status) {
        return Some("active");
    }
    let code = serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|parsed| {
            parsed
                .get("code")
                .and_then(Value::as_str)
                .map(str::to_string)
        })?;
    match (status, code.as_str()) {
        (401, "InvalidApiKey") => Some("invalid_key"),
        (403, "AccessDenied.Unpurchased") => Some("expired"),
        _ => None,
    }
}

/// Ask the inference gateway whether the key's plan still spends.
///
/// The check rides the ordinary inference route with a one-token request:
/// eligibility is refused BEFORE the body is validated (measured: a
/// well-formed body on an ineligible key answers 403 `AccessDenied.Unpurchased`,
/// a malformed body on an eligible key answers 400 `InvalidParameter`), so the
/// reply classifies the subscription without spending anything.
async fn probe_subscription_state() -> Option<&'static str> {
    let key = api_key()?;
    let http = client().ok()?;
    let request = json!({
        // Any model the catalog answers works: eligibility is refused before
        // the body is validated, so the probe never spends a token.
        "model": super::catalog::first_model_key().unwrap_or_else(|| super::DEFAULT_SELECTION.to_string()),
        "max_tokens": 1,
        "messages": [{ "role": "user", "content": "ping" }],
    });
    let response = tokio::time::timeout(
        std::time::Duration::from_millis(PROBE_TIMEOUT_MS),
        http.post(probe_url())
            .header("x-api-key", &key)
            .header("anthropic-version", "2023-06-01")
            .header("Content-Type", "application/json")
            .body(request.to_string())
            .send(),
    )
    .await
    .ok()
    .and_then(|sent| sent.ok())?;
    let status = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    classify_subscription_reply(status, &body)
}

/// The stored plan credential plus the plan gateway root its media routes live on.
///
/// The plan's speech route hangs off the SAME gateway host that serves the coding
/// wire (`/api/v1/services/audio/tts/SpeechSynthesizer`, measured 200 with the plan
/// key), so the host runs that protocol itself and needs the key to do it — on the
/// same precedent as `minimax/mediaCredential` and `model/credential/read`.
///
/// The base URL is the plan root rather than either protocol prefix, because the
/// speech path hangs off the root directly instead of under `/apps/anthropic` or
/// `/compatible-mode/v1`.
///
/// Signed out answers `{ "ok": false }` rather than an error: a conversation on this
/// source with no key stored is "not configured", not a failed call.
pub fn media_credential() -> Value {
    let Some(key) = api_key() else {
        return json!({ "ok": false });
    };
    let region = region().unwrap_or(Region::Cn);
    json!({
        "ok": true,
        "apiKey": key,
        "baseUrl": format!("https://{}", region.host()),
        "region": region.as_str(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masking_never_reveals_a_spendable_key() {
        assert_eq!(mask_key("sk-sp-abcdefghijklmnop"), "sk-sp-abc…mnop");
        assert_eq!(mask_key("short"), "••••••••");
    }

    #[test]
    fn region_parsing_accepts_the_names_a_user_would_type() {
        assert_eq!(Region::parse("cn"), Some(Region::Cn));
        assert_eq!(Region::parse("CN"), Some(Region::Cn));
        assert_eq!(Region::parse("intl"), Some(Region::Intl));
        assert_eq!(Region::parse("singapore"), Some(Region::Intl));
        assert_eq!(Region::parse("mars"), None);
        assert_eq!(Region::Cn.host(), CN_HOST);
        assert_eq!(Region::Intl.host(), INTL_HOST);
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
            "apiKey": "sk-sp-abcdefghijklmnop",
            "region": "intl",
            "apiBase": "https://token-plan.ap-southeast-1.maas.aliyuncs.com/apps/anthropic",
            "modelCount": 16,
        }))
        .expect("store write");

        assert_eq!(api_key().as_deref(), Some("sk-sp-abcdefghijklmnop"));
        assert_eq!(region(), Some(Region::Intl));
        let status = status();
        assert_eq!(status["loggedIn"], json!(true));
        assert_eq!(status["region"], json!("intl"));
        assert_eq!(status["modelCount"], json!(16));
        assert_eq!(status["apiKeyMasked"], json!("sk-sp-abc…mnop"));
        assert!(
            !status.to_string().contains("abcdefghijklmnop"),
            "the raw key must not reach a status reply: {status}"
        );
    }

    #[tokio::test]
    async fn usage_reports_a_connection_and_no_allowance() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        write_store(&json!({ "apiKey": "sk-sp-abcdefghijklmnop", "region": "cn" }))
            .expect("store write");
        let usage = usage().await.expect("usage answers");
        assert_eq!(usage["signedIn"], json!(true));
        assert_eq!(usage["quotaAvailable"], json!(false));
        // No fabricated window or credit row: the host draws the console link.
        assert!(usage.get("windows").is_none());
        assert!(usage.get("credits").is_none());
    }

    #[tokio::test]
    async fn logout_clears_the_key() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        write_store(&json!({ "apiKey": "sk-sp-x", "region": "cn" })).expect("store write");
        assert_eq!(status()["loggedIn"], json!(true));
        assert_eq!(logout(), Ok(json!({ "ok": true })));
        assert_eq!(status(), json!({ "loggedIn": false }));
        assert!(api_key().is_none());
    }

    #[tokio::test]
    async fn a_key_from_the_neighbouring_product_is_refused_before_any_call() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        // A pay-as-you-go key has the `sk-` prefix but not `sk-sp-`. Accepting it
        // would store a credential that can only fail at inference time.
        let result = set_api_key("sk-payg-abcdefghijklmnop", None).await.unwrap();
        assert_eq!(result["error"], json!("bailian_key_not_a_plan_key"));
        assert!(api_key().is_none(), "a refused key is not stored");

        let empty = set_api_key("   ", None).await.unwrap();
        assert_eq!(empty["error"], json!("bailian_api_key_empty"));
        assert!(api_key().is_none());
    }

    #[tokio::test]
    async fn an_unrecognized_site_is_refused_rather_than_silently_ignored() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        // The key is well-formed, so this fails purely on the site name. A typo'd
        // region must be refused: swallowing it would check the key against the
        // WRONG site, and since the sites do not share keys that surfaces later as
        // "the key is invalid" with no hint the real mistake was here. This returns
        // before any network call, so it needs no connectivity.
        let result = set_api_key("sk-sp-abcdefghijklmnop", Some("mars"))
            .await
            .unwrap();
        assert_eq!(result["error"], json!("bailian_region_unknown"));
        assert!(api_key().is_none(), "a refused site stores nothing");
    }

    #[tokio::test]
    async fn both_site_names_parse_and_name_their_host() {
        // The two sites Token Plan actually serves, in the spellings the picker
        // sends. Anything else is a caller error, not a site.
        assert_eq!(Region::parse("cn"), Some(Region::Cn));
        assert_eq!(Region::parse("intl"), Some(Region::Intl));
        assert_eq!(Region::Cn.host(), "token-plan.cn-beijing.maas.aliyuncs.com");
        assert_eq!(
            Region::Intl.host(),
            "token-plan.ap-southeast-1.maas.aliyuncs.com"
        );
    }

    /// The probe classifier is a pure mapping from measured gateway replies to
    /// state names, so the mapping itself is pinned here without any network.
    #[test]
    fn probe_replies_classify_into_exact_state_names() {
        assert_eq!(classify_subscription_reply(200, "{}"), Some("active"));
        assert_eq!(
            classify_subscription_reply(
                403,
                r#"{"code":"AccessDenied.Unpurchased","message":"Access to model denied."}"#
            ),
            Some("expired")
        );
        assert_eq!(
            classify_subscription_reply(
                401,
                r#"{"code":"InvalidApiKey","message":"Invalid API-key provided."}"#
            ),
            Some("invalid_key")
        );
        // Errors the probe cannot attribute say nothing: a transport hiccup or
        // an unmapped refusal must not be reported as a subscription verdict.
        assert_eq!(classify_subscription_reply(500, "oops"), None);
        assert_eq!(
            classify_subscription_reply(400, r#"{"code":"InvalidParameter"}"#),
            None
        );
        assert_eq!(
            classify_subscription_reply(403, r#"{"code":"Throttling"}"#),
            None
        );
        assert_eq!(classify_subscription_reply(403, "not json"), None);
    }

    #[tokio::test]
    async fn a_measured_state_is_cached_and_reported_until_it_goes_stale() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        write_store(&json!({
            "apiKey": "sk-sp-abcdefghijklmnop",
            "region": "cn",
            "subscriptionState": "expired",
            "subscriptionStateAt": now_ms(),
        }))
        .expect("store write");

        // A fresh measurement is read back by the status route without probing:
        // the card mounts far more often than the state can change.
        let status_now = status();
        assert_eq!(status_now["subscriptionState"], json!("expired"));

        // Allowance rows are the plan's own receipt that it is active again, so
        // a successful usage read clears a stale negative state on the spot.
        write_subscription_state("active");
        assert_eq!(
            cached_subscription_state().map(|(state, _)| state),
            Some("active".to_string())
        );

        // An unknown state name is dropped rather than parroted to the host:
        // only the three names the classifier can produce are a contract.
        write_store(&json!({
            "apiKey": "sk-sp-abcdefghijklmnop",
            "region": "cn",
            "subscriptionState": "something-else",
            "subscriptionStateAt": now_ms(),
        }))
        .expect("store write");
        assert_eq!(status().get("subscriptionState"), None);
    }
    #[tokio::test]
    async fn a_lapsed_console_session_still_answers_a_renewal_url() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        // A machine whose console session lapsed (or was never stored this run)
        // still had one once: the ever-connected flag is what makes the engine
        // renew quietly instead of waiting for the next manual click.
        write_store(&json!({
            "apiKey": "sk-sp-abcdefghijklmnop",
            "region": "cn",
            "consoleEverConnected": true,
            "consoleToken": Value::Null,
        }))
        .expect("store write");
        // No token: the read answers signed-in with no allowance data, and a
        // fresh renewal attempt starts because ever-connected is set. The URL
        // itself opens a real loopback listener, which this test does not want,
        // so the probe override keeps the eligibility part offline-free: with
        // no reachable probe the state is simply unclassified and absent.
        std::env::set_var("BAILIAN_PROBE_URL", "http://127.0.0.1:9/v1/messages");
        let answer = usage().await.expect("usage answers");
        std::env::remove_var("BAILIAN_PROBE_URL");
        assert_eq!(answer["signedIn"], json!(true));
        assert_eq!(answer["quotaAvailable"], json!(false));
        // The engine's renewal throttle (AUTO_RENEW_GAP) allows the attempt, so
        // the answer carries a URL for the host to open.
        assert!(
            answer.get("renewalUrl").and_then(Value::as_str).is_some(),
            "an ever-connected machine gets a renewal URL: {answer}"
        );
    }
}
