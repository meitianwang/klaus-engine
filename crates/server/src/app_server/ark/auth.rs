//! Volcengine Ark Coding Plan credentials and connection status.
//!
//! Coding Plan authenticates with one thing only: a plan API key minted on the plan
//! page. There is no OAuth, no device flow and no local client store to import, so
//! this module is a key holder rather than a login flow — which is why there are no
//! `start`/`poll` methods here.
//!
//! The key is verified before it is stored. Storing an unverified key would leave a
//! mistyped one looking connected on the settings card and failing inside a
//! conversation with the service's own prose; the verification costs one catalog
//! read and no inference tokens.
//!
//! ## Bearer, not x-api-key
//!
//! The plan's own integration guide configures clients with an auth TOKEN rather
//! than an API-key variable, and the gateway answers an unauthenticated request
//! with a real `401 AuthenticationError` (measured). Every request this module
//! sends therefore carries `Authorization: Bearer <key>`, and the session resolves
//! this source to a `static_bearer` auth value — the same shape the other
//! Bearer-token subscriptions use — rather than to an `api_key` value the Anthropic
//! provider would send as `x-api-key`.
//!
//! ## No shape check on the key
//!
//! The plan publishes no key grammar a client could validate against, so the only
//! honest test is the one the service performs. A regex here would reject a key
//! that works.
//!
//! ## One region, by official publication
//!
//! Coding Plan is published at one region, `cn-beijing`: the plan page, the key page
//! and every documented base URL point there. There is therefore no region picker —
//! unlike a plan sold by two platforms with separate key spaces, this one gives a
//! caller nothing to choose. An environment override exists for tests and for the
//! day the service publishes a second region, and it is honoured everywhere the
//! base is read.
//!
//! ## The allowance is not readable with the plan key
//!
//! Measured with a live plan key on 2026-10-09: every candidate allowance route on
//! the coding gateway answers `404` (`/api/coding/v3/usage`,
//! `/api/coding/v3/token_plan/remains`, `/api/coding/v3/plan/usage`,
//! `/api/coding/v3/subscription/usage`, `/api/coding/v3/quota`,
//! `/api/coding/v3/coding_plan/usage`). The service's published usage APIs are
//! control-plane actions that want an account signature or a console session, not a
//! plan key, and the plan's own guide points at the console page. So [`usage`]
//! answers "connected, no allowance data" and the host renders the console link —
//! the same honest shape the other key-only plans use.
//!
//! The read is still a loop over a candidate route list rather than a hardcoded
//! no: the moment a route is measured answering a plan key, the projection lights
//! up without restructuring anything around it, and a route that refuses or 404s
//! simply yields the no-data shape.
//!
//! ## One honest User-Agent
//!
//! The plan's terms confine it to interactive use inside coding and agent tools.
//! Nothing here pretends to be another client: every request carries Klaus's own
//! `User-Agent`, the key stays on this machine, and calls go straight from here to
//! the gateway with no server in between.

use serde_json::{json, Map, Value};
use std::path::PathBuf;

/// The region the plan is published at. Every documented base URL, the key page and
/// the plan page all point here.
const REGION: &str = "cn-beijing";
const HOST: &str = "ark.cn-beijing.volces.com";

/// The Anthropic-compatible root. Deliberately carries no `/v1` suffix: the
/// provider appends `/v1/messages` itself, and the catalog read appends
/// `/v1/models` to the same root (measured live). The catalog module owns that
/// path; this module reads the catalog through it rather than building URLs.
const ANTHROPIC_PATH: &str = "/api/coding";

/// Candidate allowance routes, tried in order by [`usage`].
///
/// None of these answers a plan key today (measured on 2026-10-09: every route on
/// the coding gateway answers 404, including the `/api/plan/v3` prefix the service's
/// own CLI uses for plan actions — that prefix is a control-plane route wanting a
/// console session, not a plan key). The list exists so the honest no-data answer
/// and a future measured answer share one code path; a route that refuses or 404s
/// simply yields the no-data shape.
const USAGE_CANDIDATE_PATHS: &[&str] = &[
    "/api/coding/v3/usage",
    "/api/coding/v3/token_plan/remains",
    "/api/coding/v3/plan/usage",
    "/api/coding/v3/subscription/usage",
    "/api/plan/v3/usage",
    "/api/plan/v3/quota",
];

/// The console page where the plan's allowance actually lives.
pub const CONSOLE_USAGE_URL: &str =
    "https://ark.volcengine.com/region:cn-beijing/subscription/coding-plan";

const HTTP_TIMEOUT_MS: u64 = 20_000;

/// The client identity sent on every request, shared with the catalog module so
/// both send one agent string.
pub(crate) fn user_agent() -> String {
    std::env::var("ARK_USER_AGENT")
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

fn store_path() -> PathBuf {
    super::config_root().join("ark-codingplan.json")
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

static STORE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn read_store_unlocked() -> Option<Value> {
    let raw = std::fs::read_to_string(store_path()).ok()?;
    serde_json::from_str(&raw).ok()
}

fn read_store() -> Option<Value> {
    let _guard = STORE_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    read_store_unlocked()
}

fn write_store(store: &Value) -> Result<(), String> {
    let _guard = STORE_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    super::console_usage::invalidate_pending_login();
    write_store_unlocked(store)
}

fn write_store_unlocked(store: &Value) -> Result<(), String> {
    let path = store_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let serialized = serde_json::to_string_pretty(store).map_err(|error| error.to_string())?;
    // Publish a complete 0600 snapshot so readers never observe a truncated secret store.
    let temporary = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| -> std::io::Result<()> {
        use std::io::Write;
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        file.write_all(serialized.as_bytes())?;
        drop(file);
        std::fs::rename(&temporary, &path)
    })();
    if let Err(error) = result {
        let _ = std::fs::remove_file(&temporary);
        return Err(error.to_string());
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

/// The whole store, for the console module that keeps its session beside the plan
/// key in this one file.
pub(crate) fn read_store_public() -> Option<Map<String, Value>> {
    read_store().and_then(|store| store.as_object().cloned())
}

/// Atomically update a credential snapshot. Returning false leaves the file
/// untouched, so a stale console refresh cannot overwrite a newer login.
pub(crate) fn update_store_public(
    update: impl FnOnce(&mut Map<String, Value>) -> bool,
) -> Result<bool, String> {
    let _guard = STORE_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let mut store = read_store_unlocked().unwrap_or_else(|| Value::Object(Map::new()));
    let object = store
        .as_object_mut()
        .ok_or_else(|| "store is not an object".to_string())?;
    if !update(object) {
        return Ok(false);
    }
    write_store_unlocked(&store)?;
    Ok(true)
}

/// The stored plan key, if there is one.
pub fn api_key() -> Option<String> {
    read_store().and_then(|store| string_field(&store, "apiKey"))
}

/// The region the stored key belongs to. The plan publishes one, so this is nearly
/// always the constant; it is stored anyway so a future second region reads back
/// the site the key was verified against.
pub fn region() -> Option<String> {
    read_store().and_then(|store| string_field(&store, "region"))
}

/// The identity a catalog snapshot belongs to: the gateway has no account
/// route, so the key itself (hashed, never stored in plaintext) plus the site
/// the key was issued against is the only fingerprint available.
pub(crate) fn catalog_fingerprint() -> Option<String> {
    use sha2::{Digest, Sha256};
    let key = api_key()?;
    Some(format!(
        "{:x}:{}",
        Sha256::digest(key.as_bytes()),
        region().unwrap_or_else(|| "cn-beijing".to_string())
    ))
}

/// The Anthropic-compatible root a session is pointed at, and the same root the
/// catalog is read from.
pub fn api_base() -> String {
    if let Ok(value) = std::env::var("ARK_API_BASE") {
        let trimmed = value.trim().trim_end_matches('/').to_string();
        if !trimmed.is_empty() {
            return trimmed;
        }
    }
    format!("https://{HOST}{ANTHROPIC_PATH}")
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
    let head: String = key.chars().take(8).collect();
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
/// wants. Unlike a gateway that hides refusals inside a 200, this one answers a bad
/// key with a real `401 AuthenticationError`, so the status alone is a sufficient
/// verdict here — but the body is still read for its model count, and a 2xx with no
/// usable list is reported as a failure rather than stored as a success.
pub async fn set_api_key(key: &str, region: Option<&str>) -> Result<Value, Value> {
    let key = key.trim();
    if key.is_empty() {
        return Ok(json!({ "ok": false, "error": "ark_api_key_empty" }));
    }
    // The plan publishes one region. An explicit value that is not it is refused
    // rather than silently ignored: swallowing it would verify the key against a
    // host the caller did not intend, and read back later as "the key is wrong".
    let region = match region {
        Some(value) if !value.trim().is_empty() => {
            let value = value.trim();
            if value != REGION {
                return Ok(json!({ "ok": false, "error": "ark_region_unknown" }));
            }
            value.to_string()
        }
        _ => self::region().unwrap_or_else(|| REGION.to_string()),
    };

    let base = api_base();
    // Clear the PREVIOUS credential's cache before verifying: which models a plan
    // serves follows the key, so a snapshot from the old one must not survive a
    // switch. verify_key seeds the fresh read into the cache on success.
    super::catalog::forget_cache();
    match verify_key(key).await {
        Ok(models) => {
            let mut store = Map::new();
            store.insert("apiKey".into(), json!(key));
            store.insert("region".into(), json!(region));
            store.insert("apiBase".into(), json!(base));
            store.insert("modelCount".into(), json!(models.len()));
            store.insert("updatedAt".into(), json!(now_ms()));
            match write_store(&Value::Object(store)) {
                Ok(()) => Ok(status()),
                Err(error) => Ok(json!({ "ok": false, "error": error })),
            }
        }
        Err(error) => Ok(json!({ "ok": false, "error": error })),
    }
}

/// Authenticate a key against the catalog route and report the projected catalog.
///
/// The read is the catalog module's own, so verification and the picker's first
/// read cannot disagree about headers, paging or filtering. Unlike a gateway that
/// hides refusals inside a 200, this one answers a bad key with a real `401
/// AuthenticationError` (measured), so the status alone is a sufficient verdict —
/// but an empty projection is still reported as a failure rather than stored as a
/// success.
///
/// The projection is seeded into the cache on the way out: the picker's first warm
/// read would otherwise pay for a second round trip to a gateway that can sit past
/// its 1500ms budget on a cold connection, and lose to it.
async fn verify_key(key: &str) -> Result<Vec<Value>, String> {
    super::catalog::fetch_live_with_key(key)
        .await
        .map_err(|error| {
            if error.login_expired {
                "ark_key_rejected".to_string()
            } else {
                format!("ark_key_check_failed:{}", error.message)
            }
        })
        .inspect(|entries| super::catalog::remember_entries(entries))
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
        json!(string_field(&store, "region").unwrap_or_else(|| REGION.to_string())),
    );
    if let Some(count) = store.get("modelCount").and_then(Value::as_i64) {
        out.insert("modelCount".into(), json!(count));
    }
    Value::Object(out)
}

pub fn logout() -> Result<Value, Value> {
    super::catalog::forget_cache();
    // The store file holds the console session token too; removing the file drops
    // both, which is what signing out of the plan must mean.
    let _guard = STORE_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    super::console_usage::invalidate_pending_login();
    match std::fs::remove_file(store_path()) {
        Ok(()) => Ok(json!({ "ok": true })),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(json!({ "ok": true })),
        Err(error) => Ok(json!({ "ok": false, "error": error.to_string() })),
    }
}

/// The plan's allowance, as far as the plan key can read it: not at all, today.
///
/// Measured with a live key, every candidate allowance route on the coding gateway
/// answers 404; the service's published usage APIs are control-plane actions that
/// want an account signature or a console session rather than a plan key, and the
/// plan's own guide points at the console page. Reporting "connected, no allowance
/// data" with the console URL beside it is the honest shape; drawing a bar out of
/// the plan's published tier totals would state a number this account never
/// reported.
///
/// The candidate loop is kept so the answer can change without restructuring: a
/// route that starts answering a plan key is projected into the same rate-window
/// shape the host's gauges already render.
pub async fn usage() -> Result<Value, String> {
    let Some(key) = api_key() else {
        return Ok(json!({ "signedIn": false }));
    };
    if super::console::console_status()["loggedIn"] == true {
        let windows = super::console::quota_windows()
            .await
            .map_err(|error| error.message())?;
        return Ok(json!({
            "signedIn": true,
            "account": mask_key(&key),
            "region": REGION,
            "quotaAvailable": !windows.is_empty(),
            "consoleLoggedIn": super::console::console_status()["loggedIn"],
            "windows": windows,
        }));
    }
    let http = client()?;
    // The candidate routes are absolute, so they need the scheme and host only.
    // Derived from the same base every other read uses, so a test override points
    // the whole module at one place rather than at two that disagree.
    let host_root = api_base()
        .trim_end_matches(ANTHROPIC_PATH)
        .trim_end_matches('/')
        .to_string();
    for path in USAGE_CANDIDATE_PATHS {
        let Ok(response) = http
            .get(format!("{host_root}{path}"))
            .header("Authorization", format!("Bearer {key}"))
            .send()
            .await
        else {
            continue;
        };
        let status_code = response.status().as_u16();
        let body = response.text().await.unwrap_or_default();
        if status_code == 401 || status_code == 403 {
            // The key went; the card should say so rather than show a stale gauge.
            return Ok(json!({ "signedIn": false }));
        }
        if !(200..300).contains(&status_code) {
            continue;
        }
        let parsed: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
        let windows = usage_windows(&parsed);
        if !windows.is_empty() {
            return Ok(json!({
                "signedIn": true,
                "account": mask_key(&key),
                "region": REGION,
                "quotaAvailable": true,
                "windows": windows,
            }));
        }
    }
    Ok(json!({
        "signedIn": true,
        "account": mask_key(&key),
        "region": REGION,
        // No `windows`: the gateway exposes none to this key, and the console is
        // where the allowance lives.
        "quotaAvailable": false,
    }))
}

/// Project an allowance body onto the rate-window shape the host's gauges render.
///
/// Empty for every body measured today; written against the field names the
/// service's own usage APIs publish (`used`/`total` pairs and a reset timestamp) so
/// a route that starts answering a plan key projects without a rewrite.
fn usage_windows(payload: &Value) -> Vec<Value> {
    let mut windows = Vec::new();
    let rows = payload
        .get("windows")
        .or_else(|| payload.get("periods"))
        .or_else(|| payload.pointer("/data/windows"))
        .and_then(Value::as_array);
    for row in rows.into_iter().flatten() {
        let (used, total) = match (
            row.get("used").and_then(Value::as_f64),
            row.get("total").and_then(Value::as_f64),
        ) {
            (Some(used), Some(total)) if total > 0.0 => (used, total),
            _ => continue,
        };
        let used_percent = (used / total * 100.0).clamp(0.0, 100.0);
        let mut window = Map::new();
        window.insert(
            "id".into(),
            json!(row
                .get("id")
                .or_else(|| row.get("period"))
                .and_then(Value::as_str)
                .unwrap_or("window")),
        );
        window.insert("usedPercent".into(), json!(used_percent));
        if let Some(reset) = row
            .get("resetsAt")
            .or_else(|| row.get("reset_at"))
            .and_then(Value::as_i64)
        {
            window.insert("resetsAt".into(), json!(reset));
        }
        windows.push(Value::Object(window));
    }
    windows
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masking_never_reveals_a_spendable_key() {
        assert_eq!(mask_key("ark-0123456789abcdef.AbCdEfGh"), "ark-0123…EfGh");
        assert_eq!(mask_key("short"), "••••••••");
        assert!(!mask_key("ark-0123456789abcdef.AbCdEfGh").contains("AbCd"));
    }

    #[test]
    fn the_plan_is_published_at_one_region() {
        // The plan page, the key page and every documented base URL point at
        // cn-beijing, so there is nothing for a caller to pick and no second host
        // to probe. The base is therefore a constant plus a test override.
        assert_eq!(api_base(), "https://ark.cn-beijing.volces.com/api/coding");
        assert_eq!(REGION, "cn-beijing");
    }

    #[test]
    fn an_allowance_body_projects_into_rate_windows_and_an_empty_one_does_not() {
        // Today every candidate route 404s, so this projects nothing; the shape it
        // would accept is the one the service's usage APIs publish, so a route that
        // starts answering lights the gauges up without a rewrite.
        let with = usage_windows(&json!({
            "windows": [
                { "id": "five_hour", "used": 30.0, "total": 1200.0, "resetsAt": 1767225600000i64 },
                { "id": "weekly", "used": 900.0, "total": 9000.0 }
            ]
        }));
        assert_eq!(with.len(), 2);
        assert_eq!(with[0]["usedPercent"], json!(2.5));
        assert_eq!(with[0]["resetsAt"], json!(1767225600000i64));
        assert_eq!(with[1]["usedPercent"], json!(10.0));
        assert!(usage_windows(&json!({})).is_empty());
        // A window with no total is not a window: dividing by zero would paint a
        // gauge out of nothing.
        assert!(usage_windows(&json!({ "windows": [{ "used": 1.0 }] })).is_empty());
    }

    #[tokio::test]
    async fn a_signed_out_store_is_an_answer_not_an_error() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        assert_eq!(status(), json!({ "loggedIn": false }));
        assert!(api_key().is_none());
        assert_eq!(usage().await, Ok(json!({ "signedIn": false })));
        assert_eq!(logout(), Ok(json!({ "ok": true })));
    }

    #[tokio::test]
    async fn a_stored_key_reads_back_and_never_leaves_the_store_in_plaintext_status() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        write_store(&json!({
            "apiKey": "ark-0123456789abcdef.SecretHalfHere",
            "region": "cn-beijing",
            "apiBase": "https://ark.cn-beijing.volces.com/api/coding",
            "modelCount": 135,
        }))
        .expect("store write");

        assert_eq!(
            api_key().as_deref(),
            Some("ark-0123456789abcdef.SecretHalfHere")
        );
        assert_eq!(region().as_deref(), Some("cn-beijing"));
        let status = status();
        assert_eq!(status["loggedIn"], json!(true));
        assert_eq!(status["region"], json!("cn-beijing"));
        assert_eq!(status["modelCount"], json!(135));
        assert_eq!(status["apiKeyMasked"], json!("ark-0123…Here"));
        assert!(
            !status.to_string().contains("SecretHalfHere"),
            "the raw key must not reach a status reply: {status}"
        );
    }

    #[tokio::test]
    async fn usage_reports_a_connection_and_no_allowance_when_no_route_answers() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        write_store(&json!({ "apiKey": "ark-0123456789abcdef.x", "region": "cn-beijing" }))
            .expect("store write");
        // Pointed at a closed port so the candidate loop fails fast instead of
        // reaching the real gateway from a test. The env write goes through the
        // runtime's locked accessor: every process environment mutation in this
        // repository enters through the one shared lock, tests included.
        let _base = BlackholeBase;
        klaus_agent_core::runtime_process::set_process_environment_var(
            "ARK_API_BASE",
            "http://127.0.0.1:1/api/coding",
        );
        let usage = usage().await.expect("usage answers");
        drop(_base);
        assert_eq!(usage["signedIn"], json!(true));
        assert_eq!(usage["quotaAvailable"], json!(false));
        // No fabricated window or credit row: the host draws the console link.
        assert!(usage.get("windows").is_none());
    }

    #[tokio::test]
    async fn logout_clears_the_key() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        write_store(&json!({ "apiKey": "ark-0123456789abcdef.x", "region": "cn-beijing" }))
            .expect("store write");
        assert_eq!(status()["loggedIn"], json!(true));
        assert_eq!(logout(), Ok(json!({ "ok": true })));
        assert_eq!(status(), json!({ "loggedIn": false }));
        assert!(api_key().is_none());
    }

    #[tokio::test]
    async fn an_empty_key_is_refused_before_any_call() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        let empty = set_api_key("   ", None).await.unwrap();
        assert_eq!(empty["error"], json!("ark_api_key_empty"));
        assert!(api_key().is_none(), "a refused key is not stored");
    }

    #[tokio::test]
    async fn a_region_the_plan_does_not_publish_is_refused_rather_than_ignored() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        // The plan publishes one region. Silently swallowing another name would
        // verify the key against a host the caller did not intend and read back
        // later as "the key is wrong". This returns before any network call.
        let result = set_api_key("ark-0123456789abcdef.Secret", Some("ap-southeast-1"))
            .await
            .unwrap();
        assert_eq!(result["error"], json!("ark_region_unknown"));
        assert!(api_key().is_none(), "a refused region stores nothing");
    }

    #[tokio::test]
    async fn a_key_the_service_refuses_is_not_stored() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        // A closed local port so the verification fails fast without spending a
        // real key or depending on the gateway being reachable. The env write goes
        // through the runtime's locked accessor: every process environment
        // mutation in this repository enters through the one shared lock, tests
        // included.
        let _base = BlackholeBase;
        klaus_agent_core::runtime_process::set_process_environment_var(
            "ARK_API_BASE",
            "http://127.0.0.1:1/api/coding",
        );
        let result = set_api_key("ark-0123456789abcdef.SomeSecret", None)
            .await
            .unwrap();
        drop(_base);
        assert!(result["error"].is_string());
        assert_ne!(result["error"], json!("ark_api_key_empty"));
        assert_ne!(result["error"], json!("ark_region_unknown"));
        assert!(api_key().is_none(), "an unverified key is not stored");
    }

    /// Clears the override when dropped, so a failing assertion cannot leak a
    /// blackholed base into the next test in this process.
    struct BlackholeBase;
    impl Drop for BlackholeBase {
        fn drop(&mut self) {
            klaus_agent_core::runtime_process::remove_process_environment_var("ARK_API_BASE");
        }
    }
}
