//! Zhipu Coding Plan credentials and connection status.
//!
//! Coding Plan authenticates with one thing only: a plan API key minted on the
//! plan page. There is no OAuth, no device flow and no local client store to
//! import, so this module is a key holder rather than a login flow — which is why
//! there are no `start`/`poll` methods here.
//!
//! The key is verified before it is stored. Storing an unverified key would leave
//! a mistyped one looking connected on the settings card and failing inside a
//! conversation with the service's own prose; the verification costs one catalog
//! read and no inference tokens.
//!
//! ## No shape check on the key
//!
//! The neighbouring Bailian source refuses a key whose prefix is not the plan's
//! own, because that service publishes a distinct `sk-sp-` prefix for plan keys.
//! This one does not: the plan's documentation gives no key grammar, and its keys
//! look like an `id.secret` pair whose two halves carry no declared length or
//! alphabet. So the only honest test is the one the service performs — a catalog
//! read that either authenticates the key or refuses it. A regex here would reject
//! a key that works.
//!
//! ## The quota route lives on the console host
//!
//! The plan meters credits against a rolling five-hour allowance and a weekly one.
//! The inference gateway exposes neither (measured: `/v1/usage`, `/v4/usage`,
//! `/v4/user_quota`, the coding-prefixed `/v4/usage` and an organization route all
//! answer 404), but the console's own monitor route — `/api/monitor/usage/quota/limit`
//! on the site root, the one the console page draws its gauges from — accepts the
//! plan key as its bearer credential and answers both windows with used percentage
//! and reset time. Measured with a live plan key on both sites, so [`usage`] reads
//! it with the same key the session already spends and projects the two windows;
//! a site or tier that answers nothing still reports the connection with
//! `quotaAvailable: false`, which is what the console link beside the gauge is for.
//!
//! ## One honest User-Agent
//!
//! The plan's terms confine it to interactive use inside AI coding tools and agent
//! tools, and name self-built applications and API backends as out of scope.
//! Nothing here pretends to be another client: every request carries Klaus's own
//! `User-Agent`, the key stays on this machine, and calls go straight from here to
//! the gateway with no server in between.

use serde_json::{json, Map, Value};
use std::path::PathBuf;

/// Coding Plan is served by two sites, and a key is bound to the one that issued
/// it. The domestic platform and the international one keep separate accounts,
/// separate subscriptions and separate pricing, so a key is never tried against
/// both: that would either mask a misconfigured site or spend a subscription the
/// user did not point at.
const CN_HOST: &str = "open.bigmodel.cn";
const INTL_HOST: &str = "api.z.ai";

/// The Anthropic-compatible root. Deliberately carries no `/v1` suffix: the
/// provider appends `/v1/messages` itself.
const ANTHROPIC_PATH: &str = "/api/anthropic";
/// Both reads share the root. Unlike the Bailian gateway, this one serves its
/// catalog on the same protocol path as inference, so there is no second base.
const MODELS_PATH: &str = "/v1/models";
/// The console's allowance route, on the site root rather than under the
/// inference base: it is a console-page endpoint that happens to accept the plan
/// key, not part of the gateway's published API surface.
const QUOTA_PATH: &str = "/api/monitor/usage/quota/limit";

const HTTP_TIMEOUT_MS: u64 = 20_000;

/// The client identity sent on every request.
///
/// Exposed to the catalog module so both modules send one agent string: the
/// plan's terms-style restriction applies to every call this engine makes on it,
/// and a request whose client identity varies by call site is a request that
/// cannot be reasoned about when the service starts refusing it.
pub(crate) fn user_agent() -> String {
    std::env::var("ZHIPU_USER_AGENT")
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
            "cn" | "china" | "bigmodel" | "zhipu" => Some(Self::Cn),
            "intl" | "international" | "global" | "z.ai" | "zai" => Some(Self::Intl),
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
    super::config_root().join("zhipu-codingplan.json")
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

/// The stored plan key, if there is one.
pub fn api_key() -> Option<String> {
    read_store().and_then(|store| string_field(&store, "apiKey"))
}

/// The region the stored key belongs to.
pub fn region() -> Option<Region> {
    read_store()
        .and_then(|store| string_field(&store, "region").and_then(|value| Region::parse(&value)))
}

/// The Anthropic-compatible root for one site, and the same root the catalog is
/// read from.
///
/// The env override is honoured for BOTH the stored region and an explicit one, so
/// a caller checking a key against a named site and a session resolving one read
/// the same base. Honoured in only one of the two places is how a key gets
/// verified against one gateway and then spent against another.
fn base_for(region: Region) -> String {
    if let Ok(value) = std::env::var("ZHIPU_API_BASE") {
        let trimmed = value.trim().trim_end_matches('/').to_string();
        if !trimmed.is_empty() {
            return trimmed;
        }
    }
    format!("https://{}{ANTHROPIC_PATH}", region.host())
}

pub fn api_base() -> String {
    base_for(region().unwrap_or(Region::Cn))
}

/// The site root the console's quota route lives on.
///
/// Separate from [`base_for`] on purpose: the quota route is served by the site
/// root (`https://open.bigmodel.cn/...`), while inference and the catalog sit one
/// path deeper. The same env override applies, so a test gateway stands in for
/// both without a second knob.
fn quota_base_for(region: Region) -> String {
    if let Ok(value) = std::env::var("ZHIPU_API_BASE") {
        let trimmed = value.trim().trim_end_matches('/').to_string();
        if !trimmed.is_empty() {
            return trimmed;
        }
    }
    format!("https://{}", region.host())
}

/// The plan's own search MCP route, on the site root like the quota route.
pub(crate) fn search_mcp_url() -> String {
    format!(
        "{}/api/mcp/web_search_prime/mcp",
        quota_base_for(region().unwrap_or(Region::Cn))
    )
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
///
/// A plan key is an `id.secret` pair, and the id half is not a secret on its own
/// — it is what the console lists the key by — but the split point is not
/// published, so the mask keeps a fixed short head rather than trying to show the
/// whole id.
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
/// wants. A key the service refuses is reported as a stable code rather than as
/// the gateway's prose, because that prose is written for the plan's own clients.
pub async fn set_api_key(key: &str, region: Option<&str>) -> Result<Value, Value> {
    let key = key.trim();
    if key.is_empty() {
        return Ok(json!({ "ok": false, "error": "zhipu_api_key_empty" }));
    }
    // An explicit site must name one of the two the plan serves. Silently
    // swallowing an unrecognized value would leave the user believing they picked a
    // site while the key was checked against a different one — and since the sites
    // do not share keys, that reads as "the key is wrong" rather than as a typo.
    let region = match region {
        Some(value) if !value.trim().is_empty() => match Region::parse(value) {
            Some(parsed) => parsed,
            None => {
                return Ok(json!({
                    "ok": false,
                    "error": "zhipu_region_unknown",
                }))
            }
        },
        // No site named: keep whichever one the stored key already belongs to,
        // rather than moving it and failing it.
        _ => self::region().unwrap_or(Region::Cn),
    };

    let http = client().map_err(|error| json!({ "ok": false, "error": error }))?;
    let base = base_for(region);
    match verify_key(&http, &base, key).await {
        Ok(models) => {
            let mut store = Map::new();
            store.insert("apiKey".into(), json!(key));
            store.insert("region".into(), json!(region.as_str()));
            store.insert("apiBase".into(), json!(base));
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

/// What a catalog body says, once the status has already been checked.
///
/// Split out from the two readers because this gateway does not use HTTP status to
/// refuse a key on the catalog route, and the rule that replaces it is worth
/// testing directly rather than through a network call.
pub(crate) enum CatalogAnswer {
    /// The catalog answered with this many models.
    Models(usize),
    /// The key was refused. The user has to re-enter it.
    Rejected,
    /// Some other failure, with a detail for the log or the card.
    Failed(String),
}

/// Read a catalog body.
///
/// THIS IS THE PART A STATUS CHECK MISSES. Measured on 2026-10-08: this gateway
/// answers a REFUSED key with HTTP **200** and puts the refusal in the body —
/// `{"code":401,"msg":"令牌已过期或验证不正确","success":false}` — and a missing
/// key with 200 and `{"code":1001,…}`. Only the inference route returns a real 401
/// status.
///
/// So a body carrying a `data` array is a success and anything else is a refusal,
/// whatever the status said. Reading the status alone would store a key that can
/// never work, and the settings card would show it as connected until the first
/// conversation failed inside a turn.
pub(crate) fn classify_catalog_body(body: &str) -> CatalogAnswer {
    let parsed: Value = serde_json::from_str(body).unwrap_or(Value::Null);
    if let Some(rows) = parsed.get("data").and_then(Value::as_array) {
        return CatalogAnswer::Models(rows.len());
    }
    let detail: String = body.chars().take(200).collect();
    match parsed.get("code").and_then(Value::as_i64) {
        // 401 is a bad or expired key, 1001 is a missing credential header. Both
        // mean "re-enter the key", and the gateway's own prose is written for its
        // own clients rather than for this card.
        Some(401) | Some(1001) => CatalogAnswer::Rejected,
        Some(code) => CatalogAnswer::Failed(format!("the Zhipu catalog answered {code}: {detail}")),
        None => CatalogAnswer::Failed(format!(
            "the Zhipu catalog listed no usable model: {detail}"
        )),
    }
}

/// Authenticate a key against the catalog route and report how many models the
/// plan serves.
async fn verify_key(http: &reqwest::Client, base: &str, key: &str) -> Result<usize, String> {
    let response = http
        .get(format!("{base}{MODELS_PATH}"))
        .header("x-api-key", key)
        .send()
        .await
        .map_err(|error| error.to_string())?;
    let status_code = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    match status_code {
        200..=299 => {}
        // The inference route's real 401/403, kept for the case the catalog route
        // ever starts using status the way the rest of the service does.
        401 | 403 => return Err("zhipu_key_rejected".to_string()),
        _ => {
            let detail: String = body.chars().take(200).collect();
            return Err(format!("zhipu_key_check_failed:{status_code}:{detail}"));
        }
    }
    // The status was 2xx, so the body decides — see [`classify_catalog_body`].
    Ok(match classify_catalog_body(&body) {
        CatalogAnswer::Models(count) => count,
        CatalogAnswer::Rejected => return Err("zhipu_key_rejected".to_string()),
        CatalogAnswer::Failed(detail) => return Err(format!("zhipu_key_check_failed:{detail}")),
    })
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

/// Coding Plan's allowance, read with the plan key the session already spends.
///
/// The console's monitor route answers both credit windows — the rolling
/// five-hour one and the weekly one — as used percentage plus reset time, which
/// is exactly the window shape the host gauge draws. A route that answers
/// nothing (an unreachable site, a tier with no windows, a refused key) falls
/// back to "connected, no allowance data" rather than failing the whole read:
/// the popover then shows the console link, which is where the numbers live
/// anyway.
pub async fn usage() -> Result<Value, String> {
    let Some(key) = api_key() else {
        return Ok(json!({ "signedIn": false }));
    };
    let region = region().unwrap_or(Region::Cn);
    let mut out = Map::new();
    out.insert("signedIn".into(), json!(true));
    out.insert("account".into(), json!(mask_key(&key)));
    out.insert("region".into(), json!(region.as_str()));
    match quota_windows(&key, region).await {
        Some(windows) if !windows.is_empty() => {
            out.insert("windows".into(), Value::Array(windows));
        }
        // An empty window list from a well-formed answer is still an answer: the
        // tier reports no allowance rows, which is not the same as a failed read.
        Some(_) => {
            out.insert("quotaAvailable".into(), json!(false));
        }
        None => {
            out.insert("quotaAvailable".into(), json!(false));
        }
    }
    Ok(Value::Object(out))
}

/// Read the console's quota route and project its credit windows.
///
/// `None` means the read itself failed (transport error, non-200, malformed
/// body); `Some(vec![])` means it answered with no usable window row. The two
/// read the same to the caller today, but keeping them apart is what lets a
/// future tier with a new window kind surface without a code change here.
async fn quota_windows(key: &str, region: Region) -> Option<Vec<Value>> {
    let http = client().ok()?;
    let url = format!("{}{QUOTA_PATH}", quota_base_for(region));
    let response = http
        .get(&url)
        .header("Authorization", format!("Bearer {key}"))
        .header("Accept", "application/json")
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    let body: Value = response.json().await.ok()?;
    let limits = body.get("data")?.get("limits")?.as_array()?;
    let mut windows = Vec::new();
    for row in limits {
        // The two credit windows differ by period: 3 is hours (the rolling
        // five-hour allowance), 6 is days (the weekly one). A period this
        // projection does not name is skipped rather than guessed at, because a
        // bar under a wrong label is worse than no bar.
        let id = match row.get("unit").and_then(Value::as_i64) {
            Some(3) => "five_hour",
            Some(6) => "weekly",
            _ => continue,
        };
        let Some(used) = row.get("percentage").and_then(Value::as_f64) else {
            continue;
        };
        let mut window = Map::new();
        window.insert("id".into(), json!(id));
        window.insert("usedPercent".into(), json!(used.clamp(0.0, 100.0)));
        if let Some(reset) = row.get("nextResetTime").and_then(Value::as_i64) {
            window.insert("resetsAt".into(), json!(reset));
        }
        windows.push(Value::Object(window));
    }
    Some(windows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masking_never_reveals_a_spendable_key() {
        // A plan key is `id.secret`; the mask keeps a short head and the last four
        // characters, which is enough to recognize the key and not enough to use
        // it — the secret half never appears.
        assert_eq!(
            mask_key("0123456789abcdef0123456789abcdef.AbCdEfGh"),
            "01234567…EfGh"
        );
        assert_eq!(mask_key("short"), "••••••••");
        assert!(!mask_key("0123456789abcdef0123456789abcdef.AbCdEfGh").contains("AbCd"));
    }

    #[test]
    fn region_parsing_accepts_the_names_a_user_would_type() {
        assert_eq!(Region::parse("cn"), Some(Region::Cn));
        assert_eq!(Region::parse("CN"), Some(Region::Cn));
        assert_eq!(Region::parse("bigmodel"), Some(Region::Cn));
        assert_eq!(Region::parse("intl"), Some(Region::Intl));
        assert_eq!(Region::parse("z.ai"), Some(Region::Intl));
        assert_eq!(Region::parse("mars"), None);
        assert_eq!(Region::Cn.host(), CN_HOST);
        assert_eq!(Region::Intl.host(), INTL_HOST);
    }

    #[test]
    fn the_two_sites_are_distinct_hosts_sharing_one_path() {
        // The sites do not share keys, so the host is what a site choice decides;
        // the protocol path is the same on both, which is why one constant serves
        // them.
        assert_eq!(
            format!("https://{}{ANTHROPIC_PATH}", Region::Cn.host()),
            "https://open.bigmodel.cn/api/anthropic"
        );
        assert_eq!(
            format!("https://{}{ANTHROPIC_PATH}", Region::Intl.host()),
            "https://api.z.ai/api/anthropic"
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
            "apiKey": "0123456789abcdef0123456789abcdef.SecretHalfHere",
            "region": "intl",
            "apiBase": "https://api.z.ai/api/anthropic",
            "modelCount": 11,
        }))
        .expect("store write");

        assert_eq!(
            api_key().as_deref(),
            Some("0123456789abcdef0123456789abcdef.SecretHalfHere")
        );
        assert_eq!(region(), Some(Region::Intl));
        let status = status();
        assert_eq!(status["loggedIn"], json!(true));
        assert_eq!(status["region"], json!("intl"));
        assert_eq!(status["modelCount"], json!(11));
        assert_eq!(status["apiKeyMasked"], json!("01234567…Here"));
        assert!(
            !status.to_string().contains("SecretHalfHere"),
            "the raw key must not reach a status reply: {status}"
        );
    }

    #[tokio::test]
    async fn usage_reports_signed_out_without_a_stored_key() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        let usage = usage().await.expect("usage answers");
        assert_eq!(usage, json!({ "signedIn": false }));
    }

    #[tokio::test]
    async fn usage_projects_the_two_credit_windows_the_quota_route_answers() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        write_store(&json!({ "apiKey": "0123456789abcdef0123456789abcdef.x", "region": "cn" }))
            .expect("store write");
        // The console's own answer shape: two CREDIT_LIMIT rows differing by
        // period unit, percentage already the used share, reset only on the
        // weekly one.
        let (addr, handle) = one_shot_gateway(
            "200 OK",
            "application/json",
            r#"{"code":200,"msg":"ok","data":{"limits":[{"type":"CREDIT_LIMIT","unit":3,"number":5,"usage":2000,"currentValue":400,"remaining":1600,"percentage":20},{"type":"CREDIT_LIMIT","unit":6,"number":1,"usage":10000,"currentValue":308,"remaining":9691,"percentage":3,"nextResetTime":1792036539982}],"level":"lite"},"success":true}"#,
        );
        let _base = BlackholeBase::set(&format!("http://{addr}"));
        let usage = usage().await.expect("usage answers");
        handle.join().ok();
        assert_eq!(usage["signedIn"], json!(true));
        assert_eq!(
            usage["windows"],
            json!([
                { "id": "five_hour", "usedPercent": 20.0 },
                { "id": "weekly", "usedPercent": 3.0, "resetsAt": 1792036539982i64 }
            ]),
            "both windows survive: they reset on different clocks"
        );
        assert!(
            usage.get("quotaAvailable").is_none(),
            "a read that answered windows must not also claim none are available"
        );
    }

    #[tokio::test]
    async fn usage_falls_back_to_no_allowance_when_the_quota_route_is_unreachable() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        write_store(&json!({ "apiKey": "0123456789abcdef0123456789abcdef.x", "region": "cn" }))
            .expect("store write");
        // A closed port: the read fails, and the honest answer is "connected,
        // no allowance data" — never an error, and never a fabricated window.
        let _base = BlackholeBase::set("http://127.0.0.1:1");
        let usage = usage().await.expect("usage answers");
        assert_eq!(usage["signedIn"], json!(true));
        assert_eq!(usage["quotaAvailable"], json!(false));
        assert!(usage.get("windows").is_none());
    }

    /// A tiny server answering one request with a caller-chosen body.
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
                let mut request = [0u8; 8192];
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
    async fn logout_clears_the_key() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        write_store(&json!({ "apiKey": "0123456789abcdef0123456789abcdef.x", "region": "cn" }))
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
        assert_eq!(empty["error"], json!("zhipu_api_key_empty"));
        assert!(api_key().is_none(), "a refused key is not stored");
    }

    #[test]
    fn a_refused_key_arrives_as_http_200_and_the_body_says_so() {
        // The whole reason this gateway needs a body check. Measured: a bad key
        // answers 200 with a refusal in the body, NOT a 401 status. A status-only
        // check would read this as success and store a key that can never work.
        assert!(matches!(
            classify_catalog_body(r#"{"code":401,"msg":"令牌已过期或验证不正确","success":false}"#),
            CatalogAnswer::Rejected
        ));
        // A missing credential header is the same shape under code 1001.
        assert!(matches!(
            classify_catalog_body(
                r#"{"code":1001,"msg":"Header中未收到Authorization参数，无法进行身份验证。","success":false}"#
            ),
            CatalogAnswer::Rejected
        ));
    }

    #[test]
    fn a_real_catalog_body_reports_its_model_count() {
        // Success is a `data` array, and the count it carries is what the card shows
        // — so the count must come from the array, not from a status check that
        // would report zero models for a plan that serves eleven.
        assert!(matches!(
            classify_catalog_body(
                r#"{"data":[{"id":"glm-4.6"},{"id":"glm-5.3"}],"firstId":"glm-4.6","hasMore":false,"lastId":"glm-5.3"}"#
            ),
            CatalogAnswer::Models(2)
        ));
    }

    #[test]
    fn an_unparseable_or_unexpected_body_is_a_failure_not_a_success() {
        // Neither a refusal the user can act on nor a model list: report it as a
        // failure with a detail, so it never masquerades as an empty-but-valid
        // catalog (which would silently store the key and paint a blank picker).
        assert!(matches!(
            classify_catalog_body("not json at all"),
            CatalogAnswer::Failed(_)
        ));
        assert!(matches!(
            classify_catalog_body(r#"{"data":"not-an-array"}"#),
            CatalogAnswer::Failed(_)
        ));
    }

    #[tokio::test]
    async fn a_key_the_service_refuses_is_not_stored() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        // A closed local port so the verification fails fast without spending a real
        // key or depending on the gateway being reachable. The env write goes through
        // the runtime's locked accessor: every process environment mutation in this
        // repository enters through the one shared lock, tests included.
        let _base = BlackholeBase::set("http://127.0.0.1:1/api/anthropic");
        let result = set_api_key("0123456789abcdef0123456789abcdef.SomeSecret", None)
            .await
            .unwrap();
        drop(_base);
        // Not stored, and not mistaken for the two refusals that ARE decided locally
        // (empty key, bad site): this one reached the network and came back a failure.
        assert!(result["error"].is_string());
        assert_ne!(result["error"], json!("zhipu_api_key_empty"));
        assert_ne!(result["error"], json!("zhipu_region_unknown"));
        assert!(api_key().is_none(), "an unverified key is not stored");
    }

    /// Points every gateway read — catalog, inference base and the quota route —
    /// at a local address for one test, and clears the override when dropped, so a
    /// failing assertion cannot leak a blackholed base into the next test in this
    /// process.
    struct BlackholeBase;
    impl BlackholeBase {
        fn set(value: &str) -> Self {
            klaus_agent_core::runtime_process::set_process_environment_var("ZHIPU_API_BASE", value);
            Self
        }
    }
    impl Drop for BlackholeBase {
        fn drop(&mut self) {
            klaus_agent_core::runtime_process::remove_process_environment_var("ZHIPU_API_BASE");
        }
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
        let result = set_api_key("0123456789abcdef0123456789abcdef.Secret", Some("mars"))
            .await
            .unwrap();
        assert_eq!(result["error"], json!("zhipu_region_unknown"));
        assert!(api_key().is_none(), "a refused site stores nothing");
    }
}
