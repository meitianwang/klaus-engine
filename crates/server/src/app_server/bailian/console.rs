//! Bailian console session login and the quota gateway it unlocks.
//!
//! The plan key (`sk-sp-…`) authenticates inference only: the gateway serves no
//! quota route to it. The allowance the console renders lives behind a second
//! credential — a console session token — and the service publishes one
//! supported way for a third-party client to obtain it: a browser authorize
//! page that posts the token back to a loopback listener. The official Bailian
//! CLI ships exactly this flow, so implementing it here is walking a published
//! door, not scraping a private one.
//!
//! ## The login contract
//!
//! 1. This module binds `127.0.0.1:0`, generates a random `state`, and hands the
//!    host an authorize URL: `{console root}/console-login?notice=127.0.0.1:{port}?state={state}`.
//!    The `notice` value is deliberately `host:port?state=…` in ONE parameter —
//!    that is the shape the console page parses, and inventing a cleaner one
//!    would produce a page that posts nothing back.
//! 2. The console page (already carrying the user's Aliyun session) posts the
//!    token to that loopback address with CORS open, as query params on GET or
//!    as a urlencoded form on POST. The listener checks `state`, keeps
//!    `access_token` plus site/region, stores them beside the plan key, answers
//!    `OK`, and shuts down.
//! 3. Nothing waits on the listener inside a facade call: the host polls
//!    `bailian/consoleStatus`, which flips to logged-in the moment the callback
//!    thread stores the token. A login that never completes simply expires.
//!
//! ## The quota gateway
//!
//! With a token stored, allowance figures come from the OneConsole server
//! gateway: `POST https://{cs gateway}/cli/api.json?action={action}&product=sfm_bailian&api={api}`
//! with a urlencoded `params` envelope (`Api`, `V`, `Data` plus the
//! `cornerstoneParam` block) and the token as `Authorization: Bearer`. The
//! gateway answers `NotLogined` when the session has lapsed; that is the
//! expiry signal, and it clears the stored token so the next read reports a
//! signed-out console instead of retrying a dead credential.
//!
//! ## What the token is
//!
//! A console session token is broader than the plan key (it reads the
//! account's subscription surface) and narrower than an AccessKey (it cannot
//! mutate cloud resources). It is stored in the same 0600 store as the plan
//! key, never leaves this machine, and every request carries this client's own
//! User-Agent.

use rand::RngCore;
use serde_json::{json, Map, Value};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::time::{Duration, Instant};

use super::auth::{region, Region};

/// How long the loopback listener stays open for a login that never completes.
const LOGIN_WINDOW: Duration = Duration::from_secs(300);
/// Per-connection read deadline: a browser that opens the port and stalls must
/// not pin the accept loop.
const CONNECTION_READ_TIMEOUT: Duration = Duration::from_secs(10);
/// One request is more than enough headroom for a form post of this size.
const MAX_REQUEST_BYTES: usize = 64 * 1024;

const CONSOLE_ROOT_CN: &str = "https://bailian.console.aliyun.com";
const CONSOLE_ROOT_INTL: &str = "https://modelstudio.console.alibabacloud.com";
const CS_GATEWAY_CN: &str = "https://bailian-cs.console.aliyun.com/cli/api.json";
const CS_GATEWAY_INTL: &str = "https://bailian-cs.console.alibabacloud.com/cli/api.json";
const GATEWAY_ACTION_CN: &str = "BroadScopeAspnGateway";
const GATEWAY_ACTION_INTL: &str = "IntlBroadScopeAspnGateway";

/// The entitlement quota rows: one per Token Plan benefit (harness tools and
/// infrastructure), each carrying what is left against its cycle total.
const API_EQUITY_INFO: &str = "zeldaEasy.bailian-commerce.tokenPlan.queryTokenPlanEquityInfo";

/// The monthly Credits pool the console card headlines: a used RATIO against
/// the month's pool plus the reset instant. Measured live on a personal
/// edition session; the route name comes from the subscription page's own
/// bundle, and it answers through the same gateway and token as the equity
/// route.
const API_MONTHLY_USAGE: &str = "zeldaHttp.apikeyMgr./tokenplan/personal/api/v2/usage";

const HTTP_TIMEOUT_MS: u64 = 20_000;

/// Store keys for the console credential, kept beside the plan key in the one
/// store file so a logout of the plan also drops the session token.
const KEY_TOKEN: &str = "consoleToken";
const KEY_SITE: &str = "consoleSite";
const KEY_REGION: &str = "consoleRegion";
const KEY_LOGIN_AT: &str = "consoleLoginAt";
/// Set once any console login has ever landed: it is what separates "this
/// machine had a session that lapsed, renew it quietly" from "the user never
/// connected a console, do not open tabs on their behalf".
const KEY_EVER_CONNECTED: &str = "consoleEverConnected";
/// When the last automatic renewal attempt started, so a dead browser session
/// cannot turn into a tab every few seconds.
const KEY_LAST_RENEW_AT: &str = "consoleLastRenewAt";

/// Minimum gap between two automatic renewal attempts. The authorize page
/// posts back on its own when the browser still holds an Aliyun session; when
/// it does not, each attempt is a wasted tab, so this bounds the waste.
const AUTO_RENEW_GAP: Duration = Duration::from_secs(600);

/// What the gateway said, when it said no.
#[derive(Debug, PartialEq, Eq)]
pub enum GatewayError {
    /// The console session lapsed; the stored token is already cleared.
    NotLogined,
    Transport(String),
    Business(String),
}

fn console_root(site: &str) -> &'static str {
    if site == "international" {
        CONSOLE_ROOT_INTL
    } else {
        CONSOLE_ROOT_CN
    }
}

fn gateway(site: &str) -> (&'static str, &'static str) {
    if site == "international" {
        (CS_GATEWAY_INTL, GATEWAY_ACTION_INTL)
    } else {
        (CS_GATEWAY_CN, GATEWAY_ACTION_CN)
    }
}

/// The site the console credential belongs to: whichever the login recorded,
/// else the site the stored plan key belongs to, else the China site.
fn effective_site() -> String {
    if let Some(store) = super::auth::read_store_public() {
        if let Some(site) = store
            .get(KEY_SITE)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            return site.to_string();
        }
    }
    match region() {
        Some(Region::Intl) => "international".to_string(),
        _ => "domestic".to_string(),
    }
}

fn effective_region() -> String {
    if let Some(store) = super::auth::read_store_public() {
        if let Some(region) = store
            .get(KEY_REGION)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            return region.to_string();
        }
    }
    match region() {
        Some(Region::Intl) => "ap-southeast-1".to_string(),
        _ => "cn-beijing".to_string(),
    }
}

/// The stored console session token, if a login completed and nothing has
/// cleared it since.
pub fn console_token() -> Option<String> {
    super::auth::read_store_public().and_then(|store| {
        store
            .get(KEY_TOKEN)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    })
}

fn patch_store(patch: Map<String, Value>) -> Result<(), String> {
    super::auth::patch_store_public(patch)
}

/// Record that a console login landed. Called from the callback path only, so
/// the flag means "a session existed", not "a login was attempted".
fn mark_ever_connected(patch: &mut Map<String, Value>) {
    patch.insert(KEY_EVER_CONNECTED.to_string(), json!(true));
}

/// Drop the console credential only, keeping the plan key: an expired session
/// must not sign the user out of inference. The login timestamp stays: it is a
/// historical fact ("a session existed here"), and it is what lets renewal
/// recognize a store written before the ever-connected flag existed.
pub fn clear_console_token() {
    let mut patch = Map::new();
    for key in [KEY_TOKEN, KEY_SITE, KEY_REGION] {
        patch.insert(key.to_string(), Value::Null);
    }
    let _ = patch_store(patch);
}

/// Whether a console session is stored, for the settings card and the popover.
pub fn console_status() -> Value {
    match console_token() {
        Some(_) => json!({
            "loggedIn": true,
            "site": effective_site(),
            "region": effective_region(),
        }),
        None => json!({ "loggedIn": false }),
    }
}

/// Start a renewal attempt without a user click, if the throttle allows it.
///
/// The console session token lapses after a while while the browser's Aliyun
/// session usually does not, so the authorize page can mint a fresh token with
/// no interaction at all. This is what makes the popover's bars come back on
/// their own. It answers None — and opens nothing — when no login ever landed
/// on this machine, or when the last attempt is still inside the gap.
pub fn auto_renewal_url() -> Option<String> {
    let store = super::auth::read_store_public()?;
    // The flag is written on every login from this build on; a login timestamp
    // left by an older build counts too, so machines that connected before the
    // flag existed still renew instead of waiting for a manual click.
    let ever = store.get(KEY_EVER_CONNECTED).and_then(Value::as_bool) == Some(true)
        || store.get(KEY_LOGIN_AT).and_then(Value::as_i64).is_some();
    if !ever {
        return None;
    }
    let now = now_ms();
    let last = store
        .get(KEY_LAST_RENEW_AT)
        .and_then(Value::as_i64)
        .unwrap_or(0);
    if now - last < AUTO_RENEW_GAP.as_millis() as i64 {
        return None;
    }
    let started = console_login_start().ok()?;
    let mut patch = Map::new();
    patch.insert(KEY_LAST_RENEW_AT.to_string(), json!(now));
    let _ = patch_store(patch);
    started
        .get("url")
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// Open the loopback listener and answer with the authorize URL.
///
/// Returns as soon as the listener is bound: the browser round trip happens on
/// the user's clock, and the host watches `console_status` flip rather than
/// holding a facade call open for it.
pub fn console_login_start() -> Result<Value, String> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .map_err(|error| format!("could not bind the login callback port: {error}"))?;
    let port = listener
        .local_addr()
        .map_err(|error| error.to_string())?
        .port();
    let mut state_bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut state_bytes);
    let state: String = state_bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let site = effective_site();
    // The console page reads `notice` as one opaque value holding the loopback
    // address AND the state; splitting them into two params would post back a
    // state of empty and fail our own check.
    let url = format!(
        "{}/console-login?notice=127.0.0.1:{}?state={}",
        console_root(&site),
        port,
        urlencoded(&state)
    );
    let expected = state.clone();
    std::thread::spawn(move || accept_loop(listener, expected));
    Ok(json!({ "ok": true, "url": url, "site": site }))
}

/// Serve callbacks until one carries our state, or the login window closes.
fn accept_loop(listener: TcpListener, expected_state: String) {
    let deadline = Instant::now() + LOGIN_WINDOW;
    // Non-blocking accept plus a poll interval: the loop must notice the
    // deadline even while no browser ever connects.
    listener.set_nonblocking(true).unwrap_or_default();
    while Instant::now() < deadline {
        match listener.accept() {
            Ok((mut stream, _peer)) => {
                stream
                    .set_read_timeout(Some(CONNECTION_READ_TIMEOUT))
                    .unwrap_or_default();
                let mut raw = Vec::new();
                let mut chunk = [0u8; 4096];
                loop {
                    match stream.read(&mut chunk) {
                        Ok(0) => break,
                        Ok(read) => {
                            raw.extend_from_slice(&chunk[..read]);
                            if raw.len() >= MAX_REQUEST_BYTES || request_complete(&raw) {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                let (status, body) = match handle_connection(&raw, &expected_state) {
                    Handled::Preflight => (204, ""),
                    Handled::Stored => (200, "OK"),
                    Handled::StoreFailed => (500, "failed to store the console session"),
                    Handled::Refused => (400, "bad state"),
                };
                let _ = write_response(&mut stream, status, body);
                if status == 200 {
                    return;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(_) => return,
        }
    }
}

/// True once the headers are in and any declared body has arrived, so the read
/// loop stops without waiting for the read timeout on every request.
fn request_complete(raw: &[u8]) -> bool {
    let Some(header_end) = find_header_end(raw) else {
        return false;
    };
    let headers = String::from_utf8_lossy(&raw[..header_end]).to_lowercase();
    let content_length = headers
        .lines()
        .find_map(|line| line.strip_prefix("content-length:"))
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(0);
    raw.len() >= header_end + 4 + content_length
}

fn find_header_end(raw: &[u8]) -> Option<usize> {
    raw.windows(4).position(|window| window == b"\r\n\r\n")
}

fn write_response(stream: &mut impl Write, status: u16, body: &str) -> std::io::Result<()> {
    let reason = match status {
        200 => "OK",
        204 => "No Content",
        500 => "Internal Server Error",
        _ => "Bad Request",
    };
    // The console page posts from its own origin, so the callback needs CORS;
    // the listener is loopback-only, so nothing remote can reach it.
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, POST, PUT, PATCH, OPTIONS\r\nAccess-Control-Allow-Headers: Content-Type\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes())?;
    stream.write_all(body.as_bytes())?;
    stream.flush()
}

/// What one connection on the login listener amounts to.
enum Handled {
    /// A CORS preflight: answered 204 with the allow-headers, no state involved.
    Preflight,
    Stored,
    StoreFailed,
    Refused,
}

fn handle_connection(raw: &[u8], expected_state: &str) -> Handled {
    let Some(header_end) = find_header_end(raw) else {
        return Handled::Refused;
    };
    let head = String::from_utf8_lossy(&raw[..header_end]).to_string();
    let method = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().next())
        .unwrap_or_default()
        .to_uppercase();
    if method == "OPTIONS" {
        // The console page posts from its own origin, so the browser asks first.
        // A preflight answered with anything but a 2xx kills the post that
        // follows, which is the whole login.
        return Handled::Preflight;
    }
    match parse_callback(raw, expected_state) {
        Some(mut credential) => {
            mark_ever_connected(&mut credential);
            match patch_store(credential) {
                Ok(()) => Handled::Stored,
                Err(_) => Handled::StoreFailed,
            }
        }
        None => Handled::Refused,
    }
}

/// Pull the credential out of one callback request, or refuse it.
///
/// Accepts the shapes the console page and its cousins use — query params on
/// GET, a JSON body on POST (what the authorize page actually sends), and a
/// urlencoded form — and refuses anything whose `state` is not the one this
/// login minted.
fn parse_callback(raw: &[u8], expected_state: &str) -> Option<Map<String, Value>> {
    let header_end = find_header_end(raw)?;
    let head = String::from_utf8_lossy(&raw[..header_end]).to_string();
    let mut lines = head.lines();
    let request_line = lines.next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_uppercase();
    let target = parts.next()?;
    let host = lines
        .find_map(|line| {
            line.to_lowercase()
                .strip_prefix("host:")
                .map(|value| value.trim().to_string())
        })
        .unwrap_or_default();
    // A callback addressed to anything but the loopback interface is not ours.
    let host_name = host.split(':').next().unwrap_or_default().to_lowercase();
    if host_name != "127.0.0.1" && host_name != "localhost" {
        return None;
    }
    let path_and_query = target.split_once('#').map_or(target, |(path, _)| path);
    let (path, query) = path_and_query
        .split_once('?')
        .unwrap_or((path_and_query, ""));
    if method == "OPTIONS" {
        return None;
    }
    let body = if method == "GET" {
        ""
    } else {
        std::str::from_utf8(&raw[header_end + 4..]).ok()?
    };
    let mut params: Map<String, Value> = Map::new();
    let content_type = head
        .lines()
        .find_map(|line| {
            line.to_lowercase()
                .strip_prefix("content-type:")
                .map(|value| value.trim().to_string())
        })
        .unwrap_or_default();
    if content_type.contains("application/json") {
        // The authorize page posts its credential as JSON, not as a form.
        let parsed: Value = serde_json::from_str(body).ok()?;
        if let Some(object) = parsed.as_object() {
            for (key, value) in object {
                params.insert(key.clone(), value.clone());
            }
        }
    } else {
        for source in [query, body] {
            for pair in source.split('&').filter(|pair| !pair.is_empty()) {
                let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
                params.insert(urldecoded(key), Value::String(urldecoded(value)));
            }
        }
    }
    for source in [query] {
        for pair in source.split('&').filter(|pair| !pair.is_empty()) {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            params.insert(urldecoded(key), Value::String(urldecoded(value)));
        }
    }
    let state = params
        .get("state")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if state != expected_state || !path.starts_with('/') {
        return None;
    }
    let token = ["access_token", "accessToken"]
        .iter()
        .find_map(|key| params.get(*key).and_then(Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    let mut store = Map::new();
    store.insert(KEY_TOKEN.to_string(), json!(token));
    store.insert(KEY_LOGIN_AT.to_string(), json!(now_ms()));
    for (param, key) in [
        ("console_site", KEY_SITE),
        ("consoleSite", KEY_SITE),
        ("console_region", KEY_REGION),
        ("consoleRegion", KEY_REGION),
    ] {
        if let Some(value) = params
            .get(param)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            store.insert(key.to_string(), json!(value));
        }
    }
    Some(store)
}

fn urlencoded(value: &str) -> String {
    let mut out = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

fn urldecoded(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' if index + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[index + 1..index + 3]).unwrap_or("");
                match u8::from_str_radix(hex, 16) {
                    Ok(byte) => {
                        out.push(byte);
                        index += 3;
                    }
                    Err(_) => {
                        out.push(bytes[index]);
                        index += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                index += 1;
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).to_string()
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Build the `params` envelope the OneConsole gateway expects: the api name,
/// a protocol version, and the api's own request body wrapped with the
/// console-product block the gateway routes on.
fn envelope(api: &str, data: Map<String, Value>) -> String {
    let mut cornerstone = Map::new();
    cornerstone.insert("protocol".into(), json!("V2"));
    cornerstone.insert("console".into(), json!("ONE_CONSOLE"));
    cornerstone.insert("productCode".into(), json!("p_efm"));
    cornerstone.insert("switchUserType".into(), json!(3));
    cornerstone.insert("consoleSite".into(), json!("BAILIAN_ALIYUN"));
    let mut wrapped = data;
    wrapped.insert("cornerstoneParam".into(), Value::Object(cornerstone));
    let payload = json!({ "Api": api, "V": "1.0", "Data": Value::Object(wrapped) });
    payload.to_string()
}

/// Call one gateway api with the stored console token.
async fn gateway_call(api: &str, data: Map<String, Value>) -> Result<Value, GatewayError> {
    let token = console_token().ok_or(GatewayError::NotLogined)?;
    let site = effective_site();
    let (gateway, action) = gateway(&site);
    let region = effective_region();
    let body = format!(
        "params={}&region={}",
        urlencoded(&envelope(api, data)),
        urlencoded(&region)
    );
    let url = format!(
        "{gateway}?action={action}&product=sfm_bailian&api={}",
        urlencoded(api)
    );
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(HTTP_TIMEOUT_MS))
        .user_agent(super::auth::user_agent())
        .build()
        .map_err(|error| GatewayError::Transport(error.to_string()))?;
    let response = client
        .post(&url)
        .header("Accept", "*/*")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .header("Authorization", format!("Bearer {token}"))
        .body(body)
        .send()
        .await
        .map_err(|error| GatewayError::Transport(error.to_string()))?;
    let status = response.status().as_u16();
    let text = response
        .text()
        .await
        .map_err(|error| GatewayError::Transport(error.to_string()))?;
    if !(200..=299).contains(&status) {
        return Err(GatewayError::Transport(format!("gateway HTTP {status}")));
    }
    let parsed: Value = serde_json::from_str(&text).map_err(|_| GatewayError::Business(text))?;
    let inner = parsed.get("data").cloned().unwrap_or(Value::Null);
    let failed = inner.get("success").and_then(Value::as_bool) == Some(false);
    if failed {
        let code = inner
            .get("errorCode")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if code.contains("NotLogined") {
            clear_console_token();
            return Err(GatewayError::NotLogined);
        }
        return Err(GatewayError::Business(code));
    }
    // The payload nests three deep: outer.data is the gateway envelope, whose
    // DataV2.data is the api result wrapper, whose own `data` is the answer.
    // Measured live; a projection that stops one level early reads an envelope
    // and reports "no rows" for a plan that has them.
    let envelope_level = inner.get("DataV2").cloned().unwrap_or(inner);
    let result_level = envelope_level
        .get("data")
        .cloned()
        .unwrap_or(envelope_level);
    Ok(result_level.get("data").cloned().unwrap_or(result_level))
}

/// The allowance rows the console card renders, projected onto rate windows.
///
/// The personal edition's exact field spelling is not published, so this reads
/// the union of shapes the service's own clients consume: a used/total pair
/// with a cycle end, or a cycle total with a cycle surplus (surplus first,
/// because that is the number a user reads as "what I have left").
pub async fn quota_windows() -> Result<Vec<Value>, GatewayError> {
    let mut equity_data = Map::new();
    equity_data.insert(
        "queryTokenPlanEquityInfoRequest".into(),
        json!({ "requestId": request_id() }),
    );
    let (monthly, equity) = tokio::join!(
        gateway_call(API_MONTHLY_USAGE, Map::new()),
        gateway_call(API_EQUITY_INFO, equity_data),
    );
    let mut windows = Vec::new();
    // The pool is the headline: it is the allowance that stops ordinary
    // inference, so it leads the list the equity rows follow.
    if let Ok(ref payload) = monthly {
        windows.extend(project_monthly(payload));
    }
    if let Ok(ref payload) = equity {
        windows.extend(project_windows(payload));
    }
    if windows.is_empty() {
        // Neither route answered a row: surface whichever call failed, so a
        // lapsed session reads as signed-out rather than as an empty plan.
        monthly?;
        equity?;
    }
    Ok(windows)
}

/// The console card's headline number: a used ratio (0..1) against the
/// monthly pool, with the instant the pool resets.
fn project_monthly(payload: &Value) -> Vec<Value> {
    let ratio = payload
        .get("per1MonthPercentage")
        .and_then(number)
        .or_else(|| {
            payload
                .get("data")
                .and_then(|inner| inner.get("per1MonthPercentage"))
                .and_then(number)
        });
    let Some(ratio) = ratio else {
        return Vec::new();
    };
    let used = (ratio * 100.0).clamp(0.0, 100.0);
    let resets = payload
        .get("per1MonthResetTime")
        .and_then(number)
        .or_else(|| {
            payload
                .get("data")
                .and_then(|inner| inner.get("per1MonthResetTime"))
                .and_then(number)
        });
    let mut window = Map::new();
    window.insert("id".into(), json!("monthly_credits"));
    window.insert("usedPercent".into(), json!(used));
    if let Some(reset) = resets {
        if reset > 0.0 {
            window.insert("resetsAt".into(), json!(reset as i64));
        }
    }
    vec![Value::Object(window)]
}

fn request_id() -> String {
    let mut bytes = [0u8; 8];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn project_windows(payload: &Value) -> Vec<Value> {
    let mut windows = Vec::new();
    fn visit(row: &Value, windows: &mut Vec<Value>) {
        // Measured live: an equity row carries what is LEFT (`availableQuota`)
        // against its cycle total, so used is the difference. The used/total
        // spelling stays as a fallback for shapes that report it directly.
        let (used, total) = match (
            row.get("availableQuota").and_then(number),
            row.get("totalQuota").and_then(number),
        ) {
            (Some(available), Some(total)) if total > 0.0 => {
                ((total - available).clamp(0.0, total), total)
            }
            _ => match (
                row.get("usedQuota").and_then(number),
                row.get("totalQuota").and_then(number),
            ) {
                (Some(used), Some(total)) if total > 0.0 => (used.clamp(0.0, total), total),
                _ => return,
            },
        };
        let resets = row
            .get("instanceEndTime")
            .and_then(number)
            .or_else(|| row.get("cycleEndTime").and_then(number));
        let mut window = Map::new();
        window.insert(
            "id".into(),
            json!(row
                .get("equityType")
                .and_then(Value::as_str)
                .unwrap_or("entitlement")),
        );
        window.insert("usedPercent".into(), json!(used / total * 100.0));
        if let Some(reset) = resets {
            if reset > 0.0 {
                window.insert("resetsAt".into(), json!(reset as i64));
            }
        }
        // No `label`: the row's id is a stable code the host localizes, and
        // shipping the code as the label is what put raw equityType strings on
        // the user's screen.
        windows.push(Value::Object(window));
    }
    if let Some(infos) = payload
        .get("tokenPlanEquityInfos")
        .and_then(Value::as_array)
    {
        for equity in infos {
            visit(equity, &mut windows);
        }
    }
    if let Some(items) = payload.get("items").and_then(Value::as_array) {
        for item in items {
            visit(item, &mut windows);
            if let Some(equities) = item.get("equityList").and_then(Value::as_array) {
                for equity in equities {
                    visit(equity, &mut windows);
                }
            }
        }
    }
    if windows.is_empty() {
        visit(payload, &mut windows);
    }
    windows
}

fn number(value: &Value) -> Option<f64> {
    value.as_f64().or_else(|| {
        value
            .as_str()
            .and_then(|text| text.trim().parse::<f64>().ok())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_request(state: &str, host: &str) -> Vec<u8> {
        format!(
            "GET /callback?state={state}&access_token=tok-123&console_site=domestic HTTP/1.1\r\nHost: {host}\r\n\r\n"
        )
        .into_bytes()
    }

    fn post_request(state: &str) -> Vec<u8> {
        let body = format!("state={state}&accessToken=tok-456&consoleRegion=cn-beijing");
        format!(
            "POST /callback HTTP/1.1\r\nHost: 127.0.0.1:53001\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    /// The shape the console authorize page actually sends: a JSON body with
    /// the credential, state riding in the query.
    fn json_post_request(state: &str) -> Vec<u8> {
        let body = serde_json::json!({
            "access_token": "tok-json-789",
            "console_site": "domestic",
            "console_region": "cn-beijing",
            "console_switch_agent": "11150171",
            "workspace_id": "llm-test",
        })
        .to_string();
        format!(
            "POST /?state={state} HTTP/1.1\r\nHost: 127.0.0.1:53001\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    fn preflight_request() -> Vec<u8> {
        b"OPTIONS /?state=abc123 HTTP/1.1\r\nHost: 127.0.0.1:53001\r\nOrigin: https://bailian.console.aliyun.com\r\nAccess-Control-Request-Method: POST\r\nAccess-Control-Request-Headers: content-type\r\n\r\n".to_vec()
    }

    #[test]
    fn a_callback_with_our_state_stores_the_token() {
        let credential = parse_callback(&post_request("abc123"), "abc123").expect("accepted");
        assert_eq!(
            credential.get("consoleToken").and_then(Value::as_str),
            Some("tok-456")
        );
        assert_eq!(
            credential.get("consoleRegion").and_then(Value::as_str),
            Some("cn-beijing")
        );
        assert!(credential.get("consoleLoginAt").is_some());
    }

    #[test]
    fn a_json_body_credential_is_read_the_way_the_authorize_page_sends_it() {
        let credential = parse_callback(&json_post_request("abc123"), "abc123").expect("accepted");
        assert_eq!(
            credential.get("consoleToken").and_then(Value::as_str),
            Some("tok-json-789")
        );
        assert_eq!(
            credential.get("consoleSite").and_then(Value::as_str),
            Some("domestic")
        );
        assert_eq!(
            credential.get("consoleRegion").and_then(Value::as_str),
            Some("cn-beijing")
        );
    }

    #[test]
    fn a_cors_preflight_is_answered_without_touching_the_store() {
        assert!(matches!(
            handle_connection(&preflight_request(), "abc123"),
            Handled::Preflight
        ));
        // A preflight carries no state and must not be refused as bad state.
        assert!(!matches!(
            handle_connection(&preflight_request(), "other"),
            Handled::Refused
        ));
    }

    #[test]
    fn a_callback_with_a_foreign_state_is_refused() {
        assert!(parse_callback(&get_request("attacker", "127.0.0.1:1"), "abc123").is_none());
        assert!(parse_callback(&post_request("abc123"), "other").is_none());
    }

    #[test]
    fn a_callback_addressed_to_a_foreign_host_is_refused() {
        assert!(parse_callback(&get_request("abc123", "evil.example:1234"), "abc123").is_none());
    }

    #[test]
    fn a_callback_without_a_token_is_refused() {
        let raw = b"GET /callback?state=abc123 HTTP/1.1\r\nHost: 127.0.0.1:1\r\n\r\n";
        assert!(parse_callback(raw, "abc123").is_none());
    }

    #[test]
    fn the_authorize_url_carries_loopback_and_state_in_one_notice_param() {
        let url = format!(
            "{}/console-login?notice=127.0.0.1:{}?state={}",
            console_root("domestic"),
            53001,
            urlencoded("abc123")
        );
        assert_eq!(
            url,
            "https://bailian.console.aliyun.com/console-login?notice=127.0.0.1:53001?state=abc123"
        );
        let intl = console_root("international");
        assert_eq!(intl, "https://modelstudio.console.alibabacloud.com");
    }

    #[test]
    fn the_envelope_names_the_api_and_the_console_product() {
        let mut data = Map::new();
        data.insert("requestId".into(), json!("r1"));
        let payload: Value =
            serde_json::from_str(&envelope(API_EQUITY_INFO, data)).expect("envelope is json");
        assert_eq!(payload["Api"], json!(API_EQUITY_INFO));
        assert_eq!(payload["V"], json!("1.0"));
        assert_eq!(
            payload["Data"]["cornerstoneParam"]["console"],
            json!("ONE_CONSOLE")
        );
        assert_eq!(
            payload["Data"]["cornerstoneParam"]["productCode"],
            json!("p_efm")
        );
    }

    #[test]
    fn a_used_total_pair_projects_onto_a_monthly_window() {
        let payload = json!({ "items": [ { "usedQuota": 877, "totalQuota": 10000,
            "instanceEndTime": 1793376000000_i64, "specType": "standard" } ] });
        let windows = project_windows(&payload);
        assert_eq!(windows.len(), 1);
        let percent = windows[0]["usedPercent"].as_f64().expect("percent");
        assert!((percent - 8.77).abs() < 0.001, "percent was {percent}");
        assert_eq!(windows[0]["resetsAt"], json!(1793376000000_i64));
        // No raw code ships as a label: the host localizes by id.
        assert!(windows[0].get("label").is_none());
    }

    #[test]
    fn an_available_quota_pair_projects_with_used_derived() {
        // The live equity shape: what is LEFT against the cycle total, so the
        // used share is the complement. 8000 of 10000 left is 20% used.
        let payload = json!({ "tokenPlanEquityInfos": [ { "availableQuota": 8000,
            "totalQuota": 10000, "instanceEndTime": 1793289600000_i64,
            "equityType": "tokenplan_harnesstool_tts" } ] });
        let windows = project_windows(&payload);
        assert_eq!(windows.len(), 1);
        let percent = windows[0]["usedPercent"].as_f64().expect("percent");
        assert!((percent - 20.0).abs() < 0.001, "percent was {percent}");
        assert_eq!(windows[0]["id"], json!("tokenplan_harnesstool_tts"));
        assert_eq!(windows[0]["resetsAt"], json!(1793289600000_i64));
    }

    #[tokio::test]
    async fn auto_renewal_stays_silent_for_a_machine_that_never_connected() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        super::super::auth::write_store(&json!({
            "apiKey": "sk-sp-abcdefghijklmnop", "region": "cn",
        }))
        .expect("store write");
        assert!(
            auto_renewal_url().is_none(),
            "no session ever landed, no tab"
        );
    }

    #[tokio::test]
    async fn auto_renewal_opens_once_per_gap_then_stays_quiet() {
        let _lock = super::super::test_env_lock().lock().await;
        let temp = tempfile::tempdir().expect("tempdir");
        super::super::set_config_root(temp.path());
        super::super::auth::write_store(&json!({
            "apiKey": "sk-sp-abcdefghijklmnop", "region": "cn",
            "consoleEverConnected": true,
        }))
        .expect("store write");
        let first = auto_renewal_url().expect("a lapsed session renews");
        assert!(first.contains("/console-login?notice=127.0.0.1:"));
        // Inside the gap the second attempt opens nothing: a dead browser
        // session must not become a tab every few seconds.
        assert!(auto_renewal_url().is_none());
        let store = super::super::auth::read_store_public().expect("store");
        assert!(
            store.get(KEY_LAST_RENEW_AT).is_some(),
            "the attempt is recorded"
        );
    }

    #[test]
    fn the_monthly_pool_ratio_projects_onto_the_headline_window() {
        // The live shape: a used RATIO against the pool, not a percent.
        let payload = json!({ "per1MonthPercentage": 0.0877,
            "per1MonthResetTime": 1793289600000_i64 });
        let windows = project_monthly(&payload);
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0]["id"], json!("monthly_credits"));
        let percent = windows[0]["usedPercent"].as_f64().expect("percent");
        assert!((percent - 8.77).abs() < 0.001, "percent was {percent}");
        assert_eq!(windows[0]["resetsAt"], json!(1793289600000_i64));
    }

    #[test]
    fn a_monthly_payload_without_a_ratio_projects_nothing() {
        assert!(project_monthly(&json!({})).is_empty());
        assert!(project_monthly(&json!({ "per1MonthPercentage": null })).is_empty());
    }

    #[test]
    fn the_live_gateway_envelope_unwraps_to_the_answer() {
        // The three-deep nesting measured on the wire: gateway envelope,
        // DataV2 result wrapper, then the api answer.
        let raw = json!({ "code": "200", "data": { "DataV2": { "ret": ["SUCCESS::ok"],
            "data": { "success": true, "data": { "tokenPlanEquityInfos": [
                { "availableQuota": 9000, "totalQuota": 10000, "equityType": "x" } ] } } } },
            "success": true });
        let inner = raw.get("data").cloned().unwrap();
        let envelope_level = inner.get("DataV2").cloned().unwrap_or(inner.clone());
        let result_level = envelope_level
            .get("data")
            .cloned()
            .unwrap_or(envelope_level);
        let answer = result_level.get("data").cloned().unwrap_or(result_level);
        assert_eq!(project_windows(&answer).len(), 1);
    }

    #[test]
    fn a_payload_without_allowance_rows_projects_nothing() {
        assert!(project_windows(&json!({ "items": [] })).is_empty());
        assert!(project_windows(&json!({})).is_empty());
        // A zero total cannot anchor a bar: drawing one would invent a scale.
        assert!(
            project_windows(&json!({ "items": [ { "usedQuota": 0, "totalQuota": 0 } ] }))
                .is_empty()
        );
    }

    #[test]
    fn request_completeness_waits_for_a_declared_body() {
        let raw = post_request("abc123");
        assert!(request_complete(&raw));
        assert!(!request_complete(&raw[..raw.len() - 3]));
        assert!(!request_complete(
            b"GET /callback HTTP/1.1\r\nHost: 127.0.0.1:1\r\n"
        ));
    }
}
