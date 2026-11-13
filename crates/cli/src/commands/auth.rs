//! Subscription sign-in: `login`, `logout`, and the status report.
//!
//! A subscription is the other half of how a session gets a model — the first
//! half, a BYO key, is `klaus engine credential`. Both end in the same place: a
//! credential this endpoint can read when `--model` names the source.
//!
//! ## Why the flows live here and not in the engine
//!
//! Driving a login is host work. It opens a browser, prints a code, waits on a
//! person, and it needs a terminal to do any of that; the engine has none of
//! those things and must not grow a dependency on them. What the engine owns is
//! what a credential MEANS: the endpoints, the record's location and shape all
//! come from [`model_sources`], so a login writes exactly what a session reads.
//!
//! ## Two different OAuth flows, deliberately
//!
//! Claude runs authorization-code + PKCE with a loopback redirect: the browser
//! comes back to a port this process is listening on. Codex runs a device flow:
//! this process asks for a user code, the person types it on another page, and we
//! poll. They are not two spellings of one thing — the services accept different
//! grants — so they are written separately rather than behind a shared shape that
//! would fit neither.
//!
//! Qoder is a third: also a device flow, but the whole of it — the PKCE pair, the
//! poll, the profile sealing — belongs to [`qoder`], because a Qoder request is
//! signed and only that module can sign one. So this file drives it rather than
//! reimplementing it: start, open the page, poll until it settles.

use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use klaus_agent_core::provider::oauth::{generate_pkce, HttpFetch, HttpRequest, ReqwestFetch};
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_server::app_server::model_sources::{self, SignIn};
use klaus_agent_server::app_server::qoder;
use serde_json::{json, Value};

use crate::cli::AuthSource;
use crate::error::{CliError, CliResult};
use crate::output::Printer;

/// The loopback port the Claude redirect comes back on. Fixed rather than
/// ephemeral because it is part of the redirect URI the authorize request
/// registers, and the service compares the two.
const CLAUDE_CALLBACK_PORT: u16 = 54545;

/// How long to wait for a person to finish in the browser before giving the
/// terminal back. Long enough for a password manager and a second factor.
const LOGIN_TIMEOUT: Duration = Duration::from_secs(300);

/// Sign in to a subscription.
pub async fn login(
    source: AuthSource,
    printer: &Printer,
    process_context: &RuntimeProcessContext,
) -> CliResult<()> {
    let root = credential_root();
    match source {
        AuthSource::Claude => claude_login(&root, printer, process_context).await,
        AuthSource::Codex => codex_login(&root, printer, process_context).await,
        AuthSource::Qoder => qoder_login(&root, printer, process_context).await,
    }
}

/// Where the credential records live: beside the engine's own config file, so a
/// login and a session cannot end up pointed at two different profiles.
fn credential_root() -> PathBuf {
    model_sources::config_root_of(&klaus_agent_core::config_store::user_config_path())
}

// ------------------------------------------------------------------ status

/// What this machine is signed in to.
pub async fn status(printer: &Printer) -> CliResult<()> {
    let root = credential_root();
    status_in(&root, printer).await
}

async fn status_in(root: &std::path::Path, printer: &Printer) -> CliResult<()> {
    for (source, state) in model_sources::sign_in_status(root) {
        let line = match state {
            SignIn::Active {
                account: Some(account),
            } => {
                format!("{source:<8} signed in  {account}")
            }
            SignIn::Active { account: None } => format!("{source:<8} signed in"),
            SignIn::Expired => {
                format!("{source:<8} expired    run `klaus login {source}`")
            }
            SignIn::Absent => format!("{source:<8} signed out run `klaus login {source}`"),
            SignIn::Unreadable { reason } => format!("{source:<8} unreadable {reason}"),
        };
        printer.note(&line);
    }
    printer.note(&qoder_status_line(root));
    // Which of them is actually in effect. The rows above say what this machine
    // COULD run on; without this, a reader with two subscriptions signed in and a
    // BYO key configured still cannot tell what a plain `klaus` will use.
    for line in effective_lines().await {
        printer.note(&line);
    }
    Ok(())
}

/// What answers when the command line names no model, and what `--model` can
/// name instead.
///
/// Read from the endpoint's own config FILE rather than from a live endpoint,
/// because this command deliberately starts none — it is a credential report, and
/// a report that needs an engine is one that cannot run when the engine is the
/// thing being diagnosed.
///
/// Nothing here re-derives which provider a selection would resolve to. That
/// resolution belongs to the endpoint, and a second copy of it in the CLI would be
/// free to disagree with the one that actually runs the turn. These lines report
/// only what the config states outright.
async fn effective_lines() -> Vec<String> {
    let path = klaus_agent_core::config_store::user_config_path();
    let Ok(document) = klaus_agent_core::config_store::load(&path).await else {
        return vec![format!(
            "in effect  unknown — {} could not be read",
            path.display()
        )];
    };
    let mut lines = Vec::new();
    let model = document
        .values
        .get("model")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|model| !model.is_empty());
    match model {
        Some(model) => lines.push(format!(
            "in effect  {model} — what a run with no `--model` asks for"
        )),
        None => lines
            .push("in effect  the endpoint's own default — the config names no model".to_string()),
    }
    // The API-key side, counted rather than listed: `auth status` deliberately
    // lists no models.
    let providers = document.product_section("modelProviders");
    let configured = providers
        .as_object()
        .map(|entries| entries.len())
        .unwrap_or(0);
    lines.push(match configured {
        0 => "           no BYO models are configured, so `--model custom:…` names nothing yet"
            .to_string(),
        1 => "           1 BYO model is configured; `--model custom:<id>` runs on it and its stored key"
            .to_string(),
        count => format!(
            "           {count} BYO models are configured; `--model custom:<id>` runs on one and its stored key"
        ),
    });
    lines.push(
        "           a subscription answers only when `--model subscription:… / codex:… / qoder:… / qwenwork:…` names one"
            .to_string(),
    );
    lines
}

/// Sign out of a subscription.
pub fn logout(source: AuthSource, printer: &Printer) -> CliResult<()> {
    let root = credential_root();
    let (label, removed) = match source {
        AuthSource::Claude => (
            "claude",
            model_sources::delete_claude_login(&root).map_err(CliError::engine)?,
        ),
        AuthSource::Codex => (
            "codex",
            model_sources::delete_codex_login(&root).map_err(CliError::engine)?,
        ),
        // Qoder keeps its own record and its own reader, so signing out goes
        // through the module that owns it rather than through a second path that
        // would have to know the file name.
        AuthSource::Qoder => {
            qoder::set_config_root(&root);
            let signed_in = qoder::signed_in();
            qoder::logout().map_err(|error| CliError::engine(render_qoder(&error)))?;
            ("qoder", signed_in)
        }
    };
    if removed {
        printer.note(&format!("klaus: signed out of {label}"));
    } else {
        printer.note(&format!("klaus: no {label} login was stored"));
    }
    Ok(())
}

// ------------------------------------------------------------------ claude

/// Authorization-code + PKCE against claude.com, with the redirect captured on a
/// loopback port.
async fn claude_login(
    root: &std::path::Path,
    printer: &Printer,
    process_context: &RuntimeProcessContext,
) -> CliResult<()> {
    let endpoints = model_sources::claude_endpoints();
    let pkce = generate_pkce();
    let state = rand_state();
    let redirect_uri = format!("http://localhost:{CLAUDE_CALLBACK_PORT}/callback");

    // Bound BEFORE the browser opens: a redirect arriving at a closed port is an
    // error page the user cannot recover from without starting over.
    let listener =
        std::net::TcpListener::bind(("127.0.0.1", CLAUDE_CALLBACK_PORT)).map_err(|error| {
            CliError::engine(format!(
                "cannot listen on 127.0.0.1:{CLAUDE_CALLBACK_PORT} for the login redirect: {error}"
            ))
        })?;

    // `code=true` and this parameter order are what the login page accepts.
    let url = format!(
        "{}?code=true&client_id={}&response_type=code&redirect_uri={}&scope={}&code_challenge={}&code_challenge_method=S256&state={}",
        endpoints.authorize_url,
        endpoints.client_id,
        urlencode(&redirect_uri),
        urlencode(&endpoints.scopes.join(" ")),
        urlencode(&pkce.challenge),
        urlencode(&state),
    );

    printer.note("klaus: opening the Claude sign-in page in your browser");
    printer.note(&format!("       if it does not open, visit:\n       {url}"));
    open_in_browser(process_context, &url).await;

    let code = wait_for_callback(listener, &state)?;
    printer.note("klaus: exchanging the authorization code");

    // JSON, not form encoding: this token endpoint is the ecosystem's own and
    // that is the body it answers — the desktop client exchanges the code the same
    // way (claude_auth.rs, shipped and in use). Core's generic OAuth helper
    // (`oauth::exchange_code`) sends `application/x-www-form-urlencoded`, which is
    // why this exchange is done here rather than through `login_with_loopback`.
    //
    // Worth knowing, and NOT resolved here: core's automatic refresh
    // (`oauth::refresh_tokens`, which the engine drives once a session outlives its
    // token) posts form encoding to this SAME endpoint. Whether it accepts that
    // shape is unproven — a direct probe with an invalid grant answers 429 for both
    // shapes, before any body parsing, so the question cannot be settled from
    // outside. If subscription refresh ever fails while the credential is intact,
    // look at the body format before suspecting the token: the fix would be a
    // per-config choice on `OAuthConfig`, never flipping `refresh_tokens` to JSON,
    // which would break every provider whose endpoint does take form.
    let body = json!({
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
        "client_id": endpoints.client_id,
        "code_verifier": pkce.verifier,
        "state": state,
    });
    let response = post_json(&endpoints.token_url, &body).await?;
    let access = string_field(&response, "access_token").ok_or_else(|| {
        CliError::engine("the token response carried no access_token".to_string())
    })?;
    let refresh = string_field(&response, "refresh_token");
    let expires_at = response
        .get("expires_in")
        .and_then(Value::as_i64)
        .map(|seconds| now_ms() + seconds * 1000);

    let mut tokens = serde_json::Map::new();
    tokens.insert("accessToken".into(), json!(access));
    if let Some(refresh) = refresh {
        tokens.insert("refreshToken".into(), json!(refresh));
    }
    if let Some(expires_at) = expires_at {
        tokens.insert("expiresAt".into(), json!(expires_at));
    }
    if let Some(scope) = string_field(&response, "scope") {
        tokens.insert(
            "scopes".into(),
            json!(scope
                .split(' ')
                .filter(|part| !part.is_empty())
                .collect::<Vec<_>>()),
        );
    }
    model_sources::write_claude_login(root, Value::Object(tokens)).map_err(CliError::engine)?;
    printer.note("klaus: signed in to Claude — run `klaus --model subscription:opus` to use it");
    Ok(())
}

/// Serve exactly one redirect, then answer the browser so the tab does not hang.
///
/// The `state` is compared before the code is accepted: without that check a
/// stray request to this port could hand the process someone else's code.
fn wait_for_callback(listener: std::net::TcpListener, expected_state: &str) -> CliResult<String> {
    listener
        .set_nonblocking(false)
        .map_err(|error| CliError::engine(error.to_string()))?;
    let deadline = Instant::now() + LOGIN_TIMEOUT;
    for stream in listener.incoming() {
        if Instant::now() > deadline {
            return Err(CliError::engine(
                "the sign-in timed out waiting for the browser".to_string(),
            ));
        }
        let Ok(mut stream) = stream else { continue };
        let request_line = {
            let mut reader = BufReader::new(&stream);
            let mut line = String::new();
            if reader.read_line(&mut line).is_err() {
                continue;
            }
            line
        };
        let target = request_line.split_whitespace().nth(1).unwrap_or_default();
        let (code, state) = parse_callback_query(target);
        let accepted = code.is_some() && state.as_deref() == Some(expected_state);
        let body = if accepted {
            "Signed in. You can close this tab and return to the terminal."
        } else {
            "This request did not carry a matching sign-in code. Start the login again."
        };
        let _ = stream.write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        );
        let _ = stream.flush();
        if accepted {
            return Ok(code.unwrap_or_default());
        }
    }
    Err(CliError::engine(
        "the sign-in ended without an authorization code".to_string(),
    ))
}

/// Pull `code` and `state` out of the redirect target.
fn parse_callback_query(target: &str) -> (Option<String>, Option<String>) {
    let query = target.split_once('?').map(|(_, query)| query).unwrap_or("");
    let mut code = None;
    let mut state = None;
    for pair in query.split('&') {
        match pair.split_once('=') {
            Some(("code", value)) => code = Some(urldecode(value)),
            Some(("state", value)) => state = Some(urldecode(value)),
            _ => {}
        }
    }
    (code, state)
}

// ------------------------------------------------------------------- qoder

/// Qoder's device flow, driven through [`qoder`]: it mints the PKCE pair, holds
/// the pending flow and seals the profile it gets back, because the credential it
/// writes is the one its signer reads. This function is the terminal half — show
/// the page, wait for the person, report the outcome.
async fn qoder_login(
    root: &std::path::Path,
    printer: &Printer,
    process_context: &RuntimeProcessContext,
) -> CliResult<()> {
    qoder::set_config_root(root);
    let start = qoder::start()
        .await
        .map_err(|error| CliError::engine(render_qoder(&error)))?;
    let flow = start
        .get("flowId")
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::engine("the Qoder login returned no flow id".to_string()))?
        .to_string();
    let auth_url = start
        .get("authUrl")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            CliError::engine("the Qoder login returned no authorization URL".to_string())
        })?
        .to_string();
    // Printed BEFORE the browser is tried, so a headless box still has a
    // completable login.
    printer.note("klaus: authorize this device at");
    printer.note(&format!("       {auth_url}"));
    open_in_browser(process_context, &auth_url).await;

    let interval = start
        .get("interval")
        .and_then(Value::as_i64)
        .filter(|seconds| *seconds > 0)
        .unwrap_or(1)
        .max(1) as u64;
    let deadline = Instant::now() + LOGIN_TIMEOUT;
    loop {
        if Instant::now() > deadline {
            return Err(CliError::engine(
                "the Qoder login timed out — run `klaus login qoder` again".to_string(),
            ));
        }
        tokio::time::sleep(Duration::from_secs(interval)).await;
        let polled = qoder::poll(&json!({ "flowId": flow }))
            .await
            .map_err(|error| CliError::engine(render_qoder(&error)))?;
        if polled.get("ok") == Some(&json!(true)) {
            break;
        }
        // `pending` is the only "keep waiting" answer; an expired flow or a
        // rejection stops here rather than spinning until the deadline.
        if polled.get("pending") == Some(&json!(true)) {
            continue;
        }
        return Err(CliError::engine(render_qoder(&polled)));
    }
    printer.note("klaus: signed in to Qoder — run `klaus --model qoder:auto` to use it");
    Ok(())
}

/// The `qoder-auth:*` answers carry their failure as an `error` field on a plain
/// object (the shape a settings page renders), so that is what a message here is.
fn render_qoder(value: &Value) -> String {
    value
        .get("error")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| value.to_string())
}

/// One `auth status` row for Qoder. Its record is read by the module that owns
/// it, so this reports what a session would actually find.
fn qoder_status_line(root: &std::path::Path) -> String {
    qoder::set_config_root(root);
    let status = qoder::auth_status();
    if status.get("loggedIn") != Some(&json!(true)) {
        return format!("{:<8} signed out run `klaus login qoder`", "qoder");
    }
    // Qoder refreshes its own token from the record, so an expiry in the past is
    // still recoverable without a new login; it is reported, not judged.
    match status.get("account").and_then(Value::as_str) {
        Some(account) => format!("{:<8} signed in  {account}", "qoder"),
        None => format!("{:<8} signed in", "qoder"),
    }
}

// ------------------------------------------------------------------- codex

/// Device flow: ask for a user code, let the person enter it elsewhere, poll
/// until the service says it was approved.
async fn codex_login(
    root: &std::path::Path,
    printer: &Printer,
    process_context: &RuntimeProcessContext,
) -> CliResult<()> {
    let host = model_sources::codex_auth_base();
    let client_id = model_sources::codex_client_id();
    let start = post_json(
        &format!("{host}/api/accounts/deviceauth/usercode"),
        &json!({ "client_id": client_id }),
    )
    .await?;
    let device_id = string_field(&start, "device_auth_id").ok_or_else(|| {
        CliError::engine("the device-auth request returned no device id".to_string())
    })?;
    let user_code = string_field(&start, "user_code").ok_or_else(|| {
        CliError::engine("the device-auth request returned no user code".to_string())
    })?;
    let interval = start
        .get("interval")
        .and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
        })
        .filter(|seconds| *seconds > 0)
        .unwrap_or(5);
    let expires_in = start
        .get("expires_in")
        .and_then(Value::as_i64)
        .filter(|seconds| *seconds > 0)
        .unwrap_or(900);

    let verification = format!("{host}/codex/device");
    printer.note(&format!("klaus: your sign-in code is {user_code}"));
    printer.note(&format!("       enter it at {verification}"));
    open_in_browser(process_context, &verification).await;

    let deadline = Instant::now() + Duration::from_secs(expires_in as u64);
    let account = loop {
        if Instant::now() > deadline {
            return Err(CliError::engine(
                "the sign-in code expired — run `klaus login codex` again".to_string(),
            ));
        }
        tokio::time::sleep(Duration::from_secs(interval as u64)).await;
        match codex_poll(&host, client_id, &device_id, &user_code).await? {
            Some(account) => break account,
            None => continue,
        }
    };
    model_sources::write_codex_login(root, account).map_err(CliError::engine)?;
    printer.note("klaus: signed in to Codex — run `klaus --model codex:auto` to use it");
    Ok(())
}

/// One poll. `Ok(None)` means "not approved yet"; the service says so with a 403
/// or a 404, and with a 2xx that carries no authorization code.
async fn codex_poll(
    host: &str,
    client_id: &str,
    device_id: &str,
    user_code: &str,
) -> CliResult<Option<Value>> {
    let (status, body) = post_json_status(
        &format!("{host}/api/accounts/deviceauth/token"),
        &json!({ "device_auth_id": device_id, "user_code": user_code }),
    )
    .await?;
    if status == 403 || status == 404 {
        return Ok(None);
    }
    if status == 410 {
        return Err(CliError::engine(
            "the sign-in code expired — run `klaus login codex` again".to_string(),
        ));
    }
    if !(200..300).contains(&status) {
        return Err(CliError::engine(format!(
            "the device-auth poll failed with HTTP {status}"
        )));
    }
    let Some(code) = string_field(&body, "authorization_code") else {
        return Ok(None);
    };
    let verifier = string_field(&body, "code_verifier").unwrap_or_default();
    let tokens = post_json(
        &format!("{host}/oauth/token"),
        &json!({
            "grant_type": "authorization_code",
            "client_id": client_id,
            "code": code,
            "code_verifier": verifier,
            "redirect_uri": format!("{host}/deviceauth/callback"),
        }),
    )
    .await?;
    let access = string_field(&tokens, "access_token").ok_or_else(|| {
        CliError::engine("the token exchange returned no access token".to_string())
    })?;
    let claims = jwt_claims(
        string_field(&tokens, "id_token")
            .unwrap_or_else(|| access.clone())
            .as_str(),
    );
    let account_id = claims
        .get("https://api.openai.com/auth")
        .and_then(|auth| auth.get("chatgpt_account_id"))
        .and_then(Value::as_str)
        .or_else(|| claims.get("sub").and_then(Value::as_str))
        .unwrap_or("codex-account")
        .to_string();
    let expires_at = tokens
        .get("expires_in")
        .and_then(Value::as_i64)
        .map(|seconds| now_ms() + seconds * 1000);
    let mut account = json!({
        "accountId": account_id,
        "email": claims.get("email").and_then(Value::as_str).unwrap_or_default(),
        "accessToken": access,
        "refreshToken": string_field(&tokens, "refresh_token").unwrap_or_default(),
        "accessTokenExpiresAt": expires_at,
        "updatedAt": now_ms(),
    });
    if let Some(id_token) = string_field(&tokens, "id_token").filter(|token| !token.is_empty()) {
        account["idToken"] = json!(id_token);
    }
    Ok(Some(account))
}

/// The payload half of a JWT, decoded without verification.
///
/// Not a security decision: the token was just received over TLS from the issuer,
/// and the only thing read out of it is which account to file the credential
/// under. Nothing is authorized on the strength of these claims.
fn jwt_claims(token: &str) -> Value {
    let Some(payload) = token.split('.').nth(1) else {
        return json!({});
    };
    let padded = match payload.len() % 4 {
        2 => format!("{payload}=="),
        3 => format!("{payload}="),
        _ => payload.to_string(),
    };
    let bytes = base64_url_decode(&padded);
    serde_json::from_slice(&bytes).unwrap_or_else(|_| json!({}))
}

// -------------------------------------------------------------------- plumbing

async fn post_json(url: &str, body: &Value) -> CliResult<Value> {
    let (status, value) = post_json_status(url, body).await?;
    if !(200..300).contains(&status) {
        return Err(CliError::engine(format!("{url} answered HTTP {status}")));
    }
    Ok(value)
}

/// POST JSON and hand back the status with the parsed body, so a caller that
/// treats some statuses as "not yet" can tell them apart from a failure.
async fn post_json_status(url: &str, body: &Value) -> CliResult<(u16, Value)> {
    let fetch: Arc<dyn HttpFetch> = Arc::new(ReqwestFetch::new());
    let response = fetch
        .fetch(
            url,
            HttpRequest {
                method: "POST".to_string(),
                headers: vec![
                    ("content-type".to_string(), "application/json".to_string()),
                    ("accept".to_string(), "application/json".to_string()),
                ],
                body: body.to_string(),
            },
        )
        .await
        .map_err(|error| CliError::engine(format!("{url}: {error}")))?;
    let parsed = serde_json::from_str::<Value>(&response.body).unwrap_or(Value::Null);
    Ok((response.status, parsed))
}

fn string_field(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
        .map(str::to_string)
}

/// Open a URL in the user's browser, best effort: a headless box has none, and
/// the URL was printed before this ran so the login is still completable.
async fn open_in_browser(process_context: &RuntimeProcessContext, url: &str) {
    let _ = process_context;
    let opener = if cfg!(target_os = "macos") {
        "open"
    } else if cfg!(target_os = "windows") {
        "explorer"
    } else {
        "xdg-open"
    };
    let _ = std::process::Command::new(opener)
        .arg(url)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_millis() as i64)
        .unwrap_or_default()
}

/// The OAuth `state` parameter: 128 unguessable bits.
///
/// This is the CSRF binding between the authorization request and the callback, so
/// it has to be unpredictable and it has to be unique per request. The previous
/// implementation mixed `SystemTime::subsec_nanos` with the address of a local
/// variable, which is neither: the address is the same on every call from the same
/// frame, so two calls within one clock tick returned the *same* state. Its own test
/// caught that by flaking.
///
/// `Uuid::new_v4` is backed by the platform CSPRNG, which is what the rest of the
/// tree already uses for OAuth state and PKCE verifiers.
fn rand_state() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

fn urlencode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
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

fn urldecode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
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
    String::from_utf8_lossy(&out).into_owned()
}

fn base64_url_decode(value: &str) -> Vec<u8> {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let index_of = |byte: u8| TABLE.iter().position(|candidate| *candidate == byte);
    let mut out: Vec<u8> = Vec::new();
    let mut buffer: u32 = 0;
    let mut bits = 0;
    for byte in value.bytes() {
        let Some(sextet) = index_of(byte) else {
            continue;
        };
        buffer = (buffer << 6) | sextet as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buffer >> bits) as u8);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_callback_query_yields_the_code_and_state() {
        let (code, state) = parse_callback_query("/callback?code=abc%2F123&state=s-1");
        assert_eq!(code.as_deref(), Some("abc/123"));
        assert_eq!(state.as_deref(), Some("s-1"));
    }

    /// A redirect with no code must not read as a successful login.
    #[test]
    fn a_callback_without_a_code_yields_nothing() {
        let (code, _) = parse_callback_query("/callback?error=access_denied");
        assert_eq!(code, None);
        let (code, _) = parse_callback_query("/favicon.ico");
        assert_eq!(code, None);
    }

    #[test]
    fn url_encoding_round_trips_the_characters_a_redirect_carries() {
        let raw = "http://localhost:54545/callback";
        assert_eq!(urldecode(&urlencode(raw)), raw);
        assert_eq!(urlencode("a b"), "a%20b");
        assert_eq!(urldecode("a+b"), "a b");
    }

    #[test]
    fn jwt_claims_reads_the_payload_without_verifying_it() {
        // {"sub":"user-1","email":"a@b.c"}
        let payload = "eyJzdWIiOiJ1c2VyLTEiLCJlbWFpbCI6ImFAYi5jIn0";
        let token = format!("header.{payload}.signature");
        let claims = jwt_claims(&token);
        assert_eq!(claims["sub"], json!("user-1"));
        assert_eq!(claims["email"], json!("a@b.c"));
    }

    #[test]
    fn a_malformed_token_reads_as_no_claims() {
        assert_eq!(jwt_claims("not-a-jwt"), json!({}));
        assert_eq!(jwt_claims("a.!!!.c"), json!({}));
    }

    #[test]
    fn two_states_differ() {
        // Not a smoke test: a repeated `state` is a CSRF hole, and the old
        // clock-plus-stack-address implementation repeated whenever two calls landed in
        // the same clock tick. Many draws, so a collision cannot hide behind luck.
        let mut seen = std::collections::HashSet::new();
        for _ in 0..1000 {
            assert!(seen.insert(rand_state()), "an OAuth state repeated");
        }
        // And each one is full-length hex, so nothing silently shortened it.
        for state in &seen {
            assert_eq!(state.len(), 32, "state {state} is not 128 bits of hex");
            assert!(state.chars().all(|c| c.is_ascii_hexdigit()));
        }
    }
}
