//! Local OpenAI-compatible bridge for the Qoder subscription.
//!
//! The Rust engine's `openai` (chat completions) provider points at this
//! loopback server; the bridge translates OpenAI chat requests into Qoder's
//! `agent_chat_generation` payload, signs them with the native COSY signer
//! ([`super::sign`]), forwards to the Qoder backend, and unwraps Qoder's SSE
//! envelope (whose inner `body` is already a standard OpenAI
//! `chat.completion.chunk`) back into a standard OpenAI stream.
//!
//! Loopback-only: binds 127.0.0.1, rejects non-loopback Host headers, and a
//! per-process bridge token gates `/v1/*` so only this host (which knows the
//! token) can drive it. This is a native re-implementation of the Qoder
//! subscription bridge; no crypto lives here — the signing stays in the wasm
//! module, exactly as the client's own signer does it.

use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use futures::StreamExt;
use klaus_agent_core::observability::provider_diagnostics as diagnostics;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub const QODER_DEFAULT_CONTEXT_TOKENS: i64 = 180_000;

/// Accept-retry window. An accept failure that leaves the LISTENER intact (the
/// process momentarily out of descriptors, an aborted handshake) is retried with
/// this backoff instead of ending the loop: the alternative, measured, was a
/// bridge that stopped listening for the rest of the process's life while every
/// session kept dialing its dead port.
const ACCEPT_RETRY_MIN: Duration = Duration::from_millis(50);
const ACCEPT_RETRY_MAX: Duration = Duration::from_millis(2_000);

// ---------------------------------------------------------------- server state

struct Running {
    base_url: String,
    token: String,
    /// The composer's thinking tier, pushed in-process. The engine only emits
    /// `reasoning_effort` for model ids it recognizes as reasoning models, and
    /// Qoder's ids are opaque keys (`ultimate`/`cmodel`/…), so the tier would
    /// never survive the OpenAI hop — the host hands it over directly.
    thinking: Arc<Mutex<Option<String>>>,
    /// True while this endpoint's accept loop is alive. The loop clears it on the
    /// way out, which is what lets [`ensure_started`] tell a live endpoint from a
    /// retired one: a cached `base_url` whose listener is gone answers every
    /// request with `Connection refused`, and nothing else would ever rebind it.
    serving: Arc<AtomicBool>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum BridgeSource {
    Qoder,
    QwenWork,
    QoderWork,
}

impl BridgeSource {
    fn label(self) -> &'static str {
        match self {
            Self::Qoder => "Qoder",
            Self::QwenWork => "QwenWork",
            Self::QoderWork => "QoderWork",
        }
    }

    fn owned_by(self) -> &'static str {
        match self {
            Self::Qoder => "qoder",
            Self::QwenWork => "qwenwork",
            Self::QoderWork => "qoderwork",
        }
    }
}

#[derive(Default)]
struct BridgeState {
    running: HashMap<BridgeSource, Running>,
    pending_thinking: HashMap<BridgeSource, Option<String>>,
}

fn state() -> &'static Mutex<BridgeState> {
    static STATE: OnceLock<Mutex<BridgeState>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(BridgeState::default()))
}

fn http_client() -> reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            reqwest::Client::builder()
                // The upstream leg is bounded on purpose. One turn is one
                // request, and reqwest's default keeps EVERY idle connection per
                // host: in the measured EMFILE incident this pool alone held 29
                // sockets to the Qoder gateway long after the turns that opened
                // them had finished.
                .pool_max_idle_per_host(8)
                .pool_idle_timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new())
        })
        .clone()
}

/// ensureStarted mirror: start the loopback server once and return its base URL
/// and the host-private bridge token. Idempotent — a second call returns the
/// already-running endpoint, and an endpoint whose accept loop has STOPPED is
/// replaced rather than handed out again.
pub(crate) async fn ensure_started() -> Result<(String, String), String> {
    ensure_started_for(BridgeSource::Qoder).await
}

pub(crate) async fn ensure_started_for(source: BridgeSource) -> Result<(String, String), String> {
    // A retired endpoint is carried forward for one thing only: the composer's
    // thinking tier. Rebinding is not a reason to silently drop the tier the host
    // pushed, and nothing re-pushes it until the user changes it.
    let carried_thinking = {
        let guard = state().lock().unwrap_or_else(|p| p.into_inner());
        match guard.running.get(&source) {
            Some(running) if running.serving.load(Ordering::Acquire) => {
                return Ok((running.base_url.clone(), running.token.clone()));
            }
            Some(running) => Some(running.thinking.clone()),
            None => guard
                .pending_thinking
                .get(&source)
                .cloned()
                .map(|level| Arc::new(Mutex::new(level))),
        }
    };
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| e.to_string())?;
    let port = listener.local_addr().map_err(|e| e.to_string())?.port();
    let base_url = format!("http://127.0.0.1:{port}");
    let token = super::uuid_v4();
    let thinking = carried_thinking.unwrap_or_else(|| Arc::new(Mutex::new(None)));
    let serving = Arc::new(AtomicBool::new(true));

    {
        let mut guard = state().lock().unwrap_or_else(|p| p.into_inner());
        // Lost a start race: drop this listener and reuse the winner's endpoint.
        let winner = guard.running.get(&source).and_then(|running| {
            running.serving.load(Ordering::Acquire).then(|| {
                (
                    running.base_url.clone(),
                    running.token.clone(),
                    running.thinking.clone(),
                )
            })
        });
        if let Some((winner_base_url, winner_token, winner_thinking)) = winner {
            if let Some(level) = guard.pending_thinking.remove(&source) {
                *winner_thinking.lock().unwrap_or_else(|p| p.into_inner()) = level;
            }
            return Ok((winner_base_url, winner_token));
        }
        // A tier may have arrived after the first state check but before the
        // listener was ready. Apply it to the endpoint that is about to win.
        if let Some(level) = guard.pending_thinking.remove(&source) {
            *thinking.lock().unwrap_or_else(|p| p.into_inner()) = level;
        }
        guard.running.insert(
            source,
            Running {
                base_url: base_url.clone(),
                token: token.clone(),
                thinking: thinking.clone(),
                serving: serving.clone(),
            },
        );
    }

    let accept_token = token.clone();
    tokio::spawn(async move {
        let mut retry = ACCEPT_RETRY_MIN;
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    retry = ACCEPT_RETRY_MIN;
                    let token = accept_token.clone();
                    let thinking = thinking.clone();
                    tokio::spawn(async move {
                        let _ = handle_conn(stream, token, thinking, source).await;
                    });
                }
                // The listener is still bound; this attempt failed for a reason
                // that passes (no descriptor to hand the connection, an aborted
                // handshake, a signal). Ending the loop here is what turned a
                // transient EMFILE into a permanently dead subscription.
                Err(error) if accept_failure_is_transient(&error) => {
                    eprintln!(
                        "[{}Bridge] accept retry in {retry:?}: {error}",
                        source.label()
                    );
                    tokio::time::sleep(retry).await;
                    retry = (retry * 2).min(ACCEPT_RETRY_MAX);
                }
                // The listener itself is unusable, so this endpoint is over. Mark
                // it retired instead of leaving the port cached: the next
                // `ensure_started` then binds a new one.
                Err(error) => {
                    eprintln!(
                        "[{}Bridge] listener closed, endpoint retired: {error}",
                        source.label()
                    );
                    break;
                }
            }
        }
        serving.store(false, Ordering::Release);
    });
    eprintln!("[{}Bridge] listening on {base_url}", source.label());
    Ok((base_url, token))
}

/// Whether an `accept()` failure leaves the listener usable.
///
/// The descriptor-exhaustion errors are the load-bearing entries: `accept` is
/// what fails first when a process hits `RLIMIT_NOFILE`, and it fails without
/// disturbing the bound socket — the pending connection stays in the backlog and
/// the next attempt takes it.
fn accept_failure_is_transient(error: &std::io::Error) -> bool {
    use std::io::ErrorKind;
    if matches!(
        error.kind(),
        ErrorKind::Interrupted
            | ErrorKind::ConnectionAborted
            | ErrorKind::ConnectionReset
            | ErrorKind::WouldBlock
            | ErrorKind::TimedOut
            | ErrorKind::OutOfMemory
    ) {
        return true;
    }
    // EMFILE / ENFILE / ENOBUFS have no stable `ErrorKind`, so they are matched
    // by errno. This is the case the incident was made of: errno 24 on a macOS
    // GUI process holding launchd's 256-descriptor soft limit.
    #[cfg(unix)]
    {
        matches!(
            error.raw_os_error(),
            Some(libc::EMFILE) | Some(libc::ENFILE) | Some(libc::ENOBUFS) | Some(libc::ENOMEM)
        )
    }
    #[cfg(not(unix))]
    {
        false
    }
}

/// setThinkingLevel mirror: push the composer's tier for the bridge to map onto
/// whatever the selected model advertises. Calls made before bridge startup are
/// retained and applied to the first endpoint for that source.
pub(crate) fn set_thinking_level(level: Option<String>) {
    set_thinking_level_for(BridgeSource::Qoder, level);
}

pub(crate) fn set_thinking_level_for(source: BridgeSource, level: Option<String>) {
    let mut guard = state().lock().unwrap_or_else(|p| p.into_inner());
    if let Some(running) = guard.running.get(&source) {
        *running.thinking.lock().unwrap_or_else(|p| p.into_inner()) = level;
    } else {
        guard.pending_thinking.insert(source, level);
    }
}

// ------------------------------------------------------------- request routing

struct HttpRequest {
    method: String,
    path: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

async fn read_http_request(stream: &mut TcpStream) -> Result<HttpRequest, String> {
    let mut buf: Vec<u8> = Vec::new();
    let mut tmp = [0u8; 4096];
    let header_end = loop {
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            break pos;
        }
        let n = stream.read(&mut tmp).await.map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("connection closed before headers".into());
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.len() > 2_000_000 {
            return Err("request headers too large".into());
        }
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("/").to_string();
    let mut headers = HashMap::new();
    for line in lines {
        if let Some((key, value)) = line.split_once(':') {
            headers.insert(key.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }
    let content_length: usize = headers
        .get("content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let mut body: Vec<u8> = buf[header_end + 4..].to_vec();
    while body.len() < content_length {
        let n = stream.read(&mut tmp).await.map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    body.truncate(content_length);
    Ok(HttpRequest {
        method,
        path,
        headers,
        body,
    })
}

/// The reason phrase for a status line, from the canonical HTTP table. Reading
/// it out of `StatusCode` instead of a hand-kept list matters: the list this
/// replaced answered `OK` for every status it had not been taught, so an
/// upstream 405 went back to the engine as `HTTP/1.1 405 OK`.
fn reason_phrase(status: u16) -> &'static str {
    reqwest::StatusCode::from_u16(status)
        .ok()
        .and_then(|status| status.canonical_reason())
        .unwrap_or("Unknown")
}

async fn write_json(stream: &mut TcpStream, status: u16, value: &Value) -> Result<(), String> {
    let body = value.to_string();
    let head = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: {}\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n",
        status,
        reason_phrase(status),
        body.len()
    );
    stream
        .write_all(head.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    stream
        .write_all(body.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    stream.flush().await.ok();
    Ok(())
}

async fn write_sse_headers(stream: &mut TcpStream) -> Result<(), String> {
    stream
        .write_all(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream; charset=utf-8\r\nCache-Control: no-cache, no-transform\r\nConnection: close\r\n\r\n",
        )
        .await
        .map_err(|e| e.to_string())
}

async fn handle_conn(
    mut stream: TcpStream,
    token: String,
    thinking: Arc<Mutex<Option<String>>>,
    source: BridgeSource,
) -> Result<(), String> {
    let request = read_http_request(&mut stream).await?;
    let path = request.path.split('?').next().unwrap_or("/").to_string();

    // /healthz bypasses the loopback check (a liveness probe needs no Host).
    if request.method == "GET" && path == "/healthz" {
        return write_json(&mut stream, 200, &json!({ "ok": true })).await;
    }

    let host = request.headers.get("host").cloned().unwrap_or_default();
    let hostname = host.split(':').next().unwrap_or("");
    if hostname != "127.0.0.1" && hostname != "localhost" {
        return write_json(
            &mut stream,
            403,
            &json!({ "error": { "type": "forbidden", "message": "Loopback only" } }),
        )
        .await;
    }

    if request.method == "GET" && path == "/v1/models" {
        let models = match source {
            BridgeSource::Qoder => super::catalog::fetch_models().await.unwrap_or_default(),
            BridgeSource::QwenWork => super::super::qwenwork::fetch_models()
                .await
                .unwrap_or_default(),
            BridgeSource::QoderWork => super::super::qoderwork::fetch_models()
                .await
                .unwrap_or_default(),
        };
        let data: Vec<Value> = models
            .iter()
            .map(|model| {
                json!({
                    "id": model.get("key").and_then(Value::as_str).unwrap_or(""),
                    "object": "model",
                    "owned_by": source.owned_by(),
                    "display_name": model.get("display_name").cloned().unwrap_or(Value::Null),
                })
            })
            .collect();
        return write_json(&mut stream, 200, &json!({ "object": "list", "data": data })).await;
    }

    if request.method == "POST" && path.ends_with("/chat/completions") {
        let authorized = request
            .headers
            .get("authorization")
            .map(|value| value == &format!("Bearer {token}"))
            .unwrap_or(false);
        if !authorized {
            // A stale bearer means the caller holds a token from an endpoint this
            // bridge retired (a rebind mints a new one). Nothing here can repair
            // that — the caller has to re-resolve the provider — but the record
            // is what separates this case from an upstream refusal later.
            diagnostics::record(
                "bridge_token_mismatch",
                json!({
                    "source": source.label(),
                    "sent": request.headers.contains_key("authorization"),
                }),
            );
            return write_json(
                &mut stream,
                401,
                &json!({ "error": { "type": "authentication_error", "message": "Invalid bridge token" } }),
            )
            .await;
        }
        let body: Value = if request.body.is_empty() {
            json!({})
        } else {
            serde_json::from_slice(&request.body).unwrap_or_else(|_| json!({}))
        };
        let level = thinking.lock().unwrap_or_else(|p| p.into_inner()).clone();
        return handle_chat_completions(&mut stream, body, level, source).await;
    }

    write_json(
        &mut stream,
        404,
        &json!({ "error": { "type": "not_found", "message": format!("Unknown endpoint: {path}") } }),
    )
    .await
}

async fn handle_chat_completions(
    stream: &mut TcpStream,
    body: Value,
    thinking_level: Option<String>,
    source: BridgeSource,
) -> Result<(), String> {
    let wants_stream = body.get("stream") == Some(&json!(true));
    let model_key = body
        .get("model")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("auto")
        .to_string();
    let model_info = match source {
        BridgeSource::Qoder => super::catalog::lookup_model(&model_key).await,
        BridgeSource::QwenWork => super::super::qwenwork::lookup_model(&model_key).await,
        BridgeSource::QoderWork => super::super::qoderwork::lookup_model(&model_key).await,
    };
    if let Err(error) = validate_fast_mode(&body, model_info.as_ref()) {
        return write_json(
            stream,
            400,
            &json!({ "error": { "type": "invalid_request_error", "message": error } }),
        )
        .await;
    }
    let qoder_body = openai_to_qoder_infer_body(
        &body,
        &model_key,
        model_info.as_ref(),
        thinking_level.as_deref(),
    );

    // Renew the token before signing, not after a 401: the signature embeds the
    // token, so a stale one would have to be discovered by the upstream refusing
    // the whole turn.
    let refresh = match source {
        BridgeSource::Qoder => super::auth::ensure_fresh_token().await,
        BridgeSource::QwenWork => super::super::qwenwork::ensure_fresh_token().await,
        BridgeSource::QoderWork => super::super::qoderwork::ensure_fresh_token().await,
    };
    if let Err(error) = refresh {
        return write_json(
            stream,
            502,
            &json!({ "error": { "type": "internal_error", "message": error } }),
        )
        .await;
    }
    let request_body = qoder_body.to_string();
    let mut attempt = 0;
    let response = loop {
        attempt += 1;
        let signed = match sign_for(source, &request_body, &model_key) {
            Ok(signed) => signed,
            Err(error) => {
                // A request that cannot be signed is missing the credential, which no
                // amount of retrying produces: answer in the auth band so the engine
                // fails the turn visibly instead of sitting in its retry hold.
                diagnostics::record(
                    "sign_failed",
                    json!({ "source": source.label(), "model": model_key, "detail": error }),
                );
                return write_json(
                    stream,
                    401,
                    &json!({ "error": { "type": "authentication_error", "message": error } }),
                )
                .await;
            }
        };
        let client = http_client();
        let mut builder = client.post(&signed.url);
        for (name, value) in &signed.headers {
            builder = builder.header(name, value);
        }
        if let Some(body) = &signed.body {
            builder = builder.body(body.clone());
        }
        let response = match builder.send().await {
            Ok(response) => response,
            Err(error) => {
                return write_json(
                    stream,
                    502,
                    &json!({ "error": { "type": "upstream_error", "message": error.to_string() } }),
                )
                .await;
            }
        };
        let status = response.status();
        if status.is_success() {
            break response;
        }

        let text = response.text().await.unwrap_or_default();
        // At most ONE repair attempt: if the reread produced a fresh credential
        // and the gateway still refuses, the token was not the problem and
        // another round would only spend another request to learn nothing.
        let (recovered, recovery) = match refusal_action(status.as_u16(), &text, attempt) {
            RefusalAction::RereadCredential => {
                let outcome = reread_local_credential(source).await;
                (outcome == "ok", format!("reread:{outcome}"))
            }
            RefusalAction::Terminal(reason) => (false, reason.to_string()),
        };
        diagnostics::record(
            "upstream_rejected",
            json!({
                "source": source.label(),
                "model": model_key,
                "status": status.as_u16(),
                "serviceCode": service_error_code(&text),
                "requestId": upstream_request_id(&text),
                "detail": text,
                "recovery": recovery,
                "attempt": attempt,
            }),
        );
        if recovered {
            continue;
        }
        let failure = classify_upstream_failure(status, &text, source.label());
        return write_json(stream, failure.status, &failure.payload).await;
    };

    let mut unwrapper = SseUnwrapper::new();
    let mut normalizer = ToolCallIndexNormalizer::new();
    let mut byte_stream = response.bytes_stream();

    if wants_stream {
        write_sse_headers(stream).await?;
        while let Some(next) = byte_stream.next().await {
            // A broken connection mid-stream used to end the response in silence:
            // the caller saw a stream that simply stopped. Record it, so "the
            // answer was cut off" has a cause on disk instead of only an effect
            // in the transcript.
            let Ok(bytes) = next else {
                diagnostics::record(
                    "stream_broken",
                    json!({ "source": source.label(), "model": model_key, "phase": "stream" }),
                );
                break;
            };
            for inner in unwrapper.push(bytes.as_ref()) {
                if inner == "[DONE]" {
                    stream
                        .write_all(b"data: [DONE]\n\n")
                        .await
                        .map_err(|e| e.to_string())?;
                    stream.flush().await.ok();
                    return Ok(());
                }
                let out = normalize_qoder_chunk_string(&inner, &mut normalizer);
                stream
                    .write_all(format!("data: {out}\n\n").as_bytes())
                    .await
                    .map_err(|e| e.to_string())?;
                stream.flush().await.ok();
            }
        }
        for inner in unwrapper.finish() {
            if inner == "[DONE]" {
                stream.write_all(b"data: [DONE]\n\n").await.ok();
                break;
            }
            let out = normalize_qoder_chunk_string(&inner, &mut normalizer);
            stream
                .write_all(format!("data: {out}\n\n").as_bytes())
                .await
                .ok();
        }
        stream.flush().await.ok();
        return Ok(());
    }

    // Non-stream: aggregate the unwrapped chunks into one chat.completion.
    let mut chunks: Vec<Value> = Vec::new();
    let mut stream_error: Option<UpstreamFailure> = None;
    let mut done = false;
    while let Some(next) = byte_stream.next().await {
        let Ok(bytes) = next else {
            diagnostics::record(
                "stream_broken",
                json!({ "source": source.label(), "model": model_key, "phase": "aggregate" }),
            );
            break;
        };
        for inner in unwrapper.push(bytes.as_ref()) {
            consume_aggregate(
                inner,
                &mut chunks,
                &mut stream_error,
                &mut done,
                &mut normalizer,
            );
            if done {
                break;
            }
        }
        if done {
            break;
        }
    }
    if !done {
        for inner in unwrapper.finish() {
            consume_aggregate(
                inner,
                &mut chunks,
                &mut stream_error,
                &mut done,
                &mut normalizer,
            );
            if done {
                break;
            }
        }
    }
    if let Some(error) = stream_error {
        // A failure inside the stream: the model was already answering, so this
        // is a mid-turn refusal rather than a rejected request. Record it — the
        // caller only sees the status — and then answer as before.
        diagnostics::record(
            "stream_error",
            json!({
                "source": source.label(),
                "model": model_key,
                "detail": error.payload,
            }),
        );
        return write_json(stream, error.status, &error.payload).await;
    }
    let aggregated = aggregate_openai_chunks(&chunks, &model_key);
    write_json(stream, 200, &aggregated).await
}

/// What the bridge answers with when the upstream call did not succeed.
struct UpstreamFailure {
    status: u16,
    payload: Value,
}

/// Longest upstream detail kept inside an error message.
const UPSTREAM_DETAIL_CHARS: usize = 400;

/// Sign one inference request for whichever subscription this bridge serves.
fn sign_for(
    source: BridgeSource,
    body_json: &str,
    model_key: &str,
) -> Result<super::sign::SignedRequest, String> {
    match source {
        BridgeSource::Qoder => {
            super::catalog::sign_infer_request(body_json, model_key).map_err(|error| error.message)
        }
        BridgeSource::QwenWork => {
            super::super::qwenwork::catalog::sign_infer_request(body_json, model_key)
                .map_err(|error| error.message)
        }
        BridgeSource::QoderWork => {
            super::super::qoderwork::catalog::sign_infer_request(body_json, model_key)
                .map_err(|error| error.message)
        }
    }
}

/// Whether a refusal means the access token is dead, i.e. the one failure this
/// layer can repair by rereading the local credential store.
///
/// The standard is the service's OWN error catalogue rather than our guesses: of
/// the authentication codes Qoder's client ships, exactly the two it marks
/// `action:reLogin` are the ones a fresh login repairs — `105` ("Login expired")
/// and the expired-session code. The others (`auth.signature.mismatch`,
/// `auth.version.not.supported`, a missing header) are not about a stale token,
/// and rereading for them would replace a working credential for nothing.
fn refusal_is_dead_login(status: u16, body: &str) -> bool {
    if status != 403 {
        return false;
    }
    let code = service_error_code(body);
    matches!(code.as_deref(), Some("105") | Some("auth.user.inactive"))
}

/// What a refusal by the upstream should cause.
#[derive(Debug, PartialEq, Eq)]
enum RefusalAction {
    /// A dead login on the FIRST attempt: reread the local store and try once
    /// more. Deliberately unavailable after the first attempt, so a gateway that
    /// keeps answering 105 cannot turn into a retry loop.
    RereadCredential,
    /// Give up, and say why in the diagnostic record.
    Terminal(&'static str),
}

fn refusal_action(status: u16, body: &str, attempt: u32) -> RefusalAction {
    if !refusal_is_dead_login(status, body) {
        return RefusalAction::Terminal("none:not_a_dead_login");
    }
    if attempt > 1 {
        return RefusalAction::Terminal("reread:exhausted");
    }
    RefusalAction::RereadCredential
}

/// The service error code out of a refusal, whichever envelope carried it. The
/// body may arrive encoded like a success payload, so decode before reading.
fn service_error_code(body: &str) -> Option<String> {
    let parsed = parse_upstream_body(body)?;
    for key in ["code", "msgCode", "errorCode"] {
        if let Some(value) = parsed.get(key) {
            let text = match value {
                Value::String(text) => text.trim().to_string(),
                Value::Number(number) => number.to_string(),
                _ => continue,
            };
            if !text.is_empty() {
                return Some(text);
            }
        }
    }
    parsed
        .get("error")
        .and_then(|error| error.get("code"))
        .and_then(|code| match code {
            Value::String(text) => Some(text.trim().to_string()),
            Value::Number(number) => Some(number.to_string()),
            _ => None,
        })
}

/// The service's own request id, so a failure can be matched against its logs.
fn upstream_request_id(body: &str) -> Option<String> {
    let parsed = parse_upstream_body(body)?;
    ["request_id", "requestId", "trace_id", "traceId"]
        .into_iter()
        .find_map(|key| {
            parsed
                .get(key)
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|text| !text.is_empty())
                .map(str::to_string)
        })
}

fn parse_upstream_body(body: &str) -> Option<Value> {
    let decoded = super::sign::decode_server_response(body).unwrap_or_else(|_| body.to_string());
    serde_json::from_str::<Value>(&decoded)
        .or_else(|_| serde_json::from_str::<Value>(body))
        .ok()
}

/// Reread this machine's official store for whichever subscription this request
/// belongs to. Returns the outcome word that goes into the diagnostic record.
///
/// Deliberately the REREAD path, not a refresh call: the official clients'
/// refresh tokens are one-time, so spending ours would invalidate the live
/// credential the user's own IDE is holding.
async fn reread_local_credential(source: BridgeSource) -> &'static str {
    let outcome = match source {
        BridgeSource::Qoder => super::auth::import_local(),
        BridgeSource::QwenWork => super::super::qwenwork::import_local(),
        BridgeSource::QoderWork => super::super::qoderwork::import_local(),
    };
    match outcome {
        Ok(result) if result.get("ok") == Some(&json!(true)) => "ok",
        Ok(_) => "no_local_credential",
        Err(_) => "failed",
    }
}

/// Split a failed upstream answer by who wrote it, WITHOUT touching the status.
/// Qoder's service answers JSON on every outcome, so a non-JSON body was written
/// by something in front of it: the Aliyun edge serves an HTML page under its own
/// status. Both keep that status, because the status is the only thing that tells
/// the engine's retry policy whether another attempt can ever succeed. An edge
/// 5xx is a transient infrastructure fault and stays retryable on its own merit;
/// an edge 403/405 is a WAF verdict on the request CONTENT and is permanent, so
/// re-labelling it as 502 turned a dead end into minutes of silent retrying that
/// could never land.
///
/// What the caller sees is the status and the SHAPE; the body itself is not
/// repeated into the message. A gateway page is a raw error document — it can
/// carry internal host names, request ids and echoed headers — and it has no
/// business in a conversation. The full body goes to the failure log instead
/// ([`diagnostics`]), where a reader who needs it can find it.
fn classify_upstream_failure(
    status: reqwest::StatusCode,
    body: &str,
    source_label: &str,
) -> UpstreamFailure {
    let trimmed = body.trim();
    if let Ok(parsed) = serde_json::from_str::<Value>(trimmed) {
        let message = service_error_message(&parsed);
        return UpstreamFailure {
            status: status.as_u16(),
            payload: json!({ "error": { "type": "upstream_error", "message": message } }),
        };
    }
    let shape = if trimmed.is_empty() {
        "empty body"
    } else if html_title(trimmed).is_some() {
        "HTML error page"
    } else {
        "non-JSON body"
    };
    let message = format!(
        "{source_label} gateway refused the request before the model saw it: HTTP {} {} ({shape}).",
        status.as_u16(),
        reason_phrase(status.as_u16()),
    );
    UpstreamFailure {
        status: status.as_u16(),
        payload: json!({ "error": { "type": "upstream_gateway_error", "message": message } }),
    }
}

/// The readable field out of a service error envelope, whichever shape it
/// arrived in: OpenAI's `error.message`, or Qoder's `message` / `msgInfo`.
///
/// This one IS repeated to the caller, because it is the service's own verdict
/// on the request rather than a transport document — and the engine's
/// classification reads it too (a 403 naming a model is what rotates to a
/// fallback model). When the envelope carries none, the status stands alone.
fn service_error_message(parsed: &Value) -> String {
    [
        parsed.get("error").and_then(|error| error.get("message")),
        parsed.get("error").filter(|error| error.is_string()),
        parsed.get("message"),
        parsed.get("msgInfo"),
    ]
    .into_iter()
    .flatten()
    .filter_map(Value::as_str)
    .map(str::trim)
    .find(|text| !text.is_empty())
    .map(|text| single_line_preview(text, UPSTREAM_DETAIL_CHARS))
    .unwrap_or_else(|| "The provider refused the request.".to_string())
}

/// The `<title>` of an HTML body, which on a gateway page is the status it
/// decided on and the only part of the page worth repeating.
fn html_title(body: &str) -> Option<String> {
    let lower = body.to_ascii_lowercase();
    let open = lower.find("<title>")? + "<title>".len();
    let close = lower[open..].find("</title>")? + open;
    let title = body[open..close].trim();
    (!title.is_empty()).then(|| single_line_preview(title, 80))
}

/// Collapse a body into a single line of at most `limit` characters, so no error
/// surface has to render a whole page of markup.
fn single_line_preview(text: &str, limit: usize) -> String {
    let mut out = String::new();
    let mut kept = 0usize;
    let mut pending_space = false;
    for ch in text.chars() {
        if ch.is_whitespace() {
            pending_space = !out.is_empty();
            continue;
        }
        if kept >= limit {
            out.push('…');
            break;
        }
        if pending_space {
            out.push(' ');
            pending_space = false;
            kept += 1;
        }
        out.push(ch);
        kept += 1;
    }
    out
}

fn consume_aggregate(
    inner: String,
    chunks: &mut Vec<Value>,
    stream_error: &mut Option<UpstreamFailure>,
    done: &mut bool,
    normalizer: &mut ToolCallIndexNormalizer,
) {
    if inner == "[DONE]" {
        *done = true;
        return;
    }
    if let Ok(mut parsed) = serde_json::from_str::<Value>(&inner) {
        if parsed.get("error").is_some_and(|error| !error.is_null()) {
            let status = parsed
                .get("error_code")
                .and_then(Value::as_u64)
                .and_then(|status| u16::try_from(status).ok())
                .filter(|status| (400..600).contains(status))
                .unwrap_or(502);
            *stream_error = Some(UpstreamFailure {
                status,
                payload: parsed,
            });
            *done = true;
            return;
        }
        normalizer.normalize(&mut parsed);
        chunks.push(parsed);
    }
}

// ------------------------------------------- OpenAI -> Qoder request translation

fn validate_fast_mode(body: &Value, model_info: Option<&Value>) -> Result<(), &'static str> {
    let Some(requested) = body.get("fast_mode") else {
        return Ok(());
    };
    let enabled = requested.as_bool().ok_or("Fast mode must be a boolean")?;
    if enabled
        && model_info
            .and_then(super::catalog::fast_mode_switch)
            .is_none()
    {
        return Err("Fast mode is unavailable for the selected model");
    }
    Ok(())
}

/// openAiToQoderInferBody mirror. Runtime knobs follow the Qoder desktop
/// client's own contract: the chosen context window and reasoning effort ride
/// BOTH on `model_config` and inside `parameters`; thinking-off is the literal
/// effort `"none"`, not an omitted field.
pub(crate) fn openai_to_qoder_infer_body(
    body: &Value,
    model_key: &str,
    model_info: Option<&Value>,
    thinking_level: Option<&str>,
) -> Value {
    let rid = super::uuid_v4();
    let messages: Vec<Value> = body
        .get("messages")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|mut message| {
            // The native wire has one trusted system-instruction channel.
            if message.get("role").and_then(Value::as_str) == Some("developer") {
                message["role"] = json!("system");
            }
            message
        })
        .map(with_assistant_content_present)
        .collect();
    let system_msg = messages
        .iter()
        .filter(|m| m.get("role").and_then(Value::as_str) == Some("system"))
        .map(|m| content_to_text(m.get("content").unwrap_or(&Value::Null)))
        .collect::<Vec<_>>()
        .join("\n\n");
    let last_user_text = messages
        .iter()
        .rev()
        .find(|m| m.get("role").and_then(Value::as_str) == Some("user"))
        .map(|m| content_to_text(m.get("content").unwrap_or(&Value::Null)))
        .unwrap_or_default();
    let max_tokens = ["max_completion_tokens", "max_tokens"]
        .into_iter()
        .find_map(|key| body.get(key).and_then(Value::as_u64).filter(|n| *n > 0))
        .or_else(|| {
            model_info
                .and_then(|model| model.get("max_output_tokens"))
                .and_then(Value::as_u64)
                .filter(|tokens| *tokens > 0)
        })
        .unwrap_or(4096);
    let is_reasoning = model_info
        .and_then(|m| m.get("is_reasoning"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    // Per-request intent takes priority over the composer's default, especially
    // for detached summaries that disable thinking while the main turn uses it.
    let request_effort =
        if body.pointer("/thinking/type").and_then(Value::as_str) == Some("disabled") {
            Some("off")
        } else {
            body.get("reasoning_effort")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .map(|value| if value == "none" { "off" } else { value })
        };
    let effort = resolve_qoder_effort(model_info, request_effort.or(thinking_level));
    let context_length = resolve_qoder_context_length(model_info);

    let display_name = model_info
        .and_then(|model| model.get("display_name"))
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .unwrap_or(model_key);
    let format = model_info
        .and_then(|model| model.get("format"))
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .unwrap_or("openai");
    let source = model_info
        .and_then(|model| model.get("source"))
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .unwrap_or("system");
    let is_vl = model_info
        .and_then(|model| model.get("is_vl"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let mut model_config = json!({
        "key": model_key,
        "display_name": display_name,
        "model": "",
        "format": format,
        "is_vl": is_vl,
        "is_reasoning": is_reasoning,
        "api_key": "",
        "url": "",
        "source": source,
        "max_input_tokens": context_length,
        "max_output_tokens": max_tokens,
    });
    if let Some(switches) = model_info.and_then(|model| model.get("function_switches")) {
        model_config["function_switches"] = switches.clone();
    }
    if !effort.is_empty() {
        model_config["reasoning_effort"] = json!(effort);
    }

    let mut parameters = json!({ "max_tokens": max_tokens, "context_length": context_length });
    if !effort.is_empty() {
        parameters["reasoning_effort"] = json!(effort);
    }

    let mut result = json!({
        "request_id": rid,
        "request_set_id": rid,
        "chat_record_id": rid,
        "session_id": super::uuid_v4(),
        "stream": true,
        "chat_task": "FREE_INPUT",
        "chat_context": {
            "text": last_user_text,
            "features": [],
            "extra": {
                "context": [],
                "modelConfig": { "key": model_key, "is_reasoning": is_reasoning },
                "originalContent": last_user_text,
            },
            "chatPrompt": "",
            "imageUrls": Value::Null,
        },
        "is_reply": true,
        "is_retry": false,
        "source": 1,
        "version": "3",
        "agent_id": "agent_common",
        "task_id": "common",
        "aliyun_user_type": "",
        "model_config": model_config,
        "system": system_msg,
        "messages": messages,
        "tools": sanitize_qoder_tools(body.get("tools").unwrap_or(&Value::Null)),
        "parameters": parameters,
    });
    if let Some(enabled) = body.get("fast_mode").and_then(Value::as_bool) {
        if model_info
            .and_then(super::catalog::fast_mode_switch)
            .is_some()
        {
            result["feature_switches"] = json!({"highspeed": enabled.to_string()});
        }
    }
    result
}

/// An assistant message ALWAYS carries a `content` key upstream, empty string
/// included. Chat Completions makes the key optional next to `tool_calls`, so a
/// tool call the model emitted with no preamble text serialises without it — and
/// Qoder's OpenAI→Anthropic conversion drops that whole message, `tool_calls`
/// included. The following `tool_result` is then orphaned and the upstream
/// rejects the entire request:
///
/// > messages.0.content.1: unexpected `tool_use_id` found in `tool_result`
/// > blocks: … Each `tool_result` block must have a corresponding `tool_use`
/// > block in the previous message.
///
/// That rejection is terminal for the SESSION, not for one turn: the offending
/// assistant message stays in the history, so every later turn rebuilds the same
/// invalid request. Backfilling the key belongs here — the omission is correct on
/// the Chat Completions wire, and this is the Qoder dialect the bridge exists to
/// speak.
fn with_assistant_content_present(mut message: Value) -> Value {
    if message.get("role").and_then(Value::as_str) != Some("assistant") {
        return message;
    }
    if message.get("content").is_none_or(Value::is_null) {
        if let Some(object) = message.as_object_mut() {
            object.insert("content".to_string(), Value::String(String::new()));
        }
    }
    message
}

fn content_to_text(content: &Value) -> String {
    match content {
        Value::String(text) => text.clone(),
        Value::Array(parts) => parts
            .iter()
            .map(|part| match part {
                Value::String(text) => text.clone(),
                other => other
                    .get("text")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
            })
            .filter(|text| !text.is_empty())
            .collect::<Vec<_>>()
            .join(""),
        _ => String::new(),
    }
}

/// Klaus pins the largest advertised context window instead of exposing Qoder's
/// window picker; falls back to `max_input_tokens`, then the default.
pub(crate) fn resolve_qoder_context_length(model_info: Option<&Value>) -> i64 {
    if let Some(info) = model_info {
        if let Some(tokens) = info.get("contextTokens").and_then(Value::as_array) {
            // parse_model_scenes stores these ascending, so the last is largest.
            if let Some(last) = tokens.iter().filter_map(Value::as_i64).next_back() {
                return last;
            }
        }
        if let Some(max) = info.get("max_input_tokens").and_then(Value::as_i64) {
            return max;
        }
    }
    QODER_DEFAULT_CONTEXT_TOKENS
}

/// resolveQoderEffort mirror: thinking-off is the literal `"none"`; thinking-on
/// with no advertised efforts sends no effort at all; the top tier prefers `max`
/// then `xhigh`; anything else degrades to the nearest lower advertised tier.
pub(crate) fn resolve_qoder_effort(model_info: Option<&Value>, level: Option<&str>) -> String {
    let advertised: Vec<String> = model_info
        .and_then(|m| m.get("efforts"))
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|v| {
                    v.as_str()
                        .map(str::trim)
                        .filter(|effort| super::catalog::engine_effort_level(effort).is_some())
                        .map(str::to_ascii_lowercase)
                })
                .collect()
        })
        .unwrap_or_default();
    let can_disable = model_info
        .and_then(|m| m.get("canDisableThinking"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let default_effort = model_info
        .and_then(|m| m.get("defaultEffort"))
        .and_then(Value::as_str)
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| advertised.contains(value))
        .unwrap_or_default();

    if level == Some("off") {
        if can_disable {
            return "none".to_string();
        }
        return if !advertised.is_empty() {
            if default_effort.is_empty() {
                advertised[0].clone()
            } else {
                default_effort.to_string()
            }
        } else {
            String::new()
        };
    }
    if advertised.is_empty() {
        return String::new();
    }
    let desired = level
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| default_effort.to_string());
    if desired.is_empty() {
        return String::new();
    }
    if desired == "ultra" {
        for candidate in ["xhigh", "high", "medium", "low", "minimal", "max"] {
            if advertised.iter().any(|effort| effort == candidate) {
                return candidate.to_string();
            }
        }
        return String::new();
    }
    if desired == "max" {
        for candidate in ["max", "xhigh", "high"] {
            if advertised.iter().any(|effort| effort == candidate) {
                return candidate.to_string();
            }
        }
    }
    if advertised.contains(&desired) {
        return desired;
    }
    if desired == "low" && advertised.iter().any(|effort| effort == "minimal") {
        return "minimal".to_string();
    }
    let ladder = ["low", "medium", "high", "xhigh", "max"];
    if let Some(pos) = ladder.iter().position(|l| *l == desired) {
        for index in (0..pos).rev() {
            if advertised.iter().any(|effort| effort == ladder[index]) {
                return ladder[index].to_string();
            }
        }
    }
    if default_effort.is_empty() {
        advertised[0].clone()
    } else {
        default_effort.to_string()
    }
}

/// Rewrite tool schemas so every Qoder upstream validator accepts them. Two
/// validators disagree about combinators: OpenAI-flavored ones require the
/// function-parameter root to be `type: "object"` (a root that lost its type to
/// a combinator rewrite reads as `type: null` and bounces the whole request),
/// while Moonshot-flavored ones reject a `type` sitting NEXT TO `anyOf`/`oneOf`
/// and want it pushed down into each branch. The root satisfies both by
/// carrying no combinator at all: branch constraints are merged into the root
/// (their `required` lists are dropped, so the union stays permissive) and the
/// root type is pinned to `object`. Nested nodes keep the push-down rewrite,
/// which is semantics-preserving there and what Moonshot asks for.
pub(crate) fn sanitize_qoder_tools(tools: &Value) -> Value {
    let Some(list) = tools.as_array() else {
        return json!([]);
    };
    let out: Vec<Value> = list
        .iter()
        .map(|tool| {
            let parameters = tool.get("function").and_then(|f| f.get("parameters"));
            match parameters {
                Some(parameters) if parameters.is_object() => {
                    let mut rewritten = tool.clone();
                    rewritten["function"]["parameters"] =
                        sanitize_root_schema(push_type_into_combinators(parameters));
                    rewritten
                }
                _ => tool.clone(),
            }
        })
        .collect();
    json!(out)
}

/// Root-level pass: drop `anyOf`/`oneOf`, fold each object branch's properties
/// into the root, and pin `type: "object"`. Branches that declare a non-object
/// type make the root typeless rather than lying about it.
fn sanitize_root_schema(node: Value) -> Value {
    let Value::Object(mut root) = node else {
        return node;
    };
    let mut branches: Vec<Value> = Vec::new();
    for combinator in ["anyOf", "oneOf"] {
        if let Some(Value::Array(items)) = root.remove(combinator) {
            branches.extend(items);
        }
    }
    if branches.is_empty() {
        root.insert("type".to_string(), json!("object"));
        return Value::Object(root);
    }
    let mut all_object = true;
    for branch in branches {
        let Value::Object(branch) = branch else {
            continue;
        };
        let is_object_branch = branch
            .get("type")
            .is_none_or(|declared| declared == &json!("object"));
        if !is_object_branch {
            all_object = false;
            continue;
        }
        if let Some(Value::Object(props)) = branch.get("properties") {
            let entry = root
                .entry("properties")
                .or_insert_with(|| Value::Object(Map::new()));
            if let Value::Object(target) = entry {
                for (key, value) in props {
                    target.entry(key.clone()).or_insert_with(|| value.clone());
                }
            }
        }
    }
    if all_object {
        root.insert("type".to_string(), json!("object"));
    } else {
        root.remove("type");
    }
    Value::Object(root)
}

fn push_type_into_combinators(node: &Value) -> Value {
    match node {
        Value::Array(items) => Value::Array(items.iter().map(push_type_into_combinators).collect()),
        Value::Object(map) => {
            let mut out = Map::new();
            for (key, value) in map {
                out.insert(key.clone(), push_type_into_combinators(value));
            }
            for combinator in ["anyOf", "oneOf"] {
                let has_type = out.contains_key("type");
                if !has_type {
                    continue;
                }
                let Some(Value::Array(branches)) = out.get(combinator).cloned() else {
                    continue;
                };
                let parent_type = out.get("type").cloned().unwrap_or(Value::Null);
                let rewritten: Vec<Value> = branches
                    .into_iter()
                    .map(|item| {
                        if item.is_object() && item.get("type").is_none() {
                            let mut merged = Map::new();
                            merged.insert("type".to_string(), parent_type.clone());
                            if let Value::Object(inner) = &item {
                                for (key, value) in inner {
                                    merged.insert(key.clone(), value.clone());
                                }
                            }
                            Value::Object(merged)
                        } else {
                            item
                        }
                    })
                    .collect();
                out.insert(combinator.to_string(), Value::Array(rewritten));
                out.remove("type");
            }
            Value::Object(out)
        }
        other => other.clone(),
    }
}

// ------------------------------------------------------------- SSE envelope

/// Unwrap Qoder's SSE envelope. Each `\n\n`-delimited block wraps a
/// `{"body":"<stringified OpenAI chunk | [DONE]>","statusCode":"OK"}`; this
/// yields the inner OpenAI chunk JSON (or the literal `[DONE]`), buffering
/// across network reads.
struct SseUnwrapper {
    buf: Vec<u8>,
}

impl SseUnwrapper {
    fn new() -> Self {
        Self { buf: Vec::new() }
    }

    fn push(&mut self, bytes: &[u8]) -> Vec<String> {
        self.buf.extend_from_slice(bytes);
        let mut out = Vec::new();
        while let Some(pos) = find_subslice(&self.buf, b"\n\n") {
            let block: Vec<u8> = self.buf.drain(..pos + 2).collect();
            let block = String::from_utf8_lossy(&block[..block.len() - 2]).to_string();
            if let Some(inner) = parse_qoder_envelope_block(&block) {
                out.push(inner);
            }
        }
        out
    }

    fn finish(&mut self) -> Vec<String> {
        if self.buf.is_empty() {
            return Vec::new();
        }
        let block = String::from_utf8_lossy(&self.buf).to_string();
        self.buf.clear();
        parse_qoder_envelope_block(&block).into_iter().collect()
    }
}

/// parseQoderEnvelopeBlock mirror. A `finish` event is dropped; a bare `error`
/// event becomes an OpenAI stream error; a non-OK envelope folds into an OpenAI
/// error chunk; otherwise the inner OpenAI chunk (or `[DONE]`) is returned. When
/// the `data:` payload is an encrypted envelope, it is decrypted through the
/// wasm module before parsing.
pub(crate) fn parse_qoder_envelope_block(block: &str) -> Option<String> {
    if block.trim().is_empty() {
        return None;
    }
    let mut data_parts: Vec<String> = Vec::new();
    let mut is_finish = false;
    let mut is_error = false;
    for line in block.split('\n') {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if let Some(event) = line.strip_prefix("event:") {
            match event.trim() {
                "finish" => is_finish = true,
                "error" => is_error = true,
                _ => {}
            }
        } else if let Some(data) = line.strip_prefix("data:") {
            data_parts.push(data.trim_start().to_string());
        }
    }
    if is_finish {
        return None;
    }
    if data_parts.is_empty() {
        return if is_error {
            Some(
                json!({ "error": { "type": "upstream_error", "message": "Qoder upstream aborted the stream (event:error)" } })
                    .to_string(),
            )
        } else {
            None
        };
    }
    let data_string = data_parts.join("\n");
    // `Encode=1` governs the REQUEST body only: the answer stays a plain envelope
    // (pinned by the live probe at the bottom of this file). An unparseable block
    // therefore means the upstream changed the envelope, not that an obfuscated
    // one needs decoding — say so instead of dropping the chunk and stalling the
    // turn on silence.
    let outer: Value = match serde_json::from_str(&data_string) {
        Ok(value) => value,
        Err(error) => {
            let detail: String = data_string.chars().take(200).collect();
            eprintln!(
                "[QoderBridge] SSE data block is not JSON ({error}); path={} block={detail}",
                super::catalog::infer_path()
            );
            return Some(
                json!({ "error": { "type": "upstream_error", "message": format!("Qoder upstream sent an unrecognised SSE envelope: {error}") } })
                    .to_string(),
            );
        }
    };
    let inner = outer.get("body").and_then(Value::as_str);
    let status_code = outer
        .get("statusCode")
        .and_then(Value::as_str)
        .unwrap_or("");
    if !status_code.is_empty() && status_code != "OK" {
        return Some(qoder_envelope_error_chunk(status_code, inner));
    }
    let inner = inner?;
    if is_error {
        return Some(qoder_envelope_error_chunk("ERROR", Some(inner)));
    }
    Some(if inner == "[DONE]" {
        "[DONE]".to_string()
    } else {
        inner.to_string()
    })
}

/// Map a Qoder envelope status word to its HTTP number. The gateway has been
/// observed sending both canonical enum names (`TOO_MANY_REQUESTS`) and
/// title-cased reason phrases (`Payment Required`), so the word is normalized
/// before matching; an unmapped word still folds to 502 so the turn fails
/// visibly instead of stalling on silence.
fn qoder_envelope_http_status(status_code: &str) -> u16 {
    let normalized: String = status_code
        .chars()
        .filter_map(|c| {
            if c.is_alphanumeric() {
                Some(c.to_ascii_uppercase())
            } else {
                None
            }
        })
        .collect();
    match normalized.as_str() {
        "BADREQUEST" => 400,
        "UNAUTHORIZED" => 401,
        "PAYMENTREQUIRED" => 402,
        "FORBIDDEN" => 403,
        "NOTFOUND" => 404,
        "TOOMANYREQUESTS" => 429,
        "INTERNALSERVERERROR" => 500,
        "SERVICEUNAVAILABLE" => 503,
        _ => 502,
    }
}

/// Select structured error records without inspecting normal generated content.
fn explicit_error(value: Value) -> Option<Value> {
    if let Some(error) = value.get("error") {
        return ["message", "type", "code"]
            .iter()
            .any(|field| {
                error
                    .get(*field)
                    .and_then(Value::as_str)
                    .is_some_and(|text| !text.trim().is_empty())
            })
            .then(|| error.clone());
    }
    if ["choices", "delta", "output"]
        .iter()
        .any(|field| value.get(*field).is_some())
    {
        return None;
    }
    value
        .get("message")
        .and_then(Value::as_str)
        .is_some_and(|message| !message.trim().is_empty())
        .then_some(value)
}

/// Failure details may contain either JSON or a complete nested SSE response.
/// Only explicit error records are selected; normal output and tool arguments
/// are never searched for error-like text.
fn nested_error_detail(details: &Value) -> Option<Value> {
    let Some(raw) = details.as_str() else {
        return explicit_error(details.clone());
    };
    if let Ok(value) = serde_json::from_str::<Value>(raw) {
        return explicit_error(value);
    }
    let normalized = raw.replace("\r\n", "\n").replace('\r', "\n");
    normalized.split("\n\n").find_map(|block| {
        let data = block
            .lines()
            .filter_map(|line| line.strip_prefix("data:"))
            .map(|line| line.strip_prefix(' ').unwrap_or(line))
            .collect::<Vec<_>>()
            .join("\n");
        serde_json::from_str::<Value>(&data)
            .ok()
            .and_then(explicit_error)
    })
}

fn error_string<'a>(candidates: &[Option<&'a Value>], field: &str) -> Option<&'a str> {
    candidates
        .iter()
        .flatten()
        .filter_map(|value| value.get(field).and_then(Value::as_str))
        .find(|text| !text.trim().is_empty())
}

/// Fold a failed native envelope into a standard provider error without losing
/// the actionable nested message or the gateway's request identifier.
fn qoder_envelope_error_chunk(status_code: &str, body: Option<&str>) -> String {
    let detail: Option<Value> = body.and_then(|raw| serde_json::from_str::<Value>(raw).ok());
    let nested = detail
        .as_ref()
        .and_then(|d| d.get("details"))
        .and_then(nested_error_detail);
    let wrapped = detail
        .as_ref()
        .and_then(|value| value.get("error"))
        .filter(|error| error.is_object());
    let candidates = [nested.as_ref(), wrapped, detail.as_ref()];
    let message = error_string(&candidates, "message")
        .map(str::to_string)
        .unwrap_or_else(|| {
            body.unwrap_or("Qoder upstream error")
                .chars()
                .take(300)
                .collect()
        });
    let http_status = qoder_envelope_http_status(status_code);
    let mut error = Map::new();
    error.insert(
        "type".to_string(),
        json!(error_string(&candidates, "type").unwrap_or("upstream_error")),
    );
    if message
        .trim()
        .eq_ignore_ascii_case("Image preprocessing failed")
    {
        error.insert("code".to_string(), json!("image_preprocessing_failed"));
    } else if let Some(code) = error_string(&candidates, "code") {
        error.insert("code".to_string(), json!(code));
    }
    error.insert(
        "message".to_string(),
        json!(format!("Qoder upstream {status_code}: {message}")),
    );
    if let Some(request_id) =
        error_string(&[detail.as_ref(), wrapped, nested.as_ref()], "request_id")
    {
        error.insert("request_id".to_string(), json!(request_id));
    }
    json!({ "error_code": http_status, "error": Value::Object(error) }).to_string()
}

// --------------------------------------------------- tool-call index normalizer

/// createQoderToolCallIndexNormalizer mirror: Qoder's Claude-format models stream
/// every parallel tool call with `index: 0`; reassign a strictly increasing index
/// per announced call id and route continuation fragments to the last call
/// announced at that upstream index.
struct ToolCallIndexNormalizer {
    next_index: i64,
    index_by_call_id: HashMap<String, i64>,
    last_at_upstream: HashMap<i64, i64>,
}

impl ToolCallIndexNormalizer {
    fn new() -> Self {
        Self {
            next_index: 0,
            index_by_call_id: HashMap::new(),
            last_at_upstream: HashMap::new(),
        }
    }

    fn normalize(&mut self, chunk: &mut Value) {
        let Some(tool_calls) = chunk
            .pointer_mut("/choices/0/delta/tool_calls")
            .and_then(Value::as_array_mut)
        else {
            return;
        };
        for call in tool_calls.iter_mut() {
            if !call.is_object() {
                continue;
            }
            let upstream = call.get("index").and_then(Value::as_i64).unwrap_or(0);
            let id = call
                .get("id")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .map(str::to_string);
            match id {
                Some(id) => {
                    let assigned = if let Some(existing) = self.index_by_call_id.get(&id) {
                        *existing
                    } else {
                        let assigned = self.next_index;
                        self.next_index += 1;
                        self.index_by_call_id.insert(id, assigned);
                        assigned
                    };
                    self.last_at_upstream.insert(upstream, assigned);
                    call["index"] = json!(assigned);
                }
                None => {
                    let assigned = self
                        .last_at_upstream
                        .get(&upstream)
                        .copied()
                        .unwrap_or(upstream);
                    call["index"] = json!(assigned);
                }
            }
        }
    }
}

fn normalize_qoder_chunk_string(chunk: &str, normalizer: &mut ToolCallIndexNormalizer) -> String {
    if !chunk.contains("\"tool_calls\"") {
        return chunk.to_string();
    }
    match serde_json::from_str::<Value>(chunk) {
        Ok(mut parsed) => {
            normalizer.normalize(&mut parsed);
            parsed.to_string()
        }
        Err(_) => chunk.to_string(),
    }
}

/// aggregateOpenAiChunks mirror: fold a chunk stream into one non-stream
/// `chat.completion` for the `stream: false` path.
fn aggregate_openai_chunks(chunks: &[Value], model: &str) -> Value {
    let mut id = format!(
        "chatcmpl-{}",
        super::uuid_v4()
            .replace('-', "")
            .chars()
            .take(24)
            .collect::<String>()
    );
    let mut content = String::new();
    let mut reasoning = String::new();
    let mut finish_reason = json!("stop");
    let mut usage: Option<Value> = None;
    let mut tool_calls: Vec<Option<Value>> = Vec::new();

    for chunk in chunks {
        if let Some(chunk_id) = chunk
            .get("id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
        {
            id = chunk_id.to_string();
        }
        if let Some(delta) = chunk.pointer("/choices/0/delta") {
            if let Some(text) = delta.get("content").and_then(Value::as_str) {
                content.push_str(text);
            }
            if let Some(text) = delta.get("reasoning_content").and_then(Value::as_str) {
                reasoning.push_str(text);
            }
            if let Some(calls) = delta.get("tool_calls").and_then(Value::as_array) {
                for call in calls {
                    let index = call
                        .get("index")
                        .and_then(Value::as_i64)
                        .map(|value| value as usize)
                        .unwrap_or(tool_calls.len());
                    if index >= tool_calls.len() {
                        tool_calls.resize(index + 1, None);
                    }
                    if tool_calls[index].is_none() {
                        tool_calls[index] = Some(json!({
                            "id": call.get("id").cloned().unwrap_or(Value::Null),
                            "type": "function",
                            "function": { "name": "", "arguments": "" },
                        }));
                    }
                    let slot = tool_calls[index].as_mut().unwrap();
                    if let Some(call_id) = call
                        .get("id")
                        .and_then(Value::as_str)
                        .filter(|v| !v.is_empty())
                    {
                        slot["id"] = json!(call_id);
                    }
                    if let Some(name) = call
                        .pointer("/function/name")
                        .and_then(Value::as_str)
                        .filter(|v| !v.is_empty())
                    {
                        slot["function"]["name"] = json!(name);
                    }
                    if let Some(args) = call.pointer("/function/arguments").and_then(Value::as_str)
                    {
                        let current = slot["function"]["arguments"]
                            .as_str()
                            .unwrap_or("")
                            .to_string();
                        slot["function"]["arguments"] = json!(format!("{current}{args}"));
                    }
                }
            }
        }
        if let Some(reason) = chunk.pointer("/choices/0/finish_reason") {
            if !reason.is_null() && reason.as_str() != Some("") {
                finish_reason = reason.clone();
            }
        }
        if let Some(chunk_usage) = chunk.get("usage") {
            if !chunk_usage.is_null() {
                usage = Some(chunk_usage.clone());
            }
        }
    }

    let mut message = json!({
        "role": "assistant",
        "content": if content.is_empty() { Value::Null } else { json!(content) },
    });
    if !reasoning.is_empty() {
        message["reasoning_content"] = json!(reasoning);
    }
    let collected: Vec<Value> = tool_calls.into_iter().flatten().collect();
    if !collected.is_empty() {
        message["tool_calls"] = json!(collected);
    }
    let mut out = json!({
        "id": id,
        "object": "chat.completion",
        "created": chrono::Utc::now().timestamp(),
        "model": model,
        "choices": [{ "index": 0, "message": message, "finish_reason": finish_reason }],
    });
    if let Some(usage) = usage {
        out["usage"] = usage;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The bridge endpoint is PROCESS-global: starting one binds a port, and
    /// retiring one invalidates whatever URL a concurrent test just obtained.
    /// Every test that touches the endpoint holds this, so they serialize
    /// instead of racing each other's listener.
    fn endpoint_lock() -> &'static tokio::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
    }

    #[test]
    fn descriptor_exhaustion_does_not_end_the_accept_loop() {
        // The incident: `accept()` answering errno 24 while the listener stayed
        // perfectly bound. Treating it as fatal retired the subscription for the
        // rest of the process's life.
        #[cfg(unix)]
        {
            for errno in [libc::EMFILE, libc::ENFILE, libc::ENOBUFS, libc::ENOMEM] {
                assert!(
                    accept_failure_is_transient(&std::io::Error::from_raw_os_error(errno)),
                    "errno {errno} must be retried, not fatal"
                );
            }
        }
        assert!(accept_failure_is_transient(&std::io::Error::from(
            std::io::ErrorKind::Interrupted
        )));
        assert!(accept_failure_is_transient(&std::io::Error::from(
            std::io::ErrorKind::ConnectionAborted
        )));
    }

    #[test]
    fn an_unusable_listener_is_fatal_so_the_endpoint_can_be_rebound() {
        // The other band: nothing the backlog can hand over again. Retrying these
        // forever would spin instead of letting `ensure_started` bind a new port.
        assert!(!accept_failure_is_transient(&std::io::Error::from(
            std::io::ErrorKind::InvalidInput
        )));
        #[cfg(unix)]
        assert!(!accept_failure_is_transient(
            &std::io::Error::from_raw_os_error(libc::EBADF)
        ));
    }

    /// One test for the whole endpoint lifecycle: the bridge state is
    /// process-global, so splitting this across tests would let them race each
    /// other's endpoint.
    #[tokio::test]
    async fn a_retired_endpoint_is_replaced_and_keeps_the_pushed_tier() {
        let _endpoint = endpoint_lock().lock().await;
        set_thinking_level_for(BridgeSource::QwenWork, Some("max".to_string()));
        let (qwenwork_first, qwenwork_second) = tokio::join!(
            ensure_started_for(BridgeSource::QwenWork),
            ensure_started_for(BridgeSource::QwenWork)
        );
        assert_eq!(
            qwenwork_first.expect("first QwenWork bridge start"),
            qwenwork_second.expect("second QwenWork bridge start"),
            "concurrent starters must converge on one endpoint"
        );
        let qwenwork_tier = state()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .running
            .get(&BridgeSource::QwenWork)
            .expect("QwenWork running")
            .thinking
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        assert_eq!(
            qwenwork_tier.as_deref(),
            Some("max"),
            "a tier pushed before startup must reach the first endpoint"
        );

        let (first, first_token) = ensure_started().await.expect("bridge starts");
        let (again, again_token) = ensure_started().await.expect("bridge is idempotent");
        assert_eq!(first, again, "a live endpoint is reused");
        assert_eq!(first_token, again_token);

        set_thinking_level(Some("high".to_string()));
        // Retire it exactly the way the accept loop does on a fatal error.
        state()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .running
            .get(&BridgeSource::Qoder)
            .expect("running")
            .serving
            .store(false, Ordering::Release);

        let (second, second_token) = ensure_started().await.expect("bridge rebinds");
        assert_ne!(
            first, second,
            "a retired endpoint must not be handed out again: every session would \
             dial a port with no listener"
        );
        assert_ne!(
            first_token, second_token,
            "a new endpoint gets a new bridge token"
        );
        let tier = state()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .running
            .get(&BridgeSource::Qoder)
            .expect("running")
            .thinking
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        assert_eq!(
            tier.as_deref(),
            Some("high"),
            "rebinding must not silently drop the tier the host pushed"
        );
    }

    fn model(efforts: &[&str], default: Option<&str>, can_disable: bool, context: &[i64]) -> Value {
        let mut m = json!({ "key": "m", "is_reasoning": true });
        if !efforts.is_empty() {
            m["efforts"] = json!(efforts);
        }
        if let Some(default) = default {
            m["defaultEffort"] = json!(default);
        }
        if can_disable {
            m["canDisableThinking"] = json!(true);
        }
        if !context.is_empty() {
            m["contextTokens"] = json!(context);
        }
        m
    }

    #[test]
    fn maps_messages_system_and_model_into_infer_payload() {
        let body = json!({
            "model": "auto",
            "max_tokens": 128,
            "messages": [
                { "role": "system", "content": "be terse" },
                { "role": "user", "content": "hello" },
            ],
            "tools": [{ "type": "function", "function": { "name": "t", "parameters": { "type": "object" } } }],
        });
        let out = openai_to_qoder_infer_body(&body, "auto", None, Some("medium"));
        assert_eq!(out["agent_id"], json!("agent_common"));
        assert_eq!(out["stream"], json!(true));
        assert_eq!(out["chat_task"], json!("FREE_INPUT"));
        assert_eq!(out["version"], json!("3"));
        assert_eq!(out["system"], json!("be terse"));
        assert_eq!(out["chat_context"]["text"], json!("hello"));
        assert_eq!(out["model_config"]["key"], json!("auto"));
        assert_eq!(out["model_config"]["format"], json!("openai"));
        assert_eq!(out["parameters"]["max_tokens"], json!(128));
        assert_eq!(out["messages"].as_array().unwrap().len(), 2);
        assert_eq!(out["tools"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn preserves_complete_mode_instructions_in_native_system_channel() {
        for mode in [
            klaus_agent_core::planning::CollaborationMode::Default,
            klaus_agent_core::planning::CollaborationMode::Plan,
        ] {
            let part = klaus_agent_core::planning::instructions::instruction_part(
                &klaus_agent_core::engine::session::resolve_config_default(),
                mode,
            );
            let text = part.text.try_to_utf8().unwrap();
            let body = json!({"messages":[
                {"role":"system","content":"Stable instructions"},
                {"role":"user","content":"Continue the saved task"},
                {"role":"developer","content":text},
            ]});
            let out = openai_to_qoder_infer_body(&body, "auto", None, None);
            assert_eq!(out["system"], format!("Stable instructions\n\n{text}"));
            assert_eq!(out["messages"][2], json!({"role":"system","content":text}));
            assert_eq!(out["messages"][1], body["messages"][1]);
        }
    }

    #[test]
    fn summary_requests_keep_their_output_budget_and_reasoning_override() {
        let info = model(&["low", "medium", "high"], Some("high"), true, &[]);
        for (request_override, expected_effort) in [
            (json!({ "reasoning_effort": "low" }), "low"),
            (json!({ "reasoning_effort": "none" }), "none"),
            (
                json!({ "thinking": { "type": "disabled" }, "reasoning_effort": "high" }),
                "none",
            ),
        ] {
            let mut body = request_override;
            body["max_completion_tokens"] = json!(20_000);
            body["max_tokens"] = json!(4096);
            body["messages"] = json!([{ "role": "user", "content": "Summarize this task." }]);
            let out = openai_to_qoder_infer_body(&body, "auto", Some(&info), Some("high"));
            assert_eq!(out["parameters"]["max_tokens"], json!(20_000));
            assert_eq!(
                out["parameters"]["reasoning_effort"],
                json!(expected_effort)
            );
            assert_eq!(
                out["model_config"]["reasoning_effort"],
                json!(expected_effort)
            );
        }
        let out = openai_to_qoder_infer_body(
            &json!({ "max_completion_tokens": 0, "max_tokens": 8192 }),
            "auto",
            Some(&info),
            Some("medium"),
        );
        assert_eq!(out["parameters"]["max_tokens"], json!(8192));
        assert_eq!(out["parameters"]["reasoning_effort"], json!("medium"));
    }

    #[test]
    fn nested_context_error_code_survives_the_qoder_envelope() {
        let nested = json!({ "error": {
            "code": "context_length_exceeded",
            "message": "Request rejected",
        } });
        for details in [json!(nested.to_string()), nested] {
            let body = json!({ "message": "Inference failed", "code": "UPSTREAM_ERROR", "details": details });
            let envelope = json!({ "statusCode": "BAD_REQUEST", "body": body.to_string() });
            let block = format!("data: {envelope}");
            let chunk = parse_qoder_envelope_block(&block).expect("error envelope is forwarded");
            let value: Value = serde_json::from_str(&chunk).expect("standard stream error");
            assert_eq!(value["error"]["code"], json!("context_length_exceeded"));
            assert_eq!(value["error_code"], json!(400));
            assert!(value["error"]["message"]
                .as_str()
                .unwrap()
                .contains("Request rejected"));
        }
    }

    #[test]
    fn nested_json_and_sse_errors_preserve_precise_fields() {
        let error = json!({
            "message": "developer is not one of ['system', 'assistant', 'user', 'tool', 'function']",
            "code": "invalid_parameter_error",
            "type": "invalid_request_error",
            "request_id": "inner-request",
        });
        let wrapped = json!({"error":error,"id":"chatcmpl-not-a-request-id"});
        for details in [
            wrapped.clone(),
            json!(wrapped.to_string()),
            error.clone(),
            json!(error.to_string()),
            json!(format!("data: {wrapped}\n\n")),
            json!(format!(
                ": heartbeat\r\nid: ignored\r\nevent: error\r\ndata: {wrapped}\r\n\r\n"
            )),
            json!(format!(
                "data: {{\n\ndata: invalid\n\ndata: [DONE]\n\ndata: {wrapped}"
            )),
            json!(format!("data: {{\ndata: \"error\":{error}\ndata: }}\n\n")),
        ] {
            let body = json!({
                "message":"Error in upstream response", "code":"provider_error",
                "type":"provider_error", "request_id":"gateway-request", "details":details,
            });
            let value: Value = serde_json::from_str(&qoder_envelope_error_chunk(
                "BAD_REQUEST",
                Some(&body.to_string()),
            ))
            .unwrap();
            assert_eq!(value["error_code"], 400);
            assert_eq!(value["error"]["code"], error["code"]);
            assert_eq!(value["error"]["type"], error["type"]);
            assert_eq!(value["error"]["request_id"], "gateway-request");
            assert!(value["error"]["message"]
                .as_str()
                .unwrap()
                .contains(error["message"].as_str().unwrap()));
        }
    }

    #[test]
    fn nested_errors_fall_back_per_field_without_using_completion_ids() {
        let body = json!({
            "message":"Outer message", "type":"outer_type", "code":"outer_code",
            "error":{"message":"Wrapped message","type":"wrapped_type","code":"wrapped_code"},
            "details":{"error":{"message":" ","type":"nested_type","code":"","request_id":"inner-request"},"id":"chatcmpl-ignored"},
        });
        let value: Value = serde_json::from_str(&qoder_envelope_error_chunk(
            "UNAUTHORIZED",
            Some(&body.to_string()),
        ))
        .unwrap();
        assert_eq!(value["error_code"], 401);
        assert_eq!(value["error"]["type"], "nested_type");
        assert_eq!(value["error"]["code"], "wrapped_code");
        assert_eq!(value["error"]["request_id"], "inner-request");
        assert!(value["error"]["message"]
            .as_str()
            .unwrap()
            .contains("Wrapped message"));
        let only_id = json!({"details":{"error":{"message":"Failure"},"id":"chatcmpl-ignored"}});
        let value: Value = serde_json::from_str(&qoder_envelope_error_chunk(
            "ERROR",
            Some(&only_id.to_string()),
        ))
        .unwrap();
        assert!(value["error"].get("request_id").is_none());
    }

    #[test]
    fn malformed_and_successful_details_keep_the_outer_failure() {
        for details in [
            Value::Null,
            json!("not JSON"),
            json!("data: [DONE]\n\n"),
            json!({"error":null,"message":"A sample"}),
            json!({"choices":[{"delta":{"content":"An error example"}}],"message":"A sample"}),
            json!("data: {\"choices\":[{\"delta\":{\"content\":\"error\"}}]}\n\n"),
            json!("data: {\"error\":null}\n\n"),
        ] {
            let body = json!({"message":"Outer failure","code":"outer_code","type":"outer_type","details":details});
            let value: Value = serde_json::from_str(&qoder_envelope_error_chunk(
                "ERROR",
                Some(&body.to_string()),
            ))
            .unwrap();
            assert_eq!(value["error_code"], 502);
            assert_eq!(value["error"]["code"], "outer_code");
            assert_eq!(value["error"]["type"], "outer_type");
            assert_eq!(
                value["error"]["message"],
                "Qoder upstream ERROR: Outer failure"
            );
        }
    }

    #[test]
    fn nested_sse_skips_success_frames_before_selecting_an_error() {
        let details = concat!(
            "data: {\"choices\":[{\"delta\":{\"content\":\"Sample error text\"}}]}\n\n",
            "data: {\"error\":null}\n\n",
            "data: {\"error\":{}}\n\n",
            "data: {\"error\":{\"message\":\"Quota exhausted\",\"code\":\"quota_exceeded\"}}\n\n",
        );
        let body = json!({"message":"Outer failure","details":details});
        let value: Value = serde_json::from_str(&qoder_envelope_error_chunk(
            "TOO_MANY_REQUESTS",
            Some(&body.to_string()),
        ))
        .unwrap();
        assert_eq!(value["error_code"], 429);
        assert_eq!(value["error"]["code"], "quota_exceeded");
        assert!(value["error"]["message"]
            .as_str()
            .unwrap()
            .contains("Quota exhausted"));
    }

    #[test]
    fn successful_native_envelopes_are_not_reinterpreted_as_errors() {
        for inner in [
            json!({"choices":[{"delta":{"content":"data: {\"error\":{\"message\":\"example\"}}"}}]}).to_string(),
            json!({"choices":[{"delta":{"tool_calls":[{"id":"call-1","function":{"name":"inspect","arguments":"{\"error\":true}"}}]}}]}).to_string(),
            "[DONE]".to_string(),
        ] {
            let envelope = json!({"statusCode":"OK","body":inner});
            assert_eq!(parse_qoder_envelope_block(&format!("data: {envelope}")), Some(inner));
        }
    }

    #[test]
    fn non_streaming_errors_keep_the_envelope_status_and_fields() {
        for (word, expected) in [
            ("BAD_REQUEST", 400),
            ("UNAUTHORIZED", 401),
            ("Payment Required", 402),
            ("FORBIDDEN", 403),
            ("TOO_MANY_REQUESTS", 429),
            ("SERVICE_UNAVAILABLE", 503),
            ("unknown", 502),
        ] {
            let body = json!({"message":"Outer failure","request_id":"gateway-id","details":"data: {\"error\":{\"message\":\"Precise failure\",\"code\":\"precise_code\",\"type\":\"precise_type\"}}\n\n"});
            let inner = qoder_envelope_error_chunk(word, Some(&body.to_string()));
            let (mut chunks, mut failure, mut done) = (Vec::new(), None, false);
            consume_aggregate(
                inner,
                &mut chunks,
                &mut failure,
                &mut done,
                &mut ToolCallIndexNormalizer::new(),
            );
            let failure = failure.unwrap();
            assert_eq!(failure.status, expected);
            assert_eq!(failure.payload["error_code"], expected);
            assert_eq!(failure.payload["error"]["code"], "precise_code");
            assert_eq!(failure.payload["error"]["type"], "precise_type");
            assert_eq!(failure.payload["error"]["request_id"], "gateway-id");
            assert!(done && chunks.is_empty());
        }
    }

    #[test]
    fn aggregation_ignores_null_errors_and_bounds_failure_statuses() {
        let (mut chunks, mut failure, mut done) = (Vec::new(), None, false);
        let mut normalizer = ToolCallIndexNormalizer::new();
        consume_aggregate(
            json!({"error":null,"choices":[]}).to_string(),
            &mut chunks,
            &mut failure,
            &mut done,
            &mut normalizer,
        );
        assert!(failure.is_none() && !done);
        assert_eq!(chunks.len(), 1);
        for status in [0, 200, 399, 600, 65536] {
            consume_aggregate(
                json!({"error":{"message":"Failure"},"error_code":status}).to_string(),
                &mut chunks,
                &mut failure,
                &mut done,
                &mut normalizer,
            );
            assert_eq!(failure.as_ref().unwrap().status, 502);
        }
    }

    /// A tool call the model emitted with no preamble text serialises WITHOUT a
    /// `content` key (Chat Completions makes it optional next to `tool_calls`).
    /// Qoder's upstream drops such a message whole, orphaning the `tool_result`
    /// that follows and rejecting the request — which poisons every later turn in
    /// the session, since the message stays in the history.
    #[test]
    fn assistant_tool_calls_without_content_are_sent_with_an_empty_content() {
        let call = json!({ "id": "call_1", "type": "function",
            "function": { "name": "Bash", "arguments": "{}" } });
        let body = json!({
            "model": "auto",
            "messages": [
                { "role": "user", "content": "run it" },
                { "role": "assistant", "tool_calls": [call.clone()] },
                { "role": "tool", "tool_call_id": "call_1", "content": "hi" },
                { "role": "assistant", "content": "done", "tool_calls": [call] },
                { "role": "assistant", "content": null },
            ],
        });
        let out = openai_to_qoder_infer_body(&body, "auto", None, None);
        let messages = out["messages"].as_array().unwrap();
        assert_eq!(
            messages[1]["content"],
            json!(""),
            "the key is backfilled, not invented as text"
        );
        assert_eq!(
            messages[1]["tool_calls"][0]["id"],
            json!("call_1"),
            "the tool call the upstream would have dropped is still there"
        );
        assert_eq!(
            messages[0]["content"],
            json!("run it"),
            "a user message is untouched"
        );
        assert_eq!(
            messages[2]["content"],
            json!("hi"),
            "a tool message is untouched"
        );
        assert_eq!(
            messages[3]["content"],
            json!("done"),
            "existing assistant text is untouched"
        );
        assert_eq!(
            messages[4]["content"],
            json!(""),
            "an explicit null is normalised too"
        );
    }

    #[test]
    fn extracts_text_from_multimodal_content() {
        let body = json!({
            "model": "auto",
            "messages": [{ "role": "user", "content": [ { "type": "text", "text": "hi " }, { "type": "text", "text": "there" } ] }],
        });
        let out = openai_to_qoder_infer_body(&body, "auto", None, None);
        assert_eq!(out["chat_context"]["text"], json!("hi there"));
    }

    #[test]
    fn context_window_is_pinned_to_largest_advertised() {
        let m = model(&["low", "high"], Some("high"), true, &[200_000, 1_000_000]);
        let out = openai_to_qoder_infer_body(
            &json!({ "model": "m", "messages": [] }),
            "m",
            Some(&m),
            Some("medium"),
        );
        assert_eq!(out["parameters"]["context_length"], json!(1_000_000));
        assert_eq!(out["model_config"]["max_input_tokens"], json!(1_000_000));
    }

    #[test]
    fn context_window_defaults_without_catalog() {
        let out = openai_to_qoder_infer_body(
            &json!({ "model": "auto", "messages": [] }),
            "auto",
            None,
            Some("medium"),
        );
        assert_eq!(out["parameters"]["context_length"], json!(180_000));
    }

    #[test]
    fn effort_off_is_none_when_disable_supported_else_default() {
        let can = model(&["low", "high"], Some("high"), true, &[]);
        assert_eq!(resolve_qoder_effort(Some(&can), Some("off")), "none");
        let cannot = model(&["high"], Some("high"), false, &[]);
        assert_eq!(resolve_qoder_effort(Some(&cannot), Some("off")), "high");
    }

    #[test]
    fn concrete_request_effort_overrides_global_ultra_in_qoder_and_qwenwork() {
        let entry = model(
            &["low", "high", "xhigh", "max", "ultra"],
            Some("ultra"),
            false,
            &[],
        );
        for (wire, global, expected) in [
            ("xhigh", "low", "xhigh"),
            ("low", "ultra", "low"),
            ("high", "ultra", "high"),
        ] {
            let body = json!({"model":"selected", "reasoning_effort":wire, "messages":[{"role":"user","content":"bounded worker"}]});
            let upstream =
                openai_to_qoder_infer_body(&body, "selected", Some(&entry), Some(global));
            assert_eq!(upstream["parameters"]["reasoning_effort"], json!(expected));
            assert_eq!(
                upstream["model_config"]["reasoning_effort"],
                json!(expected)
            );
        }
        assert_ne!(resolve_qoder_effort(Some(&entry), Some("off")), "ultra");
        assert_ne!(resolve_qoder_effort(Some(&entry), Some("unknown")), "ultra");
        for levels in [vec!["high", " ULTRA "], vec![" ULTRA "], vec!["unknown"]] {
            let entry = model(&levels, Some(" ULTRA "), false, &[]);
            for choice in [None, Some("off"), Some("unknown"), Some("ultra")] {
                let resolved = resolve_qoder_effort(Some(&entry), choice);
                assert!(resolved.is_empty() || resolved == "high");
            }
        }
    }

    #[test]
    fn ultra_side_channel_never_reaches_qoder_or_qwenwork_gateway() {
        for (levels, expected) in [
            (vec!["low", "high", "xhigh", "max"], "xhigh"),
            (vec!["medium", "high", "max"], "high"),
            (vec!["low", "medium"], "medium"),
            (vec!["low"], "low"),
            (vec!["minimal"], "minimal"),
            (vec!["minimal", "max"], "minimal"),
            (vec!["max"], "max"),
            (vec![], ""),
            (vec!["ultra"], ""),
        ] {
            let entry = model(&levels, None, false, &[]);
            assert_eq!(resolve_qoder_effort(Some(&entry), Some("ultra")), expected);
        }
    }

    #[test]
    fn effort_top_tier_prefers_max_then_xhigh_and_degrades() {
        let with_max = model(&["low", "high", "xhigh", "max"], Some("high"), true, &[]);
        assert_eq!(resolve_qoder_effort(Some(&with_max), Some("max")), "max");
        let without_max = model(&["low", "medium", "high"], Some("medium"), true, &[]);
        assert_eq!(
            resolve_qoder_effort(Some(&without_max), Some("max")),
            "high"
        );
        // No advertised efforts at all → no effort field.
        assert_eq!(
            resolve_qoder_effort(Some(&model(&[], None, false, &[])), Some("high")),
            ""
        );
    }

    #[test]
    fn sanitize_tools_pins_root_object_and_drops_required_only_combinator() {
        // The browser `batch` shape Qoder's strict validator bounced: a root
        // `type` next to a required-only anyOf used to lose the root type.
        let tools = json!([{
            "type": "function",
            "function": {
                "name": "mcp__klaus-browser__batch",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "operations": { "type": "array", "items": { "type": "object" } },
                        "actions": { "type": "array", "items": { "type": "object" } },
                        "steps": { "type": "array", "items": { "type": "object" } }
                    },
                    "additionalProperties": true,
                    "anyOf": [
                        { "required": ["operations"] },
                        { "required": ["actions"] },
                        { "required": ["steps"] }
                    ]
                }
            }
        }]);
        let params = &sanitize_qoder_tools(&tools)[0]["function"]["parameters"];
        assert_eq!(params["type"], json!("object"), "root type survives");
        assert!(
            params.get("anyOf").is_none(),
            "root combinator is eliminated"
        );
        assert!(
            params.get("oneOf").is_none(),
            "root combinator is eliminated"
        );
        for key in ["operations", "actions", "steps"] {
            assert!(params["properties"].get(key).is_some(), "{key} kept");
        }
    }

    #[test]
    fn sanitize_tools_merges_branch_properties_into_root() {
        let tools = json!([{
            "type": "function",
            "function": { "name": "t", "parameters": {
                "type": "object",
                "anyOf": [
                    { "type": "object", "properties": { "a": { "type": "string" } } },
                    { "properties": { "b": { "type": "number" } } }
                ]
            } }
        }]);
        let params = &sanitize_qoder_tools(&tools)[0]["function"]["parameters"];
        assert_eq!(params["type"], json!("object"));
        assert_eq!(params["properties"]["a"]["type"], json!("string"));
        assert_eq!(params["properties"]["b"]["type"], json!("number"));
    }

    #[test]
    fn sanitize_tools_leaves_root_typeless_when_branches_disagree_on_type() {
        let tools = json!([{
            "type": "function",
            "function": { "name": "t", "parameters": { "type": "string", "anyOf": [ {}, { "type": "number" } ] } },
        }]);
        let params = &sanitize_qoder_tools(&tools)[0]["function"]["parameters"];
        assert!(params.get("type").is_none(), "no false object claim");
        assert!(
            params.get("anyOf").is_none(),
            "root combinator still eliminated"
        );
    }

    #[test]
    fn sanitize_tools_pushes_type_into_nested_combinators_only() {
        let tools = json!([{
            "type": "function",
            "function": { "name": "t", "parameters": {
                "type": "object",
                "properties": { "v": { "type": "string", "anyOf": [ {}, { "type": "number" } ] } }
            } }
        }]);
        let nested = &sanitize_qoder_tools(&tools)[0]["function"]["parameters"]["properties"]["v"];
        assert!(
            nested.get("type").is_none(),
            "nested parent type is pushed down"
        );
        assert_eq!(nested["anyOf"][0]["type"], json!("string"));
        assert_eq!(nested["anyOf"][1]["type"], json!("number"));
    }

    #[test]
    fn sanitized_corpus_passes_both_upstream_validators() {
        // Representative corpus: the browser batch root combinator, the
        // spawn_agent type-next-to-anyOf shape, and a nested oneOf union
        // (tabs_content urls).
        let tools = json!([
            { "type": "function", "function": { "name": "batch", "parameters": {
                "type": "object",
                "properties": { "operations": { "type": "array", "items": { "type": "object" } } },
                "anyOf": [ { "required": ["operations"] }, { "required": ["steps"] } ] } } },
            { "type": "function", "function": { "name": "spawn_agent", "parameters": {
                "type": "object",
                "properties": { "fork_context": { "type": "boolean", "anyOf": [ { "type": "boolean" }, { "type": "integer" } ] } } } } },
            { "type": "function", "function": { "name": "tabs_content", "parameters": {
                "type": "object",
                "properties": { "urls": { "oneOf": [ { "type": "string" }, { "type": "array", "items": { "type": "string" } } ] } } } } }
        ]);
        for tool in sanitize_qoder_tools(&tools).as_array().unwrap() {
            let params = &tool["function"]["parameters"];
            assert_eq!(
                params.get("type"),
                Some(&json!("object")),
                "OpenAI-flavored upstream needs a typed object root"
            );
            assert_moonshot_flavored(params);
        }
    }

    /// Moonshot-flavored upstream: no node may carry `type` next to a
    /// combinator; the type lives inside each branch instead.
    fn assert_moonshot_flavored(node: &Value) {
        match node {
            Value::Object(map) => {
                let has_combinator = map.contains_key("anyOf") || map.contains_key("oneOf");
                assert!(
                    !(has_combinator && map.contains_key("type")),
                    "type next to combinator: {node}"
                );
                for value in map.values() {
                    assert_moonshot_flavored(value);
                }
            }
            Value::Array(items) => {
                for item in items {
                    assert_moonshot_flavored(item);
                }
            }
            _ => {}
        }
    }

    #[test]
    fn envelope_unwraps_inner_chunk_and_done() {
        assert_eq!(
            parse_qoder_envelope_block("data: {\"body\":\"{\\\"a\\\":1}\",\"statusCode\":\"OK\"}"),
            Some("{\"a\":1}".to_string())
        );
        assert_eq!(
            parse_qoder_envelope_block("data: {\"body\":\"[DONE]\",\"statusCode\":\"OK\"}"),
            Some("[DONE]".to_string())
        );
        assert_eq!(
            parse_qoder_envelope_block("event: finish\ndata: {\"body\":\"x\"}"),
            None
        );
    }

    #[test]
    fn envelope_non_ok_folds_into_openai_error() {
        let out = parse_qoder_envelope_block(
            "data: {\"statusCode\":\"TOO_MANY_REQUESTS\",\"body\":\"{\\\"message\\\":\\\"slow down\\\"}\"}",
        )
        .unwrap();
        let parsed: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(parsed["error_code"], json!(429));
        assert!(parsed["error"]["message"]
            .as_str()
            .unwrap()
            .contains("slow down"));
    }

    #[test]
    fn envelope_image_preprocessing_failure_preserves_status_without_billing_classification() {
        let chunk = qoder_envelope_error_chunk(
            "Payment Required",
            Some(r#"{"message":"Image preprocessing failed"}"#),
        );
        let parsed: Value = serde_json::from_str(&chunk).unwrap();
        assert_eq!(parsed["error_code"], 402);
        assert_eq!(parsed["error"]["code"], "image_preprocessing_failed");
        let classified = klaus_agent_core::provider::classify::classify_error(
            &klaus_agent_core::provider::classify::RawProviderError {
                status: Some(402),
                message: parsed["error"]["message"].as_str().unwrap().to_string(),
                code: Some(parsed["error"]["code"].as_str().unwrap().to_string()),
                ..Default::default()
            },
        );
        assert_eq!(
            classified.kind,
            klaus_agent_core::types::errors::ProviderErrorKind::InvalidRequest
        );
    }

    #[test]
    fn envelope_payment_required_maps_to_402_not_502() {
        // Live incident 2026-09-22: the gateway sent the title-cased reason
        // phrase `Payment Required`; the word map only knew enum names and
        // folded it to 502, which the classifier reads as retryable overload.
        for word in ["PAYMENT_REQUIRED", "Payment Required", "payment required"] {
            let out = parse_qoder_envelope_block(&format!(
                "data: {{\"statusCode\":\"{word}\",\"body\":\"{{\\\"message\\\":\\\"Error in upstream response\\\"}}\"}}"
            ))
            .unwrap();
            let parsed: Value = serde_json::from_str(&out).unwrap();
            assert_eq!(parsed["error_code"], json!(402), "word={word}");
            let classified = klaus_agent_core::provider::classify::classify_error(
                &klaus_agent_core::provider::classify::RawProviderError {
                    status: parsed["error_code"].as_u64().map(|s| s as u16),
                    message: parsed["error"]["message"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string(),
                    ..Default::default()
                },
            );
            assert_eq!(
                classified.kind,
                klaus_agent_core::types::errors::ProviderErrorKind::Quota,
                "word={word} must classify as quota, not overloaded"
            );
        }
    }

    #[test]
    fn envelope_status_words_keep_their_existing_mapping() {
        assert_eq!(qoder_envelope_http_status("TOO_MANY_REQUESTS"), 429);
        assert_eq!(qoder_envelope_http_status("BAD_REQUEST"), 400);
        assert_eq!(qoder_envelope_http_status("ERROR"), 502);
    }

    #[test]
    fn tool_call_index_normalizer_reassigns_parallel_calls() {
        let mut normalizer = ToolCallIndexNormalizer::new();
        let mut a = json!({ "choices": [{ "delta": { "tool_calls": [{ "index": 0, "id": "call_a", "function": { "name": "x" } }] } }] });
        normalizer.normalize(&mut a);
        assert_eq!(a["choices"][0]["delta"]["tool_calls"][0]["index"], json!(0));
        let mut b = json!({ "choices": [{ "delta": { "tool_calls": [{ "index": 0, "id": "call_b", "function": { "name": "y" } }] } }] });
        normalizer.normalize(&mut b);
        assert_eq!(
            b["choices"][0]["delta"]["tool_calls"][0]["index"],
            json!(1),
            "second call id gets a fresh index"
        );
        let mut cont = json!({ "choices": [{ "delta": { "tool_calls": [{ "index": 0, "function": { "arguments": "{}" } }] } }] });
        normalizer.normalize(&mut cont);
        assert_eq!(
            cont["choices"][0]["delta"]["tool_calls"][0]["index"],
            json!(1),
            "continuation routes to last announced"
        );
    }

    #[test]
    fn aggregate_folds_content_and_tool_calls() {
        let chunks = vec![
            json!({ "id": "c1", "choices": [{ "delta": { "content": "Hel" } }] }),
            json!({ "choices": [{ "delta": { "content": "lo" } }] }),
            json!({ "choices": [{ "delta": { "tool_calls": [{ "index": 0, "id": "t1", "function": { "name": "f", "arguments": "{\"a\":" } }] } }] }),
            json!({ "choices": [{ "delta": { "tool_calls": [{ "index": 0, "function": { "arguments": "1}" } }] }, "finish_reason": "tool_calls" }] }),
        ];
        let out = aggregate_openai_chunks(&chunks, "m");
        assert_eq!(out["id"], json!("c1"));
        assert_eq!(out["choices"][0]["message"]["content"], json!("Hello"));
        assert_eq!(
            out["choices"][0]["message"]["tool_calls"][0]["function"]["name"],
            json!("f")
        );
        assert_eq!(
            out["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"],
            json!("{\"a\":1}")
        );
        assert_eq!(out["choices"][0]["finish_reason"], json!("tool_calls"));
    }

    /// The page a live turn died on: the edge answered 405 with an Aliyun error
    /// page. It used to be re-labelled 502 so the engine would retry it, which is
    /// how one blocked request became minutes of retrying that could never land.
    const ALIYUN_405_PAGE: &str = concat!(
        "<!doctypehtml><html lang=\"zh-cn\"><meta charset=\"utf-8\">",
        "<meta name=\"data-spm\"content=\"a3c0e\"><title>405</title>",
        "<style>a,body,div,h2,html,p{margin:0;padding:0}a{text-decoration:none;color:#3b6ea3}</style>",
        "<div class=\"container\"><h2>\u{5f88}\u{62b1}\u{6b49}\u{ff0c}\u{51fa}\u{9519}\u{4e86}</h2></div>"
    );

    #[test]
    fn a_waf_verdict_keeps_its_terminal_status_and_names_the_page() {
        let failure = classify_upstream_failure(
            reqwest::StatusCode::METHOD_NOT_ALLOWED,
            ALIYUN_405_PAGE,
            "Qoder",
        );
        // 405 is a verdict on the request CONTENT: the same body is refused every
        // time, so the status must stay outside the engine's retry band.
        assert_eq!(failure.status, 405, "a content verdict must not be retried");
        let message = failure.payload["error"]["message"]
            .as_str()
            .unwrap_or_default();
        assert!(message.contains("HTTP 405 Method Not Allowed"), "{message}");
        assert!(message.contains("HTML error page"), "{message}");
        assert!(
            !message.contains('\n'),
            "error messages stay one line: {message}"
        );
        // The page itself is a raw error document and stays out of the
        // conversation; the failure log is where it goes instead.
        assert!(
            !message.contains("data-spm") && !message.contains('\u{ff0c}'),
            "the gateway page must not be repeated to the user: {message}"
        );
        assert!(
            message.chars().count() <= 300,
            "page must not pour into the message: {message}"
        );
    }

    #[test]
    fn an_edge_fault_keeps_its_retryable_status() {
        // The same HTML shape under a 503 is an infrastructure fault, not a
        // verdict on the body, and stays retryable on the status's own merit.
        let failure = classify_upstream_failure(
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
            ALIYUN_405_PAGE,
            "Qoder",
        );
        assert_eq!(failure.status, 503);
    }

    #[test]
    fn only_the_service_s_own_relogin_verdicts_are_treated_as_a_dead_login() {
        // The two codes Qoder's client marks `action:reLogin`.
        assert!(refusal_is_dead_login(403, r#"{"code":"105"}"#));
        assert!(refusal_is_dead_login(403, r#"{"code":105}"#));
        assert!(refusal_is_dead_login(
            403,
            r#"{"code":"auth.user.inactive"}"#
        ));
        // A signature/version/permission verdict is not about a stale token, so
        // rereading the store would swap a working credential for nothing.
        assert!(!refusal_is_dead_login(
            403,
            r#"{"code":"auth.signature.mismatch"}"#
        ));
        assert!(!refusal_is_dead_login(
            403,
            r#"{"code":"model_not_permitted"}"#
        ));
        // Only 403 carries this meaning; a 401 is our own bridge rejecting the
        // caller, which no credential reread can fix.
        assert!(!refusal_is_dead_login(401, r#"{"code":"105"}"#));
        assert!(!refusal_is_dead_login(500, r#"{"code":"105"}"#));
    }

    #[test]
    fn a_dead_login_is_repaired_once_and_never_loops() {
        let dead = r#"{"code":"105"}"#;
        assert_eq!(
            refusal_action(403, dead, 1),
            RefusalAction::RereadCredential,
            "the first refusal is the one worth repairing"
        );
        assert_eq!(
            refusal_action(403, dead, 2),
            RefusalAction::Terminal("reread:exhausted"),
            "a second dead-login refusal must end the turn, not retry again"
        );
        // Anything that is not a dead login terminates on the first attempt.
        assert_eq!(
            refusal_action(403, r#"{"code":"auth.signature.mismatch"}"#, 1),
            RefusalAction::Terminal("none:not_a_dead_login")
        );
        assert_eq!(
            refusal_action(429, dead, 1),
            RefusalAction::Terminal("none:not_a_dead_login"),
            "a rate limit is not a credential problem"
        );
    }

    #[test]
    fn a_dead_login_is_recognized_through_the_encoded_envelope() {
        // Success payloads arrive encoded; a refusal can too. The code has to be
        // readable either way, or the recovery never fires on a real refusal.
        let encoded = super::super::sign::encode_body(r#"{"code":"105"}"#);
        assert!(refusal_is_dead_login(403, &encoded), "{encoded}");
    }

    #[test]
    fn the_diagnostic_fields_read_the_envelope_the_service_actually_sends() {
        let body =
            r#"{"success":false,"code":"105","message":"Login expired","request_id":"req-abc"}"#;
        assert_eq!(service_error_code(body).as_deref(), Some("105"));
        assert_eq!(upstream_request_id(body).as_deref(), Some("req-abc"));

        // OpenAI-shaped and `msgCode` envelopes are read too.
        assert_eq!(
            service_error_code(r#"{"error":{"code":"rate_limit"}}"#).as_deref(),
            Some("rate_limit")
        );
        assert_eq!(
            service_error_code(r#"{"msgCode":400}"#).as_deref(),
            Some("400")
        );
        // An HTML gateway page has neither, and must not panic or invent one.
        assert_eq!(service_error_code(ALIYUN_405_PAGE), None);
        assert_eq!(upstream_request_id(ALIYUN_405_PAGE), None);
    }

    #[test]
    fn service_json_failure_keeps_its_status_and_wording() {
        let body =
            r#"{"success":false,"msgCode":400,"msgInfo":"Request method 'GET' not supported"}"#;
        let failure = classify_upstream_failure(reqwest::StatusCode::BAD_REQUEST, body, "Qoder");
        assert_eq!(failure.status, 400);
        assert_eq!(
            failure.payload["error"]["message"],
            json!("Request method 'GET' not supported")
        );
        assert_eq!(failure.payload["error"]["type"], json!("upstream_error"));
    }

    #[test]
    fn openai_shaped_failure_keeps_its_nested_message() {
        let failure = classify_upstream_failure(
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            r#"{"error":{"type":"rate_limit","message":"slow down"}}"#,
            "Qoder",
        );
        assert_eq!(failure.status, 429);
        assert_eq!(failure.payload["error"]["message"], json!("slow down"));
    }

    #[test]
    fn empty_and_plain_text_failures_still_name_the_status() {
        let empty = classify_upstream_failure(reqwest::StatusCode::BAD_GATEWAY, "   ", "Qoder");
        let message = empty.payload["error"]["message"]
            .as_str()
            .unwrap_or_default();
        assert!(message.contains("HTTP 502 Bad Gateway"), "{message}");
        assert!(message.contains("empty body"), "{message}");

        // A plain-text body is a document written in front of the service (a
        // proxy, a WAF). It names the status and its shape, and the text itself
        // stays out of the conversation.
        let text =
            classify_upstream_failure(reqwest::StatusCode::FORBIDDEN, "blocked by policy", "Qoder");
        let message = text.payload["error"]["message"]
            .as_str()
            .unwrap_or_default();
        assert_eq!(text.status, 403);
        assert!(message.contains("HTTP 403 Forbidden"), "{message}");
        assert!(message.contains("non-JSON body"), "{message}");
        assert!(
            !message.contains("blocked by policy"),
            "a gateway document must not be repeated to the user: {message}"
        );
    }

    #[test]
    fn an_envelope_without_a_readable_message_still_answers() {
        // The service's verdict is what the engine classifies; with none present
        // the status has to stand alone rather than the message going empty.
        let failure = classify_upstream_failure(
            reqwest::StatusCode::FORBIDDEN,
            r#"{"success":false,"code":"105"}"#,
            "Qoder",
        );
        assert_eq!(failure.status, 403);
        let message = failure.payload["error"]["message"]
            .as_str()
            .unwrap_or_default();
        assert!(!message.is_empty(), "a refusal must say something");
        assert_eq!(failure.payload["error"]["type"], json!("upstream_error"));
    }

    #[test]
    fn reason_phrase_comes_from_the_canonical_table() {
        assert_eq!(reason_phrase(405), "Method Not Allowed");
        assert_eq!(reason_phrase(200), "OK");
        assert_eq!(
            reason_phrase(599),
            "Unknown",
            "unknown statuses must not claim OK"
        );
    }

    #[test]
    fn single_line_preview_collapses_whitespace_and_caps_length() {
        assert_eq!(single_line_preview("  a\n\n b  ", 40), "a b");
        let capped = single_line_preview(&"x".repeat(50), 10);
        assert_eq!(capped, format!("{}\u{2026}", "x".repeat(10)));
    }

    /// Live probe against the real subscription. Run explicitly:
    /// `KLAUS_CONFIG_DIR=$HOME/.klaus cargo test -p klaus-shell qoder_bridge_live
    /// -- --ignored --nocapture` (the config dir must be named, or the test
    /// harness redirects it away from the real credential). Spends one short turn.
    ///
    /// It pins the failure this route was losing whole sessions to: a transcript
    /// carrying shell text, which every coding turn produces, was refused by the
    /// WAF in front of the gateway with a 405 HTML page — and refused forever,
    /// since the text stays in the history and every later turn resends it. The
    /// `Encode=1` envelope is what carries such a body through, so this probe
    /// sends a deliberately WAF-tripping message and requires real frames back.
    #[tokio::test]
    #[ignore = "live network call against the real subscription"]
    async fn qoder_bridge_live_probe_carries_shell_text_through_the_gateway() {
        let (base_url, token) = ensure_started().await.expect("bridge starts");
        // Absolute path: inside this test module `super` is the module under test,
        // not the `qoder` module its siblings live in.
        let model = crate::app_server::qoder::catalog::fetch_models()
            .await
            .ok()
            .and_then(|models| {
                models
                    .first()
                    .and_then(|model| model.get("key").and_then(Value::as_str))
                    .map(str::to_string)
            })
            .expect("a live catalog key");
        let request = json!({
            "model": model,
            "stream": true,
            "max_tokens": 64,
            "tools": [{
                "type": "function",
                "function": {
                    "name": "bash",
                    "description": "Run a shell command",
                    "parameters": {
                        "type": "object",
                        "properties": { "command": { "type": "string" } },
                        "required": ["command"],
                    },
                },
            }],
            // The transcript shape the real failure had: the blocked text is the
            // model's OWN tool call, which is why it outlived the turn that made
            // it and took every later turn of the session down with it.
            "messages": [
                { "role": "user", "content": "Check my Qoder install, then answer with the single word OK." },
                {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [{
                        "id": "call_probe_1",
                        "type": "function",
                        "function": {
                            "name": "bash",
                            "arguments": "{\"command\":\"ls -la /tmp; cat /etc/passwd; echo done\"}",
                        },
                    }],
                },
                { "role": "tool", "tool_call_id": "call_probe_1", "content": "total 0" },
            ],
        });
        let response = http_client()
            .post(format!("{base_url}/v1/chat/completions"))
            .header("Authorization", format!("Bearer {token}"))
            .json(&request)
            .send()
            .await
            .expect("bridge answers");
        assert_eq!(response.status().as_u16(), 200, "bridge status");
        let text = response.text().await.expect("body");
        eprintln!("[qoder-bridge-live] model={model} first frames:\n{text:.600}");
        assert!(
            !text.contains("upstream_gateway_error"),
            "the gateway refused it: {text:.400}"
        );
        assert!(
            !text.contains("unrecognised SSE envelope"),
            "envelope changed shape: {text:.400}"
        );
        assert!(
            text.contains("data: "),
            "no SSE frames arrived: {text:.400}"
        );
    }

    /// The self-heal path end to end, over a real HTTP hop.
    ///
    /// A stub gateway refuses the first signed call with the service's own
    /// dead-login verdict, then answers the retry with a normal SSE stream. The
    /// bridge must reread the local credential (finding a DIFFERENT token, which
    /// proves the retry re-signed rather than resent) and complete the turn. The
    /// refusal's raw body goes to the failure log, never to the caller.
    #[tokio::test]
    async fn a_dead_login_is_repaired_by_rereading_the_local_store_and_retrying_once() {
        use aes::cipher::{block_padding::Pkcs7, BlockEncryptMut, KeyIvInit};
        use base64::Engine as _;
        use std::sync::atomic::AtomicU32;
        use std::sync::Arc as StdArc;

        let _endpoint = endpoint_lock().lock().await;
        let _lock = super::super::test_env_lock().lock().await;
        let fixture = tempfile::tempdir().expect("fixture");
        let config = fixture.path().join("config");
        let home = fixture.path().join("qoder-home");
        let previous_qoder_home = std::env::var_os("QODER_HOME");
        klaus_agent_core::runtime_process::set_process_environment_var("QODER_HOME", &home);
        std::fs::create_dir_all(&config).expect("config dir");
        std::fs::create_dir_all(home.join(".auth")).expect("auth dir");

        // The credential Klaus starts with is the DEAD one.
        super::super::set_config_root(&config);
        super::super::catalog::forget_context();
        crate::app_server::qoder::auth::adopt_credential(json!({
            "uid": "fixture-user",
            "name": "Fixture",
            "security_oauth_token": "dead-token",
            "access_token": "dead-token",
            "expire_time": 1_900_000_000_i64,
        }))
        .expect("adopt fixture credential");

        // The official store the reread will find holds the LIVE token.
        let machine_id = "01234567-89ab-cdef-0123-456789abcdef";
        let key = &machine_id.as_bytes()[..16];
        let live = json!({
            "uid": "fixture-user",
            "name": "Fixture",
            "security_oauth_token": "live-token",
            "access_token": "live-token",
            "refresh_token": "fixture-refresh",
            "expire_time": 1_900_000_000_i64,
        });
        let encrypted = cbc::Encryptor::<aes::Aes128>::new(key.into(), key.into())
            .encrypt_padded_vec_mut::<Pkcs7>(live.to_string().as_bytes());
        std::fs::write(home.join(".auth").join("machine_id"), machine_id).expect("machine id");
        std::fs::write(
            home.join(".auth").join("user"),
            base64::engine::general_purpose::STANDARD.encode(encrypted),
        )
        .expect("encrypted profile");

        // A stub gateway: refuse the first call, stream the second.
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("stub bind");
        let port = listener.local_addr().expect("stub addr").port();
        let calls = StdArc::new(AtomicU32::new(0));
        let seen = StdArc::new(Mutex::new(Vec::<String>::new()));
        {
            let calls = calls.clone();
            let seen = seen.clone();
            tokio::spawn(async move {
                loop {
                    let Ok((mut socket, _)) = listener.accept().await else {
                        break;
                    };
                    let calls = calls.clone();
                    let seen = seen.clone();
                    tokio::spawn(async move {
                        let Ok(request) = read_http_request(&mut socket).await else {
                            return;
                        };
                        // Only the INFERENCE route counts. The catalog refresh
                        // rides the same base, and without this filter it lands
                        // as a phantom third call.
                        if !request.path.contains("agent_chat_generation") {
                            let body = json!({ "assistant": [] }).to_string();
                            let head = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                body.len()
                            );
                            let _ = socket.write_all(head.as_bytes()).await;
                            let _ = socket.write_all(body.as_bytes()).await;
                            let _ = socket.flush().await;
                            return;
                        }
                        let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
                        {
                            let mut guard = seen.lock().unwrap_or_else(|p| p.into_inner());
                            guard.push(
                                request
                                    .headers
                                    .get("authorization")
                                    .cloned()
                                    .unwrap_or_default(),
                            );
                        }
                        if call == 1 {
                            let body = json!({
                                "code": "105",
                                "message": "Login expired, please log in again",
                                "request_id": "stub-req-1",
                            })
                            .to_string();
                            let head = format!(
                                "HTTP/1.1 403 Forbidden\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                body.len()
                            );
                            let _ = socket.write_all(head.as_bytes()).await;
                            let _ = socket.write_all(body.as_bytes()).await;
                        } else {
                            // The envelope the gateway actually sends: `body`
                            // carries the inner OpenAI chunk as a JSON string.
                            let chunk = json!({
                                "choices": [{"index": 0, "delta": {"content": "ok"}, "finish_reason": null}]
                            })
                            .to_string();
                            let frame = json!({ "body": chunk, "statusCode": "OK" }).to_string();
                            let done = json!({ "body": "[DONE]", "statusCode": "OK" }).to_string();
                            let payload = format!("data: {frame}\n\ndata: {done}\n\n");
                            let head = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                payload.len()
                            );
                            let _ = socket.write_all(head.as_bytes()).await;
                            let _ = socket.write_all(payload.as_bytes()).await;
                        }
                        let _ = socket.flush().await;
                    });
                }
            });
        }

        let api_base = format!("http://127.0.0.1:{port}");
        klaus_agent_core::runtime_process::set_process_environment_var("QODER_API_BASE", &api_base);
        let previous_diagnostics = std::env::var("KLAUS_DIAGNOSTICS_DIR").ok();

        // The endpoint cache is process-global, so it can still name a listener
        // bound on a PREVIOUS test's tokio runtime. That runtime is gone, so its
        // socket is closed while `serving` still reads true. Evict the entry so
        // this test binds an endpoint on its own runtime.
        {
            let mut guard = state().lock().unwrap_or_else(|p| p.into_inner());
            guard.running.remove(&BridgeSource::Qoder);
        }

        let (base_url, token) = ensure_started().await.expect("bridge starts");
        let request = json!({
            "model": "ultimate",
            "stream": true,
            "messages": [{ "role": "user", "content": "hi" }],
        });
        let response = http_client()
            .post(format!("{base_url}/v1/chat/completions"))
            .header("Authorization", format!("Bearer {token}"))
            .json(&request)
            .send()
            .await
            .expect("bridge answers");
        let status = response.status().as_u16();
        let text = response.text().await.expect("body");

        klaus_agent_core::runtime_process::remove_process_environment_var("QODER_API_BASE");
        if let Some(value) = previous_qoder_home {
            klaus_agent_core::runtime_process::set_process_environment_var("QODER_HOME", value);
        } else {
            klaus_agent_core::runtime_process::remove_process_environment_var("QODER_HOME");
        }
        if let Some(value) = previous_diagnostics {
            klaus_agent_core::runtime_process::set_process_environment_var(
                "KLAUS_DIAGNOSTICS_DIR",
                &value,
            );
        }

        assert_eq!(calls.load(Ordering::SeqCst), 2, "exactly one retry: {text}");
        assert_eq!(status, 200, "the repaired turn must succeed: {text}");
        assert!(text.contains("data: "), "the stream arrived: {text}");
        let signatures = seen.lock().unwrap_or_else(|p| p.into_inner()).clone();
        assert_eq!(signatures.len(), 2, "two signed calls: {signatures:?}");
        assert_ne!(
            signatures[0], signatures[1],
            "the retry must be re-signed from the freshly read credential, not resent"
        );
        assert!(
            !text.contains("Login expired"),
            "the refusal's raw text must not reach the caller: {text}"
        );
    }
    #[test]
    fn speed_switch_is_per_request_and_preserves_reasoning_and_context() {
        let model = json!({
            "key":"test-model", "efforts":["high"], "contextTokens":[64000,128000],
            "feature_switches":[{"key":"highspeed","type":"bool","options":{"true":{},"false":{}}}],
        });
        let request = json!({"messages":[{"role":"user","content":"hello"}],"fast_mode":true});
        let enabled =
            openai_to_qoder_infer_body(&request, "test-model", Some(&model), Some("high"));
        let mut off = request.clone();
        off["fast_mode"] = json!(false);
        let disabled = openai_to_qoder_infer_body(&off, "test-model", Some(&model), Some("high"));
        assert_eq!(enabled["feature_switches"]["highspeed"], "true");
        assert_eq!(disabled["feature_switches"]["highspeed"], "false");
        assert_eq!(
            enabled["model_config"]["key"],
            disabled["model_config"]["key"]
        );
        assert_eq!(enabled["parameters"], disabled["parameters"]);
        assert_eq!(enabled["parameters"]["context_length"], 128000);
        assert_eq!(enabled["parameters"]["reasoning_effort"], "high");
        assert!(validate_fast_mode(&request, Some(&model)).is_ok());
        assert!(validate_fast_mode(&request, None).is_err());
        assert!(validate_fast_mode(&json!({"fast_mode":"true"}), Some(&model)).is_err());
        assert!(validate_fast_mode(&off, None).is_ok());
    }
}
