//! Browser OAuth login for signed Coding Plan allowance reads.

pub use super::console_usage::{clear_console_token, console_status, quota_windows, GatewayError};
use super::console_usage::{
    endpoints, http_client, parse_session, response_json, store_session, Session,
};
use rand::RngCore;
use serde_json::{json, Map, Value};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::time::{Duration, Instant};

/// How long the loopback listener stays open for a login that never completes.
const LOGIN_WINDOW: Duration = Duration::from_secs(300);
/// Per-connection read deadline: a browser that opens the port and stalls must not
/// pin the accept loop.
const CONNECTION_READ_TIMEOUT: Duration = Duration::from_secs(10);
/// One request is more than enough headroom for a redirect of this size.
const MAX_REQUEST_BYTES: usize = 64 * 1024;

const SIGNIN_AUTHORIZE: &str = "https://signin.volcengine.com/authorize/oauth/authorize";
/// The registered same-device client accepts the local browser callback. The
/// cross-device client accepts an HTTPS redirect, not a loopback listener.
pub(super) const CLIENT_ID: &str = "trn:signin:::devtools/same-device";
pub(super) const SCOPE: &str = "Console:All:All";
const CALLBACK_PATH: &str = "/oauth/callback";

struct PendingLogin {
    port: u16,
    state: String,
    verifier: String,
    generation: u64,
}

fn callback_redirect(port: u16) -> String {
    format!("http://127.0.0.1:{port}{CALLBACK_PATH}")
}

fn authorize_url(port: u16, state: &str, verifier: &str) -> String {
    let challenge = sha256_base64url(verifier.as_bytes());
    let redirect = callback_redirect(port);
    format!(
        "{SIGNIN_AUTHORIZE}?client_id={client}&response_type=code&code_challenge={challenge}&code_challenge_method=S256&scope={scope}&state={state}&redirect_uri={redirect}",
        client = urlencoded(CLIENT_ID),
        scope = urlencoded(SCOPE),
        redirect = urlencoded(&redirect),
    )
}

/// Open the loopback listener and answer with the authorize URL.
///
/// Returns as soon as the listener is bound: the browser round trip happens on the
/// user's clock, and the host watches `console_status` flip rather than holding a
/// facade call open for it.
pub fn console_login_start() -> Result<Value, String> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .map_err(|error| format!("could not bind the login callback port: {error}"))?;
    let port = listener
        .local_addr()
        .map_err(|error| error.to_string())?
        .port();
    let mut random = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut random);
    let state: String = random.iter().map(|byte| format!("{byte:02x}")).collect();
    // The PKCE verifier is 32 random bytes in url-safe base64 without padding: long
    // enough for S256, and free of the characters a query string would mangle.
    let mut verifier_bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut verifier_bytes);
    let verifier = url_safe_base64(&verifier_bytes);
    let url = authorize_url(port, &state, &verifier);
    let pending = PendingLogin {
        port,
        state,
        verifier,
        generation: super::console_usage::login_generation(),
    };
    std::thread::spawn(move || accept_loop(listener, pending));
    Ok(json!({ "ok": true, "url": url }))
}

/// Serve callbacks until one carries our state, or the login window closes.
fn accept_loop(listener: TcpListener, pending: PendingLogin) {
    let deadline = Instant::now() + LOGIN_WINDOW;
    // Non-blocking accept plus a poll interval: the loop must notice the deadline
    // even while no browser ever connects.
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
                let (status, body) = match handle_connection(&raw, &pending) {
                    Handled::Stored => (200, "Signed in. You can close this tab."),
                    Handled::ExchangeFailed => (400, "the sign-in service refused this code"),
                    Handled::StoreFailed => (500, "failed to store the console session"),
                    Handled::Refused => (400, "bad state"),
                    Handled::Ignored => (404, "not found"),
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

enum Handled {
    Stored,
    ExchangeFailed,
    StoreFailed,
    Refused,
    Ignored,
}

fn handle_connection(raw: &[u8], pending: &PendingLogin) -> Handled {
    let text = String::from_utf8_lossy(raw);
    let Some(first_line) = text.lines().next() else {
        return Handled::Ignored;
    };
    let mut parts = first_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let target = parts.next().unwrap_or_default();
    let (path, query) = match target.split_once('?') {
        Some((path, query)) => (path, query.to_string()),
        None => (target, String::new()),
    };
    if path != CALLBACK_PATH {
        return Handled::Ignored;
    }
    let query = if method.eq_ignore_ascii_case("post") {
        // A form post carries the params in the body; read whichever has them.
        let body = text.split("\r\n\r\n").nth(1).unwrap_or_default();
        if body.contains("code=") {
            body.to_string()
        } else {
            query
        }
    } else {
        query
    };
    let params: Map<String, Value> = query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .map(|(key, value)| (key.to_string(), json!(urldecoded(value))))
        .collect();
    let state = params
        .get("state")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if state != pending.state {
        return Handled::Refused;
    }
    let Some(code) = params.get("code").and_then(Value::as_str) else {
        return Handled::Refused;
    };
    match exchange_code(code, &pending.verifier, pending.port) {
        Ok(session) => match store_session(&session, pending.generation) {
            Ok(()) => Handled::Stored,
            Err(_) => Handled::StoreFailed,
        },
        // Signin refused the code (expired, wrong verifier, or never issued). The
        // browser page says so in words; the card's poll simply never flips.
        Err(_) => Handled::ExchangeFailed,
    }
}

/// Trade the authorization code for a session at the token endpoint.
///
/// The exchange is a form post, and `redirect_uri` must repeat the one the
/// authorize URL carried, byte for byte, or signin refuses the code.
///
/// Runs on a private current-thread runtime because the callback thread that
/// calls this is a plain `std::thread` (the listener must not live inside a task
/// that a facade call could cancel), while the HTTP client here is async-only.
fn exchange_code(code: &str, verifier: &str, port: u16) -> Result<Session, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(exchange_code_async(code, verifier, port))
}

async fn exchange_code_async(code: &str, verifier: &str, port: u16) -> Result<Session, String> {
    let http = http_client()?;
    let redirect = callback_redirect(port);
    let form = [
        ("grant_type", "authorization_code"),
        ("code", code),
        ("code_verifier", verifier),
        ("client_id", CLIENT_ID),
        ("scope", SCOPE),
        ("redirect_uri", &redirect),
    ];
    let response = http
        .post(&endpoints().token)
        .header("content-type", "application/x-www-form-urlencoded")
        .form(&form)
        .send()
        .await
        .map_err(|error| error.to_string())?;
    let status = response.status().as_u16();
    let parsed = response_json(response).await?;
    if !(200..300).contains(&status) {
        return Err(format!("signin answered HTTP {status}"));
    }
    parse_session(&parsed, None)
}

pub fn console_logout() -> Result<Value, Value> {
    clear_console_token()
        .map_err(|_| json!({"error": "could not clear the console credential"}))?;
    Ok(json!({ "ok": true }))
}

pub(super) fn urlencoded(value: &str) -> String {
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
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(byte) = u8::from_str_radix(&value[i + 1..i + 3], 16) {
                out.push(byte);
                i += 3;
                continue;
            }
        }
        if bytes[i] == b'+' {
            out.push(b' ');
        } else {
            out.push(bytes[i]);
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).to_string()
}

fn url_safe_base64(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn sha256_base64url(bytes: &[u8]) -> String {
    use base64::Engine;
    use sha2::Digest;
    let digest = sha2::Sha256::digest(bytes);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest)
}

/// True once the headers are in and any declared body has arrived, so the read loop
/// stops without waiting for the read timeout on every request.
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
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "Bad Request",
    };
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes())?;
    stream.write_all(body.as_bytes())?;
    stream.flush()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_authorize_url_carries_the_pkce_pair_and_the_loopback_redirect() {
        // The challenge must be the S256 of the verifier, url-safe and unpadded, or
        // signin refuses the exchange; and the redirect must repeat exactly what the
        // listener bound, or the code comes back to a closed port.
        let verifier = "verifier-with-enough-entropy-0123456789";
        let challenge = sha256_base64url(verifier.as_bytes());
        assert!(!challenge.contains('='));
        assert!(!challenge.contains('+'));
        assert!(!challenge.contains('/'));
        let url = authorize_url(53000, "abc", verifier);
        assert!(url.contains("code_challenge_method=S256"));
        assert!(url.contains("redirect_uri=http%3A%2F%2F127.0.0.1%3A53000%2Foauth%2Fcallback"));
        let parsed = reqwest::Url::parse(&url).unwrap();
        let redirect = parsed
            .query_pairs()
            .find(|(key, _)| key == "redirect_uri")
            .unwrap()
            .1;
        assert_eq!(redirect, callback_redirect(53000));
        assert!(url.contains(&format!("code_challenge={challenge}")));
    }

    #[test]
    fn a_callback_with_a_foreign_state_is_refused_not_stored() {
        // The state is the only thing tying a redirect to the login this listener
        // opened; accepting another one would let any page that can reach the
        // loopback port plant a session.
        let pending = PendingLogin {
            port: 53000,
            state: "expected-state".into(),
            verifier: "verifier".into(),
            generation: 0,
        };
        let raw =
            b"GET /oauth/callback?code=abc&state=someone-elses HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n";
        assert!(matches!(handle_connection(raw, &pending), Handled::Refused));
        // A path that is not the callback is ignored outright.
        let other = b"GET /other?code=abc&state=expected-state HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n";
        assert!(matches!(
            handle_connection(other, &pending),
            Handled::Ignored
        ));
    }

    #[test]
    fn console_login_uses_the_registered_same_device_redirect() {
        let started = console_login_start().expect("loopback login starts");
        assert_eq!(started["ok"], json!(true));
        let url = reqwest::Url::parse(started["url"].as_str().unwrap()).unwrap();
        println!("ARK_CONSOLE_AUTHORIZE_URL={url}");
        assert_eq!(
            url.origin().ascii_serialization(),
            "https://signin.volcengine.com"
        );
        assert_eq!(url.path(), "/authorize/oauth/authorize");
        let params: std::collections::HashMap<_, _> = url.query_pairs().collect();
        assert_eq!(params["client_id"], "trn:signin:::devtools/same-device");
        assert_eq!(params["response_type"], "code");
        assert_eq!(params["code_challenge_method"], "S256");
        assert_eq!(params["scope"], "Console:All:All");
        assert_eq!(params["code_challenge"].len(), 43);
        assert_eq!(params["state"].len(), 32);
        let redirect = reqwest::Url::parse(&params["redirect_uri"]).unwrap();
        assert_eq!(redirect.scheme(), "http");
        assert_eq!(redirect.host_str(), Some("127.0.0.1"));
        assert!(redirect.port().is_some_and(|port| port > 0));
        assert_eq!(redirect.path(), "/oauth/callback");
        assert!(redirect.query().is_none());
        assert!(redirect.fragment().is_none());
    }

    #[test]
    fn the_registered_callback_checks_state_and_ignores_the_old_path() {
        let started = console_login_start().expect("loopback login starts");
        let url = reqwest::Url::parse(started["url"].as_str().unwrap()).unwrap();
        let redirect = url
            .query_pairs()
            .find(|(key, _)| key == "redirect_uri")
            .unwrap()
            .1;
        let redirect = reqwest::Url::parse(&redirect).unwrap();
        let port = redirect.port().unwrap();
        for (path, expected) in [
            ("/oauth/callback", "400 Bad Request"),
            ("/callback", "404 Not Found"),
        ] {
            let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            write!(
                stream,
                "GET {path}?code=unused&state=foreign-state HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n"
            )
            .unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).unwrap();
            assert!(
                response.starts_with(&format!("HTTP/1.1 {expected}")),
                "{response}"
            );
        }
    }
}
