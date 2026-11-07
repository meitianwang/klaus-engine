//! The security boundary for the network transports. Three checks, all applied
//! at the HTTP upgrade / request BEFORE a socket becomes a session:
//!   1. bind policy   — a non-loopback listener MUST have a token (fail-closed at startup).
//!   2. bearer token  — `Authorization: Bearer <token>` constant-time compared, when a token is set.
//!   3. origin        — reject browser-style cross-origin upgrades unless allow-listed.

use hyper::HeaderMap;
use sha2::{Digest, Sha256};

/// True for a loopback bind host (no token required by default). Deliberately
/// STRICT: only exact `localhost`/`::1` and a real dotted-quad in 127.0.0.0/8 —
/// never a hostname that merely starts with "127." (e.g. `127.evil.com`, which
/// can resolve off-box) and never the empty string (which the OS treats as
/// all-interfaces). Getting this wrong is a fail-open on the mandatory-token
/// boundary.
pub fn is_loopback_host(host: &str) -> bool {
    let mut h = host.trim().to_lowercase();
    // Strip a single leading `[` and trailing `]` (IPv6 bracket form).
    if h.starts_with('[') {
        h.remove(0);
    }
    if h.ends_with(']') {
        h.pop();
    }
    if h == "localhost" || h == "::1" {
        return true;
    }
    is_dotted_quad_loopback(&h) // 127.0.0.0/8, dotted-quad only
}

/// Matches `^127\.\d{1,3}\.\d{1,3}\.\d{1,3}$` — the first octet is literally
/// `127`, the remaining three are 1..=3 ASCII digits each (no range check,
/// mirroring the original pattern).
fn is_dotted_quad_loopback(h: &str) -> bool {
    let parts: Vec<&str> = h.split('.').collect();
    if parts.len() != 4 {
        return false;
    }
    if parts[0] != "127" {
        return false;
    }
    parts
        .iter()
        .all(|p| !p.is_empty() && p.len() <= 3 && p.bytes().all(|b| b.is_ascii_digit()))
}

/// Fail-closed rule: refuse to start a listener reachable off-box without a
/// token. Returns an error MESSAGE when the config is unsafe (so the caller
/// throws), or `None` when it's fine.
pub fn bind_policy_error(host: &str, token: Option<&str>) -> Option<String> {
    if is_loopback_host(host) {
        return None;
    }
    if let Some(t) = token {
        if !t.is_empty() {
            return None;
        }
    }
    Some(format!(
        "refusing to start a non-loopback WebSocket listener on \"{host}\" without a token: anyone who can reach this host could drive the engine. Pass a `token`, or bind to 127.0.0.1 and use SSH port-forwarding for remote access."
    ))
}

/// Constant-time bearer-token check. No token configured (`None`/empty) → always
/// allowed (loopback dev).
pub fn bearer_allowed(headers: &HeaderMap, token: Option<&str>) -> bool {
    let token = match token {
        Some(t) if !t.is_empty() => t,
        _ => return true,
    };
    let provided = match bearer_from_headers(headers) {
        Some(p) => p,
        None => return false,
    };
    // Compare SHA-256 digests so the compare is fixed-length and the raw token
    // never gates on early-mismatch timing.
    let a = Sha256::digest(provided.as_bytes());
    let b = Sha256::digest(token.as_bytes());
    constant_time_eq(&a, &b)
}

/// Fixed-length constant-time byte comparison (both inputs are 32-byte digests).
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Extract the token from an `Authorization: Bearer <token>` header, or `None` if
/// absent/malformed.
fn bearer_from_headers(headers: &HeaderMap) -> Option<String> {
    let raw = headers.get("authorization")?.to_str().ok()?;
    let mut parts = raw.split(' ');
    let scheme = parts.next()?;
    if scheme.is_empty() || !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let t = parts.collect::<Vec<_>>().join(" ");
    let t = t.trim();
    if t.is_empty() {
        None
    } else {
        Some(t.to_string())
    }
}

/// Reject upgrades carrying an `Origin` header unless it's explicitly
/// allow-listed. Non-browser clients don't send Origin; a present Origin means a
/// web page is connecting. `allowed_origins` opts specific web origins back in.
pub fn origin_allowed(headers: &HeaderMap, allowed_origins: Option<&[String]>) -> bool {
    let origin = match headers.get("origin").and_then(|v| v.to_str().ok()) {
        Some(o) if !o.is_empty() => o,
        _ => return true, // no Origin → not a browser
    };
    allowed_origins.unwrap_or(&[]).iter().any(|o| o == origin)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::header::HeaderValue;
    use hyper::HeaderMap;

    #[test]
    fn classifies_loopback_strictly() {
        assert!(is_loopback_host("127.0.0.1"));
        assert!(is_loopback_host("127.0.0.2"));
        assert!(is_loopback_host("::1"));
        assert!(is_loopback_host("localhost"));
        assert!(is_loopback_host("LOCALHOST"));
        // The fail-open cases the review caught:
        assert!(!is_loopback_host("127.evil.com"));
        assert!(!is_loopback_host(""));
        assert!(!is_loopback_host("0.0.0.0"));
        assert!(!is_loopback_host("::"));
        assert!(!is_loopback_host("192.168.1.9"));
    }

    #[test]
    fn bind_policy_fails_closed_off_loopback_without_token() {
        assert!(bind_policy_error("0.0.0.0", None)
            .unwrap()
            .contains("token"));
        assert!(bind_policy_error("127.evil.com", None)
            .unwrap()
            .contains("token"));
        assert!(bind_policy_error("0.0.0.0", Some("x")).is_none());
        assert!(bind_policy_error("127.0.0.1", None).is_none());
    }

    #[test]
    fn bearer_and_origin_checks() {
        // No token configured → always allowed.
        let h = HeaderMap::new();
        assert!(bearer_allowed(&h, None));
        assert!(bearer_allowed(&h, Some("")));
        // Token set, none provided → denied.
        assert!(!bearer_allowed(&h, Some("sekret")));

        let mut ok = HeaderMap::new();
        ok.insert("authorization", HeaderValue::from_static("Bearer sekret"));
        assert!(bearer_allowed(&ok, Some("sekret")));

        let mut wrong = HeaderMap::new();
        wrong.insert("authorization", HeaderValue::from_static("Bearer nope"));
        assert!(!bearer_allowed(&wrong, Some("sekret")));

        // Origin: absent → allowed; present → only when allow-listed.
        let no_origin = HeaderMap::new();
        assert!(origin_allowed(&no_origin, None));
        let mut ho = HeaderMap::new();
        ho.insert("origin", HeaderValue::from_static("http://evil.example"));
        assert!(!origin_allowed(
            &ho,
            Some(&["http://app.example".to_string()])
        ));
        assert!(origin_allowed(
            &ho,
            Some(&["http://evil.example".to_string()])
        ));
    }
}
