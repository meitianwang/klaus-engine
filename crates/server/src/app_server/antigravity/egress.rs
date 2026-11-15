//! Local egress relay for Antigravity protocol traffic.
//!
//! Protocol requests bound for upstream HTTPS services route through a local
//! relay executable (`antigravity-egress`) whose own TLS and HTTP/2 stack
//! originates every upstream connection. Upstream HTTPS requests fail closed
//! with `antigravity_egress_unavailable` when no relay can be started unless
//! `KLAUS_ANTIGRAVITY_ALLOW_DIRECT=1` is explicitly set for diagnostics. If a
//! running relay child exits unexpectedly, the next request respawns it.

use std::sync::{Mutex, OnceLock};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Endpoint {
    pub(super) port: u16,
    pub(super) token: String,
}

struct Egress {
    endpoint: Endpoint,
    // The relay exits when its stdin closes; keeping the child handle keeps
    // that pipe open, and `try_wait` detects unexpected child termination so a
    // fresh relay can be spawned on demand.
    child: std::process::Child,
}

impl Drop for Egress {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Resolve the relay binary. An explicit `KLAUS_ANTIGRAVITY_EGRESS` wins
/// (empty or missing file disables lookup); otherwise look for the bundled
/// name next to the engine executable.
fn relay_binary() -> Option<std::path::PathBuf> {
    if let Ok(path) = std::env::var("KLAUS_ANTIGRAVITY_EGRESS") {
        let trimmed = path.trim();
        if trimmed.is_empty() {
            return None;
        }
        let candidate = std::path::PathBuf::from(trimmed);
        return candidate.is_file().then_some(candidate);
    }
    let exe = std::env::current_exe().ok()?;
    let dir = exe.parent()?;
    let name = if cfg!(windows) {
        "antigravity-egress.exe"
    } else {
        "antigravity-egress"
    };
    let candidate = dir.join(name);
    candidate.is_file().then_some(candidate)
}

pub(super) fn allow_direct() -> bool {
    std::env::var("KLAUS_ANTIGRAVITY_ALLOW_DIRECT")
        .ok()
        .is_some_and(|raw| {
            matches!(
                raw.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes"
            )
        })
}

fn local_token() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    format!(
        "agy-egress-{}-{}-{}",
        std::process::id(),
        now.as_secs(),
        now.subsec_nanos()
    )
}

/// Derive a stable, non-sensitive pool partition key from a credential
/// generation so distinct accounts never share an idle upstream connection.
pub(super) fn pool_tag(key: &str) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &byte in key.as_bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    format!("p-{hash:016x}")
}

fn spawn_relay() -> Option<Egress> {
    let binary = relay_binary()?;
    let token = local_token();
    let mut child = std::process::Command::new(&binary)
        .env("KLAUS_EGRESS_TOKEN", &token)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .ok()?;
    let stdout = child.stdout.take()?;
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        use std::io::BufRead;
        let mut reader = std::io::BufReader::new(stdout);
        let mut line = String::new();
        let _ = reader.read_line(&mut line);
        let _ = sender.send(line);
    });
    let line = receiver
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap_or_default();
    let Some(port) = line
        .trim()
        .strip_prefix("READY 127.0.0.1:")
        .and_then(|value| value.parse::<u16>().ok())
    else {
        let _ = child.kill();
        let _ = child.wait();
        return None;
    };
    Some(Egress {
        endpoint: Endpoint { port, token },
        child,
    })
}

fn state_slot() -> &'static Mutex<Option<Egress>> {
    static STATE: OnceLock<Mutex<Option<Egress>>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(None))
}

/// Return a live relay endpoint, spawning or respawning the child process when
/// none is running or the previous child has exited.
pub(super) fn ensure_endpoint() -> Option<Endpoint> {
    let mut guard = state_slot().lock().unwrap_or_else(|p| p.into_inner());
    if let Some(existing) = guard.as_mut() {
        match existing.child.try_wait() {
            Ok(None) => return Some(existing.endpoint.clone()),
            Ok(Some(_)) | Err(_) => {
                *guard = None;
            }
        }
    }
    let spawned = spawn_relay()?;
    let endpoint = spawned.endpoint.clone();
    *guard = Some(spawned);
    Some(endpoint)
}

/// Tear down any cached relay process after a loopback connect failure so the
/// next attempt starts a clean relay instance.
pub(super) fn invalidate() {
    let mut guard = state_slot().lock().unwrap_or_else(|p| p.into_inner());
    *guard = None;
}

/// Require a running relay endpoint for protocols that speak directly to the
/// loopback listener (such as unencrypted HTTP/2 gRPC streams).
pub(super) fn require_endpoint() -> Result<Endpoint, &'static str> {
    ensure_endpoint().ok_or("antigravity_egress_unavailable")
}

/// Close idle upstream connections held by the relay for `pool_key` (or all
/// pools when `None`). Best-effort: if no relay is running yet, nothing is
/// open to close.
pub(super) async fn reset_pool(pool_key: Option<&str>) {
    let endpoint = {
        let mut guard = state_slot().lock().unwrap_or_else(|p| p.into_inner());
        match guard.as_mut() {
            Some(existing) => {
                if matches!(existing.child.try_wait(), Ok(None)) {
                    existing.endpoint.clone()
                } else {
                    return;
                }
            }
            None => return,
        }
    };
    let Ok(client) = super::cloud::http_client() else {
        return;
    };
    let mut req = client
        .post(format!(
            "http://127.0.0.1:{}/egress-reset-pool",
            endpoint.port
        ))
        .header("x-egress-token", endpoint.token)
        .timeout(std::time::Duration::from_secs(2));
    if let Some(key) = pool_key.filter(|k| !k.is_empty()) {
        req = req.header("x-egress-pool", pool_tag(key));
    }
    let _ = req.send().await;
}

pub(super) type RoutedRequest = (String, Vec<(String, String)>);

/// Rewrite an upstream HTTPS URL to route through the relay. Plain HTTP URLs
/// (local fixtures and dev overrides) always stay direct. Upstream HTTPS URLs
/// fail closed with `antigravity_egress_unavailable` when no relay is running
/// unless `KLAUS_ANTIGRAVITY_ALLOW_DIRECT=1` is explicitly enabled.
pub(super) fn route(url: &str, pool_key: Option<&str>) -> Result<RoutedRequest, &'static str> {
    route_with(ensure_endpoint().as_ref(), allow_direct(), url, pool_key)
}

fn route_with(
    endpoint: Option<&Endpoint>,
    direct_allowed: bool,
    url: &str,
    pool_key: Option<&str>,
) -> Result<RoutedRequest, &'static str> {
    let Ok(mut routed) = url::Url::parse(url) else {
        return Ok((url.to_string(), Vec::new()));
    };
    if routed.scheme() != "https" {
        return Ok((url.to_string(), Vec::new()));
    }
    let Some(endpoint) = endpoint else {
        if direct_allowed {
            return Ok((url.to_string(), Vec::new()));
        }
        return Err("antigravity_egress_unavailable");
    };
    let Some(host) = routed.host_str() else {
        return Ok((url.to_string(), Vec::new()));
    };
    let target = match routed.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    };
    let _ = routed.set_scheme("http");
    let _ = routed.set_host(Some("127.0.0.1"));
    let _ = routed.set_port(Some(endpoint.port));
    let mut headers = vec![
        ("x-egress-target".to_string(), target),
        ("x-egress-token".to_string(), endpoint.token.clone()),
    ];
    if let Some(key) = pool_key.filter(|k| !k.is_empty()) {
        headers.push(("x-egress-pool".to_string(), pool_tag(key)));
    }
    Ok((routed.to_string(), headers))
}

#[cfg(test)]
mod tests {
    use super::{pool_tag, relay_binary, route_with, Endpoint};
    use klaus_agent_core::runtime_process::{
        remove_process_environment_var, set_process_environment_var,
    };

    fn endpoint() -> Endpoint {
        Endpoint {
            port: 4321,
            token: "tok".into(),
        }
    }

    #[test]
    fn https_endpoints_route_through_the_relay_with_target_token_and_pool() {
        let (url, headers) = route_with(
            Some(&endpoint()),
            false,
            "https://daily-cloudcode-pa.googleapis.com/v1internal:fetchAvailableModels",
            Some("gen-a"),
        )
        .unwrap();
        assert_eq!(url, "http://127.0.0.1:4321/v1internal:fetchAvailableModels");
        assert_eq!(
            headers,
            vec![
                (
                    "x-egress-target".to_string(),
                    "daily-cloudcode-pa.googleapis.com".to_string()
                ),
                ("x-egress-token".to_string(), "tok".to_string()),
                ("x-egress-pool".to_string(), pool_tag("gen-a")),
            ]
        );
    }

    #[test]
    fn query_and_explicit_port_survive_the_rewrite() {
        let (url, headers) = route_with(
            Some(&endpoint()),
            false,
            "https://oauth2.googleapis.com:8443/token?x=1",
            None,
        )
        .unwrap();
        assert_eq!(url, "http://127.0.0.1:4321/token?x=1");
        assert_eq!(headers[0].1, "oauth2.googleapis.com:8443");
        assert_eq!(headers.len(), 2);
    }

    #[test]
    fn local_plain_endpoints_stay_direct_even_without_a_relay() {
        let (url, headers) =
            route_with(None, false, "http://127.0.0.1:8080/v1internal:x", None).unwrap();
        assert_eq!(url, "http://127.0.0.1:8080/v1internal:x");
        assert!(headers.is_empty());
    }

    #[test]
    fn without_a_relay_upstream_https_fails_closed_by_default() {
        let err = route_with(
            None,
            false,
            "https://daily-cloudcode-pa.googleapis.com/v1internal:x",
            None,
        )
        .unwrap_err();
        assert_eq!(err, "antigravity_egress_unavailable");
    }

    #[test]
    fn explicit_direct_override_permits_upstream_https_without_a_relay() {
        let (url, headers) = route_with(
            None,
            true,
            "https://daily-cloudcode-pa.googleapis.com/v1internal:x",
            None,
        )
        .unwrap();
        assert_eq!(
            url,
            "https://daily-cloudcode-pa.googleapis.com/v1internal:x"
        );
        assert!(headers.is_empty());
    }

    #[test]
    fn an_explicit_relay_path_must_exist() {
        set_process_environment_var(
            "KLAUS_ANTIGRAVITY_EGRESS",
            "/definitely/missing/antigravity-egress",
        );
        assert!(relay_binary().is_none());
        remove_process_environment_var("KLAUS_ANTIGRAVITY_EGRESS");
    }
}
