//! Local egress relay for Antigravity protocol traffic.
//!
//! When a relay binary ships next to the engine binary (or the
//! `KLAUS_ANTIGRAVITY_EGRESS` environment variable points at one), protocol
//! requests route through it: the relay re-issues each request over its own
//! TLS stack, so upstream connections originate from the same HTTP/TLS stack
//! the hub protocol components use. Without a relay the engine talks to the
//! service directly and behavior is unchanged.

use std::sync::OnceLock;

struct Endpoint {
    port: u16,
    token: String,
}

pub(super) struct Egress {
    endpoint: Endpoint,
    // The relay exits when its stdin closes; keeping the child handle for the
    // process lifetime keeps that pipe open. Engine exit reaps the relay.
    _child: std::process::Child,
}

/// Resolve the relay binary. An explicit `KLAUS_ANTIGRAVITY_EGRESS` wins
/// (empty or missing file disables the relay); otherwise look for the bundled
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
        _child: child,
    })
}

/// The process-wide relay, spawned at most once. A relay that fails to start
/// leaves the engine in direct mode for the rest of the process lifetime.
pub(super) fn egress() -> Option<&'static Egress> {
    static STATE: OnceLock<Option<Egress>> = OnceLock::new();
    STATE.get_or_init(spawn_relay).as_ref()
}

/// Rewrite an upstream https URL to route through the relay when one is
/// running, returning the URL to request plus the relay headers to attach.
/// Non-https endpoints (local fixtures, dev overrides) always stay direct.
pub(super) fn route(url: &str) -> (String, Vec<(String, String)>) {
    route_with(egress().map(|state| &state.endpoint), url)
}

fn route_with(endpoint: Option<&Endpoint>, url: &str) -> (String, Vec<(String, String)>) {
    let Some(endpoint) = endpoint else {
        return (url.to_string(), Vec::new());
    };
    let Ok(mut routed) = url::Url::parse(url) else {
        return (url.to_string(), Vec::new());
    };
    if routed.scheme() != "https" {
        return (url.to_string(), Vec::new());
    }
    let Some(host) = routed.host_str() else {
        return (url.to_string(), Vec::new());
    };
    let target = match routed.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    };
    // Rewriting scheme/host/port on a parsed absolute URL cannot fail.
    let _ = routed.set_scheme("http");
    let _ = routed.set_host(Some("127.0.0.1"));
    let _ = routed.set_port(Some(endpoint.port));
    (
        routed.to_string(),
        vec![
            ("x-egress-target".to_string(), target),
            ("x-egress-token".to_string(), endpoint.token.clone()),
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::{relay_binary, route_with, Endpoint};
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
    fn https_endpoints_route_through_the_relay_with_target_and_token() {
        let (url, headers) = route_with(
            Some(&endpoint()),
            "https://daily-cloudcode-pa.googleapis.com/v1internal:fetchAvailableModels",
        );
        assert_eq!(url, "http://127.0.0.1:4321/v1internal:fetchAvailableModels");
        assert_eq!(
            headers,
            vec![
                (
                    "x-egress-target".to_string(),
                    "daily-cloudcode-pa.googleapis.com".to_string()
                ),
                ("x-egress-token".to_string(), "tok".to_string()),
            ]
        );
    }

    #[test]
    fn query_and_explicit_port_survive_the_rewrite() {
        let (url, headers) = route_with(
            Some(&endpoint()),
            "https://oauth2.googleapis.com:8443/token?x=1",
        );
        assert_eq!(url, "http://127.0.0.1:4321/token?x=1");
        assert_eq!(headers[0].1, "oauth2.googleapis.com:8443");
    }

    #[test]
    fn local_plain_endpoints_stay_direct() {
        let (url, headers) = route_with(Some(&endpoint()), "http://127.0.0.1:8080/v1internal:x");
        assert_eq!(url, "http://127.0.0.1:8080/v1internal:x");
        assert!(headers.is_empty());
    }

    #[test]
    fn without_a_relay_the_upstream_url_is_untouched() {
        let (url, headers) = route_with(
            None,
            "https://daily-cloudcode-pa.googleapis.com/v1internal:x",
        );
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
