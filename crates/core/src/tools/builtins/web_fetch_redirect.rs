//! Cross-host redirect confinement for `web_fetch` (open-redirect / SSRF defense),
//! plus the tiny HTTP-fetch seam the web tools share.
//!
//! A permissive HTTP client with follow-redirects silently follows ANY 3xx —
//! including a pivot from a user-approved host to an attacker host or a
//! cloud-metadata IP. Instead we fetch each hop with redirects DISABLED and only
//! follow a hop that is same-origin (modulo a `www.` prefix), same protocol, same
//! port, and credential-free. A cross-host redirect is NOT followed: it is
//! surfaced to the model so a re-fetch re-enters the permission gate on the new
//! host.
//!
//! The [`Fetcher`] trait is the injectable `fetch` seam: the default
//! [`HttpFetcher`] performs real HTTP requests, while tests (and any host that
//! wants to drive requests itself) supply their own implementation.

use async_trait::async_trait;
use reqwest::Url;

use crate::runtime_values::RuntimeString;
use crate::types::abort::AbortSignal;

/// Redirect status codes we confine. Includes 303 so it can't fall through to a
/// silent follow.
fn is_redirect_status(status: u16) -> bool {
    matches!(status, 301 | 302 | 303 | 307 | 308)
}

/// Max same-origin hops before giving up.
pub const MAX_REDIRECTS: usize = 10;

/// A transport-level fetch failure (network error / abort). Confinement failures
/// (too many redirects, a blocked host, …) are NOT this — they are a
/// [`RedirectOutcome::Error`] the model can act on.
#[derive(Debug, Clone)]
pub struct FetchError {
    pub message: RuntimeString,
}

/// The body of a [`FetchResponse`]: either already buffered (injected fetchers)
/// or read lazily from a live HTTP response.
enum FetchBody {
    Ready(RuntimeString),
    Lazy(reqwest::Response),
}

/// The subset of an HTTP response the web tools read: status, the response's
/// reason phrase, headers, and a lazily-read text body. The body respects the
/// same abort signal the request was issued under.
pub struct FetchResponse {
    status: u16,
    status_text: String,
    /// Header names are lowercased so lookups are case-insensitive.
    headers: Vec<(String, String)>,
    body: FetchBody,
    signal: AbortSignal,
}

impl FetchResponse {
    /// Build a response whose body is already in hand (used by non-HTTP
    /// fetchers). Header names are lowercased.
    pub fn ready(
        status: u16,
        status_text: impl Into<String>,
        headers: Vec<(String, String)>,
        body: impl Into<RuntimeString>,
    ) -> Self {
        FetchResponse {
            status,
            status_text: status_text.into(),
            headers: headers
                .into_iter()
                .map(|(k, v)| (k.to_ascii_lowercase(), v))
                .collect(),
            body: FetchBody::Ready(body.into()),
            signal: AbortSignal::new(),
        }
    }

    /// Wrap a live HTTP response, capturing status + headers up front and
    /// deferring the body read.
    fn from_reqwest(resp: reqwest::Response, signal: AbortSignal) -> Self {
        let status = resp.status().as_u16();
        let status_text = resp.status().canonical_reason().unwrap_or("").to_string();
        let headers = resp
            .headers()
            .iter()
            .map(|(n, v)| {
                (
                    n.as_str().to_ascii_lowercase(),
                    v.to_str().unwrap_or("").to_string(),
                )
            })
            .collect();
        FetchResponse {
            status,
            status_text,
            headers,
            body: FetchBody::Lazy(resp),
            signal,
        }
    }

    pub fn status(&self) -> u16 {
        self.status
    }

    /// The response's reason phrase.
    pub fn status_text(&self) -> &str {
        &self.status_text
    }

    /// Whether the status is in the 2xx range.
    pub fn ok(&self) -> bool {
        (200..=299).contains(&self.status)
    }

    /// Case-insensitive header lookup (the first matching value).
    pub fn header(&self, name: &str) -> Option<&str> {
        let lname = name.to_ascii_lowercase();
        self.headers
            .iter()
            .find(|(n, _)| *n == lname)
            .map(|(_, v)| v.as_str())
    }

    /// Read the body as text, bounded by the request's abort signal.
    pub async fn text(self) -> Result<RuntimeString, FetchError> {
        let FetchResponse { body, signal, .. } = self;
        match body {
            FetchBody::Ready(s) => Ok(s),
            FetchBody::Lazy(resp) => {
                tokio::select! {
                    _ = signal.cancelled() => Err(FetchError { message: "aborted".into() }),
                    r = resp.text() => r
                        .map(RuntimeString::from)
                        .map_err(|e| FetchError { message: e.to_string().into() }),
                }
            }
        }
    }

    /// Read the body as bytes, stopping once `max_bytes` has been reached.
    /// Returns the buffered bytes plus whether the response was truncated.
    pub async fn bytes_limited(self, max_bytes: usize) -> Result<(Vec<u8>, bool), FetchError> {
        let FetchResponse { body, signal, .. } = self;
        match body {
            FetchBody::Ready(s) => {
                let mut bytes = s.to_node_utf8();
                if bytes.len() > max_bytes {
                    bytes.truncate(max_bytes);
                    Ok((bytes, true))
                } else {
                    Ok((bytes, false))
                }
            }
            FetchBody::Lazy(mut resp) => {
                let mut out = Vec::new();
                loop {
                    let chunk = tokio::select! {
                        _ = signal.cancelled() => {
                            return Err(FetchError { message: "aborted".into() });
                        }
                        r = resp.chunk() => r.map_err(|e| FetchError { message: e.to_string().into() })?,
                    };
                    let Some(chunk) = chunk else {
                        return Ok((out, false));
                    };
                    if out.len().saturating_add(chunk.len()) > max_bytes {
                        let remaining = max_bytes.saturating_sub(out.len());
                        out.extend_from_slice(&chunk[..remaining]);
                        return Ok((out, true));
                    }
                    out.extend_from_slice(&chunk);
                }
            }
        }
    }
}

/// The injectable `fetch` seam. `manual_redirect` mirrors `redirect: 'manual'` —
/// when true, a 3xx is returned as-is (its `location` inspected by the caller)
/// rather than followed.
#[async_trait]
pub trait Fetcher: Send + Sync {
    async fn fetch(
        &self,
        url: &str,
        headers: &[(String, String)],
        manual_redirect: bool,
        signal: &AbortSignal,
    ) -> Result<FetchResponse, FetchError>;
}

/// The default HTTP fetcher. It keeps two clients — one that leaves 3xx responses
/// untouched (for confined per-hop redirect handling) and one that follows
/// redirects (for a plain GET) — since the redirect policy is fixed at client
/// build time.
pub struct HttpFetcher {
    manual: reqwest::Client,
    follow: reqwest::Client,
}

impl HttpFetcher {
    pub fn new() -> Self {
        let manual = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        let follow = reqwest::Client::builder()
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        HttpFetcher { manual, follow }
    }
}

impl Default for HttpFetcher {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Fetcher for HttpFetcher {
    async fn fetch(
        &self,
        url: &str,
        headers: &[(String, String)],
        manual_redirect: bool,
        signal: &AbortSignal,
    ) -> Result<FetchResponse, FetchError> {
        let client = if manual_redirect {
            &self.manual
        } else {
            &self.follow
        };
        let mut req = client.get(url);
        for (k, v) in headers {
            req = req.header(k.as_str(), v.as_str());
        }
        tokio::select! {
            _ = signal.cancelled() => Err(FetchError { message: "aborted".into() }),
            r = req.send() => match r {
                Ok(resp) => Ok(FetchResponse::from_reqwest(resp, signal.clone())),
                Err(e) => Err(FetchError { message: e.to_string().into() }),
            },
        }
    }
}

/// Strip a single leading `www.` (case-insensitive) from a hostname.
fn strip_www(hostname: &str) -> String {
    let lower = hostname.to_lowercase();
    lower
        .strip_prefix("www.")
        .map(|s| s.to_string())
        .unwrap_or(lower)
}

/// Whether a redirect from `from` to `to` may be followed in-process: identical
/// protocol and port, no credentials on the target, and the same host after
/// stripping a leading `www.`.
pub fn is_permitted_redirect(from: &Url, to: &Url) -> bool {
    if to.scheme() != from.scheme() {
        return false;
    }
    if to.port() != from.port() {
        return false;
    }
    if !to.username().is_empty() || to.password().map(|p| !p.is_empty()).unwrap_or(false) {
        return false;
    }
    strip_www(to.host_str().unwrap_or("")) == strip_www(from.host_str().unwrap_or(""))
}

/// The result of following (or refusing to follow) a chain of redirects.
pub enum RedirectOutcome {
    /// A final response to read.
    Response {
        res: FetchResponse,
        final_url: RuntimeString,
    },
    /// A cross-host (or otherwise not-permitted) redirect handed back to the
    /// model, not followed.
    Redirect {
        from: RuntimeString,
        to: RuntimeString,
        status: u16,
    },
    /// A confinement error (too many hops, a blocked host, a bad `Location`, …).
    Error { message: RuntimeString },
}

/// The knobs [`fetch_with_confined_redirects`] runs under.
pub struct RedirectLoopOptions<'a> {
    pub fetcher: &'a dyn Fetcher,
    pub headers: Vec<(String, String)>,
    pub signal: &'a AbortSignal,
    /// Per-hop guard: reject a hop whose host is loopback/private.
    pub is_blocked_host: &'a (dyn Fn(&str) -> bool + Sync),
    /// Optional network-domain-policy guard. A PERMITTED (same-site, www-stripped)
    /// redirect that the policy denies is surfaced to the model rather than
    /// followed.
    pub is_policy_denied: Option<&'a (dyn Fn(&str) -> bool + Sync)>,
}

/// Fetch `start`, following only PERMITTED same-origin redirects. Every hop is
/// re-validated for https + non-private host before the request. A transport
/// error propagates as `Err`; a confinement outcome is `Ok`.
pub async fn fetch_with_confined_redirects(
    start: &Url,
    opts: &RedirectLoopOptions<'_>,
) -> Result<RedirectOutcome, FetchError> {
    let mut current = start.clone();
    for _depth in 0..=MAX_REDIRECTS {
        if current.scheme() != "https" {
            return Ok(RedirectOutcome::Error {
                message: format!("Refusing a non-https redirect hop: {}:", current.scheme()).into(),
            });
        }
        let host = current.host_str().unwrap_or("").to_string();
        if (opts.is_blocked_host)(&host) {
            return Ok(RedirectOutcome::Error {
                message: format!(
                    "Refusing a redirect to a localhost/private-network address: {host}"
                )
                .into(),
            });
        }
        let res = opts
            .fetcher
            .fetch(current.as_str(), &opts.headers, true, opts.signal)
            .await?;
        if !is_redirect_status(res.status()) {
            return Ok(RedirectOutcome::Response {
                res,
                final_url: current.to_string().into(),
            });
        }
        let location = match res.header("location") {
            Some(l) if !l.is_empty() => l.to_string(),
            _ => {
                return Ok(RedirectOutcome::Error {
                    message: "Redirect response missing a Location header.".into(),
                })
            }
        };
        let status = res.status();
        let target = match current.join(&location) {
            Ok(u) => u,
            Err(_) => {
                return Ok(RedirectOutcome::Error {
                    message: format!("Redirect to an invalid URL: {location}").into(),
                })
            }
        };
        if is_permitted_redirect(&current, &target) {
            // A same-site hop the domain policy denies is surfaced, not followed —
            // the model re-fetches and re-enters the initial-host gate.
            if let Some(denied) = opts.is_policy_denied {
                if denied(target.host_str().unwrap_or("")) {
                    return Ok(RedirectOutcome::Redirect {
                        from: current.to_string().into(),
                        to: target.to_string().into(),
                        status,
                    });
                }
            }
            current = target;
            continue;
        }
        // Cross-host (or protocol/port/credential change): do NOT follow.
        return Ok(RedirectOutcome::Redirect {
            from: current.to_string().into(),
            to: target.to_string().into(),
            status,
        });
    }
    Ok(RedirectOutcome::Error {
        message: "Too many redirects.".into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    fn u(s: &str) -> Url {
        Url::parse(s).unwrap()
    }

    /// One scripted response.
    #[derive(Clone)]
    struct Scripted {
        status: u16,
        location: Option<String>,
        body: Option<String>,
    }

    /// A fetch fake driven by a {url -> response} map, recording the URLs it was
    /// asked to fetch.
    struct ScriptedFetcher {
        map: HashMap<String, Scripted>,
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl ScriptedFetcher {
        fn new(entries: Vec<(&str, Scripted)>) -> (Arc<Self>, Arc<Mutex<Vec<String>>>) {
            let calls = Arc::new(Mutex::new(Vec::new()));
            let map = entries
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect();
            (
                Arc::new(ScriptedFetcher {
                    map,
                    calls: calls.clone(),
                }),
                calls,
            )
        }
    }

    #[async_trait]
    impl Fetcher for ScriptedFetcher {
        async fn fetch(
            &self,
            url: &str,
            _headers: &[(String, String)],
            _manual_redirect: bool,
            _signal: &AbortSignal,
        ) -> Result<FetchResponse, FetchError> {
            self.calls.lock().unwrap().push(url.to_string());
            let r = self.map.get(url).cloned().unwrap_or(Scripted {
                status: 404,
                location: None,
                body: None,
            });
            let mut headers = vec![("content-type".to_string(), "text/plain".to_string())];
            if let Some(loc) = r.location {
                headers.push(("location".to_string(), loc));
            }
            Ok(FetchResponse::ready(
                r.status,
                "",
                headers,
                r.body.unwrap_or_default(),
            ))
        }
    }

    fn resp(status: u16, location: Option<&str>, body: Option<&str>) -> Scripted {
        Scripted {
            status,
            location: location.map(String::from),
            body: body.map(String::from),
        }
    }

    #[test]
    fn permits_same_host_and_www_add_strip_hops() {
        assert!(is_permitted_redirect(
            &u("https://a.com/x"),
            &u("https://a.com/y")
        ));
        assert!(is_permitted_redirect(
            &u("https://a.com/x"),
            &u("https://www.a.com/y")
        ));
        assert!(is_permitted_redirect(
            &u("https://www.a.com/x"),
            &u("https://a.com/y")
        ));
    }

    #[test]
    fn rejects_cross_host_subdomain_protocol_port_and_credentialed_hops() {
        assert!(!is_permitted_redirect(
            &u("https://a.com"),
            &u("https://evil.com")
        ));
        assert!(!is_permitted_redirect(
            &u("https://a.com"),
            &u("https://api.a.com")
        ));
        assert!(!is_permitted_redirect(
            &u("https://a.com"),
            &u("http://a.com")
        ));
        assert!(!is_permitted_redirect(
            &u("https://a.com"),
            &u("https://a.com:8443")
        ));
        assert!(!is_permitted_redirect(
            &u("https://a.com"),
            &u("https://user:pw@a.com")
        ));
    }

    fn block_none() -> impl Fn(&str) -> bool + Sync {
        |_: &str| false
    }

    #[tokio::test]
    async fn follows_a_permitted_same_origin_redirect_to_the_final_response() {
        let (fetcher, calls) = ScriptedFetcher::new(vec![
            ("https://a.com/1", resp(302, Some("https://a.com/2"), None)),
            ("https://a.com/2", resp(200, None, Some("ok"))),
        ]);
        let signal = AbortSignal::new();
        let blocked = block_none();
        let opts = RedirectLoopOptions {
            fetcher: fetcher.as_ref(),
            headers: vec![],
            signal: &signal,
            is_blocked_host: &blocked,
            is_policy_denied: None,
        };
        let out = fetch_with_confined_redirects(&u("https://a.com/1"), &opts)
            .await
            .unwrap();
        match out {
            RedirectOutcome::Response { final_url, .. } => {
                assert_eq!(final_url, "https://a.com/2");
            }
            _ => panic!("expected a response outcome"),
        }
        assert_eq!(
            *calls.lock().unwrap(),
            vec!["https://a.com/1".to_string(), "https://a.com/2".to_string()]
        );
    }

    #[tokio::test]
    async fn returns_a_cross_host_redirect_without_following_it() {
        let (fetcher, calls) = ScriptedFetcher::new(vec![
            (
                "https://a.com/1",
                resp(302, Some("https://evil.com/x"), None),
            ),
            ("https://evil.com/x", resp(200, None, Some("SECRET"))),
        ]);
        let signal = AbortSignal::new();
        let blocked = block_none();
        let opts = RedirectLoopOptions {
            fetcher: fetcher.as_ref(),
            headers: vec![],
            signal: &signal,
            is_blocked_host: &blocked,
            is_policy_denied: None,
        };
        let out = fetch_with_confined_redirects(&u("https://a.com/1"), &opts)
            .await
            .unwrap();
        match out {
            RedirectOutcome::Redirect { to, .. } => assert_eq!(to, "https://evil.com/x"),
            _ => panic!("expected a redirect outcome"),
        }
        assert_eq!(*calls.lock().unwrap(), vec!["https://a.com/1".to_string()]);
    }

    #[tokio::test]
    async fn blocks_a_redirect_that_lands_on_a_private_host() {
        let (fetcher, _calls) = ScriptedFetcher::new(vec![(
            "https://169.254.169.254/x",
            resp(200, None, Some("meta")),
        )]);
        let signal = AbortSignal::new();
        let blocked = |h: &str| h == "169.254.169.254";
        let opts = RedirectLoopOptions {
            fetcher: fetcher.as_ref(),
            headers: vec![],
            signal: &signal,
            is_blocked_host: &blocked,
            is_policy_denied: None,
        };
        let out = fetch_with_confined_redirects(&u("https://169.254.169.254/x"), &opts)
            .await
            .unwrap();
        assert!(matches!(out, RedirectOutcome::Error { .. }));
    }

    #[tokio::test]
    async fn errors_on_a_redirect_with_no_location_header() {
        let (fetcher, _calls) =
            ScriptedFetcher::new(vec![("https://a.com/1", resp(302, None, None))]);
        let signal = AbortSignal::new();
        let blocked = block_none();
        let opts = RedirectLoopOptions {
            fetcher: fetcher.as_ref(),
            headers: vec![],
            signal: &signal,
            is_blocked_host: &blocked,
            is_policy_denied: None,
        };
        let out = fetch_with_confined_redirects(&u("https://a.com/1"), &opts)
            .await
            .unwrap();
        assert!(matches!(out, RedirectOutcome::Error { .. }));
    }

    #[tokio::test]
    async fn errors_on_a_redirect_loop_exceeding_the_cap() {
        let (fetcher, _calls) = ScriptedFetcher::new(vec![(
            "https://a.com/x",
            resp(302, Some("https://a.com/x"), None),
        )]);
        let signal = AbortSignal::new();
        let blocked = block_none();
        let opts = RedirectLoopOptions {
            fetcher: fetcher.as_ref(),
            headers: vec![],
            signal: &signal,
            is_blocked_host: &blocked,
            is_policy_denied: None,
        };
        let out = fetch_with_confined_redirects(&u("https://a.com/x"), &opts)
            .await
            .unwrap();
        match out {
            RedirectOutcome::Error { message } => {
                let message = message.try_to_utf8().unwrap();
                assert!(
                    message.to_lowercase().contains("too many"),
                    "message: {message}"
                );
            }
            _ => panic!("expected an error outcome"),
        }
    }
}
