//! Provider transport prewarm: move the DNS + TCP + TLS (+ HTTP/2 ALPN)
//! handshake off the first model request's critical path.
//!
//! ## How it works
//!
//! A background task started at session
//! startup completes the handshake (and a cheap warmup
//! exchange) before the first turn; the first turn CONSUMES the warm
//! transport when ready and silently proceeds cold on timeout/failure.
//! The provider surface is HTTPS+SSE over `reqwest`, so the warmable
//! unit is the per-provider keep-alive connection pool entry:
//!
//! - at session creation a background task probes each resolved provider
//!   endpoint (first-party base, custom base URL / Foundry, Vertex, Bedrock)
//!   through the provider's OWN client, parking a fresh connection in that
//!   client's pool — the first real request then reuses it;
//! - a keep-alive loop re-probes entries before the pool's idle timeout can
//!   evict them, and re-warms after transport failures (expiry/disconnect);
//! - the first turn resolves the prewarm with the remaining connect budget:
//!   ready → "hit" (handshake already paid), timed out / failed → silent
//!   cold path. Prewarm failures NEVER fail a request;
//! - session shutdown cancels the loop.
//!
//! The whole feature is gated by `config.prewarm` (default on)
//! and emits `debug!` traces
//! for scheduled/hit/miss/expired/re-warm transitions.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::types::provider::ModelProvider;

/// Budget granted to the startup prewarm when the first turn arrives (the
/// reference uses the provider's websocket connect timeout for this).
pub const PREWARM_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Per-probe HTTP timeout (connect + one round trip).
pub const PREWARM_PROBE_TIMEOUT: Duration = Duration::from_secs(10);
/// Keep-alive tick cadence while the session is alive.
pub const PREWARM_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(30);
/// Re-warm an entry once its last probe is this old. Must stay comfortably
/// under reqwest/hyper's ~90s pool idle timeout even with one tick of
/// jitter, so the pooled connection never goes stale.
pub const PREWARM_REWARM_AGE: Duration = Duration::from_secs(45);

/// One endpoint's prewarm result.
#[derive(Debug, Clone, PartialEq)]
pub struct PrewarmOutcome {
    pub endpoint: String,
    pub warm: bool,
    pub detail: String,
    pub elapsed_ms: u64,
}

/// The first turn's view of the prewarm (reference-aligned statuses).
#[derive(Debug, Clone, PartialEq)]
pub enum PrewarmResolution {
    /// At least one endpoint finished its handshake before the first turn
    /// needed it: the first request reuses the warm pool entry.
    Ready(Vec<PrewarmOutcome>),
    /// The first turn could not use the prewarm; the request proceeds cold.
    Unavailable { status: &'static str },
}

/// Cheap transport probe: one small GET through the provider's own client.
/// ANY HTTP status (even 404) means success — the goal is the completed
/// DNS+TCP+TLS(+ALPN) handshake and the parked keep-alive connection, never
/// the payload. The response body is drained so hyper returns the
/// connection to the pool.
pub async fn probe(client: &reqwest::Client, endpoint: &str) -> PrewarmOutcome {
    let started = Instant::now();
    let outcome = tokio::time::timeout(PREWARM_PROBE_TIMEOUT, async {
        let response = client.get(endpoint).send().await?;
        let _ = response.bytes().await?;
        Ok::<_, reqwest::Error>(())
    })
    .await;
    let elapsed_ms = started.elapsed().as_millis() as u64;
    match outcome {
        Ok(Ok(())) => PrewarmOutcome {
            endpoint: endpoint.to_string(),
            warm: true,
            detail: "handshake complete".to_string(),
            elapsed_ms,
        },
        Ok(Err(error)) => PrewarmOutcome {
            endpoint: endpoint.to_string(),
            warm: false,
            detail: format!("transport error: {error}"),
            elapsed_ms,
        },
        Err(_) => PrewarmOutcome {
            endpoint: endpoint.to_string(),
            warm: false,
            detail: format!("probe exceeded {}ms", PREWARM_PROBE_TIMEOUT.as_millis()),
            elapsed_ms,
        },
    }
}

struct PrewarmShared {
    /// Outcomes of the initial probe round (`None` while still warming).
    initial: Option<Vec<PrewarmOutcome>>,
    /// When the most recent probe round completed (drives re-warm).
    last_probe: Option<Instant>,
}

/// A session-scoped transport prewarm: the initial probe plus the
/// keep-alive loop, cancellable at session shutdown.
pub struct TransportPrewarm {
    task: JoinHandle<()>,
    cancel: CancellationToken,
    shared: Arc<Mutex<PrewarmShared>>,
    ready: Arc<tokio::sync::Notify>,
    started_at: Instant,
}

impl TransportPrewarm {
    /// Spawn the startup prewarm for one provider. Returns `None` when the
    /// provider exposes no HTTP endpoints to warm (e.g. test doubles).
    pub fn schedule(provider: Arc<dyn ModelProvider>) -> Option<Self> {
        if !provider.has_prewarm_endpoints() {
            debug!("transport prewarm: not scheduled (provider exposes no http endpoints)");
            return None;
        }
        let cancel = CancellationToken::new();
        let shared = Arc::new(Mutex::new(PrewarmShared {
            initial: None,
            last_probe: None,
        }));
        let ready = Arc::new(tokio::sync::Notify::new());
        let task = {
            let cancel = cancel.clone();
            let shared = shared.clone();
            let ready = ready.clone();
            tokio::spawn(async move {
                let outcomes = provider.prewarm_transport().await;
                for outcome in &outcomes {
                    if outcome.warm {
                        debug!(
                            endpoint = %outcome.endpoint,
                            elapsed_ms = outcome.elapsed_ms,
                            "transport prewarm: warm"
                        );
                    } else {
                        debug!(
                            endpoint = %outcome.endpoint,
                            detail = %outcome.detail,
                            "transport prewarm: failed (silent degrade)"
                        );
                    }
                }
                {
                    let mut state = shared.lock().unwrap();
                    state.initial = Some(outcomes);
                    state.last_probe = Some(Instant::now());
                }
                ready.notify_waiters();

                // Keep-alive loop: re-probe entries that are old enough to be
                // at risk of pool idle eviction; this also re-warms after a
                // disconnect or a failed initial round.
                loop {
                    let tick = tokio::time::sleep(PREWARM_KEEP_ALIVE_INTERVAL);
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            debug!("transport prewarm: keep-alive cancelled (session shutdown)");
                            break;
                        }
                        _ = tick => {
                            let due = shared
                                .lock()
                                .unwrap()
                                .last_probe
                                .is_some_and(|at| at.elapsed() >= PREWARM_REWARM_AGE);
                            if !due {
                                continue;
                            }
                            debug!("transport prewarm: re-warming aged entries");
                            let outcomes = provider.prewarm_transport().await;
                            for outcome in &outcomes {
                                if outcome.warm {
                                    debug!(
                                        endpoint = %outcome.endpoint,
                                        elapsed_ms = outcome.elapsed_ms,
                                        "transport prewarm: re-warm ok"
                                    );
                                } else {
                                    debug!(
                                        endpoint = %outcome.endpoint,
                                        detail = %outcome.detail,
                                        "transport prewarm: re-warm failed (silent degrade)"
                                    );
                                }
                            }
                            shared.lock().unwrap().last_probe = Some(Instant::now());
                        }
                    }
                }
            })
        };
        debug!("transport prewarm: scheduled");
        Some(Self {
            task,
            cancel,
            shared,
            ready,
            started_at: Instant::now(),
        })
    }

    /// The first turn's consumption point (reference: resolve the startup
    /// prewarm with the remaining connect budget, then reuse or go cold).
    pub async fn resolve(&self, timeout: Duration) -> PrewarmResolution {
        let remaining = timeout.saturating_sub(self.started_at.elapsed());
        let snapshot = self.shared.lock().unwrap().initial.clone();
        let initial = if snapshot.is_some() {
            snapshot
        } else if remaining.is_zero() {
            None
        } else {
            match tokio::time::timeout(remaining, self.ready.notified()).await {
                Ok(()) => self.shared.lock().unwrap().initial.clone(),
                Err(_) => None,
            }
        };
        match initial {
            Some(outcomes) if outcomes.iter().any(|outcome| outcome.warm) => {
                debug!(
                    age_ms = self.started_at.elapsed().as_millis() as u64,
                    "transport prewarm: hit (first request reuses the warm connection)"
                );
                PrewarmResolution::Ready(outcomes)
            }
            Some(_) => {
                debug!("transport prewarm: miss (failed)");
                PrewarmResolution::Unavailable { status: "failed" }
            }
            None => {
                debug!("transport prewarm: miss (timed_out)");
                PrewarmResolution::Unavailable {
                    status: "timed_out",
                }
            }
        }
    }

    /// Stop the keep-alive loop (session shutdown).
    pub fn cancel(&self) {
        self.cancel.cancel();
        self.task.abort();
    }
}

impl Drop for TransportPrewarm {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.task.abort();
    }
}

/// Origin root (`scheme://host[:port]/`) of an absolute endpoint URL: the
/// cheapest same-origin probe target. Unparseable values pass through so the
/// probe itself reports the transport error.
pub fn endpoint_origin(url: &str) -> String {
    match reqwest::Url::parse(url) {
        Ok(parsed) => {
            let mut origin = format!("{}://{}", parsed.scheme(), parsed.host_str().unwrap_or(""));
            if let Some(port) = parsed.port() {
                origin.push(':');
                origin.push_str(&port.to_string());
            }
            origin.push('/');
            origin
        }
        Err(_) => url.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex as StdMutex;

    /// Minimal HTTP/1.1 keep-alive server: every accepted socket gets an id;
    /// every request is answered with a tiny body and the connection id is
    /// recorded per request, so tests can prove pooling reuse.
    struct ProbeServer {
        addr: std::net::SocketAddr,
        requests: Arc<StdMutex<Vec<usize>>>,
        connections: Arc<AtomicUsize>,
        shutdown: Option<std::sync::mpsc::Sender<()>>,
        join: Option<std::thread::JoinHandle<()>>,
    }

    impl ProbeServer {
        fn start() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let requests = Arc::new(StdMutex::new(Vec::new()));
            let connections = Arc::new(AtomicUsize::new(0));
            let (tx, rx) = std::sync::mpsc::channel::<()>();
            let requests_clone = requests.clone();
            let connections_clone = connections.clone();
            let join = std::thread::spawn(move || {
                listener.set_nonblocking(true).unwrap();
                let mut next_conn = 0_usize;
                let mut open: Vec<(std::net::TcpStream, usize)> = Vec::new();
                'outer: loop {
                    if rx.try_recv().is_ok() {
                        break 'outer;
                    }
                    // WouldBlock when idle; EINTR / ECONNABORTED under load.
                    // Neither invalidates the listener, so keep looping — exit is
                    // owned solely by the shutdown channel.
                    if let Ok((stream, _)) = listener.accept() {
                        // `accept` inherits the listener's nonblocking flag on
                        // BSD/macOS, which would make `set_read_timeout` below a
                        // no-op and turn every not-yet-arrived request into a
                        // WouldBlock error. The polling loop needs a blocking
                        // socket with a short read timeout instead.
                        let _ = stream.set_nonblocking(false);
                        let id = next_conn;
                        next_conn += 1;
                        connections_clone.fetch_add(1, Ordering::SeqCst);
                        open.push((stream, id));
                    }
                    let mut still_open = Vec::new();
                    for (mut stream, id) in open {
                        stream
                            .set_read_timeout(Some(Duration::from_millis(5)))
                            .unwrap();
                        let mut reader = BufReader::new(stream.try_clone().unwrap());
                        let mut request_line = String::new();
                        match reader.read_line(&mut request_line) {
                            Ok(0) => continue, // closed
                            Ok(_) => {
                                // Drain headers.
                                let mut line = String::new();
                                loop {
                                    line.clear();
                                    match reader.read_line(&mut line) {
                                        Ok(0) | Err(_) => break,
                                        Ok(_) if line == "\r\n" => break,
                                        Ok(_) => {}
                                    }
                                }
                                if request_line.starts_with("GET") {
                                    requests_clone.lock().unwrap().push(id);
                                    let body = b"ok";
                                    let _ = stream.write_all(
                                        format!(
                                            "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: keep-alive\r\n\r\n",
                                            body.len()
                                        )
                                        .as_bytes(),
                                    );
                                    let _ = stream.write_all(body);
                                    let _ = stream.flush();
                                }
                                still_open.push((stream, id));
                            }
                            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                                still_open.push((stream, id));
                            }
                            Err(_) => {}
                        }
                    }
                    open = still_open;
                    std::thread::sleep(Duration::from_millis(2));
                    if next_conn > 0
                        && open.is_empty()
                        && connections_clone.load(Ordering::SeqCst) > 0
                    {
                        // Keep serving until shutdown (connections may close).
                        if requests_clone.lock().unwrap().len() > 8 {
                            break 'outer;
                        }
                    }
                }
            });
            Self {
                addr,
                requests,
                connections,
                shutdown: Some(tx),
                join: Some(join),
            }
        }

        fn url(&self) -> String {
            format!("http://{}/", self.addr)
        }
    }

    impl Drop for ProbeServer {
        fn drop(&mut self) {
            if let Some(tx) = self.shutdown.take() {
                let _ = tx.send(());
            }
            if let Some(join) = self.join.take() {
                let _ = join.join();
            }
        }
    }

    fn test_capabilities() -> crate::types::provider::ProviderCapabilities {
        crate::types::provider::ProviderCapabilities {
            supports_image_detail_original: false,
            streaming: true,
            tools: true,
            thinking: false,
            structured_output: false,
            web_search: false,
            prompt_cache_control: false,
            context_window: 200_000,
            max_output_tokens: 8_192,
            native_pdf: false,
            vision: false,
            image_limits: None,
            max_media_blocks: None,
        }
    }

    #[tokio::test]
    async fn probe_warms_and_a_second_request_reuses_the_pooled_connection() {
        let server = ProbeServer::start();
        let client = reqwest::Client::new();

        let outcome = probe(&client, &server.url()).await;
        assert!(outcome.warm, "probe must succeed: {outcome:?}");

        // The follow-up request through the same client must land on the
        // SAME accepted socket — proof that the prewarm connection is
        // pooled and reused (the whole point of prewarming).
        let second = client.get(server.url()).send().await.unwrap();
        let _ = second.bytes().await.unwrap();

        let requests = server.requests.lock().unwrap().clone();
        assert_eq!(requests.len(), 2, "server saw {requests:?}");
        assert_eq!(
            requests[0], requests[1],
            "both requests must share one keep-alive connection"
        );
        assert_eq!(server.connections.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn probe_reports_transport_failure_without_throwing() {
        // 127.0.0.1:1 is closed by definition on loopback.
        let client = reqwest::Client::new();
        let outcome = probe(&client, "http://127.0.0.1:1/").await;
        assert!(!outcome.warm);
        assert!(outcome.detail.contains("transport error"), "{outcome:?}");
    }

    #[tokio::test]
    async fn resolve_is_ready_after_a_successful_initial_round() {
        let server = ProbeServer::start();
        struct WarmProvider(String);
        impl crate::types::provider::ModelProvider for WarmProvider {
            fn info(&self) -> &crate::types::provider::ProviderInfo {
                static INFO: std::sync::LazyLock<crate::types::provider::ProviderInfo> =
                    std::sync::LazyLock::new(|| crate::types::provider::ProviderInfo {
                        name: crate::runtime_values::RuntimeString::from("test"),
                        base_url: None,
                        env_key: None,
                        default_model: crate::runtime_values::RuntimeString::from("m"),
                    });
                &INFO
            }
            fn capabilities(
                &self,
                _model: &crate::runtime_values::RuntimeString,
            ) -> crate::types::provider::ProviderCapabilities {
                test_capabilities()
            }
            fn stream(
                &self,
                _req: crate::types::provider::ModelRequest,
                _signal: crate::types::abort::AbortSignal,
            ) -> crate::types::provider::ModelStream {
                panic!("stream is unused in prewarm tests")
            }
            fn has_prewarm_endpoints(&self) -> bool {
                true
            }
            fn prewarm_transport(
                &self,
            ) -> crate::types::tool::BoxFuture<'static, Vec<PrewarmOutcome>> {
                let url = self.0.clone();
                Box::pin(async move {
                    let client = reqwest::Client::new();
                    vec![probe(&client, &url).await]
                })
            }
        }
        let prewarm =
            TransportPrewarm::schedule(Arc::new(WarmProvider(server.url()))).expect("scheduled");
        let resolution = prewarm.resolve(PREWARM_CONNECT_TIMEOUT).await;
        match resolution {
            PrewarmResolution::Ready(outcomes) => {
                assert_eq!(outcomes.len(), 1);
                assert!(outcomes[0].warm);
            }
            other => panic!("expected Ready, got {other:?}"),
        }
        prewarm.cancel();
    }

    #[tokio::test]
    async fn resolve_reports_failure_after_a_failed_initial_round() {
        struct ColdProvider;
        impl crate::types::provider::ModelProvider for ColdProvider {
            fn info(&self) -> &crate::types::provider::ProviderInfo {
                static INFO: std::sync::LazyLock<crate::types::provider::ProviderInfo> =
                    std::sync::LazyLock::new(|| crate::types::provider::ProviderInfo {
                        name: crate::runtime_values::RuntimeString::from("test"),
                        base_url: None,
                        env_key: None,
                        default_model: crate::runtime_values::RuntimeString::from("m"),
                    });
                &INFO
            }
            fn capabilities(
                &self,
                _model: &crate::runtime_values::RuntimeString,
            ) -> crate::types::provider::ProviderCapabilities {
                test_capabilities()
            }
            fn stream(
                &self,
                _req: crate::types::provider::ModelRequest,
                _signal: crate::types::abort::AbortSignal,
            ) -> crate::types::provider::ModelStream {
                panic!("stream is unused in prewarm tests")
            }
            fn has_prewarm_endpoints(&self) -> bool {
                true
            }
            fn prewarm_transport(
                &self,
            ) -> crate::types::tool::BoxFuture<'static, Vec<PrewarmOutcome>> {
                Box::pin(async {
                    let client = reqwest::Client::new();
                    vec![probe(&client, "http://127.0.0.1:1/").await]
                })
            }
        }
        let prewarm = TransportPrewarm::schedule(Arc::new(ColdProvider)).expect("scheduled");
        let resolution = prewarm.resolve(PREWARM_CONNECT_TIMEOUT).await;
        assert_eq!(
            resolution,
            PrewarmResolution::Unavailable { status: "failed" }
        );
        prewarm.cancel();
    }

    #[tokio::test]
    async fn resolve_times_out_when_the_probe_is_still_running() {
        struct SlowProvider;
        impl crate::types::provider::ModelProvider for SlowProvider {
            fn info(&self) -> &crate::types::provider::ProviderInfo {
                static INFO: std::sync::LazyLock<crate::types::provider::ProviderInfo> =
                    std::sync::LazyLock::new(|| crate::types::provider::ProviderInfo {
                        name: crate::runtime_values::RuntimeString::from("test"),
                        base_url: None,
                        env_key: None,
                        default_model: crate::runtime_values::RuntimeString::from("m"),
                    });
                &INFO
            }
            fn capabilities(
                &self,
                _model: &crate::runtime_values::RuntimeString,
            ) -> crate::types::provider::ProviderCapabilities {
                test_capabilities()
            }
            fn stream(
                &self,
                _req: crate::types::provider::ModelRequest,
                _signal: crate::types::abort::AbortSignal,
            ) -> crate::types::provider::ModelStream {
                panic!("stream is unused in prewarm tests")
            }
            fn has_prewarm_endpoints(&self) -> bool {
                true
            }
            fn prewarm_transport(
                &self,
            ) -> crate::types::tool::BoxFuture<'static, Vec<PrewarmOutcome>> {
                Box::pin(async {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    vec![]
                })
            }
        }
        let prewarm = TransportPrewarm::schedule(Arc::new(SlowProvider)).expect("scheduled");
        let resolution = prewarm.resolve(Duration::from_millis(150)).await;
        assert_eq!(
            resolution,
            PrewarmResolution::Unavailable {
                status: "timed_out"
            }
        );
        prewarm.cancel();
    }

    #[tokio::test]
    async fn schedule_is_skipped_for_providers_without_http_endpoints() {
        struct BareProvider;
        impl crate::types::provider::ModelProvider for BareProvider {
            fn info(&self) -> &crate::types::provider::ProviderInfo {
                static INFO: std::sync::LazyLock<crate::types::provider::ProviderInfo> =
                    std::sync::LazyLock::new(|| crate::types::provider::ProviderInfo {
                        name: crate::runtime_values::RuntimeString::from("test"),
                        base_url: None,
                        env_key: None,
                        default_model: crate::runtime_values::RuntimeString::from("m"),
                    });
                &INFO
            }
            fn capabilities(
                &self,
                _model: &crate::runtime_values::RuntimeString,
            ) -> crate::types::provider::ProviderCapabilities {
                test_capabilities()
            }
            fn stream(
                &self,
                _req: crate::types::provider::ModelRequest,
                _signal: crate::types::abort::AbortSignal,
            ) -> crate::types::provider::ModelStream {
                panic!("stream is unused in prewarm tests")
            }
        }
        assert!(TransportPrewarm::schedule(Arc::new(BareProvider)).is_none());
    }

    #[test]
    fn endpoint_origin_extracts_scheme_host_and_port() {
        assert_eq!(
            endpoint_origin("https://api.anthropic.com/v1/messages"),
            "https://api.anthropic.com/"
        );
        assert_eq!(
            endpoint_origin("http://127.0.0.1:8080/v1/chat"),
            "http://127.0.0.1:8080/"
        );
        assert_eq!(
            endpoint_origin("https://us-east5-aiplatform.googleapis.com/v1/projects/p/x"),
            "https://us-east5-aiplatform.googleapis.com/"
        );
        assert_eq!(endpoint_origin("not a url"), "not a url");
    }
}
